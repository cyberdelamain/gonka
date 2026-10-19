package host

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"common/chainoracle/blocks"
	"devshard/chainoracle/blocks/headertime"
	"devshard/heightsync"
	"devshard/internal/testutil"
	"devshard/signing"
	"devshard/state"
	"devshard/stub"
	"devshard/types"
)

type headerChain struct {
	mu      sync.Mutex
	headers map[int64]*blocks.Header
}

func newHeaderChain() *headerChain {
	return &headerChain{headers: make(map[int64]*blocks.Header)}
}

func (c *headerChain) add(height int64, hash []byte, at time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.headers[height] = blocks.HashOnlyHeader(height, at, "fake-chain", hash)
}

func (c *headerChain) Latest(context.Context) (*blocks.Header, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var top *blocks.Header
	for _, h := range c.headers {
		if top == nil || h.Height > top.Height {
			top = h
		}
	}
	if top == nil {
		return nil, blocks.ErrHeaderNotFound
	}
	return top, nil
}

func (c *headerChain) At(_ context.Context, height int64) (*blocks.Header, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if h, ok := c.headers[height]; ok {
		return h, nil
	}
	return blocks.DummyHeader(height), nil
}

func (c *headerChain) Prove(context.Context, string, int64) (*blocks.Proof, error) {
	return nil, blocks.ErrProveNotImplemented
}

func (c *headerChain) Subscribe(context.Context, int64) (<-chan *blocks.Header, error) {
	return nil, errFakeOracleNotImpl
}

func TestTimeoutVoteNeedsTheFloorBlockOnTheLocalChain(t *testing.T) {
	for _, tc := range []struct {
		name      string
		chainHash []byte
		wantErr   error
	}{
		{name: "fabricated height never lands", wantErr: types.ErrHeaderPending},
		{name: "height lands on another block", chainHash: []byte{0xbb}, wantErr: types.ErrHeaderMismatch},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hosts := []*signing.Secp256k1Signer{testutil.MustGenerateKey(t), testutil.MustGenerateKey(t), testutil.MustGenerateKey(t)}
			user := testutil.MustGenerateKey(t)
			group := testutil.MakeGroup(hosts)
			config := testutil.DefaultConfig(len(hosts))
			config.ExecutionTimeout = 1
			anchorAt := time.Unix(1_800_000_000, 0)
			hash, farHash := []byte{0xaa}, []byte{0xcc}
			chain := newHeaderChain()
			chain.add(100, hash, anchorAt)
			sm, err := state.NewStateMachine("escrow-1", config, group, 1_000_000, user.Address(), signing.NewSecp256k1Verifier(),
				testutil.MustMemoryStore(t, "escrow-1", user.Address(), config, group, 1_000_000),
				state.WithHeaderTimes(headertime.New(chain)))
			require.NoError(t, err)
			h, err := NewHost(sm, hosts[0], stub.NewInferenceEngine(), "escrow-1", group, nil, WithChainOracle(chain))
			require.NoError(t, err)

			heartbeat := &types.DevshardTx{Tx: &types.DevshardTx_Heartbeat{Heartbeat: &types.MsgHeartbeat{
				SlotsNum: uint64(len(hosts)), Reason: string(heightsync.ReasonQuietSession),
			}}}
			ack := func(ref, height uint64, blockHash []byte) *types.DevshardTx {
				msg := &types.MsgHeightAck{RefNonce: ref, SlotId: 2, ObservedHeight: height, ObservedBlockHash: blockHash,
					SyncState: types.SyncState_SYNCED, PeerSeen: []byte{0xff}}
				require.NoError(t, heightsync.SignAck(hosts[2], msg))
				return &types.DevshardTx{Tx: &types.DevshardTx_HeightAck{HeightAck: msg}}
			}
			const id uint64 = 4
			start := testutil.StartTx(id)
			start.GetStartInference().ObservedHeight = 100
			start.GetStartInference().ObservedBlockHash = hash
			receipt := testutil.SignExecutorReceipt(t, hosts[1], "escrow-1", id, testutil.TestPromptHash[:], "llama", 100, testutil.TestMaxTokens,
				testutil.ReceiptStamp{Height: 100, Hash: hash})
			confirm := &types.DevshardTx{Tx: &types.DevshardTx_ConfirmStart{ConfirmStart: &types.MsgConfirmStart{
				InferenceId: id, ExecutorSig: receipt, ObservedHeight: 100, ObservedBlockHash: hash,
			}}}
			for n, txs := range [][]*types.DevshardTx{{heartbeat}, {ack(1, 100, hash)}, {heartbeat}, {start}, {confirm}, {heartbeat}, {ack(6, 1_000_000, farHash)}} {
				_, err := sm.ApplyDiff(testutil.SignDiff(t, user, "escrow-1", uint64(n+1), txs))
				require.NoError(t, err)
			}
			rec, ok := sm.GetInference(id)
			require.True(t, ok)
			require.Equal(t, types.StatusStarted, rec.Status)
			require.Equal(t, anchorAt.UnixMilli(), rec.ConfirmedAtTimeMs, "the confirm anchor carries the block time of its floor")

			if tc.chainHash != nil {
				chain.add(1_000_000, tc.chainHash, anchorAt.Add(time.Hour))
			}
			require.ErrorIs(t, h.TimeoutDue(id, types.TimeoutReason_TIMEOUT_REASON_EXECUTION), tc.wantErr,
				"a floor height the local chain cannot prove gives no block time to count")

			if tc.chainHash != nil {
				return
			}
			chain.add(1_000_000, farHash, anchorAt.Add(500*time.Millisecond))
			require.ErrorIs(t, h.TimeoutDue(id, types.TimeoutReason_TIMEOUT_REASON_EXECUTION), types.ErrTimeoutNotDue,
				"the window is block time, not the distance in heights")
		})
	}
}

func TestApplyWaitsForAFloorBlockTheLocalChainIsAboutToSee(t *testing.T) {
	hosts := []*signing.Secp256k1Signer{testutil.MustGenerateKey(t), testutil.MustGenerateKey(t), testutil.MustGenerateKey(t)}
	user := testutil.MustGenerateKey(t)
	group := testutil.MakeGroup(hosts)
	config := testutil.DefaultConfig(len(hosts))
	hash := []byte{0xaa}
	chain := newHeaderChain()
	sm, err := state.NewStateMachine("escrow-1", config, group, 1_000_000, user.Address(), signing.NewSecp256k1Verifier(),
		testutil.MustMemoryStore(t, "escrow-1", user.Address(), config, group, 1_000_000),
		state.WithHeaderTimes(headertime.New(chain)))
	require.NoError(t, err)
	h, err := NewHost(sm, hosts[0], stub.NewInferenceEngine(), "escrow-1", group, nil, WithChainOracle(chain))
	require.NoError(t, err)

	heartbeat := &types.DevshardTx{Tx: &types.DevshardTx_Heartbeat{Heartbeat: &types.MsgHeartbeat{
		SlotsNum: uint64(len(hosts)), Reason: string(heightsync.ReasonQuietSession),
	}}}
	ackMsg := &types.MsgHeightAck{RefNonce: 1, SlotId: 2, ObservedHeight: 100, ObservedBlockHash: hash,
		SyncState: types.SyncState_SYNCED, PeerSeen: []byte{0xff}}
	require.NoError(t, heightsync.SignAck(hosts[2], ackMsg))
	ack := &types.DevshardTx{Tx: &types.DevshardTx_HeightAck{HeightAck: ackMsg}}
	start := testutil.StartTx(3)
	start.GetStartInference().ObservedHeight = 100
	start.GetStartInference().ObservedBlockHash = hash
	diffs := []types.Diff{
		testutil.SignDiff(t, user, "escrow-1", 1, []*types.DevshardTx{heartbeat}),
		testutil.SignDiff(t, user, "escrow-1", 2, []*types.DevshardTx{ack}),
		testutil.SignDiff(t, user, "escrow-1", 3, []*types.DevshardTx{start}),
	}

	go func() {
		time.Sleep(300 * time.Millisecond)
		chain.add(100, hash, time.Unix(1_800_000_000, 0))
	}()
	h.mu.Lock()
	applied, err := h.applyDiffsAwaitingHeadersLocked(context.Background(), diffs)
	h.mu.Unlock()
	require.NoError(t, err, "a host one block behind waits for its node instead of rejecting the diff")
	require.NotEmpty(t, applied)
	require.Equal(t, uint64(3), sm.LatestNonce())
	rec, ok := sm.GetInference(3)
	require.True(t, ok)
	require.Equal(t, time.Unix(1_800_000_000, 0).UnixMilli(), rec.StartedAtTimeMs)
}
