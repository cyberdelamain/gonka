package user

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"common/chainoracle/blocks"
	"devshard/heightsync"
)

type liarOracle struct {
	*sessionOracle
	now func() time.Time
}

func (o *liarOracle) Latest(ctx context.Context) (*blocks.Header, error) {
	return blocks.HashOnlyHeader(o.height.Load(), o.now(), "fake-chain", append([]byte(nil), o.hash...)), nil
}
func (o *liarOracle) At(ctx context.Context, _ int64) (*blocks.Header, error) { return o.Latest(ctx) }

func TestOneHostFutureAckClosesChatGate(t *testing.T) {
	interval := heightsync.DefaultHeartbeatInterval
	clock := &steppedClock{now: time.Unix(1_700_000_000, 0).UTC()}
	oracles, followers := sessionOracles(3, 100)
	oracles[1] = &liarOracle{sessionOracle: followers[1], now: clock.Now}
	times := &chatGateTimes{}
	times.open.Store(true)
	times.set(100, clock.Now().Add(-time.Second).UnixMilli())
	session := openChatSession(t, oracles, times,
		WithHeartbeatClock(clock.Now),
		WithHeartbeatConfig(heightsync.HeartbeatConfig{Interval: interval}))
	ctx := context.Background()
	require.NoError(t, session.MaybeHeartbeat(ctx))
	require.NoError(t, session.WaitFloorChat(ctx))

	followers[1].height.Store(1_000_000)
	for i := 0; i < 6; i++ {
		clock.Advance(interval)
		h := uint64(200 + 100*i)
		times.set(h, clock.Now().Add(-time.Second).UnixMilli())
		followers[0].height.Store(int64(h))
		followers[2].height.Store(int64(h))
		_ = session.MaybeHeartbeat(ctx)
	}
	floor, _, _ := session.StateMachine().HeightSyncFloorAsOf(session.Nonce() + 1)
	require.Equal(t, uint64(1_000_000), floor)
	w, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()
	require.Error(t, session.WaitFloorChat(w))
}
