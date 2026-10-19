package observer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"common/chainoracle/blocks"

	"github.com/cometbft/cometbft/light"
	ctypes "github.com/cometbft/cometbft/rpc/core/types"
	cmttypes "github.com/cometbft/cometbft/types"
)

const (
	DefaultTrustingPeriod = 7 * 24 * time.Hour
	DefaultMaxClockDrift  = 10 * time.Second
	validatorsPerPage     = 100
	validatorsMaxPages    = 100
)

var (
	ErrUnverifiedHeader = errors.New("observer: header failed light verification")
	errBelowTrusted     = errors.New("observer: height is below the trusted header")
)

type lightRPC interface {
	Commit(ctx context.Context, height *int64) (*ctypes.ResultCommit, error)
	Validators(ctx context.Context, height *int64, page, perPage *int) (*ctypes.ResultValidators, error)
}

type TrustPoint struct {
	Height int64
	Hash   []byte
}

type lightVerifier struct {
	rpc            lightRPC
	trustingPeriod time.Duration
	maxClockDrift  time.Duration
	now            func() time.Time
	saveTrust      func(TrustPoint)

	mu          sync.Mutex
	trusted     *cmttypes.SignedHeader
	trustedVals *cmttypes.ValidatorSet
	pinned      *TrustPoint
}

func newLightVerifier(rpc lightRPC, trustingPeriod time.Duration) *lightVerifier {
	if trustingPeriod <= 0 {
		trustingPeriod = DefaultTrustingPeriod
	}
	return &lightVerifier{
		rpc:            rpc,
		trustingPeriod: trustingPeriod,
		maxClockDrift:  DefaultMaxClockDrift,
		now:            time.Now,
	}
}

func (v *lightVerifier) pin(tp TrustPoint) {
	v.mu.Lock()
	defer v.mu.Unlock()
	cp := TrustPoint{Height: tp.Height, Hash: append([]byte(nil), tp.Hash...)}
	v.pinned = &cp
}

func (v *lightVerifier) trustedHeight() int64 {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.trusted == nil {
		return 0
	}
	return v.trusted.Height
}

func (v *lightVerifier) signedHeader(ctx context.Context, height *int64) (*cmttypes.SignedHeader, error) {
	res, err := v.rpc.Commit(ctx, height)
	if err != nil {
		if isCometHeightErr(err) {
			return nil, blocks.ErrHeaderNotFound
		}
		return nil, fmt.Errorf("observer: commit rpc: %w", err)
	}
	if res == nil || res.Header == nil || res.Commit == nil {
		return nil, blocks.ErrHeaderNotFound
	}
	sh := res.SignedHeader
	return &sh, nil
}

func (v *lightVerifier) validators(ctx context.Context, height int64) (*cmttypes.ValidatorSet, error) {
	var all []*cmttypes.Validator
	perPage := validatorsPerPage
	for page := 1; page <= validatorsMaxPages; page++ {
		p := page
		res, err := v.rpc.Validators(ctx, &height, &p, &perPage)
		if err != nil {
			return nil, fmt.Errorf("observer: validators rpc at %d: %w", height, err)
		}
		all = append(all, res.Validators...)
		if len(res.Validators) == 0 || len(all) >= res.Total {
			break
		}
	}
	vals, err := cmttypes.ValidatorSetFromExistingValidators(all)
	if err != nil {
		return nil, fmt.Errorf("observer: validator set at %d: %w", height, err)
	}
	return vals, nil
}

func (v *lightVerifier) selfVerified(ctx context.Context, height *int64) (*cmttypes.SignedHeader, *cmttypes.ValidatorSet, error) {
	sh, err := v.signedHeader(ctx, height)
	if err != nil {
		return nil, nil, err
	}
	if err := sh.ValidateBasic(sh.ChainID); err != nil {
		return nil, nil, fmt.Errorf("%w: %v", ErrUnverifiedHeader, err)
	}
	vals, err := v.validators(ctx, sh.Height)
	if err != nil {
		return nil, nil, err
	}
	if !bytes.Equal(vals.Hash(), sh.ValidatorsHash) {
		return nil, nil, fmt.Errorf("%w: validator set at %d does not hash to the header", ErrUnverifiedHeader, sh.Height)
	}
	if err := vals.VerifyCommitLight(sh.ChainID, sh.Commit.BlockID, sh.Height, sh.Commit); err != nil {
		return nil, nil, fmt.Errorf("%w: %v", ErrUnverifiedHeader, err)
	}
	return sh, vals, nil
}

func (v *lightVerifier) bootstrapLocked(ctx context.Context) error {
	if v.pinned != nil {
		h := v.pinned.Height
		sh, vals, err := v.selfVerified(ctx, &h)
		if err == nil && bytes.Equal(sh.Hash(), v.pinned.Hash) && !light.HeaderExpired(sh, v.trustingPeriod, v.now()) {
			v.trusted, v.trustedVals = sh, vals
			return nil
		}
		v.pinned = nil
	}
	sh, vals, err := v.selfVerified(ctx, nil)
	if err != nil {
		return err
	}
	v.trusted, v.trustedVals = sh, vals
	v.persistLocked()
	return nil
}

func (v *lightVerifier) persistLocked() {
	if v.saveTrust != nil && v.trusted != nil {
		v.saveTrust(TrustPoint{Height: v.trusted.Height, Hash: v.trusted.Hash().Bytes()})
	}
}

func (v *lightVerifier) ensureTrusted(ctx context.Context) (*cmttypes.Header, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.trusted == nil {
		if err := v.bootstrapLocked(ctx); err != nil {
			return nil, err
		}
	}
	return v.trusted.Header, nil
}

func (v *lightVerifier) verifyForward(ctx context.Context, height int64) (*cmttypes.Header, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.trusted == nil {
		if err := v.bootstrapLocked(ctx); err != nil {
			return nil, err
		}
	}
	if height == v.trusted.Height {
		hdr := v.trusted.Header
		return hdr, nil
	}
	if height < v.trusted.Height {
		return nil, fmt.Errorf("%w: %d < %d", errBelowTrusted, height, v.trusted.Height)
	}
	sh, err := v.signedHeader(ctx, &height)
	if err != nil {
		return nil, err
	}
	vals := v.trustedVals
	if !bytes.Equal(vals.Hash(), sh.ValidatorsHash) {
		if vals, err = v.validators(ctx, height); err != nil {
			return nil, err
		}
	}
	err = light.Verify(v.trusted, v.trustedVals, sh, vals, v.trustingPeriod, v.now(), v.maxClockDrift, light.DefaultTrustLevel)
	var expired light.ErrOldHeaderExpired
	if errors.As(err, &expired) {
		v.trusted, v.trustedVals = nil, nil
		if berr := v.bootstrapLocked(ctx); berr != nil {
			return nil, berr
		}
		if height == v.trusted.Height {
			hdr := v.trusted.Header
			return hdr, nil
		}
		if height < v.trusted.Height {
			return nil, fmt.Errorf("%w: %d < %d", errBelowTrusted, height, v.trusted.Height)
		}
		err = light.Verify(v.trusted, v.trustedVals, sh, vals, v.trustingPeriod, v.now(), v.maxClockDrift, light.DefaultTrustLevel)
	}
	if err != nil {
		return nil, fmt.Errorf("%w: height %d: %v", ErrUnverifiedHeader, height, err)
	}
	v.trusted, v.trustedVals = sh, vals
	v.persistLocked()
	return sh.Header, nil
}

func verifyBackwardLink(older *cmttypes.Header, newer historyEntry, newerHeight int64, chainID string) error {
	if older == nil {
		return fmt.Errorf("%w: missing header", ErrUnverifiedHeader)
	}
	if err := older.ValidateBasic(); err != nil {
		return fmt.Errorf("%w: height %d: %v", ErrUnverifiedHeader, older.Height, err)
	}
	if older.Height != newerHeight-1 {
		return fmt.Errorf("%w: height %d does not precede %d", ErrUnverifiedHeader, older.Height, newerHeight)
	}
	if chainID != "" && older.ChainID != chainID {
		return fmt.Errorf("%w: height %d is on chain %q", ErrUnverifiedHeader, older.Height, older.ChainID)
	}
	if len(newer.prevHash) == 0 || !bytes.Equal(older.Hash(), newer.prevHash) {
		return fmt.Errorf("%w: height %d does not hash to the link from %d", ErrUnverifiedHeader, older.Height, newerHeight)
	}
	if !older.Time.Before(time.UnixMilli(newer.timeMs).Add(time.Millisecond)) {
		return fmt.Errorf("%w: height %d is not older than %d", ErrUnverifiedHeader, older.Height, newerHeight)
	}
	return nil
}
