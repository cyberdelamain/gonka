package state

import (
	"context"
	"devshard/types"
	"errors"
	"time"
)

type HeaderTimeSource interface {
	HeaderTimeMs(height uint64, hash []byte) (int64, error)
}

var fallbackHeaderTimes HeaderTimeSource

func SetFallbackHeaderTimes(src HeaderTimeSource) {
	fallbackHeaderTimes = src
}

const (
	HeaderPendingRetryInterval = time.Second
	ReplayHeaderWait           = 2 * time.Minute
)

func RetryWhileHeaderPending(ctx context.Context, maxWait time.Duration, apply func() error) error {
	err := apply()
	if !errors.Is(err, types.ErrHeaderPending) {
		return err
	}
	deadline := time.Now().Add(maxWait)
	for errors.Is(err, types.ErrHeaderPending) && time.Now().Before(deadline) {
		timer := time.NewTimer(HeaderPendingRetryInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return err
		case <-timer.C:
		}
		err = apply()
	}
	return err
}
