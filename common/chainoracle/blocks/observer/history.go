package observer

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"

	"common/chainoracle/blocks"

	ctypes "github.com/cometbft/cometbft/rpc/core/types"
)

const (
	DefaultHistoryHeights = 131_072
	BlockchainInfoBatch   = 20
)

type blockchainInfoRPC interface {
	BlockchainInfo(ctx context.Context, minHeight, maxHeight int64) (*ctypes.ResultBlockchainInfo, error)
}

type historyEntry struct {
	hash   []byte
	timeMs int64
}

type History struct {
	window int64

	mu      sync.RWMutex
	chainID string
	top     int64
	entries map[int64]historyEntry
}

func NewHistory(window int64) *History {
	if window <= 0 {
		window = DefaultHistoryHeights
	}
	return &History{window: window, entries: make(map[int64]historyEntry)}
}

func (h *History) Put(hdr *blocks.Header) {
	if h == nil || hdr == nil || hdr.Height <= 0 || hdr.Time.IsZero() || len(hdr.BlockHash) == 0 {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if hdr.ChainID != "" {
		h.chainID = hdr.ChainID
	}
	h.entries[hdr.Height] = historyEntry{hash: append([]byte(nil), hdr.BlockHash...), timeMs: hdr.Time.UnixMilli()}
	if hdr.Height > h.top {
		h.top = hdr.Height
		h.evictLocked()
	}
}

func (h *History) evictLocked() {
	floor := h.top - h.window
	if floor <= 0 || int64(len(h.entries)) <= h.window {
		return
	}
	for height := range h.entries {
		if height <= floor {
			delete(h.entries, height)
		}
	}
}

func (h *History) Get(height int64) (*blocks.Header, bool) {
	if h == nil {
		return nil, false
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	e, ok := h.entries[height]
	if !ok {
		return nil, false
	}
	return blocks.HashOnlyHeader(height, time.UnixMilli(e.timeMs).UTC(), h.chainID, e.hash), true
}

func (h *History) Len() int {
	if h == nil {
		return 0
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.entries)
}

func headersFromBlockchainInfo(res *ctypes.ResultBlockchainInfo) ([]*blocks.Header, error) {
	if res == nil {
		return nil, nil
	}
	out := make([]*blocks.Header, 0, len(res.BlockMetas))
	for _, meta := range res.BlockMetas {
		if meta == nil {
			continue
		}
		hash := meta.Header.Hash().Bytes()
		if len(hash) == 0 || len(meta.BlockID.Hash) > 0 && !bytes.Equal(hash, meta.BlockID.Hash.Bytes()) {
			return nil, fmt.Errorf("observer: block %d header hash does not match block id", meta.Header.Height)
		}
		out = append(out, blocks.HashOnlyHeader(meta.Header.Height, meta.Header.Time, meta.Header.ChainID, hash))
	}
	return out, nil
}
