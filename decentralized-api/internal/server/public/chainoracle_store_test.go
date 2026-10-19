package public

import (
	"context"
	"path/filepath"
	"testing"

	"common/chainoracle/blocks/observer"
	"decentralized-api/apiconfig"

	"github.com/stretchr/testify/require"
)

func TestChainOracleHeaderStoreRoundTripsOnSQLite(t *testing.T) {
	db, err := apiconfig.OpenSQLite(apiconfig.SqliteConfig{Path: filepath.Join(t.TempDir(), "dapi.db")})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	ctx := context.Background()
	store, err := observer.NewSQLiteHeaderStore(ctx, db)
	require.NoError(t, err)
	_, err = observer.NewSQLiteHeaderStore(ctx, db)
	require.NoError(t, err, "the schema is created once and reused")

	rows := []observer.StoredHeader{
		{Height: 10, Hash: []byte{0x0a}, PrevHash: []byte{0x09}, TimeMs: 1000, ChainID: "c"},
		{Height: 11, Hash: []byte{0x0b}, TimeMs: 1005, ChainID: "c"},
		{Height: 12, Hash: []byte{0x0c}, PrevHash: []byte{0x0b}, TimeMs: 1010, ChainID: "c"},
	}
	require.NoError(t, store.SaveHeaders(ctx, rows))
	require.NoError(t, store.SaveHeaders(ctx, []observer.StoredHeader{{Height: 11, Hash: []byte{0x0b}, PrevHash: []byte{0x0a}, TimeMs: 1005, ChainID: "c"}}))
	require.NoError(t, store.SaveHeaders(ctx, []observer.StoredHeader{{Height: 12, Hash: []byte{0x0c}, TimeMs: 1010, ChainID: "c"}}))

	top, err := store.MaxHeight(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(12), top)
	got, err := store.LoadHeaders(ctx, 11)
	require.NoError(t, err)
	require.Len(t, got, 2)
	require.Equal(t, []byte{0x0a}, got[0].PrevHash, "a later link fills the missing one")
	require.Equal(t, []byte{0x0b}, got[1].PrevHash, "a row without a link keeps the stored one")

	require.NoError(t, store.PruneHeaders(ctx, 12))
	got, err = store.LoadHeaders(ctx, 0)
	require.NoError(t, err)
	require.Len(t, got, 1)

	_, ok, err := store.LoadTrust(ctx)
	require.NoError(t, err)
	require.False(t, ok)
	require.NoError(t, store.SaveTrust(ctx, observer.TrustPoint{Height: 12, Hash: []byte{0x0c}}))
	require.NoError(t, store.SaveTrust(ctx, observer.TrustPoint{Height: 13, Hash: []byte{0x0d}}))
	tp, ok, err := store.LoadTrust(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, int64(13), tp.Height)
	require.Equal(t, []byte{0x0d}, tp.Hash)
}
