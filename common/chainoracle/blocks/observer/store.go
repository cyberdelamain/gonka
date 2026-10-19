package observer

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

type HeaderStore interface {
	LoadHeaders(ctx context.Context, fromHeight int64) ([]StoredHeader, error)
	SaveHeaders(ctx context.Context, headers []StoredHeader) error
	PruneHeaders(ctx context.Context, belowHeight int64) error
	LoadTrust(ctx context.Context) (TrustPoint, bool, error)
	SaveTrust(ctx context.Context, tp TrustPoint) error
	MaxHeight(ctx context.Context) (int64, error)
}

type SQLiteHeaderStore struct {
	db *sql.DB
}

func NewSQLiteHeaderStore(ctx context.Context, db *sql.DB) (*SQLiteHeaderStore, error) {
	if db == nil {
		return nil, errors.New("observer: nil header store db")
	}
	const schema = `
CREATE TABLE IF NOT EXISTS chainoracle_headers (
  height INTEGER PRIMARY KEY,
  hash BLOB NOT NULL,
  prev_hash BLOB,
  time_ms INTEGER NOT NULL,
  chain_id TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS chainoracle_trust (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  height INTEGER NOT NULL,
  hash BLOB NOT NULL
);`
	if _, err := db.ExecContext(ctx, schema); err != nil {
		return nil, fmt.Errorf("observer: header store schema: %w", err)
	}
	return &SQLiteHeaderStore{db: db}, nil
}

func (s *SQLiteHeaderStore) LoadHeaders(ctx context.Context, fromHeight int64) ([]StoredHeader, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT height, hash, prev_hash, time_ms, chain_id FROM chainoracle_headers WHERE height >= ? ORDER BY height`, fromHeight)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []StoredHeader
	for rows.Next() {
		var h StoredHeader
		if err := rows.Scan(&h.Height, &h.Hash, &h.PrevHash, &h.TimeMs, &h.ChainID); err != nil {
			return nil, err
		}
		out = append(out, h)
	}
	return out, rows.Err()
}

func (s *SQLiteHeaderStore) SaveHeaders(ctx context.Context, headers []StoredHeader) error {
	if len(headers) == 0 {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	stmt, err := tx.PrepareContext(ctx,
		`INSERT INTO chainoracle_headers (height, hash, prev_hash, time_ms, chain_id) VALUES (?, ?, ?, ?, ?)
ON CONFLICT(height) DO UPDATE SET hash = excluded.hash,
  prev_hash = COALESCE(excluded.prev_hash, chainoracle_headers.prev_hash),
  time_ms = excluded.time_ms, chain_id = excluded.chain_id`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, h := range headers {
		var prev any
		if len(h.PrevHash) > 0 {
			prev = h.PrevHash
		}
		if _, err := stmt.ExecContext(ctx, h.Height, h.Hash, prev, h.TimeMs, h.ChainID); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *SQLiteHeaderStore) PruneHeaders(ctx context.Context, belowHeight int64) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM chainoracle_headers WHERE height < ?`, belowHeight)
	return err
}

func (s *SQLiteHeaderStore) LoadTrust(ctx context.Context) (TrustPoint, bool, error) {
	var tp TrustPoint
	err := s.db.QueryRowContext(ctx, `SELECT height, hash FROM chainoracle_trust WHERE id = 1`).Scan(&tp.Height, &tp.Hash)
	if errors.Is(err, sql.ErrNoRows) {
		return TrustPoint{}, false, nil
	}
	if err != nil {
		return TrustPoint{}, false, err
	}
	return tp, true, nil
}

func (s *SQLiteHeaderStore) SaveTrust(ctx context.Context, tp TrustPoint) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO chainoracle_trust (id, height, hash) VALUES (1, ?, ?)
ON CONFLICT(id) DO UPDATE SET height = excluded.height, hash = excluded.hash`, tp.Height, tp.Hash)
	return err
}

func (s *SQLiteHeaderStore) MaxHeight(ctx context.Context) (int64, error) {
	var h sql.NullInt64
	if err := s.db.QueryRowContext(ctx, `SELECT MAX(height) FROM chainoracle_headers`).Scan(&h); err != nil {
		return 0, err
	}
	return h.Int64, nil
}
