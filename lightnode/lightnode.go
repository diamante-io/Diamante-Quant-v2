package lightnode

import (
	"context"
	"database/sql"
	"time"

	_ "modernc.org/sqlite"
)

type LightNode struct {
	db *sql.DB
}

func New(dbPath string) (*LightNode, error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, err
	}
	if _, err := db.Exec(`PRAGMA journal_mode=WAL;`); err != nil {
		db.Close()
		return nil, err
	}
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS blocks (height INTEGER PRIMARY KEY, header BLOB);`); err != nil {
		db.Close()
		return nil, err
	}
	return &LightNode{db: db}, nil
}

func (ln *LightNode) Close() error {
	return ln.db.Close()
}

func (ln *LightNode) AddBlock(height int, header []byte) error {
	_, err := ln.db.Exec(`INSERT OR REPLACE INTO blocks (height, header) VALUES (?, ?)`, height, header)
	return err
}

func (ln *LightNode) GetBlock(height int) ([]byte, error) {
	var header []byte
	row := ln.db.QueryRow(`SELECT header FROM blocks WHERE height=?`, height)
	err := row.Scan(&header)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return header, err
}

func (ln *LightNode) Compact(retain int) error {
	var maxHeight sql.NullInt64
	if err := ln.db.QueryRow(`SELECT MAX(height) FROM blocks`).Scan(&maxHeight); err != nil {
		return err
	}
	if !maxHeight.Valid {
		return nil
	}
	cutoff := int(maxHeight.Int64) - retain
	if cutoff <= 0 {
		return nil
	}
	_, err := ln.db.Exec(`DELETE FROM blocks WHERE height < ?`, cutoff)
	return err
}

func (ln *LightNode) Sync(ctx context.Context, net Network) error {
	headers := net.StreamHeaders(ctx)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case h, ok := <-headers:
			if !ok {
				return nil
			}
			if err := ln.AddBlock(h.Height, h.Header); err != nil {
				return err
			}
			if h.Height%1000 == 0 {
				if err := ln.Compact(1000); err != nil {
					return err
				}
				// Use context-aware timer instead of time.Sleep
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(100 * time.Millisecond):
					// Continue processing
				}
			}
		}
	}
}
