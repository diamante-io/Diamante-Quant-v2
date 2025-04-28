package mobile

import (
	"database/sql"
	"fmt"

	_ "github.com/mattn/go-sqlite3" // SQLite driver

	"diamante/common"
)

// SQLiteLightNode implements a lightweight ledger store for mobile nodes.
type SQLiteLightNode struct {
	db *sql.DB
}

// NewSQLiteLightNode opens (or creates) an SQLite database at the given path.
func NewSQLiteLightNode(dbPath string) (*SQLiteLightNode, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open SQLite db: %w", err)
	}
	node := &SQLiteLightNode{db: db}
	if err := node.migrateSchema(); err != nil {
		return nil, fmt.Errorf("failed to migrate schema: %w", err)
	}
	return node, nil
}

// migrateSchema creates required tables.
func (s *SQLiteLightNode) migrateSchema() error {
	schema := []string{
		// block_headers table for pruned block header data.
		`CREATE TABLE IF NOT EXISTS block_headers (
			block_number INTEGER PRIMARY KEY,
			hash TEXT NOT NULL,
			previous_hash TEXT,
			timestamp INTEGER NOT NULL,
			merkle_root TEXT
		);`,
		// accounts table.
		`CREATE TABLE IF NOT EXISTS accounts (
			id TEXT PRIMARY KEY,
			public_key BLOB,
			balance REAL,
			nonce INTEGER
		);`,
		// transactions table for recent transactions.
		`CREATE TABLE IF NOT EXISTS transactions (
			tx_id TEXT PRIMARY KEY,
			sender TEXT,
			receiver TEXT,
			amount REAL,
			fee REAL,
			timestamp INTEGER,
			signature BLOB,
			nonce INTEGER,
			status TEXT
		);`,
		// sync_metadata table to track last sync status.
		`CREATE TABLE IF NOT EXISTS sync_metadata (
			key TEXT PRIMARY KEY,
			value TEXT
		);`,
	}
	for _, q := range schema {
		_, err := s.db.Exec(q)
		if err != nil {
			return fmt.Errorf("failed to execute schema query %q: %w", q, err)
		}
	}
	return nil
}

// Close closes the SQLite database.
func (s *SQLiteLightNode) Close() error {
	return s.db.Close()
}

// SaveBlockHeader saves a pruned block header.
func (s *SQLiteLightNode) SaveBlockHeader(header common.Block) error {
	query := `
		INSERT OR REPLACE INTO block_headers (block_number, hash, previous_hash, timestamp, merkle_root)
		VALUES (?, ?, ?, ?, ?);`
	_, err := s.db.Exec(query, header.Number, header.Hash, header.PreviousHash, header.Timestamp, header.MerkleRoot)
	return err
}

// GetLatestBlockHeader returns the most recent block header.
func (s *SQLiteLightNode) GetLatestBlockHeader() (common.Block, error) {
	query := `
		SELECT block_number, hash, previous_hash, timestamp, merkle_root
		FROM block_headers
		ORDER BY block_number DESC LIMIT 1;`
	row := s.db.QueryRow(query)
	var header common.Block
	err := row.Scan(&header.Number, &header.Hash, &header.PreviousHash, &header.Timestamp, &header.MerkleRoot)
	if err != nil {
		return common.Block{}, err
	}
	return header, nil
}

// SaveAccount saves or updates an account.
func (s *SQLiteLightNode) SaveAccount(ac *common.Account) error {
	query := `
		INSERT OR REPLACE INTO accounts (id, public_key, balance, nonce)
		VALUES (?, ?, ?, ?);`
	_, err := s.db.Exec(query, ac.ID, ac.PublicKey, ac.Balance, ac.Nonce)
	return err
}

// GetAccount retrieves an account by id.
func (s *SQLiteLightNode) GetAccount(accountID string) (*common.Account, error) {
	query := `
		SELECT id, public_key, balance, nonce
		FROM accounts
		WHERE id = ?;`
	row := s.db.QueryRow(query, accountID)
	var ac common.Account
	err := row.Scan(&ac.ID, &ac.PublicKey, &ac.Balance, &ac.Nonce)
	if err != nil {
		return nil, err
	}
	return &ac, nil
}

// SaveTransaction saves a transaction.
func (s *SQLiteLightNode) SaveTransaction(tx *common.Transaction) error {
	query := `
		INSERT OR REPLACE INTO transactions (tx_id, sender, receiver, amount, fee, timestamp, signature, nonce, status)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);`
	_, err := s.db.Exec(query, tx.ID, tx.Sender, tx.Receiver, tx.Amount, tx.Fee, tx.Timestamp, tx.Signature, tx.Nonce, "pending")
	return err
}

// GetTransactionsForAccount retrieves transactions where the account is either sender or receiver.
func (s *SQLiteLightNode) GetTransactionsForAccount(accountID string) ([]common.Transaction, error) {
	query := `
		SELECT tx_id, sender, receiver, amount, fee, timestamp, signature, nonce, status
		FROM transactions
		WHERE sender = ? OR receiver = ?
		ORDER BY timestamp DESC;`
	rows, err := s.db.Query(query, accountID, accountID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var txs []common.Transaction
	for rows.Next() {
		var tx common.Transaction
		err := rows.Scan(&tx.ID, &tx.Sender, &tx.Receiver, &tx.Amount, &tx.Fee, &tx.Timestamp, &tx.Signature, &tx.Nonce, &tx.Status)
		if err != nil {
			return nil, err
		}
		txs = append(txs, tx)
	}
	return txs, nil
}

// SaveSyncMetadata saves a key/value pair for synchronization metadata.
func (s *SQLiteLightNode) SaveSyncMetadata(key, value string) error {
	query := `
		INSERT OR REPLACE INTO sync_metadata (key, value)
		VALUES (?, ?);`
	_, err := s.db.Exec(query, key, value)
	return err
}

// GetSyncMetadata retrieves the synchronization metadata value for a key.
func (s *SQLiteLightNode) GetSyncMetadata(key string) (string, error) {
	query := `
		SELECT value FROM sync_metadata WHERE key = ?;`
	row := s.db.QueryRow(query, key)
	var value string
	err := row.Scan(&value)
	return value, err
}
