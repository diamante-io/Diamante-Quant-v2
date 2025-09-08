package mobile

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3" // SQLite driver

	"diamante/common"
)

// Custom error types for better error handling
var (
	// ErrInvalidInput represents validation errors for input parameters
	ErrInvalidInput = errors.New("invalid input parameter")
	// ErrDatabaseConnection represents database connection issues
	ErrDatabaseConnection = errors.New("database connection error")
	// ErrTransactionFailed represents transaction operation failures
	ErrTransactionFailed = errors.New("transaction operation failed")
	// ErrRecordNotFound represents when a requested record doesn't exist
	ErrRecordNotFound = errors.New("record not found")
	// ErrDuplicateRecord represents when trying to create a duplicate record
	ErrDuplicateRecord = errors.New("duplicate record")
)

// TransactionStatus represents the possible states of a transaction
type TransactionStatus string

const (
	// StatusPending indicates a transaction is pending confirmation
	StatusPending TransactionStatus = "pending"
	// StatusConfirmed indicates a transaction has been confirmed
	StatusConfirmed TransactionStatus = "confirmed"
	// StatusFailed indicates a transaction has failed
	StatusFailed TransactionStatus = "failed"
)

// String returns the string representation of TransactionStatus
func (ts TransactionStatus) String() string {
	return string(ts)
}

// IsValid checks if the transaction status is valid
func (ts TransactionStatus) IsValid() bool {
	switch ts {
	case StatusPending, StatusConfirmed, StatusFailed:
		return true
	default:
		return false
	}
}

// Config holds configuration options for SQLiteLightNode
type Config struct {
	// DatabasePath is the file path for the SQLite database
	DatabasePath string
	// ConnectionTimeout is the timeout for database connections
	ConnectionTimeout time.Duration
	// MaxOpenConnections is the maximum number of open connections
	MaxOpenConnections int
	// MaxIdleConnections is the maximum number of idle connections
	MaxIdleConnections int
	// ConnectionMaxLifetime is the maximum lifetime of connections
	ConnectionMaxLifetime time.Duration
	// EnableWALMode enables Write-Ahead Logging mode for better performance
	EnableWALMode bool
	// BusyTimeout is the timeout for busy database operations
	BusyTimeout time.Duration
}

// DefaultConfig returns a default configuration for SQLiteLightNode
func DefaultConfig() *Config {
	return &Config{
		ConnectionTimeout:     30 * time.Second,
		MaxOpenConnections:    25,
		MaxIdleConnections:    10,
		ConnectionMaxLifetime: 5 * time.Minute,
		EnableWALMode:         true,
		BusyTimeout:           30 * time.Second,
	}
}

// SQLiteLightNode implements a lightweight ledger store for mobile nodes with
// comprehensive error handling, input validation, and transaction support.
type SQLiteLightNode struct {
	db     *sql.DB
	config *Config
	mu     sync.RWMutex

	// Prepared statements for performance
	stmts struct {
		saveBlockHeader        *sql.Stmt
		getLatestBlockHeader   *sql.Stmt
		saveAccount            *sql.Stmt
		getAccount             *sql.Stmt
		saveTransaction        *sql.Stmt
		getTransactionsForAcct *sql.Stmt
		saveSyncMetadata       *sql.Stmt
		getSyncMetadata        *sql.Stmt
	}
}

// NewSQLiteLightNode creates a new SQLite-based lightweight node with the given configuration.
// It performs comprehensive validation, establishes database connection, and prepares statements.
func NewSQLiteLightNode(config *Config) (*SQLiteLightNode, error) {
	if config == nil {
		return nil, fmt.Errorf("%w: config cannot be nil", ErrInvalidInput)
	}

	if err := validateConfig(config); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	// Construct connection string with optimized settings
	connStr := config.DatabasePath
	if config.EnableWALMode {
		connStr += "?_journal_mode=WAL&_synchronous=NORMAL&_cache_size=10000"
	}
	if config.BusyTimeout > 0 {
		timeout := int(config.BusyTimeout.Milliseconds())
		if strings.Contains(connStr, "?") {
			connStr += fmt.Sprintf("&_busy_timeout=%d", timeout)
		} else {
			connStr += fmt.Sprintf("?_busy_timeout=%d", timeout)
		}
	}

	// Open database connection with timeout context
	ctx, cancel := context.WithTimeout(context.Background(), config.ConnectionTimeout)
	defer cancel()

	db, err := sql.Open("sqlite3", connStr)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to open SQLite database at %s: %w",
			ErrDatabaseConnection, config.DatabasePath, err)
	}

	// Configure connection pool
	db.SetMaxOpenConns(config.MaxOpenConnections)
	db.SetMaxIdleConns(config.MaxIdleConnections)
	db.SetConnMaxLifetime(config.ConnectionMaxLifetime)

	// Test connection
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("%w: failed to ping database: %w", ErrDatabaseConnection, err)
	}

	node := &SQLiteLightNode{
		db:     db,
		config: config,
	}

	// Initialize database schema with transaction support
	if err := node.migrateSchema(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to migrate database schema: %w", err)
	}

	// Prepare statements for performance
	if err := node.prepareStatements(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to prepare database statements: %w", err)
	}

	return node, nil
}

// validateConfig performs comprehensive validation of the configuration
func validateConfig(config *Config) error {
	if config.DatabasePath == "" {
		return fmt.Errorf("%w: database path cannot be empty", ErrInvalidInput)
	}

	if config.ConnectionTimeout <= 0 {
		return fmt.Errorf("%w: connection timeout must be positive", ErrInvalidInput)
	}

	if config.MaxOpenConnections <= 0 {
		return fmt.Errorf("%w: max open connections must be positive", ErrInvalidInput)
	}

	if config.MaxIdleConnections < 0 {
		return fmt.Errorf("%w: max idle connections cannot be negative", ErrInvalidInput)
	}

	if config.MaxIdleConnections > config.MaxOpenConnections {
		return fmt.Errorf("%w: max idle connections cannot exceed max open connections", ErrInvalidInput)
	}

	if config.ConnectionMaxLifetime <= 0 {
		return fmt.Errorf("%w: connection max lifetime must be positive", ErrInvalidInput)
	}

	if config.BusyTimeout < 0 {
		return fmt.Errorf("%w: busy timeout cannot be negative", ErrInvalidInput)
	}

	return nil
}

// migrateSchema creates required tables with comprehensive error handling and transaction support
func (s *SQLiteLightNode) migrateSchema(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("%w: failed to begin schema migration transaction: %w", ErrTransactionFailed, err)
	}
	defer func() {
		if err != nil {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				err = fmt.Errorf("schema migration failed and rollback failed: %v (original error: %w)", rollbackErr, err)
			}
		}
	}()

	schema := []struct {
		name  string
		query string
	}{
		{
			name: "block_headers",
			query: `CREATE TABLE IF NOT EXISTS block_headers (
				block_number INTEGER PRIMARY KEY,
				hash TEXT NOT NULL UNIQUE,
				previous_hash TEXT,
				timestamp INTEGER NOT NULL CHECK (timestamp > 0),
				merkle_root TEXT,
				created_at INTEGER DEFAULT (strftime('%s', 'now')),
				CONSTRAINT valid_block_number CHECK (block_number >= 0)
			);`,
		},
		{
			name: "accounts",
			query: `CREATE TABLE IF NOT EXISTS accounts (
				id TEXT PRIMARY KEY,
				public_key BLOB NOT NULL,
				balance REAL NOT NULL DEFAULT 0.0 CHECK (balance >= 0),
				nonce INTEGER NOT NULL DEFAULT 0 CHECK (nonce >= 0),
				created_at INTEGER DEFAULT (strftime('%s', 'now')),
				updated_at INTEGER DEFAULT (strftime('%s', 'now'))
			);`,
		},
		{
			name: "transactions",
			query: `CREATE TABLE IF NOT EXISTS transactions (
				tx_id TEXT PRIMARY KEY,
				sender TEXT NOT NULL,
				receiver TEXT NOT NULL,
				amount REAL NOT NULL CHECK (amount > 0),
				fee REAL NOT NULL DEFAULT 0.0 CHECK (fee >= 0),
				timestamp INTEGER NOT NULL CHECK (timestamp > 0),
				signature BLOB NOT NULL,
				nonce INTEGER NOT NULL CHECK (nonce >= 0),
				status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'confirmed', 'failed')),
				created_at INTEGER DEFAULT (strftime('%s', 'now')),
				CONSTRAINT valid_accounts CHECK (sender != receiver),
				CONSTRAINT valid_amount_fee CHECK (amount + fee > 0)
			);`,
		},
		{
			name: "sync_metadata",
			query: `CREATE TABLE IF NOT EXISTS sync_metadata (
				key TEXT PRIMARY KEY,
				value TEXT NOT NULL,
				created_at INTEGER DEFAULT (strftime('%s', 'now')),
				updated_at INTEGER DEFAULT (strftime('%s', 'now'))
			);`,
		},
	}

	for _, table := range schema {
		if _, err = tx.ExecContext(ctx, table.query); err != nil {
			return fmt.Errorf("failed to create table %s: %w", table.name, err)
		}
	}

	// Create indices for performance
	indices := []struct {
		name  string
		query string
	}{
		{"idx_block_headers_timestamp", "CREATE INDEX IF NOT EXISTS idx_block_headers_timestamp ON block_headers(timestamp);"},
		{"idx_accounts_balance", "CREATE INDEX IF NOT EXISTS idx_accounts_balance ON accounts(balance);"},
		{"idx_transactions_sender", "CREATE INDEX IF NOT EXISTS idx_transactions_sender ON transactions(sender);"},
		{"idx_transactions_receiver", "CREATE INDEX IF NOT EXISTS idx_transactions_receiver ON transactions(receiver);"},
		{"idx_transactions_timestamp", "CREATE INDEX IF NOT EXISTS idx_transactions_timestamp ON transactions(timestamp);"},
		{"idx_transactions_status", "CREATE INDEX IF NOT EXISTS idx_transactions_status ON transactions(status);"},
	}

	for _, index := range indices {
		if _, err = tx.ExecContext(ctx, index.query); err != nil {
			return fmt.Errorf("failed to create index %s: %w", index.name, err)
		}
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("%w: failed to commit schema migration transaction: %w", ErrTransactionFailed, err)
	}

	return nil
}

// prepareStatements prepares all SQL statements for better performance
func (s *SQLiteLightNode) prepareStatements() error {
	var err error

	// Prepare save block header statement
	s.stmts.saveBlockHeader, err = s.db.Prepare(`
		INSERT OR REPLACE INTO block_headers (block_number, hash, previous_hash, timestamp, merkle_root)
		VALUES (?, ?, ?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("failed to prepare save block header statement: %w", err)
	}

	// Prepare get latest block header statement
	s.stmts.getLatestBlockHeader, err = s.db.Prepare(`
		SELECT block_number, hash, previous_hash, timestamp, merkle_root
		FROM block_headers
		ORDER BY block_number DESC LIMIT 1`)
	if err != nil {
		return fmt.Errorf("failed to prepare get latest block header statement: %w", err)
	}

	// Prepare save account statement
	s.stmts.saveAccount, err = s.db.Prepare(`
		INSERT OR REPLACE INTO accounts (id, public_key, balance, nonce, updated_at)
		VALUES (?, ?, ?, ?, strftime('%s', 'now'))`)
	if err != nil {
		return fmt.Errorf("failed to prepare save account statement: %w", err)
	}

	// Prepare get account statement
	s.stmts.getAccount, err = s.db.Prepare(`
		SELECT id, public_key, balance, nonce
		FROM accounts
		WHERE id = ?`)
	if err != nil {
		return fmt.Errorf("failed to prepare get account statement: %w", err)
	}

	// Prepare save transaction statement
	s.stmts.saveTransaction, err = s.db.Prepare(`
		INSERT OR REPLACE INTO transactions (tx_id, sender, receiver, amount, fee, timestamp, signature, nonce, status)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("failed to prepare save transaction statement: %w", err)
	}

	// Prepare get transactions for account statement
	s.stmts.getTransactionsForAcct, err = s.db.Prepare(`
		SELECT tx_id, sender, receiver, amount, fee, timestamp, signature, nonce, status
		FROM transactions
		WHERE sender = ? OR receiver = ?
		ORDER BY timestamp DESC, tx_id`)
	if err != nil {
		return fmt.Errorf("failed to prepare get transactions for account statement: %w", err)
	}

	// Prepare save sync metadata statement
	s.stmts.saveSyncMetadata, err = s.db.Prepare(`
		INSERT OR REPLACE INTO sync_metadata (key, value, updated_at)
		VALUES (?, ?, strftime('%s', 'now'))`)
	if err != nil {
		return fmt.Errorf("failed to prepare save sync metadata statement: %w", err)
	}

	// Prepare get sync metadata statement
	s.stmts.getSyncMetadata, err = s.db.Prepare(`
		SELECT value FROM sync_metadata WHERE key = ?`)
	if err != nil {
		return fmt.Errorf("failed to prepare get sync metadata statement: %w", err)
	}

	return nil
}

// Close closes the SQLite database and all prepared statements with proper resource cleanup
func (s *SQLiteLightNode) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var errs []error

	// Close all prepared statements
	statements := []*sql.Stmt{
		s.stmts.saveBlockHeader,
		s.stmts.getLatestBlockHeader,
		s.stmts.saveAccount,
		s.stmts.getAccount,
		s.stmts.saveTransaction,
		s.stmts.getTransactionsForAcct,
		s.stmts.saveSyncMetadata,
		s.stmts.getSyncMetadata,
	}

	for _, stmt := range statements {
		if stmt != nil {
			if err := stmt.Close(); err != nil {
				errs = append(errs, fmt.Errorf("failed to close prepared statement: %w", err))
			}
		}
	}

	// Close database connection
	if s.db != nil {
		if err := s.db.Close(); err != nil {
			errs = append(errs, fmt.Errorf("%w: failed to close database connection: %w", ErrDatabaseConnection, err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors occurred while closing SQLiteLightNode: %v", errs)
	}

	return nil
}

// validateBlockHeader performs comprehensive validation of block header data
func validateBlockHeader(header common.Block) error {
	if header.Number < 0 {
		return fmt.Errorf("%w: block number cannot be negative: %d", ErrInvalidInput, header.Number)
	}

	if header.Hash == "" {
		return fmt.Errorf("%w: block hash cannot be empty", ErrInvalidInput)
	}

	if header.Timestamp <= 0 {
		return fmt.Errorf("%w: block timestamp must be positive: %d", ErrInvalidInput, header.Timestamp)
	}

	// Validate hash format (should be hexadecimal)
	if len(header.Hash) < 32 {
		return fmt.Errorf("%w: block hash too short: %s", ErrInvalidInput, header.Hash)
	}

	return nil
}

// SaveBlockHeader saves a pruned block header with comprehensive validation and error handling
func (s *SQLiteLightNode) SaveBlockHeader(header common.Block) error {
	if err := validateBlockHeader(header); err != nil {
		return fmt.Errorf("block header validation failed: %w", err)
	}

	s.mu.RLock()
	stmt := s.stmts.saveBlockHeader
	s.mu.RUnlock()

	if stmt == nil {
		return fmt.Errorf("%w: save block header statement not prepared", ErrDatabaseConnection)
	}

	_, err := stmt.Exec(header.Number, header.Hash, header.PreviousHash, header.Timestamp, header.MerkleRoot)
	if err != nil {
		return fmt.Errorf("failed to save block header %d (hash: %s): %w", header.Number, header.Hash, err)
	}

	return nil
}

// GetLatestBlockHeader returns the most recent block header with proper error handling
func (s *SQLiteLightNode) GetLatestBlockHeader() (common.Block, error) {
	s.mu.RLock()
	stmt := s.stmts.getLatestBlockHeader
	s.mu.RUnlock()

	if stmt == nil {
		return common.Block{}, fmt.Errorf("%w: get latest block header statement not prepared", ErrDatabaseConnection)
	}

	row := stmt.QueryRow()
	var header common.Block
	err := row.Scan(&header.Number, &header.Hash, &header.PreviousHash, &header.Timestamp, &header.MerkleRoot)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return common.Block{}, fmt.Errorf("%w: no block headers found", ErrRecordNotFound)
		}
		return common.Block{}, fmt.Errorf("failed to retrieve latest block header: %w", err)
	}

	return header, nil
}

// validateAccount performs comprehensive validation of account data
func validateAccount(ac *common.Account) error {
	if ac == nil {
		return fmt.Errorf("%w: account cannot be nil", ErrInvalidInput)
	}

	if ac.ID == "" {
		return fmt.Errorf("%w: account ID cannot be empty", ErrInvalidInput)
	}

	if len(ac.PublicKey) == 0 {
		return fmt.Errorf("%w: account public key cannot be empty", ErrInvalidInput)
	}

	if ac.Balance < 0 {
		return fmt.Errorf("%w: account balance cannot be negative: %f", ErrInvalidInput, ac.Balance)
	}

	if ac.Nonce < 0 {
		return fmt.Errorf("%w: account nonce cannot be negative: %d", ErrInvalidInput, ac.Nonce)
	}

	return nil
}

// SaveAccount saves or updates an account with comprehensive validation and error handling
func (s *SQLiteLightNode) SaveAccount(ac *common.Account) error {
	if err := validateAccount(ac); err != nil {
		return fmt.Errorf("account validation failed: %w", err)
	}

	s.mu.RLock()
	stmt := s.stmts.saveAccount
	s.mu.RUnlock()

	if stmt == nil {
		return fmt.Errorf("%w: save account statement not prepared", ErrDatabaseConnection)
	}

	_, err := stmt.Exec(ac.ID, ac.PublicKey, ac.Balance, ac.Nonce)
	if err != nil {
		return fmt.Errorf("failed to save account %s: %w", ac.ID, err)
	}

	return nil
}

// GetAccount retrieves an account by ID with comprehensive validation and error handling
func (s *SQLiteLightNode) GetAccount(accountID string) (*common.Account, error) {
	if accountID == "" {
		return nil, fmt.Errorf("%w: account ID cannot be empty", ErrInvalidInput)
	}

	s.mu.RLock()
	stmt := s.stmts.getAccount
	s.mu.RUnlock()

	if stmt == nil {
		return nil, fmt.Errorf("%w: get account statement not prepared", ErrDatabaseConnection)
	}

	row := stmt.QueryRow(accountID)
	var ac common.Account
	err := row.Scan(&ac.ID, &ac.PublicKey, &ac.Balance, &ac.Nonce)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("%w: account %s not found", ErrRecordNotFound, accountID)
		}
		return nil, fmt.Errorf("failed to retrieve account %s: %w", accountID, err)
	}

	return &ac, nil
}

// validateTransaction performs comprehensive validation of transaction data
func validateTransaction(tx *common.Transaction) error {
	if tx == nil {
		return fmt.Errorf("%w: transaction cannot be nil", ErrInvalidInput)
	}

	if tx.ID == "" {
		return fmt.Errorf("%w: transaction ID cannot be empty", ErrInvalidInput)
	}

	if tx.Sender == "" {
		return fmt.Errorf("%w: transaction sender cannot be empty", ErrInvalidInput)
	}

	if tx.Receiver == "" {
		return fmt.Errorf("%w: transaction receiver cannot be empty", ErrInvalidInput)
	}

	if tx.Sender == tx.Receiver {
		return fmt.Errorf("%w: transaction sender and receiver cannot be the same", ErrInvalidInput)
	}

	if tx.Amount <= 0 {
		return fmt.Errorf("%w: transaction amount must be positive: %f", ErrInvalidInput, tx.Amount)
	}

	if tx.Fee < 0 {
		return fmt.Errorf("%w: transaction fee cannot be negative: %f", ErrInvalidInput, tx.Fee)
	}

	if tx.Timestamp <= 0 {
		return fmt.Errorf("%w: transaction timestamp must be positive: %d", ErrInvalidInput, tx.Timestamp)
	}

	if len(tx.Signature) == 0 {
		return fmt.Errorf("%w: transaction signature cannot be empty", ErrInvalidInput)
	}

	if tx.Nonce < 0 {
		return fmt.Errorf("%w: transaction nonce cannot be negative: %d", ErrInvalidInput, tx.Nonce)
	}

	return nil
}

// SaveTransaction saves a transaction with comprehensive validation and configurable status
func (s *SQLiteLightNode) SaveTransaction(tx *common.Transaction, status TransactionStatus) error {
	if err := validateTransaction(tx); err != nil {
		return fmt.Errorf("transaction validation failed: %w", err)
	}

	if !status.IsValid() {
		return fmt.Errorf("%w: invalid transaction status: %s", ErrInvalidInput, status)
	}

	s.mu.RLock()
	stmt := s.stmts.saveTransaction
	s.mu.RUnlock()

	if stmt == nil {
		return fmt.Errorf("%w: save transaction statement not prepared", ErrDatabaseConnection)
	}

	_, err := stmt.Exec(tx.ID, tx.Sender, tx.Receiver, tx.Amount, tx.Fee,
		tx.Timestamp, tx.Signature, tx.Nonce, status.String())
	if err != nil {
		return fmt.Errorf("failed to save transaction %s (sender: %s, receiver: %s): %w",
			tx.ID, tx.Sender, tx.Receiver, err)
	}

	return nil
}

// SaveTransactionWithDefaultStatus saves a transaction with pending status (backward compatibility)
func (s *SQLiteLightNode) SaveTransactionWithDefaultStatus(tx *common.Transaction) error {
	return s.SaveTransaction(tx, StatusPending)
}

// GetTransactionsForAccount retrieves transactions where the account is either sender or receiver
// with comprehensive validation and error handling
func (s *SQLiteLightNode) GetTransactionsForAccount(accountID string) ([]common.Transaction, error) {
	if accountID == "" {
		return nil, fmt.Errorf("%w: account ID cannot be empty", ErrInvalidInput)
	}

	s.mu.RLock()
	stmt := s.stmts.getTransactionsForAcct
	s.mu.RUnlock()

	if stmt == nil {
		return nil, fmt.Errorf("%w: get transactions for account statement not prepared", ErrDatabaseConnection)
	}

	rows, err := stmt.Query(accountID, accountID)
	if err != nil {
		return nil, fmt.Errorf("failed to query transactions for account %s: %w", accountID, err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			// Log the error but don't override the main error
			fmt.Printf("Warning: failed to close rows for account %s: %v\n", accountID, closeErr)
		}
	}()

	var txs []common.Transaction
	for rows.Next() {
		var tx common.Transaction
		err := rows.Scan(&tx.ID, &tx.Sender, &tx.Receiver, &tx.Amount, &tx.Fee,
			&tx.Timestamp, &tx.Signature, &tx.Nonce, &tx.Status)
		if err != nil {
			return nil, fmt.Errorf("failed to scan transaction for account %s: %w", accountID, err)
		}
		txs = append(txs, tx)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error occurred while iterating transactions for account %s: %w", accountID, err)
	}

	return txs, nil
}

// validateSyncMetadata performs validation of sync metadata parameters
func validateSyncMetadata(key, value string) error {
	if key == "" {
		return fmt.Errorf("%w: sync metadata key cannot be empty", ErrInvalidInput)
	}

	if value == "" {
		return fmt.Errorf("%w: sync metadata value cannot be empty", ErrInvalidInput)
	}

	// Validate key format (alphanumeric and underscores only)
	for _, r := range key {
		if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') || r == '_' || r == '-') {
			return fmt.Errorf("%w: sync metadata key contains invalid characters: %s", ErrInvalidInput, key)
		}
	}

	return nil
}

// SaveSyncMetadata saves a key/value pair for synchronization metadata with validation
func (s *SQLiteLightNode) SaveSyncMetadata(key, value string) error {
	if err := validateSyncMetadata(key, value); err != nil {
		return fmt.Errorf("sync metadata validation failed: %w", err)
	}

	s.mu.RLock()
	stmt := s.stmts.saveSyncMetadata
	s.mu.RUnlock()

	if stmt == nil {
		return fmt.Errorf("%w: save sync metadata statement not prepared", ErrDatabaseConnection)
	}

	_, err := stmt.Exec(key, value)
	if err != nil {
		return fmt.Errorf("failed to save sync metadata (key: %s): %w", key, err)
	}

	return nil
}

// GetSyncMetadata retrieves the synchronization metadata value for a key with validation
func (s *SQLiteLightNode) GetSyncMetadata(key string) (string, error) {
	if key == "" {
		return "", fmt.Errorf("%w: sync metadata key cannot be empty", ErrInvalidInput)
	}

	s.mu.RLock()
	stmt := s.stmts.getSyncMetadata
	s.mu.RUnlock()

	if stmt == nil {
		return "", fmt.Errorf("%w: get sync metadata statement not prepared", ErrDatabaseConnection)
	}

	row := stmt.QueryRow(key)
	var value string
	err := row.Scan(&value)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", fmt.Errorf("%w: sync metadata key %s not found", ErrRecordNotFound, key)
		}
		return "", fmt.Errorf("failed to retrieve sync metadata (key: %s): %w", key, err)
	}

	return value, nil
}

// BeginTransaction starts a new database transaction for atomic operations
func (s *SQLiteLightNode) BeginTransaction(ctx context.Context) (*sql.Tx, error) {
	s.mu.RLock()
	db := s.db
	s.mu.RUnlock()

	if db == nil {
		return nil, fmt.Errorf("%w: database connection not available", ErrDatabaseConnection)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to begin transaction: %w", ErrTransactionFailed, err)
	}

	return tx, nil
}

// CommitTransaction commits a database transaction
func (s *SQLiteLightNode) CommitTransaction(tx *sql.Tx) error {
	if tx == nil {
		return fmt.Errorf("%w: transaction cannot be nil", ErrInvalidInput)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("%w: failed to commit transaction: %w", ErrTransactionFailed, err)
	}

	return nil
}

// RollbackTransaction rolls back a database transaction
func (s *SQLiteLightNode) RollbackTransaction(tx *sql.Tx) error {
	if tx == nil {
		return fmt.Errorf("%w: transaction cannot be nil", ErrInvalidInput)
	}

	if err := tx.Rollback(); err != nil {
		return fmt.Errorf("%w: failed to rollback transaction: %w", ErrTransactionFailed, err)
	}

	return nil
}

// HealthCheck performs a health check on the database connection
func (s *SQLiteLightNode) HealthCheck(ctx context.Context) error {
	s.mu.RLock()
	db := s.db
	s.mu.RUnlock()

	if db == nil {
		return fmt.Errorf("%w: database connection not available", ErrDatabaseConnection)
	}

	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("%w: database health check failed: %w", ErrDatabaseConnection, err)
	}

	return nil
}

// GetDatabaseStats returns statistics about the database
func (s *SQLiteLightNode) GetDatabaseStats(ctx context.Context) (map[string]interface{}, error) {
	s.mu.RLock()
	db := s.db
	s.mu.RUnlock()

	if db == nil {
		return nil, fmt.Errorf("%w: database connection not available", ErrDatabaseConnection)
	}

	stats := make(map[string]interface{})

	// Get database stats
	dbStats := db.Stats()
	stats["max_open_connections"] = dbStats.MaxOpenConnections
	stats["open_connections"] = dbStats.OpenConnections
	stats["in_use"] = dbStats.InUse
	stats["idle"] = dbStats.Idle
	stats["wait_count"] = dbStats.WaitCount
	stats["wait_duration"] = dbStats.WaitDuration.String()
	stats["max_idle_closed"] = dbStats.MaxIdleClosed
	stats["max_idle_time_closed"] = dbStats.MaxIdleTimeClosed
	stats["max_lifetime_closed"] = dbStats.MaxLifetimeClosed

	// Get table counts
	queries := map[string]string{
		"block_headers_count": "SELECT COUNT(*) FROM block_headers",
		"accounts_count":      "SELECT COUNT(*) FROM accounts",
		"transactions_count":  "SELECT COUNT(*) FROM transactions",
		"sync_metadata_count": "SELECT COUNT(*) FROM sync_metadata",
	}

	for key, query := range queries {
		var count int
		if err := db.QueryRowContext(ctx, query).Scan(&count); err != nil {
			return nil, fmt.Errorf("failed to get %s: %w", key, err)
		}
		stats[key] = count
	}

	return stats, nil
}

// OptimizeDatabase performs database optimization operations
func (s *SQLiteLightNode) OptimizeDatabase(ctx context.Context) error {
	s.mu.RLock()
	db := s.db
	s.mu.RUnlock()

	if db == nil {
		return fmt.Errorf("%w: database connection not available", ErrDatabaseConnection)
	}

	// Run VACUUM to optimize database file
	if _, err := db.ExecContext(ctx, "VACUUM"); err != nil {
		return fmt.Errorf("failed to vacuum database: %w", err)
	}

	// Analyze tables for query optimization
	if _, err := db.ExecContext(ctx, "ANALYZE"); err != nil {
		return fmt.Errorf("failed to analyze database: %w", err)
	}

	return nil
}
