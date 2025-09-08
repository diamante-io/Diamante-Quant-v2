// storage/store.go

package storage

import (
	"context"
	"errors"
	"time"

	"diamante/common"
)

// Common errors for storage operations
var (
	ErrNotFound          = errors.New("item not found")
	ErrAlreadyExists     = errors.New("item already exists")
	ErrInvalidData       = errors.New("invalid data")
	ErrDatabaseError     = errors.New("database error")
	ErrNotImplemented    = errors.New("method not implemented")
	ErrConnectionFailed  = errors.New("database connection failed")
	ErrTransactionFailed = errors.New("database transaction failed")
)

// StoreStats represents storage statistics and metrics
type StoreStats struct {
	// Database info
	DatabaseType    string `json:"databaseType"`
	DatabaseVersion string `json:"databaseVersion,omitempty"`
	ConnectionPool  int    `json:"connectionPool,omitempty"`

	// Data counts
	BlockCount       int64 `json:"blockCount"`
	TransactionCount int64 `json:"transactionCount"`
	AccountCount     int64 `json:"accountCount"`
	ContractCount    int64 `json:"contractCount"`
	ReceiptCount     int64 `json:"receiptCount"`
	StateKeyCount    int64 `json:"stateKeyCount"`

	// Storage metrics
	TotalSize    int64   `json:"totalSize"`
	DataSize     int64   `json:"dataSize"`
	IndexSize    int64   `json:"indexSize"`
	CacheSize    int64   `json:"cacheSize"`
	CacheHitRate float64 `json:"cacheHitRate"`

	// Performance metrics
	AverageReadLatency  int64 `json:"averageReadLatency"`  // microseconds
	AverageWriteLatency int64 `json:"averageWriteLatency"` // microseconds
	QueriesPerSecond    int64 `json:"queriesPerSecond"`
	WritesPerSecond     int64 `json:"writesPerSecond"`

	// Tiered storage metrics
	CacheHits      uint64 `json:"cacheHits"`
	CacheMisses    uint64 `json:"cacheMisses"`
	PrimaryReads   uint64 `json:"primaryReads"`
	ArchiveReads   uint64 `json:"archiveReads"`
	ArchivedBlocks uint64 `json:"archivedBlocks"`
	CacheErrors    uint64 `json:"cacheErrors"`

	// Last operation timestamps
	LastBlockTime       time.Time `json:"lastBlockTime"`
	LastTransactionTime time.Time `json:"lastTransactionTime"`
	LastCompactionTime  time.Time `json:"lastCompactionTime"`
	LastBackupTime      time.Time `json:"lastBackupTime"`

	// Health indicators
	IsHealthy     bool      `json:"isHealthy"`
	IsSyncing     bool      `json:"isSyncing"`
	ErrorCount    int64     `json:"errorCount"`
	LastError     string    `json:"lastError,omitempty"`
	LastErrorTime time.Time `json:"lastErrorTime,omitempty"`
}

// LedgerStore defines the interface for all storage backends
// This allows for pluggable storage implementations (LMDB, SQLite, MongoDB)
type LedgerStore interface {
	// Block operations
	SaveBlock(block *common.Block) error
	GetBlock(height uint64) (*common.Block, error)
	GetLatestBlock() (*common.Block, error)
	GetBlockByHash(hash string) (*common.Block, error)
	GetBlockRange(startHeight, endHeight uint64) ([]*common.Block, error)
	ReplaceBlockSameHeight(height uint64, newBlock *common.Block) error // Testnet-only conflict repair

	// Transaction operations
	SaveTransaction(tx *common.Transaction, blockHeight int) error
	GetTransaction(txID string) (*common.Transaction, error)
	GetTransactionsByAddress(address string, limit, offset int) ([]*common.Transaction, error)

	// Account operations
	SaveAccount(account *common.Account) error
	GetAccount(accountID string) (*common.Account, error)

	// State operations
	GetState(key []byte) ([]byte, error)
	SaveState(key []byte, value []byte) error

	// Smart contract operations
	GetContract(address string) (*Contract, error)
	SaveContract(contract *Contract) error

	// Receipt operations
	SaveReceipt(receipt *Receipt) error
	GetReceipt(txID string) (*Receipt, error)

	// Batch operations
	WriteBatch(batch WriteBatch) error

	// Snapshot operations
	Snapshot(path string) error
	Restore(path string) error

	// Lifecycle operations
	Open() error
	Close() error
	IsOpen() bool

	// Health and metrics
	HealthCheck(ctx context.Context) error
	GetStats() (*StoreStats, error)

	// Maintenance operations
	Compact() error
	PruneData(olderThan time.Time) error
}

// SnapshotInfo contains metadata about a ledger snapshot
type SnapshotInfo struct {
	Height    uint64    `json:"height"`
	Timestamp time.Time `json:"timestamp"`
	Size      int64     `json:"size"`
	Hash      string    `json:"hash"`
}

// WriteBatch represents a batch of write operations to be performed atomically
type WriteBatch struct {
	Blocks       []*common.Block       `json:"blocks,omitempty"`
	Transactions []*common.Transaction `json:"transactions,omitempty"`
	Accounts     []*common.Account     `json:"accounts,omitempty"`
	Contracts    []*Contract           `json:"contracts,omitempty"`
	Receipts     []*Receipt            `json:"receipts,omitempty"`
	StateWrites  map[string][]byte     `json:"stateWrites,omitempty"`
	StateDeletes []string              `json:"stateDeletes,omitempty"`
}

// NewWriteBatch creates a new empty write batch
func NewWriteBatch() *WriteBatch {
	return &WriteBatch{
		StateWrites: make(map[string][]byte),
	}
}

// AddBlock adds a block to the write batch
func (wb *WriteBatch) AddBlock(block *common.Block) {
	wb.Blocks = append(wb.Blocks, block)
}

// AddTransaction adds a transaction to the write batch
func (wb *WriteBatch) AddTransaction(tx *common.Transaction) {
	wb.Transactions = append(wb.Transactions, tx)
}

// AddAccount adds an account to the write batch
func (wb *WriteBatch) AddAccount(account *common.Account) {
	wb.Accounts = append(wb.Accounts, account)
}

// AddContract adds a contract to the write batch
func (wb *WriteBatch) AddContract(contract *Contract) {
	wb.Contracts = append(wb.Contracts, contract)
}

// AddReceipt adds a receipt to the write batch
func (wb *WriteBatch) AddReceipt(receipt *Receipt) {
	wb.Receipts = append(wb.Receipts, receipt)
}

// SetState adds a state key-value pair to the write batch
func (wb *WriteBatch) SetState(key string, value []byte) {
	wb.StateWrites[key] = value
}

// DeleteState adds a state key to the delete list
func (wb *WriteBatch) DeleteState(key string) {
	wb.StateDeletes = append(wb.StateDeletes, key)
}

// Reset clears all operations in the batch
func (wb *WriteBatch) Reset() {
	wb.Blocks = nil
	wb.Transactions = nil
	wb.Accounts = nil
	wb.Contracts = nil
	wb.Receipts = nil
	wb.StateWrites = make(map[string][]byte)
	wb.StateDeletes = nil
}

// IsEmpty checks if the batch has any operations
func (wb *WriteBatch) IsEmpty() bool {
	return len(wb.Blocks) == 0 &&
		len(wb.Transactions) == 0 &&
		len(wb.Accounts) == 0 &&
		len(wb.Contracts) == 0 &&
		len(wb.Receipts) == 0 &&
		len(wb.StateWrites) == 0 &&
		len(wb.StateDeletes) == 0
}
