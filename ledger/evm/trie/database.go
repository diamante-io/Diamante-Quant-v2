// Package trie provides database helpers for the Merkle Patricia Trie
package trie

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	diamantecommon "diamante/common"
	"diamante/consensus"
	"diamante/storage"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/sirupsen/logrus"
)

// TrieDatabase wraps storage.LedgerStore to implement ethdb.Database
type TrieDatabase struct {
	store  storage.LedgerStore
	logger *logrus.Logger
	cache  *TrieCache
}

// TrieCache provides caching for trie operations
type TrieCache struct {
	nodes   map[string][]byte
	size    int
	maxSize int
	mu      sync.RWMutex
}

// NewTrieDatabase creates a new trie database
func NewTrieDatabase(store storage.LedgerStore, logger *logrus.Logger) *TrieDatabase {
	return &TrieDatabase{
		store:  store,
		logger: logger,
		cache: &TrieCache{
			nodes:   make(map[string][]byte),
			maxSize: 100 * 1024 * 1024, // 100MB cache
		},
	}
}

// NewMemoryTrieDatabase creates an in-memory trie database for testing
func NewMemoryTrieDatabase() *TrieDatabase {
	// Create a simple in-memory store
	memStore := &inMemoryStore{
		data: make(map[string][]byte),
	}
	return NewTrieDatabase(memStore, nil)
}

// StorageAdapter wraps a LedgerStore to implement ethdb.Database
type StorageAdapter struct {
	store  storage.LedgerStore
	logger *logrus.Logger
}

// NewStorageAdapter creates a new storage adapter
func NewStorageAdapter(store storage.LedgerStore) ethdb.Database {
	return &StorageAdapter{
		store:  store,
		logger: logrus.New(),
	}
}

// inMemoryStore is a simple in-memory implementation of LedgerStore
type inMemoryStore struct {
	data map[string][]byte
	mu   sync.RWMutex
}

func (m *inMemoryStore) GetState(key []byte) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if val, ok := m.data[string(key)]; ok {
		return val, nil
	}
	return nil, storage.ErrNotFound
}

func (m *inMemoryStore) IsOpen() bool {
	return true
}

func (m *inMemoryStore) Snapshot(path string) error {
	// For in-memory store used in trie testing, this is a no-op
	return nil
}

func (m *inMemoryStore) Restore(path string) error {
	// For in-memory store used in trie testing, this is a no-op
	return nil
}

func (m *inMemoryStore) SaveState(key, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[string(key)] = value
	return nil
}

func (m *inMemoryStore) SetState(key, value []byte) error {
	return m.SaveState(key, value)
}

func (m *inMemoryStore) GetBalance(accountID string) (float64, error) {
	// For in-memory store used in trie testing, return 0 balance
	return 0, nil
}

func (m *inMemoryStore) GetNonce(address string) (uint64, error) {
	// For in-memory store used in trie testing, return 0 nonce
	return 0, nil
}

func (m *inMemoryStore) UpdateBalance(accountID string, balance float64) error {
	// For in-memory store used in trie testing, this is a no-op
	return nil
}

func (m *inMemoryStore) CreateAccount(account interface{}) error {
	// For in-memory store used in trie testing, this is a no-op
	return nil
}

func (m *inMemoryStore) UpdateAccount(account *diamantecommon.Account) error {
	// For in-memory store used in trie testing, this is a no-op
	return nil
}

func (m *inMemoryStore) CreateSnapshot(height uint64) error {
	// For in-memory store used in trie testing, snapshots are not supported
	// Return nil as this is not critical for trie operations
	return nil
}

func (m *inMemoryStore) GetLatestSnapshot() (int, error) {
	// For in-memory store used in trie testing, return 0
	return 0, nil
}

func (m *inMemoryStore) UpdateSmartContract(contractID string, code string, version string) error {
	// Store contract code as state data
	key := fmt.Sprintf("contract:%s:code", contractID)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[key] = []byte(code)
	m.data[fmt.Sprintf("contract:%s:version", contractID)] = []byte(version)
	return nil
}

func (m *inMemoryStore) CreateSmartContract(contract interface{}) error {
	// For in-memory store used in trie testing, this is a no-op
	return nil
}

func (m *inMemoryStore) Backup(path string) error {
	// For in-memory store, backup is not supported
	// Return nil as this is not critical for trie operations
	return nil
}

func (m *inMemoryStore) WriteBatch(batch storage.WriteBatch) error {
	// For in-memory store, we apply the batch operations directly
	m.mu.Lock()
	defer m.mu.Unlock()

	// Apply state writes
	for key, value := range batch.StateWrites {
		m.data[key] = value
	}

	// Apply state deletes
	for _, key := range batch.StateDeletes {
		delete(m.data, key)
	}

	return nil
}

func (m *inMemoryStore) CreateBlock(block interface{}) error {
	// For in-memory store used in trie testing, this is a no-op
	return nil
}

func (m *inMemoryStore) UpdateBlock(block interface{}) error {
	// For in-memory store used in trie testing, this is a no-op
	return nil
}

func (m *inMemoryStore) Close() error {
	return nil
}

func (m *inMemoryStore) BatchWrite(batch *storage.WriteBatch) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// For now, just return nil as this is a simple in-memory implementation
	return nil
}

func (m *inMemoryStore) Compact() error {
	// No-op for in-memory store
	return nil
}

func (m *inMemoryStore) DeleteContract(contractID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Delete all contract-related data
	delete(m.data, fmt.Sprintf("contract:%s:code", contractID))
	delete(m.data, fmt.Sprintf("contract:%s:version", contractID))
	delete(m.data, fmt.Sprintf("contract:%s", contractID))

	return nil
}

func (m *inMemoryStore) DeleteState(key []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, string(key))
	return nil
}

func (m *inMemoryStore) GetAccount(accountID string) (*diamantecommon.Account, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	key := fmt.Sprintf("account:%s", accountID)
	if _, ok := m.data[key]; ok {
		// For simple in-memory implementation, return a basic account
		return &diamantecommon.Account{
			ID:      accountID,
			Balance: 0, // Balance retrieved via GetBalance
		}, nil
	}

	return nil, storage.ErrNotFound
}

func (m *inMemoryStore) GetBlock(height uint64) (*diamantecommon.Block, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	key := fmt.Sprintf("block:%d", height)
	if _, ok := m.data[key]; ok {
		// For simple in-memory implementation, return a basic block
		return &diamantecommon.Block{
			Number:       int(height),
			Hash:         fmt.Sprintf("hash_%d", height),
			PreviousHash: fmt.Sprintf("hash_%d", height-1),
			Timestamp:    consensus.ConsensusNow().Unix(),
			Transactions: []diamantecommon.Transaction{},
		}, nil
	}

	return nil, storage.ErrNotFound
}

func (m *inMemoryStore) GetBlockByHash(hash string) (*diamantecommon.Block, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Search for block with matching hash
	for key, _ := range m.data {
		if strings.HasPrefix(key, "block:") && strings.Contains(key, hash) {
			// Extract height from key
			var height uint64
			fmt.Sscanf(key, "block:%d", &height)
			return m.GetBlock(height)
		}
	}

	return nil, storage.ErrNotFound
}

func (m *inMemoryStore) GetBlockRange(start, end uint64) ([]*diamantecommon.Block, error) {
	if start > end {
		return nil, fmt.Errorf("invalid range: start %d > end %d", start, end)
	}

	blocks := make([]*diamantecommon.Block, 0)
	for height := start; height <= end; height++ {
		block, err := m.GetBlock(height)
		if err == nil {
			blocks = append(blocks, block)
		}
	}

	return blocks, nil
}

func (m *inMemoryStore) GetContract(contractID string) (*diamantecommon.SmartContract, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	codeKey := fmt.Sprintf("contract:%s:code", contractID)
	versionKey := fmt.Sprintf("contract:%s:version", contractID)

	code, hasCode := m.data[codeKey]
	version, hasVersion := m.data[versionKey]

	if hasCode {
		versionStr := "1.0.0"
		if hasVersion {
			versionStr = string(version)
		}

		return &diamantecommon.SmartContract{
			ID:      contractID,
			Code:    string(code),
			Version: versionStr,
		}, nil
	}

	return nil, storage.ErrNotFound
}

func (m *inMemoryStore) GetLatestBlock() (*diamantecommon.Block, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var latestHeight uint64 = 0

	// Find the highest block number
	for key := range m.data {
		if strings.HasPrefix(key, "block:") {
			var height uint64
			if _, err := fmt.Sscanf(key, "block:%d", &height); err == nil {
				if height > latestHeight {
					latestHeight = height
				}
			}
		}
	}

	if latestHeight > 0 {
		return m.GetBlock(latestHeight)
	}

	// Return genesis block if no blocks found
	return &diamantecommon.Block{
		Number:       0,
		Hash:         "genesis_hash",
		PreviousHash: "",
		Timestamp:    consensus.ConsensusNow().Unix(),
		Transactions: []diamantecommon.Transaction{},
	}, nil
}

func (m *inMemoryStore) GetReceipt(hash string) (*storage.Receipt, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	key := fmt.Sprintf("receipt:%s", hash)
	if _, ok := m.data[key]; ok {
		// For simple implementation, return a basic receipt
		return &storage.Receipt{
			TxID:        hash,
			BlockHeight: 0,
			Status:      true, // Receipt.Status is a bool
			GasUsed:     21000,
		}, nil
	}

	return nil, storage.ErrNotFound
}

func (m *inMemoryStore) GetStats() (*storage.StoreStats, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Count different types of entries
	blockCount := int64(0)
	txCount := int64(0)
	contractCount := int64(0)
	accountCount := int64(0)
	totalSize := int64(0)

	for key, value := range m.data {
		totalSize += int64(len(key) + len(value))

		if strings.HasPrefix(key, "block:") {
			blockCount++
		} else if strings.HasPrefix(key, "tx:") {
			txCount++
		} else if strings.HasPrefix(key, "contract:") {
			contractCount++
		} else if strings.HasPrefix(key, "account:") {
			accountCount++
		}
	}

	return &storage.StoreStats{
		DatabaseType:     "in-memory",
		BlockCount:       blockCount,
		TransactionCount: txCount,
		ContractCount:    contractCount,
		AccountCount:     accountCount,
		TotalSize:        totalSize,
		IsHealthy:        true,
	}, nil
}

func (m *inMemoryStore) GetTransaction(txID string) (*diamantecommon.Transaction, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	key := fmt.Sprintf("tx:%s", txID)
	if _, ok := m.data[key]; ok {
		// For simple implementation, return a basic transaction
		return &diamantecommon.Transaction{
			ID:        txID,
			Sender:    "0x0000000000000000000000000000000000000000",
			Receiver:  "0x0000000000000000000000000000000000000001",
			Amount:    0,
			Fee:       0,
			Timestamp: consensus.ConsensusNow().Unix(),
			Status:    "confirmed",
		}, nil
	}

	return nil, storage.ErrNotFound
}

func (m *inMemoryStore) GetTransactionsByAddress(address string, offset int, limit int) ([]*diamantecommon.Transaction, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// For simple implementation, return empty list
	// In production, this would search through all transactions
	return []*diamantecommon.Transaction{}, nil
}

func (m *inMemoryStore) GetTransactionsByBlock(blockHeight uint64) ([]*diamantecommon.Transaction, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// For simple implementation, return empty list
	// In production, this would return all transactions in the block
	return []*diamantecommon.Transaction{}, nil
}

func (m *inMemoryStore) HealthCheck(ctx context.Context) error {
	return nil
}

func (m *inMemoryStore) ListSnapshots() ([]storage.SnapshotInfo, error) {
	return []storage.SnapshotInfo{}, nil
}

func (m *inMemoryStore) Open() error {
	// No-op for in-memory store
	return nil
}

func (m *inMemoryStore) PruneData(before time.Time) error {
	// No-op for in-memory store
	return nil
}

func (m *inMemoryStore) RestoreSnapshot(height uint64) error {
	// For in-memory store, snapshot restore is not critical
	// Simply log and return nil
	return nil
}

func (m *inMemoryStore) SaveAccount(account *diamantecommon.Account) error {
	if account == nil {
		return fmt.Errorf("account cannot be nil")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	key := fmt.Sprintf("account:%s", account.ID)
	// Store basic account info
	m.data[key] = []byte(account.ID)

	// Store balance separately if needed
	if account.Balance > 0 {
		balanceKey := fmt.Sprintf("balance:%s", account.ID)
		m.data[balanceKey] = []byte(fmt.Sprintf("%f", account.Balance))
	}

	return nil
}

func (m *inMemoryStore) SaveBlock(block *diamantecommon.Block) error {
	if block == nil {
		return fmt.Errorf("block cannot be nil")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	key := fmt.Sprintf("block:%d", block.Number)
	// Store block hash as simple representation
	m.data[key] = []byte(block.Hash)

	// Also store by hash for hash-based lookups
	hashKey := fmt.Sprintf("blockhash:%s", block.Hash)
	m.data[hashKey] = []byte(fmt.Sprintf("%d", block.Number))

	return nil
}

func (m *inMemoryStore) SaveContract(contract *diamantecommon.SmartContract) error {
	if contract == nil {
		return fmt.Errorf("contract cannot be nil")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Store contract code
	codeKey := fmt.Sprintf("contract:%s:code", contract.ID)
	m.data[codeKey] = []byte(contract.Code)

	// Store contract version
	versionKey := fmt.Sprintf("contract:%s:version", contract.ID)
	m.data[versionKey] = []byte(contract.Version)

	// Store contract metadata
	metaKey := fmt.Sprintf("contract:%s", contract.ID)
	m.data[metaKey] = []byte(contract.ID)

	return nil
}

func (m *inMemoryStore) SaveReceipt(receipt *storage.Receipt) error {
	if receipt == nil {
		return fmt.Errorf("receipt cannot be nil")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	key := fmt.Sprintf("receipt:%s", receipt.TxID)
	// Store basic receipt info - convert bool to string
	statusStr := "false"
	if receipt.Status {
		statusStr = "true"
	}
	m.data[key] = []byte(statusStr)

	return nil
}

func (m *inMemoryStore) SaveTransaction(tx *diamantecommon.Transaction, blockHeight int) error {
	if tx == nil {
		return fmt.Errorf("transaction cannot be nil")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	key := fmt.Sprintf("tx:%s", tx.ID)
	// Store transaction ID as simple representation
	m.data[key] = []byte(tx.ID)

	// Store block association
	blockKey := fmt.Sprintf("tx:%s:block", tx.ID)
	m.data[blockKey] = []byte(fmt.Sprintf("%d", blockHeight))

	return nil
}

func (m *inMemoryStore) UpdateContract(contract *diamantecommon.SmartContract) error {
	// For in-memory store, update is same as save
	return m.SaveContract(contract)
}

func (m *inMemoryStore) Vacuum() error {
	// No-op for in-memory store
	return nil
}

func (m *inMemoryStore) ReplaceBlockSameHeight(height uint64, newBlock *diamantecommon.Block) error {
	// For in-memory store used in trie testing, this is a no-op
	// This method is only used for testnet conflict repair
	return nil
}

// Has checks if a key exists in the database
func (db *TrieDatabase) Has(key []byte) (bool, error) {
	// Check cache first
	if db.cache.Has(string(key)) {
		return true, nil
	}

	// Check underlying store
	_, err := db.store.GetState(makeTrieKey(key))
	if err != nil {
		if err == storage.ErrNotFound {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// Get retrieves a value from the database
func (db *TrieDatabase) Get(key []byte) ([]byte, error) {
	// Check cache first
	if value := db.cache.Get(string(key)); value != nil {
		return value, nil
	}

	// Get from underlying store
	value, err := db.store.GetState(makeTrieKey(key))
	if err != nil {
		if err == storage.ErrNotFound {
			return nil, fmt.Errorf("not found")
		}
		return nil, err
	}

	// Add to cache
	db.cache.Put(string(key), value)

	return value, nil
}

// Put stores a value in the database
func (db *TrieDatabase) Put(key []byte, value []byte) error {
	// Store in underlying store
	if err := db.store.SaveState(makeTrieKey(key), value); err != nil {
		return err
	}

	// Add to cache
	db.cache.Put(string(key), value)

	return nil
}

// Delete removes a key from the database
func (db *TrieDatabase) Delete(key []byte) error {
	// Delete from cache
	db.cache.Delete(string(key))

	// Delete from underlying store
	// Since LedgerStore doesn't have a Delete method, we'll set empty value
	return db.store.SaveState(makeTrieKey(key), nil)
}

// HasAncient returns false as ancient data is not supported
func (db *TrieDatabase) HasAncient(kind string, number uint64) (bool, error) {
	return false, nil
}

// Ancient returns an error as ancient data is not supported
func (db *TrieDatabase) Ancient(kind string, number uint64) ([]byte, error) {
	return nil, fmt.Errorf("ancient data not supported")
}

// AncientRange returns an error as ancient data is not supported
func (db *TrieDatabase) AncientRange(kind string, start, count, maxBytes uint64) ([][]byte, error) {
	return nil, fmt.Errorf("ancient data not supported")
}

// Ancients returns 0 as ancient data is not supported
func (db *TrieDatabase) Ancients() (uint64, error) {
	return 0, nil
}

// AncientSize returns 0 as ancient data is not supported
func (db *TrieDatabase) AncientSize(kind string) (uint64, error) {
	return 0, nil
}

// AncientDatadir returns empty string as ancient data is not supported
func (db *TrieDatabase) AncientDatadir() (string, error) {
	return "", fmt.Errorf("ancient data not supported")
}

// ModifyAncients returns an error as ancient data is not supported
func (db *TrieDatabase) ModifyAncients(fn func(ethdb.AncientWriteOp) error) (int64, error) {
	return 0, fmt.Errorf("ancient data not supported")
}

// TruncateHead is a no-op as ancient data is not supported
func (db *TrieDatabase) TruncateHead(n uint64) (uint64, error) {
	return 0, nil
}

// TruncateTail is a no-op as ancient data is not supported
func (db *TrieDatabase) TruncateTail(n uint64) (uint64, error) {
	return 0, nil
}

// Tail returns 0 as ancient data is not supported
func (db *TrieDatabase) Tail() (uint64, error) {
	return 0, nil
}

// Sync is a no-op for this implementation
func (db *TrieDatabase) Sync() error {
	return nil
}

// MigrateTable is a no-op as ancient data is not supported
func (db *TrieDatabase) MigrateTable(string, func([]byte) ([]byte, error)) error {
	return fmt.Errorf("ancient data not supported")
}

// ReadAncients is for compatibility with ethdb.Database interface
func (db *TrieDatabase) ReadAncients(fn func(reader ethdb.AncientReaderOp) error) error {
	return fmt.Errorf("ancient data not supported")
}

// NewBatch creates a new batch
func (db *TrieDatabase) NewBatch() ethdb.Batch {
	return &trieBatch{
		db:      db,
		writes:  make(map[string][]byte),
		deletes: make(map[string]bool),
	}
}

// NewBatchWithSize creates a new batch with size hint
func (db *TrieDatabase) NewBatchWithSize(size int) ethdb.Batch {
	return &trieBatch{
		db:      db,
		writes:  make(map[string][]byte, size),
		deletes: make(map[string]bool),
	}
}

// NewIterator creates a new iterator
func (db *TrieDatabase) NewIterator(prefix []byte, start []byte) ethdb.Iterator {
	// For now, return a simple iterator
	return &trieIterator{
		db:     db,
		prefix: prefix,
		start:  start,
	}
}

// Snapshot represents a database snapshot
type Snapshot interface {
	// Release releases the snapshot
	Release()
}

// NewSnapshot creates a new snapshot
func (db *TrieDatabase) NewSnapshot() (Snapshot, error) {
	return &trieSnapshot{
		db:       db,
		version:  consensus.ConsensusNow().UnixNano(),
		data:     make(map[string][]byte),
		refCount: 1,
	}, nil
}

// Stat returns database statistics (implements ethdb.Database)
func (db *TrieDatabase) Stat() (string, error) {
	return fmt.Sprintf("TrieDatabase stats: cache size=%d", db.cache.size), nil
}

// StatProperty returns specific database statistics
func (db *TrieDatabase) StatProperty(property string) (string, error) {
	switch property {
	case "cache.size":
		return fmt.Sprintf("%d", db.cache.size), nil
	default:
		return "", fmt.Errorf("unknown property: %s", property)
	}
}

// Compact compacts the database
func (db *TrieDatabase) Compact(start []byte, limit []byte) error {
	// Compaction handled by underlying store
	return nil
}

// Close closes the database
func (db *TrieDatabase) Close() error {
	db.cache.Clear()
	return nil
}

// StorageAdapter methods to implement ethdb.Database

// Has retrieves if a key is present in the database
func (s *StorageAdapter) Has(key []byte) (bool, error) {
	val, err := s.store.GetState(key)
	if err == storage.ErrNotFound {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return val != nil, nil
}

// Get retrieves the given key
func (s *StorageAdapter) Get(key []byte) ([]byte, error) {
	val, err := s.store.GetState(key)
	if err == storage.ErrNotFound {
		return nil, fmt.Errorf("not found")
	}
	return val, err
}

// Put inserts the given value into the database
func (s *StorageAdapter) Put(key []byte, value []byte) error {
	return s.store.SaveState(key, value)
}

// Delete removes the key from the database
func (s *StorageAdapter) Delete(key []byte) error {
	// Since LedgerStore doesn't have DeleteState, we use SaveState with nil value
	return s.store.SaveState(key, nil)
}

// NewBatch creates a write-only key-value store
func (s *StorageAdapter) NewBatch() ethdb.Batch {
	return &storageBatch{store: s.store, ops: make([]batchOp, 0)}
}

// NewBatchWithSize creates a write-only database batch with pre-allocated buffer
func (s *StorageAdapter) NewBatchWithSize(size int) ethdb.Batch {
	return &storageBatch{store: s.store, ops: make([]batchOp, 0, size)}
}

// NewIterator creates a binary-alphabetical iterator
func (s *StorageAdapter) NewIterator(prefix []byte, start []byte) ethdb.Iterator {
	// Return a no-op iterator for now
	return &emptyIterator{}
}

// Stat returns database stats
func (s *StorageAdapter) Stat() (string, error) {
	if statsProvider, ok := s.store.(interface {
		GetStats() (*storage.StoreStats, error)
	}); ok {
		stats, err := statsProvider.GetStats()
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("StorageAdapter: total_size=%d bytes, data_size=%d bytes", stats.TotalSize, stats.DataSize), nil
	}
	return "StorageAdapter: stats not available", nil
}

// Compact flattens the database
func (s *StorageAdapter) Compact(start []byte, limit []byte) error {
	// No-op for storage adapter
	return nil
}

// NewSnapshot creates a snapshot
func (s *StorageAdapter) NewSnapshot() (interface{}, error) {
	// Return a simple snapshot that doesn't do anything special
	return &storageSnapshot{store: s.store}, nil
}

// Close closes the database
func (s *StorageAdapter) Close() error {
	// No-op for storage adapter
	return nil
}

// Ancient methods - we don't support ancient data

// HasAncient returns whether ancient data exists
func (s *StorageAdapter) HasAncient(kind string, number uint64) (bool, error) {
	return false, nil
}

// Ancient retrieves ancient data - not supported
func (s *StorageAdapter) Ancient(kind string, number uint64) ([]byte, error) {
	return nil, fmt.Errorf("ancient data not supported")
}

// AncientRange retrieves multiple ancient items - not supported
func (s *StorageAdapter) AncientRange(kind string, start, count, maxBytes uint64) ([][]byte, error) {
	return nil, fmt.Errorf("ancient data not supported")
}

// Ancients returns the number of ancient items - always 0
func (s *StorageAdapter) Ancients() (uint64, error) {
	return 0, nil
}

// Tail returns the number of first stored item - always 0
func (s *StorageAdapter) Tail() (uint64, error) {
	return 0, nil
}

// AncientSize returns the size of ancient data - always 0
func (s *StorageAdapter) AncientSize(kind string) (uint64, error) {
	return 0, nil
}

// AncientDatadir returns the ancient data directory - not supported
func (s *StorageAdapter) AncientDatadir() (string, error) {
	return "", fmt.Errorf("ancient data not supported")
}

// ModifyAncients runs a modification operation on ancient data - not supported
func (s *StorageAdapter) ModifyAncients(fn func(ethdb.AncientWriteOp) error) (int64, error) {
	return 0, fmt.Errorf("ancient data not supported")
}

// TruncateHead truncates ancient data - not supported
func (s *StorageAdapter) TruncateHead(n uint64) (uint64, error) {
	return 0, fmt.Errorf("ancient data not supported")
}

// TruncateTail truncates ancient data - not supported
func (s *StorageAdapter) TruncateTail(n uint64) (uint64, error) {
	return 0, fmt.Errorf("ancient data not supported")
}

// Sync syncs ancient data - not supported
func (s *StorageAdapter) Sync() error {
	return nil
}

// MigrateTable migrates ancient data - not supported
func (s *StorageAdapter) MigrateTable(string, func([]byte) ([]byte, error)) error {
	return fmt.Errorf("ancient data not supported")
}

// ReadAncients runs a read operation on ancient data
func (s *StorageAdapter) ReadAncients(fn func(ethdb.AncientReaderOp) error) error {
	// Pass self as the reader since we implement AncientReaderOp methods
	return fn(s)
}

// storageBatch implements ethdb.Batch
type storageBatch struct {
	store storage.LedgerStore
	ops   []batchOp
	size  int
}

type batchOp struct {
	key    []byte
	value  []byte
	delete bool
}

func (b *storageBatch) Put(key, value []byte) error {
	b.ops = append(b.ops, batchOp{key: common.CopyBytes(key), value: common.CopyBytes(value)})
	b.size += len(key) + len(value)
	return nil
}

func (b *storageBatch) Delete(key []byte) error {
	b.ops = append(b.ops, batchOp{key: common.CopyBytes(key), delete: true})
	b.size += len(key)
	return nil
}

func (b *storageBatch) ValueSize() int {
	return b.size
}

func (b *storageBatch) Write() error {
	for _, op := range b.ops {
		if op.delete {
			// Since LedgerStore doesn't have DeleteState, we use SaveState with nil value
			if err := b.store.SaveState(op.key, nil); err != nil {
				return err
			}
		} else {
			if err := b.store.SaveState(op.key, op.value); err != nil {
				return err
			}
		}
	}
	return nil
}

func (b *storageBatch) Reset() {
	b.ops = b.ops[:0]
	b.size = 0
}

func (b *storageBatch) Replay(w ethdb.KeyValueWriter) error {
	for _, op := range b.ops {
		if op.delete {
			if err := w.Delete(op.key); err != nil {
				return err
			}
		} else {
			if err := w.Put(op.key, op.value); err != nil {
				return err
			}
		}
	}
	return nil
}

// storageSnapshot implements ethdb.Snapshot
type storageSnapshot struct {
	store storage.LedgerStore
}

func (s *storageSnapshot) Has(key []byte) (bool, error) {
	val, err := s.store.GetState(key)
	if err == storage.ErrNotFound {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return val != nil, nil
}

func (s *storageSnapshot) Get(key []byte) ([]byte, error) {
	val, err := s.store.GetState(key)
	if err == storage.ErrNotFound {
		return nil, fmt.Errorf("not found")
	}
	return val, err
}

func (s *storageSnapshot) Release() {}

// emptyIterator implements ethdb.Iterator with no data
type emptyIterator struct{}

func (e *emptyIterator) Next() bool    { return false }
func (e *emptyIterator) Error() error  { return nil }
func (e *emptyIterator) Key() []byte   { return nil }
func (e *emptyIterator) Value() []byte { return nil }
func (e *emptyIterator) Release()      {}

// makeTrieKey creates a prefixed key for trie storage
func makeTrieKey(key []byte) []byte {
	return append([]byte("trie:"), key...)
}

// trieBatch implements ethdb.Batch
type trieBatch struct {
	db      *TrieDatabase
	writes  map[string][]byte
	deletes map[string]bool
	size    int
	mu      sync.Mutex
}

// Put adds a put operation to the batch
func (b *trieBatch) Put(key []byte, value []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.writes[string(key)] = common.CopyBytes(value)
	b.size += len(key) + len(value)
	delete(b.deletes, string(key))
	return nil
}

// Delete adds a delete operation to the batch
func (b *trieBatch) Delete(key []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.deletes[string(key)] = true
	delete(b.writes, string(key))
	b.size += len(key)
	return nil
}

// ValueSize returns the size of the batch
func (b *trieBatch) ValueSize() int {
	return b.size
}

// Write executes the batch
func (b *trieBatch) Write() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Apply deletes
	for key := range b.deletes {
		if err := b.db.Delete([]byte(key)); err != nil {
			return err
		}
	}

	// Apply writes
	for key, value := range b.writes {
		if err := b.db.Put([]byte(key), value); err != nil {
			return err
		}
	}

	return nil
}

// Reset resets the batch
func (b *trieBatch) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.writes = make(map[string][]byte)
	b.deletes = make(map[string]bool)
	b.size = 0
}

// Replay replays the batch to another batch
func (b *trieBatch) Replay(w ethdb.KeyValueWriter) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Apply deletes
	for key := range b.deletes {
		if err := w.Delete([]byte(key)); err != nil {
			return err
		}
	}

	// Apply writes
	for key, value := range b.writes {
		if err := w.Put([]byte(key), value); err != nil {
			return err
		}
	}

	return nil
}

// trieIterator implements ethdb.Iterator
type trieIterator struct {
	db       *TrieDatabase
	prefix   []byte
	start    []byte
	key      []byte
	value    []byte
	err      error
	keys     []string
	values   [][]byte
	index    int
	finished bool
}

// Next advances the iterator
func (i *trieIterator) Next() bool {
	// If we have an error or are finished, stop iteration
	if i.err != nil || i.finished {
		return false
	}

	// Initialize on first call
	if i.keys == nil {
		i.initializeIterator()
		if i.err != nil {
			return false
		}
	}

	// Advance to next item
	i.index++
	if i.index >= len(i.keys) {
		i.finished = true
		i.key = nil
		i.value = nil
		return false
	}

	// Set current key/value
	i.key = []byte(i.keys[i.index])
	i.value = i.values[i.index]
	return true
}

// initializeIterator loads keys/values for iteration
func (i *trieIterator) initializeIterator() {
	i.keys = make([]string, 0)
	i.values = make([][]byte, 0)
	i.index = -1

	// First, try to iterate through cache
	i.db.cache.mu.RLock()
	for k, v := range i.db.cache.nodes {
		// Apply prefix filter
		if i.prefix != nil && !bytes.HasPrefix([]byte(k), i.prefix) {
			continue
		}

		// Apply start filter
		if i.start != nil && bytes.Compare([]byte(k), i.start) < 0 {
			continue
		}

		i.keys = append(i.keys, k)
		i.values = append(i.values, v)
	}
	i.db.cache.mu.RUnlock()

	// Sort keys for ordered iteration
	if len(i.keys) > 1 {
		// Simple bubble sort for small datasets
		for j := 0; j < len(i.keys)-1; j++ {
			for k := 0; k < len(i.keys)-j-1; k++ {
				if bytes.Compare([]byte(i.keys[k]), []byte(i.keys[k+1])) > 0 {
					i.keys[k], i.keys[k+1] = i.keys[k+1], i.keys[k]
					i.values[k], i.values[k+1] = i.values[k+1], i.values[k]
				}
			}
		}
	}
}

// Error returns any error
func (i *trieIterator) Error() error {
	return i.err
}

// Key returns the current key
func (i *trieIterator) Key() []byte {
	if i.key == nil {
		return nil
	}
	// Remove the "trie:" prefix before returning
	if len(i.key) > len("trie:") {
		return i.key[len("trie:"):]
	}
	return i.key
}

// Value returns the current value
func (i *trieIterator) Value() []byte {
	return i.value
}

// Release releases the iterator
func (i *trieIterator) Release() {
	i.key = nil
	i.value = nil
	i.err = nil
}

// trieSnapshot implements ethdb.Snapshot
type trieSnapshot struct {
	db       *TrieDatabase
	version  int64
	data     map[string][]byte
	refCount int
	mu       sync.RWMutex
}

// Has checks if the snapshot contains a key
func (s *trieSnapshot) Has(key []byte) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	_, exists := s.data[string(key)]
	return exists, nil
}

// Get retrieves a value from the snapshot
func (s *trieSnapshot) Get(key []byte) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if value, exists := s.data[string(key)]; exists {
		return common.CopyBytes(value), nil
	}

	// Fall back to database
	return s.db.Get(key)
}

// Release decreases the reference counter
func (s *trieSnapshot) Release() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.refCount--
	if s.refCount <= 0 {
		// Clear data when no more references
		s.data = nil
	}
}

// Cache implementation

// Has checks if key exists in cache
func (c *TrieCache) Has(key string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	_, exists := c.nodes[key]
	return exists
}

// Get retrieves value from cache
func (c *TrieCache) Get(key string) []byte {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.nodes[key]
}

// Put adds value to cache
func (c *TrieCache) Put(key string, value []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check size limit
	if c.size+len(value) > c.maxSize {
		// Simple eviction: remove 10% of entries
		toRemove := len(c.nodes) / 10
		for k := range c.nodes {
			if toRemove <= 0 {
				break
			}
			delete(c.nodes, k)
			toRemove--
		}
	}

	c.nodes[key] = value
	c.size += len(value)
}

// Delete removes value from cache
func (c *TrieCache) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if value, exists := c.nodes[key]; exists {
		c.size -= len(value)
		delete(c.nodes, key)
	}
}

// Clear clears the cache
func (c *TrieCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.nodes = make(map[string][]byte)
	c.size = 0
}
