package storage

import (
	"context"
	"fmt"
	"testing"
	"time"

	"diamante/common"
	"diamante/storage/cache"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MockLedgerStore is a simple in-memory implementation for testing
type MockLedgerStore struct {
	blocks       map[int]*common.Block
	transactions map[string]*common.Transaction
	accounts     map[string]*common.Account
	state        map[string]interface{}
	receipts     map[string]*Receipt
	isOpen       bool
	stats        StoreStats
}

func NewMockLedgerStore() *MockLedgerStore {
	return &MockLedgerStore{
		blocks:       make(map[int]*common.Block),
		transactions: make(map[string]*common.Transaction),
		accounts:     make(map[string]*common.Account),
		state:        make(map[string]interface{}),
		receipts:     make(map[string]*Receipt),
	}
}

func (m *MockLedgerStore) Open() error {
	m.isOpen = true
	return nil
}

func (m *MockLedgerStore) Close() error {
	m.isOpen = false
	return nil
}

func (m *MockLedgerStore) IsOpen() bool {
	return m.isOpen
}

func (m *MockLedgerStore) GetBlock(height uint64) (*common.Block, error) {
	if block, ok := m.blocks[int(height)]; ok {
		m.stats.PrimaryReads++
		return block, nil
	}
	return nil, ErrNotFound
}

func (m *MockLedgerStore) SaveBlock(block *common.Block) error {
	m.blocks[block.Number] = block
	return nil
}

func (m *MockLedgerStore) GetLatestBlock() (*common.Block, error) {
	var latest *common.Block
	for _, block := range m.blocks {
		if latest == nil || block.Number > latest.Number {
			latest = block
		}
	}
	if latest == nil {
		return nil, ErrNotFound
	}
	return latest, nil
}

func (m *MockLedgerStore) GetTransaction(id string) (*common.Transaction, error) {
	if tx, ok := m.transactions[id]; ok {
		return tx, nil
	}
	return nil, ErrNotFound
}

func (m *MockLedgerStore) SaveTransaction(tx *common.Transaction, blockHeight int) error {
	m.transactions[tx.ID] = tx
	return nil
}

func (m *MockLedgerStore) GetAccount(address string) (*common.Account, error) {
	if acc, ok := m.accounts[address]; ok {
		return acc, nil
	}
	return nil, ErrNotFound
}

func (m *MockLedgerStore) SaveAccount(account *common.Account) error {
	m.accounts[account.ID] = account
	return nil
}

func (m *MockLedgerStore) GetState(key []byte) ([]byte, error) {
	if val, ok := m.state[string(key)]; ok {
		if data, ok := val.([]byte); ok {
			return data, nil
		}
		// Convert to []byte if it's a string
		if str, ok := val.(string); ok {
			return []byte(str), nil
		}
	}
	return nil, ErrNotFound
}

func (m *MockLedgerStore) SaveState(key []byte, value []byte) error {
	m.state[string(key)] = value
	return nil
}

func (m *MockLedgerStore) GetContract(address string) (*Contract, error) {
	return nil, ErrNotFound
}

func (m *MockLedgerStore) SaveContract(contract *Contract) error {
	return nil
}

func (m *MockLedgerStore) GetReceipt(txID string) (*Receipt, error) {
	if receipt, ok := m.receipts[txID]; ok {
		return receipt, nil
	}
	return nil, ErrNotFound
}

func (m *MockLedgerStore) SaveReceipt(receipt *Receipt) error {
	m.receipts[receipt.TxID] = receipt
	return nil
}

func (m *MockLedgerStore) WriteBatch(batch WriteBatch) error {
	for _, block := range batch.Blocks {
		m.blocks[block.Number] = block
	}
	for _, tx := range batch.Transactions {
		m.transactions[tx.ID] = tx
	}
	for _, acc := range batch.Accounts {
		m.accounts[acc.ID] = acc
	}
	return nil
}

func (m *MockLedgerStore) Snapshot(path string) error {
	return nil
}

func (m *MockLedgerStore) Restore(path string) error {
	return nil
}

func (m *MockLedgerStore) HealthCheck(ctx context.Context) error {
	if !m.isOpen {
		return fmt.Errorf("store is closed")
	}
	return nil
}

func (m *MockLedgerStore) GetStats() (*StoreStats, error) {
	return &m.stats, nil
}

func (m *MockLedgerStore) ReplaceBlockSameHeight(height uint64, newBlock *common.Block) error {
	return nil
}

func (m *MockLedgerStore) Compact() error {
	// Mock implementation - do nothing
	return nil
}

func (m *MockLedgerStore) GetBlockByHash(hash string) (*common.Block, error) {
	for _, block := range m.blocks {
		if block.Hash == hash {
			return block, nil
		}
	}
	return nil, ErrNotFound
}

func (m *MockLedgerStore) DeleteState(key []byte) error {
	delete(m.state, string(key))
	return nil
}

func (m *MockLedgerStore) GetAccountCount() (int, error) {
	return len(m.accounts), nil
}

func (m *MockLedgerStore) GetTransactionCount() (int, error) {
	return len(m.transactions), nil
}

func (m *MockLedgerStore) GetBlockCount() (int, error) {
	return len(m.blocks), nil
}

func (m *MockLedgerStore) GetContractCount() (int, error) {
	return 0, nil
}

func (m *MockLedgerStore) BeginTransaction() error {
	return nil
}

func (m *MockLedgerStore) CommitTransaction() error {
	return nil
}

func (m *MockLedgerStore) RollbackTransaction() error {
	return nil
}

func (m *MockLedgerStore) Backup(backupPath string) error {
	return nil
}

func (m *MockLedgerStore) GetTransactionRange(start, end uint64) ([]*common.Transaction, error) {
	var txs []*common.Transaction
	for _, tx := range m.transactions {
		txs = append(txs, tx)
	}
	return txs, nil
}

func (m *MockLedgerStore) GetBlockRange(start, end uint64) ([]*common.Block, error) {
	var blocks []*common.Block
	for i := start; i <= end; i++ {
		if block, ok := m.blocks[int(i)]; ok {
			blocks = append(blocks, block)
		}
	}
	return blocks, nil
}

func (m *MockLedgerStore) GetAccountHistory(address string, limit int) ([]*common.Transaction, error) {
	var txs []*common.Transaction
	count := 0
	for _, tx := range m.transactions {
		if tx.Sender == address || tx.Receiver == address {
			txs = append(txs, tx)
			count++
			if count >= limit {
				break
			}
		}
	}
	return txs, nil
}

func (m *MockLedgerStore) GetAllAccounts() ([]*common.Account, error) {
	var accounts []*common.Account
	for _, acc := range m.accounts {
		accounts = append(accounts, acc)
	}
	return accounts, nil
}

func (m *MockLedgerStore) CreateSnapshot(height uint64) error {
	return nil
}

func (m *MockLedgerStore) DeleteContract(contractID string) error {
	return nil
}

func (m *MockLedgerStore) GetBalance(address string) (float64, error) {
	if acc, ok := m.accounts[address]; ok {
		return acc.Balance, nil
	}
	return 0, ErrNotFound
}

func (m *MockLedgerStore) GetNonce(address string) (uint64, error) {
	if acc, ok := m.accounts[address]; ok {
		return uint64(acc.Nonce), nil
	}
	return 0, ErrNotFound
}

func (m *MockLedgerStore) GetTransactionsByAddress(address string, limit, offset int) ([]*common.Transaction, error) {
	var txs []*common.Transaction
	count := 0
	for _, tx := range m.transactions {
		if tx.Sender == address || tx.Receiver == address {
			if count >= offset {
				txs = append(txs, tx)
				if len(txs) >= limit {
					break
				}
			}
			count++
		}
	}
	return txs, nil
}

func (m *MockLedgerStore) GetTransactionsByBlock(blockHeight uint64) ([]*common.Transaction, error) {
	if block, ok := m.blocks[int(blockHeight)]; ok {
		var txs []*common.Transaction
		for _, tx := range block.Transactions {
			txs = append(txs, &tx)
		}
		return txs, nil
	}
	return nil, ErrNotFound
}

func (m *MockLedgerStore) PruneData(olderThan time.Time) error {
	// Mock implementation - remove old blocks
	cutoffTime := olderThan.Unix()
	for height, block := range m.blocks {
		if block.Timestamp < cutoffTime {
			delete(m.blocks, height)
		}
	}
	return nil
}

// Tests

func TestTieredStorageManager_Basic(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)

	// Create mock primary storage
	primary := NewMockLedgerStore()

	// Create tiered storage config without cache for basic test
	config := &TieredStorageConfig{
		CacheEnabled:   false,
		ArchiveEnabled: false,
		MetricsEnabled: true,
	}

	// Create tiered storage manager
	tsm, err := NewTieredStorageManager(config, primary, logger)
	require.NoError(t, err)

	// Open storage
	err = tsm.Open()
	require.NoError(t, err)
	defer tsm.Close()

	// Test block operations
	block := &common.Block{
		Number:       1,
		Hash:         "block1",
		PreviousHash: "genesis",
		Timestamp:    time.Now().Unix(),
		Transactions: []common.Transaction{
			{ID: "tx1", Sender: "alice", Receiver: "bob", Amount: 100},
		},
	}

	// Save block
	err = tsm.SaveBlock(block)
	assert.NoError(t, err)

	// Get block
	retrieved, err := tsm.GetBlock(1)
	assert.NoError(t, err)
	assert.Equal(t, block.Hash, retrieved.Hash)

	// Test transaction operations
	tx := &common.Transaction{
		ID:       "tx2",
		Sender:   "bob",
		Receiver: "charlie",
		Amount:   50,
	}

	err = tsm.SaveTransaction(tx, 1) // Save with block height 1
	assert.NoError(t, err)

	retrieved_tx, err := tsm.GetTransaction("tx2")
	assert.NoError(t, err)
	assert.Equal(t, tx.Amount, retrieved_tx.Amount)

	// Test account operations
	account := &common.Account{
		ID:      "alice",
		Balance: 1000,
		Nonce:   1,
	}

	err = tsm.SaveAccount(account)
	assert.NoError(t, err)

	retrieved_acc, err := tsm.GetAccount("alice")
	assert.NoError(t, err)
	assert.Equal(t, account.Balance, retrieved_acc.Balance)

	// Check stats
	stats, err := tsm.GetStats()
	assert.NoError(t, err)
	assert.Greater(t, stats.PrimaryReads, uint64(0))
}

func TestTieredStorageManager_WithCache(t *testing.T) {
	t.Skip("Skipping cache test - requires Redis")

	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)

	// Create mock primary storage
	primary := NewMockLedgerStore()

	// Create tiered storage config with cache
	config := &TieredStorageConfig{
		CacheEnabled:   true,
		CacheConfig:    cache.DefaultRedisCacheConfig(),
		ArchiveEnabled: false,
		MetricsEnabled: true,
	}

	// Create tiered storage manager
	tsm, err := NewTieredStorageManager(config, primary, logger)
	require.NoError(t, err)

	// Open storage
	err = tsm.Open()
	require.NoError(t, err)
	defer tsm.Close()

	// Save and retrieve block multiple times to test caching
	block := &common.Block{
		Number:    1,
		Hash:      "block1",
		Timestamp: time.Now().Unix(),
	}

	err = tsm.SaveBlock(block)
	require.NoError(t, err)

	// First get - cache miss
	_, err = tsm.GetBlock(1)
	require.NoError(t, err)

	// Second get - should be cache hit
	_, err = tsm.GetBlock(1)
	require.NoError(t, err)

	// Check stats
	stats, err := tsm.GetStats()
	assert.NoError(t, err)
	assert.Greater(t, stats.CacheHits, uint64(0))
	assert.Greater(t, stats.CacheMisses, uint64(0))
}

func TestTieredStorageManager_WriteBatch(t *testing.T) {
	logger := logrus.New()
	primary := NewMockLedgerStore()

	config := &TieredStorageConfig{
		CacheEnabled:   false,
		ArchiveEnabled: false,
	}

	tsm, err := NewTieredStorageManager(config, primary, logger)
	require.NoError(t, err)

	err = tsm.Open()
	require.NoError(t, err)
	defer tsm.Close()

	// Create batch
	batch := WriteBatch{
		Blocks: []*common.Block{
			{Number: 1, Hash: "block1"},
			{Number: 2, Hash: "block2"},
		},
		Transactions: []*common.Transaction{
			{ID: "tx1", Sender: "alice", Receiver: "bob", Amount: 100},
			{ID: "tx2", Sender: "bob", Receiver: "charlie", Amount: 50},
		},
		Accounts: []*common.Account{
			{ID: "alice", Balance: 900},
			{ID: "bob", Balance: 150},
			{ID: "charlie", Balance: 50},
		},
	}

	// Execute batch
	err = tsm.WriteBatch(batch)
	assert.NoError(t, err)

	// Verify all items were saved
	block1, err := tsm.GetBlock(1)
	assert.NoError(t, err)
	assert.Equal(t, "block1", block1.Hash)

	tx1, err := tsm.GetTransaction("tx1")
	assert.NoError(t, err)
	assert.Equal(t, float64(100), tx1.Amount)

	alice, err := tsm.GetAccount("alice")
	assert.NoError(t, err)
	assert.Equal(t, float64(900), alice.Balance)
}

func TestStorageFactory(t *testing.T) {
	logger := logrus.New()

	// Test LMDB storage creation
	config := &StorageFactoryConfig{
		Type:          StorageTypeLMDB,
		LMDBPath:      "/tmp/test_lmdb",
		LMDBCacheSize: 100,
		Logger:        logger,
	}

	storage, err := NewStorageFromConfig(config)
	assert.NoError(t, err)
	assert.NotNil(t, storage)

	// Clean up
	if storage != nil {
		storage.Close()
	}
}
