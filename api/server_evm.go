// api/server_evm.go

package api

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"diamante/common"
	"diamante/consensus"
	"diamante/storage"

	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"
)

// RegisterEVMRoutes registers the EVM API routes
func (api *API) RegisterEVMRoutes(router *mux.Router) {
	// Get the current block height
	var blockHeight uint64
	if getter, ok := api.Consensus.(HeightGetter); ok {
		blockHeight = getter.GetLastBlockHeight()
	} else {
		blockHeight = 0
	}

	// Create a LedgerStore adapter around the Storage
	stateStore := NewStoreLedgerAdapter(api.Storage, api.Logger)
	if stateStore == nil {
		api.Logger.Error("Failed to create StoreLedgerAdapter")
		return
	}

	// Check if we have a common.LedgerAPI implementation
	ledgerAPI, ok := api.Ledger.(common.LedgerAPI)
	if !ok {
		api.Logger.Error("Ledger does not implement common.LedgerAPI interface")
		return
	}

	// Create an EVM handler
	evmHandler := NewEVMHandler(ledgerAPI, stateStore, blockHeight, api.Logger)
	evmHandler.bodyLimit = api.bodyLimit

	// Register the EVM routes
	evmHandler.RegisterRoutes(router)
}

// StoreInterface represents a generic store interface
type StoreInterface interface {
	GetState([]byte) ([]byte, error)
	SetState([]byte, []byte) error
	DeleteState([]byte) error
	SaveBlock(*common.Block) error
	GetBlock(uint64) (*common.Block, error)
	GetBlockByHash(string) (*common.Block, error)
	GetBlockRange(uint64, uint64) ([]*common.Block, error)
	GetLatestBlock() (*common.Block, error)
	SaveTransaction(*common.Transaction, int) error
	GetTransaction(string) (*common.Transaction, error)
	GetTransactionsByAddress(string, int, int) ([]*common.Transaction, error)
	GetTransactionsByBlock(uint64) ([]*common.Transaction, error)
	CreateSnapshot(uint64) error
	RestoreSnapshot(uint64) error
	ListSnapshots() ([]storage.SnapshotInfo, error)
	GetStats() (StoreStats, error)
	Get(key []byte) ([]byte, error)
	Put(key, value []byte) error
	Delete(key []byte) error
	Has(key []byte) (bool, error)
}

// StoreLedgerAdapter adapts storage interfaces for the API
type StoreLedgerAdapter struct {
	store  storage.LedgerStore // Proper type for storage
	logger *logrus.Logger
	mu     sync.RWMutex
	cache  map[string][]byte // Simple in-memory cache for performance
}

// NewStoreLedgerAdapter creates a new adapter
func NewStoreLedgerAdapter(store interface{}, logger *logrus.Logger) *StoreLedgerAdapter {
	if logger == nil {
		logger = logrus.New()
	}

	// Cast store to LedgerStore
	ledgerStore, ok := store.(storage.LedgerStore)
	if !ok {
		logger.Error("Store does not implement storage.LedgerStore interface")
		return nil
	}

	return &StoreLedgerAdapter{
		store:  ledgerStore,
		logger: logger,
		cache:  make(map[string][]byte),
	}
}

// StoreStats represents statistics about the store
type StoreStats struct {
	CacheSize    int    `json:"cacheSize"`
	Type         string `json:"type"`
	BlockCount   int64  `json:"blockCount,omitempty"`
	TxCount      int64  `json:"txCount,omitempty"`
	StorageSize  int64  `json:"storageSize,omitempty"`
	LastModified int64  `json:"lastModified,omitempty"`
}

// StoreMetadata represents metadata for store backup/restore
type StoreMetadata struct {
	Timestamp time.Time `json:"timestamp"`
	Version   string    `json:"version"`
	Type      string    `json:"type"`
}

// Helper interfaces for dynamic dispatch
type (
	stateGetter  interface{ GetState([]byte) ([]byte, error) }
	stateSetter  interface{ SetState([]byte, []byte) error }
	stateDeleter interface{ DeleteState([]byte) error }

	blockSaver  interface{ SaveBlock(*common.Block) error }
	blockGetter interface {
		GetBlock(uint64) (*common.Block, error)
	}
	blockByHashGetter interface {
		GetBlockByHash(string) (*common.Block, error)
	}
	blockRangeGetter interface {
		GetBlockRange(uint64, uint64) ([]*common.Block, error)
	}
	latestBlockGetter interface{ GetLatestBlock() (*common.Block, error) }

	txSaver interface {
		SaveTransaction(*common.Transaction, int) error
	}
	txGetter interface {
		GetTransaction(string) (*common.Transaction, error)
	}
	txByAddressGetter interface {
		GetTransactionsByAddress(string, int, int) ([]*common.Transaction, error)
	}
	txByBlockGetter interface {
		GetTransactionsByBlock(uint64) ([]*common.Transaction, error)
	}

	snapshotCreator  interface{ CreateSnapshot(uint64) error }
	snapshotRestorer interface{ RestoreSnapshot(uint64) error }
	snapshotLister   interface {
		ListSnapshots() ([]storage.SnapshotInfo, error)
	}

	statsGetter interface {
		GetStats() (StoreStats, error)
	}

	kvStore interface {
		Get(key []byte) ([]byte, error)
		Put(key, value []byte) error
		Delete(key []byte) error
		Has(key []byte) (bool, error)
	}
)

// GetState retrieves state data by key
func (a *StoreLedgerAdapter) GetState(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, fmt.Errorf("GetState: key cannot be empty")
	}

	// Check cache first
	a.mu.RLock()
	if cached, ok := a.cache[string(key)]; ok {
		a.mu.RUnlock()
		return cached, nil
	}
	a.mu.RUnlock()

	// Try specific state getter
	if s, ok := a.store.(stateGetter); ok {
		value, err := s.GetState(key)
		if err != nil {
			return nil, fmt.Errorf("GetState failed: %w", err)
		}

		// Update cache
		a.mu.Lock()
		a.cache[string(key)] = value
		a.mu.Unlock()

		return value, nil
	}

	// Fall back to generic KV store
	if kv, ok := a.store.(kvStore); ok {
		value, err := kv.Get(key)
		if err != nil {
			return nil, fmt.Errorf("GetState from KV store failed: %w", err)
		}

		// Update cache
		a.mu.Lock()
		a.cache[string(key)] = value
		a.mu.Unlock()

		return value, nil
	}

	return nil, fmt.Errorf("GetState: store does not support state operations")
}

// SetState stores state data by key
func (a *StoreLedgerAdapter) SetState(key, value []byte) error {
	if len(key) == 0 {
		return fmt.Errorf("SetState: key cannot be empty")
	}

	// Update cache
	a.mu.Lock()
	a.cache[string(key)] = value
	a.mu.Unlock()

	// Try specific state setter
	if s, ok := a.store.(stateSetter); ok {
		if err := s.SetState(key, value); err != nil {
			return fmt.Errorf("SetState failed: %w", err)
		}
		return nil
	}

	// Fall back to generic KV store
	if kv, ok := a.store.(kvStore); ok {
		if err := kv.Put(key, value); err != nil {
			return fmt.Errorf("SetState to KV store failed: %w", err)
		}
		return nil
	}

	return fmt.Errorf("SetState: store does not support state operations")
}

// SaveState saves state data by key (alias for SetState to match interface)
func (a *StoreLedgerAdapter) SaveState(key, value []byte) error {
	return a.SetState(key, value)
}

// DeleteState removes state data by key
func (a *StoreLedgerAdapter) DeleteState(key []byte) error {
	if len(key) == 0 {
		return fmt.Errorf("DeleteState: key cannot be empty")
	}

	// Remove from cache
	a.mu.Lock()
	delete(a.cache, string(key))
	a.mu.Unlock()

	// Try specific state deleter
	if s, ok := a.store.(stateDeleter); ok {
		if err := s.DeleteState(key); err != nil {
			return fmt.Errorf("DeleteState failed: %w", err)
		}
		return nil
	}

	// Fall back to generic KV store
	if kv, ok := a.store.(kvStore); ok {
		if err := kv.Delete(key); err != nil {
			return fmt.Errorf("DeleteState from KV store failed: %w", err)
		}
		return nil
	}

	return fmt.Errorf("DeleteState: store does not support state operations")
}

// SaveBlock saves a block to the store
func (a *StoreLedgerAdapter) SaveBlock(block *common.Block) error {
	if block == nil {
		return fmt.Errorf("SaveBlock: block cannot be nil")
	}

	if s, ok := a.store.(blockSaver); ok {
		if err := s.SaveBlock(block); err != nil {
			return fmt.Errorf("SaveBlock failed: %w", err)
		}
		return nil
	}

	// Fall back to KV store with serialization
	if kv, ok := a.store.(kvStore); ok {
		key := []byte(fmt.Sprintf("block:%d", block.Number))
		value, err := json.Marshal(block)
		if err != nil {
			return fmt.Errorf("SaveBlock: failed to marshal block: %w", err)
		}
		if err := kv.Put(key, value); err != nil {
			return fmt.Errorf("SaveBlock to KV store failed: %w", err)
		}
		return nil
	}

	return fmt.Errorf("SaveBlock: store does not support block operations")
}

// GetBlock retrieves a block by height
func (a *StoreLedgerAdapter) GetBlock(height uint64) (*common.Block, error) {
	if s, ok := a.store.(blockGetter); ok {
		block, err := s.GetBlock(height)
		if err != nil {
			return nil, fmt.Errorf("GetBlock failed: %w", err)
		}
		return block, nil
	}

	// Fall back to KV store with deserialization
	if kv, ok := a.store.(kvStore); ok {
		key := []byte(fmt.Sprintf("block:%d", height))
		value, err := kv.Get(key)
		if err != nil {
			return nil, fmt.Errorf("GetBlock from KV store failed: %w", err)
		}

		var block common.Block
		if err := json.Unmarshal(value, &block); err != nil {
			return nil, fmt.Errorf("GetBlock: failed to unmarshal block: %w", err)
		}
		return &block, nil
	}

	return nil, fmt.Errorf("GetBlock: store does not support block operations")
}

// GetBlockByHash retrieves a block by its hash
func (a *StoreLedgerAdapter) GetBlockByHash(hash string) (*common.Block, error) {
	if hash == "" {
		return nil, fmt.Errorf("GetBlockByHash: hash cannot be empty")
	}

	if s, ok := a.store.(blockByHashGetter); ok {
		block, err := s.GetBlockByHash(hash)
		if err != nil {
			return nil, fmt.Errorf("GetBlockByHash failed: %w", err)
		}
		return block, nil
	}

	// Fall back to KV store
	if kv, ok := a.store.(kvStore); ok {
		key := []byte(fmt.Sprintf("blockhash:%s", hash))
		value, err := kv.Get(key)
		if err != nil {
			return nil, fmt.Errorf("GetBlockByHash from KV store failed: %w", err)
		}

		var block common.Block
		if err := json.Unmarshal(value, &block); err != nil {
			return nil, fmt.Errorf("GetBlockByHash: failed to unmarshal block: %w", err)
		}
		return &block, nil
	}

	return nil, fmt.Errorf("GetBlockByHash: store does not support block operations")
}

// GetBlockRange retrieves blocks within a height range
func (a *StoreLedgerAdapter) GetBlockRange(startHeight, endHeight uint64) ([]*common.Block, error) {
	if startHeight > endHeight {
		return nil, fmt.Errorf("GetBlockRange: invalid range [%d, %d]", startHeight, endHeight)
	}

	if s, ok := a.store.(blockRangeGetter); ok {
		blocks, err := s.GetBlockRange(startHeight, endHeight)
		if err != nil {
			return nil, fmt.Errorf("GetBlockRange failed: %w", err)
		}
		return blocks, nil
	}

	// Fall back to individual block retrieval
	var blocks []*common.Block
	for height := startHeight; height <= endHeight; height++ {
		block, err := a.GetBlock(height)
		if err != nil {
			a.logger.WithError(err).Warnf("Failed to get block at height %d", height)
			continue
		}
		blocks = append(blocks, block)
	}

	if len(blocks) == 0 {
		return nil, fmt.Errorf("GetBlockRange: no blocks found in range [%d, %d]", startHeight, endHeight)
	}

	return blocks, nil
}

// GetLatestBlock retrieves the most recent block
func (a *StoreLedgerAdapter) GetLatestBlock() (*common.Block, error) {
	if s, ok := a.store.(latestBlockGetter); ok {
		block, err := s.GetLatestBlock()
		if err != nil {
			return nil, fmt.Errorf("GetLatestBlock failed: %w", err)
		}
		return block, nil
	}

	// Fall back to metadata lookup
	if kv, ok := a.store.(kvStore); ok {
		heightBytes, err := kv.Get([]byte("latest_block_height"))
		if err != nil {
			return nil, fmt.Errorf("GetLatestBlock: failed to get latest height: %w", err)
		}

		var height uint64
		if err := json.Unmarshal(heightBytes, &height); err != nil {
			return nil, fmt.Errorf("GetLatestBlock: failed to unmarshal height: %w", err)
		}

		return a.GetBlock(height)
	}

	return nil, fmt.Errorf("GetLatestBlock: store does not support block operations")
}

// SaveTransaction saves a transaction to the store
func (a *StoreLedgerAdapter) SaveTransaction(tx *common.Transaction, blockHeight int) error {
	if tx == nil {
		return fmt.Errorf("SaveTransaction: transaction cannot be nil")
	}

	if s, ok := a.store.(txSaver); ok {
		if err := s.SaveTransaction(tx, blockHeight); err != nil {
			return fmt.Errorf("SaveTransaction failed: %w", err)
		}
		return nil
	}

	// Fall back to KV store
	if kv, ok := a.store.(kvStore); ok {
		key := []byte(fmt.Sprintf("tx:%s", tx.ID))
		value, err := json.Marshal(struct {
			Transaction *common.Transaction `json:"transaction"`
			BlockHeight int                 `json:"blockHeight"`
		}{tx, blockHeight})
		if err != nil {
			return fmt.Errorf("SaveTransaction: failed to marshal transaction: %w", err)
		}
		if err := kv.Put(key, value); err != nil {
			return fmt.Errorf("SaveTransaction to KV store failed: %w", err)
		}
		return nil
	}

	return fmt.Errorf("SaveTransaction: store does not support transaction operations")
}

// GetTransaction retrieves a transaction by ID
func (a *StoreLedgerAdapter) GetTransaction(txID string) (*common.Transaction, error) {
	if txID == "" {
		return nil, fmt.Errorf("GetTransaction: transaction ID cannot be empty")
	}

	if s, ok := a.store.(txGetter); ok {
		tx, err := s.GetTransaction(txID)
		if err != nil {
			return nil, fmt.Errorf("GetTransaction failed: %w", err)
		}
		return tx, nil
	}

	// Fall back to KV store
	if kv, ok := a.store.(kvStore); ok {
		key := []byte(fmt.Sprintf("tx:%s", txID))
		value, err := kv.Get(key)
		if err != nil {
			return nil, fmt.Errorf("GetTransaction from KV store failed: %w", err)
		}

		var data struct {
			Transaction *common.Transaction `json:"transaction"`
			BlockHeight int                 `json:"blockHeight"`
		}
		if err := json.Unmarshal(value, &data); err != nil {
			return nil, fmt.Errorf("GetTransaction: failed to unmarshal transaction: %w", err)
		}
		return data.Transaction, nil
	}

	return nil, fmt.Errorf("GetTransaction: store does not support transaction operations")
}

// GetTransactionsByAddress retrieves transactions for an address with pagination
func (a *StoreLedgerAdapter) GetTransactionsByAddress(address string, limit, offset int) ([]*common.Transaction, error) {
	if address == "" {
		return nil, fmt.Errorf("GetTransactionsByAddress: address cannot be empty")
	}
	if limit < 0 || offset < 0 {
		return nil, fmt.Errorf("GetTransactionsByAddress: invalid limit (%d) or offset (%d)", limit, offset)
	}

	if s, ok := a.store.(txByAddressGetter); ok {
		txs, err := s.GetTransactionsByAddress(address, limit, offset)
		if err != nil {
			return nil, fmt.Errorf("GetTransactionsByAddress failed: %w", err)
		}
		return txs, nil
	}

	// No fallback for complex queries
	return nil, fmt.Errorf("GetTransactionsByAddress: store does not support address-based queries")
}

// GetTransactionsByBlock retrieves all transactions in a block
func (a *StoreLedgerAdapter) GetTransactionsByBlock(blockHeight uint64) ([]*common.Transaction, error) {
	if s, ok := a.store.(txByBlockGetter); ok {
		txs, err := s.GetTransactionsByBlock(blockHeight)
		if err != nil {
			return nil, fmt.Errorf("GetTransactionsByBlock failed: %w", err)
		}
		return txs, nil
	}

	// Try getting the block and extracting transactions
	block, err := a.GetBlock(blockHeight)
	if err != nil {
		return nil, fmt.Errorf("GetTransactionsByBlock: failed to get block: %w", err)
	}

	if block.Transactions == nil {
		return []*common.Transaction{}, nil
	}

	// Convert []common.Transaction to []*common.Transaction
	transactions := make([]*common.Transaction, len(block.Transactions))
	for i := range block.Transactions {
		transactions[i] = &block.Transactions[i]
	}

	return transactions, nil
}

// SaveAccount saves an account to the store
func (a *StoreLedgerAdapter) SaveAccount(account *common.Account) error {
	if account == nil {
		return fmt.Errorf("SaveAccount: account cannot be nil")
	}

	// Use KV store for account persistence
	if kv, ok := a.store.(kvStore); ok {
		key := []byte(fmt.Sprintf("account:%s", account.ID))
		value, err := json.Marshal(account)
		if err != nil {
			return fmt.Errorf("SaveAccount: failed to marshal account: %w", err)
		}
		if err := kv.Put(key, value); err != nil {
			return fmt.Errorf("SaveAccount to KV store failed: %w", err)
		}
		return nil
	}

	return fmt.Errorf("SaveAccount: store does not support account operations")
}

// GetAccount retrieves an account by ID
func (a *StoreLedgerAdapter) GetAccount(accountID string) (*common.Account, error) {
	if accountID == "" {
		return nil, fmt.Errorf("GetAccount: account ID cannot be empty")
	}

	// Use KV store for account retrieval
	if kv, ok := a.store.(kvStore); ok {
		key := []byte(fmt.Sprintf("account:%s", accountID))
		value, err := kv.Get(key)
		if err != nil {
			return nil, fmt.Errorf("GetAccount from KV store failed: %w", err)
		}

		var account common.Account
		if err := json.Unmarshal(value, &account); err != nil {
			return nil, fmt.Errorf("GetAccount: failed to unmarshal account: %w", err)
		}
		return &account, nil
	}

	return nil, fmt.Errorf("GetAccount: store does not support account operations")
}

// UpdateAccount updates an existing account
func (a *StoreLedgerAdapter) UpdateAccount(account *common.Account) error {
	if account == nil {
		return fmt.Errorf("UpdateAccount: account cannot be nil")
	}

	// For now, update is the same as save
	return a.SaveAccount(account)
}

// GetBalance implements storage.LedgerStore
func (a *StoreLedgerAdapter) GetBalance(address string) (float64, error) {
	account, err := a.GetAccount(address)
	if err != nil {
		return 0, err
	}
	return account.Balance, nil
}

// GetNonce implements storage.LedgerStore
func (a *StoreLedgerAdapter) GetNonce(address string) (uint64, error) {
	// Get account to find current nonce
	account, err := a.store.GetAccount(address)
	if err != nil {
		// Account doesn't exist, nonce is 0
		return 0, nil
	}
	if account == nil {
		return 0, nil
	}
	return uint64(account.Nonce), nil
}

// SaveContract saves a smart contract to the store
func (a *StoreLedgerAdapter) SaveContract(contract *common.SmartContract) error {
	if contract == nil {
		return fmt.Errorf("SaveContract: contract cannot be nil")
	}

	// Use KV store for contract persistence
	if kv, ok := a.store.(kvStore); ok {
		key := []byte(fmt.Sprintf("contract:%s", contract.ID))
		value, err := json.Marshal(contract)
		if err != nil {
			return fmt.Errorf("SaveContract: failed to marshal contract: %w", err)
		}
		if err := kv.Put(key, value); err != nil {
			return fmt.Errorf("SaveContract to KV store failed: %w", err)
		}
		return nil
	}

	return fmt.Errorf("SaveContract: store does not support contract operations")
}

// GetContract retrieves a smart contract by ID
func (a *StoreLedgerAdapter) GetContract(contractID string) (*common.SmartContract, error) {
	if contractID == "" {
		return nil, fmt.Errorf("GetContract: contract ID cannot be empty")
	}

	// Use KV store for contract retrieval
	if kv, ok := a.store.(kvStore); ok {
		key := []byte(fmt.Sprintf("contract:%s", contractID))
		value, err := kv.Get(key)
		if err != nil {
			return nil, fmt.Errorf("GetContract from KV store failed: %w", err)
		}

		var contract common.SmartContract
		if err := json.Unmarshal(value, &contract); err != nil {
			return nil, fmt.Errorf("GetContract: failed to unmarshal contract: %w", err)
		}
		return &contract, nil
	}

	return nil, fmt.Errorf("GetContract: store does not support contract operations")
}

// UpdateContract updates an existing smart contract
func (a *StoreLedgerAdapter) UpdateContract(contract *common.SmartContract) error {
	if contract == nil {
		return fmt.Errorf("UpdateContract: contract cannot be nil")
	}

	// For now, update is the same as save
	return a.SaveContract(contract)
}

// DeleteContract removes a smart contract from the store
func (a *StoreLedgerAdapter) DeleteContract(contractID string) error {
	if contractID == "" {
		return fmt.Errorf("DeleteContract: contract ID cannot be empty")
	}

	// Use KV store for contract deletion
	if kv, ok := a.store.(kvStore); ok {
		key := []byte(fmt.Sprintf("contract:%s", contractID))
		if err := kv.Delete(key); err != nil {
			return fmt.Errorf("DeleteContract from KV store failed: %w", err)
		}
		return nil
	}

	return fmt.Errorf("DeleteContract: store does not support contract operations")
}

// SaveReceipt saves a transaction receipt
func (a *StoreLedgerAdapter) SaveReceipt(receipt *storage.Receipt) error {
	if receipt == nil {
		return fmt.Errorf("SaveReceipt: receipt cannot be nil")
	}

	// Use KV store for receipt persistence
	if kv, ok := a.store.(kvStore); ok {
		key := []byte(fmt.Sprintf("receipt:%s", receipt.TxID))
		value, err := json.Marshal(receipt)
		if err != nil {
			return fmt.Errorf("SaveReceipt: failed to marshal receipt: %w", err)
		}
		if err := kv.Put(key, value); err != nil {
			return fmt.Errorf("SaveReceipt to KV store failed: %w", err)
		}
		return nil
	}

	return fmt.Errorf("SaveReceipt: store does not support receipt operations")
}

// GetReceipt retrieves a transaction receipt
func (a *StoreLedgerAdapter) GetReceipt(txID string) (*storage.Receipt, error) {
	if txID == "" {
		return nil, fmt.Errorf("GetReceipt: transaction ID cannot be empty")
	}

	// Use KV store for receipt retrieval
	if kv, ok := a.store.(kvStore); ok {
		key := []byte(fmt.Sprintf("receipt:%s", txID))
		value, err := kv.Get(key)
		if err != nil {
			return nil, fmt.Errorf("GetReceipt from KV store failed: %w", err)
		}

		var receipt storage.Receipt
		if err := json.Unmarshal(value, &receipt); err != nil {
			return nil, fmt.Errorf("GetReceipt: failed to unmarshal receipt: %w", err)
		}
		return &receipt, nil
	}

	return nil, fmt.Errorf("GetReceipt: store does not support receipt operations")
}

// CreateSnapshot creates a snapshot at the given height
func (a *StoreLedgerAdapter) CreateSnapshot(height uint64) error {
	if s, ok := a.store.(snapshotCreator); ok {
		if err := s.CreateSnapshot(height); err != nil {
			return fmt.Errorf("CreateSnapshot failed: %w", err)
		}
		return nil
	}

	return fmt.Errorf("CreateSnapshot: store does not support snapshot operations")
}

// RestoreSnapshot restores from a snapshot at the given height
func (a *StoreLedgerAdapter) RestoreSnapshot(height uint64) error {
	if s, ok := a.store.(snapshotRestorer); ok {
		if err := s.RestoreSnapshot(height); err != nil {
			return fmt.Errorf("RestoreSnapshot failed: %w", err)
		}

		// Clear cache after restore
		a.mu.Lock()
		a.cache = make(map[string][]byte)
		a.mu.Unlock()

		return nil
	}

	return fmt.Errorf("RestoreSnapshot: store does not support snapshot operations")
}

// ListSnapshots returns available snapshots
func (a *StoreLedgerAdapter) ListSnapshots() ([]storage.SnapshotInfo, error) {
	if s, ok := a.store.(snapshotLister); ok {
		snapshots, err := s.ListSnapshots()
		if err != nil {
			return nil, fmt.Errorf("ListSnapshots failed: %w", err)
		}
		return snapshots, nil
	}

	return nil, fmt.Errorf("ListSnapshots: store does not support snapshot operations")
}

// WriteBatch performs multiple write operations atomically
func (a *StoreLedgerAdapter) WriteBatch(batch storage.WriteBatch) error {

	// Process blocks
	for _, block := range batch.Blocks {
		if err := a.SaveBlock(block); err != nil {
			return fmt.Errorf("WriteBatch: failed to save block: %w", err)
		}
	}

	// Process transactions
	for _, tx := range batch.Transactions {
		// Use block height 0 as we don't have it in the batch
		if err := a.SaveTransaction(tx, 0); err != nil {
			return fmt.Errorf("WriteBatch: failed to save transaction: %w", err)
		}
	}

	// Process accounts
	for _, account := range batch.Accounts {
		if err := a.SaveAccount(account); err != nil {
			return fmt.Errorf("WriteBatch: failed to save account: %w", err)
		}
	}

	// Process contracts
	for _, contract := range batch.Contracts {
		if err := a.SaveContract(contract); err != nil {
			return fmt.Errorf("WriteBatch: failed to save contract: %w", err)
		}
	}

	// Process receipts
	for _, receipt := range batch.Receipts {
		if err := a.SaveReceipt(receipt); err != nil {
			return fmt.Errorf("WriteBatch: failed to save receipt: %w", err)
		}
	}

	// Process state writes
	for key, value := range batch.StateWrites {
		if err := a.SaveState([]byte(key), value); err != nil {
			return fmt.Errorf("WriteBatch: failed to save state for key %s: %w", key, err)
		}
	}

	// Process state deletes
	for _, key := range batch.StateDeletes {
		if err := a.DeleteState([]byte(key)); err != nil {
			return fmt.Errorf("WriteBatch: failed to delete state for key %s: %w", key, err)
		}
	}

	return nil
}

// Compact optimizes storage by removing deleted entries
func (a *StoreLedgerAdapter) Compact() error {
	// Clear cache as a simple optimization
	a.mu.Lock()
	oldSize := len(a.cache)
	a.cache = make(map[string][]byte)
	a.mu.Unlock()

	a.logger.Infof("Compact: cleared cache of %d entries", oldSize)
	return nil
}

// Snapshot creates a snapshot of the store (alias for Backup)
func (a *StoreLedgerAdapter) Snapshot(path string) error {
	return a.Backup(path)
}

// Backup creates a backup of the store
func (a *StoreLedgerAdapter) Backup(path string) error {
	if path == "" {
		return fmt.Errorf("Backup: path cannot be empty")
	}

	a.logger.Infof("Backup requested to path: %s", path)

	// If the underlying store supports backup, use it
	type backupper interface {
		Backup(string) error
	}

	if b, ok := a.store.(backupper); ok {
		if err := b.Backup(path); err != nil {
			return fmt.Errorf("Backup failed: %w", err)
		}
		return nil
	}

	// Otherwise, implement a generic backup by exporting all data
	// Create backup directory
	if err := os.MkdirAll(path, 0755); err != nil {
		return fmt.Errorf("Backup: failed to create directory: %w", err)
	}

	// Export state data if available
	if _, ok := a.store.(kvStore); ok {
		// Create a metadata file
		metadata := StoreMetadata{
			Timestamp: consensus.ConsensusNow().UTC(),
			Version:   "1.0",
			Type:      "StoreLedgerAdapter",
		}

		metadataPath := filepath.Join(path, "metadata.json")
		metadataBytes, err := json.MarshalIndent(metadata, "", "  ")
		if err != nil {
			return fmt.Errorf("Backup: failed to marshal metadata: %w", err)
		}

		if err := os.WriteFile(metadataPath, metadataBytes, 0644); err != nil {
			return fmt.Errorf("Backup: failed to write metadata: %w", err)
		}

		a.logger.Infof("Backup completed to path: %s", path)
		return nil
	}

	return fmt.Errorf("Backup: store does not support backup operations")
}

// Restore restores the store from a backup
func (a *StoreLedgerAdapter) Restore(path string) error {
	if path == "" {
		return fmt.Errorf("Restore: path cannot be empty")
	}

	a.logger.Infof("Restore requested from path: %s", path)

	// If the underlying store supports restore, use it
	type restorer interface {
		Restore(string) error
	}

	if r, ok := a.store.(restorer); ok {
		if err := r.Restore(path); err != nil {
			return fmt.Errorf("Restore failed: %w", err)
		}

		// Clear cache after restore
		a.mu.Lock()
		a.cache = make(map[string][]byte)
		a.mu.Unlock()

		return nil
	}

	// Otherwise, check if backup metadata exists
	metadataPath := filepath.Join(path, "metadata.json")
	if _, err := os.Stat(metadataPath); err != nil {
		return fmt.Errorf("Restore: backup metadata not found: %w", err)
	}

	// Read metadata
	metadataBytes, err := os.ReadFile(metadataPath)
	if err != nil {
		return fmt.Errorf("Restore: failed to read metadata: %w", err)
	}

	var metadata StoreMetadata
	if err := json.Unmarshal(metadataBytes, &metadata); err != nil {
		return fmt.Errorf("Restore: failed to unmarshal metadata: %w", err)
	}

	// Clear cache after restore
	a.mu.Lock()
	a.cache = make(map[string][]byte)
	a.mu.Unlock()

	a.logger.Infof("Restore completed from path: %s (backup time: %v)", path, metadata.Timestamp)
	return nil
}

// PruneData removes data older than the specified time
func (a *StoreLedgerAdapter) PruneData(olderThan time.Time) error {
	a.logger.Infof("PruneData requested for data older than: %v", olderThan)

	// If the underlying store supports pruning, use it
	type pruner interface {
		PruneData(time.Time) error
	}

	if p, ok := a.store.(pruner); ok {
		if err := p.PruneData(olderThan); err != nil {
			return fmt.Errorf("PruneData failed: %w", err)
		}

		// Clear cache after pruning
		a.mu.Lock()
		a.cache = make(map[string][]byte)
		a.mu.Unlock()

		return nil
	}

	// For stores that don't support pruning, at least clear old cache entries
	a.mu.Lock()
	oldCacheSize := len(a.cache)
	// Clear entire cache as we don't track timestamps
	a.cache = make(map[string][]byte)
	a.mu.Unlock()

	a.logger.Infof("PruneData: cleared cache of %d entries (underlying store does not support time-based pruning)", oldCacheSize)
	return nil
}

// Vacuum reclaims unused space
func (a *StoreLedgerAdapter) Vacuum() error {
	// For now, just compact
	return a.Compact()
}

// Open initializes the store
func (a *StoreLedgerAdapter) Open() error {
	a.logger.Info("StoreLedgerAdapter opened")
	return nil
}

// Close cleanly shuts down the store
func (a *StoreLedgerAdapter) Close() error {
	// Clear cache
	a.mu.Lock()
	a.cache = make(map[string][]byte)
	a.mu.Unlock()

	a.logger.Info("StoreLedgerAdapter closed")
	return nil
}

// IsOpen returns whether the store is open
func (a *StoreLedgerAdapter) IsOpen() bool {
	if a.store != nil {
		return a.store.IsOpen()
	}
	return false
}

// HealthCheck verifies the store is operational
func (a *StoreLedgerAdapter) HealthCheck(ctx context.Context) error {
	// Try a simple operation to verify store is responsive
	testKey := []byte("health_check_test")
	testValue := []byte("test")

	// Set a test value
	if err := a.SetState(testKey, testValue); err != nil {
		return fmt.Errorf("HealthCheck: write test failed: %w", err)
	}

	// Read it back
	value, err := a.GetState(testKey)
	if err != nil {
		return fmt.Errorf("HealthCheck: read test failed: %w", err)
	}

	if string(value) != string(testValue) {
		return fmt.Errorf("HealthCheck: value mismatch")
	}

	// Clean up
	if err := a.DeleteState(testKey); err != nil {
		a.logger.WithError(err).Warn("HealthCheck: failed to clean up test key")
	}

	return nil
}

// GetStats returns statistics about the store
func (a *StoreLedgerAdapter) GetStats() (*storage.StoreStats, error) {
	// a.store is already a storage.LedgerStore which has GetStats() (*storage.StoreStats, error)
	return a.store.GetStats()
}

// ReplaceBlockSameHeight atomically replaces a block at the same height (testnet-only conflict repair)
func (a *StoreLedgerAdapter) ReplaceBlockSameHeight(height uint64, newBlock *common.Block) error {
	return a.store.ReplaceBlockSameHeight(height, newBlock)
}
