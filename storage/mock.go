package storage

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"diamante/common"
	"time"
)

// MockMongoLedger is a mock implementation of the MongoLedger for testing purposes
type MockMongoLedger struct {
	accounts     map[string]*common.Account
	transactions map[string]common.Transaction
	blocks       map[int]common.Block
	contracts    map[string]*common.SmartContract
	mu           sync.RWMutex
}

// NewMockMongoLedger creates a new MockMongoLedger
func NewMockMongoLedger() *MockMongoLedger {
	return &MockMongoLedger{
		accounts:     make(map[string]*common.Account),
		transactions: make(map[string]common.Transaction),
		blocks:       make(map[int]common.Block),
		contracts:    make(map[string]*common.SmartContract),
	}
}

// MockMongoStore is a mock implementation of the MongoStore for testing purposes
type MockMongoStore struct {
	blocks   map[uint64]*common.Block
	receipts map[string]*Receipt
	mu       sync.RWMutex
}

// NewMockMongoStore creates a new MockMongoStore
func NewMockMongoStore() *MockMongoStore {
	return &MockMongoStore{
		blocks:   make(map[uint64]*common.Block),
		receipts: make(map[string]*Receipt),
	}
}

// SaveBlock stores a block in memory
func (ms *MockMongoStore) SaveBlock(block *common.Block) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if block == nil {
		return errors.New("block cannot be nil")
	}

	ms.blocks[uint64(block.Number)] = block
	return nil
}

// GetBlock retrieves a block by number
func (ms *MockMongoStore) GetBlock(blockNumber uint64) (*common.Block, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	block, exists := ms.blocks[blockNumber]
	if !exists {
		return nil, ErrNotFound
	}

	return block, nil
}

// SaveReceipt stores a receipt in memory
func (ms *MockMongoStore) SaveReceipt(receipt *Receipt) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if receipt == nil {
		return errors.New("receipt cannot be nil")
	}

	receipt.CreatedAt = time.Now()
	ms.receipts[receipt.TxID] = receipt
	return nil
}

// GetReceipt retrieves a receipt by transaction ID
func (ms *MockMongoStore) GetReceipt(txID string) (*Receipt, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	receipt, exists := ms.receipts[txID]
	if !exists {
		return nil, ErrNotFound
	}

	return receipt, nil
}

// Close is a no-op for the mock store
func (ms *MockMongoStore) Close() error {
	return nil
}

// Mock implementations for MongoLedger methods
func (ml *MockMongoLedger) CreateAccount(account *common.Account) error {
	ml.mu.Lock()
	defer ml.mu.Unlock()

	if account == nil {
		return errors.New("account cannot be nil")
	}

	if _, exists := ml.accounts[account.ID]; exists {
		return common.ErrAccountExists
	}

	ml.accounts[account.ID] = account
	return nil
}

func (ml *MockMongoLedger) UpdateAccount(account *common.Account) error {
	ml.mu.Lock()
	defer ml.mu.Unlock()

	if account == nil {
		return errors.New("account cannot be nil")
	}

	if _, exists := ml.accounts[account.ID]; !exists {
		return common.ErrAccountNotFound
	}

	ml.accounts[account.ID] = account
	return nil
}

func (ml *MockMongoLedger) GetBalance(accountID string) (float64, error) {
	ml.mu.RLock()
	defer ml.mu.RUnlock()

	account, exists := ml.accounts[accountID]
	if !exists {
		return 0, common.ErrAccountNotFound
	}

	return account.Balance, nil
}

func (ml *MockMongoLedger) UpdateAccountBalance(accountID string, amount float64) error {
	ml.mu.Lock()
	defer ml.mu.Unlock()

	account, exists := ml.accounts[accountID]
	if !exists {
		return common.ErrAccountNotFound
	}

	if account.Balance+amount < 0 {
		return common.ErrInsufficientFunds
	}

	account.Balance += amount
	return nil
}

func (ml *MockMongoLedger) AddTransaction(tx common.Transaction) error {
	ml.mu.Lock()
	defer ml.mu.Unlock()

	if err := common.ValidateTransaction(tx); err != nil {
		return err
	}

	ml.transactions[tx.ID] = tx
	return nil
}

func (ml *MockMongoLedger) IsTransactionCommitted(txID string) bool {
	ml.mu.RLock()
	defer ml.mu.RUnlock()

	_, exists := ml.transactions[txID]
	return exists
}

func (ml *MockMongoLedger) CommitBlock(block common.Block) error {
	ml.mu.Lock()
	defer ml.mu.Unlock()

	// Validate block
	if block.Number < 0 {
		return errors.New("invalid block number")
	}

	ml.blocks[block.Number] = block

	// Update transaction statuses
	for _, tx := range block.Transactions {
		if storedTx, exists := ml.transactions[tx.ID]; exists {
			storedTx.Status = "committed"
			storedTx.BlockHeight = block.Number
			ml.transactions[tx.ID] = storedTx
		}
	}

	return nil
}

func (ml *MockMongoLedger) GetBlockByNumber(num int) (common.Block, bool) {
	ml.mu.RLock()
	defer ml.mu.RUnlock()

	block, exists := ml.blocks[num]
	return block, exists
}

func (ml *MockMongoLedger) DeploySmartContract(sc *common.SmartContract) error {
	ml.mu.Lock()
	defer ml.mu.Unlock()

	if sc == nil {
		return errors.New("smart contract cannot be nil")
	}

	if sc.ID == "" {
		return errors.New("contract ID cannot be empty")
	}

	ml.contracts[sc.ID] = sc
	return nil
}

func (ml *MockMongoLedger) UpdateSmartContract(contractID, newCode, version string) error {
	ml.mu.Lock()
	defer ml.mu.Unlock()

	contract, exists := ml.contracts[contractID]
	if !exists {
		return errors.New("contract not found")
	}

	contract.Code = newCode
	contract.Version = version
	contract.UpdatedAt = time.Now()
	return nil
}

func (ml *MockMongoLedger) ExecuteSmartContract(scID, function, sender string, params *common.SmartContractParams) (*common.SmartContractResult, error) {
	ml.mu.RLock()
	defer ml.mu.RUnlock()

	contract, exists := ml.contracts[scID]
	if !exists {
		return nil, errors.New("contract not found")
	}

	// Mock execution - return success with simulated result
	result := &common.SmartContractResult{
		Success:      true,
		StringResult: "mock_execution_result",
		GasUsed:      21000,
	}

	// Add event to contract
	event := common.SmartContractEvent{
		ContractID:   scID,
		FunctionName: function,
		Params:       params,
		Result:       result,
		Timestamp:    time.Now().Unix(),
	}
	contract.Events = append(contract.Events, event)

	return result, nil
}

func (ml *MockMongoLedger) RemoveSmartContract(contractID string) error {
	ml.mu.Lock()
	defer ml.mu.Unlock()

	if _, exists := ml.contracts[contractID]; !exists {
		return errors.New("contract not found")
	}

	delete(ml.contracts, contractID)
	return nil
}

func (ml *MockMongoLedger) IntegrityCheck() error {
	ml.mu.RLock()
	defer ml.mu.RUnlock()

	// Check if there are any blocks
	if len(ml.blocks) == 0 {
		return errors.New("no blocks found in ledger")
	}

	// Mock integrity check - validate basic consistency
	for blockNum, block := range ml.blocks {
		if block.Number != blockNum {
			return errors.New("block number mismatch")
		}
	}

	return nil
}

// Additional methods needed for complete mock implementation

func (ml *MockMongoLedger) GetTransaction(txID string) (*common.Transaction, error) {
	ml.mu.RLock()
	defer ml.mu.RUnlock()

	tx, exists := ml.transactions[txID]
	if !exists {
		return nil, errors.New("transaction not found")
	}

	return &tx, nil
}

func (ml *MockMongoLedger) GetBlocksByRange(startNum, endNum int) ([]common.Block, error) {
	ml.mu.RLock()
	defer ml.mu.RUnlock()

	var blocks []common.Block
	for i := startNum; i <= endNum; i++ {
		if block, exists := ml.blocks[i]; exists {
			blocks = append(blocks, block)
		}
	}

	return blocks, nil
}

func (ml *MockMongoLedger) CreateSnapshot(height int) error {
	ml.mu.RLock()
	defer ml.mu.RUnlock()

	// Mock snapshot creation - just validate the height exists
	if _, exists := ml.blocks[height]; !exists {
		return errors.New("block not found for snapshot")
	}

	return nil
}

func (ml *MockMongoLedger) RestoreSnapshot(height int) error {
	ml.mu.Lock()
	defer ml.mu.Unlock()

	// Mock snapshot restore - just validate the height
	if _, exists := ml.blocks[height]; !exists {
		return errors.New("snapshot not found for height")
	}

	// In a real implementation, this would restore state to the snapshot
	return nil
}

// Additional helper methods for testing

func (ml *MockMongoLedger) GetAllAccounts() map[string]*common.Account {
	ml.mu.RLock()
	defer ml.mu.RUnlock()

	// Return a copy to prevent external modification
	accountsCopy := make(map[string]*common.Account)
	for id, acc := range ml.accounts {
		accountsCopy[id] = acc
	}
	return accountsCopy
}

func (ml *MockMongoLedger) GetAllTransactions() map[string]common.Transaction {
	ml.mu.RLock()
	defer ml.mu.RUnlock()

	// Return a copy to prevent external modification
	txCopy := make(map[string]common.Transaction)
	for id, tx := range ml.transactions {
		txCopy[id] = tx
	}
	return txCopy
}

func (ml *MockMongoLedger) Clear() {
	ml.mu.Lock()
	defer ml.mu.Unlock()

	ml.accounts = make(map[string]*common.Account)
	ml.transactions = make(map[string]common.Transaction)
	ml.blocks = make(map[int]common.Block)
	ml.contracts = make(map[string]*common.SmartContract)
}

// UpdateContract updates an existing contract
func (mml *MockMongoLedger) UpdateContract(contract *common.SmartContract) error {
	mml.mu.Lock()
	defer mml.mu.Unlock()

	if contract == nil {
		return errors.New("contract cannot be nil")
	}

	if _, exists := mml.contracts[contract.ID]; !exists {
		return errors.New("contract not found")
	}

	// Update timestamp
	contract.UpdatedAt = time.Now()
	mml.contracts[contract.ID] = contract
	return nil
}

// GetStats returns mock statistics
func (mml *MockMongoLedger) GetStats() (*common.LedgerStats, error) {
	mml.mu.RLock()
	defer mml.mu.RUnlock()

	stats := &common.LedgerStats{
		TotalAccounts:     int64(len(mml.accounts)),
		TotalTransactions: int64(len(mml.transactions)),
		TotalContracts:    int64(len(mml.contracts)),
		TotalBalance:      0.0,
		LastBlockHeight:   int64(mml.GetCurrentHeight()),
		NetworkHealth:     "healthy",
		ProcessingTime:    0,
	}

	// Calculate total balance
	for _, account := range mml.accounts {
		stats.TotalBalance += account.Balance
	}

	return stats, nil
}

// GetCurrentHeight returns the current block height
func (mml *MockMongoLedger) GetCurrentHeight() int {
	mml.mu.RLock()
	defer mml.mu.RUnlock()

	maxHeight := 0
	for height := range mml.blocks {
		if height > maxHeight {
			maxHeight = height
		}
	}
	return maxHeight
}

// GetBlockHeight returns the current block height
func (mml *MockMongoLedger) GetBlockHeight() (int, error) {
	return mml.GetCurrentHeight(), nil
}

// GetAccountTransactions retrieves transactions for a specific account
func (ml *MockMongoLedger) GetAccountTransactions(accountID string, limit, offset int) ([]common.Transaction, error) {
	ml.mu.RLock()
	defer ml.mu.RUnlock()

	var transactions []common.Transaction
	for _, tx := range ml.transactions {
		if tx.Sender == accountID || tx.Receiver == accountID {
			transactions = append(transactions, tx)
		}
	}

	// Apply offset and limit
	start := offset
	if start > len(transactions) {
		return []common.Transaction{}, nil
	}

	end := start + limit
	if end > len(transactions) {
		end = len(transactions)
	}

	return transactions[start:end], nil
}

// HealthCheck performs a health check on the mock ledger
func (ml *MockMongoLedger) HealthCheck(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		// Mock health check - just verify internal state
		ml.mu.RLock()
		defer ml.mu.RUnlock()

		if ml.accounts == nil || ml.transactions == nil || ml.blocks == nil || ml.contracts == nil {
			return errors.New("mock ledger is not initialized properly")
		}
		return nil
	}
}

// GetLastBlockHash returns the hash of the last block
func (ml *MockMongoLedger) GetLastBlockHash() (string, error) {
	ml.mu.RLock()
	defer ml.mu.RUnlock()

	height := ml.GetCurrentHeight()
	if height == 0 {
		return "", errors.New("no blocks in ledger")
	}

	if block, exists := ml.blocks[height]; exists {
		return block.Hash, nil
	}

	return "", errors.New("last block not found")
}

// Close closes the mock ledger
func (ml *MockMongoLedger) Close() error {
	// No-op for mock
	return nil
}

// CreateMockTransaction creates a mock transaction for testing
func (mml *MockMongoLedger) CreateMockTransaction(sender, receiver string, amount float64) common.Transaction {
	return common.Transaction{
		ID:        fmt.Sprintf("tx_%d", time.Now().UnixNano()),
		Sender:    sender,
		Receiver:  receiver,
		Amount:    amount,
		Timestamp: time.Now().Unix(),
	}
}
