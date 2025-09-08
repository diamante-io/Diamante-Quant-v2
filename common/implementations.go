// common/implementations.go
package common

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

// DefaultLedger provides a concrete implementation of LedgerAPI
type DefaultLedger struct {
	accounts     map[string]*Account
	transactions map[string]*Transaction
	blocks       []Block
	contracts    map[string]*SmartContract
	stats        map[string]interface{}
	mu           sync.RWMutex
	closed       bool
}

// NewDefaultLedger creates a new instance of DefaultLedger
func NewDefaultLedger() *DefaultLedger {
	return &DefaultLedger{
		accounts:     make(map[string]*Account),
		transactions: make(map[string]*Transaction),
		blocks:       make([]Block, 0),
		contracts:    make(map[string]*SmartContract),
		stats:        make(map[string]interface{}),
		closed:       false,
	}
}

// CreateAccount creates a new account in the ledger
func (dl *DefaultLedger) CreateAccount(ac *Account) error {
	dl.mu.Lock()
	defer dl.mu.Unlock()

	if dl.closed {
		return errors.New("ledger is closed")
	}

	if ac == nil {
		return errors.New("account cannot be nil")
	}

	if err := ac.Validate(); err != nil {
		return fmt.Errorf("account validation failed: %w", err)
	}

	if _, exists := dl.accounts[ac.ID]; exists {
		return ErrAccountExists
	}

	dl.accounts[ac.ID] = ac.Clone()
	logrus.WithField("accountID", ac.ID).Info("Account created")
	return nil
}

// UpdateAccount updates an existing account in the ledger
func (dl *DefaultLedger) UpdateAccount(ac *Account) error {
	dl.mu.Lock()
	defer dl.mu.Unlock()

	if dl.closed {
		return errors.New("ledger is closed")
	}

	if ac == nil {
		return errors.New("account cannot be nil")
	}

	if err := ac.Validate(); err != nil {
		return fmt.Errorf("account validation failed: %w", err)
	}

	if _, exists := dl.accounts[ac.ID]; !exists {
		return ErrAccountNotFound
	}

	dl.accounts[ac.ID] = ac.Clone()
	logrus.WithField("accountID", ac.ID).Info("Account updated")
	return nil
}

// GetBalance retrieves the balance for a specific account
func (dl *DefaultLedger) GetBalance(accountID string) (float64, error) {
	dl.mu.RLock()
	defer dl.mu.RUnlock()

	if dl.closed {
		return 0, errors.New("ledger is closed")
	}

	if accountID == "" {
		return 0, errors.New("account ID cannot be empty")
	}

	account, exists := dl.accounts[accountID]
	if !exists {
		return 0, ErrAccountNotFound
	}

	return account.GetBalance(), nil
}

// UpdateAccountBalance updates the balance of an account
func (dl *DefaultLedger) UpdateAccountBalance(accountID string, amount float64) error {
	dl.mu.Lock()
	defer dl.mu.Unlock()

	if dl.closed {
		return errors.New("ledger is closed")
	}

	if accountID == "" {
		return errors.New("account ID cannot be empty")
	}

	account, exists := dl.accounts[accountID]
	if !exists {
		return ErrAccountNotFound
	}

	if err := account.UpdateBalance(amount); err != nil {
		return fmt.Errorf("failed to update account balance: %w", err)
	}

	logrus.WithFields(logrus.Fields{
		"accountID": accountID,
		"amount":    amount,
	}).Info("Account balance updated")
	return nil
}

// AddTransaction adds a transaction to the ledger
func (dl *DefaultLedger) AddTransaction(tx Transaction) error {
	dl.mu.Lock()
	defer dl.mu.Unlock()

	if dl.closed {
		return errors.New("ledger is closed")
	}

	if err := tx.Validate(); err != nil {
		return fmt.Errorf("transaction validation failed: %w", err)
	}

	if _, exists := dl.transactions[tx.ID]; exists {
		return errors.New("transaction already exists")
	}

	dl.transactions[tx.ID] = &tx
	logrus.WithField("txID", tx.ID).Info("Transaction added")
	return nil
}

// IsTransactionCommitted checks if a transaction is committed
func (dl *DefaultLedger) IsTransactionCommitted(txID string) bool {
	dl.mu.RLock()
	defer dl.mu.RUnlock()

	if dl.closed {
		return false
	}

	tx, exists := dl.transactions[txID]
	if !exists {
		return false
	}

	return tx.Status == "committed"
}

// GetTransaction retrieves a transaction by ID
func (dl *DefaultLedger) GetTransaction(txID string) (*Transaction, error) {
	dl.mu.RLock()
	defer dl.mu.RUnlock()

	if dl.closed {
		return nil, errors.New("ledger is closed")
	}

	if txID == "" {
		return nil, errors.New("transaction ID cannot be empty")
	}

	tx, exists := dl.transactions[txID]
	if !exists {
		return nil, errors.New("transaction not found")
	}

	// Return a copy to prevent external modification
	txCopy := *tx
	return &txCopy, nil
}

// GetAccountTransactions retrieves transactions for a specific account
func (dl *DefaultLedger) GetAccountTransactions(accountID string, limit, offset int) ([]Transaction, error) {
	dl.mu.RLock()
	defer dl.mu.RUnlock()

	if dl.closed {
		return nil, errors.New("ledger is closed")
	}

	if accountID == "" {
		return nil, errors.New("account ID cannot be empty")
	}

	if limit <= 0 {
		limit = 100 // Default limit
	}

	var accountTxs []Transaction
	for _, tx := range dl.transactions {
		if tx.Sender == accountID || tx.Receiver == accountID {
			accountTxs = append(accountTxs, *tx)
		}
	}

	// Apply offset and limit
	start := offset
	if start >= len(accountTxs) {
		return []Transaction{}, nil
	}

	end := start + limit
	if end > len(accountTxs) {
		end = len(accountTxs)
	}

	return accountTxs[start:end], nil
}

// CommitBlock commits a block to the ledger
func (dl *DefaultLedger) CommitBlock(block Block) error {
	dl.mu.Lock()
	defer dl.mu.Unlock()

	if dl.closed {
		return errors.New("ledger is closed")
	}

	// Get previous block for validation
	var previousBlock *Block
	if len(dl.blocks) > 0 {
		previousBlock = &dl.blocks[len(dl.blocks)-1]
	}

	if err := block.Validate(previousBlock); err != nil {
		return fmt.Errorf("block validation failed: %w", err)
	}

	// Mark transactions as committed
	for _, tx := range block.Transactions {
		if storedTx, exists := dl.transactions[tx.ID]; exists {
			storedTx.Status = "committed"
			storedTx.BlockHeight = block.Number
		}
	}

	dl.blocks = append(dl.blocks, block)
	logrus.WithField("blockNumber", block.Number).Info("Block committed")
	return nil
}

// GetBlockByNumber retrieves a block by its number
func (dl *DefaultLedger) GetBlockByNumber(num int) (Block, bool) {
	dl.mu.RLock()
	defer dl.mu.RUnlock()

	if dl.closed {
		return Block{}, false
	}

	for _, block := range dl.blocks {
		if block.Number == num {
			return block, true
		}
	}

	return Block{}, false
}

// GetLastBlockHash returns the hash of the last block
func (dl *DefaultLedger) GetLastBlockHash() (string, error) {
	dl.mu.RLock()
	defer dl.mu.RUnlock()

	if dl.closed {
		return "", errors.New("ledger is closed")
	}

	if len(dl.blocks) == 0 {
		return "", nil
	}

	return dl.blocks[len(dl.blocks)-1].Hash, nil
}

// GetBlockHeight returns the current block height
func (dl *DefaultLedger) GetBlockHeight() (int, error) {
	dl.mu.RLock()
	defer dl.mu.RUnlock()

	if dl.closed {
		return 0, errors.New("ledger is closed")
	}

	return len(dl.blocks), nil
}

// GetBlocksByRange retrieves blocks within a specified range
func (dl *DefaultLedger) GetBlocksByRange(startNum, endNum int) ([]Block, error) {
	dl.mu.RLock()
	defer dl.mu.RUnlock()

	if dl.closed {
		return nil, errors.New("ledger is closed")
	}

	if startNum < 0 || endNum < startNum {
		return nil, errors.New("invalid block range")
	}

	var result []Block
	for _, block := range dl.blocks {
		if block.Number >= startNum && block.Number <= endNum {
			result = append(result, block)
		}
	}

	return result, nil
}

// CreateSnapshot creates a snapshot at the specified height
func (dl *DefaultLedger) CreateSnapshot(height int) error {
	dl.mu.RLock()
	defer dl.mu.RUnlock()

	if dl.closed {
		return errors.New("ledger is closed")
	}

	if height < 0 || height > len(dl.blocks) {
		return errors.New("invalid snapshot height")
	}

	// In a real implementation, this would persist the snapshot
	logrus.WithField("height", height).Info("Snapshot created")
	return nil
}

// RestoreSnapshot restores from a snapshot at the specified height
func (dl *DefaultLedger) RestoreSnapshot(height int) error {
	dl.mu.Lock()
	defer dl.mu.Unlock()

	if dl.closed {
		return errors.New("ledger is closed")
	}

	if height < 0 {
		return errors.New("invalid snapshot height")
	}

	// In a real implementation, this would restore from persisted snapshot
	logrus.WithField("height", height).Info("Snapshot restored")
	return nil
}

// DeploySmartContract deploys a smart contract
func (dl *DefaultLedger) DeploySmartContract(sc *SmartContract) error {
	dl.mu.Lock()
	defer dl.mu.Unlock()

	if dl.closed {
		return errors.New("ledger is closed")
	}

	if sc == nil {
		return errors.New("smart contract cannot be nil")
	}

	if sc.ID == "" {
		return errors.New("smart contract ID cannot be empty")
	}

	if _, exists := dl.contracts[sc.ID]; exists {
		return errors.New("smart contract already exists")
	}

	dl.contracts[sc.ID] = sc
	logrus.WithField("contractID", sc.ID).Info("Smart contract deployed")
	return nil
}

// UpdateSmartContract updates a smart contract
func (dl *DefaultLedger) UpdateSmartContract(contractID, newCode, version string) error {
	dl.mu.Lock()
	defer dl.mu.Unlock()

	if dl.closed {
		return errors.New("ledger is closed")
	}

	if contractID == "" {
		return errors.New("contract ID cannot be empty")
	}

	contract, exists := dl.contracts[contractID]
	if !exists {
		return errors.New("smart contract not found")
	}

	contract.Code = newCode
	contract.Version = version
	contract.UpdatedAt = ConsensusNow()
	contract.CodeHash = HashData([]byte(newCode))

	logrus.WithField("contractID", contractID).Info("Smart contract updated")
	return nil
}

// ExecuteSmartContract executes a smart contract function
func (dl *DefaultLedger) ExecuteSmartContract(scID, function, sender string, params *SmartContractParams) (*SmartContractResult, error) {
	dl.mu.RLock()
	defer dl.mu.RUnlock()

	if dl.closed {
		return nil, errors.New("ledger is closed")
	}

	if scID == "" {
		return nil, errors.New("smart contract ID cannot be empty")
	}

	contract, exists := dl.contracts[scID]
	if !exists {
		return nil, errors.New("smart contract not found")
	}

	// Create execution result
	result := &SmartContractResult{
		Success:      true,
		StringResult: "executed",
		GasUsed:      1000, // Mock gas usage
	}

	// In a real implementation, this would execute the actual contract code
	event := SmartContractEvent{
		ContractID:   scID,
		FunctionName: function,
		Params:       params,
		Result:       result,
		Timestamp:    ConsensusUnix(),
	}

	contract.Events = append(contract.Events, event)
	logrus.WithFields(logrus.Fields{
		"contractID": scID,
		"function":   function,
	}).Info("Smart contract executed")
	return result, nil
}

// RemoveSmartContract removes a smart contract
func (dl *DefaultLedger) RemoveSmartContract(contractID string) error {
	dl.mu.Lock()
	defer dl.mu.Unlock()

	if dl.closed {
		return errors.New("ledger is closed")
	}

	if contractID == "" {
		return errors.New("contract ID cannot be empty")
	}

	if _, exists := dl.contracts[contractID]; !exists {
		return errors.New("smart contract not found")
	}

	delete(dl.contracts, contractID)
	logrus.WithField("contractID", contractID).Info("Smart contract removed")
	return nil
}

// IntegrityCheck performs integrity checks on the ledger
func (dl *DefaultLedger) IntegrityCheck() error {
	dl.mu.RLock()
	defer dl.mu.RUnlock()

	if dl.closed {
		return errors.New("ledger is closed")
	}

	// Validate all accounts
	for _, account := range dl.accounts {
		if err := account.Validate(); err != nil {
			return fmt.Errorf("account integrity check failed for %s: %w", account.ID, err)
		}
	}

	// Validate all transactions
	for _, tx := range dl.transactions {
		if err := tx.Validate(); err != nil {
			return fmt.Errorf("transaction integrity check failed for %s: %w", tx.ID, err)
		}
	}

	// Validate block chain
	for i, block := range dl.blocks {
		var prevBlock *Block
		if i > 0 {
			prevBlock = &dl.blocks[i-1]
		}
		if err := block.Validate(prevBlock); err != nil {
			return fmt.Errorf("block integrity check failed for block %d: %w", block.Number, err)
		}
	}

	logrus.Info("Ledger integrity check passed")
	return nil
}

// Close closes the ledger and releases resources
func (dl *DefaultLedger) Close() error {
	dl.mu.Lock()
	defer dl.mu.Unlock()

	if dl.closed {
		return errors.New("ledger already closed")
	}

	dl.closed = true
	logrus.Info("Ledger closed")
	return nil
}

// GetStats returns ledger statistics
func (dl *DefaultLedger) GetStats() (*LedgerStats, error) {
	dl.mu.RLock()
	defer dl.mu.RUnlock()

	if dl.closed {
		return nil, errors.New("ledger is closed")
	}

	stats := &LedgerStats{
		TotalAccounts:     int64(len(dl.accounts)),
		TotalTransactions: int64(len(dl.transactions)),
		TotalContracts:    int64(len(dl.contracts)),
		TotalBalance:      0.0,
		LastBlockHeight:   int64(len(dl.blocks)) - 1,
		NetworkHealth:     "healthy",
		ProcessingTime:    0,
	}

	// Calculate total balance
	for _, account := range dl.accounts {
		stats.TotalBalance += account.Balance
	}

	return stats, nil
}

// HealthCheck performs a health check on the ledger
func (dl *DefaultLedger) HealthCheck(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	dl.mu.RLock()
	defer dl.mu.RUnlock()

	if dl.closed {
		return errors.New("ledger is closed")
	}

	// Perform basic health checks
	if dl.accounts == nil || dl.transactions == nil || dl.blocks == nil || dl.contracts == nil {
		return errors.New("ledger data structures are corrupted")
	}

	return nil
}

// DefaultSmartContractManager provides a concrete implementation of SmartContractManagerAPI
type DefaultSmartContractManager struct {
	contracts map[string]*SmartContract
	mu        sync.RWMutex
}

// NewDefaultSmartContractManager creates a new instance of DefaultSmartContractManager
func NewDefaultSmartContractManager() *DefaultSmartContractManager {
	return &DefaultSmartContractManager{
		contracts: make(map[string]*SmartContract),
	}
}

// DeploySmartContract deploys a new smart contract
func (scm *DefaultSmartContractManager) DeploySmartContract(contractCode, owner string) (string, error) {
	scm.mu.Lock()
	defer scm.mu.Unlock()

	if contractCode == "" {
		return "", errors.New("contract code cannot be empty")
	}

	if owner == "" {
		return "", errors.New("contract owner cannot be empty")
	}

	contractID := GenerateUniqueID()
	contract := &SmartContract{
		ID:       contractID,
		Code:     contractCode,
		CodeHash: HashData([]byte(contractCode)),
		Owner:    owner,
		Version:  "1.0",
		State: &SmartContractState{
			Variables:     make(map[string]string),
			Balances:      make(map[string]float64),
			Permissions:   make(map[string]bool),
			Configuration: make(map[string]string),
			Counters:      make(map[string]int64),
			LastUpdated:   ConsensusUnix(),
		},
		Language:  "wasm", // Default language
		Events:    make([]SmartContractEvent, 0),
		Functions: make(map[string]SmartContractFunction),
		Metadata: &SmartContractMetadata{
			Author:  owner,
			Version: "1.0",
		},
		CreatedAt: ConsensusNow(),
		UpdatedAt: ConsensusNow(),
	}

	scm.contracts[contractID] = contract
	logrus.WithFields(logrus.Fields{"contractID": contractID, "owner": owner}).Info("Smart contract deployed")
	return contractID, nil
}

// ExecuteContractFunction executes a function on a smart contract
func (scm *DefaultSmartContractManager) ExecuteContractFunction(contractID, functionName string, params *SmartContractParams, sender string) (*SmartContractResult, error) {
	scm.mu.RLock()
	defer scm.mu.RUnlock()

	if contractID == "" {
		return nil, errors.New("contract ID cannot be empty")
	}

	if functionName == "" {
		return nil, errors.New("function name cannot be empty")
	}

	contract, exists := scm.contracts[contractID]
	if !exists {
		return nil, errors.New("smart contract not found")
	}

	// Create execution result
	result := &SmartContractResult{
		Success:      true,
		StringResult: "executed",
		GasUsed:      1000, // Mock gas usage
	}

	// Create execution event
	event := SmartContractEvent{
		ContractID:   contractID,
		FunctionName: functionName,
		Params:       params,
		Result:       result,
		Timestamp:    ConsensusUnix(),
	}

	contract.Events = append(contract.Events, event)
	logrus.WithFields(logrus.Fields{"contractID": contractID, "function": functionName, "sender": sender}).Info("Contract function executed")
	return result, nil
}

// TerminateSmartContract terminates a smart contract
func (scm *DefaultSmartContractManager) TerminateSmartContract(contractID, requester string) error {
	scm.mu.Lock()
	defer scm.mu.Unlock()

	if contractID == "" {
		return errors.New("contract ID cannot be empty")
	}

	if requester == "" {
		return errors.New("requester cannot be empty")
	}

	contract, exists := scm.contracts[contractID]
	if !exists {
		return errors.New("smart contract not found")
	}

	if contract.Owner != requester {
		return errors.New("only contract owner can terminate the contract")
	}

	delete(scm.contracts, contractID)
	logrus.WithFields(logrus.Fields{"contractID": contractID, "requester": requester}).Info("Smart contract terminated")
	return nil
}

// GetContractState retrieves the current state of a smart contract
func (scm *DefaultSmartContractManager) GetContractState(contractID string) (*SmartContractState, error) {
	scm.mu.RLock()
	defer scm.mu.RUnlock()

	if contractID == "" {
		return nil, errors.New("contract ID cannot be empty")
	}

	contract, exists := scm.contracts[contractID]
	if !exists {
		return nil, errors.New("smart contract not found")
	}

	// Return a copy of the state
	stateCopy := &SmartContractState{
		Variables:     make(map[string]string),
		Balances:      make(map[string]float64),
		Permissions:   make(map[string]bool),
		Configuration: make(map[string]string),
		Counters:      make(map[string]int64),
		LastUpdated:   contract.State.LastUpdated,
	}

	// Copy Variables
	for k, v := range contract.State.Variables {
		stateCopy.Variables[k] = v
	}

	// Copy Balances
	for k, v := range contract.State.Balances {
		stateCopy.Balances[k] = v
	}

	// Copy Permissions
	for k, v := range contract.State.Permissions {
		stateCopy.Permissions[k] = v
	}

	// Copy Configuration
	for k, v := range contract.State.Configuration {
		stateCopy.Configuration[k] = v
	}

	// Copy Counters
	for k, v := range contract.State.Counters {
		stateCopy.Counters[k] = v
	}

	return stateCopy, nil
}

// GetContractEvents retrieves events for a smart contract within a time range
func (scm *DefaultSmartContractManager) GetContractEvents(contractID string, fromTime, toTime time.Time) ([]SmartContractEvent, error) {
	scm.mu.RLock()
	defer scm.mu.RUnlock()

	if contractID == "" {
		return nil, errors.New("contract ID cannot be empty")
	}

	contract, exists := scm.contracts[contractID]
	if !exists {
		return nil, errors.New("smart contract not found")
	}

	var filteredEvents []SmartContractEvent
	for _, event := range contract.Events {
		eventTime := time.Unix(event.Timestamp, 0)
		if eventTime.After(fromTime) && eventTime.Before(toTime) {
			filteredEvents = append(filteredEvents, event)
		}
	}

	return filteredEvents, nil
}

// UpdateContractCode updates the code of an existing smart contract
func (scm *DefaultSmartContractManager) UpdateContractCode(contractID, newCode string, requester string) error {
	scm.mu.Lock()
	defer scm.mu.Unlock()

	if contractID == "" {
		return errors.New("contract ID cannot be empty")
	}

	if newCode == "" {
		return errors.New("new code cannot be empty")
	}

	if requester == "" {
		return errors.New("requester cannot be empty")
	}

	contract, exists := scm.contracts[contractID]
	if !exists {
		return errors.New("smart contract not found")
	}

	if contract.Owner != requester {
		return errors.New("only contract owner can update the contract")
	}

	contract.Code = newCode
	contract.CodeHash = HashData([]byte(newCode))
	contract.UpdatedAt = ConsensusNow()

	logrus.WithFields(logrus.Fields{"contractID": contractID, "requester": requester}).Info("Contract code updated")
	return nil
}

// ValidateContract validates smart contract code
func (scm *DefaultSmartContractManager) ValidateContract(contractCode string) error {
	if contractCode == "" {
		return errors.New("contract code cannot be empty")
	}

	// Basic validation - in a real implementation, this would include
	// syntax checking, security analysis, etc.
	if len(contractCode) < 10 {
		return errors.New("contract code too short")
	}

	logrus.Info("Contract code validated successfully")
	return nil
}

// DefaultBlockchainAPI provides a concrete implementation of BlockchainAPI
type DefaultBlockchainAPI struct {
	ledger          LedgerAPI
	contractManager SmartContractManagerAPI
	pendingTxs      map[string]*Transaction
	networkStats    map[string]interface{}
	mu              sync.RWMutex
}

// NewDefaultBlockchainAPI creates a new instance of DefaultBlockchainAPI
func NewDefaultBlockchainAPI(ledger LedgerAPI, contractManager SmartContractManagerAPI) *DefaultBlockchainAPI {
	return &DefaultBlockchainAPI{
		ledger:          ledger,
		contractManager: contractManager,
		pendingTxs:      make(map[string]*Transaction),
		networkStats:    make(map[string]interface{}),
	}
}

// GetAccountBalance retrieves the balance of an account
func (api *DefaultBlockchainAPI) GetAccountBalance(accountID string) (float64, error) {
	if accountID == "" {
		return 0, errors.New("account ID cannot be empty")
	}

	return api.ledger.GetBalance(accountID)
}

// CreateNewAccount creates a new account with the given public key
func (api *DefaultBlockchainAPI) CreateNewAccount(publicKey []byte) (*Account, error) {
	if len(publicKey) == 0 {
		return nil, errors.New("public key cannot be empty")
	}

	accountID := GenerateUniqueID()
	account, err := NewAccount(accountID, publicKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create account: %w", err)
	}

	if err := api.ledger.CreateAccount(account); err != nil {
		return nil, fmt.Errorf("failed to save account to ledger: %w", err)
	}

	logrus.WithField("accountID", accountID).Info("New account created")
	return account, nil
}

// TransferFunds transfers funds between accounts
func (api *DefaultBlockchainAPI) TransferFunds(senderID, receiverID string, amount float64, memo string) (string, error) {
	if senderID == "" || receiverID == "" {
		return "", errors.New("sender and receiver IDs cannot be empty")
	}

	if amount <= 0 {
		return "", errors.New("transfer amount must be positive")
	}

	// Check sender balance
	senderBalance, err := api.ledger.GetBalance(senderID)
	if err != nil {
		return "", fmt.Errorf("failed to get sender balance: %w", err)
	}

	if senderBalance < amount {
		return "", ErrInsufficientFunds
	}

	// Create transaction
	tx := Transaction{
		ID:        GenerateUniqueID(),
		Sender:    senderID,
		Receiver:  receiverID,
		Amount:    amount,
		Timestamp: ConsensusUnix(),
		Status:    "pending",
		Metadata:  &TransactionMetadata{Description: memo},
	}

	// Add to pending transactions
	api.mu.Lock()
	api.pendingTxs[tx.ID] = &tx
	api.mu.Unlock()

	if err := api.ledger.AddTransaction(tx); err != nil {
		return "", fmt.Errorf("failed to add transaction to ledger: %w", err)
	}

	logrus.WithFields(logrus.Fields{"sender": senderID, "receiver": receiverID, "amount": amount}).Info("Transfer initiated")
	return tx.ID, nil
}

// GetLatestBlockHeight retrieves the latest block height
func (api *DefaultBlockchainAPI) GetLatestBlockHeight() (int, error) {
	return api.ledger.GetBlockHeight()
}

// GetBlockByHeight retrieves a block by its height
func (api *DefaultBlockchainAPI) GetBlockByHeight(height int) (*Block, error) {
	if height < 0 {
		return nil, errors.New("block height cannot be negative")
	}

	block, found := api.ledger.GetBlockByNumber(height)
	if !found {
		return nil, errors.New("block not found")
	}

	return &block, nil
}

// GetTransactionByID retrieves a transaction by its ID
func (api *DefaultBlockchainAPI) GetTransactionByID(txID string) (*Transaction, error) {
	if txID == "" {
		return nil, errors.New("transaction ID cannot be empty")
	}

	return api.ledger.GetTransaction(txID)
}

// DeployContract deploys a smart contract
func (api *DefaultBlockchainAPI) DeployContract(ownerID, contractCode, language string) (string, error) {
	if ownerID == "" {
		return "", errors.New("owner ID cannot be empty")
	}

	if contractCode == "" {
		return "", errors.New("contract code cannot be empty")
	}

	if err := api.contractManager.ValidateContract(contractCode); err != nil {
		return "", fmt.Errorf("contract validation failed: %w", err)
	}

	contractID, err := api.contractManager.DeploySmartContract(contractCode, ownerID)
	if err != nil {
		return "", fmt.Errorf("contract deployment failed: %w", err)
	}

	logrus.WithFields(logrus.Fields{"contractID": contractID, "owner": ownerID}).Info("Contract deployed")
	return contractID, nil
}

// InvokeContract invokes a smart contract function
func (api *DefaultBlockchainAPI) InvokeContract(contractID, function string, params map[string]interface{}, callerID string) (*SmartContractResult, error) {
	if contractID == "" {
		return nil, errors.New("contract ID cannot be empty")
	}

	if function == "" {
		return nil, errors.New("function name cannot be empty")
	}

	if callerID == "" {
		return nil, errors.New("caller ID cannot be empty")
	}

	// Convert map[string]interface{} to SmartContractParams
	scParams := &SmartContractParams{
		FunctionName: function,
		Caller:       callerID,
		StringParams: make(map[string]string),
		NumberParams: make(map[string]float64),
		BoolParams:   make(map[string]bool),
		ByteParams:   make(map[string][]byte),
	}

	// Type-switch to convert params to appropriate types
	for key, value := range params {
		switch v := value.(type) {
		case string:
			scParams.StringParams[key] = v
		case int:
			scParams.NumberParams[key] = float64(v)
		case int64:
			scParams.NumberParams[key] = float64(v)
		case float64:
			scParams.NumberParams[key] = v
		case bool:
			scParams.BoolParams[key] = v
		case []byte:
			scParams.ByteParams[key] = v
		default:
			// For unknown types, convert to string
			scParams.StringParams[key] = fmt.Sprintf("%v", v)
		}
	}

	result, err := api.contractManager.ExecuteContractFunction(contractID, function, scParams, callerID)
	if err != nil {
		return nil, fmt.Errorf("contract execution failed: %w", err)
	}

	logrus.WithFields(logrus.Fields{"contractID": contractID, "function": function, "caller": callerID}).Info("Contract invoked")
	return result, nil
}

// GetNetworkStats retrieves network statistics
func (api *DefaultBlockchainAPI) GetNetworkStats() (map[string]interface{}, error) {
	api.mu.RLock()
	defer api.mu.RUnlock()

	stats := make(map[string]interface{})
	for k, v := range api.networkStats {
		stats[k] = v
	}

	// Add dynamic stats
	height, _ := api.ledger.GetBlockHeight()
	ledgerStats, _ := api.ledger.GetStats()

	stats["block_height"] = height
	stats["pending_transactions"] = len(api.pendingTxs)

	// Add ledger stats to the map
	if ledgerStats != nil {
		stats["total_accounts"] = ledgerStats.TotalAccounts
		stats["total_transactions"] = ledgerStats.TotalTransactions
		stats["total_contracts"] = ledgerStats.TotalContracts
		stats["total_balance"] = ledgerStats.TotalBalance
		stats["last_block_height"] = ledgerStats.LastBlockHeight
		stats["network_health"] = ledgerStats.NetworkHealth
		stats["processing_time_ms"] = ledgerStats.ProcessingTime
	}

	return stats, nil
}

// SubmitTransaction submits a transaction to the network
func (api *DefaultBlockchainAPI) SubmitTransaction(tx *Transaction) error {
	if tx == nil {
		return errors.New("transaction cannot be nil")
	}

	if err := tx.Validate(); err != nil {
		return fmt.Errorf("transaction validation failed: %w", err)
	}

	api.mu.Lock()
	api.pendingTxs[tx.ID] = tx
	api.mu.Unlock()

	if err := api.ledger.AddTransaction(*tx); err != nil {
		return fmt.Errorf("failed to add transaction to ledger: %w", err)
	}

	logrus.WithField("txID", tx.ID).Info("Transaction submitted")
	return nil
}

// ValidateTransaction validates a transaction
func (api *DefaultBlockchainAPI) ValidateTransaction(tx *Transaction) error {
	if tx == nil {
		return errors.New("transaction cannot be nil")
	}

	return tx.Validate()
}

// DefaultConsensusAPI provides a concrete implementation of ConsensusAPI
type DefaultConsensusAPI struct {
	validators    map[[32]byte]*ValidatorInfo
	currentHeight uint64
	networkLoad   float64
	running       bool
	paused        bool
	mu            sync.RWMutex
}

// ValidatorInfo stores information about a validator
type ValidatorInfo struct {
	ID     [32]byte
	Stake  uint64
	Active bool
}

// NewDefaultConsensusAPI creates a new instance of DefaultConsensusAPI
func NewDefaultConsensusAPI() *DefaultConsensusAPI {
	return &DefaultConsensusAPI{
		validators:  make(map[[32]byte]*ValidatorInfo),
		networkLoad: 0.0,
		running:     false,
		paused:      false,
	}
}

// GetCurrentHeight returns the current consensus height
func (api *DefaultConsensusAPI) GetCurrentHeight() uint64 {
	api.mu.RLock()
	defer api.mu.RUnlock()
	return api.currentHeight
}

// GetNetworkLoad returns the current network load
func (api *DefaultConsensusAPI) GetNetworkLoad() float64 {
	api.mu.RLock()
	defer api.mu.RUnlock()
	return api.networkLoad
}

// AddValidator adds a validator to the consensus network
func (api *DefaultConsensusAPI) AddValidator(id [32]byte, stake uint64) {
	api.mu.Lock()
	defer api.mu.Unlock()

	validator := &ValidatorInfo{
		ID:     id,
		Stake:  stake,
		Active: true,
	}

	api.validators[id] = validator
	logrus.WithFields(logrus.Fields{"validatorID": fmt.Sprintf("%x", id), "stake": stake}).Info("Validator added")
}

// RemoveValidator removes a validator from the consensus network
func (api *DefaultConsensusAPI) RemoveValidator(id [32]byte) error {
	api.mu.Lock()
	defer api.mu.Unlock()

	if _, exists := api.validators[id]; !exists {
		return errors.New("validator not found")
	}

	delete(api.validators, id)
	logrus.WithField("validatorID", fmt.Sprintf("%x", id)).Info("Validator removed")
	return nil
}

// IsActiveValidator checks if a validator is active
func (api *DefaultConsensusAPI) IsActiveValidator(id [32]byte) bool {
	api.mu.RLock()
	defer api.mu.RUnlock()

	validator, exists := api.validators[id]
	if !exists {
		return false
	}

	return validator.Active
}

// ProposeBlock proposes a new block
func (api *DefaultConsensusAPI) ProposeBlock(block *Block) error {
	api.mu.Lock()
	defer api.mu.Unlock()

	if !api.running {
		return errors.New("consensus is not running")
	}

	if api.paused {
		return errors.New("consensus is paused")
	}

	if block == nil {
		return errors.New("block cannot be nil")
	}

	// Basic block validation
	if block.Number <= int(api.currentHeight) {
		return errors.New("block number must be greater than current height")
	}

	logrus.WithField("blockNumber", block.Number).Info("Block proposed")
	return nil
}

// VoteOnBlock votes on a proposed block
func (api *DefaultConsensusAPI) VoteOnBlock(blockHash string, approve bool) error {
	api.mu.RLock()
	defer api.mu.RUnlock()

	if !api.running {
		return errors.New("consensus is not running")
	}

	if api.paused {
		return errors.New("consensus is paused")
	}

	if blockHash == "" {
		return errors.New("block hash cannot be empty")
	}

	action := "rejected"
	if approve {
		action = "approved"
	}

	logrus.WithFields(logrus.Fields{"blockHash": blockHash, "action": action}).Info("Vote cast on block")
	return nil
}

// Start starts the consensus engine
func (api *DefaultConsensusAPI) Start() error {
	api.mu.Lock()
	defer api.mu.Unlock()

	if api.running {
		return errors.New("consensus is already running")
	}

	api.running = true
	api.paused = false
	logrus.Info("Consensus started")
	return nil
}

// Stop stops the consensus engine
func (api *DefaultConsensusAPI) Stop() error {
	api.mu.Lock()
	defer api.mu.Unlock()

	if !api.running {
		return errors.New("consensus is not running")
	}

	api.running = false
	api.paused = false
	logrus.Info("Consensus stopped")
	return nil
}

// Pause pauses the consensus engine
func (api *DefaultConsensusAPI) Pause() error {
	api.mu.Lock()
	defer api.mu.Unlock()

	if !api.running {
		return errors.New("consensus is not running")
	}

	if api.paused {
		return errors.New("consensus is already paused")
	}

	api.paused = true
	logrus.Info("Consensus paused")
	return nil
}

// Resume resumes the consensus engine
func (api *DefaultConsensusAPI) Resume() error {
	api.mu.Lock()
	defer api.mu.Unlock()

	if !api.running {
		return errors.New("consensus is not running")
	}

	if !api.paused {
		return errors.New("consensus is not paused")
	}

	api.paused = false
	logrus.Info("Consensus resumed")
	return nil
}

// DefaultStorageAPI provides a concrete implementation of StorageAPI
type DefaultStorageAPI struct {
	blocks       map[uint64]*Block
	transactions map[string]*Transaction
	accounts     map[string]*Account
	contracts    map[string]*SmartContract
	mu           sync.RWMutex
}

// NewDefaultStorageAPI creates a new instance of DefaultStorageAPI
func NewDefaultStorageAPI() *DefaultStorageAPI {
	return &DefaultStorageAPI{
		blocks:       make(map[uint64]*Block),
		transactions: make(map[string]*Transaction),
		accounts:     make(map[string]*Account),
		contracts:    make(map[string]*SmartContract),
	}
}

// SaveBlock saves a block to storage
func (api *DefaultStorageAPI) SaveBlock(block *Block) error {
	api.mu.Lock()
	defer api.mu.Unlock()

	if block == nil {
		return errors.New("block cannot be nil")
	}

	blockNum := uint64(block.Number)
	api.blocks[blockNum] = block
	logrus.WithField("blockNumber", block.Number).Info("Block saved")
	return nil
}

// GetBlock retrieves a block from storage
func (api *DefaultStorageAPI) GetBlock(blockNumber uint64) (*Block, error) {
	api.mu.RLock()
	defer api.mu.RUnlock()

	block, exists := api.blocks[blockNumber]
	if !exists {
		return nil, errors.New("block not found")
	}

	// Return a copy
	blockCopy := *block
	return &blockCopy, nil
}

// SaveTransaction saves a transaction to storage
func (api *DefaultStorageAPI) SaveTransaction(tx *Transaction) error {
	api.mu.Lock()
	defer api.mu.Unlock()

	if tx == nil {
		return errors.New("transaction cannot be nil")
	}

	api.transactions[tx.ID] = tx
	logrus.WithField("txID", tx.ID).Info("Transaction saved")
	return nil
}

// GetTransaction retrieves a transaction from storage
func (api *DefaultStorageAPI) GetTransaction(txID string) (*Transaction, error) {
	api.mu.RLock()
	defer api.mu.RUnlock()

	if txID == "" {
		return nil, errors.New("transaction ID cannot be empty")
	}

	tx, exists := api.transactions[txID]
	if !exists {
		return nil, errors.New("transaction not found")
	}

	// Return a copy
	txCopy := *tx
	return &txCopy, nil
}

// SaveAccount saves an account to storage
func (api *DefaultStorageAPI) SaveAccount(account *Account) error {
	api.mu.Lock()
	defer api.mu.Unlock()

	if account == nil {
		return errors.New("account cannot be nil")
	}

	api.accounts[account.ID] = account.Clone()
	logrus.WithField("accountID", account.ID).Info("Account saved")
	return nil
}

// GetAccount retrieves an account from storage
func (api *DefaultStorageAPI) GetAccount(accountID string) (*Account, error) {
	api.mu.RLock()
	defer api.mu.RUnlock()

	if accountID == "" {
		return nil, errors.New("account ID cannot be empty")
	}

	account, exists := api.accounts[accountID]
	if !exists {
		return nil, errors.New("account not found")
	}

	return account.Clone(), nil
}

// SaveContract saves a smart contract to storage
func (api *DefaultStorageAPI) SaveContract(contract *SmartContract) error {
	api.mu.Lock()
	defer api.mu.Unlock()

	if contract == nil {
		return errors.New("contract cannot be nil")
	}

	api.contracts[contract.ID] = contract
	logrus.WithField("contractID", contract.ID).Info("Contract saved")
	return nil
}

// GetContract retrieves a smart contract from storage
func (api *DefaultStorageAPI) GetContract(contractID string) (*SmartContract, error) {
	api.mu.RLock()
	defer api.mu.RUnlock()

	if contractID == "" {
		return nil, errors.New("contract ID cannot be empty")
	}

	contract, exists := api.contracts[contractID]
	if !exists {
		return nil, errors.New("contract not found")
	}

	// Return a copy
	contractCopy := *contract
	return &contractCopy, nil
}

// Backup creates a backup of the storage
func (api *DefaultStorageAPI) Backup(path string) error {
	api.mu.RLock()
	defer api.mu.RUnlock()

	if path == "" {
		return errors.New("backup path cannot be empty")
	}

	// In a real implementation, this would write to file system
	logrus.WithField("path", path).Info("Storage backup created")
	return nil
}

// Restore restores storage from a backup
func (api *DefaultStorageAPI) Restore(path string) error {
	api.mu.Lock()
	defer api.mu.Unlock()

	if path == "" {
		return errors.New("restore path cannot be empty")
	}

	// In a real implementation, this would read from file system
	logrus.WithField("path", path).Info("Storage restored")
	return nil
}

// PruneData removes old data from storage
func (api *DefaultStorageAPI) PruneData(olderThan time.Time) error {
	api.mu.Lock()
	defer api.mu.Unlock()

	pruneCount := 0
	// In a real implementation, this would remove data older than the specified time
	logrus.WithFields(logrus.Fields{"count": pruneCount, "olderThan": olderThan.Format(time.RFC3339)}).Info("Pruned old records")
	return nil
}

// Vacuum optimizes storage space
func (api *DefaultStorageAPI) Vacuum() error {
	api.mu.Lock()
	defer api.mu.Unlock()

	// In a real implementation, this would optimize storage
	logrus.Info("Storage vacuum completed")
	return nil
}
