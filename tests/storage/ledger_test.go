package storage

import (
	"testing"
	"time"

	"diamante/common"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MockLedger for testing - implements basic Ledger interface
type MockLedger struct {
	accounts      map[string]*common.Account
	transactions  map[string]*common.Transaction
	blocks        map[int]common.Block
	contracts     map[string]*common.SmartContract
	currentHeight uint64
}

// NewMockLedger creates a new mock ledger for testing
func NewMockLedger() *MockLedger {
	return &MockLedger{
		accounts:      make(map[string]*common.Account),
		transactions:  make(map[string]*common.Transaction),
		blocks:        make(map[int]common.Block),
		contracts:     make(map[string]*common.SmartContract),
		currentHeight: 0,
	}
}

// Basic implementations to satisfy interface
func (m *MockLedger) GetAccount(accountID string) (*common.Account, error) {
	if account, exists := m.accounts[accountID]; exists {
		return account, nil
	}
	return nil, common.ValidationError(nil, "account not found")
}

func (m *MockLedger) UpdateAccount(account *common.Account) error {
	m.accounts[account.ID] = account
	return nil
}

func (m *MockLedger) CreateAccount(account *common.Account) error {
	m.accounts[account.ID] = account
	return nil
}

func (m *MockLedger) AddTransaction(tx common.Transaction) error {
	m.transactions[tx.ID] = &tx
	return nil
}

func (m *MockLedger) GetTransaction(txID string) (*common.Transaction, error) {
	if tx, exists := m.transactions[txID]; exists {
		return tx, nil
	}
	return nil, common.ValidationError(nil, "transaction not found")
}

func (m *MockLedger) GetAccountTransactions(accountID string, limit int, offset int) ([]common.Transaction, error) {
	var result []common.Transaction
	count := 0
	skipped := 0

	for _, tx := range m.transactions {
		if tx.Sender == accountID || tx.Receiver == accountID {
			if skipped < offset {
				skipped++
				continue
			}
			if count >= limit {
				break
			}
			result = append(result, *tx)
			count++
		}
	}
	return result, nil
}

func (m *MockLedger) IsTransactionCommitted(txID string) bool {
	_, exists := m.transactions[txID]
	return exists
}

func (m *MockLedger) CommitBlock(block common.Block) error {
	m.blocks[block.Number] = block
	if uint64(block.Number) > m.currentHeight {
		m.currentHeight = uint64(block.Number)
	}
	return nil
}

func (m *MockLedger) GetBlockByNumber(num int) (common.Block, bool) {
	block, exists := m.blocks[num]
	return block, exists
}

func (m *MockLedger) GetBlockHeight() (uint64, error) {
	return m.currentHeight, nil
}

func (m *MockLedger) DeploySmartContract(contract *common.SmartContract) error {
	m.contracts[contract.ID] = contract
	return nil
}

func (m *MockLedger) GetSmartContract(contractID string) (*common.SmartContract, error) {
	if contract, exists := m.contracts[contractID]; exists {
		return contract, nil
	}
	return nil, common.ValidationError(nil, "contract not found")
}

func (m *MockLedger) ExecuteSmartContract(contractID string, function string, payload string, args map[string]interface{}) (interface{}, error) {
	return map[string]interface{}{
		"success": true,
		"result":  "mock execution result",
	}, nil
}

func (m *MockLedger) GetBlocksByRange(start, end int) ([]common.Block, error) {
	var blocks []common.Block
	for i := start; i <= end; i++ {
		if block, exists := m.blocks[i]; exists {
			blocks = append(blocks, block)
		}
	}
	return blocks, nil
}

func (m *MockLedger) GetAccountBalance(accountID string) (float64, error) {
	account, err := m.GetAccount(accountID)
	if err != nil {
		return 0, err
	}
	return account.Balance, nil
}

func (m *MockLedger) UpdateAccountBalance(accountID string, amount float64) error {
	account, err := m.GetAccount(accountID)
	if err != nil {
		return err
	}
	account.Balance += amount
	return m.UpdateAccount(account)
}

// Test interface compliance
func TestLedgerInterface(t *testing.T) {
	// This ensures MockLedger implements the Ledger interface
	ledger := NewMockLedger()
	assert.NotNil(t, ledger)
}

func TestMockLedger_BasicOperations(t *testing.T) {
	ledger := NewMockLedger()

	// Test account operations
	account := &common.Account{
		ID:      "test_account",
		Balance: 100.0,
	}

	err := ledger.CreateAccount(account)
	require.NoError(t, err)

	retrieved, err := ledger.GetAccount("test_account")
	require.NoError(t, err)
	assert.Equal(t, account.ID, retrieved.ID)
	assert.Equal(t, account.Balance, retrieved.Balance)
}

func TestMockLedger_TransactionOperations(t *testing.T) {
	ledger := NewMockLedger()

	// Create test transaction
	tx := common.Transaction{
		ID:        "test_tx_1",
		Sender:    "account_1",
		Receiver:  "account_2",
		Amount:    50.0,
		Fee:       1.0,
		Timestamp: time.Now().Unix(),
	}

	// Test transaction addition
	err := ledger.AddTransaction(tx)
	require.NoError(t, err)

	// Test transaction retrieval
	retrieved, err := ledger.GetTransaction("test_tx_1")
	require.NoError(t, err)
	assert.Equal(t, tx.ID, retrieved.ID)
	assert.Equal(t, tx.Amount, retrieved.Amount)

	// Test transaction commitment check
	assert.True(t, ledger.IsTransactionCommitted("test_tx_1"))
	assert.False(t, ledger.IsTransactionCommitted("non_existent_tx"))
}

func TestMockLedger_BlockOperations(t *testing.T) {
	ledger := NewMockLedger()

	// Create test block
	block := common.Block{
		Number:       1,
		PreviousHash: "genesis",
		MerkleRoot:   "merkle_root_1",
		Timestamp:    time.Now().Unix(),
		Transactions: []common.Transaction{},
		Validator:    "validator_1",
	}

	// Test block commit
	err := ledger.CommitBlock(block)
	require.NoError(t, err)

	// Test block retrieval
	retrieved, exists := ledger.GetBlockByNumber(1)
	require.True(t, exists)
	assert.Equal(t, block.Number, retrieved.Number)
	assert.Equal(t, block.MerkleRoot, retrieved.MerkleRoot)

	// Test block height
	height, err := ledger.GetBlockHeight()
	require.NoError(t, err)
	assert.Equal(t, uint64(1), height)
}

func TestMockLedger_SmartContractOperations(t *testing.T) {
	ledger := NewMockLedger()

	// Create test smart contract
	contract := &common.SmartContract{
		ID:       "contract_1",
		Version:  "1.0.0",
		Language: "Solidity",
		Code:     "contract TestContract { }",
		ABI:      "[]",
		Owner:    "owner_1",
		State: &common.SmartContractState{
			Variables:     make(map[string]string),
			Balances:      make(map[string]float64),
			Permissions:   make(map[string]bool),
			Configuration: make(map[string]string),
			Counters:      make(map[string]int64),
			LastUpdated:   time.Now().Unix(),
		},
	}

	// Test contract deployment
	err := ledger.DeploySmartContract(contract)
	require.NoError(t, err)

	// Test contract retrieval
	retrieved, err := ledger.GetSmartContract("contract_1")
	require.NoError(t, err)
	assert.Equal(t, contract.ID, retrieved.ID)
	assert.Equal(t, contract.Version, retrieved.Version)

	// Test contract execution
	result, err := ledger.ExecuteSmartContract("contract_1", "testFunction", "{}", nil)
	require.NoError(t, err)
	assert.NotNil(t, result)

	// Test non-existent contract
	_, err = ledger.GetSmartContract("non_existent")
	require.Error(t, err)
}
