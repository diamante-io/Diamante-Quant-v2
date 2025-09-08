package vm_test

import (
	"context"
	"math/big"
	"testing"
	"time"

	"diamante/common"
	"diamante/consensus"
	"diamante/ledger/evm"
	"diamante/storage"
	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// MockLedgerAPI implements common.LedgerAPI for testing
type MockLedgerAPI struct {
	mock.Mock
}

func (m *MockLedgerAPI) GetAccount(address string) (*common.Account, error) {
	args := m.Called(address)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*common.Account), args.Error(1)
}

func (m *MockLedgerAPI) GetBalance(address string) (float64, error) {
	args := m.Called(address)
	return args.Get(0).(float64), args.Error(1)
}

func (m *MockLedgerAPI) GetTransaction(txID string) (*common.Transaction, error) {
	args := m.Called(txID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*common.Transaction), args.Error(1)
}

func (m *MockLedgerAPI) UpdateBalance(address string, delta float64) error {
	args := m.Called(address, delta)
	return args.Error(0)
}

func (m *MockLedgerAPI) StoreTransaction(tx *common.Transaction) error {
	args := m.Called(tx)
	return args.Error(0)
}

func (m *MockLedgerAPI) GetBlockHeight() (int, error) {
	args := m.Called()
	return args.Get(0).(int), args.Error(1)
}

func (m *MockLedgerAPI) GetBlockByHeight(height int) (*common.Block, error) {
	args := m.Called(height)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*common.Block), args.Error(1)
}

func (m *MockLedgerAPI) GetSmartContract(contractID string) (*common.SmartContract, error) {
	args := m.Called(contractID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*common.SmartContract), args.Error(1)
}

func (m *MockLedgerAPI) StoreSmartContract(contract *common.SmartContract) error {
	args := m.Called(contract)
	return args.Error(0)
}

func (m *MockLedgerAPI) AddTransaction(tx common.Transaction) error {
	args := m.Called(tx)
	return args.Error(0)
}

// MockLedgerStore implements storage.LedgerStore for testing
type MockLedgerStore struct {
	mock.Mock
}

func (m *MockLedgerStore) GetAccount(address string) (*common.Account, error) {
	args := m.Called(address)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*common.Account), args.Error(1)
}

func (m *MockLedgerStore) SaveAccount(account *common.Account) error {
	args := m.Called(account)
	return args.Error(0)
}

func (m *MockLedgerStore) GetTransaction(txID string) (*common.Transaction, error) {
	args := m.Called(txID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*common.Transaction), args.Error(1)
}

func (m *MockLedgerStore) SaveTransaction(tx *common.Transaction, blockHeight int) error {
	args := m.Called(tx, blockHeight)
	return args.Error(0)
}

func (m *MockLedgerStore) GetBlock(height uint64) (*common.Block, error) {
	args := m.Called(height)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*common.Block), args.Error(1)
}

func (m *MockLedgerStore) SaveBlock(block *common.Block) error {
	args := m.Called(block)
	return args.Error(0)
}

func (m *MockLedgerStore) GetBlockByHash(hash string) (*common.Block, error) {
	args := m.Called(hash)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*common.Block), args.Error(1)
}

func (m *MockLedgerStore) GetLatestBlock() (*common.Block, error) {
	args := m.Called()
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*common.Block), args.Error(1)
}

func (m *MockLedgerStore) GetContract(address string) (*storage.Contract, error) {
	args := m.Called(address)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*storage.Contract), args.Error(1)
}

func (m *MockLedgerStore) SaveContract(contract *storage.Contract) error {
	args := m.Called(contract)
	return args.Error(0)
}

func (m *MockLedgerStore) GetReceipt(txHash string) (*storage.Receipt, error) {
	args := m.Called(txHash)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*storage.Receipt), args.Error(1)
}

func (m *MockLedgerStore) SaveReceipt(receipt *storage.Receipt) error {
	args := m.Called(receipt)
	return args.Error(0)
}

func (m *MockLedgerStore) GetState(key []byte) ([]byte, error) {
	args := m.Called(key)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]byte), args.Error(1)
}

func (m *MockLedgerStore) SaveState(key, value []byte) error {
	args := m.Called(key, value)
	return args.Error(0)
}

func (m *MockLedgerStore) DeleteState(key []byte) error {
	args := m.Called(key)
	return args.Error(0)
}

func (m *MockLedgerStore) GetAccountCount() (int, error) {
	args := m.Called()
	return args.Get(0).(int), args.Error(1)
}

func (m *MockLedgerStore) GetTransactionCount() (int, error) {
	args := m.Called()
	return args.Get(0).(int), args.Error(1)
}

func (m *MockLedgerStore) GetBlockCount() (int, error) {
	args := m.Called()
	return args.Get(0).(int), args.Error(1)
}

func (m *MockLedgerStore) GetContractCount() (int, error) {
	args := m.Called()
	return args.Get(0).(int), args.Error(1)
}

func (m *MockLedgerStore) BeginTransaction() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockLedgerStore) CommitTransaction() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockLedgerStore) RollbackTransaction() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockLedgerStore) Close() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockLedgerStore) Backup(backupPath string) error {
	args := m.Called(backupPath)
	return args.Error(0)
}

func (m *MockLedgerStore) Restore(backupPath string) error {
	args := m.Called(backupPath)
	return args.Error(0)
}

func (m *MockLedgerStore) GetTransactionRange(start, end uint64) ([]*common.Transaction, error) {
	args := m.Called(start, end)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*common.Transaction), args.Error(1)
}

func (m *MockLedgerStore) GetBlockRange(start, end uint64) ([]*common.Block, error) {
	args := m.Called(start, end)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*common.Block), args.Error(1)
}

func (m *MockLedgerStore) GetAccountHistory(address string, limit int) ([]*common.Transaction, error) {
	args := m.Called(address, limit)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*common.Transaction), args.Error(1)
}

func (m *MockLedgerStore) GetAllAccounts() ([]*common.Account, error) {
	args := m.Called()
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*common.Account), args.Error(1)
}

func (m *MockLedgerStore) Compact() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockLedgerStore) CreateSnapshot(height uint64) error {
	args := m.Called(height)
	return args.Error(0)
}

func (m *MockLedgerStore) DeleteContract(contractID string) error {
	args := m.Called(contractID)
	return args.Error(0)
}

func (m *MockLedgerStore) GetBalance(address string) (float64, error) {
	args := m.Called(address)
	return args.Get(0).(float64), args.Error(1)
}

func (m *MockLedgerStore) GetNonce(address string) (uint64, error) {
	args := m.Called(address)
	return args.Get(0).(uint64), args.Error(1)
}

func (m *MockLedgerStore) GetStats() (*storage.StoreStats, error) {
	args := m.Called()
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*storage.StoreStats), args.Error(1)
}

func (m *MockLedgerStore) ReplaceBlockSameHeight(height uint64, newBlock *common.Block) error {
	args := m.Called(height, newBlock)
	return args.Error(0)
}

func (m *MockLedgerStore) GetTransactionsByAddress(address string, limit, offset int) ([]*common.Transaction, error) {
	args := m.Called(address, limit, offset)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*common.Transaction), args.Error(1)
}

func (m *MockLedgerStore) GetTransactionsByBlock(blockHeight uint64) ([]*common.Transaction, error) {
	args := m.Called(blockHeight)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*common.Transaction), args.Error(1)
}

func (m *MockLedgerStore) HealthCheck(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockLedgerStore) ListSnapshots() ([]storage.SnapshotInfo, error) {
	args := m.Called()
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]storage.SnapshotInfo), args.Error(1)
}

func (m *MockLedgerStore) Open() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockLedgerStore) PruneData(olderThan time.Time) error {
	args := m.Called(olderThan)
	return args.Error(0)
}

func (m *MockLedgerStore) RestoreSnapshot(height uint64) error {
	args := m.Called(height)
	return args.Error(0)
}

func (m *MockLedgerStore) SetState(key, value []byte) error {
	args := m.Called(key, value)
	return args.Error(0)
}

func (m *MockLedgerStore) UpdateAccount(account *common.Account) error {
	args := m.Called(account)
	return args.Error(0)
}

func (m *MockLedgerStore) UpdateContract(contract *common.SmartContract) error {
	args := m.Called(contract)
	return args.Error(0)
}

func (m *MockLedgerStore) Vacuum() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockLedgerStore) IsOpen() bool {
	args := m.Called()
	return args.Bool(0)
}

func (m *MockLedgerStore) Snapshot(path string) error {
	args := m.Called(path)
	return args.Error(0)
}

func (m *MockLedgerStore) WriteBatch(batch storage.WriteBatch) error {
	args := m.Called(batch)
	return args.Error(0)
}

// Add Close method to MockLedgerAPI
func (m *MockLedgerAPI) Close() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockLedgerAPI) CommitBlock(block common.Block) error {
	args := m.Called(block)
	return args.Error(0)
}

func (m *MockLedgerAPI) CreateAccount(ac *common.Account) error {
	args := m.Called(ac)
	return args.Error(0)
}

func (m *MockLedgerAPI) DeleteAccount(address string) error {
	args := m.Called(address)
	return args.Error(0)
}

func (m *MockLedgerAPI) GetCurrentBlockHash() (string, error) {
	args := m.Called()
	return args.Get(0).(string), args.Error(1)
}

func (m *MockLedgerAPI) GetPendingTransactions() ([]common.Transaction, error) {
	args := m.Called()
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]common.Transaction), args.Error(1)
}

func (m *MockLedgerAPI) CreateSnapshot(height int) error {
	args := m.Called(height)
	return args.Error(0)
}

func (m *MockLedgerAPI) DeploySmartContract(sc *common.SmartContract) error {
	args := m.Called(sc)
	return args.Error(0)
}

func (m *MockLedgerAPI) ExecuteSmartContract(scID, function, sender string, params *common.SmartContractParams) (*common.SmartContractResult, error) {
	args := m.Called(scID, function, sender, params)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*common.SmartContractResult), args.Error(1)
}

func (m *MockLedgerAPI) GetAccountTransactions(accountID string, limit, offset int) ([]common.Transaction, error) {
	args := m.Called(accountID, limit, offset)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]common.Transaction), args.Error(1)
}

func (m *MockLedgerAPI) GetBlockByNumber(num int) (common.Block, bool) {
	args := m.Called(num)
	return args.Get(0).(common.Block), args.Bool(1)
}

func (m *MockLedgerAPI) GetBlocksByRange(startNum, endNum int) ([]common.Block, error) {
	args := m.Called(startNum, endNum)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]common.Block), args.Error(1)
}

func (m *MockLedgerAPI) GetLastBlockHash() (string, error) {
	args := m.Called()
	return args.Get(0).(string), args.Error(1)
}

func (m *MockLedgerAPI) GetStats() (*common.LedgerStats, error) {
	args := m.Called()
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*common.LedgerStats), args.Error(1)
}

func (m *MockLedgerAPI) HealthCheck(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockLedgerAPI) IntegrityCheck() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockLedgerAPI) IsTransactionCommitted(txID string) bool {
	args := m.Called(txID)
	return args.Bool(0)
}

func (m *MockLedgerAPI) RemoveSmartContract(contractID string) error {
	args := m.Called(contractID)
	return args.Error(0)
}

func (m *MockLedgerAPI) RestoreSnapshot(height int) error {
	args := m.Called(height)
	return args.Error(0)
}

func (m *MockLedgerAPI) UpdateAccount(ac *common.Account) error {
	args := m.Called(ac)
	return args.Error(0)
}

func (m *MockLedgerAPI) UpdateAccountBalance(accountID string, amount float64) error {
	args := m.Called(accountID, amount)
	return args.Error(0)
}

func (m *MockLedgerAPI) UpdateSmartContract(contractID, newCode, version string) error {
	args := m.Called(contractID, newCode, version)
	return args.Error(0)
}

// TestEVMExecutorBasicOperations tests basic EVM executor operations
func TestEVMExecutorBasicOperations(t *testing.T) {
	executor := createTestEVMExecutor(t)

	t.Run("deploy_simple_contract", func(t *testing.T) {
		// Simple storage contract bytecode
		contractCode := ethcommon.FromHex("608060405234801561001057600080fd5b50610040806100206000396000f3fe")
		caller := ethcommon.HexToAddress("0x1234567890123456789012345678901234567890")
		value := big.NewInt(0)
		gasLimit := uint64(1000000)

		contractAddr, deployedCode, gasUsed, err := executor.DeployContract(caller, contractCode, value, gasLimit)
		require.NoError(t, err)
		assert.NotEqual(t, ethcommon.Address{}, contractAddr)
		assert.NotNil(t, deployedCode)
		assert.Greater(t, gasUsed, uint64(0))
		assert.Less(t, gasUsed, gasLimit)
	})

	t.Run("execute_contract_call", func(t *testing.T) {
		// Deploy a simple contract first
		contractCode := ethcommon.FromHex("608060405234801561001057600080fd5b50610040806100206000396000f3fe")
		caller := ethcommon.HexToAddress("0x1234567890123456789012345678901234567890")
		value := big.NewInt(0)
		gasLimit := uint64(1000000)

		contractAddr, _, _, err := executor.DeployContract(caller, contractCode, value, gasLimit)
		require.NoError(t, err)

		// Execute a call to the contract
		input := []byte{} // Empty input for simple test
		result, gasUsed, err := executor.ExecuteContract(caller, contractAddr, input, value, gasLimit)
		require.NoError(t, err)
		assert.NotNil(t, result)
		assert.Greater(t, gasUsed, uint64(0))
		assert.Less(t, gasUsed, gasLimit)
	})
}

// TestEVMExecutorGasHandling tests gas limit and usage
func TestEVMExecutorGasHandling(t *testing.T) {
	executor := createTestEVMExecutor(t)

	t.Run("insufficient_gas", func(t *testing.T) {
		contractCode := ethcommon.FromHex("608060405234801561001057600080fd5b50610040806100206000396000f3fe")
		caller := ethcommon.HexToAddress("0x1234567890123456789012345678901234567890")
		value := big.NewInt(0)
		gasLimit := uint64(1000) // Very low gas limit

		_, _, gasUsed, err := executor.DeployContract(caller, contractCode, value, gasLimit)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "gas")
		assert.Equal(t, gasLimit, gasUsed) // All gas should be consumed
	})
}

// TestEVMExecutorStateManagement tests state persistence
func TestEVMExecutorStateManagement(t *testing.T) {
	executor := createTestEVMExecutor(t)
	stateDB := executor.GetStateDB()

	t.Run("account_balance", func(t *testing.T) {
		// Note: We can't directly set balance in tests, so we test through contract execution
		// that modifies balances
		t.Skip("Balance testing requires contract execution")
	})

	t.Run("account_nonce", func(t *testing.T) {
		addr := ethcommon.HexToAddress("0x1234567890123456789012345678901234567890")

		// Deploy a contract to increment nonce
		contractCode := ethcommon.FromHex("608060405234801561001057600080fd5b50610040806100206000396000f3fe")
		value := big.NewInt(0)
		gasLimit := uint64(1000000)

		_, _, _, err := executor.DeployContract(addr, contractCode, value, gasLimit)
		require.NoError(t, err)

		// Verify nonce increased
		nonce := stateDB.GetNonce(addr)
		assert.Greater(t, nonce, uint64(0))
	})

	t.Run("contract_storage", func(t *testing.T) {
		// Deploy a storage contract
		// This is a simple contract that stores and retrieves a value
		// contract Storage { uint256 public value; function set(uint256 v) public { value = v; } }
		contractCode := ethcommon.FromHex("608060405234801561001057600080fd5b50610150806100206000396000f3fe608060405234801561001057600080fd5b50600436106100365760003560e01c80633fa4f2451461003b5780635524107714610059575b600080fd5b610043610075565b60405161005091906100a1565b60405180910390f35b610073600480360381019061006e91906100ed565b61007b565b005b60005481565b8060008190555050565b6000819050919050565b61009b81610088565b82525050565b60006020820190506100b66000830184610092565b92915050565b600080fd5b6100ca81610088565b81146100d557600080fd5b50565b6000813590506100e7816100c1565b92915050565b600060208284031215610103576101026100bc565b5b6000610111848285016100d8565b9150509291505056fea2646970667358221220")

		caller := ethcommon.HexToAddress("0x1234567890123456789012345678901234567890")
		value := big.NewInt(0)
		gasLimit := uint64(1000000)

		contractAddr, _, _, err := executor.DeployContract(caller, contractCode, value, gasLimit)
		require.NoError(t, err)

		// Now test that storage is properly handled
		key := ethcommon.HexToHash("0x00")
		storedValue := stateDB.GetState(contractAddr, key)
		// Initial value should be zero
		assert.Equal(t, ethcommon.Hash{}, storedValue)
	})
}

// Helper function to create test EVM executor
func createTestEVMExecutor(t *testing.T) *evm.EVMExecutor {
	// Create mocks
	mockLedger := new(MockLedgerAPI)
	mockStore := new(MockLedgerStore)

	// Setup default mock responses
	mockLedger.On("GetBlockHeight").Return(1, nil)
	mockLedger.On("GetAccount", mock.Anything).Return(&common.Account{
		ID:        "test",
		Balance:   1000000.0,
		Nonce:     0,
		PublicKey: []byte("test-key"),
	}, nil)

	mockStore.On("GetState", mock.Anything).Return([]byte{}, nil)
	mockStore.On("SaveState", mock.Anything, mock.Anything).Return(nil)
	mockStore.On("DeleteState", mock.Anything).Return(nil)

	// Create logger
	logger := logrus.New()
	logger.SetLevel(logrus.WarnLevel)

	// Create executor
	blockHeight := uint64(consensus.ConsensusNow().Unix())
	executor := evm.NewEVMExecutor(mockLedger, mockStore, blockHeight, logger)

	return executor
}
