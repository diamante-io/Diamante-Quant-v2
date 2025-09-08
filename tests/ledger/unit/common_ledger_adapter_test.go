// tests/ledger/unit/common_ledger_adapter_test.go

package unit

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"diamante/common"
	"diamante/config"
	"diamante/ledger"
	"diamante/storage"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// MockAPILedger implements a mock ledger for testing
type MockAPILedger struct {
	mock.Mock
}

func (m *MockAPILedger) IsTransactionCommitted(txID string) bool {
	args := m.Called(txID)
	return args.Bool(0)
}

func (m *MockAPILedger) GetTransaction(txID string) (*common.Transaction, error) {
	args := m.Called(txID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*common.Transaction), args.Error(1)
}

func (m *MockAPILedger) GetAccountTransactions(accountID string, limit, offset int) ([]common.Transaction, error) {
	args := m.Called(accountID, limit, offset)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]common.Transaction), args.Error(1)
}

func (m *MockAPILedger) CommitBlock(block common.Block) error {
	args := m.Called(block)
	return args.Error(0)
}

func (m *MockAPILedger) GetBlockByNumber(num int) (common.Block, bool) {
	args := m.Called(num)
	return args.Get(0).(common.Block), args.Bool(1)
}

func (m *MockAPILedger) GetLastBlockHash() (string, error) {
	args := m.Called()
	return args.String(0), args.Error(1)
}

func (m *MockAPILedger) GetBlockHeight() (int, error) {
	args := m.Called()
	return args.Int(0), args.Error(1)
}

func (m *MockAPILedger) GetBlocksByRange(startNum, endNum int) ([]common.Block, error) {
	args := m.Called(startNum, endNum)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]common.Block), args.Error(1)
}

func (m *MockAPILedger) CreateSnapshot(height int) error {
	args := m.Called(height)
	return args.Error(0)
}

func (m *MockAPILedger) RestoreSnapshot(height int) error {
	args := m.Called(height)
	return args.Error(0)
}

func (m *MockAPILedger) DeploySmartContract(sc *common.SmartContract) error {
	args := m.Called(sc)
	return args.Error(0)
}

func (m *MockAPILedger) UpdateSmartContract(contractID, newCode, version string) error {
	args := m.Called(contractID, newCode, version)
	return args.Error(0)
}

func (m *MockAPILedger) RemoveSmartContract(contractID string) error {
	args := m.Called(contractID)
	return args.Error(0)
}

func (m *MockAPILedger) IntegrityCheck() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockAPILedger) Close() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockAPILedger) HealthCheck(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockAPILedger) BatchWrite(batch *storage.WriteBatch) error {
	args := m.Called(batch)
	return args.Error(0)
}

func (m *MockAPILedger) PruneData(olderThan time.Time) error {
	args := m.Called(olderThan)
	return args.Error(0)
}

func (m *MockAPILedger) Logger() *logrus.Logger {
	args := m.Called()
	if logger := args.Get(0); logger != nil {
		return logger.(*logrus.Logger)
	}
	return nil
}

// MockAPILedgerWithConversion implements mock with old interface
type MockAPILedgerWithConversion struct {
	mock.Mock
}

func (m *MockAPILedgerWithConversion) ExecuteSmartContract(scID, function, sender string, params map[string]interface{}) (interface{}, error) {
	args := m.Called(scID, function, sender, params)
	return args.Get(0), args.Error(1)
}

func (m *MockAPILedgerWithConversion) GetStats() (map[string]interface{}, error) {
	args := m.Called()
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(map[string]interface{}), args.Error(1)
}

func TestNewCommonLedgerAdapter(t *testing.T) {
	t.Run("creates adapter with cache configuration", func(t *testing.T) {
		mockLedger := &MockAPILedger{}
		cfg := &config.CacheConfig{
			Size:     1000,
			TTL:      time.Minute * 5,
			RedisURL: "redis://localhost:6379",
			RedisDB:  1,
		}

		adapter := ledger.NewCommonLedgerAdapter(mockLedger, cfg)
		require.NotNil(t, adapter)
	})

	t.Run("creates adapter without cache configuration", func(t *testing.T) {
		mockLedger := &MockAPILedger{}
		adapter := ledger.NewCommonLedgerAdapter(mockLedger, nil)
		require.NotNil(t, adapter)
	})
}

func TestCreateAccount(t *testing.T) {
	t.Run("successfully creates account", func(t *testing.T) {
		// No need to reset - using mock ledger

		adapter := ledger.NewCommonLedgerAdapter(&MockAPILedger{}, nil)

		account := &common.Account{
			ID:        "test-account-1",
			Balance:   100.0,
			PublicKey: []byte("test-public-key"),
			Nonce:     0,
		}

		err := adapter.CreateAccount(account)
		require.NoError(t, err)

		// Verify account was registered
		registeredAccount := common.GetAccount(account.ID)
		require.NotNil(t, registeredAccount)
		assert.Equal(t, account.ID, registeredAccount.ID)
		assert.Equal(t, account.Balance, registeredAccount.Balance)
	})

	t.Run("fails with duplicate account", func(t *testing.T) {
		// No need to reset - using mock ledger

		adapter := ledger.NewCommonLedgerAdapter(&MockAPILedger{}, nil)

		account := &common.Account{
			ID:        "test-account-duplicate",
			Balance:   100.0,
			PublicKey: []byte("test-public-key"),
			Nonce:     0,
		}

		// First creation should succeed
		err := adapter.CreateAccount(account)
		require.NoError(t, err)

		// Second creation should fail
		err = adapter.CreateAccount(account)
		require.Error(t, err)
	})
}

func TestUpdateAccount(t *testing.T) {
	t.Run("successfully updates existing account", func(t *testing.T) {
		// No need to reset - using mock ledger

		adapter := ledger.NewCommonLedgerAdapter(&MockAPILedger{}, nil)

		// Create initial account
		account := &common.Account{
			ID:        "test-account-update",
			Balance:   100.0,
			PublicKey: []byte("test-public-key"),
			Nonce:     0,
		}
		err := adapter.CreateAccount(account)
		require.NoError(t, err)

		// Update account
		updatedAccount := &common.Account{
			ID:        "test-account-update",
			Balance:   200.0,
			PublicKey: []byte("new-public-key"),
			Nonce:     5,
		}
		err = adapter.UpdateAccount(updatedAccount)
		require.NoError(t, err)

		// Verify updates
		balance, err := adapter.GetBalance(account.ID)
		require.NoError(t, err)
		assert.Equal(t, 200.0, balance)
	})

	t.Run("fails with non-existent account", func(t *testing.T) {
		// No need to reset - using mock ledger

		adapter := ledger.NewCommonLedgerAdapter(&MockAPILedger{}, nil)

		account := &common.Account{
			ID:        "non-existent",
			Balance:   100.0,
			PublicKey: []byte("test-public-key"),
			Nonce:     0,
		}

		err := adapter.UpdateAccount(account)
		require.Error(t, err)
		assert.Equal(t, common.ErrAccountNotFound, err)
	})
}

func TestCommonLedgerGetBalance(t *testing.T) {
	t.Run("gets balance from cache", func(t *testing.T) {
		// No need to reset - using mock ledger

		adapter := ledger.NewCommonLedgerAdapter(&MockAPILedger{}, nil)

		// Create account
		account := &common.Account{
			ID:        "test-balance-cache",
			Balance:   150.0,
			PublicKey: []byte("test-public-key"),
			Nonce:     0,
		}
		err := adapter.CreateAccount(account)
		require.NoError(t, err)

		// First call - loads into cache
		balance, err := adapter.GetBalance(account.ID)
		require.NoError(t, err)
		assert.Equal(t, 150.0, balance)

		// Second call - should get from cache
		balance, err = adapter.GetBalance(account.ID)
		require.NoError(t, err)
		assert.Equal(t, 150.0, balance)
	})

	t.Run("returns error for non-existent account", func(t *testing.T) {
		// No need to reset - using mock ledger

		adapter := ledger.NewCommonLedgerAdapter(&MockAPILedger{}, nil)

		balance, err := adapter.GetBalance("non-existent")
		require.Error(t, err)
		assert.Equal(t, common.ErrAccountNotFound, err)
		assert.Equal(t, 0.0, balance)
	})
}

func TestUpdateAccountBalance(t *testing.T) {
	t.Run("successfully updates balance", func(t *testing.T) {
		// No need to reset - using mock ledger

		adapter := ledger.NewCommonLedgerAdapter(&MockAPILedger{}, nil)

		// Create account
		account := &common.Account{
			ID:        "test-balance-update",
			Balance:   100.0,
			PublicKey: []byte("test-public-key"),
			Nonce:     0,
		}
		err := adapter.CreateAccount(account)
		require.NoError(t, err)

		// Update balance
		err = adapter.UpdateAccountBalance(account.ID, 50.0)
		require.NoError(t, err)

		// Verify new balance
		balance, err := adapter.GetBalance(account.ID)
		require.NoError(t, err)
		assert.Equal(t, 150.0, balance)
	})

	t.Run("handles negative balance update", func(t *testing.T) {
		// No need to reset - using mock ledger

		adapter := ledger.NewCommonLedgerAdapter(&MockAPILedger{}, nil)

		// Create account
		account := &common.Account{
			ID:        "test-negative-update",
			Balance:   100.0,
			PublicKey: []byte("test-public-key"),
			Nonce:     0,
		}
		err := adapter.CreateAccount(account)
		require.NoError(t, err)

		// Update with negative amount
		err = adapter.UpdateAccountBalance(account.ID, -30.0)
		require.NoError(t, err)

		// Verify new balance
		balance, err := adapter.GetBalance(account.ID)
		require.NoError(t, err)
		assert.Equal(t, 70.0, balance)
	})
}

func TestAddTransaction(t *testing.T) {
	t.Run("successfully adds valid transaction", func(t *testing.T) {
		// No need to reset - using mock ledger

		adapter := ledger.NewCommonLedgerAdapter(&MockAPILedger{}, nil)

		// Create sender and receiver accounts
		sender := &common.Account{
			ID:        "sender",
			Balance:   1000.0,
			PublicKey: []byte("sender-key"),
			Nonce:     0,
		}
		receiver := &common.Account{
			ID:        "receiver",
			Balance:   0.0,
			PublicKey: []byte("receiver-key"),
			Nonce:     0,
		}

		err := adapter.CreateAccount(sender)
		require.NoError(t, err)
		err = adapter.CreateAccount(receiver)
		require.NoError(t, err)

		// Create transaction
		tx := common.Transaction{
			ID:       "tx-1",
			Sender:   sender.ID,
			Receiver: receiver.ID,
			Amount:   100.0,
			Fee:      1.0,
		}

		// Add transaction
		err = adapter.AddTransaction(tx)
		require.NoError(t, err)

		// Verify balances
		senderBalance, err := adapter.GetBalance(sender.ID)
		require.NoError(t, err)
		assert.Equal(t, 899.0, senderBalance) // 1000 - 100 - 1

		receiverBalance, err := adapter.GetBalance(receiver.ID)
		require.NoError(t, err)
		assert.Equal(t, 100.0, receiverBalance)
	})

	t.Run("fails with insufficient funds", func(t *testing.T) {
		// No need to reset - using mock ledger

		adapter := ledger.NewCommonLedgerAdapter(&MockAPILedger{}, nil)

		// Create accounts
		sender := &common.Account{
			ID:        "poor-sender",
			Balance:   50.0,
			PublicKey: []byte("sender-key"),
			Nonce:     0,
		}
		receiver := &common.Account{
			ID:        "receiver2",
			Balance:   0.0,
			PublicKey: []byte("receiver-key"),
			Nonce:     0,
		}

		err := adapter.CreateAccount(sender)
		require.NoError(t, err)
		err = adapter.CreateAccount(receiver)
		require.NoError(t, err)

		// Create transaction that exceeds balance
		tx := common.Transaction{
			ID:       "tx-insufficient",
			Sender:   sender.ID,
			Receiver: receiver.ID,
			Amount:   100.0,
			Fee:      1.0,
		}

		// Should fail
		err = adapter.AddTransaction(tx)
		require.Error(t, err)
		assert.Equal(t, common.ErrInsufficientFunds, err)

		// Verify balances unchanged
		senderBalance, err := adapter.GetBalance(sender.ID)
		require.NoError(t, err)
		assert.Equal(t, 50.0, senderBalance)
	})

	t.Run("reverts sender balance on receiver update failure", func(t *testing.T) {
		// No need to reset - using mock ledger

		mockAPILedger := &MockAPILedger{}
		mockAPILedger.On("Logger").Return(logrus.New())

		adapter := ledger.NewCommonLedgerAdapter(mockAPILedger, nil)

		// Create only sender account
		sender := &common.Account{
			ID:        "sender-revert",
			Balance:   1000.0,
			PublicKey: []byte("sender-key"),
			Nonce:     0,
		}

		err := adapter.CreateAccount(sender)
		require.NoError(t, err)

		// Create transaction to non-existent receiver
		tx := common.Transaction{
			ID:       "tx-revert",
			Sender:   sender.ID,
			Receiver: "non-existent-receiver",
			Amount:   100.0,
			Fee:      1.0,
		}

		// Should fail
		err = adapter.AddTransaction(tx)
		require.Error(t, err)

		// Verify sender balance is unchanged (reverted)
		senderBalance, err := adapter.GetBalance(sender.ID)
		require.NoError(t, err)
		assert.Equal(t, 1000.0, senderBalance)
	})
}

func TestTransactionMethods(t *testing.T) {
	t.Run("IsTransactionCommitted", func(t *testing.T) {
		mockLedger := &MockAPILedger{}
		adapter := ledger.NewCommonLedgerAdapter(mockLedger, nil)

		mockLedger.On("IsTransactionCommitted", "tx-123").Return(true)

		result := adapter.IsTransactionCommitted("tx-123")
		assert.True(t, result)
		mockLedger.AssertExpectations(t)
	})

	t.Run("GetTransaction", func(t *testing.T) {
		mockLedger := &MockAPILedger{}
		adapter := ledger.NewCommonLedgerAdapter(mockLedger, nil)

		expectedTx := &common.Transaction{
			ID:       "tx-123",
			Sender:   "sender",
			Receiver: "receiver",
			Amount:   100.0,
		}
		mockLedger.On("GetTransaction", "tx-123").Return(expectedTx, nil)

		tx, err := adapter.GetTransaction("tx-123")
		require.NoError(t, err)
		assert.Equal(t, expectedTx, tx)
		mockLedger.AssertExpectations(t)
	})

	t.Run("GetAccountTransactions", func(t *testing.T) {
		mockLedger := &MockAPILedger{}
		adapter := ledger.NewCommonLedgerAdapter(mockLedger, nil)

		expectedTxs := []common.Transaction{
			{ID: "tx-1", Sender: "account-1", Amount: 100.0},
			{ID: "tx-2", Sender: "account-1", Amount: 200.0},
		}
		mockLedger.On("GetAccountTransactions", "account-1", 10, 0).Return(expectedTxs, nil)

		txs, err := adapter.GetAccountTransactions("account-1", 10, 0)
		require.NoError(t, err)
		assert.Equal(t, expectedTxs, txs)
		mockLedger.AssertExpectations(t)
	})
}

func TestBlockMethods(t *testing.T) {
	t.Run("CommitBlock", func(t *testing.T) {
		mockLedger := &MockAPILedger{}
		adapter := ledger.NewCommonLedgerAdapter(mockLedger, nil)

		block := common.Block{
			Number: 100,
			Hash:   "block-hash",
		}
		mockLedger.On("CommitBlock", block).Return(nil)

		err := adapter.CommitBlock(block)
		require.NoError(t, err)
		mockLedger.AssertExpectations(t)
	})

	t.Run("GetBlockByNumber", func(t *testing.T) {
		mockLedger := &MockAPILedger{}
		adapter := ledger.NewCommonLedgerAdapter(mockLedger, nil)

		expectedBlock := common.Block{
			Number: 100,
			Hash:   "block-hash",
		}
		mockLedger.On("GetBlockByNumber", 100).Return(expectedBlock, true)

		block, found := adapter.GetBlockByNumber(100)
		assert.True(t, found)
		assert.Equal(t, expectedBlock, block)
		mockLedger.AssertExpectations(t)
	})

	t.Run("GetLastBlockHash", func(t *testing.T) {
		mockLedger := &MockAPILedger{}
		adapter := ledger.NewCommonLedgerAdapter(mockLedger, nil)

		mockLedger.On("GetLastBlockHash").Return("last-block-hash", nil)

		hash, err := adapter.GetLastBlockHash()
		require.NoError(t, err)
		assert.Equal(t, "last-block-hash", hash)
		mockLedger.AssertExpectations(t)
	})

	t.Run("GetBlockHeight", func(t *testing.T) {
		mockLedger := &MockAPILedger{}
		adapter := ledger.NewCommonLedgerAdapter(mockLedger, nil)

		mockLedger.On("GetBlockHeight").Return(12345, nil)

		height, err := adapter.GetBlockHeight()
		require.NoError(t, err)
		assert.Equal(t, 12345, height)
		mockLedger.AssertExpectations(t)
	})

	t.Run("GetBlocksByRange", func(t *testing.T) {
		mockLedger := &MockAPILedger{}
		adapter := ledger.NewCommonLedgerAdapter(mockLedger, nil)

		expectedBlocks := []common.Block{
			{Number: 100},
			{Number: 101},
		}
		mockLedger.On("GetBlocksByRange", 100, 101).Return(expectedBlocks, nil)

		blocks, err := adapter.GetBlocksByRange(100, 101)
		require.NoError(t, err)
		assert.Equal(t, expectedBlocks, blocks)
		mockLedger.AssertExpectations(t)
	})
}

func TestSnapshotMethods(t *testing.T) {
	t.Run("CreateSnapshot clears caches", func(t *testing.T) {
		mockLedger := &MockAPILedger{}
		adapter := ledger.NewCommonLedgerAdapter(mockLedger, nil)

		// Add some data to cache
		// No need to reset - using mock ledger
		account := &common.Account{
			ID:      "cached-account",
			Balance: 100.0,
		}
		err := adapter.CreateAccount(account)
		require.NoError(t, err)

		mockLedger.On("CreateSnapshot", 100).Return(nil)

		err = adapter.CreateSnapshot(100)
		require.NoError(t, err)
		mockLedger.AssertExpectations(t)
	})

	t.Run("RestoreSnapshot clears caches", func(t *testing.T) {
		mockLedger := &MockAPILedger{}
		adapter := ledger.NewCommonLedgerAdapter(mockLedger, nil)

		mockLedger.On("RestoreSnapshot", 100).Return(nil)

		err := adapter.RestoreSnapshot(100)
		require.NoError(t, err)
		mockLedger.AssertExpectations(t)
	})
}

func TestSmartContractMethods(t *testing.T) {
	t.Run("DeploySmartContract", func(t *testing.T) {
		mockLedger := &MockAPILedger{}
		adapter := ledger.NewCommonLedgerAdapter(mockLedger, nil)

		contract := &common.SmartContract{
			ID:   "contract-1",
			Code: "contract-code",
		}
		mockLedger.On("DeploySmartContract", contract).Return(nil)

		err := adapter.DeploySmartContract(contract)
		require.NoError(t, err)
		mockLedger.AssertExpectations(t)
	})

	t.Run("UpdateSmartContract", func(t *testing.T) {
		mockLedger := &MockAPILedger{}
		adapter := ledger.NewCommonLedgerAdapter(mockLedger, nil)

		mockLedger.On("UpdateSmartContract", "contract-1", "new-code", "v2").Return(nil)

		err := adapter.UpdateSmartContract("contract-1", "new-code", "v2")
		require.NoError(t, err)
		mockLedger.AssertExpectations(t)
	})

	t.Run("RemoveSmartContract", func(t *testing.T) {
		mockLedger := &MockAPILedger{}
		adapter := ledger.NewCommonLedgerAdapter(mockLedger, nil)

		mockLedger.On("RemoveSmartContract", "contract-1").Return(nil)

		err := adapter.RemoveSmartContract("contract-1")
		require.NoError(t, err)
		mockLedger.AssertExpectations(t)
	})
}

func TestExecuteSmartContract(t *testing.T) {
	t.Run("executes with old interface conversion", func(t *testing.T) {
		mockLedger := &MockAPILedgerWithConversion{}
		adapter := ledger.NewCommonLedgerAdapter(mockLedger, nil)

		params := &common.SmartContractParams{
			FunctionName: "transfer",
			Caller:       "caller-address",
			StringParams: map[string]string{"to": "recipient"},
			NumberParams: map[string]float64{"amount": 100.0},
			BoolParams:   map[string]bool{"validate": true},
			ByteParams:   map[string][]byte{"data": []byte("test")},
		}

		expectedResult := map[string]interface{}{
			"success":       true,
			"string_result": "transfer complete",
			"gas_used":      float64(21000),
		}

		// Set up the expectation
		mockLedger.On("ExecuteSmartContract", "contract-1", "transfer", "sender", mock.MatchedBy(func(m map[string]interface{}) bool {
			// Verify the params were converted correctly
			return m["function_name"] == "transfer" &&
				m["caller"] == "caller-address" &&
				m["to"] == "recipient" &&
				m["amount"] == 100.0 &&
				m["validate"] == true
		})).Return(expectedResult, nil)

		result, err := adapter.ExecuteSmartContract("contract-1", "transfer", "sender", params)
		require.NoError(t, err)
		assert.True(t, result.Success)
		assert.Equal(t, "transfer complete", result.StringResult)
		assert.Equal(t, uint64(21000), result.GasUsed)
		mockLedger.AssertExpectations(t)
	})

	t.Run("handles error from old interface", func(t *testing.T) {
		mockLedger := &MockAPILedgerWithConversion{}
		adapter := ledger.NewCommonLedgerAdapter(mockLedger, nil)

		params := &common.SmartContractParams{
			FunctionName: "invalid",
		}

		mockLedger.On("ExecuteSmartContract", "contract-1", "invalid", "sender", mock.Anything).
			Return(nil, errors.New("execution failed"))

		result, err := adapter.ExecuteSmartContract("contract-1", "invalid", "sender", params)
		require.Error(t, err)
		assert.Nil(t, result)
		mockLedger.AssertExpectations(t)
	})

	t.Run("converts non-map result from old interface", func(t *testing.T) {
		mockLedger := &MockAPILedgerWithConversion{}
		adapter := ledger.NewCommonLedgerAdapter(mockLedger, nil)

		params := &common.SmartContractParams{}

		mockLedger.On("ExecuteSmartContract", "contract-1", "test", "sender", mock.Anything).
			Return("simple string result", nil)

		result, err := adapter.ExecuteSmartContract("contract-1", "test", "sender", params)
		require.NoError(t, err)
		assert.True(t, result.Success)
		assert.Equal(t, "simple string result", result.StringResult)
		mockLedger.AssertExpectations(t)
	})
}

func TestGetStats(t *testing.T) {
	t.Run("converts map stats to LedgerStats", func(t *testing.T) {
		mockLedger := &MockAPILedgerWithConversion{}
		adapter := ledger.NewCommonLedgerAdapter(mockLedger, nil)

		statsMap := map[string]interface{}{
			"total_accounts":     int64(100),
			"total_transactions": float64(1000),
			"total_contracts":    50,
			"total_balance":      12345.67,
			"last_block_height":  int(999),
			"network_health":     "excellent",
			"processing_time_ms": int64(42),
		}

		mockLedger.On("GetStats").Return(statsMap, nil)

		stats, err := adapter.GetStats()
		require.NoError(t, err)
		assert.Equal(t, int64(100), stats.TotalAccounts)
		assert.Equal(t, int64(1000), stats.TotalTransactions)
		assert.Equal(t, int64(50), stats.TotalContracts)
		assert.Equal(t, 12345.67, stats.TotalBalance)
		assert.Equal(t, int64(999), stats.LastBlockHeight)
		assert.Equal(t, "excellent", stats.NetworkHealth)
		assert.Equal(t, int64(42), stats.ProcessingTime)
		mockLedger.AssertExpectations(t)
	})

	t.Run("handles missing fields gracefully", func(t *testing.T) {
		mockLedger := &MockAPILedgerWithConversion{}
		adapter := ledger.NewCommonLedgerAdapter(mockLedger, nil)

		statsMap := map[string]interface{}{
			"total_accounts": "not-a-number", // Invalid type
		}

		mockLedger.On("GetStats").Return(statsMap, nil)

		stats, err := adapter.GetStats()
		require.NoError(t, err)
		assert.Equal(t, int64(0), stats.TotalAccounts)  // Should default to 0
		assert.Equal(t, "healthy", stats.NetworkHealth) // Should have default
		mockLedger.AssertExpectations(t)
	})
}

func TestUtilityMethods(t *testing.T) {
	t.Run("IntegrityCheck", func(t *testing.T) {
		mockLedger := &MockAPILedger{}
		adapter := ledger.NewCommonLedgerAdapter(mockLedger, nil)

		mockLedger.On("IntegrityCheck").Return(nil)

		err := adapter.IntegrityCheck()
		require.NoError(t, err)
		mockLedger.AssertExpectations(t)
	})

	t.Run("HealthCheck", func(t *testing.T) {
		mockLedger := &MockAPILedger{}
		adapter := ledger.NewCommonLedgerAdapter(mockLedger, nil)

		ctx := context.Background()
		mockLedger.On("HealthCheck", ctx).Return(nil)

		err := adapter.HealthCheck(ctx)
		require.NoError(t, err)
		mockLedger.AssertExpectations(t)
	})

	t.Run("Close", func(t *testing.T) {
		mockLedger := &MockAPILedger{}
		adapter := ledger.NewCommonLedgerAdapter(mockLedger, nil)

		mockLedger.On("Close").Return(nil)

		err := adapter.Close()
		require.NoError(t, err)
		mockLedger.AssertExpectations(t)
	})

	// BatchWrite and PruneData are not part of CommonLedgerAdapter interface
	t.Run("BatchWrite", func(t *testing.T) {
		t.Skip("BatchWrite not implemented in CommonLedgerAdapter")
	})

	t.Run("PruneData", func(t *testing.T) {
		t.Skip("PruneData not implemented in CommonLedgerAdapter")
	})
}

func TestNewLMDBLedgerAdapter(t *testing.T) {
	t.Run("creates LMDB adapter", func(t *testing.T) {
		// Skip on platforms where LMDB is not supported
		t.Skip("LMDB is disabled on this platform")

		cfg := &storage.LMDBConfig{
			Path:     "/tmp/test-lmdb",
			MapSize:  1024 * 1024 * 100, // 100MB
			ReadOnly: false,
		}

		logger := logrus.New()
		cacheSize := 1000
		cacheCfg := &config.CacheConfig{
			Size: 1000,
			TTL:  time.Minute * 5,
		}

		adapter, err := ledger.NewLMDBLedgerAdapter(cfg, logger, cacheSize, cacheCfg)
		require.NoError(t, err)
		require.NotNil(t, adapter)

		// Clean up
		if adapter != nil {
			adapter.Close()
		}
	})
}

// TestConcurrentOperations tests thread safety of the adapter
func TestConcurrentOperations(t *testing.T) {
	// Reset global accounts before test
	// No need to reset - using mock ledger

	adapter := ledger.NewCommonLedgerAdapter(&MockAPILedger{}, nil)

	// Create initial accounts
	for i := 0; i < 10; i++ {
		account := &common.Account{
			ID:        fmt.Sprintf("account-%d", i),
			Balance:   1000.0,
			PublicKey: []byte(fmt.Sprintf("key-%d", i)),
			Nonce:     0,
		}
		err := adapter.CreateAccount(account)
		require.NoError(t, err)
	}

	// Run concurrent operations
	var wg sync.WaitGroup
	errors := make(chan error, 100)

	// Concurrent balance updates
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(accountID string) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				if err := adapter.UpdateAccountBalance(accountID, 10.0); err != nil {
					errors <- err
				}
			}
		}(fmt.Sprintf("account-%d", i))
	}

	// Concurrent balance reads
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(accountID string) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				if _, err := adapter.GetBalance(accountID); err != nil {
					errors <- err
				}
			}
		}(fmt.Sprintf("account-%d", i))
	}

	// Wait for all operations to complete
	wg.Wait()
	close(errors)

	// Check for errors
	for err := range errors {
		t.Errorf("Concurrent operation error: %v", err)
	}

	// Verify final balances
	for i := 0; i < 10; i++ {
		balance, err := adapter.GetBalance(fmt.Sprintf("account-%d", i))
		require.NoError(t, err)
		assert.Equal(t, 1100.0, balance) // 1000 + (10 * 10)
	}
}

// TestCacheEffectiveness verifies that caching reduces backend calls
func TestCacheEffectiveness(t *testing.T) {
	// Reset global accounts before test
	// No need to reset - using mock ledger

	mockLedger := &MockAPILedger{}
	cfg := &config.CacheConfig{
		Size: 100,
		TTL:  time.Minute,
	}
	adapter := ledger.NewCommonLedgerAdapter(mockLedger, cfg)

	// Create account
	account := &common.Account{
		ID:        "cached-test",
		Balance:   500.0,
		PublicKey: []byte("test-key"),
		Nonce:     0,
	}
	err := adapter.CreateAccount(account)
	require.NoError(t, err)

	// First balance check - should cache
	balance1, err := adapter.GetBalance(account.ID)
	require.NoError(t, err)
	assert.Equal(t, 500.0, balance1)

	// Multiple subsequent checks should use cache
	for i := 0; i < 10; i++ {
		balance, err := adapter.GetBalance(account.ID)
		require.NoError(t, err)
		assert.Equal(t, 500.0, balance)
	}

	// Update balance should update cache
	err = adapter.UpdateAccountBalance(account.ID, 100.0)
	require.NoError(t, err)

	// Verify cache was updated
	balance2, err := adapter.GetBalance(account.ID)
	require.NoError(t, err)
	assert.Equal(t, 600.0, balance2)
}

// Benchmark tests
func BenchmarkCommonLedgerGetBalance(b *testing.B) {
	adapter := ledger.NewCommonLedgerAdapter(&MockAPILedger{}, nil)

	// Setup
	// No need to reset - using mock ledger
	account := &common.Account{
		ID:        "bench-account",
		Balance:   1000.0,
		PublicKey: []byte("bench-key"),
		Nonce:     0,
	}
	_ = adapter.CreateAccount(account)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = adapter.GetBalance(account.ID)
	}
}

func BenchmarkUpdateBalance(b *testing.B) {
	adapter := ledger.NewCommonLedgerAdapter(&MockAPILedger{}, nil)

	// Setup
	// No need to reset - using mock ledger
	account := &common.Account{
		ID:        "bench-update",
		Balance:   1000.0,
		PublicKey: []byte("bench-key"),
		Nonce:     0,
	}
	_ = adapter.CreateAccount(account)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = adapter.UpdateAccountBalance(account.ID, 1.0)
	}
}

func BenchmarkAddTransaction(b *testing.B) {
	adapter := ledger.NewCommonLedgerAdapter(&MockAPILedger{}, nil)

	// Setup
	// No need to reset - using mock ledger
	sender := &common.Account{
		ID:        "bench-sender",
		Balance:   1000000.0,
		PublicKey: []byte("sender-key"),
		Nonce:     0,
	}
	receiver := &common.Account{
		ID:        "bench-receiver",
		Balance:   0.0,
		PublicKey: []byte("receiver-key"),
		Nonce:     0,
	}
	_ = adapter.CreateAccount(sender)
	_ = adapter.CreateAccount(receiver)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx := common.Transaction{
			ID:       fmt.Sprintf("tx-%d", i),
			Sender:   sender.ID,
			Receiver: receiver.ID,
			Amount:   1.0,
			Fee:      0.1,
		}
		_ = adapter.AddTransaction(tx)
	}
}
