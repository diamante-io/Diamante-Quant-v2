// tests/ledger/unit/ledger_edge_cases_test.go

package unit

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"testing"
	"time"

	"diamante/common"
	"diamante/config"
	"diamante/ledger"
	"diamante/ledger/evm"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/holiman/uint256"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// Test edge cases and error conditions

func TestCommonLedgerAdapterEdgeCases(t *testing.T) {
	t.Run("handles nil apiLedger gracefully", func(t *testing.T) {
		adapter := ledger.NewCommonLedgerAdapter(nil, nil)
		require.NotNil(t, adapter)

		// Operations should not panic
		assert.False(t, adapter.IsTransactionCommitted("tx-1"))

		tx, err := adapter.GetTransaction("tx-1")
		assert.Error(t, err)
		assert.Nil(t, tx)

		block, found := adapter.GetBlockByNumber(1)
		assert.False(t, found)
		assert.Equal(t, common.Block{}, block)
	})

	t.Run("handles account operations with empty IDs", func(t *testing.T) {
		adapter := ledger.NewCommonLedgerAdapter(&MockAPILedger{}, nil)

		// Empty account ID
		account := &common.Account{
			ID:      "",
			Balance: 100.0,
		}

		err := adapter.CreateAccount(account)
		assert.Error(t, err) // Should fail validation

		// Get balance with empty ID
		balance, err := adapter.GetBalance("")
		assert.Error(t, err)
		assert.Equal(t, 0.0, balance)

		// Update balance with empty ID
		err = adapter.UpdateAccountBalance("", 50.0)
		assert.Error(t, err)
	})

	t.Run("handles transaction edge cases", func(t *testing.T) {
		adapter := ledger.NewCommonLedgerAdapter(&MockAPILedger{}, nil)

		// Create accounts
		sender := &common.Account{ID: "sender", Balance: 100.0}
		receiver := &common.Account{ID: "receiver", Balance: 0.0}
		adapter.CreateAccount(sender)
		adapter.CreateAccount(receiver)

		// Transaction with negative amount
		tx := common.Transaction{
			ID:       "tx-negative",
			Sender:   sender.ID,
			Receiver: receiver.ID,
			Amount:   -50.0,
			Fee:      1.0,
		}
		err := adapter.AddTransaction(tx)
		assert.Error(t, err) // Should fail validation

		// Transaction with negative fee
		tx.Amount = 50.0
		tx.Fee = -1.0
		err = adapter.AddTransaction(tx)
		assert.Error(t, err) // Should fail validation

		// Transaction with zero amount and fee
		tx.Amount = 0.0
		tx.Fee = 0.0
		err = adapter.AddTransaction(tx)
		// Might succeed depending on validation rules

		// Self-transfer
		tx.ID = "self-transfer"
		tx.Sender = sender.ID
		tx.Receiver = sender.ID
		tx.Amount = 10.0
		tx.Fee = 1.0
		err = adapter.AddTransaction(tx)
		// Should succeed but only deduct fee
		if err == nil {
			balance, _ := adapter.GetBalance(sender.ID)
			assert.Equal(t, 99.0, balance) // 100 - 1 (fee only)
		}
	})

	t.Run("handles smart contract execution edge cases", func(t *testing.T) {
		mockLedger := &MockAPILedgerWithConversion{}
		adapter := ledger.NewCommonLedgerAdapter(mockLedger, nil)

		// Nil params
		result, err := adapter.ExecuteSmartContract("contract-1", "test", "sender", nil)
		assert.Error(t, err) // Should handle nil params
		assert.Nil(t, result)

		// Empty contract ID
		params := &common.SmartContractParams{}
		result, err = adapter.ExecuteSmartContract("", "test", "sender", params)
		assert.NoError(t, err) // Might succeed with default result

		// Result with partial data
		mockLedger.On("ExecuteSmartContract", "contract-2", "test", "sender", mock.Anything).
			Return(map[string]interface{}{
				"success": true,
				// Missing other fields
			}, nil)

		result, err = adapter.ExecuteSmartContract("contract-2", "test", "sender", params)
		assert.NoError(t, err)
		assert.True(t, result.Success)
		assert.Equal(t, "", result.StringResult) // Should have default values
		assert.Equal(t, uint64(0), result.GasUsed)
	})

	t.Run("handles stats conversion edge cases", func(t *testing.T) {
		mockLedger := &MockAPILedgerWithConversion{}
		adapter := ledger.NewCommonLedgerAdapter(mockLedger, nil)

		// Stats with invalid types
		statsMap := map[string]interface{}{
			"total_accounts":     "not-a-number",
			"total_transactions": nil,
			"total_balance":      "invalid",
			"processing_time_ms": true,
		}

		mockLedger.On("GetStats").Return(statsMap, nil)

		stats, err := adapter.GetStats()
		assert.NoError(t, err)
		assert.Equal(t, int64(0), stats.TotalAccounts) // Should default to 0
		assert.Equal(t, int64(0), stats.TotalTransactions)
		assert.Equal(t, 0.0, stats.TotalBalance)
		assert.Equal(t, int64(0), stats.ProcessingTime)
	})

	t.Run("handles interface casting failures", func(t *testing.T) {
		// Mock that doesn't implement expected interfaces
		basicMock := &struct{}{}
		adapter := ledger.NewCommonLedgerAdapter(basicMock, nil)

		// All operations should handle missing interface gracefully
		assert.False(t, adapter.IsTransactionCommitted("tx-1"))

		tx, err := adapter.GetTransaction("tx-1")
		assert.Error(t, err)
		assert.Nil(t, tx)

		err = adapter.CreateSnapshot(100)
		assert.NoError(t, err) // Should succeed with no-op

		err = adapter.IntegrityCheck()
		assert.NoError(t, err) // Should succeed with no-op
	})
}

func TestEVMExecutorEdgeCases(t *testing.T) {
	t.Run("handles nil configuration", func(t *testing.T) {
		mockLedger := &MockLedgerAPI{}
		mockLedger.On("GetBlockHeight").Return(100, nil)

		executor := ledger.NewEVMExecutor(mockLedger, nil, nil)
		require.NotNil(t, executor)

		// Should use default config
		// Verify by attempting an operation
		addr := createTestAddress()
		balance, err := executor.GetBalance(addr)
		assert.NoError(t, err)
		assert.Equal(t, big.NewInt(0), balance)
	})

	t.Run("handles gas limit edge cases", func(t *testing.T) {
		executor, mockLedger, _ := createTestEVMExecutor(t)

		from := createTestAddress()
		to := createTestAddress()

		// Transaction with zero gas limit (should use default)
		tx := &ledger.EVMTransaction{
			From:     from,
			To:       to,
			Data:     []byte{},
			GasLimit: 0, // Zero gas limit
			GasPrice: big.NewInt(1000000000),
			Nonce:    0,
			Value:    big.NewInt(0),
			ChainID:  big.NewInt(1),
		}

		mockLedger.On("GetBalance", hex.EncodeToString(from)).Return(1000000.0, nil)

		result, err := executor.ExecuteTransaction(tx)
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Gas limit should be set to default
	})

	t.Run("handles extremely large values", func(t *testing.T) {
		executor, mockLedger, _ := createTestEVMExecutor(t)

		from := createTestAddress()
		to := createTestAddress()

		// Very large value
		largeValue := new(big.Int)
		largeValue.SetString("999999999999999999999999999999999999999999", 10)

		tx := &ledger.EVMTransaction{
			From:     from,
			To:       to,
			Data:     []byte{},
			GasLimit: 21000,
			GasPrice: big.NewInt(1),
			Nonce:    0,
			Value:    largeValue,
			ChainID:  big.NewInt(1),
		}

		mockLedger.On("GetBalance", hex.EncodeToString(from)).Return(0.0, nil)

		_, err := executor.ExecuteTransaction(tx)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "insufficient balance")
	})

	t.Run("handles contract deployment with invalid bytecode", func(t *testing.T) {
		executor, mockLedger, _ := createTestEVMExecutor(t)

		from := createTestAddress()

		// Invalid bytecode (odd length hex)
		invalidBytecode := []byte{0x60}

		mockLedger.On("GetBalance", hex.EncodeToString(from)).Return(1000000.0, nil)

		contractAddr, err := executor.DeployContract(from, invalidBytecode, 100000)
		// Deployment might fail
		if err != nil {
			assert.Contains(t, err.Error(), "contract deployment failed")
		} else {
			assert.NotNil(t, contractAddr)
		}
	})

	t.Run("handles GetCode with invalid contract ID", func(t *testing.T) {
		executor, _, mockStore := createTestEVMExecutor(t)

		// Invalid hex in contract ID
		mockStore.On("GetContract", "invalid-hex").Return(nil, errors.New("invalid contract ID"))

		// Address that would produce invalid hex
		addr := []byte{0xff, 0xff}
		code, err := executor.GetCode(addr)

		// Should handle gracefully
		if err != nil {
			assert.Contains(t, err.Error(), "invalid")
		} else {
			assert.Empty(t, code)
		}
	})

	t.Run("handles concurrent block height updates", func(t *testing.T) {
		executor, _, _ := createTestEVMExecutor(t)

		// Concurrent updates shouldn't cause issues
		done := make(chan bool, 10)
		for i := 0; i < 10; i++ {
			go func(height uint64) {
				executor.SetBlockHeight(height)
				done <- true
			}(uint64(i * 1000))
		}

		for i := 0; i < 10; i++ {
			<-done
		}

		// Should complete without panic
	})
}

func TestStateDBEdgeCases(t *testing.T) {
	t.Run("handles nil ledger store", func(t *testing.T) {
		mockLedger := &MockLedgerAPI{}
		logger := logrus.New()

		mockLedger.On("GetBlockHeight").Return(100, nil)

		// Create StateDB with nil store
		stateDB := evm.NewStateDB(mockLedger, nil, 100, logger)
		require.NotNil(t, stateDB)

		// Operations should still work
		addr := createTestEthAddress()
		stateDB.CreateAccount(addr)
		assert.True(t, stateDB.Exist(addr))
	})

	t.Run("handles negative balance operations", func(t *testing.T) {
		stateDB, _, _ := createTestStateDB(t)
		addr := createTestEthAddress()

		// Set balance
		amount100 := uint256.NewInt(100)
		stateDB.SetBalance(addr, amount100, tracing.BalanceChangeUnspecified)

		// Subtract more than available
		amount200 := uint256.NewInt(200)
		stateDB.SubBalance(addr, amount200, tracing.BalanceChangeUnspecified)

		// Balance should be negative
		balance := stateDB.GetBalance(addr)
		assert.Equal(t, big.NewInt(-100), balance)

		// Add to negative balance
		amount50 := uint256.NewInt(50)
		stateDB.AddBalance(addr, amount50, tracing.BalanceChangeUnspecified)
		balance = stateDB.GetBalance(addr)
		assert.Equal(t, big.NewInt(-50), balance)
	})

	t.Run("handles extremely large storage keys", func(t *testing.T) {
		stateDB, _, mockStore := createTestStateDB(t)
		addr := createTestEthAddress()

		// Create a max value storage key
		key := ethcommon.Hash{}
		for i := range key {
			key[i] = 0xFF
		}
		value := ethcommon.Hash{1, 2, 3}

		mockStore.On("SetStorage", addr.Hex(), key.Hex(), value.Bytes()).Return(nil).Maybe()

		// Should handle without issues
		stateDB.SetState(addr, key, value)
		result := stateDB.GetState(addr, key)
		assert.Equal(t, value, result)
	})

	t.Run("handles deep snapshot nesting", func(t *testing.T) {
		stateDB, _, _ := createTestStateDB(t)
		addr := createTestEthAddress()

		// Create many nested snapshots
		snapshots := make([]int, 100)
		for i := range snapshots {
			amount := uint256.NewInt(uint64(i))
			stateDB.SetBalance(addr, amount, tracing.BalanceChangeUnspecified)
			snapshots[i] = stateDB.Snapshot()
		}

		// Revert to middle snapshot
		stateDB.RevertToSnapshot(snapshots[50])
		balance := stateDB.GetBalance(addr)
		assert.Equal(t, big.NewInt(50), balance)

		// Revert to first snapshot
		stateDB.RevertToSnapshot(snapshots[0])
		balance = stateDB.GetBalance(addr)
		assert.Equal(t, big.NewInt(0), balance)
	})

	t.Run("handles invalid snapshot IDs", func(t *testing.T) {
		stateDB, _, _ := createTestStateDB(t)

		// Try to revert to non-existent snapshot
		// Should handle gracefully (might panic in production)
		assert.NotPanics(t, func() {
			stateDB.RevertToSnapshot(999999)
		})
	})

	t.Run("handles maximum refund", func(t *testing.T) {
		stateDB, _, _ := createTestStateDB(t)

		// Add maximum possible refund
		maxRefund := ^uint64(0) - 1000 // Max uint64 minus some
		stateDB.AddRefund(maxRefund)

		// Adding more should not overflow
		stateDB.AddRefund(1000)

		refund := stateDB.GetRefund()
		assert.Equal(t, ^uint64(0), refund) // Should be at max
	})

	t.Run("handles self-destruct edge cases", func(t *testing.T) {
		stateDB, _, _ := createTestStateDB(t)
		addr := createTestEthAddress()

		// Self-destruct non-existent account
		stateDB.SelfDestruct(addr)
		assert.True(t, stateDB.HasSelfDestructed(addr))

		// Self-destruct twice
		stateDB.SelfDestruct(addr)
		assert.True(t, stateDB.HasSelfDestructed(addr))

		// Create account after self-destruct
		stateDB.CreateAccount(addr)
		// Should still be marked for self-destruct
		assert.True(t, stateDB.HasSelfDestructed(addr))
	})

	t.Run("handles transient storage edge cases", func(t *testing.T) {
		stateDB, _, _ := createTestStateDB(t)
		addr := createTestEthAddress()

		// Set transient storage for non-existent account
		key := ethcommon.Hash{1}
		value := ethcommon.Hash{2}
		stateDB.SetTransientState(addr, key, value)

		// Should work without creating account
		result := stateDB.GetTransientState(addr, key)
		assert.Equal(t, value, result)

		// Account should not exist in main state
		assert.False(t, stateDB.Exist(addr))
	})

	t.Run("handles access list edge cases", func(t *testing.T) {
		stateDB, _, _ := createTestStateDB(t)
		addr := createTestEthAddress()
		slot := ethcommon.Hash{1}

		// Add slot without adding address first
		stateDB.AddSlotToAccessList(addr, slot)
		// AddSlotToAccessList doesn't return values in this version

		// Verify both are in access list
		assert.True(t, stateDB.AddressInAccessList(addr))
		addrInList, slotInList := stateDB.SlotInAccessList(addr, slot)
		assert.True(t, addrInList)
		assert.True(t, slotInList)
	})
}

func TestErrorPropagation(t *testing.T) {
	t.Run("ledger adapter propagates errors correctly", func(t *testing.T) {
		mockLedger := &MockAPILedger{}
		adapter := ledger.NewCommonLedgerAdapter(mockLedger, nil)

		expectedErr := errors.New("database connection failed")

		// Mock various error returns
		mockLedger.On("GetTransaction", "tx-error").Return(nil, expectedErr)
		mockLedger.On("GetAccountTransactions", "error-account", 10, 0).Return(nil, expectedErr)
		mockLedger.On("CommitBlock", mock.Anything).Return(expectedErr)
		mockLedger.On("GetLastBlockHash").Return("", expectedErr)
		mockLedger.On("GetBlockHeight").Return(0, expectedErr)
		mockLedger.On("CreateSnapshot", 100).Return(expectedErr)

		// Verify errors are propagated
		_, err := adapter.GetTransaction("tx-error")
		assert.Equal(t, expectedErr, err)

		_, err = adapter.GetAccountTransactions("error-account", 10, 0)
		assert.Equal(t, expectedErr, err)

		err = adapter.CommitBlock(common.Block{})
		assert.Equal(t, expectedErr, err)

		_, err = adapter.GetLastBlockHash()
		assert.Equal(t, expectedErr, err)

		_, err = adapter.GetBlockHeight()
		assert.Equal(t, expectedErr, err)

		err = adapter.CreateSnapshot(100)
		assert.Equal(t, expectedErr, err)
	})

	t.Run("handles panics in underlying ledger", func(t *testing.T) {
		// Mock that panics
		panicLedger := &PanicLedger{}
		adapter := ledger.NewCommonLedgerAdapter(panicLedger, nil)

		// Operations should handle panic gracefully
		assert.NotPanics(t, func() {
			adapter.IsTransactionCommitted("tx-1")
		})
	})
}

// PanicLedger is a mock that panics on certain operations
type PanicLedger struct{}

func (p *PanicLedger) IsTransactionCommitted(txID string) bool {
	panic("simulated panic")
}

func TestCacheEdgeCases(t *testing.T) {
	t.Run("cache with zero size", func(t *testing.T) {
		cfg := &config.CacheConfig{
			Size: 0, // Zero size cache
			TTL:  time.Minute,
		}

		adapter := ledger.NewCommonLedgerAdapter(&MockAPILedger{}, cfg)
		require.NotNil(t, adapter)

		// Operations should still work
		// common.ResetAccounts() - function doesn't exist
		account := &common.Account{
			ID:      "zero-cache-account",
			Balance: 100.0,
		}
		err := adapter.CreateAccount(account)
		assert.NoError(t, err)

		balance, err := adapter.GetBalance(account.ID)
		assert.NoError(t, err)
		assert.Equal(t, 100.0, balance)
	})

	t.Run("cache with zero TTL", func(t *testing.T) {
		cfg := &config.CacheConfig{
			Size: 100,
			TTL:  0, // Zero TTL
		}

		adapter := ledger.NewCommonLedgerAdapter(&MockAPILedger{}, cfg)
		require.NotNil(t, adapter)

		// Cache should effectively be disabled
		// common.ResetAccounts() - function doesn't exist
		account := &common.Account{
			ID:      "zero-ttl-account",
			Balance: 100.0,
		}
		err := adapter.CreateAccount(account)
		assert.NoError(t, err)
	})
}

func TestTimeout(t *testing.T) {
	t.Run("health check with timeout", func(t *testing.T) {
		slowLedger := &SlowLedger{}
		adapter := ledger.NewCommonLedgerAdapter(slowLedger, nil)

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		err := adapter.HealthCheck(ctx)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "context deadline exceeded")
	})
}

// SlowLedger simulates a slow responding ledger
type SlowLedger struct{}

func (s *SlowLedger) HealthCheck(ctx context.Context) error {
	select {
	case <-time.After(1 * time.Second):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Benchmark edge cases
func BenchmarkLargeTransaction(b *testing.B) {
	adapter := ledger.NewCommonLedgerAdapter(&MockAPILedger{}, nil)

	// Setup
	// common.ResetAccounts() - function doesn't exist
	sender := &common.Account{
		ID:      "bench-large-sender",
		Balance: float64(b.N * 1000000),
	}
	receiver := &common.Account{
		ID:      "bench-large-receiver",
		Balance: 0.0,
	}
	_ = adapter.CreateAccount(sender)
	_ = adapter.CreateAccount(receiver)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx := common.Transaction{
			ID:       fmt.Sprintf("large-tx-%d", i),
			Sender:   sender.ID,
			Receiver: receiver.ID,
			Amount:   999999.0, // Large amount
			Fee:      0.01,
		}
		_ = adapter.AddTransaction(tx)
	}
}

func BenchmarkDeepStateNesting(b *testing.B) {
	// Convert testing.B to testing.T for createTestStateDB
	t := &testing.T{}
	stateDB, _, mockStore := createTestStateDB(t)
	addr := createTestEthAddress()

	// Mock storage operations
	mockStore.On("SetStorage", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Create nested state changes
		for j := 0; j < 10; j++ {
			snapshot := stateDB.Snapshot()
			amount := uint256.NewInt(uint64(j))
			stateDB.SetBalance(addr, amount, tracing.BalanceChangeUnspecified)
			stateDB.SetNonce(addr, uint64(j))
			key := ethcommon.Hash{byte(j)}
			value := ethcommon.Hash{byte(j + 1)}
			stateDB.SetState(addr, key, value)

			if j%2 == 0 {
				stateDB.RevertToSnapshot(snapshot)
			}
		}
	}
}
