// Package transaction provides tests for typed transaction pool
package transaction

import (
	"testing"
	"time"

	"diamante/transaction"
	"diamante/types"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createTestPool() *transaction.TypedPool {
	maxPoolSize := 100
	txTimeout := 30 * time.Second
	minFee := 0.001
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)

	return transaction.NewTypedPool(maxPoolSize, txTimeout, minFee, logger)
}

func createTestTransaction(from string, nonce uint64) *types.TypedTransaction {
	return &types.TypedTransaction{
		Type:      types.TransactionTypeTransfer,
		ID:        generateTestTxID(from, nonce),
		From:      from,
		To:        "0xrecipient",
		Value:     1000000,
		GasLimit:  21000,
		GasPrice:  1000000000,
		Nonce:     nonce,
		Timestamp: time.Now().Unix(),
		Status:    types.TransactionStatusPending,
		Priority:  types.TransactionPriorityNormal,
	}
}

func generateTestTxID(from string, nonce uint64) string {
	return from + "-" + string(rune(nonce))
}

func TestTypedPoolBasicOperations(t *testing.T) {
	pool := createTestPool()

	t.Run("AddTransaction", func(t *testing.T) {
		tx := createTestTransaction("0xsender1", 0)

		err := pool.AddTransaction(tx)
		assert.NoError(t, err)

		// Verify transaction is in pool
		retrieved, exists := pool.GetTransaction(tx.ID)
		assert.True(t, exists)
		assert.Equal(t, tx.ID, retrieved.ID)
		assert.Equal(t, tx.From, retrieved.From)
	})

	t.Run("DuplicateTransaction", func(t *testing.T) {
		tx := createTestTransaction("0xsender2", 0)

		err := pool.AddTransaction(tx)
		assert.NoError(t, err)

		// Try to add same transaction again
		err = pool.AddTransaction(tx)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "already in pool")
	})

	t.Run("InvalidTransaction", func(t *testing.T) {
		// Transaction with zero gas limit
		tx := createTestTransaction("0xsender3", 0)
		tx.GasLimit = 0

		err := pool.AddTransaction(tx)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "gas limit is zero")

		// Transaction with low gas price
		tx2 := createTestTransaction("0xsender3", 1)
		tx2.GasPrice = 100

		err = pool.AddTransaction(tx2)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "gas price")
	})

	t.Run("RemoveTransaction", func(t *testing.T) {
		tx := createTestTransaction("0xsender4", 0)

		err := pool.AddTransaction(tx)
		require.NoError(t, err)

		// Remove transaction
		removed := pool.RemoveTransaction(tx.ID)
		assert.True(t, removed)

		// Verify it's gone
		_, exists := pool.GetTransaction(tx.ID)
		assert.False(t, exists)

		// Try to remove non-existent transaction
		removed = pool.RemoveTransaction("non-existent")
		assert.False(t, removed)
	})
}

func TestTypedPoolPriority(t *testing.T) {
	pool := createTestPool()

	// Add transactions with different priorities
	txLow := createTestTransaction("0xlow", 0)
	txLow.Priority = types.TransactionPriorityLow
	txLow.GasPrice = 1000000000

	txNormal := createTestTransaction("0xnormal", 0)
	txNormal.Priority = types.TransactionPriorityNormal
	txNormal.GasPrice = 1000000000

	txHigh := createTestTransaction("0xhigh", 0)
	txHigh.Priority = types.TransactionPriorityHigh
	txHigh.GasPrice = 1000000000

	txUrgent := createTestTransaction("0xurgent", 0)
	txUrgent.Priority = types.TransactionPriorityUrgent
	txUrgent.GasPrice = 1000000000

	// Add in random order
	require.NoError(t, pool.AddTransaction(txNormal))
	require.NoError(t, pool.AddTransaction(txLow))
	require.NoError(t, pool.AddTransaction(txUrgent))
	require.NoError(t, pool.AddTransaction(txHigh))

	// Get pending transactions
	pending := pool.GetPendingTransactions(4)

	// Should be ordered by priority
	assert.Equal(t, 4, len(pending))
	assert.Equal(t, txUrgent.ID, pending[0].ID)
	assert.Equal(t, txHigh.ID, pending[1].ID)
	assert.Equal(t, txNormal.ID, pending[2].ID)
	assert.Equal(t, txLow.ID, pending[3].ID)
}

func TestTypedPoolGasPrice(t *testing.T) {
	pool := createTestPool()

	// Add transactions with different gas prices
	tx1 := createTestTransaction("0xsender1", 0)
	tx1.GasPrice = 1000000000 // 1 gwei

	tx2 := createTestTransaction("0xsender2", 0)
	tx2.GasPrice = 2000000000 // 2 gwei

	tx3 := createTestTransaction("0xsender3", 0)
	tx3.GasPrice = 3000000000 // 3 gwei

	require.NoError(t, pool.AddTransaction(tx1))
	require.NoError(t, pool.AddTransaction(tx2))
	require.NoError(t, pool.AddTransaction(tx3))

	// Get pending - should be ordered by gas price
	pending := pool.GetPendingTransactions(3)

	assert.Equal(t, tx3.ID, pending[0].ID) // Highest gas price
	assert.Equal(t, tx2.ID, pending[1].ID)
	assert.Equal(t, tx1.ID, pending[2].ID) // Lowest gas price
}

func TestTypedPoolNonceHandling(t *testing.T) {
	pool := createTestPool()
	sender := "0xsender"

	// Add transactions with sequential nonces
	tx0 := createTestTransaction(sender, 0)
	tx1 := createTestTransaction(sender, 1)
	tx2 := createTestTransaction(sender, 2)

	require.NoError(t, pool.AddTransaction(tx0))
	require.NoError(t, pool.AddTransaction(tx1))
	require.NoError(t, pool.AddTransaction(tx2))

	// Update nonce - should remove outdated transactions
	pool.UpdateNonce(sender, 2)

	// tx0 and tx1 should be removed
	_, exists0 := pool.GetTransaction(tx0.ID)
	assert.False(t, exists0)

	_, exists1 := pool.GetTransaction(tx1.ID)
	assert.False(t, exists1)

	// tx2 should still exist
	_, exists2 := pool.GetTransaction(tx2.ID)
	assert.True(t, exists2)

	// Metrics should reflect dropped transactions
	metrics := pool.GetMetrics()
	assert.Equal(t, uint64(2), metrics.TotalDropped)
}

func TestTypedPoolEviction(t *testing.T) {
	// Create small pool for testing eviction
	maxPoolSize := 3
	txTimeout := 30 * time.Second
	minFee := 0.001
	pool := transaction.NewTypedPool(maxPoolSize, txTimeout, minFee, nil)

	// Fill pool
	tx1 := createTestTransaction("0xsender1", 0)
	tx1.GasPrice = 1000000000

	tx2 := createTestTransaction("0xsender2", 0)
	tx2.GasPrice = 2000000000

	tx3 := createTestTransaction("0xsender3", 0)
	tx3.GasPrice = 3000000000

	require.NoError(t, pool.AddTransaction(tx1))
	require.NoError(t, pool.AddTransaction(tx2))
	require.NoError(t, pool.AddTransaction(tx3))

	// Add higher priority transaction - should evict lowest
	tx4 := createTestTransaction("0xsender4", 0)
	tx4.GasPrice = 4000000000

	err := pool.AddTransaction(tx4)
	assert.NoError(t, err)

	// tx1 (lowest gas price) should be evicted
	_, exists1 := pool.GetTransaction(tx1.ID)
	assert.False(t, exists1)

	// Others should still exist
	_, exists2 := pool.GetTransaction(tx2.ID)
	assert.True(t, exists2)

	_, exists3 := pool.GetTransaction(tx3.ID)
	assert.True(t, exists3)

	_, exists4 := pool.GetTransaction(tx4.ID)
	assert.True(t, exists4)
}

func TestTypedPoolTransactionTypes(t *testing.T) {
	pool := createTestPool()

	t.Run("ContractDeploy", func(t *testing.T) {
		tx := &types.TypedTransaction{
			Type:     types.TransactionTypeContractDeploy,
			ID:       "deploy-1",
			From:     "0xdeployer",
			Value:    0,
			GasLimit: 3000000,
			GasPrice: 1000000000,
			Nonce:    0,
			Data: &types.TypedTransactionData{
				ContractDeploy: &types.ContractDeployData{
					Runtime:  "EVM",
					ByteCode: []byte{0x60, 0x60, 0x60, 0x40}, // Sample bytecode
				},
			},
			Timestamp: time.Now().Unix(),
			Status:    types.TransactionStatusPending,
			Priority:  types.TransactionPriorityNormal,
		}

		err := pool.AddTransaction(tx)
		assert.NoError(t, err)

		retrieved, exists := pool.GetTransaction(tx.ID)
		assert.True(t, exists)
		assert.Equal(t, types.TransactionTypeContractDeploy, retrieved.Type)
		assert.NotNil(t, retrieved.Data.ContractDeploy)
	})

	t.Run("ContractCall", func(t *testing.T) {
		tx := &types.TypedTransaction{
			Type:     types.TransactionTypeContractCall,
			ID:       "call-1",
			From:     "0xcaller",
			To:       "0xcontract",
			Value:    0,
			GasLimit: 100000,
			GasPrice: 1000000000,
			Nonce:    0,
			Data: &types.TypedTransactionData{
				ContractCall: &types.ContractCallData{
					ContractAddress: "0xcontract",
					Method:          "transfer",
					Arguments: []*types.ContractArgument{
						{
							Name:  "recipient",
							Type:  types.ValueTypeString,
							Value: types.StringToValue("0xrecipient"),
						},
						{
							Name:  "amount",
							Type:  types.ValueTypeUint64,
							Value: types.Uint64ToValue(1000000),
						},
					},
				},
			},
			Timestamp: time.Now().Unix(),
			Status:    types.TransactionStatusPending,
			Priority:  types.TransactionPriorityNormal,
		}

		err := pool.AddTransaction(tx)
		assert.NoError(t, err)

		retrieved, exists := pool.GetTransaction(tx.ID)
		assert.True(t, exists)
		assert.Equal(t, types.TransactionTypeContractCall, retrieved.Type)
		assert.NotNil(t, retrieved.Data.ContractCall)
		assert.Equal(t, 2, len(retrieved.Data.ContractCall.Arguments))
	})
}

func TestTypedPoolMetrics(t *testing.T) {
	pool := createTestPool()

	// Add some transactions
	for i := 0; i < 5; i++ {
		tx := createTestTransaction("0xsender", uint64(i))
		tx.GasPrice = uint64(1000000000 * (i + 1)) // Varying gas prices
		require.NoError(t, pool.AddTransaction(tx))
	}

	metrics := pool.GetMetrics()

	assert.Equal(t, uint64(5), metrics.TotalReceived)
	assert.Equal(t, uint64(5), metrics.PoolSize)
	assert.Equal(t, uint64(5), metrics.QueuedCount)
	assert.Greater(t, metrics.AvgGasUsed, uint64(0))
}

func TestConcurrentPoolOperations(t *testing.T) {
	pool := createTestPool()
	done := make(chan bool)

	// Concurrent writers
	for i := 0; i < 10; i++ {
		go func(id int) {
			for j := 0; j < 10; j++ {
				tx := createTestTransaction("0xsender"+string(rune(id)), uint64(j))
				pool.AddTransaction(tx)
			}
			done <- true
		}(i)
	}

	// Concurrent readers
	for i := 0; i < 5; i++ {
		go func() {
			for j := 0; j < 20; j++ {
				pool.GetPendingTransactions(10)
				time.Sleep(1 * time.Millisecond)
			}
			done <- true
		}()
	}

	// Wait for completion
	for i := 0; i < 15; i++ {
		<-done
	}

	// Pool should still be consistent
	metrics := pool.GetMetrics()
	assert.True(t, metrics.TotalReceived > 0)
	assert.True(t, metrics.PoolSize > 0)
}

func BenchmarkTypedPool(b *testing.B) {
	pool := createTestPool()

	b.Run("AddTransaction", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			tx := createTestTransaction("0xbench", uint64(i))
			pool.AddTransaction(tx)
		}
	})

	// Pre-populate for other benchmarks
	for i := 0; i < 1000; i++ {
		tx := createTestTransaction("0xprepop", uint64(i))
		pool.AddTransaction(tx)
	}

	b.Run("GetTransaction", func(b *testing.B) {
		txID := "0xprepop-" + string(rune(500))
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			pool.GetTransaction(txID)
		}
	})

	b.Run("GetPendingTransactions", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			pool.GetPendingTransactions(100)
		}
	})
}
