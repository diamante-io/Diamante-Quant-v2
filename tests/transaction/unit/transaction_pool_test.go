package transaction_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"diamante/common"
	"diamante/transaction"
)

// TestTransactionPoolBasicOperations tests basic pool operations
func TestTransactionPoolBasicOperations(t *testing.T) {
	pool := createTestTransactionPool(t)

	t.Run("add_and_get_transaction", func(t *testing.T) {
		tx := createTestTransaction(1, "sender1", "receiver1", 100)

		// Add transaction to pool
		err := pool.AddTransaction(tx)
		if err != nil {
			t.Fatalf("Failed to add transaction: %v", err)
		}

		// Verify transaction is in pool
		poolTx, err := pool.GetTransaction(tx.ID)
		if err != nil {
			t.Fatalf("Failed to get transaction: %v", err)
		}
		if poolTx == nil {
			t.Fatalf("Transaction not found in pool")
		}

		if poolTx.ID != tx.ID {
			t.Errorf("Transaction ID mismatch: got %s, want %s", poolTx.ID, tx.ID)
		}
	})

	t.Run("remove_transaction", func(t *testing.T) {
		tx := createTestTransaction(2, "sender2", "receiver2", 200)

		// Add transaction
		err := pool.AddTransaction(tx)
		if err != nil {
			t.Fatalf("Failed to add transaction: %v", err)
		}

		// Verify it exists
		poolTx, _ := pool.GetTransaction(tx.ID)
		if poolTx == nil {
			t.Fatalf("Transaction should exist before removal")
		}

		// Remove transaction
		err = pool.RemoveTransaction(tx.ID)
		if err != nil {
			t.Errorf("Transaction removal failed: %v", err)
		}

		// Verify it's removed
		poolTx, _ = pool.GetTransaction(tx.ID)
		if poolTx != nil {
			t.Errorf("Transaction should not exist after removal")
		}
	})

	t.Run("pool_size_management", func(t *testing.T) {
		// Get max pool size
		maxSize := pool.GetMaxPoolSize()
		if maxSize <= 0 {
			t.Errorf("Invalid max pool size: %d", maxSize)
		}

		// Add transactions up to limit
		for i := 0; i < maxSize+10; i++ {
			tx := createTestTransaction(i+100, fmt.Sprintf("sender%d", i),
				fmt.Sprintf("receiver%d", i), float64(i))
			err := pool.AddTransaction(tx)

			// Should accept transactions up to limit
			if i < maxSize && err != nil {
				t.Errorf("Should accept transaction %d: %v", i, err)
			}
		}

		// Pool should not exceed max size
		currentSize := pool.PoolSize()
		if currentSize > maxSize {
			t.Errorf("Pool size %d exceeds maximum %d", currentSize, maxSize)
		}
	})
}

// TestTransactionPoolValidation tests transaction validation
func TestTransactionPoolValidation(t *testing.T) {
	pool := createTestTransactionPool(t)

	t.Run("reject_invalid_transactions", func(t *testing.T) {
		testCases := []struct {
			name        string
			tx          common.Transaction
			expectError bool
		}{
			{
				name:        "valid_transaction",
				tx:          createTestTransaction(1, "sender1", "receiver1", 100),
				expectError: false,
			},
			{
				name: "missing_sender",
				tx: common.Transaction{
					ID:       "tx_missing_sender",
					Receiver: "receiver1",
					Amount:   100,
					Nonce:    1,
				},
				expectError: true,
			},
			{
				name: "missing_recipient",
				tx: common.Transaction{
					ID:     "tx_missing_recipient",
					Sender: "sender1",
					Amount: 100,
					Nonce:  1,
				},
				expectError: true,
			},
			{
				name: "negative_amount",
				tx: common.Transaction{
					ID:       "tx_negative_amount",
					Sender:   "sender1",
					Receiver: "receiver1",
					Amount:   -100,
					Nonce:    1,
				},
				expectError: true,
			},
			{
				name: "zero_amount",
				tx: common.Transaction{
					ID:       "tx_zero_amount",
					Sender:   "sender1",
					Receiver: "receiver1",
					Amount:   0,
					Nonce:    1,
				},
				expectError: true,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				err := pool.AddTransaction(tc.tx)
				if tc.expectError && err == nil {
					t.Errorf("Expected error for %s but got none", tc.name)
				}
				if !tc.expectError && err != nil {
					t.Errorf("Unexpected error for %s: %v", tc.name, err)
				}
			})
		}
	})

	t.Run("reject_duplicate_transactions", func(t *testing.T) {
		tx := createTestTransaction(50, "sender50", "receiver50", 500)

		// Add transaction first time
		err := pool.AddTransaction(tx)
		if err != nil {
			t.Fatalf("Failed to add transaction first time: %v", err)
		}

		// Try to add same transaction again
		err = pool.AddTransaction(tx)
		if err == nil {
			t.Errorf("Should reject duplicate transaction")
		}
	})
}

// TestTransactionPoolConcurrency tests concurrent pool operations
func TestTransactionPoolConcurrency(t *testing.T) {
	pool := createTestTransactionPool(t)

	t.Run("concurrent_additions", func(t *testing.T) {
		numGoroutines := 10
		numTxPerGoroutine := 50
		var wg sync.WaitGroup
		errors := make(chan error, numGoroutines*numTxPerGoroutine)

		// Launch concurrent writers
		for g := 0; g < numGoroutines; g++ {
			wg.Add(1)
			go func(goroutineID int) {
				defer wg.Done()
				for i := 0; i < numTxPerGoroutine; i++ {
					tx := createTestTransaction(
						goroutineID*1000+i,
						fmt.Sprintf("sender_%d_%d", goroutineID, i),
						fmt.Sprintf("receiver_%d_%d", goroutineID, i),
						float64(i+1),
					)
					err := pool.AddTransaction(tx)
					if err != nil {
						errors <- fmt.Errorf("goroutine %d, tx %d: %v", goroutineID, i, err)
					}
				}
			}(g)
		}

		wg.Wait()
		close(errors)

		// Check for errors
		var errorList []error
		for err := range errors {
			errorList = append(errorList, err)
		}

		if len(errorList) > 0 {
			t.Errorf("Encountered %d errors in concurrent additions:", len(errorList))
			for i, err := range errorList {
				if i < 5 { // Show first 5 errors
					t.Errorf("  Error %d: %v", i+1, err)
				}
			}
		}

		// Verify pool integrity
		size := pool.PoolSize()
		t.Logf("Final pool size: %d", size)
		if size == 0 {
			t.Errorf("Pool should contain transactions after concurrent additions")
		}
	})

	t.Run("concurrent_reads_writes", func(t *testing.T) {
		// Pre-populate pool
		for i := 0; i < 20; i++ {
			tx := createTestTransaction(i+2000, fmt.Sprintf("pre_sender_%d", i),
				fmt.Sprintf("pre_receiver_%d", i), float64(i*10))
			pool.AddTransaction(tx)
		}

		numReaders := 5
		numWriters := 3
		duration := 2 * time.Second

		ctx, cancel := context.WithTimeout(context.Background(), duration)
		defer cancel()

		var wg sync.WaitGroup
		errors := make(chan error, 100)

		// Launch readers
		for r := 0; r < numReaders; r++ {
			wg.Add(1)
			go func(readerID int) {
				defer wg.Done()
				for {
					select {
					case <-ctx.Done():
						return
					default:
						// Read operations
						_ = pool.PoolSize()
						_ = pool.GetAllTransactions()
						time.Sleep(10 * time.Millisecond)
					}
				}
			}(r)
		}

		// Launch writers
		for w := 0; w < numWriters; w++ {
			wg.Add(1)
			go func(writerID int) {
				defer wg.Done()
				counter := 0
				for {
					select {
					case <-ctx.Done():
						return
					default:
						tx := createTestTransaction(
							writerID*10000+counter,
							fmt.Sprintf("writer_%d_sender_%d", writerID, counter),
							fmt.Sprintf("writer_%d_receiver_%d", writerID, counter),
							float64(counter),
						)
						err := pool.AddTransaction(tx)
						if err != nil {
							errors <- fmt.Errorf("writer %d: %v", writerID, err)
						}
						counter++
						time.Sleep(20 * time.Millisecond)
					}
				}
			}(w)
		}

		wg.Wait()
		close(errors)

		// Check for errors
		var errorList []error
		for err := range errors {
			errorList = append(errorList, err)
		}

		if len(errorList) > 5 { // Allow some errors due to pool limits
			t.Errorf("Too many errors in concurrent reads/writes: %d", len(errorList))
		}
	})
}

// TestTransactionPoolPerformance tests pool performance
func TestTransactionPoolPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}

	pool := createTestTransactionPool(t)

	t.Run("high_throughput_additions", func(t *testing.T) {
		numTransactions := 10000
		start := time.Now()

		for i := 0; i < numTransactions; i++ {
			tx := createTestTransaction(i, fmt.Sprintf("perf_sender_%d", i%100),
				fmt.Sprintf("perf_receiver_%d", i%50), float64(i))
			err := pool.AddTransaction(tx)
			if err != nil {
				// Expected due to pool limits or validation
				continue
			}
		}

		duration := time.Since(start)
		tps := float64(numTransactions) / duration.Seconds()

		t.Logf("Performance stats:")
		t.Logf("  Added %d transactions in %v", numTransactions, duration)
		t.Logf("  TPS: %.2f", tps)
		t.Logf("  Final pool size: %d", pool.PoolSize())

		if tps < 1000 {
			t.Errorf("Transaction pool TPS too low: %.2f", tps)
		}
	})
}

// TestTransactionPoolCleanup tests cleanup and maintenance operations
func TestTransactionPoolCleanup(t *testing.T) {
	pool := createTestTransactionPool(t)

	t.Run("expired_transaction_cleanup", func(t *testing.T) {
		// Add transactions with different timestamps
		oldTx := createTestTransaction(1000, "old_sender", "old_receiver", 100)
		oldTx.Timestamp = time.Now().Add(-2 * time.Hour).Unix()

		recentTx := createTestTransaction(1001, "recent_sender", "recent_receiver", 100)
		recentTx.Timestamp = time.Now().Unix()

		err := pool.AddTransaction(oldTx)
		if err != nil {
			t.Fatalf("Failed to add old transaction: %v", err)
		}

		err = pool.AddTransaction(recentTx)
		if err != nil {
			t.Fatalf("Failed to add recent transaction: %v", err)
		}

		initialSize := pool.PoolSize()

		// Trigger cleanup
		removed := pool.RemoveExpiredTransactions(time.Hour)

		finalSize := pool.PoolSize()

		// Should have removed old transaction
		if removed == 0 {
			t.Errorf("Cleanup didn't remove expired transactions")
		}
		if finalSize >= initialSize {
			t.Errorf("Pool size didn't decrease after cleanup: %d -> %d",
				initialSize, finalSize)
		}

		// Recent transaction should still exist
		poolTx, _ := pool.GetTransaction(recentTx.ID)
		if poolTx == nil {
			t.Errorf("Recent transaction was incorrectly removed")
		}

		// Old transaction should be removed
		poolTx, _ = pool.GetTransaction(oldTx.ID)
		if poolTx != nil {
			t.Errorf("Old transaction was not removed")
		}
	})
}

// Helper functions

func createTestTransactionPool(t *testing.T) *transaction.TransactionPool {
	maxPoolSize := 1000
	txTimeout := 30 * time.Second
	minFee := 0.001
	maxFee := 100.0
	expirationDuration := 1 * time.Hour
	pool := transaction.NewTransactionPool(maxPoolSize, txTimeout, minFee, maxFee, expirationDuration)
	return pool
}

func createTestTransaction(id int, from, to string, amount float64) common.Transaction {
	return common.Transaction{
		ID:        fmt.Sprintf("tx_%d", id),
		Sender:    from,
		Receiver:  to,
		Amount:    amount,
		Timestamp: time.Now().Unix(),
		Signature: []byte("test-signature"),
		Nonce:     id,
		Fee:       0.001,
	}
}
