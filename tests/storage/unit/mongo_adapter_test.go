package storage_test

import (
	"fmt"
	"testing"
	"time"

	"diamante/common"
	"diamante/storage"
	"github.com/sirupsen/logrus"
)

// TestMongoAdapterBasicOperations tests basic CRUD operations
func TestMongoAdapterBasicOperations(t *testing.T) {
	adapter := createTestMongoAdapter(t)
	defer cleanupTestAdapter(t, adapter)

	// ctx := context.Background() // unused

	t.Run("store_and_retrieve_block", func(t *testing.T) {
		// Create test block
		block := &common.Block{
			Number:       1,
			Hash:         "test_hash_123",
			PreviousHash: "prev_hash_123",
			Timestamp:    time.Now().Unix(),
			Data:         []byte("test block data"),
			Transactions: []common.Transaction{
				*createTestTransaction(1),
				*createTestTransaction(2),
			},
		}

		// Store block
		err := adapter.SaveBlock(block)
		if err != nil {
			t.Fatalf("Failed to store block: %v", err)
		}

		// Retrieve block
		retrievedBlock, err := adapter.GetBlock(uint64(block.Number))
		if err != nil {
			t.Fatalf("Failed to retrieve block: %v", err)
		}

		// Verify block data
		if retrievedBlock.Hash != block.Hash {
			t.Errorf("Block hash mismatch: got %s, want %s", retrievedBlock.Hash, block.Hash)
		}
		if retrievedBlock.Number != block.Number {
			t.Errorf("Block number mismatch: got %d, want %d", retrievedBlock.Number, block.Number)
		}
		if len(retrievedBlock.Transactions) != len(block.Transactions) {
			t.Errorf("Transaction count mismatch: got %d, want %d",
				len(retrievedBlock.Transactions), len(block.Transactions))
		}
	})

	t.Run("store_and_retrieve_transaction", func(t *testing.T) {
		tx := createTestTransaction(100)

		// Store transaction (SaveTransaction requires blockHeight)
		err := adapter.SaveTransaction(tx, 1)
		if err != nil {
			t.Fatalf("Failed to store transaction: %v", err)
		}

		// Retrieve transaction
		retrievedTx, err := adapter.GetTransaction(tx.ID)
		if err != nil {
			t.Fatalf("Failed to retrieve transaction: %v", err)
		}

		// Verify transaction data
		if retrievedTx.ID != tx.ID {
			t.Errorf("Transaction ID mismatch: got %s, want %s", retrievedTx.ID, tx.ID)
		}
		if retrievedTx.Amount != tx.Amount {
			t.Errorf("Transaction amount mismatch: got %f, want %f", retrievedTx.Amount, tx.Amount)
		}
	})

	t.Run("store_and_retrieve_account", func(t *testing.T) {
		account := &common.Account{
			ID:      "test_account_123",
			Balance: 1000.0,
			Nonce:   5,
		}

		// Store account
		err := adapter.SaveAccount(account)
		if err != nil {
			t.Fatalf("Failed to store account: %v", err)
		}

		// Retrieve account
		retrievedAccount, err := adapter.GetAccount(account.ID)
		if err != nil {
			t.Fatalf("Failed to retrieve account: %v", err)
		}

		// Verify account data
		if retrievedAccount.ID != account.ID {
			t.Errorf("Account ID mismatch: got %s, want %s", retrievedAccount.ID, account.ID)
		}
		if retrievedAccount.Balance != account.Balance {
			t.Errorf("Account balance mismatch: got %f, want %f", retrievedAccount.Balance, account.Balance)
		}
		if retrievedAccount.Nonce != account.Nonce {
			t.Errorf("Account nonce mismatch: got %d, want %d", retrievedAccount.Nonce, account.Nonce)
		}
	})
}

// TestMongoAdapterBatchOperations tests batch operations
func TestMongoAdapterBatchOperations(t *testing.T) {
	adapter := createTestMongoAdapter(t)
	defer cleanupTestAdapter(t, adapter)

	// ctx := context.Background() // unused

	t.Run("batch_store_blocks", func(t *testing.T) {
		numBlocks := 10
		blocks := make([]*common.Block, numBlocks)

		for i := 0; i < numBlocks; i++ {
			blocks[i] = &common.Block{
				Number:       i + 1,
				Hash:         fmt.Sprintf("hash_%d", i),
				PreviousHash: fmt.Sprintf("prev_hash_%d", i),
				Timestamp:    time.Now().Unix() + int64(i),
				Data:         []byte(fmt.Sprintf("block data %d", i)),
			}
		}

		// Create batch data
		batch := storage.NewWriteBatch()
		for _, block := range blocks {
			batch.AddBlock(block)
		}

		// Store batch
		start := time.Now()
		err := adapter.BatchWrite(batch)
		duration := time.Since(start)

		if err != nil {
			t.Fatalf("Failed to store batch: %v", err)
		}

		t.Logf("Stored %d blocks in %v", numBlocks, duration)

		// Verify all blocks were stored
		for i, block := range blocks {
			retrievedBlock, err := adapter.GetBlock(uint64(block.Number))
			if err != nil {
				t.Errorf("Failed to retrieve block %d: %v", i, err)
				continue
			}
			if retrievedBlock.Hash != block.Hash {
				t.Errorf("Block %d hash mismatch: got %s, want %s",
					i, retrievedBlock.Hash, block.Hash)
			}
		}
	})

	t.Run("batch_store_transactions", func(t *testing.T) {
		numTx := 100
		transactions := make([]*common.Transaction, numTx)

		for i := 0; i < numTx; i++ {
			transactions[i] = createTestTransaction(i + 1000)
		}

		batch := storage.NewWriteBatch()
		for _, tx := range transactions {
			batch.AddTransaction(tx)
		}

		start := time.Now()
		err := adapter.BatchWrite(batch)
		duration := time.Since(start)

		if err != nil {
			t.Fatalf("Failed to store transaction batch: %v", err)
		}

		t.Logf("Stored %d transactions in %v", numTx, duration)

		// Verify some transactions
		for i := 0; i < min(10, numTx); i++ {
			tx := transactions[i]
			retrievedTx, err := adapter.GetTransaction(tx.ID)
			if err != nil {
				t.Errorf("Failed to retrieve transaction %d: %v", i, err)
				continue
			}
			if retrievedTx.ID != tx.ID {
				t.Errorf("Transaction %d ID mismatch", i)
			}
		}
	})
}

// TestMongoAdapterStateOperations tests state management
func TestMongoAdapterStateOperations(t *testing.T) {
	adapter := createTestMongoAdapter(t)
	defer cleanupTestAdapter(t, adapter)

	// ctx := context.Background() // unused

	t.Run("store_and_retrieve_state", func(t *testing.T) {
		key := "test_state_key"
		value := []byte("test state value with important data")

		// Store state
		err := adapter.SetState([]byte(key), value)
		if err != nil {
			t.Fatalf("Failed to store state: %v", err)
		}

		// Retrieve state
		retrievedValue, err := adapter.GetState([]byte(key))
		if err != nil {
			t.Fatalf("Failed to retrieve state: %v", err)
		}

		// Verify state data
		if string(retrievedValue) != string(value) {
			t.Errorf("State value mismatch: got %s, want %s",
				string(retrievedValue), string(value))
		}
	})

	t.Run("update_state", func(t *testing.T) {
		key := "test_update_key"
		initialValue := []byte("initial value")
		updatedValue := []byte("updated value")

		// Store initial state
		err := adapter.SetState([]byte(key), initialValue)
		if err != nil {
			t.Fatalf("Failed to store initial state: %v", err)
		}

		// Update state
		err = adapter.SetState([]byte(key), updatedValue)
		if err != nil {
			t.Fatalf("Failed to update state: %v", err)
		}

		// Verify updated state
		retrievedValue, err := adapter.GetState([]byte(key))
		if err != nil {
			t.Fatalf("Failed to retrieve updated state: %v", err)
		}

		if string(retrievedValue) != string(updatedValue) {
			t.Errorf("Updated state mismatch: got %s, want %s",
				string(retrievedValue), string(updatedValue))
		}
	})

	t.Run("delete_state", func(t *testing.T) {
		key := "test_delete_key"
		value := []byte("value to be deleted")

		// Store state
		err := adapter.SetState([]byte(key), value)
		if err != nil {
			t.Fatalf("Failed to store state: %v", err)
		}

		// Verify it exists
		_, err = adapter.GetState([]byte(key))
		if err != nil {
			t.Fatalf("State should exist before deletion: %v", err)
		}

		// Delete state
		err = adapter.DeleteState([]byte(key))
		if err != nil {
			t.Fatalf("Failed to delete state: %v", err)
		}

		// Verify it's deleted
		_, err = adapter.GetState([]byte(key))
		if err == nil {
			t.Errorf("State should not exist after deletion")
		}
	})
}

// TestMongoAdapterBackupRestore tests backup and restore functionality
// SKIP: BackupData, RestoreData, and DeleteBlock methods don't exist in current implementation
/*
func TestMongoAdapterBackupRestore(t *testing.T) {
	// Test commented out - methods not implemented
}
*/

// TestMongoAdapterConcurrency tests concurrent operations
func TestMongoAdapterConcurrency(t *testing.T) {
	adapter := createTestMongoAdapter(t)
	defer cleanupTestAdapter(t, adapter)

	// ctx := context.Background() // unused

	t.Run("concurrent_writes", func(t *testing.T) {
		numGoroutines := 10
		numOpsPerGoroutine := 50
		errChan := make(chan error, numGoroutines*numOpsPerGoroutine)

		// Launch concurrent writers
		for g := 0; g < numGoroutines; g++ {
			go func(goroutineID int) {
				for i := 0; i < numOpsPerGoroutine; i++ {
					tx := createTestTransaction(goroutineID*1000 + i)
					err := adapter.SaveTransaction(tx, 1)
					if err != nil {
						errChan <- fmt.Errorf("goroutine %d, op %d: %v", goroutineID, i, err)
					}
				}
			}(g)
		}

		// Wait and check for errors
		time.Sleep(5 * time.Second)
		close(errChan)

		var errors []error
		for err := range errChan {
			errors = append(errors, err)
		}

		if len(errors) > 0 {
			t.Errorf("Encountered %d errors in concurrent writes:", len(errors))
			for i, err := range errors {
				if i < 5 { // Show first 5 errors
					t.Errorf("  Error %d: %v", i+1, err)
				}
			}
		}
	})

	t.Run("concurrent_reads_writes", func(t *testing.T) {
		// Store initial data
		for i := 0; i < 10; i++ {
			tx := createTestTransaction(i + 2000)
			err := adapter.SaveTransaction(tx, 1)
			if err != nil {
				t.Fatalf("Failed to store initial transaction: %v", err)
			}
		}

		numReaders := 5
		numWriters := 3
		done := make(chan bool)
		errors := make(chan error, 100)

		// Launch readers
		for r := 0; r < numReaders; r++ {
			go func(readerID int) {
				for i := 0; i < 20; i++ {
					txID := fmt.Sprintf("tx_%d", (readerID%10)+2000)
					_, err := adapter.GetTransaction(txID)
					if err != nil {
						errors <- fmt.Errorf("reader %d: %v", readerID, err)
					}
					time.Sleep(50 * time.Millisecond)
				}
				done <- true
			}(r)
		}

		// Launch writers
		for w := 0; w < numWriters; w++ {
			go func(writerID int) {
				for i := 0; i < 10; i++ {
					tx := createTestTransaction(writerID*100 + i + 3000)
					err := adapter.SaveTransaction(tx, 1)
					if err != nil {
						errors <- fmt.Errorf("writer %d: %v", writerID, err)
					}
					time.Sleep(100 * time.Millisecond)
				}
				done <- true
			}(w)
		}

		// Wait for completion
		for i := 0; i < numReaders+numWriters; i++ {
			<-done
		}

		// Check for errors
		close(errors)
		var errorList []error
		for err := range errors {
			errorList = append(errorList, err)
		}

		if len(errorList) > 0 {
			t.Errorf("Encountered %d errors in concurrent reads/writes:", len(errorList))
			for i, err := range errorList {
				if i < 3 { // Show first 3 errors
					t.Errorf("  Error %d: %v", i+1, err)
				}
			}
		}
	})
}

// Helper functions

func createTestMongoAdapter(t *testing.T) *storage.MongoAdapter {
	connectionString := "mongodb://localhost:27017"
	databaseName := "diamante_test_" + fmt.Sprintf("%d", time.Now().UnixNano())
	logger := logrus.New()
	cacheSize := 1000

	adapter, err := storage.NewMongoAdapter(connectionString, databaseName, logger, cacheSize)
	if err != nil {
		t.Fatalf("Failed to create test mongo adapter: %v", err)
	}

	return adapter
}

func cleanupTestAdapter(t *testing.T, adapter *storage.MongoAdapter) {
	err := adapter.Close()
	if err != nil {
		t.Errorf("Failed to close adapter: %v", err)
	}
}

func createTestTransaction(index int) *common.Transaction {
	return &common.Transaction{
		ID:       fmt.Sprintf("tx_%d", index),
		Sender:   fmt.Sprintf("sender_%d", index%10),
		Receiver: fmt.Sprintf("receiver_%d", index%5),
		Amount:   float64(index * 10),
		Data:     []byte(fmt.Sprintf("transaction data %d", index)),
		Nonce:    index,
		Metadata: &common.TransactionMetadata{},
	}
}

func createTestBlock(number int, hash string) *common.Block {
	return &common.Block{
		Number:       number,
		Hash:         hash,
		PreviousHash: fmt.Sprintf("prev_%s", hash),
		Timestamp:    time.Now().Unix(),
		Data:         []byte(fmt.Sprintf("block data %d", number)),
		Transactions: []common.Transaction{
			*createTestTransaction(number * 10),
		},
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
