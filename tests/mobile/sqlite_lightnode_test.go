package mobile_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"diamante/common"
	"diamante/mobile"
)

// TestConfig tests the configuration validation and defaults
func TestConfig(t *testing.T) {
	t.Run("DefaultConfig", func(t *testing.T) {
		config := mobile.DefaultConfig()
		if config == nil {
			t.Fatal("DefaultConfig returned nil")
		}

		if config.ConnectionTimeout <= 0 {
			t.Error("Default connection timeout should be positive")
		}

		if config.MaxOpenConnections <= 0 {
			t.Error("Default max open connections should be positive")
		}

		if config.MaxIdleConnections < 0 {
			t.Error("Default max idle connections should not be negative")
		}

		if !config.EnableWALMode {
			t.Error("Default should enable WAL mode")
		}
	})
}

// TestNewSQLiteLightNode tests the creation of SQLiteLightNode
func TestNewSQLiteLightNode(t *testing.T) {
	// Create temporary directory for test database
	tempDir, err := os.MkdirTemp("", "sqlite-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	t.Run("SuccessfulCreation", func(t *testing.T) {
		config := mobile.DefaultConfig()
		config.DatabasePath = filepath.Join(tempDir, "test.db")

		node, err := mobile.NewSQLiteLightNode(config)
		if err != nil {
			t.Fatalf("Failed to create SQLiteLightNode: %v", err)
		}
		defer node.Close()

		// Test basic operations to ensure node is functional
		header := common.Block{
			Number:       1,
			Hash:         "0x123",
			PreviousHash: "0x000",
			Timestamp:    time.Now().Unix(),
		}

		if err := node.SaveBlockHeader(header); err != nil {
			t.Errorf("Failed to save block header: %v", err)
		}
	})

	t.Run("NilConfig", func(t *testing.T) {
		node, err := mobile.NewSQLiteLightNode(nil)
		if err == nil {
			t.Error("Expected error for nil config")
			if node != nil {
				node.Close()
			}
		}
	})
}

// TestBlockOperations tests block-related operations
func TestBlockOperations(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "sqlite-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	config := mobile.DefaultConfig()
	config.DatabasePath = filepath.Join(tempDir, "test.db")

	node, err := mobile.NewSQLiteLightNode(config)
	if err != nil {
		t.Fatalf("Failed to create node: %v", err)
	}
	defer node.Close()

	t.Run("SaveAndGetBlockHeader", func(t *testing.T) {
		header := common.Block{
			Number:       100,
			Hash:         "0xabc123",
			PreviousHash: "0xdef456",
			Timestamp:    time.Now().Unix(),
			Validator:    "validator1",
		}

		// Save block header
		if err := node.SaveBlockHeader(header); err != nil {
			t.Fatalf("Failed to save block header: %v", err)
		}

		// Get latest block header
		retrieved, err := node.GetLatestBlockHeader()
		if err != nil {
			t.Fatalf("Failed to get latest block header: %v", err)
		}

		if retrieved.Number != header.Number {
			t.Errorf("Expected block number %d, got %d", header.Number, retrieved.Number)
		}
		if retrieved.Hash != header.Hash {
			t.Errorf("Expected block hash %s, got %s", header.Hash, retrieved.Hash)
		}
	})
}

// TestAccountOperations tests account-related operations
func TestAccountOperations(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "sqlite-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	config := mobile.DefaultConfig()
	config.DatabasePath = filepath.Join(tempDir, "test.db")

	node, err := mobile.NewSQLiteLightNode(config)
	if err != nil {
		t.Fatalf("Failed to create node: %v", err)
	}
	defer node.Close()

	t.Run("SaveAndGetAccount", func(t *testing.T) {
		account := &common.Account{
			ID:        "account1",
			Balance:   1000.5,
			PublicKey: []byte("pubkey123"),
			Nonce:     10,
		}

		// Save account
		if err := node.SaveAccount(account); err != nil {
			t.Fatalf("Failed to save account: %v", err)
		}

		// Get account
		retrieved, err := node.GetAccount("account1")
		if err != nil {
			t.Fatalf("Failed to get account: %v", err)
		}

		if retrieved.ID != account.ID {
			t.Errorf("Expected account ID %s, got %s", account.ID, retrieved.ID)
		}
		if retrieved.Balance != account.Balance {
			t.Errorf("Expected balance %f, got %f", account.Balance, retrieved.Balance)
		}
		if retrieved.Nonce != account.Nonce {
			t.Errorf("Expected nonce %d, got %d", account.Nonce, retrieved.Nonce)
		}
	})
}

// TestTransactionOperations tests transaction-related operations
func TestTransactionOperations(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "sqlite-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	config := mobile.DefaultConfig()
	config.DatabasePath = filepath.Join(tempDir, "test.db")

	node, err := mobile.NewSQLiteLightNode(config)
	if err != nil {
		t.Fatalf("Failed to create node: %v", err)
	}
	defer node.Close()

	t.Run("SaveAndGetTransactions", func(t *testing.T) {
		// First save an account
		account := &common.Account{
			ID:        "testaccount",
			Balance:   1000,
			PublicKey: []byte("pubkey"),
			Nonce:     0,
		}
		if err := node.SaveAccount(account); err != nil {
			t.Fatalf("Failed to save account: %v", err)
		}

		// Save transactions using SaveTransaction with status
		tx1 := &common.Transaction{
			ID:        "tx1",
			Sender:    "testaccount",
			Receiver:  "receiver1",
			Amount:    100,
			Timestamp: time.Now().Unix(),
			Signature: []byte("sig1"),
		}

		tx2 := &common.Transaction{
			ID:        "tx2",
			Sender:    "sender2",
			Receiver:  "testaccount",
			Amount:    50,
			Timestamp: time.Now().Unix() + 1,
			Signature: []byte("sig2"),
		}

		if err := node.SaveTransaction(tx1, mobile.StatusPending); err != nil {
			t.Fatalf("Failed to save transaction 1: %v", err)
		}

		if err := node.SaveTransaction(tx2, mobile.StatusConfirmed); err != nil {
			t.Fatalf("Failed to save transaction 2: %v", err)
		}

		// Get transactions for account
		transactions, err := node.GetTransactionsForAccount("testaccount")
		if err != nil {
			t.Fatalf("Failed to get transactions: %v", err)
		}

		if len(transactions) != 2 {
			t.Errorf("Expected 2 transactions, got %d", len(transactions))
		}
	})

	t.Run("SaveTransactionWithDefaultStatus", func(t *testing.T) {
		tx := &common.Transaction{
			ID:        "tx-default",
			Sender:    "sender",
			Receiver:  "receiver",
			Amount:    100,
			Timestamp: time.Now().Unix(),
			Signature: []byte("sig"),
		}

		if err := node.SaveTransactionWithDefaultStatus(tx); err != nil {
			t.Fatalf("Failed to save transaction with default status: %v", err)
		}
	})
}

// TestSyncOperations tests sync metadata operations
func TestSyncOperations(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "sqlite-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	config := mobile.DefaultConfig()
	config.DatabasePath = filepath.Join(tempDir, "test.db")

	node, err := mobile.NewSQLiteLightNode(config)
	if err != nil {
		t.Fatalf("Failed to create node: %v", err)
	}
	defer node.Close()

	t.Run("SaveAndGetSyncMetadata", func(t *testing.T) {
		// Save sync metadata
		if err := node.SaveSyncMetadata("last_block", "1000"); err != nil {
			t.Fatalf("Failed to save sync metadata: %v", err)
		}

		if err := node.SaveSyncMetadata("last_hash", "0xabc123"); err != nil {
			t.Fatalf("Failed to save sync metadata: %v", err)
		}

		// Get sync metadata
		lastBlock, err := node.GetSyncMetadata("last_block")
		if err != nil {
			t.Fatalf("Failed to get sync metadata: %v", err)
		}

		lastHash, err := node.GetSyncMetadata("last_hash")
		if err != nil {
			t.Fatalf("Failed to get sync metadata: %v", err)
		}

		if lastBlock != "1000" {
			t.Errorf("Expected last block 1000, got %s", lastBlock)
		}
		if lastHash != "0xabc123" {
			t.Errorf("Expected last hash 0xabc123, got %s", lastHash)
		}
	})
}
