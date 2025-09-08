// Package testutils provides simplified utilities for testing across the Diamante project
package testutils

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"diamante/common"
)

// TestConfig holds basic configuration for test environments
type TestConfig struct {
	CleanupAfter bool
	TestDataDir  string
	TestTimeout  time.Duration
}

// DefaultTestConfig returns a default test configuration
func DefaultTestConfig() *TestConfig {
	return &TestConfig{
		CleanupAfter: true,
		TestDataDir:  "./testdata",
		TestTimeout:  30 * time.Second,
	}
}

// TestEnvironment provides a basic test environment setup
type TestEnvironment struct {
	Config  *TestConfig
	TestDir string
	t       *testing.T
}

// NewTestEnvironment creates a new test environment
func NewTestEnvironment(t *testing.T) *TestEnvironment {
	config := DefaultTestConfig()

	// Create test directory
	testDir := filepath.Join(os.TempDir(), fmt.Sprintf("diamante_test_%d", time.Now().Unix()))
	if err := os.MkdirAll(testDir, 0755); err != nil {
		t.Fatalf("Failed to create test directory: %v", err)
	}

	return &TestEnvironment{
		Config:  config,
		TestDir: testDir,
		t:       t,
	}
}

// Setup initializes the test environment
func (te *TestEnvironment) Setup() error {
	// Basic setup - no external dependencies
	return nil
}

// Cleanup cleans up the test environment
func (te *TestEnvironment) Cleanup() {
	if te.Config.CleanupAfter {
		// Clean up test directory
		if err := os.RemoveAll(te.TestDir); err != nil {
			te.t.Logf("Warning: Failed to remove test directory: %v", err)
		}
	}
}

// CreateTestAccount creates a test account for testing purposes
func (te *TestEnvironment) CreateTestAccount(id string, balance float64) *common.Account {
	// Create a 32-byte public key by filling with a pattern
	publicKey := make([]byte, 32)
	for i := 0; i < 32; i++ {
		publicKey[i] = byte(i + 1) // Fill with 1, 2, 3, ..., 32
	}

	account, err := common.NewAccount(id, publicKey)
	if err != nil {
		te.t.Fatalf("Failed to create test account: %v", err)
	}

	account.Balance = balance
	return account
}

// CreateTestTransaction creates a test transaction
func (te *TestEnvironment) CreateTestTransaction(sender, receiver string, amount float64) common.Transaction {
	// Ensure sender and receiver are at least 8 characters for validation
	if len(sender) < 8 {
		sender = sender + "12345678"[:8-len(sender)]
	}
	if len(receiver) < 8 {
		receiver = receiver + "12345678"[:8-len(receiver)]
	}

	return common.Transaction{
		ID:        fmt.Sprintf("tx_%d", time.Now().UnixNano()),
		Sender:    sender,
		Receiver:  receiver,
		Amount:    amount,
		Timestamp: time.Now().Unix(),
		Status:    "pending",
		Metadata: &common.TransactionMetadata{
			Description: "Test transaction",
			Category:    "test",
			Source:      sender,
			Destination: receiver,
		},
	}
}

// CreateTestBlock creates a test block
func (te *TestEnvironment) CreateTestBlock(number int, transactions []common.Transaction) common.Block {
	return common.Block{
		Number:       number,
		Hash:         fmt.Sprintf("block_hash_%d", number),
		PreviousHash: fmt.Sprintf("prev_hash_%d", number-1),
		Timestamp:    time.Now().Unix(),
		Transactions: transactions,
	}
}

// WaitForCondition waits for a condition to be true with timeout
func (te *TestEnvironment) WaitForCondition(condition func() bool, timeout time.Duration, message string) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	te.t.Fatalf("Timeout waiting for condition: %s", message)
}

// AssertNoError asserts that an error is nil
func (te *TestEnvironment) AssertNoError(err error, message string) {
	if err != nil {
		te.t.Fatalf("%s: %v", message, err)
	}
}

// AssertError asserts that an error is not nil
func (te *TestEnvironment) AssertError(err error, message string) {
	if err == nil {
		te.t.Fatalf("%s: expected error but got nil", message)
	}
}

// AssertEqual asserts that two values are equal
func (te *TestEnvironment) AssertEqual(expected, actual interface{}, message string) {
	if expected != actual {
		te.t.Fatalf("%s: expected %v, got %v", message, expected, actual)
	}
}

// AssertTrue asserts that a condition is true
func (te *TestEnvironment) AssertTrue(condition bool, message string) {
	if !condition {
		te.t.Fatalf("%s: condition is false", message)
	}
}

// AssertFalse asserts that a condition is false
func (te *TestEnvironment) AssertFalse(condition bool, message string) {
	if condition {
		te.t.Fatalf("%s: condition is true", message)
	}
}

// TestRunner provides utilities for running tests with setup/teardown
type TestRunner struct {
	env *TestEnvironment
}

// NewTestRunner creates a new test runner
func NewTestRunner(t *testing.T) *TestRunner {
	env := NewTestEnvironment(t)
	return &TestRunner{env: env}
}

// Run executes a test function with proper setup and cleanup
func (tr *TestRunner) Run(testFunc func(*TestEnvironment)) {
	// Setup
	if err := tr.env.Setup(); err != nil {
		tr.env.t.Fatalf("Failed to setup test environment: %v", err)
	}

	// Cleanup on exit
	defer tr.env.Cleanup()

	// Run the test
	testFunc(tr.env)
}
