package testutils

import (
	"fmt"
	"sync"

	"diamante/common"
	"diamante/transaction"
)

// MockTransactionManager is a transaction manager for testing purposes
type MockTransactionManager struct {
	Pool         *transaction.TransactionPool
	Transactions map[string]*common.Transaction
	NextNonce    map[string]int
	mu           sync.RWMutex
}

// NewMockTransactionManager creates a new mock transaction manager
func NewMockTransactionManager() *MockTransactionManager {
	return &MockTransactionManager{
		Transactions: make(map[string]*common.Transaction),
		NextNonce:    make(map[string]int),
	}
}

// CreateTransaction creates a transaction and stores it in the mock manager
func (m *MockTransactionManager) CreateTransaction(
	sender, receiver string,
	amount, fee float64,
	data []byte,
) (*common.Transaction, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Validate inputs
	if sender == "" {
		return nil, fmt.Errorf("sender cannot be empty")
	}
	if receiver == "" {
		return nil, fmt.Errorf("receiver cannot be empty")
	}
	if amount < 0 {
		return nil, fmt.Errorf("amount cannot be negative")
	}
	if fee < 0 {
		return nil, fmt.Errorf("fee cannot be negative")
	}

	// Generate a unique transaction ID
	txID := common.GenerateUniqueID()

	// Get next nonce for sender
	nonce := m.NextNonce[sender]
	m.NextNonce[sender] = nonce + 1

	// Create the transaction
	tx := &common.Transaction{
		ID:        txID,
		Sender:    sender,
		Receiver:  receiver,
		Amount:    amount,
		Fee:       fee,
		Data:      data,
		Timestamp: common.ConsensusUnix(),
		Nonce:     nonce,
		Status:    "pending",
	}

	// Store the transaction
	m.Transactions[txID] = tx

	return tx, nil
}

// GetTransaction retrieves a transaction by ID
func (m *MockTransactionManager) GetTransaction(txID string) (*common.Transaction, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	tx, exists := m.Transactions[txID]
	if !exists {
		return nil, fmt.Errorf("transaction %s not found", txID)
	}
	return tx, nil
}

// GetTransactionCount returns the number of transactions
func (m *MockTransactionManager) GetTransactionCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.Transactions)
}

// Clear removes all transactions (useful for test cleanup)
func (m *MockTransactionManager) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Transactions = make(map[string]*common.Transaction)
	m.NextNonce = make(map[string]int)
}
