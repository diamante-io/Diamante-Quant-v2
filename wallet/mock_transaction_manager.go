package wallet

import (
	"diamante/common"
	"diamante/transaction"
)

// MockTransactionManager is a simplified transaction manager for testing
type MockTransactionManager struct {
	Pool *transaction.TransactionPool
}

// CreateTransaction creates a transaction without signing it
func (m *MockTransactionManager) CreateTransaction(
	sender, receiver string,
	amount, fee float64,
	data []byte,
) (*common.Transaction, error) {
	// Generate a unique transaction ID
	txID := common.GenerateUniqueID()

	// Create the transaction
	tx := &common.Transaction{
		ID:        txID,
		Sender:    sender,
		Receiver:  receiver,
		Amount:    amount,
		Fee:       fee,
		Data:      data,
		Timestamp: 0, // Not important for testing
		Nonce:     1, // Not important for testing
		Status:    "pending",
	}

	return tx, nil
}
