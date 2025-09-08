package transaction

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"diamante/common"
)

// TransactionSerializer handles serialization and size checks for transactions.
type TransactionSerializer struct {
	maxSize int
}

// NewTransactionSerializer creates a serializer with a maximum size.
func NewTransactionSerializer(maxSize int) *TransactionSerializer {
	return &TransactionSerializer{maxSize: maxSize}
}

// SerializeTransaction deterministically serializes a transaction using JSON.
func (ts *TransactionSerializer) SerializeTransaction(tx *common.Transaction) ([]byte, error) {
	if tx == nil {
		return nil, errors.New("transaction cannot be nil")
	}
	// Marshal the transaction struct directly. Struct field order is
	// deterministic so this is sufficient for size checks.
	return json.Marshal(tx)
}

// ValidateTransactionSize verifies the serialized size is within the limit.
func (ts *TransactionSerializer) ValidateTransactionSize(tx *common.Transaction) error {
	data, err := ts.SerializeTransaction(tx)
	if err != nil {
		return fmt.Errorf("failed to serialize transaction: %w", err)
	}
	if ts.maxSize > 0 && len(data) > ts.maxSize {
		return fmt.Errorf("transaction size %d exceeds maximum %d bytes", len(data), ts.maxSize)
	}
	return nil
}

// ValidateTransactionEnhanced performs extended validation steps.
func (tm *TransactionManager) ValidateTransactionEnhanced(tx *common.Transaction) error {
	// Reuse existing validation logic first.
	if err := tm.ValidateTransaction(tx); err != nil {
		return err
	}

	// Size validation using serializer.
	serializer := NewTransactionSerializer(tm.maxTxSize)
	if err := serializer.ValidateTransactionSize(tx); err != nil {
		return err
	}

	// Ledger-based account state checks.
	if err := tm.validateAccountState(tx); err != nil {
		return err
	}

	// Smart contract validation if applicable.
	if tx.SmartContractID != "" {
		if err := tm.validateSmartContractCall(tx); err != nil {
			return err
		}
	}

	return nil
}

// validateAccountState ensures the ledger reflects sufficient balance and nonce.
func (tm *TransactionManager) validateAccountState(tx *common.Transaction) error {
	balance, err := tm.ledger.GetBalance(tx.Sender)
	if err != nil {
		return fmt.Errorf("sender account not found: %w", err)
	}
	required := tx.Amount + tx.Fee
	if balance < required {
		return fmt.Errorf("insufficient balance: have %f, need %f", balance, required)
	}

	sender := common.GetAccount(tx.Sender)
	if sender != nil {
		if tx.Nonce <= sender.Nonce {
			return fmt.Errorf("nonce too low: tx has %d, account has %d", tx.Nonce, sender.Nonce)
		}
	}

	if tx.Receiver != "" {
		if _, err := tm.ledger.GetBalance(tx.Receiver); err != nil {
			return fmt.Errorf("invalid receiver account: %w", err)
		}
	}
	return nil
}

// validateSmartContractCall performs comprehensive smart contract validation.
func (tm *TransactionManager) validateSmartContractCall(tx *common.Transaction) error {
	if tm.ledger == nil {
		return errors.New("ledger API not available")
	}

	// Create contract validator if not already set
	tm.mu.RLock()
	validator := tm.contractValidator
	tm.mu.RUnlock()

	if validator == nil {
		// Create a new validator instance
		validator = NewContractValidator(tm.ledger, nil, tm.logger)

		// Store it for future use
		tm.mu.Lock()
		tm.contractValidator = validator
		tm.mu.Unlock()
	}

	// Use the comprehensive contract validator
	return validator.ValidateContractCall(tx)
}

// GetTransactionsByAccountComplete returns historical and pending transactions.
func (tm *TransactionManager) GetTransactionsByAccountComplete(accountID string, limit, offset int) ([]*common.Transaction, error) {
	if accountID == "" {
		return nil, errors.New("account ID cannot be empty")
	}

	historical, err := tm.ledger.GetAccountTransactions(accountID, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to query ledger: %w", err)
	}

	pending, err := tm.GetPendingTransactionsByAccount(accountID)
	if err != nil {
		return nil, fmt.Errorf("failed to get pending transactions: %w", err)
	}

	all := make([]*common.Transaction, 0, len(historical)+len(pending))
	for i := range historical {
		txCopy := historical[i]
		all = append(all, &txCopy)
	}
	all = append(all, pending...)

	sort.Slice(all, func(i, j int) bool {
		return all[i].Timestamp > all[j].Timestamp
	})

	if len(all) > limit {
		all = all[:limit]
	}
	return all, nil
}
