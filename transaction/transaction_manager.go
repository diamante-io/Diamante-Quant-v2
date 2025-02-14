package transaction

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	"diamante/common"
	"diamante/crypto"
	"diamante/ledger"
)

// TransactionManager handles creation, validation, and processing of transactions,
// bridging the transaction pool (mempool) and the ledger.
type TransactionManager struct {
	mu                        sync.RWMutex
	pool                      *TransactionPool
	minFeeThreshold           float64
	nonceTracker              NonceTracker
	conflictResolutionEnabled bool
	ledger                    ledger.LedgerAPI
}

// NewTransactionManager creates a manager referencing:
//   - a transaction pool
//   - a minFee threshold
//   - optional conflict resolution
//   - a ledger for final commits
func NewTransactionManager(
	txPool *TransactionPool,
	minFeeThreshold float64,
	conflictResolutionEnabled bool,
	ledgerAPI ledger.LedgerAPI,
) *TransactionManager {
	return &TransactionManager{
		pool:                      txPool,
		minFeeThreshold:           minFeeThreshold,
		nonceTracker:              NewDefaultNonceTracker(),
		conflictResolutionEnabled: conflictResolutionEnabled,
		ledger:                    ledgerAPI,
	}
}

// CreateTransaction constructs a new transaction, validates, signs, and adds to pool.
func (tm *TransactionManager) CreateTransaction(
	sender, receiver string,
	amount, fee float64,
	data []byte,
) (*common.Transaction, error) {

	// 1) Generate a unique transaction ID
	txID := GenerateTransactionID(sender, receiver, amount)
	timestamp := time.Now().Unix()

	// 2) Build the transaction
	tx := &common.Transaction{
		ID:        txID,
		Sender:    sender,
		Receiver:  receiver,
		Amount:    amount,
		Fee:       fee,
		Data:      data,
		Timestamp: timestamp,
		Nonce:     tm.nonceTracker.GetNonce(sender) + 1,
	}

	// 3) Validate pre-sign
	if err := tm.ValidateTransaction(tx); err != nil {
		return nil, fmt.Errorf("CreateTransaction validation: %w", err)
	}

	// 4) Sign it (placeholder for real signing logic)
	if err := tm.signTransaction(tx); err != nil {
		return nil, fmt.Errorf("CreateTransaction signing: %w", err)
	}

	// 5) Add to pool
	if err := tm.pool.AddTransaction(*tx); err != nil {
		return nil, fmt.Errorf("CreateTransaction pool-add: %w", err)
	}

	log.Printf("Transaction %s created & added to pool.", tx.ID)
	return tx, nil
}

// ValidateTransaction checks amount, fee, etc.
func (tm *TransactionManager) ValidateTransaction(tx *common.Transaction) error {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	if tx.Amount <= 0 {
		msg := fmt.Sprintf("invalid tx amount: %f", tx.Amount)
		logrus.Error(msg)
		return errors.New(msg)
	}
	if tx.Fee < tm.minFeeThreshold {
		msg := fmt.Sprintf("fee below threshold: %f < %f", tx.Fee, tm.minFeeThreshold)
		logrus.Error(msg)
		return errors.New(msg)
	}
	// Additional checks if needed...
	return nil
}

// signTransaction is a dummy local sign for demonstration.
// Production code might fetch from a secure store or user wallet.
// transaction_manager.go (fixed)
func (tm *TransactionManager) signTransaction(tx *common.Transaction) error {
	// 1) Lookup the account in `common` to find a real Dilithium private key
	acc := common.GetAccount(tx.Sender)
	if acc == nil {
		return fmt.Errorf("cannot sign transaction: sender account %s not found", tx.Sender)
	}
	if len(acc.PrivateKey) == 0 {
		return fmt.Errorf("sender %s has no Dilithium private key set", tx.Sender)
	}

	// 2) Use the real private key with the default Dilithium level (set in dilithium.go)
	signature, err := crypto.SignDataWithDilithium(acc.PrivateKey, []byte(tx.ID))
	if err != nil {
		return fmt.Errorf("signing tx %s: %w", tx.ID, err)
	}
	tx.Signature = signature
	return nil
}

// ProcessTransaction: called after a tx is finalized in consensus.
// Removes from pool & commits to ledger.
func (tm *TransactionManager) ProcessTransaction(txID string) error {
	// 1) retrieve from pool
	tx, err := tm.pool.GetTransaction(txID)
	if err != nil {
		return fmt.Errorf("ProcessTransaction: not found in pool: %w", err)
	}

	// 2) replay protection
	if err := ReplayProtectionMiddleware(*tx, tm.nonceTracker); err != nil {
		return fmt.Errorf("ProcessTransaction replay error: %w", err)
	}

	// 3) remove from pool
	if err := tm.pool.RemoveTransaction(tx.ID); err != nil {
		return fmt.Errorf("ProcessTransaction remove: %w", err)
	}

	// 4) commit to ledger
	if err := tm.ledger.AddTransaction(*tx); err != nil {
		return fmt.Errorf("ProcessTransaction ledger commit: %w", err)
	}
	log.Printf("Transaction %s processed & committed.", tx.ID)
	return nil
}

// HandleTransactionConflicts: forcibly resolves double-spend or concurrency issues.
func (tm *TransactionManager) HandleTransactionConflicts() {
	if tm.conflictResolutionEnabled {
		tm.pool.HandleConflicts()
		log.Println("Transaction conflicts resolved (pool-level).")
	}
}

// GetTransactionStatus checks pool or ledger
func (tm *TransactionManager) GetTransactionStatus(txID string) (string, error) {
	// 1) in pool?
	if tm.pool.HasTransaction(txID) {
		return "Pending", nil
	}
	// 2) in ledger?
	if tm.ledger.IsTransactionCommitted(txID) {
		return "Committed", nil
	}
	return "Unknown", fmt.Errorf("tx %s not found", txID)
}

// MonitorTransactionPool: spawns a goroutine to watch pool usage.
func (tm *TransactionManager) MonitorTransactionPool() {
	go tm.pool.MonitorPoolSize()
}

// GetPoolSize returns the size of the transaction pool.
func (tm *TransactionManager) GetPoolSize() int {
	return tm.pool.PoolSize()
}
