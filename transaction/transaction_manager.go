// transaction/transaction_manager.go

package transaction

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"

	"diamante/common"
	"diamante/crypto"
)

var (
	// ErrTxPoolFull indicates that the transaction pool is at capacity
	ErrTxPoolFull = errors.New("transaction pool is full")

	// ErrTxAlreadyExists indicates a duplicate transaction
	ErrTxAlreadyExists = errors.New("transaction already exists in pool")

	// ErrFeeTooLow indicates that the transaction fee is below the minimum threshold
	ErrFeeTooLow = errors.New("transaction fee below minimum threshold")

	// ErrInvalidTransaction indicates general transaction validation failure
	ErrInvalidTransaction = errors.New("invalid transaction")

	// ErrSigningFailed indicates a failure during transaction signing
	ErrSigningFailed = errors.New("transaction signing failed")

	// ErrNonceTooLow indicates that the transaction has an outdated nonce
	ErrNonceTooLow = errors.New("transaction nonce too low")
)

// TransactionStats tracks performance metrics for transaction processing
type TransactionStats struct {
	TotalProcessed      uint64
	TotalRejected       uint64
	TotalConfirmed      uint64
	AvgProcessingTime   time.Duration
	MaxProcessingTime   time.Duration
	AvgConfirmationTime time.Duration
	LastProcessedTime   time.Time
}

// TransactionManager handles creation, validation, and processing of transactions,
// bridging the transaction pool (mempool) and the ledger.
type TransactionManager struct {
	mu                        sync.RWMutex
	pool                      *TransactionPool
	minFeeThreshold           float64
	nonceTracker              NonceTracker
	conflictResolutionEnabled bool
	ledger                    common.LedgerAPI // Interface for ledger operations
	cryptoManager             *crypto.CryptoManager
	logger                    *logrus.Logger

	// Performance metrics
	stats TransactionStats

	// Config options
	maxTxsPerBlock int
	maxTxSize      int
	maxFutureNonce int // How many nonces ahead we accept
	maxTxAge       time.Duration

	// Internal state
	isRunning int32 // atomic flag for background processes
	stopChan  chan struct{}
}

// TransactionManagerOption defines functional options for the TransactionManager
type TransactionManagerOption func(*TransactionManager)

// WithLogger sets a custom logger for the TransactionManager
func WithLogger(logger *logrus.Logger) TransactionManagerOption {
	return func(tm *TransactionManager) {
		tm.logger = logger
	}
}

// WithMaxTransactionsPerBlock sets the maximum number of transactions per block
func WithMaxTransactionsPerBlock(max int) TransactionManagerOption {
	return func(tm *TransactionManager) {
		if max > 0 {
			tm.maxTxsPerBlock = max
		}
	}
}

// WithMaxTransactionSize sets the maximum transaction size in bytes
func WithMaxTransactionSize(maxSize int) TransactionManagerOption {
	return func(tm *TransactionManager) {
		if maxSize > 0 {
			tm.maxTxSize = maxSize
		}
	}
}

// WithMaxFutureNonce sets how many nonces ahead of current we'll accept
func WithMaxFutureNonce(max int) TransactionManagerOption {
	return func(tm *TransactionManager) {
		if max > 0 {
			tm.maxFutureNonce = max
		}
	}
}

// WithMaxTransactionAge sets the maximum age for transactions in the pool
func WithMaxTransactionAge(maxAge time.Duration) TransactionManagerOption {
	return func(tm *TransactionManager) {
		if maxAge > 0 {
			tm.maxTxAge = maxAge
		}
	}
}

// WithCryptoManager sets a crypto manager for transaction signing and verification
func WithCryptoManager(cm *crypto.CryptoManager) TransactionManagerOption {
	return func(tm *TransactionManager) {
		tm.cryptoManager = cm
	}
}

// NewTransactionManager creates a new TransactionManager instance.
func NewTransactionManager(
	txPool *TransactionPool,
	minFeeThreshold float64,
	conflictResolutionEnabled bool,
	ledgerAPI common.LedgerAPI,
	options ...TransactionManagerOption,
) *TransactionManager {
	tm := &TransactionManager{
		pool:                      txPool,
		minFeeThreshold:           minFeeThreshold,
		nonceTracker:              NewDefaultNonceTracker(),
		conflictResolutionEnabled: conflictResolutionEnabled,
		ledger:                    ledgerAPI,
		logger:                    logrus.New(), // Default logger
		maxTxsPerBlock:            5000,         // Default values
		maxTxSize:                 1024 * 1024,  // 1MB
		maxFutureNonce:            10,           // Allow up to 10 nonces in the future
		maxTxAge:                  24 * time.Hour,
		stopChan:                  make(chan struct{}),
	}

	// Apply options
	for _, option := range options {
		option(tm)
	}

	return tm
}

// Start begins background monitoring and cleanup processes
func (tm *TransactionManager) Start() error {
	if !atomic.CompareAndSwapInt32(&tm.isRunning, 0, 1) {
		return errors.New("transaction manager is already running")
	}

	tm.logger.Info("Starting transaction manager")

	// Start background monitoring of transaction pool
	go tm.MonitorTransactionPool()

	// Start background cleanup of expired transactions
	go tm.cleanupExpiredTransactions()

	// Start background revalidation of pending transactions
	go tm.revalidatePendingTransactions()

	return nil
}

// Stop halts all background processes
func (tm *TransactionManager) Stop() error {
	if !atomic.CompareAndSwapInt32(&tm.isRunning, 1, 0) {
		return errors.New("transaction manager is not running")
	}

	tm.logger.Info("Stopping transaction manager")
	close(tm.stopChan)

	return nil
}

// CreateTransaction constructs a new transaction, validates it, signs it, and adds it to the pool.
func (tm *TransactionManager) CreateTransaction(
	sender, receiver string,
	amount, fee float64,
	data []byte,
) (*common.Transaction, error) {
	startTime := time.Now()

	// 1) Validate inputs
	if sender == "" || receiver == "" {
		return nil, errors.New("sender and receiver cannot be empty")
	}

	if amount <= 0 {
		return nil, errors.New("amount must be positive")
	}

	if fee < tm.minFeeThreshold {
		return nil, fmt.Errorf("%w: minimum required is %f", ErrFeeTooLow, tm.minFeeThreshold)
	}

	// 2) Generate a unique transaction ID.
	txID := GenerateTransactionID(sender, receiver, amount)
	timestamp := time.Now().Unix()

	// 3) Retrieve current nonce for sender
	currentNonce := tm.nonceTracker.GetNonce(sender)

	// 4) Build the transaction
	tx := &common.Transaction{
		ID:        txID,
		Sender:    sender,
		Receiver:  receiver,
		Amount:    amount,
		Fee:       fee,
		Data:      data,
		Timestamp: timestamp,
		Nonce:     int(currentNonce + 1), // Convert int64 to int
		Status:    "pending",
	}

	// 5) Validate the transaction pre-signature
	if err := tm.ValidateTransaction(tx); err != nil {
		tm.logRejection("pre-signature validation failed", tx, err)
		return nil, fmt.Errorf("validation error: %w", err)
	}

	// 6) Sign the transaction
	if err := tm.signTransaction(tx); err != nil {
		tm.logRejection("signing failed", tx, err)
		return nil, fmt.Errorf("signing error: %w", err)
	}

	// 7) Add the transaction to the pool
	if err := tm.pool.AddTransaction(*tx); err != nil {
		tm.logRejection("pool add failed", tx, err)
		return nil, fmt.Errorf("pool add error: %w", err)
	}

	// 8) Update nonce tracker
	tm.nonceTracker.SetNonce(sender, int64(tx.Nonce))

	// 9) Update stats
	tm.updateProcessingStats(startTime)

	tm.logger.WithFields(logrus.Fields{
		"txID":      tx.ID,
		"sender":    tx.Sender,
		"receiver":  tx.Receiver,
		"amount":    tx.Amount,
		"fee":       tx.Fee,
		"nonce":     tx.Nonce,
		"timestamp": time.Unix(tx.Timestamp, 0),
		"duration":  time.Since(startTime),
	}).Info("Transaction created and added to pool")

	return tx, nil
}

// logRejection consistently logs transaction rejections
func (tm *TransactionManager) logRejection(reason string, tx *common.Transaction, err error) {
	atomic.AddUint64(&tm.stats.TotalRejected, 1)

	tm.logger.WithFields(logrus.Fields{
		"txID":     tx.ID,
		"sender":   tx.Sender,
		"receiver": tx.Receiver,
		"amount":   tx.Amount,
		"fee":      tx.Fee,
		"nonce":    tx.Nonce,
		"reason":   reason,
		"error":    err,
	}).Warn("Transaction rejected")
}

// updateProcessingStats updates performance metrics
func (tm *TransactionManager) updateProcessingStats(startTime time.Time) {
	atomic.AddUint64(&tm.stats.TotalProcessed, 1)

	duration := time.Since(startTime)

	// Update avg processing time with simple moving average
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if tm.stats.AvgProcessingTime == 0 {
		tm.stats.AvgProcessingTime = duration
	} else {
		tm.stats.AvgProcessingTime = (tm.stats.AvgProcessingTime*9 + duration) / 10
	}

	// Update max processing time if this one was longer
	if duration > tm.stats.MaxProcessingTime {
		tm.stats.MaxProcessingTime = duration
	}

	tm.stats.LastProcessedTime = time.Now()
}

// ValidateTransaction performs comprehensive transaction checks
func (tm *TransactionManager) ValidateTransaction(tx *common.Transaction) error {
	// 1) Basic validation checks
	if tx == nil {
		return errors.New("transaction cannot be nil")
	}

	if tx.Amount <= 0 {
		return errors.New("transaction amount must be positive")
	}

	if tx.Fee < tm.minFeeThreshold {
		return fmt.Errorf("%w: minimum required is %f", ErrFeeTooLow, tm.minFeeThreshold)
	}

	if tx.Sender == "" || tx.Receiver == "" {
		return errors.New("sender and receiver cannot be empty")
	}

	if tx.Nonce <= 0 {
		return errors.New("transaction nonce must be positive")
	}

	// 2) Check transaction size limit
	// Note: In a real implementation we would serialize the TX and check size
	// This is just a placeholder for the pattern
	if len(tx.Data) > tm.maxTxSize {
		return fmt.Errorf("transaction data exceeds maximum size (%d > %d bytes)",
			len(tx.Data), tm.maxTxSize)
	}

	// 3) Ensure transaction timestamp is valid (not too old or in future)
	now := time.Now().Unix()
	if tx.Timestamp > now+300 { // Allow 5 minutes clock drift
		return errors.New("transaction timestamp is in the future")
	}

	if now-tx.Timestamp > int64(tm.maxTxAge.Seconds()) {
		return errors.New("transaction is too old")
	}

	// 4) Check if sender account exists and has sufficient balance
	if !common.CheckAccountBalance(tx.Sender, tx.Amount+tx.Fee) {
		return common.ErrInsufficientFunds
	}

	// 5) Nonce validation - ensure it's not too far in the future
	currentNonce := tm.nonceTracker.GetNonce(tx.Sender)
	if int64(tx.Nonce) <= currentNonce {
		return fmt.Errorf("%w: got %d, current is %d",
			ErrNonceTooLow, tx.Nonce, currentNonce)
	}

	if int64(tx.Nonce) > currentNonce+int64(tm.maxFutureNonce) {
		return fmt.Errorf("nonce too high: got %d, current is %d, max allowed is %d",
			tx.Nonce, currentNonce, currentNonce+int64(tm.maxFutureNonce))
	}

	// 6) (Optional) Additional validation for smart contract related transactions
	if tx.SmartContractID != "" {
		// Basic validation for smart contract transactions
		if len(tx.Data) == 0 {
			return errors.New("smart contract transactions must include data")
		}
	}

	return nil
}

// signTransaction signs the transaction using the sender's private key
func (tm *TransactionManager) signTransaction(tx *common.Transaction) error {
	// 1) Lookup the sender's account to obtain the public key information
	acc := common.GetAccount(tx.Sender)
	if acc == nil {
		return fmt.Errorf("cannot sign transaction: sender account %s not found", tx.Sender)
	}

	// 2) Check if we have a crypto manager available
	if tm.cryptoManager != nil {
		// Create a context with timeout for the signing operation
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Create a message to sign (transaction ID)
		message := []byte(tx.ID)

		// Create a Dilithium key pair with the account's public key
		// Note: We don't have the private key here, but the crypto manager should handle that
		dilithiumPair := &crypto.DilithiumKeyPair{
			PublicKey: acc.PublicKey,
		}

		// Try to sign with the crypto manager
		signature, err := tm.cryptoManager.SignWithContext(ctx, dilithiumPair, message)
		if err == nil && len(signature) > 0 {
			tx.Signature = signature
			return nil
		}

		// Log the error but continue to fallback method
		tm.logger.WithFields(logrus.Fields{
			"txID":  tx.ID,
			"error": err,
		}).Warn("Failed to sign with crypto manager, falling back to environment key")
	}

	// 3) Fallback to environment variable approach
	// Get the encryption key from environment
	encKey := os.Getenv("DIAMANTE_WALLET_ENCRYPTION_KEY")
	if encKey == "" {
		return errors.New("missing encryption key for wallet access (DIAMANTE_WALLET_ENCRYPTION_KEY)")
	}

	// 4) Decrypt the private key using the account's helper method
	privKey, err := acc.GetDecryptedPrivateKey(encKey)
	if err != nil {
		return fmt.Errorf("failed to decrypt private key: %w", err)
	}

	// 5) Sign the transaction ID using the decrypted private key
	signature, err := crypto.SignDataWithDilithium(privKey, []byte(tx.ID))
	if err != nil {
		return fmt.Errorf("%w: %v", ErrSigningFailed, err)
	}

	tx.Signature = signature

	// Log successful signing
	tm.logger.WithFields(logrus.Fields{
		"txID":   tx.ID,
		"sender": tx.Sender,
		"method": "environment key",
	}).Debug("Transaction signed successfully")

	return nil
}

// ProcessTransaction is called after a transaction is finalized by consensus
func (tm *TransactionManager) ProcessTransaction(txID string) error {
	startTime := time.Now()

	// 1) Retrieve the transaction from the pool
	tx, err := tm.pool.GetTransaction(txID)
	if err != nil {
		return fmt.Errorf("transaction not found in pool: %w", err)
	}

	// 2) Apply replay protection
	if err := ReplayProtectionMiddleware(*tx, tm.nonceTracker); err != nil {
		return fmt.Errorf("replay protection error: %w", err)
	}

	// 3) Remove the transaction from the pool
	if err := tm.pool.RemoveTransaction(tx.ID); err != nil {
		// Log but continue - this is a non-fatal error
		tm.logger.WithFields(logrus.Fields{
			"txID":  tx.ID,
			"error": err,
		}).Warn("Failed to remove transaction from pool")
	}

	// 4) Update transaction status
	tx.Status = "committed"

	// 5) Commit the transaction to the ledger
	if err := tm.ledger.AddTransaction(*tx); err != nil {
		return fmt.Errorf("ledger commit error: %w", err)
	}

	// 6) Update sender and receiver balances
	if err := tm.updateBalances(tx); err != nil {
		return fmt.Errorf("balance update error: %w", err)
	}

	// 7) Update stats
	atomic.AddUint64(&tm.stats.TotalConfirmed, 1)
	duration := time.Since(startTime)

	// Update avg confirmation time
	tm.mu.Lock()
	if tm.stats.AvgConfirmationTime == 0 {
		tm.stats.AvgConfirmationTime = duration
	} else {
		tm.stats.AvgConfirmationTime = (tm.stats.AvgConfirmationTime*9 + duration) / 10
	}
	tm.mu.Unlock()

	tm.logger.WithFields(logrus.Fields{
		"txID":     tx.ID,
		"sender":   tx.Sender,
		"receiver": tx.Receiver,
		"amount":   tx.Amount,
		"fee":      tx.Fee,
		"duration": duration,
	}).Info("Transaction processed successfully")

	return nil
}

// updateBalances transfers funds between accounts
func (tm *TransactionManager) updateBalances(tx *common.Transaction) error {
	// 1) Deduct amount and fee from sender
	if err := tm.ledger.UpdateAccountBalance(tx.Sender, -(tx.Amount + tx.Fee)); err != nil {
		return fmt.Errorf("failed to update sender balance: %w", err)
	}

	// 2) Add amount to receiver
	if err := tm.ledger.UpdateAccountBalance(tx.Receiver, tx.Amount); err != nil {
		// This is a critical error - we've already deducted from sender
		tm.logger.WithFields(logrus.Fields{
			"txID":     tx.ID,
			"sender":   tx.Sender,
			"receiver": tx.Receiver,
			"amount":   tx.Amount,
			"error":    err,
		}).Error("CRITICAL: Failed to credit receiver after debiting sender")

		// In production, this would trigger recovery mechanisms
		return fmt.Errorf("failed to update receiver balance: %w", err)
	}

	// 3) Handle fees (could go to validators, treasury, or be burned)
	// This is just a placeholder for where fee distribution would be handled

	return nil
}

// HandleTransactionConflicts resolves double-spend or concurrency issues
func (tm *TransactionManager) HandleTransactionConflicts() {
	if !tm.conflictResolutionEnabled {
		return
	}

	tm.logger.Info("Starting transaction conflict resolution")
	tm.pool.HandleConflicts()
	tm.logger.Info("Transaction conflicts resolved")
}

// GetTransactionStatus checks whether a transaction has been processed
func (tm *TransactionManager) GetTransactionStatus(txID string) (string, error) {
	// 1) Check in the pool first
	if tm.pool.HasTransaction(txID) {
		return "Pending", nil
	}

	// 2) Check if it's been committed to the ledger
	if tm.ledger.IsTransactionCommitted(txID) {
		return "Committed", nil
	}

	// 3) Not found in either location
	return "Unknown", fmt.Errorf("transaction %s not found", txID)
}

// GetPendingTransactions returns transactions that are still in the pool
func (tm *TransactionManager) GetPendingTransactions() ([]*common.Transaction, error) {
	items := tm.pool.GetAllTransactions()
	result := make([]*common.Transaction, len(items))

	for i, item := range items {
		tx := item.tx
		result[i] = &tx
	}

	return result, nil
}

// GetPoolSize returns the current size of the transaction pool
func (tm *TransactionManager) GetPoolSize() int {
	return tm.pool.PoolSize()
}

// GetStats returns current performance metrics
func (tm *TransactionManager) GetStats() TransactionStats {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	// Return a copy to avoid race conditions
	return TransactionStats{
		TotalProcessed:      tm.stats.TotalProcessed,
		TotalRejected:       tm.stats.TotalRejected,
		TotalConfirmed:      tm.stats.TotalConfirmed,
		AvgProcessingTime:   tm.stats.AvgProcessingTime,
		MaxProcessingTime:   tm.stats.MaxProcessingTime,
		AvgConfirmationTime: tm.stats.AvgConfirmationTime,
		LastProcessedTime:   tm.stats.LastProcessedTime,
	}
}

// MonitorTransactionPool monitors the pool and logs metrics periodically
func (tm *TransactionManager) MonitorTransactionPool() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			size := tm.pool.PoolSize()
			tm.logger.WithFields(logrus.Fields{
				"poolSize":            size,
				"processed":           tm.stats.TotalProcessed,
				"confirmed":           tm.stats.TotalConfirmed,
				"rejected":            tm.stats.TotalRejected,
				"avgProcessingTime":   tm.stats.AvgProcessingTime,
				"avgConfirmationTime": tm.stats.AvgConfirmationTime,
			}).Info("Transaction pool status")

		case <-tm.stopChan:
			return
		}
	}
}

// cleanupExpiredTransactions removes transactions that are too old
func (tm *TransactionManager) cleanupExpiredTransactions() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			tm.logger.Info("Starting expired transaction cleanup")
			removed := tm.pool.RemoveExpiredTransactions(tm.maxTxAge)
			tm.logger.WithField("removed", removed).Info("Expired transaction cleanup completed")

		case <-tm.stopChan:
			return
		}
	}
}

// revalidatePendingTransactions periodically checks all pending transactions
func (tm *TransactionManager) revalidatePendingTransactions() {
	ticker := time.NewTicker(15 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			tm.logger.Info("Starting transaction revalidation")

			// Get all transactions from the pool
			txs := tm.pool.GetAllTransactions()
			removed := 0

			for _, item := range txs {
				// Skip revalidation if the transaction recently entered the pool
				if time.Since(time.Unix(item.tx.Timestamp, 0)) < 5*time.Minute {
					continue
				}

				// Revalidate the transaction
				if err := tm.ValidateTransaction(&item.tx); err != nil {
					// Transaction no longer valid, remove it
					if removeErr := tm.pool.RemoveTransaction(item.tx.ID); removeErr == nil {
						removed++
						tm.logger.WithFields(logrus.Fields{
							"txID":   item.tx.ID,
							"reason": err.Error(),
						}).Info("Removed invalid transaction during revalidation")
					}
				}
			}

			tm.logger.WithField("removed", removed).Info("Transaction revalidation completed")

		case <-tm.stopChan:
			return
		}
	}
}

// GetTransactionsByAccount returns transactions associated with an account
func (tm *TransactionManager) GetTransactionsByAccount(accountID string, limit int) ([]*common.Transaction, error) {
	// This would typically query the ledger for historical transactions
	// For pending transactions, we can query the pool

	// Placeholder for the pattern - in production, this would query the ledger API
	pendingTxs, _ := tm.GetPendingTransactionsByAccount(accountID)

	return pendingTxs, nil
}

// GetPendingTransactionsByAccount returns pending transactions for an account
func (tm *TransactionManager) GetPendingTransactionsByAccount(accountID string) ([]*common.Transaction, error) {
	if accountID == "" {
		return nil, errors.New("account ID cannot be empty")
	}

	var result []*common.Transaction
	txs := tm.pool.GetAllTransactions()

	for _, item := range txs {
		if item.tx.Sender == accountID || item.tx.Receiver == accountID {
			tx := item.tx
			result = append(result, &tx)
		}
	}

	return result, nil
}

// EstimateFee provides a fee estimate based on current network conditions
func (tm *TransactionManager) EstimateFee() float64 {
	// Simple placeholder implementation
	// A real implementation would consider:
	// - Current pool size/backlog
	// - Recent inclusion times
	// - Recent minimum fees that got included

	size := tm.pool.PoolSize()
	maxSize := tm.pool.GetMaxPoolSize()
	utilization := float64(size) / float64(maxSize)

	// Base fee + premium based on pool utilization
	return tm.minFeeThreshold * (1.0 + utilization)
}

// SuggestNonce returns a suggested nonce for a new transaction
func (tm *TransactionManager) SuggestNonce(sender string) int64 {
	return tm.nonceTracker.GetNonce(sender) + 1
}
