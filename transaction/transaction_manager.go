// transaction/transaction_manager.go

package transaction

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"diamante/common"
	"diamante/consensus"
	"diamante/crypto"
	"diamante/fees"
	"github.com/sirupsen/logrus"
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
	contractValidator         *ContractValidator           // Smart contract validator
	metricsCollector          *TransactionMetricsCollector // Prometheus metrics

	feeDistributor fees.FeeDistributorAPI

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

	// Pending nonce tracker for parallel submissions
	pendingNonceMu sync.RWMutex
	pendingNonces  map[string]int64 // sender -> highest pending nonce

	// Batch processing
	batchQueue     chan *common.Transaction
	batchSize      int
	batchTimeout   time.Duration
	batchProcessor *sync.WaitGroup
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

// WithFeeDistributor sets a fee distributor on creation.
func WithFeeDistributor(fd fees.FeeDistributorAPI) TransactionManagerOption {
	return func(tm *TransactionManager) {
		tm.feeDistributor = fd
	}
}

// WithMetricsCollector sets a metrics collector for Prometheus metrics
func WithMetricsCollector(mc *TransactionMetricsCollector) TransactionManagerOption {
	return func(tm *TransactionManager) {
		tm.metricsCollector = mc
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
	// Use PersistentNonceTracker if ledgerAPI is available, otherwise default
	var nonceTracker NonceTracker
	if ledgerAPI != nil {
		nonceTracker = NewPersistentNonceTracker(ledgerAPI)
	} else {
		nonceTracker = NewDefaultNonceTracker()
	}

	tm := &TransactionManager{
		pool:                      txPool,
		minFeeThreshold:           minFeeThreshold,
		nonceTracker:              nonceTracker,
		conflictResolutionEnabled: conflictResolutionEnabled,
		ledger:                    ledgerAPI,
		logger:                    logrus.New(), // Default logger
		maxTxsPerBlock:            5000,         // Default values
		maxTxSize:                 1024 * 1024,  // 1MB
		maxFutureNonce:            10,           // Allow up to 10 nonces in the future
		maxTxAge:                  24 * time.Hour,
		stopChan:                  make(chan struct{}),
		pendingNonces:             make(map[string]int64),
		// Batch processing
		batchQueue:     make(chan *common.Transaction, 1000),
		batchSize:      100,
		batchTimeout:   100 * time.Millisecond,
		batchProcessor: &sync.WaitGroup{},
	}

	// Apply options
	for _, option := range options {
		option(tm)
	}

	// Create default metrics collector if none provided
	if tm.metricsCollector == nil {
		tm.metricsCollector = NewTransactionMetricsCollector("")
	}

	return tm
}

// SetFeeDistributor assigns a fee distributor after creation.
func (tm *TransactionManager) SetFeeDistributor(fd fees.FeeDistributorAPI) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.feeDistributor = fd
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

	// Start batch processor
	tm.batchProcessor.Add(1)
	go tm.processBatchQueue()

	return nil
}

// Stop halts all background processes
func (tm *TransactionManager) Stop() error {
	if !atomic.CompareAndSwapInt32(&tm.isRunning, 1, 0) {
		return errors.New("transaction manager is not running")
	}

	tm.logger.Info("Stopping transaction manager")
	close(tm.stopChan)

	// Wait for batch processor to finish
	tm.batchProcessor.Wait()

	// Close the transaction pool
	if err := tm.pool.Close(); err != nil {
		tm.logger.WithError(err).Error("Error closing transaction pool")
	}

	return nil
}

// processBatchQueue processes transactions in batches for better throughput
func (tm *TransactionManager) processBatchQueue() {
	defer tm.batchProcessor.Done()

	ticker := time.NewTicker(tm.batchTimeout)
	defer ticker.Stop()

	batch := make([]common.Transaction, 0, tm.batchSize)

	for {
		select {
		case <-tm.stopChan:
			// Process remaining batch before exiting
			if len(batch) > 0 {
				tm.processBatch(batch)
			}
			return

		case tx := <-tm.batchQueue:
			if tx != nil {
				batch = append(batch, *tx)
				if len(batch) >= tm.batchSize {
					tm.processBatch(batch)
					batch = make([]common.Transaction, 0, tm.batchSize)
				}
			}

		case <-ticker.C:
			// Process partial batch on timeout
			if len(batch) > 0 {
				tm.processBatch(batch)
				batch = make([]common.Transaction, 0, tm.batchSize)
			}
		}
	}
}

// processBatch processes a batch of transactions
func (tm *TransactionManager) processBatch(batch []common.Transaction) {
	if len(batch) == 0 {
		return
	}

	start := time.Now()

	// Submit batch to pool for parallel validation
	if err := tm.pool.AddTransactionBatch(batch); err != nil {
		tm.logger.WithFields(logrus.Fields{
			"batch_size": len(batch),
			"error":      err,
		}).Error("Batch processing failed")
	}

	duration := time.Since(start)
	tm.logger.WithFields(logrus.Fields{
		"batch_size":  len(batch),
		"duration_ms": duration.Milliseconds(),
		"throughput":  float64(len(batch)) / duration.Seconds(),
	}).Debug("Batch processed")
}

// SubmitTransactionAsync submits a transaction for async batch processing
func (tm *TransactionManager) SubmitTransactionAsync(tx *common.Transaction) error {
	select {
	case tm.batchQueue <- tx:
		return nil
	case <-tm.stopChan:
		return errors.New("transaction manager is stopped")
	default:
		// Queue is full, fall back to direct submission
		return tm.pool.AddTransaction(*tx)
	}
}

// CreateTransaction constructs a new transaction, validates it, signs it, and adds to the pool.
func (tm *TransactionManager) CreateTransaction(
	sender, receiver string,
	amount, fee float64,
	data []byte,
) (*common.Transaction, error) {
	startTime := consensus.ConsensusNow()

	// Determine transaction type for metrics
	txType := "transfer"
	if len(data) > 0 {
		txType = "data"
	}

	// 1) Validate inputs
	if sender == "" || receiver == "" {
		tm.metricsCollector.RecordTransactionRejected("empty_address")
		return nil, errors.New("sender and receiver cannot be empty")
	}

	// Normalize addresses
	sender = common.NormalizeAddress(sender)
	receiver = common.NormalizeAddress(receiver)

	if amount <= 0 {
		tm.metricsCollector.RecordTransactionRejected("invalid_amount")
		return nil, errors.New("amount must be positive")
	}

	if fee < tm.minFeeThreshold {
		tm.metricsCollector.RecordTransactionRejected("fee_too_low")
		return nil, fmt.Errorf("%w: minimum required is %f", ErrFeeTooLow, tm.minFeeThreshold)
	}

	// 2) Generate a unique transaction ID.
	txID := GenerateTransactionID(sender, receiver, amount)
	timestamp := consensus.ConsensusUnix()

	// 3) Get effective next nonce
	effectiveNonce, err := tm.GetEffectiveNextNonce(sender)
	if err != nil {
		tm.metricsCollector.RecordTransactionRejected("nonce_error")
		return nil, fmt.Errorf("failed to get effective nonce: %w", err)
	}

	// 4) Build the transaction
	tx := &common.Transaction{
		ID:        txID,
		Sender:    sender,
		Receiver:  receiver,
		Amount:    amount,
		Fee:       fee,
		Data:      data,
		Timestamp: timestamp,
		Nonce:     int(effectiveNonce),
		Status:    "pending",
	}

	// Record validation start
	validationStart := consensus.ConsensusNow()

	// 5) Validate the transaction pre-signature
	if err := tm.ValidateTransaction(tx); err != nil {
		tm.logRejection("pre-signature validation failed", tx, err)
		tm.metricsCollector.RecordValidationDuration(txType, consensus.ConsensusSince(validationStart))
		return nil, fmt.Errorf("validation error: %w", err)
	}

	tm.metricsCollector.RecordValidationDuration(txType, consensus.ConsensusSince(validationStart))

	// Record signing start
	signingStart := consensus.ConsensusNow()

	// 6) Sign the transaction
	if err := tm.signTransaction(tx); err != nil {
		tm.logRejection("signing failed", tx, err)
		return nil, fmt.Errorf("signing error: %w", err)
	}

	tm.metricsCollector.RecordSigningDuration(consensus.ConsensusSince(signingStart))

	// 7) Add the transaction to the pool
	if err := tm.pool.AddTransaction(*tx); err != nil {
		tm.logRejection("pool add failed", tx, err)

		// Provide more specific error messages
		switch {
		case errors.Is(err, ErrDuplicateTx):
			return nil, fmt.Errorf("transaction already exists in pool with ID %s", tx.ID)
		case errors.Is(err, ErrPoolFull):
			return nil, fmt.Errorf("transaction pool is full (max %d transactions)", tm.pool.GetMaxPoolSize())
		case errors.Is(err, ErrTxExpired):
			return nil, fmt.Errorf("transaction has expired (timestamp %d)", tx.Timestamp)
		default:
			return nil, fmt.Errorf("pool add error: %w", err)
		}
	}

	// 7b) Update pending nonce for parallel submission support
	tm.pendingNonceMu.Lock()
	tm.pendingNonces[sender] = int64(tx.Nonce)
	tm.pendingNonceMu.Unlock()

	// 8) Don't update nonce tracker here - wait until transaction is included in a block
	// This prevents issues with re-validation during block selection
	// tm.nonceTracker.SetNonce(sender, int64(tx.Nonce))

	// 9) Update stats and metrics
	tm.updateProcessingStats(startTime)
	tm.metricsCollector.RecordTransactionProcessed(txType, "created", consensus.ConsensusSince(startTime))
	tm.metricsCollector.RecordFeePerByte(txType, calculateFeePerByte(tx))
	tm.metricsCollector.UpdatePoolSize(tm.pool.PoolSize())

	tm.logger.WithFields(logrus.Fields{
		"txID":      tx.ID,
		"sender":    tx.Sender,
		"receiver":  tx.Receiver,
		"amount":    tx.Amount,
		"fee":       tx.Fee,
		"nonce":     tx.Nonce,
		"timestamp": time.Unix(tx.Timestamp, 0),
		"duration":  consensus.ConsensusSince(startTime),
	}).Info("txPoolAdd from=api status=pending")

	return tx, nil
}

// CreateTransactionWithContext is like CreateTransaction but respects context deadlines
func (tm *TransactionManager) CreateTransactionWithContext(
	ctx context.Context,
	sender, receiver string,
	amount, fee float64,
	data []byte,
) (*common.Transaction, error) {
	// Create a channel to receive the result
	type result struct {
		tx  *common.Transaction
		err error
	}
	resultChan := make(chan result, 1)

	// Run CreateTransaction in a goroutine
	go func() {
		tx, err := tm.CreateTransaction(sender, receiver, amount, fee, data)
		select {
		case resultChan <- result{tx: tx, err: err}:
		case <-ctx.Done():
			// Context cancelled while trying to send result
			if tx != nil && err == nil {
				// Transaction was created successfully but context expired
				// Try to remove it from the pool to avoid orphaned transactions
				if removeErr := tm.pool.RemoveTransaction(tx.ID); removeErr != nil {
					tm.logger.WithFields(logrus.Fields{
						"txID":  tx.ID,
						"error": removeErr,
					}).Warn("Failed to remove orphaned transaction after context cancellation")
				}
			}
		}
	}()

	// Wait for either the result or context cancellation
	select {
	case res := <-resultChan:
		return res.tx, res.err
	case <-ctx.Done():
		// Context expired - transaction creation is taking too long
		tm.logger.WithField("timeout", ctx.Err()).Warn("Transaction creation timed out")
		return nil, ctx.Err()
	}
}

// logRejection consistently logs transaction rejections
func (tm *TransactionManager) logRejection(reason string, tx *common.Transaction, err error) {
	atomic.AddUint64(&tm.stats.TotalRejected, 1)

	// Record rejection in metrics
	tm.metricsCollector.RecordTransactionRejected(reason)

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

	duration := consensus.ConsensusSince(startTime)

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

	tm.stats.LastProcessedTime = consensus.ConsensusNow()
}

// validateTransactionForBlock performs validation for transactions already in the pool
// It skips nonce validation since that was already done when the transaction was added
func (tm *TransactionManager) validateTransactionForBlock(tx *common.Transaction) error {
	// 1) Basic nil check
	if tx == nil {
		return errors.New("transaction is nil")
	}

	// 2) Basic field validation
	if tx.Sender == "" || tx.Receiver == "" {
		return errors.New("sender and receiver are required")
	}
	if tx.ID == "" {
		return errors.New("transaction ID is required")
	}

	// 3) Check transaction size limit
	txBytes, err := tm.serializeTransaction(tx)
	if err != nil {
		return fmt.Errorf("failed to serialize transaction: %w", err)
	}
	if len(txBytes) > int(tm.maxTxSize) {
		return fmt.Errorf("transaction size %d exceeds maximum %d bytes",
			len(txBytes), tm.maxTxSize)
	}

	// 4) Validate amounts and fees
	if tx.Amount < 0 {
		return errors.New("transaction amount cannot be negative")
	}
	if tx.Fee < 0 {
		return errors.New("transaction fee cannot be negative")
	}
	if tx.Fee < tm.minFeeThreshold {
		return fmt.Errorf("transaction fee %f is below minimum %f",
			tx.Fee, tm.minFeeThreshold)
	}

	// 5) Balance check (if ledger available)
	if tm.ledger != nil {
		// Normalize sender address for consistency
		normalizedSender := common.NormalizeAddress(tx.Sender)
		balance, err := tm.ledger.GetBalance(normalizedSender)
		if err != nil {
			return fmt.Errorf("failed to get sender balance: %w", err)
		}

		requiredBalance := tx.Amount + tx.Fee
		if balance < requiredBalance {
			return fmt.Errorf("insufficient balance: have %f, need %f",
				balance, requiredBalance)
		}
	}

	// Skip nonce validation - it was already validated when added to pool
	// Skip signature validation - signatures don't change
	// Skip timestamp validation - already checked when added

	return nil
}

// ValidateTransaction performs comprehensive transaction checks
func (tm *TransactionManager) ValidateTransaction(tx *common.Transaction) error {
	// Debug log entry
	tm.logger.Debug("ValidateTransaction: Entry", "txID", tx.ID)
	defer tm.logger.Debug("ValidateTransaction: Exit", "txID", tx.ID)

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
	// Serialize transaction to check actual size
	txBytes, err := tm.serializeTransaction(tx)
	if err != nil {
		return fmt.Errorf("failed to serialize transaction: %w", err)
	}
	if len(txBytes) > tm.maxTxSize {
		return fmt.Errorf("transaction data exceeds maximum size (%d > %d bytes)",
			len(txBytes), tm.maxTxSize)
	}

	// 3) Ensure transaction timestamp is valid (not too old or in future)
	now := consensus.ConsensusUnix()
	maxSkew := common.GetMaxTxClockSkew()
	if tx.Timestamp > now+int64(maxSkew.Seconds()) {
		return fmt.Errorf("transaction timestamp is in the future (max allowed: %d seconds)", int(maxSkew.Seconds()))
	}

	if now-tx.Timestamp > int64(tm.maxTxAge.Seconds()) {
		return errors.New("transaction is too old")
	}

	// 4) Normalize sender and receiver addresses
	normalizedSender := common.NormalizeAddress(tx.Sender)
	normalizedReceiver := common.NormalizeAddress(tx.Receiver)

	// Update transaction with normalized addresses
	tx.Sender = normalizedSender
	tx.Receiver = normalizedReceiver

	// 5) Check if sender account exists and has sufficient balance using ledger
	if tm.ledger != nil {
		balance, err := tm.ledger.GetBalance(normalizedSender)
		if err != nil {
			// Account doesn't exist or error getting balance
			return fmt.Errorf("failed to get sender balance: %w", err)
		}

		requiredBalance := tx.Amount + tx.Fee
		if balance < requiredBalance {
			return fmt.Errorf("%w: have %f, need %f", common.ErrInsufficientFunds, balance, requiredBalance)
		}
	} else {
		// Fallback to common package if ledger not available (for backwards compatibility)
		if !common.CheckAccountBalance(normalizedSender, tx.Amount+tx.Fee) {
			return common.ErrInsufficientFunds
		}
	}

	// 6) Nonce validation - use GetEffectiveNextNonce for consistency
	expectedNonce, err := tm.GetEffectiveNextNonce(normalizedSender)
	if err != nil {
		return fmt.Errorf("failed to get effective nonce: %w", err)
	}

	// If nonce is 0, it should be auto-filled by the API layer
	if tx.Nonce == 0 {
		// Allow API to set it
		return nil
	}

	// Check if provided nonce matches expected
	if int64(tx.Nonce) != expectedNonce {
		// Get detailed information for error message
		var committedNonce int64 = 0
		if tm.ledger != nil {
			balance, err := tm.ledger.GetBalance(normalizedSender)
			if err == nil && balance > 0 {
				committedNonce = tm.nonceTracker.GetNonce(normalizedSender)
			}
		}
		poolMaxNonce := tm.pool.GetMaxNonceForSender(normalizedSender)

		tm.pendingNonceMu.RLock()
		pendingNonce, hasPending := tm.pendingNonces[normalizedSender]
		tm.pendingNonceMu.RUnlock()
		if !hasPending {
			pendingNonce = 0
		}

		return fmt.Errorf("INVALID_NONCE: got=%d expected=%d (committed=%d poolMax=%d pending=%d)",
			tx.Nonce, expectedNonce, committedNonce, poolMaxNonce, pendingNonce)
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
		// Only allow development signatures in development mode
		if !common.IsDevelopmentMode() {
			return fmt.Errorf("no encryption key available and development signatures not allowed in production")
		}

		// For development/testing, create a simple deterministic signature
		// based on transaction data to ensure consistency
		hasher := sha256.New()
		hasher.Write([]byte(tx.ID))
		hasher.Write([]byte(tx.Sender))
		hasher.Write([]byte(tx.Receiver))
		hasher.Write([]byte(fmt.Sprintf("%.8f", tx.Amount)))
		hasher.Write([]byte(fmt.Sprintf("%.8f", tx.Fee)))
		hasher.Write([]byte(fmt.Sprintf("%d", tx.Nonce)))
		hasher.Write([]byte(fmt.Sprintf("%d", tx.Timestamp)))
		hash := hasher.Sum(nil)

		// Create a mock signature that's deterministic but looks like a real one
		tx.Signature = append([]byte("DEV_SIG:"), hash...)
		tm.logger.Warn("Using development signature mode - not for production")
		return nil
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
	startTime := consensus.ConsensusNow()

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
	duration := consensus.ConsensusSince(startTime)

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

// ProcessTransactionWithFees processes a transaction and distributes the fee.
func (tm *TransactionManager) ProcessTransactionWithFees(txID string, blockProducer [32]byte) error {
	if err := tm.ProcessTransaction(txID); err != nil {
		return err
	}

	if tm.feeDistributor != nil {
		tx, err := tm.ledger.GetTransaction(txID)
		if err == nil {
			if err := tm.feeDistributor.DistributeFees(tx, blockProducer); err != nil {
				tm.logger.WithField("txID", txID).Errorf("fee distribution failed: %v", err)
			}
		} else {
			tm.logger.WithField("txID", txID).Errorf("could not retrieve transaction for fee distribution: %v", err)
		}
	}

	return nil
}

// balanceUpdate tracks a balance update for rollback capability
type balanceUpdate struct {
	account string
	amount  float64
	applied bool
}

// updateBalances transfers funds between accounts with transactional consistency
func (tm *TransactionManager) updateBalances(tx *common.Transaction) error {
	updates := []balanceUpdate{
		{account: tx.Sender, amount: -(tx.Amount + tx.Fee), applied: false},
		{account: tx.Receiver, amount: tx.Amount, applied: false},
	}

	// Track which updates have been applied for rollback
	var appliedUpdates []balanceUpdate

	// Apply balance updates with rollback on failure
	for i, update := range updates {
		if err := tm.ledger.UpdateAccountBalance(update.account, update.amount); err != nil {
			// Critical error - need to rollback any applied updates
			tm.logger.WithFields(logrus.Fields{
				"txID":       tx.ID,
				"account":    update.account,
				"amount":     update.amount,
				"step":       i + 1,
				"totalSteps": len(updates),
				"error":      err,
			}).Error("Balance update failed, initiating rollback")

			// Rollback any successfully applied updates
			rollbackErr := tm.rollbackBalanceUpdates(appliedUpdates, tx.ID)
			if rollbackErr != nil {
				// Double fault - both update and rollback failed
				tm.logger.WithFields(logrus.Fields{
					"txID":          tx.ID,
					"originalError": err,
					"rollbackError": rollbackErr,
				}).Fatal("CRITICAL: Balance update rollback failed - manual intervention required")

				// In production, this would:
				// 1. Alert operations team immediately
				// 2. Mark accounts as frozen
				// 3. Log to audit trail for manual reconciliation
				// 4. Potentially halt further transactions

				return fmt.Errorf("critical balance update failure with rollback error: original=%w, rollback=%v", err, rollbackErr)
			}

			return fmt.Errorf("balance update failed and rolled back: %w", err)
		}

		// Mark this update as applied
		updates[i].applied = true
		appliedUpdates = append(appliedUpdates, updates[i])
	}

	// All updates successful
	tm.logger.WithFields(logrus.Fields{
		"txID":     tx.ID,
		"sender":   tx.Sender,
		"receiver": tx.Receiver,
		"amount":   tx.Amount,
		"fee":      tx.Fee,
	}).Debug("Balance updates completed successfully")

	return nil
}

// rollbackBalanceUpdates reverses balance updates in case of failure
func (tm *TransactionManager) rollbackBalanceUpdates(updates []balanceUpdate, txID string) error {
	tm.logger.WithFields(logrus.Fields{
		"txID":       txID,
		"numUpdates": len(updates),
	}).Warn("Starting balance update rollback")

	var rollbackErrors []error

	// Reverse the updates in opposite order
	for i := len(updates) - 1; i >= 0; i-- {
		update := updates[i]

		// Reverse the amount
		reverseAmount := -update.amount

		tm.logger.WithFields(logrus.Fields{
			"txID":           txID,
			"account":        update.account,
			"originalAmount": update.amount,
			"reverseAmount":  reverseAmount,
		}).Debug("Rolling back balance update")

		if err := tm.ledger.UpdateAccountBalance(update.account, reverseAmount); err != nil {
			rollbackErrors = append(rollbackErrors, fmt.Errorf("failed to rollback account %s: %w", update.account, err))
			tm.logger.WithError(err).WithFields(logrus.Fields{
				"txID":    txID,
				"account": update.account,
			}).Error("Failed to rollback balance update")
		}
	}

	if len(rollbackErrors) > 0 {
		return fmt.Errorf("rollback completed with %d errors: %v", len(rollbackErrors), rollbackErrors)
	}

	tm.logger.WithField("txID", txID).Info("Balance update rollback completed successfully")
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
	if tm.ledger != nil && tm.ledger.IsTransactionCommitted(txID) {
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

// GetTransactionFromPool retrieves a specific transaction from the pool by ID
func (tm *TransactionManager) GetTransactionFromPool(txID string) (*common.Transaction, bool) {
	tx, err := tm.pool.GetTransaction(txID)
	if err != nil {
		return nil, false
	}
	return tx, true
}

// GetPendingCount returns the number of pending transactions in the pool
func (tm *TransactionManager) GetPendingCount() int {
	items := tm.pool.GetAllTransactions()
	return len(items)
}

// RemoveTransactionFromPool removes a transaction from the pool after it's been included in a block
func (tm *TransactionManager) RemoveTransactionFromPool(txID string) error {
	// Get the transaction before removing it
	tx, _ := tm.pool.GetTransaction(txID)
	if tx != nil {
		// Update nonce tracker when transaction is included in a block
		tm.nonceTracker.SetNonce(tx.Sender, int64(tx.Nonce))
		tm.logger.Debug("Updated nonce tracker after block inclusion",
			"sender", tx.Sender,
			"nonce", tx.Nonce)

		// Clear pending nonce if this was the highest pending
		normalizedSender := common.NormalizeAddress(tx.Sender)
		tm.pendingNonceMu.Lock()
		if pendingNonce, exists := tm.pendingNonces[normalizedSender]; exists && pendingNonce == int64(tx.Nonce) {
			// This was the highest pending nonce, clear it
			delete(tm.pendingNonces, normalizedSender)
		}
		tm.pendingNonceMu.Unlock()
	}

	return tm.pool.RemoveTransaction(txID)
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
	// Get both pending and historical transactions
	var allTxs []*common.Transaction

	// First get pending transactions from pool
	pendingTxs, err := tm.GetPendingTransactionsByAccount(accountID)
	if err != nil {
		tm.logger.WithError(err).Error("Failed to get pending transactions")
	} else {
		allTxs = append(allTxs, pendingTxs...)
	}

	// Then get historical transactions from ledger if available
	if tm.ledger != nil {
		// Use the ledger API to get historical transactions
		historicalTxs, err := tm.ledger.GetAccountTransactions(accountID, limit, 0)
		if err != nil {
			tm.logger.WithError(err).Error("Failed to get historical transactions")
		} else {
			// Convert to transaction pointers
			for _, tx := range historicalTxs {
				txCopy := tx // Create a copy to avoid pointer issues
				allTxs = append(allTxs, &txCopy)
			}
		}
	}

	// Apply limit if specified
	if limit > 0 && len(allTxs) > limit {
		allTxs = allTxs[:limit]
	}

	return allTxs, nil
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

// GetEffectiveNextNonce returns the effective next nonce for a sender
// This is the single source of truth for nonce calculation
func (tm *TransactionManager) GetEffectiveNextNonce(address string) (int64, error) {
	// Normalize address once
	normalizedAddress := common.NormalizeAddress(address)

	// Get committed nonce from ledger/store (last committed)
	var committedNonce int64 = 0
	if tm.ledger != nil {
		// Try to get balance to see if account exists
		balance, err := tm.ledger.GetBalance(normalizedAddress)
		if err == nil && balance > 0 {
			// Account exists, use nonce tracker which should be persistent
			committedNonce = tm.nonceTracker.GetNonce(normalizedAddress)
		}
		// If account doesn't exist or has no balance, committedNonce stays 0
	} else {
		// Fallback to nonce tracker if ledger not available
		committedNonce = tm.nonceTracker.GetNonce(normalizedAddress)
	}

	// Get max nonce from pool
	poolMaxNonce := tm.pool.GetMaxNonceForSender(normalizedAddress)

	// Get pending nonce (guarded by mutex)
	tm.pendingNonceMu.RLock()
	pendingNonce, hasPending := tm.pendingNonces[normalizedAddress]
	tm.pendingNonceMu.RUnlock()

	// If no pending nonce, use 0 for comparison
	if !hasPending {
		pendingNonce = 0
	}

	// Calculate effective next nonce as max(committed+1, poolMax+1, pending+1)
	effectiveNext := committedNonce + 1
	if poolMaxNonce+1 > effectiveNext {
		effectiveNext = poolMaxNonce + 1
	}
	if hasPending && pendingNonce+1 > effectiveNext {
		effectiveNext = pendingNonce + 1
	}

	// Log the calculation for debugging
	tm.logger.WithFields(logrus.Fields{
		"address":       normalizedAddress,
		"committed":     committedNonce,
		"poolMax":       poolMaxNonce,
		"pending":       pendingNonce,
		"effectiveNext": effectiveNext,
	}).Debug("nonceCalc")

	return effectiveNext, nil
}

// SuggestNonce returns a suggested nonce for a new transaction
// Deprecated: Use GetEffectiveNextNonce instead
func (tm *TransactionManager) SuggestNonce(sender string) int64 {
	nonce, _ := tm.GetEffectiveNextNonce(sender)
	return nonce
}

// SelectTransactionsForBlock selects up to maxCount transactions for inclusion in a block
// Transactions are selected based on PoH ordering for deterministic sequencing
func (tm *TransactionManager) SelectTransactionsForBlock(maxCount int, maxGas uint64) ([]*common.Transaction, uint64) {
	if maxCount <= 0 {
		tm.mu.RLock()
		maxCount = tm.maxTxsPerBlock
		tm.mu.RUnlock()
	}

	// Don't hold lock while flushing PoH
	if err := tm.pool.FlushPoHBatch(); err != nil {
		tm.logger.WithError(err).Warn("Failed to flush PoH batch before block production")
	}

	// Get transactions in PoH order for deterministic block production
	// Only hold lock briefly to copy references
	transactions := tm.pool.GetTransactionsByPoHOrder(maxCount)
	if len(transactions) == 0 {
		return []*common.Transaction{}, 0
	}

	// Convert to pointer slice and calculate gas
	selected := make([]*common.Transaction, 0, len(transactions))
	totalGasUsed := uint64(0)

	for i := range transactions {
		tx := &transactions[i]

		// Basic gas estimation (21000 for simple transfer)
		estimatedGas := uint64(21000)
		if len(tx.Data) > 0 {
			// Add gas for data
			estimatedGas += uint64(len(tx.Data) * 16)
		}

		// Check if we have enough gas budget
		if totalGasUsed+estimatedGas > maxGas {
			continue
		}

		// Validate transaction is still valid for block inclusion
		// Skip nonce validation since it was already validated when added to pool
		tm.logger.Debug("Validating transaction for block inclusion",
			"txID", tx.ID,
			"nonce", tx.Nonce,
			"currentNonce", tm.nonceTracker.GetNonce(tx.Sender))

		if err := tm.validateTransactionForBlock(tx); err != nil {
			tm.logger.WithError(err).Warn("Skipping invalid transaction in block selection", "txID", tx.ID)
			continue
		}

		selected = append(selected, tx)
		totalGasUsed += estimatedGas
	}

	tm.logger.Info("Selected transactions for block",
		"requested", maxCount,
		"selected", len(selected),
		"totalGasUsed", totalGasUsed,
		"poolSize", len(transactions))

	return selected, totalGasUsed
}

// GetPendingAmount returns the total amount of pending transactions for an account
func (tm *TransactionManager) GetPendingAmount(accountID string) float64 {
	if accountID == "" {
		return 0.0
	}

	var pendingAmount float64
	txs := tm.pool.GetAllTransactions()

	for _, item := range txs {
		if item.tx.Sender == accountID {
			// Include both amount and fee for sender
			pendingAmount += item.tx.Amount + item.tx.Fee
		}
	}

	return pendingAmount
}

// serializeTransaction serializes a transaction to calculate its size
func (tm *TransactionManager) serializeTransaction(tx *common.Transaction) ([]byte, error) {
	// Simple serialization for size calculation
	// In production, this would use a proper serialization format like protobuf
	data := fmt.Sprintf("%s:%s:%f:%f:%d:%x:%d",
		tx.Sender,
		tx.Receiver,
		tx.Amount,
		tx.Fee,
		tx.Nonce,
		tx.Data,
		tx.Timestamp)
	return []byte(data), nil
}

// AddIncomingTransaction adds a transaction received from the network to the pool
// This method preserves the original transaction ID and signature without modification
func (tm *TransactionManager) AddIncomingTransaction(tx *common.Transaction) error {
	if tx == nil {
		return errors.New("transaction is nil")
	}

	startTime := consensus.ConsensusNow()

	// Normalize transaction ID for consistent handling
	tx.ID = common.NormalizeTransactionID(tx.ID)

	// Determine transaction type for metrics
	txType := "transfer"
	if len(tx.Data) > 0 {
		txType = "data"
	}

	// Log transaction receipt from network with grep-friendly format
	tm.logger.WithFields(logrus.Fields{
		"txID":     tx.ID,
		"sender":   tx.Sender,
		"receiver": tx.Receiver,
		"amount":   tx.Amount,
		"fee":      tx.Fee,
		"nonce":    tx.Nonce,
	}).Info("txReceived from=network")

	tm.logger.WithFields(logrus.Fields{
		"txID":     tx.ID,
		"sender":   tx.Sender,
		"receiver": tx.Receiver,
		"amount":   tx.Amount,
		"fee":      tx.Fee,
		"nonce":    tx.Nonce,
	}).Debug("Processing incoming network transaction")

	// Check if transaction already exists
	if existing, _ := tm.GetTransactionStatus(tx.ID); existing != "" {
		tm.logger.Debug("Transaction already exists", "txID", tx.ID, "status", existing)
		return ErrTxAlreadyExists
	}

	// Validate transaction structure and signature
	if err := tm.ValidateTransaction(tx); err != nil {
		tm.logRejection("validation failed", tx, err)
		tm.metricsCollector.RecordValidationDuration(txType, consensus.ConsensusSince(startTime))
		return fmt.Errorf("validation error: %w", err)
	}

	// Verify the signature (already done in peer.go but double-check)
	if err := common.VerifyTransactionSignature(tx); err != nil {
		tm.logRejection("signature verification failed", tx, err)
		return fmt.Errorf("signature verification failed: %w", err)
	}

	// Add to pool without modifying the transaction
	if err := tm.pool.AddTransaction(*tx); err != nil {
		tm.logRejection("pool add failed", tx, err)

		// Provide specific error messages
		switch {
		case errors.Is(err, ErrDuplicateTx):
			return fmt.Errorf("transaction already exists in pool with ID %s", tx.ID)
		case errors.Is(err, ErrPoolFull):
			return fmt.Errorf("transaction pool is full (max %d transactions)", tm.pool.GetMaxPoolSize())
		case errors.Is(err, ErrTxExpired):
			return fmt.Errorf("transaction has expired (timestamp %d)", tx.Timestamp)
		default:
			return fmt.Errorf("pool add error: %w", err)
		}
	}

	// Update pending nonce for parallel submission support
	normalizedSender := common.NormalizeAddress(tx.Sender)
	tm.pendingNonceMu.Lock()
	tm.pendingNonces[normalizedSender] = int64(tx.Nonce)
	tm.pendingNonceMu.Unlock()

	// Update metrics
	tm.updateProcessingStats(startTime)
	tm.metricsCollector.RecordTransactionProcessed(txType, "received", consensus.ConsensusSince(startTime))
	tm.metricsCollector.RecordFeePerByte(txType, calculateFeePerByte(tx))
	tm.metricsCollector.UpdatePoolSize(tm.pool.PoolSize())

	tm.logger.WithFields(logrus.Fields{
		"txID":     tx.ID,
		"sender":   tx.Sender,
		"receiver": tx.Receiver,
		"amount":   tx.Amount,
		"fee":      tx.Fee,
		"nonce":    tx.Nonce,
		"duration": consensus.ConsensusSince(startTime),
	}).Info("txPoolAdd from=network status=pending")

	return nil
}
