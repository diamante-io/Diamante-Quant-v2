package transaction

import (
	"container/heap"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"diamante/common"
	"diamante/consensus"
	"diamante/crypto"

	"github.com/sirupsen/logrus"
)

var (
	// ErrPoolFull indicates the transaction pool is at capacity
	ErrPoolFull = errors.New("transaction pool is full")

	// ErrTxNotFound indicates a transaction wasn't found in the pool
	ErrTxNotFound = errors.New("transaction not found in pool")

	// ErrDuplicateTx indicates a transaction already exists in the pool
	ErrDuplicateTx = errors.New("duplicate transaction in pool")

	// ErrTxExpired indicates a transaction has expired
	ErrTxExpired = errors.New("transaction has expired")

	// ErrInvalidTx indicates a transaction is invalid
	ErrInvalidTx = errors.New("transaction is invalid")
)

// TransactionPool manages unconfirmed transactions in a priority queue.
type TransactionPool struct {
	mu                        sync.RWMutex
	pendingTxs                *PriorityQueue
	txMap                     map[string]*TransactionItem
	maxPoolSize               int
	txTimeout                 time.Duration
	minFee                    float64
	maxFee                    float64
	conflictResolutionEnabled bool
	expirationDuration        time.Duration
	poolMetrics               PoolMetrics
	logger                    *logrus.Logger

	// Network load function for dynamic fee adjustment
	networkHealthFn func() int
	keyManager      *crypto.KeyManager

	// Added for better transaction lifecycle management
	accountNonces   map[string]int64               // Map of latest nonce per account
	accountTxs      map[string]map[string]struct{} // Map of account -> txIDs
	lastCleanupTime time.Time

	// Statistics
	totalAdded   uint64
	totalRemoved uint64
	totalExpired uint64
	totalEvicted uint64

	// PoH integration for transaction pre-ordering
	poh          interface{} // PoH instance (interface{} to avoid import cycle)
	pohBatchSize int         // Number of transactions to batch for PoH recording
	pohBatch     []*common.Transaction
	pohBatchMu   sync.Mutex

	// Parallel validation
	batchProcessor    *TransactionBatchProcessor
	parallelValidator *ParallelValidator
}

// PoolMetrics for logging or analysis
type PoolMetrics struct {
	TotalTxs             int
	ExpiredTxs           int
	ValidTxs             int
	InvalidTxs           int
	TotalFees            float64
	LastCleanupTime      time.Time
	LastOptimizationTime time.Time
	AvgTxSize            int
	MemoryUsage          int64
	PriorityDistribution map[string]int
}

// TransactionItem is the internal struct for the priority queue.
type TransactionItem struct {
	tx         common.Transaction
	priority   int
	index      int
	insertTime time.Time
	size       int // estimated serialized size
	// PoH fields for transaction ordering
	pohProof [32]byte
	pohCount uint64
}

// PriorityQueue satisfies the heap.Interface.
type PriorityQueue []*TransactionItem

// TransactionPoolOption defines functional options for TransactionPool
type TransactionPoolOption func(*TransactionPool)

// WithNetworkHealthFn sets a custom network health function
func WithNetworkHealthFn(fn func() int) TransactionPoolOption {
	return func(tp *TransactionPool) {
		tp.networkHealthFn = fn
	}
}

// SetNetworkHealthFunction registers a health check function after pool creation.
func (tp *TransactionPool) SetNetworkHealthFunction(fn func() int) {
	tp.mu.Lock()
	tp.networkHealthFn = fn
	tp.mu.Unlock()
}

// WithKeyManager sets a crypto key manager
func WithKeyManager(km *crypto.KeyManager) TransactionPoolOption {
	return func(tp *TransactionPool) {
		tp.keyManager = km
	}
}

// WithLogger sets a custom logger
func WithPoolLogger(logger *logrus.Logger) TransactionPoolOption {
	return func(tp *TransactionPool) {
		tp.logger = logger
	}
}

// WithPoH sets the PoH instance for transaction ordering
func WithPoH(poh interface{}) TransactionPoolOption {
	return func(tp *TransactionPool) {
		tp.poh = poh
		tp.pohBatchSize = 100 // Default batch size
	}
}

// WithPoHBatchSize sets the batch size for PoH recording
func WithPoHBatchSize(size int) TransactionPoolOption {
	return func(tp *TransactionPool) {
		if size > 0 && size <= 500 {
			tp.pohBatchSize = size
		}
	}
}

// WithConflictResolution enables transaction conflict resolution
func WithConflictResolution(enabled bool) TransactionPoolOption {
	return func(tp *TransactionPool) {
		tp.conflictResolutionEnabled = enabled
	}
}

// NewTransactionPool configures an in-memory pool.
func NewTransactionPool(
	maxPoolSize int,
	txTimeout time.Duration,
	minFee, maxFee float64,
	expirationDuration time.Duration,
	options ...TransactionPoolOption,
) *TransactionPool {
	pq := make(PriorityQueue, 0, maxPoolSize)
	heap.Init(&pq)

	// Create default logger if none provided
	logger := logrus.New()
	logger.SetLevel(logrus.InfoLevel)

	pool := &TransactionPool{
		pendingTxs:         &pq,
		txMap:              make(map[string]*TransactionItem),
		maxPoolSize:        maxPoolSize,
		txTimeout:          txTimeout,
		minFee:             minFee,
		maxFee:             maxFee,
		expirationDuration: expirationDuration,
		poolMetrics:        PoolMetrics{},
		logger:             logger,
		accountNonces:      make(map[string]int64),
		accountTxs:         make(map[string]map[string]struct{}),
		lastCleanupTime:    consensus.ConsensusNow(),
		pohBatchSize:       100, // Default batch size
		pohBatch:           make([]*common.Transaction, 0, 100),
	}

	// Apply options
	for _, opt := range options {
		opt(pool)
	}

	// Default network health function if none provided
	if pool.networkHealthFn == nil {
		pool.networkHealthFn = func() int { return 50 }
	}

	pool.logger.WithFields(logrus.Fields{
		"maxPoolSize":        maxPoolSize,
		"minFee":             minFee,
		"maxFee":             maxFee,
		"expirationDuration": expirationDuration,
	}).Info("Transaction pool initialized")

	// Initialize parallel validation with CPU count workers
	workers := runtime.NumCPU()
	pool.parallelValidator = NewParallelValidator(workers, logger)
	pool.batchProcessor = NewTransactionBatchProcessor(pool, 100, workers, logger)
	pool.batchProcessor.Start()

	return pool
}

// GetMaxPoolSize returns the maximum allowed transactions in the pool
func (tp *TransactionPool) GetMaxPoolSize() int {
	return tp.maxPoolSize
}

// AddTransactionBatch submits a batch of transactions for parallel validation and addition
func (tp *TransactionPool) AddTransactionBatch(transactions []common.Transaction) error {
	if tp.batchProcessor == nil {
		// Fallback to sequential processing if batch processor not initialized
		var lastErr error
		successCount := 0
		for _, tx := range transactions {
			if err := tp.AddTransaction(tx); err != nil {
				lastErr = err
			} else {
				successCount++
			}
		}
		tp.logger.WithFields(logrus.Fields{
			"batch_size": len(transactions),
			"success":    successCount,
			"failed":     len(transactions) - successCount,
		}).Info("Sequential batch processing complete")
		return lastErr
	}

	// Convert to pointer slice for batch processor
	txPtrs := make([]*common.Transaction, len(transactions))
	for i := range transactions {
		txPtrs[i] = &transactions[i]
	}

	return tp.batchProcessor.SubmitBatch(txPtrs)
}

// GetMaxNonceForSender returns the highest nonce for a sender in the pool
// Returns 0 if no transactions from this sender are in the pool
func (tp *TransactionPool) GetMaxNonceForSender(sender string) int64 {
	tp.mu.RLock()
	defer tp.mu.RUnlock()

	// Normalize sender address
	normalizedSender := common.NormalizeAddress(sender)

	// Check if we have tracked nonce for this sender
	if maxNonce, exists := tp.accountNonces[normalizedSender]; exists {
		return maxNonce
	}

	// If not found, return 0
	return 0
}

// GetAllTransactions returns all transaction items in the pool
func (tp *TransactionPool) GetAllTransactions() []*TransactionItem {
	tp.mu.RLock()
	defer tp.mu.RUnlock()

	result := make([]*TransactionItem, 0, len(tp.txMap))
	for _, item := range tp.txMap {
		result = append(result, item)
	}

	return result
}

// AddTransaction validates & pushes a transaction into the priority queue.
func (tp *TransactionPool) AddTransaction(tx common.Transaction) error {
	// Normalize transaction ID for consistent storage and lookup
	tx.ID = common.NormalizeTransactionID(tx.ID)

	// Debug log entry
	tp.logger.Debug("AddTransaction: Entry", "txID", tx.ID, "currentPoolSize", len(tp.txMap))

	// Log grep-friendly pool addition attempt
	tp.logger.WithFields(logrus.Fields{
		"txID":     tx.ID,
		"sender":   tx.Sender,
		"poolSize": len(tp.txMap),
	}).Info("txPoolAttempt")

	tp.mu.Lock()
	defer tp.mu.Unlock()

	defer func() {
		tp.logger.Debug("AddTransaction: Exit", "txID", tx.ID, "newPoolSize", len(tp.txMap))
	}()

	// 1) Check if the transaction already exists in the pool
	if _, exists := tp.txMap[tx.ID]; exists {
		tp.logger.WithField("txID", tx.ID).Warn("Duplicate transaction rejected")
		return ErrDuplicateTx
	}

	// 2) Optimize pool if it's near capacity
	if len(*tp.pendingTxs) >= tp.maxPoolSize {
		tp.optimizePool()
		if len(*tp.pendingTxs) >= tp.maxPoolSize {
			tp.logger.WithField("poolSize", len(*tp.pendingTxs)).Warn("Transaction pool full after optimization")
			return ErrPoolFull
		}
	}

	// 3) Perform basic validation
	if err := tp.validateTransaction(tx); err != nil {
		tp.poolMetrics.InvalidTxs++
		tp.logger.WithFields(logrus.Fields{
			"txID":  tx.ID,
			"error": err,
		}).Warn("Transaction validation failed")
		return fmt.Errorf("%w: %v", ErrInvalidTx, err)
	}

	// 4) Calculate transaction priority
	priority := tp.calculatePriority(tx)

	// 5) Estimate transaction size (for memory management)
	size := estimateTransactionSize(&tx)

	// 6) Record transaction in PoH for deterministic ordering
	// NOTE: We skip PoH recording here to make AddTransaction non-blocking
	// PoH recording will be done asynchronously in a background goroutine
	var pohProof [32]byte
	var pohCount uint64

	// Schedule PoH recording asynchronously if PoH is available
	if tp.poh != nil {
		// Make a copy of the transaction for async processing
		txCopy := tx
		go func() {
			tp.pohBatchMu.Lock()
			tp.pohBatch = append(tp.pohBatch, &txCopy)

			// Process batch if it reaches the batch size
			if len(tp.pohBatch) >= tp.pohBatchSize {
				batch := tp.pohBatch
				tp.pohBatch = make([]*common.Transaction, 0, tp.pohBatchSize)
				tp.pohBatchMu.Unlock()

				// Record batch in PoH
				if pohBatchResult, err := tp.recordTransactionBatch(batch); err == nil {
					// Update all items in the batch with their PoH values
					tp.mu.Lock()
					for _, entry := range pohBatchResult.Entries {
						if item, exists := tp.txMap[entry.Transaction.ID]; exists {
							item.pohProof = entry.PoHProof
							item.pohCount = entry.PoHCount
							// Update priority queue with PoH count for deterministic ordering
							heap.Fix(tp.pendingTxs, item.index)
						}
					}
					tp.mu.Unlock()
				} else {
					tp.logger.WithError(err).Warn("Failed to record transaction batch in PoH")
				}
			} else {
				tp.pohBatchMu.Unlock()
			}
		}()
	}

	// 7) Create and insert transaction item
	item := &TransactionItem{
		tx:         tx,
		priority:   int(priority), // Convert float64 to int
		insertTime: consensus.ConsensusNow(),
		size:       size,
		pohProof:   pohProof,
		pohCount:   pohCount,
	}
	heap.Push(tp.pendingTxs, item)
	tp.txMap[tx.ID] = item

	// 8) Update account nonce tracking - normalize sender
	normalizedSender := common.NormalizeAddress(tx.Sender)
	currentNonce, exists := tp.accountNonces[normalizedSender]
	if !exists || int64(tx.Nonce) > currentNonce {
		tp.accountNonces[normalizedSender] = int64(tx.Nonce)
	}

	// 9) Update account transaction mapping
	if _, exists := tp.accountTxs[tx.Sender]; !exists {
		tp.accountTxs[tx.Sender] = make(map[string]struct{})
	}
	tp.accountTxs[tx.Sender][tx.ID] = struct{}{}

	// 10) If receiver is also an account for which we track transactions (like a smart contract)
	if tx.SmartContractID != "" {
		if _, exists := tp.accountTxs[tx.SmartContractID]; !exists {
			tp.accountTxs[tx.SmartContractID] = make(map[string]struct{})
		}
		tp.accountTxs[tx.SmartContractID][tx.ID] = struct{}{}
	}

	// 11) Update metrics
	tp.poolMetrics.TotalTxs++
	tp.poolMetrics.ValidTxs++
	tp.poolMetrics.TotalFees += tx.Fee
	tp.poolMetrics.AvgTxSize = (tp.poolMetrics.AvgTxSize*(tp.poolMetrics.TotalTxs-1) + size) / tp.poolMetrics.TotalTxs
	atomic.AddUint64(&tp.totalAdded, 1)

	tp.logger.WithFields(logrus.Fields{
		"txID":     tx.ID,
		"sender":   tx.Sender,
		"receiver": tx.Receiver,
		"amount":   tx.Amount,
		"fee":      tx.Fee,
		"priority": priority,
		"poolSize": len(*tp.pendingTxs),
		"nonce":    tx.Nonce,
	}).Debug("Transaction added to pool")

	return nil
}

// AddValidatedTransaction adds a pre-validated transaction to the pool
// This is used by the parallel validator to bypass redundant validation
func (tp *TransactionPool) AddValidatedTransaction(tx *common.Transaction, priority float64, size int64) error {
	// Normalize transaction ID
	tx.ID = common.NormalizeTransactionID(tx.ID)

	tp.mu.Lock()
	defer tp.mu.Unlock()

	// Check if transaction already exists
	if _, exists := tp.txMap[tx.ID]; exists {
		return ErrDuplicateTx
	}

	// Check pool capacity
	if len(*tp.pendingTxs) >= tp.maxPoolSize {
		tp.optimizePool()
		if len(*tp.pendingTxs) >= tp.maxPoolSize {
			return ErrPoolFull
		}
	}

	// Create transaction item
	item := &TransactionItem{
		tx:       *tx,
		priority: int(priority),
		size:     int(size),
		index:    -1, // Will be set when pushed to heap
	}

	// Update nonce tracking
	normalizedSender := common.NormalizeAddress(tx.Sender)
	if int64(tx.Nonce) > tp.accountNonces[normalizedSender] {
		tp.accountNonces[normalizedSender] = int64(tx.Nonce)
	}

	// Track transaction by account
	if tp.accountTxs[normalizedSender] == nil {
		tp.accountTxs[normalizedSender] = make(map[string]struct{})
	}
	tp.accountTxs[normalizedSender][tx.ID] = struct{}{}

	// Add to pool
	heap.Push(tp.pendingTxs, item)
	tp.txMap[tx.ID] = item

	// Update metrics
	tp.poolMetrics.TotalTxs++
	tp.poolMetrics.ValidTxs++
	tp.poolMetrics.TotalFees += tx.Fee
	tp.poolMetrics.MemoryUsage += int64(size)
	atomic.AddUint64(&tp.totalAdded, 1)

	// Schedule PoH recording if available
	if tp.poh != nil {
		go func() {
			tp.pohBatchMu.Lock()
			tp.pohBatch = append(tp.pohBatch, tx)

			if len(tp.pohBatch) >= tp.pohBatchSize {
				batch := tp.pohBatch
				tp.pohBatch = make([]*common.Transaction, 0, tp.pohBatchSize)
				tp.pohBatchMu.Unlock()

				// Record batch in PoH
				if pohBatchResult, err := tp.recordTransactionBatch(batch); err == nil {
					tp.mu.Lock()
					for _, entry := range pohBatchResult.Entries {
						if item, exists := tp.txMap[entry.Transaction.ID]; exists {
							item.pohProof = entry.PoHProof
							item.pohCount = entry.PoHCount
							heap.Fix(tp.pendingTxs, item.index)
						}
					}
					tp.mu.Unlock()
				}
			} else {
				tp.pohBatchMu.Unlock()
			}
		}()
	}

	tp.logger.WithFields(logrus.Fields{
		"txID":     tx.ID,
		"priority": priority,
		"poolSize": len(*tp.pendingTxs),
	}).Debug("Pre-validated transaction added to pool")

	return nil
}

// estimateTransactionSize calculates an approximate size in bytes for a transaction
func estimateTransactionSize(tx *common.Transaction) int {
	// This is a simplistic estimate - in production you would serialize the tx
	// and measure the actual byte size
	baseSize := 200 // Base size for IDs, amounts, timestamps, etc.
	dataSize := len(tx.Data)
	metadataSize := 0

	if tx.Metadata != nil {
		// Estimate size based on metadata fields
		metadataSize += len(tx.Metadata.Category)
		metadataSize += len(tx.Metadata.Description)
		metadataSize += len(tx.Metadata.Reference)
		metadataSize += len(tx.Metadata.Source)
		metadataSize += len(tx.Metadata.Destination)
		metadataSize += len(tx.Metadata.Purpose)
		for _, tag := range tx.Metadata.Tags {
			metadataSize += len(tag)
		}
	}

	return baseSize + dataSize + metadataSize
}

// validateTransaction performs basic transaction validation
func (tp *TransactionPool) validateTransaction(tx common.Transaction) error {
	// 1) Skip common.ValidateTransaction as it uses account nonce instead of nonce tracker
	// The transaction manager has already validated the transaction with proper nonce tracking
	// if err := common.ValidateTransaction(tx); err != nil {
	//     return err
	// }

	// 2) Check fee requirements
	if tx.Fee < tp.minFee {
		return fmt.Errorf("fee %f below minimum %f", tx.Fee, tp.minFee)
	}

	// 3) Check if transaction is expired
	now := consensus.ConsensusUnix()
	if tx.ExpiryTime > 0 && tx.ExpiryTime <= now {
		return ErrTxExpired
	}

	// 4) Check if transaction is too old to add
	if now-tx.Timestamp > int64(tp.expirationDuration.Seconds()) {
		return fmt.Errorf("transaction too old: %s", time.Unix(tx.Timestamp, 0))
	}

	// 5) Verify signature if a key manager is available
	if tp.keyManager != nil && len(tx.Signature) > 0 {
		isValid, err := tp.verifySignature(tx)
		if err != nil {
			return fmt.Errorf("signature verification error: %w", err)
		}
		if !isValid {
			return errors.New("invalid transaction signature")
		}
	}

	// 6) Check for nonce conflicts/reuse (simpler than full replay protection)
	currentNonce, exists := tp.accountNonces[tx.Sender]
	if exists && int64(tx.Nonce) <= currentNonce {
		return fmt.Errorf("nonce %d is not higher than current account nonce %d",
			tx.Nonce, currentNonce)
	}

	// 7) Future: check if transaction would exceed gas limits

	return nil
}

// verifySignature checks the transaction signature against the sender's public key
func (tp *TransactionPool) verifySignature(tx common.Transaction) (bool, error) {
	// Look up sender account to get public key
	senderAcc := common.GetAccount(tx.Sender)
	if senderAcc == nil {
		return false, fmt.Errorf("sender account %s not found", tx.Sender)
	}

	// Get public key from account
	if len(senderAcc.PublicKey) == 0 {
		return false, fmt.Errorf("sender account %s has no public key", tx.Sender)
	}

	// Verify signature using Dilithium
	valid, err := crypto.VerifySignature(senderAcc.PublicKey, []byte(tx.ID), tx.Signature)
	if err != nil {
		return false, fmt.Errorf("signature verification error: %w", err)
	}

	return valid, nil
}

// RemoveTransaction removes a transaction from the priority queue and related maps
func (tp *TransactionPool) RemoveTransaction(txID string) error {
	tp.mu.Lock()
	defer tp.mu.Unlock()

	// Normalize transaction ID for consistent lookup
	normalizedID := common.NormalizeTransactionID(txID)

	item, exists := tp.txMap[normalizedID]
	if !exists {
		return ErrTxNotFound
	}

	// 1) Remove from priority queue
	heap.Remove(tp.pendingTxs, item.index)

	// 2) Remove from transaction map
	delete(tp.txMap, normalizedID)

	// 3) Remove from account transactions map
	sender := item.tx.Sender
	if txMap, exists := tp.accountTxs[sender]; exists {
		delete(txMap, normalizedID)
		// Clean up empty maps
		if len(txMap) == 0 {
			delete(tp.accountTxs, sender)
		}
	}

	// 4) If this is a contract transaction, remove from contract map too
	if item.tx.SmartContractID != "" {
		if txMap, exists := tp.accountTxs[item.tx.SmartContractID]; exists {
			delete(txMap, normalizedID)
			if len(txMap) == 0 {
				delete(tp.accountTxs, item.tx.SmartContractID)
			}
		}
	}

	// 5) Update account nonce tracking if this was the max nonce
	normalizedSender := common.NormalizeAddress(item.tx.Sender)
	if currentMaxNonce, exists := tp.accountNonces[normalizedSender]; exists && int64(item.tx.Nonce) == currentMaxNonce {
		// This was the max nonce, need to recalculate
		var newMaxNonce int64 = 0
		foundAny := false

		// Find the new max nonce from remaining transactions
		for _, otherItem := range tp.txMap {
			if common.NormalizeAddress(otherItem.tx.Sender) == normalizedSender {
				if int64(otherItem.tx.Nonce) > newMaxNonce {
					newMaxNonce = int64(otherItem.tx.Nonce)
					foundAny = true
				}
			}
		}

		if foundAny {
			tp.accountNonces[normalizedSender] = newMaxNonce
		} else {
			// No more transactions from this sender
			delete(tp.accountNonces, normalizedSender)
		}
	}

	// 6) Update metrics
	tp.poolMetrics.TotalTxs--
	tp.poolMetrics.TotalFees -= item.tx.Fee
	atomic.AddUint64(&tp.totalRemoved, 1)

	tp.logger.WithFields(logrus.Fields{
		"txID":     txID,
		"sender":   item.tx.Sender,
		"poolSize": len(*tp.pendingTxs),
	}).Debug("Transaction removed from pool")

	return nil
}

// GetTransaction retrieves a transaction by ID
func (tp *TransactionPool) GetTransaction(txID string) (*common.Transaction, error) {
	tp.mu.RLock()
	defer tp.mu.RUnlock()

	// Normalize transaction ID for consistent lookup
	normalizedID := common.NormalizeTransactionID(txID)

	item, exists := tp.txMap[normalizedID]
	if !exists {
		return nil, ErrTxNotFound
	}

	// Return a copy to prevent modification
	tx := item.tx
	return &tx, nil
}

// HasTransaction checks if a transaction exists in the pool
func (tp *TransactionPool) HasTransaction(txID string) bool {
	tp.mu.RLock()
	defer tp.mu.RUnlock()

	// Normalize transaction ID for consistent lookup
	normalizedID := common.NormalizeTransactionID(txID)

	_, exists := tp.txMap[normalizedID]
	return exists
}

// GetAccountTransactions returns all transactions for a specific account
func (tp *TransactionPool) GetAccountTransactions(accountID string) ([]*common.Transaction, error) {
	if accountID == "" {
		return nil, errors.New("account ID cannot be empty")
	}

	tp.mu.RLock()
	defer tp.mu.RUnlock()

	txMap, exists := tp.accountTxs[accountID]
	if !exists || len(txMap) == 0 {
		return nil, nil
	}

	result := make([]*common.Transaction, 0, len(txMap))
	for txID := range txMap {
		if item, exists := tp.txMap[txID]; exists {
			tx := item.tx
			result = append(result, &tx)
		}
	}

	return result, nil
}

// HandleConflicts resolves conflicting transactions (same sender+nonce)
func (tp *TransactionPool) HandleConflicts() {
	if !tp.conflictResolutionEnabled {
		return
	}

	tp.mu.Lock()
	defer tp.mu.Unlock()

	// Track txs by sender+nonce for deduplication
	conflicts := make(map[string]map[string]*TransactionItem)
	toRemove := make([]string, 0)

	// 1) Find all conflicts (same sender+nonce)
	for _, item := range tp.txMap {
		key := fmt.Sprintf("%s:%d", item.tx.Sender, item.tx.Nonce)
		if _, exists := conflicts[key]; !exists {
			conflicts[key] = make(map[string]*TransactionItem)
		}
		conflicts[key][item.tx.ID] = item
	}

	// 2) For each conflict set, keep only highest fee transaction
	for _, conflictSet := range conflicts {
		// Skip if there's only one transaction
		if len(conflictSet) <= 1 {
			continue
		}

		// Find the item with the highest fee
		var highestFee float64
		var highestFeeID string

		for txID, item := range conflictSet {
			if item.tx.Fee > highestFee {
				highestFee = item.tx.Fee
				highestFeeID = txID
			}
		}

		// Mark all except highest fee for removal
		for txID := range conflictSet {
			if txID != highestFeeID {
				toRemove = append(toRemove, txID)
			}
		}
	}

	// 3) Remove conflicting transactions (lower fees)
	for _, txID := range toRemove {
		if item, exists := tp.txMap[txID]; exists {
			heap.Remove(tp.pendingTxs, item.index)
			delete(tp.txMap, txID)

			tp.logger.WithFields(logrus.Fields{
				"txID":   txID,
				"sender": item.tx.Sender,
				"nonce":  item.tx.Nonce,
				"fee":    item.tx.Fee,
			}).Info("Removed conflicting transaction")
		}
	}

	if len(toRemove) > 0 {
		tp.logger.WithField("count", len(toRemove)).Info("Conflict resolution complete")
	}
}

// optimizePool evicts low-priority transactions if the pool is at capacity
func (tp *TransactionPool) optimizePool() {
	// Already holding lock from caller

	// 1) Get current pool size and capacity
	currentSize := len(*tp.pendingTxs)
	targetSize := int(float64(tp.maxPoolSize) * 0.8) // Target 80% capacity

	// 2) If pool is not over target, no need to optimize
	if currentSize <= targetSize {
		return
	}

	// 3) Adjust minimum fee based on network load
	netLoad := 50
	if tp.networkHealthFn != nil {
		netLoad = tp.networkHealthFn()
	}
	tp.adjustFeesBasedOnLoad(netLoad)

	// 4) Calculate number of transactions to evict
	toEvict := currentSize - targetSize
	evicted := 0

	// 5) Evict lowest priority transactions
	for i := 0; i < toEvict; i++ {
		if len(*tp.pendingTxs) == 0 {
			break
		}

		lowestPriorityItem := heap.Pop(tp.pendingTxs).(*TransactionItem)
		txID := lowestPriorityItem.tx.ID
		delete(tp.txMap, txID)

		// Update account transaction maps
		sender := lowestPriorityItem.tx.Sender
		if accTxs, exists := tp.accountTxs[sender]; exists {
			delete(accTxs, txID)
			if len(accTxs) == 0 {
				delete(tp.accountTxs, sender)
			}
		}

		tp.logger.WithFields(logrus.Fields{
			"txID":     txID,
			"priority": lowestPriorityItem.priority,
			"fee":      lowestPriorityItem.tx.Fee,
		}).Debug("Evicted low-priority transaction during optimization")

		evicted++
		atomic.AddUint64(&tp.totalEvicted, 1)
	}

	// 6) Update metrics
	tp.poolMetrics.LastOptimizationTime = consensus.ConsensusNow()
	tp.poolMetrics.TotalTxs = len(*tp.pendingTxs)

	tp.logger.WithFields(logrus.Fields{
		"evicted":     evicted,
		"remaining":   len(*tp.pendingTxs),
		"minFee":      tp.minFee,
		"networkLoad": netLoad,
	}).Info("Pool optimization complete")
}

// adjustFeesBasedOnLoad dynamically adjusts minimum fee based on network load
func (tp *TransactionPool) adjustFeesBasedOnLoad(networkLoad int) {
	// Already holding lock from caller

	// Map network load 0-100 to adjustment factor
	loadFactor := float64(networkLoad) / 100.0

	// Original min fee is the baseline
	originalMinFee := tp.minFee

	// Adjust based on load - higher load = higher minimum fee
	if networkLoad > 75 {
		// High load: increase min fee substantially
		tp.minFee = originalMinFee * (1.0 + loadFactor)
	} else if networkLoad < 25 {
		// Low load: decrease min fee slightly
		tp.minFee = originalMinFee * (0.8 + 0.2*loadFactor)
	} else {
		// Medium load: minor adjustments
		tp.minFee = originalMinFee * (0.9 + 0.2*loadFactor)
	}

	// Ensure min fee is always above a floor value
	if tp.minFee < originalMinFee*0.5 {
		tp.minFee = originalMinFee * 0.5
	}

	// Ensure min fee doesn't exceed max fee
	if tp.minFee > tp.maxFee {
		tp.minFee = tp.maxFee
	}

	tp.logger.WithFields(logrus.Fields{
		"originalMinFee": originalMinFee,
		"newMinFee":      tp.minFee,
		"networkLoad":    networkLoad,
	}).Debug("Adjusted minimum fee based on network load")
}

// RemoveExpiredTransactions removes transactions older than the given duration
func (tp *TransactionPool) RemoveExpiredTransactions(maxAge time.Duration) int {
	tp.mu.Lock()
	defer tp.mu.Unlock()

	// 1) Set up cutoff time
	cutoffTime := consensus.ConsensusNow().Add(-maxAge)
	toRemove := make([]string, 0)

	// 2) Find expired transactions
	for txID, item := range tp.txMap {
		// Check if transaction has explicit expiry time
		if item.tx.ExpiryTime > 0 && consensus.ConsensusUnix() > item.tx.ExpiryTime {
			toRemove = append(toRemove, txID)
			continue
		}

		// Check if transaction is older than max age
		txTime := time.Unix(item.tx.Timestamp, 0)
		if txTime.Before(cutoffTime) {
			toRemove = append(toRemove, txID)
		}
	}

	// 3) Remove expired transactions
	for _, txID := range toRemove {
		if item, exists := tp.txMap[txID]; exists {
			heap.Remove(tp.pendingTxs, item.index)
			delete(tp.txMap, txID)

			// Update account transaction maps
			sender := item.tx.Sender
			if accTxs, exists := tp.accountTxs[sender]; exists {
				delete(accTxs, txID)
				if len(accTxs) == 0 {
					delete(tp.accountTxs, sender)
				}
			}
		}
	}

	// 4) Update metrics
	tp.poolMetrics.ExpiredTxs += len(toRemove)
	tp.poolMetrics.LastCleanupTime = consensus.ConsensusNow()
	tp.lastCleanupTime = consensus.ConsensusNow()
	atomic.AddUint64(&tp.totalExpired, uint64(len(toRemove)))

	tp.logger.WithField("removed", len(toRemove)).Info("Expired transaction cleanup complete")

	return len(toRemove)
}

// PoolSize returns the number of pending transactions in the pool
func (tp *TransactionPool) PoolSize() int {
	tp.mu.RLock()
	defer tp.mu.RUnlock()
	return len(*tp.pendingTxs)
}

// GetMetrics returns various statistical metrics about the transaction pool
func (tp *TransactionPool) GetMetrics() PoolMetrics {
	tp.mu.RLock()
	defer tp.mu.RUnlock()

	// Add priority distribution
	metrics := tp.poolMetrics
	metrics.PriorityDistribution = tp.getPriorityDistribution()

	// Calculate memory usage (rough estimate)
	metrics.MemoryUsage = int64(metrics.TotalTxs * metrics.AvgTxSize)

	return metrics
}

// getPriorityDistribution calculates distribution of transactions by priority bands
func (tp *TransactionPool) getPriorityDistribution() map[string]int {
	// Already holding lock from caller

	result := map[string]int{
		"veryLow":  0,
		"low":      0,
		"medium":   0,
		"high":     0,
		"veryHigh": 0,
	}

	for _, item := range tp.txMap {
		switch {
		case item.priority < 20:
			result["veryLow"]++
		case item.priority < 40:
			result["low"]++
		case item.priority < 60:
			result["medium"]++
		case item.priority < 80:
			result["high"]++
		default:
			result["veryHigh"]++
		}
	}

	return result
}

// calculatePriority computes transaction priority based on fee and age
func (tp *TransactionPool) calculatePriority(tx common.Transaction) float64 {
	// Calculate priority based on various factors
	basePriority := float64(tx.Priority)

	// Age factor (older transactions get slightly higher priority)
	ageSeconds := consensus.ConsensusUnix() - tx.Timestamp
	ageFactor := float64(ageSeconds) * 0.001 // Small age bonus

	// Fee factor (higher fees get higher priority)
	feeFactor := tx.Fee * 10

	return basePriority + ageFactor + feeFactor
}

// PoolStats represents comprehensive pool statistics
type PoolStats struct {
	Size            int         `json:"size"`
	MaxSize         int         `json:"max_size"`
	TotalAdded      uint64      `json:"total_added"`
	TotalRemoved    uint64      `json:"total_removed"`
	TotalExpired    uint64      `json:"total_expired"`
	TotalEvicted    uint64      `json:"total_evicted"`
	LastCleanupTime time.Time   `json:"last_cleanup_time"`
	CurrentMinFee   float64     `json:"current_min_fee"`
	Metrics         PoolMetrics `json:"metrics"`
	LoadFactor      float64     `json:"load_factor"`
	Health          string      `json:"health"`
}

// PriorityQueue methods for the heap interface
func (pq PriorityQueue) Len() int { return len(pq) }

// Less gives higher priority values precedence (max heap)
// When priorities are equal, use PoH count for deterministic ordering
func (pq PriorityQueue) Less(i, j int) bool {
	if pq[i].priority != pq[j].priority {
		return pq[i].priority > pq[j].priority
	}
	// Use PoH count for deterministic ordering when priorities are equal
	if pq[i].pohCount != 0 && pq[j].pohCount != 0 {
		return pq[i].pohCount < pq[j].pohCount // Lower count = earlier in PoH sequence
	}
	// Fallback to insertion time
	return pq[i].insertTime.Before(pq[j].insertTime)
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*TransactionItem)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // Avoid memory leak
	item.index = -1 // Mark as removed
	*pq = old[0 : n-1]
	return item
}

// Update modifies the priority of an item in the queue
func (pq *PriorityQueue) Update(item *TransactionItem, priority int) {
	item.priority = priority
	heap.Fix(pq, item.index)
}

// recordTransactionBatch records a batch of transactions in PoH
func (tp *TransactionPool) recordTransactionBatch(batch []*common.Transaction) (*pohBatchResult, error) {
	if tp.poh == nil || len(batch) == 0 {
		return nil, errors.New("PoH not configured or empty batch")
	}

	// Convert to interface slice for PoH
	txInterfaces := make([]interface{}, len(batch))
	for i, tx := range batch {
		txInterfaces[i] = tx
	}

	// Record batch in PoH
	recorder, ok := tp.poh.(interface {
		BatchRecordTransactions([]interface{}) (interface{}, error)
	})
	if !ok {
		return nil, errors.New("PoH instance does not support batch recording")
	}

	result, err := recorder.BatchRecordTransactions(txInterfaces)
	if err != nil {
		return nil, fmt.Errorf("failed to record batch in PoH: %w", err)
	}

	// Try to convert to TransactionBatch type from PoH
	// The actual type is diamantepoh.TransactionBatch but we can't import that due to cycles
	// So we'll use type assertion with the fields we need
	batchValue := reflect.ValueOf(result)
	if batchValue.Kind() == reflect.Ptr {
		batchValue = batchValue.Elem()
	}

	if batchValue.Kind() == reflect.Struct && batchValue.FieldByName("Entries").IsValid() {
		entries := batchValue.FieldByName("Entries")
		if entries.Kind() == reflect.Slice {
			var pohEntries []struct {
				Transaction *common.Transaction
				PoHProof    [32]byte
				PoHCount    uint64
			}

			for i := 0; i < entries.Len(); i++ {
				entry := entries.Index(i)
				if entry.Kind() == reflect.Struct {
					tx := entry.FieldByName("Transaction")
					proof := entry.FieldByName("PoHProof")
					count := entry.FieldByName("PoHCount")

					if tx.IsValid() && proof.IsValid() && count.IsValid() {
						if txPtr, ok := tx.Interface().(*common.Transaction); ok {
							pohEntry := struct {
								Transaction *common.Transaction
								PoHProof    [32]byte
								PoHCount    uint64
							}{
								Transaction: txPtr,
								PoHCount:    count.Uint(),
							}

							// Copy proof array
							if proofArray, ok := proof.Interface().([32]byte); ok {
								pohEntry.PoHProof = proofArray
							}

							pohEntries = append(pohEntries, pohEntry)
						}
					}
				}
			}

			return &pohBatchResult{
				Entries: pohEntries,
			}, nil
		}
	}

	return nil, errors.New("unexpected PoH batch result type")
}

// pohBatchResult represents the result of recording a batch in PoH
type pohBatchResult struct {
	Entries []struct {
		Transaction *common.Transaction
		PoHProof    [32]byte
		PoHCount    uint64
	}
}

// GetTransactionsByPoHOrder returns transactions sorted by their PoH count
// This provides the deterministic ordering for block production
func (tp *TransactionPool) GetTransactionsByPoHOrder(limit int) []common.Transaction {
	tp.mu.RLock()
	defer tp.mu.RUnlock()

	tp.logger.Info("GetTransactionsByPoHOrder called",
		"limit", limit,
		"poolSize", len(tp.txMap),
		"pendingSize", len(*tp.pendingTxs))

	// Process any remaining transactions in the PoH batch
	if tp.poh != nil && len(tp.pohBatch) > 0 {
		tp.pohBatchMu.Lock()
		if len(tp.pohBatch) > 0 {
			batch := tp.pohBatch
			tp.pohBatch = make([]*common.Transaction, 0, tp.pohBatchSize)
			tp.pohBatchMu.Unlock()

			// Record remaining batch
			if pohBatchResult, err := tp.recordTransactionBatch(batch); err != nil {
				tp.logger.WithError(err).Warn("Failed to record final batch in PoH")
			} else {
				// Update all items in the batch with their PoH values
				tp.mu.Lock()
				for _, entry := range pohBatchResult.Entries {
					if item, exists := tp.txMap[entry.Transaction.ID]; exists {
						item.pohProof = entry.PoHProof
						item.pohCount = entry.PoHCount
					}
				}
				tp.mu.Unlock()
			}
		} else {
			tp.pohBatchMu.Unlock()
		}
	}

	// Create a slice of all transactions with PoH ordering
	type pohTx struct {
		tx       common.Transaction
		pohCount uint64
		priority int
	}

	var pohTxs []pohTx
	for _, item := range tp.txMap {
		// Include all transactions, even those not yet recorded in PoH
		// They will get recorded when FlushPoHBatch is called
		pohTxs = append(pohTxs, pohTx{
			tx:       item.tx,
			pohCount: item.pohCount,
			priority: item.priority,
		})
	}

	// Sort by PoH count (deterministic order)
	sort.Slice(pohTxs, func(i, j int) bool {
		if pohTxs[i].pohCount != pohTxs[j].pohCount {
			return pohTxs[i].pohCount < pohTxs[j].pohCount
		}
		// If PoH counts are equal (e.g., both 0 for new transactions),
		// sort by transaction ID for deterministic ordering
		if pohTxs[i].pohCount == pohTxs[j].pohCount && pohTxs[i].pohCount == 0 {
			return pohTxs[i].tx.ID < pohTxs[j].tx.ID
		}
		// Otherwise fallback to priority
		return pohTxs[i].priority > pohTxs[j].priority
	})

	// Apply limit and return transactions
	if limit > 0 && limit < len(pohTxs) {
		pohTxs = pohTxs[:limit]
	}

	result := make([]common.Transaction, len(pohTxs))
	for i, ptx := range pohTxs {
		result[i] = ptx.tx
	}

	return result
}

// FlushPoHBatch forces processing of any pending PoH batch
// This should be called before block production to ensure all transactions are ordered
func (tp *TransactionPool) FlushPoHBatch() error {
	if tp.poh == nil {
		return nil
	}

	tp.pohBatchMu.Lock()
	if len(tp.pohBatch) == 0 {
		tp.pohBatchMu.Unlock()
		return nil
	}

	batch := tp.pohBatch
	tp.pohBatch = make([]*common.Transaction, 0, tp.pohBatchSize)
	tp.pohBatchMu.Unlock()

	pohBatchResult, err := tp.recordTransactionBatch(batch)
	if err != nil {
		return err
	}

	// Update all items in the batch with their PoH values
	tp.mu.Lock()
	for _, entry := range pohBatchResult.Entries {
		if item, exists := tp.txMap[entry.Transaction.ID]; exists {
			item.pohProof = entry.PoHProof
			item.pohCount = entry.PoHCount
		}
	}
	tp.mu.Unlock()

	return nil
}

// Close shuts down the transaction pool and its batch processor
func (tp *TransactionPool) Close() error {
	if tp.batchProcessor != nil {
		tp.batchProcessor.Stop()
	}
	return nil
}

// GetStats returns current pool statistics including parallel validation metrics
func (tp *TransactionPool) GetStats() map[string]interface{} {
	tp.mu.RLock()
	defer tp.mu.RUnlock()

	stats := map[string]interface{}{
		"pool_size":       len(*tp.pendingTxs),
		"total_added":     atomic.LoadUint64(&tp.totalAdded),
		"total_removed":   atomic.LoadUint64(&tp.totalRemoved),
		"total_expired":   atomic.LoadUint64(&tp.totalExpired),
		"total_evicted":   atomic.LoadUint64(&tp.totalEvicted),
		"max_pool_size":   tp.maxPoolSize,
		"total_fees":      tp.poolMetrics.TotalFees,
		"avg_tx_size":     tp.poolMetrics.AvgTxSize,
		"memory_usage":    tp.poolMetrics.MemoryUsage,
		"active_accounts": len(tp.accountNonces),
	}

	// Add parallel validation metrics if available
	if tp.parallelValidator != nil {
		stats["parallel_validation"] = tp.parallelValidator.GetMetrics()
	}

	return stats
}
