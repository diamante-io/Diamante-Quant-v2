package transaction

import (
	"container/heap"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"

	"diamante/common"
	"diamante/crypto"
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
		lastCleanupTime:    time.Now(),
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

	return pool
}

// GetMaxPoolSize returns the maximum allowed transactions in the pool
func (tp *TransactionPool) GetMaxPoolSize() int {
	return tp.maxPoolSize
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
	tp.mu.Lock()
	defer tp.mu.Unlock()

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
	size := estimateTransactionSize(tx)

	// 6) Create and insert transaction item
	item := &TransactionItem{
		tx:         tx,
		priority:   priority,
		insertTime: time.Now(),
		size:       size,
	}
	heap.Push(tp.pendingTxs, item)
	tp.txMap[tx.ID] = item

	// 7) Update account nonce tracking
	currentNonce, exists := tp.accountNonces[tx.Sender]
	if !exists || int64(tx.Nonce) > currentNonce {
		tp.accountNonces[tx.Sender] = int64(tx.Nonce)
	}

	// 8) Update account transaction mapping
	if _, exists := tp.accountTxs[tx.Sender]; !exists {
		tp.accountTxs[tx.Sender] = make(map[string]struct{})
	}
	tp.accountTxs[tx.Sender][tx.ID] = struct{}{}

	// 9) If receiver is also an account for which we track transactions (like a smart contract)
	if tx.SmartContractID != "" {
		if _, exists := tp.accountTxs[tx.SmartContractID]; !exists {
			tp.accountTxs[tx.SmartContractID] = make(map[string]struct{})
		}
		tp.accountTxs[tx.SmartContractID][tx.ID] = struct{}{}
	}

	// 10) Update metrics
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

// estimateTransactionSize calculates an approximate size in bytes for a transaction
func estimateTransactionSize(tx common.Transaction) int {
	// This is a simplistic estimate - in production you would serialize the tx
	// and measure the actual byte size
	baseSize := 200 // Base size for IDs, amounts, timestamps, etc.
	dataSize := len(tx.Data)
	metadataSize := 0

	if tx.Metadata != nil {
		// Rough estimate of metadata size
		for k := range tx.Metadata {
			metadataSize += len(k)
			metadataSize += 50
		}
	}

	return baseSize + dataSize + metadataSize
}

// validateTransaction performs basic transaction validation
func (tp *TransactionPool) validateTransaction(tx common.Transaction) error {
	// 1) Call the transaction's own validate method
	if err := common.ValidateTransaction(tx); err != nil {
		return err
	}

	// 2) Check fee requirements
	if tx.Fee < tp.minFee {
		return fmt.Errorf("fee %f below minimum %f", tx.Fee, tp.minFee)
	}

	// 3) Check if transaction is expired
	now := time.Now().Unix()
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

	item, exists := tp.txMap[txID]
	if !exists {
		return ErrTxNotFound
	}

	// 1) Remove from priority queue
	heap.Remove(tp.pendingTxs, item.index)

	// 2) Remove from transaction map
	delete(tp.txMap, txID)

	// 3) Remove from account transactions map
	sender := item.tx.Sender
	if txMap, exists := tp.accountTxs[sender]; exists {
		delete(txMap, txID)
		// Clean up empty maps
		if len(txMap) == 0 {
			delete(tp.accountTxs, sender)
		}
	}

	// 4) If this is a contract transaction, remove from contract map too
	if item.tx.SmartContractID != "" {
		if txMap, exists := tp.accountTxs[item.tx.SmartContractID]; exists {
			delete(txMap, txID)
			if len(txMap) == 0 {
				delete(tp.accountTxs, item.tx.SmartContractID)
			}
		}
	}

	// 5) Update metrics
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

	item, exists := tp.txMap[txID]
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

	_, exists := tp.txMap[txID]
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
	tp.poolMetrics.LastOptimizationTime = time.Now()
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
	cutoffTime := time.Now().Add(-maxAge)
	toRemove := make([]string, 0)

	// 2) Find expired transactions
	for txID, item := range tp.txMap {
		// Check if transaction has explicit expiry time
		if item.tx.ExpiryTime > 0 && time.Now().Unix() > item.tx.ExpiryTime {
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
	tp.poolMetrics.LastCleanupTime = time.Now()
	tp.lastCleanupTime = time.Now()
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
func (tp *TransactionPool) calculatePriority(tx common.Transaction) int {
	// Base priority starts with fee-per-byte metric
	// In production, use actual serialized size instead of this estimate
	estimatedSize := 1024 // 1KB baseline estimate
	if len(tx.Data) > 0 {
		estimatedSize += len(tx.Data)
	}

	feePerByte := (tx.Fee * 1024) / float64(estimatedSize)

	// Age factor (older transactions get a small boost)
	ageSeconds := time.Now().Unix() - tx.Timestamp
	ageFactor := float64(ageSeconds) / 3600.0 // Hours old
	if ageFactor > 10 {
		ageFactor = 10 // Cap age boost
	}

	// Combine factors: fee is primary, age is secondary
	rawPriority := (feePerByte * 10) + ageFactor

	// Scale to 0-100 range
	scaledPriority := int(rawPriority * 5)
	if scaledPriority > 100 {
		scaledPriority = 100
	}
	if scaledPriority < 0 {
		scaledPriority = 0
	}

	return scaledPriority
}

// GetStats returns operational statistics
func (tp *TransactionPool) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"size":            tp.PoolSize(),
		"maxSize":         tp.maxPoolSize,
		"totalAdded":      atomic.LoadUint64(&tp.totalAdded),
		"totalRemoved":    atomic.LoadUint64(&tp.totalRemoved),
		"totalExpired":    atomic.LoadUint64(&tp.totalExpired),
		"totalEvicted":    atomic.LoadUint64(&tp.totalEvicted),
		"lastCleanupTime": tp.lastCleanupTime,
		"currentMinFee":   tp.minFee,
		"metrics":         tp.GetMetrics(),
	}
}

// PriorityQueue methods for the heap interface
func (pq PriorityQueue) Len() int { return len(pq) }

// Less gives higher priority values precedence (max heap)
func (pq PriorityQueue) Less(i, j int) bool {
	return pq[i].priority > pq[j].priority
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
