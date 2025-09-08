package transaction

import (
	"container/heap"
	"container/list"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"diamante/common"
	"diamante/consensus"

	"github.com/sirupsen/logrus"
)

// OptimizedTransactionPool provides additional performance optimizations
type OptimizedTransactionPool struct {
	*TransactionPool

	// Sharded maps for concurrent access
	shardCount    int
	txShards      []*txShard
	accountShards []*accountShard

	// Pre-allocated object pools
	itemPool sync.Pool

	// Lock-free counters
	totalTxCount atomic.Uint64
	evictedCount atomic.Uint64

	// Background workers
	workerWg sync.WaitGroup
	stopCh   chan struct{}

	// Metrics
	shardMetrics []ShardMetrics
}

// txShard represents a shard of transactions
type txShard struct {
	mu    sync.RWMutex
	items map[string]*TransactionItem
}

// accountShard represents a shard of account data
type accountShard struct {
	mu      sync.RWMutex
	nonces  map[string]int64
	txLists map[string]*list.List // Ordered list of tx IDs per account
}

// ShardMetrics tracks per-shard statistics
type ShardMetrics struct {
	TxCount      atomic.Uint64
	LockWaitTime atomic.Uint64
	AccessCount  atomic.Uint64
}

// NewOptimizedTransactionPool creates an optimized transaction pool
func NewOptimizedTransactionPool(
	maxPoolSize int,
	txTimeout time.Duration,
	minFee, maxFee float64,
	expirationDuration time.Duration,
	shardCount int,
	options ...TransactionPoolOption,
) *OptimizedTransactionPool {
	if shardCount <= 0 {
		shardCount = 16 // Default shard count
	}

	// Create base pool
	basePool := NewTransactionPool(maxPoolSize, txTimeout, minFee, maxFee, expirationDuration, options...)

	// Initialize shards
	txShards := make([]*txShard, shardCount)
	accountShards := make([]*accountShard, shardCount)
	shardMetrics := make([]ShardMetrics, shardCount)

	for i := 0; i < shardCount; i++ {
		txShards[i] = &txShard{
			items: make(map[string]*TransactionItem),
		}
		accountShards[i] = &accountShard{
			nonces:  make(map[string]int64),
			txLists: make(map[string]*list.List),
		}
	}

	pool := &OptimizedTransactionPool{
		TransactionPool: basePool,
		shardCount:      shardCount,
		txShards:        txShards,
		accountShards:   accountShards,
		stopCh:          make(chan struct{}),
		shardMetrics:    shardMetrics,
		itemPool: sync.Pool{
			New: func() interface{} {
				return &TransactionItem{}
			},
		},
	}

	// Start background workers
	pool.startWorkers()

	return pool
}

// AddTransactionOptimized adds a transaction with optimized sharding
func (otp *OptimizedTransactionPool) AddTransactionOptimized(tx common.Transaction) error {
	// Normalize transaction ID
	tx.ID = common.NormalizeTransactionID(tx.ID)

	// Get shard indices
	txShardIdx := otp.getShardIndex(tx.ID)
	accountShardIdx := otp.getShardIndex(tx.Sender)

	// Quick existence check without full lock
	txShard := otp.txShards[txShardIdx]
	txShard.mu.RLock()
	if _, exists := txShard.items[tx.ID]; exists {
		txShard.mu.RUnlock()
		return ErrDuplicateTx
	}
	txShard.mu.RUnlock()

	// Validate transaction
	if err := otp.validateTransaction(tx); err != nil {
		return err
	}

	// Check pool capacity
	currentSize := otp.totalTxCount.Load()
	if currentSize >= uint64(otp.maxPoolSize) {
		// Try to evict low priority transactions
		if !otp.evictLowPriorityTransactions(1) {
			return ErrPoolFull
		}
	}

	// Get item from pool
	item := otp.itemPool.Get().(*TransactionItem)
	item.tx = tx
	item.priority = int(otp.calculatePriority(tx))
	item.size = estimateTransactionSize(&tx)
	item.index = -1

	// Update account shard
	accountShard := otp.accountShards[accountShardIdx]
	accountShard.mu.Lock()

	// Update nonce
	normalizedSender := common.NormalizeAddress(tx.Sender)
	if int64(tx.Nonce) > accountShard.nonces[normalizedSender] {
		accountShard.nonces[normalizedSender] = int64(tx.Nonce)
	}

	// Add to account's transaction list
	if accountShard.txLists[normalizedSender] == nil {
		accountShard.txLists[normalizedSender] = list.New()
	}
	accountShard.txLists[normalizedSender].PushBack(tx.ID)

	accountShard.mu.Unlock()

	// Add to transaction shard
	txShard.mu.Lock()
	txShard.items[tx.ID] = item
	txShard.mu.Unlock()

	// Update metrics
	otp.totalTxCount.Add(1)
	otp.shardMetrics[txShardIdx].TxCount.Add(1)
	otp.shardMetrics[txShardIdx].AccessCount.Add(1)

	// Add to base pool's priority queue (still needed for ordering)
	otp.mu.Lock()
	otp.txMap[tx.ID] = item
	heap.Push(otp.pendingTxs, item)
	otp.mu.Unlock()

	otp.logger.WithFields(logrus.Fields{
		"txID":     tx.ID,
		"shard":    txShardIdx,
		"poolSize": currentSize + 1,
	}).Debug("Transaction added to optimized pool")

	return nil
}

// GetTransactionOptimized retrieves a transaction with sharded lookup
func (otp *OptimizedTransactionPool) GetTransactionOptimized(txID string) (*common.Transaction, bool) {
	txID = common.NormalizeTransactionID(txID)
	shardIdx := otp.getShardIndex(txID)

	shard := otp.txShards[shardIdx]
	shard.mu.RLock()
	defer shard.mu.RUnlock()

	if item, exists := shard.items[txID]; exists {
		return &item.tx, true
	}

	return nil, false
}

// RemoveTransactionOptimized removes a transaction with sharded access
func (otp *OptimizedTransactionPool) RemoveTransactionOptimized(txID string) bool {
	txID = common.NormalizeTransactionID(txID)
	shardIdx := otp.getShardIndex(txID)

	shard := otp.txShards[shardIdx]
	shard.mu.Lock()
	item, exists := shard.items[txID]
	if !exists {
		shard.mu.Unlock()
		return false
	}

	// Remove from shard
	delete(shard.items, txID)
	shard.mu.Unlock()

	// Update account shard
	accountShardIdx := otp.getShardIndex(item.tx.Sender)
	accountShard := otp.accountShards[accountShardIdx]
	accountShard.mu.Lock()

	// Remove from account's transaction list
	normalizedSender := common.NormalizeAddress(item.tx.Sender)
	if txList := accountShard.txLists[normalizedSender]; txList != nil {
		for e := txList.Front(); e != nil; e = e.Next() {
			if e.Value.(string) == txID {
				txList.Remove(e)
				break
			}
		}

		// Clean up empty lists
		if txList.Len() == 0 {
			delete(accountShard.txLists, normalizedSender)
		}
	}

	accountShard.mu.Unlock()

	// Remove from base pool
	otp.mu.Lock()
	delete(otp.txMap, txID)
	// Note: Not removing from heap as it's expensive; will be cleaned during GetTransactions
	otp.mu.Unlock()

	// Return item to pool
	item.tx = common.Transaction{}
	item.index = -1
	otp.itemPool.Put(item)

	// Update metrics
	otp.totalTxCount.Add(^uint64(0)) // Decrement
	otp.shardMetrics[shardIdx].TxCount.Add(^uint64(0))

	return true
}

// GetTransactionsByAccountOptimized gets all transactions for an account efficiently
func (otp *OptimizedTransactionPool) GetTransactionsByAccountOptimized(account string) []common.Transaction {
	account = common.NormalizeAddress(account)
	shardIdx := otp.getShardIndex(account)

	accountShard := otp.accountShards[shardIdx]
	accountShard.mu.RLock()
	txList := accountShard.txLists[account]
	if txList == nil || txList.Len() == 0 {
		accountShard.mu.RUnlock()
		return nil
	}

	// Collect transaction IDs
	txIDs := make([]string, 0, txList.Len())
	for e := txList.Front(); e != nil; e = e.Next() {
		txIDs = append(txIDs, e.Value.(string))
	}
	accountShard.mu.RUnlock()

	// Fetch transactions from shards
	transactions := make([]common.Transaction, 0, len(txIDs))
	for _, txID := range txIDs {
		if tx, exists := otp.GetTransactionOptimized(txID); exists {
			transactions = append(transactions, *tx)
		}
	}

	return transactions
}

// evictLowPriorityTransactions evicts transactions with the lowest priority
func (otp *OptimizedTransactionPool) evictLowPriorityTransactions(count int) bool {
	otp.mu.Lock()
	defer otp.mu.Unlock()

	evicted := 0

	// Create a temporary slice of all transactions sorted by priority
	allTxs := make([]*TransactionItem, 0, len(*otp.pendingTxs))
	for _, item := range *otp.pendingTxs {
		if item != nil && otp.txMap[item.tx.ID] != nil {
			allTxs = append(allTxs, item)
		}
	}

	// Sort by priority (ascending - lowest priority first)
	sort.Slice(allTxs, func(i, j int) bool {
		return allTxs[i].priority < allTxs[j].priority
	})

	// Evict lowest priority transactions
	for i := 0; i < count && i < len(allTxs); i++ {
		item := allTxs[i]
		otp.RemoveTransactionOptimized(item.tx.ID)
		evicted++
		otp.evictedCount.Add(1)
	}

	return evicted > 0
}

// getShardIndex calculates the shard index for a given key
func (otp *OptimizedTransactionPool) getShardIndex(key string) int {
	hash := uint32(0)
	for i := 0; i < len(key); i++ {
		hash = hash*31 + uint32(key[i])
	}
	return int(hash % uint32(otp.shardCount))
}

// startWorkers starts background maintenance workers
func (otp *OptimizedTransactionPool) startWorkers() {
	// Metrics aggregator
	otp.workerWg.Add(1)
	go func() {
		defer otp.workerWg.Done()
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				otp.aggregateMetrics()
			case <-otp.stopCh:
				return
			}
		}
	}()

	// Expired transaction cleaner
	otp.workerWg.Add(1)
	go func() {
		defer otp.workerWg.Done()
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				otp.cleanExpiredTransactions()
			case <-otp.stopCh:
				return
			}
		}
	}()
}

// aggregateMetrics aggregates shard metrics for monitoring
func (otp *OptimizedTransactionPool) aggregateMetrics() {
	totalAccess := uint64(0)
	totalTx := uint64(0)

	for i := range otp.shardMetrics {
		totalAccess += otp.shardMetrics[i].AccessCount.Load()
		totalTx += otp.shardMetrics[i].TxCount.Load()
	}

	if totalAccess > 0 {
		otp.logger.WithFields(logrus.Fields{
			"total_tx":      totalTx,
			"total_access":  totalAccess,
			"evicted_count": otp.evictedCount.Load(),
			"shards":        otp.shardCount,
		}).Debug("Transaction pool metrics")
	}
}

// cleanExpiredTransactions removes expired transactions from all shards
func (otp *OptimizedTransactionPool) cleanExpiredTransactions() {
	now := consensus.ConsensusUnix()
	expiredCount := 0

	// Collect expired transactions
	expiredTxIDs := make([]string, 0)

	for i := 0; i < otp.shardCount; i++ {
		shard := otp.txShards[i]
		shard.mu.RLock()

		for txID, item := range shard.items {
			if item.tx.ExpiryTime > 0 && item.tx.ExpiryTime <= now {
				expiredTxIDs = append(expiredTxIDs, txID)
			}
		}

		shard.mu.RUnlock()
	}

	// Remove expired transactions
	for _, txID := range expiredTxIDs {
		if otp.RemoveTransactionOptimized(txID) {
			expiredCount++
		}
	}

	if expiredCount > 0 {
		otp.logger.WithField("count", expiredCount).Info("Cleaned expired transactions")
	}
}

// Close shuts down the optimized pool
func (otp *OptimizedTransactionPool) Close() error {
	close(otp.stopCh)
	otp.workerWg.Wait()
	return otp.TransactionPool.Close()
}

// GetStats returns extended statistics
func (otp *OptimizedTransactionPool) GetStats() map[string]interface{} {
	stats := otp.TransactionPool.GetStats()

	// Add optimized pool stats
	stats["shard_count"] = otp.shardCount
	stats["total_tx_optimized"] = otp.totalTxCount.Load()
	stats["evicted_count"] = otp.evictedCount.Load()

	// Per-shard stats
	shardStats := make([]map[string]interface{}, otp.shardCount)
	for i := 0; i < otp.shardCount; i++ {
		shardStats[i] = map[string]interface{}{
			"tx_count":     otp.shardMetrics[i].TxCount.Load(),
			"access_count": otp.shardMetrics[i].AccessCount.Load(),
		}
	}
	stats["shard_stats"] = shardStats

	return stats
}
