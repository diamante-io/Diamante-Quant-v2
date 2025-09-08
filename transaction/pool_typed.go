// Package transaction provides a typed transaction pool implementation
package transaction

import (
	"container/heap"
	"fmt"
	"sync"
	"time"

	"diamante/consensus"
	"diamante/types"

	"github.com/sirupsen/logrus"
)

// TypedPool represents a typed transaction pool
type TypedPool struct {
	// Core components
	transactions map[string]*types.TransactionPoolItem
	queue        *TypedPriorityQueue
	pending      map[string][]*types.TypedTransaction // Transactions by sender

	// Configuration
	maxPoolSize int
	txTimeout   time.Duration
	minFee      float64
	logger      *logrus.Logger

	// State
	mu           sync.RWMutex
	nonces       map[string]uint64 // Current nonce for each address
	priceHistory []uint64          // Gas price history for estimation

	// Metrics
	metrics *types.TransactionMetrics
}

// TypedPriorityQueue implements heap.Interface for transaction prioritization
type TypedPriorityQueue struct {
	items []*TypedTransactionItem
}

// TypedTransactionItem wraps a transaction for priority queue
type TypedTransactionItem struct {
	tx       *types.TypedTransaction
	priority int // Calculated based on gas price and other factors
	index    int // Index in the heap
}

// NewTypedPool creates a new typed transaction pool
func NewTypedPool(maxPoolSize int, txTimeout time.Duration, minFee float64, logger *logrus.Logger) *TypedPool {
	if logger == nil {
		logger = logrus.New()
	}

	pool := &TypedPool{
		transactions: make(map[string]*types.TransactionPoolItem),
		queue:        &TypedPriorityQueue{items: make([]*TypedTransactionItem, 0)},
		pending:      make(map[string][]*types.TypedTransaction),
		maxPoolSize:  maxPoolSize,
		txTimeout:    txTimeout,
		minFee:       minFee,
		logger:       logger,
		nonces:       make(map[string]uint64),
		priceHistory: make([]uint64, 0, 100),
		metrics: &types.TransactionMetrics{
			TotalReceived:  0,
			TotalProcessed: 0,
			TotalFailed:    0,
			TotalDropped:   0,
		},
	}

	heap.Init(pool.queue)
	return pool
}

// AddTransaction adds a typed transaction to the pool
func (p *TypedPool) AddTransaction(tx *types.TypedTransaction) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Validate transaction
	if err := p.validateTransaction(tx); err != nil {
		return fmt.Errorf("transaction validation failed: %w", err)
	}

	// Check if transaction already exists
	if _, exists := p.transactions[tx.ID]; exists {
		return fmt.Errorf("transaction %s already in pool", tx.ID)
	}

	// Check pool capacity
	if len(p.transactions) >= p.maxPoolSize {
		// Try to evict lower priority transaction
		if !p.evictLowerPriority(tx) {
			p.metrics.TotalDropped++
			return fmt.Errorf("pool is full")
		}
	}

	// Create pool item
	poolItem := &types.TransactionPoolItem{
		Transaction: tx,
		ReceivedAt:  consensus.ConsensusNow(),
		Priority:    tx.Priority,
		Retries:     0,
	}

	// Add to pool
	p.transactions[tx.ID] = poolItem

	// Add to pending by sender
	p.pending[tx.From] = append(p.pending[tx.From], tx)

	// Add to priority queue
	item := &TypedTransactionItem{
		tx:       tx,
		priority: p.calculatePriority(tx),
	}
	heap.Push(p.queue, item)

	// Update metrics
	p.metrics.TotalReceived++
	p.metrics.PoolSize = uint64(len(p.transactions))

	// Track gas price
	p.updateGasPriceHistory(tx.GasPrice)

	p.logger.WithFields(logrus.Fields{
		"tx_id":     tx.ID,
		"type":      tx.Type,
		"from":      tx.From,
		"gas_price": tx.GasPrice,
		"priority":  item.priority,
	}).Debug("Transaction added to pool")

	return nil
}

// GetTransaction retrieves a transaction by ID
func (p *TypedPool) GetTransaction(id string) (*types.TypedTransaction, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	item, exists := p.transactions[id]
	if !exists {
		return nil, false
	}

	return item.Transaction, true
}

// GetPendingTransactions returns pending transactions up to the limit
func (p *TypedPool) GetPendingTransactions(limit int) []*types.TypedTransaction {
	p.mu.RLock()
	defer p.mu.RUnlock()

	result := make([]*types.TypedTransaction, 0, limit)

	// Create a copy of the priority queue
	tempQueue := make([]*TypedTransactionItem, len(p.queue.items))
	copy(tempQueue, p.queue.items)

	// Sort by priority
	heap.Init(&TypedPriorityQueue{items: tempQueue})

	// Get top transactions
	for i := 0; i < limit && len(tempQueue) > 0; i++ {
		item := heap.Pop(&TypedPriorityQueue{items: tempQueue}).(*TypedTransactionItem)
		result = append(result, item.tx)
	}

	return result
}

// RemoveTransaction removes a transaction from the pool
func (p *TypedPool) RemoveTransaction(id string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	item, exists := p.transactions[id]
	if !exists {
		return false
	}

	// Remove from transactions map
	delete(p.transactions, id)

	// Remove from pending
	if txs, ok := p.pending[item.Transaction.From]; ok {
		filtered := make([]*types.TypedTransaction, 0, len(txs))
		for _, tx := range txs {
			if tx.ID != id {
				filtered = append(filtered, tx)
			}
		}
		if len(filtered) == 0 {
			delete(p.pending, item.Transaction.From)
		} else {
			p.pending[item.Transaction.From] = filtered
		}
	}

	// Remove from priority queue (mark as nil, will be cleaned on next heap operation)
	for i, qItem := range p.queue.items {
		if qItem != nil && qItem.tx.ID == id {
			p.queue.items[i] = nil
			break
		}
	}

	// Update metrics
	p.metrics.PoolSize = uint64(len(p.transactions))

	return true
}

// ValidateTransaction validates a transaction
func (p *TypedPool) validateTransaction(tx *types.TypedTransaction) error {
	// Basic validation
	if tx.ID == "" {
		return fmt.Errorf("transaction ID is empty")
	}

	if tx.From == "" {
		return fmt.Errorf("sender address is empty")
	}

	if tx.GasLimit == 0 {
		return fmt.Errorf("gas limit is zero")
	}

	if tx.GasPrice == 0 {
		return fmt.Errorf("gas price is zero")
	}

	// Check minimum gas price (using minFee as proxy for gas price)
	if float64(tx.GasPrice) < p.minFee {
		return fmt.Errorf("gas price %d below minimum %f", tx.GasPrice, p.minFee)
	}

	// Type-specific validation
	switch tx.Type {
	case types.TransactionTypeTransfer:
		if tx.To == "" {
			return fmt.Errorf("transfer transaction missing recipient")
		}

	case types.TransactionTypeContractDeploy:
		if tx.Data == nil || tx.Data.ContractDeploy == nil {
			return fmt.Errorf("contract deploy transaction missing data")
		}
		if len(tx.Data.ContractDeploy.ByteCode) == 0 {
			return fmt.Errorf("contract bytecode is empty")
		}

	case types.TransactionTypeContractCall:
		if tx.To == "" {
			return fmt.Errorf("contract call missing contract address")
		}
		if tx.Data == nil || tx.Data.ContractCall == nil {
			return fmt.Errorf("contract call transaction missing data")
		}
	}

	// Check nonce
	if expectedNonce, ok := p.nonces[tx.From]; ok {
		if tx.Nonce < expectedNonce {
			return fmt.Errorf("nonce %d is less than expected %d", tx.Nonce, expectedNonce)
		}
	}

	return nil
}

// calculatePriority calculates transaction priority
func (p *TypedPool) calculatePriority(tx *types.TypedTransaction) int {
	// Base priority on gas price
	priority := int(tx.GasPrice / 1000000000) // Convert to gwei

	// Adjust for transaction type
	switch tx.Type {
	case types.TransactionTypeValidatorUpdate:
		priority += 1000 // High priority for validator updates
	case types.TransactionTypeGovernance:
		priority += 500 // Medium-high priority for governance
	case types.TransactionTypeContractDeploy:
		priority += 100 // Slight boost for deployments
	}

	// Adjust for explicit priority
	switch tx.Priority {
	case types.TransactionPriorityUrgent:
		priority *= 4
	case types.TransactionPriorityHigh:
		priority *= 2
	case types.TransactionPriorityLow:
		priority /= 2
	}

	// Consider age (older transactions get slight boost)
	age := consensus.ConsensusSince(time.Unix(tx.Timestamp, 0))
	priority += int(age.Minutes())

	return priority
}

// evictLowerPriority attempts to evict a lower priority transaction
func (p *TypedPool) evictLowerPriority(newTx *types.TypedTransaction) bool {
	newPriority := p.calculatePriority(newTx)

	// Find lowest priority transaction
	var lowestItem *TypedTransactionItem
	lowestPriority := newPriority

	for _, item := range p.queue.items {
		if item != nil && item.priority < lowestPriority {
			lowestPriority = item.priority
			lowestItem = item
		}
	}

	if lowestItem != nil {
		// Evict the lowest priority transaction
		p.RemoveTransaction(lowestItem.tx.ID)
		p.metrics.TotalDropped++

		p.logger.WithFields(logrus.Fields{
			"evicted_tx":   lowestItem.tx.ID,
			"new_tx":       newTx.ID,
			"old_priority": lowestPriority,
			"new_priority": newPriority,
		}).Debug("Evicted lower priority transaction")

		return true
	}

	return false
}

// updateGasPriceHistory updates the gas price history
func (p *TypedPool) updateGasPriceHistory(gasPrice uint64) {
	p.priceHistory = append(p.priceHistory, gasPrice)
	if len(p.priceHistory) > 100 {
		p.priceHistory = p.priceHistory[1:]
	}
}

// GetMetrics returns pool metrics
func (p *TypedPool) GetMetrics() *types.TransactionMetrics {
	p.mu.RLock()
	defer p.mu.RUnlock()

	metrics := *p.metrics
	metrics.QueuedCount = uint64(p.queue.Len())

	// Calculate average gas price from history
	if len(p.priceHistory) > 0 {
		var total uint64
		for _, price := range p.priceHistory {
			total += price
		}
		metrics.AvgGasUsed = total / uint64(len(p.priceHistory))
	}

	return &metrics
}

// UpdateNonce updates the expected nonce for an address
func (p *TypedPool) UpdateNonce(address string, nonce uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.nonces[address] = nonce

	// Remove any transactions with outdated nonces
	if txs, ok := p.pending[address]; ok {
		valid := make([]*types.TypedTransaction, 0)
		for _, tx := range txs {
			if tx.Nonce >= nonce {
				valid = append(valid, tx)
			} else {
				// Remove outdated transaction
				delete(p.transactions, tx.ID)
				p.metrics.TotalDropped++
			}
		}
		if len(valid) == 0 {
			delete(p.pending, address)
		} else {
			p.pending[address] = valid
		}
	}
}

// Priority queue implementation

func (pq *TypedPriorityQueue) Len() int { return len(pq.items) }

func (pq *TypedPriorityQueue) Less(i, j int) bool {
	// Higher priority = earlier in queue
	return pq.items[i].priority > pq.items[j].priority
}

func (pq *TypedPriorityQueue) Swap(i, j int) {
	pq.items[i], pq.items[j] = pq.items[j], pq.items[i]
	pq.items[i].index = i
	pq.items[j].index = j
}

func (pq *TypedPriorityQueue) Push(x interface{}) {
	n := len(pq.items)
	item := x.(*TypedTransactionItem)
	item.index = n
	pq.items = append(pq.items, item)
}

func (pq *TypedPriorityQueue) Pop() interface{} {
	old := pq.items
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // avoid memory leak
	item.index = -1 // for safety
	pq.items = old[0 : n-1]
	return item
}

// Clean removes nil entries from the queue
func (pq *TypedPriorityQueue) Clean() {
	cleaned := make([]*TypedTransactionItem, 0, len(pq.items))
	for _, item := range pq.items {
		if item != nil {
			cleaned = append(cleaned, item)
		}
	}
	pq.items = cleaned
	heap.Init(pq)
}
