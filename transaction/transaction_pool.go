package transaction

import (
	"container/heap"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"diamante/common"
	"diamante/crypto"
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

	// If you need dynamic network load, define:
	networkHealthFn func() int
	keyManager      *crypto.KeyManager
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
}

// TransactionItem is the internal struct for the priority queue.
type TransactionItem struct {
	tx       common.Transaction
	priority int
	index    int
}

// PriorityQueue satisfies the heap.Interface.
type PriorityQueue []*TransactionItem

// NewTransactionPool configures an in-memory pool.
func NewTransactionPool(
	maxPoolSize int,
	txTimeout time.Duration,
	minFee, maxFee float64,
	conflictResolutionEnabled bool,
	expirationDuration time.Duration,
	networkHealthFn func() int,
	keyMgr *crypto.KeyManager,
) *TransactionPool {
	pq := make(PriorityQueue, 0, maxPoolSize)
	heap.Init(&pq)

	return &TransactionPool{
		pendingTxs:                &pq,
		txMap:                     make(map[string]*TransactionItem),
		maxPoolSize:               maxPoolSize,
		txTimeout:                 txTimeout,
		minFee:                    minFee,
		maxFee:                    maxFee,
		conflictResolutionEnabled: conflictResolutionEnabled,
		expirationDuration:        expirationDuration,
		poolMetrics:               PoolMetrics{},
		networkHealthFn:           networkHealthFn,
		keyManager:                keyMgr,
	}
}

// AddTransaction => validates & pushes into the priority queue.
func (tp *TransactionPool) AddTransaction(tx common.Transaction) error {
	tp.mu.Lock()
	defer tp.mu.Unlock()

	// Possibly optimize if near capacity
	if len(*tp.pendingTxs) >= tp.maxPoolSize {
		tp.optimizePool()
		if len(*tp.pendingTxs) >= tp.maxPoolSize {
			return errors.New("pool is still full after optimization")
		}
	}

	// no duplicates
	if _, exists := tp.txMap[tx.ID]; exists {
		return fmt.Errorf("tx %s already in pool", tx.ID)
	}

	// validate
	if err := tp.validateTransaction(tx); err != nil {
		tp.poolMetrics.InvalidTxs++
		return err
	}

	item := &TransactionItem{
		tx:       tx,
		priority: tp.calculatePriority(tx),
	}
	heap.Push(tp.pendingTxs, item)
	tp.txMap[tx.ID] = item

	tp.poolMetrics.ValidTxs++
	tp.poolMetrics.TotalFees += tx.Fee
	tp.poolMetrics.TotalTxs++

	log.Printf("Pool: transaction %s added (priority=%d).", tx.ID, item.priority)
	return nil
}

// validateTransaction uses local validation + extra checks you want.
func (tp *TransactionPool) validateTransaction(tx common.Transaction) error {
	ephemeralNT := NewDefaultNonceTracker()

	if err := ValidateTransaction(tx, tp.minFee, ephemeralNT); err != nil {
		return err
	}
	if tx.Fee < tp.minFee {
		return fmt.Errorf("fee %f < minFee %f", tx.Fee, tp.minFee)
	}
	// check signature
	if !tp.verifySignature(tx) {
		return errors.New("invalid signature")
	}
	// check double-spend
	if err := tp.checkDoubleSpendOrNonce(tx); err != nil {
		return err
	}
	return nil
}

// verifySignature => uses default Dilithium-based check
func (tp *TransactionPool) verifySignature(tx common.Transaction) bool {
	senderAcc := common.GetAccount(tx.Sender)
	if senderAcc == nil {
		log.Printf("Pool: cannot verify %s, sender %s not found", tx.ID, tx.Sender)
		return false
	}
	valid, err := crypto.VerifySignature(senderAcc.PublicKey, []byte(tx.ID), tx.Signature)
	if err != nil {
		log.Printf("Pool: error verifying sig for %s: %v", tx.ID, err)
		return false
	}
	return valid
}

// checkDoubleSpendOrNonce => if same sender+nonce is already in the pool, conflict.
func (tp *TransactionPool) checkDoubleSpendOrNonce(tx common.Transaction) error {
	for _, item := range tp.txMap {
		if item.tx.Sender == tx.Sender && item.tx.Nonce == tx.Nonce {
			return fmt.Errorf("double-spend or nonce reuse for tx %s", tx.ID)
		}
	}
	return nil
}

// RemoveTransaction => remove from priority queue & map
func (tp *TransactionPool) RemoveTransaction(txID string) error {
	tp.mu.Lock()
	defer tp.mu.Unlock()

	item, exists := tp.txMap[txID]
	if !exists {
		return fmt.Errorf("tx %s not found", txID)
	}
	heap.Remove(tp.pendingTxs, item.index)
	delete(tp.txMap, txID)
	tp.poolMetrics.TotalTxs--

	log.Printf("Pool: transaction %s removed.", txID)
	return nil
}

// GetTransaction => fetch from map
func (tp *TransactionPool) GetTransaction(txID string) (*common.Transaction, error) {
	tp.mu.RLock()
	defer tp.mu.RUnlock()

	item, exists := tp.txMap[txID]
	if !exists {
		return nil, fmt.Errorf("tx %s not in pool", txID)
	}
	return &item.tx, nil
}

// HasTransaction => check map
func (tp *TransactionPool) HasTransaction(txID string) bool {
	tp.mu.RLock()
	defer tp.mu.RUnlock()

	_, exists := tp.txMap[txID]
	return exists
}

// HandleConflicts => optional conflict resolution
func (tp *TransactionPool) HandleConflicts() {
	if !tp.conflictResolutionEnabled {
		return
	}
	log.Println("Pool: conflict resolution triggered.")
	// e.g., keep highest fee among same (sender+nonce)
}

// CleanupPool => remove expired or invalid transactions
func (tp *TransactionPool) CleanupPool() {
	tp.mu.Lock()
	defer tp.mu.Unlock()

	now := time.Now().Unix()
	var toRemove []*TransactionItem

	for _, item := range *tp.pendingTxs {
		ephemeralNT := NewDefaultNonceTracker()
		if now > item.tx.ExpiryTime ||
			!tp.verifySignature(item.tx) ||
			ValidateTransaction(item.tx, tp.minFee, ephemeralNT) != nil {
			toRemove = append(toRemove, item)
		}
	}

	for _, rm := range toRemove {
		heap.Remove(tp.pendingTxs, rm.index)
		delete(tp.txMap, rm.tx.ID)
		tp.poolMetrics.ExpiredTxs++
		log.Printf("Pool: removed expired/invalid tx %s.", rm.tx.ID)
	}
	tp.poolMetrics.LastCleanupTime = time.Now()
}

// optimizePool => evict low-priority if over capacity, possibly adjust fees
func (tp *TransactionPool) optimizePool() {
	tp.mu.Lock()
	defer tp.mu.Unlock()

	log.Println("Pool: optimizing...")

	var netLoad int
	if tp.networkHealthFn != nil {
		netLoad = tp.networkHealthFn()
	}
	tp.AdjustFees(netLoad)

	for len(*tp.pendingTxs) > tp.maxPoolSize {
		lowest := heap.Pop(tp.pendingTxs).(*TransactionItem)
		delete(tp.txMap, lowest.tx.ID)
		log.Printf("Pool: evicting low-priority tx %s to free space.", lowest.tx.ID)
	}
	tp.poolMetrics.LastOptimizationTime = time.Now()
}

// AdjustFees => modifies minFee according to some network load
func (tp *TransactionPool) AdjustFees(networkLoad int) {
	tp.mu.Lock()
	defer tp.mu.Unlock()

	if networkLoad > 75 {
		tp.minFee *= 1.1
	} else if networkLoad < 25 {
		tp.minFee *= 0.9
	}
	log.Printf("Pool: minFee adjusted to %f (load=%d).", tp.minFee, networkLoad)
}

// MonitorPoolSize => logs size on a fixed interval
func (tp *TransactionPool) MonitorPoolSize() {
	ticker := time.NewTicker(30 * time.Second)
	for range ticker.C {
		tp.mu.RLock()
		sz := len(*tp.pendingTxs)
		tp.mu.RUnlock()
		log.Printf("TransactionPool: size = %d", sz)
	}
}

// calculatePriority => simple (feePerByte + age) metric
func (tp *TransactionPool) calculatePriority(tx common.Transaction) int {
	feePerByte := tx.Fee / float64(len(tx.ID))
	ageSeconds := time.Now().Unix() - tx.Timestamp
	return int(feePerByte) + int(ageSeconds/60)
}

// PriorityQueue methods for the heap
func (pq PriorityQueue) Len() int           { return len(pq) }
func (pq PriorityQueue) Less(i, j int) bool { return pq[i].priority > pq[j].priority }
func (pq PriorityQueue) Swap(i, j int)      { pq[i], pq[j] = pq[j], pq[i]; pq[i].index = i; pq[j].index = j }
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
	item.index = -1
	*pq = old[0 : n-1]
	return item
}

// PoolSize returns the number of pending transactions in the pool.
func (tp *TransactionPool) PoolSize() int {
	tp.mu.RLock()
	defer tp.mu.RUnlock()
	return len(*tp.pendingTxs)
}
