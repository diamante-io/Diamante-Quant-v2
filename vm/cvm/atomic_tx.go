package cvm

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
)

// AtomicTransactionManager manages atomic cross-VM transactions
type AtomicTransactionManager struct {
	activeTxs   map[string]*AtomicTransaction
	snapshots   map[string][]StateSnapshot
	rollbackMgr *RollbackManager
	mu          sync.RWMutex
	logger      *logrus.Logger

	// Configuration
	maxConcurrentTxs int
	txTimeout        time.Duration
}

// StateSnapshot represents a VM state snapshot
type StateSnapshot struct {
	VM         VMType
	SnapshotID string
	StateRoot  []byte
	Timestamp  time.Time
	Data       map[string][]byte // Additional state data
}

// RollbackManager handles state rollbacks
type RollbackManager struct {
	rollbackHandlers map[VMType]RollbackHandler
	mu               sync.RWMutex
}

// RollbackHandler defines the interface for VM-specific rollback
type RollbackHandler interface {
	Rollback(checkpointID string) error
	VerifyRollback(checkpointID string) error
}

// NewAtomicTransactionManager creates a new transaction manager
func NewAtomicTransactionManager(logger *logrus.Logger) *AtomicTransactionManager {
	return &AtomicTransactionManager{
		activeTxs:        make(map[string]*AtomicTransaction),
		snapshots:        make(map[string][]StateSnapshot),
		rollbackMgr:      NewRollbackManager(),
		logger:           logger,
		maxConcurrentTxs: 1000,
		txTimeout:        30 * time.Second,
	}
}

// NewRollbackManager creates a new rollback manager
func NewRollbackManager() *RollbackManager {
	return &RollbackManager{
		rollbackHandlers: make(map[VMType]RollbackHandler),
	}
}

// BeginTransaction starts a new atomic transaction
func (m *AtomicTransactionManager) BeginTransaction(ctx context.Context, initialMsg CVMMessage) (*AtomicTransaction, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check concurrent transaction limit
	if len(m.activeTxs) >= m.maxConcurrentTxs {
		return nil, fmt.Errorf("maximum concurrent transactions reached: %d", m.maxConcurrentTxs)
	}

	// Create new transaction
	tx := &AtomicTransaction{
		ID:        uuid.New().String(),
		Messages:  []CVMMessage{initialMsg},
		State:     TxStatePending,
		StartTime: time.Now(),
		GasLimit:  initialMsg.GasLimit,
	}

	m.activeTxs[tx.ID] = tx
	m.snapshots[tx.ID] = []StateSnapshot{}

	// Start timeout monitor
	go m.monitorTimeout(ctx, tx.ID)

	m.logger.Infof("Started atomic transaction: %s", tx.ID)
	return tx, nil
}

// AddCheckpoint adds a checkpoint to the transaction
func (m *AtomicTransactionManager) AddCheckpoint(txID string, checkpoint Checkpoint) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	tx, exists := m.activeTxs[txID]
	if !exists {
		return fmt.Errorf("transaction not found: %s", txID)
	}

	if tx.State != TxStatePending && tx.State != TxStateExecuting {
		return fmt.Errorf("cannot add checkpoint to transaction in state: %v", tx.State)
	}

	tx.Checkpoints = append(tx.Checkpoints, checkpoint)

	// Create state snapshot
	snapshot := StateSnapshot{
		VM:         checkpoint.VM,
		SnapshotID: checkpoint.CheckpointID,
		StateRoot:  checkpoint.StateRoot,
		Timestamp:  checkpoint.Timestamp,
		Data:       make(map[string][]byte),
	}

	m.snapshots[txID] = append(m.snapshots[txID], snapshot)

	return nil
}

// CommitTransaction commits an atomic transaction
func (m *AtomicTransactionManager) CommitTransaction(txID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	tx, exists := m.activeTxs[txID]
	if !exists {
		return fmt.Errorf("transaction not found: %s", txID)
	}

	if tx.State != TxStateExecuting && tx.State != TxStatePending {
		return fmt.Errorf("cannot commit transaction in state: %v", tx.State)
	}

	// Update state
	tx.State = TxStateCommitting

	// Perform two-phase commit
	if err := m.performTwoPhaseCommit(tx); err != nil {
		tx.State = TxStateFailed
		return fmt.Errorf("two-phase commit failed: %w", err)
	}

	// Finalize transaction
	tx.State = TxStateCommitted
	endTime := time.Now()
	tx.EndTime = &endTime

	// Clean up snapshots
	delete(m.snapshots, txID)

	m.logger.Infof("Committed transaction %s in %v", txID, endTime.Sub(tx.StartTime))

	// Remove from active transactions after a delay to allow queries
	go func() {
		time.Sleep(5 * time.Second)
		m.mu.Lock()
		delete(m.activeTxs, txID)
		m.mu.Unlock()
	}()

	return nil
}

// RollbackTransaction rolls back an atomic transaction
func (m *AtomicTransactionManager) RollbackTransaction(txID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	tx, exists := m.activeTxs[txID]
	if !exists {
		return fmt.Errorf("transaction not found: %s", txID)
	}

	if tx.State == TxStateCommitted || tx.State == TxStateRolledBack {
		return fmt.Errorf("cannot rollback transaction in state: %v", tx.State)
	}

	// Update state
	tx.State = TxStateRollingBack

	// Rollback in reverse order
	snapshots := m.snapshots[txID]
	for i := len(snapshots) - 1; i >= 0; i-- {
		snapshot := snapshots[i]
		if err := m.rollbackSnapshot(snapshot); err != nil {
			m.logger.Errorf("Failed to rollback snapshot %s: %v", snapshot.SnapshotID, err)
			tx.State = TxStateFailed
			return fmt.Errorf("rollback failed at snapshot %s: %w", snapshot.SnapshotID, err)
		}
	}

	// Finalize rollback
	tx.State = TxStateRolledBack
	endTime := time.Now()
	tx.EndTime = &endTime

	// Clean up
	delete(m.snapshots, txID)
	delete(m.activeTxs, txID)

	m.logger.Infof("Rolled back transaction %s", txID)
	return nil
}

// performTwoPhaseCommit implements two-phase commit protocol
func (m *AtomicTransactionManager) performTwoPhaseCommit(tx *AtomicTransaction) error {
	// Phase 1: Prepare
	preparedVMs := make(map[VMType]bool)
	for _, checkpoint := range tx.Checkpoints {
		// TODO: Implement prepare phase for each VM
		// For now, we assume all VMs are prepared
		preparedVMs[checkpoint.VM] = true
	}

	// Check if all VMs are prepared
	for vm, prepared := range preparedVMs {
		if !prepared {
			return fmt.Errorf("VM %s failed to prepare", vm)
		}
	}

	// Phase 2: Commit
	for _, checkpoint := range tx.Checkpoints {
		// TODO: Implement commit phase for each VM
		// For now, we assume successful commit
		m.logger.Debugf("Committing checkpoint %s for VM %s", checkpoint.CheckpointID, checkpoint.VM)
	}

	return nil
}

// rollbackSnapshot rolls back a single snapshot
func (m *AtomicTransactionManager) rollbackSnapshot(snapshot StateSnapshot) error {
	handler := m.rollbackMgr.GetHandler(snapshot.VM)
	if handler == nil {
		return fmt.Errorf("no rollback handler for VM %s", snapshot.VM)
	}

	// Perform rollback
	if err := handler.Rollback(snapshot.SnapshotID); err != nil {
		return fmt.Errorf("rollback failed: %w", err)
	}

	// Verify rollback
	if err := handler.VerifyRollback(snapshot.SnapshotID); err != nil {
		return fmt.Errorf("rollback verification failed: %w", err)
	}

	return nil
}

// monitorTimeout monitors transaction timeout
func (m *AtomicTransactionManager) monitorTimeout(ctx context.Context, txID string) {
	timer := time.NewTimer(m.txTimeout)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return
	case <-timer.C:
		m.mu.RLock()
		tx, exists := m.activeTxs[txID]
		m.mu.RUnlock()

		if exists && tx.State != TxStateCommitted && tx.State != TxStateRolledBack {
			m.logger.Warnf("Transaction %s timed out, initiating rollback", txID)
			if err := m.RollbackTransaction(txID); err != nil {
				m.logger.Errorf("Failed to rollback timed-out transaction %s: %v", txID, err)
			}
		}
	}
}

// GetTransaction retrieves a transaction by ID
func (m *AtomicTransactionManager) GetTransaction(txID string) (*AtomicTransaction, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	tx, exists := m.activeTxs[txID]
	if !exists {
		return nil, fmt.Errorf("transaction not found: %s", txID)
	}

	// Return a copy to prevent external modification
	copy := *tx
	return &copy, nil
}

// GetActiveTransactionCount returns the number of active transactions
func (m *AtomicTransactionManager) GetActiveTransactionCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return len(m.activeTxs)
}

// RegisterRollbackHandler registers a rollback handler for a VM type
func (r *RollbackManager) RegisterHandler(vmType VMType, handler RollbackHandler) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.rollbackHandlers[vmType]; exists {
		return fmt.Errorf("rollback handler already registered for VM %s", vmType)
	}

	r.rollbackHandlers[vmType] = handler
	return nil
}

// GetHandler returns the rollback handler for a VM type
func (r *RollbackManager) GetHandler(vmType VMType) RollbackHandler {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.rollbackHandlers[vmType]
}

// TransactionMetrics provides transaction statistics
type TransactionMetrics struct {
	TotalTransactions      int64
	CommittedTransactions  int64
	RolledBackTransactions int64
	FailedTransactions     int64
	AverageCommitTime      time.Duration
	ActiveTransactions     int
}

// GetMetrics returns transaction metrics
func (m *AtomicTransactionManager) GetMetrics() TransactionMetrics {
	m.mu.RLock()
	defer m.mu.RUnlock()

	metrics := TransactionMetrics{
		ActiveTransactions: len(m.activeTxs),
	}

	var totalCommitTime time.Duration
	var committedCount int64

	for _, tx := range m.activeTxs {
		metrics.TotalTransactions++

		switch tx.State {
		case TxStateCommitted:
			metrics.CommittedTransactions++
			if tx.EndTime != nil {
				totalCommitTime += tx.EndTime.Sub(tx.StartTime)
				committedCount++
			}
		case TxStateRolledBack:
			metrics.RolledBackTransactions++
		case TxStateFailed:
			metrics.FailedTransactions++
		}
	}

	if committedCount > 0 {
		metrics.AverageCommitTime = totalCommitTime / time.Duration(committedCount)
	}

	return metrics
}
