// consensus/lock_ordering.go

package consensus

import (
	"fmt"
	"sync"
)

// LockOrderViolationError represents an error when locks are acquired in the wrong order
type LockOrderViolationError struct {
	CurrentLock string
	HeldLock    string
}

func (e *LockOrderViolationError) Error() string {
	return fmt.Sprintf("lock order violation: attempting to acquire %s while holding %s", e.CurrentLock, e.HeldLock)
}

// LockOrder defines the ordering of locks to prevent deadlocks
type LockOrder struct {
	// Map of lock name to its order (lower numbers must be acquired before higher numbers)
	lockOrders map[string]int

	// Map to track currently held locks by goroutine
	heldLocksMu sync.Mutex
	heldLocks   map[int64]map[string]bool

	// Deadlock detector for monitoring lock acquisition times
	detector *DeadlockDetector

	// Whether to enforce lock ordering
	enforceOrdering bool

	// Logger for reporting violations
	logger *hybridConsensusLogger
}

// NewLockOrder creates a new LockOrder with the specified lock ordering
func NewLockOrder(logger *hybridConsensusLogger, detector *DeadlockDetector, enforceOrdering bool) *LockOrder {
	return &LockOrder{
		lockOrders:      make(map[string]int),
		heldLocks:       make(map[int64]map[string]bool),
		detector:        detector,
		enforceOrdering: enforceOrdering,
		logger:          logger,
	}
}

// RegisterLock registers a lock with its order
func (lo *LockOrder) RegisterLock(lockName string, order int) {
	lo.lockOrders[lockName] = order
}

// RegisterLocks registers multiple locks with their orders
func (lo *LockOrder) RegisterLocks(locks map[string]int) {
	for lockName, order := range locks {
		lo.RegisterLock(lockName, order)
	}
}

// BeforeAcquire should be called before acquiring a lock to check for lock order violations
func (lo *LockOrder) BeforeAcquire(lockName string, goroutineID int64) error {
	if !lo.enforceOrdering {
		return nil
	}

	lo.heldLocksMu.Lock()
	defer lo.heldLocksMu.Unlock()

	// Get the order of the current lock
	currentOrder, exists := lo.lockOrders[lockName]
	if !exists {
		lo.logger.Warn("Lock not registered in ordering system", LogKeyValue{Key: "lockName", Value: lockName})
		return nil
	}

	// Get the locks held by this goroutine
	heldLocks, exists := lo.heldLocks[goroutineID]
	if !exists {
		// No locks held by this goroutine yet
		lo.heldLocks[goroutineID] = map[string]bool{lockName: true}
		return nil
	}

	// Check if acquiring this lock would violate the lock ordering
	for heldLock := range heldLocks {
		heldOrder, exists := lo.lockOrders[heldLock]
		if !exists {
			continue
		}

		if currentOrder < heldOrder {
			// Violation: trying to acquire a lock with lower order than a held lock
			err := &LockOrderViolationError{
				CurrentLock: lockName,
				HeldLock:    heldLock,
			}

			lo.logger.Error("Lock order violation detected",
				LogKeyValue{Key: "currentLock", Value: lockName},
				LogKeyValue{Key: "currentOrder", Value: fmt.Sprintf("%d", currentOrder)},
				LogKeyValue{Key: "heldLock", Value: heldLock},
				LogKeyValue{Key: "heldOrder", Value: fmt.Sprintf("%d", heldOrder)},
				LogKeyValue{Key: "goroutineID", Value: fmt.Sprintf("%d", goroutineID)})

			return err
		}
	}

	// No violation, record that this lock will be held
	heldLocks[lockName] = true
	return nil
}

// AfterRelease should be called after releasing a lock
func (lo *LockOrder) AfterRelease(lockName string, goroutineID int64) {
	if !lo.enforceOrdering {
		return
	}

	lo.heldLocksMu.Lock()
	defer lo.heldLocksMu.Unlock()

	// Get the locks held by this goroutine
	heldLocks, exists := lo.heldLocks[goroutineID]
	if !exists {
		// No locks held by this goroutine, which is unexpected
		lo.logger.Warn("Releasing lock that wasn't recorded as acquired",
			LogKeyValue{Key: "lockName", Value: lockName},
			LogKeyValue{Key: "goroutineID", Value: fmt.Sprintf("%d", goroutineID)})
		return
	}

	// Remove this lock from the held locks
	delete(heldLocks, lockName)

	// If no more locks are held by this goroutine, remove the entry
	if len(heldLocks) == 0 {
		delete(lo.heldLocks, goroutineID)
	}
}

// OrderedMutex is a mutex that enforces lock ordering
type OrderedMutex struct {
	mu        sync.Mutex
	name      string
	lockOrder *LockOrder
	detector  *DeadlockDetector
	logger    *hybridConsensusLogger
}

// NewOrderedMutex creates a new mutex that enforces lock ordering
func NewOrderedMutex(name string, lockOrder *LockOrder, detector *DeadlockDetector, logger *hybridConsensusLogger) *OrderedMutex {
	return &OrderedMutex{
		name:      name,
		lockOrder: lockOrder,
		detector:  detector,
		logger:    logger,
	}
}

// Lock acquires the mutex, checking for lock order violations
func (m *OrderedMutex) Lock() error {
	// Check for lock order violations
	goroutineID := getGoroutineID()
	if m.lockOrder != nil {
		if err := m.lockOrder.BeforeAcquire(m.name, goroutineID); err != nil {
			// Log the error and return it instead of panicking
			if m.logger != nil {
				m.logger.Error("Failed to acquire lock due to ordering violation",
					LogKeyValue{Key: "lockName", Value: m.name},
					LogKeyValue{Key: "error", Value: err.Error()})
			}
			return fmt.Errorf("failed to acquire lock %s: %w", m.name, err)
		}
	}

	// Acquire the lock
	m.mu.Lock()

	// Notify the deadlock detector
	if m.detector != nil {
		m.detector.LockAcquired(m.name)
	}

	return nil
}

// TryLock attempts to acquire the mutex without blocking
func (m *OrderedMutex) TryLock() (bool, error) {
	// Check for lock order violations
	goroutineID := getGoroutineID()
	if m.lockOrder != nil {
		if err := m.lockOrder.BeforeAcquire(m.name, goroutineID); err != nil {
			// Log the error and return it
			if m.logger != nil {
				m.logger.Error("Failed to acquire lock due to ordering violation",
					LogKeyValue{Key: "lockName", Value: m.name},
					LogKeyValue{Key: "error", Value: err.Error()})
			}
			return false, fmt.Errorf("failed to acquire lock %s: %w", m.name, err)
		}
	}

	// Try to acquire the lock
	if m.mu.TryLock() {
		// Notify the deadlock detector
		if m.detector != nil {
			m.detector.LockAcquired(m.name)
		}
		return true, nil
	}

	// Failed to acquire, cleanup lock order tracking
	if m.lockOrder != nil {
		m.lockOrder.AfterRelease(m.name, goroutineID)
	}

	return false, nil
}

// Unlock releases the mutex
func (m *OrderedMutex) Unlock() {
	// Notify the deadlock detector
	if m.detector != nil {
		m.detector.LockReleased(m.name)
	}

	// Release the lock
	m.mu.Unlock()

	// Update the lock order tracker
	goroutineID := getGoroutineID()
	if m.lockOrder != nil {
		m.lockOrder.AfterRelease(m.name, goroutineID)
	}
}

// OrderedRWMutex is an RWMutex that enforces lock ordering
type OrderedRWMutex struct {
	mu        sync.RWMutex
	name      string
	lockOrder *LockOrder
	detector  *DeadlockDetector
	logger    *hybridConsensusLogger
}

// NewOrderedRWMutex creates a new RWMutex that enforces lock ordering
func NewOrderedRWMutex(name string, lockOrder *LockOrder, detector *DeadlockDetector, logger *hybridConsensusLogger) *OrderedRWMutex {
	return &OrderedRWMutex{
		name:      name,
		lockOrder: lockOrder,
		detector:  detector,
		logger:    logger,
	}
}

// Lock acquires the write lock, checking for lock order violations
func (m *OrderedRWMutex) Lock() error {
	// Check for lock order violations
	goroutineID := getGoroutineID()
	if m.lockOrder != nil {
		if err := m.lockOrder.BeforeAcquire(m.name+".write", goroutineID); err != nil {
			// Log the error and return it instead of panicking
			if m.logger != nil {
				m.logger.Error("Failed to acquire write lock due to ordering violation",
					LogKeyValue{Key: "lockName", Value: m.name},
					LogKeyValue{Key: "error", Value: err.Error()})
			}
			return fmt.Errorf("failed to acquire write lock %s: %w", m.name, err)
		}
	}

	// Acquire the lock
	m.mu.Lock()

	// Notify the deadlock detector
	if m.detector != nil {
		m.detector.LockAcquired(m.name + ".write")
	}

	return nil
}

// TryLock attempts to acquire the write lock without blocking
func (m *OrderedRWMutex) TryLock() (bool, error) {
	// Check for lock order violations
	goroutineID := getGoroutineID()
	if m.lockOrder != nil {
		if err := m.lockOrder.BeforeAcquire(m.name+".write", goroutineID); err != nil {
			// Log the error and return it
			if m.logger != nil {
				m.logger.Error("Failed to acquire write lock due to ordering violation",
					LogKeyValue{Key: "lockName", Value: m.name},
					LogKeyValue{Key: "error", Value: err.Error()})
			}
			return false, fmt.Errorf("failed to acquire write lock %s: %w", m.name, err)
		}
	}

	// Try to acquire the lock
	if m.mu.TryLock() {
		// Notify the deadlock detector
		if m.detector != nil {
			m.detector.LockAcquired(m.name + ".write")
		}
		return true, nil
	}

	// Failed to acquire, cleanup lock order tracking
	if m.lockOrder != nil {
		m.lockOrder.AfterRelease(m.name+".write", goroutineID)
	}

	return false, nil
}

// Unlock releases the write lock
func (m *OrderedRWMutex) Unlock() {
	// Notify the deadlock detector
	if m.detector != nil {
		m.detector.LockReleased(m.name + ".write")
	}

	// Release the lock
	m.mu.Unlock()

	// Update the lock order tracker
	goroutineID := getGoroutineID()
	if m.lockOrder != nil {
		m.lockOrder.AfterRelease(m.name+".write", goroutineID)
	}
}

// RLock acquires the read lock, checking for lock order violations
func (m *OrderedRWMutex) RLock() error {
	// Check for lock order violations
	goroutineID := getGoroutineID()
	if m.lockOrder != nil {
		if err := m.lockOrder.BeforeAcquire(m.name+".read", goroutineID); err != nil {
			// Log the error and return it instead of panicking
			if m.logger != nil {
				m.logger.Error("Failed to acquire read lock due to ordering violation",
					LogKeyValue{Key: "lockName", Value: m.name},
					LogKeyValue{Key: "error", Value: err.Error()})
			}
			return fmt.Errorf("failed to acquire read lock %s: %w", m.name, err)
		}
	}

	// Acquire the lock
	m.mu.RLock()

	// Notify the deadlock detector
	if m.detector != nil {
		m.detector.LockAcquired(m.name + ".read")
	}

	return nil
}

// TryRLock attempts to acquire the read lock without blocking
func (m *OrderedRWMutex) TryRLock() (bool, error) {
	// Check for lock order violations
	goroutineID := getGoroutineID()
	if m.lockOrder != nil {
		if err := m.lockOrder.BeforeAcquire(m.name+".read", goroutineID); err != nil {
			// Log the error and return it
			if m.logger != nil {
				m.logger.Error("Failed to acquire read lock due to ordering violation",
					LogKeyValue{Key: "lockName", Value: m.name},
					LogKeyValue{Key: "error", Value: err.Error()})
			}
			return false, fmt.Errorf("failed to acquire read lock %s: %w", m.name, err)
		}
	}

	// Try to acquire the lock
	if m.mu.TryRLock() {
		// Notify the deadlock detector
		if m.detector != nil {
			m.detector.LockAcquired(m.name + ".read")
		}
		return true, nil
	}

	// Failed to acquire, cleanup lock order tracking
	if m.lockOrder != nil {
		m.lockOrder.AfterRelease(m.name+".read", goroutineID)
	}

	return false, nil
}

// RUnlock releases the read lock
func (m *OrderedRWMutex) RUnlock() {
	// Notify the deadlock detector
	if m.detector != nil {
		m.detector.LockReleased(m.name + ".read")
	}

	// Release the lock
	m.mu.RUnlock()

	// Update the lock order tracker
	goroutineID := getGoroutineID()
	if m.lockOrder != nil {
		m.lockOrder.AfterRelease(m.name+".read", goroutineID)
	}
}

// DefaultLockOrdering returns the default lock ordering for the consensus module
func DefaultLockOrdering() map[string]int {
	return map[string]int{
		// HybridConsensus locks
		"stateMu":           1,
		"blockHeightMu":     2,
		"lastBlockHashMu":   3,
		"finalizedEventsMu": 4,
		"pendingEventsMu":   5,
		"checkpointsMu":     6,
		"errorCountMu":      7,
		"eventProcessingMu": 8,

		// ValidatorManager locks
		"validatorsMu":  10,
		"stakeMu":       11,
		"performanceMu": 12,
		"rewardMu":      13,

		// EventFlowManager locks
		"eventFlowMu": 20,
		"metricsMu":   21,

		// SlashingManager locks
		"slashingMu": 30,

		// Read locks (should have higher numbers than their write counterparts)
		"stateMu.read":           101,
		"blockHeightMu.read":     102,
		"lastBlockHashMu.read":   103,
		"finalizedEventsMu.read": 104,
		"pendingEventsMu.read":   105,
		"checkpointsMu.read":     106,
		"errorCountMu.read":      107,
		"validatorsMu.read":      110,
		"stakeMu.read":           111,
		"performanceMu.read":     112,
		"rewardMu.read":          113,
		"eventFlowMu.read":       120,
		"metricsMu.read":         121,
		"slashingMu.read":        130,
	}
}
