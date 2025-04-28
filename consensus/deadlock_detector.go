// consensus/deadlock_detector.go

package consensus

import (
	"fmt"
	"runtime"
	"sync"
	"time"
)

// DeadlockDetector provides utilities for detecting potential deadlocks
// by monitoring lock acquisition times.
type DeadlockDetector struct {
	// Configuration
	enabled          bool
	warningThreshold time.Duration
	errorThreshold   time.Duration

	// Lock tracking
	locksMu     sync.Mutex
	activeLocks map[string]*lockInfo
	logger      *hybridConsensusLogger
}

// lockInfo tracks information about an acquired lock
type lockInfo struct {
	lockName      string
	acquiredAt    time.Time
	goroutineID   int64
	stackTrace    string
	warningLogged bool
	errorLogged   bool
}

// NewDeadlockDetector creates a new DeadlockDetector with the given configuration.
func NewDeadlockDetector(logger *hybridConsensusLogger, enabled bool, warningThreshold, errorThreshold time.Duration) *DeadlockDetector {
	dd := &DeadlockDetector{
		enabled:          enabled,
		warningThreshold: warningThreshold,
		errorThreshold:   errorThreshold,
		activeLocks:      make(map[string]*lockInfo),
		logger:           logger,
	}

	// Start the monitoring goroutine if enabled
	if enabled {
		go dd.monitor()
	}

	return dd
}

// LockAcquired should be called when a lock is acquired.
func (dd *DeadlockDetector) LockAcquired(lockName string) {
	if !dd.enabled {
		return
	}

	// Get the current goroutine ID
	goroutineID := getGoroutineID()

	// Get the stack trace
	stackTrace := getStackTrace()

	// Record the lock acquisition
	dd.locksMu.Lock()
	defer dd.locksMu.Unlock()

	dd.activeLocks[lockName] = &lockInfo{
		lockName:      lockName,
		acquiredAt:    time.Now(),
		goroutineID:   goroutineID,
		stackTrace:    stackTrace,
		warningLogged: false,
		errorLogged:   false,
	}
}

// LockReleased should be called when a lock is released.
func (dd *DeadlockDetector) LockReleased(lockName string) {
	if !dd.enabled {
		return
	}

	// Remove the lock from the active locks
	dd.locksMu.Lock()
	defer dd.locksMu.Unlock()

	delete(dd.activeLocks, lockName)
}

// monitor periodically checks for locks that have been held for too long.
func (dd *DeadlockDetector) monitor() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		dd.checkLocks()
	}
}

// checkLocks checks for locks that have been held for too long.
func (dd *DeadlockDetector) checkLocks() {
	dd.locksMu.Lock()
	defer dd.locksMu.Unlock()

	now := time.Now()
	for _, lock := range dd.activeLocks {
		heldDuration := now.Sub(lock.acquiredAt)

		// Check for error threshold
		if heldDuration > dd.errorThreshold && !lock.errorLogged {
			dd.logger.Error("Potential deadlock detected",
				"lockName", lock.lockName,
				"heldFor", heldDuration.String(),
				"goroutineID", lock.goroutineID,
				"stackTrace", lock.stackTrace)
			lock.errorLogged = true
		} else if heldDuration > dd.warningThreshold && !lock.warningLogged {
			// Check for warning threshold
			dd.logger.Warn("Lock held for a long time",
				"lockName", lock.lockName,
				"heldFor", heldDuration.String(),
				"goroutineID", lock.goroutineID)
			lock.warningLogged = true
		}
	}
}

// getGoroutineID returns the ID of the current goroutine.
func getGoroutineID() int64 {
	// This is a hack to get the goroutine ID
	// It's not guaranteed to work in all Go versions
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	idField := string(buf[:n])
	var id int64
	fmt.Sscanf(idField, "goroutine %d ", &id)
	return id
}

// getStackTrace returns a stack trace for the current goroutine.
func getStackTrace() string {
	buf := make([]byte, 4096)
	n := runtime.Stack(buf, false)
	return string(buf[:n])
}

// MutexWithDeadlockDetection wraps a sync.Mutex with deadlock detection.
type MutexWithDeadlockDetection struct {
	mu       sync.Mutex
	name     string
	detector *DeadlockDetector
}

// NewMutexWithDeadlockDetection creates a new mutex with deadlock detection.
func NewMutexWithDeadlockDetection(name string, detector *DeadlockDetector) *MutexWithDeadlockDetection {
	return &MutexWithDeadlockDetection{
		name:     name,
		detector: detector,
	}
}

// Lock acquires the mutex and notifies the deadlock detector.
func (m *MutexWithDeadlockDetection) Lock() {
	m.mu.Lock()
	if m.detector != nil {
		m.detector.LockAcquired(m.name)
	}
}

// Unlock releases the mutex and notifies the deadlock detector.
func (m *MutexWithDeadlockDetection) Unlock() {
	if m.detector != nil {
		m.detector.LockReleased(m.name)
	}
	m.mu.Unlock()
}

// RWMutexWithDeadlockDetection wraps a sync.RWMutex with deadlock detection.
type RWMutexWithDeadlockDetection struct {
	mu       sync.RWMutex
	name     string
	detector *DeadlockDetector
}

// NewRWMutexWithDeadlockDetection creates a new RW mutex with deadlock detection.
func NewRWMutexWithDeadlockDetection(name string, detector *DeadlockDetector) *RWMutexWithDeadlockDetection {
	return &RWMutexWithDeadlockDetection{
		name:     name,
		detector: detector,
	}
}

// Lock acquires the write lock and notifies the deadlock detector.
func (m *RWMutexWithDeadlockDetection) Lock() {
	m.mu.Lock()
	if m.detector != nil {
		m.detector.LockAcquired(m.name + ".write")
	}
}

// Unlock releases the write lock and notifies the deadlock detector.
func (m *RWMutexWithDeadlockDetection) Unlock() {
	if m.detector != nil {
		m.detector.LockReleased(m.name + ".write")
	}
	m.mu.Unlock()
}

// RLock acquires the read lock and notifies the deadlock detector.
func (m *RWMutexWithDeadlockDetection) RLock() {
	m.mu.RLock()
	if m.detector != nil {
		m.detector.LockAcquired(m.name + ".read")
	}
}

// RUnlock releases the read lock and notifies the deadlock detector.
func (m *RWMutexWithDeadlockDetection) RUnlock() {
	if m.detector != nil {
		m.detector.LockReleased(m.name + ".read")
	}
	m.mu.RUnlock()
}
