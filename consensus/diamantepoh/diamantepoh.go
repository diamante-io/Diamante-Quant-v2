// consensus/diamantepoh/diamantepoh.go

package diamantepoh

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"time"
)

const (
	MaxIterations    = 1000000
	MaxBatchSize     = 500
	SyncThreshold    = 1000
	VerificationTime = 200 * time.Millisecond
)

// PoH provides a Proof-of-History mechanism.
type PoH struct {
	state     [32]byte
	count     uint64
	lastTick  time.Time
	tickDelay time.Duration
	logger    Logger

	stateMu     sync.RWMutex
	lastTickMu  sync.RWMutex
	tickDelayMu sync.RWMutex

	// Context for cancellation.
	ctx    context.Context
	cancel context.CancelFunc
}

// Logger provides a minimal interface for logging informational and error messages.
type Logger interface {
	Info(msg string, keyvals ...interface{})
	Error(msg string, keyvals ...interface{})
}

// NewPoH creates a new PoH instance. If initialState is zero, a default seed is used.
// It also initializes a cancellable context.
func NewPoH(initialState [32]byte, tickDelay time.Duration, logger Logger) *PoH {
	zero := [32]byte{}
	if initialState == zero {
		initialState = sha256.Sum256([]byte("Diamante Default Seed"))
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &PoH{
		state:     initialState,
		count:     0,
		lastTick:  time.Now(),
		tickDelay: tickDelay,
		logger:    logger,
		ctx:       ctx,
		cancel:    cancel,
	}
}

// Tick checks whether enough time has passed since the last tick and advances the state.
func (p *PoH) Tick() {
	p.stateMu.Lock()
	defer p.stateMu.Unlock()

	p.lastTickMu.Lock()
	defer p.lastTickMu.Unlock()

	p.tickLocked()
}

// tickLocked assumes p.lastTickMu and p.stateMu are already locked.
func (p *PoH) tickLocked() {
	now := time.Now()
	p.tickDelayMu.RLock()
	tDelay := p.tickDelay
	p.tickDelayMu.RUnlock()

	if now.Sub(p.lastTick) >= tDelay {
		p.advanceState(1)
		p.lastTick = now
		if p.logger != nil {
			p.logger.Info("PoH Tick", "count", p.count, "state", fmt.Sprintf("%x", p.state))
		}
	}
}

// GetTickDelay returns the current tick delay.
func (p *PoH) GetTickDelay() time.Duration {
	p.tickDelayMu.RLock()
	defer p.tickDelayMu.RUnlock()
	return p.tickDelay
}

// SetTickDelay updates the tick delay; returns an error if delay is non-positive.
func (p *PoH) SetTickDelay(delay time.Duration) error {
	if delay <= 0 {
		return errors.New("tick delay must be positive")
	}
	p.tickDelayMu.Lock()
	defer p.tickDelayMu.Unlock()
	p.tickDelay = delay
	return nil
}

// Record advances the PoH state with a new data record and returns the resulting proof.
func (p *PoH) Record(data []byte) [32]byte {
	p.stateMu.Lock()
	defer p.stateMu.Unlock()

	p.lastTickMu.Lock()
	p.tickLocked()
	p.lastTickMu.Unlock()

	currentState := p.state
	proof := sha256.Sum256(append(currentState[:], data...))

	p.state = proof
	p.count++
	return proof
}

// Verify computes the expected proof from prevState and data, comparing it with the provided proof.
// Enhanced with better security checks and validation.
func (p *PoH) Verify(prevState [32]byte, data []byte, proof [32]byte, count uint64) bool {
	p.stateMu.RLock()
	currentCount := p.count
	p.stateMu.RUnlock()

	if p.logger != nil {
		p.logger.Info("Starting PoH verification",
			"prevState", fmt.Sprintf("%x", prevState),
			"count", count,
			"currentCount", currentCount,
			"dataSize", len(data))
	}

	// Additional security checks

	// 1. Check if data exceeds maximum size
	if len(data) > 1024*1024 { // 1MB limit
		if p.logger != nil {
			p.logger.Error("PoH verification failed: data exceeds maximum size",
				"size", len(data),
				"maxSize", 1024*1024)
		}
		return false
	}

	// 2. Check if count is too far in the future (possible time manipulation)
	if count > currentCount+10000 {
		if p.logger != nil {
			p.logger.Error("PoH verification failed: count too far in future",
				"count", count,
				"currentCount", currentCount,
				"difference", count-currentCount)
		}
		return false
	}

	// 3. Check for zero state (never valid in production)
	zeroState := [32]byte{}
	if prevState == zeroState {
		if p.logger != nil {
			p.logger.Error("PoH verification failed: zero previous state")
		}
		return false
	}

	// 4. Check for null data with non-null proof (suspicious)
	if len(data) == 0 && proof != zeroState {
		if p.logger != nil {
			p.logger.Error("PoH verification failed: empty data with non-empty proof")
		}
		return false
	}

	// Now do the actual verification
	expectedProof := sha256.Sum256(append(prevState[:], data...))
	result := (expectedProof == proof)

	if !result {
		// Log detailed information on verification failure
		if p.logger != nil {
			p.logger.Error("PoH verification failed: proof mismatch",
				"expectedProof", fmt.Sprintf("%x", expectedProof),
				"actualProof", fmt.Sprintf("%x", proof),
				"prevState", fmt.Sprintf("%x", prevState),
				"dataPrefix", fmt.Sprintf("%x", data[:min(len(data), 32)]),
				"count", count)
		}
	} else {
		if p.logger != nil {
			p.logger.Info("PoH verification completed successfully",
				"count", count)
		}
	}
	return result
}

// Helper function min returns the smaller of a and b
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// GetState returns the current PoH state.
func (p *PoH) GetState() [32]byte {
	p.stateMu.RLock()
	defer p.stateMu.RUnlock()
	return p.state
}

// GetCount returns the current PoH counter.
func (p *PoH) GetCount() uint64 {
	p.stateMu.RLock()
	defer p.stateMu.RUnlock()
	return p.count
}

// AdvanceState advances the PoH state a given number of iterations.
func (p *PoH) AdvanceState(iterations uint64) {
	p.stateMu.Lock()
	defer p.stateMu.Unlock()

	p.advanceState(iterations)
	if p.logger != nil {
		p.logger.Info("Advanced state", "iterations", iterations, "new_count", p.count, "new_state", fmt.Sprintf("%x", p.state))
	}
}

// advanceState advances the state without locking; caller must hold p.stateMu.
func (p *PoH) advanceState(iterations uint64) {
	for i := uint64(0); i < iterations; i++ {
		p.count++
		countBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(countBytes, p.count)
		p.state = sha256.Sum256(append(p.state[:], countBytes...))
	}
}

// Synchronize forces a full sync to the target state and count.
func (p *PoH) Synchronize(targetState [32]byte, targetCount uint64) error {
	p.stateMu.Lock()
	defer p.stateMu.Unlock()

	p.lastTickMu.Lock()
	defer p.lastTickMu.Unlock()

	if targetCount < p.count {
		return fmt.Errorf("target count (%d) is less than current count (%d)", targetCount, p.count)
	}
	// Full assignment.
	p.state = targetState
	p.count = targetCount
	p.lastTick = time.Now()

	if p.logger != nil {
		p.logger.Info("PoH Full Sync forced", "target_count", targetCount)
	}
	return nil
}

// GenerateProof advances the state for a number of iterations and returns a proof along with the start state and count.
func (p *PoH) GenerateProof(data []byte, iterations uint64) ([32]byte, [32]byte, uint64, error) {
	if iterations > MaxIterations {
		return [32]byte{}, [32]byte{}, 0, fmt.Errorf("iterations exceed maximum allowed (%d)", MaxIterations)
	}

	p.stateMu.Lock()
	defer p.stateMu.Unlock()

	startState := p.state
	startCount := p.count

	for i := uint64(0); i < iterations; i++ {
		p.count++
		countBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(countBytes, p.count)
		p.state = sha256.Sum256(append(p.state[:], countBytes...))
	}
	proof := sha256.Sum256(append(p.state[:], data...))

	if p.logger != nil {
		p.logger.Info("PoH Proof Generated",
			"iterations", iterations,
			"start_count", startCount,
			"end_count", p.count,
			"start_state", fmt.Sprintf("%x", startState),
			"end_state", fmt.Sprintf("%x", p.state))
	}
	return proof, startState, startCount, nil
}

// VerifyProof replays the state advancement from (startState, startCount) for a given number of iterations,
// then verifies the final proof against the provided data.
func (p *PoH) VerifyProof(startState [32]byte, data []byte, proof [32]byte, startCount, iterations uint64) (bool, error) {
	if iterations > MaxIterations {
		return false, fmt.Errorf("iterations exceed maximum allowed (%d)", MaxIterations)
	}

	verifyStart := time.Now()
	state := startState
	count := startCount

	for i := uint64(0); i < iterations; i++ {
		// Check for cancellation in case this loop takes too long.
		select {
		case <-p.ctx.Done():
			return false, errors.New("verification canceled")
		default:
		}

		count++
		countBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(countBytes, count)
		state = sha256.Sum256(append(state[:], countBytes...))

		if time.Since(verifyStart) > VerificationTime {
			return false, fmt.Errorf("verification timeout after %d iterations", i+1)
		}
	}

	expectedProof := sha256.Sum256(append(state[:], data...))
	isValid := (expectedProof == proof)

	if p.logger != nil {
		p.logger.Info("PoH Proof Verified",
			"iterations", iterations,
			"start_count", startCount,
			"end_count", count,
			"start_state", fmt.Sprintf("%x", startState),
			"end_state", fmt.Sprintf("%x", state),
			"is_valid", isValid)
	}
	return isValid, nil
}

// BatchRecord processes a batch of data records and returns their resulting hashes.
// If the batch size exceeds MaxBatchSize, it will be truncated.
func (p *PoH) BatchRecord(dataList [][]byte) [][32]byte {
	p.stateMu.Lock()
	defer p.stateMu.Unlock()

	p.lastTickMu.Lock()
	defer p.lastTickMu.Unlock()

	// Safety check: limit batch size
	if len(dataList) > MaxBatchSize {
		dataList = dataList[:MaxBatchSize]
		if p.logger != nil {
			p.logger.Info("BatchRecord: dataList truncated", "max_batch_size", MaxBatchSize)
		}
	}

	hashes := make([][32]byte, len(dataList))
	for i, data := range dataList {
		p.tickLocked()
		hash := sha256.Sum256(append(p.state[:], data...))
		p.state = hash
		p.count++
		hashes[i] = hash
	}
	return hashes
}

// EstimateTimeToCount returns the estimated duration to reach the target count.
func (p *PoH) EstimateTimeToCount(targetCount uint64) time.Duration {
	p.stateMu.RLock()
	curCount := p.count
	p.stateMu.RUnlock()

	p.tickDelayMu.RLock()
	tDelay := p.tickDelay
	p.tickDelayMu.RUnlock()

	if targetCount <= curCount {
		return 0
	}
	countDiff := targetCount - curCount
	return time.Duration(countDiff) * tDelay
}

// VerifyHashRange verifies a sequence of hashes matches the expected advancement.
func (p *PoH) VerifyHashRange(startState [32]byte, startCount uint64, hashes [][32]byte) bool {
	state := startState
	count := startCount

	for _, h := range hashes {
		count++
		countBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(countBytes, count)
		expectedHash := sha256.Sum256(append(state[:], countBytes...))
		if expectedHash != h {
			return false
		}
		state = h
	}
	return true
}

// GetSnapshot returns the current state and count.
func (p *PoH) GetSnapshot() (state [32]byte, count uint64) {
	p.stateMu.RLock()
	defer p.stateMu.RUnlock()
	return p.state, p.count
}

// EstimateTime returns an estimate for reaching targetCount.
func (p *PoH) EstimateTime(targetCount uint64) (time.Duration, error) {
	p.stateMu.RLock()
	curCount := p.count
	p.stateMu.RUnlock()

	if targetCount < curCount {
		return 0, errors.New("target count is in the past")
	}
	return p.EstimateTimeToCount(targetCount), nil
}

// RestoreSnapshot restores the PoH state and count from a snapshot.
func (p *PoH) RestoreSnapshot(state [32]byte, count uint64) error {
	p.stateMu.Lock()
	defer p.stateMu.Unlock()
	p.lastTickMu.Lock()
	defer p.lastTickMu.Unlock()

	if count > p.count {
		return fmt.Errorf("cannot restore to a future state (current: %d, target: %d)", p.count, count)
	}

	p.state = state
	p.count = count
	p.lastTick = time.Now()
	if p.logger != nil {
		p.logger.Info("Snapshot restored", "state", fmt.Sprintf("%x", state), "count", count)
	}
	return nil
}

// RecoverFromError reverts the state to a known good state.
func (p *PoH) RecoverFromError(lastKnownState [32]byte, lastKnownCount uint64) error {
	p.stateMu.Lock()
	defer p.stateMu.Unlock()
	p.lastTickMu.Lock()
	defer p.lastTickMu.Unlock()

	if lastKnownCount > p.count {
		return fmt.Errorf("invalid recovery state: future count (current: %d, target: %d)", p.count, lastKnownCount)
	}

	if p.logger != nil {
		if lastKnownCount < p.count {
			p.logger.Info("Recovering to a past state", "current_count", p.count, "target_count", lastKnownCount)
		} else {
			p.logger.Info("Recovering state with matching count", "count", p.count)
		}
	}
	p.state = lastKnownState
	p.count = lastKnownCount
	p.lastTick = time.Now()
	if p.logger != nil {
		p.logger.Info("Recovered state", "state", fmt.Sprintf("%x", p.state), "count", p.count)
	}
	return nil
}
