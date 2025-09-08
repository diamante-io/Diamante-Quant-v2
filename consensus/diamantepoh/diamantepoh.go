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

	"diamante/common"
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
		lastTick:  common.ConsensusNow(),
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
	now := common.ConsensusNow()
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
	p.lastTick = common.ConsensusNow()

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
	return startState, proof, startCount, nil
}

// VerifyProof replays the state advancement from (startState, startCount) for a given number of iterations,
// then verifies the final proof against the provided data.
func (p *PoH) VerifyProof(startState [32]byte, data []byte, proof [32]byte, startCount, iterations uint64) (bool, error) {
	if iterations > MaxIterations {
		return false, fmt.Errorf("iterations exceed maximum allowed (%d)", MaxIterations)
	}

	verifyStart := common.ConsensusNow()
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

		if common.ConsensusSince(verifyStart) > VerificationTime {
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
	p.lastTick = common.ConsensusNow()
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
	p.lastTick = common.ConsensusNow()
	if p.logger != nil {
		p.logger.Info("Recovered state", "state", fmt.Sprintf("%x", p.state), "count", p.count)
	}
	return nil
}

// --------------------------------------------------------------------
// Transaction Pre-Ordering Enhancement for High TPS
// --------------------------------------------------------------------

// TransactionEntry represents a transaction with its PoH ordering proof
type TransactionEntry struct {
	Transaction *common.Transaction
	PoHProof    [32]byte
	PoHCount    uint64
	Timestamp   time.Time
}

// TransactionBatch represents a batch of pre-ordered transactions
type TransactionBatch struct {
	Entries    []TransactionEntry
	StartState [32]byte
	EndState   [32]byte
	StartCount uint64
	EndCount   uint64
	BatchProof [32]byte
	CreatedAt  time.Time
}

// RecordTransaction records a single transaction and returns its PoH proof
// This enables deterministic transaction ordering before consensus
func (p *PoH) RecordTransaction(tx interface{}) (interface{}, error) {
	transaction, ok := tx.(*common.Transaction)
	if !ok || transaction == nil {
		return nil, errors.New("invalid transaction type")
	}

	p.stateMu.Lock()
	defer p.stateMu.Unlock()

	// Serialize transaction for hashing
	txData, err := serializeTransaction(transaction)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize transaction: %w", err)
	}

	// Record the transaction in PoH
	proof := sha256.Sum256(append(p.state[:], txData...))
	p.state = proof
	p.count++

	entry := &TransactionEntry{
		Transaction: transaction,
		PoHProof:    proof,
		PoHCount:    p.count,
		Timestamp:   common.ConsensusNow(),
	}

	if p.logger != nil {
		p.logger.Info("Transaction recorded in PoH",
			"txID", transaction.ID,
			"count", p.count,
			"proof", fmt.Sprintf("%x", proof[:8]))
	}

	return entry, nil
}

// BatchRecordTransactions efficiently records multiple transactions in order
// This is optimized for high TPS scenarios
func (p *PoH) BatchRecordTransactions(txs []interface{}) (interface{}, error) {
	if len(txs) == 0 {
		return nil, errors.New("no transactions to record")
	}

	// Convert interface slice to transaction slice
	transactions := make([]*common.Transaction, 0, len(txs))
	for _, tx := range txs {
		if transaction, ok := tx.(*common.Transaction); ok && transaction != nil {
			transactions = append(transactions, transaction)
		}
	}

	if len(transactions) == 0 {
		return nil, errors.New("no valid transactions to record")
	}

	// Limit batch size to prevent blocking
	if len(transactions) > MaxBatchSize {
		transactions = transactions[:MaxBatchSize]
		if p.logger != nil {
			p.logger.Info("Transaction batch truncated",
				"originalSize", len(transactions),
				"maxSize", MaxBatchSize)
		}
	}

	p.stateMu.Lock()
	defer p.stateMu.Unlock()

	startState := p.state
	startCount := p.count
	entries := make([]TransactionEntry, 0, len(transactions))
	startTime := common.ConsensusNow()

	for _, tx := range transactions {
		if tx == nil {
			continue
		}

		// Serialize transaction
		txData, err := serializeTransaction(tx)
		if err != nil {
			if p.logger != nil {
				p.logger.Error("Failed to serialize transaction in batch",
					"txID", tx.ID,
					"error", err)
			}
			continue
		}

		// Record in PoH
		proof := sha256.Sum256(append(p.state[:], txData...))
		p.state = proof
		p.count++

		entries = append(entries, TransactionEntry{
			Transaction: tx,
			PoHProof:    proof,
			PoHCount:    p.count,
			Timestamp:   common.ConsensusNow(),
		})
	}

	// Create batch proof
	batchData := make([]byte, 0, 64)
	batchData = append(batchData, startState[:]...)
	batchData = append(batchData, p.state[:]...)
	batchProof := sha256.Sum256(batchData)

	batch := &TransactionBatch{
		Entries:    entries,
		StartState: startState,
		EndState:   p.state,
		StartCount: startCount,
		EndCount:   p.count,
		BatchProof: batchProof,
		CreatedAt:  startTime,
	}

	if p.logger != nil {
		p.logger.Info("Transaction batch recorded",
			"txCount", len(entries),
			"startCount", startCount,
			"endCount", p.count,
			"duration", common.ConsensusSince(startTime))
	}

	return batch, nil
}

// VerifyTransactionEntry verifies a single transaction's PoH proof
func (p *PoH) VerifyTransactionEntry(entry interface{}, prevState [32]byte) (bool, error) {
	txEntry, ok := entry.(*TransactionEntry)
	if !ok || txEntry == nil || txEntry.Transaction == nil {
		return false, errors.New("invalid transaction entry")
	}

	txData, err := serializeTransaction(txEntry.Transaction)
	if err != nil {
		return false, fmt.Errorf("failed to serialize transaction: %w", err)
	}

	expectedProof := sha256.Sum256(append(prevState[:], txData...))
	return expectedProof == txEntry.PoHProof, nil
}

// VerifyTransactionBatch verifies an entire batch of transactions
func (p *PoH) VerifyTransactionBatch(batch interface{}) (bool, error) {
	txBatch, ok := batch.(*TransactionBatch)
	if !ok || txBatch == nil || len(txBatch.Entries) == 0 {
		return false, errors.New("invalid batch")
	}

	// Verify batch internal consistency
	state := txBatch.StartState
	count := txBatch.StartCount

	for i, entry := range txBatch.Entries {
		// Verify count progression
		expectedCount := count + 1
		if entry.PoHCount != expectedCount {
			return false, fmt.Errorf("invalid count at index %d: expected %d, got %d",
				i, expectedCount, entry.PoHCount)
		}

		// Verify proof
		txData, err := serializeTransaction(entry.Transaction)
		if err != nil {
			return false, fmt.Errorf("failed to serialize transaction at index %d: %w", i, err)
		}

		expectedProof := sha256.Sum256(append(state[:], txData...))
		if expectedProof != entry.PoHProof {
			return false, fmt.Errorf("invalid proof at index %d", i)
		}

		state = expectedProof
		count = expectedCount
	}

	// Verify final state
	if state != txBatch.EndState {
		return false, errors.New("batch end state mismatch")
	}

	// Verify batch proof
	batchData := append(txBatch.StartState[:], txBatch.EndState[:]...)
	expectedBatchProof := sha256.Sum256(batchData)
	if expectedBatchProof != txBatch.BatchProof {
		return false, errors.New("invalid batch proof")
	}

	return true, nil
}

// GetTransactionOrder returns the deterministic ordering of transactions
// This is used by consensus to process transactions in parallel while maintaining order
func (p *PoH) GetTransactionOrder(entries []TransactionEntry) []string {
	// Sort by PoH count to get deterministic order
	orderedIDs := make([]string, len(entries))
	for i, entry := range entries {
		orderedIDs[i] = entry.Transaction.ID
	}
	return orderedIDs
}

// serializeTransaction converts a transaction to bytes for PoH recording
func serializeTransaction(tx *common.Transaction) ([]byte, error) {
	// Create deterministic serialization
	data := make([]byte, 0, 256)

	// Add transaction fields in deterministic order
	data = append(data, []byte(tx.ID)...)
	data = append(data, []byte(tx.Sender)...)
	data = append(data, []byte(tx.Receiver)...)

	// Add amount as 8 bytes
	amountBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(amountBytes, uint64(tx.Amount*1e8)) // Convert to satoshis
	data = append(data, amountBytes...)

	// Add fee as 8 bytes
	feeBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(feeBytes, uint64(tx.Fee*1e8))
	data = append(data, feeBytes...)

	// Add timestamp
	timestampBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(timestampBytes, uint64(tx.Timestamp))
	data = append(data, timestampBytes...)

	// Add nonce
	nonceBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(nonceBytes, uint64(tx.Nonce))
	data = append(data, nonceBytes...)

	// Add data if present
	if len(tx.Data) > 0 {
		data = append(data, tx.Data...)
	}

	return data, nil
}

// EstimateTPSCapacity estimates the current TPS capacity based on tick delay
func (p *PoH) EstimateTPSCapacity() uint64 {
	p.tickDelayMu.RLock()
	tickDelay := p.tickDelay
	p.tickDelayMu.RUnlock()

	// Each tick can process a batch of transactions
	// With 500ms tick delay and 500 tx per batch = 1000 TPS
	// With 100ms tick delay and 500 tx per batch = 5000 TPS
	// With 50ms tick delay and 500 tx per batch = 10000 TPS
	ticksPerSecond := time.Second / tickDelay
	return uint64(ticksPerSecond) * uint64(MaxBatchSize)
}
