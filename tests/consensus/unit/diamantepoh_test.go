package consensus_test

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"sync"
	"testing"
	"time"

	"diamante/consensus/diamantepoh"
)

// MockLogger implements the Logger interface for testing
type MockLogger struct {
	logs []string
	mu   sync.Mutex
}

func (m *MockLogger) Info(msg string, keyvals ...interface{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.logs = append(m.logs, "INFO: "+msg)
}

func (m *MockLogger) Error(msg string, keyvals ...interface{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.logs = append(m.logs, "ERROR: "+msg)
}

func TestNewPoH(t *testing.T) {
	tickDelay := 100 * time.Millisecond
	var initialState [32]byte
	logger := &MockLogger{}
	poh := diamantepoh.NewPoH(initialState, tickDelay, logger)

	if poh == nil {
		t.Fatal("Expected non-nil PoH instance")
	}

	// Check initial state
	state := poh.GetState()

	// Initial state should not be zero (default seed is used)
	var zeroState [32]byte
	if state == zeroState {
		t.Error("Expected non-zero initial state")
	}

	if poh.GetCount() != 0 {
		t.Errorf("Expected count 0, got %d", poh.GetCount())
	}
}

func TestPoHWithZeroInitialState(t *testing.T) {
	tickDelay := 50 * time.Millisecond
	var zeroState [32]byte
	logger := &MockLogger{}
	poh := diamantepoh.NewPoH(zeroState, tickDelay, logger)

	// Get initial state
	state := poh.GetState()

	// When zero initial state is provided, a default seed should be used
	if state == zeroState {
		t.Error("Expected non-zero state when zero initial state provided")
	}
}

func TestRecord(t *testing.T) {
	var initialState [32]byte
	copy(initialState[:], []byte("test seed"))
	logger := &MockLogger{}
	poh := diamantepoh.NewPoH(initialState, 50*time.Millisecond, logger)

	// Record some data
	data := []byte("test data")
	hash := poh.Record(data)

	if poh.GetCount() != 1 {
		t.Errorf("Expected count 1 after first record, got %d", poh.GetCount())
	}

	// Record more data
	hash2 := poh.Record([]byte("more data"))

	if poh.GetCount() != 2 {
		t.Errorf("Expected count 2, got %d", poh.GetCount())
	}

	// Hashes should be different
	if bytes.Equal(hash[:], hash2[:]) {
		t.Error("Expected different hashes for different data")
	}
}

func TestVerify(t *testing.T) {
	var initialState [32]byte
	copy(initialState[:], []byte("verify test seed"))
	logger := &MockLogger{}
	poh := diamantepoh.NewPoH(initialState, 50*time.Millisecond, logger)

	// Get initial state
	prevState := poh.GetState()

	// Record some data
	data := []byte("verification test")
	proof := poh.Record(data)
	currentCount := poh.GetCount()

	// Verify the hash
	valid := poh.Verify(prevState, data, proof, currentCount)
	if !valid {
		t.Error("Expected valid verification for correct proof")
	}

	// Verify with wrong count
	valid = poh.Verify(prevState, data, proof, currentCount+1)
	if valid {
		t.Error("Expected invalid verification for wrong count")
	}

	// Verify with wrong data
	valid = poh.Verify(prevState, []byte("wrong data"), proof, currentCount)
	if valid {
		t.Error("Expected invalid verification for wrong data")
	}

	// Verify with wrong previous state
	var wrongPrevState [32]byte
	rand.Read(wrongPrevState[:])
	valid = poh.Verify(wrongPrevState, data, proof, currentCount)
	if valid {
		t.Error("Expected invalid verification for wrong previous state")
	}
}

func TestBatchRecord(t *testing.T) {
	var initialState [32]byte
	logger := &MockLogger{}
	poh := diamantepoh.NewPoH(initialState, 50*time.Millisecond, logger)

	// Create batch of data
	batch := [][]byte{
		[]byte("data1"),
		[]byte("data2"),
		[]byte("data3"),
		[]byte("data4"),
		[]byte("data5"),
	}

	// Record batch
	hashes := poh.BatchRecord(batch)

	if len(hashes) != len(batch) {
		t.Errorf("Expected %d hashes, got %d", len(batch), len(hashes))
	}

	if poh.GetCount() != uint64(len(batch)) {
		t.Errorf("Expected final count %d, got %d", len(batch), poh.GetCount())
	}

	// All hashes should be different
	seen := make(map[[32]byte]bool)
	for i, h := range hashes {
		if seen[h] {
			t.Errorf("Duplicate hash at index %d", i)
		}
		seen[h] = true
	}
}

func TestAdvanceState(t *testing.T) {
	var initialState [32]byte
	logger := &MockLogger{}
	poh := diamantepoh.NewPoH(initialState, 10*time.Millisecond, logger)

	// Get initial state
	initialCount := poh.GetCount()

	// Advance state
	numTicks := uint64(5)
	poh.AdvanceState(numTicks)

	// Check count increased
	if poh.GetCount() != initialCount+numTicks {
		t.Errorf("Expected count %d, got %d", initialCount+numTicks, poh.GetCount())
	}
}

func TestSynchronize(t *testing.T) {
	var initialState [32]byte
	logger := &MockLogger{}
	poh := diamantepoh.NewPoH(initialState, 50*time.Millisecond, logger)

	// Record some entries to create a state
	for i := 0; i < 10; i++ {
		poh.Record([]byte("sync test"))
	}

	// Get current state
	currentCount := poh.GetCount()

	// Synchronize to a future state
	targetCount := currentCount + 20
	var targetHash [32]byte
	rand.Read(targetHash[:])

	err := poh.Synchronize(targetHash, targetCount)
	if err != nil {
		t.Fatalf("Failed to synchronize: %v", err)
	}

	// Verify synchronization
	newState := poh.GetState()
	newCount := poh.GetCount()

	if newCount != targetCount {
		t.Errorf("Expected count %d after sync, got %d", targetCount, newCount)
	}

	if newState != targetHash {
		t.Error("Hash mismatch after synchronization")
	}
}

func TestGenerateAndVerifyProof(t *testing.T) {
	var initialState [32]byte
	logger := &MockLogger{}
	poh := diamantepoh.NewPoH(initialState, 50*time.Millisecond, logger)

	// Get starting state
	startState := poh.GetState()
	startCount := poh.GetCount()

	// Generate proof with data and iterations
	data := []byte("proof test data")
	iterations := uint64(10)

	finalState, proof, finalCount, err := poh.GenerateProof(data, iterations)
	if err != nil {
		t.Fatalf("Failed to generate proof: %v", err)
	}

	// Verify proof structure
	if finalCount != startCount+iterations {
		t.Errorf("Expected final count %d, got %d", startCount+iterations, finalCount)
	}

	// Verify the proof
	valid, err := poh.VerifyProof(startState, data, proof, startCount, iterations)
	if err != nil {
		t.Fatalf("Failed to verify proof: %v", err)
	}
	if !valid {
		t.Error("Expected valid proof verification")
	}

	// Verify final state matches
	if poh.GetState() != finalState {
		t.Error("Final state mismatch after proof generation")
	}

	// Tamper with proof and verify it fails
	var tamperedProof [32]byte
	copy(tamperedProof[:], proof[:])
	tamperedProof[0] ^= 0xFF

	valid, err = poh.VerifyProof(startState, data, tamperedProof, startCount, iterations)
	if err != nil {
		t.Fatalf("Failed to verify tampered proof: %v", err)
	}
	if valid {
		t.Error("Expected invalid proof after tampering")
	}
}

func TestConcurrentRecords(t *testing.T) {
	var initialState [32]byte
	logger := &MockLogger{}
	poh := diamantepoh.NewPoH(initialState, 10*time.Millisecond, logger)

	// Run concurrent records
	var wg sync.WaitGroup
	numGoroutines := 10
	recordsPerGoroutine := 100

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < recordsPerGoroutine; j++ {
				data := []byte(string(rune('A' + id)))
				poh.Record(data)
			}
		}(i)
	}

	wg.Wait()

	// Verify final count
	finalCount := poh.GetCount()
	expectedCount := uint64(numGoroutines * recordsPerGoroutine)
	if finalCount != expectedCount {
		t.Errorf("Expected final count %d, got %d", expectedCount, finalCount)
	}
}

func TestGetSnapshot(t *testing.T) {
	var initialState [32]byte
	logger := &MockLogger{}
	poh := diamantepoh.NewPoH(initialState, 50*time.Millisecond, logger)

	// Build some state
	for i := 0; i < 50; i++ {
		poh.Record([]byte("snapshot test"))
	}

	// Get snapshot
	snapState, snapCount := poh.GetSnapshot()

	// Verify snapshot matches current state
	if snapState != poh.GetState() {
		t.Error("Snapshot state doesn't match current state")
	}

	if snapCount != poh.GetCount() {
		t.Error("Snapshot count doesn't match current count")
	}
}

func TestRestoreSnapshot(t *testing.T) {
	var initialState [32]byte
	logger := &MockLogger{}
	poh1 := diamantepoh.NewPoH(initialState, 50*time.Millisecond, logger)

	// Build some state
	for i := 0; i < 50; i++ {
		poh1.Record([]byte("snapshot test"))
	}

	// Get snapshot from first PoH
	snapState, snapCount := poh1.GetSnapshot()

	// Create new PoH and restore snapshot
	poh2 := diamantepoh.NewPoH(initialState, 50*time.Millisecond, logger)
	err := poh2.RestoreSnapshot(snapState, snapCount)
	if err != nil {
		t.Fatalf("Failed to restore snapshot: %v", err)
	}

	// Verify states match
	if poh2.GetState() != snapState {
		t.Error("Restored state doesn't match snapshot")
	}

	if poh2.GetCount() != snapCount {
		t.Error("Restored count doesn't match snapshot")
	}
}

func TestVerifyHashRange(t *testing.T) {
	var initialState [32]byte
	logger := &MockLogger{}
	poh := diamantepoh.NewPoH(initialState, 50*time.Millisecond, logger)

	// Build a chain of hashes
	var hashes [][32]byte
	startState := poh.GetState()
	startCount := poh.GetCount()

	for i := 0; i < 10; i++ {
		hash := poh.Record([]byte("range test"))
		hashes = append(hashes, hash)
	}

	// Verify the hash range
	valid := poh.VerifyHashRange(startState, startCount, hashes)
	if !valid {
		t.Error("Expected valid hash range")
	}

	// Test with tampered hash
	tamperedHashes := make([][32]byte, len(hashes))
	copy(tamperedHashes, hashes)
	tamperedHashes[5][0] ^= 0xFF

	valid = poh.VerifyHashRange(startState, startCount, tamperedHashes)
	if valid {
		t.Error("Expected invalid for tampered hash range")
	}

	// Test with wrong start state
	var wrongStartState [32]byte
	rand.Read(wrongStartState[:])

	valid = poh.VerifyHashRange(wrongStartState, startCount, hashes)
	if valid {
		t.Error("Expected invalid for wrong start state")
	}
}

func TestTickDelay(t *testing.T) {
	tickDelay := 50 * time.Millisecond
	var initialState [32]byte
	logger := &MockLogger{}
	poh := diamantepoh.NewPoH(initialState, tickDelay, logger)

	// Get and verify tick delay
	delay := poh.GetTickDelay()
	if delay != tickDelay {
		t.Errorf("Expected tick delay %v, got %v", tickDelay, delay)
	}

	// Set new tick delay
	newDelay := 100 * time.Millisecond
	err := poh.SetTickDelay(newDelay)
	if err != nil {
		t.Fatalf("Failed to set tick delay: %v", err)
	}

	delay = poh.GetTickDelay()
	if delay != newDelay {
		t.Errorf("Expected new tick delay %v, got %v", newDelay, delay)
	}

	// Test invalid delay
	err = poh.SetTickDelay(0)
	if err == nil {
		t.Error("Expected error for zero tick delay")
	}
}

func TestEstimateTime(t *testing.T) {
	tickDelay := 100 * time.Millisecond
	var initialState [32]byte
	logger := &MockLogger{}
	poh := diamantepoh.NewPoH(initialState, tickDelay, logger)

	// Record some entries
	poh.Record([]byte("time1"))
	currentCount := poh.GetCount()

	// Estimate time to future count
	targetCount := currentCount + 10
	estimatedTime, err := poh.EstimateTime(targetCount)
	if err != nil {
		t.Fatalf("Failed to estimate time: %v", err)
	}

	// The estimated time should be approximately tickDelay * count difference
	// EstimateTimeToCount likely uses the formula: (targetCount - currentCount) * tickDelay
	expectedTime := tickDelay * time.Duration(targetCount-currentCount)

	// Allow some tolerance
	tolerance := tickDelay / 2
	if estimatedTime < expectedTime-tolerance || estimatedTime > expectedTime+tolerance {
		t.Errorf("Estimated time %v is not reasonable (expected around %v)", estimatedTime, expectedTime)
	}

	// Test error case - target count in the past
	_, err = poh.EstimateTime(currentCount - 1)
	if err == nil {
		t.Error("Expected error when estimating time to past count")
	}
}

func TestRecoverFromError(t *testing.T) {
	var initialState [32]byte
	logger := &MockLogger{}
	poh := diamantepoh.NewPoH(initialState, 50*time.Millisecond, logger)

	// Record normal data
	poh.Record([]byte("normal"))
	count1 := poh.GetCount()

	// Try to synchronize to an invalid state (count going backwards)
	var invalidState [32]byte
	err := poh.Synchronize(invalidState, count1-10)
	if err == nil {
		t.Error("Expected error when synchronizing to past count")
	}

	// PoH should still be usable
	poh.Record([]byte("after error"))
	count2 := poh.GetCount()

	if count2 <= count1 {
		t.Error("Count should have increased despite previous error")
	}

	// Test RecoverFromError
	lastKnownState := poh.GetState()
	lastKnownCount := poh.GetCount()

	err = poh.RecoverFromError(lastKnownState, lastKnownCount)
	if err != nil {
		t.Fatalf("Failed to recover from error: %v", err)
	}
}

func TestDeterminism(t *testing.T) {
	// Use the same initial state for both
	initialState := sha256.Sum256([]byte("determinism seed"))
	delay := 50 * time.Millisecond
	logger := &MockLogger{}

	poh1 := diamantepoh.NewPoH(initialState, delay, logger)
	poh2 := diamantepoh.NewPoH(initialState, delay, logger)

	// Perform same operations on both
	data := []byte("determinism test")

	hash1 := poh1.Record(data)
	hash2 := poh2.Record(data)

	// Results should be identical
	if poh1.GetCount() != poh2.GetCount() {
		t.Errorf("Count mismatch: %d vs %d", poh1.GetCount(), poh2.GetCount())
	}

	if hash1 != hash2 {
		t.Error("Hash mismatch for same operations")
	}

	// Advance both by same amount
	advanceAmount := uint64(10)
	poh1.AdvanceState(advanceAmount)
	poh2.AdvanceState(advanceAmount)

	if poh1.GetCount() != poh2.GetCount() {
		t.Errorf("Final count mismatch: %d vs %d", poh1.GetCount(), poh2.GetCount())
	}

	if poh1.GetState() != poh2.GetState() {
		t.Error("Final state mismatch for same advance")
	}
}
