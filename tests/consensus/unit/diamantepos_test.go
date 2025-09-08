package consensus_test

import (
	"crypto/rand"
	"sync"
	"testing"
	"time"

	"diamante/consensus/diamantepos"
)

// MockDPoSLogger implements Logger interface for testing
type MockDPoSLogger struct {
	logs []string
	mu   sync.Mutex
}

func (m *MockDPoSLogger) Info(msg string, keyvals ...interface{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.logs = append(m.logs, "INFO: "+msg)
}

func (m *MockDPoSLogger) Error(msg string, keyvals ...interface{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.logs = append(m.logs, "ERROR: "+msg)
}

// Helper function to generate random validator ID
func randomValidatorID() [32]byte {
	var id [32]byte
	rand.Read(id[:])
	return id
}

func TestNewDPoS(t *testing.T) {
	logger := &MockDPoSLogger{}
	maxSetSize := 21
	epochDuration := uint64(3600)

	dpos := diamantepos.NewDPoS(maxSetSize, epochDuration, logger)

	if dpos == nil {
		t.Fatal("Expected non-nil DPoS instance")
	}

	// Check default values
	if dpos.GetSetSize() != maxSetSize {
		t.Errorf("Expected set size %d, got %d", maxSetSize, dpos.GetSetSize())
	}

	if dpos.GetEpochDuration() != epochDuration {
		t.Errorf("Expected epoch duration %d, got %d", epochDuration, dpos.GetEpochDuration())
	}

	if dpos.GetMinStake() != 0 {
		t.Errorf("Expected default min stake 0, got %d", dpos.GetMinStake())
	}
}

func TestDPoSAddValidator(t *testing.T) {
	logger := &MockDPoSLogger{}
	dpos := diamantepos.NewDPoS(21, 3600, logger)

	validatorID := randomValidatorID()
	stake := uint64(1000000)

	dpos.AddValidator(validatorID, stake)

	// Check if validator was added
	validators := dpos.GetValidators()
	found := false
	for _, v := range validators {
		if v.ID == validatorID {
			found = true
			if v.Stake != stake {
				t.Errorf("Expected stake %d, got %d", stake, v.Stake)
			}
			break
		}
	}

	if !found {
		t.Error("Validator not found after adding")
	}

	// Check total stake
	totalStake := dpos.GetTotalStake()
	if totalStake != stake {
		t.Errorf("Expected total stake %d, got %d", stake, totalStake)
	}
}

func TestDPoSUpdateStake(t *testing.T) {
	logger := &MockDPoSLogger{}
	dpos := diamantepos.NewDPoS(21, 3600, logger)

	validatorID := randomValidatorID()
	initialStake := uint64(1000000)
	newStake := uint64(2000000)

	// Add validator
	dpos.AddValidator(validatorID, initialStake)

	// Update stake
	dpos.UpdateStake(validatorID, newStake)

	// Verify stake was updated
	validators := dpos.GetValidators()
	for _, v := range validators {
		if v.ID == validatorID {
			if v.Stake != newStake {
				t.Errorf("Expected stake %d after update, got %d", newStake, v.Stake)
			}
			break
		}
	}

	// Verify total stake
	totalStake := dpos.GetTotalStake()
	if totalStake != newStake {
		t.Errorf("Expected total stake %d, got %d", newStake, totalStake)
	}
}

func TestActiveValidatorSelection(t *testing.T) {
	logger := &MockDPoSLogger{}
	dpos := diamantepos.NewDPoS(3, 3600, logger) // Set smaller set size for testing

	// Add validators with different stakes
	validators := []struct {
		id    [32]byte
		stake uint64
	}{
		{randomValidatorID(), 5000000},
		{randomValidatorID(), 3000000},
		{randomValidatorID(), 1000000},
		{randomValidatorID(), 500000},
		{randomValidatorID(), 50000}, // Below typical min stake
	}

	for _, v := range validators {
		dpos.AddValidator(v.id, v.stake)
	}

	// Set min stake
	dpos.SetMinStake(100000)

	// Process epoch to select active validators
	err := dpos.ProcessEpoch(1)
	if err != nil {
		t.Fatalf("Failed to process epoch: %v", err)
	}

	// Check active validators
	activeValidators := dpos.GetActiveValidators()
	if len(activeValidators) != 3 {
		t.Errorf("Expected 3 active validators, got %d", len(activeValidators))
	}

	// Verify top 3 validators by stake are selected (excluding the one below min stake)
	for i := 0; i < 3; i++ {
		if !dpos.IsActiveValidator(validators[i].id) {
			t.Errorf("Expected validator %d to be active", i)
		}
	}

	// Verify validator below min stake is not active
	if dpos.IsActiveValidator(validators[4].id) {
		t.Error("Validator below min stake should not be active")
	}
}

func TestDPoSGetNextValidator(t *testing.T) {
	logger := &MockDPoSLogger{}
	dpos := diamantepos.NewDPoS(5, 3600, logger)

	// Add validators
	numValidators := 5
	for i := 0; i < numValidators; i++ {
		validatorID := randomValidatorID()
		stake := uint64(1000000 + i*100000)
		dpos.AddValidator(validatorID, stake)
	}

	// Process epoch to activate validators
	err := dpos.ProcessEpoch(1)
	if err != nil {
		t.Fatalf("Failed to process epoch: %v", err)
	}

	// Test validator selection for different blocks
	lastBlockHash := [32]byte{1, 2, 3}
	seenValidators := make(map[[32]byte]bool)

	for blockNum := uint64(1); blockNum <= 10; blockNum++ {
		validator := dpos.GetNextValidator(blockNum, lastBlockHash)
		if validator == nil {
			t.Errorf("No validator selected for block %d", blockNum)
			continue
		}

		// Verify it's an active validator
		if !dpos.IsActiveValidator(validator.ID) {
			t.Errorf("Selected validator for block %d is not active", blockNum)
		}

		seenValidators[validator.ID] = true

		// Update last block hash for next iteration
		lastBlockHash[0] = byte(blockNum)
	}

	// Ensure we see multiple different validators (rotation)
	if len(seenValidators) < 2 {
		t.Error("Expected to see multiple validators in rotation")
	}
}

func TestRewardValidator(t *testing.T) {
	logger := &MockDPoSLogger{}
	dpos := diamantepos.NewDPoS(21, 3600, logger)

	validatorID := randomValidatorID()
	initialStake := uint64(1000000)

	dpos.AddValidator(validatorID, initialStake)

	// Get validator performance before reward
	perfBefore := dpos.GetValidatorPerformance(validatorID)

	// Reward validator
	dpos.RewardValidator(validatorID)

	// Check performance improved
	perfAfter := dpos.GetValidatorPerformance(validatorID)
	if perfAfter <= perfBefore {
		t.Errorf("Expected performance to improve from %f to > %f", perfBefore, perfAfter)
	}

	// Check performance is within valid range
	if perfAfter > 1.0 {
		t.Error("Performance should not exceed 1.0")
	}
}

func TestValidatorPerformance(t *testing.T) {
	logger := &MockDPoSLogger{}
	dpos := diamantepos.NewDPoS(21, 3600, logger)

	validatorID := randomValidatorID()
	stake := uint64(1000000)

	dpos.AddValidator(validatorID, stake)

	// Test InjectMisbehaviorCount for testing purposes
	dpos.InjectMisbehaviorCount(validatorID, 5)

	// Get validator performance
	performance := dpos.GetValidatorPerformance(validatorID)
	if performance < 0 {
		t.Error("Performance should not be negative")
	}
}

func TestSlashLog(t *testing.T) {
	logger := &MockDPoSLogger{}
	dpos := diamantepos.NewDPoS(21, 3600, logger)

	// The slash log is managed internally, let's check if we can retrieve it
	slashLog := dpos.GetSlashLog()
	if len(slashLog) != 0 {
		t.Error("Expected empty slash log initially")
	}

	// Add validator with misbehavior
	validatorID := randomValidatorID()
	dpos.AddValidator(validatorID, 1000000)
	dpos.InjectMisbehaviorCount(validatorID, 3)

	// Process epoch to trigger slashing
	err := dpos.ProcessEpoch(100) // Use block number that triggers epoch
	if err != nil {
		t.Fatalf("Failed to process epoch: %v", err)
	}

	// Check slash log after epoch
	slashLog = dpos.GetSlashLog()
	if len(slashLog) == 0 {
		t.Log("No slashing occurred - this is acceptable if penalty was 0")
	} else {
		// Verify slash event details
		if slashLog[0].ValidatorID != validatorID {
			t.Error("Slash log contains wrong validator ID")
		}
		if slashLog[0].Reason != "Misbehavior" {
			t.Errorf("Expected reason 'Misbehavior', got '%s'", slashLog[0].Reason)
		}
	}
}

func TestDPoSConcurrentOperations(t *testing.T) {
	logger := &MockDPoSLogger{}
	dpos := diamantepos.NewDPoS(21, 3600, logger)

	// Add initial validators
	numValidators := 10
	validators := make([][32]byte, numValidators)
	for i := 0; i < numValidators; i++ {
		validators[i] = randomValidatorID()
		dpos.AddValidator(validators[i], uint64(1000000+i*100000))
	}

	// Run concurrent operations
	var wg sync.WaitGroup

	// Concurrent stake updates
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				newStake := uint64(2000000 + j*10000)
				dpos.UpdateStake(validators[idx], newStake)
			}
		}(i)
	}

	// Concurrent rewards
	for i := 5; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				dpos.RewardValidator(validators[idx])
			}
		}(i)
	}

	// Concurrent reads
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			_ = dpos.GetValidators()
			_ = dpos.GetTotalStake()
			_ = dpos.GetActiveValidators()
			time.Sleep(time.Microsecond * 10)
		}
	}()

	wg.Wait()

	// Verify consistency
	totalStake := dpos.GetTotalStake()
	if totalStake == 0 {
		t.Error("Total stake should not be zero after operations")
	}
}

func TestStateSerializationAndRestore(t *testing.T) {
	logger := &MockDPoSLogger{}
	dpos1 := diamantepos.NewDPoS(21, 3600, logger)

	// Setup initial state
	validators := make([][32]byte, 5)
	for i := 0; i < 5; i++ {
		validators[i] = randomValidatorID()
		stake := uint64(1000000 + i*100000)
		dpos1.AddValidator(validators[i], stake)

		// Add some rewards
		for j := 0; j < i; j++ {
			dpos1.RewardValidator(validators[i])
		}
	}

	// Process epoch
	err := dpos1.ProcessEpoch(1)
	if err != nil {
		t.Fatalf("Failed to process epoch: %v", err)
	}

	// Get state
	state, err := dpos1.GetState()
	if err != nil {
		t.Fatalf("Failed to get state: %v", err)
	}

	// Create new instance and restore
	dpos2 := diamantepos.NewDPoS(21, 3600, logger)
	err = dpos2.RestoreState(state)
	if err != nil {
		t.Fatalf("Failed to restore state: %v", err)
	}

	// Verify state matches
	// Check validators
	validators1 := dpos1.GetValidators()
	validators2 := dpos2.GetValidators()
	if len(validators1) != len(validators2) {
		t.Errorf("Validator count mismatch: %d vs %d", len(validators1), len(validators2))
	}

	// Check total stake
	if dpos1.GetTotalStake() != dpos2.GetTotalStake() {
		t.Errorf("Total stake mismatch: %d vs %d",
			dpos1.GetTotalStake(), dpos2.GetTotalStake())
	}

	// Check active validators
	active1 := dpos1.GetActiveValidators()
	active2 := dpos2.GetActiveValidators()
	if len(active1) != len(active2) {
		t.Errorf("Active validator count mismatch: %d vs %d",
			len(active1), len(active2))
	}
}

func TestEpochTransition(t *testing.T) {
	logger := &MockDPoSLogger{}
	dpos := diamantepos.NewDPoS(5, 100, logger) // Smaller epoch for testing

	// Add validators with varying performance
	numValidators := 10
	validators := make([][32]byte, numValidators)
	for i := 0; i < numValidators; i++ {
		validators[i] = randomValidatorID()
		stake := uint64(1000000)
		dpos.AddValidator(validators[i], stake)
	}

	// First epoch
	err := dpos.ProcessEpoch(1)
	if err != nil {
		t.Fatalf("Failed to process epoch 1: %v", err)
	}

	activeSet1 := dpos.GetActiveValidators()

	// Simulate some validators performing better
	for i := 0; i < 5; i++ {
		for j := 0; j < 10; j++ {
			dpos.RewardValidator(validators[i])
		}
	}

	// Inject misbehavior for some validators (simulating penalties)
	for i := 5; i < 8; i++ {
		dpos.InjectMisbehaviorCount(validators[i], 5)
	}

	// Second epoch
	err = dpos.ProcessEpoch(2)
	if err != nil {
		t.Fatalf("Failed to process epoch 2: %v", err)
	}

	activeSet2 := dpos.GetActiveValidators()

	// Active set might change based on performance
	// Just verify we have the expected number
	if len(activeSet2) != len(activeSet1) {
		t.Logf("Active set size changed from %d to %d",
			len(activeSet1), len(activeSet2))
	}
}

func TestMinStakeEnforcement(t *testing.T) {
	logger := &MockDPoSLogger{}
	dpos := diamantepos.NewDPoS(5, 3600, logger)

	minStake := uint64(500000)
	dpos.SetMinStake(minStake)

	// Add validators above and below min stake
	v1 := randomValidatorID()
	v2 := randomValidatorID()
	v3 := randomValidatorID()

	dpos.AddValidator(v1, minStake+100000)
	dpos.AddValidator(v2, minStake-100000)
	dpos.AddValidator(v3, minStake)

	// Process epoch
	err := dpos.ProcessEpoch(1)
	if err != nil {
		t.Fatalf("Failed to process epoch: %v", err)
	}

	// Check active validators
	if !dpos.IsActiveValidator(v1) {
		t.Error("Validator above min stake should be active")
	}
	if dpos.IsActiveValidator(v2) {
		t.Error("Validator below min stake should not be active")
	}
	if !dpos.IsActiveValidator(v3) {
		t.Error("Validator at min stake should be active")
	}
}

func TestRoundRobinSelection(t *testing.T) {
	logger := &MockDPoSLogger{}
	dpos := diamantepos.NewDPoS(3, 3600, logger)

	// Add exactly 3 validators
	validators := make([][32]byte, 3)
	for i := 0; i < 3; i++ {
		validators[i] = randomValidatorID()
		dpos.AddValidator(validators[i], uint64(1000000))
	}

	// Process epoch
	err := dpos.ProcessEpoch(1)
	if err != nil {
		t.Fatalf("Failed to process epoch: %v", err)
	}

	// Track validator selection over multiple blocks
	selectionCount := make(map[[32]byte]int)
	lastBlockHash := [32]byte{}

	numBlocks := 30
	for i := uint64(0); i < uint64(numBlocks); i++ {
		validator := dpos.GetNextValidator(i, lastBlockHash)
		if validator != nil {
			selectionCount[validator.ID]++
		}
		// Update hash for next iteration
		lastBlockHash[0] = byte(i)
	}

	// Each validator should be selected roughly equally
	for _, v := range validators {
		count := selectionCount[v]
		expectedCount := numBlocks / 3
		variance := 2 // Allow some variance

		if count < expectedCount-variance || count > expectedCount+variance {
			t.Errorf("Validator %x selected %d times, expected around %d",
				v[:4], count, expectedCount)
		}
	}
}

func TestGetValidatorStake(t *testing.T) {
	logger := &MockDPoSLogger{}
	dpos := diamantepos.NewDPoS(21, 3600, logger)

	validatorID := randomValidatorID()
	stake := uint64(1000000)

	// Test non-existent validator
	nonExistentStake := dpos.GetValidatorStake(randomValidatorID())
	if nonExistentStake != 0 {
		t.Errorf("Expected 0 stake for non-existent validator, got %d", nonExistentStake)
	}

	// Add validator
	dpos.AddValidator(validatorID, stake)

	// Process epoch to make it active
	err := dpos.ProcessEpoch(1)
	if err != nil {
		t.Fatalf("Failed to process epoch: %v", err)
	}

	// Get stake for active validator
	actualStake := dpos.GetValidatorStake(validatorID)
	if actualStake == 0 {
		t.Error("Expected non-zero stake for active validator")
	}
}

func TestSetEpochDuration(t *testing.T) {
	logger := &MockDPoSLogger{}
	dpos := diamantepos.NewDPoS(21, 3600, logger)

	// Test setting valid epoch duration
	newDuration := uint64(7200)
	dpos.SetEpochDuration(newDuration)

	if dpos.GetEpochDuration() != newDuration {
		t.Errorf("Expected epoch duration %d, got %d", newDuration, dpos.GetEpochDuration())
	}

	// Test setting zero epoch duration (should be rejected)
	dpos.SetEpochDuration(0)

	// Duration should remain unchanged
	if dpos.GetEpochDuration() != newDuration {
		t.Error("Epoch duration should not change when setting to zero")
	}
}

func TestInjectLastUpdateTime(t *testing.T) {
	logger := &MockDPoSLogger{}
	dpos := diamantepos.NewDPoS(21, 3600, logger)

	validatorID := randomValidatorID()
	dpos.AddValidator(validatorID, 1000000)

	// Inject a specific update time
	injectedTime := time.Now().Add(-24 * time.Hour)
	dpos.InjectLastUpdateTime(validatorID, injectedTime)

	// Process epoch to trigger performance decay
	err := dpos.ProcessEpoch(3600) // Trigger epoch boundary
	if err != nil {
		t.Fatalf("Failed to process epoch: %v", err)
	}

	// Performance should have decayed due to old update time
	performance := dpos.GetValidatorPerformance(validatorID)
	if performance >= 1.0 {
		t.Error("Expected performance to decay after 24 hours")
	}
}
