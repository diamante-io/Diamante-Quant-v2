// Package consensus_test provides integration tests for Byzantine fault tolerance
package consensus_test

import (
	"encoding/hex"
	"sync"
	"testing"
	"time"

	"diamante/consensus"
	"diamante/consensus/types"
	"diamante/tests/consensus/testutil"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestByzantineFaultTolerance tests the consensus system's ability to handle Byzantine validators
func TestByzantineFaultTolerance(t *testing.T) {
	// Create test configuration with Byzantine nodes
	config := testutil.DefaultTestConfig()
	config.NumValidators = 7  // Total validators
	config.ByzantineNodes = 2 // Byzantine validators (less than 1/3)
	config.BlockTime = 200 * time.Millisecond
	config.NetworkLatency = 20 * time.Millisecond

	// Create test environment
	env := testutil.NewTestEnvironment(t, config)
	defer env.Cleanup()

	// Create consensus with test configuration
	consensusConfig := consensus.TestHybridConfig()
	consensusConfig.VotingThreshold = 0.66 // Require 2/3 majority
	hc := consensus.NewHybridConsensusWithConfig(consensusConfig)
	require.NotNil(t, hc)

	// Start consensus
	err := hc.Start()
	require.NoError(t, err)
	defer hc.Stop()

	// Add validators to consensus
	for _, validator := range env.Validators {
		var validatorID [32]byte
		copy(validatorID[:], validator.ID)
		hc.AddValidator(validatorID, validator.Stake)
	}

	// Activate all validators
	err = env.ActivateValidators()
	require.NoError(t, err)
	env.Consensus = hc

	// Wait for initialization
	time.Sleep(500 * time.Millisecond)

	// Simulate Byzantine behavior
	byzantineValidators := env.Validators[:config.ByzantineNodes]
	for _, bv := range byzantineValidators {
		testutil.SimulateByzantineBehavior(bv)
	}

	// Test 1: Byzantine validators create conflicting events
	t.Run("ConflictingEvents", func(t *testing.T) {
		testConflictingEvents(t, hc, env, byzantineValidators)
	})

	// Test 2: Byzantine validators vote inconsistently
	t.Run("InconsistentVoting", func(t *testing.T) {
		testInconsistentVoting(t, hc, env, byzantineValidators)
	})

	// Test 3: Byzantine validators attempt double-spending
	t.Run("DoubleSpending", func(t *testing.T) {
		testDoubleSpending(t, hc, env, byzantineValidators)
	})

	// Test 4: Network partition with Byzantine nodes
	t.Run("NetworkPartitionWithByzantine", func(t *testing.T) {
		testNetworkPartitionWithByzantine(t, hc, env, byzantineValidators)
	})

	// Verify consensus maintains safety and liveness
	finalizedEvents, err := hc.GetFinalizedEvents(0, 100)
	assert.NoError(t, err)
	t.Logf("Total finalized events despite Byzantine behavior: %d", len(finalizedEvents))
}

// testConflictingEvents tests handling of conflicting events from Byzantine validators
func testConflictingEvents(t *testing.T, hc *consensus.HybridConsensus, env *testutil.TestEnvironment, byzantineValidators []*testutil.TestValidator) {
	// Honest validator creates a legitimate event
	honestValidator := env.Validators[len(env.Validators)-1]
	var honestID [32]byte
	copy(honestID[:], honestValidator.ID)

	legitimateData := []byte("legitimate transaction data")
	legitimateEvent := hc.CreateEvent(honestID, nil, legitimateData)

	// Byzantine validators create conflicting events
	conflictingEvents := make([]*types.Event, 0)
	for _, bv := range byzantineValidators {
		var byzantineID [32]byte
		copy(byzantineID[:], bv.ID)

		// Create event with same data but different metadata
		conflictingData := []byte("conflicting transaction data")
		event := hc.CreateEvent(byzantineID, nil, conflictingData)
		if event != nil {
			conflictingEvents = append(conflictingEvents, event)
		}
	}

	// Wait for event processing
	time.Sleep(1 * time.Second)

	// Verify that consensus handles conflicts correctly
	// The system should not finalize conflicting events
	finalizedCount := 0
	if legitimateEvent != nil {
		finalized, _ := hc.FinalizeEvent(legitimateEvent)
		if finalized {
			finalizedCount++
		}
	}

	for _, event := range conflictingEvents {
		finalized, _ := hc.FinalizeEvent(event)
		if finalized {
			finalizedCount++
		}
	}

	// With proper Byzantine fault tolerance, conflicting events should not all be finalized
	assert.LessOrEqual(t, finalizedCount, 1, "At most one event should be finalized when conflicts exist")
}

// testInconsistentVoting tests Byzantine validators voting inconsistently
func testInconsistentVoting(t *testing.T, hc *consensus.HybridConsensus, env *testutil.TestEnvironment, byzantineValidators []*testutil.TestValidator) {
	// Create multiple events from honest validators
	honestEvents := make([]*types.Event, 0)
	for i := len(byzantineValidators); i < len(env.Validators); i++ {
		validator := env.Validators[i]
		var validatorID [32]byte
		copy(validatorID[:], validator.ID)

		data := []byte("honest event " + validator.ID)
		event := hc.CreateEvent(validatorID, nil, data)
		if event != nil {
			honestEvents = append(honestEvents, event)
		}
	}

	// Simulate Byzantine validators voting for multiple conflicting events
	// In a real implementation, this would involve the voting mechanism
	// For now, we verify the system continues to function

	// Wait for consensus rounds
	time.Sleep(2 * time.Second)

	// Check that some honest events are finalized despite Byzantine voting
	finalizedHonestEvents := 0
	for _, event := range honestEvents {
		if event.Finalized {
			finalizedHonestEvents++
		}
	}

	t.Logf("Finalized %d honest events despite Byzantine voting", finalizedHonestEvents)
	assert.Greater(t, finalizedHonestEvents, 0, "Some honest events should be finalized")
}

// testDoubleSpending tests Byzantine validators attempting double-spending
func testDoubleSpending(t *testing.T, hc *consensus.HybridConsensus, env *testutil.TestEnvironment, byzantineValidators []*testutil.TestValidator) {
	if len(byzantineValidators) == 0 {
		t.Skip("No Byzantine validators to test")
	}

	// Byzantine validator attempts to create two conflicting transactions
	byzantineValidator := byzantineValidators[0]
	var byzantineID [32]byte
	copy(byzantineID[:], byzantineValidator.ID)

	// Create two conflicting transactions (double-spend attempt)
	tx1Data := []byte("send 100 coins to Alice")
	tx2Data := []byte("send 100 coins to Bob") // Same coins, different recipient

	event1 := hc.CreateEvent(byzantineID, nil, tx1Data)
	event2 := hc.CreateEvent(byzantineID, nil, tx2Data)

	// Try to finalize both events
	var finalized1, finalized2 bool
	if event1 != nil {
		finalized1, _ = hc.FinalizeEvent(event1)
	}
	if event2 != nil {
		finalized2, _ = hc.FinalizeEvent(event2)
	}

	// Verify that double-spending is prevented
	assert.False(t, finalized1 && finalized2, "Both conflicting transactions should not be finalized")
	t.Logf("Double-spend prevention: Event1 finalized=%v, Event2 finalized=%v", finalized1, finalized2)
}

// testNetworkPartitionWithByzantine tests network partition scenarios with Byzantine nodes
func testNetworkPartitionWithByzantine(t *testing.T, hc *consensus.HybridConsensus, env *testutil.TestEnvironment, byzantineValidators []*testutil.TestValidator) {
	// Partition Byzantine validators from the network
	for _, bv := range byzantineValidators {
		env.Network.PartitionNode(bv.ID)
	}

	// Create events from honest validators
	honestEvents := make([]*types.Event, 0)
	for i := len(byzantineValidators); i < len(env.Validators); i++ {
		validator := env.Validators[i]
		var validatorID [32]byte
		copy(validatorID[:], validator.ID)

		data := []byte("event during partition " + validator.ID)
		event := hc.CreateEvent(validatorID, nil, data)
		if event != nil {
			honestEvents = append(honestEvents, event)
		}
	}

	// Wait for consensus to progress
	time.Sleep(2 * time.Second)

	// Heal the partition
	for _, bv := range byzantineValidators {
		env.Network.HealPartition(bv.ID)
	}

	// Byzantine validators try to create conflicting history
	byzantineEvents := make([]*types.Event, 0)
	for _, bv := range byzantineValidators {
		var byzantineID [32]byte
		copy(byzantineID[:], bv.ID)

		data := []byte("byzantine event after partition " + bv.ID)
		event := hc.CreateEvent(byzantineID, nil, data)
		if event != nil {
			byzantineEvents = append(byzantineEvents, event)
		}
	}

	// Wait for reconciliation
	time.Sleep(2 * time.Second)

	// Verify consensus maintains consistency
	finalizedEvents, err := hc.GetFinalizedEvents(0, 1000)
	assert.NoError(t, err)
	t.Logf("Total finalized events after partition healing: %d", len(finalizedEvents))

	// The honest majority should have made progress during partition
	assert.Greater(t, len(finalizedEvents), 0, "Consensus should make progress with honest majority")
}

// TestByzantineValidatorDetection tests the system's ability to detect Byzantine behavior
func TestByzantineValidatorDetection(t *testing.T) {
	config := testutil.DefaultTestConfig()
	config.NumValidators = 4
	config.ByzantineNodes = 1

	env := testutil.NewTestEnvironment(t, config)
	defer env.Cleanup()

	consensusConfig := consensus.TestHybridConfig()
	hc := consensus.NewHybridConsensusWithConfig(consensusConfig)
	require.NotNil(t, hc)

	err := hc.Start()
	require.NoError(t, err)
	defer hc.Stop()

	// Add validators
	for _, validator := range env.Validators {
		var validatorID [32]byte
		copy(validatorID[:], validator.ID)
		hc.AddValidator(validatorID, validator.Stake)
	}

	// Activate all validators
	env.Consensus = hc
	err = env.ActivateValidators()
	require.NoError(t, err)

	// Byzantine validator creates invalid events
	byzantineValidator := env.Validators[0]
	var byzantineID [32]byte
	copy(byzantineID[:], byzantineValidator.ID)

	// Create events with invalid PoH proofs
	invalidEvents := 0
	for i := 0; i < 5; i++ {
		event := &types.Event{
			ID:        testutil.TestEvent([32]byte{}, 0, nil).ID,
			Creator:   byzantineID,
			Data:      []byte("invalid event"),
			Timestamp: time.Now(),
			Height:    uint64(i + 1),
			PoHProof:  [32]byte{}, // Invalid proof
		}

		finalized, err := hc.FinalizeEvent(event)
		if err != nil || !finalized {
			invalidEvents++
		}
	}

	// All invalid events should be rejected
	assert.Equal(t, 5, invalidEvents, "All events with invalid proofs should be rejected")
}

// TestConsensusRecoveryFromByzantineAttack tests recovery after Byzantine attack
func TestConsensusRecoveryFromByzantineAttack(t *testing.T) {
	config := testutil.DefaultTestConfig()
	config.NumValidators = 7
	config.ByzantineNodes = 2

	env := testutil.NewTestEnvironment(t, config)
	defer env.Cleanup()

	consensusConfig := consensus.TestHybridConfig()
	consensusConfig.CheckpointInterval = 5 // Frequent checkpoints for testing
	hc := consensus.NewHybridConsensusWithConfig(consensusConfig)
	require.NotNil(t, hc)

	err := hc.Start()
	require.NoError(t, err)
	defer hc.Stop()

	// Add validators
	for _, validator := range env.Validators {
		var validatorID [32]byte
		copy(validatorID[:], validator.ID)
		hc.AddValidator(validatorID, validator.Stake)
	}

	// Phase 1: Normal operation
	normalEvents := createEventsFromHonestValidators(t, hc, env, 5)
	time.Sleep(1 * time.Second)

	initialHeight := hc.GetLastBlockHeight()
	t.Logf("Initial block height: %d", initialHeight)

	// Phase 2: Byzantine attack - flood with invalid events
	byzantineValidators := env.Validators[:config.ByzantineNodes]
	var wg sync.WaitGroup
	for _, bv := range byzantineValidators {
		wg.Add(1)
		go func(validator *testutil.TestValidator) {
			defer wg.Done()
			var validatorID [32]byte
			copy(validatorID[:], validator.ID)

			// Flood with events
			for i := 0; i < 20; i++ {
				data := []byte("byzantine flood " + hex.EncodeToString(validatorID[:]))
				hc.CreateEvent(validatorID, nil, data)
				time.Sleep(10 * time.Millisecond)
			}
		}(bv)
	}
	wg.Wait()

	// Phase 3: Recovery - honest validators continue
	recoveryEvents := createEventsFromHonestValidators(t, hc, env, 5)
	time.Sleep(2 * time.Second)

	// Verify recovery
	finalHeight := hc.GetLastBlockHeight()
	t.Logf("Final block height: %d", finalHeight)

	// System should have made progress despite attack
	assert.Greater(t, finalHeight, initialHeight, "Consensus should progress despite Byzantine attack")

	// Check finalized events
	finalizedNormal := countFinalizedEvents(normalEvents)
	finalizedRecovery := countFinalizedEvents(recoveryEvents)

	t.Logf("Finalized normal events: %d/%d", finalizedNormal, len(normalEvents))
	t.Logf("Finalized recovery events: %d/%d", finalizedRecovery, len(recoveryEvents))

	// Most honest events should be finalized
	assert.Greater(t, finalizedNormal+finalizedRecovery, len(normalEvents)/2,
		"Majority of honest events should be finalized")
}

// TestByzantineMessageDropping tests Byzantine validators dropping messages
func TestByzantineMessageDropping(t *testing.T) {
	config := testutil.DefaultTestConfig()
	config.NumValidators = 5
	config.ByzantineNodes = 1

	env := testutil.NewTestEnvironment(t, config)
	defer env.Cleanup()

	// Configure network to simulate message dropping by Byzantine nodes
	env.Network.SetDropRate(0.3) // 30% message drop rate for Byzantine nodes

	consensusConfig := consensus.TestHybridConfig()
	hc := consensus.NewHybridConsensusWithConfig(consensusConfig)
	require.NotNil(t, hc)

	err := hc.Start()
	require.NoError(t, err)
	defer hc.Stop()

	// Add validators
	for _, validator := range env.Validators {
		var validatorID [32]byte
		copy(validatorID[:], validator.ID)
		hc.AddValidator(validatorID, validator.Stake)
	}

	// Create events and measure finalization rate
	startTime := time.Now()
	events := createEventsFromHonestValidators(t, hc, env, 10)

	// Wait for consensus
	time.Sleep(3 * time.Second)

	// Measure performance despite message dropping
	finalizedCount := countFinalizedEvents(events)
	duration := time.Since(startTime)

	t.Logf("Finalized %d/%d events in %v with 30%% message drop rate",
		finalizedCount, len(events), duration)

	// System should still function with degraded performance
	assert.Greater(t, finalizedCount, 0, "Some events should be finalized despite message dropping")
}

// Helper functions

func createEventsFromHonestValidators(t *testing.T, hc *consensus.HybridConsensus, env *testutil.TestEnvironment, count int) []*types.Event {
	events := make([]*types.Event, 0)
	honestValidators := env.Validators[env.Config.ByzantineNodes:]

	for i := 0; i < count; i++ {
		validator := honestValidators[i%len(honestValidators)]
		var validatorID [32]byte
		copy(validatorID[:], validator.ID)

		data := []byte("honest event " + validator.ID)
		event := hc.CreateEvent(validatorID, nil, data)
		if event != nil {
			events = append(events, event)
		}
		time.Sleep(50 * time.Millisecond)
	}

	return events
}

func countFinalizedEvents(events []*types.Event) int {
	count := 0
	for _, event := range events {
		if event.Finalized {
			count++
		}
	}
	return count
}

// BenchmarkByzantineFaultTolerance benchmarks consensus performance under Byzantine conditions
func BenchmarkByzantineFaultTolerance(b *testing.B) {
	config := testutil.DefaultTestConfig()
	config.NumValidators = 10
	config.ByzantineNodes = 3

	env := testutil.NewTestEnvironment(&testing.T{}, config)
	defer env.Cleanup()

	consensusConfig := consensus.TestHybridConfig()
	hc := consensus.NewHybridConsensusWithConfig(consensusConfig)

	err := hc.Start()
	if err != nil {
		b.Fatal(err)
	}
	defer hc.Stop()

	// Add validators
	for _, validator := range env.Validators {
		var validatorID [32]byte
		copy(validatorID[:], validator.ID)
		hc.AddValidator(validatorID, validator.Stake)
	}

	// Activate all validators
	env.Consensus = hc
	err = env.ActivateValidators()
	require.NoError(b, err)

	// Wait for initialization
	time.Sleep(500 * time.Millisecond)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Create event from random honest validator
		validatorIdx := config.ByzantineNodes + (i % (len(env.Validators) - config.ByzantineNodes))
		validator := env.Validators[validatorIdx]
		var validatorID [32]byte
		copy(validatorID[:], validator.ID)

		data := []byte("benchmark event")
		event := hc.CreateEvent(validatorID, nil, data)
		if event != nil {
			hc.FinalizeEvent(event)
		}
	}
}
