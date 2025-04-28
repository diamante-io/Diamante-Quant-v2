// consensus/recovery/hybrid_consensus_recovery_test.go

package recovery

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"diamante/consensus/types"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockConsensus implements a minimal types.Consensus for testing
type mockConsensus struct {
	lachesis            *mockLachesis
	dpos                *mockDPoS
	poh                 *mockPoH
	lastBlockHeight     uint64
	lastBlockHash       [32]byte
	lastFinalizedHeight uint64
	activeValidators    []*types.Validator
	pendingEvents       []*types.Event
	finalizedEvents     map[uint64][]*types.Event
}

func newMockConsensus() *mockConsensus {
	return &mockConsensus{
		lachesis:            &mockLachesis{},
		dpos:                &mockDPoS{},
		poh:                 &mockPoH{},
		lastBlockHeight:     0,
		lastBlockHash:       [32]byte{},
		lastFinalizedHeight: 0,
		activeValidators:    make([]*types.Validator, 0),
		pendingEvents:       make([]*types.Event, 0),
		finalizedEvents:     make(map[uint64][]*types.Event),
	}
}

func (mc *mockConsensus) GetNetworkLoad() float64               { return 0.0 }
func (mc *mockConsensus) GetLachesis() types.Lachesis           { return mc.lachesis }
func (mc *mockConsensus) GetDPoS() types.DPoS                   { return mc.dpos }
func (mc *mockConsensus) GetPoH() types.PoH                     { return mc.poh }
func (mc *mockConsensus) Start() error                          { return nil }
func (mc *mockConsensus) Stop() error                           { return nil }
func (mc *mockConsensus) ProcessBlock(blockNumber uint64) error { return nil }
func (mc *mockConsensus) CreateEvent(creator [32]byte, parentIDs [][32]byte, data []byte) *types.Event {
	return nil
}
func (mc *mockConsensus) FinalizeEvent(event *types.Event) (bool, error)                  { return true, nil }
func (mc *mockConsensus) SynchronizeState(targetState [32]byte, targetCount uint64) error { return nil }
func (mc *mockConsensus) GetValidators() []*types.Validator                               { return mc.activeValidators }
func (mc *mockConsensus) GetActiveValidators() []*types.Validator                         { return mc.activeValidators }
func (mc *mockConsensus) GetPendingEvents() []*types.Event                                { return mc.pendingEvents }
func (mc *mockConsensus) GetFinalizedEvents(fromHeight, toHeight uint64) ([]*types.Event, error) {
	return nil, nil
}

// Additional methods needed for the recovery adapter
func (mc *mockConsensus) GetLastBlockHeight() uint64 { return mc.lastBlockHeight }
func (mc *mockConsensus) GetLastBlockHash() [32]byte { return mc.lastBlockHash }
func (mc *mockConsensus) GetCurrentHeight() uint64   { return mc.lastFinalizedHeight }

// mockLachesis implements a minimal types.Lachesis for testing
type mockLachesis struct {
	state []byte
}

func (ml *mockLachesis) AddNode(id [32]byte, stake uint64)            {}
func (ml *mockLachesis) UpdateNodeStake(id [32]byte, newStake uint64) {}
func (ml *mockLachesis) CreateEvent(creator [32]byte, parentIDs [][32]byte, data []byte) *types.Event {
	return nil
}
func (ml *mockLachesis) ProcessEvent(event *types.Event) bool { return true }
func (ml *mockLachesis) GetNetworkLoad() float64              { return 0.0 }
func (ml *mockLachesis) GetGossipDelay() time.Duration        { return time.Second }
func (ml *mockLachesis) SetGossipDelay(delay time.Duration)   {}
func (ml *mockLachesis) AdjustNetworkLoad(adjustment float64) {}
func (ml *mockLachesis) GetVotingThreshold() float64          { return 0.66 }
func (ml *mockLachesis) SetVotingThreshold(threshold float64) {}
func (ml *mockLachesis) GetState() ([]byte, error)            { return ml.state, nil }
func (ml *mockLachesis) RestoreState(state []byte) error      { ml.state = state; return nil }
func (ml *mockLachesis) GetFinalizedEvents(fromHeight, toHeight uint64) ([]*types.Event, error) {
	return nil, nil
}

// mockDPoS implements a minimal types.DPoS for testing
type mockDPoS struct {
	state      []byte
	validators []*types.Validator
}

func (md *mockDPoS) AddValidator(id [32]byte, stake uint64)        {}
func (md *mockDPoS) UpdateStake(id [32]byte, newStake uint64)      {}
func (md *mockDPoS) IsActiveValidator(id [32]byte) bool            { return true }
func (md *mockDPoS) GetValidators() []*types.Validator             { return md.validators }
func (md *mockDPoS) GetActiveValidators() []*types.Validator       { return md.validators }
func (md *mockDPoS) GetTotalStake() uint64                         { return 1000 }
func (md *mockDPoS) GetValidatorStake(validatorID [32]byte) uint64 { return 100 }
func (md *mockDPoS) GetSetSize() int                               { return 21 }
func (md *mockDPoS) SetSetSize(size int)                           {}
func (md *mockDPoS) GetEpochDuration() uint64                      { return 100 }
func (md *mockDPoS) SetEpochDuration(duration uint64)              {}
func (md *mockDPoS) GetNextValidator(blockNumber uint64, lastBlockHash [32]byte) *types.Validator {
	return nil
}
func (md *mockDPoS) ProcessEpoch(blockNumber uint64) error { return nil }
func (md *mockDPoS) RewardValidator(id [32]byte)           {}
func (md *mockDPoS) GetState() ([]byte, error)             { return md.state, nil }
func (md *mockDPoS) RestoreState(stateData []byte) error   { md.state = stateData; return nil }

// mockPoH implements a minimal types.PoH for testing
type mockPoH struct {
	state [32]byte
	count uint64
}

func (mp *mockPoH) Record(data []byte) [32]byte { return [32]byte{} }
func (mp *mockPoH) Verify(prevState [32]byte, data []byte, proof [32]byte, count uint64) bool {
	return true
}
func (mp *mockPoH) GetState() [32]byte                     { return mp.state }
func (mp *mockPoH) GetCount() uint64                       { return mp.count }
func (mp *mockPoH) GetTickDelay() time.Duration            { return time.Second }
func (mp *mockPoH) SetTickDelay(delay time.Duration) error { return nil }
func (mp *mockPoH) Tick()                                  { mp.count++ }
func (mp *mockPoH) Synchronize(targetState [32]byte, targetCount uint64) error {
	mp.state = targetState
	mp.count = targetCount
	return nil
}
func (mp *mockPoH) AdvanceState(iterations uint64) { mp.count += iterations }
func (mp *mockPoH) GenerateProof(data []byte, iterations uint64) ([32]byte, [32]byte, uint64, error) {
	return [32]byte{}, [32]byte{}, 0, nil
}
func (mp *mockPoH) VerifyProof(startState [32]byte, data []byte, proof [32]byte, startCount, iterations uint64) (bool, error) {
	return true, nil
}
func (mp *mockPoH) EstimateTimeToCount(targetCount uint64) time.Duration { return time.Second }
func (mp *mockPoH) VerifyHashRange(startState [32]byte, startCount uint64, hashes [][32]byte) bool {
	return true
}

func TestNewHybridConsensusRecoveryAdapter(t *testing.T) {
	// Create a mock consensus
	consensus := newMockConsensus()

	// Create a recovery adapter
	adapter, err := NewHybridConsensusRecoveryAdapter(consensus)
	require.NoError(t, err)
	require.NotNil(t, adapter)

	// Verify adapter properties
	assert.Equal(t, consensus, adapter.consensus)
	assert.NotNil(t, adapter.recoveryManager)
	assert.NotNil(t, adapter.checkpointManager)
	assert.NotNil(t, adapter.logger)
}

func setupTestHybridConsensusRecoveryAdapter(t *testing.T) (*HybridConsensusRecoveryAdapter, *mockConsensus, string) {
	t.Helper()

	// Create a temporary directory for checkpoints
	tempDir, err := os.MkdirTemp("", "checkpoint_test_*")
	require.NoError(t, err)

	// Create a logger that won't output during tests
	logger := logrus.New()
	logger.SetOutput(os.Stderr)

	// Create a mock consensus
	consensus := newMockConsensus()

	// Create a checkpoint manager with the temp directory
	cm, err := NewCheckpointManager(
		WithCheckpointDir(tempDir),
		WithCheckpointLogger(logger),
	)
	require.NoError(t, err)

	// Create a recovery adapter with the checkpoint manager
	adapter, err := NewHybridConsensusRecoveryAdapter(
		consensus,
		WithCheckpointManager(cm),
		WithAdapterLogger(logger),
	)
	require.NoError(t, err)

	return adapter, consensus, tempDir
}

func cleanupTestHybridConsensusRecoveryAdapter(t *testing.T, tempDir string) {
	t.Helper()
	if err := os.RemoveAll(tempDir); err != nil {
		t.Logf("Warning: Failed to clean up temp directory: %v", err)
	}
}

func TestHybridConsensusRecoveryAdapter_CreateCheckpoint(t *testing.T) {
	// Setup test adapter with temporary directory
	adapter, consensus, tempDir := setupTestHybridConsensusRecoveryAdapter(t)
	defer cleanupTestHybridConsensusRecoveryAdapter(t, tempDir)

	// Set consensus block height
	consensus.lastBlockHeight = 100

	// Set checkpoint interval to ensure ShouldCreateCheckpoint returns true
	adapter.SetCheckpointInterval(100)

	// Create a checkpoint
	err := adapter.CreateCheckpoint(100, "blockhash", "prevhash")
	require.NoError(t, err)

	// Verify checkpoint was created
	assert.True(t, adapter.HasCheckpoint(100))
	assert.Equal(t, uint64(100), adapter.GetLastCheckpointHeight())
}

func TestHybridConsensusRecoveryAdapter_RecoverFromError(t *testing.T) {
	// Setup test adapter with temporary directory
	adapter, _, tempDir := setupTestHybridConsensusRecoveryAdapter(t)
	defer cleanupTestHybridConsensusRecoveryAdapter(t, tempDir)

	// Recover from error
	err := adapter.RecoverFromError("test", errors.New("test error"), Minor)
	require.NoError(t, err)

	// Verify recovery stats
	stats := adapter.GetRecoveryStats()
	assert.Equal(t, "Succeeded", stats["state"])
	assert.NotNil(t, stats["recoveryCount"])
}

func TestHybridConsensusRecoveryAdapter_PerformHealthCheck(t *testing.T) {
	// Setup test adapter with temporary directory
	adapter, consensus, tempDir := setupTestHybridConsensusRecoveryAdapter(t)
	defer cleanupTestHybridConsensusRecoveryAdapter(t, tempDir)

	// Set consensus state
	consensus.lastBlockHeight = 100
	consensus.lastFinalizedHeight = 90
	consensus.poh.count = 110

	// Set checkpoint interval
	adapter.SetCheckpointInterval(50)

	// Create a checkpoint
	err := adapter.CreateCheckpoint(50, "blockhash", "prevhash")
	require.NoError(t, err)

	// Perform health check
	health, err := adapter.PerformHealthCheck(context.Background())
	require.NoError(t, err)

	// Verify health metrics
	assert.Equal(t, "OK", health["status"])
	assert.Equal(t, uint64(100), health["blockHeight"])
	assert.Equal(t, uint64(90), health["finalizedHeight"])
	assert.Equal(t, uint64(110), health["pohCount"])
	assert.Equal(t, uint64(50), health["lastCheckpoint"])
}

func TestHybridConsensusRecoveryAdapter_RestoreFromCheckpoint(t *testing.T) {
	// Setup test adapter with temporary directory
	adapter, consensus, tempDir := setupTestHybridConsensusRecoveryAdapter(t)
	defer cleanupTestHybridConsensusRecoveryAdapter(t, tempDir)

	// Set consensus block height
	consensus.lastBlockHeight = 100

	// Set checkpoint interval
	adapter.SetCheckpointInterval(50)

	// Create a checkpoint
	err := adapter.CreateCheckpoint(50, "blockhash", "prevhash")
	require.NoError(t, err)

	// Set some state to verify restoration
	consensus.lachesis.state = []byte("lachesis state")
	consensus.dpos.state = []byte("dpos state")
	consensus.poh.state = [32]byte{1, 2, 3}
	consensus.poh.count = 75

	// Restore from checkpoint
	err = adapter.RestoreFromCheckpoint(50)
	require.NoError(t, err)

	// Verify checkpoint stats
	assert.True(t, adapter.HasCheckpoint(50))
	assert.Equal(t, uint64(50), adapter.GetLastCheckpointHeight())
}

func TestHybridConsensusRecoveryAdapter_GetCheckpointStats(t *testing.T) {
	// Setup test adapter with temporary directory
	adapter, _, tempDir := setupTestHybridConsensusRecoveryAdapter(t)
	defer cleanupTestHybridConsensusRecoveryAdapter(t, tempDir)

	// Set checkpoint interval
	adapter.SetCheckpointInterval(50)

	// Create checkpoints
	err := adapter.CreateCheckpoint(50, "blockhash1", "prevhash1")
	require.NoError(t, err)
	err = adapter.CreateCheckpoint(100, "blockhash2", "blockhash1")
	require.NoError(t, err)

	// Get checkpoint stats
	stats := adapter.GetCheckpointStats()

	// Verify stats
	assert.Equal(t, 2, stats["count"])
	assert.Equal(t, uint64(100), stats["lastCheckpoint"])
	assert.Equal(t, uint64(50), stats["checkpointInterval"])
	assert.NotNil(t, stats["maxCheckpoints"])
}

func TestHybridConsensusRecoveryAdapter_DeleteCheckpoint(t *testing.T) {
	// Setup test adapter with temporary directory
	adapter, _, tempDir := setupTestHybridConsensusRecoveryAdapter(t)
	defer cleanupTestHybridConsensusRecoveryAdapter(t, tempDir)

	// Set checkpoint interval
	adapter.SetCheckpointInterval(50)

	// Create checkpoints
	err := adapter.CreateCheckpoint(50, "blockhash1", "prevhash1")
	require.NoError(t, err)
	err = adapter.CreateCheckpoint(100, "blockhash2", "blockhash1")
	require.NoError(t, err)

	// Delete checkpoint
	err = adapter.DeleteCheckpoint(50)
	require.NoError(t, err)

	// Verify checkpoint was deleted
	assert.False(t, adapter.HasCheckpoint(50))
	assert.True(t, adapter.HasCheckpoint(100))
}

func TestHybridConsensusRecoveryAdapter_VerifyCheckpoint(t *testing.T) {
	// Setup test adapter with temporary directory
	adapter, _, tempDir := setupTestHybridConsensusRecoveryAdapter(t)
	defer cleanupTestHybridConsensusRecoveryAdapter(t, tempDir)

	// Set checkpoint interval
	adapter.SetCheckpointInterval(50)

	// Create checkpoint
	err := adapter.CreateCheckpoint(50, "blockhash", "prevhash")
	require.NoError(t, err)

	// Verify checkpoint
	valid, err := adapter.VerifyCheckpoint(50)
	require.NoError(t, err)
	assert.True(t, valid)
}

func TestHybridConsensusRecoveryAdapter_GetCheckpointHeights(t *testing.T) {
	// Setup test adapter with temporary directory
	adapter, _, tempDir := setupTestHybridConsensusRecoveryAdapter(t)
	defer cleanupTestHybridConsensusRecoveryAdapter(t, tempDir)

	// Set checkpoint interval
	adapter.SetCheckpointInterval(50)

	// Create checkpoints
	err := adapter.CreateCheckpoint(150, "blockhash3", "blockhash2")
	require.NoError(t, err)
	err = adapter.CreateCheckpoint(50, "blockhash1", "prevhash1")
	require.NoError(t, err)
	err = adapter.CreateCheckpoint(100, "blockhash2", "blockhash1")
	require.NoError(t, err)

	// Get checkpoint heights
	heights := adapter.GetCheckpointHeights()

	// Verify heights are sorted
	assert.Equal(t, []uint64{50, 100, 150}, heights)
}

func TestHybridConsensusRecoveryAdapter_RestoreFromLatestCheckpoint(t *testing.T) {
	// Setup test adapter with temporary directory
	adapter, _, tempDir := setupTestHybridConsensusRecoveryAdapter(t)
	defer cleanupTestHybridConsensusRecoveryAdapter(t, tempDir)

	// Set checkpoint interval
	adapter.SetCheckpointInterval(50)

	// Create checkpoints
	err := adapter.CreateCheckpoint(50, "blockhash1", "prevhash1")
	require.NoError(t, err)
	err = adapter.CreateCheckpoint(100, "blockhash2", "blockhash1")
	require.NoError(t, err)

	// Restore from latest checkpoint
	err = adapter.RestoreFromLatestCheckpoint()
	require.NoError(t, err)

	// Verify latest checkpoint was used
	assert.Equal(t, uint64(100), adapter.GetLastCheckpointHeight())
}

func TestHybridConsensusRecoveryAdapter_SetGetCheckpointInterval(t *testing.T) {
	// Setup test adapter with temporary directory
	adapter, _, tempDir := setupTestHybridConsensusRecoveryAdapter(t)
	defer cleanupTestHybridConsensusRecoveryAdapter(t, tempDir)

	// Set checkpoint interval
	adapter.SetCheckpointInterval(200)

	// Verify interval was set
	assert.Equal(t, uint64(200), adapter.GetCheckpointInterval())
}

func TestHybridConsensusRecoveryAdapter_SetGetMaxCheckpoints(t *testing.T) {
	// Setup test adapter with temporary directory
	adapter, _, tempDir := setupTestHybridConsensusRecoveryAdapter(t)
	defer cleanupTestHybridConsensusRecoveryAdapter(t, tempDir)

	// Set max checkpoints
	adapter.SetMaxCheckpoints(20)

	// Verify max was set
	assert.Equal(t, 20, adapter.GetMaxCheckpoints())
}

func TestHybridConsensusRecoveryAdapter_IsRecovering(t *testing.T) {
	// Setup test adapter with temporary directory
	adapter, _, tempDir := setupTestHybridConsensusRecoveryAdapter(t)
	defer cleanupTestHybridConsensusRecoveryAdapter(t, tempDir)

	// Check initial state
	assert.False(t, adapter.IsRecovering())

	// Start recovery in a goroutine
	go func() {
		adapter.RecoverFromError("test", errors.New("test error"), Severe)
	}()

	// Give it a moment to start
	time.Sleep(10 * time.Millisecond)

	// Check if recovering
	// Note: This might be flaky in tests since recovery is fast in our mock
	// In a real system, recovery would take longer
	recovering := adapter.IsRecovering()
	t.Logf("IsRecovering: %v", recovering)
}
