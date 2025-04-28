// consensus/recovery/checkpoint_manager_test.go

package recovery

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockComponent implements ComponentCheckpointHandler for testing
type mockComponent struct {
	name  string
	state []byte
}

func (m *mockComponent) GetCheckpointState() ([]byte, error) {
	return m.state, nil
}

func (m *mockComponent) RestoreFromCheckpoint(state []byte) error {
	m.state = state
	return nil
}

func (m *mockComponent) GetComponentName() string {
	return m.name
}

func setupTestCheckpointManager(t *testing.T) (*CheckpointManager, string) {
	t.Helper()

	// Create a temporary directory for checkpoints
	tempDir, err := os.MkdirTemp("", "checkpoint_test_*")
	require.NoError(t, err)

	// Create a logger that won't output during tests
	logger := logrus.New()
	logger.SetOutput(os.Stderr)

	// Create checkpoint manager
	cm, err := NewCheckpointManager(
		WithCheckpointDir(tempDir),
		WithCheckpointInterval(10),
		WithMaxCheckpoints(3),
		WithCompressionEnabled(false),
		WithAutoVerify(true),
		WithCheckpointLogger(logger),
	)
	require.NoError(t, err)

	return cm, tempDir
}

func cleanupTestCheckpointManager(t *testing.T, tempDir string) {
	t.Helper()
	if err := os.RemoveAll(tempDir); err != nil {
		t.Logf("Warning: Failed to clean up temp directory: %v", err)
	}
}

func TestCheckpointManager_NewCheckpointManager(t *testing.T) {
	cm, tempDir := setupTestCheckpointManager(t)
	defer cleanupTestCheckpointManager(t, tempDir)

	assert.NotNil(t, cm)
	assert.Equal(t, tempDir, cm.checkpointDir)
	assert.Equal(t, uint64(10), cm.checkpointInterval)
	assert.Equal(t, 3, cm.maxCheckpoints)
	assert.False(t, cm.compressionEnabled)
	assert.True(t, cm.autoVerify)
	assert.NotNil(t, cm.checkpoints)
	assert.NotNil(t, cm.components)
	assert.NotNil(t, cm.logger)
}

func TestCheckpointManager_RegisterComponent(t *testing.T) {
	cm, tempDir := setupTestCheckpointManager(t)
	defer cleanupTestCheckpointManager(t, tempDir)

	// Register a component
	comp := &mockComponent{name: "test", state: []byte("test state")}
	cm.RegisterComponent(comp)

	// Verify component was registered
	assert.Len(t, cm.components, 1)
	assert.Equal(t, comp, cm.components["test"])
}

func TestCheckpointManager_UnregisterComponent(t *testing.T) {
	cm, tempDir := setupTestCheckpointManager(t)
	defer cleanupTestCheckpointManager(t, tempDir)

	// Register a component
	comp := &mockComponent{name: "test", state: []byte("test state")}
	cm.RegisterComponent(comp)

	// Verify component was registered
	assert.Len(t, cm.components, 1)

	// Unregister component
	cm.UnregisterComponent("test")

	// Verify component was unregistered
	assert.Len(t, cm.components, 0)
}

func TestCheckpointManager_ShouldCreateCheckpoint(t *testing.T) {
	cm, tempDir := setupTestCheckpointManager(t)
	defer cleanupTestCheckpointManager(t, tempDir)

	// Should create checkpoint at height 10 (divisible by interval)
	assert.True(t, cm.ShouldCreateCheckpoint(10))

	// Should not create checkpoint at height 15 (not divisible by interval)
	assert.False(t, cm.ShouldCreateCheckpoint(15))

	// Create a checkpoint at height 10
	_, err := cm.CreateCheckpoint(10, "hash10", "prevhash")
	require.NoError(t, err)

	// Should not create another checkpoint at height 10
	assert.False(t, cm.ShouldCreateCheckpoint(10))
}

func TestCheckpointManager_CreateCheckpoint(t *testing.T) {
	cm, tempDir := setupTestCheckpointManager(t)
	defer cleanupTestCheckpointManager(t, tempDir)

	// Create a checkpoint
	cp, err := cm.CreateCheckpoint(10, "hash10", "prevhash")
	require.NoError(t, err)

	// Verify checkpoint properties
	assert.Equal(t, uint64(10), cp.Metadata.Height)
	assert.Equal(t, "hash10", cp.Metadata.BlockHash)
	assert.Equal(t, "prevhash", cp.Metadata.PreviousHash)
	assert.Equal(t, CheckpointPending, cp.Status)
	assert.NotEmpty(t, cp.Path)
	assert.NotNil(t, cp.ComponentStates)

	// Verify checkpoint directory was created
	_, err = os.Stat(cp.Path)
	assert.NoError(t, err)

	// Verify pending checkpoint is set
	assert.Equal(t, cp, cm.pendingCheckpoint)

	// Attempt to create another checkpoint while one is pending
	_, err = cm.CreateCheckpoint(20, "hash20", "hash10")
	assert.Error(t, err)
}

func TestCheckpointManager_FinalizeCheckpoint(t *testing.T) {
	cm, tempDir := setupTestCheckpointManager(t)
	defer cleanupTestCheckpointManager(t, tempDir)

	// Register components
	comp1 := &mockComponent{name: "comp1", state: []byte("state1")}
	comp2 := &mockComponent{name: "comp2", state: []byte("state2")}
	cm.RegisterComponent(comp1)
	cm.RegisterComponent(comp2)

	// Create a checkpoint
	_, err := cm.CreateCheckpoint(10, "hash10", "prevhash")
	require.NoError(t, err)

	// Finalize checkpoint
	err = cm.FinalizeCheckpoint()
	require.NoError(t, err)

	// Verify checkpoint status
	cp, err := cm.GetCheckpoint(10)
	require.NoError(t, err)
	assert.Equal(t, CheckpointComplete, cp.Status)

	// Verify component states were saved
	assert.Contains(t, cp.Metadata.Components, "comp1")
	assert.Contains(t, cp.Metadata.Components, "comp2")

	// Verify metadata file was created
	metadataFile := filepath.Join(cp.Path, "metadata.json")
	_, err = os.Stat(metadataFile)
	assert.NoError(t, err)

	// Verify component state files were created
	comp1File := filepath.Join(cp.Path, "comp1.state")
	comp2File := filepath.Join(cp.Path, "comp2.state")
	_, err = os.Stat(comp1File)
	assert.NoError(t, err)
	_, err = os.Stat(comp2File)
	assert.NoError(t, err)

	// Verify pending checkpoint was cleared
	assert.Nil(t, cm.pendingCheckpoint)

	// Verify last checkpoint was updated
	assert.Equal(t, uint64(10), cm.lastCheckpoint)
}

func TestCheckpointManager_GetCheckpoint(t *testing.T) {
	cm, tempDir := setupTestCheckpointManager(t)
	defer cleanupTestCheckpointManager(t, tempDir)

	// Create and finalize a checkpoint
	_, err := cm.CreateCheckpoint(10, "hash10", "prevhash")
	require.NoError(t, err)
	err = cm.FinalizeCheckpoint()
	require.NoError(t, err)

	// Get checkpoint
	cp, err := cm.GetCheckpoint(10)
	require.NoError(t, err)
	assert.Equal(t, uint64(10), cp.Metadata.Height)

	// Attempt to get non-existent checkpoint
	_, err = cm.GetCheckpoint(20)
	assert.Error(t, err)
}

func TestCheckpointManager_GetLatestCheckpoint(t *testing.T) {
	cm, tempDir := setupTestCheckpointManager(t)
	defer cleanupTestCheckpointManager(t, tempDir)

	// Attempt to get latest checkpoint when none exist
	_, err := cm.GetLatestCheckpoint()
	assert.Error(t, err)

	// Create and finalize checkpoints
	_, err = cm.CreateCheckpoint(10, "hash10", "prevhash")
	require.NoError(t, err)
	err = cm.FinalizeCheckpoint()
	require.NoError(t, err)

	_, err = cm.CreateCheckpoint(20, "hash20", "hash10")
	require.NoError(t, err)
	err = cm.FinalizeCheckpoint()
	require.NoError(t, err)

	// Get latest checkpoint
	cp, err := cm.GetLatestCheckpoint()
	require.NoError(t, err)
	assert.Equal(t, uint64(20), cp.Metadata.Height)
}

func TestCheckpointManager_GetCheckpointHeights(t *testing.T) {
	cm, tempDir := setupTestCheckpointManager(t)
	defer cleanupTestCheckpointManager(t, tempDir)

	// Create and finalize checkpoints
	_, err := cm.CreateCheckpoint(30, "hash30", "prevhash")
	require.NoError(t, err)
	err = cm.FinalizeCheckpoint()
	require.NoError(t, err)

	_, err = cm.CreateCheckpoint(10, "hash10", "prevhash")
	require.NoError(t, err)
	err = cm.FinalizeCheckpoint()
	require.NoError(t, err)

	_, err = cm.CreateCheckpoint(20, "hash20", "hash10")
	require.NoError(t, err)
	err = cm.FinalizeCheckpoint()
	require.NoError(t, err)

	// Get checkpoint heights
	heights := cm.GetCheckpointHeights()
	assert.Equal(t, []uint64{10, 20, 30}, heights)
}

func TestCheckpointManager_RestoreFromCheckpoint(t *testing.T) {
	cm, tempDir := setupTestCheckpointManager(t)
	defer cleanupTestCheckpointManager(t, tempDir)

	// Register components
	comp1 := &mockComponent{name: "comp1", state: []byte("state1")}
	comp2 := &mockComponent{name: "comp2", state: []byte("state2")}
	cm.RegisterComponent(comp1)
	cm.RegisterComponent(comp2)

	// Create and finalize a checkpoint
	_, err := cm.CreateCheckpoint(10, "hash10", "prevhash")
	require.NoError(t, err)
	err = cm.FinalizeCheckpoint()
	require.NoError(t, err)

	// Modify component states
	comp1.state = []byte("modified1")
	comp2.state = []byte("modified2")

	// Restore from checkpoint
	err = cm.RestoreFromCheckpoint(10)
	require.NoError(t, err)

	// Verify component states were restored
	assert.Equal(t, []byte("state1"), comp1.state)
	assert.Equal(t, []byte("state2"), comp2.state)
}

func TestCheckpointManager_VerifyCheckpoint(t *testing.T) {
	cm, tempDir := setupTestCheckpointManager(t)
	defer cleanupTestCheckpointManager(t, tempDir)

	// Create and finalize a checkpoint
	_, err := cm.CreateCheckpoint(10, "hash10", "prevhash")
	require.NoError(t, err)
	err = cm.FinalizeCheckpoint()
	require.NoError(t, err)

	// Verify checkpoint
	valid, err := cm.VerifyCheckpoint(10)
	require.NoError(t, err)
	assert.True(t, valid)

	// Verify checkpoint status was updated
	cp, err := cm.GetCheckpoint(10)
	require.NoError(t, err)
	assert.Equal(t, CheckpointVerified, cp.Status)
}

func TestCheckpointManager_DeleteCheckpoint(t *testing.T) {
	cm, tempDir := setupTestCheckpointManager(t)
	defer cleanupTestCheckpointManager(t, tempDir)

	// Create and finalize a checkpoint
	_, err := cm.CreateCheckpoint(10, "hash10", "prevhash")
	require.NoError(t, err)
	err = cm.FinalizeCheckpoint()
	require.NoError(t, err)

	// Get checkpoint path
	cp, err := cm.GetCheckpoint(10)
	require.NoError(t, err)
	path := cp.Path

	// Delete checkpoint
	err = cm.DeleteCheckpoint(10)
	require.NoError(t, err)

	// Verify checkpoint was removed
	_, err = cm.GetCheckpoint(10)
	assert.Error(t, err)

	// Verify checkpoint directory was deleted
	_, err = os.Stat(path)
	assert.True(t, os.IsNotExist(err))
}

func TestCheckpointManager_cleanupOldCheckpoints(t *testing.T) {
	cm, tempDir := setupTestCheckpointManager(t)
	defer cleanupTestCheckpointManager(t, tempDir)

	// Set max checkpoints to 2
	cm.SetMaxCheckpoints(2)

	// Create and finalize 3 checkpoints
	_, err := cm.CreateCheckpoint(10, "hash10", "prevhash")
	require.NoError(t, err)
	err = cm.FinalizeCheckpoint()
	require.NoError(t, err)

	_, err = cm.CreateCheckpoint(20, "hash20", "hash10")
	require.NoError(t, err)
	err = cm.FinalizeCheckpoint()
	require.NoError(t, err)

	_, err = cm.CreateCheckpoint(30, "hash30", "hash20")
	require.NoError(t, err)
	err = cm.FinalizeCheckpoint()
	require.NoError(t, err)

	// Verify oldest checkpoint was removed
	_, err = cm.GetCheckpoint(10)
	assert.Error(t, err)

	// Verify newer checkpoints still exist
	_, err = cm.GetCheckpoint(20)
	assert.NoError(t, err)
	_, err = cm.GetCheckpoint(30)
	assert.NoError(t, err)
}

func TestCheckpointManager_GetSetCheckpointInterval(t *testing.T) {
	cm, tempDir := setupTestCheckpointManager(t)
	defer cleanupTestCheckpointManager(t, tempDir)

	// Verify initial interval
	assert.Equal(t, uint64(10), cm.GetCheckpointInterval())

	// Set new interval
	cm.SetCheckpointInterval(20)
	assert.Equal(t, uint64(20), cm.GetCheckpointInterval())

	// Attempt to set invalid interval
	cm.SetCheckpointInterval(0)
	assert.Equal(t, uint64(20), cm.GetCheckpointInterval())
}

func TestCheckpointManager_GetSetMaxCheckpoints(t *testing.T) {
	cm, tempDir := setupTestCheckpointManager(t)
	defer cleanupTestCheckpointManager(t, tempDir)

	// Verify initial max
	assert.Equal(t, 3, cm.GetMaxCheckpoints())

	// Set new max
	cm.SetMaxCheckpoints(5)
	assert.Equal(t, 5, cm.GetMaxCheckpoints())

	// Attempt to set invalid max
	cm.SetMaxCheckpoints(0)
	assert.Equal(t, 5, cm.GetMaxCheckpoints())
}

func TestCheckpointManager_GetLastCheckpointHeight(t *testing.T) {
	cm, tempDir := setupTestCheckpointManager(t)
	defer cleanupTestCheckpointManager(t, tempDir)

	// Verify initial height
	assert.Equal(t, uint64(0), cm.GetLastCheckpointHeight())

	// Create and finalize a checkpoint
	_, err := cm.CreateCheckpoint(10, "hash10", "prevhash")
	require.NoError(t, err)
	err = cm.FinalizeCheckpoint()
	require.NoError(t, err)

	// Verify height was updated
	assert.Equal(t, uint64(10), cm.GetLastCheckpointHeight())
}

func TestCheckpointManager_HasCheckpoint(t *testing.T) {
	cm, tempDir := setupTestCheckpointManager(t)
	defer cleanupTestCheckpointManager(t, tempDir)

	// Verify no checkpoint exists
	assert.False(t, cm.HasCheckpoint(10))

	// Create and finalize a checkpoint
	_, err := cm.CreateCheckpoint(10, "hash10", "prevhash")
	require.NoError(t, err)
	err = cm.FinalizeCheckpoint()
	require.NoError(t, err)

	// Verify checkpoint exists
	assert.True(t, cm.HasCheckpoint(10))
}

func TestCheckpointManager_GetCheckpointCount(t *testing.T) {
	cm, tempDir := setupTestCheckpointManager(t)
	defer cleanupTestCheckpointManager(t, tempDir)

	// Verify initial count
	assert.Equal(t, 0, cm.GetCheckpointCount())

	// Create and finalize checkpoints
	_, err := cm.CreateCheckpoint(10, "hash10", "prevhash")
	require.NoError(t, err)
	err = cm.FinalizeCheckpoint()
	require.NoError(t, err)

	_, err = cm.CreateCheckpoint(20, "hash20", "hash10")
	require.NoError(t, err)
	err = cm.FinalizeCheckpoint()
	require.NoError(t, err)

	// Verify count was updated
	assert.Equal(t, 2, cm.GetCheckpointCount())
}

func TestCheckpointManager_GetCheckpointStats(t *testing.T) {
	cm, tempDir := setupTestCheckpointManager(t)
	defer cleanupTestCheckpointManager(t, tempDir)

	// Create and finalize checkpoints
	_, err := cm.CreateCheckpoint(10, "hash10", "prevhash")
	require.NoError(t, err)
	err = cm.FinalizeCheckpoint()
	require.NoError(t, err)

	_, err = cm.CreateCheckpoint(20, "hash20", "hash10")
	require.NoError(t, err)
	err = cm.FinalizeCheckpoint()
	require.NoError(t, err)

	// Get stats
	stats := cm.GetCheckpointStats()

	// Verify stats
	assert.Equal(t, 2, stats["count"])
	assert.Equal(t, uint64(20), stats["lastCheckpoint"])
	assert.Equal(t, uint64(10), stats["checkpointInterval"])
	assert.Equal(t, 3, stats["maxCheckpoints"])
	assert.Equal(t, false, stats["compressionEnabled"])
	assert.Equal(t, true, stats["autoVerify"])
	assert.Equal(t, []uint64{10, 20}, stats["heights"])
}

func TestCheckpointStatus_String(t *testing.T) {
	assert.Equal(t, "Pending", CheckpointPending.String())
	assert.Equal(t, "Complete", CheckpointComplete.String())
	assert.Equal(t, "Corrupted", CheckpointCorrupted.String())
	assert.Equal(t, "Verified", CheckpointVerified.String())
	assert.Equal(t, "Unknown", CheckpointStatus(99).String())
}

func TestCheckpointManager_loadCheckpoints(t *testing.T) {
	// Create a temporary directory
	tempDir, err := os.MkdirTemp("", "checkpoint_load_test_*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create a checkpoint directory
	checkpointDir := filepath.Join(tempDir, "checkpoint_10")
	err = os.MkdirAll(checkpointDir, 0755)
	require.NoError(t, err)

	// Create metadata file
	metadata := CheckpointMetadata{
		Height:       10,
		Timestamp:    time.Now(),
		BlockHash:    "hash10",
		PreviousHash: "prevhash",
		Components:   []string{"comp1", "comp2"},
		Metrics:      make(map[string]interface{}),
		Version:      "1.0",
	}

	// Write metadata to file
	metadataBytes, err := json.MarshalIndent(metadata, "", "  ")
	require.NoError(t, err)
	err = os.WriteFile(filepath.Join(checkpointDir, "metadata.json"), metadataBytes, 0644)
	require.NoError(t, err)

	// Create checkpoint manager with the temp directory
	cm, err := NewCheckpointManager(WithCheckpointDir(tempDir))
	require.NoError(t, err)

	// Verify checkpoint was loaded
	assert.True(t, cm.HasCheckpoint(10))
	assert.Equal(t, uint64(10), cm.GetLastCheckpointHeight())
}
