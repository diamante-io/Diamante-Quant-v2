// consensus/recovery/checkpoint_manager.go

package recovery

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	"diamante/consensus"
)

// CheckpointMetadata contains metadata about a checkpoint
type CheckpointMetadata struct {
	Height       uint64                 `json:"height"`
	Timestamp    time.Time              `json:"timestamp"`
	BlockHash    string                 `json:"blockHash"`
	PreviousHash string                 `json:"previousHash"`
	StateSize    int64                  `json:"stateSize"`
	Components   []string               `json:"components"`
	Metrics      map[string]interface{} `json:"metrics"`
	Version      string                 `json:"version"`
}

// CheckpointStatus represents the status of a checkpoint
type CheckpointStatus int

const (
	// CheckpointPending indicates a checkpoint is being created
	CheckpointPending CheckpointStatus = iota
	// CheckpointComplete indicates a checkpoint is complete
	CheckpointComplete
	// CheckpointCorrupted indicates a checkpoint is corrupted
	CheckpointCorrupted
	// CheckpointVerified indicates a checkpoint has been verified
	CheckpointVerified
)

// String returns a string representation of the checkpoint status
func (s CheckpointStatus) String() string {
	switch s {
	case CheckpointPending:
		return "Pending"
	case CheckpointComplete:
		return "Complete"
	case CheckpointCorrupted:
		return "Corrupted"
	case CheckpointVerified:
		return "Verified"
	default:
		return "Unknown"
	}
}

// Checkpoint represents a point-in-time snapshot of the system state
type Checkpoint struct {
	Metadata CheckpointMetadata `json:"metadata"`
	Status   CheckpointStatus   `json:"status"`
	Path     string             `json:"path"`
	// ComponentStates stores serialized state data for each component
	ComponentStates map[string][]byte `json:"-"` // Not serialized directly
}

// ComponentCheckpointHandler defines the interface for components that support checkpointing
type ComponentCheckpointHandler interface {
	// GetCheckpointState returns the serialized state for checkpointing
	GetCheckpointState() ([]byte, error)
	// RestoreFromCheckpoint restores the component state from a checkpoint
	RestoreFromCheckpoint(state []byte) error
	// GetComponentName returns the name of the component
	GetComponentName() string
}

// CheckpointManager handles the creation and management of checkpoints
type CheckpointManager struct {
	// Configuration
	checkpointDir      string
	checkpointInterval uint64
	maxCheckpoints     int
	compressionEnabled bool
	autoVerify         bool

	// State
	mu                sync.RWMutex
	checkpoints       map[uint64]*Checkpoint
	lastCheckpoint    uint64
	components        map[string]ComponentCheckpointHandler
	pendingCheckpoint *Checkpoint

	// Logger
	logger *logrus.Logger
}

// CheckpointOption defines functional options for CheckpointManager
type CheckpointOption func(*CheckpointManager)

// WithCheckpointDir sets the directory where checkpoints are stored
func WithCheckpointDir(dir string) CheckpointOption {
	return func(cm *CheckpointManager) {
		cm.checkpointDir = dir
	}
}

// WithCheckpointInterval sets the interval between checkpoints
func WithCheckpointInterval(interval uint64) CheckpointOption {
	return func(cm *CheckpointManager) {
		if interval > 0 {
			cm.checkpointInterval = interval
		}
	}
}

// WithMaxCheckpoints sets the maximum number of checkpoints to keep
func WithMaxCheckpoints(max int) CheckpointOption {
	return func(cm *CheckpointManager) {
		if max > 0 {
			cm.maxCheckpoints = max
		}
	}
}

// WithCompressionEnabled enables or disables checkpoint compression
func WithCompressionEnabled(enabled bool) CheckpointOption {
	return func(cm *CheckpointManager) {
		cm.compressionEnabled = enabled
	}
}

// WithAutoVerify enables or disables automatic checkpoint verification
func WithAutoVerify(enabled bool) CheckpointOption {
	return func(cm *CheckpointManager) {
		cm.autoVerify = enabled
	}
}

// WithCheckpointLogger sets a custom logger
func WithCheckpointLogger(logger *logrus.Logger) CheckpointOption {
	return func(cm *CheckpointManager) {
		if logger != nil {
			cm.logger = logger
		}
	}
}

// NewCheckpointManager creates a new CheckpointManager with the given options
func NewCheckpointManager(options ...CheckpointOption) (*CheckpointManager, error) {
	cm := &CheckpointManager{
		checkpointDir:      "checkpoints",
		checkpointInterval: 1000,
		maxCheckpoints:     10,
		compressionEnabled: true,
		autoVerify:         true,
		checkpoints:        make(map[uint64]*Checkpoint),
		components:         make(map[string]ComponentCheckpointHandler),
		logger:             logrus.New(),
	}

	// Apply options
	for _, option := range options {
		option(cm)
	}

	// Create checkpoint directory if it doesn't exist
	if err := os.MkdirAll(cm.checkpointDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create checkpoint directory: %w", err)
	}

	// Load existing checkpoints
	if err := cm.loadCheckpoints(); err != nil {
		cm.logger.WithError(err).Warn("Failed to load existing checkpoints")
	}

	return cm, nil
}

// RegisterComponent registers a component for checkpointing
func (cm *CheckpointManager) RegisterComponent(component ComponentCheckpointHandler) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	name := component.GetComponentName()
	cm.components[name] = component
	cm.logger.WithField("component", name).Info("Registered component for checkpointing")
}

// UnregisterComponent unregisters a component from checkpointing
func (cm *CheckpointManager) UnregisterComponent(name string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if _, exists := cm.components[name]; exists {
		delete(cm.components, name)
		cm.logger.WithField("component", name).Info("Unregistered component from checkpointing")
	}
}

// ShouldCreateCheckpoint returns true if a checkpoint should be created at the given height
func (cm *CheckpointManager) ShouldCreateCheckpoint(height uint64) bool {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	// Check if height is divisible by checkpoint interval
	if height%cm.checkpointInterval != 0 {
		return false
	}

	// Check if we already have a checkpoint at this height
	if _, exists := cm.checkpoints[height]; exists {
		return false
	}

	// Check if we have a pending checkpoint
	if cm.pendingCheckpoint != nil {
		return false
	}

	return true
}

// CreateCheckpoint creates a new checkpoint at the given height
func (cm *CheckpointManager) CreateCheckpoint(height uint64, blockHash, prevHash string) (*Checkpoint, error) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	// Check if height is valid
	if height == 0 {
		return nil, errors.New("checkpoint height must be greater than 0")
	}

	// Check if we already have a checkpoint at this height
	if _, exists := cm.checkpoints[height]; exists {
		return nil, fmt.Errorf("checkpoint already exists at height %d", height)
	}

	// Check if we have a pending checkpoint
	if cm.pendingCheckpoint != nil {
		return nil, errors.New("another checkpoint is already in progress")
	}

	// Create checkpoint directory
	checkpointPath := filepath.Join(cm.checkpointDir, fmt.Sprintf("checkpoint_%d", height))
	if err := os.MkdirAll(checkpointPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create checkpoint directory: %w", err)
	}

	// Create checkpoint
	checkpoint := &Checkpoint{
		Metadata: CheckpointMetadata{
			Height:       height,
			Timestamp:    consensus.ConsensusNow(),
			BlockHash:    blockHash,
			PreviousHash: prevHash,
			Components:   make([]string, 0, len(cm.components)),
			Metrics:      make(map[string]interface{}),
			Version:      "1.0",
		},
		Status:          CheckpointPending,
		Path:            checkpointPath,
		ComponentStates: make(map[string][]byte),
	}

	// Set as pending checkpoint
	cm.pendingCheckpoint = checkpoint

	cm.logger.WithFields(logrus.Fields{
		"height":    height,
		"blockHash": blockHash,
		"path":      checkpointPath,
	}).Info("Created new checkpoint")

	return checkpoint, nil
}

// FinalizeCheckpoint finalizes a pending checkpoint
func (cm *CheckpointManager) FinalizeCheckpoint() error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	// Check if we have a pending checkpoint
	if cm.pendingCheckpoint == nil {
		return errors.New("no pending checkpoint to finalize")
	}

	checkpoint := cm.pendingCheckpoint
	checkpoint.Status = CheckpointComplete

	// Collect component states
	var totalSize int64
	for name, component := range cm.components {
		state, err := component.GetCheckpointState()
		if err != nil {
			cm.logger.WithFields(logrus.Fields{
				"component": name,
				"error":     err,
			}).Error("Failed to get component state for checkpoint")
			continue
		}

		// Store component state
		checkpoint.ComponentStates[name] = state
		checkpoint.Metadata.Components = append(checkpoint.Metadata.Components, name)
		totalSize += int64(len(state))

		// Write component state to file
		stateFile := filepath.Join(checkpoint.Path, fmt.Sprintf("%s.state", name))
		if err := os.WriteFile(stateFile, state, 0644); err != nil {
			cm.logger.WithFields(logrus.Fields{
				"component": name,
				"file":      stateFile,
				"error":     err,
			}).Error("Failed to write component state to file")
			continue
		}
	}

	// Update metadata
	checkpoint.Metadata.StateSize = totalSize

	// Write metadata to file
	metadataFile := filepath.Join(checkpoint.Path, "metadata.json")
	metadataBytes, err := json.MarshalIndent(checkpoint.Metadata, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal checkpoint metadata: %w", err)
	}

	if err := os.WriteFile(metadataFile, metadataBytes, 0644); err != nil {
		return fmt.Errorf("failed to write checkpoint metadata: %w", err)
	}

	// Add checkpoint to map
	cm.checkpoints[checkpoint.Metadata.Height] = checkpoint
	cm.lastCheckpoint = checkpoint.Metadata.Height
	cm.pendingCheckpoint = nil

	// Clean up old checkpoints if needed
	cm.cleanupOldCheckpoints()

	cm.logger.WithFields(logrus.Fields{
		"height":     checkpoint.Metadata.Height,
		"components": checkpoint.Metadata.Components,
		"stateSize":  checkpoint.Metadata.StateSize,
	}).Info("Finalized checkpoint")

	return nil
}

// GetCheckpoint returns the checkpoint at the given height
func (cm *CheckpointManager) GetCheckpoint(height uint64) (*Checkpoint, error) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	checkpoint, exists := cm.checkpoints[height]
	if !exists {
		return nil, fmt.Errorf("checkpoint not found at height %d", height)
	}

	return checkpoint, nil
}

// GetLatestCheckpoint returns the latest checkpoint
func (cm *CheckpointManager) GetLatestCheckpoint() (*Checkpoint, error) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	if cm.lastCheckpoint == 0 {
		return nil, errors.New("no checkpoints available")
	}

	return cm.checkpoints[cm.lastCheckpoint], nil
}

// GetCheckpointHeights returns a sorted list of checkpoint heights
func (cm *CheckpointManager) GetCheckpointHeights() []uint64 {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	heights := make([]uint64, 0, len(cm.checkpoints))
	for height := range cm.checkpoints {
		heights = append(heights, height)
	}

	sort.Slice(heights, func(i, j int) bool {
		return heights[i] < heights[j]
	})

	return heights
}

// RestoreFromCheckpoint restores the system state from a checkpoint
func (cm *CheckpointManager) RestoreFromCheckpoint(height uint64) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	// Get checkpoint
	checkpoint, exists := cm.checkpoints[height]
	if !exists {
		return fmt.Errorf("checkpoint not found at height %d", height)
	}

	// Check if checkpoint is complete
	if checkpoint.Status != CheckpointComplete && checkpoint.Status != CheckpointVerified {
		return fmt.Errorf("checkpoint at height %d is not complete", height)
	}

	// Restore component states
	for name, component := range cm.components {
		// Check if component state exists in checkpoint
		stateFile := filepath.Join(checkpoint.Path, fmt.Sprintf("%s.state", name))
		state, err := os.ReadFile(stateFile)
		if err != nil {
			cm.logger.WithFields(logrus.Fields{
				"component": name,
				"file":      stateFile,
				"error":     err,
			}).Error("Failed to read component state from checkpoint")
			continue
		}

		// Restore component state
		if err := component.RestoreFromCheckpoint(state); err != nil {
			cm.logger.WithFields(logrus.Fields{
				"component": name,
				"error":     err,
			}).Error("Failed to restore component state from checkpoint")
			continue
		}

		cm.logger.WithFields(logrus.Fields{
			"component": name,
			"height":    height,
		}).Info("Restored component state from checkpoint")
	}

	cm.logger.WithFields(logrus.Fields{
		"height":     height,
		"components": checkpoint.Metadata.Components,
	}).Info("Restored system state from checkpoint")

	return nil
}

// VerifyCheckpoint verifies the integrity of a checkpoint
func (cm *CheckpointManager) VerifyCheckpoint(height uint64) (bool, error) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	// Get checkpoint
	checkpoint, exists := cm.checkpoints[height]
	if !exists {
		return false, fmt.Errorf("checkpoint not found at height %d", height)
	}

	// Check if checkpoint is complete
	if checkpoint.Status != CheckpointComplete {
		return false, fmt.Errorf("checkpoint at height %d is not complete", height)
	}

	// Verify metadata file exists
	metadataFile := filepath.Join(checkpoint.Path, "metadata.json")
	if _, err := os.Stat(metadataFile); err != nil {
		checkpoint.Status = CheckpointCorrupted
		return false, fmt.Errorf("checkpoint metadata file not found: %w", err)
	}

	// Verify component state files exist
	for _, componentName := range checkpoint.Metadata.Components {
		stateFile := filepath.Join(checkpoint.Path, fmt.Sprintf("%s.state", componentName))
		if _, err := os.Stat(stateFile); err != nil {
			checkpoint.Status = CheckpointCorrupted
			return false, fmt.Errorf("component state file not found for %s: %w", componentName, err)
		}
	}

	// Mark checkpoint as verified
	checkpoint.Status = CheckpointVerified

	cm.logger.WithFields(logrus.Fields{
		"height":     height,
		"components": checkpoint.Metadata.Components,
	}).Info("Verified checkpoint integrity")

	return true, nil
}

// DeleteCheckpoint deletes a checkpoint
func (cm *CheckpointManager) DeleteCheckpoint(height uint64) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	// Get checkpoint
	checkpoint, exists := cm.checkpoints[height]
	if !exists {
		return fmt.Errorf("checkpoint not found at height %d", height)
	}

	// Delete checkpoint directory
	if err := os.RemoveAll(checkpoint.Path); err != nil {
		return fmt.Errorf("failed to delete checkpoint directory: %w", err)
	}

	// Remove checkpoint from map
	delete(cm.checkpoints, height)

	// Update last checkpoint if needed
	if cm.lastCheckpoint == height {
		// Find new last checkpoint
		var newLast uint64
		for h := range cm.checkpoints {
			if h > newLast {
				newLast = h
			}
		}
		cm.lastCheckpoint = newLast
	}

	cm.logger.WithField("height", height).Info("Deleted checkpoint")

	return nil
}

// cleanupOldCheckpoints removes old checkpoints to stay within the maximum limit
func (cm *CheckpointManager) cleanupOldCheckpoints() {
	// If we have fewer checkpoints than the maximum, do nothing
	if len(cm.checkpoints) <= cm.maxCheckpoints {
		return
	}

	// Get sorted heights
	heights := make([]uint64, 0, len(cm.checkpoints))
	for height := range cm.checkpoints {
		heights = append(heights, height)
	}

	sort.Slice(heights, func(i, j int) bool {
		return heights[i] < heights[j]
	})

	// Delete oldest checkpoints
	for i := 0; i < len(heights)-cm.maxCheckpoints; i++ {
		height := heights[i]
		checkpoint := cm.checkpoints[height]

		// Delete checkpoint directory
		if err := os.RemoveAll(checkpoint.Path); err != nil {
			cm.logger.WithFields(logrus.Fields{
				"height": height,
				"error":  err,
			}).Error("Failed to delete old checkpoint directory")
			continue
		}

		// Remove checkpoint from map
		delete(cm.checkpoints, height)

		cm.logger.WithField("height", height).Info("Cleaned up old checkpoint")
	}
}

// loadCheckpoints loads existing checkpoints from disk
func (cm *CheckpointManager) loadCheckpoints() error {
	// Check if checkpoint directory exists
	if _, err := os.Stat(cm.checkpointDir); os.IsNotExist(err) {
		return nil // Directory doesn't exist, nothing to load
	}

	// List checkpoint directories
	entries, err := os.ReadDir(cm.checkpointDir)
	if err != nil {
		return fmt.Errorf("failed to read checkpoint directory: %w", err)
	}

	// Load each checkpoint
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		// Check if directory name matches checkpoint pattern
		var height uint64
		if _, err := fmt.Sscanf(entry.Name(), "checkpoint_%d", &height); err != nil {
			continue
		}

		// Load checkpoint metadata
		metadataFile := filepath.Join(cm.checkpointDir, entry.Name(), "metadata.json")
		metadataBytes, err := os.ReadFile(metadataFile)
		if err != nil {
			cm.logger.WithFields(logrus.Fields{
				"file":  metadataFile,
				"error": err,
			}).Warn("Failed to read checkpoint metadata")
			continue
		}

		var metadata CheckpointMetadata
		if err := json.Unmarshal(metadataBytes, &metadata); err != nil {
			cm.logger.WithFields(logrus.Fields{
				"file":  metadataFile,
				"error": err,
			}).Warn("Failed to unmarshal checkpoint metadata")
			continue
		}

		// Create checkpoint
		checkpoint := &Checkpoint{
			Metadata:        metadata,
			Status:          CheckpointComplete,
			Path:            filepath.Join(cm.checkpointDir, entry.Name()),
			ComponentStates: make(map[string][]byte),
		}

		// Add checkpoint to map
		cm.checkpoints[height] = checkpoint

		// Update last checkpoint if needed
		if height > cm.lastCheckpoint {
			cm.lastCheckpoint = height
		}

		cm.logger.WithFields(logrus.Fields{
			"height":     height,
			"components": metadata.Components,
			"timestamp":  metadata.Timestamp,
		}).Info("Loaded existing checkpoint")
	}

	return nil
}

// GetCheckpointInterval returns the checkpoint interval
func (cm *CheckpointManager) GetCheckpointInterval() uint64 {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.checkpointInterval
}

// SetCheckpointInterval sets the checkpoint interval
func (cm *CheckpointManager) SetCheckpointInterval(interval uint64) {
	if interval == 0 {
		return
	}
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.checkpointInterval = interval
}

// GetMaxCheckpoints returns the maximum number of checkpoints to keep
func (cm *CheckpointManager) GetMaxCheckpoints() int {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.maxCheckpoints
}

// SetMaxCheckpoints sets the maximum number of checkpoints to keep
func (cm *CheckpointManager) SetMaxCheckpoints(max int) {
	if max <= 0 {
		return
	}
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.maxCheckpoints = max
	cm.cleanupOldCheckpoints()
}

// GetLastCheckpointHeight returns the height of the last checkpoint
func (cm *CheckpointManager) GetLastCheckpointHeight() uint64 {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.lastCheckpoint
}

// HasCheckpoint returns true if a checkpoint exists at the given height
func (cm *CheckpointManager) HasCheckpoint(height uint64) bool {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	_, exists := cm.checkpoints[height]
	return exists
}

// GetCheckpointCount returns the number of checkpoints
func (cm *CheckpointManager) GetCheckpointCount() int {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return len(cm.checkpoints)
}

// GetCheckpointStats returns statistics about checkpoints
func (cm *CheckpointManager) GetCheckpointStats() map[string]interface{} {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	stats := make(map[string]interface{})
	stats["count"] = len(cm.checkpoints)
	stats["lastCheckpoint"] = cm.lastCheckpoint
	stats["checkpointInterval"] = cm.checkpointInterval
	stats["maxCheckpoints"] = cm.maxCheckpoints
	stats["compressionEnabled"] = cm.compressionEnabled
	stats["autoVerify"] = cm.autoVerify

	// Get checkpoint heights
	heights := make([]uint64, 0, len(cm.checkpoints))
	for height := range cm.checkpoints {
		heights = append(heights, height)
	}
	sort.Slice(heights, func(i, j int) bool {
		return heights[i] < heights[j]
	})
	stats["heights"] = heights

	return stats
}
