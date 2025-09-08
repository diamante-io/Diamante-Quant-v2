// consensus/recovery/hybrid_consensus_recovery.go

package recovery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	consensus "diamante/consensus"
	"diamante/consensus/types"

	"github.com/sirupsen/logrus"
)

// HybridConsensusRecoveryAdapter adapts the HybridConsensus to use the recovery and checkpoint managers
type HybridConsensusRecoveryAdapter struct {
	// The underlying consensus implementation
	consensus types.Consensus

	// Recovery and checkpoint managers
	recoveryManager   *RecoveryManager
	checkpointManager *CheckpointManager

	// Logger
	logger *logrus.Logger
}

// HybridConsensusRecoveryOption defines functional options for HybridConsensusRecoveryAdapter
type HybridConsensusRecoveryOption func(*HybridConsensusRecoveryAdapter)

// WithRecoveryManager sets the recovery manager
func WithRecoveryManager(rm *RecoveryManager) HybridConsensusRecoveryOption {
	return func(hcra *HybridConsensusRecoveryAdapter) {
		hcra.recoveryManager = rm
	}
}

// WithCheckpointManager sets the checkpoint manager
func WithCheckpointManager(cm *CheckpointManager) HybridConsensusRecoveryOption {
	return func(hcra *HybridConsensusRecoveryAdapter) {
		hcra.checkpointManager = cm
	}
}

// WithAdapterLogger sets the logger for the adapter
func WithAdapterLogger(logger *logrus.Logger) HybridConsensusRecoveryOption {
	return func(hcra *HybridConsensusRecoveryAdapter) {
		hcra.logger = logger
	}
}

// NewHybridConsensusRecoveryAdapter creates a new HybridConsensusRecoveryAdapter
func NewHybridConsensusRecoveryAdapter(
	consensus types.Consensus,
	options ...HybridConsensusRecoveryOption,
) (*HybridConsensusRecoveryAdapter, error) {
	if consensus == nil {
		return nil, errors.New("consensus cannot be nil")
	}

	logger := logrus.New()
	logger.SetFormatter(&logrus.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: time.RFC3339Nano,
	})

	adapter := &HybridConsensusRecoveryAdapter{
		consensus: consensus,
		logger:    logger,
	}

	// Apply options
	for _, option := range options {
		option(adapter)
	}

	// Create checkpoint manager first if not provided
	if adapter.checkpointManager == nil {
		cm, err := NewCheckpointManager(
			WithCheckpointLogger(logger),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create checkpoint manager: %w", err)
		}
		adapter.checkpointManager = cm
	}

	// Create recovery manager if not provided
	if adapter.recoveryManager == nil {
		adapter.recoveryManager = NewRecoveryManager(
			WithLogger(logger),
			WithRecoveryConsensus(consensus),
			WithRecoveryCheckpointManager(adapter.checkpointManager),
			WithOnRecoveryStart(func(component string, err error) error {
				adapter.logger.WithFields(logrus.Fields{
					"component": component,
					"error":     err,
				}).Info("Starting recovery process")
				return nil
			}),
			WithOnRecoveryComplete(func(component string, success bool) error {
				adapter.logger.WithFields(logrus.Fields{
					"component": component,
					"success":   success,
				}).Info("Recovery process completed")
				return nil
			}),
		)
	}

	// Register components with the checkpoint manager
	adapter.registerComponents()

	return adapter, nil
}

// registerComponents registers the consensus components with the checkpoint manager
func (hcra *HybridConsensusRecoveryAdapter) registerComponents() {
	// Register Lachesis component
	hcra.checkpointManager.RegisterComponent(&consensusComponentAdapter{
		name:      "lachesis",
		consensus: hcra.consensus,
		getState: func(c types.Consensus) ([]byte, error) {
			return c.GetLachesis().(interface{ GetState() ([]byte, error) }).GetState()
		},
		restoreState: func(c types.Consensus, state []byte) error {
			return c.GetLachesis().(interface{ RestoreState([]byte) error }).RestoreState(state)
		},
	})

	// Register DPoS component
	hcra.checkpointManager.RegisterComponent(&consensusComponentAdapter{
		name:      "dpos",
		consensus: hcra.consensus,
		getState: func(c types.Consensus) ([]byte, error) {
			return c.GetDPoS().(interface{ GetState() ([]byte, error) }).GetState()
		},
		restoreState: func(c types.Consensus, state []byte) error {
			return c.GetDPoS().(interface{ RestoreState([]byte) error }).RestoreState(state)
		},
	})

	// Register PoH component
	hcra.checkpointManager.RegisterComponent(&pohComponentAdapter{
		name:      "poh",
		consensus: hcra.consensus,
	})

	// Register validator manager component
	hcra.checkpointManager.RegisterComponent(&consensusComponentAdapter{
		name:      "validators",
		consensus: hcra.consensus,
		getState: func(c types.Consensus) ([]byte, error) {
			if hc, ok := c.(*consensus.HybridConsensus); ok {
				return hc.GetValidatorManager().GetState()
			}
			return nil, errors.New("unsupported consensus type")
		},
		restoreState: func(c types.Consensus, state []byte) error {
			if hc, ok := c.(*consensus.HybridConsensus); ok {
				return hc.GetValidatorManager().RestoreState(state)
			}
			return errors.New("unsupported consensus type")
		},
	})

	// Register main consensus component
	hcra.checkpointManager.RegisterComponent(&consensusComponentAdapter{
		name:      "consensus",
		consensus: hcra.consensus,
		getState: func(c types.Consensus) ([]byte, error) {
			// Type assertion to get the HybridConsensus methods
			hc, ok := c.(interface {
				GetLastBlockHeight() uint64
				GetLastBlockHash() [32]byte
			})
			if !ok {
				return nil, errors.New("consensus does not implement required methods")
			}

			// Serialize the current block height and other metadata
			state := struct {
				LastBlockHeight uint64    `json:"lastBlockHeight"`
				LastBlockHash   [32]byte  `json:"lastBlockHash"`
				Timestamp       time.Time `json:"timestamp"`
			}{
				LastBlockHeight: hc.GetLastBlockHeight(),
				LastBlockHash:   hc.GetLastBlockHash(),
				Timestamp:       consensus.ConsensusNow(),
			}
			return json.Marshal(state)
		},
		restoreState: func(c types.Consensus, state []byte) error {
			// This is a placeholder - the actual implementation would
			// need to restore the consensus state from the serialized data
			return nil
		},
	})
}

// consensusComponentAdapter adapts the consensus components to the ComponentCheckpointHandler interface
type consensusComponentAdapter struct {
	name         string
	consensus    types.Consensus
	getState     func(types.Consensus) ([]byte, error)
	restoreState func(types.Consensus, []byte) error
}

func (cca *consensusComponentAdapter) GetComponentName() string {
	return cca.name
}

func (cca *consensusComponentAdapter) GetCheckpointState() ([]byte, error) {
	return cca.getState(cca.consensus)
}

func (cca *consensusComponentAdapter) RestoreFromCheckpoint(state []byte) error {
	return cca.restoreState(cca.consensus, state)
}

// pohComponentAdapter adapts the PoH component to the ComponentCheckpointHandler interface
type pohComponentAdapter struct {
	name      string
	consensus types.Consensus
}

func (pca *pohComponentAdapter) GetComponentName() string {
	return pca.name
}

func (pca *pohComponentAdapter) GetCheckpointState() ([]byte, error) {
	poh := pca.consensus.GetPoH()
	state := poh.GetState()
	count := poh.GetCount()

	// Serialize the PoH state and count
	pohState := struct {
		State [32]byte `json:"state"`
		Count uint64   `json:"count"`
	}{
		State: state,
		Count: count,
	}

	return json.Marshal(pohState)
}

func (pca *pohComponentAdapter) RestoreFromCheckpoint(state []byte) error {
	var pohState struct {
		State [32]byte `json:"state"`
		Count uint64   `json:"count"`
	}

	if err := json.Unmarshal(state, &pohState); err != nil {
		return fmt.Errorf("failed to unmarshal PoH state: %w", err)
	}

	// Synchronize the PoH state
	return pca.consensus.GetPoH().Synchronize(pohState.State, pohState.Count)
}

// RecoverFromError attempts to recover from an error
func (hcra *HybridConsensusRecoveryAdapter) RecoverFromError(component string, err error, severity ErrorSeverity) error {
	return hcra.recoveryManager.RecoverFromError(component, err, severity)
}

// CreateCheckpoint creates a checkpoint at the given height
func (hcra *HybridConsensusRecoveryAdapter) CreateCheckpoint(height uint64, blockHash, prevHash string) error {
	// Check if a checkpoint should be created
	if !hcra.checkpointManager.ShouldCreateCheckpoint(height) {
		return nil
	}

	// Create checkpoint
	cp, err := hcra.checkpointManager.CreateCheckpoint(height, blockHash, prevHash)
	if err != nil {
		return fmt.Errorf("failed to create checkpoint: %w", err)
	}

	// Finalize checkpoint
	if err := hcra.checkpointManager.FinalizeCheckpoint(); err != nil {
		return fmt.Errorf("failed to finalize checkpoint: %w", err)
	}

	hcra.logger.WithFields(logrus.Fields{
		"height":    height,
		"blockHash": blockHash,
		"path":      cp.Path,
	}).Info("Checkpoint created and finalized")

	return nil
}

// RestoreFromCheckpoint restores the system state from a checkpoint
func (hcra *HybridConsensusRecoveryAdapter) RestoreFromCheckpoint(height uint64) error {
	// Check if checkpoint exists
	if !hcra.checkpointManager.HasCheckpoint(height) {
		return fmt.Errorf("checkpoint not found at height %d", height)
	}

	// Restore from checkpoint
	if err := hcra.checkpointManager.RestoreFromCheckpoint(height); err != nil {
		return fmt.Errorf("failed to restore from checkpoint: %w", err)
	}

	hcra.logger.WithField("height", height).Info("Restored from checkpoint")

	return nil
}

// RestoreFromLatestCheckpoint restores the system state from the latest checkpoint
func (hcra *HybridConsensusRecoveryAdapter) RestoreFromLatestCheckpoint() error {
	// Get latest checkpoint
	cp, err := hcra.checkpointManager.GetLatestCheckpoint()
	if err != nil {
		return fmt.Errorf("failed to get latest checkpoint: %w", err)
	}

	// Restore from checkpoint
	if err := hcra.checkpointManager.RestoreFromCheckpoint(cp.Metadata.Height); err != nil {
		return fmt.Errorf("failed to restore from checkpoint: %w", err)
	}

	hcra.logger.WithField("height", cp.Metadata.Height).Info("Restored from latest checkpoint")

	return nil
}

// GetCheckpointHeights returns a sorted list of checkpoint heights
func (hcra *HybridConsensusRecoveryAdapter) GetCheckpointHeights() []uint64 {
	return hcra.checkpointManager.GetCheckpointHeights()
}

// HasCheckpoint returns true if a checkpoint exists at the given height
func (hcra *HybridConsensusRecoveryAdapter) HasCheckpoint(height uint64) bool {
	return hcra.checkpointManager.HasCheckpoint(height)
}

// GetLastCheckpointHeight returns the height of the last checkpoint
func (hcra *HybridConsensusRecoveryAdapter) GetLastCheckpointHeight() uint64 {
	return hcra.checkpointManager.GetLastCheckpointHeight()
}

// GetCheckpointStats returns statistics about checkpoints
func (hcra *HybridConsensusRecoveryAdapter) GetCheckpointStats() map[string]interface{} {
	return hcra.checkpointManager.GetCheckpointStats()
}

// GetRecoveryStats returns statistics about recovery attempts
func (hcra *HybridConsensusRecoveryAdapter) GetRecoveryStats() map[string]interface{} {
	return hcra.recoveryManager.GetRecoveryStats()
}

// IsRecovering returns true if a recovery is in progress
func (hcra *HybridConsensusRecoveryAdapter) IsRecovering() bool {
	return hcra.recoveryManager.IsRecovering()
}

// VerifyCheckpoint verifies the integrity of a checkpoint
func (hcra *HybridConsensusRecoveryAdapter) VerifyCheckpoint(height uint64) (bool, error) {
	return hcra.checkpointManager.VerifyCheckpoint(height)
}

// DeleteCheckpoint deletes a checkpoint
func (hcra *HybridConsensusRecoveryAdapter) DeleteCheckpoint(height uint64) error {
	return hcra.checkpointManager.DeleteCheckpoint(height)
}

// SetCheckpointInterval sets the checkpoint interval
func (hcra *HybridConsensusRecoveryAdapter) SetCheckpointInterval(interval uint64) {
	hcra.checkpointManager.SetCheckpointInterval(interval)
}

// GetCheckpointInterval returns the checkpoint interval
func (hcra *HybridConsensusRecoveryAdapter) GetCheckpointInterval() uint64 {
	return hcra.checkpointManager.GetCheckpointInterval()
}

// SetMaxCheckpoints sets the maximum number of checkpoints to keep
func (hcra *HybridConsensusRecoveryAdapter) SetMaxCheckpoints(max int) {
	hcra.checkpointManager.SetMaxCheckpoints(max)
}

// GetMaxCheckpoints returns the maximum number of checkpoints to keep
func (hcra *HybridConsensusRecoveryAdapter) GetMaxCheckpoints() int {
	return hcra.checkpointManager.GetMaxCheckpoints()
}

// PerformHealthCheck performs a health check on the consensus system
func (hcra *HybridConsensusRecoveryAdapter) PerformHealthCheck(ctx context.Context) (map[string]interface{}, error) {
	// Type assertion to get the HybridConsensus methods
	hc, ok := hcra.consensus.(interface {
		GetLastBlockHeight() uint64
		GetLastBlockHash() [32]byte
		GetCurrentHeight() uint64
	})
	if !ok {
		return nil, errors.New("consensus does not implement required methods")
	}

	// Get component states
	pohCount := hcra.consensus.GetPoH().GetCount()
	blockHeight := hc.GetLastBlockHeight()
	finalizedHeight := hc.GetCurrentHeight()
	lastBlockHash := hc.GetLastBlockHash()

	// Get checkpoint stats
	checkpointStats := hcra.GetCheckpointStats()
	lastCheckpoint := hcra.GetLastCheckpointHeight()

	// Get recovery stats
	recoveryStats := hcra.GetRecoveryStats()

	// Collect health metrics
	health := map[string]interface{}{
		"status":           "OK",
		"blockHeight":      blockHeight,
		"finalizedHeight":  finalizedHeight,
		"pohCount":         pohCount,
		"lastBlockHash":    fmt.Sprintf("%x", lastBlockHash),
		"lastCheckpoint":   lastCheckpoint,
		"checkpointCount":  checkpointStats["count"],
		"recoveryState":    recoveryStats["state"],
		"recoveryCount":    recoveryStats["recoveryCount"],
		"lastRecovery":     recoveryStats["lastRecovery"],
		"activeValidators": len(hcra.consensus.GetActiveValidators()),
		"timestamp":        consensus.ConsensusNow(),
	}

	// Check for PoH/block height inconsistency
	if blockHeight > 0 && pohCount < blockHeight {
		health["status"] = "WARNING"
		health["warnings"] = []string{
			fmt.Sprintf("PoH count (%d) behind block height (%d)", pohCount, blockHeight),
		}
	}

	// Check for checkpoint availability
	checkpointInterval := hcra.GetCheckpointInterval()
	if blockHeight > checkpointInterval && lastCheckpoint < (blockHeight-2*checkpointInterval) {
		if health["status"] == "OK" {
			health["status"] = "WARNING"
			health["warnings"] = []string{
				fmt.Sprintf("Latest checkpoint (%d) too old for current height (%d)", lastCheckpoint, blockHeight),
			}
		} else {
			warnings := health["warnings"].([]string)
			warnings = append(warnings, fmt.Sprintf("Latest checkpoint (%d) too old for current height (%d)", lastCheckpoint, blockHeight))
			health["warnings"] = warnings
		}
	}

	// Check if recovery is in progress
	if hcra.IsRecovering() {
		health["status"] = "RECOVERING"
		health["lastError"] = recoveryStats["lastError"]
	}

	return health, nil
}
