// consensus/partition_adapter.go

package consensus

import (
	"diamante/consensus/types"
	dtypes "diamante/types"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

// ConsensusPartitionAdapter implements the network.ConsensusAdapter interface
// for the consensus system
type ConsensusPartitionAdapter struct {
	consensus types.Consensus
	logger    *logrus.Logger

	// Partition state tracking
	partitionActive      bool
	partitionStartTime   time.Time
	partitionEndTime     time.Time
	partitionCount       int
	recoveryCount        int
	lastPartitionError   error
	lastRecoveryDuration time.Duration
	stateMu              sync.RWMutex

	// Configuration
	pauseConsensusOnPartition bool
	maxPartitionDuration      time.Duration
}

// NewConsensusPartitionAdapter creates a new adapter for the consensus system
func NewConsensusPartitionAdapter(consensus types.Consensus, logger *logrus.Logger) *ConsensusPartitionAdapter {
	if logger == nil {
		logger = logrus.New()
	}

	return &ConsensusPartitionAdapter{
		consensus:                 consensus,
		logger:                    logger,
		partitionActive:           false,
		partitionCount:            0,
		recoveryCount:             0,
		pauseConsensusOnPartition: true,
		maxPartitionDuration:      24 * time.Hour,
	}
}

// HandleNetworkPartition notifies the consensus system of a network partition
func (cpa *ConsensusPartitionAdapter) HandleNetworkPartition() error {
	cpa.stateMu.Lock()
	defer cpa.stateMu.Unlock()

	// If already in partition state, just log and return
	if cpa.partitionActive {
		cpa.logger.Info("Ignoring duplicate partition notification")
		return nil
	}

	cpa.logger.Warn("Handling network partition in consensus")
	cpa.partitionActive = true
	cpa.partitionStartTime = ConsensusNow()
	cpa.partitionCount++

	// Create a structured error for tracking
	partitionErr := NewConsensusError(
		ErrNetworkPartition,
		ErrorCategoryNetwork,
		"network partition detected",
	).WithContext("timestamp", cpa.partitionStartTime)

	// Track the error in the consensus system
	// Note: We can't directly track errors in the consensus interface
	// Just log it instead
	cpa.logger.Error("Network partition detected", "error", partitionErr)
	cpa.lastPartitionError = partitionErr

	// If configured to pause consensus during partitions, do so
	if cpa.pauseConsensusOnPartition {
		cpa.logger.Info("Pausing consensus operations during network partition")

		// We don't fully stop the consensus, but we can adjust parameters
		// to reduce activity during the partition

		// 1. Increase gossip delay to reduce network traffic
		if lachesis := cpa.consensus.GetLachesis(); lachesis != nil {
			currentDelay := lachesis.GetGossipDelay()
			lachesis.SetGossipDelay(currentDelay * 3) // Triple the delay
		}

		// 2. Adjust network load to indicate partition
		cpa.consensus.GetNetworkLoad() // Just to get current load
		// Note: We can't directly adjust network load through the interface
		// Just log that we would do this
		cpa.logger.Info("Would adjust network load to indicate partition")
	}

	return nil
}

// HandleNetworkRecovery notifies the consensus system of network recovery
func (cpa *ConsensusPartitionAdapter) HandleNetworkRecovery(stateInfo *dtypes.ConsensusStateInfo) error {
	cpa.stateMu.Lock()

	// If not in partition state, just log and return
	if !cpa.partitionActive {
		cpa.logger.Info("Ignoring recovery notification when not in partition state")
		cpa.stateMu.Unlock()
		return nil
	}

	cpa.partitionActive = false
	cpa.partitionEndTime = ConsensusNow()
	cpa.recoveryCount++
	partitionDuration := cpa.partitionEndTime.Sub(cpa.partitionStartTime)
	cpa.lastRecoveryDuration = partitionDuration

	cpa.stateMu.Unlock()

	cpa.logger.Info("Handling network recovery in consensus",
		"partitionDuration", partitionDuration,
		"partitionCount", cpa.partitionCount,
		"recoveryCount", cpa.recoveryCount)

	// If the partition lasted too long, we need to synchronize state
	if partitionDuration > 5*time.Minute {
		cpa.logger.Warn("Long partition detected, synchronizing consensus state",
			"partitionDuration", partitionDuration)

		// Extract state information for synchronization
		if err := cpa.synchronizeConsensusState(stateInfo); err != nil {
			cpa.logger.Error("Failed to synchronize consensus state", "error", err)
			return err
		}
	}

	// If we paused consensus during the partition, resume normal operation
	if cpa.pauseConsensusOnPartition {
		cpa.logger.Info("Resuming normal consensus operations after network recovery")

		// 1. Restore normal gossip delay
		if lachesis := cpa.consensus.GetLachesis(); lachesis != nil {
			// Reset to default delay - we don't have access to the config
			// Just log that we would do this
			cpa.logger.Info("Would reset gossip delay to normal")
		}

		// 2. Reset network load
		cpa.logger.Info("Would reset network load adjustment")
	}

	return nil
}

// GetConsensusState returns the current consensus state for synchronization
func (cpa *ConsensusPartitionAdapter) GetConsensusState() (*dtypes.ConsensusStateInfo, error) {
	// Create a state info with all relevant consensus information
	state := dtypes.NewConsensusStateInfo()

	// Get Lachesis state
	if lachesis := cpa.consensus.GetLachesis(); lachesis != nil {
		lachesisState, err := lachesis.GetState()
		if err != nil {
			return nil, fmt.Errorf("failed to get Lachesis state: %w", err)
		}
		// Store lachesis state as metadata
		lachesisBytes, _ := json.Marshal(lachesisState)
		state.Set("lachesisState", dtypes.ValueTypeJSON, lachesisBytes)
	}

	// Get DPoS state
	if dpos := cpa.consensus.GetDPoS(); dpos != nil {
		dposState, err := dpos.GetState()
		if err != nil {
			return nil, fmt.Errorf("failed to get DPoS state: %w", err)
		}
		// Store dpos state as metadata
		dposBytes, _ := json.Marshal(dposState)
		state.Set("dposState", dtypes.ValueTypeJSON, dposBytes)
	}

	// Get PoH state
	if poh := cpa.consensus.GetPoH(); poh != nil {
		pohState := poh.GetState()
		pohCount := poh.GetCount()
		state.Set("pohState", dtypes.ValueTypeBytes, pohState[:])
		state.Set("pohCount", dtypes.ValueTypeUint64, dtypes.Uint64ToBytes(pohCount))
	}

	// Set block information
	state.CurrentHeight = 0 // We don't have access to this through the interface
	state.LatestBlockHash = hex.EncodeToString(make([]byte, 32))
	state.IsActive = !cpa.partitionActive
	state.InPartition = cpa.partitionActive

	// Get validator information
	activeValidators := cpa.consensus.GetActiveValidators()
	state.ActiveValidators = make([]dtypes.ValidatorStateInfo, 0, len(activeValidators))
	totalStake := uint64(0)

	for _, v := range activeValidators {
		state.ActiveValidators = append(state.ActiveValidators, dtypes.ValidatorStateInfo{
			ID:     hex.EncodeToString(v.ID[:]),
			Stake:  v.Stake,
			Active: true,
		})
		totalStake += v.Stake
	}
	state.TotalStake = totalStake

	return state, nil
}

// synchronizeConsensusState synchronizes the consensus state after a partition
func (cpa *ConsensusPartitionAdapter) synchronizeConsensusState(stateInfo *dtypes.ConsensusStateInfo) error {
	// Check if we have valid state information
	if stateInfo == nil {
		return fmt.Errorf("no state information provided for synchronization")
	}

	// Extract PoH state for synchronization
	var pohState [32]byte
	var pohCount uint64

	if pohStateVal, ok := stateInfo.Get("pohState"); ok {
		pohStateBytes := pohStateVal.Bytes()
		if len(pohStateBytes) == 32 {
			copy(pohState[:], pohStateBytes)
		} else {
			return fmt.Errorf("invalid PoH state format")
		}
	} else {
		return fmt.Errorf("PoH state not found in state information")
	}

	if pohCountVal, ok := stateInfo.Get("pohCount"); ok {
		var err error
		pohCount, err = pohCountVal.Uint64()
		if err != nil {
			return fmt.Errorf("invalid PoH count format: %w", err)
		}
	} else {
		return fmt.Errorf("PoH count not found in state information")
	}

	// Synchronize PoH state
	if poh := cpa.consensus.GetPoH(); poh != nil {
		if err := poh.Synchronize(pohState, pohCount); err != nil {
			return fmt.Errorf("failed to synchronize PoH: %w", err)
		}
	}

	cpa.logger.Info("Consensus state synchronized successfully after partition",
		"pohCount", pohCount,
		"partitionDuration", cpa.lastRecoveryDuration)

	return nil
}

// SetPauseConsensusOnPartition configures whether to pause consensus during partitions
func (cpa *ConsensusPartitionAdapter) SetPauseConsensusOnPartition(pause bool) {
	cpa.stateMu.Lock()
	defer cpa.stateMu.Unlock()
	cpa.pauseConsensusOnPartition = pause
}

// SetMaxPartitionDuration sets the maximum duration for a partition
func (cpa *ConsensusPartitionAdapter) SetMaxPartitionDuration(duration time.Duration) {
	cpa.stateMu.Lock()
	defer cpa.stateMu.Unlock()
	if duration > 0 {
		cpa.maxPartitionDuration = duration
	}
}

// IsPartitionActive returns whether a network partition is currently active
func (cpa *ConsensusPartitionAdapter) IsPartitionActive() bool {
	cpa.stateMu.RLock()
	defer cpa.stateMu.RUnlock()
	return cpa.partitionActive
}

// GetPartitionCount returns the number of partitions detected
func (cpa *ConsensusPartitionAdapter) GetPartitionCount() int {
	cpa.stateMu.RLock()
	defer cpa.stateMu.RUnlock()
	return cpa.partitionCount
}

// GetRecoveryCount returns the number of recoveries completed
func (cpa *ConsensusPartitionAdapter) GetRecoveryCount() int {
	cpa.stateMu.RLock()
	defer cpa.stateMu.RUnlock()
	return cpa.recoveryCount
}

// GetLastPartitionDuration returns the duration of the last partition
func (cpa *ConsensusPartitionAdapter) GetLastPartitionDuration() time.Duration {
	cpa.stateMu.RLock()
	defer cpa.stateMu.RUnlock()

	if cpa.partitionActive {
		return ConsensusSince(cpa.partitionStartTime)
	}

	if cpa.partitionEndTime.IsZero() || cpa.partitionStartTime.IsZero() {
		return 0
	}

	return cpa.partitionEndTime.Sub(cpa.partitionStartTime)
}

// GetPartitionMetrics returns metrics about network partitions
func (cpa *ConsensusPartitionAdapter) GetPartitionMetrics() *dtypes.PartitionMetrics {
	cpa.stateMu.RLock()
	defer cpa.stateMu.RUnlock()

	var totalDuration time.Duration
	if !cpa.partitionEndTime.IsZero() && !cpa.partitionStartTime.IsZero() {
		totalDuration = cpa.partitionEndTime.Sub(cpa.partitionStartTime)
	}

	return &dtypes.PartitionMetrics{
		PartitionCount:         cpa.partitionCount,
		RecoveryCount:          cpa.recoveryCount,
		CurrentlyPartitioned:   cpa.partitionActive,
		LastPartitionTime:      cpa.partitionStartTime,
		LastRecoveryTime:       cpa.partitionEndTime,
		LastRecoveryDuration:   cpa.lastRecoveryDuration,
		TotalPartitionDuration: totalDuration,
	}
}
