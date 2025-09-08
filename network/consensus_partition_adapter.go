// network/consensus_partition_adapter.go

package network

import (
	"context"
	"fmt"
	"sync"
	"time"

	"diamante/common"

	"github.com/sirupsen/logrus"
)

// ConsensusAdapter defines the interface for interacting with the consensus system
type ConsensusAdapter interface {
	// HandleNetworkPartition notifies the consensus system of a network partition
	HandleNetworkPartition() error

	// HandleNetworkRecovery notifies the consensus system of network recovery
	HandleNetworkRecovery(stateInfo map[string]interface{}) error

	// GetConsensusState returns the current consensus state for synchronization
	GetConsensusState() (map[string]interface{}, error)
}

// ConsensusPartitionAdapter connects the partition handler to the consensus system
type ConsensusPartitionAdapter struct {
	partitionHandler *PartitionHandler
	consensusAdapter ConsensusAdapter
	logger           *logrus.Logger

	// State tracking
	lastPartitionTime time.Time
	lastRecoveryTime  time.Time
	partitionActive   bool
	stateMu           sync.RWMutex

	// Control channels
	stopChan chan struct{}
	running  bool
	runLock  sync.Mutex
}

// NewConsensusPartitionAdapter creates a new adapter between partition handler and consensus
func NewConsensusPartitionAdapter(
	partitionHandler *PartitionHandler,
	consensusAdapter ConsensusAdapter,
	logger *logrus.Logger,
) *ConsensusPartitionAdapter {
	if logger == nil {
		logger = logrus.New()
	}

	adapter := &ConsensusPartitionAdapter{
		partitionHandler:  partitionHandler,
		consensusAdapter:  consensusAdapter,
		logger:            logger,
		lastPartitionTime: time.Time{},
		lastRecoveryTime:  time.Time{},
		partitionActive:   false,
		stopChan:          make(chan struct{}),
		running:           false,
	}

	// Set up callbacks on the partition handler
	partitionHandler.SetPartitionDetectedCallback(adapter.onPartitionDetected)
	partitionHandler.SetPartitionRecoveredCallback(adapter.onPartitionRecovered)

	return adapter
}

// Start begins monitoring for partition events
func (cpa *ConsensusPartitionAdapter) Start() error {
	cpa.runLock.Lock()
	defer cpa.runLock.Unlock()

	if cpa.running {
		return fmt.Errorf("consensus partition adapter is already running")
	}

	cpa.running = true
	cpa.stopChan = make(chan struct{})

	// Start background monitoring
	go cpa.monitorPartitionStatus()

	cpa.logger.Info("Consensus partition adapter started")
	return nil
}

// Stop halts monitoring for partition events
func (cpa *ConsensusPartitionAdapter) Stop() error {
	cpa.runLock.Lock()
	defer cpa.runLock.Unlock()

	if !cpa.running {
		return fmt.Errorf("consensus partition adapter is not running")
	}

	// Signal stop - the channel is created in Start() and should only be closed once
	if cpa.stopChan != nil {
		close(cpa.stopChan)
		cpa.stopChan = nil
	}
	cpa.running = false

	cpa.logger.Info("Consensus partition adapter stopped")
	return nil
}

// IsPartitionActive returns whether a network partition is currently active
func (cpa *ConsensusPartitionAdapter) IsPartitionActive() bool {
	cpa.stateMu.RLock()
	defer cpa.stateMu.RUnlock()
	return cpa.partitionActive
}

// GetLastPartitionTime returns the time of the last detected partition
func (cpa *ConsensusPartitionAdapter) GetLastPartitionTime() time.Time {
	cpa.stateMu.RLock()
	defer cpa.stateMu.RUnlock()
	return cpa.lastPartitionTime
}

// GetLastRecoveryTime returns the time of the last recovery from a partition
func (cpa *ConsensusPartitionAdapter) GetLastRecoveryTime() time.Time {
	cpa.stateMu.RLock()
	defer cpa.stateMu.RUnlock()
	return cpa.lastRecoveryTime
}

// GetPartitionDuration returns the duration of the current or last partition
func (cpa *ConsensusPartitionAdapter) GetPartitionDuration() time.Duration {
	cpa.stateMu.RLock()
	defer cpa.stateMu.RUnlock()

	if cpa.lastPartitionTime.IsZero() {
		return 0
	}

	if cpa.partitionActive {
		return time.Since(cpa.lastPartitionTime)
	}

	if cpa.lastRecoveryTime.IsZero() {
		return 0
	}

	return cpa.lastRecoveryTime.Sub(cpa.lastPartitionTime)
}

// GetPartitionMetrics returns metrics about network partitions
func (cpa *ConsensusPartitionAdapter) GetPartitionMetrics() map[string]interface{} {
	metrics := cpa.partitionHandler.GetPartitionMetrics()

	cpa.stateMu.RLock()
	defer cpa.stateMu.RUnlock()

	return map[string]interface{}{
		"partitionsDetected":     metrics.PartitionsDetected,
		"partitionsRecovered":    metrics.PartitionsRecovered,
		"conflictsDetected":      metrics.ConflictsDetected,
		"conflictsResolved":      metrics.ConflictsResolved,
		"averageRecoveryTime":    metrics.AverageRecoveryTime,
		"lastPartitionTime":      cpa.lastPartitionTime,
		"lastRecoveryTime":       cpa.lastRecoveryTime,
		"partitionActive":        cpa.partitionActive,
		"currentPartitionStatus": metrics.CurrentStatus.String(),
	}
}

// SynchronizeConsensusState requests the current consensus state for synchronization
func (cpa *ConsensusPartitionAdapter) SynchronizeConsensusState() (map[string]interface{}, error) {
	// Create a context with timeout for the state request
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Create a buffered channel for the result to prevent goroutine leak
	resultChan := make(chan struct {
		state map[string]interface{}
		err   error
	}, 1) // Buffer of 1 to ensure goroutine can send even if we timeout
	defer close(resultChan)

	// Get the state in a goroutine to respect the timeout
	go func() {
		state, err := cpa.consensusAdapter.GetConsensusState()
		select {
		case resultChan <- struct {
			state map[string]interface{}
			err   error
		}{state, err}:
		case <-ctx.Done():
			// Context cancelled, exit goroutine
		}
	}()

	// Wait for the result or timeout
	select {
	case result := <-resultChan:
		return result.state, result.err
	case <-ctx.Done():
		return nil, fmt.Errorf("timeout getting consensus state: %w", ctx.Err())
	}
}

// onPartitionDetected is called when a network partition is detected
func (cpa *ConsensusPartitionAdapter) onPartitionDetected() {
	cpa.logger.Warn("Network partition detected")

	cpa.stateMu.Lock()
	cpa.partitionActive = true
	cpa.lastPartitionTime = common.ConsensusNow()
	cpa.stateMu.Unlock()

	// Notify consensus system
	if err := cpa.consensusAdapter.HandleNetworkPartition(); err != nil {
		cpa.logger.Error("Failed to notify consensus of network partition", "error", err)
	}
}

// onPartitionRecovered is called when recovery from a network partition is detected
func (cpa *ConsensusPartitionAdapter) onPartitionRecovered() {
	cpa.logger.Info("Network partition recovered")

	cpa.stateMu.Lock()
	cpa.partitionActive = false
	cpa.lastRecoveryTime = common.ConsensusNow()
	partitionDuration := cpa.lastRecoveryTime.Sub(cpa.lastPartitionTime)
	cpa.stateMu.Unlock()

	cpa.logger.Info("Partition duration", "duration", partitionDuration)

	// Get consensus state for synchronization
	state, err := cpa.SynchronizeConsensusState()
	if err != nil {
		cpa.logger.Error("Failed to get consensus state for recovery", "error", err)
		return
	}

	// Notify consensus system with state information
	if err := cpa.consensusAdapter.HandleNetworkRecovery(state); err != nil {
		cpa.logger.Error("Failed to notify consensus of network recovery", "error", err)
	}
}

// monitorPartitionStatus periodically checks the partition status
func (cpa *ConsensusPartitionAdapter) monitorPartitionStatus() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			status := cpa.partitionHandler.GetPartitionStatus()
			metrics := cpa.partitionHandler.GetPartitionMetrics()

			cpa.logger.Info("Partition status",
				"status", status,
				"partitionsDetected", metrics.PartitionsDetected,
				"partitionsRecovered", metrics.PartitionsRecovered,
				"conflictsDetected", metrics.ConflictsDetected,
				"conflictsResolved", metrics.ConflictsResolved)

		case <-cpa.stopChan:
			return
		}
	}
}
