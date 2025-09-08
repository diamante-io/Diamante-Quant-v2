package network

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"diamante/common"
	"diamante/consensus"

	"github.com/sirupsen/logrus"
)

// ConsensusState represents the structured consensus state
type ConsensusState struct {
	// Core consensus information
	BlockHeight      uint64 `json:"block_height"`
	BlockHash        string `json:"block_hash"`
	StateRoot        string `json:"state_root"`
	LastCommitHash   string `json:"last_commit_hash"`
	ValidatorSetHash string `json:"validator_set_hash"`

	// Validator information
	ValidatorID      string `json:"validator_id"`
	ValidatorStatus  string `json:"validator_status"`
	ValidatorPower   uint64 `json:"validator_power"`
	ValidatorAddress string `json:"validator_address"`

	// Consensus timing
	LastBlockTime int64  `json:"last_block_time"`
	BlockInterval int64  `json:"block_interval"`
	Round         uint64 `json:"round"`
	Step          string `json:"step"`

	// Network state
	PeerCount           int    `json:"peer_count"`
	ConnectedValidators int    `json:"connected_validators"`
	NetworkID           string `json:"network_id"`

	// Operational flags
	ConsensusPaused          bool `json:"consensus_paused"`
	CanPropose               bool `json:"can_propose"`
	CanVote                  bool `json:"can_vote"`
	AcceptingTransactions    bool `json:"accepting_transactions"`
	ParticipatingInConsensus bool `json:"participating_in_consensus"`

	// Partition recovery information
	PausedAt                   int64  `json:"paused_at,omitempty"`
	ResumedAt                  int64  `json:"resumed_at,omitempty"`
	PausedReason               string `json:"paused_reason,omitempty"`
	ValidatorStatusBeforePause string `json:"validator_status_before_pause,omitempty"`

	// Synchronization metadata
	Timestamp       int64  `json:"timestamp"`
	NodeInPartition bool   `json:"node_in_partition"`
	LastSyncTime    int64  `json:"last_sync_time"`
	SyncStatus      string `json:"sync_status"`
}

// ConsensusStateValidationFunc validates consensus state
type ConsensusStateValidationFunc func(state *ConsensusState) error

// ConsensusStateApplicationFunc applies consensus state
type ConsensusStateApplicationFunc func(state *ConsensusState) error

// ConsensusRecoveryCallbackFunc handles recovery with consensus state
type ConsensusRecoveryCallbackFunc func(state *ConsensusState) error

// ConsensusAdapterImpl is a default implementation of the ConsensusAdapter interface
type ConsensusAdapterImpl struct {
	mu     sync.RWMutex
	logger *logrus.Logger
	nm     *NetworkManager // Network manager for broadcasting

	// State tracking
	currentState       *ConsensusState
	lastPartitionTime  time.Time
	isInPartition      bool
	partitionCallbacks []func() error
	recoveryCallbacks  []ConsensusRecoveryCallbackFunc

	// State validation and application functions
	validateStateFn ConsensusStateValidationFunc
	applyStateFn    ConsensusStateApplicationFunc
}

// NewConsensusAdapterImpl creates a new consensus adapter implementation
func NewConsensusAdapterImpl(logger *logrus.Logger) *ConsensusAdapterImpl {
	if logger == nil {
		logger = logrus.New()
	}

	return &ConsensusAdapterImpl{
		logger: logger,
		currentState: &ConsensusState{
			ValidatorStatus:          "initializing",
			NetworkID:                "diamante-mainnet",
			CanPropose:               true,
			CanVote:                  true,
			AcceptingTransactions:    true,
			ParticipatingInConsensus: true,
			SyncStatus:               "synced",
			Timestamp:                consensus.ConsensusUnix(),
		},
		partitionCallbacks: make([]func() error, 0),
		recoveryCallbacks:  make([]ConsensusRecoveryCallbackFunc, 0),
	}
}

// HandleNetworkPartition notifies the consensus system of a network partition
func (ca *ConsensusAdapterImpl) HandleNetworkPartition() error {
	ca.mu.Lock()
	defer ca.mu.Unlock()

	ca.logger.Warn("Handling network partition in consensus")

	// Update partition state
	ca.isInPartition = true
	ca.lastPartitionTime = consensus.ConsensusNow()

	// Execute all registered partition callbacks
	var errs []error
	for _, callback := range ca.partitionCallbacks {
		if err := callback(); err != nil {
			errs = append(errs, err)
			ca.logger.Error("Partition callback failed", "error", err)
		}
	}

	// Pause consensus operations if in partition
	if err := ca.pauseConsensusOperations(); err != nil {
		return fmt.Errorf("failed to pause consensus operations: %w", err)
	}

	if len(errs) > 0 {
		return fmt.Errorf("partition handling had %d errors, first error: %w", len(errs), errs[0])
	}

	ca.logger.Info("Network partition handling completed")
	return nil
}

// HandleNetworkRecovery notifies the consensus system of network recovery
func (ca *ConsensusAdapterImpl) HandleNetworkRecovery(stateInfo map[string]interface{}) error {
	ca.mu.Lock()
	defer ca.mu.Unlock()

	ca.logger.Info("Handling network recovery in consensus")

	// Convert map to ConsensusState
	newState := &ConsensusState{}

	// Extract values from map with type assertions
	if v, ok := stateInfo["block_height"].(uint64); ok {
		newState.BlockHeight = v
	}
	if v, ok := stateInfo["block_hash"].(string); ok {
		newState.BlockHash = v
	}
	if v, ok := stateInfo["state_root"].(string); ok {
		newState.StateRoot = v
	}
	if v, ok := stateInfo["last_commit_hash"].(string); ok {
		newState.LastCommitHash = v
	}
	if v, ok := stateInfo["validator_set_hash"].(string); ok {
		newState.ValidatorSetHash = v
	}
	if v, ok := stateInfo["validator_id"].(string); ok {
		newState.ValidatorID = v
	}
	if v, ok := stateInfo["validator_status"].(string); ok {
		newState.ValidatorStatus = v
	}
	if v, ok := stateInfo["validator_power"].(uint64); ok {
		newState.ValidatorPower = v
	}
	if v, ok := stateInfo["validator_address"].(string); ok {
		newState.ValidatorAddress = v
	}
	if v, ok := stateInfo["last_block_time"].(int64); ok {
		newState.LastBlockTime = v
	}
	if v, ok := stateInfo["block_interval"].(int64); ok {
		newState.BlockInterval = v
	}
	if v, ok := stateInfo["round"].(uint64); ok {
		newState.Round = v
	}
	if v, ok := stateInfo["step"].(string); ok {
		newState.Step = v
	}
	if v, ok := stateInfo["peer_count"].(int); ok {
		newState.PeerCount = v
	}
	if v, ok := stateInfo["connected_validators"].(int); ok {
		newState.ConnectedValidators = v
	}
	if v, ok := stateInfo["network_id"].(string); ok {
		newState.NetworkID = v
	}
	if v, ok := stateInfo["consensus_paused"].(bool); ok {
		newState.ConsensusPaused = v
	}
	if v, ok := stateInfo["can_propose"].(bool); ok {
		newState.CanPropose = v
	}
	if v, ok := stateInfo["can_vote"].(bool); ok {
		newState.CanVote = v
	}
	if v, ok := stateInfo["accepting_transactions"].(bool); ok {
		newState.AcceptingTransactions = v
	}
	if v, ok := stateInfo["participating_in_consensus"].(bool); ok {
		newState.ParticipatingInConsensus = v
	}
	if v, ok := stateInfo["timestamp"].(int64); ok {
		newState.Timestamp = v
	}
	if v, ok := stateInfo["node_in_partition"].(bool); ok {
		newState.NodeInPartition = v
	}
	if v, ok := stateInfo["last_sync_time"].(int64); ok {
		newState.LastSyncTime = v
	}
	if v, ok := stateInfo["sync_status"].(string); ok {
		newState.SyncStatus = v
	}

	// Validate the incoming state
	if ca.validateStateFn != nil {
		if err := ca.validateStateFn(newState); err != nil {
			return fmt.Errorf("state validation failed: %w", err)
		}
	}

	// Apply the synchronized state
	if ca.applyStateFn != nil {
		if err := ca.applyStateFn(newState); err != nil {
			return fmt.Errorf("state application failed: %w", err)
		}
	}

	// Update recovery state
	ca.isInPartition = false
	ca.currentState = newState

	// Execute all registered recovery callbacks
	var errs []error
	for _, callback := range ca.recoveryCallbacks {
		if err := callback(newState); err != nil {
			errs = append(errs, err)
			ca.logger.Error("Recovery callback failed", "error", err)
		}
	}

	// Resume consensus operations
	if err := ca.resumeConsensusOperations(newState); err != nil {
		return fmt.Errorf("failed to resume consensus operations: %w", err)
	}

	if len(errs) > 0 {
		return fmt.Errorf("recovery handling had %d errors, first error: %w", len(errs), errs[0])
	}

	ca.logger.Info("Network recovery handling completed")
	return nil
}

// GetConsensusState returns the current consensus state for synchronization
func (ca *ConsensusAdapterImpl) GetConsensusState() (map[string]interface{}, error) {
	state, err := ca.GetConsensusStateTyped()
	if err != nil {
		return nil, err
	}

	// Convert to map[string]interface{} for the interface
	return map[string]interface{}{
		"block_height":                  state.BlockHeight,
		"block_hash":                    state.BlockHash,
		"state_root":                    state.StateRoot,
		"last_commit_hash":              state.LastCommitHash,
		"validator_set_hash":            state.ValidatorSetHash,
		"validator_id":                  state.ValidatorID,
		"validator_status":              state.ValidatorStatus,
		"validator_power":               state.ValidatorPower,
		"validator_address":             state.ValidatorAddress,
		"last_block_time":               state.LastBlockTime,
		"block_interval":                state.BlockInterval,
		"round":                         state.Round,
		"step":                          state.Step,
		"peer_count":                    state.PeerCount,
		"connected_validators":          state.ConnectedValidators,
		"network_id":                    state.NetworkID,
		"consensus_paused":              state.ConsensusPaused,
		"can_propose":                   state.CanPropose,
		"can_vote":                      state.CanVote,
		"accepting_transactions":        state.AcceptingTransactions,
		"participating_in_consensus":    state.ParticipatingInConsensus,
		"paused_at":                     state.PausedAt,
		"resumed_at":                    state.ResumedAt,
		"paused_reason":                 state.PausedReason,
		"validator_status_before_pause": state.ValidatorStatusBeforePause,
		"timestamp":                     state.Timestamp,
		"node_in_partition":             state.NodeInPartition,
		"last_sync_time":                state.LastSyncTime,
		"sync_status":                   state.SyncStatus,
	}, nil
}

// GetConsensusStateTyped returns the current consensus state as a typed struct
func (ca *ConsensusAdapterImpl) GetConsensusStateTyped() (*ConsensusState, error) {
	ca.mu.RLock()
	defer ca.mu.RUnlock()

	// Create a copy of the current state to avoid concurrent modification
	stateCopy := &ConsensusState{
		BlockHeight:                ca.currentState.BlockHeight,
		BlockHash:                  ca.currentState.BlockHash,
		StateRoot:                  ca.currentState.StateRoot,
		LastCommitHash:             ca.currentState.LastCommitHash,
		ValidatorSetHash:           ca.currentState.ValidatorSetHash,
		ValidatorID:                ca.currentState.ValidatorID,
		ValidatorStatus:            ca.currentState.ValidatorStatus,
		ValidatorPower:             ca.currentState.ValidatorPower,
		ValidatorAddress:           ca.currentState.ValidatorAddress,
		LastBlockTime:              ca.currentState.LastBlockTime,
		BlockInterval:              ca.currentState.BlockInterval,
		Round:                      ca.currentState.Round,
		Step:                       ca.currentState.Step,
		PeerCount:                  ca.currentState.PeerCount,
		ConnectedValidators:        ca.currentState.ConnectedValidators,
		NetworkID:                  ca.currentState.NetworkID,
		ConsensusPaused:            ca.currentState.ConsensusPaused,
		CanPropose:                 ca.currentState.CanPropose,
		CanVote:                    ca.currentState.CanVote,
		AcceptingTransactions:      ca.currentState.AcceptingTransactions,
		ParticipatingInConsensus:   ca.currentState.ParticipatingInConsensus,
		PausedAt:                   ca.currentState.PausedAt,
		ResumedAt:                  ca.currentState.ResumedAt,
		PausedReason:               ca.currentState.PausedReason,
		ValidatorStatusBeforePause: ca.currentState.ValidatorStatusBeforePause,
		LastSyncTime:               ca.currentState.LastSyncTime,
		SyncStatus:                 ca.currentState.SyncStatus,
	}

	// Add current timestamp and partition status
	stateCopy.Timestamp = consensus.ConsensusUnix()
	stateCopy.NodeInPartition = ca.isInPartition

	ca.logger.Debug("Returning consensus state", "blockHeight", stateCopy.BlockHeight, "validatorID", stateCopy.ValidatorID)
	return stateCopy, nil
}

// SetState sets the current consensus state (for testing and initialization)
func (ca *ConsensusAdapterImpl) SetState(state *ConsensusState) error {
	ca.mu.Lock()
	defer ca.mu.Unlock()

	if state == nil {
		return fmt.Errorf("state cannot be nil")
	}

	ca.currentState = state
	ca.logger.Info("Consensus state updated", "blockHeight", state.BlockHeight, "validatorID", state.ValidatorID)
	return nil
}

// RegisterPartitionCallback registers a callback to be executed on partition detection
func (ca *ConsensusAdapterImpl) RegisterPartitionCallback(callback func() error) {
	ca.mu.Lock()
	defer ca.mu.Unlock()

	ca.partitionCallbacks = append(ca.partitionCallbacks, callback)
	ca.logger.Debug("Registered partition callback")
}

// RegisterRecoveryCallback registers a callback to be executed on recovery
func (ca *ConsensusAdapterImpl) RegisterRecoveryCallback(callback ConsensusRecoveryCallbackFunc) {
	ca.mu.Lock()
	defer ca.mu.Unlock()

	ca.recoveryCallbacks = append(ca.recoveryCallbacks, callback)
	ca.logger.Debug("Registered recovery callback")
}

// SetValidateStateFunction sets the function used to validate incoming state
func (ca *ConsensusAdapterImpl) SetValidateStateFunction(fn ConsensusStateValidationFunc) {
	ca.mu.Lock()
	defer ca.mu.Unlock()
	ca.validateStateFn = fn
}

// SetApplyStateFunction sets the function used to apply synchronized state
func (ca *ConsensusAdapterImpl) SetApplyStateFunction(fn ConsensusStateApplicationFunc) {
	ca.mu.Lock()
	defer ca.mu.Unlock()
	ca.applyStateFn = fn
}

// IsInPartition returns whether the consensus is currently in a partition
func (ca *ConsensusAdapterImpl) IsInPartition() bool {
	ca.mu.RLock()
	defer ca.mu.RUnlock()
	return ca.isInPartition
}

// GetLastPartitionTime returns the time of the last partition
func (ca *ConsensusAdapterImpl) GetLastPartitionTime() time.Time {
	ca.mu.RLock()
	defer ca.mu.RUnlock()
	return ca.lastPartitionTime
}

// pauseConsensusOperations pauses consensus operations during partition
func (ca *ConsensusAdapterImpl) pauseConsensusOperations() error {
	ca.logger.Info("Pausing consensus operations due to network partition")

	// Update state to indicate consensus is paused
	ca.currentState.ConsensusPaused = true
	ca.currentState.PausedAt = consensus.ConsensusUnix()
	ca.currentState.PausedReason = "network_partition"

	// Set operational flags
	ca.currentState.CanPropose = false
	ca.currentState.CanVote = false
	ca.currentState.AcceptingTransactions = false
	ca.currentState.ParticipatingInConsensus = false

	// Store current validator state if applicable
	if ca.currentState.ValidatorID != "" {
		ca.currentState.ValidatorStatusBeforePause = ca.currentState.ValidatorStatus
		ca.currentState.ValidatorStatus = "inactive"
	}

	// Notify consensus module through callbacks
	for _, callback := range ca.partitionCallbacks {
		if err := callback(); err != nil {
			ca.logger.Error("Failed to execute partition callback", "error", err)
		}
	}

	ca.logger.Info("Consensus operations paused successfully",
		"timestamp", ca.currentState.PausedAt,
		"validatorID", ca.currentState.ValidatorID)

	return nil
}

// resumeConsensusOperations resumes consensus operations after recovery
func (ca *ConsensusAdapterImpl) resumeConsensusOperations(newState *ConsensusState) error {
	ca.logger.Info("Resuming consensus operations after network recovery")

	// Check if we were actually paused
	if !ca.currentState.ConsensusPaused {
		return fmt.Errorf("consensus operations were not paused")
	}

	// Calculate pause duration for metrics
	if ca.currentState.PausedAt > 0 {
		pauseDuration := consensus.ConsensusUnix() - ca.currentState.PausedAt
		ca.logger.WithField("pauseDuration", pauseDuration).Info("Consensus was paused for duration")
	}

	// Update state to indicate consensus is resumed
	ca.currentState.ConsensusPaused = false
	ca.currentState.ResumedAt = consensus.ConsensusUnix()
	ca.currentState.PausedReason = ""

	// Restore operational flags
	ca.currentState.CanPropose = true
	ca.currentState.CanVote = true
	ca.currentState.AcceptingTransactions = true
	ca.currentState.ParticipatingInConsensus = true

	// Restore validator state if applicable
	if ca.currentState.ValidatorID != "" {
		if ca.currentState.ValidatorStatusBeforePause != "" {
			ca.currentState.ValidatorStatus = ca.currentState.ValidatorStatusBeforePause
			ca.currentState.ValidatorStatusBeforePause = ""
		} else {
			ca.currentState.ValidatorStatus = "active"
		}
	}

	ca.logger.Info("Consensus operations resumed successfully",
		"timestamp", ca.currentState.ResumedAt,
		"validatorID", ca.currentState.ValidatorID)

	// Apply new state if provided
	if newState != nil {
		ca.mergeConsensusState(newState)
	}

	return nil
}

// mergeConsensusState merges new state into current state
func (ca *ConsensusAdapterImpl) mergeConsensusState(newState *ConsensusState) {
	if newState.BlockHeight > ca.currentState.BlockHeight {
		ca.currentState.BlockHeight = newState.BlockHeight
		ca.currentState.BlockHash = newState.BlockHash
		ca.currentState.StateRoot = newState.StateRoot
		ca.currentState.LastCommitHash = newState.LastCommitHash
	}

	if newState.ValidatorSetHash != "" {
		ca.currentState.ValidatorSetHash = newState.ValidatorSetHash
	}

	if newState.PeerCount > 0 {
		ca.currentState.PeerCount = newState.PeerCount
	}

	if newState.ConnectedValidators > 0 {
		ca.currentState.ConnectedValidators = newState.ConnectedValidators
	}

	ca.currentState.LastSyncTime = consensus.ConsensusUnix()
	ca.currentState.SyncStatus = "synced"
}

// SetNetworkManager sets the network manager for broadcasting
func (ca *ConsensusAdapterImpl) SetNetworkManager(nm *NetworkManager) {
	ca.mu.Lock()
	defer ca.mu.Unlock()
	ca.nm = nm
}

// BroadcastBlock implements the consensus.BlockBroadcaster interface
func (ca *ConsensusAdapterImpl) BroadcastBlock(block *common.Block) error {
	ca.mu.RLock()
	nm := ca.nm
	ca.mu.RUnlock()

	if nm == nil {
		return fmt.Errorf("network manager not set")
	}

	// Log broadcast start
	hash8 := ""
	if len(block.Hash) >= 8 {
		hash8 = block.Hash[:8]
	} else {
		hash8 = block.Hash
	}

	peers := nm.GetPeers()
	ca.logger.WithFields(logrus.Fields{
		"event":     "BroadcastBlockProposal",
		"height":    block.Number,
		"hash8":     hash8,
		"peerCount": len(peers),
	}).Info("Broadcasting block proposal to peers")

	// Convert consensus.Block to BlockPayload for network transmission
	blockPayload := &BlockPayload{
		BlockHash:       block.Hash,
		BlockNumber:     uint64(block.Number),
		ParentHash:      block.PreviousHash,
		StateRoot:       block.StateRoot,
		TransactionRoot: block.TransactionRoot,
		Timestamp:       block.Timestamp,
		Proposer:        block.Validator,
		TransactionIDs:  make([]string, len(block.Transactions)),
		Transactions:    block.Transactions, // Include full transaction objects
		Signature:       string(block.Signature),
		Size:            uint64(len(block.Data)), // Block data size
		GasUsed:         block.GasUsed,
		GasLimit:        block.GasLimit,
	}

	// Extract transaction IDs
	for i, tx := range block.Transactions {
		blockPayload.TransactionIDs[i] = tx.ID
	}

	ca.logger.WithFields(logrus.Fields{
		"blockNumber": block.Number,
		"txCount":     len(block.Transactions),
	}).Info("Broadcasting block with full transactions")

	// Create a block message
	msg := NewBlockMessage(nm.localAddr, blockPayload)

	// Broadcast the block to all peers
	if err := nm.Broadcast(*msg); err != nil {
		ca.logger.WithFields(logrus.Fields{
			"event":  "BroadcastBlockFailed",
			"height": block.Number,
			"hash8":  hash8,
			"error":  err.Error(),
		}).Error("Failed to broadcast block")
		return fmt.Errorf("failed to broadcast block: %w", err)
	}

	ca.logger.WithFields(logrus.Fields{
		"event":       "BroadcastBlockSuccess",
		"blockNumber": block.Number,
		"blockHash":   block.Hash,
		"hash8":       hash8,
		"peerCount":   len(peers),
		"validator":   block.Validator,
	}).Info("Successfully broadcast block")

	return nil
}

// RequestSync implements the consensus.BlockBroadcaster interface
func (ca *ConsensusAdapterImpl) RequestSync(fromHeight, toHeight uint64) error {
	ca.mu.RLock()
	nm := ca.nm
	ca.mu.RUnlock()

	if nm == nil {
		return fmt.Errorf("network manager not set")
	}

	ca.logger.WithFields(logrus.Fields{
		"fromHeight": fromHeight,
		"toHeight":   toHeight,
	}).Info("Requesting block sync from network")

	// Create a sync request payload
	syncPayload := &SyncPayload{
		RequestType: "blocks",
		FromHeight:  fromHeight,
		ToHeight:    toHeight,
	}

	// Marshal the payload
	payloadBytes, err := json.Marshal(syncPayload)
	if err != nil {
		return fmt.Errorf("failed to marshal sync payload: %w", err)
	}

	// Create sync request message
	msg := Message{
		Type:      MessageTypeSync,
		Payload:   payloadBytes,
		IsRequest: true,
		Timestamp: consensus.ConsensusUnixNano(),
		Sender:    nm.GetNodeID(),
	}

	// Broadcast sync request to all peers
	if err := nm.Broadcast(msg); err != nil {
		ca.logger.WithError(err).Error("Failed to broadcast sync request")
		return fmt.Errorf("failed to broadcast sync request: %w", err)
	}

	ca.logger.Info("Sync request broadcast successfully")
	return nil
}

// ConsensusAdapterMetrics represents metrics for the consensus adapter
type ConsensusAdapterMetrics struct {
	IsInPartition      bool   `json:"is_in_partition"`
	LastPartitionTime  int64  `json:"last_partition_time"`
	PartitionCallbacks int    `json:"partition_callbacks"`
	RecoveryCallbacks  int    `json:"recovery_callbacks"`
	CurrentBlockHeight uint64 `json:"current_block_height"`
	ValidatorID        string `json:"validator_id"`
	ValidatorStatus    string `json:"validator_status"`
	ConsensusPaused    bool   `json:"consensus_paused"`
}

// GetMetrics returns consensus adapter metrics
func (ca *ConsensusAdapterImpl) GetMetrics() *ConsensusAdapterMetrics {
	ca.mu.RLock()
	defer ca.mu.RUnlock()

	return &ConsensusAdapterMetrics{
		IsInPartition:      ca.isInPartition,
		LastPartitionTime:  ca.lastPartitionTime.Unix(),
		PartitionCallbacks: len(ca.partitionCallbacks),
		RecoveryCallbacks:  len(ca.recoveryCallbacks),
		CurrentBlockHeight: ca.currentState.BlockHeight,
		ValidatorID:        ca.currentState.ValidatorID,
		ValidatorStatus:    ca.currentState.ValidatorStatus,
		ConsensusPaused:    ca.currentState.ConsensusPaused,
	}
}
