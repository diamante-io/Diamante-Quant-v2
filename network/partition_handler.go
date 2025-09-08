// network/partition_handler.go

package network

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"sync"
	"time"

	"diamante/consensus"
	monitormetrics "diamante/monitoring/metrics"
)

// PartitionStatus represents the current network partition status
type PartitionStatus int

const (
	// PartitionStatusNormal indicates normal network operation
	PartitionStatusNormal PartitionStatus = iota

	// PartitionStatusSuspected indicates a suspected network partition
	PartitionStatusSuspected

	// PartitionStatusConfirmed indicates a confirmed network partition
	PartitionStatusConfirmed

	// PartitionStatusRecovering indicates the network is recovering from a partition
	PartitionStatusRecovering
)

// String returns a string representation of the partition status
func (ps PartitionStatus) String() string {
	switch ps {
	case PartitionStatusNormal:
		return "Normal"
	case PartitionStatusSuspected:
		return "Suspected"
	case PartitionStatusConfirmed:
		return "Confirmed"
	case PartitionStatusRecovering:
		return "Recovering"
	default:
		return "Unknown"
	}
}

// PartitionConfig holds configuration for the partition handler
type PartitionConfig struct {
	// HeartbeatInterval is the interval at which heartbeats are sent
	HeartbeatInterval time.Duration

	// HeartbeatTimeout is the timeout for heartbeat responses
	HeartbeatTimeout time.Duration

	// SuspicionThreshold is the number of missed heartbeats before suspecting a partition
	SuspicionThreshold int

	// ConfirmationThreshold is the number of missed heartbeats before confirming a partition
	ConfirmationThreshold int

	// RecoveryThreshold is the number of successful heartbeats before considering recovery complete
	RecoveryThreshold int

	// MinimumPeersForQuorum is the minimum number of peers required for a quorum
	MinimumPeersForQuorum int

	// MaxStateAgeForRecovery is the maximum age of state that can be recovered
	MaxStateAgeForRecovery time.Duration

	// EnableAutomaticRecovery determines whether to automatically recover from partitions
	EnableAutomaticRecovery bool

	// EnableConflictResolution determines whether to automatically resolve conflicts
	EnableConflictResolution bool

	// ConflictResolutionStrategy determines how to resolve conflicts
	ConflictResolutionStrategy string
}

// DefaultPartitionConfig returns a default configuration for the partition handler
func DefaultPartitionConfig() *PartitionConfig {
	return &PartitionConfig{
		HeartbeatInterval:          5 * time.Second,
		HeartbeatTimeout:           2 * time.Second,
		SuspicionThreshold:         3,
		ConfirmationThreshold:      5,
		RecoveryThreshold:          3,
		MinimumPeersForQuorum:      3,
		MaxStateAgeForRecovery:     24 * time.Hour,
		EnableAutomaticRecovery:    true,
		EnableConflictResolution:   true,
		ConflictResolutionStrategy: "longest-chain",
	}
}

// PartitionMetrics tracks statistics about network partitions
type PartitionMetrics struct {
	PartitionsDetected     int
	PartitionsRecovered    int
	ConflictsDetected      int
	ConflictsResolved      int
	AverageRecoveryTime    time.Duration
	LastPartitionTime      time.Time
	LastRecoveryTime       time.Time
	TotalPartitionDuration time.Duration
	CurrentStatus          PartitionStatus
}

// PeerHeartbeatStatus tracks the heartbeat status of a peer
type PeerHeartbeatStatus struct {
	PeerID             string
	LastHeartbeatSent  time.Time
	LastHeartbeatRecv  time.Time
	MissedHeartbeats   int
	ConsecutiveSuccess int
	IsResponding       bool
}

// PartitionRecoveryState represents the structured state for partition recovery
type PartitionRecoveryState struct {
	// Peer state information
	Peers map[string]*PeerState `json:"peers"`

	// Recovery metadata
	RecoveryTimestamp  int64  `json:"recovery_timestamp"`
	RecoveryInitiator  string `json:"recovery_initiator"`
	RecoveryDuration   int64  `json:"recovery_duration_ms"`
	PeersParticipating int    `json:"peers_participating"`
	ConflictsDetected  int    `json:"conflicts_detected"`
	ConflictsResolved  int    `json:"conflicts_resolved"`

	// Network consensus state
	ConsensusState *ConsensusState `json:"consensus_state,omitempty"`

	// Recovery status
	RecoveryStatus    string `json:"recovery_status"`
	RecoveryPhase     string `json:"recovery_phase"`
	LastSyncTimestamp int64  `json:"last_sync_timestamp"`
}

// PeerState represents the state of a peer during partition recovery
type PeerState struct {
	PeerID          string  `json:"peer_id"`
	BlockHeight     uint64  `json:"block_height"`
	BlockHash       string  `json:"block_hash"`
	StateRoot       string  `json:"state_root"`
	LastBlockTime   int64   `json:"last_block_time"`
	ValidatorStatus string  `json:"validator_status"`
	NetworkLatency  int64   `json:"network_latency_ms"`
	IsResponding    bool    `json:"is_responding"`
	StateTimestamp  int64   `json:"state_timestamp"`
	Weight          float64 `json:"weight"`
}

// ConflictInfo represents information about detected conflicts
type ConflictInfo struct {
	ConflictType     string            `json:"conflict_type"`
	ConflictedPeers  []string          `json:"conflicted_peers"`
	ConflictDetails  map[string]string `json:"conflict_details"`
	Timestamp        int64             `json:"timestamp"`
	Severity         string            `json:"severity"`
	ResolutionNeeded bool              `json:"resolution_needed"`
}

// ResolutionInfo represents information about conflict resolution
type ResolutionInfo struct {
	ResolutionType    string            `json:"resolution_type"`
	ChosenState       *PeerState        `json:"chosen_state"`
	RejectedPeers     []string          `json:"rejected_peers"`
	ResolutionReason  string            `json:"resolution_reason"`
	Timestamp         int64             `json:"timestamp"`
	ResolutionMetrics map[string]string `json:"resolution_metrics"`
}

// ConflictDetectionFunc callback for conflict detection
type ConflictDetectionFunc func(conflictInfo *ConflictInfo)

// ConflictResolutionFunc callback for conflict resolution
type ConflictResolutionFunc func(resolutionInfo *ResolutionInfo)

// PartitionHandler manages detection and recovery from network partitions
type PartitionHandler struct {
	config     *PartitionConfig
	logger     *log.Logger
	metrics    PartitionMetrics
	monitoring *monitormetrics.Registry

	// Network adapter for sending messages
	networkAdapter NetworkAdapter

	// Peer heartbeat status
	peerStatus     map[string]*PeerHeartbeatStatus
	peerStatusLock sync.RWMutex

	// Partition status
	partitionStatus     PartitionStatus
	partitionStatusLock sync.RWMutex
	partitionStartTime  time.Time

	// Recovery state
	recoveryState     *PartitionRecoveryState
	recoveryStateLock sync.RWMutex // Used to protect access to recoveryState

	// State synchronizer
	stateSynchronizer *StateSynchronizer

	// Control channels
	stopChan chan struct{}
	running  bool
	runLock  sync.Mutex

	// Callbacks
	onPartitionDetected  func()
	onPartitionRecovered func()
	onConflictDetected   ConflictDetectionFunc
	onConflictResolved   ConflictResolutionFunc
}

// NewPartitionHandler creates a new partition handler
func NewPartitionHandler(networkAdapter NetworkAdapter, config *PartitionConfig, logger *log.Logger, registry *monitormetrics.Registry) *PartitionHandler {
	if config == nil {
		config = DefaultPartitionConfig()
	}

	if logger == nil {
		logger = log.New(log.Writer(), "[PartitionHandler] ", log.LstdFlags)
	}

	ph := &PartitionHandler{
		config:          config,
		logger:          logger,
		monitoring:      registry,
		networkAdapter:  networkAdapter,
		peerStatus:      make(map[string]*PeerHeartbeatStatus),
		partitionStatus: PartitionStatusNormal,
		recoveryState: &PartitionRecoveryState{
			Peers:             make(map[string]*PeerState),
			RecoveryStatus:    "idle",
			RecoveryPhase:     "none",
			RecoveryTimestamp: consensus.ConsensusUnix(),
		},
		stopChan: make(chan struct{}),
		running:  false,
	}

	// Create state synchronizer
	ph.stateSynchronizer = NewStateSynchronizer(networkAdapter, logger)

	return ph
}

// Start begins the partition detection and recovery process
func (ph *PartitionHandler) Start() error {
	ph.runLock.Lock()
	defer ph.runLock.Unlock()

	if ph.running {
		return errors.New("partition handler is already running")
	}

	ph.running = true
	ph.stopChan = make(chan struct{})

	// Start background heartbeat monitoring
	go ph.heartbeatLoop()

	// Start background recovery monitoring if enabled
	if ph.config.EnableAutomaticRecovery {
		go ph.recoveryLoop()
	}

	ph.logger.Printf("Partition handler started, heartbeatInterval: %s, suspicionThreshold: %d, confirmationThreshold: %d",
		ph.config.HeartbeatInterval, ph.config.SuspicionThreshold, ph.config.ConfirmationThreshold)

	return nil
}

// Stop halts the partition detection and recovery process
func (ph *PartitionHandler) Stop() error {
	ph.runLock.Lock()
	defer ph.runLock.Unlock()

	if !ph.running {
		return errors.New("partition handler is not running")
	}

	close(ph.stopChan)
	ph.running = false

	ph.logger.Println("Partition handler stopped")
	return nil
}

// heartbeatLoop periodically sends heartbeats to peers and monitors responses
func (ph *PartitionHandler) heartbeatLoop() {
	ticker := time.NewTicker(ph.config.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			ph.sendHeartbeats()
			ph.checkHeartbeats()
			ph.updatePartitionStatus()

		case <-ph.stopChan:
			return
		}
	}
}

// recoveryLoop periodically checks if recovery is needed and initiates it
func (ph *PartitionHandler) recoveryLoop() {
	ticker := time.NewTicker(ph.config.HeartbeatInterval * 2)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			ph.checkRecoveryStatus()

		case <-ph.stopChan:
			return
		}
	}
}

// sendHeartbeats sends heartbeats to all peers
func (ph *PartitionHandler) sendHeartbeats() {
	peers := ph.networkAdapter.GetPeerList()

	for _, peerID := range peers {
		// Get or create peer status
		ph.peerStatusLock.Lock()
		status, exists := ph.peerStatus[peerID]
		if !exists {
			status = &PeerHeartbeatStatus{
				PeerID:       peerID,
				IsResponding: true,
			}
			ph.peerStatus[peerID] = status
		}
		status.LastHeartbeatSent = consensus.ConsensusNow()
		ph.peerStatusLock.Unlock()

		// Send heartbeat asynchronously
		go func(peerID string, s *PeerHeartbeatStatus) {
			ctx, cancel := context.WithTimeout(context.Background(), ph.config.HeartbeatTimeout)
			defer cancel()

			// Create heartbeat message
			heartbeat := NewMessage(MessageTypeHeartbeat, ph.networkAdapter.GetNodeID(), nil)

			// Send heartbeat and wait for response
			peer := ph.networkAdapter.GetPeerByID(peerID)
			if peer == nil {
				ph.logger.Printf("Peer not found, peerID: %s", peerID)
				ph.peerStatusLock.Lock()
				s.MissedHeartbeats++
				s.ConsecutiveSuccess = 0
				s.IsResponding = false
				ph.peerStatusLock.Unlock()
				return
			}
			response, err := ph.networkAdapter.SendMessageWithResponse(peer, heartbeat, ctx)

			ph.peerStatusLock.Lock()
			defer ph.peerStatusLock.Unlock()

			if err != nil {
				// Heartbeat failed
				s.MissedHeartbeats++
				s.ConsecutiveSuccess = 0
				s.IsResponding = false

				ph.logger.Printf("Heartbeat failed, peer: %s, missed: %d, error: %v", peerID, s.MissedHeartbeats, err)
			} else {
				// Heartbeat succeeded
				s.LastHeartbeatRecv = consensus.ConsensusNow()
				s.MissedHeartbeats = 0
				s.ConsecutiveSuccess++
				s.IsResponding = true

				ph.logger.Printf("Heartbeat succeeded, peer: %s, consecutive: %d, response: %v", peerID, s.ConsecutiveSuccess, response)
			}
		}(peerID, status)
	}
}

// checkHeartbeats checks the status of heartbeats for all peers
func (ph *PartitionHandler) checkHeartbeats() {
	ph.peerStatusLock.RLock()
	defer ph.peerStatusLock.RUnlock()

	respondingPeers := 0
	totalPeers := len(ph.peerStatus)

	for peerID, status := range ph.peerStatus {
		if status.IsResponding {
			respondingPeers++
		} else {
			ph.logger.Printf("Peer not responding, peer: %s, missed: %d, lastSent: %s, lastRecv: %s", peerID, status.MissedHeartbeats, status.LastHeartbeatSent, status.LastHeartbeatRecv)
		}
	}

	ph.logger.Printf("Heartbeat status, responding: %d, total: %d, quorumRequired: %d",
		respondingPeers, totalPeers, ph.config.MinimumPeersForQuorum)
}

// updatePartitionStatus updates the current partition status based on heartbeat results
func (ph *PartitionHandler) updatePartitionStatus() {
	ph.peerStatusLock.RLock()
	respondingPeers := 0
	totalPeers := len(ph.peerStatus)

	// Count peers with different missed heartbeat thresholds
	suspectedPeers := 0
	confirmedPeers := 0

	for _, status := range ph.peerStatus {
		if status.IsResponding {
			respondingPeers++
		} else {
			if status.MissedHeartbeats >= ph.config.SuspicionThreshold {
				suspectedPeers++
			}
			if status.MissedHeartbeats >= ph.config.ConfirmationThreshold {
				confirmedPeers++
			}
		}
	}
	ph.peerStatusLock.RUnlock()

	// Update partition status based on responding peers and thresholds
	ph.partitionStatusLock.Lock()
	defer ph.partitionStatusLock.Unlock()

	previousStatus := ph.partitionStatus

	// Determine new status
	if respondingPeers >= ph.config.MinimumPeersForQuorum {
		// We have enough peers for a quorum
		if ph.partitionStatus == PartitionStatusConfirmed ||
			ph.partitionStatus == PartitionStatusRecovering {
			// We were in a partition, now we're recovering
			ph.partitionStatus = PartitionStatusRecovering
		} else {
			// Normal operation
			ph.partitionStatus = PartitionStatusNormal
		}
	} else if totalPeers > 0 && float64(suspectedPeers)/float64(totalPeers) > 0.5 {
		// More than half of peers are suspected of being partitioned
		ph.partitionStatus = PartitionStatusSuspected
	} else if totalPeers > 0 && float64(confirmedPeers)/float64(totalPeers) > 0.5 {
		// More than half of peers are confirmed to be partitioned
		ph.partitionStatus = PartitionStatusConfirmed

		// Record partition start time if this is a new partition
		if previousStatus != PartitionStatusConfirmed {
			ph.partitionStartTime = consensus.ConsensusNow()
			ph.metrics.PartitionsDetected++
			ph.metrics.LastPartitionTime = consensus.ConsensusNow()
			if ph.monitoring != nil {
				// Update partition status as boolean (true = partitioned)
				ph.monitoring.Network.UpdatePartitionStatus(true)
			}

			// Call partition detected callback if set
			if ph.onPartitionDetected != nil {
				go ph.onPartitionDetected()
			}
		}
	}

	// Log status change
	if previousStatus != ph.partitionStatus {
		ph.logger.Printf("Partition status changed, from: %s, to: %s, respondingPeers: %d, totalPeers: %d, suspectedPeers: %d, confirmedPeers: %d",
			previousStatus, ph.partitionStatus, respondingPeers, totalPeers, suspectedPeers, confirmedPeers)

		// Update metrics
		ph.metrics.CurrentStatus = ph.partitionStatus
		if ph.monitoring != nil {
			// Update partition status as boolean (true if not normal)
			ph.monitoring.Network.UpdatePartitionStatus(ph.partitionStatus != PartitionStatusNormal)
			ph.monitoring.Network.UpdatePeers(totalPeers)
		}
	}
}

// checkRecoveryStatus checks if recovery is needed and initiates it
func (ph *PartitionHandler) checkRecoveryStatus() {
	ph.partitionStatusLock.RLock()
	status := ph.partitionStatus
	ph.partitionStatusLock.RUnlock()

	if status == PartitionStatusRecovering {
		// Check if we have enough consecutive successful heartbeats from enough peers
		ph.peerStatusLock.RLock()
		recoveredPeers := 0
		totalPeers := len(ph.peerStatus)

		for _, status := range ph.peerStatus {
			if status.ConsecutiveSuccess >= ph.config.RecoveryThreshold {
				recoveredPeers++
			}
		}
		ph.peerStatusLock.RUnlock()

		// If we have enough recovered peers, initiate state synchronization
		if recoveredPeers >= ph.config.MinimumPeersForQuorum {
			ph.logger.Printf("Initiating recovery from partition, recoveredPeers: %d, totalPeers: %d, threshold: %d",
				recoveredPeers, totalPeers, ph.config.RecoveryThreshold)

			// Initiate recovery process
			go ph.recoverFromPartition()
		}
	}
}

// recoverFromPartition initiates recovery from a network partition
func (ph *PartitionHandler) recoverFromPartition() {
	ph.logger.Println("Starting partition recovery process")

	// 1. Collect state information from peers
	stateInfo, err := ph.collectStateFromPeers()
	if err != nil {
		ph.logger.Printf("Failed to collect state from peers, error: %v", err)
		return
	}

	// 2. Detect conflicts in state
	conflicts, err := ph.detectStateConflicts(stateInfo)
	if err != nil {
		ph.logger.Printf("Failed to detect state conflicts, error: %v", err)
		return
	}

	// 3. If conflicts exist and conflict resolution is enabled, resolve them
	if len(conflicts) > 0 {
		ph.metrics.ConflictsDetected++

		// Call conflict detected callback if set
		if ph.onConflictDetected != nil {
			go ph.onConflictDetected(&ConflictInfo{
				ConflictType:     "generic_conflict",
				ConflictedPeers:  []string{},
				ConflictDetails:  map[string]string{},
				Timestamp:        consensus.ConsensusUnix(),
				Severity:         "high",
				ResolutionNeeded: true,
			})
		}

		if ph.config.EnableConflictResolution {
			resolution, err := ph.resolveStateConflicts(conflicts, stateInfo)
			if err != nil {
				ph.logger.Printf("Failed to resolve state conflicts, error: %v", err)
				return
			}

			ph.metrics.ConflictsResolved++

			// Call conflict resolved callback if set
			if ph.onConflictResolved != nil {
				go ph.onConflictResolved(resolution)
			}
		} else {
			ph.logger.Printf("State conflicts detected but automatic resolution is disabled, conflicts: %v", conflicts)
			return
		}
	}

	// 4. Synchronize state with the network
	err = ph.synchronizeState(stateInfo)
	if err != nil {
		ph.logger.Printf("Failed to synchronize state, error: %v", err)
		return
	}

	// Store the state information for future reference
	ph.recoveryStateLock.Lock()
	ph.recoveryState = stateInfo
	ph.recoveryStateLock.Unlock()

	// 5. Mark recovery as complete
	ph.partitionStatusLock.Lock()

	// Calculate recovery duration
	recoveryDuration := time.Since(ph.partitionStartTime)

	// Update metrics
	ph.metrics.PartitionsRecovered++
	ph.metrics.LastRecoveryTime = consensus.ConsensusNow()
	ph.metrics.TotalPartitionDuration += recoveryDuration

	// Calculate average recovery time
	if ph.metrics.PartitionsRecovered > 0 {
		ph.metrics.AverageRecoveryTime = time.Duration(
			int64(ph.metrics.TotalPartitionDuration) / int64(ph.metrics.PartitionsRecovered),
		)
	}

	// Update status
	ph.partitionStatus = PartitionStatusNormal
	ph.metrics.CurrentStatus = PartitionStatusNormal
	if ph.monitoring != nil {
		// Update partition status as boolean (false = no partition)
		ph.monitoring.Network.UpdatePartitionStatus(false)
	}

	ph.partitionStatusLock.Unlock()

	ph.logger.Printf("Partition recovery completed, duration: %s, conflicts: %d",
		recoveryDuration, len(conflicts))

	// Call recovery callback if set
	if ph.onPartitionRecovered != nil {
		go ph.onPartitionRecovered()
	}
}

// collectStateFromPeers collects state information from all responding peers
func (ph *PartitionHandler) collectStateFromPeers() (*PartitionRecoveryState, error) {
	ph.logger.Println("Collecting state information from peers")

	// Get list of responding peers
	ph.peerStatusLock.RLock()
	var respondingPeers []string

	for peerID, status := range ph.peerStatus {
		if status.IsResponding && status.ConsecutiveSuccess >= ph.config.RecoveryThreshold {
			respondingPeers = append(respondingPeers, peerID)
		}
	}
	ph.peerStatusLock.RUnlock()

	if len(respondingPeers) < ph.config.MinimumPeersForQuorum {
		return nil, fmt.Errorf("not enough responding peers for quorum: %d < %d",
			len(respondingPeers), ph.config.MinimumPeersForQuorum)
	}

	// Create recovery state
	recoveryState := &PartitionRecoveryState{
		Peers:              make(map[string]*PeerState),
		RecoveryTimestamp:  consensus.ConsensusUnix(),
		RecoveryInitiator:  ph.networkAdapter.GetNodeID(),
		PeersParticipating: len(respondingPeers),
		RecoveryStatus:     "collecting_state",
		RecoveryPhase:      "peer_state_collection",
		LastSyncTimestamp:  consensus.ConsensusUnix(),
	}

	// Collect state from each peer
	for _, peerID := range respondingPeers {
		// Create state request message
		stateRequest := NewMessage(MessageTypeStateRequest, ph.networkAdapter.GetNodeID(), &SyncPayload{
			RequestType: "state",
			NodeID:      ph.networkAdapter.GetNodeID(),
			Timestamp:   consensus.ConsensusUnix(),
			Signature:   "state_request_signature", // In real implementation, this would be properly signed
		})

		// Get peer
		peer := ph.networkAdapter.GetPeerByID(peerID)
		if peer == nil {
			ph.logger.Printf("Peer not found, peerID: %s", peerID)
			continue
		}

		// Create context with timeout for this specific request
		func() {
			ctx, cancel := context.WithTimeout(context.Background(), ph.config.HeartbeatTimeout*3)
			defer cancel()

			response, err := ph.networkAdapter.SendMessageWithResponse(peer, stateRequest, ctx)
			if err != nil {
				ph.logger.Printf("Failed to get state from peer, peer: %s, error: %v", peerID, err)
				return
			}

			// Store peer state
			if response.Type == MessageTypeStateResponse {
				var statePayload StatePayload
				if err := json.Unmarshal(response.Payload, &statePayload); err == nil {
					peerState := &PeerState{
						PeerID:          peerID,
						BlockHeight:     statePayload.BlockHeight,
						BlockHash:       statePayload.StateHash,
						StateRoot:       statePayload.StateHash,
						LastBlockTime:   statePayload.Timestamp,
						ValidatorStatus: "active",
						NetworkLatency:  0, // Would be calculated from request/response timing
						IsResponding:    true,
						StateTimestamp:  statePayload.Timestamp,
						Weight:          1.0,
					}

					recoveryState.Peers[peerID] = peerState
					ph.logger.Printf("Received state from peer, peer: %s, blockHeight: %d", peerID, peerState.BlockHeight)
				} else {
					ph.logger.Printf("Invalid state data format from peer, peer: %s", peerID)
				}
			} else {
				ph.logger.Printf("Unexpected response type from peer, peer: %s, type: %s", peerID, response.Type)
			}
		}()
	}

	return recoveryState, nil
}

// detectStateConflicts detects conflicts in state information from peers
func (ph *PartitionHandler) detectStateConflicts(stateInfo *PartitionRecoveryState) ([]*ConflictInfo, error) {
	ph.logger.Println("Detecting state conflicts")

	var conflicts []*ConflictInfo

	if len(stateInfo.Peers) < 2 {
		return conflicts, nil
	}

	// Check for block height conflicts
	blockHeights := make(map[uint64][]string)
	for peerID, peerState := range stateInfo.Peers {
		if peerState.IsResponding {
			blockHeights[peerState.BlockHeight] = append(blockHeights[peerState.BlockHeight], peerID)
		}
	}

	// If we have different block heights, that's a conflict
	if len(blockHeights) > 1 {
		conflictedPeers := make([]string, 0)
		conflictDetails := make(map[string]string)

		for height, peers := range blockHeights {
			for _, peer := range peers {
				conflictedPeers = append(conflictedPeers, peer)
				conflictDetails[peer] = fmt.Sprintf("block_height_%d", height)
			}
		}

		conflict := &ConflictInfo{
			ConflictType:     "block_height_mismatch",
			ConflictedPeers:  conflictedPeers,
			ConflictDetails:  conflictDetails,
			Timestamp:        consensus.ConsensusUnix(),
			Severity:         "high",
			ResolutionNeeded: true,
		}
		conflicts = append(conflicts, conflict)
	}

	// Check for state root conflicts
	stateRoots := make(map[string][]string)
	for peerID, peerState := range stateInfo.Peers {
		if peerState.IsResponding {
			stateRoots[peerState.StateRoot] = append(stateRoots[peerState.StateRoot], peerID)
		}
	}

	if len(stateRoots) > 1 {
		conflictedPeers := make([]string, 0)
		conflictDetails := make(map[string]string)

		for root, peers := range stateRoots {
			for _, peer := range peers {
				conflictedPeers = append(conflictedPeers, peer)
				conflictDetails[peer] = fmt.Sprintf("state_root_%s", root[:8]) // First 8 chars for brevity
			}
		}

		conflict := &ConflictInfo{
			ConflictType:     "state_root_mismatch",
			ConflictedPeers:  conflictedPeers,
			ConflictDetails:  conflictDetails,
			Timestamp:        consensus.ConsensusUnix(),
			Severity:         "high",
			ResolutionNeeded: true,
		}
		conflicts = append(conflicts, conflict)
	}

	ph.logger.Printf("State conflict detection completed, conflicts: %d", len(conflicts))
	return conflicts, nil
}

// resolveStateConflicts resolves conflicts in state information
func (ph *PartitionHandler) resolveStateConflicts(conflicts []*ConflictInfo, stateInfo *PartitionRecoveryState) (*ResolutionInfo, error) {
	ph.logger.Printf("Resolving state conflicts, conflicts: %d", len(conflicts))

	switch ph.config.ConflictResolutionStrategy {
	case "longest_chain":
		return ph.resolveLongestChainConflict(conflicts, stateInfo)
	case "majority_vote":
		return ph.resolveMajorityVoteConflict(conflicts, stateInfo)
	case "weighted_vote":
		return ph.resolveWeightedVoteConflict(conflicts, stateInfo)
	default:
		return ph.resolveLongestChainConflict(conflicts, stateInfo)
	}
}

// resolveLongestChainConflict resolves conflicts using longest chain rule
func (ph *PartitionHandler) resolveLongestChainConflict(conflicts []*ConflictInfo, stateInfo *PartitionRecoveryState) (*ResolutionInfo, error) {
	ph.logger.Println("Resolving conflicts using longest chain rule")

	// Find the peer with the highest block height
	var chosenPeer string
	var chosenState *PeerState
	var highestHeight uint64 = 0

	for peerID, peerState := range stateInfo.Peers {
		if peerState.IsResponding && peerState.BlockHeight > highestHeight {
			highestHeight = peerState.BlockHeight
			chosenPeer = peerID
			chosenState = peerState
		}
	}

	if chosenState == nil {
		return nil, fmt.Errorf("no valid peer state found for conflict resolution")
	}

	// Identify rejected peers
	var rejectedPeers []string
	for peerID, peerState := range stateInfo.Peers {
		if peerID != chosenPeer && peerState.IsResponding {
			rejectedPeers = append(rejectedPeers, peerID)
		}
	}

	resolution := &ResolutionInfo{
		ResolutionType:   "longest_chain",
		ChosenState:      chosenState,
		RejectedPeers:    rejectedPeers,
		ResolutionReason: fmt.Sprintf("Peer %s has highest block height: %d", chosenPeer, highestHeight),
		Timestamp:        consensus.ConsensusUnix(),
		ResolutionMetrics: map[string]string{
			"chosen_peer":    chosenPeer,
			"chosen_height":  fmt.Sprintf("%d", highestHeight),
			"rejected_count": fmt.Sprintf("%d", len(rejectedPeers)),
		},
	}

	ph.logger.Printf("Longest chain conflict resolution completed, chosenPeer: %s, chosenHeight: %d, rejectedPeers: %d",
		chosenPeer, highestHeight, len(rejectedPeers))

	return resolution, nil
}

// resolveMajorityVoteConflict resolves conflicts by majority vote
func (ph *PartitionHandler) resolveMajorityVoteConflict(conflicts []*ConflictInfo, stateInfo *PartitionRecoveryState) (*ResolutionInfo, error) {
	ph.logger.Println("Resolving conflicts using majority vote rule")

	// Count votes for each block hash
	hashVotes := make(map[string]int)
	hashToPeerState := make(map[string]*PeerState)

	// Count how many peers have each block hash
	for _, peerState := range stateInfo.Peers {
		if peerState.IsResponding {
			hashVotes[peerState.BlockHash]++
			// Keep track of one peer state for each hash
			if _, exists := hashToPeerState[peerState.BlockHash]; !exists {
				hashToPeerState[peerState.BlockHash] = peerState
			}
		}
	}

	// Find the hash with the most votes
	var winningHash string
	var winningVotes int
	var chosenState *PeerState

	for hash, votes := range hashVotes {
		if winningHash == "" || votes > winningVotes {
			winningVotes = votes
			winningHash = hash
			chosenState = hashToPeerState[hash]
		}
	}

	if chosenState == nil {
		return nil, fmt.Errorf("no valid peer state found for conflict resolution")
	}

	// Find peers that disagree with the winning hash
	var rejectedPeers []string
	for peerID, peerState := range stateInfo.Peers {
		if peerState.IsResponding && peerState.BlockHash != winningHash {
			rejectedPeers = append(rejectedPeers, peerID)
		}
	}

	resolution := &ResolutionInfo{
		ResolutionType:   "majority_vote",
		ChosenState:      chosenState,
		RejectedPeers:    rejectedPeers,
		ResolutionReason: fmt.Sprintf("Block hash %s has majority with %d votes", winningHash[:8], winningVotes),
		Timestamp:        consensus.ConsensusUnix(),
		ResolutionMetrics: map[string]string{
			"winning_hash":   winningHash,
			"vote_count":     fmt.Sprintf("%d", winningVotes),
			"total_voters":   fmt.Sprintf("%d", len(stateInfo.Peers)),
			"rejected_count": fmt.Sprintf("%d", len(rejectedPeers)),
		},
	}

	ph.logger.Printf("Majority vote conflict resolution completed, winningHash: %s, voteCount: %d, totalVoters: %d",
		winningHash[:8], winningVotes, len(stateInfo.Peers))

	return resolution, nil
}

// resolveWeightedVoteConflict resolves conflicts by weighted voting based on multiple factors
func (ph *PartitionHandler) resolveWeightedVoteConflict(conflicts []*ConflictInfo, stateInfo *PartitionRecoveryState) (*ResolutionInfo, error) {
	ph.logger.Println("Resolving conflicts using weighted vote rule")

	// Calculate weights for each peer based on multiple factors
	peerWeights := make(map[string]float64)

	// Convert PeerState to interface{} map for calculatePeerWeights
	peerStatesMap := make(map[string]interface{})
	for peerID, peerState := range stateInfo.Peers {
		peerStatesMap[peerID] = map[string]interface{}{
			"blockHeight": int(peerState.BlockHeight),
			"stake":       1000000.0, // Default stake value
			"uptime":      1.0,       // Default uptime
		}
		// Start with base weight
		peerWeights[peerID] = 1.0
	}

	// Get calculated weights
	calculatedWeights := ph.calculatePeerWeights(peerStatesMap)

	// Find the state with the highest weighted vote
	hashWeights := make(map[string]float64)
	hashToPeerState := make(map[string]*PeerState)

	for peerID, peerState := range stateInfo.Peers {
		if peerState.IsResponding {
			weight := calculatedWeights[peerID]
			hashWeights[peerState.BlockHash] += weight
			// Keep track of the peer state with highest weight for each hash
			if existing, exists := hashToPeerState[peerState.BlockHash]; !exists || weight > calculatedWeights[existing.PeerID] {
				hashToPeerState[peerState.BlockHash] = peerState
			}
		}
	}

	// Find the hash with the highest weighted score
	var winningHash string
	var maxWeight float64
	var chosenState *PeerState

	for hash, weight := range hashWeights {
		if winningHash == "" || weight > maxWeight {
			maxWeight = weight
			winningHash = hash
			chosenState = hashToPeerState[hash]
		}
	}

	if chosenState == nil {
		return nil, fmt.Errorf("no valid peer state found for weighted conflict resolution")
	}

	// Find peers that disagree with the winning hash
	var rejectedPeers []string
	for peerID, peerState := range stateInfo.Peers {
		if peerState.IsResponding && peerState.BlockHash != winningHash {
			rejectedPeers = append(rejectedPeers, peerID)
		}
	}

	resolution := &ResolutionInfo{
		ResolutionType:   "weighted_vote",
		ChosenState:      chosenState,
		RejectedPeers:    rejectedPeers,
		ResolutionReason: fmt.Sprintf("Block hash %s has highest weighted score: %.4f", winningHash[:8], maxWeight),
		Timestamp:        consensus.ConsensusUnix(),
		ResolutionMetrics: map[string]string{
			"winning_hash":   winningHash,
			"total_weight":   fmt.Sprintf("%.4f", maxWeight),
			"rejected_count": fmt.Sprintf("%d", len(rejectedPeers)),
		},
	}

	ph.logger.Printf("Weighted vote conflict resolution completed, winningHash: %s, totalWeight: %.4f, rejectedPeers: %d",
		winningHash[:8], maxWeight, len(rejectedPeers))

	return resolution, nil
}

// calculatePeerWeights calculates weights for each peer based on multiple factors
func (ph *PartitionHandler) calculatePeerWeights(peerStates map[string]interface{}) map[string]float64 {
	weights := make(map[string]float64)

	for peerID, stateData := range peerStates {
		weight := 1.0 // Base weight

		if state, ok := stateData.(map[string]interface{}); ok {
			// Factor 1: Stake (if available)
			if stake, ok := state["stake"].(float64); ok && stake > 0 {
				// Normalize stake to a reasonable weight factor (e.g., log scale)
				weight *= 1.0 + math.Log10(stake/1000000) // Assuming stake in millions
			}

			// Factor 2: Uptime/Reliability
			if uptime, ok := state["uptime"].(float64); ok {
				// Higher uptime gives more weight (0-1 scale)
				weight *= 0.5 + (uptime * 0.5)
			}

			// Factor 3: Historical performance (based on consecutive successful heartbeats)
			ph.peerStatusLock.RLock()
			if peerStatus, exists := ph.peerStatus[peerID]; exists {
				// More consecutive successes = higher weight
				reliability := float64(peerStatus.ConsecutiveSuccess) / float64(ph.config.RecoveryThreshold*2)
				if reliability > 1.0 {
					reliability = 1.0
				}
				weight *= 0.8 + (reliability * 0.2)
			}
			ph.peerStatusLock.RUnlock()

			// Factor 4: Block height (prefer peers with more recent data)
			if height, ok := state["blockHeight"].(int); ok {
				// Find max height across all peers
				maxHeight := 0
				for _, peerState := range peerStates {
					if s, ok := peerState.(map[string]interface{}); ok {
						if h, ok := s["blockHeight"].(int); ok && h > maxHeight {
							maxHeight = h
						}
					}
				}

				// Penalize peers that are too far behind
				if maxHeight > 0 && height > 0 {
					heightRatio := float64(height) / float64(maxHeight)
					if heightRatio < 0.95 { // More than 5% behind
						weight *= heightRatio
					}
				}
			}

			// Factor 5: Geographic distribution (if available)
			if region, ok := state["region"].(string); ok && region != "" {
				// This would normally check against a preferred distribution
				// For now, just give a small bonus for having region info
				weight *= 1.1
			}
		}

		weights[peerID] = weight
		ph.logger.Printf("Calculated peer weight, peer: %s, weight: %f", peerID, weight)
	}

	// Normalize weights so they sum to 1.0
	totalWeight := 0.0
	for _, w := range weights {
		totalWeight += w
	}

	if totalWeight > 0 {
		for peerID := range weights {
			weights[peerID] /= totalWeight
		}
	}

	return weights
}

// synchronizeState synchronizes the local state with the network
func (ph *PartitionHandler) synchronizeState(stateInfo *PartitionRecoveryState) error {
	// Validate stateInfo parameter
	if stateInfo == nil {
		return fmt.Errorf("stateInfo parameter is nil")
	}

	ph.logger.Printf("Synchronizing state with network, stateInfoPeers: %d", len(stateInfo.Peers))

	// Convert PartitionRecoveryState to map for StateSynchronizer
	stateMap := make(map[string]interface{})

	// Convert peers to map format
	peersMap := make(map[string]interface{})
	for peerID, peerState := range stateInfo.Peers {
		peersMap[peerID] = map[string]interface{}{
			"peer_id":          peerState.PeerID,
			"block_height":     peerState.BlockHeight,
			"block_hash":       peerState.BlockHash,
			"state_root":       peerState.StateRoot,
			"last_block_time":  peerState.LastBlockTime,
			"validator_status": peerState.ValidatorStatus,
			"network_latency":  peerState.NetworkLatency,
			"is_responding":    peerState.IsResponding,
			"state_timestamp":  peerState.StateTimestamp,
			"weight":           peerState.Weight,
		}
	}

	stateMap["peers"] = peersMap
	stateMap["recovery_timestamp"] = stateInfo.RecoveryTimestamp
	stateMap["recovery_initiator"] = stateInfo.RecoveryInitiator
	stateMap["recovery_duration_ms"] = stateInfo.RecoveryDuration
	stateMap["peers_participating"] = stateInfo.PeersParticipating
	stateMap["conflicts_detected"] = stateInfo.ConflictsDetected
	stateMap["conflicts_resolved"] = stateInfo.ConflictsResolved
	stateMap["recovery_status"] = stateInfo.RecoveryStatus
	stateMap["recovery_phase"] = stateInfo.RecoveryPhase
	stateMap["last_sync_timestamp"] = stateInfo.LastSyncTimestamp

	// Include consensus state if available
	if stateInfo.ConsensusState != nil {
		stateMap["consensus_state"] = stateInfo.ConsensusState
	}

	// Use the state synchronizer to perform synchronization
	if err := ph.stateSynchronizer.SynchronizeState(stateMap); err != nil {
		return fmt.Errorf("state synchronization failed: %w", err)
	}

	ph.logger.Println("State synchronization completed")
	return nil
}

// GetPartitionStatus returns the current partition status
func (ph *PartitionHandler) GetPartitionStatus() PartitionStatus {
	ph.partitionStatusLock.RLock()
	defer ph.partitionStatusLock.RUnlock()

	return ph.partitionStatus
}

// GetPartitionMetrics returns the current partition metrics
func (ph *PartitionHandler) GetPartitionMetrics() PartitionMetrics {
	ph.partitionStatusLock.RLock()
	defer ph.partitionStatusLock.RUnlock()

	// Return a copy to avoid race conditions
	return PartitionMetrics{
		PartitionsDetected:     ph.metrics.PartitionsDetected,
		PartitionsRecovered:    ph.metrics.PartitionsRecovered,
		ConflictsDetected:      ph.metrics.ConflictsDetected,
		ConflictsResolved:      ph.metrics.ConflictsResolved,
		AverageRecoveryTime:    ph.metrics.AverageRecoveryTime,
		LastPartitionTime:      ph.metrics.LastPartitionTime,
		LastRecoveryTime:       ph.metrics.LastRecoveryTime,
		TotalPartitionDuration: ph.metrics.TotalPartitionDuration,
		CurrentStatus:          ph.metrics.CurrentStatus,
	}
}

// GetPartitionMetricsMap returns the current partition metrics as a map for the metrics system
func (ph *PartitionHandler) GetPartitionMetricsMap() map[string]interface{} {
	ph.partitionStatusLock.RLock()
	defer ph.partitionStatusLock.RUnlock()

	// Count peers in different states
	ph.peerStatusLock.RLock()
	respondingPeers := 0
	suspectedPeers := 0
	confirmedPeers := 0

	for _, status := range ph.peerStatus {
		if status.IsResponding {
			respondingPeers++
		} else {
			if status.MissedHeartbeats >= ph.config.SuspicionThreshold {
				suspectedPeers++
			}
			if status.MissedHeartbeats >= ph.config.ConfirmationThreshold {
				confirmedPeers++
			}
		}
	}
	ph.peerStatusLock.RUnlock()

	// Calculate current partition duration if active
	var currentDuration time.Duration
	if ph.partitionStatus == PartitionStatusConfirmed || ph.partitionStatus == PartitionStatusRecovering {
		if !ph.partitionStartTime.IsZero() {
			currentDuration = time.Since(ph.partitionStartTime)
		}
	}

	// Calculate max partition duration
	maxDuration := ph.metrics.TotalPartitionDuration
	if ph.metrics.PartitionsRecovered > 0 && maxDuration > 0 {
		maxDuration = ph.metrics.TotalPartitionDuration / time.Duration(ph.metrics.PartitionsRecovered)
	}

	// Return metrics as a map
	return map[string]interface{}{
		"partitionsDetected":       ph.metrics.PartitionsDetected,
		"partitionsRecovered":      ph.metrics.PartitionsRecovered,
		"conflictsDetected":        ph.metrics.ConflictsDetected,
		"conflictsResolved":        ph.metrics.ConflictsResolved,
		"averagePartitionDuration": ph.metrics.AverageRecoveryTime,
		"currentPartitionDuration": currentDuration,
		"maxPartitionDuration":     maxDuration,
		"lastPartitionTime":        ph.metrics.LastPartitionTime,
		"lastRecoveryTime":         ph.metrics.LastRecoveryTime,
		"totalPartitionDuration":   ph.metrics.TotalPartitionDuration,
		"recoveryAttempts":         ph.metrics.PartitionsDetected, // Use as proxy for attempts
		"recoverySuccesses":        ph.metrics.PartitionsRecovered,
		"recoveryFailures":         ph.metrics.PartitionsDetected - ph.metrics.PartitionsRecovered,
		"averageRecoveryTime":      ph.metrics.AverageRecoveryTime,
		"respondingPeers":          respondingPeers,
		"suspectedPeers":           suspectedPeers,
		"confirmedPeers":           confirmedPeers,
	}
}

// GetPartitionStatusString returns the current partition status as a string
func (ph *PartitionHandler) GetPartitionStatusString() string {
	ph.partitionStatusLock.RLock()
	defer ph.partitionStatusLock.RUnlock()

	return ph.partitionStatus.String()
}

// SetPartitionDetectedCallback sets the callback for when a partition is detected
func (ph *PartitionHandler) SetPartitionDetectedCallback(callback func()) {
	ph.onPartitionDetected = callback
}

// SetPartitionRecoveredCallback sets the callback for when a partition is recovered
func (ph *PartitionHandler) SetPartitionRecoveredCallback(callback func()) {
	ph.onPartitionRecovered = callback
}
