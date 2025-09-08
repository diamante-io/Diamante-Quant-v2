package network

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"diamante/consensus"
)

// StateSynchronizer handles state synchronization during network partition recovery
type StateSynchronizer struct {
	networkAdapter NetworkAdapter
	logger         *log.Logger
	mu             sync.RWMutex

	// State validation function provided by consensus
	validateStateFn func(state map[string]interface{}) error

	// State apply function provided by consensus
	applyStateFn func(state map[string]interface{}) error
}

// NewStateSynchronizer creates a new state synchronizer
func NewStateSynchronizer(networkAdapter NetworkAdapter, logger *log.Logger) *StateSynchronizer {
	if logger == nil {
		logger = log.New(log.Writer(), "[StateSynchronizer] ", log.LstdFlags)
	}

	return &StateSynchronizer{
		networkAdapter: networkAdapter,
		logger:         logger,
	}
}

// SetValidateStateFunction sets the function used to validate incoming state
func (ss *StateSynchronizer) SetValidateStateFunction(fn func(state map[string]interface{}) error) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	ss.validateStateFn = fn
}

// SetApplyStateFunction sets the function used to apply synchronized state
func (ss *StateSynchronizer) SetApplyStateFunction(fn func(state map[string]interface{}) error) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	ss.applyStateFn = fn
}

// SynchronizeState performs state synchronization with the network
func (ss *StateSynchronizer) SynchronizeState(stateInfo map[string]interface{}) error {
	ss.logger.Println("Starting state synchronization")

	// Extract peer states
	peerStatesRaw, ok := stateInfo["peers"]
	if !ok {
		return fmt.Errorf("no peer states found in state info")
	}

	peerStates, ok := peerStatesRaw.(map[string]interface{})
	if !ok || len(peerStates) == 0 {
		return fmt.Errorf("invalid or empty peer states")
	}

	// Determine the canonical state using the resolution info
	canonicalState, err := ss.determineCanonicalState(stateInfo)
	if err != nil {
		return fmt.Errorf("failed to determine canonical state: %w", err)
	}

	// Validate the canonical state
	if err := ss.validateState(canonicalState); err != nil {
		return fmt.Errorf("canonical state validation failed: %w", err)
	}

	// Apply the canonical state
	if err := ss.applyState(canonicalState); err != nil {
		return fmt.Errorf("failed to apply canonical state: %w", err)
	}

	// Broadcast state synchronization completion to peers
	if err := ss.broadcastSyncCompletion(canonicalState); err != nil {
		ss.logger.Println("Failed to broadcast sync completion", "error", err)
	}

	ss.logger.Println("State synchronization completed successfully")
	return nil
}

// determineCanonicalState determines the canonical state based on resolution info
func (ss *StateSynchronizer) determineCanonicalState(stateInfo map[string]interface{}) (map[string]interface{}, error) {
	// Check if we have resolution info from conflict resolution
	if resolution, ok := stateInfo["resolution"]; ok {
		if resolutionMap, ok := resolution.(map[string]interface{}); ok {
			return ss.extractStateFromResolution(resolutionMap, stateInfo)
		}
	}

	// If no resolution info, try to find the most agreed-upon state
	return ss.findMajorityState(stateInfo)
}

// extractStateFromResolution extracts the canonical state from resolution info
func (ss *StateSynchronizer) extractStateFromResolution(resolution, stateInfo map[string]interface{}) (map[string]interface{}, error) {
	strategy, _ := resolution["strategy"].(string)

	switch strategy {
	case "longest-chain":
		selectedPeer, ok := resolution["selectedPeer"].(string)
		if !ok {
			return nil, fmt.Errorf("no selected peer in longest-chain resolution")
		}

		peerStates, _ := stateInfo["peers"].(map[string]interface{})
		if selectedState, ok := peerStates[selectedPeer]; ok {
			if state, ok := selectedState.(map[string]interface{}); ok {
				return state, nil
			}
		}
		return nil, fmt.Errorf("selected peer state not found")

	case "majority-vote":
		selectedHash, ok := resolution["selectedHash"].(string)
		if !ok {
			return nil, fmt.Errorf("no selected hash in majority-vote resolution")
		}

		// Find a peer with this hash
		peerStates, _ := stateInfo["peers"].(map[string]interface{})
		for _, peerState := range peerStates {
			if state, ok := peerState.(map[string]interface{}); ok {
				if hash, _ := state["latestBlockHash"].(string); hash == selectedHash {
					return state, nil
				}
			}
		}
		return nil, fmt.Errorf("no peer found with selected hash")

	default:
		return nil, fmt.Errorf("unknown resolution strategy: %s", strategy)
	}
}

// findMajorityState finds the state that the majority of peers agree on
func (ss *StateSynchronizer) findMajorityState(stateInfo map[string]interface{}) (map[string]interface{}, error) {
	peerStates, _ := stateInfo["peers"].(map[string]interface{})

	// Count occurrences of each state hash
	stateHashes := make(map[string]int)
	stateByHash := make(map[string]map[string]interface{})

	for _, peerState := range peerStates {
		if state, ok := peerState.(map[string]interface{}); ok {
			hash, _ := state["latestBlockHash"].(string)
			if hash != "" {
				stateHashes[hash]++
				stateByHash[hash] = state
			}
		}
	}

	// Find the hash with the most votes
	var majorityHash string
	var maxVotes int

	for hash, votes := range stateHashes {
		if votes > maxVotes {
			maxVotes = votes
			majorityHash = hash
		}
	}

	if majorityHash == "" {
		return nil, fmt.Errorf("no majority state found")
	}

	return stateByHash[majorityHash], nil
}

// validateState validates the state before applying it
func (ss *StateSynchronizer) validateState(state map[string]interface{}) error {
	ss.mu.RLock()
	validateFn := ss.validateStateFn
	ss.mu.RUnlock()

	if validateFn == nil {
		// Basic validation if no custom function provided
		if _, ok := state["blockHeight"]; !ok {
			return fmt.Errorf("missing blockHeight in state")
		}
		if _, ok := state["latestBlockHash"]; !ok {
			return fmt.Errorf("missing latestBlockHash in state")
		}
		return nil
	}

	return validateFn(state)
}

// applyState applies the synchronized state
func (ss *StateSynchronizer) applyState(state map[string]interface{}) error {
	ss.mu.RLock()
	applyFn := ss.applyStateFn
	ss.mu.RUnlock()

	if applyFn == nil {
		// Log the state if no apply function is provided
		ss.logger.Println("Would apply state", "state", state)
		return nil
	}

	return applyFn(state)
}

// broadcastSyncCompletion broadcasts sync completion to peers
func (ss *StateSynchronizer) broadcastSyncCompletion(state map[string]interface{}) error {
	// Create a proper sync payload
	blockHeight, _ := state["blockHeight"].(uint64)

	payload := &SyncPayload{
		RequestType: "sync_complete",
		FromHeight:  blockHeight,
		ToHeight:    blockHeight,
		NodeID:      ss.networkAdapter.GetNodeID(),
		Timestamp:   consensus.ConsensusUnix(),
		Signature:   "sync_complete_signature", // In real implementation, this would be properly signed
	}

	message := NewMessage("SyncComplete", ss.networkAdapter.GetNodeID(), payload)

	return ss.networkAdapter.BroadcastMessage(message)
}

// RequestStateFromPeer requests state information from a specific peer
func (ss *StateSynchronizer) RequestStateFromPeer(peerID string, timeout time.Duration) (map[string]interface{}, error) {
	peer := ss.networkAdapter.GetPeerByID(peerID)
	if peer == nil {
		return nil, fmt.Errorf("peer not found: %s", peerID)
	}

	// Create state request with proper payload
	requestPayload := &SyncPayload{
		RequestType: "state",
		NodeID:      ss.networkAdapter.GetNodeID(),
		Timestamp:   consensus.ConsensusUnix(),
		Signature:   "state_request_signature", // In real implementation, this would be properly signed
	}
	request := NewMessage(MessageTypeStateRequest, ss.networkAdapter.GetNodeID(), requestPayload)

	// Create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// Send request and wait for response
	response, err := ss.networkAdapter.SendMessageWithResponse(peer, request, ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get state from peer %s: %w", peerID, err)
	}

	// Extract state from response
	if response.Type != MessageTypeStateResponse {
		return nil, fmt.Errorf("unexpected response type: %s", response.Type)
	}

	// Handle StatePayload response
	var statePayload StatePayload
	if err := json.Unmarshal(response.Payload, &statePayload); err == nil {
		// Convert StateData to map[string]interface{}
		state := make(map[string]interface{})
		state["blockHeight"] = statePayload.BlockHeight
		state["latestBlockHash"] = statePayload.StateHash
		state["timestamp"] = statePayload.Timestamp

		// Add state data
		for k, v := range statePayload.StateData {
			state[k] = v
		}

		return state, nil
	}

	return nil, fmt.Errorf("invalid state format in response")
}

// VerifyStateConsistency verifies state consistency across peers
func (ss *StateSynchronizer) VerifyStateConsistency(threshold float64) (bool, map[string]interface{}, error) {
	peers := ss.networkAdapter.GetPeerList()
	if len(peers) == 0 {
		return false, nil, fmt.Errorf("no peers available")
	}

	// Collect states from all peers
	states := make(map[string]map[string]interface{})
	var wg sync.WaitGroup
	var mu sync.Mutex

	for _, peerID := range peers {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()

			state, err := ss.RequestStateFromPeer(id, 5*time.Second)
			if err != nil {
				ss.logger.Println("Failed to get state from peer", "peer", id, "error", err)
				return
			}

			mu.Lock()
			states[id] = state
			mu.Unlock()
		}(peerID)
	}

	wg.Wait()

	// Check consistency
	if len(states) == 0 {
		return false, nil, fmt.Errorf("no states collected from peers")
	}

	// Count occurrences of each state hash
	hashCounts := make(map[string]int)
	stateByHash := make(map[string]map[string]interface{})

	for _, state := range states {
		if hash, ok := state["latestBlockHash"].(string); ok && hash != "" {
			hashCounts[hash]++
			stateByHash[hash] = state
		}
	}

	// Find the most common state
	var mostCommonHash string
	var maxCount int

	for hash, count := range hashCounts {
		if count > maxCount {
			maxCount = count
			mostCommonHash = hash
		}
	}

	// Calculate consistency percentage
	consistencyRatio := float64(maxCount) / float64(len(states))
	isConsistent := consistencyRatio >= threshold

	var consistentState map[string]interface{}
	if mostCommonHash != "" {
		consistentState = stateByHash[mostCommonHash]
	}

	ss.logger.Println("State consistency check",
		"consistent", isConsistent,
		"ratio", consistencyRatio,
		"threshold", threshold,
		"total_peers", len(peers),
		"responding_peers", len(states))

	return isConsistent, consistentState, nil
}
