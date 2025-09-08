// consensus/diamantefinality/voting.go

package diamantefinality

import (
	"diamante/consensus/types"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"sort"
	"sync"
)

var (
	// ErrInvalidThreshold indicates an invalid voting threshold
	ErrInvalidThreshold = errors.New("invalid voting threshold: must be between 0 and 100")

	// ErrNilDAG indicates the DAG reference is nil
	ErrNilDAG = errors.New("nil DAG reference in VirtualVoting")

	// ErrNilEvent indicates a nil event was passed for voting
	ErrNilEvent = errors.New("nil event passed to Vote method")
)

// VirtualVoting implements a simplified voting mechanism for finality checks.
// Each validator "sees" an event if it can trace a path in the DAG from the event
// up to an event created by that validator. If enough stake "has seen" the event,
// it is considered voted for finality.
type VirtualVoting struct {
	dag       *DAG   // Reference to the DAG
	threshold uint32 // Percentage of total stake required for a successful vote (0-100)
	mu        sync.RWMutex
	logger    *log.Logger // Logger for structured logging
}

// NewVirtualVoting creates a new VirtualVoting instance for a given DAG,
// with a default threshold of 66% stake required for finality.
func NewVirtualVoting(dag *DAG) *VirtualVoting {
	// Create default logger if needed
	logger := log.New(os.Stdout, "VirtualVoting: ", log.Ldate|log.Ltime|log.Lshortfile)

	return &VirtualVoting{
		dag:       dag,
		threshold: 66, // 66% as integer percentage
		logger:    logger,
	}
}

// SetLogger sets a custom logger for the VirtualVoting instance
func (v *VirtualVoting) SetLogger(logger *log.Logger) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.logger = logger
}

// SetThreshold updates the percentage of total stake needed for an event to pass the vote.
// Returns an error if threshold is invalid (outside 0-100 range).
func (v *VirtualVoting) SetThreshold(threshold float64) {
	v.mu.Lock()
	defer v.mu.Unlock()

	// Add validation but handle error internally
	if threshold <= 0 || threshold > 100 {
		// Log the error instead of returning it
		if v.logger != nil {
			v.logger.Printf("Invalid threshold value: %v (must be between 0 and 100)", threshold)
		}
		// Use a default value or keep current value
		return
	}

	v.threshold = uint32(threshold)
	if v.logger != nil {
		v.logger.Printf("Threshold updated to %v%%", threshold)
	}
}

// GetThreshold returns the current voting threshold
func (v *VirtualVoting) GetThreshold() uint32 {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.threshold
}

// Vote determines whether the event has enough stake support to be considered finalized.
// Enhanced with better Byzantine fault detection.
func (v *VirtualVoting) Vote(event *types.Event) bool {
	// Safety checks
	if event == nil {
		// Log error and return false for nil event
		if v.logger != nil {
			v.logger.Printf("Error: Attempted to vote on nil event")
		}
		return false
	}

	if v.dag == nil {
		// Log error and return false for nil DAG
		if v.logger != nil {
			v.logger.Printf("Error: Cannot vote with nil DAG reference")
		}
		return false
	}

	v.mu.RLock()
	threshold := v.threshold
	v.mu.RUnlock()

	// Get total stake and compute required threshold
	totalStake := v.dag.GetTotalStake()
	if totalStake == 0 {
		// No stake in the system, can't reach any threshold
		if v.logger != nil {
			v.logger.Printf("Warning: No total stake in the system, vote fails")
		}
		return false
	}

	// Use integer arithmetic: requiredStake = (totalStake * threshold) / 100
	requiredStake := (totalStake * uint64(threshold)) / 100
	accumulatedStake := uint64(0)

	// Performance optimization: if threshold is 0, vote always passes
	if threshold == 0 {
		if v.logger != nil {
			v.logger.Printf("Vote always passes: threshold is %v", threshold)
		}
		return true
	}

	// For each active node, check if that node has "seen" the event
	activeNodes := v.dag.GetActiveNodes()
	if len(activeNodes) == 0 {
		// No active nodes, can't reach any threshold
		if v.logger != nil {
			v.logger.Printf("Warning: No active nodes in the DAG, vote fails")
		}
		return false
	}

	// Track nodes and stakes to detect potential Byzantine behavior
	nodeVotes := make(map[[32]byte]bool)
	nodeStakes := make(map[[32]byte]uint64)

	// First check the creator's own stake (optimization)
	creatorStake, err := v.dag.GetNodeStake(event.Creator)
	if err == nil {
		accumulatedStake += creatorStake
		nodeVotes[event.Creator] = true
		nodeStakes[event.Creator] = creatorStake

		if accumulatedStake >= requiredStake {
			if v.logger != nil {
				v.logger.Printf("Event %x voted final: creator's stake %v reached required %v",
					event.ID, accumulatedStake, requiredStake)
			}
			return true
		}
	}

	for _, nodeID := range activeNodes {
		// Skip the creator - we already counted them
		if nodeID == event.Creator {
			continue
		}

		hasSeen := v.HasSeen(nodeID, event)
		nodeVotes[nodeID] = hasSeen

		if hasSeen {
			stake, err := v.dag.GetNodeStake(nodeID)
			if err != nil {
				// Node might have been removed or an error occurred;
				// skip this node's stake.
				if v.logger != nil {
					v.logger.Printf("Error getting stake for node %x: %v", nodeID, err)
				}
				continue
			}

			nodeStakes[nodeID] = stake
			accumulatedStake += stake

			if accumulatedStake >= requiredStake {
				// Before returning success, check for suspicious voting patterns
				if v.detectSuspiciousVoting(event, nodeVotes, nodeStakes) {
					v.logger.Printf("Warning: Suspicious voting pattern detected for event %x",
						event.ID)
					// Still allow the vote to pass, but log the suspicion
				}

				if v.logger != nil {
					v.logger.Printf("Event %x voted final: accumulated stake %v reached required %v",
						event.ID, accumulatedStake, requiredStake)
				}
				return true
			}
		}
	}

	// Did not reach required stake
	if v.logger != nil {
		v.logger.Printf("Event %x not finalized: accumulated stake %v below required %v",
			event.ID, accumulatedStake, requiredStake)
	}
	return false
}

// New method to detect suspicious voting patterns that might indicate Byzantine behavior
func (v *VirtualVoting) detectSuspiciousVoting(event *types.Event, votes map[[32]byte]bool, stakes map[[32]byte]uint64) bool {
	if len(votes) < 4 {
		// Not enough votes to detect patterns
		return false
	}

	// Check for unusual voting patterns where validators with similar stake amounts
	// vote the same way (potential collusion)
	sameVoteSamples := make(map[bool]uint64)
	for nodeID, vote := range votes {
		stake := stakes[nodeID]
		sameVoteSamples[vote] += stake
	}

	// If all votes are the same, that's suspicious with many validators
	if len(sameVoteSamples) == 1 && len(votes) > 10 {
		return true
	}

	// Check for stake distribution anomalies
	// In a healthy network, we expect some distribution of stake
	stakeValues := make([]uint64, 0, len(stakes))
	for _, stake := range stakes {
		stakeValues = append(stakeValues, stake)
	}

	// Sort the stake values
	sort.Slice(stakeValues, func(i, j int) bool {
		return stakeValues[i] < stakeValues[j]
	})

	// Calculate the Gini coefficient to measure stake distribution inequality
	// High inequality might suggest centralization or Sybil attacks
	gini := calculateGiniCoefficient(stakeValues)

	// A very high Gini coefficient (close to 1000) suggests high stake concentration
	// Using fixed point: 0.9 = 900/1000
	return gini > 900
}

// Calculate Gini coefficient to measure inequality in stake distribution
// Returns fixed-point result scaled by 1000 (0.9 = 900)
func calculateGiniCoefficient(values []uint64) uint64 {
	n := len(values)
	if n <= 1 {
		return 0
	}

	// Sum of absolute differences
	var sumAbsDiff uint64 = 0
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			if values[i] > values[j] {
				sumAbsDiff += values[i] - values[j]
			} else {
				sumAbsDiff += values[j] - values[i]
			}
		}
	}

	// Mean value
	var sum uint64 = 0
	for _, v := range values {
		sum += v
	}

	// Calculate Gini coefficient using fixed-point arithmetic
	// Gini = sumAbsDiff / (2 * n * n * mean)
	// Scale by 1000 to get fixed-point result
	if sum == 0 {
		return 0
	}

	denominator := 2 * uint64(n*n) * sum / uint64(n) // 2 * n * n * mean
	if denominator == 0 {
		return 0
	}

	return (sumAbsDiff * 1000) / denominator
}

// HasSeen checks if validatorID has created an ancestor event of 'event' in the DAG.
// We do this by traversing upward (parents) from 'event' and seeing if we find an event
// with Creator == validatorID.
func (v *VirtualVoting) HasSeen(validatorID [32]byte, event *types.Event) bool {
	// Safety checks
	if event == nil {
		return false
	}

	// Optimization: if validatorID is the creator of this event, return true immediately
	if event.Creator == validatorID {
		return true
	}

	v.mu.RLock()
	defer v.mu.RUnlock()

	seen := make(map[[32]byte]bool)
	queue := []*types.Event{event}

	for len(queue) > 0 {
		e := queue[0]
		queue = queue[1:]

		// If we've already visited this event, skip
		if seen[e.ID] {
			continue
		}
		seen[e.ID] = true

		// If the validator created this event, we conclude "HasSeen" is true
		if e.Creator == validatorID {
			return true
		}

		// Move to the parents
		for _, parentID := range e.ParentIDs {
			parent, err := v.dag.GetEvent(parentID)
			if err != nil {
				// Missing parent, skip
				if v.logger != nil {
					v.logger.Printf("Missing parent %x for event %x: %v", parentID, e.ID, err)
				}
				continue
			}
			queue = append(queue, parent)
		}
	}

	return false
}

// RecalculateWeights is a no-op here; we do a direct stake check in Vote().
func (v *VirtualVoting) RecalculateWeights() {
	// No-op implementation, but we can log it for debugging
	if v.logger != nil {
		v.logger.Printf("RecalculateWeights called (no-op)")
	}
}

// GetConsensusEvents finds all events at a given height for which Vote(event) is true.
func (v *VirtualVoting) GetConsensusEvents(height uint64) []*types.Event {
	// Safety check
	if v.dag == nil {
		if v.logger != nil {
			v.logger.Printf("Error: Cannot get consensus events with nil DAG reference")
		}
		return nil
	}

	var consensusEvents []*types.Event
	allEvents := v.dag.GetEvents()

	// Log the process start
	if v.logger != nil {
		v.logger.Printf("Finding consensus events at height %d from %d total events",
			height, len(allEvents))
	}

	for _, ev := range allEvents {
		if ev.Height == height && v.Vote(ev) {
			consensusEvents = append(consensusEvents, ev)
		}
	}

	// Log the results
	if v.logger != nil {
		v.logger.Printf("Found %d consensus events at height %d", len(consensusEvents), height)
	}

	return consensusEvents
}

// GetState serializes the current threshold (uint32) into JSON.
func (v *VirtualVoting) GetState() ([]byte, error) {
	v.mu.RLock()
	defer v.mu.RUnlock()

	state := struct {
		Threshold uint32 `json:"threshold"`
	}{
		Threshold: v.threshold,
	}

	data, err := json.Marshal(state)
	if err != nil {
		if v.logger != nil {
			v.logger.Printf("Error serializing state: %v", err)
		}
		return nil, fmt.Errorf("failed to marshal voting state: %w", err)
	}

	return data, nil
}

// RestoreState loads a threshold uint32 from JSON into this VirtualVoting instance.
func (v *VirtualVoting) RestoreState(stateData []byte) error {
	// Safety check
	if len(stateData) == 0 {
		return errors.New("empty state data provided")
	}

	var state struct {
		Threshold uint32 `json:"threshold"`
	}

	if err := json.Unmarshal(stateData, &state); err != nil {
		if v.logger != nil {
			v.logger.Printf("Error deserializing state: %v", err)
		}
		return fmt.Errorf("failed to unmarshal voting state: %w", err)
	}

	// Validate threshold
	if state.Threshold == 0 || state.Threshold > 100 {
		return ErrInvalidThreshold
	}

	v.mu.Lock()
	defer v.mu.Unlock()
	v.threshold = state.Threshold

	if v.logger != nil {
		v.logger.Printf("Threshold restored to %v%%", state.Threshold)
	}

	return nil
}

// Validate performs a validation check on the VirtualVoting instance
func (v *VirtualVoting) Validate() error {
	if v.dag == nil {
		return ErrNilDAG
	}

	v.mu.RLock()
	defer v.mu.RUnlock()

	if v.threshold == 0 || v.threshold > 100 {
		return ErrInvalidThreshold
	}

	return nil
}
