// Package types provides state synchronization type definitions
package types

import (
	"time"

	"diamante/common"
)

// StateSyncFunctions defines the interface for state synchronization functions
type StateSyncFunctions struct {
	ValidateStateFn func(state *SyncStateInfo) error
	ApplyStateFn    func(state *SyncStateInfo) error
}

// StateInfoConverter provides conversion functions between old and new state formats
type StateInfoConverter struct{}

// ConvertFromLegacyState converts legacy map[string]interface{} to typed SyncStateInfo
func (sic *StateInfoConverter) ConvertFromLegacyState(legacy map[string]interface{}) *SyncStateInfo {
	state := &SyncStateInfo{
		Attributes: make(map[string]*Value),
	}

	if blockHeight, ok := legacy["blockHeight"].(uint64); ok {
		state.BlockHeight = blockHeight
	}

	if latestBlockHash, ok := legacy["latestBlockHash"].(string); ok {
		state.LatestBlockHash = latestBlockHash
	}

	if stateRoot, ok := legacy["stateRoot"].(string); ok {
		state.StateRoot = stateRoot
	}

	if nodeID, ok := legacy["nodeID"].(string); ok {
		state.NodeID = nodeID
	}

	if version, ok := legacy["version"].(string); ok {
		state.Version = version
	}

	if networkStatus, ok := legacy["networkStatus"].(string); ok {
		state.NetworkStatus = networkStatus
	}

	if peerCount, ok := legacy["peerCount"].(int); ok {
		state.PeerCount = peerCount
	}

	if validatorSet, ok := legacy["validatorSet"].(string); ok {
		state.ValidatorSet = validatorSet
	}

	// Handle timestamp conversion
	if timestamp, ok := legacy["timestamp"].(string); ok {
		if t, err := time.Parse(time.RFC3339, timestamp); err == nil {
			state.Timestamp = t
		}
	}

	// Convert other arbitrary fields to attributes
	for key, value := range legacy {
		if key != "blockHeight" && key != "latestBlockHash" && key != "stateRoot" &&
			key != "nodeID" && key != "version" && key != "networkStatus" &&
			key != "peerCount" && key != "validatorSet" && key != "timestamp" {
			state.Attributes[key] = InterfaceToValue(value)
		}
	}

	return state
}

// ConvertToLegacyState converts typed SyncStateInfo to legacy map[string]interface{}
func (sic *StateInfoConverter) ConvertToLegacyState(state *SyncStateInfo) map[string]interface{} {
	legacy := make(map[string]interface{})

	legacy["blockHeight"] = state.BlockHeight
	legacy["latestBlockHash"] = state.LatestBlockHash
	legacy["stateRoot"] = state.StateRoot
	legacy["nodeID"] = state.NodeID
	legacy["version"] = state.Version
	legacy["networkStatus"] = state.NetworkStatus
	legacy["peerCount"] = state.PeerCount
	legacy["validatorSet"] = state.ValidatorSet
	legacy["timestamp"] = state.Timestamp.Format(time.RFC3339)

	// Convert attributes back to interface{} values
	for key, value := range state.Attributes {
		legacy[key] = sic.valueToInterface(value)
	}

	return legacy
}

// ConvertFromLegacyStateSyncRequest converts legacy map[string]interface{} to StateSyncRequest
func (sic *StateInfoConverter) ConvertFromLegacyStateSyncRequest(legacy map[string]interface{}) *StateSyncRequest {
	request := &StateSyncRequest{
		Peers: make(map[string]*SyncStateInfo),
	}

	// Convert peers
	if peersRaw, ok := legacy["peers"].(map[string]interface{}); ok {
		for peerID, peerStateRaw := range peersRaw {
			if peerStateMap, ok := peerStateRaw.(map[string]interface{}); ok {
				request.Peers[peerID] = sic.ConvertFromLegacyState(peerStateMap)
			}
		}
	}

	// Convert resolution info
	if resolutionRaw, ok := legacy["resolution"].(map[string]interface{}); ok {
		request.Resolution = &StateResolution{}

		if strategy, ok := resolutionRaw["strategy"].(string); ok {
			request.Resolution.Strategy = strategy
		}

		if selectedPeer, ok := resolutionRaw["selectedPeer"].(string); ok {
			request.Resolution.SelectedPeer = selectedPeer
		}

		if selectedHash, ok := resolutionRaw["selectedHash"].(string); ok {
			request.Resolution.SelectedHash = selectedHash
		}

		if confidence, ok := resolutionRaw["confidence"].(float64); ok {
			request.Resolution.Confidence = confidence
		}

		request.Resolution.Timestamp = common.ConsensusNow()
	}

	return request
}

// ConvertToLegacyStateSyncRequest converts StateSyncRequest to legacy map[string]interface{}
func (sic *StateInfoConverter) ConvertToLegacyStateSyncRequest(request *StateSyncRequest) map[string]interface{} {
	legacy := make(map[string]interface{})

	// Convert peers
	peers := make(map[string]interface{})
	for peerID, peerState := range request.Peers {
		peers[peerID] = sic.ConvertToLegacyState(peerState)
	}
	legacy["peers"] = peers

	// Convert resolution info
	if request.Resolution != nil {
		resolution := make(map[string]interface{})
		resolution["strategy"] = request.Resolution.Strategy
		resolution["selectedPeer"] = request.Resolution.SelectedPeer
		resolution["selectedHash"] = request.Resolution.SelectedHash
		resolution["confidence"] = request.Resolution.Confidence
		legacy["resolution"] = resolution
	}

	return legacy
}

// valueToInterface converts a typed Value back to interface{}
func (sic *StateInfoConverter) valueToInterface(value *Value) interface{} {
	switch value.Type {
	case ValueTypeString:
		return string(value.Data)
	case ValueTypeInt64:
		if val, err := value.Int64(); err == nil {
			return val
		}
	case ValueTypeUint64:
		if val, err := value.Uint64(); err == nil {
			return val
		}
	case ValueTypeBool:
		if val, err := value.Bool(); err == nil {
			return val
		}
	case ValueTypeBytes:
		return value.Data
	case ValueTypeTimestamp:
		if t, err := time.Parse(time.RFC3339, string(value.Data)); err == nil {
			return t
		}
	case ValueTypeJSON:
		return string(value.Data)
	}
	return string(value.Data)
}
