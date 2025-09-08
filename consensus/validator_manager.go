// consensus/validator_manager.go

package consensus

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"diamante/consensus/types"
	dtypes "diamante/types"
)

// ValidatorStatus represents the current status of a validator
type ValidatorStatus int

const (
	// ValidatorStatusActive indicates the validator is active and can participate in consensus
	ValidatorStatusActive ValidatorStatus = iota
	// ValidatorStatusInactive indicates the validator is registered but not active
	ValidatorStatusInactive
	// ValidatorStatusSlashed indicates the validator has been slashed for misbehavior
	ValidatorStatusSlashed
	// ValidatorStatusJailed indicates the validator is temporarily jailed
	ValidatorStatusJailed
)

// ValidatorInfo extends the basic Validator type with additional information
type ValidatorInfo struct {
	ID               [32]byte
	Stake            uint64
	Status           ValidatorStatus
	Performance      float64
	BlocksProduced   uint64
	EventsFinalized  uint64
	LastRewardTime   time.Time
	JoinTime         time.Time
	MisbehaviorCount uint64
}

// ValidatorManager centralizes validator operations across consensus components
type ValidatorManager struct {
	hc *HybridConsensus

	// Validator state
	validators       map[[32]byte]*ValidatorInfo
	activeValidators []*ValidatorInfo
	validatorsMu     sync.RWMutex

	// Stake tracking
	totalStake       uint64
	activeTotalStake uint64
	stakeMu          sync.RWMutex

	// Performance metrics
	performanceDecayRate float64
	minPerformance       float64
	maxPerformance       float64
	performanceMu        sync.RWMutex

	// Reward distribution
	blockRewardWeight float64 // Weight for block production rewards
	eventRewardWeight float64 // Weight for event finalization rewards
	lastRewardHeight  uint64
	rewardMu          sync.RWMutex
}

// NewValidatorManager creates a new ValidatorManager instance
func NewValidatorManager(hc *HybridConsensus) *ValidatorManager {
	return &ValidatorManager{
		hc:                   hc,
		validators:           make(map[[32]byte]*ValidatorInfo),
		performanceDecayRate: 0.99,
		minPerformance:       0.1,
		maxPerformance:       1.0,
		blockRewardWeight:    0.6, // 60% of rewards for block production
		eventRewardWeight:    0.4, // 40% of rewards for event finalization
	}
}

// AddValidator registers a new validator with the given stake
// It ensures the validator is properly registered in all consensus components
func (vm *ValidatorManager) AddValidator(id [32]byte, stake uint64) error {
	vm.validatorsMu.Lock()
	defer vm.validatorsMu.Unlock()

	vm.stakeMu.Lock()
	defer vm.stakeMu.Unlock()

	// Check if validator already exists
	if _, exists := vm.validators[id]; exists {
		return NewConsensusError(
			ErrInvalidValidator,
			ErrorCategoryTemporary,
			fmt.Sprintf("validator %x already exists", id),
		).WithValidatorID(id).
			WithContext("stake", stake)
	}

	// Validate stake
	if stake == 0 {
		return NewConsensusError(
			ErrInsufficientStake,
			ErrorCategoryTemporary,
			"validator stake cannot be zero",
		).WithValidatorID(id)
	}

	// Create new validator info with more detailed initialization
	consensusTime := ConsensusNow()
	validator := &ValidatorInfo{
		ID:             id,
		Stake:          stake,
		Status:         ValidatorStatusInactive, // Start as inactive
		Performance:    1.0,                     // Start with perfect performance
		LastRewardTime: consensusTime,
		JoinTime:       consensusTime,
	}

	// Add to validators map
	vm.validators[id] = validator
	vm.totalStake += stake

	// Add to DPoS and Lachesis
	vm.hc.dpos.AddValidator(id, stake)
	vm.hc.lachesis.AddNode(id, stake)

	// Update active validators
	vm.updateActiveValidators()

	vm.hc.logger.Info("Validator added",
		ValidatorIDField(id),
		IntField("stake", int(stake)),
		IntField("totalValidators", len(vm.validators)),
		IntField("activeValidators", len(vm.activeValidators)))

	return nil
}

// UpdateStake modifies the stake of an existing validator
func (vm *ValidatorManager) UpdateStake(id [32]byte, newStake uint64) error {
	vm.validatorsMu.Lock()
	defer vm.validatorsMu.Unlock()

	vm.stakeMu.Lock()
	defer vm.stakeMu.Unlock()

	// Check if validator exists
	validator, exists := vm.validators[id]
	if !exists {
		return NewConsensusError(
			ErrValidatorNotFound,
			ErrorCategoryTemporary,
			fmt.Sprintf("validator %x not found", id),
		).WithValidatorID(id)
	}

	// Validate stake
	if newStake == 0 {
		return NewConsensusError(
			ErrInsufficientStake,
			ErrorCategoryTemporary,
			"validator stake cannot be zero",
		).WithValidatorID(id)
	}

	// Update total stake
	vm.totalStake -= validator.Stake
	vm.totalStake += newStake

	// Update active total stake if validator is active
	if validator.Status == ValidatorStatusActive {
		vm.activeTotalStake -= validator.Stake
		vm.activeTotalStake += newStake
	}

	// Update validator stake
	oldStake := validator.Stake
	validator.Stake = newStake

	// Update stake in DPoS and Lachesis
	vm.hc.dpos.UpdateStake(id, newStake)
	vm.hc.lachesis.UpdateNodeStake(id, newStake)

	// Update active validators
	vm.updateActiveValidators()

	vm.hc.logger.Info("Validator stake updated",
		ValidatorIDField(id),
		IntField("oldStake", int(oldStake)),
		IntField("newStake", int(newStake)))

	return nil
}

// ActivateValidator activates a validator
func (vm *ValidatorManager) ActivateValidator(id [32]byte) error {
	vm.validatorsMu.Lock()
	defer vm.validatorsMu.Unlock()

	// Check if validator exists
	validator, exists := vm.validators[id]
	if !exists {
		return NewConsensusError(
			ErrValidatorNotFound,
			ErrorCategoryTemporary,
			fmt.Sprintf("validator %x not found", id),
		).WithValidatorID(id)
	}

	// Check if validator is already active
	if validator.Status == ValidatorStatusActive {
		return nil
	}

	// Check if validator is jailed
	if validator.Status == ValidatorStatusJailed {
		return NewConsensusError(
			ErrInvalidValidator,
			ErrorCategoryTemporary,
			fmt.Sprintf("validator %x is jailed", id),
		).WithValidatorID(id).
			WithContext("status", "jailed")
	}

	// Check if validator is slashed
	if validator.Status == ValidatorStatusSlashed {
		return NewConsensusError(
			ErrInvalidValidator,
			ErrorCategoryTemporary,
			fmt.Sprintf("validator %x is slashed", id),
		).WithValidatorID(id).
			WithContext("status", "slashed")
	}

	// Activate validator
	validator.Status = ValidatorStatusActive

	// Update active validators
	vm.updateActiveValidators()

	vm.hc.logger.Info("Validator activated",
		ValidatorIDField(id),
		IntField("activeValidators", len(vm.activeValidators)))

	return nil
}

// DeactivateValidator deactivates a validator
func (vm *ValidatorManager) DeactivateValidator(id [32]byte) error {
	vm.validatorsMu.Lock()
	defer vm.validatorsMu.Unlock()

	// Check if validator exists
	validator, exists := vm.validators[id]
	if !exists {
		return NewConsensusError(
			ErrValidatorNotFound,
			ErrorCategoryTemporary,
			fmt.Sprintf("validator %x not found", id),
		).WithValidatorID(id)
	}

	// Check if validator is already inactive
	if validator.Status == ValidatorStatusInactive {
		return nil
	}

	// Deactivate validator
	validator.Status = ValidatorStatusInactive

	// Update active validators
	vm.updateActiveValidators()

	vm.hc.logger.Info("Validator deactivated",
		ValidatorIDField(id),
		IntField("activeValidators", len(vm.activeValidators)))

	return nil
}

// SlashValidator slashes a validator for misbehavior
func (vm *ValidatorManager) SlashValidator(id [32]byte, slashAmount uint64, reason string) error {
	// Acquire locks in the correct order: validatorsMu then stakeMu
	vm.validatorsMu.Lock()
	defer vm.validatorsMu.Unlock()

	vm.stakeMu.Lock()
	defer vm.stakeMu.Unlock()

	// Check if validator exists
	validator, exists := vm.validators[id]
	if !exists {
		return NewConsensusError(
			ErrValidatorNotFound,
			ErrorCategoryTemporary,
			fmt.Sprintf("validator %x not found", id),
		).WithValidatorID(id)
	}

	// Cap slash amount to validator's stake
	if slashAmount > validator.Stake {
		slashAmount = validator.Stake
	}

	// Update validator stake
	oldStake := validator.Stake
	validator.Stake -= slashAmount
	validator.Status = ValidatorStatusSlashed
	validator.MisbehaviorCount++

	// Update total stake
	vm.totalStake -= slashAmount

	// Update active total stake if validator is active
	if validator.Status == ValidatorStatusActive {
		vm.activeTotalStake -= slashAmount
	}

	// Update stake in DPoS and Lachesis
	vm.hc.dpos.UpdateStake(id, validator.Stake)
	vm.hc.lachesis.UpdateNodeStake(id, validator.Stake)

	// Update active validators
	vm.updateActiveValidators()

	vm.hc.logger.Info("Validator slashed",
		ValidatorIDField(id),
		IntField("slashAmount", int(slashAmount)),
		IntField("oldStake", int(oldStake)),
		IntField("newStake", int(validator.Stake)),
		LogField{Key: "reason", Value: dtypes.NewValue(dtypes.ValueTypeString, []byte(reason))},
		IntField("misbehaviorCount", int(validator.MisbehaviorCount)))

	return nil
}

// JailValidator temporarily jails a validator
func (vm *ValidatorManager) JailValidator(id [32]byte, reason string) error {
	vm.validatorsMu.Lock()
	defer vm.validatorsMu.Unlock()

	// Check if validator exists
	validator, exists := vm.validators[id]
	if !exists {
		return NewConsensusError(
			ErrValidatorNotFound,
			ErrorCategoryTemporary,
			fmt.Sprintf("validator %x not found", id),
		).WithValidatorID(id)
	}

	// Check if validator is already jailed
	if validator.Status == ValidatorStatusJailed {
		return nil
	}

	// Jail validator
	validator.Status = ValidatorStatusJailed
	validator.MisbehaviorCount++

	// Update active validators
	vm.updateActiveValidators()

	vm.hc.logger.Info("Validator jailed",
		ValidatorIDField(id),
		LogField{Key: "reason", Value: dtypes.NewValue(dtypes.ValueTypeString, []byte(reason))},
		IntField("misbehaviorCount", int(validator.MisbehaviorCount)),
		IntField("activeValidators", len(vm.activeValidators)))

	return nil
}

// UnjailValidator removes a validator from jail
func (vm *ValidatorManager) UnjailValidator(id [32]byte) error {
	vm.validatorsMu.Lock()
	defer vm.validatorsMu.Unlock()

	// Check if validator exists
	validator, exists := vm.validators[id]
	if !exists {
		return NewConsensusError(
			ErrValidatorNotFound,
			ErrorCategoryTemporary,
			fmt.Sprintf("validator %x not found", id),
		).WithValidatorID(id)
	}

	// Check if validator is jailed
	if validator.Status != ValidatorStatusJailed {
		return NewConsensusError(
			ErrInvalidValidator,
			ErrorCategoryTemporary,
			fmt.Sprintf("validator %x is not jailed", id),
		).WithValidatorID(id).
			WithContext("status", validator.Status)
	}

	// Unjail validator
	validator.Status = ValidatorStatusInactive

	vm.hc.logger.Info("Validator unjailed",
		ValidatorIDField(id),
		IntField("misbehaviorCount", int(validator.MisbehaviorCount)))

	return nil
}

// RewardBlockProduction rewards a validator for producing a block
func (vm *ValidatorManager) RewardBlockProduction(id [32]byte, blockHeight uint64) error {
	vm.validatorsMu.Lock()
	defer vm.validatorsMu.Unlock()

	// Check if validator exists
	validator, exists := vm.validators[id]
	if !exists {
		return NewConsensusError(
			ErrValidatorNotFound,
			ErrorCategoryTemporary,
			fmt.Sprintf("validator %x not found", id),
		).WithValidatorID(id).
			WithContext("blockHeight", blockHeight)
	}

	// Increment blocks produced
	validator.BlocksProduced++

	// Update performance
	vm.updateValidatorPerformance(validator)

	// Reward validator in DPoS
	vm.hc.dpos.RewardValidator(id)

	vm.hc.logger.Info("Validator rewarded for block production",
		ValidatorIDField(id),
		BlockHeightField(blockHeight),
		IntField("blocksProduced", int(validator.BlocksProduced)),
		Float64Field("performance", validator.Performance))

	return nil
}

// RewardEventFinalization rewards a validator for finalizing an event
func (vm *ValidatorManager) RewardEventFinalization(id [32]byte, eventHeight uint64) error {
	vm.validatorsMu.Lock()
	defer vm.validatorsMu.Unlock()

	// Check if validator exists
	validator, exists := vm.validators[id]
	if !exists {
		return NewConsensusError(
			ErrValidatorNotFound,
			ErrorCategoryTemporary,
			fmt.Sprintf("validator %x not found", id),
		).WithValidatorID(id).
			WithContext("eventHeight", eventHeight)
	}

	// Increment events finalized
	validator.EventsFinalized++

	// Update performance
	vm.updateValidatorPerformance(validator)

	vm.hc.logger.Info("Validator rewarded for event finalization",
		ValidatorIDField(id),
		BlockHeightField(eventHeight),
		IntField("eventsFinalized", int(validator.EventsFinalized)),
		Float64Field("performance", validator.Performance))

	return nil
}

// updateValidatorPerformance updates a validator's performance metric
func (vm *ValidatorManager) updateValidatorPerformance(validator *ValidatorInfo) {
	vm.performanceMu.Lock()
	defer vm.performanceMu.Unlock()

	// Calculate time since last update
	consensusTime := ConsensusNow()
	hoursSinceLastUpdate := consensusTime.Sub(validator.LastRewardTime).Hours()

	// Apply time-based decay
	if hoursSinceLastUpdate > 0 {
		decayFactor := math.Pow(vm.performanceDecayRate, hoursSinceLastUpdate)
		validator.Performance *= decayFactor
	}

	// Boost performance for activity
	validator.Performance *= 1.01

	// Clamp performance to valid range
	validator.Performance = math.Max(vm.minPerformance, math.Min(validator.Performance, vm.maxPerformance))

	// Update last reward time
	validator.LastRewardTime = consensusTime
}

// updateActiveValidators updates the list of active validators
func (vm *ValidatorManager) updateActiveValidators() {
	// Reset active validators list
	vm.activeValidators = nil
	vm.activeTotalStake = 0

	// IMPORTANT: Sort validator IDs first to ensure deterministic iteration order
	// This prevents different nodes from building different active validator lists
	var validatorIDs [][32]byte
	for id := range vm.validators {
		validatorIDs = append(validatorIDs, id)
	}

	// Sort validator IDs deterministically
	sort.Slice(validatorIDs, func(i, j int) bool {
		return bytes.Compare(validatorIDs[i][:], validatorIDs[j][:]) < 0
	})

	// Add active validators to the list in deterministic order
	for _, id := range validatorIDs {
		validator := vm.validators[id]
		if validator.Status == ValidatorStatusActive {
			vm.activeValidators = append(vm.activeValidators, validator)
			vm.activeTotalStake += validator.Stake
		}
	}

	// Sort active validators by stake * performance (descending)
	sortValidatorsByScore(vm.activeValidators)

	// Limit active validators to max set size
	maxSetSize := vm.hc.dpos.GetSetSize()
	if len(vm.activeValidators) > maxSetSize {
		vm.activeValidators = vm.activeValidators[:maxSetSize]

		// Recalculate active total stake
		vm.activeTotalStake = 0
		for _, validator := range vm.activeValidators {
			vm.activeTotalStake += validator.Stake
		}
	}
}

// sortValidatorsByScore sorts validators by stake * performance (descending)
func sortValidatorsByScore(validators []*ValidatorInfo) {
	for i := 0; i < len(validators); i++ {
		for j := i + 1; j < len(validators); j++ {
			// Use fixed-point math for deterministic scoring
			stakeI := NewFixedPointFromUint64(validators[i].Stake, DefaultPrecision)
			perfI := NewFixedPointFromRatio(uint64(validators[i].Performance*1000000), 1000000, DefaultPrecision)
			scoreI := stakeI.Mul(perfI)

			stakeJ := NewFixedPointFromUint64(validators[j].Stake, DefaultPrecision)
			perfJ := NewFixedPointFromRatio(uint64(validators[j].Performance*1000000), 1000000, DefaultPrecision)
			scoreJ := stakeJ.Mul(perfJ)

			// Compare scores first
			cmp := scoreJ.Compare(scoreI)
			if cmp > 0 {
				validators[i], validators[j] = validators[j], validators[i]
			} else if cmp == 0 {
				// If scores are equal, use validator ID as deterministic tie-breaker
				// This ensures the same ordering across all nodes
				if bytes.Compare(validators[j].ID[:], validators[i].ID[:]) < 0 {
					validators[i], validators[j] = validators[j], validators[i]
				}
			}
		}
	}
}

// ProcessEpoch processes validator updates at epoch boundaries
func (vm *ValidatorManager) ProcessEpoch(blockHeight uint64) error {
	vm.validatorsMu.Lock()
	defer vm.validatorsMu.Unlock()

	vm.rewardMu.Lock()
	defer vm.rewardMu.Unlock()

	// Check if we need to distribute rewards
	epochDuration := vm.hc.dpos.GetEpochDuration()
	if blockHeight%epochDuration != 0 {
		return nil
	}

	// Calculate epoch number
	epoch := blockHeight / epochDuration

	// Distribute rewards
	vm.distributeEpochRewards(epoch)

	// Update validator performance
	vm.decayAllValidatorPerformance()

	// Update active validators
	vm.updateActiveValidators()

	// Update last reward height
	vm.lastRewardHeight = blockHeight

	vm.hc.logger.Info("Processed validator epoch",
		BlockHeightField(blockHeight),
		IntField("epoch", int(epoch)),
		IntField("activeValidators", len(vm.activeValidators)),
		IntField("totalStake", int(vm.totalStake)),
		IntField("activeTotalStake", int(vm.activeTotalStake)))

	return nil
}

// distributeEpochRewards distributes rewards to validators based on their performance
func (vm *ValidatorManager) distributeEpochRewards(epoch uint64) {
	// Calculate total reward for this epoch
	totalReward := calculateEpochReward(epoch)

	if vm.activeTotalStake == 0 {
		vm.hc.logger.Info("No active stake; skipping rewards")
		return
	}

	// Distribute rewards to active validators
	for _, validator := range vm.activeValidators {
		// Calculate reward based on stake proportion and performance using fixed-point math
		stakeRatio := NewFixedPointFromRatio(validator.Stake, vm.activeTotalStake, DefaultPrecision)

		// Calculate block production component
		blockRewardWeightFP := NewFixedPointFromRatio(uint64(vm.blockRewardWeight*1000000), 1000000, DefaultPrecision)
		blockComponentFP := blockRewardWeightFP.Mul(stakeRatio).MulUint64(uint64(validator.BlocksProduced))

		// Calculate event finalization component
		eventRewardWeightFP := NewFixedPointFromRatio(uint64(vm.eventRewardWeight*1000000), 1000000, DefaultPrecision)
		eventComponentFP := eventRewardWeightFP.Mul(stakeRatio).MulUint64(uint64(validator.EventsFinalized))

		// Calculate total reward for this validator (proportion of total epoch reward)
		totalComponentFP := blockComponentFP.Add(eventComponentFP)
		activeValidatorCountFP := NewFixedPointFromUint64(uint64(len(vm.activeValidators)), DefaultPrecision)
		validatorRewardRatioFP := totalComponentFP.Div(activeValidatorCountFP)

		// Calculate final reward amount
		totalRewardFP := NewFixedPointFromUint64(totalReward, DefaultPrecision)
		rewardFP := totalRewardFP.Mul(validatorRewardRatioFP)
		reward := rewardFP.ToUint64()

		// Apply reward
		oldStake := validator.Stake
		validator.Stake += reward
		vm.totalStake += reward
		vm.activeTotalStake += reward

		// Reset counters for next epoch
		validator.BlocksProduced = 0
		validator.EventsFinalized = 0

		vm.hc.logger.Info("Epoch reward distributed",
			ValidatorIDField(validator.ID),
			IntField("oldStake", int(oldStake)),
			IntField("reward", int(reward)),
			IntField("newStake", int(validator.Stake)),
			Float64Field("performance", validator.Performance))
	}
}

// decayAllValidatorPerformance applies performance decay to all validators
func (vm *ValidatorManager) decayAllValidatorPerformance() {
	vm.performanceMu.Lock()
	defer vm.performanceMu.Unlock()

	consensusTime := ConsensusNow()
	for _, validator := range vm.validators {
		// Calculate time since last update
		hoursSinceLastUpdate := consensusTime.Sub(validator.LastRewardTime).Hours()

		// Apply time-based decay
		if hoursSinceLastUpdate > 0 {
			oldPerformance := validator.Performance
			decayFactor := math.Pow(vm.performanceDecayRate, hoursSinceLastUpdate)
			validator.Performance *= decayFactor

			// Clamp performance to valid range
			validator.Performance = math.Max(vm.minPerformance, math.Min(validator.Performance, vm.maxPerformance))

			validator.LastRewardTime = consensusTime

			vm.hc.logger.Info("Validator performance decayed",
				ValidatorIDField(validator.ID),
				Float64Field("oldPerformance", oldPerformance),
				Float64Field("newPerformance", validator.Performance))
		}
	}
}

// IsActiveValidator checks if a validator is active
func (vm *ValidatorManager) IsActiveValidator(id [32]byte) bool {
	vm.validatorsMu.RLock()
	defer vm.validatorsMu.RUnlock()

	validator, exists := vm.validators[id]
	return exists && validator.Status == ValidatorStatusActive
}

// GetValidators returns all validators
func (vm *ValidatorManager) GetValidators() []*types.Validator {
	vm.validatorsMu.RLock()
	defer vm.validatorsMu.RUnlock()

	validators := make([]*types.Validator, 0, len(vm.validators))
	for _, validator := range vm.validators {
		validators = append(validators, &types.Validator{
			ID:    validator.ID,
			Stake: validator.Stake,
		})
	}

	return validators
}

// GetActiveValidators returns active validators
func (vm *ValidatorManager) GetActiveValidators() []*types.Validator {
	vm.validatorsMu.RLock()
	defer vm.validatorsMu.RUnlock()

	validators := make([]*types.Validator, 0, len(vm.activeValidators))
	for _, validator := range vm.activeValidators {
		validators = append(validators, &types.Validator{
			ID:    validator.ID,
			Stake: validator.Stake,
		})
	}

	return validators
}

// GetTotalStake returns the total stake of all validators
func (vm *ValidatorManager) GetTotalStake() uint64 {
	vm.stakeMu.RLock()
	defer vm.stakeMu.RUnlock()
	return vm.totalStake
}

// GetActiveTotalStake returns the total stake of active validators
func (vm *ValidatorManager) GetActiveTotalStake() uint64 {
	vm.stakeMu.RLock()
	defer vm.stakeMu.RUnlock()
	return vm.activeTotalStake
}

// GetNextValidator returns the next validator for block production
func (vm *ValidatorManager) GetNextValidator(blockNumber uint64, lastBlockHash [32]byte) *types.Validator {
	// Check if single-node mode is enabled
	if vm.hc != nil && vm.hc.singleNodeMode {
		// In single-node mode, always return the current validator
		vm.validatorsMu.RLock()
		defer vm.validatorsMu.RUnlock()

		// Get the current validator ID
		currentID := vm.hc.GetCurrentValidatorIDBytes()
		if currentID == nil {
			vm.hc.logger.Error("No current validator ID configured in single-node mode")
			return nil
		}

		// Find and return the current validator
		if validator, exists := vm.validators[*currentID]; exists {
			vm.hc.logger.Info("Single-node mode: returning current validator",
				LogField{Key: "validatorID", Value: dtypes.NewValue(dtypes.ValueTypeBytes, currentID[:])},
				BlockHeightField(blockNumber))
			return &types.Validator{
				ID:    validator.ID,
				Stake: validator.Stake,
			}
		}

		// If not found in validator set, create a temporary validator
		vm.hc.logger.Warn("Current validator not in validator set, creating temporary validator for single-node mode")
		return &types.Validator{
			ID:    *currentID,
			Stake: 100000, // Default stake
		}
	}

	// Normal mode: delegate to DPoS
	return vm.hc.dpos.GetNextValidator(blockNumber, lastBlockHash)
}

// GetState returns the serialized state of the validator manager
func (vm *ValidatorManager) GetState() ([]byte, error) {
	vm.validatorsMu.RLock()
	defer vm.validatorsMu.RUnlock()

	vm.stakeMu.RLock()
	defer vm.stakeMu.RUnlock()

	vm.performanceMu.RLock()
	defer vm.performanceMu.RUnlock()

	vm.rewardMu.RLock()
	defer vm.rewardMu.RUnlock()

	// Create serializable state
	type validatorState struct {
		ID               string  `json:"id"`
		Stake            uint64  `json:"stake"`
		Status           int     `json:"status"`
		Performance      float64 `json:"performance"`
		BlocksProduced   uint64  `json:"blocks_produced"`
		EventsFinalized  uint64  `json:"events_finalized"`
		LastRewardTime   int64   `json:"last_reward_time"`
		JoinTime         int64   `json:"join_time"`
		MisbehaviorCount uint64  `json:"misbehavior_count"`
	}

	type state struct {
		Validators           map[string]validatorState `json:"validators"`
		ActiveValidators     []string                  `json:"active_validators"`
		TotalStake           uint64                    `json:"total_stake"`
		ActiveTotalStake     uint64                    `json:"active_total_stake"`
		PerformanceDecayRate float64                   `json:"performance_decay_rate"`
		MinPerformance       float64                   `json:"min_performance"`
		MaxPerformance       float64                   `json:"max_performance"`
		BlockRewardWeight    float64                   `json:"block_reward_weight"`
		EventRewardWeight    float64                   `json:"event_reward_weight"`
		LastRewardHeight     uint64                    `json:"last_reward_height"`
	}

	s := state{
		Validators:           make(map[string]validatorState),
		ActiveValidators:     make([]string, 0, len(vm.activeValidators)),
		TotalStake:           vm.totalStake,
		ActiveTotalStake:     vm.activeTotalStake,
		PerformanceDecayRate: vm.performanceDecayRate,
		MinPerformance:       vm.minPerformance,
		MaxPerformance:       vm.maxPerformance,
		BlockRewardWeight:    vm.blockRewardWeight,
		EventRewardWeight:    vm.eventRewardWeight,
		LastRewardHeight:     vm.lastRewardHeight,
	}

	// Add validators - sort by ID for deterministic iteration
	var validatorIDs [][32]byte
	for id := range vm.validators {
		validatorIDs = append(validatorIDs, id)
	}
	sort.Slice(validatorIDs, func(i, j int) bool {
		return bytes.Compare(validatorIDs[i][:], validatorIDs[j][:]) < 0
	})

	for _, id := range validatorIDs {
		validator := vm.validators[id]
		idStr := fmt.Sprintf("%x", id)
		s.Validators[idStr] = validatorState{
			ID:               idStr,
			Stake:            validator.Stake,
			Status:           int(validator.Status),
			Performance:      validator.Performance,
			BlocksProduced:   validator.BlocksProduced,
			EventsFinalized:  validator.EventsFinalized,
			LastRewardTime:   validator.LastRewardTime.Unix(),
			JoinTime:         validator.JoinTime.Unix(),
			MisbehaviorCount: validator.MisbehaviorCount,
		}
	}

	// Add active validators
	for _, validator := range vm.activeValidators {
		s.ActiveValidators = append(s.ActiveValidators, fmt.Sprintf("%x", validator.ID))
	}

	return json.Marshal(s)
}

// RestoreState restores the validator manager state from serialized data
func (vm *ValidatorManager) RestoreState(data []byte) error {
	vm.validatorsMu.Lock()
	defer vm.validatorsMu.Unlock()

	vm.stakeMu.Lock()
	defer vm.stakeMu.Unlock()

	vm.performanceMu.Lock()
	defer vm.performanceMu.Unlock()

	vm.rewardMu.Lock()
	defer vm.rewardMu.Unlock()

	// Define state structure
	type validatorState struct {
		ID               string  `json:"id"`
		Stake            uint64  `json:"stake"`
		Status           int     `json:"status"`
		Performance      float64 `json:"performance"`
		BlocksProduced   uint64  `json:"blocks_produced"`
		EventsFinalized  uint64  `json:"events_finalized"`
		LastRewardTime   int64   `json:"last_reward_time"`
		JoinTime         int64   `json:"join_time"`
		MisbehaviorCount uint64  `json:"misbehavior_count"`
	}

	type state struct {
		Validators           map[string]validatorState `json:"validators"`
		ActiveValidators     []string                  `json:"active_validators"`
		TotalStake           uint64                    `json:"total_stake"`
		ActiveTotalStake     uint64                    `json:"active_total_stake"`
		PerformanceDecayRate float64                   `json:"performance_decay_rate"`
		MinPerformance       float64                   `json:"min_performance"`
		MaxPerformance       float64                   `json:"max_performance"`
		BlockRewardWeight    float64                   `json:"block_reward_weight"`
		EventRewardWeight    float64                   `json:"event_reward_weight"`
		LastRewardHeight     uint64                    `json:"last_reward_height"`
	}

	// Unmarshal state
	var s state
	if err := json.Unmarshal(data, &s); err != nil {
		return NewConsensusError(
			ErrStateInconsistency,
			ErrorCategoryState,
			"failed to unmarshal validator manager state",
		).WithContext("error", err.Error())
	}

	// Restore validators - sort keys for deterministic iteration
	vm.validators = make(map[[32]byte]*ValidatorInfo)
	var validatorKeys []string
	for idStr := range s.Validators {
		validatorKeys = append(validatorKeys, idStr)
	}
	sort.Strings(validatorKeys)

	for _, idStr := range validatorKeys {
		valState := s.Validators[idStr]
		var id [32]byte
		if _, err := fmt.Sscanf(idStr, "%x", &id); err != nil {
			return NewConsensusError(
				ErrStateInconsistency,
				ErrorCategoryState,
				fmt.Sprintf("failed to parse validator ID %s", idStr),
			).WithContext("error", err.Error())
		}

		vm.validators[id] = &ValidatorInfo{
			ID:               id,
			Stake:            valState.Stake,
			Status:           ValidatorStatus(valState.Status),
			Performance:      valState.Performance,
			BlocksProduced:   valState.BlocksProduced,
			EventsFinalized:  valState.EventsFinalized,
			LastRewardTime:   time.Unix(valState.LastRewardTime, 0),
			JoinTime:         time.Unix(valState.JoinTime, 0),
			MisbehaviorCount: valState.MisbehaviorCount,
		}
	}

	// Restore active validators
	vm.activeValidators = make([]*ValidatorInfo, 0, len(s.ActiveValidators))
	for _, idStr := range s.ActiveValidators {
		var id [32]byte
		if _, err := fmt.Sscanf(idStr, "%x", &id); err != nil {
			return NewConsensusError(
				ErrStateInconsistency,
				ErrorCategoryState,
				fmt.Sprintf("failed to parse active validator ID %s", idStr),
			).WithContext("error", err.Error())
		}

		if validator, exists := vm.validators[id]; exists {
			vm.activeValidators = append(vm.activeValidators, validator)
		}
	}

	// Restore other state
	vm.totalStake = s.TotalStake
	vm.activeTotalStake = s.ActiveTotalStake
	vm.performanceDecayRate = s.PerformanceDecayRate
	vm.minPerformance = s.MinPerformance
	vm.maxPerformance = s.MaxPerformance
	vm.blockRewardWeight = s.BlockRewardWeight
	vm.eventRewardWeight = s.EventRewardWeight
	vm.lastRewardHeight = s.LastRewardHeight

	return nil
}

// calculateEpochReward calculates the total reward for an epoch.
//
// The production formula halves the reward every `halvingPeriod` epochs
// but never drops below `minEpochReward` to maintain an incentive floor.
// This mirrors how many Proof-of-Stake networks gradually reduce
// issuance over time while keeping a minimum payout.
func calculateEpochReward(epoch uint64) uint64 {
	const (
		baseEpochReward = uint64(1_000_000) // starting reward per epoch
		halvingPeriod   = uint64(100)       // epochs between reward halvings
		minEpochReward  = uint64(1_000)     // minimum reward floor
	)

	// Determine how many halvings have occurred
	halvings := epoch / halvingPeriod

	// Use fixed-point math to calculate reward after halvings
	// Start with base reward
	reward := baseEpochReward

	// Apply halvings by dividing by 2 for each halving
	for i := uint64(0); i < halvings && reward > minEpochReward; i++ {
		reward = reward / 2
		if reward < minEpochReward {
			reward = minEpochReward
			break
		}
	}

	return reward
}
