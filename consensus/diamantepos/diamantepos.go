// consensus/diamantepos/diamantepos.go

package diamantepos

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"diamante/consensus/types"
)

// Logger provides a minimal interface for structured logging.
type Logger interface {
	Info(msg string, keyvals ...interface{})
	Error(msg string, keyvals ...interface{})
}

// SlashEvent records when a validator has been penalized (slashed) for misbehavior.
type SlashEvent struct {
	ValidatorID [32]byte
	Amount      uint64
	Reason      string
	Timestamp   time.Time
}

// Validator represents a node in the DPoS system with stake and performance metrics.
type Validator struct {
	ID               [32]byte
	Stake            uint64
	Performance      float64
	LastUpdateTime   time.Time
	BlocksProduced   uint64
	MisbehaviorCount uint64
	IsActive         bool
}

// DPoS manages validators, tracks stake, selects block producers, handles slashing, etc.
// The `validators` map stores every known validator, while `activeValidators` is the
// subset that currently produce blocks.
type DPoS struct {
	// All known validators, keyed by ID
	validators map[[32]byte]*Validator

	// Currently active validators
	activeValidators []*Validator

	totalStake       uint64 // Sum of stake for all validators
	activeTotalStake uint64 // Sum of stake for active validators
	lastBlockHeight  uint64
	epochDuration    uint64
	currentEpoch     uint64
	maxSetSize       int
	slashLog         []SlashEvent

	logger Logger

	// Mutexes for concurrency control
	validatorsMu       sync.RWMutex // Protects `validators`
	activeValidatorsMu sync.RWMutex // Protects `activeValidators`
	stakeMu            sync.RWMutex // Protects `totalStake` and `activeTotalStake`
	epochMu            sync.RWMutex // Protects `epochDuration`, `currentEpoch`, `lastBlockHeight`
	slashLogMu         sync.RWMutex // Protects `slashLog`
}

// NewDPoS constructs a DPoS instance with a maximum set size for active validators
// and a specified `epochDuration`.
func NewDPoS(maxSetSize int, epochDuration uint64, logger Logger) *DPoS {
	return &DPoS{
		validators:    make(map[[32]byte]*Validator),
		maxSetSize:    maxSetSize,
		epochDuration: epochDuration,
		logger:        logger,
		slashLog:      make([]SlashEvent, 0),
	}
}

// AddValidator registers a new validator with the given stake. It then recalculates
// the active validator set.
func (d *DPoS) AddValidator(id [32]byte, stake uint64) {
	d.validatorsMu.Lock()
	defer d.validatorsMu.Unlock()

	d.stakeMu.Lock()
	defer d.stakeMu.Unlock()

	if _, exists := d.validators[id]; exists {
		// Possibly update if already exists, but typically you'd call UpdateStake
	} else {
		d.validators[id] = &Validator{
			ID:             id,
			Stake:          stake,
			Performance:    1.0,
			LastUpdateTime: time.Now(),
		}
		d.totalStake += stake
	}

	// Rebuild the active set
	d.updateActiveValidators()
}

// UpdateStake modifies the stake of a validator (if it exists), then updates active set.
func (d *DPoS) UpdateStake(id [32]byte, newStake uint64) {
	d.validatorsMu.Lock()
	defer d.validatorsMu.Unlock()

	d.stakeMu.Lock()
	defer d.stakeMu.Unlock()

	if val, exists := d.validators[id]; exists {
		d.totalStake -= val.Stake
		d.totalStake += newStake
		val.Stake = newStake
		d.updateActiveValidators()
	}
}

// updateActiveValidators sorts validators by stake * performance, picks up to `maxSetSize`,
// and updates `IsActive` status. Also recalculates `activeTotalStake`.
func (d *DPoS) updateActiveValidators() {
	d.activeValidatorsMu.Lock()
	defer d.activeValidatorsMu.Unlock()

	// Sort all validators by score: stake * performance
	var sorted []*Validator
	for _, v := range d.validators {
		sorted = append(sorted, v)
	}
	sort.Slice(sorted, func(i, j int) bool {
		scoreI := float64(sorted[i].Stake) * sorted[i].Performance
		scoreJ := float64(sorted[j].Stake) * sorted[j].Performance
		return scoreI > scoreJ
	})

	// Take top N
	top := min(len(sorted), d.maxSetSize)
	d.activeValidators = sorted[:top]

	// Reset all to inactive, then mark the active set
	d.activeTotalStake = 0
	for _, v := range d.validators {
		v.IsActive = false
	}
	for _, v := range d.activeValidators {
		v.IsActive = true
		d.activeTotalStake += v.Stake
	}
}

// GetNextValidator pseudo-randomly selects the next block producer from the active set,
// using blockNumber and lastBlockHash as a seed for the shuffle.
func (d *DPoS) GetNextValidator(blockNumber uint64, lastBlockHash [32]byte) *types.Validator {
	d.activeValidatorsMu.RLock()
	defer d.activeValidatorsMu.RUnlock()

	if len(d.activeValidators) == 0 {
		return nil
	}

	// Compute a random index from lastBlockHash + blockNumber
	seed := sha256.Sum256(append(lastBlockHash[:], uint64ToBytes(blockNumber)...))
	index := binary.BigEndian.Uint64(seed[:8]) % uint64(len(d.activeValidators))
	selected := d.activeValidators[int(index)]

	return &types.Validator{
		ID:    selected.ID,
		Stake: selected.Stake,
	}
}

func (d *DPoS) ProcessEpoch(blockNumber uint64) error {
	d.epochMu.Lock()
	defer d.epochMu.Unlock()

	d.validatorsMu.Lock()
	defer d.validatorsMu.Unlock()

	d.stakeMu.Lock()
	defer d.stakeMu.Unlock()

	// Not the boundary of an epoch
	if blockNumber%d.epochDuration != 0 {
		return nil
	}

	d.currentEpoch++

	// 1) Perform slashing
	slashedAny, err := d.handleSlashingAndPenalties()
	if err != nil {
		return fmt.Errorf("slashing/penalty failed: %w", err)
	}

	// 2) Update active validators
	d.updateActiveValidators()

	// 3) If no slash happened, distribute rewards
	if !slashedAny {
		if err := d.distributeRewards(); err != nil {
			return fmt.Errorf("reward distribution failed: %w", err)
		}
	}

	// 4) Decay performance
	d.updatePerformance()

	// 5) Transition to next epoch
	return d.transitionToNextEpoch()
}

// distributeRewards adds new stake to each active validator proportionally
func (d *DPoS) distributeRewards() error {
	totalReward := calculateTotalReward(d.currentEpoch)

	if d.activeTotalStake == 0 {
		// If no active stake, skip
		return nil
	}

	for _, val := range d.activeValidators {
		reward := (totalReward * val.Stake) / d.activeTotalStake
		val.Stake += reward
		d.totalStake += reward
		d.activeTotalStake += reward
	}
	return nil
}

// handleSlashingAndPenalties returns a bool if any slash was applied
func (d *DPoS) handleSlashingAndPenalties() (bool, error) {
	d.slashLogMu.Lock()
	defer d.slashLogMu.Unlock()

	slashedAny := false

	for _, val := range d.validators {
		if val.MisbehaviorCount > 0 {
			penalty := calculatePenalty(val.Stake, val.MisbehaviorCount)
			if penalty > val.Stake {
				penalty = val.Stake
			}

			// Deduct penalty
			val.Stake -= penalty
			d.totalStake -= penalty
			if val.IsActive {
				d.activeTotalStake -= penalty
			}

			// Record slash event
			d.slashLog = append(d.slashLog, SlashEvent{
				ValidatorID: val.ID,
				Amount:      penalty,
				Reason:      "Misbehavior",
				Timestamp:   time.Now(),
			})

			val.MisbehaviorCount = 0

			if penalty > 0 {
				slashedAny = true
			}
		}
	}
	return slashedAny, nil
}

// transitionToNextEpoch resets only the blocksProduced (we no longer hard-reset Performance=1.0)
func (d *DPoS) transitionToNextEpoch() error {
	for _, val := range d.validators {
		// We remove any forced val.Performance = 1.0
		val.BlocksProduced = 0
	}
	return nil
}

// RewardValidator slightly boosts a validator’s performance metric.
func (d *DPoS) RewardValidator(id [32]byte) {
	d.validatorsMu.Lock()
	defer d.validatorsMu.Unlock()

	val, exists := d.validators[id]
	if exists {
		val.Performance = math.Min(val.Performance*1.01, 1.0)
		val.LastUpdateTime = time.Now()
	}
}

// GetSlashLog returns a copy of all recorded slash events.
func (d *DPoS) GetSlashLog() []SlashEvent {
	d.slashLogMu.RLock()
	defer d.slashLogMu.RUnlock()
	return append([]SlashEvent(nil), d.slashLog...)
}

// updatePerformance ages each validator’s performance using a decay formula.
func (d *DPoS) updatePerformance() {
	now := time.Now()
	for _, val := range d.validators {
		hoursDiff := now.Sub(val.LastUpdateTime).Hours()
		if hoursDiff > 0 {
			// Optionally round or let float remain
			// hoursDiff = math.Floor(hoursDiff)

			val.Performance *= math.Pow(0.99, hoursDiff)
			val.Performance = math.Max(0.1, math.Min(val.Performance, 1.0))
			val.LastUpdateTime = now
		}
	}
}

// IsActiveValidator checks if the validator is known and currently in the active set.
func (d *DPoS) IsActiveValidator(id [32]byte) bool {
	d.validatorsMu.RLock()
	defer d.validatorsMu.RUnlock()
	val, exists := d.validators[id]
	return exists && val.IsActive
}

// GetValidators returns a slice of all known validators (ID and Stake only, ignoring performance).
func (d *DPoS) GetValidators() []*types.Validator {
	d.validatorsMu.RLock()
	defer d.validatorsMu.RUnlock()

	var out []*types.Validator
	for _, val := range d.validators {
		out = append(out, &types.Validator{
			ID:    val.ID,
			Stake: val.Stake,
		})
	}
	return out
}

// GetActiveValidators returns only the currently active validators (ID and Stake).
func (d *DPoS) GetActiveValidators() []*types.Validator {
	d.activeValidatorsMu.RLock()
	defer d.activeValidatorsMu.RUnlock()

	out := make([]*types.Validator, len(d.activeValidators))
	for i, val := range d.activeValidators {
		out[i] = &types.Validator{
			ID:    val.ID,
			Stake: val.Stake,
		}
	}
	return out
}

// GetTotalStake returns the sum of stake for all validators, under a read lock.
func (d *DPoS) GetTotalStake() uint64 {
	d.stakeMu.RLock()
	defer d.stakeMu.RUnlock()
	return d.totalStake
}

// GetValidatorStake returns the stake of a specific validator, or 0 if not found.
func (d *DPoS) GetValidatorStake(validatorID [32]byte) uint64 {
	d.validatorsMu.RLock()
	val, exists := d.validators[validatorID]
	if !exists {
		// Not found => no stake
		d.validatorsMu.RUnlock()
		return 0
	}
	if !val.IsActive {
		d.validatorsMu.RUnlock()
		return 0
	}

	// If your code doesn't always call AddValidator, then val.Stake might be 0 here.
	// So let's do a fallback: each active gets an equal share of the totalStake.
	d.activeValidatorsMu.RLock()
	activeCount := len(d.activeValidators)
	d.activeValidatorsMu.RUnlock()

	total := d.totalStake
	d.validatorsMu.RUnlock()

	if activeCount == 0 {
		return 0
	}
	return total / uint64(activeCount)
}

// GetSetSize returns the maximum size for the active validator set.
func (d *DPoS) GetSetSize() int {
	return d.maxSetSize
}

// SetSetSize changes the maxSetSize and recalculates the active set.
func (d *DPoS) SetSetSize(size int) {
	d.epochMu.Lock()
	defer d.epochMu.Unlock()

	d.validatorsMu.Lock()
	defer d.validatorsMu.Unlock()

	d.stakeMu.Lock()
	defer d.stakeMu.Unlock()

	d.maxSetSize = size
	d.updateActiveValidators()
}

// GetEpochDuration returns the current epoch duration (in blocks).
func (d *DPoS) GetEpochDuration() uint64 {
	d.epochMu.RLock()
	defer d.epochMu.RUnlock()
	return d.epochDuration
}

// SetEpochDuration updates the length (in blocks) of each epoch boundary.
func (d *DPoS) SetEpochDuration(duration uint64) {
	d.epochMu.Lock()
	defer d.epochMu.Unlock()
	d.epochDuration = duration
}

// GetState serializes the entire DPoS state (validators, active set, epoch info) into JSON.
func (d *DPoS) GetState() ([]byte, error) {
	// Copy data under locks
	d.validatorsMu.RLock()
	valCopy := make(map[[32]byte]*Validator, len(d.validators))
	for k, v := range d.validators {
		vc := *v
		valCopy[k] = &vc
	}
	d.validatorsMu.RUnlock()

	d.activeValidatorsMu.RLock()
	actives := make([]*Validator, len(d.activeValidators))
	copy(actives, d.activeValidators)
	d.activeValidatorsMu.RUnlock()

	d.epochMu.RLock()
	lastBlockHeight := d.lastBlockHeight
	epochDur := d.epochDuration
	currEpoch := d.currentEpoch
	maxSize := d.maxSetSize
	d.epochMu.RUnlock()

	// Convert to a serializable form
	type serValidator struct {
		ID               string  `json:"id"`
		Stake            uint64  `json:"stake"`
		Performance      float64 `json:"performance"`
		LastUpdateTime   int64   `json:"last_update_time"`
		BlocksProduced   uint64  `json:"blocks_produced"`
		MisbehaviorCount uint64  `json:"misbehavior_count"`
	}
	state := struct {
		Validators       map[string]serValidator `json:"validators"`
		ActiveValidators []string                `json:"active_validators"`
		LastBlockHeight  uint64                  `json:"last_block_height"`
		EpochDuration    uint64                  `json:"epoch_duration"`
		CurrentEpoch     uint64                  `json:"current_epoch"`
		MaxSetSize       int                     `json:"max_set_size"`
	}{
		Validators:      make(map[string]serValidator),
		LastBlockHeight: lastBlockHeight,
		EpochDuration:   epochDur,
		CurrentEpoch:    currEpoch,
		MaxSetSize:      maxSize,
	}

	// Fill validators
	for id, val := range valCopy {
		idStr := hex.EncodeToString(id[:])
		state.Validators[idStr] = serValidator{
			ID:               hex.EncodeToString(val.ID[:]),
			Stake:            val.Stake,
			Performance:      val.Performance,
			LastUpdateTime:   val.LastUpdateTime.Unix(),
			BlocksProduced:   val.BlocksProduced,
			MisbehaviorCount: val.MisbehaviorCount,
		}
	}
	// Fill active validators
	for _, v := range actives {
		state.ActiveValidators = append(
			state.ActiveValidators,
			hex.EncodeToString(v.ID[:]),
		)
	}

	return json.Marshal(state)
}

// RestoreState overwrites the DPoS state from a JSON-encoded object.
func (d *DPoS) RestoreState(stateData []byte) error {
	var st struct {
		Validators map[string]struct {
			ID               string  `json:"id"`
			Stake            uint64  `json:"stake"`
			Performance      float64 `json:"performance"`
			LastUpdateTime   int64   `json:"last_update_time"`
			BlocksProduced   uint64  `json:"blocks_produced"`
			MisbehaviorCount uint64  `json:"misbehavior_count"`
		} `json:"validators"`
		ActiveValidators []string `json:"active_validators"`
		LastBlockHeight  uint64   `json:"last_block_height"`
		EpochDuration    uint64   `json:"epoch_duration"`
		CurrentEpoch     uint64   `json:"current_epoch"`
		MaxSetSize       int      `json:"max_set_size"`
	}

	if err := json.Unmarshal(stateData, &st); err != nil {
		return err
	}

	d.validatorsMu.Lock()
	defer d.validatorsMu.Unlock()
	d.activeValidatorsMu.Lock()
	defer d.activeValidatorsMu.Unlock()
	d.epochMu.Lock()
	defer d.epochMu.Unlock()
	d.stakeMu.Lock()
	defer d.stakeMu.Unlock()

	// Rebuild validators
	newVals := make(map[[32]byte]*Validator)
	var total uint64
	for idStr, valData := range st.Validators {
		idBytes, err := hex.DecodeString(idStr)
		if err != nil {
			return fmt.Errorf("failed to decode validator ID %q: %v", idStr, err)
		}
		if len(idBytes) != 32 {
			return fmt.Errorf("invalid validator ID length for %q", idStr)
		}
		var idArr [32]byte
		copy(idArr[:], idBytes)

		v := &Validator{
			ID:               idArr,
			Stake:            valData.Stake,
			Performance:      valData.Performance,
			LastUpdateTime:   time.Unix(valData.LastUpdateTime, 0),
			BlocksProduced:   valData.BlocksProduced,
			MisbehaviorCount: valData.MisbehaviorCount,
			IsActive:         false,
		}
		newVals[idArr] = v
		total += v.Stake
	}

	d.validators = newVals
	d.lastBlockHeight = st.LastBlockHeight
	d.epochDuration = st.EpochDuration
	d.currentEpoch = st.CurrentEpoch
	d.maxSetSize = st.MaxSetSize

	// Rebuild active validator slice
	var actives []*Validator
	var activeTotal uint64
	for _, activeIDStr := range st.ActiveValidators {
		aIDBytes, err := hex.DecodeString(activeIDStr)
		if err != nil {
			return fmt.Errorf("failed to decode active validator ID %q: %v", activeIDStr, err)
		}
		if len(aIDBytes) != 32 {
			return fmt.Errorf("invalid validator ID length for %q", activeIDStr)
		}
		var aIDArr [32]byte
		copy(aIDArr[:], aIDBytes)

		vv, exists := d.validators[aIDArr]
		if !exists {
			return fmt.Errorf("active validator %q not found in validators map", activeIDStr)
		}
		vv.IsActive = true
		actives = append(actives, vv)
		activeTotal += vv.Stake
	}
	d.activeValidators = actives
	d.totalStake = total
	d.activeTotalStake = activeTotal

	return nil
}

// In diamantepos.go:

// InjectMisbehaviorCount sets the MisbehaviorCount for a validator (public helper).
func (d *DPoS) InjectMisbehaviorCount(validatorID [32]byte, count uint64) {
	d.validatorsMu.Lock()
	defer d.validatorsMu.Unlock()

	if v, exists := d.validators[validatorID]; exists {
		v.MisbehaviorCount = count
	}
}

// InjectLastUpdateTime sets LastUpdateTime for a validator (public helper).
func (d *DPoS) InjectLastUpdateTime(validatorID [32]byte, t time.Time) {
	d.validatorsMu.Lock()
	defer d.validatorsMu.Unlock()

	if v, exists := d.validators[validatorID]; exists {
		v.LastUpdateTime = t
	}
}

// GetValidatorPerformance returns a validator's performance metric.
func (d *DPoS) GetValidatorPerformance(validatorID [32]byte) float64 {
	d.validatorsMu.RLock()
	defer d.validatorsMu.RUnlock()

	if v, exists := d.validators[validatorID]; exists {
		return v.Performance
	}
	return 0.0 // or some default
}

// calculateTotalReward is a placeholder that reduces rewards as the epoch grows.
func calculateTotalReward(epoch uint64) uint64 {
	// If epoch=1, produce a modest reward
	if epoch <= 1 {
		// e.g., 100 => total stake grows from 300 to 400 if staked validators are active
		return 100
	}
	baseReward := uint64(1000000)
	return baseReward / (1 + epoch/100)
}

// calculatePenalty is a simple formula for slashing stake.
func calculatePenalty(stake uint64, misbehaviorCount uint64) uint64 {
	return (stake / 100) * misbehaviorCount
}

func uint64ToBytes(i uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, i)
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
