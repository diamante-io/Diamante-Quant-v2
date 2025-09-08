// consensus/common/validation.go

package common

import (
	"fmt"
	"time"

	"diamante/consensus"
	"diamante/consensus/types"
)

// ValidationResult represents the result of a validation operation
type ValidationResult struct {
	Valid   bool
	Error   error
	Context *TypedValidationContext
}

// NewValidationResult creates a new validation result
func NewValidationResult(valid bool, err error) *ValidationResult {
	return &ValidationResult{
		Valid:   valid,
		Error:   err,
		Context: NewTypedValidationContext(),
	}
}

// WithContext adds context information to the validation result
func (vr *ValidationResult) WithContext(key string, value string) *ValidationResult {
	vr.Context.SetString(key, value)
	return vr
}

// WithContextInt adds integer context information to the validation result
func (vr *ValidationResult) WithContextInt(key string, value int) *ValidationResult {
	vr.Context.SetInt(key, value)
	return vr
}

// WithContextUint64 adds uint64 context information to the validation result
func (vr *ValidationResult) WithContextUint64(key string, value uint64) *ValidationResult {
	vr.Context.SetUint64(key, value)
	return vr
}

// WithContextBool adds boolean context information to the validation result
func (vr *ValidationResult) WithContextBool(key string, value bool) *ValidationResult {
	vr.Context.SetBool(key, value)
	return vr
}

// WithContextTime adds time context information to the validation result
func (vr *ValidationResult) WithContextTime(key string, value time.Time) *ValidationResult {
	vr.Context.SetTime(key, value)
	return vr
}

// WithContextInt64 adds int64 context information to the validation result
func (vr *ValidationResult) WithContextInt64(key string, value int64) *ValidationResult {
	vr.Context.SetInt(key, int(value))
	return vr
}

// ValidationFramework provides centralized validation logic for consensus operations
type ValidationFramework struct {
	validatorManager ValidatorManager
	pohManager       PoHManager
	logger           Logger
	errorFramework   *ErrorFramework
}

// ValidatorManager interface for validator operations
type ValidatorManager interface {
	IsActiveValidator(id [32]byte) bool
	GetValidatorStake(id [32]byte) uint64
	GetTotalStake() uint64
}

// PoHManager interface for Proof of History operations
type PoHManager interface {
	GetState() [32]byte
	GetCount() uint64
	Verify(state [32]byte, data []byte, proof [32]byte, count uint64) bool
}

// Logger interface for logging operations
type Logger interface {
	Info(msg string, keyvals ...interface{})
	Error(msg string, keyvals ...interface{})
	Warn(msg string, keyvals ...interface{})
}

// NewValidationFramework creates a new validation framework
func NewValidationFramework(
	validatorManager ValidatorManager,
	pohManager PoHManager,
	logger Logger,
	errorFramework *ErrorFramework,
) *ValidationFramework {
	return &ValidationFramework{
		validatorManager: validatorManager,
		pohManager:       pohManager,
		logger:           logger,
		errorFramework:   errorFramework,
	}
}

// ValidateCreator validates that a creator is an active validator
func (vf *ValidationFramework) ValidateCreator(creator [32]byte) *ValidationResult {
	if !vf.validatorManager.IsActiveValidator(creator) {
		err := vf.errorFramework.CreateError(
			ErrInvalidValidator,
			ErrorCategoryTemporary,
			"creator is not an active validator",
		).WithValidatorID(creator)

		return NewValidationResult(false, err).
			WithContext("creator", fmt.Sprintf("%x", creator)).
			WithContextBool("isActive", false)
	}

	stake := vf.validatorManager.GetValidatorStake(creator)
	return NewValidationResult(true, nil).
		WithContext("creator", fmt.Sprintf("%x", creator)).
		WithContextUint64("stake", stake).
		WithContextBool("isActive", true)
}

// ValidateEvent performs comprehensive validation of an event
func (vf *ValidationFramework) ValidateEvent(event *types.Event) *ValidationResult {
	if event == nil {
		err := vf.errorFramework.CreateError(
			ErrEventValidationFailed,
			ErrorCategoryTemporary,
			"event is nil",
		)
		return NewValidationResult(false, err)
	}

	// Validate creator
	creatorResult := vf.ValidateCreator(event.Creator)
	if !creatorResult.Valid {
		return creatorResult.WithContext("eventID", fmt.Sprintf("%x", event.ID))
	}

	// Validate timestamp
	timestampResult := vf.ValidateTimestamp(event.Timestamp)
	if !timestampResult.Valid {
		return timestampResult.WithContext("eventID", fmt.Sprintf("%x", event.ID))
	}

	// Validate PoH information
	pohResult := vf.ValidatePoH(event.PoHState, event.Data, event.PoHProof, event.PoHCount)
	if !pohResult.Valid {
		return pohResult.WithContext("eventID", fmt.Sprintf("%x", event.ID))
	}

	// Validate event height
	if event.Height == 0 {
		err := vf.errorFramework.CreateError(
			ErrEventValidationFailed,
			ErrorCategoryTemporary,
			"event height cannot be zero",
		).WithEventID(event.ID)

		return NewValidationResult(false, err).
			WithContext("eventID", fmt.Sprintf("%x", event.ID)).
			WithContextUint64("height", event.Height)
	}

	// All validations passed
	return NewValidationResult(true, nil).
		WithContext("eventID", fmt.Sprintf("%x", event.ID)).
		WithContext("creator", fmt.Sprintf("%x", event.Creator)).
		WithContextUint64("height", event.Height).
		WithContextInt("dataSize", len(event.Data))
}

// ValidatePoH validates Proof of History information
func (vf *ValidationFramework) ValidatePoH(
	state [32]byte,
	data []byte,
	proof [32]byte,
	count uint64,
) *ValidationResult {
	currentCount := vf.pohManager.GetCount()
	currentState := vf.pohManager.GetState()

	// Validate PoH count is not too far ahead
	const maxDrift = uint64(100) // Allow some drift for network delays
	if count > currentCount+maxDrift {
		err := vf.errorFramework.CreateError(
			ErrPoHDriftExceeded,
			ErrorCategoryTemporary,
			fmt.Sprintf("PoH count too far ahead: %d > %d + %d", count, currentCount, maxDrift),
		).WithContext("eventCount", fmt.Sprintf("%d", count)).
			WithContext("currentCount", fmt.Sprintf("%d", currentCount)).
			WithContext("maxDrift", fmt.Sprintf("%d", maxDrift))

		return NewValidationResult(false, err).
			WithContextUint64("eventCount", count).
			WithContextUint64("currentCount", currentCount).
			WithContextUint64("drift", count-currentCount)
	}

	// Verify PoH proof
	if !vf.pohManager.Verify(state, data, proof, count) {
		err := vf.errorFramework.CreateError(
			ErrPoHVerificationFailed,
			ErrorCategoryByzantine,
			"PoH verification failed",
		).WithContext("eventCount", fmt.Sprintf("%d", count)).
			WithContext("currentCount", fmt.Sprintf("%d", currentCount)).
			WithContext("state", fmt.Sprintf("%x", state)).
			WithContext("currentState", fmt.Sprintf("%x", currentState))

		return NewValidationResult(false, err).
			WithContextUint64("eventCount", count).
			WithContextUint64("currentCount", currentCount).
			WithContextBool("verified", false)
	}

	return NewValidationResult(true, nil).
		WithContextUint64("eventCount", count).
		WithContextUint64("currentCount", currentCount).
		WithContextBool("verified", true)
}

// ValidateTimestamp validates that a timestamp is reasonable
func (vf *ValidationFramework) ValidateTimestamp(timestamp time.Time) *ValidationResult {
	now := consensus.ConsensusNow()

	// Check if timestamp is zero
	if timestamp.IsZero() {
		err := vf.errorFramework.CreateError(
			ErrEventValidationFailed,
			ErrorCategoryTemporary,
			"timestamp is zero",
		)
		return NewValidationResult(false, err).
			WithContextTime("timestamp", timestamp).
			WithContextBool("isZero", true)
	}

	// Check if timestamp is too far in the future (allow 5 second tolerance)
	const futureTolerance = 5 * time.Second
	if timestamp.After(now.Add(futureTolerance)) {
		err := vf.errorFramework.CreateError(
			ErrEventValidationFailed,
			ErrorCategoryTemporary,
			"timestamp is too far in the future",
		).WithContext("timestamp", timestamp.Format(time.RFC3339)).
			WithContext("currentTime", now.Format(time.RFC3339)).
			WithContext("tolerance", futureTolerance.String())

		return NewValidationResult(false, err).
			WithContextTime("timestamp", timestamp).
			WithContextTime("currentTime", now).
			WithContext("futureOffset", timestamp.Sub(now).String())
	}

	// Check if timestamp is too far in the past (allow 1 hour tolerance)
	const pastTolerance = 1 * time.Hour
	if timestamp.Before(now.Add(-pastTolerance)) {
		err := vf.errorFramework.CreateError(
			ErrEventValidationFailed,
			ErrorCategoryTemporary,
			"timestamp is too far in the past",
		).WithContext("timestamp", timestamp.Format(time.RFC3339)).
			WithContext("currentTime", now.Format(time.RFC3339)).
			WithContext("tolerance", pastTolerance.String())

		return NewValidationResult(false, err).
			WithContextTime("timestamp", timestamp).
			WithContextTime("currentTime", now).
			WithContext("pastOffset", now.Sub(timestamp).String())
	}

	return NewValidationResult(true, nil).
		WithContextTime("timestamp", timestamp).
		WithContextTime("currentTime", now).
		WithContextBool("valid", true)
}

// ValidateStake validates that a stake amount is reasonable
func (vf *ValidationFramework) ValidateStake(stake uint64, validatorID [32]byte) *ValidationResult {
	// Check minimum stake
	const minStake = uint64(1000) // Minimum stake requirement
	if stake < minStake {
		err := vf.errorFramework.CreateError(
			ErrInsufficientStake,
			ErrorCategoryTemporary,
			fmt.Sprintf("stake %d is below minimum %d", stake, minStake),
		).WithValidatorID(validatorID).
			WithContext("stake", fmt.Sprintf("%d", stake)).
			WithContext("minStake", fmt.Sprintf("%d", minStake))

		return NewValidationResult(false, err).
			WithContextUint64("stake", stake).
			WithContextUint64("minStake", minStake).
			WithContext("validatorID", fmt.Sprintf("%x", validatorID))
	}

	// Check maximum stake (prevent single validator from having too much power)
	totalStake := vf.validatorManager.GetTotalStake()
	maxStakeRatio := 0.33 // Maximum 33% of total stake
	maxStake := uint64(float64(totalStake) * maxStakeRatio)

	if totalStake > 0 && stake > maxStake {
		err := vf.errorFramework.CreateError(
			ErrInvalidValidator,
			ErrorCategoryTemporary,
			fmt.Sprintf("stake %d exceeds maximum allowed %d (%.1f%% of total)", stake, maxStake, maxStakeRatio*100),
		).WithValidatorID(validatorID).
			WithContext("stake", fmt.Sprintf("%d", stake)).
			WithContext("maxStake", fmt.Sprintf("%d", maxStake)).
			WithContext("totalStake", fmt.Sprintf("%d", totalStake))

		return NewValidationResult(false, err).
			WithContextUint64("stake", stake).
			WithContextUint64("maxStake", maxStake).
			WithContextUint64("totalStake", totalStake).
			WithContext("validatorID", fmt.Sprintf("%x", validatorID))
	}

	return NewValidationResult(true, nil).
		WithContextUint64("stake", stake).
		WithContextUint64("totalStake", totalStake).
		WithContext("validatorID", fmt.Sprintf("%x", validatorID))
}

// ValidateBlockNumber validates that a block number is sequential and reasonable
func (vf *ValidationFramework) ValidateBlockNumber(blockNumber, expectedNumber uint64) *ValidationResult {
	if blockNumber != expectedNumber {
		err := vf.errorFramework.CreateError(
			ErrInvalidBlockNumber,
			ErrorCategoryTemporary,
			fmt.Sprintf("invalid block number: expected %d, got %d", expectedNumber, blockNumber),
		).WithBlockNumber(blockNumber).
			WithContext("expectedNumber", fmt.Sprintf("%d", expectedNumber))

		return NewValidationResult(false, err).
			WithContextUint64("blockNumber", blockNumber).
			WithContextUint64("expectedNumber", expectedNumber).
			WithContextInt64("gap", int64(blockNumber)-int64(expectedNumber))
	}

	return NewValidationResult(true, nil).
		WithContextUint64("blockNumber", blockNumber).
		WithContextUint64("expectedNumber", expectedNumber)
}

// ValidateEventExists checks if an event exists in the given collections
func (vf *ValidationFramework) ValidateEventExists(
	eventID [32]byte,
	pendingEvents map[[32]byte]*types.Event,
	finalizedEvents map[[32]byte]*types.Event,
) *ValidationResult {
	// Check pending events
	if event, exists := pendingEvents[eventID]; exists {
		return NewValidationResult(true, nil).
			WithContext("eventID", fmt.Sprintf("%x", eventID)).
			WithContext("status", "pending").
			WithContextUint64("height", event.Height)
	}

	// Check finalized events
	if event, exists := finalizedEvents[eventID]; exists {
		return NewValidationResult(true, nil).
			WithContext("eventID", fmt.Sprintf("%x", eventID)).
			WithContext("status", "finalized").
			WithContextUint64("height", event.Height)
	}

	// Event not found
	err := vf.errorFramework.CreateError(
		ErrEventValidationFailed,
		ErrorCategoryTemporary,
		"event not found",
	).WithEventID(eventID)

	return NewValidationResult(false, err).
		WithContext("eventID", fmt.Sprintf("%x", eventID)).
		WithContext("status", "not_found")
}

// ValidateEventDuplicate checks if an event is a duplicate
func (vf *ValidationFramework) ValidateEventDuplicate(
	eventID [32]byte,
	pendingEvents map[[32]byte]*types.Event,
	finalizedEvents map[[32]byte]*types.Event,
) *ValidationResult {
	// Check pending events
	if event, exists := pendingEvents[eventID]; exists {
		err := vf.errorFramework.CreateError(
			ErrEventDuplicate,
			ErrorCategoryTemporary,
			"duplicate event found in pending",
		).WithEventID(eventID).
			WithContext("duplicateHeight", fmt.Sprintf("%d", event.Height))

		return NewValidationResult(false, err).
			WithContext("eventID", fmt.Sprintf("%x", eventID)).
			WithContext("duplicateStatus", "pending").
			WithContextUint64("duplicateHeight", event.Height)
	}

	// Check finalized events
	if event, exists := finalizedEvents[eventID]; exists {
		err := vf.errorFramework.CreateError(
			ErrEventDuplicate,
			ErrorCategoryTemporary,
			"duplicate event found in finalized",
		).WithEventID(eventID).
			WithContext("duplicateHeight", fmt.Sprintf("%d", event.Height))

		return NewValidationResult(false, err).
			WithContext("eventID", fmt.Sprintf("%x", eventID)).
			WithContext("duplicateStatus", "finalized").
			WithContextUint64("duplicateHeight", event.Height)
	}

	// No duplicate found
	return NewValidationResult(true, nil).
		WithContext("eventID", fmt.Sprintf("%x", eventID)).
		WithContextBool("isDuplicate", false)
}

// ValidateParentEvents validates that all parent events exist and are valid
func (vf *ValidationFramework) ValidateParentEvents(
	parentIDs [][32]byte,
	pendingEvents map[[32]byte]*types.Event,
	finalizedEvents map[[32]byte]*types.Event,
) *ValidationResult {
	if len(parentIDs) == 0 {
		// No parents is valid for genesis events
		return NewValidationResult(true, nil).
			WithContextInt("parentCount", 0).
			WithContextBool("isGenesis", true)
	}

	missingParents := []string{}
	validParents := []string{}

	for _, parentID := range parentIDs {
		parentExists := vf.ValidateEventExists(parentID, pendingEvents, finalizedEvents)
		if !parentExists.Valid {
			missingParents = append(missingParents, fmt.Sprintf("%x", parentID))
		} else {
			validParents = append(validParents, fmt.Sprintf("%x", parentID))
		}
	}

	// If we have missing parents, this might not be fatal
	// (parents might exist in Lachesis but not in local tracking)
	if len(missingParents) > 0 {
		vf.logger.Warn("Event references unknown parents",
			"missingParents", missingParents,
			"validParents", validParents,
			"totalParents", len(parentIDs))

		// Return warning but not error
		return NewValidationResult(true, nil).
			WithContextInt("parentCount", len(parentIDs)).
			WithContext("missingParents", fmt.Sprintf("%v", missingParents)).
			WithContext("validParents", fmt.Sprintf("%v", validParents)).
			WithContextBool("hasWarning", true)
	}

	return NewValidationResult(true, nil).
		WithContextInt("parentCount", len(parentIDs)).
		WithContext("validParents", fmt.Sprintf("%v", validParents)).
		WithContextBool("allParentsValid", true)
}
