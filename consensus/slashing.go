// consensus/slashing.go

package consensus

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"diamante/consensus/types"
)

// SlashingReason defines the reason for slashing a validator
type SlashingReason int

const (
	// DoubleSigning occurs when a validator signs two conflicting blocks/events
	DoubleSigning SlashingReason = iota
	// Downtime occurs when a validator is offline for too long
	Downtime
	// MaliciousBehavior occurs when a validator exhibits malicious behavior
	MaliciousBehavior
	// LowParticipation occurs when a validator has low participation rate
	LowParticipation
)

// String returns the string representation of a slashing reason
func (r SlashingReason) String() string {
	switch r {
	case DoubleSigning:
		return "DoubleSigning"
	case Downtime:
		return "Downtime"
	case MaliciousBehavior:
		return "MaliciousBehavior"
	case LowParticipation:
		return "LowParticipation"
	default:
		return "Unknown"
	}
}

// SlashingConfig defines the configuration for the slashing mechanism
type SlashingConfig struct {
	// Enabled determines if slashing is enabled
	Enabled bool
	// DowntimeThreshold is the number of consecutive blocks a validator can miss before being slashed
	DowntimeThreshold uint64
	// DowntimeSlashingPercentage is the percentage of stake to slash for downtime
	DowntimeSlashingPercentage float64
	// DoubleSigningSlashingPercentage is the percentage of stake to slash for double signing
	DoubleSigningSlashingPercentage float64
	// MaliciousBehaviorSlashingPercentage is the percentage of stake to slash for malicious behavior
	MaliciousBehaviorSlashingPercentage float64
	// LowParticipationThreshold is the minimum participation rate required
	LowParticipationThreshold float64
	// LowParticipationSlashingPercentage is the percentage of stake to slash for low participation
	LowParticipationSlashingPercentage float64
	// JailTime is the duration a validator is jailed after being slashed
	JailTime time.Duration
	// AppealEnabled determines if validators can appeal slashing decisions
	AppealEnabled bool
	// AppealTimeWindow is the time window for appealing a slashing decision
	AppealTimeWindow time.Duration
}

// DefaultSlashingConfig returns the default slashing configuration
func DefaultSlashingConfig() *SlashingConfig {
	return &SlashingConfig{
		Enabled:                             true,
		DowntimeThreshold:                   100,
		DowntimeSlashingPercentage:          0.01, // 1% of stake
		DoubleSigningSlashingPercentage:     0.10, // 10% of stake
		MaliciousBehaviorSlashingPercentage: 0.20, // 20% of stake
		LowParticipationThreshold:           0.80, // 80% participation required
		LowParticipationSlashingPercentage:  0.05, // 5% of stake
		JailTime:                            24 * time.Hour,
		AppealEnabled:                       true,
		AppealTimeWindow:                    48 * time.Hour,
	}
}

// SlashingEvidence represents evidence for slashing
type SlashingEvidence struct {
	// ValidatorID is the ID of the validator to be slashed
	ValidatorID [32]byte
	// Reason is the reason for slashing
	Reason SlashingReason
	// Evidence is the evidence for slashing
	Evidence SlashingEvidenceData
	// Timestamp is the time the evidence was submitted
	Timestamp time.Time
	// BlockHeight is the block height at which the evidence was submitted
	BlockHeight uint64
	// Reporter is the ID of the validator who reported the evidence (if any)
	Reporter [32]byte
}

// SlashingEvidenceData represents the actual evidence data
type SlashingEvidenceData struct {
	Type        string            `json:"type"`
	BlockHash   string            `json:"block_hash,omitempty"`
	Signatures  []string          `json:"signatures,omitempty"`
	Timestamps  []time.Time       `json:"timestamps,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
	Description string            `json:"description"`
}

// SlashingRecord represents a record of slashing
type SlashingRecord struct {
	// ValidatorID is the ID of the slashed validator
	ValidatorID [32]byte
	// Reason is the reason for slashing
	Reason SlashingReason
	// Amount is the amount of stake slashed
	Amount uint64
	// Timestamp is the time the validator was slashed
	Timestamp time.Time
	// BlockHeight is the block height at which the validator was slashed
	BlockHeight uint64
	// JailedUntil is the time until which the validator is jailed
	JailedUntil time.Time
	// Evidence is the evidence that led to slashing
	Evidence SlashingEvidenceData
	// AppealDeadline is the deadline for appealing the slashing decision
	AppealDeadline time.Time
	// Appealed indicates if the validator has appealed the slashing decision
	Appealed bool
	// AppealResolved indicates if the appeal has been resolved
	AppealResolved bool
	// AppealOutcome indicates the outcome of the appeal (true = successful, false = unsuccessful)
	AppealOutcome bool
}

// Appeal represents an appeal for a slashing decision
type Appeal struct {
	// ValidatorID is the ID of the validator appealing
	ValidatorID [32]byte
	// SlashingRecord is the slashing record being appealed
	SlashingRecord *SlashingRecord
	// Reason is the reason for the appeal
	Reason string
	// Evidence is the evidence supporting the appeal
	Evidence SlashingEvidenceData
	// Timestamp is the time the appeal was submitted
	Timestamp time.Time
	// Resolved indicates if the appeal has been resolved
	Resolved bool
	// Outcome indicates the outcome of the appeal (true = successful, false = unsuccessful)
	Outcome bool
	// ResolvedTimestamp is the time the appeal was resolved
	ResolvedTimestamp time.Time
	// Resolver is the ID of the entity that resolved the appeal
	Resolver [32]byte
}

// SlashingManager manages the slashing of validators
type SlashingManager struct {
	config *SlashingConfig
	logger Logger

	// Mutex for thread safety
	mu sync.RWMutex

	// Maps for tracking validator status
	missedBlocks      map[[32]byte]uint64
	participationRate map[[32]byte]float64
	jailedValidators  map[[32]byte]time.Time
	slashingRecords   map[[32]byte][]*SlashingRecord
	pendingAppeals    map[[32]byte][]*Appeal

	// Treasury for slashed funds
	treasury uint64

	// Consensus reference for validator operations
	consensus types.Consensus
}

// NewSlashingManager creates a new slashing manager
func NewSlashingManager(config *SlashingConfig, logger Logger, consensus types.Consensus) *SlashingManager {
	if config == nil {
		config = DefaultSlashingConfig()
	}

	return &SlashingManager{
		config:            config,
		logger:            logger,
		missedBlocks:      make(map[[32]byte]uint64),
		participationRate: make(map[[32]byte]float64),
		jailedValidators:  make(map[[32]byte]time.Time),
		slashingRecords:   make(map[[32]byte][]*SlashingRecord),
		pendingAppeals:    make(map[[32]byte][]*Appeal),
		consensus:         consensus,
	}
}

// RecordMissedBlock records a missed block for a validator
func (sm *SlashingManager) RecordMissedBlock(validatorID [32]byte, blockHeight uint64) {
	if !sm.config.Enabled {
		return
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Check if validator is already jailed
	if jailTime, jailed := sm.jailedValidators[validatorID]; jailed {
		consensusTime := ConsensusNow()
		if consensusTime.Before(jailTime) {
			// Validator is still jailed, no need to record missed blocks
			return
		}
		// Validator's jail time is over, remove from jailed validators
		delete(sm.jailedValidators, validatorID)
	}

	// Increment missed blocks counter
	sm.missedBlocks[validatorID]++

	// Check if validator has missed too many blocks
	if sm.missedBlocks[validatorID] >= sm.config.DowntimeThreshold {
		// Slash validator for downtime
		evidence := SlashingEvidenceData{
			Type:        "downtime",
			Description: fmt.Sprintf("Validator missed %d consecutive blocks (threshold: %d)", sm.missedBlocks[validatorID], sm.config.DowntimeThreshold),
			Metadata: map[string]string{
				"missedBlocks": fmt.Sprintf("%d", sm.missedBlocks[validatorID]),
				"threshold":    fmt.Sprintf("%d", sm.config.DowntimeThreshold),
				"blockHeight":  fmt.Sprintf("%d", blockHeight),
			},
		}
		sm.slashValidator(validatorID, Downtime, evidence, blockHeight)

		// Reset missed blocks counter
		sm.missedBlocks[validatorID] = 0
	}
}

// RecordParticipation records participation for a validator
func (sm *SlashingManager) RecordParticipation(validatorID [32]byte, participated bool) {
	if !sm.config.Enabled {
		return
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Check if validator is already jailed
	if jailTime, jailed := sm.jailedValidators[validatorID]; jailed {
		consensusTime := ConsensusNow()
		if consensusTime.Before(jailTime) {
			// Validator is still jailed, no need to record participation
			return
		}
		// Validator's jail time is over, remove from jailed validators
		delete(sm.jailedValidators, validatorID)
	}

	// Update participation rate using exponential moving average
	rate, exists := sm.participationRate[validatorID]
	if !exists {
		// Initialize with current participation
		if participated {
			sm.participationRate[validatorID] = 1.0
		} else {
			sm.participationRate[validatorID] = 0.0
		}
	} else {
		// Update using EMA with alpha = 0.1
		alpha := 0.1
		newRate := rate
		if participated {
			newRate = rate*(1-alpha) + 1.0*alpha
		} else {
			newRate = rate * (1 - alpha)
		}
		sm.participationRate[validatorID] = newRate
	}

	// Check if participation rate is too low
	if sm.participationRate[validatorID] < sm.config.LowParticipationThreshold {
		// Use a default block height since we can't get it directly
		// In a real implementation, this would be passed in or tracked internally
		blockHeight := uint64(0)

		// Slash validator for low participation
		evidence := SlashingEvidenceData{
			Type:        "low_participation",
			Description: fmt.Sprintf("Validator participation rate %.2f%% below threshold %.2f%%", sm.participationRate[validatorID]*100, sm.config.LowParticipationThreshold*100),
			Metadata: map[string]string{
				"participationRate": fmt.Sprintf("%.4f", sm.participationRate[validatorID]),
				"threshold":         fmt.Sprintf("%.4f", sm.config.LowParticipationThreshold),
				"blockHeight":       fmt.Sprintf("%d", blockHeight),
			},
		}
		sm.slashValidator(validatorID, LowParticipation, evidence, blockHeight)

		// Reset participation rate
		sm.participationRate[validatorID] = 1.0
	}
}

// ReportDoubleSigning reports a validator for double signing
func (sm *SlashingManager) ReportDoubleSigning(validatorID [32]byte, evidence SlashingEvidenceData, reporter [32]byte) error {
	if !sm.config.Enabled {
		return errors.New("slashing is disabled")
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Verify evidence
	if evidence.Type == "" {
		return errors.New("evidence type cannot be empty")
	}

	// Use a default block height since we can't get it directly
	// In a real implementation, this would be passed in or tracked internally
	blockHeight := uint64(0)

	// Slash validator for double signing
	sm.slashValidator(validatorID, DoubleSigning, evidence, blockHeight)

	return nil
}

// ReportMaliciousBehavior reports a validator for malicious behavior
func (sm *SlashingManager) ReportMaliciousBehavior(validatorID [32]byte, evidence SlashingEvidenceData, reporter [32]byte) error {
	if !sm.config.Enabled {
		return errors.New("slashing is disabled")
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Verify evidence
	if evidence.Type == "" {
		return errors.New("evidence type cannot be empty")
	}

	// Use a default block height since we can't get it directly
	// In a real implementation, this would be passed in or tracked internally
	blockHeight := uint64(0)

	// Slash validator for malicious behavior
	sm.slashValidator(validatorID, MaliciousBehavior, evidence, blockHeight)

	return nil
}

// slashValidator slashes a validator for the given reason
func (sm *SlashingManager) slashValidator(validatorID [32]byte, reason SlashingReason, evidence SlashingEvidenceData, blockHeight uint64) {
	// Find the validator in the active validators
	var validator *types.Validator
	for _, v := range sm.consensus.GetActiveValidators() {
		if v.ID == validatorID {
			validator = v
			break
		}
	}

	// If not found in active validators, check all validators
	if validator == nil {
		for _, v := range sm.consensus.GetValidators() {
			if v.ID == validatorID {
				validator = v
				break
			}
		}
	}

	// If still not found, log error and return
	if validator == nil {
		sm.logger.Error("Failed to slash validator: validator not found", "validatorID", fmt.Sprintf("%x", validatorID))
		return
	}

	// Calculate slashing amount based on reason
	slashingPercentage := 0.0
	switch reason {
	case DoubleSigning:
		slashingPercentage = sm.config.DoubleSigningSlashingPercentage
	case Downtime:
		slashingPercentage = sm.config.DowntimeSlashingPercentage
	case MaliciousBehavior:
		slashingPercentage = sm.config.MaliciousBehaviorSlashingPercentage
	case LowParticipation:
		slashingPercentage = sm.config.LowParticipationSlashingPercentage
	}

	// Convert percentage to fixed-point (e.g., 0.1 = 10%)
	slashingPercentageFP := NewFixedPointFromRatio(uint64(slashingPercentage*1000000), 1000000, DefaultPrecision)
	stakeFP := NewFixedPointFromUint64(validator.Stake, DefaultPrecision)
	slashAmountFP := stakeFP.Mul(slashingPercentageFP)
	slashAmount := slashAmountFP.ToUint64()

	if slashAmount == 0 {
		slashAmount = 1 // Minimum slashing amount
	}

	// Ensure we don't slash more than the validator's stake
	if slashAmount > validator.Stake {
		slashAmount = validator.Stake
	}

	// Update validator stake
	newStake := validator.Stake - slashAmount

	// Use DPoS to update stake
	sm.consensus.GetDPoS().UpdateStake(validatorID, newStake)

	// Add slashed amount to treasury
	sm.treasury += slashAmount

	// Jail validator
	consensusTime := ConsensusNow()
	jailUntil := consensusTime.Add(sm.config.JailTime)
	sm.jailedValidators[validatorID] = jailUntil

	// Create slashing record
	record := &SlashingRecord{
		ValidatorID:    validatorID,
		Reason:         reason,
		Amount:         slashAmount,
		Timestamp:      consensusTime,
		BlockHeight:    blockHeight,
		JailedUntil:    jailUntil,
		Evidence:       evidence,
		AppealDeadline: consensusTime.Add(sm.config.AppealTimeWindow),
		Appealed:       false,
		AppealResolved: false,
		AppealOutcome:  false,
	}

	// Add record to slashing records
	sm.slashingRecords[validatorID] = append(sm.slashingRecords[validatorID], record)

	sm.logger.Info("Validator slashed",
		"validatorID", fmt.Sprintf("%x", validatorID),
		"reason", reason.String(),
		"amount", slashAmount,
		"newStake", newStake,
		"jailedUntil", jailUntil)
}

// SubmitAppeal submits an appeal for a slashing decision
func (sm *SlashingManager) SubmitAppeal(validatorID [32]byte, recordIndex int, reason string, evidence SlashingEvidenceData) error {
	if !sm.config.Enabled || !sm.config.AppealEnabled {
		return errors.New("appeals are disabled")
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Check if validator has slashing records
	records, exists := sm.slashingRecords[validatorID]
	if !exists || len(records) <= recordIndex {
		return fmt.Errorf("no slashing record found for validator %x at index %d", validatorID, recordIndex)
	}

	record := records[recordIndex]

	// Check if appeal deadline has passed
	consensusTime := ConsensusNow()
	if consensusTime.After(record.AppealDeadline) {
		return fmt.Errorf("appeal deadline has passed for slashing record at index %d", recordIndex)
	}

	// Check if record has already been appealed
	if record.Appealed {
		return fmt.Errorf("slashing record at index %d has already been appealed", recordIndex)
	}

	// Create appeal
	appeal := &Appeal{
		ValidatorID:       validatorID,
		SlashingRecord:    record,
		Reason:            reason,
		Evidence:          evidence,
		Timestamp:         ConsensusNow(),
		Resolved:          false,
		Outcome:           false,
		ResolvedTimestamp: time.Time{},
		Resolver:          [32]byte{},
	}

	// Mark record as appealed
	record.Appealed = true

	// Add appeal to pending appeals
	sm.pendingAppeals[validatorID] = append(sm.pendingAppeals[validatorID], appeal)

	sm.logger.Info("Appeal submitted",
		"validatorID", fmt.Sprintf("%x", validatorID),
		"slashingReason", record.Reason.String(),
		"appealReason", reason)

	return nil
}

// ResolveAppeal resolves an appeal
func (sm *SlashingManager) ResolveAppeal(validatorID [32]byte, appealIndex int, outcome bool, resolver [32]byte) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Check if validator has pending appeals
	appeals, exists := sm.pendingAppeals[validatorID]
	if !exists || len(appeals) <= appealIndex {
		return fmt.Errorf("no pending appeal found for validator %x at index %d", validatorID, appealIndex)
	}

	appeal := appeals[appealIndex]

	// Check if appeal has already been resolved
	if appeal.Resolved {
		return fmt.Errorf("appeal at index %d has already been resolved", appealIndex)
	}

	// Resolve appeal
	appeal.Resolved = true
	appeal.Outcome = outcome
	appeal.ResolvedTimestamp = ConsensusNow()
	appeal.Resolver = resolver

	// Update slashing record
	appeal.SlashingRecord.AppealResolved = true
	appeal.SlashingRecord.AppealOutcome = outcome

	// If appeal was successful, restore slashed stake
	if outcome {
		// Find the validator in the active validators
		var validator *types.Validator
		for _, v := range sm.consensus.GetActiveValidators() {
			if v.ID == validatorID {
				validator = v
				break
			}
		}

		// If not found in active validators, check all validators
		if validator == nil {
			for _, v := range sm.consensus.GetValidators() {
				if v.ID == validatorID {
					validator = v
					break
				}
			}
		}

		// If still not found, return error
		if validator == nil {
			return fmt.Errorf("validator %x not found", validatorID)
		}

		// Restore stake
		newStake := validator.Stake + appeal.SlashingRecord.Amount
		sm.consensus.GetDPoS().UpdateStake(validatorID, newStake)

		// Remove from treasury
		sm.treasury -= appeal.SlashingRecord.Amount

		// Unjail validator
		delete(sm.jailedValidators, validatorID)

		sm.logger.Info("Appeal resolved successfully, stake restored",
			"validatorID", fmt.Sprintf("%x", validatorID),
			"restoredAmount", appeal.SlashingRecord.Amount,
			"newStake", newStake)
	} else {
		sm.logger.Info("Appeal resolved unsuccessfully",
			"validatorID", fmt.Sprintf("%x", validatorID))
	}

	return nil
}

// IsValidatorJailed checks if a validator is jailed
func (sm *SlashingManager) IsValidatorJailed(validatorID [32]byte) bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	if jailTime, exists := sm.jailedValidators[validatorID]; exists {
		return ConsensusNow().Before(jailTime)
	}
	return false
}

// GetJailTimeRemaining returns the remaining jail time for a validator
func (sm *SlashingManager) GetJailTimeRemaining(validatorID [32]byte) time.Duration {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	if jailTime, exists := sm.jailedValidators[validatorID]; exists {
		remaining := jailTime.Sub(ConsensusNow())
		if remaining > 0 {
			return remaining
		}
	}
	return 0
}

// GetSlashingRecords returns the slashing records for a validator
func (sm *SlashingManager) GetSlashingRecords(validatorID [32]byte) []*SlashingRecord {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	records, exists := sm.slashingRecords[validatorID]
	if !exists {
		return nil
	}
	return records
}

// GetPendingAppeals returns the pending appeals for a validator
func (sm *SlashingManager) GetPendingAppeals(validatorID [32]byte) []*Appeal {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	appeals, exists := sm.pendingAppeals[validatorID]
	if !exists {
		return nil
	}
	return appeals
}

// GetTreasuryBalance returns the current treasury balance
func (sm *SlashingManager) GetTreasuryBalance() uint64 {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	return sm.treasury
}

// ProcessBlock processes a block for slashing purposes
func (sm *SlashingManager) ProcessBlock(blockHeight uint64, blockProducer [32]byte, participants [][32]byte) {
	if !sm.config.Enabled {
		return
	}

	// Get all active validators
	activeValidators := sm.consensus.GetActiveValidators()

	// Create a map for quick lookup of participants
	participantMap := make(map[[32]byte]bool)
	for _, participant := range participants {
		participantMap[participant] = true
	}

	// Check each active validator
	for _, validator := range activeValidators {
		// Skip the block producer
		if validator.ID == blockProducer {
			continue
		}

		// Check if validator participated
		participated := participantMap[validator.ID]

		// Record participation
		sm.RecordParticipation(validator.ID, participated)

		// If validator didn't participate, record missed block
		if !participated {
			sm.RecordMissedBlock(validator.ID, blockHeight)
		}
	}
}

// UnjailValidator manually unjails a validator (requires authority)
func (sm *SlashingManager) UnjailValidator(validatorID [32]byte, authority [32]byte) error {
	if !sm.config.Enabled {
		return errors.New("slashing is disabled")
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Check if validator is jailed
	if _, exists := sm.jailedValidators[validatorID]; !exists {
		return fmt.Errorf("validator %x is not jailed", validatorID)
	}

	// Verify authority (in a real implementation, this would check governance permissions)
	// For now, we'll accept any non-zero authority
	var zeroAuthority [32]byte
	if authority == zeroAuthority {
		return errors.New("invalid authority")
	}

	// Unjail the validator
	delete(sm.jailedValidators, validatorID)

	sm.logger.Info("Validator manually unjailed",
		"validatorID", fmt.Sprintf("%x", validatorID),
		"authority", fmt.Sprintf("%x", authority),
		"timestamp", ConsensusNow())

	return nil
}

// SetSlashingConfig updates the slashing configuration
func (sm *SlashingManager) SetSlashingConfig(config *SlashingConfig) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.config = config
	sm.logger.Info("Slashing configuration updated")
}

// GetSlashingConfig returns the current slashing configuration
func (sm *SlashingManager) GetSlashingConfig() *SlashingConfig {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	return sm.config
}

// WithdrawFromTreasury withdraws funds from the treasury
func (sm *SlashingManager) WithdrawFromTreasury(amount uint64, recipient [32]byte, authority [32]byte) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Check if there are enough funds in the treasury
	if amount > sm.treasury {
		return fmt.Errorf("insufficient funds in treasury: requested %d, available %d", amount, sm.treasury)
	}

	// Withdraw funds
	sm.treasury -= amount

	// In a real implementation, we would transfer the funds to the recipient
	// For now, we just log the withdrawal
	sm.logger.Info("Funds withdrawn from treasury",
		"amount", amount,
		"recipient", fmt.Sprintf("%x", recipient),
		"authority", fmt.Sprintf("%x", authority),
		"remainingBalance", sm.treasury)

	return nil
}
