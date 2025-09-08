// consensus/slashing_integration.go

package consensus

import (
	"fmt"
	"time"

	"diamante/consensus/types"
)

// slashingLoggerAdapter adapts the hybridConsensusLogger to the Logger interface
type slashingLoggerAdapter struct {
	logger *hybridConsensusLogger
}

// Info implements the Logger interface
func (sla *slashingLoggerAdapter) Info(msg string, keyvals ...interface{}) {
	// Convert interface{} keyvals to LogKeyValue
	var logFields []LogKeyValue
	for i := 0; i < len(keyvals); i += 2 {
		if i+1 < len(keyvals) {
			key := fmt.Sprintf("%v", keyvals[i])
			value := fmt.Sprintf("%v", keyvals[i+1])
			logFields = append(logFields, LogKeyValue{Key: key, Value: value})
		}
	}
	sla.logger.Info(msg, logFields...)
}

// Error implements the Logger interface
func (sla *slashingLoggerAdapter) Error(msg string, keyvals ...interface{}) {
	// Convert interface{} keyvals to LogKeyValue
	var logFields []LogKeyValue
	for i := 0; i < len(keyvals); i += 2 {
		if i+1 < len(keyvals) {
			key := fmt.Sprintf("%v", keyvals[i])
			value := fmt.Sprintf("%v", keyvals[i+1])
			logFields = append(logFields, LogKeyValue{Key: key, Value: value})
		}
	}
	sla.logger.Error(msg, logFields...)
}

// Warn implements the Logger interface
func (sla *slashingLoggerAdapter) Warn(msg string, keyvals ...interface{}) {
	// Convert interface{} keyvals to LogKeyValue
	var logFields []LogKeyValue
	for i := 0; i < len(keyvals); i += 2 {
		if i+1 < len(keyvals) {
			key := fmt.Sprintf("%v", keyvals[i])
			value := fmt.Sprintf("%v", keyvals[i+1])
			logFields = append(logFields, LogKeyValue{Key: key, Value: value})
		}
	}
	sla.logger.Warn(msg, logFields...)
}

// Debug implements the Logger interface
func (sla *slashingLoggerAdapter) Debug(msg string, keyvals ...interface{}) {
	// hybridConsensusLogger doesn't have a Debug method, so we'll use Info instead
	// Convert interface{} keyvals to LogKeyValue
	var logFields []LogKeyValue
	for i := 0; i < len(keyvals); i += 2 {
		if i+1 < len(keyvals) {
			key := fmt.Sprintf("%v", keyvals[i])
			value := fmt.Sprintf("%v", keyvals[i+1])
			logFields = append(logFields, LogKeyValue{Key: key, Value: value})
		}
	}
	sla.logger.Info("DEBUG: "+msg, logFields...)
}

// SlashingIntegration provides methods to integrate the SlashingManager with HybridConsensus
type SlashingIntegration struct {
	slashingManager *SlashingManager
	consensus       *HybridConsensus
	logger          *hybridConsensusLogger
}

// NewSlashingIntegration creates a new SlashingIntegration instance
func NewSlashingIntegration(consensus *HybridConsensus) *SlashingIntegration {
	// Create slashing config
	config := DefaultSlashingConfig()

	// Create a logger adapter that implements the Logger interface
	logger := &slashingLoggerAdapter{logger: consensus.legacyLogger}

	// Create slashing manager
	slashingManager := NewSlashingManager(config, logger, consensus)

	return &SlashingIntegration{
		slashingManager: slashingManager,
		consensus:       consensus,
		logger:          consensus.legacyLogger,
	}
}

// Start initializes the slashing integration
func (si *SlashingIntegration) Start() error {
	si.logger.Info("Starting slashing integration")
	return nil
}

// Stop stops the slashing integration
func (si *SlashingIntegration) Stop() error {
	si.logger.Info("Stopping slashing integration")
	return nil
}

// ProcessBlock processes a block for slashing purposes
func (si *SlashingIntegration) ProcessBlock(blockNumber uint64, blockProducer [32]byte, participants [][32]byte) error {
	si.slashingManager.ProcessBlock(blockNumber, blockProducer, participants)
	return nil
}

// ReportDoubleSigning reports a validator for double signing
func (si *SlashingIntegration) ReportDoubleSigning(validatorID [32]byte, evidence SlashingEvidenceData, reporter [32]byte) error {
	return si.slashingManager.ReportDoubleSigning(validatorID, evidence, reporter)
}

// ReportMaliciousBehavior reports a validator for malicious behavior
func (si *SlashingIntegration) ReportMaliciousBehavior(validatorID [32]byte, evidence SlashingEvidenceData, reporter [32]byte) error {
	return si.slashingManager.ReportMaliciousBehavior(validatorID, evidence, reporter)
}

// IsValidatorJailed checks if a validator is jailed
func (si *SlashingIntegration) IsValidatorJailed(validatorID [32]byte) bool {
	return si.slashingManager.IsValidatorJailed(validatorID)
}

// GetJailTimeRemaining returns the remaining jail time for a validator
func (si *SlashingIntegration) GetJailTimeRemaining(validatorID [32]byte) time.Duration {
	return si.slashingManager.GetJailTimeRemaining(validatorID)
}

// UnjailValidator manually unjails a validator
func (si *SlashingIntegration) UnjailValidator(validatorID [32]byte, authority [32]byte) error {
	return si.slashingManager.UnjailValidator(validatorID, authority)
}

// GetSlashingRecords returns the slashing records for a validator
func (si *SlashingIntegration) GetSlashingRecords(validatorID [32]byte) []*SlashingRecord {
	return si.slashingManager.GetSlashingRecords(validatorID)
}

// GetPendingAppeals returns the pending appeals for a validator
func (si *SlashingIntegration) GetPendingAppeals(validatorID [32]byte) []*Appeal {
	return si.slashingManager.GetPendingAppeals(validatorID)
}

// SubmitAppeal submits an appeal for a slashing decision
func (si *SlashingIntegration) SubmitAppeal(validatorID [32]byte, recordIndex int, reason string, evidence SlashingEvidenceData) error {
	return si.slashingManager.SubmitAppeal(validatorID, recordIndex, reason, evidence)
}

// ResolveAppeal resolves an appeal
func (si *SlashingIntegration) ResolveAppeal(validatorID [32]byte, appealIndex int, outcome bool, resolver [32]byte) error {
	return si.slashingManager.ResolveAppeal(validatorID, appealIndex, outcome, resolver)
}

// GetTreasuryBalance returns the current treasury balance
func (si *SlashingIntegration) GetTreasuryBalance() uint64 {
	return si.slashingManager.GetTreasuryBalance()
}

// WithdrawFromTreasury withdraws funds from the treasury
func (si *SlashingIntegration) WithdrawFromTreasury(amount uint64, recipient [32]byte, authority [32]byte) error {
	return si.slashingManager.WithdrawFromTreasury(amount, recipient, authority)
}

// SetSlashingConfig updates the slashing configuration
func (si *SlashingIntegration) SetSlashingConfig(config *SlashingConfig) {
	si.slashingManager.SetSlashingConfig(config)
}

// GetSlashingConfig returns the current slashing configuration
func (si *SlashingIntegration) GetSlashingConfig() *SlashingConfig {
	return si.slashingManager.GetSlashingConfig()
}

// DetectDoubleSigningEvidence detects double signing evidence from events
func (si *SlashingIntegration) DetectDoubleSigningEvidence(events []*types.Event) {
	// Create a map to track events by creator and height
	eventsByCreatorAndHeight := make(map[string]map[uint64][]*types.Event)

	// Organize events by creator and height
	for _, event := range events {
		creatorStr := fmt.Sprintf("%x", event.Creator)
		if _, exists := eventsByCreatorAndHeight[creatorStr]; !exists {
			eventsByCreatorAndHeight[creatorStr] = make(map[uint64][]*types.Event)
		}
		eventsByCreatorAndHeight[creatorStr][event.Height] = append(eventsByCreatorAndHeight[creatorStr][event.Height], event)
	}

	// Check for double signing
	for creatorStr, heightMap := range eventsByCreatorAndHeight {
		for height, eventsAtHeight := range heightMap {
			if len(eventsAtHeight) > 1 {
				// Found potential double signing
				si.logger.Warn("Potential double signing detected",
					LogKeyValue{Key: "validator", Value: creatorStr},
					LogKeyValue{Key: "height", Value: fmt.Sprintf("%d", height)},
					LogKeyValue{Key: "eventCount", Value: fmt.Sprintf("%d", len(eventsAtHeight))})

				// Convert creator string to byte array
				var creator [32]byte
				fmt.Sscanf(creatorStr, "%x", &creator)

				// Create evidence
				evidence := SlashingEvidenceData{
					Type:        "double_signing",
					Description: fmt.Sprintf("Double signing detected at height %d with %d events", height, len(eventsAtHeight)),
					Metadata: map[string]string{
						"height":     fmt.Sprintf("%d", height),
						"eventCount": fmt.Sprintf("%d", len(eventsAtHeight)),
						"detector":   "automated_consensus_monitor",
					},
				}

				// Report double signing
				if err := si.ReportDoubleSigning(creator, evidence, [32]byte{}); err != nil {
					si.logger.Error("Failed to report double signing", LogKeyValue{Key: "error", Value: err.Error()})
				}
			}
		}
	}
}

// DetectDowntime detects validator downtime
func (si *SlashingIntegration) DetectDowntime(blockHeight uint64, activeValidators []*types.Validator, participants [][32]byte) {
	// Create a map for quick lookup of participants
	participantMap := make(map[string]bool)
	for _, participant := range participants {
		participantMap[fmt.Sprintf("%x", participant)] = true
	}

	// Check each active validator
	for _, validator := range activeValidators {
		validatorStr := fmt.Sprintf("%x", validator.ID)
		if !participantMap[validatorStr] {
			// Validator didn't participate in this block
			si.slashingManager.RecordMissedBlock(validator.ID, blockHeight)
		}
	}
}

// MonitorValidatorPerformance monitors validator performance
func (si *SlashingIntegration) MonitorValidatorPerformance(blockHeight uint64) {
	// This is a placeholder for more sophisticated performance monitoring
	// In a real implementation, this would track various performance metrics
	// and potentially slash validators for poor performance
}
