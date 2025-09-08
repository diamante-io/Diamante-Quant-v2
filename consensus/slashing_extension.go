// consensus/slashing_extension.go

package consensus

import (
	"diamante/consensus/types"
	dtypes "diamante/types"
)

// SlashingExtension provides a way to integrate slashing functionality with HybridConsensus
// without modifying the HybridConsensus struct directly
type SlashingExtension struct {
	// The HybridConsensus instance
	consensus *HybridConsensus

	// The SlashingIntegration instance
	slashing *SlashingIntegration
}

// NewSlashingExtension creates a new SlashingExtension
func NewSlashingExtension(consensus *HybridConsensus) *SlashingExtension {
	// Create the SlashingIntegration
	slashing := NewSlashingIntegration(consensus)

	// Create the SlashingExtension
	extension := &SlashingExtension{
		consensus: consensus,
		slashing:  slashing,
	}

	// Return the extension
	return extension
}

// ProcessBlock processes a block for slashing purposes
func (se *SlashingExtension) ProcessBlock(blockNumber uint64) error {
	// Get the block producer and participants
	// In a real implementation, we would get this information from the block
	// For now, we'll use dummy values
	var blockProducer [32]byte
	var participants [][32]byte

	// Call the SlashingIntegration's ProcessBlock method
	se.slashing.ProcessBlock(blockNumber, blockProducer, participants)

	return nil
}

// CheckValidatorJailed checks if a validator is jailed before creating an event
func (se *SlashingExtension) CheckValidatorJailed(creator [32]byte) bool {
	// Check if the validator is jailed
	if se.slashing.IsValidatorJailed(creator) {
		se.consensus.logger.Warn("Jailed validator attempted to create event",
			ValidatorIDField(creator),
			LogField{Key: "jailTimeRemaining", Value: dtypes.NewValue(dtypes.ValueTypeString, []byte(se.slashing.GetJailTimeRemaining(creator).String()))})
		return true
	}

	return false
}

// DetectDoubleSigningEvidence detects double signing evidence from events
func (se *SlashingExtension) DetectDoubleSigningEvidence(events []*types.Event) {
	se.slashing.DetectDoubleSigningEvidence(events)
}

// DetectDowntime detects validator downtime
func (se *SlashingExtension) DetectDowntime(blockHeight uint64, activeValidators []*types.Validator, participants [][32]byte) {
	se.slashing.DetectDowntime(blockHeight, activeValidators, participants)
}

// MonitorValidatorPerformance monitors validator performance
func (se *SlashingExtension) MonitorValidatorPerformance(blockHeight uint64) {
	se.slashing.MonitorValidatorPerformance(blockHeight)
}

// GetSlashingManager returns the underlying SlashingManager
func (se *SlashingExtension) GetSlashingManager() *SlashingManager {
	return se.slashing.slashingManager
}

// GetSlashingIntegration returns the underlying SlashingIntegration
func (se *SlashingExtension) GetSlashingIntegration() *SlashingIntegration {
	return se.slashing
}

// IntegrateSlashingWithHybridConsensus integrates slashing with HybridConsensus
// This function should be called from main.go after creating the HybridConsensus instance
func IntegrateSlashingWithHybridConsensus(hc *HybridConsensus) *SlashingExtension {
	// Create the SlashingExtension
	extension := NewSlashingExtension(hc)

	// Log that we've integrated slashing
	hc.logger.Info("Slashing integration installed successfully")

	// Return the extension
	return extension
}

// Example usage in main.go:
//
// // Create HybridConsensus
// hc := NewHybridConsensus(...)
//
// // Integrate slashing
// slashingExt := consensus.IntegrateSlashingWithHybridConsensus(hc)
//
// // Use slashing functionality
// hc.ProcessBlock = func(blockNumber uint64) error {
//     // Call original ProcessBlock
//     err := originalProcessBlock(blockNumber)
//     if err != nil {
//         return err
//     }
//
//     // Process block for slashing
//     slashingExt.ProcessBlock(blockNumber)
//
//     return nil
// }
