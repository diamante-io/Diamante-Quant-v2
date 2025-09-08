package fees

import (
	"fmt"
	"sync"

	"diamante/common"

	"github.com/shopspring/decimal"
	"github.com/sirupsen/logrus"
)

// ConsensusIntegration handles the integration between fee distribution and consensus
type ConsensusIntegration struct {
	mu             sync.RWMutex
	feeDistributor FeeDistributorAPI
	logger         *logrus.Logger
	enabled        bool
}

// NewConsensusIntegration creates a new consensus integration handler
func NewConsensusIntegration(fd FeeDistributorAPI, logger *logrus.Logger) *ConsensusIntegration {
	if logger == nil {
		logger = logrus.New()
	}

	return &ConsensusIntegration{
		feeDistributor: fd,
		logger:         logger,
		enabled:        true,
	}
}

// OnBlockFinalized is called when a block is finalized in consensus
// It distributes fees for all transactions in the block
func (ci *ConsensusIntegration) OnBlockFinalized(block *common.Block) error {
	ci.mu.RLock()
	if !ci.enabled {
		ci.mu.RUnlock()
		return nil
	}
	ci.mu.RUnlock()

	if block == nil {
		return fmt.Errorf("block cannot be nil")
	}

	// Convert validator string to [32]byte
	var blockProducer [32]byte
	if len(block.Validator) > 0 {
		copy(blockProducer[:], []byte(block.Validator))
	} else {
		return fmt.Errorf("block validator cannot be empty")
	}

	// Distribute fees for each transaction in the block
	totalFees := decimal.Zero
	errors := 0

	for _, tx := range block.Transactions {
		if err := ci.feeDistributor.DistributeFees(&tx, blockProducer); err != nil {
			ci.logger.WithError(err).WithField("txID", tx.ID).Error("failed to distribute fees for transaction")
			errors++
		} else {
			totalFees = totalFees.Add(decimal.NewFromFloat(tx.Fee))
		}
	}

	ci.logger.WithFields(logrus.Fields{
		"blockNumber":  block.Number,
		"validator":    block.Validator,
		"transactions": len(block.Transactions),
		"totalFees":    totalFees.String(),
		"errors":       errors,
	}).Info("block fees distributed")

	if errors > 0 {
		return fmt.Errorf("failed to distribute fees for %d transactions", errors)
	}

	return nil
}

// Enable enables fee distribution on block finalization
func (ci *ConsensusIntegration) Enable() {
	ci.mu.Lock()
	defer ci.mu.Unlock()
	ci.enabled = true
	ci.logger.Info("fee distribution enabled")
}

// Disable disables fee distribution on block finalization
func (ci *ConsensusIntegration) Disable() {
	ci.mu.Lock()
	defer ci.mu.Unlock()
	ci.enabled = false
	ci.logger.Info("fee distribution disabled")
}

// IsEnabled returns whether fee distribution is enabled
func (ci *ConsensusIntegration) IsEnabled() bool {
	ci.mu.RLock()
	defer ci.mu.RUnlock()
	return ci.enabled
}

// Close performs cleanup
func (ci *ConsensusIntegration) Close() error {
	ci.Disable()

	// If the fee distributor implements Close, call it
	if closer, ok := ci.feeDistributor.(interface{ Close() error }); ok {
		return closer.Close()
	}

	return nil
}

// ConsensusCallback is the interface that consensus modules should call
type ConsensusCallback interface {
	OnBlockFinalized(block *common.Block) error
}

// FeeDistributionHook provides a hook for consensus to notify about finalized blocks
type FeeDistributionHook struct {
	integration *ConsensusIntegration
}

// NewFeeDistributionHook creates a new hook for consensus integration
func NewFeeDistributionHook(fd FeeDistributorAPI, logger *logrus.Logger) *FeeDistributionHook {
	return &FeeDistributionHook{
		integration: NewConsensusIntegration(fd, logger),
	}
}

// OnBlockFinalized implements the consensus callback
func (h *FeeDistributionHook) OnBlockFinalized(block *common.Block) error {
	return h.integration.OnBlockFinalized(block)
}

// Enable enables the hook
func (h *FeeDistributionHook) Enable() {
	h.integration.Enable()
}

// Disable disables the hook
func (h *FeeDistributionHook) Disable() {
	h.integration.Disable()
}

// Close closes the hook
func (h *FeeDistributionHook) Close() error {
	return h.integration.Close()
}
