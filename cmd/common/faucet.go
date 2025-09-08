// Package common provides shared faucet functionality
package common

import (
	"fmt"
	"sync"
	"time"

	"diamante/consensus"
	"diamante/storage"
)

// FaucetService handles faucet operations with security measures
type FaucetService struct {
	ledger       *storage.MongoDBLedger
	fundAmount   int64
	maxFunds     int64
	cooldownTime time.Duration
	requestLog   map[string]time.Time
	mutex        sync.RWMutex
	logger       Logger
	metrics      *MetricsCollector
}

// FaucetResponse represents the response from a faucet request
type FaucetResponse struct {
	Status    string `json:"status"`
	Message   string `json:"message"`
	Timestamp string `json:"timestamp"`
}

// NewFaucetService creates a new faucet service
func NewFaucetService(ledger *storage.MongoDBLedger, fundAmount, maxFunds int64, cooldownTime time.Duration, logger Logger, metrics *MetricsCollector) *FaucetService {
	return &FaucetService{
		ledger:       ledger,
		fundAmount:   fundAmount,
		maxFunds:     maxFunds,
		cooldownTime: cooldownTime,
		requestLog:   make(map[string]time.Time),
		logger:       logger,
		metrics:      metrics,
	}
}

// FundAddress funds the specified address if allowed
func (fs *FaucetService) FundAddress(address string) (*FaucetResponse, error) {
	fs.mutex.Lock()
	defer fs.mutex.Unlock()

	// Check if address has requested funds recently
	if lastRequest, exists := fs.requestLog[address]; exists {
		if time.Since(lastRequest) < fs.cooldownTime {
			remainingTime := fs.cooldownTime - time.Since(lastRequest)
			return &FaucetResponse{
				Status:    "error",
				Message:   fmt.Sprintf("Please wait %v before requesting again", remainingTime.Round(time.Second)),
				Timestamp: consensus.ConsensusNow().UTC().Format(time.RFC3339),
			}, nil
		}
	}

	// Get current balance
	currentBalance, err := fs.ledger.GetBalance(address)
	if err != nil {
		fs.logger.Error("Failed to get balance", "address", address, "error", err)
		return &FaucetResponse{
			Status:    "error",
			Message:   "Failed to check account balance",
			Timestamp: consensus.ConsensusNow().UTC().Format(time.RFC3339),
		}, err
	}

	// Check if account already has max funds
	if currentBalance >= float64(fs.maxFunds) {
		return &FaucetResponse{
			Status:    "error",
			Message:   "Account already has maximum allowed funds",
			Timestamp: consensus.ConsensusNow().UTC().Format(time.RFC3339),
		}, nil
	}

	// Calculate funding amount
	fundingAmount := float64(fs.fundAmount)
	if currentBalance+fundingAmount > float64(fs.maxFunds) {
		fundingAmount = float64(fs.maxFunds) - currentBalance
	}

	// Perform the funding
	// Get the account first
	account, err := fs.ledger.GetAccount(address)
	if err != nil {
		return &FaucetResponse{
			Status:    "error",
			Message:   "Account does not exist - create account first",
			Timestamp: consensus.ConsensusNow().UTC().Format(time.RFC3339),
		}, fmt.Errorf("account %s does not exist: %w", address, err)
	}

	// Update existing account balance
	account.Balance += fundingAmount
	err = fs.ledger.UpdateAccount(account)

	if err != nil {
		fs.logger.Error("Failed to fund address", "address", address, "amount", fundingAmount, "error", err)
		if fs.metrics != nil {
			fs.metrics.IncrementCounter("faucet_requests_funding_failed")
		}
		return &FaucetResponse{
			Status:    "error",
			Message:   "Failed to fund address",
			Timestamp: consensus.ConsensusNow().UTC().Format(time.RFC3339),
		}, fmt.Errorf("failed to update account balance: %w", err)
	}

	// Record successful funding
	fs.requestLog[address] = consensus.ConsensusNow()
	if fs.metrics != nil {
		fs.metrics.IncrementCounter("faucet_requests_successful")
		fs.metrics.ObserveHistogram("faucet_amount_distributed", fundingAmount)
	}

	fs.logger.Info("Successfully funded address", "address", address, "amount", fundingAmount)

	return &FaucetResponse{
		Status:    "success",
		Message:   fmt.Sprintf("Successfully funded %.2f tokens", fundingAmount),
		Timestamp: consensus.ConsensusNow().UTC().Format(time.RFC3339),
	}, nil
}

// HealthCheck performs a health check on the faucet service
func (fs *FaucetService) HealthCheck() error {
	fs.mutex.RLock()
	defer fs.mutex.RUnlock()

	// Check if ledger is accessible
	if fs.ledger == nil {
		return fmt.Errorf("ledger is not initialized")
	}

	// Try to get the status (basic connectivity check)
	_, err := fs.ledger.GetBlockHeight()
	if err != nil {
		return fmt.Errorf("ledger connectivity check failed: %w", err)
	}

	return nil
}

// GetStats returns faucet service statistics
func (fs *FaucetService) GetStats() map[string]interface{} {
	fs.mutex.RLock()
	defer fs.mutex.RUnlock()

	stats := map[string]interface{}{
		"total_requests":    len(fs.requestLog),
		"fund_amount":       fs.fundAmount,
		"max_funds":         fs.maxFunds,
		"cooldown_duration": fs.cooldownTime.String(),
		"last_cleanup":      consensus.ConsensusNow().UTC().Format(time.RFC3339),
	}

	return stats
}

// CleanupOldRequests removes old request entries from the log
func (fs *FaucetService) CleanupOldRequests() {
	fs.mutex.Lock()
	defer fs.mutex.Unlock()

	now := consensus.ConsensusNow()
	for address, lastRequest := range fs.requestLog {
		if now.Sub(lastRequest) > fs.cooldownTime*2 {
			delete(fs.requestLog, address)
		}
	}

	fs.logger.Info("Cleaned up old faucet requests", "remaining_count", len(fs.requestLog))
}
