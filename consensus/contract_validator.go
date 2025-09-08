// Package consensus provides contract validation for the hybrid consensus mechanism
package consensus

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"diamante/consensus/types"
	dtypes "diamante/types"

	"github.com/sirupsen/logrus"
)

// DeploymentRequest represents a contract deployment request (local copy to avoid import cycle)
type DeploymentRequest struct {
	ContractID      string                    `json:"contract_id"`
	Code            []byte                    `json:"code"`
	Language        string                    `json:"language"`
	Deployer        string                    `json:"deployer"`
	GasLimit        uint64                    `json:"gas_limit"`
	Value           uint64                    `json:"value"`
	ConstructorArgs *dtypes.ContractArguments `json:"constructor_args,omitempty"`
}

// ContractCall represents a contract call (local copy to avoid import cycle)
type ContractCall struct {
	ContractID string                    `json:"contract_id"`
	Function   string                    `json:"function"`
	Args       *dtypes.ContractArguments `json:"args"`
	Caller     string                    `json:"caller"`
	Value      uint64                    `json:"value"`
	GasLimit   uint64                    `json:"gas_limit"`
}

// RuntimeType represents a runtime type (local copy to avoid import cycle)
type RuntimeType string

const (
	RuntimeTypeEVM       RuntimeType = "evm"
	RuntimeTypeChaincode RuntimeType = "chaincode"
	RuntimeTypeNative    RuntimeType = "native"
)

// RuntimeManager interface (local copy to avoid import cycle)
type RuntimeManager interface {
	IsRuntimeActive(runtimeType RuntimeType) bool
}

// ContractValidator validates contract-related transactions
type ContractValidator interface {
	// ValidateTransaction validates a transaction for contract operations
	ValidateTransaction(tx types.Transaction) error

	// ValidateDeployment validates a contract deployment
	ValidateDeployment(deployment *DeploymentRequest) error

	// ValidateContractCall validates a contract call
	ValidateContractCall(call *ContractCall) error

	// SetRuntimeManager sets the runtime manager for validation
	SetRuntimeManager(rm RuntimeManager)
}

// HybridContractValidator implements ContractValidator for the hybrid consensus
type HybridContractValidator struct {
	runtimeManager RuntimeManager
	logger         *logrus.Logger
	mu             sync.RWMutex

	// Validation rules
	maxContractSize uint64
	maxGasLimit     uint64
	allowedRuntimes map[RuntimeType]bool
	bannedAddresses map[string]bool
}

// NewContractValidator creates a new contract validator
func NewContractValidator(
	runtimeManager RuntimeManager,
	logger *logrus.Logger,
) ContractValidator {
	return &HybridContractValidator{
		runtimeManager:  runtimeManager,
		logger:          logger,
		maxContractSize: 10 * 1024 * 1024, // 10MB
		maxGasLimit:     100000000,        // 100M gas
		allowedRuntimes: map[RuntimeType]bool{
			RuntimeTypeEVM:       true,
			RuntimeTypeChaincode: true,
			RuntimeTypeNative:    true,
		},
		bannedAddresses: make(map[string]bool),
	}
}

// ValidateTransaction validates a transaction for contract operations
func (v *HybridContractValidator) ValidateTransaction(tx types.Transaction) error {
	v.mu.RLock()
	defer v.mu.RUnlock()

	// Check if transaction is contract-related
	if !v.isContractTransaction(tx) {
		// Not a contract transaction, no validation needed
		return nil
	}

	// Check banned addresses
	if v.bannedAddresses[tx.Sender] {
		return fmt.Errorf("sender address %s is banned", tx.Sender)
	}
	if v.bannedAddresses[tx.Receiver] {
		return fmt.Errorf("recipient address %s is banned", tx.Receiver)
	}

	// Validate fee as a proxy for gas (since there's no explicit fee field, use amount)
	estimatedGas := uint64(tx.Amount * 1000000) // Convert amount to gas units
	if estimatedGas > v.maxGasLimit {
		return fmt.Errorf("estimated gas %d exceeds maximum %d", estimatedGas, v.maxGasLimit)
	}

	return nil
}

// ValidateDeployment validates a contract deployment
func (v *HybridContractValidator) ValidateDeployment(deployment *DeploymentRequest) error {
	if deployment == nil {
		return errors.New("deployment request is nil")
	}

	// Determine runtime type from language
	runtimeType := v.languageToRuntime(deployment.Language)

	// Check runtime type
	if !v.allowedRuntimes[runtimeType] {
		return fmt.Errorf("runtime for language %s is not allowed", deployment.Language)
	}

	// Validate code size
	if uint64(len(deployment.Code)) > v.maxContractSize {
		return fmt.Errorf("contract size %d exceeds maximum %d", len(deployment.Code), v.maxContractSize)
	}

	// Validate gas limit
	if deployment.GasLimit > v.maxGasLimit {
		return fmt.Errorf("gas limit %d exceeds maximum %d", deployment.GasLimit, v.maxGasLimit)
	}

	// Check if runtime is available
	if v.runtimeManager != nil && !v.runtimeManager.IsRuntimeActive(runtimeType) {
		return fmt.Errorf("runtime for language %s is not active", deployment.Language)
	}

	// Check banned deployer
	if v.bannedAddresses[deployment.Deployer] {
		return fmt.Errorf("deployer %s is banned", deployment.Deployer)
	}

	return nil
}

// ValidateContractCall validates a contract call
func (v *HybridContractValidator) ValidateContractCall(call *ContractCall) error {
	if call == nil {
		return errors.New("contract call is nil")
	}

	// Basic contract existence check would go here
	// In production, this would check against the actual contract registry
	// For now, we just check if the contract ID is not empty
	if call.ContractID == "" {
		return errors.New("contract ID cannot be empty")
	}

	// Validate gas limit
	if call.GasLimit > v.maxGasLimit {
		return fmt.Errorf("gas limit %d exceeds maximum %d", call.GasLimit, v.maxGasLimit)
	}

	// Check banned addresses
	if v.bannedAddresses[call.Caller] {
		return fmt.Errorf("caller %s is banned", call.Caller)
	}

	return nil
}

// SetRuntimeManager sets the runtime manager for validation
func (v *HybridContractValidator) SetRuntimeManager(rm RuntimeManager) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.runtimeManager = rm
}

// Helper methods

func (v *HybridContractValidator) isContractTransaction(tx types.Transaction) bool {
	// Basic check - in production this would be more sophisticated
	// For now, assume any transaction with specific ID patterns is a contract transaction
	if len(tx.ID) > 10 && (tx.ID[:3] == "CTX" || tx.ID[:3] == "DPL") {
		return true
	}

	// Check if it's a regular transaction to a contract
	if tx.Receiver != "" && tx.Amount > 0 {
		// Could be a contract call
		// In production, this would check against the contract registry
		return true
	}

	return false
}

// languageToRuntime converts a language string to runtime type
func (v *HybridContractValidator) languageToRuntime(language string) RuntimeType {
	switch language {
	case "solidity", "vyper":
		return RuntimeTypeEVM
	case "go", "node", "javascript", "typescript":
		return RuntimeTypeChaincode
	case "native", "rust", "c", "cpp":
		return RuntimeTypeNative
	default:
		// Default to native for unknown languages
		return RuntimeTypeNative
	}
}

// SetMaxContractSize sets the maximum allowed contract size
func (v *HybridContractValidator) SetMaxContractSize(size uint64) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.maxContractSize = size
}

// SetMaxGasLimit sets the maximum allowed gas limit
func (v *HybridContractValidator) SetMaxGasLimit(limit uint64) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.maxGasLimit = limit
}

// BanAddress bans an address from contract operations
func (v *HybridContractValidator) BanAddress(address string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.bannedAddresses[address] = true
	v.logger.Warnf("Address %s has been banned from contract operations", address)
}

// UnbanAddress removes an address from the ban list
func (v *HybridContractValidator) UnbanAddress(address string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	delete(v.bannedAddresses, address)
	v.logger.Infof("Address %s has been unbanned", address)
}

// ValidateBlock validates all contract transactions in a block
func (v *HybridContractValidator) ValidateBlock(block *types.Block) error {
	if block == nil {
		return errors.New("block is nil")
	}

	// Create a context with timeout for block validation
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Validate each transaction
	for i, tx := range block.Transactions {
		select {
		case <-ctx.Done():
			return fmt.Errorf("block validation timeout at transaction %d", i)
		default:
			if err := v.ValidateTransaction(tx); err != nil {
				return fmt.Errorf("transaction %d (%s) validation failed: %w", i, tx.ID, err)
			}
		}
	}

	// Additional block-level validation
	contractTxCount := 0
	totalGasUsed := uint64(0)

	for _, tx := range block.Transactions {
		if v.isContractTransaction(tx) {
			contractTxCount++
			// Use amount as a proxy for gas calculation
			estimatedGas := uint64(tx.Amount * 1000000)
			totalGasUsed += estimatedGas
		}
	}

	// Check if total gas usage is reasonable
	maxBlockGas := v.maxGasLimit * 10 // Allow 10x single tx limit for entire block
	if totalGasUsed > maxBlockGas {
		return fmt.Errorf("block total gas %d exceeds maximum %d", totalGasUsed, maxBlockGas)
	}

	v.logger.Debugf("Block %d validated: %d contract transactions, %d total gas",
		block.Number, contractTxCount, totalGasUsed)

	return nil
}
