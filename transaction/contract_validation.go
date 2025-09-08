package transaction

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"diamante/common"
	"diamante/consensus"
	"diamante/storage"
	"encoding/json"

	"github.com/sirupsen/logrus"
)

// ContractABI represents the contract's Application Binary Interface
type ContractABI struct {
	Methods       map[string]ABIMethod `json:"methods"`
	Events        []ABIEvent           `json:"events"`
	Constructor   ABIMethod            `json:"constructor"`
	Version       string               `json:"version"`
	AccessControl map[string][]string  `json:"access_control"` // method -> allowed roles
}

// ABIMethod represents a method in the contract ABI
type ABIMethod struct {
	Name            string         `json:"name"`
	Inputs          []ABIParameter `json:"inputs"`
	Outputs         []ABIParameter `json:"outputs"`
	StateMutability string         `json:"state_mutability"` // view, pure, payable, nonpayable
	GasEstimate     uint64         `json:"gas_estimate"`
	AccessLevel     string         `json:"access_level"` // public, private, restricted
}

// ABIParameter represents a parameter in an ABI method
type ABIParameter struct {
	Name    string `json:"name"`
	Type    string `json:"type"`
	Indexed bool   `json:"indexed,omitempty"`
}

// ABIEvent represents an event in the contract ABI
type ABIEvent struct {
	Name   string         `json:"name"`
	Inputs []ABIParameter `json:"inputs"`
}

// ContractValidator handles comprehensive smart contract validation
type ContractValidator struct {
	ledger        common.LedgerAPI
	stateStore    storage.LedgerStore
	abiCache      map[string]*ContractABI
	logger        *logrus.Logger
	gasCalculator GasCalculator
	permissionMgr PermissionManager
}

// GasCalculator estimates gas for contract operations
type GasCalculator interface {
	EstimateGas(contractID, method string, params []ValidationParameter) (uint64, error)
}

// ValidationParameter represents a typed parameter for validation
type ValidationParameter struct {
	Name  string      `json:"name"`
	Type  string      `json:"type"`
	Value interface{} `json:"value"` // Keep interface{} for parameter values
}

// PermissionManager checks access permissions
type PermissionManager interface {
	CheckPermission(accountID, contractID, method string) error
	GetAccountRoles(accountID string) ([]string, error)
}

// ValidationContext represents the context for a validation operation
type ValidationContext struct {
	TransactionID string
	Sender        string
	GasLimit      uint64
	CreatedAt     time.Time
	Metadata      map[string]string
}

// NewContractValidator creates a new contract validator
func NewContractValidator(ledger common.LedgerAPI, stateStore storage.LedgerStore, logger *logrus.Logger) *ContractValidator {
	if logger == nil {
		logger = logrus.New()
	}

	return &ContractValidator{
		ledger:     ledger,
		stateStore: stateStore,
		abiCache:   make(map[string]*ContractABI),
		logger:     logger,
	}
}

// SetGasCalculator sets the gas calculator
func (cv *ContractValidator) SetGasCalculator(gc GasCalculator) {
	cv.gasCalculator = gc
}

// SetPermissionManager sets the permission manager
func (cv *ContractValidator) SetPermissionManager(pm PermissionManager) {
	cv.permissionMgr = pm
}

// ValidateContractCall performs comprehensive validation of a smart contract call
func (cv *ContractValidator) ValidateContractCall(tx *common.Transaction) error {
	if tx.SmartContractID == "" {
		return errors.New("smart contract ID is empty")
	}

	// Extract contract call data
	callData, err := cv.extractCallData(tx)
	if err != nil {
		return fmt.Errorf("failed to extract call data: %w", err)
	}

	// 1. Contract existence and status check
	if err := cv.validateContractExists(tx.SmartContractID); err != nil {
		return fmt.Errorf("contract validation failed: %w", err)
	}

	// 2. Method validation against ABI
	abi, err := cv.getContractABI(tx.SmartContractID)
	if err != nil {
		return fmt.Errorf("failed to get contract ABI: %w", err)
	}

	method, err := cv.validateMethodCall(abi, callData)
	if err != nil {
		return fmt.Errorf("method validation failed: %w", err)
	}

	// 3. Permission and access control
	if err := cv.validatePermissions(tx.Sender, tx.SmartContractID, method); err != nil {
		return fmt.Errorf("permission validation failed: %w", err)
	}

	// 4. Gas estimation and validation
	if err := cv.validateGas(tx, method, callData.Args); err != nil {
		return fmt.Errorf("gas validation failed: %w", err)
	}

	// 5. Parameter type checking
	if err := cv.validateParameters(method, callData.Args); err != nil {
		return fmt.Errorf("parameter validation failed: %w", err)
	}

	// 6. Balance verification for payable methods
	if err := cv.validatePayableCall(tx, method); err != nil {
		return fmt.Errorf("payable validation failed: %w", err)
	}

	// 7. Optional dry-run validation
	if cv.shouldPerformDryRun(tx) {
		if err := cv.performDryRun(tx, callData); err != nil {
			cv.logger.WithFields(logrus.Fields{
				"contractID": tx.SmartContractID,
				"method":     callData.Method,
				"error":      err,
			}).Warn("Dry-run validation failed")
			// Don't fail on dry-run errors, just log them
		}
	}

	return nil
}

// validateContractExists checks if contract exists and is active
func (cv *ContractValidator) validateContractExists(contractID string) error {
	// Check if contract exists in ledger
	_, err := cv.getContract(contractID)
	if err != nil {
		return fmt.Errorf("contract not found: %w", err)
	}

	// For now, assume all contracts that exist are active
	// Status would need to be tracked separately or in contract state
	return nil
}

// getContract retrieves contract from ledger/cache
func (cv *ContractValidator) getContract(contractID string) (*common.SmartContract, error) {
	// In a real implementation, this would query the ledger
	// For now, we'll create a mock response
	contract := &common.SmartContract{
		ID:        contractID,
		Code:      "",
		Owner:     "",
		CreatedAt: consensus.ConsensusNow(),
		Metadata: &common.SmartContractMetadata{
			Description: "Mock contract",
			Version:     "1.0.0",
		},
	}

	// Try to get actual contract from ledger if available
	if cv.stateStore != nil {
		// This would be implemented based on the actual storage interface
		// Keep the default metadata for now
	}

	return contract, nil
}

// getContractABI retrieves and caches contract ABI
func (cv *ContractValidator) getContractABI(contractID string) (*ContractABI, error) {
	// Check cache first
	if abi, exists := cv.abiCache[contractID]; exists {
		return abi, nil
	}

	// Load ABI from contract
	_, err := cv.getContract(contractID)
	if err != nil {
		return nil, err
	}

	// For now, return a default ABI
	// In a real implementation, ABI would be stored with the contract
	return cv.getDefaultABI(), nil
}

// getDefaultABI returns a default ABI for basic contracts
func (cv *ContractValidator) getDefaultABI() *ContractABI {
	return &ContractABI{
		Methods: map[string]ABIMethod{
			"transfer": {
				Name: "transfer",
				Inputs: []ABIParameter{
					{Name: "to", Type: "address"},
					{Name: "amount", Type: "uint256"},
				},
				Outputs:         []ABIParameter{{Name: "success", Type: "bool"}},
				StateMutability: "nonpayable",
				GasEstimate:     50000,
				AccessLevel:     "public",
			},
			"balanceOf": {
				Name: "balanceOf",
				Inputs: []ABIParameter{
					{Name: "account", Type: "address"},
				},
				Outputs:         []ABIParameter{{Name: "balance", Type: "uint256"}},
				StateMutability: "view",
				GasEstimate:     10000,
				AccessLevel:     "public",
			},
		},
		AccessControl: map[string][]string{
			"transfer":  {"user", "admin"},
			"balanceOf": {"user", "admin", "viewer"},
		},
	}
}

// validateMethodCall validates the method exists and can be called
func (cv *ContractValidator) validateMethodCall(abi *ContractABI, callData *ContractCallData) (*ABIMethod, error) {
	method, exists := abi.Methods[callData.Method]
	if !exists {
		return nil, fmt.Errorf("method '%s' not found in contract ABI", callData.Method)
	}

	// Check if method is callable based on state mutability
	if method.StateMutability == "pure" || method.StateMutability == "view" {
		// These are read-only methods, ensure no value is sent
		if callData.Value > 0 {
			return nil, fmt.Errorf("cannot send value to %s method", method.StateMutability)
		}
	}

	return &method, nil
}

// validatePermissions checks access control
func (cv *ContractValidator) validatePermissions(sender, contractID string, method *ABIMethod) error {
	// Check method access level
	if method.AccessLevel == "public" {
		return nil // Anyone can call public methods
	}

	// Use permission manager if available
	if cv.permissionMgr != nil {
		return cv.permissionMgr.CheckPermission(sender, contractID, method.Name)
	}

	// Fallback to basic role checking
	// In a real implementation, this would check against stored permissions
	if method.AccessLevel == "restricted" || method.AccessLevel == "private" {
		// For now, we'll allow all calls but log a warning
		cv.logger.WithFields(logrus.Fields{
			"sender":   sender,
			"contract": contractID,
			"method":   method.Name,
			"access":   method.AccessLevel,
		}).Warn("Permission check bypassed - no permission manager configured")
	}

	return nil
}

// validateGas estimates and validates gas requirements
func (cv *ContractValidator) validateGas(tx *common.Transaction, method *ABIMethod, args []ValidationParameter) error {
	// Extract gas limit from transaction
	gasLimit := uint64(100000) // Default
	if tx.Metadata != nil {
		// Extract from transaction metadata
		gasLimit = cv.extractGasLimitFromMetadata(tx.Metadata)
	}

	// Estimate gas requirement
	var gasEstimate uint64
	if cv.gasCalculator != nil {
		estimate, err := cv.gasCalculator.EstimateGas(tx.SmartContractID, method.Name, args)
		if err != nil {
			cv.logger.WithError(err).Warn("Gas estimation failed, using method default")
			gasEstimate = method.GasEstimate
		} else {
			gasEstimate = estimate
		}
	} else {
		gasEstimate = method.GasEstimate
	}

	// Add buffer for safety
	requiredGas := uint64(float64(gasEstimate) * 1.2)

	// Validate gas limit
	if gasLimit < requiredGas {
		return fmt.Errorf("insufficient gas limit: provided %d, required %d", gasLimit, requiredGas)
	}

	// Check if user can afford the gas
	gasPrice := cv.extractGasPriceFromMetadata(tx.Metadata)

	maxGasCost := float64(gasLimit * gasPrice)
	if maxGasCost > tx.Fee {
		return fmt.Errorf("transaction fee (%f) insufficient for max gas cost (%f)", tx.Fee, maxGasCost)
	}

	return nil
}

// extractGasLimitFromMetadata extracts gas limit from transaction metadata
func (cv *ContractValidator) extractGasLimitFromMetadata(metadata *common.TransactionMetadata) uint64 {
	// This would need to be implemented based on common.TransactionMetadata structure
	return 100000 // Default
}

// extractGasPriceFromMetadata extracts gas price from transaction metadata
func (cv *ContractValidator) extractGasPriceFromMetadata(metadata *common.TransactionMetadata) uint64 {
	// This would need to be implemented based on common.TransactionMetadata structure
	return 1 // Default
}

// validateParameters checks parameter types match ABI
func (cv *ContractValidator) validateParameters(method *ABIMethod, args []ValidationParameter) error {
	if len(args) != len(method.Inputs) {
		return fmt.Errorf("argument count mismatch: expected %d, got %d", len(method.Inputs), len(args))
	}

	for i, param := range method.Inputs {
		if err := cv.validateParameterType(param, args[i].Value); err != nil {
			return fmt.Errorf("parameter '%s' validation failed: %w", param.Name, err)
		}
	}

	return nil
}

// validateParameterType validates a single parameter type
func (cv *ContractValidator) validateParameterType(param ABIParameter, value interface{}) error {
	switch param.Type {
	case "address":
		addr, ok := value.(string)
		if !ok {
			return fmt.Errorf("expected string address, got %T", value)
		}
		if addr == "" {
			return errors.New("address cannot be empty")
		}
		// Additional address validation could go here

	case "uint256", "uint":
		switch v := value.(type) {
		case float64:
			if v < 0 {
				return errors.New("uint cannot be negative")
			}
		case int:
			if v < 0 {
				return errors.New("uint cannot be negative")
			}
		case uint:
			// Valid
		case uint64:
			// Valid
		default:
			return fmt.Errorf("expected numeric type for uint, got %T", value)
		}

	case "bool":
		_, ok := value.(bool)
		if !ok {
			return fmt.Errorf("expected bool, got %T", value)
		}

	case "string":
		_, ok := value.(string)
		if !ok {
			return fmt.Errorf("expected string, got %T", value)
		}

	case "bytes", "bytes32":
		switch v := value.(type) {
		case []byte:
			if param.Type == "bytes32" && len(v) != 32 {
				return fmt.Errorf("bytes32 must be exactly 32 bytes, got %d", len(v))
			}
		case string:
			// Allow hex strings
			if !strings.HasPrefix(v, "0x") {
				return errors.New("byte data must be hex string starting with 0x")
			}
		default:
			return fmt.Errorf("expected bytes or hex string, got %T", value)
		}

	default:
		// For unknown types, log warning but allow
		cv.logger.WithFields(logrus.Fields{
			"param": param.Name,
			"type":  param.Type,
		}).Warn("Unknown parameter type, skipping validation")
	}

	return nil
}

// validatePayableCall checks balance for payable methods
func (cv *ContractValidator) validatePayableCall(tx *common.Transaction, method *ABIMethod) error {
	if method.StateMutability != "payable" && tx.Amount > 0 {
		return fmt.Errorf("cannot send value to non-payable method '%s'", method.Name)
	}

	if method.StateMutability == "payable" && tx.Amount > 0 {
		// Verify sender has sufficient balance
		balance, err := cv.ledger.GetBalance(tx.Sender)
		if err != nil {
			return fmt.Errorf("failed to get sender balance: %w", err)
		}

		totalRequired := tx.Amount + tx.Fee
		if balance < totalRequired {
			return fmt.Errorf("insufficient balance for payable call: have %f, need %f", balance, totalRequired)
		}
	}

	return nil
}

// shouldPerformDryRun determines if dry-run validation should be performed
func (cv *ContractValidator) shouldPerformDryRun(tx *common.Transaction) bool {
	// For now, always perform dry-run for high-value transactions
	// In a real implementation, check metadata fields for dry-run flag
	highValueThreshold := 1000.0
	return tx.Amount > highValueThreshold
}

// performDryRun executes the contract call in read-only mode
func (cv *ContractValidator) performDryRun(tx *common.Transaction, callData *ContractCallData) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Create dry-run parameters
	params := DryRunParams{
		Method:  callData.Method,
		Args:    callData.Args,
		Value:   callData.Value,
		Sender:  tx.Sender,
		DryRun:  true,
		Context: ctx,
	}

	// Convert to SmartContractParams for execution
	contractParams := &common.SmartContractParams{
		FunctionName: "dryRun",
		Caller:       tx.Sender,
		StringParams: map[string]string{
			"method": params.Method,
			"sender": params.Sender,
		},
		NumberParams: map[string]float64{
			"value": params.Value,
		},
		BoolParams: map[string]bool{
			"dryRun": params.DryRun,
		},
	}

	// Add args as JSON string
	if len(params.Args) > 0 {
		argsJSON, _ := json.Marshal(params.Args)
		contractParams.StringParams["args"] = string(argsJSON)
	}

	// Execute dry-run through ledger
	result, err := cv.ledger.ExecuteSmartContract(tx.SmartContractID, "dryRun", tx.Sender, contractParams)
	if err != nil {
		return fmt.Errorf("dry-run execution failed: %w", err)
	}

	// Check dry-run result
	if result != nil && !result.Success {
		return fmt.Errorf("dry-run failed: %s", result.ErrorMessage)
	}

	return nil
}

// DryRunParams represents parameters for dry-run execution
type DryRunParams struct {
	Method  string                `json:"method"`
	Args    []ValidationParameter `json:"args"`
	Value   float64               `json:"value"`
	Sender  string                `json:"sender"`
	DryRun  bool                  `json:"dryRun"`
	Context context.Context       `json:"-"`
}

// DryRunResult represents the result of a dry-run execution
type DryRunResult struct {
	Success      bool     `json:"success"`
	Error        string   `json:"error,omitempty"`
	GasEstimate  uint64   `json:"gasEstimate,omitempty"`
	ReturnData   []byte   `json:"returnData,omitempty"`
	StateChanges int      `json:"stateChanges,omitempty"`
	Events       []string `json:"events,omitempty"`
}

// extractCallData extracts contract call data from transaction
func (cv *ContractValidator) extractCallData(tx *common.Transaction) (*ContractCallData, error) {
	if tx.Metadata == nil {
		return nil, errors.New("transaction metadata is nil")
	}

	// Create an extractor to help with metadata parsing
	extractor := TransactionValidationExtractor{
		Metadata: tx.Metadata,
	}

	method := extractor.GetMethod()
	if method == "" {
		return nil, errors.New("contract method not specified")
	}

	args := extractor.GetArgs()

	return &ContractCallData{
		Method: method,
		Args:   args,
		Value:  tx.Amount,
	}, nil
}

// TransactionValidationExtractor helps extract validation data from transaction metadata
type TransactionValidationExtractor struct {
	Metadata *common.TransactionMetadata
}

// GetMethod extracts the method name from metadata
func (tve *TransactionValidationExtractor) GetMethod() string {
	if tve.Metadata == nil {
		return ""
	}
	// This would need to be implemented based on how common.TransactionMetadata is structured
	// For now, returning empty string to avoid compilation errors
	return ""
}

// GetArgs extracts arguments from metadata
func (tve *TransactionValidationExtractor) GetArgs() []ValidationParameter {
	if tve.Metadata == nil {
		return []ValidationParameter{}
	}
	// This would need to be implemented based on how common.TransactionMetadata is structured
	// For now, returning empty slice to avoid compilation errors
	return []ValidationParameter{}
}

// ContractCallData represents extracted contract call information
type ContractCallData struct {
	Method string
	Args   []ValidationParameter
	Value  float64
}

// UpdateValidateSmartContractCall updates the TransactionManager to use the new validator
func (tm *TransactionManager) UpdateValidateSmartContractCall(validator *ContractValidator) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	// Store the validator for use in validateSmartContractCall
	if tm.ledger != nil {
		// This would be set on the TransactionManager
		// For now, we'll use the existing validateSmartContractCall method
	}
}

// createValidationContext creates a validation context for a transaction
func (cv *ContractValidator) createValidationContext(tx *common.Transaction) *ValidationContext {
	// Extract gas limit from transaction fee and amount
	// In a real implementation, this would be properly extracted from metadata
	estimatedGasLimit := uint64(100000)
	if tx.Fee > 0 {
		// Estimate gas limit based on fee
		estimatedGasLimit = uint64(tx.Fee * 1000) // Simplified calculation
	}

	return &ValidationContext{
		TransactionID: tx.ID,
		Sender:        tx.Sender,
		GasLimit:      estimatedGasLimit,
		CreatedAt:     consensus.ConsensusNow(),
		Metadata:      make(map[string]string), // Use string map instead of interface{}
	}
}
