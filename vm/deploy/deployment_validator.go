// Package deploy provides deployment validation
package deploy

import (
	"errors"
	"fmt"
)

// DefaultDeploymentValidator is the default implementation of DeploymentValidator
type DefaultDeploymentValidator struct {
	// Maximum code size in bytes
	maxCodeSize int

	// Maximum gas limit
	maxGasLimit uint64

	// Allowed languages
	allowedLanguages map[string]bool
}

// NewDefaultDeploymentValidator creates a new default deployment validator
func NewDefaultDeploymentValidator() DeploymentValidator {
	return &DefaultDeploymentValidator{
		maxCodeSize: 24576, // 24KB default max code size
		maxGasLimit: 10000000,
		allowedLanguages: map[string]bool{
			"solidity":  true,
			"vyper":     true,
			"evm":       true,
			"EVM":       true,
			"go":        true,
			"node":      true,
			"chaincode": true,
			"fabric":    true,
			"native":    true,
			"diamante":  true,
		},
	}
}

// ValidateDeployment validates a deployment request
func (v *DefaultDeploymentValidator) ValidateDeployment(req DeploymentRequest) error {
	// Validate deployer
	if req.Deployer == "" {
		return errors.New("deployer address is required")
	}

	// Validate language
	if req.Language == "" {
		return errors.New("contract language is required")
	}

	if !v.allowedLanguages[req.Language] {
		return fmt.Errorf("unsupported language: %s", req.Language)
	}

	// Validate code
	if len(req.Code) == 0 {
		return errors.New("contract code is required")
	}

	if len(req.Code) > v.maxCodeSize {
		return fmt.Errorf("contract code size exceeds maximum of %d bytes", v.maxCodeSize)
	}

	// Validate gas limit
	if req.GasLimit == 0 {
		return errors.New("gas limit must be greater than zero")
	}

	if req.GasLimit > v.maxGasLimit {
		return fmt.Errorf("gas limit exceeds maximum of %d", v.maxGasLimit)
	}

	// Validate constructor args if present
	if req.ConstructorArgs != nil {
		// Basic validation - can be enhanced based on ABI
		if len(req.ConstructorArgs) > 100 {
			return errors.New("too many constructor arguments")
		}
	}

	return nil
}

// ValidateUpgrade validates an upgrade request
func (v *DefaultDeploymentValidator) ValidateUpgrade(req UpgradeRequest) error {
	// Validate contract ID
	if req.ContractID == "" {
		return errors.New("contract ID is required")
	}

	// Validate authorizer
	if req.Authorizer == "" {
		return errors.New("authorizer address is required")
	}

	// Validate new version
	if req.NewVersion == "" {
		return errors.New("new version is required")
	}

	// Validate new code
	if len(req.NewCode) == 0 {
		return errors.New("new contract code is required")
	}

	if len(req.NewCode) > v.maxCodeSize {
		return fmt.Errorf("new contract code size exceeds maximum of %d bytes", v.maxCodeSize)
	}

	// Validate migration data if present
	if len(req.MigrationData) > 1048576 { // 1MB max migration data
		return errors.New("migration data too large")
	}

	return nil
}

// ConfigurableDeploymentValidator allows configuration of validation rules
type ConfigurableDeploymentValidator struct {
	DefaultDeploymentValidator

	// Custom validation functions
	deploymentValidators []func(DeploymentRequest) error
	upgradeValidators    []func(UpgradeRequest) error
}

// NewConfigurableDeploymentValidator creates a new configurable validator
func NewConfigurableDeploymentValidator(maxCodeSize int, maxGasLimit uint64) *ConfigurableDeploymentValidator {
	return &ConfigurableDeploymentValidator{
		DefaultDeploymentValidator: DefaultDeploymentValidator{
			maxCodeSize: maxCodeSize,
			maxGasLimit: maxGasLimit,
			allowedLanguages: map[string]bool{
				"solidity":  true,
				"vyper":     true,
				"evm":       true,
				"EVM":       true,
				"go":        true,
				"node":      true,
				"chaincode": true,
				"fabric":    true,
				"native":    true,
				"diamante":  true,
			},
		},
		deploymentValidators: []func(DeploymentRequest) error{},
		upgradeValidators:    []func(UpgradeRequest) error{},
	}
}

// AddDeploymentValidator adds a custom deployment validator
func (v *ConfigurableDeploymentValidator) AddDeploymentValidator(validator func(DeploymentRequest) error) {
	v.deploymentValidators = append(v.deploymentValidators, validator)
}

// AddUpgradeValidator adds a custom upgrade validator
func (v *ConfigurableDeploymentValidator) AddUpgradeValidator(validator func(UpgradeRequest) error) {
	v.upgradeValidators = append(v.upgradeValidators, validator)
}

// ValidateDeployment validates a deployment request with custom validators
func (v *ConfigurableDeploymentValidator) ValidateDeployment(req DeploymentRequest) error {
	// Run default validation
	if err := v.DefaultDeploymentValidator.ValidateDeployment(req); err != nil {
		return err
	}

	// Run custom validators
	for _, validator := range v.deploymentValidators {
		if err := validator(req); err != nil {
			return err
		}
	}

	return nil
}

// ValidateUpgrade validates an upgrade request with custom validators
func (v *ConfigurableDeploymentValidator) ValidateUpgrade(req UpgradeRequest) error {
	// Run default validation
	if err := v.DefaultDeploymentValidator.ValidateUpgrade(req); err != nil {
		return err
	}

	// Run custom validators
	for _, validator := range v.upgradeValidators {
		if err := validator(req); err != nil {
			return err
		}
	}

	return nil
}
