// Package deploy provides types for contract deployment management
package deploy

import (
	"time"

	"diamante/vm/runtime"
)

// DeploymentStatus represents the status of a deployment
type DeploymentStatus string

const (
	// DeploymentStatusPending indicates deployment is pending
	DeploymentStatusPending DeploymentStatus = "pending"

	// DeploymentStatusInProgress indicates deployment is in progress
	DeploymentStatusInProgress DeploymentStatus = "in_progress"

	// DeploymentStatusSuccess indicates deployment succeeded
	DeploymentStatusSuccess DeploymentStatus = "success"

	// DeploymentStatusFailed indicates deployment failed
	DeploymentStatusFailed DeploymentStatus = "failed"
)

// UpgradeStatus represents the status of an upgrade
type UpgradeStatus string

const (
	// UpgradeStatusPending indicates upgrade is pending
	UpgradeStatusPending UpgradeStatus = "pending"

	// UpgradeStatusInProgress indicates upgrade is in progress
	UpgradeStatusInProgress UpgradeStatus = "in_progress"

	// UpgradeStatusSuccess indicates upgrade succeeded
	UpgradeStatusSuccess UpgradeStatus = "success"

	// UpgradeStatusFailed indicates upgrade failed
	UpgradeStatusFailed UpgradeStatus = "failed"
)

// ConstructorArgument represents a typed constructor argument
type ConstructorArgument struct {
	Name     string `json:"name"`
	Type     string `json:"type"`                // string, int, uint, bool, address, bytes
	Value    string `json:"value"`               // String representation of the value
	ArrayLen int    `json:"array_len,omitempty"` // For array types
}

// DeploymentMetadata represents typed metadata for deployments
type DeploymentMetadata struct {
	Name        string   `json:"name,omitempty"`
	Description string   `json:"description,omitempty"`
	Version     string   `json:"version,omitempty"`
	Author      string   `json:"author,omitempty"`
	License     string   `json:"license,omitempty"`
	Tags        []string `json:"tags,omitempty"`
	Repository  string   `json:"repository,omitempty"`
	Website     string   `json:"website,omitempty"`
}

// DeploymentOptions represents typed deployment options
type DeploymentOptions struct {
	EnableOptimization bool   `json:"enable_optimization,omitempty"`
	OptimizationLevel  int    `json:"optimization_level,omitempty"`
	DebugMode          bool   `json:"debug_mode,omitempty"`
	Timeout            int64  `json:"timeout,omitempty"` // Deployment timeout in seconds
	Priority           int    `json:"priority,omitempty"`
	RetryAttempts      int    `json:"retry_attempts,omitempty"`
	Environment        string `json:"environment,omitempty"` // dev, test, staging, production
	CompilerVersion    string `json:"compiler_version,omitempty"`
}

// EventParameter represents a typed event parameter
type EventParameter struct {
	Name    string `json:"name"`
	Type    string `json:"type"`
	Value   string `json:"value"`
	Indexed bool   `json:"indexed,omitempty"`
}

// ContextData represents typed context data for deployment/upgrade
type ContextData struct {
	TransactionHash      string            `json:"transaction_hash,omitempty"`
	BlockNumber          uint64            `json:"block_number,omitempty"`
	BlockHash            string            `json:"block_hash,omitempty"`
	GasPrice             uint64            `json:"gas_price,omitempty"`
	MaxFeePerGas         uint64            `json:"max_fee_per_gas,omitempty"`
	MaxPriorityFeePerGas uint64            `json:"max_priority_fee_per_gas,omitempty"`
	RuntimeVersion       string            `json:"runtime_version,omitempty"`
	CompilationTime      int64             `json:"compilation_time_ms,omitempty"`
	ValidationResults    []string          `json:"validation_results,omitempty"`
	Warnings             []string          `json:"warnings,omitempty"`
	DebugInfo            map[string]string `json:"debug_info,omitempty"`
}

// DeploymentRequest represents a request to deploy a contract
type DeploymentRequest struct {
	// Contract ID (optional, will be generated if not provided)
	ContractID string

	// Contract language (solidity, vyper, go, node, native)
	Language string

	// Contract source code or bytecode
	Code []byte

	// Account deploying the contract
	Deployer string

	// Constructor arguments
	ConstructorArgs []ConstructorArgument

	// Initial value to send to contract
	InitialValue uint64

	// Gas limit for deployment
	GasLimit uint64

	// Additional metadata
	Metadata DeploymentMetadata

	// Deployment options
	Options DeploymentOptions
}

// DeploymentResponse represents the response from a deployment
type DeploymentResponse struct {
	// Deployed contract ID/address
	ContractID string

	// Transaction hash of deployment
	TransactionHash string

	// Gas used during deployment
	GasUsed uint64

	// Contract version
	Version string

	// Deployment timestamp
	DeployedAt time.Time

	// Events emitted during deployment
	Events []DeploymentEvent

	// Runtime type that was used for deployment
	RuntimeType runtime.RuntimeType
}

// DeploymentEvent represents an event during deployment
type DeploymentEvent struct {
	// Event name
	Name string

	// Event parameters
	Parameters []EventParameter

	// Raw event data
	Data []byte
}

// UpgradeRequest represents a request to upgrade a contract
type UpgradeRequest struct {
	// Contract ID to upgrade
	ContractID string

	// New version number
	NewVersion string

	// New contract code
	NewCode []byte

	// Account authorizing the upgrade
	Authorizer string

	// Migration data for state upgrade
	MigrationData []byte

	// Additional metadata
	Metadata DeploymentMetadata

	// Upgrade options
	Options DeploymentOptions
}

// UpgradeResponse represents the response from an upgrade
type UpgradeResponse struct {
	// Contract ID
	ContractID string

	// Previous version
	PreviousVersion string

	// New version
	NewVersion string

	// Upgrade timestamp
	UpgradedAt time.Time

	// Success status
	Success bool

	// Error message if failed
	Error string
}

// ContractVersion represents a version of a deployed contract
type ContractVersion struct {
	// Version number
	Version string

	// Contract ID
	ContractID string

	// Deployment transaction hash
	DeploymentHash string

	// Contract code
	Code []byte

	// Hash of the code
	CodeHash string

	// Deployment timestamp
	DeployedAt time.Time

	// Account that deployed this version
	DeployedBy string

	// Runtime type
	RuntimeType runtime.RuntimeType

	// Version metadata
	Metadata DeploymentMetadata

	// Whether this version is active
	Active bool

	// Previous version (for upgrades)
	PreviousVersion string
}

// DeploymentContext tracks the context of a deployment
type DeploymentContext struct {
	// Original request
	Request DeploymentRequest

	// Contract ID (after generation)
	ContractID string

	// Start time
	StartTime time.Time

	// End time
	EndTime time.Time

	// Current status
	Status DeploymentStatus

	// Gas used
	GasUsed uint64

	// Error message if failed
	Error string

	// Additional context data
	Data ContextData
}

// UpgradeContext tracks the context of an upgrade
type UpgradeContext struct {
	// Original request
	Request UpgradeRequest

	// Current version before upgrade
	CurrentVersion string

	// New version after upgrade
	NewVersion string

	// Start time
	StartTime time.Time

	// End time
	EndTime time.Time

	// Current status
	Status UpgradeStatus

	// Error message if failed
	Error string

	// Additional context data
	Data ContextData
}

// DeploymentValidator validates deployment requests
type DeploymentValidator interface {
	// ValidateDeployment validates a deployment request
	ValidateDeployment(req DeploymentRequest) error

	// ValidateUpgrade validates an upgrade request
	ValidateUpgrade(req UpgradeRequest) error
}

// VersionTracker tracks contract versions
type VersionTracker interface {
	// AddVersion adds a new version
	AddVersion(version *ContractVersion) error

	// GetVersion retrieves a specific version
	GetVersion(contractID, version string) (*ContractVersion, error)

	// GetCurrentVersion retrieves the current active version
	GetCurrentVersion(contractID string) (*ContractVersion, error)

	// GetVersionHistory retrieves all versions for a contract
	GetVersionHistory(contractID string) ([]*ContractVersion, error)

	// DeactivateVersion deactivates a version
	DeactivateVersion(contractID, version string) error
}

// DeploymentHistory tracks deployment history
type DeploymentHistory interface {
	// RecordDeploymentAttempt records a deployment attempt
	RecordDeploymentAttempt(ctx *DeploymentContext) error

	// UpdateDeploymentStatus updates the status of a deployment
	UpdateDeploymentStatus(ctx *DeploymentContext) error

	// RecordUpgradeAttempt records an upgrade attempt
	RecordUpgradeAttempt(ctx *UpgradeContext) error

	// UpdateUpgradeStatus updates the status of an upgrade
	UpdateUpgradeStatus(ctx *UpgradeContext) error

	// GetContractHistory retrieves deployment history for a contract
	GetContractHistory(contractID string) ([]*DeploymentContext, error)
}
