// Package evm provides runtime types to avoid circular dependencies
package evm

import (
	"context"
	"time"
)

// RuntimeType identifies the type of runtime
type RuntimeType string

const (
	// RuntimeTypeEVM represents the Ethereum Virtual Machine runtime
	RuntimeTypeEVM RuntimeType = "evm"
)

// Runtime defines the interface for a contract runtime
type Runtime interface {
	// Type returns the runtime type identifier
	Type() RuntimeType

	// Initialize sets up the runtime with necessary configuration
	Initialize(config RuntimeConfig) error

	// Compile validates and compiles contract code for this runtime
	Compile(code []byte, metadata map[string]interface{}) (*CompiledContract, error)

	// Deploy deploys a compiled contract to the blockchain
	Deploy(ctx context.Context, contract *CompiledContract, args DeploymentArgs) (*DeploymentResult, error)

	// Execute executes a function on a deployed contract
	Execute(ctx context.Context, call ContractCall) (*ExecutionResult, error)

	// Upgrade upgrades an existing contract to a new version
	Upgrade(ctx context.Context, contractID string, newCode []byte, args UpgradeArgs) error

	// GetContractInfo retrieves information about a deployed contract
	GetContractInfo(contractID string) (*ContractInfo, error)

	// Start starts the runtime
	Start() error

	// Stop gracefully stops the runtime
	Stop() error

	// HealthCheck returns the health status of the runtime
	HealthCheck() error
}

// RuntimeConfig contains configuration for initializing a runtime
type RuntimeConfig struct {
	ChainID         string
	LedgerAPI       interface{}
	StateStore      interface{}
	Logger          interface{}
	RuntimeSpecific map[string]interface{}
}

// CompiledContract represents a compiled smart contract
type CompiledContract struct {
	Runtime              RuntimeType
	Code                 []byte
	ABI                  string
	SourceHash           string
	Metadata             map[string]interface{}
	ResourceRequirements ResourceRequirements
}

// ResourceRequirements specifies the resource needs for contract execution
type ResourceRequirements struct {
	MemoryMB             int
	CPUCores             float64
	StorageMB            int
	NetworkBandwidthKbps int
}

// DeploymentArgs contains arguments for contract deployment
type DeploymentArgs struct {
	Deployer        string
	Value           uint64
	GasLimit        uint64
	ConstructorArgs []interface{}
	Options         map[string]interface{}
}

// DeploymentResult contains the result of a contract deployment
type DeploymentResult struct {
	ContractID      string
	TransactionHash string
	GasUsed         uint64
	Timestamp       time.Time
	Events          []ContractEvent
}

// ContractCall represents a call to a contract function
type ContractCall struct {
	ContractID string
	Caller     string
	Function   string
	Arguments  []interface{}
	Value      uint64
	GasLimit   uint64
	Options    map[string]interface{}
}

// ExecutionResult contains the result of contract execution
type ExecutionResult struct {
	Success       bool
	ReturnData    []interface{}
	RawReturnData []byte
	GasUsed       uint64
	Events        []ContractEvent
	StateChanges  []StateChange
	Error         string
}

// UpgradeArgs contains arguments for contract upgrade
type UpgradeArgs struct {
	Authorizer string
	Version    string
	Options    map[string]interface{}
}

// ContractInfo contains information about a deployed contract
type ContractInfo struct {
	ContractID string
	Runtime    RuntimeType
	Owner      string
	DeployedAt time.Time
	Version    string
	StateHash  string
	Active     bool
	Metadata   map[string]interface{}
}

// ContractEvent represents an event emitted by a contract
type ContractEvent struct {
	ContractID      string
	Name            string
	Parameters      map[string]interface{}
	Data            []byte
	BlockNumber     uint64
	TransactionHash string
	Index           uint
}

// StateChange represents a change in contract state
type StateChange struct {
	Key        []byte
	OldValue   []byte
	NewValue   []byte
	ContractID string
}
