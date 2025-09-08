// Package runtime provides the core interfaces and types for the hybrid VM architecture
package runtime

import (
	"context"
	"diamante/common"
	"diamante/storage"
	"time"

	"github.com/sirupsen/logrus"
)

// RuntimeType represents the type of runtime (EVM, Chaincode, Native)
type RuntimeType string

const (
	// RuntimeTypeEVM represents the Ethereum Virtual Machine runtime
	RuntimeTypeEVM RuntimeType = "evm"

	// RuntimeTypeChaincode represents the Hyperledger Fabric chaincode runtime
	RuntimeTypeChaincode RuntimeType = "chaincode"

	// RuntimeTypeNative represents the Diamante native contract runtime
	RuntimeTypeNative RuntimeType = "native"

	// RuntimeTypeWASM represents the WebAssembly runtime
	RuntimeTypeWASM RuntimeType = "wasm"
)

// ContractParameters represents typed contract parameters
type ContractParameters struct {
	StringParams  map[string]string  `json:"string_params,omitempty"`
	IntParams     map[string]int64   `json:"int_params,omitempty"`
	FloatParams   map[string]float64 `json:"float_params,omitempty"`
	BoolParams    map[string]bool    `json:"bool_params,omitempty"`
	BytesParams   map[string][]byte  `json:"bytes_params,omitempty"`
	AddressParams map[string]string  `json:"address_params,omitempty"`
}

// GetString retrieves a string parameter by key
func (cp *ContractParameters) GetString(key string) (string, bool) {
	if cp == nil || cp.StringParams == nil {
		return "", false
	}
	val, ok := cp.StringParams[key]
	return val, ok
}

// GetBool retrieves a bool parameter by key
func (cp *ContractParameters) GetBool(key string) (bool, bool) {
	if cp == nil || cp.BoolParams == nil {
		return false, false
	}
	val, ok := cp.BoolParams[key]
	return val, ok
}

// SetString sets a string parameter
func (cp *ContractParameters) SetString(key, value string) {
	if cp.StringParams == nil {
		cp.StringParams = make(map[string]string)
	}
	cp.StringParams[key] = value
}

// SetBool sets a bool parameter
func (cp *ContractParameters) SetBool(key string, value bool) {
	if cp.BoolParams == nil {
		cp.BoolParams = make(map[string]bool)
	}
	cp.BoolParams[key] = value
}

// IsEmpty checks if parameters are empty
func (cp *ContractParameters) IsEmpty() bool {
	if cp == nil {
		return true
	}
	return len(cp.StringParams) == 0 && len(cp.IntParams) == 0 &&
		len(cp.FloatParams) == 0 && len(cp.BoolParams) == 0 &&
		len(cp.BytesParams) == 0 && len(cp.AddressParams) == 0
}

// DeploymentOptions contains typed deployment configuration
type DeploymentOptions struct {
	EnvironmentVars map[string]string `json:"environment_vars,omitempty"`
	ResourceLimits  ResourceLimits    `json:"resource_limits,omitempty"`
	SecurityPolicy  SecurityPolicy    `json:"security_policy,omitempty"`
	NetworkPolicy   NetworkPolicy     `json:"network_policy,omitempty"`
}

// ResourceLimits defines resource constraints
type ResourceLimits struct {
	MaxMemoryMB      int           `json:"max_memory_mb"`
	MaxCPUPercent    float64       `json:"max_cpu_percent"`
	MaxStorageMB     int           `json:"max_storage_mb"`
	MaxNetworkKbps   int           `json:"max_network_kbps"`
	ExecutionTimeout time.Duration `json:"execution_timeout"`
}

// SecurityPolicy defines security constraints
type SecurityPolicy struct {
	AllowNetworkAccess bool     `json:"allow_network_access"`
	AllowFileAccess    bool     `json:"allow_file_access"`
	AllowedDomains     []string `json:"allowed_domains,omitempty"`
	RestrictedAPIs     []string `json:"restricted_apis,omitempty"`
	RequireSignature   bool     `json:"require_signature"`
	TrustedPublishers  []string `json:"trusted_publishers,omitempty"`
}

// NetworkPolicy defines network access rules
type NetworkPolicy struct {
	IngressRules []NetworkRule `json:"ingress_rules,omitempty"`
	EgressRules  []NetworkRule `json:"egress_rules,omitempty"`
}

// NetworkRule defines a network access rule
type NetworkRule struct {
	Protocol    string   `json:"protocol"`
	Ports       []int    `json:"ports,omitempty"`
	Addresses   []string `json:"addresses,omitempty"`
	Action      string   `json:"action"` // "allow" or "deny"
	Description string   `json:"description,omitempty"`
}

// RuntimeSpecificConfig contains runtime-specific configuration
type RuntimeSpecificConfig struct {
	EVMConfig       *EVMConfig       `json:"evm_config,omitempty"`
	ChaincodeConfig *ChaincodeConfig `json:"chaincode_config,omitempty"`
	NativeConfig    *NativeConfig    `json:"native_config,omitempty"`
	WASMConfig      *WASMConfig      `json:"wasm_config,omitempty"`
}

// IsEmpty checks if the config has any values set
func (rsc *RuntimeSpecificConfig) IsEmpty() bool {
	return rsc.EVMConfig == nil && rsc.ChaincodeConfig == nil && rsc.NativeConfig == nil && rsc.WASMConfig == nil
}

// ToMap converts the config to a map for validation
func (rsc *RuntimeSpecificConfig) ToMap() map[string]interface{} {
	result := make(map[string]interface{})
	if rsc.EVMConfig != nil {
		result["evm_config"] = rsc.EVMConfig
	}
	if rsc.ChaincodeConfig != nil {
		result["chaincode_config"] = rsc.ChaincodeConfig
	}
	if rsc.NativeConfig != nil {
		result["native_config"] = rsc.NativeConfig
	}
	if rsc.WASMConfig != nil {
		result["wasm_config"] = rsc.WASMConfig
	}
	return result
}

// GetConfigForRuntime returns the config for a specific runtime type
func (rsc *RuntimeSpecificConfig) GetConfigForRuntime(runtimeType RuntimeType) interface{} {
	switch runtimeType {
	case RuntimeTypeEVM:
		return rsc.EVMConfig
	case RuntimeTypeChaincode:
		return rsc.ChaincodeConfig
	case RuntimeTypeNative:
		return rsc.NativeConfig
	case RuntimeTypeWASM:
		return rsc.WASMConfig
	default:
		return nil
	}
}

// EVMConfig contains EVM-specific configuration
type EVMConfig struct {
	ChainID             uint64 `json:"chain_id"`
	GasLimit            uint64 `json:"gas_limit"`
	GasPrice            uint64 `json:"gas_price"`
	EnableOptimizations bool   `json:"enable_optimizations"`
	DebugMode           bool   `json:"debug_mode"`
}

// ChaincodeConfig contains chaincode-specific configuration
type ChaincodeConfig struct {
	Language         string        `json:"language"`
	DockerEndpoint   string        `json:"docker_endpoint"`
	NetworkMode      string        `json:"network_mode"`
	MaxContainers    int           `json:"max_containers"`
	ContainerTimeout time.Duration `json:"container_timeout"`
	BuildTimeout     time.Duration `json:"build_timeout"`
}

// NativeConfig contains native runtime configuration
type NativeConfig struct {
	PluginPath    string        `json:"plugin_path"`
	EnableJIT     bool          `json:"enable_jit"`
	EnableSandbox bool          `json:"enable_sandbox"`
	MaxPlugins    int           `json:"max_plugins"`
	PluginTimeout time.Duration `json:"plugin_timeout"`
}

// WASMConfig contains WASM-specific configuration
type WASMConfig struct {
	MaxMemoryPages  uint32        `json:"max_memory_pages"`
	MaxTableSize    uint32        `json:"max_table_size"`
	EnableJIT       bool          `json:"enable_jit"`
	EnableAOT       bool          `json:"enable_aot"`
	OptimizationLvl int           `json:"optimization_level"`
	Timeout         time.Duration `json:"timeout"`
}

// ContractValue represents a typed contract value
type ContractValue struct {
	Type      string  `json:"type"` // "string", "int", "float", "bool", "bytes", "address"
	StringVal string  `json:"string_val,omitempty"`
	IntVal    int64   `json:"int_val,omitempty"`
	FloatVal  float64 `json:"float_val,omitempty"`
	BoolVal   bool    `json:"bool_val,omitempty"`
	BytesVal  []byte  `json:"bytes_val,omitempty"`
}

// Runtime is the core interface that all VM runtimes must implement
type Runtime interface {
	// Type returns the runtime type identifier
	Type() RuntimeType

	// Initialize sets up the runtime with necessary configuration
	Initialize(config RuntimeConfig) error

	// Compile validates and compiles contract code for this runtime
	Compile(code []byte, metadata RuntimeMetadata) (*CompiledContract, error)

	// Deploy deploys a compiled contract to the blockchain
	Deploy(ctx context.Context, contract *CompiledContract, args DeploymentArgs) (*DeploymentResult, error)

	// Execute executes a function on a deployed contract
	Execute(ctx context.Context, call ContractCall) (*ExecutionResult, error)

	// Upgrade upgrades an existing contract to a new version
	Upgrade(ctx context.Context, contractID string, newCode []byte, args UpgradeArgs) error

	// GetContractInfo retrieves information about a deployed contract
	GetContractInfo(contractID string) (*ContractInfo, error)

	// Start starts the runtime (for long-running runtimes like chaincode containers)
	Start() error

	// Stop gracefully stops the runtime
	Stop() error

	// HealthCheck returns the health status of the runtime
	HealthCheck() error
}

// RuntimeConfig contains configuration for initializing a runtime
type RuntimeConfig struct {
	// Common configuration
	ChainID    string
	LedgerAPI  common.LedgerAPI
	StateStore storage.LedgerStore
	Logger     *logrus.Logger

	// Runtime-specific configuration
	RuntimeSpecific RuntimeSpecificConfig
}

// CompiledContract represents a compiled smart contract ready for deployment
type CompiledContract struct {
	// Runtime type
	Runtime RuntimeType

	// Compiled bytecode or package
	Code []byte

	// Application Binary Interface
	ABI string

	// Source code hash for verification
	SourceHash string

	// Compilation metadata
	Metadata RuntimeMetadata

	// Estimated resource requirements
	ResourceRequirements ResourceRequirements
}

// ResourceRequirements specifies the resources needed by a contract
type ResourceRequirements struct {
	// Estimated memory usage in MB
	MemoryMB int

	// Estimated CPU cores needed
	CPUCores float64

	// Estimated storage in MB
	StorageMB int

	// Network bandwidth requirements
	NetworkBandwidthKbps int
}

// DeploymentArgs contains arguments for deploying a contract
type DeploymentArgs struct {
	// Deployer account ID
	Deployer string

	// Constructor arguments
	ConstructorArgs ContractParameters

	// Initial contract value (for payable constructors)
	Value uint64

	// Gas limit for deployment
	GasLimit uint64

	// Additional deployment options
	Options DeploymentOptions
}

// DeploymentResult contains the result of a contract deployment
type DeploymentResult struct {
	// Deployed contract ID/address
	ContractID string

	// Transaction hash of deployment
	TransactionHash string

	// Gas used during deployment
	GasUsed uint64

	// Deployment timestamp
	Timestamp time.Time

	// Any events emitted during deployment
	Events []ContractEvent
}

// ContractCall represents a call to a contract function
type ContractCall struct {
	// Contract ID/address to call
	ContractID string

	// Function name to call
	Function string

	// Function arguments
	Args ContractParameters

	// Caller account ID
	Caller string

	// Value to send with the call
	Value uint64

	// Gas limit for the call
	GasLimit uint64

	// Additional call options
	Options DeploymentOptions
}

// ExecutionResult contains the result of a contract execution
type ExecutionResult struct {
	// Return values from the function
	ReturnData []ContractValue

	// Raw return bytes
	RawReturnData []byte

	// Gas used during execution
	GasUsed uint64

	// Execution status
	Success bool

	// Error message if execution failed
	Error string

	// Events emitted during execution
	Events []ContractEvent

	// State changes made during execution
	StateChanges []StateChange
}

// ContractEvent represents an event emitted by a contract
type ContractEvent struct {
	// Contract that emitted the event
	ContractID string

	// Event name
	Name string

	// Event parameters
	Parameters ContractParameters

	// Raw event data
	Data []byte

	// Block number where event was emitted
	BlockNumber uint64

	// Transaction hash
	TransactionHash string

	// Event index in the transaction
	Index uint
}

// StateChange represents a state change made during contract execution
type StateChange struct {
	// Key that was changed
	Key []byte

	// Previous value
	OldValue []byte

	// New value
	NewValue []byte

	// Contract that made the change
	ContractID string
}

// UpgradeArgs contains arguments for upgrading a contract
type UpgradeArgs struct {
	// Account authorizing the upgrade
	Authorizer string

	// New contract version
	Version string

	// Migration data for state upgrade
	MigrationData []byte

	// Additional upgrade options
	Options DeploymentOptions
}

// ContractInfo contains information about a deployed contract
type ContractInfo struct {
	// Contract ID/address
	ContractID string

	// Runtime type
	Runtime RuntimeType

	// Contract owner
	Owner string

	// Deployment timestamp
	DeployedAt time.Time

	// Current version
	Version string

	// Contract state hash
	StateHash string

	// Whether contract is active
	Active bool

	// Contract metadata
	Metadata RuntimeMetadata
}

// ContractState represents typed contract state
type ContractState struct {
	StringState  map[string]string  `json:"string_state,omitempty"`
	IntState     map[string]int64   `json:"int_state,omitempty"`
	FloatState   map[string]float64 `json:"float_state,omitempty"`
	BoolState    map[string]bool    `json:"bool_state,omitempty"`
	BytesState   map[string][]byte  `json:"bytes_state,omitempty"`
	AddressState map[string]string  `json:"address_state,omitempty"`
}

// RuntimeEventHandler handles events from all runtimes
type RuntimeEventHandler interface {
	// HandleEvent processes an event from any runtime
	HandleEvent(event ContractEvent) error
}

// RuntimeStateManager manages state across all runtimes
type RuntimeStateManager interface {
	// GetState retrieves state for a contract
	GetState(contractID string, key []byte) ([]byte, error)

	// SetState sets state for a contract
	SetState(contractID string, key []byte, value []byte) error

	// DeleteState deletes state for a contract
	DeleteState(contractID string, key []byte) error

	// HasState checks if state exists
	HasState(contractID string, key []byte) (bool, error)

	// GetAllState retrieves all state for a contract
	GetAllState(contractID string) (ContractState, error)
}
