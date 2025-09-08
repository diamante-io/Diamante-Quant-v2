// Package chaincode provides enterprise-grade chaincode adapter for comprehensive contract lifecycle management
package chaincode

import (
	"context"
	"fmt"
	"sync"
	"time"

	"diamante/storage"
	"diamante/vm/runtime"

	"github.com/sirupsen/logrus"
)

// ChaincodeAdapter provides seamless integration between chaincode runtime and hybrid VM ecosystem
type ChaincodeAdapter struct {
	// Core components
	runtime      *ChaincodeRuntime
	stateManager *ChaincodeStateManager
	migrator     *ChaincodeMigrator
	persistence  *ChaincodePersistence
	metadata     *ChaincodeMetadataStore
	integrator   *HybridVMIntegrator

	// Configuration
	config *ChaincodeAdapterConfig
	logger *logrus.Logger

	// Lifecycle management
	lifecycle *ChaincodeLifecycleManager

	// Thread safety
	mu sync.RWMutex

	// Status tracking
	initialized bool
	running     bool
}

// ChaincodeAdapterConfig configures the chaincode adapter
type ChaincodeAdapterConfig struct {
	// Integration settings
	EnableHybridVM         bool
	EnableStatePersistence bool
	EnableMetadataStore    bool
	EnableMigration        bool

	// Performance settings
	StateTimeout          time.Duration
	MigrationTimeout      time.Duration
	PersistenceInterval   time.Duration
	MetadataFlushInterval time.Duration

	// Resource limits
	MaxStateSize       int64
	MaxMetadataSize    int64
	MaxMigrationJobs   int
	MaxPersistenceJobs int

	// Storage settings
	StateStorePath    string
	MetadataStorePath string
	BackupPath        string
}

// ChaincodeStateManager handles enterprise-grade state management for chaincodes
type ChaincodeStateManager struct {
	store       storage.LedgerStore
	cache       map[string]map[string][]byte // contractID -> key -> value
	dirtyFlags  map[string]map[string]bool   // contractID -> key -> dirty
	locks       map[string]*sync.RWMutex     // contractID -> lock
	mu          sync.RWMutex
	logger      *logrus.Logger
	persistence *ChaincodePersistence

	// Metrics
	stateReads  int64
	stateWrites int64
	cacheHits   int64
	cacheMisses int64
}

// ChaincodeMigrator handles contract migration operations
type ChaincodeMigrator struct {
	stateManager *ChaincodeStateManager
	persistence  *ChaincodePersistence
	jobs         map[string]*MigrationJob
	mu           sync.RWMutex
	logger       *logrus.Logger

	// Migration tracking
	completedMigrations map[string]time.Time
	migrationHistory    []MigrationRecord
}

// ChaincodePersistence manages contract data persistence and backup/recovery
type ChaincodePersistence struct {
	store          storage.LedgerStore
	backupStore    storage.LedgerStore
	scheduler      *PersistenceScheduler
	checksums      map[string]string // contractID -> checksum
	backupMetadata map[string]BackupMetadata
	mu             sync.RWMutex
	logger         *logrus.Logger

	// Persistence metrics
	persistenceOps     int64
	backupOps          int64
	recoveryOps        int64
	checksumMismatches int64
}

// ChaincodeMetadataStore manages contract metadata with versioning
type ChaincodeMetadataStore struct {
	store     storage.LedgerStore
	cache     map[string]*ContractMetadata
	versions  map[string][]*MetadataVersion
	mu        sync.RWMutex
	logger    *logrus.Logger
	flushChan chan string

	// Metadata statistics
	metadataReads  int64
	metadataWrites int64
	versionCount   int64
}

// HybridVMIntegrator manages integration with the hybrid VM ecosystem
type HybridVMIntegrator struct {
	evmBridge       *EVMBridge
	nativeBridge    *NativeBridge
	crossCallRouter *CrossCallRouter
	eventBus        *runtime.UnifiedEventBus
	resourceManager *runtime.ResourceManager
	mu              sync.RWMutex
	logger          *logrus.Logger

	// Integration metrics
	crossCalls         int64
	evmInteractions    int64
	nativeInteractions int64
	routingErrors      int64
}

// ChaincodeLifecycleManager manages complete contract lifecycle
type ChaincodeLifecycleManager struct {
	adapter   *ChaincodeAdapter
	stages    map[string]*LifecycleStage
	workflows map[string]*LifecycleWorkflow
	mu        sync.RWMutex
	logger    *logrus.Logger

	// Lifecycle tracking
	deploymentsManaged  int64
	upgradesManaged     int64
	terminationsManaged int64
}

// Data structures for specialized operations

// MigrationJob represents a contract migration operation
type MigrationJob struct {
	ID          string
	ContractID  string
	FromVersion string
	ToVersion   string
	Status      MigrationStatus
	StartTime   time.Time
	EndTime     time.Time
	Operations  []MigrationOperation
	Rollback    []RollbackOperation
	Error       error
	Progress    float64
}

// MigrationStatus represents the status of a migration
type MigrationStatus string

const (
	MigrationPending    MigrationStatus = "pending"
	MigrationRunning    MigrationStatus = "running"
	MigrationCompleted  MigrationStatus = "completed"
	MigrationFailed     MigrationStatus = "failed"
	MigrationRolledBack MigrationStatus = "rolled_back"
)

// MigrationRecord tracks completed migrations
type MigrationRecord struct {
	JobID       string
	ContractID  string
	FromVersion string
	ToVersion   string
	Timestamp   time.Time
	Duration    time.Duration
	Success     bool
	Error       string
}

// BackupMetadata contains backup operation metadata
type BackupMetadata struct {
	ContractID    string
	BackupTime    time.Time
	DataSize      int64
	Checksum      string
	Version       string
	RestorePoints []time.Time
}

// ContractConfiguration represents typed contract configuration
type ContractConfiguration struct {
	// Runtime settings
	MaxExecutionTime int64  `json:"max_execution_time_ms,omitempty"`
	MaxMemoryUsage   int64  `json:"max_memory_mb,omitempty"`
	MaxStateSize     int64  `json:"max_state_size_bytes,omitempty"`
	EnableLogging    bool   `json:"enable_logging,omitempty"`
	LogLevel         string `json:"log_level,omitempty"`

	// Resource limits
	CPULimit         int   `json:"cpu_limit,omitempty"`
	NetworkBandwidth int64 `json:"network_bandwidth_kbps,omitempty"`
	StorageQuota     int64 `json:"storage_quota_bytes,omitempty"`

	// Feature flags
	EnableEvents      bool `json:"enable_events,omitempty"`
	EnableCrossCalls  bool `json:"enable_cross_calls,omitempty"`
	EnablePersistence bool `json:"enable_persistence,omitempty"`

	// Environment
	Environment string `json:"environment,omitempty"` // dev, test, staging, prod
	NetworkID   string `json:"network_id,omitempty"`
	ChainID     string `json:"chain_id,omitempty"`
}

// ContractPermissions represents typed contract permissions
type ContractPermissions struct {
	// Access control
	ReadAccess    []string `json:"read_access,omitempty"`    // Addresses with read access
	WriteAccess   []string `json:"write_access,omitempty"`   // Addresses with write access
	ExecuteAccess []string `json:"execute_access,omitempty"` // Addresses with execute access
	AdminAccess   []string `json:"admin_access,omitempty"`   // Addresses with admin access

	// Operation permissions
	AllowUpgrade     bool `json:"allow_upgrade,omitempty"`
	AllowMigration   bool `json:"allow_migration,omitempty"`
	AllowTermination bool `json:"allow_termination,omitempty"`
	AllowStateExport bool `json:"allow_state_export,omitempty"`

	// Call permissions
	AllowedCallers   []string `json:"allowed_callers,omitempty"`
	BlockedCallers   []string `json:"blocked_callers,omitempty"`
	AllowedFunctions []string `json:"allowed_functions,omitempty"`
	BlockedFunctions []string `json:"blocked_functions,omitempty"`
}

// ContractCustomAttributes represents typed custom attributes
type ContractCustomAttributes struct {
	// Business metadata
	Category    string `json:"category,omitempty"`
	Subcategory string `json:"subcategory,omitempty"`
	Industry    string `json:"industry,omitempty"`
	UseCase     string `json:"use_case,omitempty"`

	// Technical metadata
	Framework    string   `json:"framework,omitempty"`
	SDKVersion   string   `json:"sdk_version,omitempty"`
	Dependencies []string `json:"dependencies,omitempty"`
	Interfaces   []string `json:"interfaces,omitempty"`

	// Compliance
	ComplianceType string   `json:"compliance_type,omitempty"`
	Certifications []string `json:"certifications,omitempty"`
	AuditReports   []string `json:"audit_reports,omitempty"`

	// Additional metadata as key-value pairs
	CustomFields map[string]string `json:"custom_fields,omitempty"`
}

// ContractMetadata represents contract metadata
type ContractMetadata struct {
	ContractID       string
	Name             string
	Version          string
	Description      string
	Tags             []string
	Owner            string
	Collaborators    []string
	CreatedAt        time.Time
	UpdatedAt        time.Time
	SchemaVersion    string
	Dependencies     []string
	Configuration    ContractConfiguration
	Permissions      ContractPermissions
	CustomAttributes ContractCustomAttributes
}

// PersistenceScheduler represents a scheduler for persistence jobs
type PersistenceScheduler struct {
	jobs     []*PersistenceJob
	ticker   *time.Ticker
	stopChan chan bool
	mu       sync.RWMutex
	logger   *logrus.Logger
	running  bool
}

// PersistenceJob represents a scheduled persistence operation
type PersistenceJob struct {
	ID         string
	ContractID string
	Type       PersistenceType
	Schedule   time.Duration
	LastRun    time.Time
	NextRun    time.Time
	Enabled    bool
}

// PersistenceType defines types of persistence operations
type PersistenceType string

const (
	PersistenceTypeState    PersistenceType = "state"
	PersistenceTypeMetadata PersistenceType = "metadata"
	PersistenceTypeBackup   PersistenceType = "backup"
	PersistenceTypeSnapshot PersistenceType = "snapshot"
)

// Bridge structures for hybrid VM integration

// EVMBridge enables chaincode-EVM interoperability
type EVMBridge struct {
	evmRuntime runtime.Runtime
	converter  *TypeConverter
	gasMapper  *GasMapper
	logger     *logrus.Logger
}

// NativeBridge enables chaincode-native interoperability
type NativeBridge struct {
	nativeRuntime runtime.Runtime
	converter     *TypeConverter
	securityGuard *SecurityGuard
	logger        *logrus.Logger
}

// CrossCallRouter routes calls between different runtimes
type CrossCallRouter struct {
	routes   map[RuntimeRoute]*RouteHandler
	policies map[string]*RoutingPolicy
	mu       sync.RWMutex
	logger   *logrus.Logger
}

// Lifecycle management structures

// LifecycleStage represents a stage in contract lifecycle
type LifecycleStage struct {
	Name         string
	Description  string
	Dependencies []string
	Validators   []StageValidator
	Executors    []StageExecutor
	Rollback     []RollbackHandler
}

// LifecycleWorkflow defines a complete lifecycle workflow
type LifecycleWorkflow struct {
	Name         string
	Description  string
	Stages       []*LifecycleStage
	CurrentStage int
	Status       WorkflowStatus
	StartTime    time.Time
	EndTime      time.Time
	Metadata     WorkflowMetadata
}

// Supporting types and interfaces

type RuntimeRoute struct {
	From RuntimeType
	To   RuntimeType
}

type RouteHandler struct {
	Handler    func(call runtime.ContractCall) (*runtime.ExecutionResult, error)
	Middleware []RouteMiddleware
}

type RoutingPolicy struct {
	AllowedRoutes []RuntimeRoute
	GasLimits     map[RuntimeRoute]uint64
	Timeouts      map[RuntimeRoute]time.Duration
}

type RouteMiddleware interface {
	Process(call runtime.ContractCall) error
}

// AdapterState represents comprehensive adapter state
type AdapterState struct {
	StateValues map[string]StateValue `json:"state_values"`
	Metadata    *ContractMetadata     `json:"_metadata,omitempty"`
	Persistence StatePersistenceInfo  `json:"_persistence,omitempty"`
}

// AdapterMetrics represents comprehensive adapter metrics
type AdapterMetrics struct {
	Initialized  bool                 `json:"initialized"`
	Running      bool                 `json:"running"`
	StateManager StateManagerMetrics  `json:"stateManager,omitempty"`
	Migrator     MigratorMetrics      `json:"migrator,omitempty"`
	Persistence  PersistenceMetrics   `json:"persistence,omitempty"`
	Metadata     MetadataStoreMetrics `json:"metadata,omitempty"`
	HybridVM     HybridVMMetrics      `json:"hybridVM,omitempty"`
	Lifecycle    LifecycleMetrics     `json:"lifecycle,omitempty"`
}

// TypeConverter struct with typed conversion maps
type TypeConverter struct {
	fromChaincode TypeConversionMap
	toChaincode   TypeConversionMap
}

// TypeConversionMap represents type conversion mappings
type TypeConversionMap struct {
	StringToString map[string]string `json:"string_to_string,omitempty"`
	StringToInt    map[string]int64  `json:"string_to_int,omitempty"`
	StringToBool   map[string]bool   `json:"string_to_bool,omitempty"`
	IntToString    map[int64]string  `json:"int_to_string,omitempty"`
	BoolToString   map[bool]string   `json:"bool_to_string,omitempty"`
}

type GasMapper struct {
	evmToChaincode map[uint64]uint64
	chaincodeToEVM map[uint64]uint64
}

type SecurityGuard struct {
	policies []SecurityPolicy
	logger   *logrus.Logger
}

type SecurityPolicy interface {
	Validate(call runtime.ContractCall) error
}

type StageValidator interface {
	Validate(context.Context, interface{}) error
}

type StageExecutor interface {
	Execute(context.Context, interface{}) error
}

type RollbackHandler interface {
	Rollback(context.Context, interface{}) error
}

type WorkflowStatus string

const (
	WorkflowStatusPending    WorkflowStatus = "pending"
	WorkflowStatusRunning    WorkflowStatus = "running"
	WorkflowStatusCompleted  WorkflowStatus = "completed"
	WorkflowStatusFailed     WorkflowStatus = "failed"
	WorkflowStatusRolledBack WorkflowStatus = "rolled_back"
)

// WorkflowMetadata represents typed workflow metadata
type WorkflowMetadata struct {
	ContractID   string            `json:"contract_id"`
	Version      string            `json:"version"`
	Initiator    string            `json:"initiator"`
	Priority     int               `json:"priority"`
	Tags         []string          `json:"tags,omitempty"`
	CustomFields map[string]string `json:"custom_fields,omitempty"`
}

// NewChaincodeAdapter creates a new enterprise-grade chaincode adapter
func NewChaincodeAdapter(chaincodeRuntime *ChaincodeRuntime, config *ChaincodeAdapterConfig, logger *logrus.Logger) (*ChaincodeAdapter, error) {
	if chaincodeRuntime == nil {
		return nil, fmt.Errorf("chaincode runtime cannot be nil")
	}
	if config == nil {
		config = DefaultChaincodeAdapterConfig()
	}
	if logger == nil {
		logger = logrus.New()
	}

	adapter := &ChaincodeAdapter{
		runtime: chaincodeRuntime,
		config:  config,
		logger:  logger,
	}

	// Initialize components
	if err := adapter.initializeComponents(); err != nil {
		return nil, fmt.Errorf("failed to initialize adapter components: %w", err)
	}

	adapter.initialized = true
	logger.Info("Chaincode adapter initialized with enterprise-grade components")

	return adapter, nil
}

// DefaultChaincodeAdapterConfig returns default configuration
func DefaultChaincodeAdapterConfig() *ChaincodeAdapterConfig {
	return &ChaincodeAdapterConfig{
		EnableHybridVM:         true,
		EnableStatePersistence: true,
		EnableMetadataStore:    true,
		EnableMigration:        true,
		StateTimeout:           30 * time.Second,
		MigrationTimeout:       5 * time.Minute,
		PersistenceInterval:    1 * time.Minute,
		MetadataFlushInterval:  30 * time.Second,
		MaxStateSize:           100 * 1024 * 1024, // 100MB
		MaxMetadataSize:        10 * 1024 * 1024,  // 10MB
		MaxMigrationJobs:       10,
		MaxPersistenceJobs:     20,
		StateStorePath:         "/tmp/diamante/chaincode/state",
		MetadataStorePath:      "/tmp/diamante/chaincode/metadata",
		BackupPath:             "/tmp/diamante/chaincode/backups",
	}
}

// initializeComponents initializes all adapter components
func (ca *ChaincodeAdapter) initializeComponents() error {
	// Initialize state manager
	stateManager, err := NewChaincodeStateManager(ca.runtime.stateStore, ca.logger)
	if err != nil {
		return fmt.Errorf("failed to initialize state manager: %w", err)
	}
	ca.stateManager = stateManager

	// Initialize persistence layer
	persistence, err := NewChaincodePersistence(ca.runtime.stateStore, ca.config, ca.logger)
	if err != nil {
		return fmt.Errorf("failed to initialize persistence: %w", err)
	}
	ca.persistence = persistence

	// Initialize metadata store
	metadata, err := NewChaincodeMetadataStore(ca.runtime.stateStore, ca.config, ca.logger)
	if err != nil {
		return fmt.Errorf("failed to initialize metadata store: %w", err)
	}
	ca.metadata = metadata

	// Initialize migrator
	migrator, err := NewChaincodeMigrator(ca.stateManager, ca.persistence, ca.logger)
	if err != nil {
		return fmt.Errorf("failed to initialize migrator: %w", err)
	}
	ca.migrator = migrator

	// Initialize hybrid VM integrator if enabled
	if ca.config.EnableHybridVM {
		integrator, err := NewHybridVMIntegrator(ca.logger)
		if err != nil {
			return fmt.Errorf("failed to initialize hybrid VM integrator: %w", err)
		}
		ca.integrator = integrator
	}

	// Initialize lifecycle manager
	lifecycle, err := NewChaincodeLifecycleManager(ca, ca.logger)
	if err != nil {
		return fmt.Errorf("failed to initialize lifecycle manager: %w", err)
	}
	ca.lifecycle = lifecycle

	return nil
}

// Start starts the chaincode adapter and all its components
func (ca *ChaincodeAdapter) Start() error {
	ca.mu.Lock()
	defer ca.mu.Unlock()

	if !ca.initialized {
		return fmt.Errorf("adapter not initialized")
	}

	if ca.running {
		return nil // Already running
	}

	// Start underlying runtime
	if err := ca.runtime.Start(); err != nil {
		return fmt.Errorf("failed to start chaincode runtime: %w", err)
	}

	// Start persistence scheduler
	if ca.config.EnableStatePersistence {
		if err := ca.persistence.StartScheduler(); err != nil {
			return fmt.Errorf("failed to start persistence scheduler: %w", err)
		}
	}

	// Start metadata flusher
	if ca.config.EnableMetadataStore {
		ca.metadata.StartFlusher()
	}

	// Start hybrid VM integrator
	if ca.config.EnableHybridVM && ca.integrator != nil {
		if err := ca.integrator.Start(); err != nil {
			return fmt.Errorf("failed to start hybrid VM integrator: %w", err)
		}
	}

	ca.running = true
	ca.logger.Info("Chaincode adapter started successfully with all enterprise components")

	return nil
}

// Stop gracefully stops the chaincode adapter
func (ca *ChaincodeAdapter) Stop() error {
	ca.mu.Lock()
	defer ca.mu.Unlock()

	if !ca.running {
		return nil // Already stopped
	}

	var errors []error

	// Stop hybrid VM integrator
	if ca.integrator != nil {
		if err := ca.integrator.Stop(); err != nil {
			errors = append(errors, fmt.Errorf("hybrid VM integrator stop: %w", err))
		}
	}

	// Stop metadata flusher
	if ca.metadata != nil {
		ca.metadata.StopFlusher()
	}

	// Stop persistence scheduler
	if ca.persistence != nil {
		if err := ca.persistence.StopScheduler(); err != nil {
			errors = append(errors, fmt.Errorf("persistence scheduler stop: %w", err))
		}
	}

	// Stop underlying runtime
	if err := ca.runtime.Stop(); err != nil {
		errors = append(errors, fmt.Errorf("chaincode runtime stop: %w", err))
	}

	ca.running = false

	if len(errors) > 0 {
		return fmt.Errorf("adapter stopped with errors: %v", errors)
	}

	ca.logger.Info("Chaincode adapter stopped gracefully")
	return nil
}

// DeployContract deploys a contract with full lifecycle management
func (ca *ChaincodeAdapter) DeployContract(ctx context.Context, contract *runtime.CompiledContract, args runtime.DeploymentArgs) (*runtime.DeploymentResult, error) {
	if !ca.running {
		return nil, fmt.Errorf("adapter not running")
	}

	// Create deployment workflow
	workflow, err := ca.lifecycle.CreateDeploymentWorkflow(contract, args)
	if err != nil {
		return nil, fmt.Errorf("failed to create deployment workflow: %w", err)
	}

	// Execute deployment through lifecycle manager
	result, err := ca.lifecycle.ExecuteWorkflow(ctx, workflow)
	if err != nil {
		return nil, fmt.Errorf("deployment workflow failed: %w", err)
	}

	// Store contract metadata
	if ca.config.EnableMetadataStore {
		metadata := ca.extractContractMetadata(contract, args, result)
		if err := ca.metadata.StoreMetadata(result.ContractID, metadata); err != nil {
			ca.logger.WithError(err).Warn("Failed to store contract metadata")
		}
	}

	// Initialize state management for the contract
	if err := ca.stateManager.InitializeContract(result.ContractID); err != nil {
		ca.logger.WithError(err).Warn("Failed to initialize state management")
	}

	// Schedule persistence
	if ca.config.EnableStatePersistence {
		if err := ca.persistence.ScheduleContractPersistence(result.ContractID); err != nil {
			ca.logger.WithError(err).Warn("Failed to schedule contract persistence")
		}
	}

	ca.logger.WithFields(logrus.Fields{
		"contractID": result.ContractID,
		"deployer":   args.Deployer,
		"runtime":    contract.Runtime,
	}).Info("Contract deployed successfully through adapter")

	return result, nil
}

// ExecuteContract executes a contract with enhanced capabilities
func (ca *ChaincodeAdapter) ExecuteContract(ctx context.Context, call runtime.ContractCall) (*runtime.ExecutionResult, error) {
	if !ca.running {
		return nil, fmt.Errorf("adapter not running")
	}

	// Pre-execution processing
	if err := ca.preExecutionProcessing(ctx, &call); err != nil {
		return nil, fmt.Errorf("pre-execution processing failed: %w", err)
	}

	// Load contract state
	if err := ca.stateManager.LoadContractState(call.ContractID); err != nil {
		return nil, fmt.Errorf("failed to load contract state: %w", err)
	}

	// Execute through runtime
	result, err := ca.runtime.Execute(ctx, call)
	if err != nil {
		return nil, fmt.Errorf("runtime execution failed: %w", err)
	}

	// Post-execution processing
	if err := ca.postExecutionProcessing(ctx, &call, result); err != nil {
		ca.logger.WithError(err).Warn("Post-execution processing failed")
	}

	// Persist state changes
	if ca.config.EnableStatePersistence && len(result.StateChanges) > 0 {
		if err := ca.stateManager.PersistStateChanges(call.ContractID, result.StateChanges); err != nil {
			ca.logger.WithError(err).Warn("Failed to persist state changes")
		}
	}

	return result, nil
}

// MigrateContract performs enterprise-grade contract migration
func (ca *ChaincodeAdapter) MigrateContract(ctx context.Context, contractID string, newCode []byte, args runtime.UpgradeArgs) error {
	if !ca.running {
		return fmt.Errorf("adapter not running")
	}

	if !ca.config.EnableMigration {
		return fmt.Errorf("migration not enabled")
	}

	// Create migration job
	job, err := ca.migrator.CreateMigrationJob(contractID, newCode, args)
	if err != nil {
		return fmt.Errorf("failed to create migration job: %w", err)
	}

	// Execute migration
	if err := ca.migrator.ExecuteMigration(ctx, job); err != nil {
		return fmt.Errorf("migration execution failed: %w", err)
	}

	// Update metadata
	if ca.config.EnableMetadataStore {
		if err := ca.metadata.UpdateVersionMetadata(contractID, args.Version); err != nil {
			ca.logger.WithError(err).Warn("Failed to update metadata after migration")
		}
	}

	ca.logger.WithFields(logrus.Fields{
		"contractID": contractID,
		"version":    args.Version,
		"jobID":      job.ID,
	}).Info("Contract migration completed successfully")

	return nil
}

// GetContractState retrieves comprehensive contract state
func (ca *ChaincodeAdapter) GetContractState(contractID string) (*AdapterState, error) {
	if !ca.running {
		return nil, fmt.Errorf("adapter not running")
	}

	// Get state from state manager
	stateData, err := ca.stateManager.GetContractState(contractID)
	if err != nil {
		return nil, fmt.Errorf("failed to get contract state: %w", err)
	}

	adapterState := &AdapterState{
		StateValues: stateData.StateValues,
	}

	// Enhance with metadata if available
	if ca.config.EnableMetadataStore {
		metadata, err := ca.metadata.GetMetadata(contractID)
		if err == nil {
			adapterState.Metadata = metadata
		}
	}

	// Add persistence information
	if ca.config.EnableStatePersistence {
		persistenceInfo := ca.persistence.GetPersistenceInfo(contractID)
		adapterState.Persistence = persistenceInfo
	}

	return adapterState, nil
}

// GetContractMetadata retrieves contract metadata
func (ca *ChaincodeAdapter) GetContractMetadata(contractID string) (*ContractMetadata, error) {
	if !ca.config.EnableMetadataStore {
		return nil, fmt.Errorf("metadata store not enabled")
	}

	return ca.metadata.GetMetadata(contractID)
}

// BackupContract creates a comprehensive backup of contract data
func (ca *ChaincodeAdapter) BackupContract(contractID string) (*BackupMetadata, error) {
	if !ca.config.EnableStatePersistence {
		return nil, fmt.Errorf("persistence not enabled")
	}

	return ca.persistence.CreateBackup(contractID)
}

// RestoreContract restores contract from backup
func (ca *ChaincodeAdapter) RestoreContract(contractID string, backupTime time.Time) error {
	if !ca.config.EnableStatePersistence {
		return fmt.Errorf("persistence not enabled")
	}

	return ca.persistence.RestoreFromBackup(contractID, backupTime)
}

// CrossRuntimeCall enables calls between different runtimes
func (ca *ChaincodeAdapter) CrossRuntimeCall(ctx context.Context, call runtime.ContractCall, targetRuntime runtime.RuntimeType) (*runtime.ExecutionResult, error) {
	if !ca.config.EnableHybridVM || ca.integrator == nil {
		return nil, fmt.Errorf("hybrid VM integration not enabled")
	}

	return ca.integrator.RouteCall(ctx, call, targetRuntime)
}

// GetAdapterMetrics returns comprehensive adapter metrics
func (ca *ChaincodeAdapter) GetAdapterMetrics() AdapterMetrics {
	metrics := AdapterMetrics{
		Initialized: ca.initialized,
		Running:     ca.running,
	}

	if ca.stateManager != nil {
		metrics.StateManager = ca.stateManager.GetMetrics()
	}

	if ca.migrator != nil {
		metrics.Migrator = ca.migrator.GetMetrics()
	}

	if ca.persistence != nil {
		metrics.Persistence = ca.persistence.GetMetrics()
	}

	if ca.metadata != nil {
		metrics.Metadata = ca.metadata.GetMetrics()
	}

	if ca.integrator != nil {
		metrics.HybridVM = ca.integrator.GetMetrics()
	}

	if ca.lifecycle != nil {
		metrics.Lifecycle = ca.lifecycle.GetMetrics()
	}

	return metrics
}

// Helper methods for processing

// preExecutionProcessing handles pre-execution setup
func (ca *ChaincodeAdapter) preExecutionProcessing(ctx context.Context, call *runtime.ContractCall) error {
	// Validate call parameters
	if err := ca.validateContractCall(call); err != nil {
		return fmt.Errorf("call validation failed: %w", err)
	}

	// Apply routing if needed
	if ca.integrator != nil {
		if err := ca.integrator.PrepareCall(ctx, call); err != nil {
			return fmt.Errorf("call preparation failed: %w", err)
		}
	}

	return nil
}

// postExecutionProcessing handles post-execution cleanup
func (ca *ChaincodeAdapter) postExecutionProcessing(ctx context.Context, call *runtime.ContractCall, result *runtime.ExecutionResult) error {
	// Process events
	if len(result.Events) > 0 {
		if err := ca.processEvents(result.Events); err != nil {
			return fmt.Errorf("event processing failed: %w", err)
		}
	}

	// Update metrics
	ca.updateExecutionMetrics(call, result)

	return nil
}

// validateContractCall validates contract call parameters
func (ca *ChaincodeAdapter) validateContractCall(call *runtime.ContractCall) error {
	if call.ContractID == "" {
		return fmt.Errorf("contract ID cannot be empty")
	}

	if call.Function == "" {
		return fmt.Errorf("function name cannot be empty")
	}

	if call.GasLimit == 0 {
		return fmt.Errorf("gas limit must be greater than 0")
	}

	return nil
}

// processEvents processes contract events
func (ca *ChaincodeAdapter) processEvents(events []runtime.ContractEvent) error {
	for _, event := range events {
		if ca.integrator != nil {
			if err := ca.integrator.ProcessEvent(event); err != nil {
				ca.logger.WithError(err).WithField("event", event.Name).Warn("Failed to process event")
			}
		}
	}
	return nil
}

// updateExecutionMetrics updates execution metrics
func (ca *ChaincodeAdapter) updateExecutionMetrics(call *runtime.ContractCall, result *runtime.ExecutionResult) {
	// Update state manager metrics
	if ca.stateManager != nil {
		ca.stateManager.UpdateMetrics(call, result)
	}

	// Update integrator metrics
	if ca.integrator != nil {
		ca.integrator.UpdateMetrics(call, result)
	}
}

// extractContractMetadata extracts metadata from deployment
func (ca *ChaincodeAdapter) extractContractMetadata(contract *runtime.CompiledContract, args runtime.DeploymentArgs, result *runtime.DeploymentResult) *ContractMetadata {
	metadata := &ContractMetadata{
		ContractID:       result.ContractID,
		Owner:            args.Deployer,
		CreatedAt:        result.Timestamp,
		UpdatedAt:        result.Timestamp,
		SchemaVersion:    "1.0",
		Configuration:    ContractConfiguration{},    // Initialize with default values
		Permissions:      ContractPermissions{},      // Initialize with default values
		CustomAttributes: ContractCustomAttributes{}, // Initialize with default values
	}

	// Extract from contract metadata (now a struct)
	if contract.Metadata.Name != "" {
		metadata.Name = contract.Metadata.Name
	}
	if contract.Metadata.Version != "" {
		metadata.Version = contract.Metadata.Version
	}
	if contract.Metadata.Description != "" {
		metadata.Description = contract.Metadata.Description
	}
	if contract.Metadata.Author != "" {
		metadata.CustomAttributes.CustomFields = make(map[string]string)
		metadata.CustomAttributes.CustomFields["author"] = contract.Metadata.Author
	}
	if contract.Metadata.License != "" {
		metadata.CustomAttributes.CustomFields["license"] = contract.Metadata.License
	}
	if contract.Metadata.Repository != "" {
		metadata.CustomAttributes.CustomFields["repository"] = contract.Metadata.Repository
	}

	// Extract capabilities as tags
	if len(contract.Metadata.Capabilities) > 0 {
		metadata.Tags = make([]string, len(contract.Metadata.Capabilities))
		for i, cap := range contract.Metadata.Capabilities {
			metadata.Tags[i] = string(cap)
		}
	}

	// Extract from deployment args options (now a struct)
	extractDeploymentOptions(args.Options, metadata)

	return metadata
}

// extractDeploymentOptions extracts deployment options into metadata
func extractDeploymentOptions(options runtime.DeploymentOptions, metadata *ContractMetadata) {
	// Extract configuration from resource limits
	if options.ResourceLimits.MaxMemoryMB > 0 {
		metadata.Configuration.MaxMemoryUsage = int64(options.ResourceLimits.MaxMemoryMB)
	}
	if options.ResourceLimits.MaxCPUPercent > 0 {
		metadata.Configuration.CPULimit = int(options.ResourceLimits.MaxCPUPercent)
	}
	if options.ResourceLimits.MaxStorageMB > 0 {
		metadata.Configuration.StorageQuota = int64(options.ResourceLimits.MaxStorageMB) * 1024 * 1024 // Convert MB to bytes
	}
	if options.ResourceLimits.MaxNetworkKbps > 0 {
		metadata.Configuration.NetworkBandwidth = int64(options.ResourceLimits.MaxNetworkKbps)
	}
	if options.ResourceLimits.ExecutionTimeout > 0 {
		metadata.Configuration.MaxExecutionTime = int64(options.ResourceLimits.ExecutionTimeout.Milliseconds())
	}

	// Extract from security policy
	if !options.SecurityPolicy.AllowNetworkAccess {
		metadata.Permissions.BlockedFunctions = append(metadata.Permissions.BlockedFunctions, "network_access")
	}
	if !options.SecurityPolicy.AllowFileAccess {
		metadata.Permissions.BlockedFunctions = append(metadata.Permissions.BlockedFunctions, "file_access")
	}
	if len(options.SecurityPolicy.AllowedDomains) > 0 {
		if metadata.CustomAttributes.CustomFields == nil {
			metadata.CustomAttributes.CustomFields = make(map[string]string)
		}
		metadata.CustomAttributes.CustomFields["allowed_domains"] = fmt.Sprintf("%v", options.SecurityPolicy.AllowedDomains)
	}
	if options.SecurityPolicy.RequireSignature {
		if metadata.CustomAttributes.CustomFields == nil {
			metadata.CustomAttributes.CustomFields = make(map[string]string)
		}
		metadata.CustomAttributes.CustomFields["require_signature"] = "true"
	}
	if len(options.SecurityPolicy.TrustedPublishers) > 0 {
		metadata.Permissions.AdminAccess = options.SecurityPolicy.TrustedPublishers
	}

	// Extract environment variables
	for k, v := range options.EnvironmentVars {
		if k == "ENVIRONMENT" {
			metadata.Configuration.Environment = v
		} else if k == "LOG_LEVEL" {
			metadata.Configuration.LogLevel = v
		} else if k == "ENABLE_LOGGING" && v == "true" {
			metadata.Configuration.EnableLogging = true
		} else if k == "ENABLE_EVENTS" && v == "true" {
			metadata.Configuration.EnableEvents = true
		} else if k == "ENABLE_CROSS_CALLS" && v == "true" {
			metadata.Configuration.EnableCrossCalls = true
		} else if k == "ENABLE_PERSISTENCE" && v == "true" {
			metadata.Configuration.EnablePersistence = true
		} else if k == "NETWORK_ID" {
			metadata.Configuration.NetworkID = v
		} else if k == "CHAIN_ID" {
			metadata.Configuration.ChainID = v
		} else {
			// Store other env vars as custom fields
			if metadata.CustomAttributes.CustomFields == nil {
				metadata.CustomAttributes.CustomFields = make(map[string]string)
			}
			metadata.CustomAttributes.CustomFields[k] = v
		}
	}
}

// HealthCheck performs comprehensive health check of the adapter
func (ca *ChaincodeAdapter) HealthCheck() error {
	if !ca.initialized {
		return fmt.Errorf("adapter not initialized")
	}

	if !ca.running {
		return fmt.Errorf("adapter not running")
	}

	// Check runtime health
	if err := ca.runtime.HealthCheck(); err != nil {
		return fmt.Errorf("runtime health check failed: %w", err)
	}

	// Check state manager health
	if ca.stateManager != nil {
		if err := ca.stateManager.HealthCheck(); err != nil {
			return fmt.Errorf("state manager health check failed: %w", err)
		}
	}

	// Check persistence health
	if ca.persistence != nil {
		if err := ca.persistence.HealthCheck(); err != nil {
			return fmt.Errorf("persistence health check failed: %w", err)
		}
	}

	// Check metadata store health
	if ca.metadata != nil {
		if err := ca.metadata.HealthCheck(); err != nil {
			return fmt.Errorf("metadata store health check failed: %w", err)
		}
	}

	// Check integrator health
	if ca.integrator != nil {
		if err := ca.integrator.HealthCheck(); err != nil {
			return fmt.Errorf("hybrid VM integrator health check failed: %w", err)
		}
	}

	// Check lifecycle manager health
	if ca.lifecycle != nil {
		if err := ca.lifecycle.HealthCheck(); err != nil {
			return fmt.Errorf("lifecycle manager health check failed: %w", err)
		}
	}

	return nil
}
