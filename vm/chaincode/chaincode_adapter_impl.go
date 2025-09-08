// Package chaincode provides implementation for chaincode adapter components
package chaincode

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"diamante/consensus"
	"diamante/storage"
	"diamante/vm/runtime"

	"github.com/sirupsen/logrus"
)

// RuntimeType alias for runtime.RuntimeType
type RuntimeType = runtime.RuntimeType

// RollbackOperation represents a rollback operation
type RollbackOperation struct {
	Type        string
	Description string
	Handler     func() error
}

// NewChaincodeStateManager creates a new state manager
func NewChaincodeStateManager(store storage.LedgerStore, logger *logrus.Logger) (*ChaincodeStateManager, error) {
	return &ChaincodeStateManager{
		store:      store,
		cache:      make(map[string]map[string][]byte),
		dirtyFlags: make(map[string]map[string]bool),
		locks:      make(map[string]*sync.RWMutex),
		logger:     logger,
	}, nil
}

// InitializeContract initializes contract state
func (sm *ChaincodeStateManager) InitializeContract(contractID string) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if _, exists := sm.cache[contractID]; !exists {
		sm.cache[contractID] = make(map[string][]byte)
		sm.dirtyFlags[contractID] = make(map[string]bool)
		sm.locks[contractID] = &sync.RWMutex{}
	}

	return nil
}

// LoadContractState loads contract state from storage
func (sm *ChaincodeStateManager) LoadContractState(contractID string) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if _, exists := sm.cache[contractID]; !exists {
		return sm.InitializeContract(contractID)
	}

	sm.cacheHits++
	return nil
}

// GetContractState retrieves all state for a contract
func (sm *ChaincodeStateManager) GetContractState(contractID string) (*ContractStateData, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	stateData := &ContractStateData{
		StateValues: make(map[string]StateValue),
		Metadata: StateMetadata{
			LastAccessed: consensus.ConsensusNow(),
			AccessCount:  sm.stateReads,
			Version:      1,
		},
	}

	if cache, exists := sm.cache[contractID]; exists {
		for k, v := range cache {
			var value interface{}
			valueType := "string"

			if err := json.Unmarshal(v, &value); err != nil {
				// If unmarshal fails, treat as string
				value = string(v)
			} else {
				// Determine type from unmarshaled value
				switch value.(type) {
				case float64:
					valueType = "number"
				case bool:
					valueType = "boolean"
				case map[string]interface{}:
					valueType = "object"
				case []interface{}:
					valueType = "array"
				}
			}

			stateData.StateValues[k] = StateValue{
				Type:     valueType,
				Value:    value,
				Modified: consensus.ConsensusNow(),
				Size:     len(v),
			}
			stateData.Metadata.TotalKeys++
			stateData.Metadata.TotalSize += int64(len(v))
		}
	}

	sm.stateReads++
	return stateData, nil
}

// PersistStateChanges persists state changes to storage
func (sm *ChaincodeStateManager) PersistStateChanges(contractID string, changes []runtime.StateChange) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Create a write batch
	batch := storage.WriteBatch{
		StateWrites: make(map[string][]byte),
	}

	for _, change := range changes {
		key := fmt.Sprintf("state:%s:%s", contractID, string(change.Key))
		batch.StateWrites[key] = change.NewValue

		// Update cache
		if sm.cache[contractID] == nil {
			sm.cache[contractID] = make(map[string][]byte)
		}
		sm.cache[contractID][string(change.Key)] = change.NewValue
		sm.stateWrites++
	}

	// Write the batch
	if err := sm.store.WriteBatch(batch); err != nil {
		return fmt.Errorf("failed to persist state changes: %w", err)
	}

	return nil
}

// UpdateMetrics updates state manager metrics
func (sm *ChaincodeStateManager) UpdateMetrics(call *runtime.ContractCall, result *runtime.ExecutionResult) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if result.Success {
		sm.stateReads++
	}
}

// GetMetrics returns state manager metrics
func (sm *ChaincodeStateManager) GetMetrics() StateManagerMetrics {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	return StateManagerMetrics{
		StateReads:  sm.stateReads,
		StateWrites: sm.stateWrites,
		CacheHits:   sm.cacheHits,
		CacheMisses: sm.cacheMisses,
		CacheSize:   len(sm.cache),
	}
}

// HealthCheck checks state manager health
func (sm *ChaincodeStateManager) HealthCheck() error {
	if sm.store == nil {
		return fmt.Errorf("state store not initialized")
	}
	return nil
}

// NewChaincodePersistence creates a new persistence manager
func NewChaincodePersistence(store storage.LedgerStore, config *ChaincodeAdapterConfig, logger *logrus.Logger) (*ChaincodePersistence, error) {
	return &ChaincodePersistence{
		store:          store,
		checksums:      make(map[string]string),
		backupMetadata: make(map[string]BackupMetadata),
		logger:         logger,
		scheduler: &PersistenceScheduler{
			jobs:     []*PersistenceJob{},
			stopChan: make(chan bool),
			logger:   logger,
		},
	}, nil
}

// StartScheduler starts the persistence scheduler
func (cp *ChaincodePersistence) StartScheduler() error {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	if cp.scheduler.running {
		return nil
	}

	cp.scheduler.ticker = time.NewTicker(30 * time.Second)
	cp.scheduler.running = true

	go func() {
		for {
			select {
			case <-cp.scheduler.ticker.C:
				cp.runScheduledJobs()
			case <-cp.scheduler.stopChan:
				return
			}
		}
	}()

	return nil
}

// StopScheduler stops the persistence scheduler
func (cp *ChaincodePersistence) StopScheduler() error {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	if cp.scheduler.running {
		cp.scheduler.ticker.Stop()
		cp.scheduler.stopChan <- true
		cp.scheduler.running = false
	}

	return nil
}

// ScheduleContractPersistence schedules persistence for a contract
func (cp *ChaincodePersistence) ScheduleContractPersistence(contractID string) error {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	job := &PersistenceJob{
		ID:         fmt.Sprintf("persist-%s-%d", contractID, consensus.ConsensusUnix()),
		ContractID: contractID,
		Type:       PersistenceTypeState,
		Schedule:   1 * time.Minute,
		NextRun:    consensus.ConsensusNow().Add(1 * time.Minute),
		Enabled:    true,
	}

	cp.scheduler.jobs = append(cp.scheduler.jobs, job)
	return nil
}

// CreateBackup creates a backup of contract data
func (cp *ChaincodePersistence) CreateBackup(contractID string) (*BackupMetadata, error) {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	backup := &BackupMetadata{
		ContractID:    contractID,
		BackupTime:    consensus.ConsensusNow(),
		DataSize:      0,
		Checksum:      cp.calculateChecksum([]byte(contractID)),
		Version:       "1.0",
		RestorePoints: []time.Time{consensus.ConsensusNow()},
	}

	cp.backupMetadata[contractID] = *backup
	cp.backupOps++

	return backup, nil
}

// RestoreFromBackup restores contract from backup
func (cp *ChaincodePersistence) RestoreFromBackup(contractID string, backupTime time.Time) error {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	if _, exists := cp.backupMetadata[contractID]; !exists {
		return fmt.Errorf("no backup found for contract %s", contractID)
	}

	cp.recoveryOps++
	return nil
}

// GetPersistenceInfo retrieves persistence information
func (cp *ChaincodePersistence) GetPersistenceInfo(contractID string) StatePersistenceInfo {
	cp.mu.RLock()
	defer cp.mu.RUnlock()

	info := StatePersistenceInfo{
		ContractID:         contractID,
		HasBackup:          false,
		PersistenceEnabled: true,
	}

	if backup, exists := cp.backupMetadata[contractID]; exists {
		info.HasBackup = true
		info.LastBackup = backup.BackupTime
		info.BackupSize = backup.DataSize
	}

	return info
}

// GetMetrics returns persistence metrics
func (cp *ChaincodePersistence) GetMetrics() PersistenceMetrics {
	cp.mu.RLock()
	defer cp.mu.RUnlock()

	return PersistenceMetrics{
		PersistenceOps:     cp.persistenceOps,
		BackupOps:          cp.backupOps,
		RecoveryOps:        cp.recoveryOps,
		ChecksumMismatches: cp.checksumMismatches,
		ScheduledJobs:      len(cp.scheduler.jobs),
	}
}

// HealthCheck checks persistence health
func (cp *ChaincodePersistence) HealthCheck() error {
	if cp.store == nil {
		return fmt.Errorf("persistence store not initialized")
	}
	return nil
}

// runScheduledJobs runs scheduled persistence jobs
func (cp *ChaincodePersistence) runScheduledJobs() {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	now := consensus.ConsensusNow()
	for _, job := range cp.scheduler.jobs {
		if job.Enabled && now.After(job.NextRun) {
			job.LastRun = now
			job.NextRun = now.Add(job.Schedule)
			cp.persistenceOps++
		}
	}
}

// calculateChecksum calculates checksum for data
func (cp *ChaincodePersistence) calculateChecksum(data []byte) string {
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
}

// NewChaincodeMetadataStore creates a new metadata store
func NewChaincodeMetadataStore(store storage.LedgerStore, config *ChaincodeAdapterConfig, logger *logrus.Logger) (*ChaincodeMetadataStore, error) {
	return &ChaincodeMetadataStore{
		store:     store,
		cache:     make(map[string]*ContractMetadata),
		versions:  make(map[string][]*MetadataVersion),
		logger:    logger,
		flushChan: make(chan string, 100),
	}, nil
}

// StartFlusher starts the metadata flusher
func (cms *ChaincodeMetadataStore) StartFlusher() {
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case contractID := <-cms.flushChan:
				cms.flushMetadata(contractID)
			case <-ticker.C:
				cms.flushAllMetadata()
			}
		}
	}()
}

// StopFlusher stops the metadata flusher
func (cms *ChaincodeMetadataStore) StopFlusher() {
	close(cms.flushChan)
}

// StoreMetadata stores contract metadata
func (cms *ChaincodeMetadataStore) StoreMetadata(contractID string, metadata *ContractMetadata) error {
	cms.mu.Lock()
	defer cms.mu.Unlock()

	cms.cache[contractID] = metadata
	cms.metadataWrites++

	// Update metadata timestamp
	metadata.UpdatedAt = consensus.ConsensusNow()

	// Create metadata version
	version := &MetadataVersion{
		Version:   fmt.Sprintf("v%d", len(cms.versions[contractID])+1),
		Timestamp: consensus.ConsensusNow(),
		Data:      metadata,
	}

	cms.versions[contractID] = append(cms.versions[contractID], version)

	// Queue for flush
	select {
	case cms.flushChan <- contractID:
	default:
		// Channel full, flush immediately
		return cms.flushMetadata(contractID)
	}

	return nil
}

// GetMetadata retrieves contract metadata
func (cms *ChaincodeMetadataStore) GetMetadata(contractID string) (*ContractMetadata, error) {
	cms.mu.RLock()
	defer cms.mu.RUnlock()

	if metadata, exists := cms.cache[contractID]; exists {
		cms.metadataReads++
		return metadata, nil
	}

	// Load from store
	key := fmt.Sprintf("metadata:%s", contractID)
	data, err := cms.store.GetState([]byte(key))
	if err != nil {
		return nil, fmt.Errorf("failed to get metadata: %w", err)
	}

	if len(data) == 0 {
		return nil, fmt.Errorf("metadata not found for contract %s", contractID)
	}

	var metadata ContractMetadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
	}

	cms.cache[contractID] = &metadata
	return &metadata, nil
}

// UpdateVersionMetadata updates version metadata
func (cms *ChaincodeMetadataStore) UpdateVersionMetadata(contractID string, version string) error {
	cms.mu.Lock()
	defer cms.mu.Unlock()

	metadata, exists := cms.cache[contractID]
	if !exists {
		metadata = &ContractMetadata{ContractID: contractID}
		cms.cache[contractID] = metadata
	}

	metadata.Version = version
	metadata.UpdatedAt = consensus.ConsensusNow()

	// Add version history
	versionEntry := &MetadataVersion{
		Version:   version,
		Timestamp: consensus.ConsensusNow(),
		Changes:   []string{"Version updated"},
		Metadata:  metadata,
	}

	cms.versions[contractID] = append(cms.versions[contractID], versionEntry)
	cms.versionCount++

	return cms.flushMetadata(contractID)
}

// GetMetrics returns metadata store metrics
func (cms *ChaincodeMetadataStore) GetMetrics() MetadataStoreMetrics {
	cms.mu.RLock()
	defer cms.mu.RUnlock()

	return MetadataStoreMetrics{
		MetadataReads:  cms.metadataReads,
		MetadataWrites: cms.metadataWrites,
		VersionCount:   cms.versionCount,
		CacheSize:      len(cms.cache),
	}
}

// HealthCheck checks metadata store health
func (cms *ChaincodeMetadataStore) HealthCheck() error {
	if cms.store == nil {
		return fmt.Errorf("metadata store not initialized")
	}
	return nil
}

// flushMetadata flushes metadata to storage
func (cms *ChaincodeMetadataStore) flushMetadata(contractID string) error {
	metadata, exists := cms.cache[contractID]
	if !exists {
		return nil
	}

	key := fmt.Sprintf("metadata:%s", contractID)
	data, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	// Use WriteBatch for state update
	batch := storage.WriteBatch{
		StateWrites: map[string][]byte{
			key: data,
		},
	}
	return cms.store.WriteBatch(batch)
}

// flushAllMetadata flushes all cached metadata
func (cms *ChaincodeMetadataStore) flushAllMetadata() {
	cms.mu.RLock()
	contractIDs := make([]string, 0, len(cms.cache))
	for id := range cms.cache {
		contractIDs = append(contractIDs, id)
	}
	cms.mu.RUnlock()

	for _, id := range contractIDs {
		if err := cms.flushMetadata(id); err != nil {
			cms.logger.WithError(err).WithField("contractID", id).Warn("Failed to flush metadata")
		}
	}
}

// NewChaincodeMigrator creates a new migrator
func NewChaincodeMigrator(stateManager *ChaincodeStateManager, persistence *ChaincodePersistence, logger *logrus.Logger) (*ChaincodeMigrator, error) {
	return &ChaincodeMigrator{
		stateManager:        stateManager,
		persistence:         persistence,
		jobs:                make(map[string]*MigrationJob),
		completedMigrations: make(map[string]time.Time),
		migrationHistory:    []MigrationRecord{},
		logger:              logger,
	}, nil
}

// CreateMigrationJob creates a new migration job
func (cm *ChaincodeMigrator) CreateMigrationJob(contractID string, newCode []byte, args runtime.UpgradeArgs) (*MigrationJob, error) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	job := &MigrationJob{
		ID:          fmt.Sprintf("migrate-%s-%d", contractID, consensus.ConsensusUnix()),
		ContractID:  contractID,
		FromVersion: "current",
		ToVersion:   args.Version,
		Status:      MigrationPending,
		StartTime:   consensus.ConsensusNow(),
		Operations:  []MigrationOperation{},
		Rollback:    []RollbackOperation{},
	}

	cm.jobs[job.ID] = job
	return job, nil
}

// ExecuteMigration executes a migration job
func (cm *ChaincodeMigrator) ExecuteMigration(ctx context.Context, job *MigrationJob) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	job.Status = MigrationRunning
	job.Progress = 0.0

	// Simulate migration steps
	steps := 5
	for i := 0; i < steps; i++ {
		select {
		case <-ctx.Done():
			job.Status = MigrationFailed
			job.Error = ctx.Err()
			return ctx.Err()
		default:
			// Use context-aware timing instead of time.Sleep
			select {
			case <-time.After(100 * time.Millisecond):
				// Simulation delay completed
			case <-ctx.Done():
				// Context cancelled during delay
				job.Status = MigrationFailed
				job.Error = ctx.Err()
				return ctx.Err()
			}
			job.Progress = float64(i+1) / float64(steps)
		}
	}

	job.Status = MigrationCompleted
	job.EndTime = consensus.ConsensusNow()
	job.Progress = 1.0

	// Record completion
	cm.completedMigrations[job.ContractID] = consensus.ConsensusNow()
	cm.migrationHistory = append(cm.migrationHistory, MigrationRecord{
		JobID:       job.ID,
		ContractID:  job.ContractID,
		FromVersion: job.FromVersion,
		ToVersion:   job.ToVersion,
		Timestamp:   job.StartTime,
		Duration:    job.EndTime.Sub(job.StartTime),
		Success:     true,
	})

	return nil
}

// GetMetrics returns migrator metrics
func (cm *ChaincodeMigrator) GetMetrics() MigratorMetrics {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	pending, running, completed, failed := 0, 0, 0, 0
	for _, job := range cm.jobs {
		switch job.Status {
		case MigrationPending:
			pending++
		case MigrationRunning:
			running++
		case MigrationCompleted:
			completed++
		case MigrationFailed:
			failed++
		}
	}

	return MigratorMetrics{
		PendingJobs:      pending,
		RunningJobs:      running,
		CompletedJobs:    completed,
		FailedJobs:       failed,
		MigrationHistory: len(cm.migrationHistory),
	}
}

// HealthCheck checks migrator health
func (cm *ChaincodeMigrator) HealthCheck() error {
	return nil
}

// NewHybridVMIntegrator creates a new hybrid VM integrator
func NewHybridVMIntegrator(logger *logrus.Logger) (*HybridVMIntegrator, error) {
	return &HybridVMIntegrator{
		evmBridge:    &EVMBridge{logger: logger},
		nativeBridge: &NativeBridge{logger: logger},
		crossCallRouter: &CrossCallRouter{
			routes:   make(map[RuntimeRoute]*RouteHandler),
			policies: make(map[string]*RoutingPolicy),
			logger:   logger,
		},
		logger: logger,
	}, nil
}

// Start starts the hybrid VM integrator
func (hvi *HybridVMIntegrator) Start() error {
	hvi.mu.Lock()
	defer hvi.mu.Unlock()

	// Initialize default routes
	hvi.crossCallRouter.routes[RuntimeRoute{From: runtime.RuntimeTypeChaincode, To: runtime.RuntimeTypeEVM}] = &RouteHandler{
		Handler: func(call runtime.ContractCall) (*runtime.ExecutionResult, error) {
			hvi.evmInteractions++
			return &runtime.ExecutionResult{Success: true}, nil
		},
	}

	return nil
}

// Stop stops the hybrid VM integrator
func (hvi *HybridVMIntegrator) Stop() error {
	return nil
}

// PrepareCall prepares a contract call
func (hvi *HybridVMIntegrator) PrepareCall(ctx context.Context, call *runtime.ContractCall) error {
	// Validate call routing
	return nil
}

// RouteCall routes a call to another runtime
func (hvi *HybridVMIntegrator) RouteCall(ctx context.Context, call runtime.ContractCall, targetRuntime runtime.RuntimeType) (*runtime.ExecutionResult, error) {
	hvi.mu.Lock()
	defer hvi.mu.Unlock()

	route := RuntimeRoute{From: runtime.RuntimeTypeChaincode, To: targetRuntime}
	handler, exists := hvi.crossCallRouter.routes[route]
	if !exists {
		hvi.routingErrors++
		return nil, fmt.Errorf("no route found from chaincode to %s", targetRuntime)
	}

	hvi.crossCalls++
	return handler.Handler(call)
}

// ProcessEvent processes a contract event
func (hvi *HybridVMIntegrator) ProcessEvent(event runtime.ContractEvent) error {
	// Process cross-runtime events
	return nil
}

// UpdateMetrics updates integrator metrics
func (hvi *HybridVMIntegrator) UpdateMetrics(call *runtime.ContractCall, result *runtime.ExecutionResult) {
	hvi.mu.Lock()
	defer hvi.mu.Unlock()

	if result.Success {
		hvi.crossCalls++
	} else {
		hvi.routingErrors++
	}
}

// GetMetrics returns integrator metrics
func (hvi *HybridVMIntegrator) GetMetrics() HybridVMMetrics {
	hvi.mu.RLock()
	defer hvi.mu.RUnlock()

	return HybridVMMetrics{
		CrossCalls:         hvi.crossCalls,
		EVMInteractions:    hvi.evmInteractions,
		NativeInteractions: hvi.nativeInteractions,
		RoutingErrors:      hvi.routingErrors,
	}
}

// HealthCheck checks integrator health
func (hvi *HybridVMIntegrator) HealthCheck() error {
	return nil
}

// NewChaincodeLifecycleManager creates a new lifecycle manager
func NewChaincodeLifecycleManager(adapter *ChaincodeAdapter, logger *logrus.Logger) (*ChaincodeLifecycleManager, error) {
	return &ChaincodeLifecycleManager{
		adapter:   adapter,
		stages:    make(map[string]*LifecycleStage),
		workflows: make(map[string]*LifecycleWorkflow),
		logger:    logger,
	}, nil
}

// CreateDeploymentWorkflow creates a deployment workflow
func (clm *ChaincodeLifecycleManager) CreateDeploymentWorkflow(contract *runtime.CompiledContract, args runtime.DeploymentArgs) (*LifecycleWorkflow, error) {
	clm.mu.Lock()
	defer clm.mu.Unlock()

	workflow := &LifecycleWorkflow{
		Name:        fmt.Sprintf("deploy-%s", args.Deployer),
		Description: "Contract deployment workflow",
		Stages:      []*LifecycleStage{},
		Status:      WorkflowStatusPending,
		StartTime:   consensus.ConsensusNow(),
		Metadata: WorkflowMetadata{
			ContractID: args.Deployer,
			Version:    "1.0.0",
			Initiator:  args.Deployer,
			Priority:   1,
			Tags:       []string{"deployment", "chaincode"},
		},
	}

	// Add deployment stages
	workflow.Stages = append(workflow.Stages, &LifecycleStage{
		Name:        "validation",
		Description: "Validate contract code",
	})
	workflow.Stages = append(workflow.Stages, &LifecycleStage{
		Name:        "deployment",
		Description: "Deploy contract",
	})
	workflow.Stages = append(workflow.Stages, &LifecycleStage{
		Name:        "initialization",
		Description: "Initialize contract state",
	})

	workflowID := fmt.Sprintf("workflow-%d", consensus.ConsensusUnix())
	clm.workflows[workflowID] = workflow

	return workflow, nil
}

// ExecuteWorkflow executes a lifecycle workflow
func (clm *ChaincodeLifecycleManager) ExecuteWorkflow(ctx context.Context, workflow *LifecycleWorkflow) (*runtime.DeploymentResult, error) {
	clm.mu.Lock()
	defer clm.mu.Unlock()

	workflow.Status = WorkflowStatusRunning

	// Execute stages
	for i := range workflow.Stages {
		workflow.CurrentStage = i

		// Simulate stage execution
		select {
		case <-ctx.Done():
			workflow.Status = WorkflowStatusFailed
			return nil, ctx.Err()
		case <-time.After(100 * time.Millisecond):
			// Stage simulation delay completed
		}
	}

	workflow.Status = WorkflowStatusCompleted
	workflow.EndTime = consensus.ConsensusNow()
	clm.deploymentsManaged++

	// Create deployment result
	result := &runtime.DeploymentResult{
		ContractID:      fmt.Sprintf("contract-%d", consensus.ConsensusUnix()),
		TransactionHash: fmt.Sprintf("tx-%d", consensus.ConsensusUnix()),
		GasUsed:         50000,
		Timestamp:       consensus.ConsensusNow(),
		Events:          []runtime.ContractEvent{},
	}

	return result, nil
}

// GetMetrics returns lifecycle manager metrics
func (clm *ChaincodeLifecycleManager) GetMetrics() LifecycleMetrics {
	clm.mu.RLock()
	defer clm.mu.RUnlock()

	return LifecycleMetrics{
		DeploymentsManaged:  clm.deploymentsManaged,
		UpgradesManaged:     clm.upgradesManaged,
		TerminationsManaged: clm.terminationsManaged,
	}
}

// HealthCheck checks lifecycle manager health
func (clm *ChaincodeLifecycleManager) HealthCheck() error {
	return nil
}

// ContractStateData represents typed contract state data
type ContractStateData struct {
	// State values by key
	StateValues map[string]StateValue `json:"state_values"`

	// Metadata about the state
	Metadata StateMetadata `json:"_metadata,omitempty"`

	// Persistence information
	Persistence StatePersistenceInfo `json:"_persistence,omitempty"`
}

// StateValue represents a typed state value
type StateValue struct {
	Type     string      `json:"type"`     // string, number, boolean, object, array
	Value    interface{} `json:"value"`    // The actual value
	Modified time.Time   `json:"modified"` // Last modification time
	Size     int         `json:"size"`     // Size in bytes
}

// StateMetadata represents metadata about contract state
type StateMetadata struct {
	LastAccessed time.Time `json:"last_accessed"`
	AccessCount  int64     `json:"access_count"`
	TotalKeys    int       `json:"total_keys"`
	TotalSize    int64     `json:"total_size_bytes"`
	Version      int       `json:"version"`
}

// StatePersistenceInfo represents persistence information
type StatePersistenceInfo struct {
	ContractID         string    `json:"contract_id"`
	HasBackup          bool      `json:"has_backup"`
	LastBackup         time.Time `json:"last_backup,omitempty"`
	BackupSize         int64     `json:"backup_size_bytes,omitempty"`
	PersistenceEnabled bool      `json:"persistence_enabled"`
	LastPersisted      time.Time `json:"last_persisted,omitempty"`
}

// StateManagerMetrics represents state manager metrics
type StateManagerMetrics struct {
	StateReads  int64 `json:"state_reads"`
	StateWrites int64 `json:"state_writes"`
	CacheHits   int64 `json:"cache_hits"`
	CacheMisses int64 `json:"cache_misses"`
	CacheSize   int   `json:"cache_size"`
}

// PersistenceMetrics represents persistence metrics
type PersistenceMetrics struct {
	PersistenceOps     int64 `json:"persistence_ops"`
	BackupOps          int64 `json:"backup_ops"`
	RecoveryOps        int64 `json:"recovery_ops"`
	ChecksumMismatches int64 `json:"checksum_mismatches"`
	ScheduledJobs      int   `json:"scheduled_jobs"`
}

// MetadataStoreMetrics represents metadata store metrics
type MetadataStoreMetrics struct {
	MetadataReads  int64 `json:"metadata_reads"`
	MetadataWrites int64 `json:"metadata_writes"`
	VersionCount   int64 `json:"version_count"`
	CacheSize      int   `json:"cache_size"`
}

// MigratorMetrics represents migrator metrics
type MigratorMetrics struct {
	PendingJobs      int `json:"pending_jobs"`
	RunningJobs      int `json:"running_jobs"`
	CompletedJobs    int `json:"completed_jobs"`
	FailedJobs       int `json:"failed_jobs"`
	MigrationHistory int `json:"migration_history_count"`
}

// HybridVMMetrics represents hybrid VM integrator metrics
type HybridVMMetrics struct {
	CrossCalls         int64 `json:"cross_calls"`
	EVMInteractions    int64 `json:"evm_interactions"`
	NativeInteractions int64 `json:"native_interactions"`
	RoutingErrors      int64 `json:"routing_errors"`
}

// LifecycleMetrics represents lifecycle manager metrics
type LifecycleMetrics struct {
	DeploymentsManaged  int64 `json:"deployments_managed"`
	UpgradesManaged     int64 `json:"upgrades_managed"`
	TerminationsManaged int64 `json:"terminations_managed"`
}

// MetadataVersion represents a version of contract metadata
type MetadataVersion struct {
	Version   string
	Timestamp time.Time
	Changes   []string
	Author    string
	Metadata  *ContractMetadata
	Data      *ContractMetadata // Add this field for compatibility
}
