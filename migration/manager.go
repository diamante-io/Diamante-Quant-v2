package migration

import (
	"context"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"diamante/common"
	"diamante/types"
)

// MigrationManager coordinates all migration operations
type MigrationManager struct {
	registry      *MigrationRegistry
	storage       MigrationStorage
	backupManager *BackupManager
	logger        common.StructuredLogger
	config        *MigrationConfig
	mutex         sync.RWMutex
	activePlans   map[string]*MigrationExecution
	planHistory   []*MigrationPlan
}

// MigrationConfig contains migration manager configuration
type MigrationConfig struct {
	WorkingDirectory string        `json:"working_directory"`
	BackupDirectory  string        `json:"backup_directory"`
	MaxConcurrent    int           `json:"max_concurrent"`
	DefaultTimeout   time.Duration `json:"default_timeout"`
	EnableBackups    bool          `json:"enable_backups"`
	EnableDryRun     bool          `json:"enable_dry_run"`
	RetryPolicy      *RetryPolicy  `json:"retry_policy"`
	ValidationMode   string        `json:"validation_mode"` // strict, lenient, disabled
}

// MigrationExecution tracks a running migration
type MigrationExecution struct {
	Plan           *MigrationPlan
	Context        *MigrationContext
	StartTime      time.Time
	CurrentStepIdx int
	Cancel         func()
	mutex          sync.RWMutex
}

// MigrationStorage interface for persisting migration data
type MigrationStorage interface {
	SavePlan(plan *MigrationPlan) error
	LoadPlan(id string) (*MigrationPlan, error)
	ListPlans() ([]*MigrationPlan, error)
	DeletePlan(id string) error
	SaveExecution(execution *MigrationExecution) error
	LoadExecution(planID string) (*MigrationExecution, error)
}

// NewMigrationManager creates a new migration manager
func NewMigrationManager(config *MigrationConfig, storage MigrationStorage, logger common.StructuredLogger) *MigrationManager {
	registry := NewMigrationRegistry()
	backupManager := NewBackupManager(config.BackupDirectory, logger)

	return &MigrationManager{
		registry:      registry,
		storage:       storage,
		backupManager: backupManager,
		logger:        logger,
		config:        config,
		activePlans:   make(map[string]*MigrationExecution),
		planHistory:   make([]*MigrationPlan, 0),
	}
}

// CreatePlan creates a new migration plan
func (mm *MigrationManager) CreatePlan(name, description, fromVersion, toVersion string) *MigrationPlan {
	plan := &MigrationPlan{
		ID:              generateID(),
		Name:            name,
		Description:     description,
		FromVersion:     fromVersion,
		ToVersion:       toVersion,
		MigrationSteps:  make([]*MigrationStep, 0),
		ValidationSteps: make([]*ValidationStep, 0),
		RollbackSteps:   make([]*RollbackStep, 0),
		Config:          types.NewTypedMap(),
		CreatedAt:       common.ConsensusNow(),
		UpdatedAt:       common.ConsensusNow(),
		Status:          MigrationStatusPending,
	}

	return plan
}

// AddMigrationStep adds a migration step to a plan
func (mm *MigrationManager) AddMigrationStep(plan *MigrationPlan, step *MigrationStep) error {
	if step.ID == "" {
		step.ID = generateID()
	}

	// Validate handler exists
	if _, exists := mm.registry.GetHandler(step.Handler); !exists {
		return fmt.Errorf("migration handler '%s' not found", step.Handler)
	}

	// Set order if not specified
	if step.Order == 0 {
		step.Order = len(plan.MigrationSteps) + 1
	}

	step.Status = StepStatusPending
	plan.MigrationSteps = append(plan.MigrationSteps, step)
	plan.UpdatedAt = common.ConsensusNow()

	// Sort by order
	sort.Slice(plan.MigrationSteps, func(i, j int) bool {
		return plan.MigrationSteps[i].Order < plan.MigrationSteps[j].Order
	})

	return nil
}

// AddValidationStep adds a validation step to a plan
func (mm *MigrationManager) AddValidationStep(plan *MigrationPlan, step *ValidationStep) error {
	if step.ID == "" {
		step.ID = generateID()
	}

	// Validate handler exists
	if _, exists := mm.registry.GetValidationHandler(step.Handler); !exists {
		return fmt.Errorf("validation handler '%s' not found", step.Handler)
	}

	step.Status = StepStatusPending
	plan.ValidationSteps = append(plan.ValidationSteps, step)
	plan.UpdatedAt = common.ConsensusNow()

	return nil
}

// SavePlan persists a migration plan
func (mm *MigrationManager) SavePlan(plan *MigrationPlan) error {
	return mm.storage.SavePlan(plan)
}

// LoadPlan loads a migration plan by ID
func (mm *MigrationManager) LoadPlan(id string) (*MigrationPlan, error) {
	return mm.storage.LoadPlan(id)
}

// ListPlans returns all available migration plans
func (mm *MigrationManager) ListPlans() ([]*MigrationPlan, error) {
	return mm.storage.ListPlans()
}

// ExecutePlan executes a migration plan
func (mm *MigrationManager) ExecutePlan(ctx context.Context, planID string, options *ExecutionOptions) error {
	plan, err := mm.LoadPlan(planID)
	if err != nil {
		return fmt.Errorf("failed to load plan: %w", err)
	}

	if options == nil {
		options = &ExecutionOptions{}
	}

	mm.mutex.Lock()
	if _, exists := mm.activePlans[planID]; exists {
		mm.mutex.Unlock()
		return fmt.Errorf("plan %s is already executing", planID)
	}

	// Check concurrent execution limit
	if len(mm.activePlans) >= mm.config.MaxConcurrent {
		mm.mutex.Unlock()
		return fmt.Errorf("maximum concurrent migrations reached (%d)", mm.config.MaxConcurrent)
	}

	// Create execution context
	ctxWithCancel, cancel := context.WithCancel(ctx)
	migrationCtx := &MigrationContext{
		Plan:          plan,
		Config:        options.Config,
		State:         types.NewTypedMap(),
		Logger:        mm.logger,
		Metrics:       &MigrationMetrics{},
		CancelFunc:    cancel,
		DryRun:        options.DryRun,
		BackupEnabled: mm.config.EnableBackups && !options.SkipBackup,
	}

	execution := &MigrationExecution{
		Plan:           plan,
		Context:        migrationCtx,
		StartTime:      common.ConsensusNow(),
		CurrentStepIdx: 0,
		Cancel:         cancel,
	}

	mm.activePlans[planID] = execution
	mm.mutex.Unlock()

	// Execute in goroutine
	go func() {
		defer func() {
			mm.mutex.Lock()
			delete(mm.activePlans, planID)
			mm.mutex.Unlock()
		}()

		err := mm.executePlanInternal(ctxWithCancel, execution)
		if err != nil {
			mm.logger.Error("Migration failed", common.StringField("plan_id", planID), common.ErrorField(err))
			plan.Status = MigrationStatusFailed
		} else {
			mm.logger.Info("Migration completed successfully", common.StringField("plan_id", planID))
			plan.Status = MigrationStatusCompleted
		}

		// Save final state
		if saveErr := mm.storage.SavePlan(plan); saveErr != nil {
			mm.logger.Error("Failed to save final plan state", common.ErrorField(saveErr))
		}
	}()

	return nil
}

// executePlanInternal performs the actual migration execution
func (mm *MigrationManager) executePlanInternal(ctx context.Context, execution *MigrationExecution) error {
	plan := execution.Plan
	migrationCtx := execution.Context

	mm.logger.Info("Starting migration", common.StringField("plan_id", plan.ID), common.StringField("name", plan.Name))
	plan.Status = MigrationStatusRunning

	// Create backup if enabled
	if migrationCtx.BackupEnabled {
		backupInfo, err := mm.backupManager.CreateBackup(ctx, plan.FromVersion)
		if err != nil {
			return fmt.Errorf("backup failed: %w", err)
		}
		migrationCtx.BackupPath = backupInfo.Path
		mm.logger.Info("Backup created", common.StringField("path", backupInfo.Path))
	}

	// Execute migration steps
	for i, step := range plan.MigrationSteps {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		execution.mutex.Lock()
		execution.CurrentStepIdx = i
		execution.mutex.Unlock()

		migrationCtx.CurrentStep = step

		if err := mm.executeStep(ctx, migrationCtx, step); err != nil {
			mm.logger.Error("Migration step failed", common.StringField("step_id", step.ID), common.ErrorField(err))

			// Attempt rollback if configured
			if mm.shouldRollback(plan, step, err) {
				if rollbackErr := mm.rollbackPlan(ctx, execution); rollbackErr != nil {
					mm.logger.Error("Rollback failed", common.ErrorField(rollbackErr))
				}
				plan.Status = MigrationStatusRolledBack
			}

			return fmt.Errorf("step %s failed: %w", step.Name, err)
		}

		mm.logger.Info("Migration step completed", common.StringField("step_id", step.ID), common.StringField("name", step.Name))
	}

	// Run validation steps
	if mm.config.ValidationMode != "disabled" {
		for _, validationStep := range plan.ValidationSteps {
			if err := mm.executeValidation(ctx, migrationCtx, validationStep); err != nil {
				if mm.config.ValidationMode == "strict" || validationStep.Critical {
					return fmt.Errorf("validation failed: %w", err)
				}
				mm.logger.Warn("Validation failed but continuing", common.ErrorField(err))
			}
		}
	}

	mm.logger.Info("Migration validation completed", common.StringField("plan_id", plan.ID))
	return nil
}

// executeStep executes a single migration step
func (mm *MigrationManager) executeStep(ctx context.Context, migrationCtx *MigrationContext, step *MigrationStep) error {
	handler, exists := mm.registry.GetHandler(step.Handler)
	if !exists {
		return fmt.Errorf("handler %s not found", step.Handler)
	}

	mm.logger.Info("Executing migration step", common.StringField("step_id", step.ID), common.StringField("name", step.Name))
	step.Status = StepStatusRunning
	step.StartedAt = timePtr(common.ConsensusNow())

	// Set timeout
	timeout := step.Timeout
	if timeout == 0 {
		timeout = mm.config.DefaultTimeout
	}

	stepCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// Execute with retry policy
	var result *StepResult
	var err error

	retryPolicy := step.RetryPolicy
	if retryPolicy == nil {
		retryPolicy = mm.config.RetryPolicy
	}

	for attempt := 0; attempt <= retryPolicy.MaxRetries; attempt++ {
		if attempt > 0 {
			step.Status = StepStatusRetrying
			delay := mm.calculateRetryDelay(retryPolicy, attempt)
			mm.logger.Info("Retrying step", common.StringField("step_id", step.ID), common.Field("attempt", attempt), common.Field("delay", delay))

			select {
			case <-time.After(delay):
			case <-stepCtx.Done():
				return stepCtx.Err()
			}
		}

		result, err = handler.Execute(migrationCtx, step)
		if err == nil {
			break
		}

		mm.logger.Warn("Step execution failed", common.StringField("step_id", step.ID), common.Field("attempt", attempt), common.ErrorField(err))
	}

	step.CompletedAt = timePtr(common.ConsensusNow())
	step.Result = result

	if err != nil {
		step.Status = StepStatusFailed
		step.Error = err.Error()
		return err
	}

	step.Status = StepStatusCompleted
	return nil
}

// executeValidation executes a validation step
func (mm *MigrationManager) executeValidation(ctx context.Context, migrationCtx *MigrationContext, step *ValidationStep) error {
	handler, exists := mm.registry.GetValidationHandler(step.Handler)
	if !exists {
		return fmt.Errorf("validation handler %s not found", step.Handler)
	}

	mm.logger.Info("Executing validation step", common.StringField("step_id", step.ID), common.StringField("name", step.Name))
	step.Status = StepStatusRunning

	timeout := step.Timeout
	if timeout == 0 {
		timeout = mm.config.DefaultTimeout
	}

	_, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	result, err := handler.Validate(migrationCtx, step)
	step.Result = result

	if err != nil {
		step.Status = StepStatusFailed
		step.Error = err.Error()
		return err
	}

	if !result.Valid {
		step.Status = StepStatusFailed
		err = fmt.Errorf("validation failed: %s", result.Message)
		step.Error = err.Error()
		return err
	}

	step.Status = StepStatusCompleted
	return nil
}

// rollbackPlan performs rollback operations
func (mm *MigrationManager) rollbackPlan(ctx context.Context, execution *MigrationExecution) error {
	plan := execution.Plan
	migrationCtx := execution.Context

	mm.logger.Info("Starting rollback", common.StringField("plan_id", plan.ID))

	// Execute rollback steps in reverse order
	for i := len(plan.RollbackSteps) - 1; i >= 0; i-- {
		rollbackStep := plan.RollbackSteps[i]

		handler, exists := mm.registry.GetHandler(rollbackStep.Handler)
		if !exists {
			mm.logger.Error("Rollback handler not found", common.StringField("handler", rollbackStep.Handler))
			continue
		}

		if !handler.CanRollback() {
			mm.logger.Warn("Handler does not support rollback", common.StringField("handler", rollbackStep.Handler))
			continue
		}

		mm.logger.Info("Executing rollback step", common.StringField("step_id", rollbackStep.ID))
		rollbackStep.Status = StepStatusRunning

		result, err := handler.Rollback(migrationCtx, &MigrationStep{
			ID:      rollbackStep.ID,
			Name:    rollbackStep.Name,
			Type:    rollbackStep.Type,
			Handler: rollbackStep.Handler,
			Config:  rollbackStep.Config,
		})

		rollbackStep.Result = result

		if err != nil {
			rollbackStep.Status = StepStatusFailed
			rollbackStep.Error = err.Error()
			mm.logger.Error("Rollback step failed", common.StringField("step_id", rollbackStep.ID), common.ErrorField(err))
		} else {
			rollbackStep.Status = StepStatusCompleted
		}
	}

	// Restore from backup if available
	if migrationCtx.BackupPath != "" {
		if err := mm.backupManager.RestoreBackup(ctx, migrationCtx.BackupPath); err != nil {
			mm.logger.Error("Failed to restore backup", common.ErrorField(err))
			return err
		}
		mm.logger.Info("Backup restored", common.StringField("path", migrationCtx.BackupPath))
	}

	return nil
}

// shouldRollback determines if rollback should be performed
func (mm *MigrationManager) shouldRollback(plan *MigrationPlan, failedStep *MigrationStep, err error) bool {
	// Check if plan has rollback steps
	if len(plan.RollbackSteps) == 0 {
		return false
	}

	// Check if backup is available
	if mm.config.EnableBackups {
		return true
	}

	// Check step configuration
	// This could be enhanced with step-specific rollback policies
	return true
}

// calculateRetryDelay calculates delay for retry attempts
func (mm *MigrationManager) calculateRetryDelay(policy *RetryPolicy, attempt int) time.Duration {
	delay := time.Duration(float64(policy.InitialDelay) * math.Pow(policy.Multiplier, float64(attempt-1)))
	if delay > policy.MaxDelay {
		delay = policy.MaxDelay
	}
	return delay
}

// CancelMigration cancels a running migration
func (mm *MigrationManager) CancelMigration(planID string) error {
	mm.mutex.RLock()
	execution, exists := mm.activePlans[planID]
	mm.mutex.RUnlock()

	if !exists {
		return fmt.Errorf("no active migration found for plan %s", planID)
	}

	execution.Cancel()
	execution.Plan.Status = MigrationStatusCanceled

	return mm.storage.SavePlan(execution.Plan)
}

// GetMigrationStatus returns the status of a migration
func (mm *MigrationManager) GetMigrationStatus(planID string) (*MigrationStatus, error) {
	mm.mutex.RLock()
	execution, isActive := mm.activePlans[planID]
	mm.mutex.RUnlock()

	if isActive {
		status := execution.Plan.Status
		return &status, nil
	}

	plan, err := mm.LoadPlan(planID)
	if err != nil {
		return nil, err
	}

	return &plan.Status, nil
}

// GetRegistry returns the migration registry
func (mm *MigrationManager) GetRegistry() *MigrationRegistry {
	return mm.registry
}

// GetBackupManager returns the backup manager
func (mm *MigrationManager) GetBackupManager() *BackupManager {
	return mm.backupManager
}

// DeletePlan deletes a migration plan by ID
func (mm *MigrationManager) DeletePlan(planID string) error {
	mm.mutex.Lock()
	defer mm.mutex.Unlock()

	// Check if plan is currently being executed
	if execution, exists := mm.activePlans[planID]; exists {
		return fmt.Errorf("cannot delete plan %s: currently being executed (started at %s)",
			planID, execution.StartTime.Format(time.RFC3339))
	}

	// Delete from storage
	if err := mm.storage.DeletePlan(planID); err != nil {
		mm.logger.Error("Failed to delete plan from storage",
			common.StringField("plan_id", planID),
			common.ErrorField(err))
		return fmt.Errorf("failed to delete plan: %w", err)
	}

	// Remove from plan history if present
	newHistory := make([]*MigrationPlan, 0, len(mm.planHistory))
	for _, plan := range mm.planHistory {
		if plan.ID != planID {
			newHistory = append(newHistory, plan)
		}
	}
	mm.planHistory = newHistory

	mm.logger.Info("Migration plan deleted successfully",
		common.StringField("plan_id", planID))
	return nil
}

// ExecutionOptions contains options for migration execution
type ExecutionOptions struct {
	Config     *types.TypedMap `json:"config,omitempty"`
	DryRun     bool            `json:"dry_run"`
	SkipBackup bool            `json:"skip_backup"`
}

// Helper functions
func generateID() string {
	return fmt.Sprintf("mig_%d", common.ConsensusNow().UnixNano())
}

func timePtr(t time.Time) *time.Time {
	return &t
}
