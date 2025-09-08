package migration

import (
	"encoding/json"
	"time"

	"diamante/types"
)

// MigrationType represents the type of migration being performed
type MigrationType string

const (
	MigrationTypeGenesis   MigrationType = "genesis"
	MigrationTypeState     MigrationType = "state"
	MigrationTypeValidator MigrationType = "validator"
	MigrationTypeConsensus MigrationType = "consensus"
	MigrationTypeStorage   MigrationType = "storage"
	MigrationTypeNetwork   MigrationType = "network"
	MigrationTypeContract  MigrationType = "contract"
)

// MigrationPlan defines a complete migration strategy
type MigrationPlan struct {
	ID              string            `json:"id"`
	Name            string            `json:"name"`
	Description     string            `json:"description"`
	FromVersion     string            `json:"from_version"`
	ToVersion       string            `json:"to_version"`
	MigrationSteps  []*MigrationStep  `json:"migration_steps"`
	ValidationSteps []*ValidationStep `json:"validation_steps"`
	RollbackSteps   []*RollbackStep   `json:"rollback_steps"`
	Config          *types.TypedMap   `json:"config"`
	CreatedAt       time.Time         `json:"created_at"`
	UpdatedAt       time.Time         `json:"updated_at"`
	Status          MigrationStatus   `json:"status"`
}

// MigrationStep represents a single migration operation
type MigrationStep struct {
	ID            string          `json:"id"`
	Name          string          `json:"name"`
	Type          MigrationType   `json:"type"`
	Description   string          `json:"description"`
	Order         int             `json:"order"`
	Handler       string          `json:"handler"`
	Config        *types.TypedMap `json:"config"`
	Prerequisites []string        `json:"prerequisites"`
	Dependencies  []string        `json:"dependencies"`
	Timeout       time.Duration   `json:"timeout"`
	RetryPolicy   *RetryPolicy    `json:"retry_policy"`
	Status        StepStatus      `json:"status"`
	Result        *StepResult     `json:"result,omitempty"`
	StartedAt     *time.Time      `json:"started_at,omitempty"`
	CompletedAt   *time.Time      `json:"completed_at,omitempty"`
	Error         string          `json:"error,omitempty"`
}

// ValidationStep represents validation after migration
type ValidationStep struct {
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	Type        ValidationType    `json:"type"`
	Description string            `json:"description"`
	Handler     string            `json:"handler"`
	Config      *types.TypedMap   `json:"config"`
	Timeout     time.Duration     `json:"timeout"`
	Critical    bool              `json:"critical"`
	Status      StepStatus        `json:"status"`
	Result      *ValidationResult `json:"result,omitempty"`
	Error       string            `json:"error,omitempty"`
}

// RollbackStep represents rollback operations
type RollbackStep struct {
	ID          string          `json:"id"`
	Name        string          `json:"name"`
	Type        MigrationType   `json:"type"`
	Description string          `json:"description"`
	Handler     string          `json:"handler"`
	Config      *types.TypedMap `json:"config"`
	Timeout     time.Duration   `json:"timeout"`
	Status      StepStatus      `json:"status"`
	Result      *StepResult     `json:"result,omitempty"`
	Error       string          `json:"error,omitempty"`
}

// MigrationStatus represents the overall migration status
type MigrationStatus string

const (
	MigrationStatusPending    MigrationStatus = "pending"
	MigrationStatusRunning    MigrationStatus = "running"
	MigrationStatusCompleted  MigrationStatus = "completed"
	MigrationStatusFailed     MigrationStatus = "failed"
	MigrationStatusRolledBack MigrationStatus = "rolled_back"
	MigrationStatusCanceled   MigrationStatus = "canceled"
)

// StepStatus represents individual step status
type StepStatus string

const (
	StepStatusPending   StepStatus = "pending"
	StepStatusRunning   StepStatus = "running"
	StepStatusCompleted StepStatus = "completed"
	StepStatusFailed    StepStatus = "failed"
	StepStatusSkipped   StepStatus = "skipped"
	StepStatusRetrying  StepStatus = "retrying"
)

// ValidationType represents different validation checks
type ValidationType string

const (
	ValidationTypeIntegrity     ValidationType = "integrity"
	ValidationTypeConsistency   ValidationType = "consistency"
	ValidationTypePerformance   ValidationType = "performance"
	ValidationTypeSecurity      ValidationType = "security"
	ValidationTypeCompatibility ValidationType = "compatibility"
	ValidationTypeCustom        ValidationType = "custom"
)

// RetryPolicy defines retry behavior for failed steps
type RetryPolicy struct {
	MaxRetries   int           `json:"max_retries"`
	InitialDelay time.Duration `json:"initial_delay"`
	MaxDelay     time.Duration `json:"max_delay"`
	Multiplier   float64       `json:"multiplier"`
}

// StepResult contains the result of a migration step
type StepResult struct {
	Success  bool              `json:"success"`
	Data     *types.TypedMap   `json:"data,omitempty"`
	Metrics  *MigrationMetrics `json:"metrics,omitempty"`
	Message  string            `json:"message,omitempty"`
	Warnings []string          `json:"warnings,omitempty"`
	Logs     []string          `json:"logs,omitempty"`
}

// ValidationResult contains validation check results
type ValidationResult struct {
	Valid   bool               `json:"valid"`
	Score   float64            `json:"score"`
	Details *types.TypedMap    `json:"details,omitempty"`
	Issues  []ValidationIssue  `json:"issues,omitempty"`
	Metrics *ValidationMetrics `json:"metrics,omitempty"`
	Message string             `json:"message,omitempty"`
}

// ValidationIssue represents a specific validation problem
type ValidationIssue struct {
	ID         string          `json:"id"`
	Type       string          `json:"type"`
	Severity   string          `json:"severity"`
	Message    string          `json:"message"`
	Details    *types.TypedMap `json:"details,omitempty"`
	Suggestion string          `json:"suggestion,omitempty"`
	CanIgnore  bool            `json:"can_ignore"`
}

// MigrationMetrics tracks migration performance
type MigrationMetrics struct {
	Duration       time.Duration   `json:"duration"`
	ProcessedItems int64           `json:"processed_items"`
	FailedItems    int64           `json:"failed_items"`
	SkippedItems   int64           `json:"skipped_items"`
	BytesProcessed int64           `json:"bytes_processed"`
	MemoryUsed     int64           `json:"memory_used"`
	DiskUsed       int64           `json:"disk_used"`
	CustomMetrics  *types.TypedMap `json:"custom_metrics,omitempty"`
}

// ValidationMetrics tracks validation performance
type ValidationMetrics struct {
	Duration       time.Duration   `json:"duration"`
	ChecksRun      int             `json:"checks_run"`
	ChecksPassed   int             `json:"checks_passed"`
	ChecksFailed   int             `json:"checks_failed"`
	ItemsValidated int64           `json:"items_validated"`
	ErrorRate      float64         `json:"error_rate"`
	CustomMetrics  *types.TypedMap `json:"custom_metrics,omitempty"`
}

// MigrationContext provides runtime context for migrations
type MigrationContext struct {
	Plan          *MigrationPlan    `json:"plan"`
	CurrentStep   *MigrationStep    `json:"current_step,omitempty"`
	Config        *types.TypedMap   `json:"config"`
	State         *types.TypedMap   `json:"state"`
	Logger        interface{}       `json:"-"`
	Metrics       *MigrationMetrics `json:"metrics"`
	CancelFunc    func()            `json:"-"`
	DryRun        bool              `json:"dry_run"`
	BackupEnabled bool              `json:"backup_enabled"`
	BackupPath    string            `json:"backup_path,omitempty"`
}

// MigrationHandler defines the interface for migration step handlers
type MigrationHandler interface {
	// Execute performs the migration step
	Execute(ctx *MigrationContext, step *MigrationStep) (*StepResult, error)

	// Validate checks if the migration can be performed
	Validate(ctx *MigrationContext, step *MigrationStep) error

	// EstimateTime estimates how long the migration will take
	EstimateTime(ctx *MigrationContext, step *MigrationStep) (time.Duration, error)

	// CanRollback indicates if this step supports rollback
	CanRollback() bool

	// Rollback reverses the migration step
	Rollback(ctx *MigrationContext, step *MigrationStep) (*StepResult, error)
}

// ValidationHandler defines the interface for validation handlers
type ValidationHandler interface {
	// Validate performs the validation check
	Validate(ctx *MigrationContext, step *ValidationStep) (*ValidationResult, error)

	// GetRequirements returns validation requirements
	GetRequirements() []string

	// IsCritical indicates if validation failure should stop migration
	IsCritical() bool
}

// BackupInfo contains information about created backups
type BackupInfo struct {
	ID        string          `json:"id"`
	Type      string          `json:"type"`
	Path      string          `json:"path"`
	Size      int64           `json:"size"`
	Checksum  string          `json:"checksum"`
	CreatedAt time.Time       `json:"created_at"`
	Version   string          `json:"version"`
	Metadata  *types.TypedMap `json:"metadata,omitempty"`
}

// MigrationRegistry manages available migration handlers
type MigrationRegistry struct {
	handlers           map[string]MigrationHandler
	validationHandlers map[string]ValidationHandler
}

// NewMigrationRegistry creates a new migration registry
func NewMigrationRegistry() *MigrationRegistry {
	return &MigrationRegistry{
		handlers:           make(map[string]MigrationHandler),
		validationHandlers: make(map[string]ValidationHandler),
	}
}

// RegisterHandler registers a migration handler
func (r *MigrationRegistry) RegisterHandler(name string, handler MigrationHandler) {
	r.handlers[name] = handler
}

// RegisterValidationHandler registers a validation handler
func (r *MigrationRegistry) RegisterValidationHandler(name string, handler ValidationHandler) {
	r.validationHandlers[name] = handler
}

// GetHandler retrieves a migration handler
func (r *MigrationRegistry) GetHandler(name string) (MigrationHandler, bool) {
	handler, exists := r.handlers[name]
	return handler, exists
}

// GetValidationHandler retrieves a validation handler
func (r *MigrationRegistry) GetValidationHandler(name string) (ValidationHandler, bool) {
	handler, exists := r.validationHandlers[name]
	return handler, exists
}

// ListHandlers returns all registered handler names
func (r *MigrationRegistry) ListHandlers() []string {
	names := make([]string, 0, len(r.handlers))
	for name := range r.handlers {
		names = append(names, name)
	}
	return names
}

// ListValidationHandlers returns all registered validation handler names
func (r *MigrationRegistry) ListValidationHandlers() []string {
	names := make([]string, 0, len(r.validationHandlers))
	for name := range r.validationHandlers {
		names = append(names, name)
	}
	return names
}

// JSON marshaling helper methods
func (mp *MigrationPlan) MarshalJSON() ([]byte, error) {
	type Alias MigrationPlan
	return json.Marshal(&struct {
		*Alias
		CreatedAt string `json:"created_at"`
		UpdatedAt string `json:"updated_at"`
	}{
		Alias:     (*Alias)(mp),
		CreatedAt: mp.CreatedAt.Format(time.RFC3339),
		UpdatedAt: mp.UpdatedAt.Format(time.RFC3339),
	})
}

func (mp *MigrationPlan) UnmarshalJSON(data []byte) error {
	type Alias MigrationPlan
	aux := &struct {
		*Alias
		CreatedAt string `json:"created_at"`
		UpdatedAt string `json:"updated_at"`
	}{
		Alias: (*Alias)(mp),
	}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	if aux.CreatedAt != "" {
		if t, err := time.Parse(time.RFC3339, aux.CreatedAt); err == nil {
			mp.CreatedAt = t
		}
	}

	if aux.UpdatedAt != "" {
		if t, err := time.Parse(time.RFC3339, aux.UpdatedAt); err == nil {
			mp.UpdatedAt = t
		}
	}

	return nil
}
