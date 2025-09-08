package migration

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// FileMigrationStorage implements MigrationStorage using file system
type FileMigrationStorage struct {
	basePath string
	mutex    sync.RWMutex
}

// NewFileMigrationStorage creates a new file-based migration storage
func NewFileMigrationStorage(basePath string) (*FileMigrationStorage, error) {
	if err := os.MkdirAll(basePath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create storage directory: %w", err)
	}

	return &FileMigrationStorage{
		basePath: basePath,
	}, nil
}

// SavePlan saves a migration plan to disk
func (fs *FileMigrationStorage) SavePlan(plan *MigrationPlan) error {
	fs.mutex.Lock()
	defer fs.mutex.Unlock()

	planPath := filepath.Join(fs.basePath, fmt.Sprintf("%s.json", plan.ID))

	data, err := json.MarshalIndent(plan, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal plan: %w", err)
	}

	if err := ioutil.WriteFile(planPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write plan file: %w", err)
	}

	return nil
}

// LoadPlan loads a migration plan from disk
func (fs *FileMigrationStorage) LoadPlan(id string) (*MigrationPlan, error) {
	fs.mutex.RLock()
	defer fs.mutex.RUnlock()

	planPath := filepath.Join(fs.basePath, fmt.Sprintf("%s.json", id))

	data, err := ioutil.ReadFile(planPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("plan %s not found", id)
		}
		return nil, fmt.Errorf("failed to read plan file: %w", err)
	}

	var plan MigrationPlan
	if err := json.Unmarshal(data, &plan); err != nil {
		return nil, fmt.Errorf("failed to unmarshal plan: %w", err)
	}

	return &plan, nil
}

// ListPlans returns all available migration plans
func (fs *FileMigrationStorage) ListPlans() ([]*MigrationPlan, error) {
	fs.mutex.RLock()
	defer fs.mutex.RUnlock()

	files, err := ioutil.ReadDir(fs.basePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read storage directory: %w", err)
	}

	plans := make([]*MigrationPlan, 0)
	for _, file := range files {
		if file.IsDir() || !strings.HasSuffix(file.Name(), ".json") {
			continue
		}

		// Skip execution files
		if strings.Contains(file.Name(), "_execution.json") {
			continue
		}

		planID := strings.TrimSuffix(file.Name(), ".json")
		plan, err := fs.LoadPlan(planID)
		if err != nil {
			continue // Skip invalid plans
		}

		plans = append(plans, plan)
	}

	return plans, nil
}

// DeletePlan removes a migration plan from disk
func (fs *FileMigrationStorage) DeletePlan(id string) error {
	fs.mutex.Lock()
	defer fs.mutex.Unlock()

	planPath := filepath.Join(fs.basePath, fmt.Sprintf("%s.json", id))

	if err := os.Remove(planPath); err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("plan %s not found", id)
		}
		return fmt.Errorf("failed to delete plan file: %w", err)
	}

	// Also remove execution file if it exists
	executionPath := filepath.Join(fs.basePath, fmt.Sprintf("%s_execution.json", id))
	if _, err := os.Stat(executionPath); err == nil {
		os.Remove(executionPath)
	}

	return nil
}

// SaveExecution saves migration execution state
func (fs *FileMigrationStorage) SaveExecution(execution *MigrationExecution) error {
	fs.mutex.Lock()
	defer fs.mutex.Unlock()

	executionPath := filepath.Join(fs.basePath, fmt.Sprintf("%s_execution.json", execution.Plan.ID))

	// Create a serializable version of execution
	execData := struct {
		PlanID         string            `json:"plan_id"`
		StartTime      time.Time         `json:"start_time"`
		CurrentStepIdx int               `json:"current_step_idx"`
		Context        *MigrationContext `json:"context"`
	}{
		PlanID:         execution.Plan.ID,
		StartTime:      execution.StartTime,
		CurrentStepIdx: execution.CurrentStepIdx,
		Context:        execution.Context,
	}

	data, err := json.MarshalIndent(execData, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal execution: %w", err)
	}

	if err := ioutil.WriteFile(executionPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write execution file: %w", err)
	}

	return nil
}

// LoadExecution loads migration execution state
func (fs *FileMigrationStorage) LoadExecution(planID string) (*MigrationExecution, error) {
	fs.mutex.RLock()
	defer fs.mutex.RUnlock()

	executionPath := filepath.Join(fs.basePath, fmt.Sprintf("%s_execution.json", planID))

	data, err := ioutil.ReadFile(executionPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("execution for plan %s not found", planID)
		}
		return nil, fmt.Errorf("failed to read execution file: %w", err)
	}

	var execData struct {
		PlanID         string            `json:"plan_id"`
		StartTime      time.Time         `json:"start_time"`
		CurrentStepIdx int               `json:"current_step_idx"`
		Context        *MigrationContext `json:"context"`
	}

	if err := json.Unmarshal(data, &execData); err != nil {
		return nil, fmt.Errorf("failed to unmarshal execution: %w", err)
	}

	// Load the associated plan
	plan, err := fs.LoadPlan(planID)
	if err != nil {
		return nil, fmt.Errorf("failed to load plan for execution: %w", err)
	}

	execution := &MigrationExecution{
		Plan:           plan,
		Context:        execData.Context,
		StartTime:      execData.StartTime,
		CurrentStepIdx: execData.CurrentStepIdx,
	}

	return execution, nil
}

// InMemoryMigrationStorage implements MigrationStorage in memory (for testing)
type InMemoryMigrationStorage struct {
	plans      map[string]*MigrationPlan
	executions map[string]*MigrationExecution
	mutex      sync.RWMutex
}

// NewInMemoryMigrationStorage creates a new in-memory migration storage
func NewInMemoryMigrationStorage() *InMemoryMigrationStorage {
	return &InMemoryMigrationStorage{
		plans:      make(map[string]*MigrationPlan),
		executions: make(map[string]*MigrationExecution),
	}
}

// SavePlan saves a migration plan in memory
func (ms *InMemoryMigrationStorage) SavePlan(plan *MigrationPlan) error {
	ms.mutex.Lock()
	defer ms.mutex.Unlock()

	// Deep copy the plan to avoid reference issues
	planData, err := json.Marshal(plan)
	if err != nil {
		return err
	}

	var planCopy MigrationPlan
	if err := json.Unmarshal(planData, &planCopy); err != nil {
		return err
	}

	ms.plans[plan.ID] = &planCopy
	return nil
}

// LoadPlan loads a migration plan from memory
func (ms *InMemoryMigrationStorage) LoadPlan(id string) (*MigrationPlan, error) {
	ms.mutex.RLock()
	defer ms.mutex.RUnlock()

	plan, exists := ms.plans[id]
	if !exists {
		return nil, fmt.Errorf("plan %s not found", id)
	}

	// Return a copy to avoid reference issues
	planData, err := json.Marshal(plan)
	if err != nil {
		return nil, err
	}

	var planCopy MigrationPlan
	if err := json.Unmarshal(planData, &planCopy); err != nil {
		return nil, err
	}

	return &planCopy, nil
}

// ListPlans returns all available migration plans from memory
func (ms *InMemoryMigrationStorage) ListPlans() ([]*MigrationPlan, error) {
	ms.mutex.RLock()
	defer ms.mutex.RUnlock()

	plans := make([]*MigrationPlan, 0, len(ms.plans))
	for _, plan := range ms.plans {
		// Return copies to avoid reference issues
		planData, err := json.Marshal(plan)
		if err != nil {
			continue
		}

		var planCopy MigrationPlan
		if err := json.Unmarshal(planData, &planCopy); err != nil {
			continue
		}

		plans = append(plans, &planCopy)
	}

	return plans, nil
}

// DeletePlan removes a migration plan from memory
func (ms *InMemoryMigrationStorage) DeletePlan(id string) error {
	ms.mutex.Lock()
	defer ms.mutex.Unlock()

	if _, exists := ms.plans[id]; !exists {
		return fmt.Errorf("plan %s not found", id)
	}

	delete(ms.plans, id)
	delete(ms.executions, id) // Also remove execution if it exists

	return nil
}

// SaveExecution saves migration execution state in memory
func (ms *InMemoryMigrationStorage) SaveExecution(execution *MigrationExecution) error {
	ms.mutex.Lock()
	defer ms.mutex.Unlock()

	// Deep copy the execution to avoid reference issues
	execData, err := json.Marshal(execution)
	if err != nil {
		return err
	}

	var execCopy MigrationExecution
	if err := json.Unmarshal(execData, &execCopy); err != nil {
		return err
	}

	ms.executions[execution.Plan.ID] = &execCopy
	return nil
}

// LoadExecution loads migration execution state from memory
func (ms *InMemoryMigrationStorage) LoadExecution(planID string) (*MigrationExecution, error) {
	ms.mutex.RLock()
	defer ms.mutex.RUnlock()

	execution, exists := ms.executions[planID]
	if !exists {
		return nil, fmt.Errorf("execution for plan %s not found", planID)
	}

	// Return a copy to avoid reference issues
	execData, err := json.Marshal(execution)
	if err != nil {
		return nil, err
	}

	var execCopy MigrationExecution
	if err := json.Unmarshal(execData, &execCopy); err != nil {
		return nil, err
	}

	return &execCopy, nil
}

// DatabaseMigrationStorage implements MigrationStorage using a database
type DatabaseMigrationStorage struct {
	db    MigrationDatabase
	mutex sync.RWMutex
}

// MigrationDatabase interface for database operations
type MigrationDatabase interface {
	SavePlan(plan *MigrationPlan) error
	LoadPlan(id string) (*MigrationPlan, error)
	ListPlans() ([]*MigrationPlan, error)
	DeletePlan(id string) error
	SaveExecution(execution *MigrationExecution) error
	LoadExecution(planID string) (*MigrationExecution, error)
	Close() error
}

// NewDatabaseMigrationStorage creates a new database-backed migration storage
func NewDatabaseMigrationStorage(db MigrationDatabase) *DatabaseMigrationStorage {
	return &DatabaseMigrationStorage{
		db: db,
	}
}

// SavePlan saves a migration plan to database
func (ds *DatabaseMigrationStorage) SavePlan(plan *MigrationPlan) error {
	ds.mutex.Lock()
	defer ds.mutex.Unlock()

	return ds.db.SavePlan(plan)
}

// LoadPlan loads a migration plan from database
func (ds *DatabaseMigrationStorage) LoadPlan(id string) (*MigrationPlan, error) {
	ds.mutex.RLock()
	defer ds.mutex.RUnlock()

	return ds.db.LoadPlan(id)
}

// ListPlans returns all available migration plans from database
func (ds *DatabaseMigrationStorage) ListPlans() ([]*MigrationPlan, error) {
	ds.mutex.RLock()
	defer ds.mutex.RUnlock()

	return ds.db.ListPlans()
}

// DeletePlan removes a migration plan from database
func (ds *DatabaseMigrationStorage) DeletePlan(id string) error {
	ds.mutex.Lock()
	defer ds.mutex.Unlock()

	return ds.db.DeletePlan(id)
}

// SaveExecution saves migration execution state to database
func (ds *DatabaseMigrationStorage) SaveExecution(execution *MigrationExecution) error {
	ds.mutex.Lock()
	defer ds.mutex.Unlock()

	return ds.db.SaveExecution(execution)
}

// LoadExecution loads migration execution state from database
func (ds *DatabaseMigrationStorage) LoadExecution(planID string) (*MigrationExecution, error) {
	ds.mutex.RLock()
	defer ds.mutex.RUnlock()

	return ds.db.LoadExecution(planID)
}

// Close closes the database connection
func (ds *DatabaseMigrationStorage) Close() error {
	return ds.db.Close()
}
