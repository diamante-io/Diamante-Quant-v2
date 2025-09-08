package handlers

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"diamante/common"
	"diamante/migration"
	"diamante/types"
	"github.com/sirupsen/logrus"
)

// GenesisHandler handles genesis file migrations and upgrades
type GenesisHandler struct {
	genesisPath string
}

// NewGenesisHandler creates a new genesis handler
func NewGenesisHandler(genesisPath string) *GenesisHandler {
	return &GenesisHandler{
		genesisPath: genesisPath,
	}
}

// Execute performs the genesis migration
func (h *GenesisHandler) Execute(ctx *migration.MigrationContext, step *migration.MigrationStep) (*migration.StepResult, error) {
	startTime := common.ConsensusNow()

	// Get migration configuration
	sourceGenesis, _ := step.Config.GetString("source_genesis")
	targetGenesis, _ := step.Config.GetString("target_genesis")
	transformations, _ := step.Config.Get("transformations")

	if sourceGenesis == "" {
		sourceGenesis = h.genesisPath
	}

	if targetGenesis == "" {
		targetGenesis = filepath.Join(filepath.Dir(sourceGenesis), "genesis_migrated.json")
	}

	if logger, ok := ctx.Logger.(*logrus.Logger); ok {
		logger.Info("Starting genesis migration", logrus.Fields{"source": sourceGenesis, "target": targetGenesis})
	}

	// Load source genesis
	sourceData, err := h.loadGenesis(sourceGenesis)
	if err != nil {
		return nil, fmt.Errorf("failed to load source genesis: %w", err)
	}

	// Apply transformations
	targetData := sourceData
	if transformations != nil {
		// Transformations is already a Value, try to extract array/slice data
		if val := transformations; val != nil {
			// Try to unmarshal as array
			var transformList []interface{}
			if err := json.Unmarshal(val.Data, &transformList); err == nil {
				for _, transform := range transformList {
					// Convert each transform to TypedMap
					transformBytes, _ := json.Marshal(transform)
					transformMap := types.NewTypedMap()
					if err := json.Unmarshal(transformBytes, &transformMap); err == nil {
						targetData, err = h.applyTransformation(targetData, transformMap)
						if err != nil {
							return nil, fmt.Errorf("failed to apply transformation: %w", err)
						}
					}
				}
			}
		}
	}

	// Update chain metadata
	if err := h.updateChainMetadata(targetData, ctx, step); err != nil {
		return nil, fmt.Errorf("failed to update chain metadata: %w", err)
	}

	// Validate migrated genesis
	if err := h.validateGenesis(targetData); err != nil {
		return nil, fmt.Errorf("genesis validation failed: %w", err)
	}

	// Save migrated genesis
	if !ctx.DryRun {
		if err := h.saveGenesis(targetData, targetGenesis); err != nil {
			return nil, fmt.Errorf("failed to save migrated genesis: %w", err)
		}
	}

	duration := time.Since(startTime)

	result := &migration.StepResult{
		Success: true,
		Data:    types.NewTypedMap(),
		Metrics: &migration.MigrationMetrics{
			Duration:       duration,
			ProcessedItems: 1,
			BytesProcessed: int64(len(targetData)),
		},
		Message: fmt.Sprintf("Genesis migrated successfully from %s to %s", sourceGenesis, targetGenesis),
	}

	result.Data.Set("source_genesis", types.StringToValue(sourceGenesis))
	result.Data.Set("target_genesis", types.StringToValue(targetGenesis))
	// Count transformations
	transformCount := 0
	if transformations != nil {
		if val := transformations; val != nil {
			var transformList []interface{}
			if err := json.Unmarshal(val.Data, &transformList); err == nil {
				transformCount = len(transformList)
			}
		}
	}
	result.Data.Set("transformations_applied", types.IntToValue(int64(transformCount)))

	return result, nil
}

// Validate checks if the genesis migration can be performed
func (h *GenesisHandler) Validate(ctx *migration.MigrationContext, step *migration.MigrationStep) error {
	sourceGenesis, _ := step.Config.GetString("source_genesis")
	if sourceGenesis == "" {
		sourceGenesis = h.genesisPath
	}

	// Check if source genesis exists
	if _, err := os.Stat(sourceGenesis); os.IsNotExist(err) {
		return fmt.Errorf("source genesis file does not exist: %s", sourceGenesis)
	}

	// Validate source genesis format
	if _, err := h.loadGenesis(sourceGenesis); err != nil {
		return fmt.Errorf("source genesis is invalid: %w", err)
	}

	// Check write permissions for target directory
	targetGenesis, _ := step.Config.GetString("target_genesis")
	if targetGenesis == "" {
		targetGenesis = filepath.Join(filepath.Dir(sourceGenesis), "genesis_migrated.json")
	}

	targetDir := filepath.Dir(targetGenesis)
	if _, err := os.Stat(targetDir); os.IsNotExist(err) {
		return fmt.Errorf("target directory does not exist: %s", targetDir)
	}

	// Test write permissions
	testFile := filepath.Join(targetDir, ".write_test")
	if err := os.WriteFile(testFile, []byte("test"), 0644); err != nil {
		return fmt.Errorf("no write permission to target directory: %w", err)
	}
	os.Remove(testFile)

	return nil
}

// EstimateTime estimates how long the genesis migration will take
func (h *GenesisHandler) EstimateTime(ctx *migration.MigrationContext, step *migration.MigrationStep) (time.Duration, error) {
	sourceGenesis, _ := step.Config.GetString("source_genesis")
	if sourceGenesis == "" {
		sourceGenesis = h.genesisPath
	}

	// Get file size to estimate processing time
	stat, err := os.Stat(sourceGenesis)
	if err != nil {
		return 0, err
	}

	// Estimate ~1MB per second for processing
	size := stat.Size()
	estimatedSeconds := size / (1024 * 1024)
	if estimatedSeconds < 1 {
		estimatedSeconds = 1
	}

	return time.Duration(estimatedSeconds) * time.Second, nil
}

// CanRollback indicates if this handler supports rollback
func (h *GenesisHandler) CanRollback() bool {
	return true
}

// Rollback reverses the genesis migration
func (h *GenesisHandler) Rollback(ctx *migration.MigrationContext, step *migration.MigrationStep) (*migration.StepResult, error) {
	sourceGenesis, _ := step.Config.GetString("source_genesis")
	targetGenesis, _ := step.Config.GetString("target_genesis")

	if sourceGenesis == "" {
		sourceGenesis = h.genesisPath
	}

	if targetGenesis == "" {
		targetGenesis = filepath.Join(filepath.Dir(sourceGenesis), "genesis_migrated.json")
	}

	// Restore original genesis if target was created
	if _, err := os.Stat(targetGenesis); err == nil {
		if !ctx.DryRun {
			if err := os.Remove(targetGenesis); err != nil {
				return nil, fmt.Errorf("failed to remove migrated genesis: %w", err)
			}
		}
	}

	result := &migration.StepResult{
		Success: true,
		Message: "Genesis migration rolled back successfully",
	}

	return result, nil
}

// loadGenesis loads and parses a genesis file
func (h *GenesisHandler) loadGenesis(path string) (map[string]interface{}, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var genesis map[string]interface{}
	if err := json.Unmarshal(data, &genesis); err != nil {
		return nil, err
	}

	return genesis, nil
}

// saveGenesis saves a genesis file
func (h *GenesisHandler) saveGenesis(genesis map[string]interface{}, path string) error {
	data, err := json.MarshalIndent(genesis, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(path, data, 0644)
}

// applyTransformation applies a transformation to the genesis data
func (h *GenesisHandler) applyTransformation(genesis map[string]interface{}, transform *types.TypedMap) (map[string]interface{}, error) {
	transformType, _ := transform.GetString("type")

	switch transformType {
	case "update_chain_id":
		return h.updateChainID(genesis, transform)
	case "update_consensus_params":
		return h.updateConsensusParams(genesis, transform)
	case "update_app_state":
		return h.updateAppState(genesis, transform)
	case "migrate_accounts":
		return h.migrateAccounts(genesis, transform)
	case "migrate_validators":
		return h.migrateValidators(genesis, transform)
	default:
		return nil, fmt.Errorf("unknown transformation type: %s", transformType)
	}
}

// updateChainID updates the chain ID in genesis
func (h *GenesisHandler) updateChainID(genesis map[string]interface{}, transform *types.TypedMap) (map[string]interface{}, error) {
	newChainID, _ := transform.GetString("chain_id")
	if newChainID == "" {
		return nil, fmt.Errorf("chain_id is required for update_chain_id transformation")
	}

	genesis["chain_id"] = newChainID
	return genesis, nil
}

// updateConsensusParams updates consensus parameters
func (h *GenesisHandler) updateConsensusParams(genesis map[string]interface{}, transform *types.TypedMap) (map[string]interface{}, error) {
	consensusParams, ok := genesis["consensus_params"].(map[string]interface{})
	if !ok {
		consensusParams = make(map[string]interface{})
		genesis["consensus_params"] = consensusParams
	}

	// Update block parameters
	if blockValue, ok := transform.Get("block"); ok && blockValue != nil {
		// Try to unmarshal as a map
		var blockParams map[string]interface{}
		if err := json.Unmarshal(blockValue.Data, &blockParams); err == nil {
			blockParamsMap, ok := consensusParams["block"].(map[string]interface{})
			if !ok {
				blockParamsMap = make(map[string]interface{})
				consensusParams["block"] = blockParamsMap
			}

			if maxBytes, ok := blockParams["max_bytes"].(float64); ok && maxBytes > 0 {
				blockParamsMap["max_bytes"] = int64(maxBytes)
			}

			if maxGas, ok := blockParams["max_gas"].(float64); ok && maxGas > 0 {
				blockParamsMap["max_gas"] = int64(maxGas)
			}
		}
	}

	// Update evidence parameters
	if evidenceValue, ok := transform.Get("evidence"); ok && evidenceValue != nil {
		// Try to extract evidence params as TypedMap
		evidenceParams := types.NewTypedMap()
		if err := json.Unmarshal(evidenceValue.Data, &evidenceParams); err == nil {
			evidenceParamsMap, ok := consensusParams["evidence"].(map[string]interface{})
			if !ok {
				evidenceParamsMap = make(map[string]interface{})
				consensusParams["evidence"] = evidenceParamsMap
			}

			if maxAgeNumBlocks, err := evidenceParams.GetInt64("max_age_num_blocks"); err == nil && maxAgeNumBlocks > 0 {
				evidenceParamsMap["max_age_num_blocks"] = maxAgeNumBlocks
			}

			if maxAgeDuration, err := evidenceParams.GetString("max_age_duration"); err == nil && maxAgeDuration != "" {
				evidenceParamsMap["max_age_duration"] = maxAgeDuration
			}
		}
	}

	return genesis, nil
}

// updateAppState updates application state
func (h *GenesisHandler) updateAppState(genesis map[string]interface{}, transform *types.TypedMap) (map[string]interface{}, error) {
	appState, ok := genesis["app_state"].(map[string]interface{})
	if !ok {
		appState = make(map[string]interface{})
		genesis["app_state"] = appState
	}

	// Apply module-specific updates
	if updatesValue, ok := transform.Get("updates"); ok && updatesValue != nil {
		// Try to unmarshal as a map of module updates
		var updates map[string]interface{}
		if err := json.Unmarshal(updatesValue.Data, &updates); err == nil {
			for module, moduleUpdates := range updates {
				// Convert module updates to TypedMap
				moduleBytes, _ := json.Marshal(moduleUpdates)
				moduleMap := types.NewTypedMap()
				if err := json.Unmarshal(moduleBytes, &moduleMap); err == nil {
					h.updateModuleState(appState, module, moduleMap)
				}
			}
		}
	}

	return genesis, nil
}

// updateModuleState updates state for a specific module
func (h *GenesisHandler) updateModuleState(appState map[string]interface{}, module string, updates *types.TypedMap) {
	moduleState, ok := appState[module].(map[string]interface{})
	if !ok {
		moduleState = make(map[string]interface{})
		appState[module] = moduleState
	}

	// Apply updates to module state
	for _, key := range updates.Keys() {
		if value, ok := updates.Get(key); ok && value != nil {
			// Convert Value to appropriate type
			var val interface{}
			if err := json.Unmarshal(value.Data, &val); err == nil {
				moduleState[key] = val
			}
		}
	}
}

// migrateAccounts migrates account data
func (h *GenesisHandler) migrateAccounts(genesis map[string]interface{}, transform *types.TypedMap) (map[string]interface{}, error) {
	appState, ok := genesis["app_state"].(map[string]interface{})
	if !ok {
		return genesis, nil
	}

	authState, ok := appState["auth"].(map[string]interface{})
	if !ok {
		return genesis, nil
	}

	accounts, ok := authState["accounts"].([]interface{})
	if !ok {
		return genesis, nil
	}

	// Apply account transformations
	migratedAccounts := make([]interface{}, 0, len(accounts))
	for _, account := range accounts {
		if accountMap, ok := account.(map[string]interface{}); ok {
			migratedAccount := h.migrateAccount(accountMap, transform)
			migratedAccounts = append(migratedAccounts, migratedAccount)
		}
	}

	authState["accounts"] = migratedAccounts
	return genesis, nil
}

// migrateAccount migrates a single account
func (h *GenesisHandler) migrateAccount(account map[string]interface{}, transform *types.TypedMap) map[string]interface{} {
	// Apply account-specific transformations
	if accountType, _ := transform.GetString("account_type"); accountType != "" {
		account["@type"] = accountType
	}

	// Update sequence number if specified
	if sequence, err := transform.GetInt64("sequence"); err == nil && sequence >= 0 {
		account["sequence"] = fmt.Sprintf("%d", sequence)
	}

	return account
}

// migrateValidators migrates validator data
func (h *GenesisHandler) migrateValidators(genesis map[string]interface{}, transform *types.TypedMap) (map[string]interface{}, error) {
	appState, ok := genesis["app_state"].(map[string]interface{})
	if !ok {
		return genesis, nil
	}

	stakingState, ok := appState["staking"].(map[string]interface{})
	if !ok {
		return genesis, nil
	}

	validators, ok := stakingState["validators"].([]interface{})
	if !ok {
		return genesis, nil
	}

	// Apply validator transformations
	migratedValidators := make([]interface{}, 0, len(validators))
	for _, validator := range validators {
		if validatorMap, ok := validator.(map[string]interface{}); ok {
			migratedValidator := h.migrateValidator(validatorMap, transform)
			migratedValidators = append(migratedValidators, migratedValidator)
		}
	}

	stakingState["validators"] = migratedValidators
	return genesis, nil
}

// migrateValidator migrates a single validator
func (h *GenesisHandler) migrateValidator(validator map[string]interface{}, transform *types.TypedMap) map[string]interface{} {
	// Apply validator-specific transformations
	if commissionRate, _ := transform.GetString("commission_rate"); commissionRate != "" {
		if commission, ok := validator["commission"].(map[string]interface{}); ok {
			if commissionRates, ok := commission["commission_rates"].(map[string]interface{}); ok {
				commissionRates["rate"] = commissionRate
			}
		}
	}

	// Update status if specified
	if status, _ := transform.GetString("status"); status != "" {
		validator["status"] = status
	}

	return validator
}

// updateChainMetadata updates chain metadata like genesis time
func (h *GenesisHandler) updateChainMetadata(genesis map[string]interface{}, ctx *migration.MigrationContext, step *migration.MigrationStep) error {
	// Update genesis time if not in dry run mode
	if !ctx.DryRun {
		if updateTime, err := step.Config.GetBool("update_genesis_time"); err == nil && updateTime {
			genesis["genesis_time"] = common.ConsensusNow().UTC().Format(time.RFC3339)
		}
	}

	// Update version info if provided
	if version, _ := step.Config.GetString("app_version"); version != "" {
		if appState, ok := genesis["app_state"].(map[string]interface{}); ok {
			appState["version"] = version
		}
	}

	return nil
}

// validateGenesis validates the migrated genesis
func (h *GenesisHandler) validateGenesis(genesis map[string]interface{}) error {
	// Check required fields
	requiredFields := []string{"chain_id", "genesis_time", "app_state"}
	for _, field := range requiredFields {
		if _, exists := genesis[field]; !exists {
			return fmt.Errorf("required field missing: %s", field)
		}
	}

	// Validate chain ID format
	chainID, ok := genesis["chain_id"].(string)
	if !ok || chainID == "" {
		return fmt.Errorf("invalid chain_id")
	}

	// Validate genesis time format
	genesisTime, ok := genesis["genesis_time"].(string)
	if !ok || genesisTime == "" {
		return fmt.Errorf("invalid genesis_time")
	}

	if _, err := time.Parse(time.RFC3339, genesisTime); err != nil {
		return fmt.Errorf("invalid genesis_time format: %w", err)
	}

	// Validate app state structure
	appState, ok := genesis["app_state"].(map[string]interface{})
	if !ok {
		return fmt.Errorf("invalid app_state")
	}

	// Check for required modules
	requiredModules := []string{"auth", "bank", "staking"}
	for _, module := range requiredModules {
		if _, exists := appState[module]; !exists {
			return fmt.Errorf("required module missing in app_state: %s", module)
		}
	}

	return nil
}
