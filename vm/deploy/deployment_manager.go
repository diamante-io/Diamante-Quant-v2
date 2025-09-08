// Package deploy provides contract deployment management across all runtime types
package deploy

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"diamante/common"
	"diamante/storage"
	"diamante/vm/runtime"

	"github.com/sirupsen/logrus"
)

var (
	// ErrDeploymentFailed is returned when deployment fails
	ErrDeploymentFailed = errors.New("deployment failed")

	// ErrInvalidContract is returned when contract is invalid
	ErrInvalidContract = errors.New("invalid contract")

	// ErrContractExists is returned when contract already exists
	ErrContractExists = errors.New("contract already exists")

	// ErrUnauthorized is returned when user is not authorized
	ErrUnauthorized = errors.New("unauthorized operation")

	// ErrVersionConflict is returned when there's a version conflict
	ErrVersionConflict = errors.New("version conflict")
)

// DeploymentManager manages contract deployments across all runtimes
type DeploymentManager struct {
	// Runtime manager for dispatching to appropriate runtime
	runtimeManager *runtime.RuntimeManager

	// Storage for contract metadata
	store storage.LedgerStore

	// Version tracker
	versionTracker VersionTracker

	// Deployment validator
	validator DeploymentValidator

	// Logger
	logger *logrus.Logger

	// Deployment history
	history DeploymentHistory

	// Mutex for thread safety
	mu sync.RWMutex
}

// NewDeploymentManager creates a new deployment manager
func NewDeploymentManager(
	runtimeManager *runtime.RuntimeManager,
	store storage.LedgerStore,
	logger *logrus.Logger,
) *DeploymentManager {
	if logger == nil {
		logger = logrus.New()
		logger.SetLevel(logrus.InfoLevel)
	}

	return &DeploymentManager{
		runtimeManager: runtimeManager,
		store:          store,
		versionTracker: NewVersionTracker(store, logger),
		validator:      NewDefaultDeploymentValidator(),
		logger:         logger,
		history:        NewDeploymentHistory(store, logger),
	}
}

// DeployContract deploys a new contract
func (dm *DeploymentManager) DeployContract(
	ctx context.Context,
	req DeploymentRequest,
) (*DeploymentResponse, error) {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	// Validate deployment request
	if err := dm.validator.ValidateDeployment(req); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidContract, err)
	}

	// Check if contract already exists
	if req.ContractID != "" {
		exists, err := dm.contractExists(req.ContractID)
		if err != nil {
			return nil, fmt.Errorf("failed to check contract existence: %w", err)
		}
		if exists {
			return nil, ErrContractExists
		}
	}

	// Generate contract ID if not provided
	if req.ContractID == "" {
		req.ContractID = dm.generateContractID(req)
	}

	// Create deployment context
	deployCtx := &DeploymentContext{
		Request:   req,
		StartTime: common.ConsensusNow(),
		Status:    DeploymentStatusPending,
	}

	// Record deployment attempt
	if err := dm.history.RecordDeploymentAttempt(deployCtx); err != nil {
		dm.logger.WithError(err).Error("Failed to record deployment attempt")
	}

	// Deploy through runtime manager
	deploymentArgs := runtime.DeploymentArgs{
		Deployer:        req.Deployer,
		ConstructorArgs: dm.convertConstructorArgs(req.ConstructorArgs),
		Value:           req.InitialValue,
		GasLimit:        req.GasLimit,
		Options:         dm.convertDeploymentOptions(req.Options),
	}

	// Convert metadata to RuntimeMetadata
	runtimeMetadata := runtime.RuntimeMetadata{
		Name:         req.Metadata.Name,
		Description:  req.Metadata.Description,
		Version:      req.Metadata.Version,
		Author:       req.Metadata.Author,
		License:      req.Metadata.License,
		Repository:   req.Metadata.Repository,
		Capabilities: []runtime.RuntimeCapability{},
		CreatedAt:    common.ConsensusNow(),
		UpdatedAt:    common.ConsensusNow(),
	}

	// Convert RuntimeMetadata to map for DeployContract
	metadataMap := map[string]interface{}{
		"name":         runtimeMetadata.Name,
		"description":  runtimeMetadata.Description,
		"version":      runtimeMetadata.Version,
		"author":       runtimeMetadata.Author,
		"license":      runtimeMetadata.License,
		"repository":   runtimeMetadata.Repository,
		"capabilities": runtimeMetadata.Capabilities,
		"createdAt":    runtimeMetadata.CreatedAt,
		"updatedAt":    runtimeMetadata.UpdatedAt,
	}

	result, err := dm.runtimeManager.DeployContract(
		ctx,
		req.Language,
		req.Code,
		deploymentArgs,
		metadataMap,
	)
	if err != nil {
		deployCtx.Status = DeploymentStatusFailed
		deployCtx.Error = err.Error()
		dm.history.UpdateDeploymentStatus(deployCtx)
		return nil, fmt.Errorf("%w: %v", ErrDeploymentFailed, err)
	}

	// Update deployment context
	deployCtx.Status = DeploymentStatusSuccess
	deployCtx.EndTime = common.ConsensusNow()
	deployCtx.GasUsed = result.GasUsed

	// Create initial version
	version := &ContractVersion{
		ContractID:     req.ContractID,
		Version:        "1.0.0",
		Code:           req.Code,
		CodeHash:       dm.hashCode(req.Code),
		Metadata:       req.Metadata,
		DeployedBy:     req.Deployer,
		DeployedAt:     common.ConsensusNow(),
		Active:         true,
		RuntimeType:    dm.getRuntimeType(req.Language),
		DeploymentHash: result.TransactionHash,
	}

	// Store version
	if err := dm.versionTracker.AddVersion(version); err != nil {
		dm.logger.WithError(err).Error("Failed to store contract version")
	}

	// Update deployment history
	dm.history.UpdateDeploymentStatus(deployCtx)

	// Create response
	response := &DeploymentResponse{
		ContractID:      req.ContractID,
		TransactionHash: result.TransactionHash,
		Version:         version.Version,
		GasUsed:         result.GasUsed,
		DeployedAt:      common.ConsensusNow(),
		Events:          dm.convertEvents(result.Events),
		RuntimeType:     dm.getRuntimeType(req.Language),
	}

	dm.logger.WithFields(logrus.Fields{
		"contractID": response.ContractID,
		"version":    response.Version,
		"gasUsed":    response.GasUsed,
	}).Info("Contract deployed successfully")

	return response, nil
}

// UpgradeContract upgrades an existing contract
func (dm *DeploymentManager) UpgradeContract(
	ctx context.Context,
	req UpgradeRequest,
) (*UpgradeResponse, error) {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	// Validate upgrade request
	if err := dm.validator.ValidateUpgrade(req); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidContract, err)
	}

	// Check authorization
	if err := dm.checkUpgradeAuthorization(req.ContractID, req.Authorizer); err != nil {
		return nil, err
	}

	// Get current version
	currentVersion, err := dm.versionTracker.GetCurrentVersion(req.ContractID)
	if err != nil {
		return nil, fmt.Errorf("failed to get current version: %w", err)
	}

	// Check version compatibility
	if err := dm.checkVersionCompatibility(currentVersion.Version, req.NewVersion); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrVersionConflict, err)
	}

	// Create upgrade context
	upgradeCtx := &UpgradeContext{
		Request:   req,
		StartTime: common.ConsensusNow(),
		Status:    UpgradeStatusPending,
	}

	// Record upgrade attempt
	if err := dm.history.RecordUpgradeAttempt(upgradeCtx); err != nil {
		dm.logger.WithError(err).Error("Failed to record upgrade attempt")
	}

	// Perform upgrade through runtime manager
	upgradeArgs := runtime.UpgradeArgs{
		Authorizer:    req.Authorizer,
		Version:       req.NewVersion,
		MigrationData: req.MigrationData,
		Options:       dm.convertDeploymentOptions(req.Options),
	}

	err = dm.runtimeManager.UpgradeContract(
		ctx,
		req.ContractID,
		req.NewCode,
		upgradeArgs,
	)
	if err != nil {
		upgradeCtx.Status = UpgradeStatusFailed
		upgradeCtx.Error = err.Error()
		dm.history.UpdateUpgradeStatus(upgradeCtx)
		return nil, fmt.Errorf("upgrade failed: %w", err)
	}

	// Update upgrade context
	upgradeCtx.Status = UpgradeStatusSuccess
	upgradeCtx.EndTime = common.ConsensusNow()

	// Create new version
	newVersion := &ContractVersion{
		Version:         req.NewVersion,
		ContractID:      req.ContractID,
		Code:            req.NewCode,
		CodeHash:        dm.hashCode(req.NewCode),
		DeployedAt:      common.ConsensusNow(),
		DeployedBy:      req.Authorizer,
		RuntimeType:     currentVersion.RuntimeType,
		Metadata:        req.Metadata,
		Active:          true,
		PreviousVersion: currentVersion.Version,
	}

	// Track new version
	if err := dm.versionTracker.AddVersion(newVersion); err != nil {
		return nil, fmt.Errorf("failed to track new version: %w", err)
	}

	// Deactivate old version
	if err := dm.versionTracker.DeactivateVersion(req.ContractID, currentVersion.Version); err != nil {
		dm.logger.WithError(err).Error("Failed to deactivate old version")
	}

	// Update upgrade context
	upgradeCtx.Status = UpgradeStatusSuccess
	upgradeCtx.EndTime = common.ConsensusNow()
	upgradeCtx.NewVersion = req.NewVersion

	// Record successful upgrade
	if err := dm.history.UpdateUpgradeStatus(upgradeCtx); err != nil {
		dm.logger.WithError(err).Error("Failed to update upgrade status")
	}

	// Create response
	response := &UpgradeResponse{
		ContractID:      req.ContractID,
		PreviousVersion: currentVersion.Version,
		NewVersion:      req.NewVersion,
		UpgradedAt:      newVersion.DeployedAt,
		Success:         true,
	}

	dm.logger.WithFields(logrus.Fields{
		"contractID":      response.ContractID,
		"previousVersion": response.PreviousVersion,
		"newVersion":      response.NewVersion,
	}).Info("Contract upgraded successfully")

	return response, nil
}

// GetContractVersions retrieves all versions of a contract
func (dm *DeploymentManager) GetContractVersions(contractID string) ([]*ContractVersion, error) {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	return dm.versionTracker.GetVersionHistory(contractID)
}

// RollbackContract rolls back a contract to a previous version
func (dm *DeploymentManager) RollbackContract(
	ctx context.Context,
	contractID string,
	targetVersion string,
	authorizer string,
) error {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	// Check authorization
	if err := dm.checkUpgradeAuthorization(contractID, authorizer); err != nil {
		return err
	}

	// Get target version
	targetVersionInfo, err := dm.versionTracker.GetVersion(contractID, targetVersion)
	if err != nil {
		return fmt.Errorf("failed to get target version: %w", err)
	}

	// Create rollback request
	rollbackReq := UpgradeRequest{
		ContractID:    contractID,
		NewVersion:    fmt.Sprintf("%s-rollback-%d", targetVersion, common.ConsensusNow().UnixNano()),
		NewCode:       targetVersionInfo.Code,
		Authorizer:    authorizer,
		MigrationData: []byte("rollback"),
		Options: DeploymentOptions{
			EnableOptimization: false,
			DebugMode:          false,
			Environment:        "rollback",
			CompilerVersion:    "", // Empty for rollback
		},
		Metadata: DeploymentMetadata{
			Name:        targetVersionInfo.Metadata.Name,
			Version:     targetVersion,
			Description: fmt.Sprintf("Rollback to version %s", targetVersion),
			Author:      authorizer,
		},
	}

	// Perform rollback as an upgrade
	_, err = dm.UpgradeContract(ctx, rollbackReq)
	if err != nil {
		return fmt.Errorf("rollback failed: %w", err)
	}

	return nil
}

// GetDeploymentHistory retrieves deployment history for a contract
func (dm *DeploymentManager) GetDeploymentHistory(contractID string) ([]*DeploymentContext, error) {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	return dm.history.GetContractHistory(contractID)
}

// SetDeploymentValidator sets a custom deployment validator
func (dm *DeploymentManager) SetDeploymentValidator(validator DeploymentValidator) {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	dm.validator = validator
}

// Helper methods

func (dm *DeploymentManager) contractExists(contractID string) (bool, error) {
	_, err := dm.runtimeManager.GetContractInfo(contractID)
	if err != nil {
		if errors.Is(err, runtime.ErrContractNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// generateContractID generates a unique contract ID
func (dm *DeploymentManager) generateContractID(req DeploymentRequest) string {
	// Use metadata name if available, otherwise use deployer address
	contractName := req.Metadata.Name
	if contractName == "" {
		contractName = req.Deployer
	}

	data := fmt.Sprintf("%s:%s:%s:%d",
		req.Deployer,
		req.Language,
		contractName,
		common.ConsensusNow().UnixNano(),
	)
	hash := sha256.Sum256([]byte(data))
	return hex.EncodeToString(hash[:])
}

func (dm *DeploymentManager) hashCode(code []byte) string {
	hash := sha256.Sum256(code)
	return hex.EncodeToString(hash[:])
}

func (dm *DeploymentManager) getRuntimeType(language string) runtime.RuntimeType {
	// Convert language to runtime type
	switch language {
	case "solidity", "vyper", "EVM", "evm":
		return runtime.RuntimeTypeEVM
	case "go", "node", "chaincode", "fabric":
		return runtime.RuntimeTypeChaincode
	case "native", "diamante":
		return runtime.RuntimeTypeNative
	default:
		// Default to EVM if unknown
		return runtime.RuntimeTypeEVM
	}
}

func (dm *DeploymentManager) checkUpgradeAuthorization(contractID, authorizer string) error {
	info, err := dm.runtimeManager.GetContractInfo(contractID)
	if err != nil {
		return fmt.Errorf("failed to get contract info: %w", err)
	}

	if info.Owner != authorizer {
		return fmt.Errorf("%w: not contract owner", ErrUnauthorized)
	}

	return nil
}

func (dm *DeploymentManager) checkVersionCompatibility(currentVersion, newVersion string) error {
	// Simple version check - can be enhanced with semantic versioning
	if currentVersion >= newVersion {
		return fmt.Errorf("new version must be greater than current version")
	}
	return nil
}

// convertConstructorArgs converts deployment constructor arguments to runtime parameters
func (dm *DeploymentManager) convertConstructorArgs(args []ConstructorArgument) runtime.ContractParameters {
	params := runtime.ContractParameters{
		StringParams:  make(map[string]string),
		IntParams:     make(map[string]int64),
		BoolParams:    make(map[string]bool),
		AddressParams: make(map[string]string),
		BytesParams:   make(map[string][]byte),
	}

	for _, arg := range args {
		switch arg.Type {
		case "string":
			params.StringParams[arg.Name] = arg.Value
		case "int", "uint":
			// Parse int value - for simplicity, assuming valid input
			params.StringParams[arg.Name] = arg.Value // Store as string for now
		case "bool":
			params.BoolParams[arg.Name] = arg.Value == "true"
		case "address":
			params.AddressParams[arg.Name] = arg.Value
		case "bytes":
			params.BytesParams[arg.Name] = []byte(arg.Value)
		}
	}

	return params
}

// convertDeploymentOptions converts deployment options to runtime deployment options
func (dm *DeploymentManager) convertDeploymentOptions(opts DeploymentOptions) runtime.DeploymentOptions {
	return runtime.DeploymentOptions{
		EnvironmentVars: map[string]string{
			"ENVIRONMENT":      opts.Environment,
			"DEBUG_MODE":       fmt.Sprintf("%v", opts.DebugMode),
			"OPTIMIZATION":     fmt.Sprintf("%v", opts.EnableOptimization),
			"COMPILER_VERSION": opts.CompilerVersion,
		},
		ResourceLimits: runtime.ResourceLimits{
			MaxMemoryMB:      1024, // Default
			MaxCPUPercent:    50.0, // Default
			MaxStorageMB:     100,  // Default
			MaxNetworkKbps:   1000, // Default
			ExecutionTimeout: time.Duration(opts.Timeout) * time.Second,
		},
		SecurityPolicy: runtime.SecurityPolicy{
			AllowNetworkAccess: opts.Environment == "production",
			AllowFileAccess:    false,
			RequireSignature:   opts.Environment == "production",
		},
		NetworkPolicy: runtime.NetworkPolicy{
			EgressRules: []runtime.NetworkRule{},
		},
	}
}

func (dm *DeploymentManager) convertEvents(events []runtime.ContractEvent) []DeploymentEvent {
	result := make([]DeploymentEvent, len(events))
	for i, event := range events {
		// Convert contract parameters to event parameters
		var params []EventParameter
		for name, value := range event.Parameters.StringParams {
			params = append(params, EventParameter{
				Name:  name,
				Type:  "string",
				Value: value,
			})
		}
		for name, value := range event.Parameters.IntParams {
			params = append(params, EventParameter{
				Name:  name,
				Type:  "int",
				Value: fmt.Sprintf("%d", value),
			})
		}
		for name, value := range event.Parameters.BoolParams {
			params = append(params, EventParameter{
				Name:  name,
				Type:  "bool",
				Value: fmt.Sprintf("%v", value),
			})
		}

		result[i] = DeploymentEvent{
			Name:       event.Name,
			Parameters: params,
			Data:       event.Data,
		}
	}
	return result
}
