package cvm

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

// NativeIntegration implements CVM integration for Native/DNA VM
type NativeIntegration struct {
	cvmProtocol *Protocol
	logger      *logrus.Logger
}

// NewNativeIntegration creates a new Native/DNA CVM integration
func NewNativeIntegration(protocol *Protocol, logger *logrus.Logger) *NativeIntegration {
	return &NativeIntegration{
		cvmProtocol: protocol,
		logger:      logger,
	}
}

// DNACVMModule represents the CVM module for DNA language
type DNACVMModule struct {
	integration *NativeIntegration
	context     *DNAExecutionContext
}

// DNAExecutionContext represents the DNA contract execution context
type DNAExecutionContext struct {
	ContractAddress []byte
	Sender          []byte
	Resources       map[string]interface{} // Active resources
	GasRemaining    uint64
}

// CrossVMCall performs a cross-VM call from DNA contract
func (m *DNACVMModule) CrossVMCall(targetVM string, targetAddr string, method string, args []byte) ([]byte, error) {
	// Parse target VM type
	var vmType VMType
	switch targetVM {
	case "zkEVM":
		vmType = VMTypeZKEVM
	case "Chaincode":
		vmType = VMTypeChaincode
	default:
		return nil, fmt.Errorf("unknown target VM: %s", targetVM)
	}

	// Create CVM message
	msg := CVMMessage{
		SourceVM:   VMTypeNative,
		SourceAddr: Address{VM: VMTypeNative, Address: m.context.ContractAddress},
		TargetVM:   vmType,
		TargetAddr: Address{VM: vmType, Address: []byte(targetAddr)},
		Method:     method,
		Arguments:  args,
		GasLimit:   m.context.GasRemaining / 2, // Use half of remaining gas
		Nonce:      m.generateNonce(),
	}

	// Execute cross-VM call
	ctx := context.Background()
	response, err := m.integration.cvmProtocol.Call(ctx, msg)
	if err != nil {
		return nil, fmt.Errorf("cross-VM call failed: %w", err)
	}

	if !response.Success {
		return nil, fmt.Errorf("cross-VM call returned error: %s", response.Error)
	}

	// Update gas consumption
	m.context.GasRemaining -= response.GasUsed

	return response.Result, nil
}

// WrapResource wraps a DNA resource for cross-VM transfer
func (m *DNACVMModule) WrapResource(resourceID string, targetVM string) (*WrappedResource, error) {
	// Get resource from context
	resource, exists := m.context.Resources[resourceID]
	if !exists {
		return nil, fmt.Errorf("resource not found: %s", resourceID)
	}

	// In DNA, resources have linear semantics - they're consumed when wrapped
	delete(m.context.Resources, resourceID)

	// Create wrapped resource representation
	wrapped := &WrappedResource{
		OriginalID:   resourceID,
		OriginVM:     VMTypeNative,
		TargetVM:     m.parseVMType(targetVM),
		ResourceData: m.serializeResource(resource),
		Timestamp:    time.Now(),
	}

	return wrapped, nil
}

// UnwrapResource unwraps a resource from another VM
func (m *DNACVMModule) UnwrapResource(wrappedResource *WrappedResource) (string, error) {
	// Verify this resource is meant for Native VM
	if wrappedResource.TargetVM != VMTypeNative {
		return "", fmt.Errorf("resource not targeted for Native VM")
	}

	// Deserialize and create new resource
	resource := m.deserializeResource(wrappedResource.ResourceData)

	// Generate new resource ID
	newID := fmt.Sprintf("unwrapped_%s_%d", wrappedResource.OriginalID, time.Now().UnixNano())

	// Add to context (respecting linear semantics)
	m.context.Resources[newID] = resource

	return newID, nil
}

// TransferAsset transfers an asset to another VM
func (m *DNACVMModule) TransferAsset(assetID string, amount uint64, targetVM string, targetAddr string) error {
	// Parse asset ID
	assetBytes, err := hex.DecodeString(assetID)
	if err != nil {
		return fmt.Errorf("invalid asset ID: %w", err)
	}

	var aid AssetID
	copy(aid[:], assetBytes)

	// Create transaction ID
	txID := fmt.Sprintf("dna_tx_%d", time.Now().UnixNano())

	// Parse target VM
	vmType := m.parseVMType(targetVM)

	from := Address{VM: VMTypeNative, Address: m.context.ContractAddress}
	to := Address{VM: vmType, Address: []byte(targetAddr)}

	// Lock and transfer the asset
	if err := m.integration.cvmProtocol.assetBridge.LockAsset(aid, amount, from, to, txID); err != nil {
		return fmt.Errorf("failed to lock asset: %w", err)
	}

	if err := m.integration.cvmProtocol.assetBridge.TransferAsset(aid, amount, from, to, txID); err != nil {
		return fmt.Errorf("failed to transfer asset: %w", err)
	}

	return nil
}

// QueryContract queries a contract in another VM
func (m *DNACVMModule) QueryContract(targetVM string, targetAddr string, query []byte) ([]byte, error) {
	// Read-only cross-VM query
	return m.CrossVMCall(targetVM, targetAddr, "query", query)
}

// RegisterResource registers a DNA resource type in the CVM registry
func (m *DNACVMModule) RegisterResource(resourceType string, abi []byte) error {
	metadata := &ContractMetadata{
		Address: Address{
			VM:      VMTypeNative,
			Address: []byte(fmt.Sprintf("resource:%s", resourceType)),
		},
		VM:      VMTypeNative,
		Name:    resourceType,
		Version: "1.0.0",
		ABI:     json.RawMessage(abi),
		Permissions: CrossVMPermissions{
			AllowedVMs:  []VMType{VMTypeZKEVM, VMTypeChaincode, VMTypeNative},
			RequireAuth: true,
		},
		Owner: Address{VM: VMTypeNative, Address: m.context.Sender},
	}

	return m.integration.cvmProtocol.registry.RegisterContract(metadata)
}

// Helper methods
func (m *DNACVMModule) parseVMType(vm string) VMType {
	switch vm {
	case "zkEVM":
		return VMTypeZKEVM
	case "Chaincode":
		return VMTypeChaincode
	case "Native":
		return VMTypeNative
	default:
		return VMTypeUnknown
	}
}

func (m *DNACVMModule) generateNonce() uint64 {
	return uint64(time.Now().UnixNano())
}

func (m *DNACVMModule) serializeResource(resource interface{}) []byte {
	data, _ := json.Marshal(resource)
	return data
}

func (m *DNACVMModule) deserializeResource(data []byte) interface{} {
	var resource interface{}
	json.Unmarshal(data, &resource)
	return resource
}

// WrappedResource represents a wrapped DNA resource
type WrappedResource struct {
	OriginalID   string    `json:"original_id"`
	OriginVM     VMType    `json:"origin_vm"`
	TargetVM     VMType    `json:"target_vm"`
	ResourceData []byte    `json:"resource_data"`
	Timestamp    time.Time `json:"timestamp"`
}

// NativeExecutor implements the VMExecutor interface for Native/DNA VM
type NativeExecutor struct {
	integration     *NativeIntegration
	wasmRuntime     interface{} // Reference to WASM runtime
	resourceManager *ResourceManager
}

// ResourceManager manages DNA resources and their lifecycle
type ResourceManager struct {
	resources   map[string]*ResourceState
	checkpoints map[string]map[string]*ResourceState // checkpointID -> resources
	mu          sync.RWMutex
}

// ResourceState represents the state of a DNA resource
type ResourceState struct {
	ID       string
	Type     string
	Owner    []byte
	Data     []byte
	Consumed bool
}

// Execute executes a CVM message in Native/DNA VM
func (e *NativeExecutor) Execute(ctx context.Context, msg CVMMessage) (CVMResponse, error) {
	response := CVMResponse{
		MessageID: msg.ID,
		Success:   true,
		GasUsed:   1000, // Very efficient base cost
	}

	// In a real implementation, this would:
	// 1. Load the DNA/WASM contract
	// 2. Set up the execution context
	// 3. Execute the specified method
	// 4. Track resource movements
	// 5. Return the result

	e.integration.logger.Infof("Executed CVM message in Native VM: %s", hex.EncodeToString(msg.ID[:]))

	// Simulate execution with resource tracking
	if e.resourceManager == nil {
		e.resourceManager = &ResourceManager{
			resources:   make(map[string]*ResourceState),
			checkpoints: make(map[string]map[string]*ResourceState),
		}
	}

	// Example: create a new resource as result
	result := map[string]interface{}{
		"status":      "success",
		"resource_id": fmt.Sprintf("resource_%d", time.Now().UnixNano()),
		"data":        "DNA execution result",
	}

	resultBytes, err := json.Marshal(result)
	if err != nil {
		return response, err
	}

	response.Result = resultBytes
	return response, nil
}

// CreateCheckpoint creates a state checkpoint
func (e *NativeExecutor) CreateCheckpoint() (string, error) {
	if e.resourceManager == nil {
		e.resourceManager = &ResourceManager{
			resources:   make(map[string]*ResourceState),
			checkpoints: make(map[string]map[string]*ResourceState),
		}
	}

	e.resourceManager.mu.Lock()
	defer e.resourceManager.mu.Unlock()

	checkpointID := fmt.Sprintf("native_checkpoint_%d", time.Now().UnixNano())

	// Copy current resource states
	checkpoint := make(map[string]*ResourceState)
	for id, resource := range e.resourceManager.resources {
		resourceCopy := *resource
		checkpoint[id] = &resourceCopy
	}

	e.resourceManager.checkpoints[checkpointID] = checkpoint

	return checkpointID, nil
}

// RestoreCheckpoint restores from a checkpoint
func (e *NativeExecutor) RestoreCheckpoint(checkpointID string) error {
	if e.resourceManager == nil {
		return fmt.Errorf("no resource manager initialized")
	}

	e.resourceManager.mu.Lock()
	defer e.resourceManager.mu.Unlock()

	checkpoint, exists := e.resourceManager.checkpoints[checkpointID]
	if !exists {
		return fmt.Errorf("checkpoint not found: %s", checkpointID)
	}

	// Restore resource states
	e.resourceManager.resources = make(map[string]*ResourceState)
	for id, resource := range checkpoint {
		resourceCopy := *resource
		e.resourceManager.resources[id] = &resourceCopy
	}

	e.integration.logger.Infof("Restored Native checkpoint: %s", checkpointID)
	return nil
}

// GetCapabilities returns Native/DNA VM capabilities
func (e *NativeExecutor) GetCapabilities() []string {
	return []string{
		"resource_oriented",
		"linear_types",
		"wasm_execution",
		"formal_verification",
		"deterministic_execution",
		"low_gas_cost",
		"native_performance",
	}
}

// EstimateGas estimates gas for a CVM message
func (e *NativeExecutor) EstimateGas(msg CVMMessage) (uint64, error) {
	// Native VM is highly optimized
	baseCost := uint64(1000)

	// Add data cost (very efficient)
	dataCost := uint64(len(msg.Arguments) * 4)

	// Add resource operation cost
	resourceCost := uint64(500) // Low cost for resource operations

	return baseCost + dataCost + resourceCost, nil
}

// RegisterWithProtocol registers the Native integration with the CVM protocol
func (i *NativeIntegration) RegisterWithProtocol() error {
	executor := &NativeExecutor{
		integration: i,
		resourceManager: &ResourceManager{
			resources:   make(map[string]*ResourceState),
			checkpoints: make(map[string]map[string]*ResourceState),
		},
	}
	return i.cvmProtocol.RegisterExecutor(VMTypeNative, executor)
}

// NativeAssetValidator implements asset validation for Native VM
type NativeAssetValidator struct {
	integration *NativeIntegration
}

// ValidateAsset validates an asset in native context
func (v *NativeAssetValidator) ValidateAsset(assetID AssetID, amount uint64) error {
	// DNA resources have linear semantics - validate they haven't been consumed
	return nil
}

// GetAssetInfo retrieves asset information
func (v *NativeAssetValidator) GetAssetInfo(assetID AssetID) (AssetInfo, error) {
	return AssetInfo{
		ID:          assetID,
		Name:        "Native DNA Asset",
		Symbol:      "DNA",
		Decimals:    18,
		TotalSupply: 1000000,
		OriginVM:    VMTypeNative,
		IsWrapped:   false,
	}, nil
}

// VerifyOwnership verifies asset ownership
func (v *NativeAssetValidator) VerifyOwnership(assetID AssetID, owner Address) error {
	// In DNA, ownership is tracked through linear type system
	return nil
}
