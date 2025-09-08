package cvm

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
)

// ChaincodeIntegration implements CVM integration for Chaincode VM
type ChaincodeIntegration struct {
	cvmProtocol *Protocol
	logger      *logrus.Logger
}

// NewChaincodeIntegration creates a new Chaincode CVM integration
func NewChaincodeIntegration(protocol *Protocol, logger *logrus.Logger) *ChaincodeIntegration {
	return &ChaincodeIntegration{
		cvmProtocol: protocol,
		logger:      logger,
	}
}

// CVMShimExtension extends the Chaincode shim API with CVM functions
type CVMShimExtension struct {
	integration *ChaincodeIntegration
	stubContext ChaincodeStubContext // Reference to current chaincode context
}

// ChaincodeStubContext represents the chaincode execution context
type ChaincodeStubContext interface {
	GetTxID() string
	GetChannelID() string
	GetMSPID() string
	GetCreator() ([]byte, error)
}

// CrossVMInvoke performs a cross-VM invocation from chaincode
func (s *CVMShimExtension) CrossVMInvoke(targetVM string, targetAddr string, method string, args []byte) ([]byte, error) {
	// Parse target VM type
	var vmType VMType
	switch targetVM {
	case "zkEVM":
		vmType = VMTypeZKEVM
	case "Native":
		vmType = VMTypeNative
	default:
		return nil, fmt.Errorf("unknown target VM: %s", targetVM)
	}

	// Get sender information from chaincode context
	creator, err := s.stubContext.GetCreator()
	if err != nil {
		return nil, fmt.Errorf("failed to get creator: %w", err)
	}

	// Create CVM message
	msg := CVMMessage{
		SourceVM:   VMTypeChaincode,
		SourceAddr: Address{VM: VMTypeChaincode, Address: creator},
		TargetVM:   vmType,
		TargetAddr: Address{VM: vmType, Address: []byte(targetAddr)},
		Method:     method,
		Arguments:  args,
		GasLimit:   1000000, // Default gas limit
		Nonce:      s.getNonce(),
	}

	// Execute cross-VM call
	ctx := context.Background()
	response, err := s.integration.cvmProtocol.Call(ctx, msg)
	if err != nil {
		return nil, fmt.Errorf("cross-VM call failed: %w", err)
	}

	if !response.Success {
		return nil, fmt.Errorf("cross-VM call returned error: %s", response.Error)
	}

	return response.Result, nil
}

// CrossVMQuery performs a read-only cross-VM query
func (s *CVMShimExtension) CrossVMQuery(targetVM string, targetAddr string, method string, args []byte) ([]byte, error) {
	// Similar to CrossVMInvoke but read-only
	// This would not create state changes or consume gas

	result, err := s.CrossVMInvoke(targetVM, targetAddr, method, args)
	if err != nil {
		return nil, fmt.Errorf("cross-VM query failed: %w", err)
	}

	return result, nil
}

// LockAssetForTransfer locks an asset for cross-VM transfer
func (s *CVMShimExtension) LockAssetForTransfer(assetID string, amount uint64, targetVM string, targetAddr string) error {
	// Parse asset ID
	assetBytes, err := hex.DecodeString(assetID)
	if err != nil {
		return fmt.Errorf("invalid asset ID: %w", err)
	}

	var aid AssetID
	copy(aid[:], assetBytes)

	// Get transaction ID from context
	txID := s.stubContext.GetTxID()

	// Parse target VM
	var vmType VMType
	switch targetVM {
	case "zkEVM":
		vmType = VMTypeZKEVM
	case "Native":
		vmType = VMTypeNative
	default:
		return fmt.Errorf("unknown target VM: %s", targetVM)
	}

	// Get creator as source address
	creator, err := s.stubContext.GetCreator()
	if err != nil {
		return fmt.Errorf("failed to get creator: %w", err)
	}

	from := Address{VM: VMTypeChaincode, Address: creator}
	to := Address{VM: vmType, Address: []byte(targetAddr)}

	// Lock the asset
	return s.integration.cvmProtocol.assetBridge.LockAsset(aid, amount, from, to, txID)
}

// GetContractInfo retrieves contract information from the CVM registry
func (s *CVMShimExtension) GetContractInfo(addressOrAlias string) (*ContractMetadata, error) {
	// Try as alias first
	contract, err := s.integration.cvmProtocol.registry.GetContractByAlias(addressOrAlias)
	if err == nil {
		return contract, nil
	}

	// Try as address
	// For chaincode, addresses are typically channel/chaincode format
	addr := Address{
		VM:      VMTypeChaincode,
		Address: []byte(addressOrAlias),
	}

	return s.integration.cvmProtocol.registry.GetContract(addr)
}

// RegisterContract registers a chaincode in the CVM registry
func (s *CVMShimExtension) RegisterContract(name string, version string, abi []byte, permissions CrossVMPermissions) error {
	creator, err := s.stubContext.GetCreator()
	if err != nil {
		return fmt.Errorf("failed to get creator: %w", err)
	}

	metadata := &ContractMetadata{
		Address: Address{
			VM:      VMTypeChaincode,
			Address: []byte(fmt.Sprintf("%s/%s", s.stubContext.GetChannelID(), name)),
		},
		VM:          VMTypeChaincode,
		Name:        name,
		Version:     version,
		ABI:         json.RawMessage(abi),
		Permissions: permissions,
		Owner:       Address{VM: VMTypeChaincode, Address: creator},
	}

	return s.integration.cvmProtocol.registry.RegisterContract(metadata)
}

// EmitCrossVMEvent emits an event that can be observed by other VMs
func (s *CVMShimExtension) EmitCrossVMEvent(eventName string, payload []byte) error {
	// This would integrate with the event system
	// For now, just log it
	s.integration.logger.Infof("Cross-VM event emitted: %s", eventName)
	return nil
}

// Helper method to get nonce
func (s *CVMShimExtension) getNonce() uint64 {
	// In a real implementation, this would track nonces per account
	return uint64(time.Now().UnixNano())
}

// ChaincodeExecutor implements the VMExecutor interface for Chaincode
type ChaincodeExecutor struct {
	integration    *ChaincodeIntegration
	runtime        interface{}       // Reference to chaincode runtime
	stateSnapshots map[string][]byte // Simplified state snapshot storage
}

// Execute executes a CVM message in Chaincode VM
func (e *ChaincodeExecutor) Execute(ctx context.Context, msg CVMMessage) (CVMResponse, error) {
	response := CVMResponse{
		MessageID: msg.ID,
		Success:   true,
		GasUsed:   5000, // Base chaincode execution cost
	}

	// Parse the target chaincode address (channel/chaincode format)
	targetAddr := string(msg.TargetAddr.Address)

	// In a real implementation, this would:
	// 1. Route to the appropriate chaincode
	// 2. Check endorsement policies
	// 3. Execute the chaincode function
	// 4. Collect endorsements if needed
	// 5. Return the result

	e.integration.logger.Infof("Executed CVM message in Chaincode: %s targeting %s.%s",
		hex.EncodeToString(msg.ID[:]), targetAddr, msg.Method)

	// Simulate successful execution
	result := map[string]interface{}{
		"status": "success",
		"data":   "chaincode execution result",
	}

	resultBytes, err := json.Marshal(result)
	if err != nil {
		return response, err
	}

	response.Result = resultBytes
	return response, nil
}

// CreateCheckpoint creates a state checkpoint
func (e *ChaincodeExecutor) CreateCheckpoint() (string, error) {
	// In a real implementation, this would:
	// 1. Get current world state
	// 2. Create a snapshot of relevant keys
	// 3. Store it for potential rollback

	checkpointID := fmt.Sprintf("chaincode_checkpoint_%d", time.Now().UnixNano())

	// Simulate checkpoint creation
	if e.stateSnapshots == nil {
		e.stateSnapshots = make(map[string][]byte)
	}

	// Store current state (simplified)
	stateData := []byte("current_chaincode_state")
	e.stateSnapshots[checkpointID] = stateData

	return checkpointID, nil
}

// RestoreCheckpoint restores from a checkpoint
func (e *ChaincodeExecutor) RestoreCheckpoint(checkpointID string) error {
	// In a real implementation, this would restore the world state

	stateData, exists := e.stateSnapshots[checkpointID]
	if !exists {
		return fmt.Errorf("checkpoint not found: %s", checkpointID)
	}

	// Restore state (simplified)
	e.integration.logger.Infof("Restored chaincode checkpoint: %s with %d bytes",
		checkpointID, len(stateData))

	return nil
}

// GetCapabilities returns Chaincode VM capabilities
func (e *ChaincodeExecutor) GetCapabilities() []string {
	return []string{
		"endorsement_policies",
		"private_data_collections",
		"multi_org_consensus",
		"state_based_endorsement",
		"chaincode_lifecycle",
		"fabric_compatibility",
	}
}

// EstimateGas estimates gas for a CVM message
func (e *ChaincodeExecutor) EstimateGas(msg CVMMessage) (uint64, error) {
	// Base cost for chaincode execution
	baseCost := uint64(5000)

	// Add data cost
	dataCost := uint64(len(msg.Arguments) * 8) // Chaincode is more efficient

	// Add endorsement cost (if multiple orgs)
	endorsementCost := uint64(2000) // Per endorser

	return baseCost + dataCost + endorsementCost, nil
}

// RegisterWithProtocol registers the Chaincode integration with the CVM protocol
func (i *ChaincodeIntegration) RegisterWithProtocol() error {
	executor := &ChaincodeExecutor{
		integration:    i,
		stateSnapshots: make(map[string][]byte),
	}
	return i.cvmProtocol.RegisterExecutor(VMTypeChaincode, executor)
}

// ChaincodeAssetValidator implements asset validation for Chaincode
type ChaincodeAssetValidator struct {
	integration *ChaincodeIntegration
}

// ValidateAsset validates an asset in chaincode context
func (v *ChaincodeAssetValidator) ValidateAsset(assetID AssetID, amount uint64) error {
	// In a real implementation, this would check:
	// 1. Asset exists in chaincode state
	// 2. Amount is valid
	// 3. Asset is not locked

	return nil
}

// GetAssetInfo retrieves asset information
func (v *ChaincodeAssetValidator) GetAssetInfo(assetID AssetID) (AssetInfo, error) {
	// In a real implementation, query chaincode state

	return AssetInfo{
		ID:          assetID,
		Name:        "Chaincode Asset",
		Symbol:      "CCA",
		Decimals:    18,
		TotalSupply: 1000000,
		OriginVM:    VMTypeChaincode,
		IsWrapped:   false,
	}, nil
}

// VerifyOwnership verifies asset ownership
func (v *ChaincodeAssetValidator) VerifyOwnership(assetID AssetID, owner Address) error {
	// In a real implementation, check chaincode state for ownership
	return nil
}
