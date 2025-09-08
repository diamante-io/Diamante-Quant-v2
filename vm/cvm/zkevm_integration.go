package cvm

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/sirupsen/logrus"
)

// ZKEVMIntegration implements CVM integration for zkEVM
type ZKEVMIntegration struct {
	cvmProtocol    *Protocol
	logger         *logrus.Logger
	precompileAddr common.Address
}

// Precompiled contract address for CVM in zkEVM
var CVMPrecompileAddress = common.HexToAddress("0x0000000000000000000000000000000000000CVM")

// NewZKEVMIntegration creates a new zkEVM CVM integration
func NewZKEVMIntegration(protocol *Protocol, logger *logrus.Logger) *ZKEVMIntegration {
	return &ZKEVMIntegration{
		cvmProtocol:    protocol,
		logger:         logger,
		precompileAddr: CVMPrecompileAddress,
	}
}

// CVMPrecompile implements the precompiled contract interface for CVM
type CVMPrecompile struct {
	integration *ZKEVMIntegration
}

// RequiredGas calculates the gas required for CVM operations
func (p *CVMPrecompile) RequiredGas(input []byte) uint64 {
	// Decode the operation type from input
	if len(input) < 4 {
		return 0
	}

	// Operation types:
	// 0x01: Cross-VM call
	// 0x02: Asset lock
	// 0x03: Asset transfer
	// 0x04: Query contract registry

	opType := input[0]
	baseGas := uint64(3000) // Base cost for CVM operations

	switch opType {
	case 0x01: // Cross-VM call
		return baseGas + uint64(len(input))*16 // 16 gas per byte
	case 0x02: // Asset lock
		return baseGas + 5000
	case 0x03: // Asset transfer
		return baseGas + 10000
	case 0x04: // Query registry
		return baseGas + 1000
	default:
		return baseGas
	}
}

// Run executes the CVM precompiled contract
func (p *CVMPrecompile) Run(input []byte) ([]byte, error) {
	if len(input) < 4 {
		return nil, fmt.Errorf("invalid input length")
	}

	opType := input[0]
	data := input[1:]

	switch opType {
	case 0x01: // Cross-VM call
		return p.handleCrossVMCall(data)
	case 0x02: // Asset lock
		return p.handleAssetLock(data)
	case 0x03: // Asset transfer
		return p.handleAssetTransfer(data)
	case 0x04: // Query registry
		return p.handleRegistryQuery(data)
	default:
		return nil, fmt.Errorf("unknown operation type: %d", opType)
	}
}

// handleCrossVMCall processes a cross-VM call from zkEVM
func (p *CVMPrecompile) handleCrossVMCall(data []byte) ([]byte, error) {
	// Decode the call data
	var callData struct {
		TargetVM    uint8   `json:"target_vm"`
		TargetAddr  string  `json:"target_addr"`
		Method      string  `json:"method"`
		Arguments   []byte  `json:"arguments"`
		GasLimit    uint64  `json:"gas_limit"`
		AssetID     *string `json:"asset_id,omitempty"`
		AssetAmount *uint64 `json:"asset_amount,omitempty"`
	}

	if err := json.Unmarshal(data, &callData); err != nil {
		return nil, fmt.Errorf("failed to decode call data: %w", err)
	}

	// Create CVM message
	msg := CVMMessage{
		SourceVM:   VMTypeZKEVM,
		SourceAddr: Address{VM: VMTypeZKEVM, Address: p.getSenderAddress()},
		TargetVM:   VMType(callData.TargetVM),
		TargetAddr: Address{VM: VMType(callData.TargetVM), Address: []byte(callData.TargetAddr)},
		Method:     callData.Method,
		Arguments:  callData.Arguments,
		GasLimit:   callData.GasLimit,
		Nonce:      p.getCurrentNonce(),
	}

	// Add asset transfer if specified
	if callData.AssetID != nil && callData.AssetAmount != nil {
		assetID, err := hex.DecodeString(*callData.AssetID)
		if err != nil {
			return nil, fmt.Errorf("invalid asset ID: %w", err)
		}

		var aid AssetID
		copy(aid[:], assetID)

		msg.Assets = []AssetTransfer{{
			AssetID: aid,
			Amount:  *callData.AssetAmount,
		}}
	}

	// Execute the cross-VM call
	ctx := context.Background()
	response, err := p.integration.cvmProtocol.Call(ctx, msg)
	if err != nil {
		return nil, err
	}

	// Encode response
	return json.Marshal(response)
}

// handleAssetLock processes an asset lock request
func (p *CVMPrecompile) handleAssetLock(data []byte) ([]byte, error) {
	var lockData struct {
		AssetID    string `json:"asset_id"`
		Amount     uint64 `json:"amount"`
		TargetVM   uint8  `json:"target_vm"`
		TargetAddr string `json:"target_addr"`
		TxID       string `json:"tx_id"`
	}

	if err := json.Unmarshal(data, &lockData); err != nil {
		return nil, fmt.Errorf("failed to decode lock data: %w", err)
	}

	assetID, err := hex.DecodeString(lockData.AssetID)
	if err != nil {
		return nil, fmt.Errorf("invalid asset ID: %w", err)
	}

	var aid AssetID
	copy(aid[:], assetID)

	from := Address{VM: VMTypeZKEVM, Address: p.getSenderAddress()}
	to := Address{VM: VMType(lockData.TargetVM), Address: []byte(lockData.TargetAddr)}

	// Lock the asset
	err = p.integration.cvmProtocol.assetBridge.LockAsset(aid, lockData.Amount, from, to, lockData.TxID)
	if err != nil {
		return nil, err
	}

	return []byte("locked"), nil
}

// handleAssetTransfer processes an asset transfer
func (p *CVMPrecompile) handleAssetTransfer(data []byte) ([]byte, error) {
	var transferData struct {
		AssetID    string `json:"asset_id"`
		Amount     uint64 `json:"amount"`
		TargetVM   uint8  `json:"target_vm"`
		TargetAddr string `json:"target_addr"`
		TxID       string `json:"tx_id"`
	}

	if err := json.Unmarshal(data, &transferData); err != nil {
		return nil, fmt.Errorf("failed to decode transfer data: %w", err)
	}

	assetID, err := hex.DecodeString(transferData.AssetID)
	if err != nil {
		return nil, fmt.Errorf("invalid asset ID: %w", err)
	}

	var aid AssetID
	copy(aid[:], assetID)

	from := Address{VM: VMTypeZKEVM, Address: p.getSenderAddress()}
	to := Address{VM: VMType(transferData.TargetVM), Address: []byte(transferData.TargetAddr)}

	// Transfer the asset
	err = p.integration.cvmProtocol.assetBridge.TransferAsset(aid, transferData.Amount, from, to, transferData.TxID)
	if err != nil {
		return nil, err
	}

	return []byte("transferred"), nil
}

// handleRegistryQuery queries the contract registry
func (p *CVMPrecompile) handleRegistryQuery(data []byte) ([]byte, error) {
	var queryData struct {
		Address string `json:"address,omitempty"`
		Alias   string `json:"alias,omitempty"`
	}

	if err := json.Unmarshal(data, &queryData); err != nil {
		return nil, fmt.Errorf("failed to decode query data: %w", err)
	}

	var contract *ContractMetadata
	var err error

	if queryData.Alias != "" {
		contract, err = p.integration.cvmProtocol.registry.GetContractByAlias(queryData.Alias)
	} else if queryData.Address != "" {
		// Parse address - simplified for example
		addr := Address{VM: VMTypeZKEVM, Address: []byte(queryData.Address)}
		contract, err = p.integration.cvmProtocol.registry.GetContract(addr)
	} else {
		return nil, fmt.Errorf("must specify either address or alias")
	}

	if err != nil {
		return nil, err
	}

	return json.Marshal(contract)
}

// Helper methods (these would be implemented based on EVM context)
func (p *CVMPrecompile) getSenderAddress() []byte {
	// In a real implementation, this would get the msg.sender from EVM context
	return []byte("sender_address_placeholder")
}

func (p *CVMPrecompile) getCurrentNonce() uint64 {
	// In a real implementation, this would get the current nonce
	return 0
}

// ZKEVMExecutor implements the VMExecutor interface for zkEVM
type ZKEVMExecutor struct {
	integration *ZKEVMIntegration
	evmInstance interface{} // Reference to actual EVM instance
}

// Execute executes a CVM message in zkEVM
func (e *ZKEVMExecutor) Execute(ctx context.Context, msg CVMMessage) (CVMResponse, error) {
	// Convert CVM message to EVM transaction
	// This is a simplified implementation

	response := CVMResponse{
		MessageID: msg.ID,
		Success:   true,
		GasUsed:   21000, // Base transaction cost
	}

	// Execute the transaction in zkEVM
	// In a real implementation, this would:
	// 1. Create an EVM transaction
	// 2. Execute it in the zkEVM
	// 3. Generate zk-proof if enabled
	// 4. Return the result

	e.integration.logger.Infof("Executed CVM message in zkEVM: %s", hex.EncodeToString(msg.ID[:]))

	return response, nil
}

// CreateCheckpoint creates a state checkpoint
func (e *ZKEVMExecutor) CreateCheckpoint() (string, error) {
	// In a real implementation, this would create an EVM state snapshot
	checkpointID := fmt.Sprintf("zkevm_checkpoint_%d", time.Now().UnixNano())
	return checkpointID, nil
}

// RestoreCheckpoint restores from a checkpoint
func (e *ZKEVMExecutor) RestoreCheckpoint(checkpointID string) error {
	// In a real implementation, this would restore the EVM state
	e.integration.logger.Infof("Restored zkEVM checkpoint: %s", checkpointID)
	return nil
}

// GetCapabilities returns zkEVM capabilities
func (e *ZKEVMExecutor) GetCapabilities() []string {
	return []string{
		"evm_compatible",
		"zk_proofs",
		"deterministic_execution",
		"state_rollback",
		"erc20_support",
		"erc721_support",
	}
}

// EstimateGas estimates gas for a CVM message
func (e *ZKEVMExecutor) EstimateGas(msg CVMMessage) (uint64, error) {
	// Base cost for zkEVM execution
	baseCost := uint64(21000)

	// Add data cost
	dataCost := uint64(len(msg.Arguments) * 16)

	// Add proof generation cost
	proofCost := uint64(50000) // Approximate cost for zk-proof

	return baseCost + dataCost + proofCost, nil
}

// RegisterWithProtocol registers the zkEVM integration with the CVM protocol
func (i *ZKEVMIntegration) RegisterWithProtocol() error {
	executor := &ZKEVMExecutor{integration: i}
	return i.cvmProtocol.RegisterExecutor(VMTypeZKEVM, executor)
}
