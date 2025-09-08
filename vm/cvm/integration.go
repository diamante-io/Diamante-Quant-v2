package cvm

import (
	"fmt"

	"github.com/sirupsen/logrus"
)

// Integration provides the main entry point for CVM protocol integration
type Integration struct {
	protocol             *Protocol
	zkevmIntegration     *ZKEVMIntegration
	chaincodeIntegration *ChaincodeIntegration
	nativeIntegration    *NativeIntegration
	logger               *logrus.Logger
}

// NewIntegration creates a new CVM integration instance
func NewIntegration(logger *logrus.Logger) (*Integration, error) {
	// Create the core protocol
	protocol := NewProtocol(logger)

	// Create VM-specific integrations
	zkevmInt := NewZKEVMIntegration(protocol, logger)
	chaincodeInt := NewChaincodeIntegration(protocol, logger)
	nativeInt := NewNativeIntegration(protocol, logger)

	// Register VM executors
	if err := zkevmInt.RegisterWithProtocol(); err != nil {
		return nil, fmt.Errorf("failed to register zkEVM integration: %w", err)
	}

	if err := chaincodeInt.RegisterWithProtocol(); err != nil {
		return nil, fmt.Errorf("failed to register chaincode integration: %w", err)
	}

	if err := nativeInt.RegisterWithProtocol(); err != nil {
		return nil, fmt.Errorf("failed to register native integration: %w", err)
	}

	// Register asset validators
	zkevmValidator := &ZKEVMAssetValidator{integration: zkevmInt}
	chaincodeValidator := &ChaincodeAssetValidator{integration: chaincodeInt}
	nativeValidator := &NativeAssetValidator{integration: nativeInt}

	if err := protocol.assetBridge.RegisterValidator(VMTypeZKEVM, zkevmValidator); err != nil {
		return nil, fmt.Errorf("failed to register zkEVM validator: %w", err)
	}

	if err := protocol.assetBridge.RegisterValidator(VMTypeChaincode, chaincodeValidator); err != nil {
		return nil, fmt.Errorf("failed to register chaincode validator: %w", err)
	}

	if err := protocol.assetBridge.RegisterValidator(VMTypeNative, nativeValidator); err != nil {
		return nil, fmt.Errorf("failed to register native validator: %w", err)
	}

	integration := &Integration{
		protocol:             protocol,
		zkevmIntegration:     zkevmInt,
		chaincodeIntegration: chaincodeInt,
		nativeIntegration:    nativeInt,
		logger:               logger,
	}

	logger.Info("CVM Protocol integration initialized successfully")

	return integration, nil
}

// GetProtocol returns the CVM protocol instance
func (i *Integration) GetProtocol() *Protocol {
	return i.protocol
}

// GetZKEVMPrecompile returns the zkEVM precompile for CVM
func (i *Integration) GetZKEVMPrecompile() *CVMPrecompile {
	return &CVMPrecompile{integration: i.zkevmIntegration}
}

// GetChaincodeShimExtension returns the chaincode shim extension
func (i *Integration) GetChaincodeShimExtension(stubContext ChaincodeStubContext) *CVMShimExtension {
	return &CVMShimExtension{
		integration: i.chaincodeIntegration,
		stubContext: stubContext,
	}
}

// GetDNACVMModule returns the DNA CVM module
func (i *Integration) GetDNACVMModule(context *DNAExecutionContext) *DNACVMModule {
	return &DNACVMModule{
		integration: i.nativeIntegration,
		context:     context,
	}
}

// RegisterContract registers a contract in the CVM registry
func (i *Integration) RegisterContract(metadata *ContractMetadata) error {
	return i.protocol.registry.RegisterContract(metadata)
}

// GetMetrics returns comprehensive CVM metrics
func (i *Integration) GetMetrics() map[string]interface{} {
	protocolMetrics := i.protocol.GetMetrics()
	registryStats := i.protocol.registry.GetStats()
	txMetrics := i.protocol.atomicTxMgr.GetMetrics()
	bridgeMetrics := i.protocol.assetBridge.GetMetrics()
	gasMetrics := i.protocol.gasManager.GetMetrics()
	routingTable := i.protocol.router.GetRoutingTable()

	return map[string]interface{}{
		"protocol":     protocolMetrics,
		"registry":     registryStats,
		"transactions": txMetrics,
		"asset_bridge": bridgeMetrics,
		"gas":          gasMetrics,
		"routing":      routingTable,
	}
}

// Shutdown gracefully shuts down the CVM integration
func (i *Integration) Shutdown() error {
	i.logger.Info("Shutting down CVM Protocol integration")

	// Clean up any resources
	// In a real implementation, this would:
	// - Stop background workers
	// - Flush pending transactions
	// - Save state if needed

	return nil
}

// ZKEVMAssetValidator implements asset validation for zkEVM
type ZKEVMAssetValidator struct {
	integration *ZKEVMIntegration
}

func (v *ZKEVMAssetValidator) ValidateAsset(assetID AssetID, amount uint64) error {
	// In a real implementation, this would check ERC20/ERC721 contracts
	return nil
}

func (v *ZKEVMAssetValidator) GetAssetInfo(assetID AssetID) (AssetInfo, error) {
	return AssetInfo{
		ID:          assetID,
		Name:        "zkEVM Asset",
		Symbol:      "ZKA",
		Decimals:    18,
		TotalSupply: 1000000,
		OriginVM:    VMTypeZKEVM,
		IsWrapped:   false,
	}, nil
}

func (v *ZKEVMAssetValidator) VerifyOwnership(assetID AssetID, owner Address) error {
	// In a real implementation, check EVM state
	return nil
}
