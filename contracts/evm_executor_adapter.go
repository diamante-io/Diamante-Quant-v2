package contracts

import (
	"fmt"
	"math/big"

	"diamante/ledger"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/sirupsen/logrus"
)

// EVMExecutorAdapter adapts the ledger.EVMExecutor to implement the contracts.EVMExecutor interface
type EVMExecutorAdapter struct {
	evmExecutor *ledger.EVMExecutor
	logger      *logrus.Logger
}

// NewEVMExecutorAdapter creates a new adapter that wraps ledger.EVMExecutor
func NewEVMExecutorAdapter(evmExecutor *ledger.EVMExecutor) *EVMExecutorAdapter {
	return &EVMExecutorAdapter{
		evmExecutor: evmExecutor,
		logger:      logrus.New(),
	}
}

// DeployContract implements the EVMExecutor interface
func (e *EVMExecutorAdapter) DeployContract(caller ethcommon.Address, code []byte, value *big.Int, gasLimit uint64) (ethcommon.Address, []byte, uint64, error) {
	if e.evmExecutor == nil {
		return ethcommon.Address{}, nil, 0, fmt.Errorf("EVM executor not initialized")
	}

	// Convert caller address to bytes
	callerBytes := caller.Bytes()

	// If gas limit is 0, use a default value
	if gasLimit == 0 {
		gasLimit = 3000000 // Default gas limit for contract deployment
	}

	// Deploy the contract using the underlying EVM executor
	contractAddr, err := e.evmExecutor.DeployContract(callerBytes, code, gasLimit)
	if err != nil {
		return ethcommon.Address{}, nil, 0, fmt.Errorf("failed to deploy contract: %w", err)
	}

	// Convert the contract address bytes to ethcommon.Address
	contractAddress := ethcommon.BytesToAddress(contractAddr)

	// Get the deployed code (return data is empty for deployments)
	deployedCode, err := e.evmExecutor.GetCode(contractAddr)
	if err != nil {
		e.logger.WithError(err).Warn("Failed to get deployed contract code")
		deployedCode = []byte{}
	}

	// For now, we'll estimate gas used as 80% of gas limit (in production, get actual gas used)
	gasUsed := uint64(float64(gasLimit) * 0.8)

	e.logger.WithFields(logrus.Fields{
		"caller":       caller.Hex(),
		"contract":     contractAddress.Hex(),
		"codeSize":     len(code),
		"gasLimit":     gasLimit,
		"gasUsed":      gasUsed,
		"deployedSize": len(deployedCode),
	}).Info("Contract deployed successfully")

	return contractAddress, deployedCode, gasUsed, nil
}

// CallContract calls a contract method (helper method, not part of interface)
func (e *EVMExecutorAdapter) CallContract(caller ethcommon.Address, contractAddr ethcommon.Address, data []byte, gasLimit uint64) ([]byte, error) {
	if e.evmExecutor == nil {
		return nil, fmt.Errorf("EVM executor not initialized")
	}

	callerBytes := caller.Bytes()
	contractBytes := contractAddr.Bytes()

	// If gas limit is 0, use a default value
	if gasLimit == 0 {
		gasLimit = 1000000 // Default gas limit for contract calls
	}

	result, err := e.evmExecutor.CallContract(callerBytes, contractBytes, data, gasLimit)
	if err != nil {
		return nil, fmt.Errorf("failed to call contract: %w", err)
	}

	return result, nil
}

// GetCode retrieves the code at a given address
func (e *EVMExecutorAdapter) GetCode(addr ethcommon.Address) ([]byte, error) {
	if e.evmExecutor == nil {
		return nil, fmt.Errorf("EVM executor not initialized")
	}

	return e.evmExecutor.GetCode(addr.Bytes())
}

// GetBalance retrieves the balance of an address
func (e *EVMExecutorAdapter) GetBalance(addr ethcommon.Address) (*big.Int, error) {
	if e.evmExecutor == nil {
		return nil, fmt.Errorf("EVM executor not initialized")
	}

	return e.evmExecutor.GetBalance(addr.Bytes())
}

// GetNonce retrieves the nonce of an address
func (e *EVMExecutorAdapter) GetNonce(addr ethcommon.Address) (uint64, error) {
	if e.evmExecutor == nil {
		return 0, fmt.Errorf("EVM executor not initialized")
	}

	return e.evmExecutor.GetNonce(addr.Bytes())
}

// SetBlockHeight updates the block height in the EVM executor
func (e *EVMExecutorAdapter) SetBlockHeight(height uint64) {
	if e.evmExecutor != nil {
		e.evmExecutor.SetBlockHeight(height)
	}
}

// CreateEVMExecutorFromLedger creates an EVMExecutorAdapter from a ledger instance
func CreateEVMExecutorFromLedger(ledgerAPI interface{}, logger *logrus.Logger) EVMExecutor {
	// Check if the ledger provides an EVM executor
	if evmProvider, ok := ledgerAPI.(interface{ GetEVMExecutor() *ledger.EVMExecutor }); ok {
		evmExec := evmProvider.GetEVMExecutor()
		if evmExec != nil {
			return NewEVMExecutorAdapter(evmExec)
		}
	}

	// If not available directly, create a new EVM executor
	// Note: LMDB support disabled on Windows
	// if ledgerImpl, ok := ledgerAPI.(*ledger.LMDBLedger); ok {
	//	config := ledger.DefaultEVMConfig()
	//	evmExec := ledger.NewEVMExecutor(ledgerImpl, config, logger)
	//	return NewEVMExecutorAdapter(evmExec)
	// }

	// Return nil if we can't create an EVM executor
	return nil
}

// EncodeContractCall encodes a contract function call with parameters
func EncodeContractCall(functionSig string, params ...interface{}) ([]byte, error) {
	// This is a simplified version. In production, use go-ethereum's abi package
	// to properly encode function calls according to the contract ABI

	// For now, just return the function signature as bytes
	// This would need proper ABI encoding in production
	return []byte(functionSig), nil
}

// DecodeContractResult decodes the result of a contract call
func DecodeContractResult(data []byte, outputTypes ...interface{}) error {
	if len(data) == 0 {
		return fmt.Errorf("empty result data")
	}

	if len(outputTypes) == 0 {
		return fmt.Errorf("no output types specified")
	}

	// Basic decoding for common types
	// In production, this would use the full ABI decoder
	offset := 0
	for i, outputType := range outputTypes {
		if offset >= len(data) {
			return fmt.Errorf("insufficient data for output %d", i)
		}

		switch v := outputType.(type) {
		case *big.Int:
			if len(data[offset:]) < 32 {
				return fmt.Errorf("insufficient data for uint256 at output %d", i)
			}
			v.SetBytes(data[offset : offset+32])
			offset += 32

		case *ethcommon.Address:
			if len(data[offset:]) < 32 {
				return fmt.Errorf("insufficient data for address at output %d", i)
			}
			copy(v[:], data[offset+12:offset+32]) // addresses are 20 bytes, right-padded
			offset += 32

		case *bool:
			if len(data[offset:]) < 32 {
				return fmt.Errorf("insufficient data for bool at output %d", i)
			}
			*v = data[offset+31] != 0
			offset += 32

		case *[]byte:
			// For dynamic bytes, first 32 bytes is offset to data
			if len(data[offset:]) < 32 {
				return fmt.Errorf("insufficient data for bytes offset at output %d", i)
			}
			dataOffset := new(big.Int).SetBytes(data[offset : offset+32]).Uint64()
			if dataOffset >= uint64(len(data)) {
				return fmt.Errorf("invalid data offset for bytes at output %d", i)
			}

			// Next 32 bytes at dataOffset is the length
			if len(data[dataOffset:]) < 32 {
				return fmt.Errorf("insufficient data for bytes length at output %d", i)
			}
			length := new(big.Int).SetBytes(data[dataOffset : dataOffset+32]).Uint64()

			// Then the actual data
			if len(data[dataOffset+32:]) < int(length) {
				return fmt.Errorf("insufficient data for bytes content at output %d", i)
			}
			*v = make([]byte, length)
			copy(*v, data[dataOffset+32:dataOffset+32+length])
			offset += 32

		case *string:
			// Strings are encoded the same as dynamic bytes
			if len(data[offset:]) < 32 {
				return fmt.Errorf("insufficient data for string offset at output %d", i)
			}
			dataOffset := new(big.Int).SetBytes(data[offset : offset+32]).Uint64()
			if dataOffset >= uint64(len(data)) {
				return fmt.Errorf("invalid data offset for string at output %d", i)
			}

			if len(data[dataOffset:]) < 32 {
				return fmt.Errorf("insufficient data for string length at output %d", i)
			}
			length := new(big.Int).SetBytes(data[dataOffset : dataOffset+32]).Uint64()

			if len(data[dataOffset+32:]) < int(length) {
				return fmt.Errorf("insufficient data for string content at output %d", i)
			}
			*v = string(data[dataOffset+32 : dataOffset+32+length])
			offset += 32

		default:
			return fmt.Errorf("unsupported output type at index %d: %T", i, outputType)
		}
	}

	return nil
}

// EstimateGas estimates the gas required for a transaction
func (e *EVMExecutorAdapter) EstimateGas(caller ethcommon.Address, to *ethcommon.Address, data []byte, value *big.Int) (uint64, error) {
	// Simplified gas estimation
	baseGas := uint64(21000)          // Base transaction cost
	dataGas := uint64(len(data)) * 16 // Approximate gas per byte of data

	if to == nil {
		// Contract creation requires more gas
		return baseGas + dataGas + 32000, nil
	}

	// Contract call
	return baseGas + dataGas, nil
}
