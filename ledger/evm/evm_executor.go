// ledger/evm/evm_executor.go

package evm

import (
	"errors"
	"math/big"

	"diamante/common"
	"diamante/storage"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/types"
	gethvm "github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/params"
	"github.com/holiman/uint256"
	"github.com/sirupsen/logrus"
)

// ErrGasUintOverflow is returned when a gas value overflows uint64
var ErrGasUintOverflow = errors.New("gas uint64 overflow")

// contractRef implements vm.ContractRef interface
type contractRef struct {
	address ethcommon.Address
}

func (c *contractRef) Address() ethcommon.Address {
	return c.address
}

// Message implements the go-ethereum vm.Message interface. Only a subset of
// fields required by Diamante is stored. Fee-related values are always zero.
type Message struct {
	to       *ethcommon.Address
	from     ethcommon.Address
	nonce    uint64
	value    *big.Int
	gasLimit uint64
	data     []byte
}

func (m *Message) From() ethcommon.Address      { return m.from }
func (m *Message) To() *ethcommon.Address       { return m.to }
func (m *Message) Gas() uint64                  { return m.gasLimit }
func (m *Message) GasPrice() *big.Int           { return big.NewInt(0) }
func (m *Message) GasFeeCap() *big.Int          { return big.NewInt(0) }
func (m *Message) GasTipCap() *big.Int          { return big.NewInt(0) }
func (m *Message) Value() *big.Int              { return m.value }
func (m *Message) Nonce() uint64                { return m.nonce }
func (m *Message) Data() []byte                 { return m.data }
func (m *Message) AccessList() types.AccessList { return nil }
func (m *Message) BlobHashes() []ethcommon.Hash { return nil }
func (m *Message) BlobGas() uint64              { return 0 }
func (m *Message) SkipAccountChecks() bool      { return false }
func (m *Message) IsFake() bool                 { return false }

// EVMExecutor executes EVM bytecode using the go-ethereum EVM
type EVMExecutor struct {
	stateDB      *StateDB
	eventManager *EventManager
	logger       *logrus.Logger
}

// NewEVMExecutor creates a new EVMExecutor
func NewEVMExecutor(ledger common.LedgerAPI, stateStore storage.LedgerStore, blockHeight uint64, logger *logrus.Logger) *EVMExecutor {
	if logger == nil {
		logger = logrus.New()
		logger.SetLevel(logrus.InfoLevel)
	}

	stateDB := NewStateDB(ledger, stateStore, blockHeight, logger)
	eventManager := NewEventManager()

	return &EVMExecutor{
		stateDB:      stateDB,
		eventManager: eventManager,
		logger:       logger,
	}
}

// ExecuteContract executes a contract call
func (e *EVMExecutor) ExecuteContract(caller, contract ethcommon.Address, input []byte, value *big.Int, gasLimit uint64) ([]byte, uint64, error) {
	// Convert big.Int to uint256.Int
	valueUint256, overflow := uint256.FromBig(value)
	if overflow {
		return nil, 0, ErrGasUintOverflow
	}

	// Check if the contract is a precompiled contract
	if precompiled, ok := PrecompiledContractsDiamante[contract]; ok {
		e.logger.WithFields(logrus.Fields{
			"caller":   caller.Hex(),
			"contract": contract.Hex(),
			"value":    valueUint256.String(),
			"gasLimit": gasLimit,
		}).Info("Executing precompiled contract")

		// Check if we have enough gas
		requiredGas := precompiled.RequiredGas(input)
		if requiredGas > gasLimit {
			return nil, 0, errors.New("out of gas")
		}

		// Execute the precompiled contract
		output, err := precompiled.Run(input)
		if err != nil {
			return nil, 0, err
		}

		return output, requiredGas, nil
	}

	// Execute using go-ethereum EVM
	e.logger.WithFields(logrus.Fields{
		"caller":   caller.Hex(),
		"contract": contract.Hex(),
		"value":    valueUint256.String(),
		"gasLimit": gasLimit,
	}).Info("Executing contract")

	// Set up EVM context
	blockCtx := gethvm.BlockContext{
		CanTransfer: core.CanTransfer,
		Transfer:    core.Transfer,
		GetHash:     func(uint64) ethcommon.Hash { return ethcommon.Hash{} },
		Coinbase:    caller,
		GasLimit:    gasLimit,
		BlockNumber: new(big.Int).SetUint64(e.stateDB.blockHeight),
		Time:        0,
		Difficulty:  new(big.Int).SetUint64(0),
		BaseFee:     new(big.Int).SetUint64(0),
	}

	txCtx := gethvm.TxContext{
		Origin:   caller,
		GasPrice: new(big.Int).SetUint64(0),
	}

	evm := gethvm.NewEVM(blockCtx, txCtx, e.stateDB, params.MainnetChainConfig, gethvm.Config{})

	// Create a contract reference wrapper
	callerRef := &contractRef{address: caller}
	contractRef := &contractRef{address: contract}

	ret, left, err := evm.Call(callerRef, contractRef.address, input, gasLimit, valueUint256)
	if err != nil {
		return nil, gasLimit - left, err
	}

	return ret, gasLimit - left, nil
}

// DeployContract deploys a new contract
func (e *EVMExecutor) DeployContract(caller ethcommon.Address, code []byte, value *big.Int, gasLimit uint64) (ethcommon.Address, []byte, uint64, error) {
	// Convert big.Int to uint256.Int
	valueUint256, overflow := uint256.FromBig(value)
	if overflow {
		return ethcommon.Address{}, nil, 0, ErrGasUintOverflow
	}

	e.logger.WithFields(logrus.Fields{
		"caller":   caller.Hex(),
		"codeSize": len(code),
		"value":    valueUint256.String(),
		"gasLimit": gasLimit,
	}).Info("Deploying contract")

	blockCtx := gethvm.BlockContext{
		CanTransfer: core.CanTransfer,
		Transfer:    core.Transfer,
		GetHash:     func(uint64) ethcommon.Hash { return ethcommon.Hash{} },
		Coinbase:    caller,
		GasLimit:    gasLimit,
		BlockNumber: new(big.Int).SetUint64(e.stateDB.blockHeight),
		Time:        0,
		Difficulty:  new(big.Int).SetUint64(0),
		BaseFee:     new(big.Int).SetUint64(0),
	}

	txCtx := gethvm.TxContext{
		Origin:   caller,
		GasPrice: new(big.Int).SetUint64(0),
	}

	evm := gethvm.NewEVM(blockCtx, txCtx, e.stateDB, params.MainnetChainConfig, gethvm.Config{})

	// Create a contract reference wrapper
	callerRef := &contractRef{address: caller}

	ret, contractAddr, left, err := evm.Create(callerRef, code, gasLimit, valueUint256)
	if err != nil {
		return ethcommon.Address{}, nil, gasLimit - left, err
	}

	return contractAddr, ret, gasLimit - left, nil
}

// UpgradeContract updates code for an existing contract
func (e *EVMExecutor) UpgradeContract(contract ethcommon.Address, code []byte, version string) error {
	e.logger.WithFields(logrus.Fields{
		"contract": contract.Hex(),
		"version":  version,
	}).Info("Upgrading contract")
	e.stateDB.SetCodeVersion(contract, code, version)
	return nil
}

// Commit commits the state changes
func (e *EVMExecutor) Commit() (ethcommon.Hash, error) {
	return e.stateDB.Commit(true)
}

// EstimateGas estimates the gas required for a contract call or deployment
func (e *EVMExecutor) EstimateGas(caller, contract ethcommon.Address, input []byte, value *big.Int) (uint64, error) {
	// Convert big.Int to uint256.Int
	valueUint256, overflow := uint256.FromBig(value)
	if overflow {
		return 0, ErrGasUintOverflow
	}

	e.logger.WithFields(logrus.Fields{
		"caller":   caller.Hex(),
		"contract": contract.Hex(),
		"value":    valueUint256.String(),
	}).Info("Estimating gas")

	// Check if the contract is a precompiled contract
	if precompiled, ok := PrecompiledContractsDiamante[contract]; ok {
		// For precompiled contracts, return the required gas
		return precompiled.RequiredGas(input), nil
	}

	// For contract deployment
	if (contract == ethcommon.Address{}) {
		baseCost := uint64(53000)
		byteCost := uint64(200)
		return baseCost + uint64(len(input))*byteCost, nil
	}

	// For regular contract execution
	baseCost := uint64(21000)
	var zeroBytes, nonZeroBytes int
	for _, b := range input {
		if b == 0 {
			zeroBytes++
		} else {
			nonZeroBytes++
		}
	}
	return baseCost + uint64(zeroBytes)*4 + uint64(nonZeroBytes)*16, nil
}

// GetStateDB returns the state database
func (e *EVMExecutor) GetStateDB() *StateDB {
	return e.stateDB
}

// GetEventManager returns the event manager
func (e *EVMExecutor) GetEventManager() *EventManager {
	return e.eventManager
}

// AddEventLog adds an event log to the event manager
func (e *EVMExecutor) AddEventLog(log EventLog) {
	e.eventManager.AddLog(log)
}

// GetEventLogs returns all event logs that match the given filter
func (e *EVMExecutor) GetEventLogs(filter EventFilter) []EventLog {
	return e.eventManager.GetLogs(filter)
}

// GetEventLogsByTxHash returns all event logs for a specific transaction
func (e *EVMExecutor) GetEventLogsByTxHash(txHash ethcommon.Hash) []EventLog {
	return e.eventManager.GetLogsByTxHash(txHash)
}

// GetEventLogsByAddress returns all event logs for a specific contract address
func (e *EVMExecutor) GetEventLogsByAddress(address ethcommon.Address) []EventLog {
	return e.eventManager.GetLogsByAddress(address)
}

// GetEventLogsByBlockRange returns all event logs within a block range
func (e *EVMExecutor) GetEventLogsByBlockRange(fromBlock, toBlock uint64) []EventLog {
	return e.eventManager.GetLogsByBlockRange(fromBlock, toBlock)
}

// ClearEventLogs clears all event logs
func (e *EVMExecutor) ClearEventLogs() {
	e.eventManager.Clear()
}

// GetEventLogCount returns the number of event logs
func (e *EVMExecutor) GetEventLogCount() int {
	return e.eventManager.Count()
}
