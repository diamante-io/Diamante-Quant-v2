// ledger/evm_executor.go

package ledger

import (
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"sync"

	"diamante/common"
	"diamante/ledger/evm"
	"diamante/storage"

	"diamante/consensus"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/params"
	"github.com/holiman/uint256"
	"github.com/sirupsen/logrus"
)

var (
	// ErrInvalidEVMTransaction is returned when an EVM transaction is invalid
	ErrInvalidEVMTransaction = errors.New("invalid EVM transaction")

	// ErrContractCreationFailed is returned when contract creation fails
	ErrContractCreationFailed = errors.New("contract creation failed")

	// ErrContractExecutionFailed is returned when contract execution fails
	ErrContractExecutionFailed = errors.New("contract execution failed")

	// ErrInsufficientGas is returned when there is not enough gas to execute a transaction
	ErrInsufficientGas = errors.New("insufficient gas for execution")

	// ErrSmartContractNotFound is returned when a smart contract is not found
	ErrSmartContractNotFound = errors.New("smart contract not found")

	// ErrInsufficientBalance is returned when account has insufficient balance
	ErrInsufficientBalance = errors.New("insufficient balance for transfer")
)

// EVMConfig holds configuration for the EVM executor
type EVMConfig struct {
	ChainID              *big.Int
	GasLimit             uint64
	GasPrice             *big.Int
	EnablePrecompiles    bool
	AllowUnprotectedTxs  bool
	MaxCodeSize          uint64
	MaxStackDepth        int
	EnableStaticCalls    bool
	EnableCreateContract bool
	EnableSelfDestruct   bool
}

// DefaultEVMConfig returns the default EVM configuration
func DefaultEVMConfig() *EVMConfig {
	return &EVMConfig{
		ChainID:              big.NewInt(1),
		GasLimit:             10000000,
		GasPrice:             big.NewInt(1000000000), // 1 Gwei
		EnablePrecompiles:    true,
		AllowUnprotectedTxs:  false,
		MaxCodeSize:          24576,
		MaxStackDepth:        1024,
		EnableStaticCalls:    true,
		EnableCreateContract: true,
		EnableSelfDestruct:   true,
	}
}

// EVMExecutor manages the execution of EVM bytecode using actual go-ethereum EVM
type EVMExecutor struct {
	config        *EVMConfig
	ledger        common.LedgerAPI
	stateDB       *evm.StateDB
	stateStore    storage.LedgerStore
	logger        *logrus.Logger
	chainConfig   *params.ChainConfig
	blockHeight   uint64
	contractCache map[string]*common.SmartContract
	mu            sync.RWMutex
}

// NewEVMExecutor creates a new EVM executor with actual EVM execution support
func NewEVMExecutor(ledger common.LedgerAPI, config *EVMConfig, logger *logrus.Logger) *EVMExecutor {
	if config == nil {
		config = DefaultEVMConfig()
	}

	if logger == nil {
		logger = logrus.New()
		logger.SetLevel(logrus.InfoLevel)
	}

	// Get state store from ledger if possible
	var stateStore storage.LedgerStore
	if ls, ok := ledger.(interface{ GetStateStore() storage.LedgerStore }); ok {
		stateStore = ls.GetStateStore()
	}

	// Create state database for EVM
	blockHeight := uint64(0)
	if bh, err := ledger.GetBlockHeight(); err == nil {
		blockHeight = uint64(bh)
	}

	stateDB := evm.NewStateDB(ledger, stateStore, blockHeight, logger)

	// Create chain config compatible with Ethereum
	chainConfig := &params.ChainConfig{
		ChainID:                       config.ChainID,
		HomesteadBlock:                big.NewInt(0),
		DAOForkBlock:                  nil,
		DAOForkSupport:                false,
		EIP150Block:                   big.NewInt(0),
		EIP155Block:                   big.NewInt(0),
		EIP158Block:                   big.NewInt(0),
		ByzantiumBlock:                big.NewInt(0),
		ConstantinopleBlock:           big.NewInt(0),
		PetersburgBlock:               big.NewInt(0),
		IstanbulBlock:                 big.NewInt(0),
		MuirGlacierBlock:              big.NewInt(0),
		BerlinBlock:                   big.NewInt(0),
		LondonBlock:                   big.NewInt(0),
		ArrowGlacierBlock:             big.NewInt(0),
		GrayGlacierBlock:              big.NewInt(0),
		MergeNetsplitBlock:            big.NewInt(0),
		ShanghaiTime:                  nil,
		CancunTime:                    nil,
		PragueTime:                    nil,
		VerkleTime:                    nil,
		TerminalTotalDifficulty:       nil,
		TerminalTotalDifficultyPassed: true,
	}

	return &EVMExecutor{
		config:        config,
		ledger:        ledger,
		stateDB:       stateDB,
		stateStore:    stateStore,
		logger:        logger,
		chainConfig:   chainConfig,
		blockHeight:   blockHeight,
		contractCache: make(map[string]*common.SmartContract),
	}
}

// ExecuteTransaction executes an EVM transaction using actual go-ethereum EVM
func (e *EVMExecutor) ExecuteTransaction(tx *EVMTransaction) (*EVMExecutionResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Validate the transaction
	if err := tx.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidEVMTransaction, err)
	}

	// Set gas limit
	gasLimit := tx.GasLimit
	if gasLimit == 0 {
		gasLimit = e.config.GasLimit
	}

	// Ensure sender has sufficient balance
	fromAddr := ethcommon.BytesToAddress(tx.From)
	balance := e.stateDB.GetBalance(fromAddr)
	totalCost := new(big.Int).Mul(tx.GasPrice, new(big.Int).SetUint64(gasLimit))
	if tx.Value != nil {
		totalCost.Add(totalCost, tx.Value)
	}
	// Convert totalCost to uint256.Int for comparison
	totalCostU256 := uint256.MustFromBig(totalCost)
	if balance.Cmp(totalCostU256) < 0 {
		return nil, fmt.Errorf("%w: need %s, have %s", ErrInsufficientBalance, totalCost.String(), balance.String())
	}

	// Create block context
	blockCtx := vm.BlockContext{
		CanTransfer: core.CanTransfer,
		Transfer:    core.Transfer,
		GetHash: func(n uint64) ethcommon.Hash {
			// In production, fetch actual block hashes
			return ethcommon.BytesToHash(crypto.Keccak256([]byte(fmt.Sprintf("block-%d", n))))
		},
		Coinbase:    ethcommon.Address{},
		BlockNumber: big.NewInt(int64(e.blockHeight)),
		Time:        uint64(consensus.ConsensusNow().Unix()),
		Difficulty:  big.NewInt(0),
		GasLimit:    e.config.GasLimit,
		BaseFee:     e.config.GasPrice,
	}

	// Create transaction context
	txCtx := vm.TxContext{
		Origin:   fromAddr,
		GasPrice: tx.GasPrice,
	}

	// Create EVM instance
	evmConfig := vm.Config{
		NoBaseFee: false,
	}
	evmInstance := vm.NewEVM(blockCtx, txCtx, e.stateDB, e.chainConfig, evmConfig)

	// Take snapshot for potential revert
	snapshot := e.stateDB.Snapshot()

	var (
		ret          []byte
		gasUsed      uint64
		contractAddr []byte
		err          error
	)

	// Check if this is a contract creation or a contract call
	if tx.To == nil {
		// Contract creation
		if !e.config.EnableCreateContract {
			e.stateDB.RevertToSnapshot(snapshot)
			return nil, errors.New("contract creation is disabled")
		}

		// Create contract using EVM
		var addr ethcommon.Address
		valueU256 := uint256.MustFromBig(tx.Value)
		ret, addr, gasUsed, err = evmInstance.Create(
			vm.AccountRef(fromAddr),
			tx.Data,
			gasLimit,
			valueU256,
		)
		if addr != (ethcommon.Address{}) {
			contractAddr = addr.Bytes()
		}

		if err == nil {
			// Store contract metadata
			contractID := hex.EncodeToString(contractAddr)
			contract := &common.SmartContract{
				ID:       contractID,
				Code:     hex.EncodeToString(tx.Data),
				Owner:    hex.EncodeToString(tx.From),
				Language: "EVM",
				State: &common.SmartContractState{
					Variables:     make(map[string]string),
					Balances:      make(map[string]float64),
					Permissions:   make(map[string]bool),
					Configuration: make(map[string]string),
					Counters:      make(map[string]int64),
					LastUpdated:   consensus.ConsensusUnix(),
				},
			}

			// Deploy the contract metadata
			if deployErr := e.ledger.DeploySmartContract(contract); deployErr != nil {
				e.logger.WithError(deployErr).Error("Failed to store contract metadata")
			}
			e.contractCache[contractID] = contract
		}

		e.logger.WithFields(logrus.Fields{
			"from":     hex.EncodeToString(tx.From),
			"gasUsed":  gasUsed,
			"gasLimit": gasLimit,
			"contract": hex.EncodeToString(contractAddr),
			"success":  err == nil,
		}).Info("Contract creation executed")
	} else {
		// Contract call
		toAddr := ethcommon.BytesToAddress(tx.To)
		valueU256 := uint256.MustFromBig(tx.Value)
		ret, gasUsed, err = evmInstance.Call(
			vm.AccountRef(fromAddr),
			toAddr,
			tx.Data,
			gasLimit,
			valueU256,
		)

		e.logger.WithFields(logrus.Fields{
			"from":      hex.EncodeToString(tx.From),
			"to":        hex.EncodeToString(tx.To),
			"gasUsed":   gasUsed,
			"gasLimit":  gasLimit,
			"returnLen": len(ret),
			"success":   err == nil,
		}).Info("Contract call executed")
	}

	// Handle execution error
	if err != nil {
		e.stateDB.RevertToSnapshot(snapshot)
		return &EVMExecutionResult{
			ReturnData:   nil,
			GasUsed:      gasUsed,
			ContractAddr: contractAddr,
			Error:        err,
		}, nil
	}

	// Commit state changes
	root, commitErr := e.stateDB.Commit(true)
	if commitErr != nil {
		e.stateDB.RevertToSnapshot(snapshot)
		return nil, fmt.Errorf("failed to commit state changes: %w", commitErr)
	}

	e.logger.WithField("stateRoot", root.Hex()).Debug("State committed")

	// Create the execution result
	result := &EVMExecutionResult{
		ReturnData:   ret,
		GasUsed:      gasUsed,
		ContractAddr: contractAddr,
		Error:        nil,
	}

	return result, nil
}

// DeployContract deploys a new EVM contract using actual EVM
func (e *EVMExecutor) DeployContract(from []byte, bytecode []byte, gasLimit uint64) ([]byte, error) {
	// Get nonce for the account
	nonce, err := e.GetNonce(from)
	if err != nil {
		return nil, fmt.Errorf("failed to get nonce: %w", err)
	}

	// Create a transaction for contract deployment
	tx := &EVMTransaction{
		From:     from,
		To:       nil, // nil recipient means contract creation
		Data:     bytecode,
		GasLimit: gasLimit,
		GasPrice: e.config.GasPrice,
		Nonce:    nonce,
		Value:    big.NewInt(0),
		ChainID:  e.config.ChainID,
	}

	// Execute the transaction
	result, err := e.ExecuteTransaction(tx)
	if err != nil {
		return nil, fmt.Errorf("contract deployment failed: %w", err)
	}

	// Check if the contract was created successfully
	if result.Error != nil {
		return nil, fmt.Errorf("%w: %v", ErrContractCreationFailed, result.Error)
	}

	return result.ContractAddr, nil
}

// CallContract calls an existing EVM contract using actual EVM
func (e *EVMExecutor) CallContract(from []byte, to []byte, data []byte, gasLimit uint64) ([]byte, error) {
	// Get nonce for the account
	nonce, err := e.GetNonce(from)
	if err != nil {
		return nil, fmt.Errorf("failed to get nonce: %w", err)
	}

	// Create a transaction for contract call
	tx := &EVMTransaction{
		From:     from,
		To:       to,
		Data:     data,
		GasLimit: gasLimit,
		GasPrice: e.config.GasPrice,
		Nonce:    nonce,
		Value:    big.NewInt(0),
		ChainID:  e.config.ChainID,
	}

	// Execute the transaction
	result, err := e.ExecuteTransaction(tx)
	if err != nil {
		return nil, fmt.Errorf("contract call failed: %w", err)
	}

	// Check if the contract call was successful
	if result.Error != nil {
		return nil, fmt.Errorf("%w: %v", ErrContractExecutionFailed, result.Error)
	}

	return result.ReturnData, nil
}

// GetCode returns the code at the given address
func (e *EVMExecutor) GetCode(address []byte) ([]byte, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	// Use StateDB to get code
	addr := ethcommon.BytesToAddress(address)
	code := e.stateDB.GetCode(addr)

	if len(code) == 0 {
		// Try to get from contract cache
		contractID := hex.EncodeToString(address)
		if contract, ok := e.contractCache[contractID]; ok {
			return hex.DecodeString(contract.Code)
		}

		// Try to get from ledger using getSmartContract
		contract, err := e.getSmartContract(contractID)
		if err != nil {
			return nil, err
		}

		code, err := hex.DecodeString(contract.Code)
		if err != nil {
			return nil, fmt.Errorf("failed to decode contract code: %w", err)
		}

		return code, nil
	}

	return code, nil
}

// GetBalance returns the balance of the given address
func (e *EVMExecutor) GetBalance(address []byte) (*big.Int, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	// Use StateDB to get balance
	addr := ethcommon.BytesToAddress(address)
	balance := e.stateDB.GetBalance(addr)

	// Convert uint256.Int to *big.Int
	return balance.ToBig(), nil
}

// GetNonce returns the nonce of the given address
func (e *EVMExecutor) GetNonce(address []byte) (uint64, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	// Use StateDB to get nonce
	addr := ethcommon.BytesToAddress(address)
	nonce := e.stateDB.GetNonce(addr)

	return nonce, nil
}

// getSmartContract retrieves a smart contract from storage
func (e *EVMExecutor) getSmartContract(contractID string) (*common.SmartContract, error) {
	// Check cache first
	if contract, ok := e.contractCache[contractID]; ok {
		return contract, nil
	}

	// If we have a contract store, use it
	if e.stateStore != nil {
		contract, err := e.stateStore.GetContract(contractID)
		if err != nil {
			return nil, fmt.Errorf("failed to get contract: %w", err)
		}
		e.contractCache[contractID] = contract
		return contract, nil
	}

	// Fallback: try to get code from StateDB
	addr, err := hex.DecodeString(contractID)
	if err != nil {
		return nil, fmt.Errorf("invalid contract ID: %w", err)
	}

	code := e.stateDB.GetCode(ethcommon.BytesToAddress(addr))
	if len(code) == 0 {
		return nil, ErrSmartContractNotFound
	}

	// Create a basic contract object
	contract := &common.SmartContract{
		ID:       contractID,
		Code:     hex.EncodeToString(code),
		Owner:    "unknown",
		Language: "EVM",
		State: &common.SmartContractState{
			Variables:     make(map[string]string),
			Balances:      make(map[string]float64),
			Permissions:   make(map[string]bool),
			Configuration: make(map[string]string),
			Counters:      make(map[string]int64),
			LastUpdated:   consensus.ConsensusUnix(),
		},
	}

	e.contractCache[contractID] = contract
	return contract, nil
}

// SetBlockHeight updates the current block height
func (e *EVMExecutor) SetBlockHeight(height uint64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.blockHeight = height
}

// EVMTransaction represents an EVM transaction
type EVMTransaction struct {
	From     []byte
	To       []byte
	Data     []byte
	GasLimit uint64
	GasPrice *big.Int
	Nonce    uint64
	Value    *big.Int
	ChainID  *big.Int
}

// Validate validates the transaction
func (tx *EVMTransaction) Validate() error {
	if len(tx.From) == 0 {
		return errors.New("sender address cannot be empty")
	}

	if tx.To == nil && len(tx.Data) == 0 {
		return errors.New("contract creation requires data")
	}

	if tx.GasLimit == 0 {
		return errors.New("gas limit cannot be zero")
	}

	if tx.GasPrice == nil || tx.GasPrice.Sign() <= 0 {
		return errors.New("gas price must be positive")
	}

	return nil
}

// Hash returns the hash of the transaction
func (tx *EVMTransaction) Hash() string {
	// Create a hash of the transaction fields
	data := fmt.Sprintf("%x:%x:%x:%d:%s:%d:%s",
		tx.From,
		tx.To,
		tx.Data,
		tx.GasLimit,
		tx.GasPrice.String(),
		tx.Nonce,
		tx.ChainID.String(),
	)
	return hex.EncodeToString(crypto.Keccak256([]byte(data)))
}

// EVMExecutionResult represents the result of an EVM execution
type EVMExecutionResult struct {
	ReturnData   []byte
	GasUsed      uint64
	ContractAddr []byte
	Error        error
}

// EVMStateDB implements a simplified state database for the EVM (deprecated, use evm.StateDB)
type EVMStateDB struct {
	ledger common.LedgerAPI
	logger *logrus.Logger
	mu     sync.RWMutex
}

// NewEVMStateDB creates a new EVM state database (deprecated, use evm.NewStateDB)
func NewEVMStateDB(ledger common.LedgerAPI, logger *logrus.Logger) *EVMStateDB {
	return &EVMStateDB{
		ledger: ledger,
		logger: logger,
	}
}

// EVMTransactionData represents the data for an EVM transaction
type EVMTransactionData struct {
	From     string `json:"from"`
	To       string `json:"to,omitempty"`
	Data     string `json:"data"`
	GasLimit uint64 `json:"gasLimit"`
	GasPrice string `json:"gasPrice"`
	Value    string `json:"value,omitempty"`
	Nonce    uint64 `json:"nonce"`
}

// ParseEVMTransactionData parses EVM transaction data from a map
func ParseEVMTransactionData(data map[string]interface{}) (*EVMTransactionData, error) {
	txData := &EVMTransactionData{}

	// Parse from address
	from, ok := data["from"].(string)
	if !ok {
		return nil, errors.New("from address is required")
	}
	txData.From = from

	// Parse to address (optional for contract creation)
	if to, ok := data["to"].(string); ok {
		txData.To = to
	}

	// Parse data
	dataHex, ok := data["data"].(string)
	if !ok {
		return nil, errors.New("data is required")
	}
	txData.Data = dataHex

	// Parse gas limit
	gasLimit, ok := data["gasLimit"].(float64)
	if !ok {
		return nil, errors.New("gasLimit is required")
	}
	txData.GasLimit = uint64(gasLimit)

	// Parse gas price
	gasPrice, ok := data["gasPrice"].(string)
	if !ok {
		return nil, errors.New("gasPrice is required")
	}
	txData.GasPrice = gasPrice

	// Parse value (optional)
	if value, ok := data["value"].(string); ok {
		txData.Value = value
	}

	// Parse nonce
	nonce, ok := data["nonce"].(float64)
	if !ok {
		return nil, errors.New("nonce is required")
	}
	txData.Nonce = uint64(nonce)

	return txData, nil
}

// ToEVMTransaction converts EVMTransactionData to an EVMTransaction
func (txData *EVMTransactionData) ToEVMTransaction() (*EVMTransaction, error) {
	// Parse from address
	from, err := hex.DecodeString(txData.From)
	if err != nil {
		return nil, fmt.Errorf("invalid from address: %w", err)
	}

	// Parse to address (optional for contract creation)
	var to []byte
	if txData.To != "" {
		to, err = hex.DecodeString(txData.To)
		if err != nil {
			return nil, fmt.Errorf("invalid to address: %w", err)
		}
	}

	// Parse data
	data, err := hexutil.Decode(txData.Data)
	if err != nil {
		return nil, fmt.Errorf("invalid data: %w", err)
	}

	// Parse gas price
	gasPrice, ok := new(big.Int).SetString(txData.GasPrice, 10)
	if !ok {
		return nil, errors.New("invalid gas price")
	}

	// Parse value (optional)
	var value *big.Int
	if txData.Value != "" {
		value, ok = new(big.Int).SetString(txData.Value, 10)
		if !ok {
			return nil, errors.New("invalid value")
		}
	} else {
		value = big.NewInt(0)
	}

	// Create the transaction
	tx := &EVMTransaction{
		From:     from,
		To:       to,
		Data:     data,
		GasLimit: txData.GasLimit,
		GasPrice: gasPrice,
		Nonce:    txData.Nonce,
		Value:    value,
	}

	return tx, nil
}

// BytesToAddress converts a byte slice to an Ethereum address
func BytesToAddress(b []byte) ethcommon.Address {
	var a ethcommon.Address
	if len(b) > len(a) {
		b = b[len(b)-len(a):]
	}
	copy(a[len(a)-len(b):], b)
	return a
}

// HexToAddress converts a hex string to an Ethereum address
func HexToAddress(s string) (ethcommon.Address, error) {
	b, err := hex.DecodeString(s)
	if err != nil {
		return ethcommon.Address{}, err
	}
	return BytesToAddress(b), nil
}
