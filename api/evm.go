// api/evm.go

package api

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"math/big"
	"net/http"

	"diamante/common"
	"diamante/ledger/evm"
	"diamante/storage"

	ethcommon "github.com/ethereum/go-ethereum/common"
	gethvm "github.com/ethereum/go-ethereum/core/vm"
	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"
)

// EVMHandler handles EVM-related API requests
type EVMHandler struct {
	ledger      common.LedgerAPI
	stateStore  storage.LedgerStore
	blockHeight uint64
	logger      *logrus.Logger
	bodyLimit   int64
}

// NewEVMHandler creates a new EVMHandler
func NewEVMHandler(ledger common.LedgerAPI, stateStore storage.LedgerStore, blockHeight uint64, logger *logrus.Logger) *EVMHandler {
	if logger == nil {
		logger = logrus.New()
		logger.SetLevel(logrus.InfoLevel)
	}

	return &EVMHandler{
		ledger:      ledger,
		stateStore:  stateStore,
		blockHeight: blockHeight,
		logger:      logger,
		bodyLimit:   1 << 20,
	}
}

// RegisterRoutes registers the EVM API routes
func (h *EVMHandler) RegisterRoutes(router *mux.Router) {
	router.HandleFunc("/evm/execute", h.ExecuteContract).Methods("POST")
	router.HandleFunc("/evm/deploy", h.DeployContract).Methods("POST")
	router.HandleFunc("/evm/estimate-gas", h.EstimateGas).Methods("POST")
	router.HandleFunc("/evm/balance/{address}", h.GetBalance).Methods("GET")
	router.HandleFunc("/evm/code/{address}", h.GetCode).Methods("GET")
	router.HandleFunc("/evm/events", h.GetEventLogs).Methods("POST")
	router.HandleFunc("/evm/events/{txHash}", h.GetEventLogsByTxHash).Methods("GET")
	router.HandleFunc("/evm/events/address/{address}", h.GetEventLogsByAddress).Methods("GET")
}

// ExecuteContractRequest represents a request to execute a contract
type ExecuteContractRequest struct {
	Caller   string `json:"caller"`
	Contract string `json:"contract"`
	Input    string `json:"input"`
	Value    string `json:"value"`
	GasLimit uint64 `json:"gasLimit"`
}

// ExecuteContractResponse represents a response from executing a contract
type ExecuteContractResponse struct {
	Result  string `json:"result"`
	GasUsed uint64 `json:"gasUsed"`
}

// ExecuteContract executes a contract
func (h *EVMHandler) ExecuteContract(w http.ResponseWriter, r *http.Request) {
	// Parse the request
	var req ExecuteContractRequest
	if err := decodeJSONBody(w, r, &req, h.bodyLimit); err != nil {
		if errors.Is(err, ErrBodyTooLarge) {
			http.Error(w, "payload too large", http.StatusRequestEntityTooLarge)
		} else {
			http.Error(w, err.Error(), http.StatusBadRequest)
		}
		return
	}

	// Validate the request
	if req.Caller == "" || req.Contract == "" {
		http.Error(w, "caller and contract are required", http.StatusBadRequest)
		return
	}
	if len(req.Caller) > 64 || len(req.Contract) > 64 || len(req.Input) > 10240 {
		http.Error(w, "fields too long", http.StatusBadRequest)
		return
	}

	// Parse the caller and contract addresses
	caller := ethcommon.HexToAddress(req.Caller)
	contract := ethcommon.HexToAddress(req.Contract)

	// Parse the input
	input, err := hex.DecodeString(req.Input)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Parse the value
	value := new(big.Int)
	if req.Value != "" {
		var ok bool
		value, ok = value.SetString(req.Value, 10)
		if !ok {
			http.Error(w, "invalid value format", http.StatusBadRequest)
			return
		}
	}

	// Create an EVM executor
	executor := evm.NewEVMExecutor(h.ledger, h.stateStore, h.blockHeight, h.logger)

	// Execute the contract
	result, gasUsed, err := executor.ExecuteContract(caller, contract, input, value, req.GasLimit)
	if err != nil {
		if errors.Is(err, gethvm.ErrExecutionReverted) {
			http.Error(w, "execution reverted", http.StatusBadRequest)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Commit the state changes
	if _, err := executor.Commit(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Return the response
	resp := ExecuteContractResponse{
		Result:  hex.EncodeToString(result),
		GasUsed: gasUsed,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// DeployContractRequest represents a request to deploy a contract
type DeployContractRequest struct {
	Caller   string `json:"caller"`
	Code     string `json:"code"`
	Value    string `json:"value"`
	GasLimit uint64 `json:"gasLimit"`
}

// DeployContractResponse represents a response from deploying a contract
type DeployContractResponse struct {
	Address string `json:"address"`
	Code    string `json:"code"`
	GasUsed uint64 `json:"gasUsed"`
}

// DeployContract deploys a contract
func (h *EVMHandler) DeployContract(w http.ResponseWriter, r *http.Request) {
	// Parse the request
	var req DeployContractRequest
	if err := decodeJSONBody(w, r, &req, h.bodyLimit); err != nil {
		if errors.Is(err, ErrBodyTooLarge) {
			http.Error(w, "payload too large", http.StatusRequestEntityTooLarge)
		} else {
			http.Error(w, err.Error(), http.StatusBadRequest)
		}
		return
	}

	// Validate the request
	if req.Caller == "" || req.Code == "" {
		http.Error(w, "caller and code are required", http.StatusBadRequest)
		return
	}
	if len(req.Caller) > 64 || len(req.Code) > 65536 {
		http.Error(w, "fields too long", http.StatusBadRequest)
		return
	}

	// Parse the caller address
	caller := ethcommon.HexToAddress(req.Caller)

	// Parse the code
	code, err := hex.DecodeString(req.Code)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Parse the value
	value := new(big.Int)
	if req.Value != "" {
		var ok bool
		value, ok = value.SetString(req.Value, 10)
		if !ok {
			http.Error(w, "invalid value format", http.StatusBadRequest)
			return
		}
	}

	// Create an EVM executor
	executor := evm.NewEVMExecutor(h.ledger, h.stateStore, h.blockHeight, h.logger)

	// Deploy the contract
	address, code, gasUsed, err := executor.DeployContract(caller, code, value, req.GasLimit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Commit the state changes
	if _, err := executor.Commit(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Return the response
	resp := DeployContractResponse{
		Address: address.Hex(),
		Code:    hex.EncodeToString(code),
		GasUsed: gasUsed,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// GetBalance gets the balance of an account
func (h *EVMHandler) GetBalance(w http.ResponseWriter, r *http.Request) {
	// Get the address from the URL
	vars := mux.Vars(r)
	address := vars["address"]

	// Validate the address
	if address == "" {
		http.Error(w, "address is required", http.StatusBadRequest)
		return
	}

	// Parse the address
	addr := ethcommon.HexToAddress(address)

	// Create an EVM executor
	executor := evm.NewEVMExecutor(h.ledger, h.stateStore, h.blockHeight, h.logger)

	// Get the state DB
	stateDB := executor.GetStateDB()

	// Get the balance
	balance := stateDB.GetBalance(addr)

	// Return the response
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"address": addr.Hex(),
		"balance": balance.String(),
	})
}

// EstimateGasRequest represents a request to estimate gas
type EstimateGasRequest struct {
	Caller   string `json:"caller"`
	Contract string `json:"contract,omitempty"` // Empty for contract deployment
	Input    string `json:"input"`              // Contract code for deployment, call data for execution
	Value    string `json:"value"`
}

// EstimateGasResponse represents a response from estimating gas
type EstimateGasResponse struct {
	GasEstimate uint64 `json:"gasEstimate"`
}

// EstimateGas estimates the gas required for a contract call or deployment
func (h *EVMHandler) EstimateGas(w http.ResponseWriter, r *http.Request) {
	// Parse the request
	var req EstimateGasRequest
	if err := decodeJSONBody(w, r, &req, h.bodyLimit); err != nil {
		if errors.Is(err, ErrBodyTooLarge) {
			http.Error(w, "payload too large", http.StatusRequestEntityTooLarge)
		} else {
			http.Error(w, err.Error(), http.StatusBadRequest)
		}
		return
	}

	// Validate the request
	if req.Caller == "" || req.Input == "" {
		http.Error(w, "caller and input are required", http.StatusBadRequest)
		return
	}
	if len(req.Caller) > 64 || len(req.Input) > 65536 || len(req.Contract) > 64 {
		http.Error(w, "fields too long", http.StatusBadRequest)
		return
	}

	// Parse the caller address
	caller := ethcommon.HexToAddress(req.Caller)

	// Parse the contract address (if provided)
	var contract ethcommon.Address
	if req.Contract != "" {
		contract = ethcommon.HexToAddress(req.Contract)
	}

	// Parse the input
	input, err := hex.DecodeString(req.Input)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Parse the value
	value := new(big.Int)
	if req.Value != "" {
		var ok bool
		value, ok = value.SetString(req.Value, 10)
		if !ok {
			http.Error(w, "invalid value format", http.StatusBadRequest)
			return
		}
	}

	// Create an EVM executor
	executor := evm.NewEVMExecutor(h.ledger, h.stateStore, h.blockHeight, h.logger)

	// Estimate gas
	gasEstimate, err := executor.EstimateGas(caller, contract, input, value)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Return the response
	resp := EstimateGasResponse{
		GasEstimate: gasEstimate,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// GetCode gets the code of a contract
func (h *EVMHandler) GetCode(w http.ResponseWriter, r *http.Request) {
	// Get the address from the URL
	vars := mux.Vars(r)
	address := vars["address"]

	// Validate the address
	if address == "" {
		http.Error(w, "address is required", http.StatusBadRequest)
		return
	}

	// Parse the address
	addr := ethcommon.HexToAddress(address)

	// Create an EVM executor
	executor := evm.NewEVMExecutor(h.ledger, h.stateStore, h.blockHeight, h.logger)

	// Get the state DB
	stateDB := executor.GetStateDB()

	// Get the code
	code := stateDB.GetCode(addr)

	// Return the response
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"address": addr.Hex(),
		"code":    hex.EncodeToString(code),
	})
}

// GetEventLogsRequest represents a request to get event logs with filters
type GetEventLogsRequest struct {
	FromBlock string   `json:"fromBlock,omitempty"`
	ToBlock   string   `json:"toBlock,omitempty"`
	Addresses []string `json:"addresses,omitempty"`
	Topics    []string `json:"topics,omitempty"`
	Limit     int      `json:"limit,omitempty"`
}

// GetEventLogsResponse represents a response containing event logs
type GetEventLogsResponse struct {
	Logs []evm.EventLog `json:"logs"`
}

// GetEventLogs gets event logs with optional filters
func (h *EVMHandler) GetEventLogs(w http.ResponseWriter, r *http.Request) {
	// Parse the request
	var req GetEventLogsRequest
	if err := decodeJSONBody(w, r, &req, h.bodyLimit); err != nil {
		if errors.Is(err, ErrBodyTooLarge) {
			http.Error(w, "payload too large", http.StatusRequestEntityTooLarge)
		} else {
			http.Error(w, err.Error(), http.StatusBadRequest)
		}
		return
	}

	if len(req.Addresses) > 20 || len(req.Topics) > 20 {
		http.Error(w, "too many filters", http.StatusBadRequest)
		return
	}

	// Create an EVM executor
	executor := evm.NewEVMExecutor(h.ledger, h.stateStore, h.blockHeight, h.logger)

	// Build the filter
	filter := evm.EventFilter{
		Limit: req.Limit,
	}

	// Parse block range
	if req.FromBlock != "" {
		fromBlock := new(big.Int)
		var ok bool
		fromBlock, ok = fromBlock.SetString(req.FromBlock, 10)
		if !ok {
			http.Error(w, "invalid fromBlock format", http.StatusBadRequest)
			return
		}
		filter.FromBlock = fromBlock
	}
	if req.ToBlock != "" {
		toBlock := new(big.Int)
		var ok bool
		toBlock, ok = toBlock.SetString(req.ToBlock, 10)
		if !ok {
			http.Error(w, "invalid toBlock format", http.StatusBadRequest)
			return
		}
		filter.ToBlock = toBlock
	}

	// Parse addresses
	if len(req.Addresses) > 0 {
		addresses := make([]ethcommon.Address, len(req.Addresses))
		for i, addr := range req.Addresses {
			addresses[i] = ethcommon.HexToAddress(addr)
		}
		filter.Addresses = addresses
	}

	// Parse topics
	if len(req.Topics) > 0 {
		topics := make([][]ethcommon.Hash, len(req.Topics))
		for i, topic := range req.Topics {
			if topic != "" {
				topics[i] = []ethcommon.Hash{ethcommon.HexToHash(topic)}
			}
		}
		filter.Topics = topics
	}

	// Get the logs
	logs := executor.GetEventLogs(filter)

	// Return the response
	resp := GetEventLogsResponse{
		Logs: logs,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// GetEventLogsByTxHash gets event logs for a specific transaction
func (h *EVMHandler) GetEventLogsByTxHash(w http.ResponseWriter, r *http.Request) {
	// Get the transaction hash from the URL
	vars := mux.Vars(r)
	txHashStr := vars["txHash"]

	// Validate the transaction hash
	if txHashStr == "" {
		http.Error(w, "transaction hash is required", http.StatusBadRequest)
		return
	}

	// Parse the transaction hash
	txHash := ethcommon.HexToHash(txHashStr)

	// Create an EVM executor
	executor := evm.NewEVMExecutor(h.ledger, h.stateStore, h.blockHeight, h.logger)

	// Get the logs
	logs := executor.GetEventLogsByTxHash(txHash)

	// Return the response
	resp := GetEventLogsResponse{
		Logs: logs,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// GetEventLogsByAddress gets event logs for a specific contract address
func (h *EVMHandler) GetEventLogsByAddress(w http.ResponseWriter, r *http.Request) {
	// Get the address from the URL
	vars := mux.Vars(r)
	addressStr := vars["address"]

	// Validate the address
	if addressStr == "" {
		http.Error(w, "address is required", http.StatusBadRequest)
		return
	}

	// Parse the address
	address := ethcommon.HexToAddress(addressStr)

	// Create an EVM executor
	executor := evm.NewEVMExecutor(h.ledger, h.stateStore, h.blockHeight, h.logger)

	// Get the logs
	logs := executor.GetEventLogsByAddress(address)

	// Return the response
	resp := GetEventLogsResponse{
		Logs: logs,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}
