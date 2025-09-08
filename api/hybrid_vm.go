// Package api provides HTTP handlers for hybrid VM operations
package api

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"diamante/common"
	"diamante/consensus"
	"diamante/storage"
	"diamante/transaction"
	"diamante/vm/deploy"
	"diamante/vm/runtime"

	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"
)

// HybridVMHandler handles HTTP requests for hybrid VM operations
type HybridVMHandler struct {
	hybridProcessor   *transaction.HybridTransactionProcessor
	runtimeManager    *runtime.RuntimeManager
	deploymentManager *deploy.DeploymentManager
	ledger            common.LedgerAPI
	stateStore        storage.LedgerStore
	logger            *logrus.Logger
	bodyLimit         int64
}

// NewHybridVMHandler creates a new handler for hybrid VM operations
func NewHybridVMHandler(
	hybridProcessor *transaction.HybridTransactionProcessor,
	runtimeManager *runtime.RuntimeManager,
	deploymentManager *deploy.DeploymentManager,
	ledger common.LedgerAPI,
	stateStore storage.LedgerStore,
	logger *logrus.Logger,
) *HybridVMHandler {
	return &HybridVMHandler{
		hybridProcessor:   hybridProcessor,
		runtimeManager:    runtimeManager,
		deploymentManager: deploymentManager,
		ledger:            ledger,
		stateStore:        stateStore,
		logger:            logger,
		bodyLimit:         10 * 1024 * 1024, // 10MB limit
	}
}

// RegisterRoutes registers hybrid VM routes
func (h *HybridVMHandler) RegisterRoutes(router *mux.Router) {
	// Contract deployment (unified for all runtimes)
	router.HandleFunc("/contract/deploy", h.DeployContract).Methods("POST")

	// Contract execution
	router.HandleFunc("/contract/execute", h.ExecuteContract).Methods("POST")

	// Contract information
	router.HandleFunc("/contract/{id}/info", h.GetContractInfo).Methods("GET")

	// Contract upgrade
	router.HandleFunc("/contract/{id}/upgrade", h.UpgradeContract).Methods("POST")

	// Contract state
	router.HandleFunc("/contract/{id}/state", h.GetContractState).Methods("GET")

	// Runtime information
	router.HandleFunc("/runtime/list", h.ListRuntimes).Methods("GET")
	router.HandleFunc("/runtime/{type}/info", h.GetRuntimeInfo).Methods("GET")

	// Transaction status
	router.HandleFunc("/transaction/{id}/status", h.GetTransactionStatus).Methods("GET")
	router.HandleFunc("/transaction/{id}/receipt", h.GetTransactionReceipt).Methods("GET")

	// Metrics
	router.HandleFunc("/hybrid/metrics", h.GetMetrics).Methods("GET")
}

// ContractArgument represents a typed argument for contract operations
type ContractArgument struct {
	Type  string      `json:"type"`  // string, number, boolean, address, bytes, array
	Value interface{} `json:"value"` // Keep interface{} here for flexibility in argument values
}

// ContractMetadata represents typed metadata for contracts
type ContractMetadata struct {
	Name        string            `json:"name,omitempty"`
	Description string            `json:"description,omitempty"`
	Version     string            `json:"version,omitempty"`
	Author      string            `json:"author,omitempty"`
	License     string            `json:"license,omitempty"`
	Tags        []string          `json:"tags,omitempty"`
	Properties  map[string]string `json:"properties,omitempty"`
}

// ResourceUsage represents resource usage information
type ResourceUsage struct {
	MemoryBytes   int64 `json:"memoryBytes"`
	StorageBytes  int64 `json:"storageBytes"`
	CPUMicros     int64 `json:"cpuMicros"`
	NetworkBytes  int64 `json:"networkBytes"`
	StateReads    int64 `json:"stateReads"`
	StateWrites   int64 `json:"stateWrites"`
	ExternalCalls int64 `json:"externalCalls"`
}

// EventParameter represents a typed event parameter
type EventParameter struct {
	Name  string      `json:"name"`
	Type  string      `json:"type"`
	Value interface{} `json:"value"` // Keep interface{} for event values
}

// MetricsResponse represents hybrid VM metrics
type MetricsResponse struct {
	TotalProcessed      int64            `json:"totalProcessed"`
	TotalFailed         int64            `json:"totalFailed"`
	AverageGasUsed      uint64           `json:"averageGasUsed"`
	AverageProcessTime  string           `json:"averageProcessTime"`
	RuntimeDistribution map[string]int64 `json:"runtimeDistribution"`
}

// TransactionStatusInfo represents transaction status
type TransactionStatusInfo struct {
	TransactionID string `json:"transactionId"`
	Status        string `json:"status"`
	Timestamp     int64  `json:"timestamp"`
}

// ContractStateResponse represents contract state
type ContractStateResponse struct {
	ContractID string      `json:"contractId"`
	Key        string      `json:"key"`
	State      interface{} `json:"state"` // Keep interface{} for state values
}

// HybridDeployRequest represents a contract deployment request
type HybridDeployRequest struct {
	Language        string             `json:"language"` // solidity, vyper, chaincode, native
	Code            string             `json:"code"`     // Base64 or hex encoded
	ConstructorArgs []ContractArgument `json:"constructorArgs"`
	InitialValue    float64            `json:"initialValue"`
	GasLimit        uint64             `json:"gasLimit"`
	GasPrice        uint64             `json:"gasPrice"`
	Metadata        *ContractMetadata  `json:"metadata"`
}

// HybridDeployResponse represents the deployment response
type HybridDeployResponse struct {
	TransactionID string            `json:"transactionId"`
	ContractID    string            `json:"contractId"`
	Runtime       string            `json:"runtime"`
	GasUsed       uint64            `json:"gasUsed"`
	Events        []EventResponse   `json:"events"`
	Metadata      *ContractMetadata `json:"metadata"`
}

// HybridExecuteRequest represents a contract execution request
type HybridExecuteRequest struct {
	ContractID string             `json:"contractId"`
	Function   string             `json:"function"`
	Args       []ContractArgument `json:"args"`
	Value      float64            `json:"value"`
	GasLimit   uint64             `json:"gasLimit"`
	GasPrice   uint64             `json:"gasPrice"`
}

// HybridExecuteResponse represents the execution response
type HybridExecuteResponse struct {
	TransactionID string          `json:"transactionId"`
	Success       bool            `json:"success"`
	Result        interface{}     `json:"result"` // Keep interface{} for execution results
	GasUsed       uint64          `json:"gasUsed"`
	Events        []EventResponse `json:"events"`
	Error         string          `json:"error,omitempty"`
}

// UpgradeContractRequest represents a contract upgrade request
type UpgradeContractRequest struct {
	NewVersion    string            `json:"newVersion"`
	NewCode       string            `json:"newCode"` // Base64 or hex encoded
	MigrationData string            `json:"migrationData"`
	Metadata      *ContractMetadata `json:"metadata"`
}

// UpgradeContractResponse represents a contract upgrade response
type UpgradeContractResponse struct {
	TransactionID string `json:"transactionId"`
	Success       bool   `json:"success"`
	GasUsed       uint64 `json:"gasUsed"`
}

// ContractInfoResponse represents contract information
type ContractInfoResponse struct {
	ContractID    string            `json:"contractId"`
	Runtime       string            `json:"runtime"`
	Owner         string            `json:"owner"`
	Version       string            `json:"version"`
	DeployedAt    time.Time         `json:"deployedAt"`
	Active        bool              `json:"active"`
	StateHash     string            `json:"stateHash"`
	Metadata      *ContractMetadata `json:"metadata"`
	ResourceUsage *ResourceUsage    `json:"resourceUsage,omitempty"`
}

// EventResponse represents a contract event
type EventResponse struct {
	Name       string           `json:"name"`
	Parameters []EventParameter `json:"parameters"`
	Data       string           `json:"data"`
}

// RuntimeInfoResponse represents runtime information
type RuntimeInfoResponse struct {
	Type         string   `json:"type"`
	Name         string   `json:"name"`
	Description  string   `json:"description"`
	Version      string   `json:"version"`
	Capabilities []string `json:"capabilities"`
	Active       bool     `json:"active"`
}

// DeployContract handles contract deployment for any runtime
func (h *HybridVMHandler) DeployContract(w http.ResponseWriter, r *http.Request) {
	// Parse request
	var req HybridDeployRequest
	if err := h.decodeJSONBody(w, r, &req); err != nil {
		h.sendError(w, http.StatusBadRequest, "Invalid request body", err)
		return
	}

	// Validate request
	if req.Language == "" {
		h.sendError(w, http.StatusBadRequest, "Missing language", nil)
		return
	}
	if req.Code == "" {
		h.sendError(w, http.StatusBadRequest, "Missing code", nil)
		return
	}

	// Decode code
	code, err := h.decodeCode(req.Code)
	if err != nil {
		h.sendError(w, http.StatusBadRequest, "Invalid code encoding", err)
		return
	}

	// Get caller from header or use default
	caller := r.Header.Get("X-Caller-Address")
	if caller == "" {
		caller = "default-deployer"
	}

	// Create metadata for transaction
	metadata := &common.TransactionMetadata{
		Category:    "deploy",
		Tags:        []string{"contract", "deployment"},
		Description: fmt.Sprintf("Deploy %s contract", req.Language),
		Purpose:     "contract_deployment",
	}

	tx := common.Transaction{
		ID:        common.GenerateUniqueID(),
		Sender:    caller,
		Receiver:  "", // No receiver for deployment
		Amount:    req.InitialValue,
		Timestamp: consensus.ConsensusUnix(),
		Status:    "pending",
		Metadata:  metadata,
		Data:      code, // Add contract code as transaction data
	}

	// Process through hybrid processor
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := h.hybridProcessor.ProcessTransaction(ctx, tx)
	if err != nil {
		h.sendError(w, http.StatusInternalServerError, "Deployment failed", err)
		return
	}

	// Check result type and extract deployment response
	var deployResp *deploy.DeploymentResponse
	if result.Result.DeploymentInfo != nil {
		// Create deployment response from result
		deployResp = &deploy.DeploymentResponse{
			ContractID:  result.Result.DeploymentInfo.ContractID,
			RuntimeType: runtime.RuntimeTypeEVM, // Default to EVM
		}

		// Prepare response
		response := HybridDeployResponse{
			TransactionID: result.TransactionID,
			ContractID:    deployResp.ContractID,
			Runtime:       string(deployResp.RuntimeType),
			GasUsed:       result.GasUsed,
			Events:        h.convertEvents(result.Events),
			Metadata:      nil, // Will be filled if available
		}

		h.sendJSON(w, http.StatusCreated, response)
	} else {
		h.sendError(w, http.StatusInternalServerError, "Invalid deployment response", nil)
		return
	}
}

// ExecuteContract handles contract execution
func (h *HybridVMHandler) ExecuteContract(w http.ResponseWriter, r *http.Request) {
	// Parse request
	var req HybridExecuteRequest
	if err := h.decodeJSONBody(w, r, &req); err != nil {
		h.sendError(w, http.StatusBadRequest, "Invalid request body", err)
		return
	}

	// Validate request
	if req.ContractID == "" {
		h.sendError(w, http.StatusBadRequest, "Missing contract ID", nil)
		return
	}
	if req.Function == "" {
		h.sendError(w, http.StatusBadRequest, "Missing function", nil)
		return
	}

	// Get caller from header or use default
	caller := r.Header.Get("X-Caller-Address")
	if caller == "" {
		caller = "default-caller"
	}

	// Create metadata for transaction
	metadata := &common.TransactionMetadata{
		Category:    "execute",
		Tags:        []string{"contract", "execution"},
		Description: fmt.Sprintf("Execute %s on contract %s", req.Function, req.ContractID),
		Purpose:     "contract_execution",
		Reference:   req.ContractID,
	}

	tx := common.Transaction{
		ID:              common.GenerateUniqueID(),
		Sender:          caller,
		Receiver:        req.ContractID,
		Amount:          req.Value,
		SmartContractID: req.ContractID,
		Timestamp:       consensus.ConsensusUnix(),
		Status:          "pending",
		Metadata:        metadata,
		Data:            []byte(req.Function), // Add function name as data
	}

	// Process through hybrid processor
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := h.hybridProcessor.ProcessTransaction(ctx, tx)
	if err != nil {
		h.sendError(w, http.StatusInternalServerError, "Execution failed", err)
		return
	}

	// Prepare response
	response := HybridExecuteResponse{
		TransactionID: result.TransactionID,
		Success:       result.Success,
		Result:        result.Result,
		GasUsed:       result.GasUsed,
		Events:        h.convertEvents(result.Events),
		Error:         result.Error,
	}

	h.sendJSON(w, http.StatusOK, response)
}

// GetContractInfo retrieves contract information
func (h *HybridVMHandler) GetContractInfo(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	contractID := vars["id"]

	if contractID == "" {
		h.sendError(w, http.StatusBadRequest, "Missing contract ID", nil)
		return
	}

	// Get contract info from runtime manager
	info, err := h.runtimeManager.GetContractInfo(contractID)
	if err != nil {
		h.sendError(w, http.StatusNotFound, "Contract not found", err)
		return
	}

	// Prepare response
	response := ContractInfoResponse{
		ContractID: info.ContractID,
		Runtime:    string(info.Runtime),
		Owner:      info.Owner,
		Version:    info.Version,
		DeployedAt: info.DeployedAt,
		Active:     info.Active,
		StateHash:  info.StateHash,
		Metadata:   nil, // Will be filled if available
	}

	// Add metadata if available - info.Metadata is RuntimeMetadata struct
	response.Metadata = &ContractMetadata{
		Name:        info.Metadata.Name,
		Description: info.Metadata.Description,
		Version:     info.Metadata.Version,
		Author:      info.Metadata.Author,
		License:     info.Metadata.License,
	}

	// Resource usage tracking is not available in ContractInfo
	// Leave response.ResourceUsage as nil

	h.sendJSON(w, http.StatusOK, response)
}

// UpgradeContract handles contract upgrades
func (h *HybridVMHandler) UpgradeContract(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	contractID := vars["id"]

	if contractID == "" {
		h.sendError(w, http.StatusBadRequest, "Missing contract ID", nil)
		return
	}

	// Parse request
	var req UpgradeContractRequest
	if err := h.decodeJSONBody(w, r, &req); err != nil {
		h.sendError(w, http.StatusBadRequest, "Invalid request body", err)
		return
	}

	// Validate request
	if req.NewVersion == "" {
		h.sendError(w, http.StatusBadRequest, "Missing new version", nil)
		return
	}
	if req.NewCode == "" {
		h.sendError(w, http.StatusBadRequest, "Missing new code", nil)
		return
	}

	// Decode code
	code, err := h.decodeCode(req.NewCode)
	if err != nil {
		h.sendError(w, http.StatusBadRequest, "Invalid code encoding", err)
		return
	}

	// Get caller from header
	caller := r.Header.Get("X-Caller-Address")
	if caller == "" {
		h.sendError(w, http.StatusUnauthorized, "Missing caller address", nil)
		return
	}

	// Create transaction for upgrade
	metadata := &common.TransactionMetadata{
		Category:    "upgrade",
		Tags:        []string{"contract", "upgrade"},
		Description: fmt.Sprintf("Upgrade contract %s to version %s", contractID, req.NewVersion),
		Purpose:     "contract_upgrade",
		Reference:   contractID,
	}
	// Properties is in the API ContractMetadata struct, not in TransactionMetadata
	// If we need to store extra properties, we could encode them in the Description

	tx := common.Transaction{
		ID:              common.GenerateUniqueID(),
		Sender:          caller,
		SmartContractID: contractID,
		Timestamp:       consensus.ConsensusUnix(),
		Status:          "pending",
		Metadata:        metadata,
		Data:            code, // Use the new code as transaction data
	}

	// Process through hybrid processor
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := h.hybridProcessor.ProcessTransaction(ctx, tx)
	if err != nil {
		h.sendError(w, http.StatusInternalServerError, "Upgrade failed", err)
		return
	}

	// Send success response
	response := UpgradeContractResponse{
		TransactionID: result.TransactionID,
		Success:       result.Success,
		GasUsed:       result.GasUsed,
	}
	h.sendJSON(w, http.StatusOK, response)
}

// GetContractState retrieves contract state
func (h *HybridVMHandler) GetContractState(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	contractID := vars["id"]

	if contractID == "" {
		h.sendError(w, http.StatusBadRequest, "Missing contract ID", nil)
		return
	}

	// Get key from query params (optional)
	key := r.URL.Query().Get("key")

	// Get state from runtime manager
	state, err := h.runtimeManager.GetContractState(contractID, key)
	if err != nil {
		h.sendError(w, http.StatusNotFound, "State not found", err)
		return
	}

	response := ContractStateResponse{
		ContractID: contractID,
		Key:        key,
		State:      state,
	}
	h.sendJSON(w, http.StatusOK, response)
}

// ListRuntimes lists available runtimes
func (h *HybridVMHandler) ListRuntimes(w http.ResponseWriter, r *http.Request) {
	runtimes := h.runtimeManager.ListRuntimes()

	response := make([]RuntimeInfoResponse, 0, len(runtimes))
	for _, rt := range runtimes {
		info := RuntimeInfoResponse{
			Type:   string(rt),
			Active: h.runtimeManager.IsRuntimeActive(rt),
		}

		// Get metadata
		if metadata, exists := runtime.GetRuntimeMetadata(rt); exists {
			info.Name = metadata.Name
			info.Description = metadata.Description
			info.Version = metadata.Version
			info.Capabilities = h.convertCapabilities(metadata.Capabilities)
		}

		response = append(response, info)
	}

	h.sendJSON(w, http.StatusOK, response)
}

// GetRuntimeInfo gets information about a specific runtime
func (h *HybridVMHandler) GetRuntimeInfo(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	runtimeType := vars["type"]

	rt := runtime.RuntimeType(runtimeType)

	// Check if runtime exists
	if !h.runtimeManager.HasRuntime(rt) {
		h.sendError(w, http.StatusNotFound, "Runtime not found", nil)
		return
	}

	info := RuntimeInfoResponse{
		Type:   string(rt),
		Active: h.runtimeManager.IsRuntimeActive(rt),
	}

	// Get metadata
	if metadata, exists := runtime.GetRuntimeMetadata(rt); exists {
		info.Name = metadata.Name
		info.Description = metadata.Description
		info.Version = metadata.Version
		info.Capabilities = h.convertCapabilities(metadata.Capabilities)
	}

	h.sendJSON(w, http.StatusOK, info)
}

// GetTransactionStatus gets the status of a transaction
func (h *HybridVMHandler) GetTransactionStatus(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	txID := vars["id"]

	if txID == "" {
		h.sendError(w, http.StatusBadRequest, "Missing transaction ID", nil)
		return
	}

	// Get transaction from ledger
	tx, err := h.ledger.GetTransaction(txID)
	if err != nil {
		h.sendError(w, http.StatusNotFound, "Transaction not found", err)
		return
	}

	response := TransactionStatusInfo{
		TransactionID: tx.ID,
		Status:        tx.Status,
		Timestamp:     tx.Timestamp,
	}
	h.sendJSON(w, http.StatusOK, response)
}

// GetTransactionReceipt gets the receipt of a transaction
func (h *HybridVMHandler) GetTransactionReceipt(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	txID := vars["id"]

	if txID == "" {
		h.sendError(w, http.StatusBadRequest, "Missing transaction ID", nil)
		return
	}

	// Get receipt from state store
	receipt, err := h.stateStore.GetReceipt(txID)
	if err != nil {
		h.sendError(w, http.StatusNotFound, "Receipt not found", err)
		return
	}

	h.sendJSON(w, http.StatusOK, receipt)
}

// GetMetrics gets hybrid VM metrics
func (h *HybridVMHandler) GetMetrics(w http.ResponseWriter, r *http.Request) {
	metrics := h.hybridProcessor.GetMetrics()

	response := MetricsResponse{
		TotalProcessed:      int64(metrics.TotalProcessed),
		TotalFailed:         int64(metrics.TotalFailed),
		AverageGasUsed:      metrics.AverageGasUsed,
		AverageProcessTime:  metrics.AverageProcessTime.String(),
		RuntimeDistribution: h.convertRuntimeDistribution(metrics.RuntimeDistribution),
	}

	h.sendJSON(w, http.StatusOK, response)
}

// Helper methods

func (h *HybridVMHandler) decodeJSONBody(w http.ResponseWriter, r *http.Request, dst interface{}) error {
	if r.Body == nil {
		return fmt.Errorf("missing request body")
	}

	// Limit body size
	r.Body = http.MaxBytesReader(w, r.Body, h.bodyLimit)

	// Decode JSON
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()

	err := dec.Decode(dst)
	if err != nil {
		return err
	}

	// Check for extra data
	err = dec.Decode(&struct{}{})
	if err != io.EOF {
		return fmt.Errorf("request body must contain only one JSON object")
	}

	return nil
}

func (h *HybridVMHandler) decodeCode(code string) ([]byte, error) {
	// Try hex decoding first (with or without 0x prefix)
	if strings.HasPrefix(code, "0x") {
		return hex.DecodeString(code[2:])
	}

	// Try as raw hex
	if decoded, err := hex.DecodeString(code); err == nil {
		return decoded, nil
	}

	// Assume it's base64
	// In production, you might want to add base64 decoding here

	// Return as raw bytes if nothing else works
	return []byte(code), nil
}

func (h *HybridVMHandler) convertEvents(events []runtime.ContractEvent) []EventResponse {
	result := make([]EventResponse, len(events))
	for i, evt := range events {
		params := make([]EventParameter, 0)
		// Convert event parameters properly
		// ContractParameters is a struct with typed fields
		// Convert string params
		for name, value := range evt.Parameters.StringParams {
			params = append(params, EventParameter{
				Name:  name,
				Type:  "string",
				Value: value,
			})
		}
		// Convert int params
		for name, value := range evt.Parameters.IntParams {
			params = append(params, EventParameter{
				Name:  name,
				Type:  "int",
				Value: value,
			})
		}
		// Convert bool params
		for name, value := range evt.Parameters.BoolParams {
			params = append(params, EventParameter{
				Name:  name,
				Type:  "bool",
				Value: value,
			})
		}
		// Convert float params
		for name, value := range evt.Parameters.FloatParams {
			params = append(params, EventParameter{
				Name:  name,
				Type:  "float",
				Value: value,
			})
		}
		result[i] = EventResponse{
			Name:       evt.Name,
			Parameters: params,
			Data:       string(evt.Data),
		}
	}
	return result
}

func (h *HybridVMHandler) convertCapabilities(caps []runtime.RuntimeCapability) []string {
	result := make([]string, len(caps))
	for i, cap := range caps {
		result[i] = string(cap)
	}
	return result
}

func (h *HybridVMHandler) convertRuntimeDistribution(dist map[runtime.RuntimeType]uint64) map[string]int64 {
	result := make(map[string]int64)
	for rt, count := range dist {
		result[string(rt)] = int64(count)
	}
	return result
}

func (h *HybridVMHandler) sendJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	if err := json.NewEncoder(w).Encode(data); err != nil {
		h.logger.WithError(err).Error("Failed to encode JSON response")
	}
}

func (h *HybridVMHandler) sendError(w http.ResponseWriter, status int, message string, err error) {
	response := map[string]interface{}{
		"error": message,
	}

	if err != nil {
		response["details"] = err.Error()
	}

	h.sendJSON(w, status, response)
}

// Helper functions
func getStringValue(m map[string]interface{}, key string) string {
	if v, ok := m[key].(string); ok {
		return v
	}
	return ""
}

func getInt64Value(m map[string]interface{}, key string) int64 {
	if v, ok := m[key].(int64); ok {
		return v
	}
	if v, ok := m[key].(int); ok {
		return int64(v)
	}
	if v, ok := m[key].(float64); ok {
		return int64(v)
	}
	return 0
}
