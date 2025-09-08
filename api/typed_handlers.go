// Package api provides typed HTTP handlers for the blockchain API
package api

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"diamante/common"
	"diamante/config"
	"diamante/consensus"
	"diamante/storage"
	"diamante/transaction"
	"diamante/types"

	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"
	"golang.org/x/time/rate"
)

// TypedAPIServer handles typed HTTP requests
type TypedAPIServer struct {
	config       *config.Config
	logger       *logrus.Logger
	txPool       *transaction.TypedPool
	storage      *storage.TypedMongoAdapter
	router       *mux.Router
	startTime    time.Time
	rateLimiters map[string]*rate.Limiter
	mu           sync.Mutex
}

// TypedAPIResponse wraps API responses with metadata
type TypedAPIResponse struct {
	Success   bool              `json:"success"`
	Data      *types.Value      `json:"data,omitempty"`
	Error     string            `json:"error,omitempty"`
	Metadata  map[string]string `json:"metadata,omitempty"`
	Timestamp int64             `json:"timestamp"`
}

// NewTypedAPIServer creates a new typed API server
func NewTypedAPIServer(config *config.Config, txPool *transaction.TypedPool, storage *storage.TypedMongoAdapter, logger *logrus.Logger) *TypedAPIServer {
	if logger == nil {
		logger = logrus.New()
	}

	server := &TypedAPIServer{
		config:       config,
		logger:       logger,
		txPool:       txPool,
		storage:      storage,
		router:       mux.NewRouter(),
		rateLimiters: make(map[string]*rate.Limiter),
		startTime:    consensus.ConsensusNow(),
	}

	server.setupRoutes()
	return server
}

// setupRoutes configures API routes
func (s *TypedAPIServer) setupRoutes() {
	// Health check
	s.router.HandleFunc("/health", s.handleHealth).Methods("GET")

	// Node info
	s.router.HandleFunc("/node/info", s.handleNodeInfo).Methods("GET")
	s.router.HandleFunc("/node/metrics", s.handleNodeMetrics).Methods("GET")

	// Blocks
	s.router.HandleFunc("/blocks/{number}", s.handleGetBlock).Methods("GET")
	s.router.HandleFunc("/blocks/latest", s.handleGetLatestBlock).Methods("GET")

	// Transactions
	s.router.HandleFunc("/transactions", s.handleSubmitTransaction).Methods("POST")
	s.router.HandleFunc("/transactions/{id}", s.handleGetTransaction).Methods("GET")
	s.router.HandleFunc("/transactions/{id}/receipt", s.handleGetTransactionReceipt).Methods("GET")
	s.router.HandleFunc("/transactions/query", s.handleQueryTransactions).Methods("POST")

	// State
	s.router.HandleFunc("/state/{key}", s.handleGetState).Methods("GET")

	// Pool
	s.router.HandleFunc("/pool/pending", s.handleGetPendingTransactions).Methods("GET")
	s.router.HandleFunc("/pool/metrics", s.handleGetPoolMetrics).Methods("GET")

	// zkEVM endpoints
	s.router.HandleFunc("/proof/verify", s.handleProofVerification).Methods("POST")
	s.router.HandleFunc("/zkevm/metrics", s.handleZKEVMMetrics).Methods("GET")
	s.router.HandleFunc("/batch/status/{batchId}", s.handleBatchProofStatus).Methods("GET")

	// Apply middleware
	s.router.Use(s.loggingMiddleware)
	s.router.Use(s.corsMiddleware)
	s.router.Use(s.rateLimitMiddleware)
}

// handleHealth handles health check requests
func (s *TypedAPIServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	health := map[string]string{
		"status":  "ok",
		"version": "1.0.0",
		"uptime":  time.Since(s.startTime).String(),
	}

	healthValue, _ := types.JSONToValue(health)

	s.sendResponse(w, &TypedAPIResponse{
		Success: true,
		Data:    healthValue,
		Metadata: map[string]string{
			"service": "diamante-api",
		},
	})
}

// handleNodeInfo handles node information requests
func (s *TypedAPIServer) handleNodeInfo(w http.ResponseWriter, r *http.Request) {
	info := NodeInfo{
		ChainID:   "diamante-1",
		ID:        "diamante-node",
		Version:   "1.0.0",
		NetworkID: "mainnet", // Default network ID
	}

	infoValue, _ := types.JSONToValue(info)

	s.sendResponse(w, &TypedAPIResponse{
		Success: true,
		Data:    infoValue,
	})
}

// StorageMetricsInfo represents storage metrics for API response
type StorageMetricsInfo struct {
	ReadOps         uint64 `json:"read_ops"`
	WriteOps        uint64 `json:"write_ops"`
	TotalSize       uint64 `json:"total_size"`
	KeyCount        uint64 `json:"key_count"`
	AvgReadLatency  string `json:"avg_read_latency"`
	AvgWriteLatency string `json:"avg_write_latency"`
}

// PoolMetricsInfo represents transaction pool metrics for API response
type PoolMetricsInfo struct {
	TotalReceived     uint64 `json:"total_received"`
	TotalProcessed    uint64 `json:"total_processed"`
	TotalFailed       uint64 `json:"total_failed"`
	PoolSize          uint64 `json:"pool_size"`
	QueuedCount       uint64 `json:"queued_count"`
	AvgProcessingTime string `json:"avg_processing_time"`
}

// NodeMetricsInfo represents combined node metrics
type NodeMetricsInfo struct {
	Storage         StorageMetricsInfo `json:"storage"`
	TransactionPool PoolMetricsInfo    `json:"transaction_pool"`
}

// handleNodeMetrics handles node metrics requests
func (s *TypedAPIServer) handleNodeMetrics(w http.ResponseWriter, r *http.Request) {
	// Get storage metrics
	storageMetrics := s.storage.GetMetrics()

	// Get pool metrics
	poolMetrics := s.txPool.GetMetrics()

	// Combine metrics
	metrics := NodeMetricsInfo{
		Storage: StorageMetricsInfo{
			ReadOps:         storageMetrics.ReadOps,
			WriteOps:        storageMetrics.WriteOps,
			TotalSize:       storageMetrics.TotalSize,
			KeyCount:        storageMetrics.KeyCount,
			AvgReadLatency:  storageMetrics.AvgReadLatency.String(),
			AvgWriteLatency: storageMetrics.AvgWriteLatency.String(),
		},
		TransactionPool: PoolMetricsInfo{
			TotalReceived:     poolMetrics.TotalReceived,
			TotalProcessed:    poolMetrics.TotalProcessed,
			TotalFailed:       poolMetrics.TotalFailed,
			PoolSize:          poolMetrics.PoolSize,
			QueuedCount:       poolMetrics.QueuedCount,
			AvgProcessingTime: poolMetrics.AvgProcessingTime.String(),
		},
	}

	metricsValue, _ := types.JSONToValue(metrics)

	s.sendResponse(w, &TypedAPIResponse{
		Success: true,
		Data:    metricsValue,
	})
}

// handleGetBlock handles block retrieval requests
func (s *TypedAPIServer) handleGetBlock(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	numberStr := vars["number"]

	number, err := strconv.ParseUint(numberStr, 10, 64)
	if err != nil {
		s.sendError(w, http.StatusBadRequest, "Invalid block number")
		return
	}

	block, err := s.storage.GetBlock(number)
	if err != nil {
		s.sendError(w, http.StatusNotFound, "Block not found")
		return
	}

	// Convert to enhanced response with zkEVM data
	blockResp := ConvertTypedBlockToResponse(block)
	blockValue, _ := types.JSONToValue(blockResp)

	s.sendResponse(w, &TypedAPIResponse{
		Success: true,
		Data:    blockValue,
	})
}

// handleGetLatestBlock handles getting the latest block
func (s *TypedAPIServer) handleGetLatestBlock(w http.ResponseWriter, r *http.Request) {
	// Get the actual latest block height from storage
	latestHeight, err := s.storage.GetLatestBlockHeight()
	if err != nil {
		s.sendError(w, http.StatusInternalServerError, "Failed to get latest block height")
		return
	}

	// Get the block at the latest height
	block, err := s.storage.GetBlock(latestHeight)
	if err != nil {
		s.sendError(w, http.StatusNotFound, "Latest block not found")
		return
	}

	// Convert to enhanced response with zkEVM data
	blockResp := ConvertTypedBlockToResponse(block)
	blockValue, _ := types.JSONToValue(blockResp)

	s.sendResponse(w, &TypedAPIResponse{
		Success: true,
		Data:    blockValue,
	})
}

// handleSubmitTransaction handles transaction submission
func (s *TypedAPIServer) handleSubmitTransaction(w http.ResponseWriter, r *http.Request) {
	var req TransactionSubmitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.sendError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	// Create typed transaction using available fields from TransactionSubmitRequest
	tx := &types.TypedTransaction{
		Type:      types.TransactionTypeTransfer, // Default to transfer
		From:      req.Sender,
		To:        req.Receiver,
		Value:     uint64(req.Amount * 1e18), // Convert float to wei
		GasLimit:  21000,                     // Default gas limit
		GasPrice:  uint64(req.Fee * 1e9),     // Convert fee to gwei
		Nonce:     0,                         // Will be set by transaction pool
		Signature: []byte{},                  // Will be signed later
		Timestamp: common.ConsensusUnix(),
		Status:    types.TransactionStatusPending,
		Priority:  types.TransactionPriorityNormal,
	}

	// Handle transaction data based on type
	if tx.Type == types.TransactionTypeContractDeploy && req.Data != "" {
		// Parse contract deployment data - req.Data is a string
		tx.Data = &types.TypedTransactionData{
			ContractDeploy: &types.ContractDeployData{
				Runtime:  "wasm",           // Default runtime
				ByteCode: []byte(req.Data), // Convert string to bytes
			},
		}
	} else if tx.Type == types.TransactionTypeContractCall && req.Data != "" {
		// Parse contract call data - req.Data is a string
		tx.Data = &types.TypedTransactionData{
			ContractCall: &types.ContractCallData{
				ContractAddress: req.Receiver, // Use receiver as contract address
				Method:          "execute",    // Default method
			},
		}
	}

	// Generate transaction ID
	tx.ID = generateTypedTransactionID(tx)

	// Add to pool
	if err := s.txPool.AddTransaction(tx); err != nil {
		s.sendError(w, http.StatusBadRequest, err.Error())
		return
	}

	// Return transaction ID
	result := map[string]string{
		"transaction_id": tx.ID,
		"status":         "pending",
	}

	resultValue, _ := types.JSONToValue(result)

	s.sendResponse(w, &TypedAPIResponse{
		Success: true,
		Data:    resultValue,
		Metadata: map[string]string{
			"pool_size": strconv.FormatUint(s.txPool.GetMetrics().PoolSize, 10),
		},
	})
}

// handleGetTransaction handles transaction retrieval
func (s *TypedAPIServer) handleGetTransaction(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	txID := vars["id"]

	tx, exists := s.txPool.GetTransaction(txID)
	if !exists {
		// Try storage
		filter := &types.TransactionFilter{
			Type:   nil,
			Status: nil,
		}

		txs, err := s.storage.QueryTransactions(filter)
		if err != nil || len(txs) == 0 {
			s.sendError(w, http.StatusNotFound, "Transaction not found")
			return
		}

		// Find specific transaction
		for _, t := range txs {
			if t.ID == txID {
				tx = t
				break
			}
		}

		if tx == nil {
			s.sendError(w, http.StatusNotFound, "Transaction not found")
			return
		}
	}

	txValue, _ := types.JSONToValue(tx)

	s.sendResponse(w, &TypedAPIResponse{
		Success: true,
		Data:    txValue,
	})
}

// handleGetTransactionReceipt handles transaction receipt retrieval with zkEVM proof data
func (s *TypedAPIServer) handleGetTransactionReceipt(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	txID := vars["id"]

	// Get transaction from pool or storage
	tx, exists := s.txPool.GetTransaction(txID)
	if !exists {
		// Try storage
		filter := &types.TransactionFilter{
			Type:   nil,
			Status: nil,
		}

		txs, err := s.storage.QueryTransactions(filter)
		if err != nil || len(txs) == 0 {
			s.sendError(w, http.StatusNotFound, "Transaction not found")
			return
		}

		// Find specific transaction
		for _, t := range txs {
			if t.ID == txID {
				tx = t
				break
			}
		}

		if tx == nil {
			s.sendError(w, http.StatusNotFound, "Transaction not found")
			return
		}
	}

	// Build receipt response
	receipt := ZKTransactionReceiptResponse{
		ZKTransactionResponse: ZKTransactionResponse{
			ID:          tx.ID,
			Sender:      tx.From,
			Receiver:    tx.To,
			Amount:      float64(tx.Value) / 1e18,   // Convert wei to tokens
			Fee:         float64(tx.GasPrice) / 1e9, // Convert to gwei
			Timestamp:   tx.Timestamp,
			Status:      string(tx.Status),
			BlockHeight: 0,     // Block height is not stored with transaction
			GasUsed:     21000, // Default gas for simple transfer
		},
		Success: tx.Status == types.TransactionStatusExecuted,
	}

	// Add execution proof if transaction was executed with zkEVM
	if tx.Status == types.TransactionStatusExecuted {
		// Check if zkEVM is enabled (defaulting to false for now)
		zkevmEnabled := false

		if zkevmEnabled {
			// Generate deterministic proof based on transaction data
			proofData := fmt.Sprintf("%s:%s:%d:%d", tx.ID, tx.From, tx.Timestamp, 21000)
			proofHash := common.HashData([]byte(proofData))

			receipt.ExecutionProof = &ZKProofResponse{
				Type:         "single_execution",
				Proof:        proofHash[:64], // Use first 32 bytes as hex string
				PublicInputs: base64.StdEncoding.EncodeToString([]byte(tx.ID)),
				Verified:     true,
				GeneratedAt:  tx.Timestamp,
			}
		}
	}

	// Set error if transaction failed
	if tx.Status == types.TransactionStatusFailed {
		receipt.Error = "Transaction execution failed"
	}

	receiptValue, _ := types.JSONToValue(receipt)

	s.sendResponse(w, &TypedAPIResponse{
		Success: true,
		Data:    receiptValue,
	})
}

// handleQueryTransactions handles transaction queries
func (s *TypedAPIServer) handleQueryTransactions(w http.ResponseWriter, r *http.Request) {
	var req TransactionQueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.sendError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	// Build filter
	filter := &types.TransactionFilter{
		From:      req.From,
		To:        req.To,
		StartTime: req.StartTime,
		EndTime:   req.EndTime,
		MinValue:  req.MinValue,
		MaxValue:  req.MaxValue,
	}

	if req.Type != "" {
		txType := parseTransactionType(req.Type)
		filter.Type = &txType
	}

	if req.Status != "" {
		status := parseTransactionStatus(req.Status)
		filter.Status = &status
	}

	// Query transactions
	txs, err := s.storage.QueryTransactions(filter)
	if err != nil {
		s.sendError(w, http.StatusInternalServerError, "Query failed")
		return
	}

	// TransactionListResponse represents a list of transactions
	type TransactionListResponse struct {
		Transactions []*types.TypedTransaction `json:"transactions"`
		Count        int                       `json:"count"`
	}

	// Convert to response
	result := TransactionListResponse{
		Transactions: txs,
		Count:        len(txs),
	}

	resultValue, _ := types.JSONToValue(result)

	s.sendResponse(w, &TypedAPIResponse{
		Success: true,
		Data:    resultValue,
	})
}

// handleGetState handles state retrieval
func (s *TypedAPIServer) handleGetState(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	state, err := s.storage.GetState(key)
	if err != nil {
		s.sendError(w, http.StatusNotFound, "State not found")
		return
	}

	// StateResponse represents state query response
	type StateResponse struct {
		Key   string       `json:"key"`
		Value *types.Value `json:"value"`
		Proof [][]byte     `json:"proof,omitempty"`
	}

	// Convert state entry to response
	result := StateResponse{
		Key:   state.Key,
		Value: state.Value,
		Proof: state.Proof,
	}

	resultValue, _ := types.JSONToValue(result)

	s.sendResponse(w, &TypedAPIResponse{
		Success: true,
		Data:    resultValue,
		Metadata: map[string]string{
			"state_root": state.StateRoot,
		},
	})
}

// handleGetPendingTransactions handles pending transaction retrieval
func (s *TypedAPIServer) handleGetPendingTransactions(w http.ResponseWriter, r *http.Request) {
	// Parse limit
	limitStr := r.URL.Query().Get("limit")
	limit := 100 // default

	if limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 {
			limit = l
		}
	}

	// Get pending transactions
	pending := s.txPool.GetPendingTransactions(limit)

	// PendingTransactionsResponse represents pending transactions response
	type PendingTransactionsResponse struct {
		Transactions []*types.TypedTransaction `json:"transactions"`
		Count        int                       `json:"count"`
		PoolSize     uint64                    `json:"pool_size"`
	}

	result := PendingTransactionsResponse{
		Transactions: pending,
		Count:        len(pending),
		PoolSize:     s.txPool.GetMetrics().PoolSize,
	}

	resultValue, _ := types.JSONToValue(result)

	s.sendResponse(w, &TypedAPIResponse{
		Success: true,
		Data:    resultValue,
	})
}

// handleGetPoolMetrics handles pool metrics retrieval
func (s *TypedAPIServer) handleGetPoolMetrics(w http.ResponseWriter, r *http.Request) {
	metrics := s.txPool.GetMetrics()

	metricsValue, _ := types.JSONToValue(metrics)

	s.sendResponse(w, &TypedAPIResponse{
		Success: true,
		Data:    metricsValue,
	})
}

// handleProofVerification handles zkEVM proof verification requests
func (s *TypedAPIServer) handleProofVerification(w http.ResponseWriter, r *http.Request) {
	// zkEVM proof verification is not implemented in this version
	s.sendError(w, http.StatusNotImplemented, "zkEVM proof verification is not implemented")
}

// handleZKEVMMetrics handles zkEVM metrics requests
func (s *TypedAPIServer) handleZKEVMMetrics(w http.ResponseWriter, r *http.Request) {
	// zkEVM is not implemented in this version
	s.sendError(w, http.StatusNotImplemented, "zkEVM metrics are not available")
	return
}

// handleBatchProofStatus handles batch proof status requests
func (s *TypedAPIServer) handleBatchProofStatus(w http.ResponseWriter, r *http.Request) {
	// zkEVM batch proof is not implemented in this version
	s.sendError(w, http.StatusNotImplemented, "zkEVM batch proof status is not available")
	return
}

// Middleware functions

func (s *TypedAPIServer) loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := common.ConsensusNow()

		// Create wrapped writer to capture status code
		wrapped := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}

		next.ServeHTTP(wrapped, r)

		s.logger.WithFields(logrus.Fields{
			"method":     r.Method,
			"path":       r.URL.Path,
			"status":     wrapped.statusCode,
			"duration":   time.Since(start),
			"remote":     r.RemoteAddr,
			"user_agent": r.UserAgent(),
		}).Info("API request")
	})
}

func (s *TypedAPIServer) corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Set CORS headers
		origin := r.Header.Get("Origin")
		if s.isAllowedOrigin(origin) {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-API-Key")
			w.Header().Set("Access-Control-Max-Age", "3600")
		}

		// Handle preflight
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}

func (s *TypedAPIServer) rateLimitMiddleware(next http.Handler) http.Handler {
	// Implement token bucket rate limiting per IP address
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		clientIP := getClientIP(r)

		// Get or create rate limiter for this IP
		s.mu.Lock()
		limiter, exists := s.rateLimiters[clientIP]
		if !exists {
			// Create new rate limiter: 100 requests per minute with burst of 20
			limiter = rate.NewLimiter(rate.Every(600*time.Millisecond), 20)
			s.rateLimiters[clientIP] = limiter
		}
		s.mu.Unlock()

		// Check rate limit
		if !limiter.Allow() {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusTooManyRequests)
			json.NewEncoder(w).Encode(map[string]string{
				"error": "rate limit exceeded",
			})
			return
		}

		next.ServeHTTP(w, r)
	})
}

// Helper functions

// getClientIP extracts the client IP address from the request
func getClientIP(r *http.Request) string {
	// Check X-Forwarded-For header
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		// Take the first IP in the chain
		ips := strings.Split(xff, ",")
		if len(ips) > 0 {
			ip := strings.TrimSpace(ips[0])
			if parsedIP := net.ParseIP(ip); parsedIP != nil {
				return ip
			}
		}
	}

	// Check X-Real-IP header
	if xri := r.Header.Get("X-Real-IP"); xri != "" {
		if parsedIP := net.ParseIP(xri); parsedIP != nil {
			return xri
		}
	}

	// Fall back to RemoteAddr
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func (s *TypedAPIServer) sendResponse(w http.ResponseWriter, resp *TypedAPIResponse) {
	resp.Timestamp = common.ConsensusUnix()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	if err := json.NewEncoder(w).Encode(resp); err != nil {
		s.logger.WithError(err).Error("Failed to encode response")
	}
}

func (s *TypedAPIServer) sendError(w http.ResponseWriter, status int, message string) {
	resp := &TypedAPIResponse{
		Success:   false,
		Error:     message,
		Timestamp: common.ConsensusUnix(),
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	if err := json.NewEncoder(w).Encode(resp); err != nil {
		s.logger.WithError(err).Error("Failed to encode error response")
	}
}

func (s *TypedAPIServer) isAllowedOrigin(origin string) bool {
	// Get allowed origins from environment or config
	allowedOrigins := s.getAllowedOrigins()

	for _, allowed := range allowedOrigins {
		if allowed == "*" || allowed == origin {
			return true
		}
	}
	return false
}

// getAllowedOrigins returns the allowed CORS origins from environment or defaults
func (s *TypedAPIServer) getAllowedOrigins() []string {
	// Get from environment variable
	corsOrigins := os.Getenv("CORS_ORIGINS")
	if corsOrigins != "" {
		return strings.Split(corsOrigins, ",")
	}

	// Check server config if available
	if s.config != nil && s.config.API.CORS.AllowedOrigins != nil && len(s.config.API.CORS.AllowedOrigins) > 0 {
		return s.config.API.CORS.AllowedOrigins
	}

	// Safe production defaults (no wildcard)
	return []string{"http://localhost:3000", "https://localhost:3000"}
}

// Request/Response types - Using TransactionSubmitRequest from transactions.go

type TxData struct {
	Runtime  string            `json:"runtime,omitempty"`
	ByteCode []byte            `json:"byte_code,omitempty"`
	Method   string            `json:"method,omitempty"`
	Args     []json.RawMessage `json:"args,omitempty"`
}

type TransactionQueryRequest struct {
	Type      string `json:"type,omitempty"`
	Status    string `json:"status,omitempty"`
	From      string `json:"from,omitempty"`
	To        string `json:"to,omitempty"`
	StartTime int64  `json:"start_time,omitempty"`
	EndTime   int64  `json:"end_time,omitempty"`
	MinValue  uint64 `json:"min_value,omitempty"`
	MaxValue  uint64 `json:"max_value,omitempty"`
}

// responseWriter wraps http.ResponseWriter to capture status code
type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (w *responseWriter) WriteHeader(code int) {
	w.statusCode = code
	w.ResponseWriter.WriteHeader(code)
}

// Utility functions

func generateTypedTransactionID(tx *types.TypedTransaction) string {
	// Simple ID generation - in production use proper hashing
	return fmt.Sprintf("%s-%d-%d", tx.From, tx.Nonce, common.ConsensusUnixNano())
}

func parseTransactionType(typeStr string) types.TransactionType {
	switch typeStr {
	case "transfer":
		return types.TransactionTypeTransfer
	case "contract_deploy":
		return types.TransactionTypeContractDeploy
	case "contract_call":
		return types.TransactionTypeContractCall
	case "stake":
		return types.TransactionTypeStake
	case "unstake":
		return types.TransactionTypeUnstake
	case "validator_update":
		return types.TransactionTypeValidatorUpdate
	case "governance":
		return types.TransactionTypeGovernance
	default:
		return types.TransactionTypeTransfer
	}
}

func parseTransactionStatus(statusStr string) types.TransactionStatus {
	switch statusStr {
	case "pending":
		return types.TransactionStatusPending
	case "queued":
		return types.TransactionStatusQueued
	case "processing":
		return types.TransactionStatusProcessing
	case "executed":
		return types.TransactionStatusExecuted
	case "failed":
		return types.TransactionStatusFailed
	case "dropped":
		return types.TransactionStatusDropped
	default:
		return types.TransactionStatusPending
	}
}

// Server lifecycle

func (s *TypedAPIServer) Start() error {
	s.startTime = common.ConsensusNow()

	addr := ":8080" // Default API server address

	s.logger.WithField("address", addr).Info("Starting API server")

	return http.ListenAndServe(addr, s.router)
}

func (s *TypedAPIServer) Stop() error {
	// In production, implement graceful shutdown
	return nil
}
