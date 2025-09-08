// api/server.go
package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"

	"diamante/common"
	"diamante/config"
	"diamante/consensus/governance"
	"diamante/consensus/types"
	"diamante/storage"
	"diamante/transaction"
)

var (
	// Package-level logger for utility functions
	pkgLogger = logrus.WithField("component", "api")
)

// HeightGetter defines an interface to get the last block height.
type HeightGetter interface {
	GetLastBlockHeight() uint64
}

// LedgerAPI defines the interface for ledger operations
type LedgerAPI interface {
	GetBlockByNumber(num int) (common.Block, bool)
	CreateSnapshot(height int) error
	RestoreSnapshot(height int) error
}

// TransactionBroadcaster is a function type for broadcasting transactions to peers
type TransactionBroadcaster func(tx *common.Transaction) error

// API aggregates the core modules needed for API requests.
type API struct {
	Ledger        LedgerAPI
	Consensus     types.Consensus
	TxManager     *transaction.TransactionManager
	Storage       storage.LedgerStore
	Governance    *governance.Governance
	Logger        *logrus.Logger          // Keep for backward compatibility
	StructLogger  common.StructuredLogger // New structured logger
	Config        *config.Config
	limiter       *rateLimiter
	ipLimiter     *ipRateLimiter
	bodyLimit     int64
	healthFn      func() int
	TxBroadcaster TransactionBroadcaster // Function to broadcast transactions to peers
}

// NewAPI creates a new API instance with its dependencies.
func NewAPI(
	ledgerInst LedgerAPI,
	consensus types.Consensus,
	txManager *transaction.TransactionManager,
	store storage.LedgerStore,
	gov *governance.Governance,
	logger *logrus.Logger,
	cfg *config.Config,
	healthFn func() int,
) *API {
	if logger == nil {
		logger = logrus.New()
		logger.SetLevel(logrus.InfoLevel)
	}

	// Create structured logger for API
	structLogger := common.NewStructuredLogger("api-server")

	api := &API{
		Ledger:       ledgerInst,
		Consensus:    consensus,
		TxManager:    txManager,
		Storage:      store,
		Governance:   gov,
		Logger:       logger,
		StructLogger: structLogger,
		Config:       cfg,
		bodyLimit:    1 << 20, // 1 MiB default
		healthFn:     healthFn,
	}

	if cfg != nil {
		rl := newRateLimiter(cfg.API.RateLimit, cfg.API.RateBurst)
		api.limiter = rl
		api.ipLimiter = newIPRateLimiter(cfg.API.IPRateLimit, cfg.API.IPRateBurst)
		if cfg.API.BodyLimit > 0 {
			api.bodyLimit = cfg.API.BodyLimit
		}
	}

	return api
}

// StartServer starts the HTTP server on the specified port.
func (api *API) StartServer(port string) error {
	router := mux.NewRouter()

	router.Use(api.authMiddleware)
	router.Use(api.ipRateLimitMiddleware)
	router.Use(api.rateLimitMiddleware)

	// Health endpoint at root level for load balancers
	router.HandleFunc("/health", api.handleHealth).Methods("GET")

	// Create versioned API subrouter
	v1 := router.PathPrefix("/api/v1").Subrouter()

	// Standard endpoints under v1
	v1.HandleFunc("/status", api.handleStatus).Methods("GET")
	v1.HandleFunc("/accounts/{id}", api.handleGetAccount).Methods("GET")
	v1.HandleFunc("/blocks/{number}", api.handleGetBlock).Methods("GET")

	// Register transaction routes under v1
	api.RegisterTransactionRoutes(v1)

	// Register EVM routes under v1
	api.RegisterEVMRoutes(v1)

	// Register Governance routes under v1
	api.RegisterGovernanceRoutes(v1)

	// Register Hybrid VM routes under v1 if available
	api.RegisterHybridVMRoutes(v1)

	// Wallet endpoints under v1
	v1.HandleFunc("/wallets", api.handleCreateWallet).Methods("POST")
	v1.HandleFunc("/wallets/{id}", api.handleGetWallet).Methods("GET")
	v1.HandleFunc("/wallets/{id}", api.handleDeleteWallet).Methods("DELETE")
	v1.HandleFunc("/wallets/{id}/balance", api.handleGetWalletBalance).Methods("GET")
	v1.HandleFunc("/wallets/{id}/transactions", api.handleGetWalletTransactions).Methods("GET")
	v1.HandleFunc("/wallets/{id}/transfer", api.handleTransferFunds).Methods("POST")
	v1.HandleFunc("/wallets/{id}/fund", api.handleFundWallet).Methods("POST") // For testing only

	// Token supply endpoints under v1
	v1.HandleFunc("/token-supply", api.handleGetTokenSupply).Methods("GET")

	// Ledger snapshot endpoints under v1
	v1.HandleFunc("/ledger/snapshot/{height}", api.handleCreateSnapshot).Methods("GET")
	v1.HandleFunc("/ledger/restore/{height}", api.handleRestoreSnapshot).Methods("POST")

	// Storage endpoint under v1
	v1.HandleFunc("/storage/block/{number}", api.handleStorageGetBlock).Methods("GET")

	// zkEVM endpoints - return 501 when not enabled
	v1.HandleFunc("/proof/verify", api.handleZKNotImplemented).Methods("POST")
	v1.HandleFunc("/zkevm/metrics", api.handleZKNotImplemented).Methods("GET")
	v1.HandleFunc("/batch/status/{id}", api.handleZKNotImplemented).Methods("GET")

	api.StructLogger.Info("Starting API server",
		common.StringField("port", port),
		common.StringField("endpoints", "REST API with rate limiting"))

	return http.ListenAndServe(":"+port, router)
}

// StatusResponse represents the server status response
type StatusResponse struct {
	CurrentBlockHeight uint64  `json:"currentBlockHeight"`
	NetworkLoad        float64 `json:"networkLoad"`
}

// HealthResponse represents the health check response
type HealthCheckResponse struct {
	HealthScore int `json:"healthScore"`
}

// MessageResponse represents a simple message response
type MessageResponse struct {
	Message string `json:"message"`
}

// ErrorResponse represents an error response
type ErrorResponse struct {
	Error string `json:"error"`
}

// handleStatus returns basic status information.
func (api *API) handleStatus(w http.ResponseWriter, r *http.Request) {
	var currentBlockHeight uint64
	if getter, ok := api.Consensus.(HeightGetter); ok {
		currentBlockHeight = getter.GetLastBlockHeight()
	} else {
		currentBlockHeight = 0
	}

	status := StatusResponse{
		CurrentBlockHeight: currentBlockHeight,
		NetworkLoad:        api.Consensus.GetNetworkLoad(),
	}

	api.StructLogger.Debug("Status request processed",
		common.BlockHeightField(currentBlockHeight),
		common.Float64Field("networkLoad", api.Consensus.GetNetworkLoad()))

	respondWithJSON(w, http.StatusOK, status)
}

// handleHealth returns the computed health score.
func (api *API) handleHealth(w http.ResponseWriter, r *http.Request) {
	score := 0
	if api.healthFn != nil {
		score = api.healthFn()
	}

	api.StructLogger.Debug("Health check processed",
		common.IntField("healthScore", score))

	resp := HealthCheckResponse{
		HealthScore: score,
	}

	respondWithJSON(w, http.StatusOK, resp)
}

// handleGetAccount retrieves account information by account ID.
func (api *API) handleGetAccount(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	accountID := vars["id"]

	account := common.GetAccount(accountID)
	if account == nil {
		api.StructLogger.Warn("Account not found",
			common.StringField("accountID", accountID),
			common.StringField("clientIP", clientIP(r.RemoteAddr)))
		respondWithError(w, http.StatusNotFound, fmt.Sprintf("Account %s not found", accountID))
		return
	}

	api.StructLogger.Debug("Account retrieved",
		common.StringField("accountID", accountID),
		common.Float64Field("balance", account.Balance))

	respondWithJSON(w, http.StatusOK, account)
}

// handleGetBlock retrieves block details by block number from the ledger.
func (api *API) handleGetBlock(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	numStr := vars["number"]

	num, err := strconv.Atoi(numStr)
	if err != nil {
		api.StructLogger.Warn("Invalid block number in request",
			common.StringField("blockNumber", numStr),
			common.StringField("clientIP", clientIP(r.RemoteAddr)),
			common.ErrorField(err))
		respondWithError(w, http.StatusBadRequest, "Invalid block number")
		return
	}

	block, exists := api.Ledger.GetBlockByNumber(num)
	if !exists {
		api.StructLogger.Warn("Block not found",
			common.IntField("blockNumber", num),
			common.StringField("clientIP", clientIP(r.RemoteAddr)))
		respondWithError(w, http.StatusNotFound, fmt.Sprintf("Block %d not found", num))
		return
	}

	api.StructLogger.Debug("Block retrieved",
		common.IntField("blockNumber", num),
		common.IntField("transactionCount", len(block.Transactions)))

	respondWithJSON(w, http.StatusOK, block)
}

// handleCreateSnapshot creates a snapshot of the ledger state at the given height.
func (api *API) handleCreateSnapshot(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	heightStr := vars["height"]
	height, err := strconv.Atoi(heightStr)
	if err != nil {
		api.StructLogger.Warn("Invalid snapshot height",
			common.StringField("height", heightStr),
			common.ErrorField(err))
		respondWithError(w, http.StatusBadRequest, "Invalid snapshot height")
		return
	}

	api.StructLogger.Info("Creating ledger snapshot",
		common.IntField("height", height))

	err = api.Ledger.CreateSnapshot(height)
	if err != nil {
		api.StructLogger.Error("Snapshot creation failed",
			common.IntField("height", height),
			common.ErrorField(err))
		respondWithError(w, http.StatusInternalServerError, "Snapshot creation failed: "+err.Error())
		return
	}

	api.StructLogger.Info("Snapshot created successfully",
		common.IntField("height", height))

	resp := MessageResponse{
		Message: fmt.Sprintf("Snapshot created at height %d", height),
	}
	respondWithJSON(w, http.StatusOK, resp)
}

// handleRestoreSnapshot restores the ledger state to the snapshot at the given height.
func (api *API) handleRestoreSnapshot(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	heightStr := vars["height"]
	height, err := strconv.Atoi(heightStr)
	if err != nil {
		api.StructLogger.Warn("Invalid restore height",
			common.StringField("height", heightStr),
			common.ErrorField(err))
		respondWithError(w, http.StatusBadRequest, "Invalid snapshot height")
		return
	}

	api.StructLogger.Info("Restoring ledger from snapshot",
		common.IntField("height", height))

	err = api.Ledger.RestoreSnapshot(height)
	if err != nil {
		api.StructLogger.Error("Snapshot restore failed",
			common.IntField("height", height),
			common.ErrorField(err))
		respondWithError(w, http.StatusInternalServerError, "Snapshot restore failed: "+err.Error())
		return
	}

	api.StructLogger.Info("Ledger restored successfully",
		common.IntField("height", height))

	resp := MessageResponse{
		Message: fmt.Sprintf("Ledger restored to snapshot at height %d", height),
	}
	respondWithJSON(w, http.StatusOK, resp)
}

// handleStorageGetBlock retrieves a block from the persistent JSON store.
func (api *API) handleStorageGetBlock(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	numStr := vars["number"]

	num, err := strconv.Atoi(numStr)
	if err != nil {
		api.StructLogger.Warn("Invalid storage block number",
			common.StringField("blockNumber", numStr),
			common.ErrorField(err))
		respondWithError(w, http.StatusBadRequest, "Invalid block number")
		return
	}

	block, err := api.Storage.GetBlock(uint64(num))
	if err != nil {
		api.StructLogger.Warn("Block not found in storage",
			common.IntField("blockNumber", num),
			common.ErrorField(err))
		respondWithError(w, http.StatusNotFound, fmt.Sprintf("Block %d not found in storage", num))
		return
	}

	api.StructLogger.Debug("Block retrieved from storage",
		common.IntField("blockNumber", num))

	respondWithJSON(w, http.StatusOK, block)
}

// authMiddleware validates the bearer token or API key on each request.
func (api *API) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if api.Config == nil || (api.Config.API.BearerToken == "" && api.Config.API.APIKey == "" && api.Config.API.APIKeyHash == "") {
			next.ServeHTTP(w, r)
			return
		}

		authHeader := r.Header.Get("Authorization")
		if api.Config.API.BearerToken != "" && strings.HasPrefix(authHeader, "Bearer ") {
			token := strings.TrimPrefix(authHeader, "Bearer ")
			if token == api.Config.API.BearerToken {
				next.ServeHTTP(w, r)
				return
			}
		}

		apiKey := r.Header.Get("X-API-Key")
		// SECURITY: Use secure API key validation with bcrypt hashing
		if apiKey != "" && api.Config.API.ValidateAPIKeyFromRequest(apiKey) {
			next.ServeHTTP(w, r)
			return
		}

		// Fallback to plain text API key for backward compatibility (deprecated)
		if api.Config.API.APIKey != "" && apiKey == api.Config.API.APIKey {
			// Log warning about insecure API key usage
			api.StructLogger.Warn("SECURITY WARNING: Using plain text API key - migrate to hashed API key",
				common.StringField("clientIP", clientIP(r.RemoteAddr)),
				common.SecurityAuditField("deprecated_auth", "medium"))
			next.ServeHTTP(w, r)
			return
		}

		api.StructLogger.Warn("Unauthorized API access attempt",
			common.StringField("clientIP", clientIP(r.RemoteAddr)),
			common.StringField("endpoint", r.URL.Path),
			common.StringField("method", r.Method),
			common.SecurityAuditField("unauthorized_access", "medium"))

		respondWithError(w, http.StatusUnauthorized, "unauthorized")
	})
}

// AuthMiddleware exposes the authentication middleware for external use,
// wrapping the unexported authMiddleware method.
func (api *API) AuthMiddleware(next http.Handler) http.Handler {
	return api.authMiddleware(next)
}

// rateLimitMiddleware enforces request rate limiting.
func (api *API) rateLimitMiddleware(next http.Handler) http.Handler {
	if api.limiter == nil {
		return next
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !api.limiter.allow() {
			api.StructLogger.Warn("Rate limit exceeded",
				common.StringField("clientIP", clientIP(r.RemoteAddr)),
				common.StringField("endpoint", r.URL.Path),
				common.SecurityAuditField("rate_limit_exceeded", "low"))
			respondWithError(w, http.StatusTooManyRequests, "rate limit exceeded")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// RateLimitMiddleware exposes the request rate limiting middleware for tests
// and other external packages.
func (api *API) RateLimitMiddleware(next http.Handler) http.Handler {
	return api.rateLimitMiddleware(next)
}

// ipRateLimitMiddleware enforces per-IP rate limiting.
func (api *API) ipRateLimitMiddleware(next http.Handler) http.Handler {
	if api.ipLimiter == nil {
		return next
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ip := clientIP(r.RemoteAddr)
		if !api.ipLimiter.allow(ip) {
			api.StructLogger.Warn("IP rate limit exceeded",
				common.StringField("clientIP", ip),
				common.StringField("endpoint", r.URL.Path),
				common.SecurityAuditField("ip_rate_limit_exceeded", "medium"))
			respondWithError(w, http.StatusTooManyRequests, "ip rate limit exceeded")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// IPRateLimitMiddleware exposes the per-IP rate limiting middleware.
func (api *API) IPRateLimitMiddleware(next http.Handler) http.Handler {
	return api.ipRateLimitMiddleware(next)
}

// RegisterHybridVMRoutes registers hybrid VM routes if runtime manager is available
func (api *API) RegisterHybridVMRoutes(router *mux.Router) {
	// For now, skip if not available - this will be wired up when runtime manager is available
	// The hybrid VM handler needs runtime manager and deployment manager which aren't in the API struct yet
}

// handleZKNotImplemented returns 501 for zkEVM endpoints when feature is not enabled
func (api *API) handleZKNotImplemented(w http.ResponseWriter, r *http.Request) {
	api.StructLogger.Debug("zkEVM endpoint called but feature not enabled",
		common.StringField("endpoint", r.URL.Path),
		common.StringField("method", r.Method))

	resp := ErrorResponse{
		Error: "zkEVM feature is not enabled or available",
	}
	respondWithJSON(w, http.StatusNotImplemented, resp)
}

// Helper: respondWithError sends a JSON error response.
func respondWithError(w http.ResponseWriter, code int, message string) {
	resp := ErrorResponse{
		Error: message,
	}
	respondWithJSON(w, code, resp)
}

// Helper: isClientDisconnectError checks if the error is due to client disconnection
func isClientDisconnectError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return strings.Contains(errStr, "broken pipe") ||
		strings.Contains(errStr, "connection reset by peer") ||
		strings.Contains(errStr, "write: connection reset by peer")
}

// Helper: respondWithJSON sends a JSON response.
func respondWithJSON(w http.ResponseWriter, code int, payload interface{}) {
	response, err := json.Marshal(payload)
	if err != nil {
		http.Error(w, "JSON marshal error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	// Write errors are typically due to client disconnection
	// Log the error for monitoring but don't fail the request
	if _, writeErr := w.Write(response); writeErr != nil {
		// Don't log every client disconnect, but log unusual write errors
		if !isClientDisconnectError(writeErr) {
			pkgLogger.WithError(writeErr).Warn("HTTP response write error")
		}
	}
}
