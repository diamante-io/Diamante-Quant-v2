// api/transactions.go
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"

	"diamante/common"
)

// TransactionMetadata represents typed metadata for transactions
type TransactionMetadata struct {
	Type        string            `json:"type,omitempty"`
	Description string            `json:"description,omitempty"`
	Tags        []string          `json:"tags,omitempty"`
	Properties  map[string]string `json:"properties,omitempty"`
}

// TransactionResponse represents the response for transaction-related endpoints
type TransactionResponse struct {
	ID        string              `json:"id"`
	Sender    string              `json:"sender"`
	Receiver  string              `json:"receiver"`
	Amount    float64             `json:"amount"`
	Fee       float64             `json:"fee"`
	Timestamp int64               `json:"timestamp"`
	Status    string              `json:"status"`
	BlockID   string              `json:"blockId,omitempty"`
	Nonce     int                 `json:"nonce"`
	Data      string              `json:"data,omitempty"`
	Metadata  TransactionMetadata `json:"metadata,omitempty"`
}

// TransactionSubmitRequest represents the request body for submitting a transaction
type TransactionSubmitRequest struct {
	Sender   string              `json:"sender"`
	Receiver string              `json:"receiver"`
	Amount   float64             `json:"amount"`
	Fee      float64             `json:"fee"`
	Nonce    int                 `json:"nonce,omitempty"` // Optional, will be auto-filled if 0
	Data     string              `json:"data,omitempty"`  // plain text or base64 encoded as needed
	Metadata TransactionMetadata `json:"metadata,omitempty"`
}

// TransactionStatusResponse represents the response for transaction status endpoint
type TransactionStatusResponse struct {
	ID        string `json:"id"`
	Status    string `json:"status"`
	Message   string `json:"message,omitempty"`
	Timestamp int64  `json:"timestamp,omitempty"`
	BlockID   string `json:"blockId,omitempty"`
}

// TransactionFeeEstimateResponse represents the response for fee estimation endpoint
type TransactionFeeEstimateResponse struct {
	EstimatedFee       float64 `json:"estimatedFee"`
	MinimumFee         float64 `json:"minimumFee"`
	RecommendedFee     float64 `json:"recommendedFee"`
	HighPriorityFee    float64 `json:"highPriorityFee"`
	CurrentNetworkLoad float64 `json:"currentNetworkLoad"`
}

// TransactionPoolStats represents transaction pool statistics
type TransactionPoolStats struct {
	PoolSize            int    `json:"poolSize"`
	TotalProcessed      int64  `json:"totalProcessed"`
	TotalConfirmed      int64  `json:"totalConfirmed"`
	TotalRejected       int64  `json:"totalRejected"`
	AvgProcessingTime   string `json:"avgProcessingTime"`
	MaxProcessingTime   string `json:"maxProcessingTime"`
	AvgConfirmationTime string `json:"avgConfirmationTime"`
	LastProcessedTime   int64  `json:"lastProcessedTime"`
}

// NonceResponse represents the response for nonce suggestion endpoint
type NonceResponse struct {
	AccountID      string `json:"accountId"`
	SuggestedNonce int    `json:"suggestedNonce"`
}

// TransactionSubmitResponse represents the standardized response for transaction submission
type TransactionSubmitResponse struct {
	ID     string  `json:"id"`
	Status string  `json:"status"`
	Fee    float64 `json:"fee,omitempty"`
	Nonce  int     `json:"nonce,omitempty"`
}

// TransactionErrorResponse represents the standardized error response
type TransactionErrorResponse struct {
	Error     string `json:"error"`
	Code      string `json:"code"`
	NextNonce int    `json:"nextNonce,omitempty"` // Include next nonce for retry
}

// respondWithTransactionError sends a standardized JSON error response
func respondWithTransactionError(w http.ResponseWriter, statusCode int, message string, code string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	resp := TransactionErrorResponse{
		Error: message,
		Code:  code,
	}
	json.NewEncoder(w).Encode(resp)
}

// handleSubmitTransaction accepts a new transaction from a client.
func (api *API) handleSubmitTransaction(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()

	// Generate correlation ID for request tracking
	correlationID := fmt.Sprintf("tx-%d-%d", time.Now().UnixNano(), rand.Int31())
	api.Logger.WithFields(logrus.Fields{
		"correlation_id": correlationID,
		"method":         "POST",
		"path":           "/api/v1/transactions",
	}).Info("Transaction submit: handler started")

	var req TransactionSubmitRequest
	if err := decodeJSONBody(w, r, &req, api.bodyLimit); err != nil {
		api.Logger.WithFields(logrus.Fields{
			"correlation_id": correlationID,
			"duration_ms":    time.Since(startTime).Milliseconds(),
			"error":          err.Error(),
		}).Warn("Transaction submit: decode failed")

		if errors.Is(err, ErrBodyTooLarge) {
			respondWithTransactionError(w, http.StatusRequestEntityTooLarge, "payload too large", "PAYLOAD_TOO_LARGE")
		} else {
			respondWithTransactionError(w, http.StatusBadRequest, err.Error(), "INVALID_REQUEST")
		}
		return
	}

	api.Logger.WithFields(logrus.Fields{
		"correlation_id": correlationID,
		"duration_ms":    time.Since(startTime).Milliseconds(),
		"sender":         req.Sender,
		"receiver":       req.Receiver,
		"amount":         req.Amount,
	}).Debug("Transaction submit: request decoded")

	// Validate request
	if req.Sender == "" || req.Receiver == "" {
		api.Logger.WithFields(logrus.Fields{
			"correlation_id": correlationID,
			"duration_ms":    time.Since(startTime).Milliseconds(),
		}).Warn("Transaction submit: validation failed - missing sender/receiver")
		respondWithTransactionError(w, http.StatusBadRequest, "Sender and receiver are required", "MISSING_FIELDS")
		return
	}
	if len(req.Sender) > 128 || len(req.Receiver) > 128 {
		respondWithTransactionError(w, http.StatusBadRequest, "sender or receiver too long", "INVALID_ADDRESS")
		return
	}

	if req.Amount <= 0 {
		respondWithTransactionError(w, http.StatusBadRequest, "Amount must be positive", "INVALID_AMOUNT")
		return
	}
	if len(req.Data) > 1024 {
		respondWithTransactionError(w, http.StatusBadRequest, "data field too large", "DATA_TOO_LARGE")
		return
	}

	api.Logger.WithFields(logrus.Fields{
		"correlation_id": correlationID,
		"duration_ms":    time.Since(startTime).Milliseconds(),
	}).Debug("Transaction submit: validation passed")

	// Get effective next nonce if not provided
	if req.Nonce == 0 {
		effectiveNext, err := api.TxManager.GetEffectiveNextNonce(req.Sender)
		if err != nil {
			api.Logger.WithFields(logrus.Fields{
				"correlation_id": correlationID,
				"duration_ms":    time.Since(startTime).Milliseconds(),
				"error":          err.Error(),
			}).Warn("Transaction submit: failed to get effective nonce")
			respondWithTransactionError(w, http.StatusInternalServerError, "Failed to determine nonce", "NONCE_ERROR")
			return
		}
		req.Nonce = int(effectiveNext)

		api.Logger.WithFields(logrus.Fields{
			"correlation_id": correlationID,
			"sender":         req.Sender,
			"expected":       effectiveNext,
			"got":            0,
			"autoFilled":     true,
		}).Debug("nonceDecision")
	}

	// Create transaction with timeout context
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	createStartTime := time.Now()

	// Create a modified transaction request with the nonce
	tx, err := api.TxManager.CreateTransactionWithContext(ctx, req.Sender, req.Receiver, req.Amount, req.Fee, []byte(req.Data))

	api.Logger.WithFields(logrus.Fields{
		"correlation_id":     correlationID,
		"create_duration_ms": time.Since(createStartTime).Milliseconds(),
		"total_duration_ms":  time.Since(startTime).Milliseconds(),
		"success":            err == nil,
		"error":              fmt.Sprintf("%v", err),
	}).Info("Transaction submit: create completed")

	if err != nil {
		// Context timeout
		if errors.Is(err, context.DeadlineExceeded) {
			api.Logger.WithFields(logrus.Fields{
				"correlation_id": correlationID,
				"duration_ms":    time.Since(startTime).Milliseconds(),
			}).Error("Transaction submit: timeout exceeded")
			respondWithTransactionError(w, http.StatusGatewayTimeout, "Transaction creation timeout", "TIMEOUT")
			return
		}

		// Check for specific error types
		errMsg := err.Error()

		// Duplicate transaction
		if strings.Contains(errMsg, "already exists in pool") {
			respondWithTransactionError(w, http.StatusConflict, errMsg, "DUPLICATE_TRANSACTION")
			return
		}

		// Nonce errors
		if strings.Contains(errMsg, "nonce") || strings.Contains(errMsg, "INVALID_NONCE") {
			// Try to get the effective next nonce for retry
			effectiveNext, _ := api.TxManager.GetEffectiveNextNonce(req.Sender)

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnprocessableEntity)
			resp := TransactionErrorResponse{
				Error:     errMsg,
				Code:      "INVALID_NONCE",
				NextNonce: int(effectiveNext),
			}
			json.NewEncoder(w).Encode(resp)
			return
		}

		// Insufficient funds
		if strings.Contains(errMsg, "insufficient") {
			respondWithTransactionError(w, http.StatusUnprocessableEntity, errMsg, "INSUFFICIENT_FUNDS")
			return
		}

		// Signature errors
		if strings.Contains(errMsg, "signature") {
			respondWithTransactionError(w, http.StatusUnprocessableEntity, errMsg, "INVALID_SIGNATURE")
			return
		}

		// Validation errors
		if strings.Contains(errMsg, "validation failed") || strings.Contains(errMsg, "fee below minimum") {
			respondWithTransactionError(w, http.StatusUnprocessableEntity, errMsg, "VALIDATION_FAILED")
			return
		}

		// Pool full
		if strings.Contains(errMsg, "pool is full") {
			respondWithTransactionError(w, http.StatusServiceUnavailable, errMsg, "POOL_FULL")
			return
		}

		// Default to internal server error
		respondWithTransactionError(w, http.StatusInternalServerError, "Transaction creation failed: "+err.Error(), "INTERNAL_ERROR")
		return
	}

	// Broadcast transaction to peers if broadcaster is available
	if api.TxBroadcaster != nil {
		broadcastStartTime := time.Now()

		api.Logger.WithFields(logrus.Fields{
			"correlation_id": correlationID,
			"tx_id":          tx.ID,
		}).Info("Calling TxBroadcaster for transaction")

		if err := api.TxBroadcaster(tx); err != nil {
			api.Logger.WithFields(logrus.Fields{
				"correlation_id":        correlationID,
				"tx_id":                 tx.ID,
				"error":                 err.Error(),
				"broadcast_duration_ms": time.Since(broadcastStartTime).Milliseconds(),
			}).Warn("Transaction submit: broadcast failed")
			// Don't fail the request if broadcast fails
		} else {
			api.Logger.WithFields(logrus.Fields{
				"correlation_id":        correlationID,
				"tx_id":                 tx.ID,
				"broadcast_duration_ms": time.Since(broadcastStartTime).Milliseconds(),
			}).Info("Transaction broadcast successful")
		}
	} else {
		api.Logger.WithFields(logrus.Fields{
			"correlation_id": correlationID,
			"tx_id":          tx.ID,
		}).Warn("TxBroadcaster is nil - transaction will not be propagated to peers")
	}

	// Prepare standardized success response
	resp := TransactionSubmitResponse{
		ID:     tx.ID,
		Status: "pending",
		Fee:    tx.Fee,
		Nonce:  tx.Nonce,
	}

	api.Logger.WithFields(logrus.Fields{
		"correlation_id": correlationID,
		"duration_ms":    time.Since(startTime).Milliseconds(),
		"tx_id":          tx.ID,
	}).Info("Transaction submit: sending response")

	// Always set Content-Type to application/json
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(resp)

	api.Logger.WithFields(logrus.Fields{
		"correlation_id":    correlationID,
		"total_duration_ms": time.Since(startTime).Milliseconds(),
		"tx_id":             tx.ID,
		"status":            "completed",
	}).Info("Transaction submit: handler completed")
}

// handleGetTransactionStatus retrieves the status of a transaction by ID
func (api *API) handleGetTransactionStatus(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	txID := vars["id"]

	if txID == "" {
		respondWithError(w, http.StatusBadRequest, "Transaction ID is required")
		return
	}

	// Normalize transaction ID for consistent lookup
	txID = common.NormalizeTransactionID(txID)

	// Check transaction status
	status, err := api.TxManager.GetTransactionStatus(txID)
	if err != nil {
		respondWithError(w, http.StatusNotFound, "Transaction not found: "+err.Error())
		return
	}

	// Prepare response
	resp := TransactionStatusResponse{
		ID:     txID,
		Status: status,
	}

	respondWithJSON(w, http.StatusOK, resp)
}

// handleGetTransaction retrieves a transaction by ID
func (api *API) handleGetTransaction(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	txID := vars["id"]

	if txID == "" {
		respondWithError(w, http.StatusBadRequest, "Transaction ID is required")
		return
	}

	// Normalize transaction ID for consistent lookup
	txID = common.NormalizeTransactionID(txID)

	// First check if it's in the pending pool
	if tx, found := api.TxManager.GetTransactionFromPool(txID); found {
		api.Logger.WithFields(logrus.Fields{
			"tx_id":  txID,
			"status": "pending",
		}).Info("txQuery resolved=pending")

		resp := TransactionResponse{
			ID:        tx.ID,
			Sender:    tx.Sender,
			Receiver:  tx.Receiver,
			Amount:    tx.Amount,
			Fee:       tx.Fee,
			Timestamp: tx.Timestamp,
			Status:    "pending",
			Nonce:     tx.Nonce,
			Data:      string(tx.Data),
		}
		respondWithJSON(w, http.StatusOK, resp)
		return
	}

	// If not in pool, check the ledger
	ledgerAPI, ok := api.Ledger.(common.LedgerAPI)
	if !ok {
		respondWithError(w, http.StatusInternalServerError, "Ledger API not available")
		return
	}

	tx, err := ledgerAPI.GetTransaction(txID)
	if err != nil {
		respondWithError(w, http.StatusNotFound, "Transaction not found: "+err.Error())
		return
	}

	api.Logger.WithFields(logrus.Fields{
		"tx_id":  txID,
		"status": "committed",
	}).Info("txQuery resolved=committed")

	// Prepare response
	resp := TransactionResponse{
		ID:        tx.ID,
		Sender:    tx.Sender,
		Receiver:  tx.Receiver,
		Amount:    tx.Amount,
		Fee:       tx.Fee,
		Timestamp: tx.Timestamp,
		Status:    "committed",
		Nonce:     tx.Nonce,
		Data:      string(tx.Data),
	}

	respondWithJSON(w, http.StatusOK, resp)
}

// handleGetPendingTransactions retrieves all pending transactions
func (api *API) handleGetPendingTransactions(w http.ResponseWriter, r *http.Request) {
	// Parse query parameters
	query := r.URL.Query()
	limitStr := query.Get("limit")
	accountID := query.Get("account")

	limit := 50 // Default limit
	if limitStr != "" {
		parsedLimit, err := strconv.Atoi(limitStr)
		if err == nil && parsedLimit > 0 {
			limit = parsedLimit
		}
	}

	var pendingTxs []*common.Transaction
	var err error

	// If account ID is provided, get transactions for that account
	if accountID != "" {
		pendingTxs, err = api.TxManager.GetPendingTransactionsByAccount(accountID)
	} else {
		pendingTxs, err = api.TxManager.GetPendingTransactions()
	}

	if err != nil {
		respondWithError(w, http.StatusInternalServerError, "Failed to retrieve pending transactions: "+err.Error())
		return
	}

	// Apply limit
	if len(pendingTxs) > limit {
		pendingTxs = pendingTxs[:limit]
	}

	// Convert to response format
	resp := make([]TransactionResponse, len(pendingTxs))
	for i, tx := range pendingTxs {
		resp[i] = TransactionResponse{
			ID:        tx.ID,
			Sender:    tx.Sender,
			Receiver:  tx.Receiver,
			Amount:    tx.Amount,
			Fee:       tx.Fee,
			Timestamp: tx.Timestamp,
			Status:    "pending",
			Nonce:     tx.Nonce,
			Data:      string(tx.Data),
		}
	}

	respondWithJSON(w, http.StatusOK, resp)
}

// handleGetAccountTransactions retrieves transaction history for an account
func (api *API) handleGetAccountTransactions(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	accountID := vars["id"]

	if accountID == "" {
		respondWithError(w, http.StatusBadRequest, "Account ID is required")
		return
	}

	// Parse query parameters
	query := r.URL.Query()
	limitStr := query.Get("limit")
	offsetStr := query.Get("offset")

	limit := 20 // Default limit
	if limitStr != "" {
		parsedLimit, err := strconv.Atoi(limitStr)
		if err == nil && parsedLimit > 0 {
			limit = parsedLimit
		}
	}

	offset := 0 // Default offset
	if offsetStr != "" {
		parsedOffset, err := strconv.Atoi(offsetStr)
		if err == nil && parsedOffset >= 0 {
			offset = parsedOffset
		}
	}

	// Get account transactions from ledger
	ledgerAPI, ok := api.Ledger.(common.LedgerAPI)
	if !ok {
		respondWithError(w, http.StatusInternalServerError, "Ledger API not available")
		return
	}

	txs, err := ledgerAPI.GetAccountTransactions(accountID, limit, offset)
	if err != nil {
		respondWithError(w, http.StatusInternalServerError, "Failed to retrieve account transactions: "+err.Error())
		return
	}

	// Convert to response format
	resp := make([]TransactionResponse, len(txs))
	for i, tx := range txs {
		resp[i] = TransactionResponse{
			ID:        tx.ID,
			Sender:    tx.Sender,
			Receiver:  tx.Receiver,
			Amount:    tx.Amount,
			Fee:       tx.Fee,
			Timestamp: tx.Timestamp,
			Status:    "committed",
			Nonce:     tx.Nonce,
			Data:      string(tx.Data),
		}
	}

	// Also get pending transactions for this account
	pendingTxs, _ := api.TxManager.GetPendingTransactionsByAccount(accountID)
	for _, tx := range pendingTxs {
		pendingResp := TransactionResponse{
			ID:        tx.ID,
			Sender:    tx.Sender,
			Receiver:  tx.Receiver,
			Amount:    tx.Amount,
			Fee:       tx.Fee,
			Timestamp: tx.Timestamp,
			Status:    "pending",
			Nonce:     tx.Nonce,
			Data:      string(tx.Data),
		}
		resp = append(resp, pendingResp)
	}

	respondWithJSON(w, http.StatusOK, resp)
}

// handleEstimateTransactionFee estimates the fee for a transaction
func (api *API) handleEstimateTransactionFee(w http.ResponseWriter, r *http.Request) {
	// Get current network load
	networkLoad := api.Consensus.GetNetworkLoad()

	// Get base fee from transaction manager
	baseFee := api.TxManager.EstimateFee()

	// Calculate different fee levels
	minFee := baseFee * 0.8
	recommendedFee := baseFee
	highPriorityFee := baseFee * 1.5

	// Prepare response
	resp := TransactionFeeEstimateResponse{
		EstimatedFee:       baseFee,
		MinimumFee:         minFee,
		RecommendedFee:     recommendedFee,
		HighPriorityFee:    highPriorityFee,
		CurrentNetworkLoad: networkLoad,
	}

	respondWithJSON(w, http.StatusOK, resp)
}

// handleGetTransactionPoolStats retrieves statistics about the transaction pool
func (api *API) handleGetTransactionPoolStats(w http.ResponseWriter, r *http.Request) {
	// Get transaction manager stats
	stats := api.TxManager.GetStats()

	// Get pool size
	poolSize := api.TxManager.GetPoolSize()

	// Prepare response
	resp := TransactionPoolStats{
		PoolSize:            poolSize,
		TotalProcessed:      int64(stats.TotalProcessed),
		TotalConfirmed:      int64(stats.TotalConfirmed),
		TotalRejected:       int64(stats.TotalRejected),
		AvgProcessingTime:   stats.AvgProcessingTime.String(),
		MaxProcessingTime:   stats.MaxProcessingTime.String(),
		AvgConfirmationTime: stats.AvgConfirmationTime.String(),
		LastProcessedTime:   stats.LastProcessedTime.Unix(),
	}

	respondWithJSON(w, http.StatusOK, resp)
}

// handleSuggestTransactionNonce suggests a nonce for a new transaction
func (api *API) handleSuggestTransactionNonce(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	accountID := vars["id"]

	if accountID == "" {
		respondWithError(w, http.StatusBadRequest, "Account ID is required")
		return
	}

	// Get suggested nonce from transaction manager
	nonce := api.TxManager.SuggestNonce(accountID)

	// Prepare response
	resp := NonceResponse{
		AccountID:      accountID,
		SuggestedNonce: int(nonce),
	}

	respondWithJSON(w, http.StatusOK, resp)
}

// RegisterTransactionRoutes registers all transaction-related routes
func (api *API) RegisterTransactionRoutes(router *mux.Router) {
	// Submit a new transaction
	router.HandleFunc("/transactions", api.handleSubmitTransaction).Methods("POST")

	// Get transaction status
	router.HandleFunc("/transactions/{id}/status", api.handleGetTransactionStatus).Methods("GET")

	// Get transaction details
	router.HandleFunc("/transactions/{id}", api.handleGetTransaction).Methods("GET")

	// Get pending transactions
	router.HandleFunc("/transactions/pending", api.handleGetPendingTransactions).Methods("GET")

	// Get account transactions
	router.HandleFunc("/accounts/{id}/transactions", api.handleGetAccountTransactions).Methods("GET")

	// Estimate transaction fee
	router.HandleFunc("/transactions/fee/estimate", api.handleEstimateTransactionFee).Methods("GET")

	// Get transaction pool stats
	router.HandleFunc("/transactions/pool/stats", api.handleGetTransactionPoolStats).Methods("GET")

	// Suggest transaction nonce
	router.HandleFunc("/accounts/{id}/nonce", api.handleSuggestTransactionNonce).Methods("GET")
}
