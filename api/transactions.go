// api/transactions.go
package api

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"

	"diamante/common"
)

// TransactionResponse represents the response for transaction-related endpoints
type TransactionResponse struct {
	ID        string                 `json:"id"`
	Sender    string                 `json:"sender"`
	Receiver  string                 `json:"receiver"`
	Amount    float64                `json:"amount"`
	Fee       float64                `json:"fee"`
	Timestamp int64                  `json:"timestamp"`
	Status    string                 `json:"status"`
	BlockID   string                 `json:"blockId,omitempty"`
	Nonce     int                    `json:"nonce"`
	Data      string                 `json:"data,omitempty"`
	Metadata  map[string]interface{} `json:"metadata,omitempty"`
}

// TransactionSubmitRequest represents the request body for submitting a transaction
type TransactionSubmitRequest struct {
	Sender   string                 `json:"sender"`
	Receiver string                 `json:"receiver"`
	Amount   float64                `json:"amount"`
	Fee      float64                `json:"fee"`
	Data     string                 `json:"data,omitempty"` // plain text or base64 encoded as needed
	Metadata map[string]interface{} `json:"metadata,omitempty"`
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

// handleSubmitTransaction accepts a new transaction from a client.
func (api *API) handleSubmitTransaction(w http.ResponseWriter, r *http.Request) {
	var req TransactionSubmitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondWithError(w, http.StatusBadRequest, "Invalid request payload: "+err.Error())
		return
	}

	// Validate request
	if req.Sender == "" || req.Receiver == "" {
		respondWithError(w, http.StatusBadRequest, "Sender and receiver are required")
		return
	}

	if req.Amount <= 0 {
		respondWithError(w, http.StatusBadRequest, "Amount must be positive")
		return
	}

	// Create transaction
	tx, err := api.TxManager.CreateTransaction(req.Sender, req.Receiver, req.Amount, req.Fee, []byte(req.Data))
	if err != nil {
		respondWithError(w, http.StatusInternalServerError, "Transaction creation failed: "+err.Error())
		return
	}

	// Prepare response
	resp := TransactionResponse{
		ID:        tx.ID,
		Sender:    tx.Sender,
		Receiver:  tx.Receiver,
		Amount:    tx.Amount,
		Fee:       tx.Fee,
		Timestamp: tx.Timestamp,
		Status:    tx.Status,
		Nonce:     tx.Nonce,
		Data:      string(tx.Data),
	}

	respondWithJSON(w, http.StatusCreated, resp)
}

// handleGetTransactionStatus retrieves the status of a transaction by ID
func (api *API) handleGetTransactionStatus(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	txID := vars["id"]

	if txID == "" {
		respondWithError(w, http.StatusBadRequest, "Transaction ID is required")
		return
	}

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

	// First check if it's in the pending pool
	pendingTxs, _ := api.TxManager.GetPendingTransactions()
	for _, tx := range pendingTxs {
		if tx.ID == txID {
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
	resp := map[string]interface{}{
		"poolSize":            poolSize,
		"totalProcessed":      stats.TotalProcessed,
		"totalConfirmed":      stats.TotalConfirmed,
		"totalRejected":       stats.TotalRejected,
		"avgProcessingTime":   stats.AvgProcessingTime.String(),
		"maxProcessingTime":   stats.MaxProcessingTime.String(),
		"avgConfirmationTime": stats.AvgConfirmationTime.String(),
		"lastProcessedTime":   stats.LastProcessedTime,
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
	resp := map[string]interface{}{
		"accountId":      accountID,
		"suggestedNonce": nonce,
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
