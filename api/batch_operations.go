// api/batch_operations.go
package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"diamante/consensus"

	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"
)

// BatchTransactionRequest represents a batch transaction request
type BatchTransactionRequest struct {
	Transactions []TransactionItem `json:"transactions"`
	FailOnError  bool              `json:"fail_on_error"` // If true, fail entire batch on any error
}

// TransactionItem represents a single transaction in a batch
type TransactionItem struct {
	From   string  `json:"from"`
	To     string  `json:"to"`
	Amount float64 `json:"amount"`
	Fee    float64 `json:"fee"`
	Data   string  `json:"data,omitempty"`
}

// BatchTransactionResponse represents the response for batch operations
type BatchTransactionResponse struct {
	BatchID        string                   `json:"batch_id"`
	Status         string                   `json:"status"`
	TotalCount     int                      `json:"total_count"`
	SuccessCount   int                      `json:"success_count"`
	FailureCount   int                      `json:"failure_count"`
	Results        []BatchTransactionResult `json:"results"`
	Timestamp      time.Time                `json:"timestamp"`
	ProcessingTime time.Duration            `json:"processing_time"`
}

// BatchTransactionResult represents the result of a single transaction in a batch
type BatchTransactionResult struct {
	Index         int    `json:"index"`
	TransactionID string `json:"transaction_id,omitempty"`
	Status        string `json:"status"`
	Error         string `json:"error,omitempty"`
}

// BatchStatusRequest represents a request to check batch status
type BatchStatusRequest struct {
	BatchID string `json:"batch_id"`
}

// BatchStatusResponse represents the response for batch status
type BatchStatusResponse struct {
	BatchID        string                   `json:"batch_id"`
	Status         string                   `json:"status"`
	TotalCount     int                      `json:"total_count"`
	ProcessedCount int                      `json:"processed_count"`
	SuccessCount   int                      `json:"success_count"`
	FailureCount   int                      `json:"failure_count"`
	Results        []BatchTransactionResult `json:"results"`
	CreatedAt      time.Time                `json:"created_at"`
	UpdatedAt      time.Time                `json:"updated_at"`
}

// SubmitBatchTransactions handles batch transaction submission
func (api *API) SubmitBatchTransactions(w http.ResponseWriter, r *http.Request) {
	api.Logger.Info("Processing batch transaction submission")

	var req BatchTransactionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// Validate batch size
	if len(req.Transactions) == 0 {
		http.Error(w, "Batch cannot be empty", http.StatusBadRequest)
		return
	}

	if len(req.Transactions) > 1000 { // Configurable limit
		http.Error(w, "Batch size exceeds maximum limit of 1000", http.StatusBadRequest)
		return
	}

	startTime := consensus.ConsensusNow()
	batchID := fmt.Sprintf("batch_%d", consensus.ConsensusUnix())

	api.Logger.WithFields(logrus.Fields{
		"batch_id":          batchID,
		"transaction_count": len(req.Transactions),
		"fail_on_error":     req.FailOnError,
	}).Info("Starting batch processing")

	var results []BatchTransactionResult
	successCount := 0
	failureCount := 0

	// Process each transaction in the batch
	for i, txItem := range req.Transactions {
		result := api.processBatchTransaction(i, txItem, req.FailOnError)
		results = append(results, result)

		if result.Status == "success" {
			successCount++
		} else {
			failureCount++

			// If fail_on_error is true and we encounter an error, stop processing
			if req.FailOnError {
				api.Logger.WithField("failed_at_index", i).Warn("Stopping batch processing due to error")
				break
			}
		}
	}

	// Determine overall batch status
	batchStatus := "completed"
	if failureCount > 0 && req.FailOnError {
		batchStatus = "failed"
	} else if failureCount > 0 {
		batchStatus = "partial_success"
	}

	processingTime := time.Since(startTime)

	response := BatchTransactionResponse{
		BatchID:        batchID,
		Status:         batchStatus,
		TotalCount:     len(req.Transactions),
		SuccessCount:   successCount,
		FailureCount:   failureCount,
		Results:        results,
		Timestamp:      consensus.ConsensusNow(),
		ProcessingTime: processingTime,
	}

	api.Logger.WithFields(logrus.Fields{
		"batch_id":        batchID,
		"status":          batchStatus,
		"success_count":   successCount,
		"failure_count":   failureCount,
		"processing_time": processingTime,
	}).Info("Batch processing completed")

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

// processBatchTransaction processes a single transaction within a batch
func (api *API) processBatchTransaction(index int, txItem TransactionItem, failOnError bool) BatchTransactionResult {
	// Validate transaction
	if txItem.From == "" || txItem.To == "" {
		return BatchTransactionResult{
			Index:  index,
			Status: "failed",
			Error:  "Missing sender or receiver",
		}
	}

	if txItem.Amount <= 0 {
		return BatchTransactionResult{
			Index:  index,
			Status: "failed",
			Error:  "Amount must be positive",
		}
	}

	// Create transaction using the transaction manager
	tx, err := api.TxManager.CreateTransaction(
		txItem.From,
		txItem.To,
		txItem.Amount,
		txItem.Fee,
		[]byte(txItem.Data),
	)
	if err != nil {
		return BatchTransactionResult{
			Index:  index,
			Status: "failed",
			Error:  fmt.Sprintf("Failed to create transaction: %v", err),
		}
	}

	return BatchTransactionResult{
		Index:         index,
		TransactionID: tx.ID,
		Status:        "success",
	}
}

// GetBatchStatus retrieves the status of a batch transaction
func (api *API) GetBatchStatus(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	batchID := vars["batch_id"]

	if batchID == "" {
		http.Error(w, "Batch ID is required", http.StatusBadRequest)
		return
	}

	api.Logger.WithField("batch_id", batchID).Info("Retrieving batch status")

	// Query the database for batch status
	batch, err := api.getBatchFromStorage(batchID)
	if err != nil {
		api.Logger.WithError(err).Error("Failed to retrieve batch from storage")
		http.Error(w, "Failed to retrieve batch status", http.StatusInternalServerError)
		return
	}

	if batch == nil {
		http.Error(w, "Batch not found", http.StatusNotFound)
		return
	}

	response := BatchStatusResponse{
		BatchID:        batch.ID,
		Status:         batch.Status,
		TotalCount:     batch.TotalCount,
		ProcessedCount: batch.ProcessedCount,
		SuccessCount:   batch.SuccessCount,
		FailureCount:   batch.FailureCount,
		Results:        batch.Results,
		CreatedAt:      batch.CreatedAt,
		UpdatedAt:      batch.UpdatedAt,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

// CancelBatch cancels a pending batch transaction
func (api *API) CancelBatch(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	batchID := vars["batch_id"]

	if batchID == "" {
		http.Error(w, "Batch ID is required", http.StatusBadRequest)
		return
	}

	api.Logger.WithField("batch_id", batchID).Info("Cancelling batch")

	// In a real implementation, this would:
	// 1. Check if batch exists and is in a cancellable state
	// 2. Stop processing any pending transactions
	// 3. Update batch status to "cancelled"
	// 4. Return appropriate response

	// CancelBatchResponse represents the response for batch cancellation
	type CancelBatchResponse struct {
		BatchID   string    `json:"batch_id"`
		Status    string    `json:"status"`
		Message   string    `json:"message"`
		Timestamp time.Time `json:"timestamp"`
	}

	response := CancelBatchResponse{
		BatchID:   batchID,
		Status:    "cancelled",
		Message:   "Batch has been cancelled successfully",
		Timestamp: consensus.ConsensusNow(),
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

// GetBatchHistory returns the history of batch operations
func (api *API) GetBatchHistory(w http.ResponseWriter, r *http.Request) {
	api.Logger.Info("Retrieving batch history")

	// Parse query parameters
	limitStr := r.URL.Query().Get("limit")
	offsetStr := r.URL.Query().Get("offset")
	status := r.URL.Query().Get("status")

	limit := 50 // Default limit
	if limitStr != "" {
		if l, err := fmt.Sscanf(limitStr, "%d", &limit); err != nil || l != 1 || limit < 1 || limit > 1000 {
			http.Error(w, "Invalid limit parameter", http.StatusBadRequest)
			return
		}
	}

	offset := 0 // Default offset
	if offsetStr != "" {
		if l, err := fmt.Sscanf(offsetStr, "%d", &offset); err != nil || l != 1 || offset < 0 {
			http.Error(w, "Invalid offset parameter", http.StatusBadRequest)
			return
		}
	}

	api.Logger.WithFields(logrus.Fields{
		"limit":  limit,
		"offset": offset,
		"status": status,
	}).Info("Querying batch history")

	// In a real implementation, this would query the database
	// For now, return mock data
	batches := []BatchStatusResponse{
		{
			BatchID:        "batch_1640000000001",
			Status:         "completed",
			TotalCount:     10,
			ProcessedCount: 10,
			SuccessCount:   9,
			FailureCount:   1,
			CreatedAt:      consensus.ConsensusNow().Add(-1 * time.Hour),
			UpdatedAt:      consensus.ConsensusNow().Add(-55 * time.Minute),
		},
		{
			BatchID:        "batch_1640000000002",
			Status:         "completed",
			TotalCount:     5,
			ProcessedCount: 5,
			SuccessCount:   5,
			FailureCount:   0,
			CreatedAt:      consensus.ConsensusNow().Add(-2 * time.Hour),
			UpdatedAt:      consensus.ConsensusNow().Add(-1 * time.Hour),
		},
	}

	// Filter by status if provided
	if status != "" {
		var filtered []BatchStatusResponse
		for _, batch := range batches {
			if batch.Status == status {
				filtered = append(filtered, batch)
			}
		}
		batches = filtered
	}

	// Apply pagination
	total := len(batches)
	if offset >= total {
		batches = []BatchStatusResponse{}
	} else {
		end := offset + limit
		if end > total {
			end = total
		}
		batches = batches[offset:end]
	}

	// BatchHistoryResponse represents the response for batch history
	type BatchHistoryResponse struct {
		Batches    []BatchStatusResponse `json:"batches"`
		TotalCount int                   `json:"total_count"`
		Limit      int                   `json:"limit"`
		Offset     int                   `json:"offset"`
		HasMore    bool                  `json:"has_more"`
	}

	response := BatchHistoryResponse{
		Batches:    batches,
		TotalCount: total,
		Limit:      limit,
		Offset:     offset,
		HasMore:    offset+limit < total,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

// ValidationError represents a validation error
type ValidationError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

// ValidationResult represents the result of validating a single item
type ValidationResult struct {
	Index  int      `json:"index"`
	Valid  bool     `json:"valid"`
	Errors []string `json:"errors"`
}

// BatchValidationResponse represents the response for batch validation
type BatchValidationResponse struct {
	Valid             bool               `json:"valid"`
	TotalCount        int                `json:"total_count"`
	ValidCount        int                `json:"valid_count"`
	InvalidCount      int                `json:"invalid_count"`
	ValidationResults []ValidationResult `json:"validation_results"`
	Timestamp         time.Time          `json:"timestamp"`
}

// ValidateBatch validates a batch request without processing it
func (api *API) ValidateBatch(w http.ResponseWriter, r *http.Request) {
	api.Logger.Info("Validating batch transaction request")

	var req BatchTransactionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	validationResults := make([]ValidationResult, len(req.Transactions))
	totalValid := 0
	totalInvalid := 0

	for i, txItem := range req.Transactions {
		result := ValidationResult{
			Index:  i,
			Valid:  true,
			Errors: []string{},
		}

		var errors []string

		// Validate required fields
		if txItem.From == "" {
			errors = append(errors, "sender is required")
		}
		if txItem.To == "" {
			errors = append(errors, "receiver is required")
		}
		if txItem.Amount <= 0 {
			errors = append(errors, "amount must be positive")
		}
		if txItem.Fee < 0 {
			errors = append(errors, "fee cannot be negative")
		}

		// Additional business logic validations could go here
		// e.g., check account existence, balance validation, etc.

		if len(errors) > 0 {
			result.Valid = false
			result.Errors = errors
			totalInvalid++
		} else {
			totalValid++
		}

		validationResults[i] = result
	}

	response := BatchValidationResponse{
		Valid:             totalInvalid == 0,
		TotalCount:        len(req.Transactions),
		ValidCount:        totalValid,
		InvalidCount:      totalInvalid,
		ValidationResults: validationResults,
		Timestamp:         consensus.ConsensusNow(),
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

// getBatchFromStorage retrieves a batch from storage by ID
func (api *API) getBatchFromStorage(batchID string) (*BatchInfo, error) {
	// In a real implementation, this would query a database
	// For now, return a simulated batch or nil if not found

	// Simulate batch storage with some sample data
	sampleBatches := map[string]*BatchInfo{
		"batch_001": {
			ID:             "batch_001",
			Status:         "completed",
			TotalCount:     3,
			ProcessedCount: 3,
			SuccessCount:   2,
			FailureCount:   1,
			Results: []BatchTransactionResult{
				{Index: 0, TransactionID: "tx_001", Status: "success"},
				{Index: 1, TransactionID: "tx_002", Status: "success"},
				{Index: 2, TransactionID: "tx_003", Status: "failed", Error: "insufficient balance"},
			},
			CreatedAt: consensus.ConsensusNow().Add(-10 * time.Minute),
			UpdatedAt: consensus.ConsensusNow().Add(-5 * time.Minute),
		},
		"batch_002": {
			ID:             "batch_002",
			Status:         "processing",
			TotalCount:     5,
			ProcessedCount: 2,
			SuccessCount:   2,
			FailureCount:   0,
			Results: []BatchTransactionResult{
				{Index: 0, TransactionID: "tx_004", Status: "success"},
				{Index: 1, TransactionID: "tx_005", Status: "success"},
				{Index: 2, TransactionID: "tx_006", Status: "pending"},
				{Index: 3, TransactionID: "tx_007", Status: "pending"},
				{Index: 4, TransactionID: "tx_008", Status: "pending"},
			},
			CreatedAt: consensus.ConsensusNow().Add(-5 * time.Minute),
			UpdatedAt: consensus.ConsensusNow().Add(-1 * time.Minute),
		},
	}

	return sampleBatches[batchID], nil
}

// BatchInfo represents stored batch information
type BatchInfo struct {
	ID             string
	Status         string
	TotalCount     int
	ProcessedCount int
	SuccessCount   int
	FailureCount   int
	Results        []BatchTransactionResult
	CreatedAt      time.Time
	UpdatedAt      time.Time
}
