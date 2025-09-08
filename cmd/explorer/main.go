// Package main implements a production-ready blockchain explorer service for the Diamante blockchain
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/gorilla/mux"

	"diamante/cmd/common"
	"diamante/cmd/config"
	"diamante/storage"
)

// ExplorerServer represents the main explorer server
type ExplorerServer struct {
	config        *config.ExplorerConfig
	logger        common.Logger
	metrics       *common.MetricsCollector
	store         *storage.MongoAdapter
	httpServer    *http.Server
	metricsServer *http.Server
}

// NewExplorerServer creates a new explorer server instance
func NewExplorerServer() (*ExplorerServer, error) {
	// Load configuration
	cfg, err := config.LoadExplorerConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to load configuration: %w", err)
	}

	// Initialize logger
	logger, err := common.NewStructuredLogger(cfg.LogLevel, cfg.LogFormat)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize logger: %w", err)
	}

	logger.Info("Starting Diamante explorer service",
		"version", "1.0.0",
		"port", cfg.Port,
		"log_level", cfg.LogLevel,
	)

	// Initialize metrics collector
	var metrics *common.MetricsCollector
	if cfg.EnableMetrics {
		metrics = common.NewMetricsCollector()
		logger.Info("Metrics collection enabled", "metrics_port", cfg.MetricsPort)
	}

	// Initialize MongoDB storage adapter
	store, err := storage.NewMongoAdapter(
		cfg.MongoURL,
		cfg.DatabaseName,
		nil, // Use default configuration
		0,   // Use default timeout
	)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize storage adapter: %w", err)
	}

	if err := store.Open(); err != nil {
		return nil, fmt.Errorf("failed to open storage adapter: %w", err)
	}

	logger.Info("Connected to MongoDB",
		"url", maskSensitiveURL(cfg.MongoURL),
		"database", cfg.DatabaseName,
	)

	return &ExplorerServer{
		config:  cfg,
		logger:  logger,
		metrics: metrics,
		store:   store,
	}, nil
}

// setupRoutes configures HTTP routes
func (es *ExplorerServer) setupRoutes() *mux.Router {
	// Create rate limiter
	rateLimiter := common.NewRateLimiter(
		es.config.RateLimit,
		es.config.RateBurst,
		es.config.RateWindow,
	)

	// Setup router with middleware
	router := common.SetupRouter(rateLimiter, es.logger, es.config.EnableCORS)

	// API routes
	apiRouter := router.PathPrefix("/api/v1").Subrouter()
	apiRouter.HandleFunc("/blocks", es.handleBlocks).Methods("GET")
	apiRouter.HandleFunc("/block/{height:[0-9]+}", es.handleBlockByHeight).Methods("GET")
	apiRouter.HandleFunc("/transaction/{hash:[a-fA-F0-9]+}", es.handleTransaction).Methods("GET")
	apiRouter.HandleFunc("/receipt/{hash:[a-fA-F0-9]+}", es.handleReceipt).Methods("GET")
	apiRouter.HandleFunc("/events/{hash:[a-fA-F0-9]+}", es.handleEvents).Methods("GET")
	apiRouter.HandleFunc("/search", es.handleSearch).Methods("GET")

	// Health check routes
	router.HandleFunc("/health", common.HealthCheck()).Methods("GET")
	router.HandleFunc("/ready", common.ReadinessCheck([]common.HealthChecker{es})).Methods("GET")

	// Add metrics middleware if enabled
	if es.metrics != nil {
		router.Use(es.metrics.MetricsMiddleware)
	}

	return router
}

// handleBlocks returns a range of blocks
func (es *ExplorerServer) handleBlocks(w http.ResponseWriter, r *http.Request) {
	startStr := common.SanitizeString(r.URL.Query().Get("start"))
	endStr := common.SanitizeString(r.URL.Query().Get("end"))

	// Validate block range
	start, end, err := common.ValidateBlockRange(startStr, endStr)
	if err != nil {
		es.logger.Warn("Invalid block range request",
			"start", startStr,
			"end", endStr,
			"error", err,
			"client_ip", getClientIP(r),
		)
		http.Error(w, fmt.Sprintf("Invalid block range: %v", err), http.StatusBadRequest)
		return
	}

	if es.metrics != nil {
		es.metrics.IncrementCounter("explorer_blocks_requests")
	}

	// Get blocks from storage
	blocks, err := es.store.GetBlockRange(start, end)
	if err != nil {
		es.logger.Error("Failed to get block range",
			"start", start,
			"end", end,
			"error", err,
		)
		if es.metrics != nil {
			es.metrics.IncrementCounter("explorer_blocks_errors")
		}
		http.Error(w, "Failed to retrieve blocks", http.StatusInternalServerError)
		return
	}

	// Return successful response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	if err := json.NewEncoder(w).Encode(map[string]interface{}{
		"blocks": blocks,
		"count":  len(blocks),
		"range": map[string]uint64{
			"start": start,
			"end":   end,
		},
	}); err != nil {
		es.logger.Error("Failed to encode blocks response", "error", err)
	}

	es.logger.Info("Blocks request served",
		"start", start,
		"end", end,
		"count", len(blocks),
		"client_ip", getClientIP(r),
	)
}

// handleBlockByHeight returns a specific block by height
func (es *ExplorerServer) handleBlockByHeight(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	heightStr := vars["height"]

	height, err := strconv.ParseUint(heightStr, 10, 64)
	if err != nil {
		es.logger.Warn("Invalid block height",
			"height", heightStr,
			"error", err,
			"client_ip", getClientIP(r),
		)
		http.Error(w, "Invalid block height", http.StatusBadRequest)
		return
	}

	if es.metrics != nil {
		es.metrics.IncrementCounter("explorer_block_requests")
	}

	// Get single block
	blocks, err := es.store.GetBlockRange(height, height)
	if err != nil {
		es.logger.Error("Failed to get block",
			"height", height,
			"error", err,
		)
		if es.metrics != nil {
			es.metrics.IncrementCounter("explorer_block_errors")
		}
		http.Error(w, "Failed to retrieve block", http.StatusInternalServerError)
		return
	}

	if len(blocks) == 0 {
		http.Error(w, "Block not found", http.StatusNotFound)
		return
	}

	// Return successful response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	if err := json.NewEncoder(w).Encode(blocks[0]); err != nil {
		es.logger.Error("Failed to encode block response", "error", err)
	}

	es.logger.Info("Block request served",
		"height", height,
		"client_ip", getClientIP(r),
	)
}

// handleTransaction returns transaction details
func (es *ExplorerServer) handleTransaction(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	txHash := common.SanitizeString(vars["hash"])

	// Validate transaction ID format
	if err := common.ValidateTransactionID(txHash); err != nil {
		es.logger.Warn("Invalid transaction hash",
			"hash", txHash,
			"error", err,
			"client_ip", getClientIP(r),
		)
		http.Error(w, fmt.Sprintf("Invalid transaction hash: %v", err), http.StatusBadRequest)
		return
	}

	if es.metrics != nil {
		es.metrics.IncrementCounter("explorer_transaction_requests")
	}

	// Get transaction from storage
	tx, err := es.store.GetTransaction(txHash)
	if err != nil {
		es.logger.Error("Failed to get transaction",
			"hash", txHash,
			"error", err,
		)
		if es.metrics != nil {
			es.metrics.IncrementCounter("explorer_transaction_errors")
		}
		http.Error(w, "Failed to retrieve transaction", http.StatusInternalServerError)
		return
	}

	// Return successful response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	if err := json.NewEncoder(w).Encode(tx); err != nil {
		es.logger.Error("Failed to encode transaction response", "error", err)
	}

	es.logger.Info("Transaction request served",
		"hash", txHash,
		"client_ip", getClientIP(r),
	)
}

// handleReceipt returns transaction receipt
func (es *ExplorerServer) handleReceipt(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	txHash := common.SanitizeString(vars["hash"])

	// Validate transaction ID format
	if err := common.ValidateTransactionID(txHash); err != nil {
		es.logger.Warn("Invalid transaction hash for receipt",
			"hash", txHash,
			"error", err,
			"client_ip", getClientIP(r),
		)
		http.Error(w, fmt.Sprintf("Invalid transaction hash: %v", err), http.StatusBadRequest)
		return
	}

	if es.metrics != nil {
		es.metrics.IncrementCounter("explorer_receipt_requests")
	}

	// Get receipt from storage
	receipt, err := es.store.GetReceipt(txHash)
	if err != nil {
		es.logger.Error("Failed to get receipt",
			"hash", txHash,
			"error", err,
		)
		if es.metrics != nil {
			es.metrics.IncrementCounter("explorer_receipt_errors")
		}
		http.Error(w, "Failed to retrieve receipt", http.StatusInternalServerError)
		return
	}

	// Return successful response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	if err := json.NewEncoder(w).Encode(receipt); err != nil {
		es.logger.Error("Failed to encode receipt response", "error", err)
	}

	es.logger.Info("Receipt request served",
		"hash", txHash,
		"client_ip", getClientIP(r),
	)
}

// handleEvents returns transaction events/logs
func (es *ExplorerServer) handleEvents(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	txHash := common.SanitizeString(vars["hash"])

	// Validate transaction ID format
	if err := common.ValidateTransactionID(txHash); err != nil {
		es.logger.Warn("Invalid transaction hash for events",
			"hash", txHash,
			"error", err,
			"client_ip", getClientIP(r),
		)
		http.Error(w, fmt.Sprintf("Invalid transaction hash: %v", err), http.StatusBadRequest)
		return
	}

	if es.metrics != nil {
		es.metrics.IncrementCounter("explorer_events_requests")
	}

	// Get receipt to extract events
	receipt, err := es.store.GetReceipt(txHash)
	if err != nil {
		es.logger.Error("Failed to get receipt for events",
			"hash", txHash,
			"error", err,
		)
		if es.metrics != nil {
			es.metrics.IncrementCounter("explorer_events_errors")
		}
		http.Error(w, "Failed to retrieve events", http.StatusInternalServerError)
		return
	}

	// Extract events/logs from receipt
	events := map[string]interface{}{
		"transaction_hash": txHash,
		"logs":             receipt.Logs,
		"events_count":     len(receipt.Logs),
	}

	// Return successful response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	if err := json.NewEncoder(w).Encode(events); err != nil {
		es.logger.Error("Failed to encode events response", "error", err)
	}

	es.logger.Info("Events request served",
		"hash", txHash,
		"events_count", len(receipt.Logs),
		"client_ip", getClientIP(r),
	)
}

// handleSearch provides search functionality
func (es *ExplorerServer) handleSearch(w http.ResponseWriter, r *http.Request) {
	query := common.SanitizeString(r.URL.Query().Get("q"))
	searchType := common.SanitizeString(r.URL.Query().Get("type"))

	if query == "" {
		http.Error(w, "Search query is required", http.StatusBadRequest)
		return
	}

	if es.metrics != nil {
		es.metrics.IncrementCounter("explorer_search_requests")
	}

	var result interface{}
	var err error

	switch searchType {
	case "block":
		if height, parseErr := strconv.ParseUint(query, 10, 64); parseErr == nil {
			result, err = es.store.GetBlockRange(height, height)
		} else {
			http.Error(w, "Invalid block height for search", http.StatusBadRequest)
			return
		}
	case "transaction":
		result, err = es.store.GetTransaction(query)
	case "address":
		// For address search, we'd need additional methods in storage
		result = map[string]string{"message": "Address search not yet implemented"}
	default:
		// Auto-detect search type based on query format
		if height, parseErr := strconv.ParseUint(query, 10, 64); parseErr == nil {
			// Looks like a block height
			result, err = es.store.GetBlockRange(height, height)
		} else if len(query) == 64 {
			// Looks like a transaction hash
			result, err = es.store.GetTransaction(query)
		} else {
			http.Error(w, "Unable to determine search type", http.StatusBadRequest)
			return
		}
	}

	if err != nil {
		es.logger.Error("Search failed",
			"query", query,
			"type", searchType,
			"error", err,
		)
		if es.metrics != nil {
			es.metrics.IncrementCounter("explorer_search_errors")
		}
		http.Error(w, "Search failed", http.StatusInternalServerError)
		return
	}

	// Return successful response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	searchResult := map[string]interface{}{
		"query":  query,
		"type":   searchType,
		"result": result,
	}

	if err := json.NewEncoder(w).Encode(searchResult); err != nil {
		es.logger.Error("Failed to encode search response", "error", err)
	}

	es.logger.Info("Search request served",
		"query", query,
		"type", searchType,
		"client_ip", getClientIP(r),
	)
}

// Start starts the explorer server
func (es *ExplorerServer) Start() error {
	// Setup routes
	router := es.setupRoutes()

	// Configure main HTTP server
	address := fmt.Sprintf("%s:%d", es.config.Host, es.config.Port)
	es.httpServer = &http.Server{
		Addr:         address,
		Handler:      router,
		ReadTimeout:  es.config.Timeout,
		WriteTimeout: es.config.Timeout,
		IdleTimeout:  es.config.Timeout * 2,
	}

	// Start metrics server if enabled
	if es.config.EnableMetrics && es.metrics != nil {
		es.startMetricsServer()
	}

	// Start system metrics update routine
	if es.metrics != nil {
		go es.startMetricsRoutine()
	}

	es.logger.Info("Explorer server starting", "address", address)

	// Start server
	if es.config.EnableHTTPS {
		return es.httpServer.ListenAndServeTLS(es.config.TLSCertFile, es.config.TLSKeyFile)
	}
	return es.httpServer.ListenAndServe()
}

// startMetricsServer starts the metrics HTTP server
func (es *ExplorerServer) startMetricsServer() {
	metricsAddr := fmt.Sprintf("%s:%d", es.config.Host, es.config.MetricsPort)

	metricsRouter := mux.NewRouter()
	metricsRouter.Handle("/metrics", es.metrics.MetricsHandler())

	es.metricsServer = &http.Server{
		Addr:    metricsAddr,
		Handler: metricsRouter,
	}

	go func() {
		es.logger.Info("Metrics server starting", "address", metricsAddr)
		if err := es.metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			es.logger.Error("Metrics server error", "error", err)
		}
	}()
}

// startMetricsRoutine starts the periodic metrics update routine
func (es *ExplorerServer) startMetricsRoutine() {
	ticker := time.NewTicker(30 * time.Second) // Update every 30 seconds
	defer ticker.Stop()

	for range ticker.C {
		es.metrics.UpdateSystemMetrics()
	}
}

// Shutdown gracefully shuts down the server
func (es *ExplorerServer) Shutdown(ctx context.Context) error {
	es.logger.Info("Shutting down explorer server")

	// Shutdown main HTTP server
	if err := es.httpServer.Shutdown(ctx); err != nil {
		es.logger.Error("Error shutting down HTTP server", "error", err)
		return err
	}

	// Shutdown metrics server
	if es.metricsServer != nil {
		if err := es.metricsServer.Shutdown(ctx); err != nil {
			es.logger.Error("Error shutting down metrics server", "error", err)
		}
	}

	// Close storage adapter
	if es.store != nil {
		if err := es.store.Close(); err != nil {
			es.logger.Error("Error closing storage adapter", "error", err)
		}
	}

	es.logger.Info("Explorer server shutdown completed")
	return nil
}

// HealthCheck implements the HealthChecker interface
func (es *ExplorerServer) HealthCheck() error {
	if es.store == nil {
		return fmt.Errorf("storage adapter is not initialized")
	}

	// We could add more sophisticated health checks here
	// For now, just check that the server is properly configured
	if es.config.Port <= 0 {
		return fmt.Errorf("invalid port configuration")
	}

	return nil
}

// Helper functions

// getClientIP extracts client IP from request
func getClientIP(r *http.Request) string {
	// Check X-Forwarded-For header first
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		return xff
	}

	// Check X-Real-IP header
	if xri := r.Header.Get("X-Real-IP"); xri != "" {
		return xri
	}

	// Fall back to RemoteAddr
	return r.RemoteAddr
}

// maskSensitiveURL masks credentials in MongoDB URL for logging
func maskSensitiveURL(url string) string {
	// Simple masking - in production, use a more robust solution
	if len(url) > 20 {
		return url[:10] + "***" + url[len(url)-7:]
	}
	return "***"
}

// main is the entry point for the explorer service
func main() {
	// Create server instance
	server, err := NewExplorerServer()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize explorer server: %v\n", err)
		os.Exit(1)
	}

	// Setup graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	// Start server in a goroutine
	serverErr := make(chan error, 1)
	go func() {
		if err := server.Start(); err != nil && err != http.ErrServerClosed {
			serverErr <- fmt.Errorf("server failed to start: %w", err)
		}
	}()

	// Wait for shutdown signal or server error
	select {
	case err := <-serverErr:
		server.logger.Error("Server error", "error", err)
		os.Exit(1)
	case sig := <-quit:
		server.logger.Info("Received shutdown signal", "signal", sig.String())

		// Create shutdown context with timeout
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		// Perform graceful shutdown
		if err := server.Shutdown(ctx); err != nil {
			server.logger.Error("Graceful shutdown failed", "error", err)
			os.Exit(1)
		}

		server.logger.Info("Explorer service stopped successfully")
	}
}
