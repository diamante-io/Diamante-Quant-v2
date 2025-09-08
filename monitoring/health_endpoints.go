package monitoring

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"runtime"
	"time"

	"diamante/consensus"
	"diamante/storage"

	"github.com/sirupsen/logrus"
)

// MetadataValue represents a typed metadata value
type MetadataValue struct {
	Type  string      `json:"type"`
	Value interface{} `json:"value"`
}

// ComponentMetadata represents metadata for a component
type ComponentMetadata struct {
	Properties map[string]MetadataValue `json:"properties"`
}

// NewComponentMetadata creates a new component metadata instance
func NewComponentMetadata() ComponentMetadata {
	return ComponentMetadata{
		Properties: make(map[string]MetadataValue),
	}
}

// SetString sets a string metadata value
func (m ComponentMetadata) SetString(key, value string) {
	m.Properties[key] = MetadataValue{Type: "string", Value: value}
}

// SetNumber sets a numeric metadata value
func (m ComponentMetadata) SetNumber(key string, value float64) {
	m.Properties[key] = MetadataValue{Type: "number", Value: value}
}

// SetBool sets a boolean metadata value
func (m ComponentMetadata) SetBool(key string, value bool) {
	m.Properties[key] = MetadataValue{Type: "bool", Value: value}
}

// SetTime sets a time metadata value
func (m ComponentMetadata) SetTime(key string, value time.Time) {
	m.Properties[key] = MetadataValue{Type: "time", Value: value}
}

// ThresholdValue represents a typed threshold value
type ThresholdValue struct {
	Type   string  `json:"type"`
	Number float64 `json:"number,omitempty"`
	String string  `json:"string,omitempty"`
	Bool   bool    `json:"bool,omitempty"`
}

// CurrentValue represents a typed current value
type CurrentValue struct {
	Type   string  `json:"type"`
	Number float64 `json:"number,omitempty"`
	String string  `json:"string,omitempty"`
	Bool   bool    `json:"bool,omitempty"`
}

// HealthResponse represents a structured health check response
type HealthResponse struct {
	Status    string    `json:"status"`
	Timestamp time.Time `json:"timestamp"`
	Uptime    string    `json:"uptime,omitempty"`
	Ready     bool      `json:"ready,omitempty"`
	Alive     bool      `json:"alive,omitempty"`
}

// MetricsResponse represents a structured metrics response
type MetricsResponse struct {
	Timestamp     time.Time          `json:"timestamp"`
	SystemMetrics SystemMetrics      `json:"system_metrics"`
	AppMetrics    ApplicationMetrics `json:"app_metrics"`
}

// SystemMetrics represents system-level metrics
type SystemMetrics struct {
	CPUUsage       float64 `json:"cpu_usage"`
	MemoryUsage    float64 `json:"memory_usage"`
	GoroutineCount int     `json:"goroutine_count"`
	HeapAlloc      int64   `json:"heap_alloc"`
	HeapSys        int64   `json:"heap_sys"`
}

// ApplicationMetrics represents application-level metrics
type ApplicationMetrics struct {
	BlockHeight      uint64 `json:"block_height"`
	TransactionCount int64  `json:"transaction_count"`
	PeerCount        int    `json:"peer_count"`
	ConsensusHealth  string `json:"consensus_health"`
}

// DetailedHealthResponse represents a detailed health response
type DetailedHealthResponse struct {
	Status      string                   `json:"status"`
	Timestamp   time.Time                `json:"timestamp"`
	Components  map[string]ComponentInfo `json:"components"`
	SystemInfo  SystemInfo               `json:"system_info"`
	RuntimeInfo RuntimeInfo              `json:"runtime_info"`
	ConfigInfo  ConfigInfo               `json:"config_info"`
}

// ComponentInfo represents information about a component
type ComponentInfo struct {
	Status       string            `json:"status"`
	ResponseTime time.Duration     `json:"response_time"`
	ErrorMessage string            `json:"error_message,omitempty"`
	Details      map[string]string `json:"details"`
}

// SystemInfo represents system information
type SystemInfo struct {
	OS           string `json:"os"`
	Architecture string `json:"architecture"`
	CPUCount     int    `json:"cpu_count"`
	Hostname     string `json:"hostname"`
}

// RuntimeInfo represents runtime information
type RuntimeInfo struct {
	GoVersion       string `json:"go_version"`
	NumGoroutines   int    `json:"num_goroutines"`
	MemAllocMB      int64  `json:"mem_alloc_mb"`
	MemTotalAllocMB int64  `json:"mem_total_alloc_mb"`
	MemSysMB        int64  `json:"mem_sys_mb"`
	NumGC           uint32 `json:"num_gc"`
}

// ConfigInfo represents configuration information
type ConfigInfo struct {
	NodeID        string `json:"node_id"`
	NetworkID     string `json:"network_id"`
	ChainID       string `json:"chain_id"`
	ConsensusType string `json:"consensus_type"`
}

// HealthEndpoints provides comprehensive health monitoring endpoints
type HealthEndpoints struct {
	ledger    storage.Ledger
	logger    *logrus.Logger
	startTime time.Time

	// Health check configurations
	dbTimeout        time.Duration
	networkTimeout   time.Duration
	consensusTimeout time.Duration

	// Cached health data
	lastHealthCheck time.Time
	healthStatus    *HealthInfo

	// Dependencies for health checks
	dependencies []HealthDependency
}

// HealthInfo represents the overall system health
type HealthInfo struct {
	Status    string        `json:"status"`
	Timestamp time.Time     `json:"timestamp"`
	Uptime    time.Duration `json:"uptime"`
	Version   string        `json:"version"`

	// System metrics
	System SystemHealth `json:"system"`

	// Component health
	Database  ComponentHealth `json:"database"`
	Network   ComponentHealth `json:"network"`
	Consensus ComponentHealth `json:"consensus"`
	Storage   ComponentHealth `json:"storage"`
	API       ComponentHealth `json:"api"`
	VM        ComponentHealth `json:"vm"`

	// Dependencies health
	Dependencies []DependencyHealth `json:"dependencies"`

	// Performance metrics
	Performance PerformanceMetrics `json:"performance"`

	// Alerts and warnings
	Alerts   []HealthAlert   `json:"alerts"`
	Warnings []HealthWarning `json:"warnings"`

	// Overall health score (0-100)
	HealthScore int `json:"health_score"`
}

// SystemHealth represents system resource health
type SystemHealth struct {
	CPU         CPUHealth     `json:"cpu"`
	Memory      MemoryHealth  `json:"memory"`
	Disk        DiskHealth    `json:"disk"`
	Network     NetworkHealth `json:"network"`
	LoadAverage []float64     `json:"load_average"`
	Processes   ProcessHealth `json:"processes"`
}

// ComponentHealth represents individual component health
type ComponentHealth struct {
	Status       string            `json:"status"`
	Message      string            `json:"message"`
	LastChecked  time.Time         `json:"last_checked"`
	ResponseTime time.Duration     `json:"response_time"`
	ErrorCount   int               `json:"error_count"`
	Metadata     ComponentMetadata `json:"metadata"`
}

// DependencyHealth represents external dependency health
type DependencyHealth struct {
	Name         string            `json:"name"`
	Type         string            `json:"type"`
	Status       string            `json:"status"`
	Endpoint     string            `json:"endpoint"`
	ResponseTime time.Duration     `json:"response_time"`
	LastChecked  time.Time         `json:"last_checked"`
	ErrorMessage string            `json:"error_message,omitempty"`
	Metadata     ComponentMetadata `json:"metadata"`
}

// PerformanceMetrics represents system performance metrics
type PerformanceMetrics struct {
	TPS          float64       `json:"transactions_per_second"`
	BlockTime    time.Duration `json:"average_block_time"`
	PendingTxs   int           `json:"pending_transactions"`
	BlockHeight  uint64        `json:"current_block_height"`
	PeerCount    int           `json:"connected_peers"`
	SyncProgress float64       `json:"sync_progress"`

	// API metrics
	APIRequests int64         `json:"api_requests_total"`
	APILatency  time.Duration `json:"api_average_latency"`
	APIErrors   int64         `json:"api_errors_total"`

	// Contract metrics
	ContractCalls int64 `json:"contract_calls_total"`
	ContractGas   int64 `json:"contract_gas_used"`

	// Storage metrics
	StorageSize int64 `json:"storage_size_bytes"`
	StorageOps  int64 `json:"storage_operations"`
}

// HealthAlert represents a critical health alert
type HealthAlert struct {
	ID          string            `json:"id"`
	Type        string            `json:"type"`
	Severity    string            `json:"severity"`
	Message     string            `json:"message"`
	Timestamp   time.Time         `json:"timestamp"`
	Component   string            `json:"component"`
	Metadata    ComponentMetadata `json:"metadata"`
	ActionTaken string            `json:"action_taken,omitempty"`
}

// HealthWarning represents a health warning
type HealthWarning struct {
	ID             string         `json:"id"`
	Type           string         `json:"type"`
	Message        string         `json:"message"`
	Timestamp      time.Time      `json:"timestamp"`
	Component      string         `json:"component"`
	Threshold      ThresholdValue `json:"threshold"`
	CurrentValue   CurrentValue   `json:"current_value"`
	Recommendation string         `json:"recommendation"`
}

// CPUHealth represents CPU health metrics
type CPUHealth struct {
	Usage       float64   `json:"usage_percent"`
	LoadAverage []float64 `json:"load_average"`
	Cores       int       `json:"cores"`
	Temperature float64   `json:"temperature,omitempty"`
	Status      string    `json:"status"`
}

// MemoryHealth represents memory health metrics
type MemoryHealth struct {
	UsedBytes    int64   `json:"used_bytes"`
	TotalBytes   int64   `json:"total_bytes"`
	UsagePercent float64 `json:"usage_percent"`
	Available    int64   `json:"available_bytes"`
	Cached       int64   `json:"cached_bytes"`
	Buffers      int64   `json:"buffers_bytes"`
	SwapUsed     int64   `json:"swap_used_bytes"`
	SwapTotal    int64   `json:"swap_total_bytes"`
	Status       string  `json:"status"`
}

// DiskHealth represents disk health metrics
type DiskHealth struct {
	UsedBytes    int64   `json:"used_bytes"`
	TotalBytes   int64   `json:"total_bytes"`
	UsagePercent float64 `json:"usage_percent"`
	Available    int64   `json:"available_bytes"`
	IOPSRead     int64   `json:"iops_read"`
	IOPSWrite    int64   `json:"iops_write"`
	Status       string  `json:"status"`
}

// NetworkHealth represents network health metrics
type NetworkHealth struct {
	BytesIn    int64  `json:"bytes_in"`
	BytesOut   int64  `json:"bytes_out"`
	PacketsIn  int64  `json:"packets_in"`
	PacketsOut int64  `json:"packets_out"`
	ErrorsIn   int64  `json:"errors_in"`
	ErrorsOut  int64  `json:"errors_out"`
	DroppedIn  int64  `json:"dropped_in"`
	DroppedOut int64  `json:"dropped_out"`
	Status     string `json:"status"`
}

// ProcessHealth represents process health metrics
type ProcessHealth struct {
	PID             int     `json:"pid"`
	CPU             float64 `json:"cpu_percent"`
	Memory          int64   `json:"memory_bytes"`
	Threads         int     `json:"threads"`
	FileDescriptors int     `json:"file_descriptors"`
	Status          string  `json:"status"`
}

// HealthDependency represents an external dependency to check
type HealthDependency struct {
	Name      string
	Type      string
	Endpoint  string
	Timeout   time.Duration
	CheckFunc func(context.Context) error
	Critical  bool
}

// NewHealthEndpoints creates a new health endpoints instance
func NewHealthEndpoints(ledger storage.Ledger, logger *logrus.Logger) *HealthEndpoints {
	return &HealthEndpoints{
		ledger:           ledger,
		logger:           logger,
		startTime:        consensus.ConsensusNow(),
		dbTimeout:        5 * time.Second,
		networkTimeout:   3 * time.Second,
		consensusTimeout: 10 * time.Second,
		dependencies:     []HealthDependency{},
	}
}

// RegisterDependency registers an external dependency for health checking
func (h *HealthEndpoints) RegisterDependency(dep HealthDependency) {
	h.dependencies = append(h.dependencies, dep)
}

// HealthHandler handles the main health check endpoint
func (h *HealthEndpoints) HealthHandler(w http.ResponseWriter, r *http.Request) {
	// Check if we should return cached health status
	if consensus.ConsensusSince(h.lastHealthCheck) < 10*time.Second && h.healthStatus != nil {
		h.writeJSONResponse(w, h.healthStatus)
		return
	}

	// Perform comprehensive health check
	status := h.performHealthCheck(r.Context())

	// Cache the result
	h.lastHealthCheck = consensus.ConsensusNow()
	h.healthStatus = status

	// Set appropriate HTTP status code
	httpStatusCode := http.StatusOK
	if status.Status == "unhealthy" {
		httpStatusCode = http.StatusServiceUnavailable
	} else if status.Status == "degraded" {
		httpStatusCode = http.StatusPartialContent
	}

	w.WriteHeader(httpStatusCode)
	h.writeJSONResponse(w, status)
}

// ReadinessHandler handles the Kubernetes readiness probe
func (h *HealthEndpoints) ReadinessHandler(w http.ResponseWriter, r *http.Request) {
	// Check critical components only
	ready := h.checkReadiness(r.Context())

	response := HealthResponse{
		Ready:     ready,
		Timestamp: consensus.ConsensusNow(),
		Status:    "ready",
	}

	if ready {
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
	}

	h.writeJSONResponse(w, response)
}

// LivenessHandler handles the Kubernetes liveness probe
func (h *HealthEndpoints) LivenessHandler(w http.ResponseWriter, r *http.Request) {
	// Simple check to see if the service is responding
	alive := h.checkLiveness(r.Context())

	response := HealthResponse{
		Alive:     alive,
		Timestamp: consensus.ConsensusNow(),
		Uptime:    consensus.ConsensusSince(h.startTime).String(),
		Status:    "alive",
	}

	if alive {
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
	}

	h.writeJSONResponse(w, response)
}

// MetricsHandler handles the metrics endpoint
func (h *HealthEndpoints) MetricsHandler(w http.ResponseWriter, r *http.Request) {
	metrics := h.collectMetrics(r.Context())

	// Return metrics in Prometheus format or JSON based on Accept header
	if r.Header.Get("Accept") == "application/json" {
		h.writeJSONResponse(w, metrics)
	} else {
		// Return Prometheus format
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		h.writePrometheusMetrics(w, metrics)
	}
}

// DetailedHealthHandler provides detailed health information
func (h *HealthEndpoints) DetailedHealthHandler(w http.ResponseWriter, r *http.Request) {
	detailed := h.getDetailedHealth(r.Context())
	h.writeJSONResponse(w, detailed)
}

// performHealthCheck performs a comprehensive health check
func (h *HealthEndpoints) performHealthCheck(ctx context.Context) *HealthInfo {
	status := &HealthInfo{
		Timestamp: consensus.ConsensusNow(),
		Uptime:    consensus.ConsensusSince(h.startTime),
		Version:   "1.0.0",
		System:    h.getSystemHealth(),
		Alerts:    []HealthAlert{},
		Warnings:  []HealthWarning{},
	}

	// Check individual components
	status.Database = h.checkDatabaseHealth(ctx)
	status.Network = h.checkNetworkHealth(ctx)
	status.Consensus = h.checkConsensusHealth(ctx)
	status.Storage = h.checkStorageHealth(ctx)
	status.API = h.checkAPIHealth(ctx)
	status.VM = h.checkVMHealth(ctx)

	// Check dependencies
	status.Dependencies = h.checkDependencies(ctx)

	// Collect performance metrics
	status.Performance = h.collectPerformanceMetrics(ctx)

	// Calculate overall health score
	status.HealthScore = h.calculateHealthScore(status)

	// Determine overall status
	status.Status = h.determineOverallStatus(status)

	return status
}

// checkReadiness checks if the service is ready to serve traffic
func (h *HealthEndpoints) checkReadiness(ctx context.Context) bool {
	// Check database connectivity
	if !h.isDatabaseHealthy(ctx) {
		return false
	}

	// Check if consensus is running
	if !h.isConsensusHealthy(ctx) {
		return false
	}

	// Check storage accessibility
	if !h.isStorageHealthy(ctx) {
		return false
	}

	return true
}

// checkLiveness checks if the service is alive
func (h *HealthEndpoints) checkLiveness(ctx context.Context) bool {
	// Simple check - if we can respond, we're alive
	// In a more complex scenario, you might check for deadlocks, etc.
	return true
}

// isDatabaseHealthy checks if database is healthy
func (h *HealthEndpoints) isDatabaseHealthy(ctx context.Context) bool {
	ctx, cancel := context.WithTimeout(ctx, h.dbTimeout)
	defer cancel()

	// Try to get the latest block height as a health check
	_, err := h.ledger.GetBlockHeight()
	return err == nil
}

// isConsensusHealthy checks if consensus is healthy
func (h *HealthEndpoints) isConsensusHealthy(ctx context.Context) bool {
	// Try to get the latest block height
	height, err := h.ledger.GetBlockHeight()
	if err != nil {
		h.logger.WithError(err).Error("Failed to get block height for consensus health check")
		return false
	}

	// If we have blocks, assume consensus is working
	if height > 0 {
		return true
	}

	// If no blocks, check if ledger is responsive
	ctx, cancel := context.WithTimeout(ctx, h.consensusTimeout)
	defer cancel()

	// Try a simple ledger operation to check if it's responsive
	_, err = h.ledger.GetBlockHeight()
	return err == nil
}

// isStorageHealthy checks if storage is healthy
func (h *HealthEndpoints) isStorageHealthy(ctx context.Context) bool {
	// Create a context with timeout for storage operations
	ctx, cancel := context.WithTimeout(ctx, h.dbTimeout)
	defer cancel()

	// Try to perform a simple read operation
	_, err := h.ledger.GetBlockHeight()
	if err != nil {
		h.logger.WithError(err).Error("Storage health check failed - unable to get block height")
		return false
	}

	// Try to check if storage is responsive with a simple query
	// This will help detect if database connection is healthy
	startTime := consensus.ConsensusNow()
	_, err = h.ledger.GetBlockHeight()
	elapsed := consensus.ConsensusSince(startTime)

	if err != nil {
		h.logger.WithError(err).Error("Storage health check failed - second query failed")
		return false
	}

	// If query takes too long, consider storage unhealthy
	if elapsed > h.dbTimeout/2 {
		h.logger.WithField("elapsed", elapsed).Warn("Storage health check - query slow")
		return false
	}

	return true
}

// checkDatabaseHealth checks database connectivity and performance
func (h *HealthEndpoints) checkDatabaseHealth(ctx context.Context) ComponentHealth {
	startTime := consensus.ConsensusNow()

	// Test database connectivity
	health := ComponentHealth{
		Status:      "healthy",
		LastChecked: consensus.ConsensusNow(),
		Metadata:    NewComponentMetadata(),
	}

	// Test basic database operation
	_, err := h.ledger.GetBlockHeight()
	if err != nil {
		health.Status = "unhealthy"
		health.Metadata.SetString("error", err.Error())
	} else {
		health.ResponseTime = consensus.ConsensusSince(startTime)
	}

	return health
}

// checkNetworkHealth checks network connectivity and peer status
func (h *HealthEndpoints) checkNetworkHealth(ctx context.Context) ComponentHealth {
	start := consensus.ConsensusNow()

	health := ComponentHealth{
		Status:      "healthy",
		LastChecked: consensus.ConsensusNow(),
		Metadata:    NewComponentMetadata(),
	}

	// Add network-specific checks here
	health.ResponseTime = consensus.ConsensusSince(start)

	return health
}

// checkConsensusHealth checks consensus mechanism health
func (h *HealthEndpoints) checkConsensusHealth(ctx context.Context) ComponentHealth {
	start := consensus.ConsensusNow()
	health := ComponentHealth{
		Status:      "healthy",
		LastChecked: consensus.ConsensusNow(),
		Metadata:    NewComponentMetadata(),
	}

	// Check if we can get block height (indicates consensus is working)
	currentHeight, err := h.ledger.GetBlockHeight()
	if err != nil {
		health.Status = "unhealthy"
		health.Message = fmt.Sprintf("failed to get block height: %v", err)
		health.Metadata.SetString("error", err.Error())
	} else {
		// Check if blocks are being produced
		health.Metadata.SetNumber("blockHeight", float64(currentHeight))

		// For now, we can only check the block height
		// If we have blocks, consensus is at least partially working
		if currentHeight == 0 {
			health.Status = "degraded"
			health.Message = "no blocks in chain"
		}
	}

	health.ResponseTime = consensus.ConsensusSince(start)
	return health
}

// checkStorageHealth checks storage system health
func (h *HealthEndpoints) checkStorageHealth(ctx context.Context) ComponentHealth {
	start := consensus.ConsensusNow()
	health := ComponentHealth{
		Status:      "healthy",
		LastChecked: consensus.ConsensusNow(),
		Metadata:    NewComponentMetadata(),
	}

	// Test storage connectivity and performance
	// Try to perform a simple read operation
	testStart := consensus.ConsensusNow()
	_, err := h.ledger.GetBlockHeight()
	readLatency := consensus.ConsensusSince(testStart)

	if err != nil {
		health.Status = "unhealthy"
		health.Message = fmt.Sprintf("storage read failed: %v", err)
		health.Metadata.SetString("error", err.Error())
	} else {
		health.Metadata.SetNumber("readLatency", float64(readLatency.Milliseconds()))

		// Check if read latency is acceptable
		if readLatency > 100*time.Millisecond {
			health.Status = "degraded"
			health.Message = fmt.Sprintf("high storage latency: %v", readLatency)
		}

		// Additional storage health checks could be added here
		// For example, try to read a recent transaction
		// or verify account data consistency
	}

	health.ResponseTime = consensus.ConsensusSince(start)
	return health
}

// checkAPIHealth checks API endpoint health
func (h *HealthEndpoints) checkAPIHealth(ctx context.Context) ComponentHealth {
	start := consensus.ConsensusNow()
	health := ComponentHealth{
		Status:      "healthy",
		LastChecked: consensus.ConsensusNow(),
		Metadata:    NewComponentMetadata(),
	}

	// Check if API is responsive by testing internal endpoints
	// Since we're inside the API, we consider it healthy if we can process requests
	health.Metadata.SetNumber("requestsProcessed", 1)
	health.Metadata.SetBool("isResponsive", true)

	// Check goroutine count to detect potential leaks
	goroutineCount := runtime.NumGoroutine()
	health.Metadata.SetNumber("goroutineCount", float64(goroutineCount))

	// If goroutine count is extremely high, API might have issues
	if goroutineCount > 10000 {
		health.Status = "degraded"
		health.Message = fmt.Sprintf("high goroutine count: %d", goroutineCount)
	}

	health.ResponseTime = consensus.ConsensusSince(start)
	return health
}

// checkVMHealth checks virtual machine health
func (h *HealthEndpoints) checkVMHealth(ctx context.Context) ComponentHealth {
	start := consensus.ConsensusNow()
	health := ComponentHealth{
		Status:      "healthy",
		LastChecked: consensus.ConsensusNow(),
		Metadata:    NewComponentMetadata(),
	}

	// Check VM runtime health
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	// Add memory metrics
	health.Metadata.SetNumber("heapAlloc", float64(memStats.HeapAlloc))
	health.Metadata.SetNumber("heapSys", float64(memStats.HeapSys))
	health.Metadata.SetNumber("gcPauseTotal", float64(memStats.PauseTotalNs))
	health.Metadata.SetNumber("numGC", float64(memStats.NumGC))

	// Check memory usage
	memoryUsage := float64(memStats.HeapAlloc) / float64(memStats.HeapSys) * 100
	health.Metadata.SetNumber("memoryUsagePercent", memoryUsage)

	// If memory usage is too high, VM might have issues
	if memoryUsage > 90 {
		health.Status = "degraded"
		health.Message = fmt.Sprintf("high memory usage: %.2f%%", memoryUsage)
	}

	// Check GC pause time (last GC pause)
	if memStats.NumGC > 0 {
		lastGCPause := memStats.PauseNs[(memStats.NumGC+255)%256]
		health.Metadata.SetNumber("lastGCPause", float64(lastGCPause))

		// If GC pause is too long (>100ms), VM might be under pressure
		if lastGCPause > 100_000_000 { // 100ms in nanoseconds
			health.Status = "degraded"
			health.Message = fmt.Sprintf("high GC pause: %dms", lastGCPause/1_000_000)
		}
	}

	health.ResponseTime = consensus.ConsensusSince(start)
	return health
}

// checkDependencies checks health of external dependencies
func (h *HealthEndpoints) checkDependencies(ctx context.Context) []DependencyHealth {
	var deps []DependencyHealth

	for _, dep := range h.dependencies {
		depHealth := DependencyHealth{
			Name:        dep.Name,
			Type:        dep.Type,
			Endpoint:    dep.Endpoint,
			LastChecked: consensus.ConsensusNow(),
			Metadata:    NewComponentMetadata(),
		}

		start := consensus.ConsensusNow()
		err := dep.CheckFunc(ctx)
		depHealth.ResponseTime = consensus.ConsensusSince(start)

		if err != nil {
			depHealth.Status = "unhealthy"
			depHealth.ErrorMessage = err.Error()
		} else {
			depHealth.Status = "healthy"
		}

		deps = append(deps, depHealth)
	}

	return deps
}

// getSystemHealth gets system resource health
func (h *HealthEndpoints) getSystemHealth() SystemHealth {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	return SystemHealth{
		CPU: CPUHealth{
			Usage:  float64(runtime.NumGoroutine()) / 1000.0, // Proxy CPU usage using goroutine count
			Cores:  runtime.NumCPU(),
			Status: "healthy",
		},
		Memory: MemoryHealth{
			UsedBytes:    int64(mem.Alloc),
			TotalBytes:   int64(mem.Sys),
			UsagePercent: float64(mem.Alloc) / float64(mem.Sys) * 100,
			Status:       "healthy",
		},
		Disk: DiskHealth{
			Status: "healthy",
		},
		Network: NetworkHealth{
			Status: "healthy",
		},
		Processes: ProcessHealth{
			PID:     os.Getpid(),
			Threads: runtime.NumGoroutine(),
			Status:  "healthy",
		},
	}
}

// collectPerformanceMetrics collects performance metrics
func (h *HealthEndpoints) collectPerformanceMetrics(ctx context.Context) PerformanceMetrics {
	metrics := PerformanceMetrics{
		TPS:           0.0,
		BlockTime:     5 * time.Second,
		PendingTxs:    0,
		PeerCount:     0,
		SyncProgress:  100.0,
		APIRequests:   0,
		APILatency:    0,
		APIErrors:     0,
		ContractCalls: 0,
		ContractGas:   0,
		StorageSize:   0,
		StorageOps:    0,
	}

	// Get current block height
	if height, err := h.ledger.GetBlockHeight(); err == nil {
		metrics.BlockHeight = height
	}

	return metrics
}

// calculateHealthScore calculates overall health score (0-100)
func (h *HealthEndpoints) calculateHealthScore(status *HealthInfo) int {
	score := 100

	// Deduct points for unhealthy components
	components := []ComponentHealth{
		status.Database,
		status.Network,
		status.Consensus,
		status.Storage,
		status.API,
		status.VM,
	}

	for _, comp := range components {
		switch comp.Status {
		case "unhealthy":
			score -= 20
		case "degraded":
			score -= 10
		case "warning":
			score -= 5
		}
	}

	// Deduct points for unhealthy dependencies
	for _, dep := range status.Dependencies {
		if dep.Status == "unhealthy" {
			score -= 10
		}
	}

	// Deduct points for alerts
	score -= len(status.Alerts) * 5

	// Ensure score doesn't go below 0
	if score < 0 {
		score = 0
	}

	return score
}

// determineOverallStatus determines overall system status
func (h *HealthEndpoints) determineOverallStatus(status *HealthInfo) string {
	if status.HealthScore >= 90 {
		return "healthy"
	} else if status.HealthScore >= 70 {
		return "degraded"
	} else {
		return "unhealthy"
	}
}

// collectMetrics collects all system metrics
func (h *HealthEndpoints) collectMetrics(ctx context.Context) MetricsResponse {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	sysMetrics := SystemMetrics{
		CPUUsage:       float64(runtime.NumGoroutine()) / 1000.0,
		MemoryUsage:    float64(mem.Alloc) / float64(mem.Sys) * 100,
		GoroutineCount: runtime.NumGoroutine(),
		HeapAlloc:      int64(mem.Alloc),
		HeapSys:        int64(mem.Sys),
	}

	perf := h.collectPerformanceMetrics(ctx)
	appMetrics := ApplicationMetrics{
		BlockHeight:      perf.BlockHeight,
		TransactionCount: 0,
		PeerCount:        perf.PeerCount,
		ConsensusHealth:  "healthy",
	}

	return MetricsResponse{
		Timestamp:     consensus.ConsensusNow(),
		SystemMetrics: sysMetrics,
		AppMetrics:    appMetrics,
	}
}

// getDetailedHealth provides detailed health information
func (h *HealthEndpoints) getDetailedHealth(ctx context.Context) DetailedHealthResponse {
	health := h.performHealthCheck(ctx)

	components := make(map[string]ComponentInfo)
	components["database"] = ComponentInfo{
		Status:       health.Database.Status,
		ResponseTime: health.Database.ResponseTime,
		Details:      make(map[string]string),
	}
	components["network"] = ComponentInfo{
		Status:       health.Network.Status,
		ResponseTime: health.Network.ResponseTime,
		Details:      make(map[string]string),
	}
	components["consensus"] = ComponentInfo{
		Status:       health.Consensus.Status,
		ResponseTime: health.Consensus.ResponseTime,
		Details:      make(map[string]string),
	}

	hostname, _ := os.Hostname()

	return DetailedHealthResponse{
		Status:     health.Status,
		Timestamp:  health.Timestamp,
		Components: components,
		SystemInfo: SystemInfo{
			OS:           runtime.GOOS,
			Architecture: runtime.GOARCH,
			CPUCount:     runtime.NumCPU(),
			Hostname:     hostname,
		},
		RuntimeInfo: h.getRuntimeInfoStruct(),
		ConfigInfo:  h.getConfigurationInfoStruct(),
	}
}

// getRuntimeInfoStruct gets runtime information as a struct
func (h *HealthEndpoints) getRuntimeInfoStruct() RuntimeInfo {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	return RuntimeInfo{
		GoVersion:       runtime.Version(),
		NumGoroutines:   runtime.NumGoroutine(),
		MemAllocMB:      int64(mem.Alloc / 1024 / 1024),
		MemTotalAllocMB: int64(mem.TotalAlloc / 1024 / 1024),
		MemSysMB:        int64(mem.Sys / 1024 / 1024),
		NumGC:           mem.NumGC,
	}
}

// getConfigurationInfoStruct gets configuration information as a struct
func (h *HealthEndpoints) getConfigurationInfoStruct() ConfigInfo {
	return ConfigInfo{
		NodeID:        "node-01",                  // This should come from actual config
		NetworkID:     "mainnet",                  // This should come from actual config
		ChainID:       "diamante-1",               // This should come from actual config
		ConsensusType: "hybrid-poh-dpos-lachesis", // This should come from actual config
	}
}

// SystemEvent represents a system event
type SystemEvent struct {
	Type      string `json:"type"`
	Name      string `json:"name"`
	Timestamp int64  `json:"timestamp"`
	Error     string `json:"error,omitempty"`
}

// getRecentEvents gets recent system events
func (h *HealthEndpoints) getRecentEvents() []SystemEvent {
	// Get recent events from dependencies
	events := []SystemEvent{}
	for _, dep := range h.dependencies {
		// Check dependency health
		ctx, cancel := context.WithTimeout(context.Background(), dep.Timeout)
		err := dep.CheckFunc(ctx)
		cancel()

		if err == nil {
			events = append(events, SystemEvent{
				Type:      "dependency_healthy",
				Name:      dep.Name,
				Timestamp: consensus.ConsensusUnix(),
			})
		} else {
			events = append(events, SystemEvent{
				Type:      "dependency_unhealthy",
				Name:      dep.Name,
				Timestamp: consensus.ConsensusUnix(),
				Error:     err.Error(),
			})
		}
	}
	return events
}

// TroubleshootingInfo represents troubleshooting information
type TroubleshootingInfo struct {
	CommonIssues   []string `json:"common_issues"`
	SupportContact string   `json:"support_contact"`
	Documentation  string   `json:"documentation"`
}

// getTroubleshootingInfo gets troubleshooting information
func (h *HealthEndpoints) getTroubleshootingInfo() TroubleshootingInfo {
	return TroubleshootingInfo{
		CommonIssues: []string{
			"Check database connectivity",
			"Verify network configuration",
			"Check consensus participation",
			"Validate storage permissions",
		},
		SupportContact: "support@diamante.io",
		Documentation:  "https://docs.diamante.io/troubleshooting",
	}
}

// writeJSONResponse writes a JSON response
func (h *HealthEndpoints) writeJSONResponse(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(data); err != nil {
		h.logger.WithError(err).Error("Failed to write JSON response")
	}
}

// writePrometheusMetrics writes metrics in Prometheus format
func (h *HealthEndpoints) writePrometheusMetrics(w http.ResponseWriter, metrics MetricsResponse) {
	// Basic Prometheus metrics format implementation
	fmt.Fprintf(w, "# HELP diamante_uptime_seconds Time since startup\n")
	fmt.Fprintf(w, "# TYPE diamante_uptime_seconds counter\n")
	fmt.Fprintf(w, "diamante_uptime_seconds %f\n", time.Since(h.startTime).Seconds())

	// System metrics
	fmt.Fprintf(w, "# HELP diamante_cpu_usage CPU usage percentage\n")
	fmt.Fprintf(w, "# TYPE diamante_cpu_usage gauge\n")
	fmt.Fprintf(w, "diamante_cpu_usage %f\n", metrics.SystemMetrics.CPUUsage)

	fmt.Fprintf(w, "# HELP diamante_memory_usage Memory usage percentage\n")
	fmt.Fprintf(w, "# TYPE diamante_memory_usage gauge\n")
	fmt.Fprintf(w, "diamante_memory_usage %f\n", metrics.SystemMetrics.MemoryUsage)

	fmt.Fprintf(w, "# HELP diamante_goroutines Number of goroutines\n")
	fmt.Fprintf(w, "# TYPE diamante_goroutines gauge\n")
	fmt.Fprintf(w, "diamante_goroutines %d\n", metrics.SystemMetrics.GoroutineCount)

	// Application metrics
	fmt.Fprintf(w, "# HELP diamante_block_height Current block height\n")
	fmt.Fprintf(w, "# TYPE diamante_block_height counter\n")
	fmt.Fprintf(w, "diamante_block_height %d\n", metrics.AppMetrics.BlockHeight)
}

// RegisterHealthEndpoints registers all health endpoints with a router
func RegisterHealthEndpoints(ledger storage.Ledger, logger *logrus.Logger) *HealthEndpoints {
	endpoints := NewHealthEndpoints(ledger, logger)

	// Register common dependencies
	endpoints.RegisterDependency(HealthDependency{
		Name:     "database",
		Type:     "mongodb",
		Endpoint: "internal",
		Timeout:  5 * time.Second,
		CheckFunc: func(ctx context.Context) error {
			_, err := ledger.GetBlockHeight()
			return err
		},
		Critical: true,
	})

	return endpoints
}
