// Package benchmarks provides comprehensive performance benchmarking for Diamante
package benchmarks

import (
	"context"
	"diamante/common"
	"encoding/json"
	"fmt"
	"runtime"
	"sync"
	"time"

	"diamante/types"
	"github.com/sirupsen/logrus"
)

// BenchmarkSuite manages and runs performance benchmarks
type BenchmarkSuite struct {
	name       string
	benchmarks map[string]Benchmark
	results    map[string]*BenchmarkResult
	config     *BenchmarkConfig
	logger     *logrus.Logger
	mu         sync.RWMutex
}

// Benchmark represents a single benchmark test
type Benchmark interface {
	// Name returns the benchmark name
	Name() string

	// Description returns a description of what the benchmark tests
	Description() string

	// Setup prepares the benchmark environment
	Setup(ctx context.Context) error

	// Run executes the benchmark
	Run(ctx context.Context, iterations int) (*BenchmarkMetrics, error)

	// Cleanup cleans up after the benchmark
	Cleanup(ctx context.Context) error

	// Validate validates the benchmark results
	Validate(metrics *BenchmarkMetrics) error
}

// BenchmarkConfig contains configuration for benchmarks
type BenchmarkConfig struct {
	Iterations    int            `json:"iterations"`
	WarmupRuns    int            `json:"warmup_runs"`
	Timeout       time.Duration  `json:"timeout"`
	Parallel      bool           `json:"parallel"`
	CPUProfile    bool           `json:"cpu_profile"`
	MemProfile    bool           `json:"mem_profile"`
	TraceProfile  bool           `json:"trace_profile"`
	OutputFormat  string         `json:"output_format"` // json, csv, table
	OutputPath    string         `json:"output_path"`
	TargetMetrics *TargetMetrics `json:"target_metrics"`
}

// TargetMetrics defines performance targets
type TargetMetrics struct {
	MinTPS        float64       `json:"min_tps"`         // Minimum transactions per second
	MaxLatencyP99 time.Duration `json:"max_latency_p99"` // Maximum 99th percentile latency
	MaxMemoryMB   float64       `json:"max_memory_mb"`   // Maximum memory usage in MB
	MaxCPUPercent float64       `json:"max_cpu_percent"` // Maximum CPU usage percentage
}

// BenchmarkResult contains the results of a benchmark run
type BenchmarkResult struct {
	Name        string            `json:"name"`
	Description string            `json:"description"`
	StartTime   time.Time         `json:"start_time"`
	EndTime     time.Time         `json:"end_time"`
	Duration    time.Duration     `json:"duration"`
	Iterations  int               `json:"iterations"`
	Metrics     *BenchmarkMetrics `json:"metrics"`
	SystemInfo  *SystemInfo       `json:"system_info"`
	Success     bool              `json:"success"`
	Error       string            `json:"error,omitempty"`
	Metadata    *types.TypedMap   `json:"metadata"`
}

// BenchmarkMetrics contains performance metrics
type BenchmarkMetrics struct {
	TotalOperations int64              `json:"total_operations"`
	TotalDuration   time.Duration      `json:"total_duration"`
	TPS             float64            `json:"tps"` // Transactions per second
	Latency         *LatencyMetrics    `json:"latency"`
	Throughput      *ThroughputMetrics `json:"throughput"`
	Resources       *ResourceMetrics   `json:"resources"`
	Errors          *ErrorMetrics      `json:"errors"`
	Custom          map[string]float64 `json:"custom,omitempty"`
}

// LatencyMetrics contains latency statistics
type LatencyMetrics struct {
	Min    time.Duration `json:"min"`
	Max    time.Duration `json:"max"`
	Mean   time.Duration `json:"mean"`
	Median time.Duration `json:"median"`
	P90    time.Duration `json:"p90"`
	P95    time.Duration `json:"p95"`
	P99    time.Duration `json:"p99"`
	StdDev time.Duration `json:"std_dev"`
}

// ThroughputMetrics contains throughput statistics
type ThroughputMetrics struct {
	BytesPerSecond    float64 `json:"bytes_per_second"`
	MessagesPerSecond float64 `json:"messages_per_second"`
	BlocksPerSecond   float64 `json:"blocks_per_second"`
}

// ResourceMetrics contains resource usage statistics
type ResourceMetrics struct {
	CPUUsagePercent float64       `json:"cpu_usage_percent"`
	MemoryUsageMB   float64       `json:"memory_usage_mb"`
	MemoryAllocMB   float64       `json:"memory_alloc_mb"`
	GoroutineCount  int           `json:"goroutine_count"`
	GCCount         uint32        `json:"gc_count"`
	GCPauseTotal    time.Duration `json:"gc_pause_total"`
}

// ErrorMetrics contains error statistics
type ErrorMetrics struct {
	TotalErrors  int64            `json:"total_errors"`
	ErrorRate    float64          `json:"error_rate"`
	ErrorsByType map[string]int64 `json:"errors_by_type"`
}

// SystemInfo contains system information
type SystemInfo struct {
	OS        string    `json:"os"`
	Arch      string    `json:"arch"`
	CPUCores  int       `json:"cpu_cores"`
	GoVersion string    `json:"go_version"`
	Timestamp time.Time `json:"timestamp"`
}

// NewBenchmarkSuite creates a new benchmark suite
func NewBenchmarkSuite(name string, config *BenchmarkConfig, logger *logrus.Logger) *BenchmarkSuite {
	if logger == nil {
		logger = logrus.New()
	}

	if config == nil {
		config = DefaultBenchmarkConfig()
	}

	return &BenchmarkSuite{
		name:       name,
		benchmarks: make(map[string]Benchmark),
		results:    make(map[string]*BenchmarkResult),
		config:     config,
		logger:     logger,
	}
}

// DefaultBenchmarkConfig returns default benchmark configuration
func DefaultBenchmarkConfig() *BenchmarkConfig {
	return &BenchmarkConfig{
		Iterations:   1000,
		WarmupRuns:   100,
		Timeout:      5 * time.Minute,
		Parallel:     false,
		OutputFormat: "json",
		TargetMetrics: &TargetMetrics{
			MinTPS:        100000, // 100k TPS target
			MaxLatencyP99: 10 * time.Millisecond,
			MaxMemoryMB:   2048,
			MaxCPUPercent: 80,
		},
	}
}

// Register registers a benchmark
func (s *BenchmarkSuite) Register(benchmark Benchmark) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	name := benchmark.Name()
	if _, exists := s.benchmarks[name]; exists {
		return fmt.Errorf("benchmark already registered: %s", name)
	}

	s.benchmarks[name] = benchmark
	s.logger.WithField("benchmark", name).Debug("Benchmark registered")

	return nil
}

// Run executes all registered benchmarks
func (s *BenchmarkSuite) Run(ctx context.Context) error {
	s.logger.WithField("suite", s.name).Info("Starting benchmark suite")

	// Get system info
	sysInfo := s.getSystemInfo()

	// Run benchmarks
	if s.config.Parallel {
		return s.runParallel(ctx, sysInfo)
	}

	return s.runSequential(ctx, sysInfo)
}

// RunBenchmark runs a specific benchmark
func (s *BenchmarkSuite) RunBenchmark(ctx context.Context, name string) (*BenchmarkResult, error) {
	s.mu.RLock()
	benchmark, exists := s.benchmarks[name]
	s.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("benchmark not found: %s", name)
	}

	return s.executeBenchmark(ctx, benchmark, s.getSystemInfo())
}

// runSequential runs benchmarks sequentially
func (s *BenchmarkSuite) runSequential(ctx context.Context, sysInfo *SystemInfo) error {
	s.mu.RLock()
	benchmarks := make([]Benchmark, 0, len(s.benchmarks))
	for _, b := range s.benchmarks {
		benchmarks = append(benchmarks, b)
	}
	s.mu.RUnlock()

	for _, benchmark := range benchmarks {
		result, err := s.executeBenchmark(ctx, benchmark, sysInfo)
		if err != nil {
			s.logger.WithError(err).WithField("benchmark", benchmark.Name()).Error("Benchmark failed")
		}

		s.mu.Lock()
		s.results[benchmark.Name()] = result
		s.mu.Unlock()
	}

	return nil
}

// runParallel runs benchmarks in parallel
func (s *BenchmarkSuite) runParallel(ctx context.Context, sysInfo *SystemInfo) error {
	s.mu.RLock()
	benchmarks := make([]Benchmark, 0, len(s.benchmarks))
	for _, b := range s.benchmarks {
		benchmarks = append(benchmarks, b)
	}
	s.mu.RUnlock()

	var wg sync.WaitGroup
	resultsChan := make(chan struct {
		name   string
		result *BenchmarkResult
	}, len(benchmarks))

	for _, benchmark := range benchmarks {
		wg.Add(1)
		go func(b Benchmark) {
			defer wg.Done()

			result, err := s.executeBenchmark(ctx, b, sysInfo)
			if err != nil {
				s.logger.WithError(err).WithField("benchmark", b.Name()).Error("Benchmark failed")
			}

			resultsChan <- struct {
				name   string
				result *BenchmarkResult
			}{name: b.Name(), result: result}
		}(benchmark)
	}

	// Wait for all benchmarks to complete
	go func() {
		wg.Wait()
		close(resultsChan)
	}()

	// Collect results
	for r := range resultsChan {
		s.mu.Lock()
		s.results[r.name] = r.result
		s.mu.Unlock()
	}

	return nil
}

// executeBenchmark executes a single benchmark
func (s *BenchmarkSuite) executeBenchmark(ctx context.Context, benchmark Benchmark, sysInfo *SystemInfo) (*BenchmarkResult, error) {
	s.logger.WithField("benchmark", benchmark.Name()).Info("Starting benchmark")

	result := &BenchmarkResult{
		Name:        benchmark.Name(),
		Description: benchmark.Description(),
		StartTime:   common.ConsensusNow(),
		SystemInfo:  sysInfo,
		Iterations:  s.config.Iterations,
		Metadata:    types.NewTypedMap(),
	}

	// Create timeout context
	benchCtx, cancel := context.WithTimeout(ctx, s.config.Timeout)
	defer cancel()

	// Setup
	if err := benchmark.Setup(benchCtx); err != nil {
		result.Success = false
		result.Error = fmt.Sprintf("setup failed: %v", err)
		result.EndTime = common.ConsensusNow()
		result.Duration = result.EndTime.Sub(result.StartTime)
		return result, err
	}

	// Warmup runs
	if s.config.WarmupRuns > 0 {
		s.logger.WithField("runs", s.config.WarmupRuns).Debug("Running warmup")
		_, err := benchmark.Run(benchCtx, s.config.WarmupRuns)
		if err != nil {
			result.Success = false
			result.Error = fmt.Sprintf("warmup failed: %v", err)
			result.EndTime = common.ConsensusNow()
			result.Duration = result.EndTime.Sub(result.StartTime)
			benchmark.Cleanup(context.Background())
			return result, err
		}

		// Reset metrics after warmup
		runtime.GC()
	}

	// Run benchmark
	metrics, err := benchmark.Run(benchCtx, s.config.Iterations)
	if err != nil {
		result.Success = false
		result.Error = fmt.Sprintf("run failed: %v", err)
		result.EndTime = common.ConsensusNow()
		result.Duration = result.EndTime.Sub(result.StartTime)
		benchmark.Cleanup(context.Background())
		return result, err
	}

	// Validate results
	if err := benchmark.Validate(metrics); err != nil {
		result.Success = false
		result.Error = fmt.Sprintf("validation failed: %v", err)
	} else {
		result.Success = true
	}

	// Cleanup
	if err := benchmark.Cleanup(context.Background()); err != nil {
		s.logger.WithError(err).Warn("Cleanup failed")
	}

	// Set final results
	result.EndTime = common.ConsensusNow()
	result.Duration = result.EndTime.Sub(result.StartTime)
	result.Metrics = metrics

	// Check against targets
	s.checkTargets(result)

	s.logger.WithFields(logrus.Fields{
		"benchmark": benchmark.Name(),
		"duration":  result.Duration,
		"tps":       metrics.TPS,
		"success":   result.Success,
	}).Info("Benchmark completed")

	return result, nil
}

// checkTargets checks if benchmark met performance targets
func (s *BenchmarkSuite) checkTargets(result *BenchmarkResult) {
	if s.config.TargetMetrics == nil || result.Metrics == nil {
		return
	}

	targets := s.config.TargetMetrics
	metrics := result.Metrics

	failures := []string{}

	// Check TPS
	if metrics.TPS < targets.MinTPS {
		failures = append(failures, fmt.Sprintf("TPS %.2f < target %.2f", metrics.TPS, targets.MinTPS))
	}

	// Check latency
	if metrics.Latency != nil && metrics.Latency.P99 > targets.MaxLatencyP99 {
		failures = append(failures, fmt.Sprintf("P99 latency %v > target %v", metrics.Latency.P99, targets.MaxLatencyP99))
	}

	// Check memory
	if metrics.Resources != nil && metrics.Resources.MemoryUsageMB > targets.MaxMemoryMB {
		failures = append(failures, fmt.Sprintf("Memory %.2fMB > target %.2fMB", metrics.Resources.MemoryUsageMB, targets.MaxMemoryMB))
	}

	// Check CPU
	if metrics.Resources != nil && metrics.Resources.CPUUsagePercent > targets.MaxCPUPercent {
		failures = append(failures, fmt.Sprintf("CPU %.2f%% > target %.2f%%", metrics.Resources.CPUUsagePercent, targets.MaxCPUPercent))
	}

	if len(failures) > 0 {
		result.Metadata.Set("target_failures", types.NewValue(types.ValueTypeJSON, mustMarshalJSON(failures)))
	}
}

// GetResults returns all benchmark results
func (s *BenchmarkSuite) GetResults() map[string]*BenchmarkResult {
	s.mu.RLock()
	defer s.mu.RUnlock()

	results := make(map[string]*BenchmarkResult)
	for k, v := range s.results {
		results[k] = v
	}

	return results
}

// GenerateReport generates a benchmark report
func (s *BenchmarkSuite) GenerateReport() (*BenchmarkReport, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	report := &BenchmarkReport{
		Suite:      s.name,
		Timestamp:  common.ConsensusNow(),
		Config:     s.config,
		Results:    make([]*BenchmarkResult, 0, len(s.results)),
		Summary:    s.generateSummary(),
		SystemInfo: s.getSystemInfo(),
	}

	for _, result := range s.results {
		report.Results = append(report.Results, result)
	}

	return report, nil
}

// generateSummary generates a summary of benchmark results
func (s *BenchmarkSuite) generateSummary() *BenchmarkSummary {
	summary := &BenchmarkSummary{
		TotalBenchmarks:   len(s.benchmarks),
		SuccessfulRuns:    0,
		FailedRuns:        0,
		AverageTPS:        0,
		AverageLatencyP99: 0,
		TotalDuration:     0,
	}

	var totalTPS float64
	var totalLatency time.Duration
	var latencyCount int

	for _, result := range s.results {
		summary.TotalDuration += result.Duration

		if result.Success {
			summary.SuccessfulRuns++
		} else {
			summary.FailedRuns++
		}

		if result.Metrics != nil {
			totalTPS += result.Metrics.TPS

			if result.Metrics.Latency != nil {
				totalLatency += result.Metrics.Latency.P99
				latencyCount++
			}
		}
	}

	if summary.SuccessfulRuns > 0 {
		summary.AverageTPS = totalTPS / float64(summary.SuccessfulRuns)

		if latencyCount > 0 {
			summary.AverageLatencyP99 = totalLatency / time.Duration(latencyCount)
		}
	}

	return summary
}

// getSystemInfo gathers system information
func (s *BenchmarkSuite) getSystemInfo() *SystemInfo {
	return &SystemInfo{
		OS:        runtime.GOOS,
		Arch:      runtime.GOARCH,
		CPUCores:  runtime.NumCPU(),
		GoVersion: runtime.Version(),
		Timestamp: common.ConsensusNow(),
	}
}

// SaveResults saves benchmark results to file
func (s *BenchmarkSuite) SaveResults() error {
	if s.config.OutputPath == "" {
		return nil
	}

	report, err := s.GenerateReport()
	if err != nil {
		return fmt.Errorf("failed to generate report: %w", err)
	}

	switch s.config.OutputFormat {
	case "json":
		return s.saveJSON(report)
	case "csv":
		return s.saveCSV(report)
	case "table":
		return s.saveTable(report)
	default:
		return fmt.Errorf("unsupported output format: %s", s.config.OutputFormat)
	}
}

func (s *BenchmarkSuite) saveJSON(report *BenchmarkReport) error {
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}

	return writeFile(s.config.OutputPath, data)
}

func (s *BenchmarkSuite) saveCSV(report *BenchmarkReport) error {
	// CSV implementation
	return fmt.Errorf("CSV output not implemented")
}

func (s *BenchmarkSuite) saveTable(report *BenchmarkReport) error {
	// Table implementation
	return fmt.Errorf("Table output not implemented")
}

// BenchmarkReport represents a complete benchmark report
type BenchmarkReport struct {
	Suite      string             `json:"suite"`
	Timestamp  time.Time          `json:"timestamp"`
	Config     *BenchmarkConfig   `json:"config"`
	Results    []*BenchmarkResult `json:"results"`
	Summary    *BenchmarkSummary  `json:"summary"`
	SystemInfo *SystemInfo        `json:"system_info"`
}

// BenchmarkSummary contains summary statistics
type BenchmarkSummary struct {
	TotalBenchmarks   int           `json:"total_benchmarks"`
	SuccessfulRuns    int           `json:"successful_runs"`
	FailedRuns        int           `json:"failed_runs"`
	AverageTPS        float64       `json:"average_tps"`
	AverageLatencyP99 time.Duration `json:"average_latency_p99"`
	TotalDuration     time.Duration `json:"total_duration"`
}

// Helper functions

func mustMarshalJSON(v interface{}) []byte {
	data, _ := json.Marshal(v)
	return data
}

func writeFile(path string, data []byte) error {
	// File writing implementation would go here
	return nil
}
