// Package benchmarks provides the main benchmark runner
package benchmarks

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
)

// Runner manages and executes benchmarks
type Runner struct {
	suite  *BenchmarkSuite
	config *RunnerConfig
	logger *logrus.Logger
}

// BenchmarkConfigValues holds specific benchmark configurations
type BenchmarkConfigValues struct {
	Transaction *TransactionBenchmarkConfig `json:"transaction,omitempty"`
	Consensus   *ConsensusBenchmarkConfig   `json:"consensus,omitempty"`
	Storage     *StorageBenchmarkConfig     `json:"storage,omitempty"`
}

// NetworkBenchmarkConfig placeholder
type NetworkBenchmarkConfig struct {
	// Add network benchmark configuration fields
}

// CryptoBenchmarkConfig placeholder
type CryptoBenchmarkConfig struct {
	// Add crypto benchmark configuration fields
}

// WASMBenchmarkConfig placeholder
type WASMBenchmarkConfig struct {
	// Add WASM benchmark configuration fields
}

// RunnerConfig contains configuration for the benchmark runner
type RunnerConfig struct {
	SuiteName        string                 `json:"suite_name"`
	OutputDir        string                 `json:"output_dir"`
	EnableProfiling  bool                   `json:"enable_profiling"`
	BenchmarkConfigs *BenchmarkConfigValues `json:"benchmark_configs"`
	RunOnly          []string               `json:"run_only"`
	SkipBenchmarks   []string               `json:"skip_benchmarks"`
	Iterations       int                    `json:"iterations"`
	WarmupRuns       int                    `json:"warmup_runs"`
	Timeout          time.Duration          `json:"timeout"`
	Parallel         bool                   `json:"parallel"`
}

// NewRunner creates a new benchmark runner
func NewRunner(config *RunnerConfig, logger *logrus.Logger) *Runner {
	if logger == nil {
		logger = logrus.New()
		logger.SetLevel(logrus.InfoLevel)
		logger.SetFormatter(&logrus.TextFormatter{
			FullTimestamp: true,
		})
	}

	if config == nil {
		config = DefaultRunnerConfig()
	}

	// Create benchmark suite
	suiteConfig := &BenchmarkConfig{
		Iterations:   config.Iterations,
		WarmupRuns:   config.WarmupRuns,
		Timeout:      config.Timeout,
		Parallel:     config.Parallel,
		OutputFormat: "json",
		OutputPath:   filepath.Join(config.OutputDir, "results.json"),
	}

	suite := NewBenchmarkSuite(config.SuiteName, suiteConfig, logger)

	return &Runner{
		suite:  suite,
		config: config,
		logger: logger,
	}
}

// DefaultRunnerConfig returns default runner configuration
func DefaultRunnerConfig() *RunnerConfig {
	return &RunnerConfig{
		SuiteName:        "Diamante Performance Benchmarks",
		OutputDir:        "./benchmark-results",
		EnableProfiling:  false,
		Iterations:       1000,
		WarmupRuns:       100,
		Timeout:          5 * time.Minute,
		Parallel:         false,
		BenchmarkConfigs: &BenchmarkConfigValues{},
	}
}

// RegisterDefaultBenchmarks registers all default benchmarks
func (r *Runner) RegisterDefaultBenchmarks() error {
	// Transaction benchmark
	txConfig := &TransactionBenchmarkConfig{
		PoolSize:        10000,
		WorkerCount:     10,
		TransactionSize: 256,
		BatchSize:       100,
	}

	if r.config.BenchmarkConfigs.Transaction != nil {
		txConfig = r.config.BenchmarkConfigs.Transaction
	}

	txBenchmark := NewTransactionBenchmark(txConfig, r.logger)
	if err := r.suite.Register(txBenchmark); err != nil {
		return err
	}

	// Consensus benchmark
	consensusConfig := &ConsensusBenchmarkConfig{
		ValidatorCount:       100,
		BlockSize:            1024 * 1024,
		TransactionsPerBlock: 1000,
		NetworkLatency:       10 * time.Millisecond,
		ByzantineNodes:       0,
		ConsensusTimeout:     5 * time.Second,
	}

	if r.config.BenchmarkConfigs.Consensus != nil {
		consensusConfig = r.config.BenchmarkConfigs.Consensus
	}

	consensusBenchmark := NewConsensusBenchmark(consensusConfig, r.logger)
	if err := r.suite.Register(consensusBenchmark); err != nil {
		return err
	}

	// Storage benchmark
	storageConfig := &StorageBenchmarkConfig{
		StorageType:     "memory",
		DataSize:        1024,
		KeyPrefix:       "bench",
		ReadWriteRatio:  0.8,
		QueryComplexity: 10,
		BatchSize:       100,
		ConcurrentOps:   10,
	}

	if r.config.BenchmarkConfigs.Storage != nil {
		storageConfig = r.config.BenchmarkConfigs.Storage
	}

	storageBenchmark := NewStorageBenchmark(storageConfig, r.logger)
	if err := r.suite.Register(storageBenchmark); err != nil {
		return err
	}

	// Add more benchmarks here...

	return nil
}

// Run executes all registered benchmarks
func (r *Runner) Run(ctx context.Context) error {
	r.logger.WithField("suite", r.config.SuiteName).Info("Starting benchmark suite")

	// Create output directory
	if err := os.MkdirAll(r.config.OutputDir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	// Filter benchmarks if needed
	if len(r.config.RunOnly) > 0 || len(r.config.SkipBenchmarks) > 0 {
		if err := r.filterBenchmarks(); err != nil {
			return err
		}
	}

	// Run benchmarks
	if err := r.suite.Run(ctx); err != nil {
		return fmt.Errorf("benchmark suite failed: %w", err)
	}

	// Generate and save report
	report, err := r.suite.GenerateReport()
	if err != nil {
		return fmt.Errorf("failed to generate report: %w", err)
	}

	// Save results
	if err := r.saveResults(report); err != nil {
		return fmt.Errorf("failed to save results: %w", err)
	}

	// Print summary
	r.printSummary(report)

	return nil
}

// filterBenchmarks filters benchmarks based on configuration
func (r *Runner) filterBenchmarks() error {
	// Implementation would filter benchmarks based on RunOnly and SkipBenchmarks
	return nil
}

// saveResults saves benchmark results
func (r *Runner) saveResults(report *BenchmarkReport) error {
	// Save JSON report
	jsonPath := filepath.Join(r.config.OutputDir, "report.json")
	jsonData, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}

	if err := os.WriteFile(jsonPath, jsonData, 0644); err != nil {
		return err
	}

	// Save individual benchmark results
	for _, result := range report.Results {
		resultPath := filepath.Join(r.config.OutputDir, fmt.Sprintf("%s.json", result.Name))
		resultData, err := json.MarshalIndent(result, "", "  ")
		if err != nil {
			continue
		}

		os.WriteFile(resultPath, resultData, 0644)
	}

	// Save summary
	summaryPath := filepath.Join(r.config.OutputDir, "summary.txt")
	summaryData := r.generateTextSummary(report)

	return os.WriteFile(summaryPath, []byte(summaryData), 0644)
}

// printSummary prints a summary of benchmark results
func (r *Runner) printSummary(report *BenchmarkReport) {
	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Printf("Benchmark Suite: %s\n", report.Suite)
	fmt.Printf("Timestamp: %s\n", report.Timestamp.Format(time.RFC3339))
	fmt.Printf("System: %s/%s (%d cores)\n", report.SystemInfo.OS, report.SystemInfo.Arch, report.SystemInfo.CPUCores)
	fmt.Println(strings.Repeat("=", 80))

	fmt.Printf("\nSummary:\n")
	fmt.Printf("  Total Benchmarks: %d\n", report.Summary.TotalBenchmarks)
	fmt.Printf("  Successful: %d\n", report.Summary.SuccessfulRuns)
	fmt.Printf("  Failed: %d\n", report.Summary.FailedRuns)
	fmt.Printf("  Average TPS: %.2f\n", report.Summary.AverageTPS)
	fmt.Printf("  Average P99 Latency: %v\n", report.Summary.AverageLatencyP99)
	fmt.Printf("  Total Duration: %v\n", report.Summary.TotalDuration)

	fmt.Println("\nIndividual Results:")
	fmt.Println(strings.Repeat("-", 80))
	fmt.Printf("%-30s %-10s %-15s %-15s %-10s\n", "Benchmark", "Status", "TPS", "P99 Latency", "Duration")
	fmt.Println(strings.Repeat("-", 80))

	for _, result := range report.Results {
		status := "PASS"
		if !result.Success {
			status = "FAIL"
		}

		tps := "N/A"
		if result.Metrics != nil {
			tps = fmt.Sprintf("%.2f", result.Metrics.TPS)
		}

		latency := "N/A"
		if result.Metrics != nil && result.Metrics.Latency != nil {
			latency = result.Metrics.Latency.P99.String()
		}

		fmt.Printf("%-30s %-10s %-15s %-15s %-10s\n",
			result.Name,
			status,
			tps,
			latency,
			result.Duration.Round(time.Millisecond),
		)

		if !result.Success && result.Error != "" {
			fmt.Printf("  Error: %s\n", result.Error)
		}
	}

	fmt.Println(strings.Repeat("=", 80))
	fmt.Printf("\nResults saved to: %s\n", r.config.OutputDir)
}

// generateTextSummary generates a text summary of results
func (r *Runner) generateTextSummary(report *BenchmarkReport) string {
	var summary strings.Builder

	summary.WriteString(fmt.Sprintf("Benchmark Suite: %s\n", report.Suite))
	summary.WriteString(fmt.Sprintf("Generated: %s\n", report.Timestamp.Format(time.RFC3339)))
	summary.WriteString(fmt.Sprintf("System: %s/%s (%d cores, Go %s)\n\n",
		report.SystemInfo.OS,
		report.SystemInfo.Arch,
		report.SystemInfo.CPUCores,
		report.SystemInfo.GoVersion))

	summary.WriteString("Configuration:\n")
	configData, _ := json.MarshalIndent(report.Config, "  ", "  ")
	summary.Write(configData)
	summary.WriteString("\n\n")

	summary.WriteString("Results:\n")
	for _, result := range report.Results {
		summary.WriteString(fmt.Sprintf("\n%s:\n", result.Name))
		summary.WriteString(fmt.Sprintf("  Status: %v\n", result.Success))
		summary.WriteString(fmt.Sprintf("  Duration: %v\n", result.Duration))

		if result.Metrics != nil {
			summary.WriteString(fmt.Sprintf("  TPS: %.2f\n", result.Metrics.TPS))
			summary.WriteString(fmt.Sprintf("  Total Operations: %d\n", result.Metrics.TotalOperations))

			if result.Metrics.Latency != nil {
				summary.WriteString("  Latency:\n")
				summary.WriteString(fmt.Sprintf("    P50: %v\n", result.Metrics.Latency.Median))
				summary.WriteString(fmt.Sprintf("    P90: %v\n", result.Metrics.Latency.P90))
				summary.WriteString(fmt.Sprintf("    P95: %v\n", result.Metrics.Latency.P95))
				summary.WriteString(fmt.Sprintf("    P99: %v\n", result.Metrics.Latency.P99))
			}

			if result.Metrics.Resources != nil {
				summary.WriteString("  Resources:\n")
				summary.WriteString(fmt.Sprintf("    Memory: %.2f MB\n", result.Metrics.Resources.MemoryUsageMB))
				summary.WriteString(fmt.Sprintf("    Goroutines: %d\n", result.Metrics.Resources.GoroutineCount))
			}
		}

		if result.Error != "" {
			summary.WriteString(fmt.Sprintf("  Error: %s\n", result.Error))
		}
	}

	return summary.String()
}

// applyCustomConfig is no longer needed with typed configurations

// LoadConfig loads runner configuration from file
func LoadConfig(path string) (*RunnerConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var config RunnerConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, err
	}

	return &config, nil
}
