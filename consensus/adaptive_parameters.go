// consensus/adaptive_parameters.go

package consensus

import (
	"fmt"
	"math"
	"sync"
	"time"

	dtypes "diamante/types"
)

// AdaptiveParameterConfig holds configuration for the adaptive parameter system
type AdaptiveParameterConfig struct {
	// EnableAdaptiveGossipDelay enables dynamic adjustment of gossip delay
	EnableAdaptiveGossipDelay bool

	// EnableAdaptivePoHDelay enables dynamic adjustment of PoH tick delay
	EnableAdaptivePoHDelay bool

	// EnableAdaptiveVotingThreshold enables dynamic adjustment of voting threshold
	EnableAdaptiveVotingThreshold bool

	// EnableAdaptiveBatchSize enables dynamic adjustment of batch size
	EnableAdaptiveBatchSize bool

	// AdaptationInterval is how often parameters are adjusted
	AdaptationInterval time.Duration

	// NetworkLoadThresholds defines thresholds for network load categories
	NetworkLoadThresholds NetworkLoadThresholds

	// PerformanceThresholds defines thresholds for performance categories
	PerformanceThresholds PerformanceThresholds
}

// NetworkLoadThresholds defines thresholds for different network load categories
type NetworkLoadThresholds struct {
	// Low is the threshold below which network load is considered low
	Low float64

	// Medium is the threshold below which network load is considered medium
	Medium float64

	// High is the threshold below which network load is considered high
	High float64

	// Critical is the threshold below which network load is considered critical
	Critical float64
}

// PerformanceThresholds defines thresholds for different performance categories
type PerformanceThresholds struct {
	// EventProcessingTimeThreshold is the threshold for event processing time (ms)
	EventProcessingTimeThreshold float64

	// BlockProcessingTimeThreshold is the threshold for block processing time (ms)
	BlockProcessingTimeThreshold float64

	// CPUUtilizationThreshold is the threshold for CPU utilization (%)
	CPUUtilizationThreshold float64

	// MemoryUtilizationThreshold is the threshold for memory utilization (%)
	MemoryUtilizationThreshold float64
}

// DefaultAdaptiveParameterConfig returns a default configuration
func DefaultAdaptiveParameterConfig() *AdaptiveParameterConfig {
	return &AdaptiveParameterConfig{
		EnableAdaptiveGossipDelay:     true,
		EnableAdaptivePoHDelay:        true,
		EnableAdaptiveVotingThreshold: false, // Disabled by default for safety
		EnableAdaptiveBatchSize:       true,
		AdaptationInterval:            30 * time.Second,
		NetworkLoadThresholds: NetworkLoadThresholds{
			Low:      0.3,
			Medium:   0.6,
			High:     0.8,
			Critical: 0.95,
		},
		PerformanceThresholds: PerformanceThresholds{
			EventProcessingTimeThreshold: 50.0,  // 50ms
			BlockProcessingTimeThreshold: 500.0, // 500ms
			CPUUtilizationThreshold:      80.0,  // 80%
			MemoryUtilizationThreshold:   80.0,  // 80%
		},
	}
}

// TestAdaptiveParameterConfig returns a configuration suitable for testing
func TestAdaptiveParameterConfig() *AdaptiveParameterConfig {
	config := DefaultAdaptiveParameterConfig()
	config.AdaptationInterval = 5 * time.Second // Faster adaptation for tests
	return config
}

// AdaptiveParameters manages dynamic adjustment of consensus parameters
type AdaptiveParameters struct {
	config *AdaptiveParameterConfig
	logger *hybridConsensusLogger

	// Current parameter values
	gossipDelay     time.Duration
	pohTickDelay    time.Duration
	votingThreshold float64
	batchSize       int
	parametersMu    sync.RWMutex

	// Metrics for adaptation
	networkLoad         float64
	eventProcessingTime time.Duration
	blockProcessingTime time.Duration
	cpuUtilization      float64
	memoryUtilization   float64
	metricsMu           sync.RWMutex

	// Parameter ranges
	minGossipDelay     time.Duration
	maxGossipDelay     time.Duration
	minPoHDelay        time.Duration
	maxPoHDelay        time.Duration
	minVotingThreshold float64
	maxVotingThreshold float64
	minBatchSize       int
	maxBatchSize       int

	// Control
	stopChan  chan struct{}
	isRunning bool
	runMu     sync.Mutex
}

// NewAdaptiveParameters creates a new adaptive parameter manager
func NewAdaptiveParameters(
	config *AdaptiveParameterConfig,
	logger *hybridConsensusLogger,
	initialGossipDelay time.Duration,
	initialPoHDelay time.Duration,
	initialVotingThreshold float64,
	initialBatchSize int,
) *AdaptiveParameters {
	if config == nil {
		config = DefaultAdaptiveParameterConfig()
	}

	return &AdaptiveParameters{
		config:             config,
		logger:             logger,
		gossipDelay:        initialGossipDelay,
		pohTickDelay:       initialPoHDelay,
		votingThreshold:    initialVotingThreshold,
		batchSize:          initialBatchSize,
		minGossipDelay:     5 * time.Millisecond,
		maxGossipDelay:     1 * time.Second,
		minPoHDelay:        1 * time.Millisecond,
		maxPoHDelay:        5 * time.Second,
		minVotingThreshold: 0.51, // Minimum for safety
		maxVotingThreshold: 0.9,  // Maximum for liveness
		minBatchSize:       10,
		maxBatchSize:       1000,
		stopChan:           make(chan struct{}),
	}
}

// Start begins the adaptive parameter adjustment process
func (ap *AdaptiveParameters) Start() error {
	ap.runMu.Lock()
	defer ap.runMu.Unlock()

	if ap.isRunning {
		return nil // Already running
	}

	ap.stopChan = make(chan struct{})
	ap.isRunning = true

	go ap.adaptationLoop()

	ap.logger.Info("Adaptive parameters started",
		LogKeyValue{Key: "gossipDelay", Value: fmt.Sprintf("%v", ap.gossipDelay)},
		LogKeyValue{Key: "pohTickDelay", Value: fmt.Sprintf("%v", ap.pohTickDelay)},
		LogKeyValue{Key: "votingThreshold", Value: fmt.Sprintf("%v", ap.votingThreshold)},
		LogKeyValue{Key: "batchSize", Value: fmt.Sprintf("%v", ap.batchSize)})

	return nil
}

// Stop halts the adaptive parameter adjustment process
func (ap *AdaptiveParameters) Stop() error {
	ap.runMu.Lock()
	defer ap.runMu.Unlock()

	if !ap.isRunning {
		return nil // Already stopped
	}

	close(ap.stopChan)
	ap.isRunning = false

	ap.logger.Info("Adaptive parameters stopped")
	return nil
}

// adaptationLoop periodically adjusts parameters based on metrics
func (ap *AdaptiveParameters) adaptationLoop() {
	ticker := time.NewTicker(ap.config.AdaptationInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			ap.adaptParameters()
		case <-ap.stopChan:
			return
		}
	}
}

// adaptParameters adjusts parameters based on current metrics
func (ap *AdaptiveParameters) adaptParameters() {
	// Get current metrics
	networkLoad := ap.GetNetworkLoad()
	eventProcessingTime := ap.GetEventProcessingTime()
	blockProcessingTime := ap.GetBlockProcessingTime()
	cpuUtilization := ap.GetCPUUtilization()
	memoryUtilization := ap.GetMemoryUtilization()

	// Log current metrics
	ap.logger.Info("Adapting parameters",
		LogKeyValue{Key: "networkLoad", Value: fmt.Sprintf("%.2f", networkLoad)},
		LogKeyValue{Key: "eventProcessingTime", Value: fmt.Sprintf("%v", eventProcessingTime)},
		LogKeyValue{Key: "blockProcessingTime", Value: fmt.Sprintf("%v", blockProcessingTime)},
		LogKeyValue{Key: "cpuUtilization", Value: fmt.Sprintf("%.2f", cpuUtilization)},
		LogKeyValue{Key: "memoryUtilization", Value: fmt.Sprintf("%.2f", memoryUtilization)})

	// Adapt gossip delay
	if ap.config.EnableAdaptiveGossipDelay {
		newGossipDelay := ap.adaptGossipDelay(networkLoad, eventProcessingTime)
		ap.SetGossipDelay(newGossipDelay)
	}

	// Adapt PoH tick delay
	if ap.config.EnableAdaptivePoHDelay {
		newPoHDelay := ap.adaptPoHDelay(cpuUtilization, blockProcessingTime)
		ap.SetPoHTickDelay(newPoHDelay)
	}

	// Adapt voting threshold
	if ap.config.EnableAdaptiveVotingThreshold {
		newVotingThreshold := ap.adaptVotingThreshold(networkLoad)
		ap.SetVotingThreshold(newVotingThreshold)
	}

	// Adapt batch size
	if ap.config.EnableAdaptiveBatchSize {
		newBatchSize := ap.adaptBatchSize(eventProcessingTime, cpuUtilization)
		ap.SetBatchSize(newBatchSize)
	}

	// Log new parameter values
	ap.logger.Info("Parameters adapted",
		LogKeyValue{Key: "gossipDelay", Value: fmt.Sprintf("%v", ap.GetGossipDelay())},
		LogKeyValue{Key: "pohTickDelay", Value: fmt.Sprintf("%v", ap.GetPoHTickDelay())},
		LogKeyValue{Key: "votingThreshold", Value: fmt.Sprintf("%.2f", ap.GetVotingThreshold())},
		LogKeyValue{Key: "batchSize", Value: fmt.Sprintf("%d", ap.GetBatchSize())})
}

// adaptGossipDelay adjusts gossip delay based on network load and event processing time
func (ap *AdaptiveParameters) adaptGossipDelay(networkLoad float64, eventProcessingTime time.Duration) time.Duration {
	// Get current gossip delay
	currentDelay := ap.GetGossipDelay()

	// Base adjustment on network load
	var targetDelay time.Duration
	switch {
	case networkLoad < ap.config.NetworkLoadThresholds.Low:
		// Low load: decrease delay for faster propagation
		targetDelay = ap.minGossipDelay + time.Duration(float64(ap.maxGossipDelay-ap.minGossipDelay)*0.2)
	case networkLoad < ap.config.NetworkLoadThresholds.Medium:
		// Medium load: moderate delay
		targetDelay = ap.minGossipDelay + time.Duration(float64(ap.maxGossipDelay-ap.minGossipDelay)*0.4)
	case networkLoad < ap.config.NetworkLoadThresholds.High:
		// High load: increase delay to reduce network congestion
		targetDelay = ap.minGossipDelay + time.Duration(float64(ap.maxGossipDelay-ap.minGossipDelay)*0.6)
	case networkLoad < ap.config.NetworkLoadThresholds.Critical:
		// Very high load: significant delay
		targetDelay = ap.minGossipDelay + time.Duration(float64(ap.maxGossipDelay-ap.minGossipDelay)*0.8)
	default:
		// Critical load: maximum delay to prevent network collapse
		targetDelay = ap.maxGossipDelay
	}

	// Further adjust based on event processing time
	if eventProcessingTime > time.Duration(ap.config.PerformanceThresholds.EventProcessingTimeThreshold)*time.Millisecond {
		// If event processing is slow, increase delay to give more processing time
		targetDelay = time.Duration(float64(targetDelay) * 1.2)
	}

	// Ensure delay is within bounds
	if targetDelay < ap.minGossipDelay {
		targetDelay = ap.minGossipDelay
	} else if targetDelay > ap.maxGossipDelay {
		targetDelay = ap.maxGossipDelay
	}

	// Smooth transition: move 20% toward target
	newDelay := currentDelay + time.Duration(float64(targetDelay-currentDelay)*0.2)

	return newDelay
}

// adaptPoHDelay adjusts PoH tick delay based on CPU utilization and block processing time
func (ap *AdaptiveParameters) adaptPoHDelay(cpuUtilization float64, blockProcessingTime time.Duration) time.Duration {
	// Get current PoH tick delay
	currentDelay := ap.GetPoHTickDelay()

	// Base adjustment on CPU utilization
	var targetDelay time.Duration
	switch {
	case cpuUtilization < 30:
		// Low CPU: decrease delay for faster PoH
		targetDelay = ap.minPoHDelay + time.Duration(float64(ap.maxPoHDelay-ap.minPoHDelay)*0.2)
	case cpuUtilization < 50:
		// Moderate CPU: balanced delay
		targetDelay = ap.minPoHDelay + time.Duration(float64(ap.maxPoHDelay-ap.minPoHDelay)*0.4)
	case cpuUtilization < 70:
		// High CPU: increase delay to reduce CPU load
		targetDelay = ap.minPoHDelay + time.Duration(float64(ap.maxPoHDelay-ap.minPoHDelay)*0.6)
	case cpuUtilization < 85:
		// Very high CPU: significant delay
		targetDelay = ap.minPoHDelay + time.Duration(float64(ap.maxPoHDelay-ap.minPoHDelay)*0.8)
	default:
		// Critical CPU: maximum delay to prevent system overload
		targetDelay = ap.maxPoHDelay
	}

	// Further adjust based on block processing time
	if blockProcessingTime > time.Duration(ap.config.PerformanceThresholds.BlockProcessingTimeThreshold)*time.Millisecond {
		// If block processing is slow, increase delay to give more processing time
		targetDelay = time.Duration(float64(targetDelay) * 1.2)
	}

	// Ensure delay is within bounds
	if targetDelay < ap.minPoHDelay {
		targetDelay = ap.minPoHDelay
	} else if targetDelay > ap.maxPoHDelay {
		targetDelay = ap.maxPoHDelay
	}

	// Smooth transition: move 20% toward target
	newDelay := currentDelay + time.Duration(float64(targetDelay-currentDelay)*0.2)

	return newDelay
}

// adaptVotingThreshold adjusts voting threshold based on network load
func (ap *AdaptiveParameters) adaptVotingThreshold(networkLoad float64) float64 {
	// Get current voting threshold
	currentThreshold := ap.GetVotingThreshold()

	// Base adjustment on network load
	var targetThreshold float64
	switch {
	case networkLoad < ap.config.NetworkLoadThresholds.Low:
		// Low load: higher threshold for stronger security
		targetThreshold = ap.minVotingThreshold + (ap.maxVotingThreshold-ap.minVotingThreshold)*0.8
	case networkLoad < ap.config.NetworkLoadThresholds.Medium:
		// Medium load: balanced threshold
		targetThreshold = ap.minVotingThreshold + (ap.maxVotingThreshold-ap.minVotingThreshold)*0.6
	case networkLoad < ap.config.NetworkLoadThresholds.High:
		// High load: lower threshold for better liveness
		targetThreshold = ap.minVotingThreshold + (ap.maxVotingThreshold-ap.minVotingThreshold)*0.4
	case networkLoad < ap.config.NetworkLoadThresholds.Critical:
		// Very high load: low threshold to maintain liveness
		targetThreshold = ap.minVotingThreshold + (ap.maxVotingThreshold-ap.minVotingThreshold)*0.2
	default:
		// Critical load: minimum threshold to ensure progress
		targetThreshold = ap.minVotingThreshold
	}

	// Ensure threshold is within bounds
	if targetThreshold < ap.minVotingThreshold {
		targetThreshold = ap.minVotingThreshold
	} else if targetThreshold > ap.maxVotingThreshold {
		targetThreshold = ap.maxVotingThreshold
	}

	// Smooth transition: move 10% toward target (more conservative for voting threshold)
	newThreshold := currentThreshold + (targetThreshold-currentThreshold)*0.1

	return newThreshold
}

// adaptBatchSize adjusts batch size based on event processing time and CPU utilization
func (ap *AdaptiveParameters) adaptBatchSize(eventProcessingTime time.Duration, cpuUtilization float64) int {
	// Get current batch size
	currentBatchSize := ap.GetBatchSize()

	// Calculate target batch size based on event processing time
	var targetBatchSize int
	eventProcessingMs := float64(eventProcessingTime) / float64(time.Millisecond)

	if eventProcessingMs < ap.config.PerformanceThresholds.EventProcessingTimeThreshold/2 {
		// Fast processing: increase batch size
		targetBatchSize = int(float64(currentBatchSize) * 1.2)
	} else if eventProcessingMs < ap.config.PerformanceThresholds.EventProcessingTimeThreshold {
		// Acceptable processing: slight increase
		targetBatchSize = int(float64(currentBatchSize) * 1.1)
	} else if eventProcessingMs < ap.config.PerformanceThresholds.EventProcessingTimeThreshold*1.5 {
		// Slow processing: slight decrease
		targetBatchSize = int(float64(currentBatchSize) * 0.9)
	} else {
		// Very slow processing: significant decrease
		targetBatchSize = int(float64(currentBatchSize) * 0.8)
	}

	// Further adjust based on CPU utilization
	if cpuUtilization > ap.config.PerformanceThresholds.CPUUtilizationThreshold {
		// High CPU: reduce batch size
		targetBatchSize = int(float64(targetBatchSize) * 0.9)
	}

	// Ensure batch size is within bounds
	if targetBatchSize < ap.minBatchSize {
		targetBatchSize = ap.minBatchSize
	} else if targetBatchSize > ap.maxBatchSize {
		targetBatchSize = ap.maxBatchSize
	}

	return targetBatchSize
}

// UpdateMetrics updates the metrics used for parameter adaptation
func (ap *AdaptiveParameters) UpdateMetrics(
	networkLoad float64,
	eventProcessingTime time.Duration,
	blockProcessingTime time.Duration,
	cpuUtilization float64,
	memoryUtilization float64,
) {
	ap.metricsMu.Lock()
	defer ap.metricsMu.Unlock()

	ap.networkLoad = networkLoad
	ap.eventProcessingTime = eventProcessingTime
	ap.blockProcessingTime = blockProcessingTime
	ap.cpuUtilization = cpuUtilization
	ap.memoryUtilization = memoryUtilization
}

// GetNetworkLoad returns the current network load
func (ap *AdaptiveParameters) GetNetworkLoad() float64 {
	ap.metricsMu.RLock()
	defer ap.metricsMu.RUnlock()
	return ap.networkLoad
}

// GetEventProcessingTime returns the current event processing time
func (ap *AdaptiveParameters) GetEventProcessingTime() time.Duration {
	ap.metricsMu.RLock()
	defer ap.metricsMu.RUnlock()
	return ap.eventProcessingTime
}

// GetBlockProcessingTime returns the current block processing time
func (ap *AdaptiveParameters) GetBlockProcessingTime() time.Duration {
	ap.metricsMu.RLock()
	defer ap.metricsMu.RUnlock()
	return ap.blockProcessingTime
}

// GetCPUUtilization returns the current CPU utilization
func (ap *AdaptiveParameters) GetCPUUtilization() float64 {
	ap.metricsMu.RLock()
	defer ap.metricsMu.RUnlock()
	return ap.cpuUtilization
}

// GetMemoryUtilization returns the current memory utilization
func (ap *AdaptiveParameters) GetMemoryUtilization() float64 {
	ap.metricsMu.RLock()
	defer ap.metricsMu.RUnlock()
	return ap.memoryUtilization
}

// GetGossipDelay returns the current gossip delay
func (ap *AdaptiveParameters) GetGossipDelay() time.Duration {
	ap.parametersMu.RLock()
	defer ap.parametersMu.RUnlock()
	return ap.gossipDelay
}

// SetGossipDelay sets the gossip delay
func (ap *AdaptiveParameters) SetGossipDelay(delay time.Duration) {
	ap.parametersMu.Lock()
	defer ap.parametersMu.Unlock()

	// Ensure delay is within bounds
	if delay < ap.minGossipDelay {
		delay = ap.minGossipDelay
	} else if delay > ap.maxGossipDelay {
		delay = ap.maxGossipDelay
	}

	// Only log if there's a significant change
	if math.Abs(float64(delay-ap.gossipDelay)) > float64(ap.gossipDelay)*0.05 {
		ap.logger.Info("Gossip delay adjusted",
			LogKeyValue{Key: "oldDelay", Value: fmt.Sprintf("%v", ap.gossipDelay)},
			LogKeyValue{Key: "newDelay", Value: fmt.Sprintf("%v", delay)})
	}

	ap.gossipDelay = delay
}

// GetPoHTickDelay returns the current PoH tick delay
func (ap *AdaptiveParameters) GetPoHTickDelay() time.Duration {
	ap.parametersMu.RLock()
	defer ap.parametersMu.RUnlock()
	return ap.pohTickDelay
}

// SetPoHTickDelay sets the PoH tick delay
func (ap *AdaptiveParameters) SetPoHTickDelay(delay time.Duration) {
	ap.parametersMu.Lock()
	defer ap.parametersMu.Unlock()

	// Ensure delay is within bounds
	if delay < ap.minPoHDelay {
		delay = ap.minPoHDelay
	} else if delay > ap.maxPoHDelay {
		delay = ap.maxPoHDelay
	}

	// Only log if there's a significant change
	if math.Abs(float64(delay-ap.pohTickDelay)) > float64(ap.pohTickDelay)*0.05 {
		ap.logger.Info("PoH tick delay adjusted",
			LogKeyValue{Key: "oldDelay", Value: fmt.Sprintf("%v", ap.pohTickDelay)},
			LogKeyValue{Key: "newDelay", Value: fmt.Sprintf("%v", delay)})
	}

	ap.pohTickDelay = delay
}

// GetVotingThreshold returns the current voting threshold
func (ap *AdaptiveParameters) GetVotingThreshold() float64 {
	ap.parametersMu.RLock()
	defer ap.parametersMu.RUnlock()
	return ap.votingThreshold
}

// SetVotingThreshold sets the voting threshold
func (ap *AdaptiveParameters) SetVotingThreshold(threshold float64) {
	ap.parametersMu.Lock()
	defer ap.parametersMu.Unlock()

	// Ensure threshold is within bounds
	if threshold < ap.minVotingThreshold {
		threshold = ap.minVotingThreshold
	} else if threshold > ap.maxVotingThreshold {
		threshold = ap.maxVotingThreshold
	}

	// Only log if there's a significant change
	if math.Abs(threshold-ap.votingThreshold) > ap.votingThreshold*0.05 {
		ap.logger.Info("Voting threshold adjusted",
			LogKeyValue{Key: "oldThreshold", Value: fmt.Sprintf("%.2f", ap.votingThreshold)},
			LogKeyValue{Key: "newThreshold", Value: fmt.Sprintf("%.2f", threshold)})
	}

	ap.votingThreshold = threshold
}

// GetBatchSize returns the current batch size
func (ap *AdaptiveParameters) GetBatchSize() int {
	ap.parametersMu.RLock()
	defer ap.parametersMu.RUnlock()
	return ap.batchSize
}

// SetBatchSize sets the batch size
func (ap *AdaptiveParameters) SetBatchSize(size int) {
	ap.parametersMu.Lock()
	defer ap.parametersMu.Unlock()

	// Ensure size is within bounds
	if size < ap.minBatchSize {
		size = ap.minBatchSize
	} else if size > ap.maxBatchSize {
		size = ap.maxBatchSize
	}

	// Only log if there's a significant change
	if math.Abs(float64(size-ap.batchSize)) > float64(ap.batchSize)*0.05 {
		ap.logger.Info("Batch size adjusted",
			LogKeyValue{Key: "oldSize", Value: fmt.Sprintf("%d", ap.batchSize)},
			LogKeyValue{Key: "newSize", Value: fmt.Sprintf("%d", size)})
	}

	ap.batchSize = size
}

// GetMetrics returns the current metrics
func (ap *AdaptiveParameters) GetMetrics() *dtypes.AdaptiveParametersMetrics {
	ap.metricsMu.RLock()
	defer ap.metricsMu.RUnlock()

	ap.parametersMu.RLock()
	defer ap.parametersMu.RUnlock()

	return &dtypes.AdaptiveParametersMetrics{
		NetworkLoad:         ap.networkLoad,
		EventProcessingTime: ap.eventProcessingTime,
		BlockProcessingTime: ap.blockProcessingTime,
		CPUUtilization:      ap.cpuUtilization,
		MemoryUtilization:   ap.memoryUtilization,
		GossipDelay:         ap.gossipDelay,
		PoHTickDelay:        ap.pohTickDelay,
		VotingThreshold:     ap.votingThreshold,
		BatchSize:           ap.batchSize,
	}
}
