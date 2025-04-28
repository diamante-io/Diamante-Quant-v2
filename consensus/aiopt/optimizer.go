// consensus/aiopt/optimizer.go

package aiopt

import (
	"diamante/consensus/types"
	"math"
	"reflect"
	"sync"
	"time"
)

// Logger is a minimal interface for logging informational and error messages.
// In production, you can integrate structured loggers (like Zap or Logrus) instead.
type Logger interface {
	Info(msg string, keyvals ...interface{})
	Error(msg string, keyvals ...interface{})
}

// Optimizer observes network load, block times, and dynamically adjusts consensus parameters
// to maintain target performance metrics (e.g., block finality time).
type Optimizer struct {
	consensus types.Consensus

	// Thread-safe samples of recent network load and block times
	mu            sync.RWMutex
	samples       []float64       // Rolling record of observed network loads
	blockTimes    []time.Duration // Rolling record of observed block intervals
	lastBlockTime time.Time       // Tracks the last time we measured a block interval

	// Cached exponential moving average (EMA) for network load
	ema float64

	// Logging
	logger Logger
}

// PerformanceMetrics aggregates the data needed to evaluate how close we are
// to the target block time and throughput goals.
type PerformanceMetrics struct {
	averageBlockTime time.Duration
	tps              float64
	networkLoad      float64
}

const (
	targetBlockTime = 5 * time.Second // Our ideal block time
	sampleSize      = 100             // Max samples stored for load/block time
	alpha           = 0.2             // Exponential smoothing factor for EMA
)

// NewOptimizer creates a new Optimizer instance.
//   - c is the consensus interface providing data (network load, etc.).
//   - logger is used for logging adjustments and performance stats.
func NewOptimizer(c types.Consensus, logger Logger) *Optimizer {
	return &Optimizer{
		consensus:     c,
		samples:       make([]float64, 0, sampleSize),
		blockTimes:    make([]time.Duration, 0, sampleSize),
		lastBlockTime: time.Now(),
		ema:           0,
		logger:        logger,
	}
}

// CollectSample records the latest network load and the time since the last observed block.
// Call this periodically (e.g., once per second) to gather metrics for adaptation.
func (o *Optimizer) CollectSample() {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.consensus == nil {
		if o.logger != nil {
			o.logger.Error("CollectSample: consensus interface is nil")
		}
		return // Safety check: no consensus interface
	}

	load := o.consensus.GetNetworkLoad()
	o.samples = append(o.samples, load)
	if len(o.samples) > sampleSize {
		o.samples = o.samples[1:]
	}

	// Update cached EMA incrementally.
	if len(o.samples) == 1 {
		o.ema = load
	} else {
		o.ema = alpha*load + (1-alpha)*o.ema
	}

	now := time.Now()
	blockTime := now.Sub(o.lastBlockTime)
	o.blockTimes = append(o.blockTimes, blockTime)
	if len(o.blockTimes) > sampleSize {
		o.blockTimes = o.blockTimes[1:]
	}
	o.lastBlockTime = now
}

// PredictLoad uses the cached EMA plus a simple linear trend to forecast near-future network load.
// Returns a value clamped to [0,1]. If insufficient samples are collected, it returns a default medium load.
func (o *Optimizer) PredictLoad() float64 {
	o.mu.RLock()
	defer o.mu.RUnlock()

	if len(o.samples) < 10 {
		return 0.5 // Not enough data, assume medium load
	}

	// Calculate trend: (lastSample - firstSample) / sampleCount
	trend := (o.samples[len(o.samples)-1] - o.samples[0]) / float64(len(o.samples))
	predictedLoad := o.ema + trend

	// Clamp result to [0, 1]
	return math.Max(0, math.Min(1, predictedLoad))
}

// OptimizeConsensus triggers the adaptation flow: predict load, adapt parameters, and monitor performance.
func (o *Optimizer) OptimizeConsensus() {
	predictedLoad := o.PredictLoad()
	o.logger.Info("Optimizing consensus", "predicted_load", predictedLoad)
	o.adaptParameters(predictedLoad)
	o.monitorPerformance()
}

// adaptParameters adjusts Lachesis, DPoS, and PoH parameters based on the predicted load.
func (o *Optimizer) adaptParameters(predictedLoad float64) {
	// Check if consensus is nil.
	if o.consensus == nil {
		if o.logger != nil {
			o.logger.Error("adaptParameters: consensus interface is nil")
		}
		return
	}

	// Retrieve lachesis and check its underlying value.
	lachesis := o.consensus.GetLachesis()
	if lachesis == nil || reflect.ValueOf(lachesis).IsNil() {
		if o.logger != nil {
			o.logger.Error("adaptParameters: lachesis submodule is nil")
		}
		return
	}

	// Retrieve DPoS and check.
	dpos := o.consensus.GetDPoS()
	if dpos == nil || reflect.ValueOf(dpos).IsNil() {
		if o.logger != nil {
			o.logger.Error("adaptParameters: dpos submodule is nil")
		}
		return
	}

	// Retrieve PoH and check.
	poh := o.consensus.GetPoH()
	if poh == nil || reflect.ValueOf(poh).IsNil() {
		if o.logger != nil {
			o.logger.Error("adaptParameters: poh submodule is nil")
		}
		return
	}

	// Check if we're in test mode (if consensus implements this interface)
	isTestMode := false
	if testModeCheck, ok := o.consensus.(interface{ IsTestMode() bool }); ok {
		isTestMode = testModeCheck.IsTestMode()
	}

	// --- Lachesis adjustments ---
	oldGossipDelay := lachesis.GetGossipDelay()
	newGossipDelay := time.Duration(float64(oldGossipDelay) * math.Pow(2, predictedLoad-0.5))
	lachesis.SetGossipDelay(newGossipDelay)

	// Only adjust voting threshold in production mode, not test mode
	oldVotingThreshold := lachesis.GetVotingThreshold()
	if !isTestMode {
		newVotingThreshold := oldVotingThreshold * math.Pow(1.1, predictedLoad-0.5)
		newVotingThreshold = math.Max(0.66, math.Min(newVotingThreshold, 0.9))
		lachesis.SetVotingThreshold(newVotingThreshold)
	}

	// --- DPoS adjustments ---
	oldSetSize := dpos.GetSetSize()
	newSetSize := int(float64(oldSetSize) * math.Pow(1.2, predictedLoad-0.5))
	newSetSize = maxInt(4, minInt(newSetSize, 100))
	dpos.SetSetSize(newSetSize)

	// --- PoH adjustments ---
	oldTickDelay := poh.GetTickDelay()
	newTickDelay := time.Duration(float64(oldTickDelay) * math.Pow(1.1, predictedLoad-0.5))
	poh.SetTickDelay(newTickDelay)

	if o.logger != nil {
		o.logger.Info("adaptParameters: parameters updated",
			"predicted_load", predictedLoad,
			"old_gossip_delay", oldGossipDelay, "new_gossip_delay", newGossipDelay,
			"old_voting_threshold", oldVotingThreshold,
			"new_voting_threshold", lachesis.GetVotingThreshold(), // Get current, which may be unchanged in test mode
			"old_set_size", oldSetSize, "new_set_size", newSetSize,
			"old_tick_delay", oldTickDelay, "new_tick_delay", newTickDelay)
	}
}

// monitorPerformance checks how our actual block time compares to the target,
// then calls fine-tuning methods as needed.
func (o *Optimizer) monitorPerformance() {
	metrics := o.collectPerformanceMetrics()

	targetTimeSeconds := targetBlockTime.Seconds()
	actualTimeSeconds := metrics.averageBlockTime.Seconds()

	// Decide on adjustment based on block time deviation
	if actualTimeSeconds > targetTimeSeconds*1.2 {
		o.logger.Info("monitorPerformance: slow block time detected", "actual", actualTimeSeconds, "target", targetTimeSeconds)
		o.adjustForSlowPerformance()
	} else if actualTimeSeconds < targetTimeSeconds*0.8 {
		o.logger.Info("monitorPerformance: fast block time detected", "actual", actualTimeSeconds, "target", targetTimeSeconds)
		o.adjustForFastPerformance()
	} else {
		o.finetunePerformance(actualTimeSeconds, targetTimeSeconds)
	}

	o.logPerformanceMetrics(metrics, targetTimeSeconds, actualTimeSeconds)
}

// finetunePerformance makes small incremental tweaks if block time deviates slightly from target.
func (o *Optimizer) finetunePerformance(actualTime, targetTime float64) {
	lachesis := o.consensus.GetLachesis()
	dpos := o.consensus.GetDPoS()
	poh := o.consensus.GetPoH()

	// Use reflection to check underlying nil values.
	if lachesis == nil || reflect.ValueOf(lachesis).IsNil() ||
		dpos == nil || reflect.ValueOf(dpos).IsNil() ||
		poh == nil || reflect.ValueOf(poh).IsNil() {
		if o.logger != nil {
			o.logger.Error("finetunePerformance: one or more submodules are nil")
		}
		return
	}

	deviation := (actualTime - targetTime) / targetTime

	// Adjust Lachesis gossip delay.
	oldGossipDelay := lachesis.GetGossipDelay()
	newGossipDelay := time.Duration(float64(oldGossipDelay) * (1 - 0.1*deviation))
	lachesis.SetGossipDelay(newGossipDelay)

	// Adjust DPoS set size.
	oldSetSize := dpos.GetSetSize()
	newSetSize := int(float64(oldSetSize) * (1 - 0.1*deviation))
	dpos.SetSetSize(newSetSize)

	// Adjust PoH tick delay.
	oldTickDelay := poh.GetTickDelay()
	newTickDelay := time.Duration(float64(oldTickDelay) * (1 - 0.1*deviation))
	poh.SetTickDelay(newTickDelay)

	if o.logger != nil {
		o.logger.Info("finetunePerformance: incremental adjustments applied",
			"deviation", deviation,
			"old_gossip_delay", oldGossipDelay, "new_gossip_delay", newGossipDelay,
			"old_set_size", oldSetSize, "new_set_size", newSetSize,
			"old_tick_delay", oldTickDelay, "new_tick_delay", newTickDelay)
	}
}

// logPerformanceMetrics prints performance metrics while safely handling nil submodules.
func (o *Optimizer) logPerformanceMetrics(
	metrics PerformanceMetrics,
	targetTime, actualTime float64,
) {
	if o.logger == nil {
		return
	}

	var gossipDelay interface{} = "nil"
	var votingThreshold interface{} = "nil"
	var dposSetSize interface{} = "nil"
	var pohTickDelay interface{} = "nil"

	if o.consensus != nil {
		// Retrieve lachesis and check its underlying value.
		lachesis := o.consensus.GetLachesis()
		if lachesis != nil && !reflect.ValueOf(lachesis).IsNil() {
			gossipDelay = lachesis.GetGossipDelay()
			votingThreshold = lachesis.GetVotingThreshold()
		}

		// Retrieve DPoS and check.
		dpos := o.consensus.GetDPoS()
		if dpos != nil && !reflect.ValueOf(dpos).IsNil() {
			dposSetSize = dpos.GetSetSize()
		}

		// Retrieve PoH and check.
		poh := o.consensus.GetPoH()
		if poh != nil && !reflect.ValueOf(poh).IsNil() {
			pohTickDelay = poh.GetTickDelay()
		}
	}

	o.logger.Info("Performance Metrics",
		"target_block_time", targetTime,
		"actual_block_time", actualTime,
		"tps", metrics.tps,
		"network_load", metrics.networkLoad,
		"lachesis_gossip_delay", gossipDelay,
		"voting_threshold", votingThreshold,
		"dpos_set_size", dposSetSize,
		"poh_tick_delay", pohTickDelay,
	)
}

// collectPerformanceMetrics calculates average block time, TPS, and reads current network load.
func (o *Optimizer) collectPerformanceMetrics() PerformanceMetrics {
	o.mu.RLock()
	defer o.mu.RUnlock()

	var totalBlockTime time.Duration
	for _, bt := range o.blockTimes {
		totalBlockTime += bt
	}

	var avgBlockTime time.Duration
	if len(o.blockTimes) > 0 {
		avgBlockTime = totalBlockTime / time.Duration(len(o.blockTimes))
	} else {
		// If we have no samples, assume target
		avgBlockTime = targetBlockTime
	}

	var tps float64
	if totalBlockTime.Seconds() > 0 {
		tps = float64(len(o.blockTimes)) / totalBlockTime.Seconds()
	} else {
		tps = 0
	}

	netLoad := 0.0
	if o.consensus != nil {
		netLoad = o.consensus.GetNetworkLoad()
	}

	return PerformanceMetrics{
		averageBlockTime: avgBlockTime,
		tps:              tps,
		networkLoad:      netLoad,
	}
}

// adjustForSlowPerformance increments a few parameters to speed up finality.
func (o *Optimizer) adjustForSlowPerformance() {
	lachesis := o.consensus.GetLachesis()
	dpos := o.consensus.GetDPoS()
	poh := o.consensus.GetPoH()

	if lachesis == nil || dpos == nil || poh == nil {
		if o.logger != nil {
			o.logger.Error("adjustForSlowPerformance: one or more submodules are nil")
		}
		return
	}

	// Slightly reduce gossip delay, set size, and PoH tick to speed up finality
	oldGossipDelay := lachesis.GetGossipDelay()
	lachesis.SetGossipDelay(time.Duration(float64(oldGossipDelay) * 0.9))

	oldSetSize := dpos.GetSetSize()
	dpos.SetSetSize(maxInt(4, oldSetSize-1))

	oldTickDelay := poh.GetTickDelay()
	poh.SetTickDelay(time.Duration(float64(oldTickDelay) * 0.9))

	if o.logger != nil {
		o.logger.Info("adjustForSlowPerformance: adjusted parameters",
			"new_gossip_delay", lachesis.GetGossipDelay(),
			"new_set_size", dpos.GetSetSize(),
			"new_tick_delay", poh.GetTickDelay(),
		)
	}
}

// adjustForFastPerformance decreases times/intervals to slow down if blocks are too fast.
func (o *Optimizer) adjustForFastPerformance() {
	lachesis := o.consensus.GetLachesis()
	dpos := o.consensus.GetDPoS()
	poh := o.consensus.GetPoH()

	if lachesis == nil || dpos == nil || poh == nil {
		if o.logger != nil {
			o.logger.Error("adjustForFastPerformance: one or more submodules are nil")
		}
		return
	}

	oldGossipDelay := lachesis.GetGossipDelay()
	lachesis.SetGossipDelay(time.Duration(float64(oldGossipDelay) * 1.1))

	oldSetSize := dpos.GetSetSize()
	dpos.SetSetSize(minInt(100, oldSetSize+1))

	oldTickDelay := poh.GetTickDelay()
	poh.SetTickDelay(time.Duration(float64(oldTickDelay) * 1.1))

	if o.logger != nil {
		o.logger.Info("adjustForFastPerformance: adjusted parameters",
			"new_gossip_delay", lachesis.GetGossipDelay(),
			"new_set_size", dpos.GetSetSize(),
			"new_tick_delay", poh.GetTickDelay(),
		)
	}
}

// Run periodically collects samples and optimizes consensus parameters until stopChan is closed.
func (o *Optimizer) Run(stopChan chan struct{}) {
	// Immediately collect a sample so that we have data quickly.
	o.CollectSample()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Collect sample and optimize consensus parameters.
			o.CollectSample()
			o.OptimizeConsensus()
		case <-stopChan:
			if o.logger != nil {
				o.logger.Info("Optimizer Run: received stop signal, exiting")
			}
			return
		}
	}
}

// GetOptimizationStats provides a snapshot of the current optimizer state.
func (o *Optimizer) GetOptimizationStats() map[string]interface{} {
	o.mu.RLock()
	defer o.mu.RUnlock()

	metrics := o.collectPerformanceMetrics()
	stats := make(map[string]interface{})

	// Last recorded load sample
	if len(o.samples) > 0 {
		stats["current_load"] = o.samples[len(o.samples)-1]
	} else {
		stats["current_load"] = 0.5
	}

	stats["predicted_load"] = o.PredictLoad()
	stats["sample_count"] = len(o.samples)
	stats["average_block_time"] = metrics.averageBlockTime
	stats["tps"] = metrics.tps
	stats["network_load"] = metrics.networkLoad

	if o.consensus != nil {
		lachesis := o.consensus.GetLachesis()
		dpos := o.consensus.GetDPoS()
		poh := o.consensus.GetPoH()
		if lachesis != nil {
			stats["lachesis_gossip_delay"] = lachesis.GetGossipDelay()
			stats["lachesis_voting_threshold"] = lachesis.GetVotingThreshold()
		}
		if dpos != nil {
			stats["dpos_set_size"] = dpos.GetSetSize()
			stats["dpos_epoch_duration"] = dpos.GetEpochDuration()
		}
		if poh != nil {
			stats["poh_tick_delay"] = poh.GetTickDelay()
		}
	}
	return stats
}

// ResetOptimization clears all collected samples and resets consensus parameters to defaults.
func (o *Optimizer) ResetOptimization() {
	o.mu.Lock()
	defer o.mu.Unlock()

	o.samples = make([]float64, 0, sampleSize)
	o.blockTimes = make([]time.Duration, 0, sampleSize)
	o.lastBlockTime = time.Now()
	o.ema = 0

	if o.logger != nil {
		o.logger.Info("ResetOptimization: clearing samples and resetting parameters to defaults")
	}

	if o.consensus == nil {
		if o.logger != nil {
			o.logger.Error("ResetOptimization: consensus interface is nil")
		}
		return
	}
	lachesis := o.consensus.GetLachesis()
	dpos := o.consensus.GetDPoS()
	poh := o.consensus.GetPoH()

	if lachesis == nil || dpos == nil || poh == nil {
		if o.logger != nil {
			o.logger.Error("ResetOptimization: one or more submodules are nil")
		}
		return
	}
	// Reset to some default values (these are example values)
	lachesis.SetGossipDelay(100 * time.Millisecond)
	lachesis.SetVotingThreshold(0.66)
	dpos.SetSetSize(21)
	dpos.SetEpochDuration(100)
	poh.SetTickDelay(50 * time.Millisecond)
}

// maxInt and minInt are helper functions to clamp integer values.
func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
