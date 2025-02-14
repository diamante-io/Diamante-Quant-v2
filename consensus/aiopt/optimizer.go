// consensus/aiopt/optimizer.go

package aiopt

import (
	"diamante/consensus/types"
	"math"
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

	// Thread-safe samples of recent network load + block times
	mu            sync.RWMutex
	samples       []float64       // Rolling record of observed network loads
	blockTimes    []time.Duration // Rolling record of observed block intervals
	lastBlockTime time.Time       // Tracks the last time we measured a block interval

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
		logger:        logger,
	}
}

// CollectSample records the latest network load and the time since the last observed block.
// Call this periodically (e.g., once per second) to gather metrics for adaptation.
func (o *Optimizer) CollectSample() {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.consensus == nil {
		return // Safety check: no consensus interface
	}

	load := o.consensus.GetNetworkLoad()
	o.samples = append(o.samples, load)
	if len(o.samples) > sampleSize {
		o.samples = o.samples[1:]
	}

	now := time.Now()
	blockTime := now.Sub(o.lastBlockTime)
	o.blockTimes = append(o.blockTimes, blockTime)
	if len(o.blockTimes) > sampleSize {
		o.blockTimes = o.blockTimes[1:]
	}
	o.lastBlockTime = now
}

// PredictLoad uses an exponential moving average (EMA) plus a simple linear trend
// to forecast near-future network load. Returns a value ∈ [0,1].
func (o *Optimizer) PredictLoad() float64 {
	o.mu.RLock()
	defer o.mu.RUnlock()

	if len(o.samples) < 10 {
		// Not enough data, assume medium load
		return 0.5
	}

	// Start EMA with the first sample
	ema := o.samples[0]
	for i := 1; i < len(o.samples); i++ {
		ema = alpha*o.samples[i] + (1-alpha)*ema
	}

	// Calculate a simple trend: (lastSample - firstSample) / sampleCount
	trend := (o.samples[len(o.samples)-1] - o.samples[0]) / float64(len(o.samples))
	predictedLoad := ema + trend

	// Clamp result to [0, 1]
	return math.Max(0, math.Min(1, predictedLoad))
}

// OptimizeConsensus triggers the adaptation flow: predict load, adapt params, and monitor performance.
func (o *Optimizer) OptimizeConsensus() {
	predictedLoad := o.PredictLoad()
	o.adaptParameters(predictedLoad)
	o.monitorPerformance()
}

// adaptParameters adjusts Lachesis, DPoS, and PoH parameters based on the predicted load.
func (o *Optimizer) adaptParameters(predictedLoad float64) {
	if o.consensus == nil {
		return
	}
	lachesis := o.consensus.GetLachesis()
	dpos := o.consensus.GetDPoS()
	poh := o.consensus.GetPoH()

	// Basic safety checks in case any submodule is nil
	if lachesis == nil || dpos == nil || poh == nil {
		if o.logger != nil {
			o.logger.Error("Cannot adapt parameters: submodule is nil")
		}
		return
	}

	// --- Lachesis ---
	newGossipDelay := time.Duration(float64(lachesis.GetGossipDelay()) *
		math.Pow(2, predictedLoad-0.5))
	lachesis.SetGossipDelay(newGossipDelay)

	newVotingThreshold := lachesis.GetVotingThreshold() *
		math.Pow(1.1, predictedLoad-0.5)
	newVotingThreshold = math.Max(0.66, math.Min(newVotingThreshold, 0.9))
	lachesis.SetVotingThreshold(newVotingThreshold)

	// --- DPoS ---
	newSetSize := int(float64(dpos.GetSetSize()) *
		math.Pow(1.2, predictedLoad-0.5))
	newSetSize = maxInt(4, minInt(newSetSize, 100))
	dpos.SetSetSize(newSetSize)

	// --- PoH ---
	newTickDelay := time.Duration(float64(poh.GetTickDelay()) *
		math.Pow(1.1, predictedLoad-0.5))
	poh.SetTickDelay(newTickDelay)
}

// monitorPerformance checks how our actual block time compares to the target,
// then calls fine-tuning methods as needed.
func (o *Optimizer) monitorPerformance() {
	metrics := o.collectPerformanceMetrics()

	targetTimeSeconds := targetBlockTime.Seconds()
	actualTimeSeconds := metrics.averageBlockTime.Seconds()

	if actualTimeSeconds > targetTimeSeconds*1.2 {
		o.adjustForSlowPerformance()
	} else if actualTimeSeconds < targetTimeSeconds*0.8 {
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

	if lachesis == nil || dpos == nil || poh == nil {
		return
	}

	deviation := (actualTime - targetTime) / targetTime

	// Adjust Lachesis gossip delay
	gossipDelay := lachesis.GetGossipDelay()
	lachesis.SetGossipDelay(time.Duration(float64(gossipDelay) * (1 - 0.1*deviation)))

	// Adjust DPoS set size
	setSize := dpos.GetSetSize()
	dpos.SetSetSize(int(float64(setSize) * (1 - 0.1*deviation)))

	// Adjust PoH tick delay
	tickDelay := poh.GetTickDelay()
	poh.SetTickDelay(time.Duration(float64(tickDelay) * (1 - 0.1*deviation)))
}

// logPerformanceMetrics is a helper that prints performance info to the logger.
func (o *Optimizer) logPerformanceMetrics(
	metrics PerformanceMetrics,
	targetTime, actualTime float64,
) {
	if o.logger == nil {
		return
	}
	o.logger.Info("Performance Metrics",
		"target_block_time", targetTime,
		"actual_block_time", actualTime,
		"tps", metrics.tps,
		"network_load", metrics.networkLoad,
		"gossip_delay", o.consensus.GetLachesis().GetGossipDelay(),
		"voting_threshold", o.consensus.GetLachesis().GetVotingThreshold(),
		"dpos_set_size", o.consensus.GetDPoS().GetSetSize(),
		"poh_tick_delay", o.consensus.GetPoH().GetTickDelay(),
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
		return
	}

	// Slightly reduce gossip delay, set size, and PoH tick to speed up
	gossipDelay := lachesis.GetGossipDelay()
	lachesis.SetGossipDelay(time.Duration(float64(gossipDelay) * 0.9))

	setSize := dpos.GetSetSize()
	dpos.SetSetSize(maxInt(4, setSize-1))

	tickDelay := poh.GetTickDelay()
	poh.SetTickDelay(time.Duration(float64(tickDelay) * 0.9))

	if o.logger != nil {
		o.logger.Info("Adjusting for slow performance",
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
		return
	}

	gossipDelay := lachesis.GetGossipDelay()
	lachesis.SetGossipDelay(time.Duration(float64(gossipDelay) * 1.1))

	setSize := dpos.GetSetSize()
	dpos.SetSetSize(minInt(100, setSize+1))

	tickDelay := poh.GetTickDelay()
	poh.SetTickDelay(time.Duration(float64(tickDelay) * 1.1))

	if o.logger != nil {
		o.logger.Info("Adjusting for fast performance",
			"new_gossip_delay", lachesis.GetGossipDelay(),
			"new_set_size", dpos.GetSetSize(),
			"new_tick_delay", poh.GetTickDelay(),
		)
	}
}

// Run periodically collects samples and optimizes consensus parameters until stopChan is closed.
func (o *Optimizer) Run(stopChan chan struct{}) {
	// 1) Immediately collect one sample so we have sample_count >= 1 quickly
	o.CollectSample()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// 2) Periodically collect more samples & optimize as usual
			o.CollectSample()
			o.OptimizeConsensus()
		case <-stopChan:
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

	if o.consensus == nil {
		return
	}
	lachesis := o.consensus.GetLachesis()
	dpos := o.consensus.GetDPoS()
	poh := o.consensus.GetPoH()

	if lachesis == nil || dpos == nil || poh == nil {
		return
	}
	// Reset to some defaults (these are arbitrary example values)
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
