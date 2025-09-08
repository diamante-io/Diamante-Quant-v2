// consensus/common/metrics.go

package common

import (
	"fmt"
	"sync"
	"time"

	"diamante/consensus"
)

// MetricsFramework provides centralized metrics collection for consensus operations
type MetricsFramework struct {
	counters map[string]*Counter
	timers   map[string]*Timer
	gauges   map[string]*Gauge
	logger   Logger
	mu       sync.RWMutex
}

// Counter represents a monotonically increasing counter
type Counter struct {
	name  string
	value uint64
	mu    sync.RWMutex
}

// Timer represents a timer for measuring durations
type Timer struct {
	name         string
	totalTime    time.Duration
	count        uint64
	minTime      time.Duration
	maxTime      time.Duration
	lastDuration time.Duration
	mu           sync.RWMutex
}

// Gauge represents a gauge that can go up and down
type Gauge struct {
	name  string
	value float64
	mu    sync.RWMutex
}

// TimerOperation represents an ongoing timing operation
type TimerOperation struct {
	timer     *Timer
	startTime time.Time
}

// NewMetricsFramework creates a new metrics framework
func NewMetricsFramework(logger Logger) *MetricsFramework {
	return &MetricsFramework{
		counters: make(map[string]*Counter),
		timers:   make(map[string]*Timer),
		gauges:   make(map[string]*Gauge),
		logger:   logger,
	}
}

// Counter returns or creates a counter with the given name
func (mf *MetricsFramework) Counter(name string) *Counter {
	mf.mu.RLock()
	if counter, exists := mf.counters[name]; exists {
		mf.mu.RUnlock()
		return counter
	}
	mf.mu.RUnlock()

	mf.mu.Lock()
	defer mf.mu.Unlock()

	// Double-check after acquiring write lock
	if counter, exists := mf.counters[name]; exists {
		return counter
	}

	counter := &Counter{
		name: name,
	}
	mf.counters[name] = counter
	return counter
}

// Timer returns or creates a timer with the given name
func (mf *MetricsFramework) Timer(name string) *Timer {
	mf.mu.RLock()
	if timer, exists := mf.timers[name]; exists {
		mf.mu.RUnlock()
		return timer
	}
	mf.mu.RUnlock()

	mf.mu.Lock()
	defer mf.mu.Unlock()

	// Double-check after acquiring write lock
	if timer, exists := mf.timers[name]; exists {
		return timer
	}

	timer := &Timer{
		name:    name,
		minTime: time.Duration(^uint64(0) >> 1), // Max duration as initial min
	}
	mf.timers[name] = timer
	return timer
}

// Gauge returns or creates a gauge with the given name
func (mf *MetricsFramework) Gauge(name string) *Gauge {
	mf.mu.RLock()
	if gauge, exists := mf.gauges[name]; exists {
		mf.mu.RUnlock()
		return gauge
	}
	mf.mu.RUnlock()

	mf.mu.Lock()
	defer mf.mu.Unlock()

	// Double-check after acquiring write lock
	if gauge, exists := mf.gauges[name]; exists {
		return gauge
	}

	gauge := &Gauge{
		name: name,
	}
	mf.gauges[name] = gauge
	return gauge
}

// LogMetrics logs all metrics for a component
func (mf *MetricsFramework) LogMetrics(component string, additionalMetrics TypedMetricsMap) {
	mf.mu.RLock()
	defer mf.mu.RUnlock()

	metrics := make(TypedMetricsMap)

	// Add counter metrics
	for name, counter := range mf.counters {
		counter.mu.RLock()
		metrics.SetUint64(name, counter.value)
		counter.mu.RUnlock()
	}

	// Add timer metrics
	for name, timer := range mf.timers {
		timer.mu.RLock()
		if timer.count > 0 {
			avgTime := timer.totalTime / time.Duration(timer.count)
			metrics.SetDuration(name+"_avg", avgTime)
			metrics.SetDuration(name+"_min", timer.minTime)
			metrics.SetDuration(name+"_max", timer.maxTime)
			metrics.SetUint64(name+"_count", timer.count)
			metrics.SetDuration(name+"_total", timer.totalTime)
			metrics.SetDuration(name+"_last", timer.lastDuration)
		}
		timer.mu.RUnlock()
	}

	// Add gauge metrics
	for name, gauge := range mf.gauges {
		gauge.mu.RLock()
		metrics.SetFloat64(name, gauge.value)
		gauge.mu.RUnlock()
	}

	// Add additional metrics
	if additionalMetrics != nil {
		metrics.Merge(additionalMetrics)
	}

	// Log all metrics
	// Convert to loggable format
	logData := make(map[string]string)
	for k, v := range metrics {
		logData[k] = v.String()
	}
	mf.logger.Info(fmt.Sprintf("%s metrics", component), "metrics", logData)
}

// GetAllMetrics returns all current metrics as a map
func (mf *MetricsFramework) GetAllMetrics() TypedMetricsMap {
	mf.mu.RLock()
	defer mf.mu.RUnlock()

	metrics := make(TypedMetricsMap)

	// Add counter metrics
	for name, counter := range mf.counters {
		counter.mu.RLock()
		metrics.SetUint64(name, counter.value)
		counter.mu.RUnlock()
	}

	// Add timer metrics
	for name, timer := range mf.timers {
		timer.mu.RLock()
		timerStats := &TimerStats{
			Count: timer.count,
			Total: timer.totalTime,
			Last:  timer.lastDuration,
		}
		if timer.count > 0 {
			timerStats.Avg = timer.totalTime / time.Duration(timer.count)
			timerStats.Min = timer.minTime
			timerStats.Max = timer.maxTime
		}
		metrics.SetTimerStats(name, timerStats)
		timer.mu.RUnlock()
	}

	// Add gauge metrics
	for name, gauge := range mf.gauges {
		gauge.mu.RLock()
		metrics.SetFloat64(name, gauge.value)
		gauge.mu.RUnlock()
	}

	return metrics
}

// Reset resets all metrics
func (mf *MetricsFramework) Reset() {
	mf.mu.Lock()
	defer mf.mu.Unlock()

	// Reset counters
	for _, counter := range mf.counters {
		counter.mu.Lock()
		counter.value = 0
		counter.mu.Unlock()
	}

	// Reset timers
	for _, timer := range mf.timers {
		timer.mu.Lock()
		timer.totalTime = 0
		timer.count = 0
		timer.minTime = time.Duration(^uint64(0) >> 1)
		timer.maxTime = 0
		timer.lastDuration = 0
		timer.mu.Unlock()
	}

	// Reset gauges
	for _, gauge := range mf.gauges {
		gauge.mu.Lock()
		gauge.value = 0
		gauge.mu.Unlock()
	}
}

// Counter methods

// Inc increments the counter by 1
func (c *Counter) Inc() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.value++
}

// Add adds the given value to the counter
func (c *Counter) Add(value uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.value += value
}

// Get returns the current value of the counter
func (c *Counter) Get() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.value
}

// Set sets the counter to the given value
func (c *Counter) Set(value uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.value = value
}

// Reset resets the counter to 0
func (c *Counter) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.value = 0
}

// Timer methods

// Start starts a timing operation and returns a function to stop it
func (t *Timer) Start() func() {
	startTime := consensus.ConsensusNow()
	return func() {
		t.Record(consensus.ConsensusSince(startTime))
	}
}

// StartOperation starts a timing operation and returns a TimerOperation
func (t *Timer) StartOperation() *TimerOperation {
	return &TimerOperation{
		timer:     t,
		startTime: consensus.ConsensusNow(),
	}
}

// Record records a duration
func (t *Timer) Record(duration time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.totalTime += duration
	t.count++
	t.lastDuration = duration

	if duration < t.minTime {
		t.minTime = duration
	}
	if duration > t.maxTime {
		t.maxTime = duration
	}
}

// GetAverage returns the average duration
func (t *Timer) GetAverage() time.Duration {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.count == 0 {
		return 0
	}
	return t.totalTime / time.Duration(t.count)
}

// GetCount returns the number of recorded durations
func (t *Timer) GetCount() uint64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.count
}

// GetTotal returns the total recorded time
func (t *Timer) GetTotal() time.Duration {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.totalTime
}

// GetMin returns the minimum recorded duration
func (t *Timer) GetMin() time.Duration {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.count == 0 {
		return 0
	}
	return t.minTime
}

// GetMax returns the maximum recorded duration
func (t *Timer) GetMax() time.Duration {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.maxTime
}

// GetLast returns the last recorded duration
func (t *Timer) GetLast() time.Duration {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.lastDuration
}

// Reset resets all timer statistics
func (t *Timer) Reset() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.totalTime = 0
	t.count = 0
	t.minTime = time.Duration(^uint64(0) >> 1)
	t.maxTime = 0
	t.lastDuration = 0
}

// TimerOperation methods

// Stop stops the timing operation and records the duration
func (to *TimerOperation) Stop() {
	duration := consensus.ConsensusSince(to.startTime)
	to.timer.Record(duration)
}

// Gauge methods

// Set sets the gauge to the given value
func (g *Gauge) Set(value float64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.value = value
}

// Add adds the given value to the gauge
func (g *Gauge) Add(value float64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.value += value
}

// Sub subtracts the given value from the gauge
func (g *Gauge) Sub(value float64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.value -= value
}

// Inc increments the gauge by 1
func (g *Gauge) Inc() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.value++
}

// Dec decrements the gauge by 1
func (g *Gauge) Dec() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.value--
}

// Get returns the current value of the gauge
func (g *Gauge) Get() float64 {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.value
}

// Reset resets the gauge to 0
func (g *Gauge) Reset() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.value = 0
}

// Convenience functions for common metrics patterns

// IncrementCounter is a convenience function to increment a counter
func (mf *MetricsFramework) IncrementCounter(name string) {
	mf.Counter(name).Inc()
}

// AddToCounter is a convenience function to add to a counter
func (mf *MetricsFramework) AddToCounter(name string, value uint64) {
	mf.Counter(name).Add(value)
}

// RecordTime is a convenience function to record a duration
func (mf *MetricsFramework) RecordTime(name string, duration time.Duration) {
	mf.Timer(name).Record(duration)
}

// SetGauge is a convenience function to set a gauge value
func (mf *MetricsFramework) SetGauge(name string, value float64) {
	mf.Gauge(name).Set(value)
}

// TimeOperation is a convenience function to time an operation
func (mf *MetricsFramework) TimeOperation(name string, operation func()) {
	stop := mf.Timer(name).Start()
	defer stop()
	operation()
}

// TimeOperationWithResult is a convenience function to time an operation that returns a result
func (mf *MetricsFramework) TimeOperationWithResult(name string, operation func() error) error {
	stop := mf.Timer(name).Start()
	defer stop()
	return operation()
}

// MetricsSnapshot represents a snapshot of metrics at a point in time
type MetricsSnapshot struct {
	Timestamp time.Time
	Metrics   TypedMetricsMap
}

// TakeSnapshot takes a snapshot of all current metrics
func (mf *MetricsFramework) TakeSnapshot() *MetricsSnapshot {
	return &MetricsSnapshot{
		Timestamp: consensus.ConsensusNow(),
		Metrics:   mf.GetAllMetrics(),
	}
}

// MetricsCollector collects metrics periodically
type MetricsCollector struct {
	framework *MetricsFramework
	interval  time.Duration
	logger    Logger
	stopCh    chan struct{}
	component string
}

// NewMetricsCollector creates a new metrics collector
func NewMetricsCollector(framework *MetricsFramework, component string, interval time.Duration, logger Logger) *MetricsCollector {
	return &MetricsCollector{
		framework: framework,
		interval:  interval,
		logger:    logger,
		stopCh:    make(chan struct{}),
		component: component,
	}
}

// Start starts the metrics collection
func (mc *MetricsCollector) Start() {
	go mc.collectLoop()
}

// Stop stops the metrics collection
func (mc *MetricsCollector) Stop() {
	close(mc.stopCh)
}

// collectLoop runs the metrics collection loop
func (mc *MetricsCollector) collectLoop() {
	ticker := time.NewTicker(mc.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			mc.framework.LogMetrics(mc.component, nil)
		case <-mc.stopCh:
			return
		}
	}
}

// Common metric names used across consensus modules
const (
	// Event metrics
	MetricEventsCreated         = "events.created"
	MetricEventsFinalized       = "events.finalized"
	MetricEventsPending         = "events.pending"
	MetricEventValidationErrors = "events.validation_errors"
	MetricEventFinalizationTime = "events.finalization_time"
	MetricEventRetries          = "events.retries"
	MetricEventTimeouts         = "events.timeouts"
	MetricEventDuplicates       = "events.duplicates"

	// Block metrics
	MetricBlocksProduced        = "blocks.produced"
	MetricBlocksValidated       = "blocks.validated"
	MetricBlockProcessingTime   = "blocks.processing_time"
	MetricBlockValidationErrors = "blocks.validation_errors"
	MetricBlockHeight           = "blocks.height"

	// Validator metrics
	MetricValidatorsActive    = "validators.active"
	MetricValidatorsTotal     = "validators.total"
	MetricValidatorStakeTotal = "validators.stake_total"
	MetricValidatorRewards    = "validators.rewards"
	MetricValidatorSlashings  = "validators.slashings"

	// PoH metrics
	MetricPoHCount              = "poh.count"
	MetricPoHVerifications      = "poh.verifications"
	MetricPoHVerificationErrors = "poh.verification_errors"
	MetricPoHTickTime           = "poh.tick_time"

	// Network metrics
	MetricNetworkLoad       = "network.load"
	MetricNetworkPartitions = "network.partitions"
	MetricNetworkRecoveries = "network.recoveries"

	// Performance metrics
	MetricCPUUtilization    = "performance.cpu_utilization"
	MetricMemoryUtilization = "performance.memory_utilization"
	MetricGoroutineCount    = "performance.goroutine_count"

	// Error metrics
	MetricErrorsTotal     = "errors.total"
	MetricErrorsTemporary = "errors.temporary"
	MetricErrorsPermanent = "errors.permanent"
	MetricErrorsByzantine = "errors.byzantine"
	MetricErrorRecoveries = "errors.recoveries"
)

// CreateStandardMetrics creates standard metrics for a consensus component
func (mf *MetricsFramework) CreateStandardMetrics() {
	// Pre-create common metrics to avoid lock contention during operation
	standardMetrics := []string{
		MetricEventsCreated,
		MetricEventsFinalized,
		MetricEventsPending,
		MetricEventValidationErrors,
		MetricEventRetries,
		MetricEventTimeouts,
		MetricEventDuplicates,
		MetricBlocksProduced,
		MetricBlocksValidated,
		MetricBlockValidationErrors,
		MetricValidatorsActive,
		MetricValidatorsTotal,
		MetricValidatorRewards,
		MetricValidatorSlashings,
		MetricPoHVerifications,
		MetricPoHVerificationErrors,
		MetricNetworkPartitions,
		MetricNetworkRecoveries,
		MetricErrorsTotal,
		MetricErrorsTemporary,
		MetricErrorsPermanent,
		MetricErrorsByzantine,
		MetricErrorRecoveries,
	}

	for _, metric := range standardMetrics {
		mf.Counter(metric)
	}

	// Pre-create common timers
	standardTimers := []string{
		MetricEventFinalizationTime,
		MetricBlockProcessingTime,
		MetricPoHTickTime,
	}

	for _, timer := range standardTimers {
		mf.Timer(timer)
	}

	// Pre-create common gauges
	standardGauges := []string{
		MetricBlockHeight,
		MetricValidatorStakeTotal,
		MetricPoHCount,
		MetricNetworkLoad,
		MetricCPUUtilization,
		MetricMemoryUtilization,
		MetricGoroutineCount,
	}

	for _, gauge := range standardGauges {
		mf.Gauge(gauge)
	}
}
