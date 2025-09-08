package optimization

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"diamante/common"

	"github.com/sirupsen/logrus"
)

// DBPool provides a production-ready database connection pool with advanced features
type DBPool struct {
	*sql.DB

	// Configuration
	config *DBPoolConfig

	// Metrics
	activeConnections  atomic.Int64
	totalConnections   atomic.Int64
	failedConnections  atomic.Int64
	connectionWaitTime atomic.Int64
	queryCount         atomic.Int64
	queryErrors        atomic.Int64

	// Health monitoring
	healthChecker *HealthChecker
	logger        *logrus.Logger

	// Circuit breaker
	circuitBreaker *CircuitBreaker

	// Context for lifecycle
	ctx    context.Context
	cancel context.CancelFunc

	// Connection tracking
	connTracker *ConnectionTracker
}

// DBPoolConfig holds configuration for database pool
type DBPoolConfig struct {
	DriverName           string
	DataSourceName       string
	MaxOpenConns         int
	MaxIdleConns         int
	ConnMaxLifetime      time.Duration
	ConnMaxIdleTime      time.Duration
	HealthCheckInterval  time.Duration
	SlowQueryThreshold   time.Duration
	EnableMetrics        bool
	EnableTracing        bool
	RetryPolicy          *RetryPolicy
	CircuitBreakerConfig *CircuitBreakerConfig
}

// DefaultDBPoolConfig returns production-ready default configuration
func DefaultDBPoolConfig() *DBPoolConfig {
	return &DBPoolConfig{
		MaxOpenConns:        100,
		MaxIdleConns:        10,
		ConnMaxLifetime:     30 * time.Minute,
		ConnMaxIdleTime:     5 * time.Minute,
		HealthCheckInterval: 30 * time.Second,
		SlowQueryThreshold:  1 * time.Second,
		EnableMetrics:       true,
		EnableTracing:       true,
		RetryPolicy: &RetryPolicy{
			MaxRetries:     3,
			InitialBackoff: 100 * time.Millisecond,
			MaxBackoff:     5 * time.Second,
			Multiplier:     2,
		},
		CircuitBreakerConfig: &CircuitBreakerConfig{
			FailureThreshold: 5,
			SuccessThreshold: 2,
			Timeout:          60 * time.Second,
		},
	}
}

// RetryPolicy defines retry behavior
type RetryPolicy struct {
	MaxRetries     int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	Multiplier     float64
}

// CircuitBreakerConfig defines circuit breaker behavior
type CircuitBreakerConfig struct {
	FailureThreshold int
	SuccessThreshold int
	Timeout          time.Duration
}

// CircuitBreaker implements circuit breaker pattern
type CircuitBreaker struct {
	config          *CircuitBreakerConfig
	failures        atomic.Int32
	successes       atomic.Int32
	lastFailureTime atomic.Int64
	state           atomic.Uint32 // 0: closed, 1: open, 2: half-open
	mu              sync.RWMutex
}

// HealthChecker performs database health checks
type HealthChecker struct {
	pool            *DBPool
	ticker          *time.Ticker
	lastCheckTime   atomic.Int64
	lastCheckStatus atomic.Bool
	checkQuery      string
}

// ConnectionTracker tracks active connections
type ConnectionTracker struct {
	mu          sync.RWMutex
	connections map[string]*TrackedConnection
}

// TrackedConnection represents a tracked database connection
type TrackedConnection struct {
	ID         string
	StartTime  time.Time
	Query      string
	Context    context.Context
	CancelFunc context.CancelFunc
}

// NewDBPool creates a new production-ready database pool
func NewDBPool(config *DBPoolConfig) (*DBPool, error) {
	if config == nil {
		config = DefaultDBPoolConfig()
	}

	// Validate configuration
	if err := validateDBPoolConfig(config); err != nil {
		return nil, fmt.Errorf("invalid pool configuration: %w", err)
	}

	// Open database connection
	db, err := sql.Open(config.DriverName, config.DataSourceName)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Configure connection pool
	db.SetMaxOpenConns(config.MaxOpenConns)
	db.SetMaxIdleConns(config.MaxIdleConns)
	db.SetConnMaxLifetime(config.ConnMaxLifetime)
	db.SetConnMaxIdleTime(config.ConnMaxIdleTime)

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	logger := logrus.New()
	logger.SetLevel(logrus.InfoLevel)

	poolCtx, poolCancel := context.WithCancel(context.Background())

	pool := &DBPool{
		DB:             db,
		config:         config,
		logger:         logger,
		ctx:            poolCtx,
		cancel:         poolCancel,
		circuitBreaker: newCircuitBreaker(config.CircuitBreakerConfig),
		connTracker:    &ConnectionTracker{connections: make(map[string]*TrackedConnection)},
	}

	// Start health checker
	if config.HealthCheckInterval > 0 {
		pool.healthChecker = &HealthChecker{
			pool:       pool,
			checkQuery: "SELECT 1",
		}
		pool.healthChecker.Start()
	}

	return pool, nil
}

// validateDBPoolConfig validates pool configuration
func validateDBPoolConfig(config *DBPoolConfig) error {
	if config.DriverName == "" {
		return fmt.Errorf("driver name is required")
	}
	if config.DataSourceName == "" {
		return fmt.Errorf("data source name is required")
	}
	if config.MaxOpenConns <= 0 {
		return fmt.Errorf("max open connections must be positive")
	}
	if config.MaxIdleConns < 0 {
		return fmt.Errorf("max idle connections cannot be negative")
	}
	if config.MaxIdleConns > config.MaxOpenConns {
		return fmt.Errorf("max idle connections cannot exceed max open connections")
	}
	return nil
}

// newCircuitBreaker creates a new circuit breaker
func newCircuitBreaker(config *CircuitBreakerConfig) *CircuitBreaker {
	if config == nil {
		config = &CircuitBreakerConfig{
			FailureThreshold: 5,
			SuccessThreshold: 2,
			Timeout:          60 * time.Second,
		}
	}
	return &CircuitBreaker{config: config}
}

// QueryContext executes a query with enhanced monitoring and retry logic
func (p *DBPool) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	startTime := common.ConsensusNow()

	// Check circuit breaker
	if !p.circuitBreaker.Allow() {
		p.failedConnections.Add(1)
		return nil, fmt.Errorf("circuit breaker is open")
	}

	// Track connection
	connID := p.trackConnection(ctx, query)
	defer p.untrackConnection(connID)

	// Execute with retry
	var rows *sql.Rows
	var err error

	retryCount := 0
	backoff := p.config.RetryPolicy.InitialBackoff

	for {
		rows, err = p.DB.QueryContext(ctx, query, args...)

		if err == nil {
			p.circuitBreaker.RecordSuccess()
			p.queryCount.Add(1)

			// Log slow queries
			duration := common.ConsensusSince(startTime)
			if duration > p.config.SlowQueryThreshold {
				p.logger.WithFields(logrus.Fields{
					"query":    query,
					"duration": duration,
					"args":     args,
				}).Warn("Slow query detected")
			}

			return rows, nil
		}

		// Check if we should retry
		if !isRetryableError(err) || retryCount >= p.config.RetryPolicy.MaxRetries {
			p.circuitBreaker.RecordFailure()
			p.queryErrors.Add(1)
			return nil, fmt.Errorf("query failed after %d retries: %w", retryCount, err)
		}

		// Wait before retry
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(backoff):
			retryCount++
			backoff = time.Duration(float64(backoff) * p.config.RetryPolicy.Multiplier)
			if backoff > p.config.RetryPolicy.MaxBackoff {
				backoff = p.config.RetryPolicy.MaxBackoff
			}
		}
	}
}

// ExecContext executes a command with enhanced monitoring
func (p *DBPool) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	startTime := common.ConsensusNow()

	// Check circuit breaker
	if !p.circuitBreaker.Allow() {
		p.failedConnections.Add(1)
		return nil, fmt.Errorf("circuit breaker is open")
	}

	// Track connection
	connID := p.trackConnection(ctx, query)
	defer p.untrackConnection(connID)

	// Execute with retry
	result, err := p.execWithRetry(ctx, query, args...)

	// Log slow queries
	duration := common.ConsensusSince(startTime)
	if duration > p.config.SlowQueryThreshold {
		p.logger.WithFields(logrus.Fields{
			"query":    query,
			"duration": duration,
			"args":     args,
		}).Warn("Slow query detected")
	}

	return result, err
}

// execWithRetry executes a command with retry logic
func (p *DBPool) execWithRetry(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	retryCount := 0
	backoff := p.config.RetryPolicy.InitialBackoff

	for {
		result, err := p.DB.ExecContext(ctx, query, args...)

		if err == nil {
			p.circuitBreaker.RecordSuccess()
			p.queryCount.Add(1)
			return result, nil
		}

		// Check if we should retry
		if !isRetryableError(err) || retryCount >= p.config.RetryPolicy.MaxRetries {
			p.circuitBreaker.RecordFailure()
			p.queryErrors.Add(1)
			return nil, fmt.Errorf("exec failed after %d retries: %w", retryCount, err)
		}

		// Wait before retry
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(backoff):
			retryCount++
			backoff = time.Duration(float64(backoff) * p.config.RetryPolicy.Multiplier)
			if backoff > p.config.RetryPolicy.MaxBackoff {
				backoff = p.config.RetryPolicy.MaxBackoff
			}
		}
	}
}

// BeginTx begins a transaction with timeout
func (p *DBPool) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	// Check circuit breaker
	if !p.circuitBreaker.Allow() {
		p.failedConnections.Add(1)
		return nil, fmt.Errorf("circuit breaker is open")
	}

	tx, err := p.DB.BeginTx(ctx, opts)
	if err != nil {
		p.circuitBreaker.RecordFailure()
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}

	p.circuitBreaker.RecordSuccess()
	p.activeConnections.Add(1)

	// Create a wrapper to track when transaction completes
	wrapper := &txWrapper{
		tx:   tx,
		pool: p,
		ctx:  ctx,
	}

	// Set up a finalizer to ensure we decrement counter if transaction is abandoned
	go func() {
		<-ctx.Done()
		wrapper.cleanup()
	}()

	return tx, nil
}

// txWrapper tracks transaction lifecycle
type txWrapper struct {
	tx   *sql.Tx
	pool *DBPool
	ctx  context.Context
	done atomic.Bool
}

func (w *txWrapper) cleanup() {
	if w.done.CompareAndSwap(false, true) {
		w.pool.activeConnections.Add(-1)
	}
}

// trackConnection tracks an active connection
func (p *DBPool) trackConnection(ctx context.Context, query string) string {
	connID := fmt.Sprintf("conn_%d_%d", common.ConsensusNow().UnixNano(), p.totalConnections.Add(1))

	trackedCtx, cancel := context.WithCancel(ctx)

	conn := &TrackedConnection{
		ID:         connID,
		StartTime:  common.ConsensusNow(),
		Query:      query,
		Context:    trackedCtx,
		CancelFunc: cancel,
	}

	p.connTracker.mu.Lock()
	p.connTracker.connections[connID] = conn
	p.connTracker.mu.Unlock()

	p.activeConnections.Add(1)

	return connID
}

// untrackConnection removes a tracked connection
func (p *DBPool) untrackConnection(connID string) {
	p.connTracker.mu.Lock()
	if conn, ok := p.connTracker.connections[connID]; ok {
		conn.CancelFunc()
		delete(p.connTracker.connections, connID)
	}
	p.connTracker.mu.Unlock()

	p.activeConnections.Add(-1)
}

// GetStats returns pool statistics
func (p *DBPool) GetStats() DBPoolStats {
	dbStats := p.DB.Stats()

	return DBPoolStats{
		OpenConnections:     dbStats.OpenConnections,
		InUse:               dbStats.InUse,
		Idle:                dbStats.Idle,
		WaitCount:           dbStats.WaitCount,
		WaitDuration:        dbStats.WaitDuration,
		MaxIdleClosed:       dbStats.MaxIdleClosed,
		MaxIdleTimeClosed:   dbStats.MaxIdleTimeClosed,
		MaxLifetimeClosed:   dbStats.MaxLifetimeClosed,
		ActiveConnections:   p.activeConnections.Load(),
		TotalConnections:    p.totalConnections.Load(),
		FailedConnections:   p.failedConnections.Load(),
		QueryCount:          p.queryCount.Load(),
		QueryErrors:         p.queryErrors.Load(),
		CircuitBreakerState: p.circuitBreaker.State(),
		HealthStatus:        p.healthChecker != nil && p.healthChecker.IsHealthy(),
	}
}

// DBPoolStats holds pool statistics
type DBPoolStats struct {
	OpenConnections     int
	InUse               int
	Idle                int
	WaitCount           int64
	WaitDuration        time.Duration
	MaxIdleClosed       int64
	MaxIdleTimeClosed   int64
	MaxLifetimeClosed   int64
	ActiveConnections   int64
	TotalConnections    int64
	FailedConnections   int64
	QueryCount          int64
	QueryErrors         int64
	CircuitBreakerState string
	HealthStatus        bool
}

// SetLogger sets a custom logger
func (p *DBPool) SetLogger(logger *logrus.Logger) {
	if logger != nil {
		p.logger = logger
	}
}

// Close closes the pool and cleans up resources
func (p *DBPool) Close() error {
	// Cancel context
	p.cancel()

	// Stop health checker
	if p.healthChecker != nil {
		p.healthChecker.Stop()
	}

	// Cancel all active connections
	p.connTracker.mu.Lock()
	for _, conn := range p.connTracker.connections {
		conn.CancelFunc()
	}
	p.connTracker.connections = make(map[string]*TrackedConnection)
	p.connTracker.mu.Unlock()

	// Close database
	if err := p.DB.Close(); err != nil {
		return fmt.Errorf("failed to close database: %w", err)
	}

	p.logger.Info("Database pool closed")
	return nil
}

// Circuit Breaker methods

// Allow checks if request is allowed
func (cb *CircuitBreaker) Allow() bool {
	state := cb.getState()

	switch state {
	case 1: // Open
		// Check if timeout has passed
		lastFailure := cb.lastFailureTime.Load()
		if time.Since(time.Unix(0, lastFailure)) > cb.config.Timeout {
			cb.setState(2) // Half-open
			cb.successes.Store(0)
			return true
		}
		return false

	case 2: // Half-open
		return true

	default: // Closed
		return true
	}
}

// RecordSuccess records a successful operation
func (cb *CircuitBreaker) RecordSuccess() {
	state := cb.getState()

	if state == 2 { // Half-open
		successes := cb.successes.Add(1)
		if successes >= int32(cb.config.SuccessThreshold) {
			cb.setState(0) // Closed
			cb.failures.Store(0)
		}
	} else if state == 0 { // Closed
		cb.failures.Store(0)
	}
}

// RecordFailure records a failed operation
func (cb *CircuitBreaker) RecordFailure() {
	failures := cb.failures.Add(1)
	cb.lastFailureTime.Store(common.ConsensusNow().UnixNano())

	if failures >= int32(cb.config.FailureThreshold) {
		cb.setState(1) // Open
	}
}

// State returns the current state as string
func (cb *CircuitBreaker) State() string {
	switch cb.getState() {
	case 0:
		return "closed"
	case 1:
		return "open"
	case 2:
		return "half-open"
	default:
		return "unknown"
	}
}

func (cb *CircuitBreaker) getState() uint32 {
	return cb.state.Load()
}

func (cb *CircuitBreaker) setState(state uint32) {
	cb.state.Store(state)
}

// Health Checker methods

// Start begins health checking
func (hc *HealthChecker) Start() {
	hc.ticker = time.NewTicker(hc.pool.config.HealthCheckInterval)

	go func() {
		for {
			select {
			case <-hc.pool.ctx.Done():
				return
			case <-hc.ticker.C:
				hc.performCheck()
			}
		}
	}()

	// Perform initial check
	hc.performCheck()
}

// Stop stops health checking
func (hc *HealthChecker) Stop() {
	if hc.ticker != nil {
		hc.ticker.Stop()
	}
}

// performCheck performs a health check
func (hc *HealthChecker) performCheck() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := hc.pool.DB.ExecContext(ctx, hc.checkQuery)

	hc.lastCheckTime.Store(common.ConsensusNow().Unix())
	hc.lastCheckStatus.Store(err == nil)

	if err != nil {
		hc.pool.logger.WithError(err).Error("Database health check failed")
	}
}

// IsHealthy returns the current health status
func (hc *HealthChecker) IsHealthy() bool {
	// Check if last check is recent
	lastCheck := hc.lastCheckTime.Load()
	if time.Since(time.Unix(lastCheck, 0)) > 2*hc.pool.config.HealthCheckInterval {
		return false
	}

	return hc.lastCheckStatus.Load()
}

// isRetryableError determines if an error is retryable
func isRetryableError(err error) bool {
	if err == nil {
		return false
	}

	// Check for common retryable errors
	errStr := err.Error()
	retryablePatterns := []string{
		"connection refused",
		"connection reset",
		"broken pipe",
		"timeout",
		"temporary failure",
		"deadlock",
		"lock wait timeout",
	}

	for _, pattern := range retryablePatterns {
		if containsIgnoreCase(errStr, pattern) {
			return true
		}
	}

	return false
}

// containsIgnoreCase checks if a string contains a substring (case-insensitive)
func containsIgnoreCase(s, substr string) bool {
	s = strings.ToLower(s)
	substr = strings.ToLower(substr)
	return strings.Contains(s, substr)
}

// QueryRow is a convenience method that wraps QueryRowContext
func (p *DBPool) QueryRow(ctx context.Context, query string, args ...interface{}) *sql.Row {
	// For single row queries, we can use the standard method
	// The circuit breaker and tracking are handled by the underlying QueryContext
	return p.DB.QueryRowContext(ctx, query, args...)
}

// PrepareContext prepares a statement with monitoring
func (p *DBPool) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	// Check circuit breaker
	if !p.circuitBreaker.Allow() {
		return nil, fmt.Errorf("circuit breaker is open")
	}

	stmt, err := p.DB.PrepareContext(ctx, query)
	if err != nil {
		p.circuitBreaker.RecordFailure()
		return nil, fmt.Errorf("failed to prepare statement: %w", err)
	}

	p.circuitBreaker.RecordSuccess()
	return stmt, nil
}
