// security/production_hardening.go
package security

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"net"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"diamante/apperrors"
	"diamante/common"

	"github.com/sirupsen/logrus"
)

// ProductionHardening implements comprehensive security hardening for production
type ProductionHardening struct {
	config *HardeningConfig
	logger *logrus.Logger

	// Rate limiting
	rateLimiters map[string]*RateLimiter
	rlMutex      sync.RWMutex

	// Request tracking
	requestTracker *RequestTracker

	// Security monitors
	monitors []SecurityCheckMonitor

	// Incident response
	incidentHandler *IncidentHandler

	// Metrics
	metrics *SecurityMetrics

	// Control
	running int32
	ctx     context.Context
	cancel  context.CancelFunc
}

// HardeningConfig holds security hardening configuration
type HardeningConfig struct {
	// Rate limiting
	GlobalRateLimit int           // Requests per second globally
	PerIPRateLimit  int           // Requests per second per IP
	BurstLimit      int           // Burst allowance
	RateLimitWindow time.Duration // Rate limit window

	// Request validation
	MaxRequestSize int64         // Maximum request body size
	MaxHeaderSize  int           // Maximum header size
	RequestTimeout time.Duration // Maximum request processing time

	// Security headers
	EnableSecurityHeaders   bool
	StrictTransportSecurity bool
	ContentSecurityPolicy   string

	// Input validation
	EnableInputSanitization bool
	MaxInputLength          int
	BlockedPatterns         []string

	// Monitoring
	EnableSecurityMonitoring bool
	MonitoringInterval       time.Duration
	AlertThresholds          map[string]float64

	// Incident response
	EnableIncidentResponse bool
	AutoBlockEnabled       bool
	BlockDuration          time.Duration

	// Audit logging
	EnableAuditLogging bool
	AuditLogPath       string
	LogRotationSize    int64
}

// DefaultHardeningConfig returns production-grade security configuration
func DefaultHardeningConfig() *HardeningConfig {
	return &HardeningConfig{
		GlobalRateLimit:         1000,
		PerIPRateLimit:          10,
		BurstLimit:              50,
		RateLimitWindow:         time.Minute,
		MaxRequestSize:          1024 * 1024, // 1MB
		MaxHeaderSize:           8192,        // 8KB
		RequestTimeout:          30 * time.Second,
		EnableSecurityHeaders:   true,
		StrictTransportSecurity: true,
		ContentSecurityPolicy:   "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'",
		EnableInputSanitization: true,
		MaxInputLength:          10000,
		BlockedPatterns: []string{
			"<script", "javascript:", "vbscript:", "onload=", "onerror=",
			"eval(", "exec(", "../", "\\x", "0x", "DROP TABLE", "UNION SELECT",
		},
		EnableSecurityMonitoring: true,
		MonitoringInterval:       10 * time.Second,
		AlertThresholds: map[string]float64{
			"error_rate":       0.05, // 5% error rate
			"request_rate":     1000, // 1000 req/s
			"blocked_requests": 100,  // 100 blocked requests
		},
		EnableIncidentResponse: true,
		AutoBlockEnabled:       true,
		BlockDuration:          time.Hour,
		EnableAuditLogging:     true,
		AuditLogPath:           "/var/log/diamante/security.log",
		LogRotationSize:        100 * 1024 * 1024, // 100MB
	}
}

// SecurityMetrics tracks security-related metrics
type SecurityMetrics struct {
	TotalRequests       int64
	BlockedRequests     int64
	RateLimitedRequests int64
	SuspiciousRequests  int64
	IncidentsTriggered  int64
	LastSecurityScan    time.Time

	// Attack patterns detected
	SQLInjectionAttempts     int64
	XSSAttempts              int64
	PathTraversalAttempts    int64
	CommandInjectionAttempts int64
}

// NewProductionHardening creates a new production hardening system
func NewProductionHardening(config *HardeningConfig, logger *logrus.Logger) *ProductionHardening {
	if config == nil {
		config = DefaultHardeningConfig()
	}
	if logger == nil {
		logger = logrus.New()
	}

	ctx, cancel := context.WithCancel(context.Background())

	ph := &ProductionHardening{
		config:       config,
		logger:       logger,
		rateLimiters: make(map[string]*RateLimiter),
		ctx:          ctx,
		cancel:       cancel,
		metrics:      &SecurityMetrics{},
	}

	// Initialize components
	trackerConfig := &TrackerConfig{
		MaxRequests:         config.PerIPRateLimit * 60, // requests per hour
		TimeWindow:          config.RateLimitWindow,
		SuspiciousThreshold: 10,
		CleanupInterval:     time.Minute * 30,
	}
	ph.requestTracker = NewRequestTracker(trackerConfig)

	incidentConfig := &IncidentConfig{
		AutoResponse:      config.EnableIncidentResponse,
		ResponseTimeout:   config.RequestTimeout,
		MaxIncidents:      1000,
		RetentionPeriod:   time.Hour * 24,
		EscalationRules:   make(map[string]EscalationRule),
		NotificationRules: make(map[string]NotificationRule),
	}
	ph.incidentHandler = NewIncidentHandler(incidentConfig, nil)

	// Initialize security monitors
	ph.initializeMonitors()

	return ph
}

// Start begins the security hardening system
func (ph *ProductionHardening) Start() error {
	if !atomic.CompareAndSwapInt32(&ph.running, 0, 1) {
		return common.SecurityError(nil, "security hardening already running")
	}

	// Start request tracker
	if err := ph.requestTracker.Start(); err != nil {
		return common.NewContextualError(
			err,
			apperrors.ModuleSecurity,
			apperrors.CodeInternal,
			common.SeverityHigh,
			"failed to start request tracker",
		)
	}

	// Start incident handler
	if err := ph.incidentHandler.Start(); err != nil {
		return common.NewContextualError(
			err,
			apperrors.ModuleSecurity,
			apperrors.CodeInternal,
			common.SeverityHigh,
			"failed to start incident handler",
		)
	}

	// Start security monitors
	if ph.config.EnableSecurityMonitoring {
		go ph.runSecurityMonitoring()
	}

	ph.logger.Info("Production security hardening started")
	return nil
}

// Stop shuts down the security hardening system
func (ph *ProductionHardening) Stop() error {
	if !atomic.CompareAndSwapInt32(&ph.running, 1, 0) {
		return nil
	}

	ph.cancel()

	// Stop components
	ph.requestTracker.Stop()
	ph.incidentHandler.Stop()

	ph.logger.Info("Production security hardening stopped")
	return nil
}

// SecureHTTPMiddleware creates a security middleware for HTTP handlers
func (ph *ProductionHardening) SecureHTTPMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := common.ConsensusNow()
		clientIP := ph.getClientIP(r)

		atomic.AddInt64(&ph.metrics.TotalRequests, 1)

		// Apply security headers
		if ph.config.EnableSecurityHeaders {
			ph.applySecurityHeaders(w)
		}

		// Rate limiting
		if ph.isRateLimited(clientIP) {
			atomic.AddInt64(&ph.metrics.RateLimitedRequests, 1)
			ph.logSecurityEvent("rate_limit_exceeded", clientIP, r)
			http.Error(w, "Rate limit exceeded", http.StatusTooManyRequests)
			return
		}

		// Request size validation
		if r.ContentLength > ph.config.MaxRequestSize {
			atomic.AddInt64(&ph.metrics.BlockedRequests, 1)
			ph.logSecurityEvent("request_too_large", clientIP, r)
			http.Error(w, "Request too large", http.StatusRequestEntityTooLarge)
			return
		}

		// Input validation
		if ph.config.EnableInputSanitization {
			if suspicious, pattern := ph.validateRequest(r); suspicious {
				atomic.AddInt64(&ph.metrics.SuspiciousRequests, 1)
				ph.logSecurityEvent("suspicious_request", clientIP, r, "pattern", pattern)

				// Trigger incident response if enabled
				if ph.config.EnableIncidentResponse {
					ph.incidentHandler.HandleSuspiciousActivity(clientIP, pattern, r)
				}

				http.Error(w, "Request blocked", http.StatusForbidden)
				return
			}
		}

		// Track request
		reqInfo := &RequestInfo{
			ID:        fmt.Sprintf("%s-%d", clientIP, start.UnixNano()),
			Source:    clientIP,
			Timestamp: start,
			Path:      r.URL.Path,
			Method:    r.Method,
			Headers:   make(map[string]string),
			IP:        clientIP,
			UserAgent: r.Header.Get("User-Agent"),
		}

		// Copy headers
		for k, v := range r.Header {
			if len(v) > 0 {
				reqInfo.Headers[k] = v[0]
			}
		}

		ph.requestTracker.TrackRequest(r.Context(), reqInfo)

		// Set timeout
		ctx, cancel := context.WithTimeout(r.Context(), ph.config.RequestTimeout)
		defer cancel()

		r = r.WithContext(ctx)

		// Process request
		next.ServeHTTP(w, r)

		// Track completion
		duration := time.Since(start)
		ph.requestTracker.TrackCompletion(clientIP, duration)
	})
}

// applySecurityHeaders adds security headers to HTTP responses
func (ph *ProductionHardening) applySecurityHeaders(w http.ResponseWriter) {
	headers := w.Header()

	// Prevent MIME type sniffing
	headers.Set("X-Content-Type-Options", "nosniff")

	// Prevent clickjacking
	headers.Set("X-Frame-Options", "DENY")

	// XSS protection
	headers.Set("X-XSS-Protection", "1; mode=block")

	// Content Security Policy
	if ph.config.ContentSecurityPolicy != "" {
		headers.Set("Content-Security-Policy", ph.config.ContentSecurityPolicy)
	}

	// HSTS (HTTP Strict Transport Security)
	if ph.config.StrictTransportSecurity {
		headers.Set("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
	}

	// Referrer Policy
	headers.Set("Referrer-Policy", "strict-origin-when-cross-origin")

	// Permissions Policy (Feature Policy)
	headers.Set("Permissions-Policy", "camera=(), microphone=(), geolocation=()")
}

// validateRequest performs comprehensive input validation
func (ph *ProductionHardening) validateRequest(r *http.Request) (bool, string) {
	// Check URL for suspicious patterns
	if suspicious, pattern := ph.checkForSuspiciousPatterns(r.URL.Path); suspicious {
		ph.categorizeAttack("path_traversal")
		return true, pattern
	}

	// Check query parameters
	for key, values := range r.URL.Query() {
		for _, value := range values {
			if suspicious, pattern := ph.checkForSuspiciousPatterns(value); suspicious {
				ph.categorizeAttack("query_injection")
				return true, fmt.Sprintf("%s in query param %s", pattern, key)
			}
		}
	}

	// Check headers
	for key, values := range r.Header {
		for _, value := range values {
			if suspicious, pattern := ph.checkForSuspiciousPatterns(value); suspicious {
				ph.categorizeAttack("header_injection")
				return true, fmt.Sprintf("%s in header %s", pattern, key)
			}
		}
	}

	// Check User-Agent for known bad patterns
	userAgent := r.Header.Get("User-Agent")
	if ph.isSuspiciousUserAgent(userAgent) {
		return true, "suspicious_user_agent"
	}

	return false, ""
}

// checkForSuspiciousPatterns checks input against known attack patterns
func (ph *ProductionHardening) checkForSuspiciousPatterns(input string) (bool, string) {
	if len(input) > ph.config.MaxInputLength {
		return true, "input_too_long"
	}

	inputLower := strings.ToLower(input)

	for _, pattern := range ph.config.BlockedPatterns {
		if strings.Contains(inputLower, strings.ToLower(pattern)) {
			return true, pattern
		}
	}

	// Check for SQL injection patterns
	sqlPatterns := []string{
		"' or '1'='1", "' or 1=1", "union select", "drop table",
		"insert into", "delete from", "update set", "exec(",
		"xp_cmdshell", "sp_executesql",
	}

	for _, pattern := range sqlPatterns {
		if strings.Contains(inputLower, pattern) {
			atomic.AddInt64(&ph.metrics.SQLInjectionAttempts, 1)
			return true, "sql_injection:" + pattern
		}
	}

	// Check for XSS patterns
	xssPatterns := []string{
		"<script", "javascript:", "vbscript:", "onload=", "onerror=",
		"onmouseover=", "onclick=", "onfocus=", "alert(", "confirm(",
	}

	for _, pattern := range xssPatterns {
		if strings.Contains(inputLower, pattern) {
			atomic.AddInt64(&ph.metrics.XSSAttempts, 1)
			return true, "xss:" + pattern
		}
	}

	// Check for path traversal
	pathTraversalPatterns := []string{
		"../", "..\\", "....//", "....\\\\", "%2e%2e/", "%2e%2e\\",
		"..%2f", "..%5c", "%252e%252e/", "%252e%252e\\",
	}

	for _, pattern := range pathTraversalPatterns {
		if strings.Contains(inputLower, pattern) {
			atomic.AddInt64(&ph.metrics.PathTraversalAttempts, 1)
			return true, "path_traversal:" + pattern
		}
	}

	// Check for command injection
	cmdPatterns := []string{
		"; cat ", "; ls ", "; rm ", "; wget ", "; curl ",
		"| cat ", "| ls ", "| rm ", "| wget ", "| curl ",
		"&& cat ", "&& ls ", "&& rm ", "&& wget ", "&& curl ",
		"`cat ", "`ls ", "`rm ", "`wget ", "`curl ",
		"$(cat ", "$(ls ", "$(rm ", "$(wget ", "$(curl ",
	}

	for _, pattern := range cmdPatterns {
		if strings.Contains(inputLower, pattern) {
			atomic.AddInt64(&ph.metrics.CommandInjectionAttempts, 1)
			return true, "command_injection:" + pattern
		}
	}

	return false, ""
}

// isSuspiciousUserAgent checks for suspicious user agents
func (ph *ProductionHardening) isSuspiciousUserAgent(userAgent string) bool {
	if userAgent == "" {
		return true // Empty user agent is suspicious
	}

	suspicious := []string{
		"sqlmap", "nmap", "nikto", "burp", "scanner", "bot",
		"crawler", "spider", "scraper", "harvester", "extractor",
		"wget", "curl", "python-requests", "go-http-client",
	}

	userAgentLower := strings.ToLower(userAgent)
	for _, pattern := range suspicious {
		if strings.Contains(userAgentLower, pattern) {
			return true
		}
	}

	return false
}

// isRateLimited checks if a client IP is rate limited
func (ph *ProductionHardening) isRateLimited(clientIP string) bool {
	ph.rlMutex.Lock()
	defer ph.rlMutex.Unlock()

	// Get or create rate limiter for this IP
	limiter, exists := ph.rateLimiters[clientIP]
	if !exists {
		limiter = NewRateLimiter(ph.config.PerIPRateLimit, ph.config.RateLimitWindow)
		ph.rateLimiters[clientIP] = limiter

		// Clean up old rate limiters periodically
		if len(ph.rateLimiters) > 10000 { // Prevent memory leaks
			ph.cleanupRateLimiters()
		}
	}

	return !limiter.Allow(clientIP)
}

// cleanupRateLimiters removes inactive rate limiters
func (ph *ProductionHardening) cleanupRateLimiters() {
	now := common.ConsensusNow()
	for ip, limiter := range ph.rateLimiters {
		if now.Sub(limiter.LastAccess()) > time.Hour {
			delete(ph.rateLimiters, ip)
		}
	}
}

// getClientIP extracts the real client IP from the request
func (ph *ProductionHardening) getClientIP(r *http.Request) string {
	// Check X-Forwarded-For header (from proxies)
	xff := r.Header.Get("X-Forwarded-For")
	if xff != "" {
		// Take the first IP (client IP)
		ips := strings.Split(xff, ",")
		if len(ips) > 0 {
			ip := strings.TrimSpace(ips[0])
			if net.ParseIP(ip) != nil {
				return ip
			}
		}
	}

	// Check X-Real-IP header
	xri := r.Header.Get("X-Real-IP")
	if xri != "" && net.ParseIP(xri) != nil {
		return xri
	}

	// Fall back to RemoteAddr
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// logSecurityEvent logs security-related events
func (ph *ProductionHardening) logSecurityEvent(eventType, clientIP string, r *http.Request, extra ...interface{}) {
	fields := logrus.Fields{
		"event_type": eventType,
		"client_ip":  clientIP,
		"method":     r.Method,
		"path":       r.URL.Path,
		"user_agent": r.Header.Get("User-Agent"),
		"timestamp":  common.ConsensusNow().Unix(),
	}

	// Add extra fields
	for i := 0; i < len(extra); i += 2 {
		if i+1 < len(extra) {
			if key, ok := extra[i].(string); ok {
				fields[key] = extra[i+1]
			}
		}
	}

	ph.logger.WithFields(fields).Warn("Security event detected")
}

// categorizeAttack categorizes and counts different types of attacks
func (ph *ProductionHardening) categorizeAttack(attackType string) {
	switch attackType {
	case "sql_injection":
		atomic.AddInt64(&ph.metrics.SQLInjectionAttempts, 1)
	case "xss":
		atomic.AddInt64(&ph.metrics.XSSAttempts, 1)
	case "path_traversal":
		atomic.AddInt64(&ph.metrics.PathTraversalAttempts, 1)
	case "command_injection":
		atomic.AddInt64(&ph.metrics.CommandInjectionAttempts, 1)
	}
}

// initializeMonitors sets up security monitoring
func (ph *ProductionHardening) initializeMonitors() {
	ph.monitors = []SecurityCheckMonitor{
		NewAttackPatternMonitor(ph.logger),
		NewResourceMonitor(ph.logger),
		NewNetworkAnomalyMonitor(ph.logger),
	}
}

// runSecurityMonitoring runs the security monitoring loop
func (ph *ProductionHardening) runSecurityMonitoring() {
	ticker := time.NewTicker(ph.config.MonitoringInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			ph.performSecurityScan()
		case <-ph.ctx.Done():
			return
		}
	}
}

// performSecurityScan performs a comprehensive security scan
func (ph *ProductionHardening) performSecurityScan() {
	ph.metrics.LastSecurityScan = common.ConsensusNow()

	// Run all security monitors
	for _, monitor := range ph.monitors {
		if alerts := monitor.Check(); len(alerts) > 0 {
			for _, alert := range alerts {
				ph.handleSecurityAlert(alert)
			}
		}
	}

	// Check for threshold violations
	ph.checkAlertThresholds()
}

// handleSecurityAlert processes security alerts
func (ph *ProductionHardening) handleSecurityAlert(alert SecurityAlert) {
	ph.logger.WithFields(logrus.Fields{
		"alert_type": alert.Type,
		"severity":   alert.Severity,
		"message":    alert.Description,
		"details":    alert.Metadata,
	}).Warn("Security alert triggered")

	atomic.AddInt64(&ph.metrics.IncidentsTriggered, 1)

	// Forward to incident handler
	if ph.config.EnableIncidentResponse {
		ph.incidentHandler.HandleAlert(context.Background(), alert)
	}
}

// checkAlertThresholds checks if any metrics exceed alert thresholds
func (ph *ProductionHardening) checkAlertThresholds() {
	metrics := ph.GetMetrics()

	// Check error rate
	if threshold, exists := ph.config.AlertThresholds["error_rate"]; exists {
		if metrics.TotalRequests > 0 {
			errorRate := float64(metrics.BlockedRequests) / float64(metrics.TotalRequests)
			if errorRate > threshold {
				alert := SecurityAlert{
					Type:        "high_error_rate",
					Severity:    "high",
					Description: fmt.Sprintf("Error rate %.2f%% exceeds threshold %.2f%%", errorRate*100, threshold*100),
					Timestamp:   common.ConsensusNow(),
					Metadata:    map[string]interface{}{"error_rate": errorRate, "threshold": threshold},
				}
				ph.handleSecurityAlert(alert)
			}
		}
	}

	// Check blocked requests
	if threshold, exists := ph.config.AlertThresholds["blocked_requests"]; exists {
		if float64(metrics.BlockedRequests) > threshold {
			alert := SecurityAlert{
				Type:        "high_blocked_requests",
				Severity:    "medium",
				Description: fmt.Sprintf("Blocked requests %d exceeds threshold %.0f", metrics.BlockedRequests, threshold),
				Timestamp:   common.ConsensusNow(),
				Metadata:    map[string]interface{}{"blocked_requests": metrics.BlockedRequests, "threshold": threshold},
			}
			ph.handleSecurityAlert(alert)
		}
	}
}

// GetMetrics returns current security metrics
func (ph *ProductionHardening) GetMetrics() SecurityMetrics {
	return SecurityMetrics{
		TotalRequests:            atomic.LoadInt64(&ph.metrics.TotalRequests),
		BlockedRequests:          atomic.LoadInt64(&ph.metrics.BlockedRequests),
		RateLimitedRequests:      atomic.LoadInt64(&ph.metrics.RateLimitedRequests),
		SuspiciousRequests:       atomic.LoadInt64(&ph.metrics.SuspiciousRequests),
		IncidentsTriggered:       atomic.LoadInt64(&ph.metrics.IncidentsTriggered),
		LastSecurityScan:         ph.metrics.LastSecurityScan,
		SQLInjectionAttempts:     atomic.LoadInt64(&ph.metrics.SQLInjectionAttempts),
		XSSAttempts:              atomic.LoadInt64(&ph.metrics.XSSAttempts),
		PathTraversalAttempts:    atomic.LoadInt64(&ph.metrics.PathTraversalAttempts),
		CommandInjectionAttempts: atomic.LoadInt64(&ph.metrics.CommandInjectionAttempts),
	}
}

// GenerateSecurityToken generates a cryptographically secure token
func (ph *ProductionHardening) GenerateSecurityToken(length int) (string, error) {
	bytes := make([]byte, length)
	if _, err := rand.Read(bytes); err != nil {
		return "", common.CryptoError(err, "failed to generate security token")
	}
	return hex.EncodeToString(bytes), nil
}

// SecureCompare performs a constant-time comparison of two strings
func (ph *ProductionHardening) SecureCompare(a, b string) bool {
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

// GetSystemInfo returns security-relevant system information
func (ph *ProductionHardening) GetSystemInfo() map[string]interface{} {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	return map[string]interface{}{
		"go_version":       runtime.Version(),
		"num_goroutines":   runtime.NumGoroutine(),
		"memory_alloc":     m.Alloc,
		"memory_sys":       m.Sys,
		"gc_count":         m.NumGC,
		"running_since":    common.ConsensusNow().Sub(time.Unix(0, int64(m.LastGC))),
		"rate_limiters":    len(ph.rateLimiters),
		"security_running": atomic.LoadInt32(&ph.running) == 1,
	}
}
