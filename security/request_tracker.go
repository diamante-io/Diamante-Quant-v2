package security

import (
	"context"
	"diamante/common"
	"fmt"
	"strings"
	"sync"
	"time"
)

// RequestTracker tracks and analyzes request patterns for security monitoring
type RequestTracker struct {
	mu            sync.RWMutex
	requests      map[string]*RequestInfo
	patterns      map[string]*PatternAnalysis
	config        *TrackerConfig
	alertChan     chan SecurityAlert
	cleanupTicker *time.Ticker
}

// RequestInfo contains information about a request
type RequestInfo struct {
	ID        string
	Source    string
	Timestamp time.Time
	Path      string
	Method    string
	Headers   map[string]string
	Body      []byte
	UserAgent string
	IP        string
}

// PatternAnalysis contains analysis of request patterns
type PatternAnalysis struct {
	RequestCount    int
	LastSeen        time.Time
	FirstSeen       time.Time
	SuspiciousScore int
	Patterns        []string
}

// TrackerConfig contains configuration for request tracking
type TrackerConfig struct {
	MaxRequests         int
	TimeWindow          time.Duration
	SuspiciousThreshold int
	CleanupInterval     time.Duration
}

// SecurityAlert represents a security alert
type SecurityAlert struct {
	Type        string
	Severity    string
	Source      string
	Description string
	Timestamp   time.Time
	Metadata    map[string]interface{}
}

// NewRequestTracker creates a new request tracker
func NewRequestTracker(config *TrackerConfig) *RequestTracker {
	if config == nil {
		config = &TrackerConfig{
			MaxRequests:         1000,
			TimeWindow:          time.Hour,
			SuspiciousThreshold: 10,
			CleanupInterval:     time.Minute * 30,
		}
	}

	rt := &RequestTracker{
		requests:      make(map[string]*RequestInfo),
		patterns:      make(map[string]*PatternAnalysis),
		config:        config,
		alertChan:     make(chan SecurityAlert, 100),
		cleanupTicker: time.NewTicker(config.CleanupInterval),
	}

	// Start cleanup goroutine
	go rt.cleanup()

	return rt
}

// Start starts the request tracker
func (rt *RequestTracker) Start() error {
	// Already started in constructor
	return nil
}

// TrackRequest tracks a new request
func (rt *RequestTracker) TrackRequest(ctx context.Context, req *RequestInfo) error {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	// Store request info
	rt.requests[req.ID] = req

	// Analyze patterns
	rt.analyzePattern(req)

	return nil
}

// analyzePattern analyzes request patterns for suspicious activity
func (rt *RequestTracker) analyzePattern(req *RequestInfo) {
	key := req.Source + ":" + req.IP

	pattern, exists := rt.patterns[key]
	if !exists {
		pattern = &PatternAnalysis{
			FirstSeen: req.Timestamp,
			Patterns:  make([]string, 0),
		}
		rt.patterns[key] = pattern
	}

	pattern.RequestCount++
	pattern.LastSeen = req.Timestamp

	// Check for suspicious patterns
	if pattern.RequestCount > rt.config.MaxRequests {
		pattern.SuspiciousScore += 5
		rt.generateAlert("HIGH_FREQUENCY", "medium", req.Source,
			"High frequency requests detected", req.Timestamp,
			map[string]interface{}{
				"request_count": pattern.RequestCount,
				"source":        req.Source,
				"ip":            req.IP,
			})
	}

	// Check for SQL injection patterns
	if rt.containsSQLInjection(req) {
		pattern.SuspiciousScore += 10
		rt.generateAlert("SQL_INJECTION", "high", req.Source,
			"Potential SQL injection attempt detected", req.Timestamp,
			map[string]interface{}{
				"path":   req.Path,
				"method": req.Method,
				"ip":     req.IP,
			})
	}

	// Check for XSS patterns
	if rt.containsXSS(req) {
		pattern.SuspiciousScore += 8
		rt.generateAlert("XSS_ATTEMPT", "high", req.Source,
			"Potential XSS attempt detected", req.Timestamp,
			map[string]interface{}{
				"path":   req.Path,
				"method": req.Method,
				"ip":     req.IP,
			})
	}
}

// containsSQLInjection checks for SQL injection patterns
func (rt *RequestTracker) containsSQLInjection(req *RequestInfo) bool {
	sqlPatterns := []string{
		"' OR '1'='1",
		"' OR 1=1",
		"UNION SELECT",
		"DROP TABLE",
		"INSERT INTO",
		"DELETE FROM",
		"UPDATE SET",
		"'; DROP",
	}

	content := req.Path + string(req.Body)
	for _, pattern := range sqlPatterns {
		if containsIgnoreCase(content, pattern) {
			return true
		}
	}
	return false
}

// containsXSS checks for XSS patterns
func (rt *RequestTracker) containsXSS(req *RequestInfo) bool {
	xssPatterns := []string{
		"<script>",
		"javascript:",
		"onload=",
		"onerror=",
		"onclick=",
		"<iframe",
		"eval(",
		"document.cookie",
	}

	content := req.Path + string(req.Body)
	for _, pattern := range xssPatterns {
		if containsIgnoreCase(content, pattern) {
			return true
		}
	}
	return false
}

// generateAlert generates a security alert
func (rt *RequestTracker) generateAlert(alertType, severity, source, description string, timestamp time.Time, metadata map[string]interface{}) {
	alert := SecurityAlert{
		Type:        alertType,
		Severity:    severity,
		Source:      source,
		Description: description,
		Timestamp:   timestamp,
		Metadata:    metadata,
	}

	select {
	case rt.alertChan <- alert:
	default:
		// Channel is full, log the issue
	}
}

// GetAlerts returns the alert channel
func (rt *RequestTracker) GetAlerts() <-chan SecurityAlert {
	return rt.alertChan
}

// GetStats returns current tracker statistics
func (rt *RequestTracker) GetStats() map[string]interface{} {
	rt.mu.RLock()
	defer rt.mu.RUnlock()

	totalSuspicious := 0
	for _, pattern := range rt.patterns {
		if pattern.SuspiciousScore >= rt.config.SuspiciousThreshold {
			totalSuspicious++
		}
	}

	return map[string]interface{}{
		"total_requests":     len(rt.requests),
		"tracked_patterns":   len(rt.patterns),
		"suspicious_sources": totalSuspicious,
		"alerts_pending":     len(rt.alertChan),
	}
}

// cleanup removes old requests and patterns
func (rt *RequestTracker) cleanup() {
	defer rt.cleanupTicker.Stop()

	for range rt.cleanupTicker.C {
		rt.mu.Lock()
		now := common.ConsensusNow()

		// Clean up old requests
		for id, req := range rt.requests {
			if now.Sub(req.Timestamp) > rt.config.TimeWindow {
				delete(rt.requests, id)
			}
		}

		// Clean up old patterns
		for key, pattern := range rt.patterns {
			if now.Sub(pattern.LastSeen) > rt.config.TimeWindow {
				delete(rt.patterns, key)
			}
		}

		rt.mu.Unlock()
	}
}

// TrackCompletion tracks request completion
func (rt *RequestTracker) TrackCompletion(clientIP string, duration time.Duration) {
	// This can be used for performance monitoring
	// For now, we'll just log slow requests
	if duration > time.Second*5 {
		rt.generateAlert("SLOW_REQUEST", "low", clientIP,
			fmt.Sprintf("Slow request detected: %v", duration), common.ConsensusNow(),
			map[string]interface{}{
				"duration": duration.String(),
			})
	}
}

// Stop stops the request tracker
func (rt *RequestTracker) Stop() {
	rt.cleanupTicker.Stop()
	close(rt.alertChan)
}

// containsIgnoreCase checks if a string contains a substring (case-insensitive)
func containsIgnoreCase(s, substr string) bool {
	return strings.Contains(strings.ToLower(s), strings.ToLower(substr))
}
