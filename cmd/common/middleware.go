// Package common provides shared utilities for cmd services
package common

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"diamante/consensus"
	"github.com/gorilla/mux"
)

// RateLimiter implements rate limiting middleware using a simple token bucket
type RateLimiter struct {
	tokens     int
	maxTokens  int
	refillRate int
	lastRefill time.Time
	mutex      sync.Mutex
}

// NewRateLimiter creates a new rate limiter
func NewRateLimiter(limit int, burst int, window time.Duration) *RateLimiter {
	return &RateLimiter{
		tokens:     burst,
		maxTokens:  burst,
		refillRate: limit,
		lastRefill: consensus.ConsensusNow(),
	}
}

// Middleware returns rate limiting middleware
func (rl *RateLimiter) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !rl.allow() {
			http.Error(w, "Rate limit exceeded", http.StatusTooManyRequests)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// allow checks if the request is allowed based on rate limiting
func (rl *RateLimiter) allow() bool {
	rl.mutex.Lock()
	defer rl.mutex.Unlock()

	now := consensus.ConsensusNow()
	elapsed := now.Sub(rl.lastRefill)

	// Refill tokens based on elapsed time
	tokensToAdd := int(elapsed.Seconds()) * rl.refillRate / 60 // refill per minute
	if tokensToAdd > 0 {
		rl.tokens += tokensToAdd
		if rl.tokens > rl.maxTokens {
			rl.tokens = rl.maxTokens
		}
		rl.lastRefill = now
	}

	if rl.tokens > 0 {
		rl.tokens--
		return true
	}

	return false
}

// CORSConfig holds CORS configuration
type CORSConfig struct {
	AllowedOrigins []string
	AllowedMethods []string
	AllowedHeaders []string
}

// NewCORSMiddleware creates CORS middleware with configuration
func NewCORSMiddleware(config CORSConfig) func(http.Handler) http.Handler {
	// Default to restrictive settings if not configured
	if len(config.AllowedOrigins) == 0 {
		// Get from environment or use secure defaults
		allowedOrigins := os.Getenv("CORS_ALLOWED_ORIGINS")
		if allowedOrigins != "" {
			config.AllowedOrigins = strings.Split(allowedOrigins, ",")
		} else {
			// Secure production defaults - no localhost
			config.AllowedOrigins = []string{"https://diamante.io", "https://app.diamante.io"}
		}
	}
	if len(config.AllowedMethods) == 0 {
		config.AllowedMethods = []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"}
	}
	if len(config.AllowedHeaders) == 0 {
		config.AllowedHeaders = []string{"Content-Type", "Authorization"}
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			origin := r.Header.Get("Origin")

			// Check if origin is allowed
			originAllowed := false
			for _, allowed := range config.AllowedOrigins {
				if allowed == "*" || allowed == origin {
					originAllowed = true
					break
				}
			}

			if originAllowed {
				w.Header().Set("Access-Control-Allow-Origin", origin)
				w.Header().Set("Access-Control-Allow-Methods", strings.Join(config.AllowedMethods, ", "))
				w.Header().Set("Access-Control-Allow-Headers", strings.Join(config.AllowedHeaders, ", "))
				w.Header().Set("Access-Control-Max-Age", "3600")
			}

			if r.Method == "OPTIONS" {
				w.WriteHeader(http.StatusOK)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

// CORSMiddleware adds CORS headers (deprecated - use NewCORSMiddleware)
func CORSMiddleware(next http.Handler) http.Handler {
	// For backward compatibility, use restrictive defaults
	// Get allowed origins from environment
	allowedOrigins := os.Getenv("CORS_ALLOWED_ORIGINS")
	origins := []string{"https://diamante.io", "https://app.diamante.io"}
	if allowedOrigins != "" {
		origins = strings.Split(allowedOrigins, ",")
	}

	config := CORSConfig{
		AllowedOrigins: origins,
		AllowedMethods: []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowedHeaders: []string{"Content-Type", "Authorization"},
	}
	return NewCORSMiddleware(config)(next)
}

// LoggingMiddleware logs HTTP requests
func LoggingMiddleware(logger Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := consensus.ConsensusNow()

			// Wrap response writer to capture status code
			wrappedWriter := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}

			next.ServeHTTP(wrappedWriter, r)

			logger.Info("HTTP request",
				"method", r.Method,
				"path", r.URL.Path,
				"status", wrappedWriter.statusCode,
				"duration", consensus.ConsensusSince(start).String(),
				"remote_addr", r.RemoteAddr,
				"user_agent", r.UserAgent(),
			)
		})
	}
}

// responseWriter wraps http.ResponseWriter to capture status code
type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

// WriteHeader captures the status code
func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

// ValidationMiddleware provides request validation
func ValidationMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Set max request size
		r.Body = http.MaxBytesReader(w, r.Body, 1<<20) // 1MB

		next.ServeHTTP(w, r)
	})
}

// ValidateAddress validates blockchain address format
func ValidateAddress(address string) error {
	if address == "" {
		return fmt.Errorf("address cannot be empty")
	}

	// Basic validation - adjust regex based on your address format
	matched, err := regexp.MatchString("^[a-fA-F0-9]{40}$", address)
	if err != nil {
		return fmt.Errorf("failed to validate address format: %w", err)
	}

	if !matched {
		return fmt.Errorf("invalid address format")
	}

	return nil
}

// ValidateBlockRange validates block range parameters
func ValidateBlockRange(startStr, endStr string) (uint64, uint64, error) {
	if startStr == "" || endStr == "" {
		return 0, 0, fmt.Errorf("start and end parameters are required")
	}

	start, err := strconv.ParseUint(startStr, 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid start parameter: %w", err)
	}

	end, err := strconv.ParseUint(endStr, 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid end parameter: %w", err)
	}

	if start > end {
		return 0, 0, fmt.Errorf("start cannot be greater than end")
	}

	if end-start > 1000 {
		return 0, 0, fmt.Errorf("range cannot exceed 1000 blocks")
	}

	return start, end, nil
}

// ValidateTransactionID validates transaction ID format
func ValidateTransactionID(txID string) error {
	if txID == "" {
		return fmt.Errorf("transaction ID cannot be empty")
	}

	// Basic validation - adjust based on your transaction ID format
	matched, err := regexp.MatchString("^[a-fA-F0-9]{64}$", txID)
	if err != nil {
		return fmt.Errorf("failed to validate transaction ID format: %w", err)
	}

	if !matched {
		return fmt.Errorf("invalid transaction ID format")
	}

	return nil
}

// GracefulShutdown handles graceful server shutdown
func GracefulShutdown(server *http.Server, logger Logger, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	logger.Info("Starting graceful shutdown")

	if err := server.Shutdown(ctx); err != nil {
		logger.Error("Server shutdown failed", "error", err)
		return
	}

	logger.Info("Server shutdown completed")
}

// SetupRouter creates a new mux router with common middleware
func SetupRouter(rateLimiter *RateLimiter, logger Logger, enableCORS bool) *mux.Router {
	router := mux.NewRouter()

	// Add validation middleware
	router.Use(ValidationMiddleware)

	// Add rate limiting
	if rateLimiter != nil {
		router.Use(rateLimiter.Middleware)
	}

	// Add CORS if enabled
	if enableCORS {
		router.Use(CORSMiddleware)
	}

	// Add logging middleware
	router.Use(LoggingMiddleware(logger))

	return router
}

// HealthCheck returns a basic health check handler
func HealthCheck() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"healthy","timestamp":"` + consensus.ConsensusNow().UTC().Format(time.RFC3339) + `"}`))
	}
}

// ReadinessCheck returns a readiness check handler
func ReadinessCheck(dependencies []HealthChecker) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		for _, dep := range dependencies {
			if err := dep.HealthCheck(); err != nil {
				w.WriteHeader(http.StatusServiceUnavailable)
				w.Write([]byte(`{"status":"not_ready","error":"` + err.Error() + `"}`))
				return
			}
		}

		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ready","timestamp":"` + consensus.ConsensusNow().UTC().Format(time.RFC3339) + `"}`))
	}
}

// HealthChecker interface for dependency health checks
type HealthChecker interface {
	HealthCheck() error
}

// SanitizeString removes potentially dangerous characters from user input
func SanitizeString(input string) string {
	// Remove null bytes and control characters
	input = strings.ReplaceAll(input, "\x00", "")
	input = regexp.MustCompile(`[\x00-\x1f\x7f]`).ReplaceAllString(input, "")

	// Trim whitespace
	input = strings.TrimSpace(input)

	return input
}
