// tests/config/test_config.go
package config

import (
	"os"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
)

// TestConfig holds configuration for test environments
type TestConfig struct {
	// Database settings
	MongoURI   string
	TestDBName string
	CleanupDB  bool

	// Network settings
	TestPort   int
	TLSEnabled bool

	// Consensus settings
	FastConsensus bool
	TestMode      bool

	// Logging
	LogLevel  logrus.Level
	LogToFile bool

	// Performance testing
	StressTestDuration time.Duration
	MaxConcurrency     int

	// Integration testing
	EnableIntegration bool
	ExternalServices  map[string]string
}

// NewTestConfig creates a default test configuration
func NewTestConfig() *TestConfig {
	return &TestConfig{
		MongoURI:           getEnvOrDefault("TEST_MONGO_URI", "mongodb://127.0.0.1:27017"),
		TestDBName:         getEnvOrDefault("TEST_DB_NAME", "diamante_test"),
		CleanupDB:          getEnvBoolOrDefault("TEST_CLEANUP_DB", true),
		TestPort:           8080,
		TLSEnabled:         false,
		FastConsensus:      true,
		TestMode:           true,
		LogLevel:           logrus.WarnLevel, // Reduce log noise in tests
		LogToFile:          false,
		StressTestDuration: 30 * time.Second,
		MaxConcurrency:     100,
		EnableIntegration:  getEnvBoolOrDefault("ENABLE_INTEGRATION_TESTS", false),
		ExternalServices:   make(map[string]string),
	}
}

// SetupIntegrationTest prepares the environment for integration testing
func (tc *TestConfig) SetupIntegrationTest(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	if !tc.EnableIntegration {
		t.Skip("Integration tests disabled")
	}
}

// SetupPerformanceTest prepares the environment for performance testing
func (tc *TestConfig) SetupPerformanceTest(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}
}

// CleanupTest performs cleanup after test execution
func (tc *TestConfig) CleanupTest(t *testing.T) {
	if tc.CleanupDB {
		// Database cleanup would go here
		t.Log("Cleaning up test database")
	}
}

// Helper functions
func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvBoolOrDefault(key string, defaultValue bool) bool {
	if value := os.Getenv(key); value != "" {
		return value == "true" || value == "1"
	}
	return defaultValue
}

// Test categories for organizing test execution
const (
	CategoryUnit        = "unit"
	CategoryIntegration = "integration"
	CategoryPerformance = "performance"
	CategorySecurity    = "security"
	CategoryStress      = "stress"
)

// TestCategories defines which test categories to run
type TestCategories struct {
	Unit        bool
	Integration bool
	Performance bool
	Security    bool
	Stress      bool
}

// DefaultTestCategories returns the default set of test categories
func DefaultTestCategories() *TestCategories {
	return &TestCategories{
		Unit:        true,
		Integration: getEnvBoolOrDefault("ENABLE_INTEGRATION_TESTS", false),
		Performance: getEnvBoolOrDefault("ENABLE_PERFORMANCE_TESTS", false),
		Security:    getEnvBoolOrDefault("ENABLE_SECURITY_TESTS", false),
		Stress:      getEnvBoolOrDefault("ENABLE_STRESS_TESTS", false),
	}
}

// ShouldRunCategory checks if a test category should be executed
func (tc *TestCategories) ShouldRunCategory(category string) bool {
	switch category {
	case CategoryUnit:
		return tc.Unit
	case CategoryIntegration:
		return tc.Integration
	case CategoryPerformance:
		return tc.Performance
	case CategorySecurity:
		return tc.Security
	case CategoryStress:
		return tc.Stress
	default:
		return false
	}
}
