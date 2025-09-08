// Package config provides configuration management for cmd services
package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// ExplorerConfig holds configuration for the explorer service
type ExplorerConfig struct {
	// Server settings
	Port    int           `json:"port"`
	Host    string        `json:"host"`
	Timeout time.Duration `json:"timeout"`

	// Database settings
	MongoURL     string        `json:"mongo_url"`
	DatabaseName string        `json:"database_name"`
	MaxPoolSize  int           `json:"max_pool_size"`
	ConnTimeout  time.Duration `json:"conn_timeout"`

	// Rate limiting
	RateLimit  int           `json:"rate_limit"`
	RateBurst  int           `json:"rate_burst"`
	RateWindow time.Duration `json:"rate_window"`

	// Security
	EnableHTTPS bool   `json:"enable_https"`
	TLSCertFile string `json:"tls_cert_file"`
	TLSKeyFile  string `json:"tls_key_file"`
	EnableCORS  bool   `json:"enable_cors"`

	// Logging
	LogLevel  string `json:"log_level"`
	LogFormat string `json:"log_format"`

	// Metrics
	EnableMetrics bool `json:"enable_metrics"`
	MetricsPort   int  `json:"metrics_port"`
}

// FaucetConfig holds configuration for the faucet service
type FaucetConfig struct {
	// Server settings
	Port    int           `json:"port"`
	Host    string        `json:"host"`
	Timeout time.Duration `json:"timeout"`

	// Database settings
	MongoURL     string        `json:"mongo_url"`
	DatabaseName string        `json:"database_name"`
	MaxPoolSize  int           `json:"max_pool_size"`
	ConnTimeout  time.Duration `json:"conn_timeout"`

	// Faucet settings
	FundAmount   int64         `json:"fund_amount"`
	MaxFunds     int64         `json:"max_funds"`
	CooldownTime time.Duration `json:"cooldown_time"`

	// Rate limiting
	RateLimit  int           `json:"rate_limit"`
	RateBurst  int           `json:"rate_burst"`
	RateWindow time.Duration `json:"rate_window"`

	// Security
	EnableHTTPS bool   `json:"enable_https"`
	TLSCertFile string `json:"tls_cert_file"`
	TLSKeyFile  string `json:"tls_key_file"`
	EnableCORS  bool   `json:"enable_cors"`

	// Logging
	LogLevel  string `json:"log_level"`
	LogFormat string `json:"log_format"`

	// Metrics
	EnableMetrics bool `json:"enable_metrics"`
	MetricsPort   int  `json:"metrics_port"`
}

// LoadExplorerConfig loads configuration for explorer service from environment variables
func LoadExplorerConfig() (*ExplorerConfig, error) {
	config := &ExplorerConfig{
		// Default values
		Port:          getEnvInt("EXPLORER_PORT", 8091),
		Host:          getEnvString("EXPLORER_HOST", "0.0.0.0"),
		Timeout:       getEnvDuration("EXPLORER_TIMEOUT", 30*time.Second),
		MongoURL:      getEnvString("MONGO_URL", "mongodb://127.0.0.1:27017"),
		DatabaseName:  getEnvString("DATABASE_NAME", "diamante"),
		MaxPoolSize:   getEnvInt("MONGO_MAX_POOL_SIZE", 100),
		ConnTimeout:   getEnvDuration("MONGO_CONN_TIMEOUT", 10*time.Second),
		RateLimit:     getEnvInt("EXPLORER_RATE_LIMIT", 100),
		RateBurst:     getEnvInt("EXPLORER_RATE_BURST", 10),
		RateWindow:    getEnvDuration("EXPLORER_RATE_WINDOW", time.Minute),
		EnableHTTPS:   getEnvBool("EXPLORER_ENABLE_HTTPS", false),
		TLSCertFile:   getEnvString("EXPLORER_TLS_CERT_FILE", ""),
		TLSKeyFile:    getEnvString("EXPLORER_TLS_KEY_FILE", ""),
		EnableCORS:    getEnvBool("EXPLORER_ENABLE_CORS", true),
		LogLevel:      getEnvString("LOG_LEVEL", "info"),
		LogFormat:     getEnvString("LOG_FORMAT", "json"),
		EnableMetrics: getEnvBool("EXPLORER_ENABLE_METRICS", true),
		MetricsPort:   getEnvInt("EXPLORER_METRICS_PORT", 9091),
	}

	if err := config.validate(); err != nil {
		return nil, fmt.Errorf("invalid explorer configuration: %w", err)
	}

	return config, nil
}

// LoadFaucetConfig loads configuration for faucet service from environment variables
func LoadFaucetConfig() (*FaucetConfig, error) {
	config := &FaucetConfig{
		// Default values
		Port:          getEnvInt("FAUCET_PORT", 8090),
		Host:          getEnvString("FAUCET_HOST", "0.0.0.0"),
		Timeout:       getEnvDuration("FAUCET_TIMEOUT", 30*time.Second),
		MongoURL:      getEnvString("MONGO_URL", "mongodb://127.0.0.1:27017"),
		DatabaseName:  getEnvString("DATABASE_NAME", "diamante"),
		MaxPoolSize:   getEnvInt("MONGO_MAX_POOL_SIZE", 100),
		ConnTimeout:   getEnvDuration("MONGO_CONN_TIMEOUT", 10*time.Second),
		FundAmount:    getEnvInt64("FAUCET_FUND_AMOUNT", 10),
		MaxFunds:      getEnvInt64("FAUCET_MAX_FUNDS", 1000),
		CooldownTime:  getEnvDuration("FAUCET_COOLDOWN_TIME", 24*time.Hour),
		RateLimit:     getEnvInt("FAUCET_RATE_LIMIT", 10),
		RateBurst:     getEnvInt("FAUCET_RATE_BURST", 5),
		RateWindow:    getEnvDuration("FAUCET_RATE_WINDOW", time.Minute),
		EnableHTTPS:   getEnvBool("FAUCET_ENABLE_HTTPS", false),
		TLSCertFile:   getEnvString("FAUCET_TLS_CERT_FILE", ""),
		TLSKeyFile:    getEnvString("FAUCET_TLS_KEY_FILE", ""),
		EnableCORS:    getEnvBool("FAUCET_ENABLE_CORS", true),
		LogLevel:      getEnvString("LOG_LEVEL", "info"),
		LogFormat:     getEnvString("LOG_FORMAT", "json"),
		EnableMetrics: getEnvBool("FAUCET_ENABLE_METRICS", true),
		MetricsPort:   getEnvInt("FAUCET_METRICS_PORT", 9090),
	}

	if err := config.validate(); err != nil {
		return nil, fmt.Errorf("invalid faucet configuration: %w", err)
	}

	return config, nil
}

// validate validates the explorer configuration
func (c *ExplorerConfig) validate() error {
	if c.Port <= 0 || c.Port > 65535 {
		return fmt.Errorf("invalid port: %d", c.Port)
	}

	if c.Host == "" {
		return fmt.Errorf("host cannot be empty")
	}

	if c.MongoURL == "" {
		return fmt.Errorf("mongo URL cannot be empty")
	}

	if c.DatabaseName == "" {
		return fmt.Errorf("database name cannot be empty")
	}

	if c.MaxPoolSize <= 0 {
		return fmt.Errorf("max pool size must be positive")
	}

	if c.RateLimit <= 0 {
		return fmt.Errorf("rate limit must be positive")
	}

	if c.RateBurst <= 0 {
		return fmt.Errorf("rate burst must be positive")
	}

	if c.EnableHTTPS && (c.TLSCertFile == "" || c.TLSKeyFile == "") {
		return fmt.Errorf("TLS cert and key files required when HTTPS is enabled")
	}

	if c.MetricsPort <= 0 || c.MetricsPort > 65535 {
		return fmt.Errorf("invalid metrics port: %d", c.MetricsPort)
	}

	return nil
}

// validate validates the faucet configuration
func (c *FaucetConfig) validate() error {
	if c.Port <= 0 || c.Port > 65535 {
		return fmt.Errorf("invalid port: %d", c.Port)
	}

	if c.Host == "" {
		return fmt.Errorf("host cannot be empty")
	}

	if c.MongoURL == "" {
		return fmt.Errorf("mongo URL cannot be empty")
	}

	if c.DatabaseName == "" {
		return fmt.Errorf("database name cannot be empty")
	}

	if c.MaxPoolSize <= 0 {
		return fmt.Errorf("max pool size must be positive")
	}

	if c.FundAmount <= 0 {
		return fmt.Errorf("fund amount must be positive")
	}

	if c.MaxFunds <= 0 {
		return fmt.Errorf("max funds must be positive")
	}

	if c.RateLimit <= 0 {
		return fmt.Errorf("rate limit must be positive")
	}

	if c.RateBurst <= 0 {
		return fmt.Errorf("rate burst must be positive")
	}

	if c.EnableHTTPS && (c.TLSCertFile == "" || c.TLSKeyFile == "") {
		return fmt.Errorf("TLS cert and key files required when HTTPS is enabled")
	}

	if c.MetricsPort <= 0 || c.MetricsPort > 65535 {
		return fmt.Errorf("invalid metrics port: %d", c.MetricsPort)
	}

	return nil
}

// Helper functions for environment variable parsing
func getEnvString(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}
	return defaultValue
}

func getEnvInt64(key string, defaultValue int64) int64 {
	if value := os.Getenv(key); value != "" {
		if intValue, err := strconv.ParseInt(value, 10, 64); err == nil {
			return intValue
		}
	}
	return defaultValue
}

func getEnvBool(key string, defaultValue bool) bool {
	if value := os.Getenv(key); value != "" {
		if boolValue, err := strconv.ParseBool(value); err == nil {
			return boolValue
		}
	}
	return defaultValue
}

func getEnvDuration(key string, defaultValue time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		if duration, err := time.ParseDuration(value); err == nil {
			return duration
		}
	}
	return defaultValue
}
