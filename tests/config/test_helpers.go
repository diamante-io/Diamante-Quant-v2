package config

import (
	"diamante/config"
	"os"
	"strconv"
)

// ExplorerConfig for testing
type ExplorerConfig struct {
	Port        int
	Host        string
	DatabaseURL string
}

// FaucetConfig for testing
type FaucetConfig struct {
	Port           int
	Host           string
	DatabaseURL    string
	TokensPerClaim int
}

// LoadExplorerConfig loads explorer configuration for testing
func LoadExplorerConfig() (*ExplorerConfig, error) {
	return &ExplorerConfig{
		Port:        getEnvInt("EXPLORER_PORT", 8080),
		Host:        getEnvString("EXPLORER_HOST", "localhost"),
		DatabaseURL: getEnvString("EXPLORER_DB_URL", "mongodb://localhost:27017"),
	}, nil
}

// LoadFaucetConfig loads faucet configuration for testing
func LoadFaucetConfig() (*FaucetConfig, error) {
	return &FaucetConfig{
		Port:           getEnvInt("FAUCET_PORT", 8081),
		Host:           getEnvString("FAUCET_HOST", "localhost"),
		DatabaseURL:    getEnvString("FAUCET_DB_URL", "mongodb://localhost:27017"),
		TokensPerClaim: getEnvInt("FAUCET_TOKENS_PER_CLAIM", 100),
	}, nil
}

// getEnvString gets a string environment variable with a default
func getEnvString(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// getEnvInt gets an integer environment variable with a default
func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}
	return defaultValue
}

// getEnvBool gets a boolean environment variable with a default
func getEnvBool(key string, defaultValue bool) bool {
	if value := os.Getenv(key); value != "" {
		if boolValue, err := strconv.ParseBool(value); err == nil {
			return boolValue
		}
	}
	return defaultValue
}

// Expose config types for testing
type (
	NetworkConfig  = config.NetworkConfig
	DatabaseConfig = config.DatabaseConfig
	CryptoConfig   = config.CryptoConfig
	CacheConfig    = config.CacheConfig
)
