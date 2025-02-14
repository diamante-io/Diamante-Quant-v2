//config.go

package config

import (
	"fmt"
	"log"
	"path/filepath"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/go-playground/validator/v10"
	"github.com/mitchellh/mapstructure"
	"github.com/spf13/viper"
)

type CryptoConfig struct {
	// Existing fields
	DefaultAlgorithm     string        `mapstructure:"default_algorithm" validate:"oneof=Dilithium Kyber"`
	KeyExpirationTime    time.Duration `mapstructure:"key_expiration_time" validate:"required"`
	KeyExchangeAlgorithm string        `mapstructure:"key_exchange_algorithm" validate:"oneof=Kyber512 Kyber768 Kyber1024"`
	SignatureAlgorithm   string        `mapstructure:"signature_algorithm" validate:"oneof=Dilithium2 Dilithium3 Dilithium5"`

	// New fields
	KyberSecurityLevel     int           `mapstructure:"kyber_security_level" validate:"oneof=512 768 1024"`
	DilithiumSecurityLevel int           `mapstructure:"dilithium_security_level" validate:"oneof=2 3 5"`
	EnableKeyRotation      bool          `mapstructure:"enable_key_rotation"`
	KeyRotationInterval    time.Duration `mapstructure:"key_rotation_interval"`
	MinKeyGenerationTime   time.Duration `mapstructure:"min_key_generation_time"`
	MaxKeyGenerationRetry  int           `mapstructure:"max_key_generation_retry" validate:"min=1,max=10"`
}

type ConsensusConfig struct {
	GossipDelay       time.Duration
	DPoSSetSize       int
	DPoSEpochDuration uint64
	PoHTickDelay      time.Duration
	VotingDuration    time.Duration
	CryptoConfig      CryptoConfig
}

func NewDefaultCryptoConfig() *CryptoConfig {
	return &CryptoConfig{
		// Existing defaults
		DefaultAlgorithm:     "Dilithium",
		KeyExpirationTime:    24 * time.Hour,
		KeyExchangeAlgorithm: "Kyber1024",
		SignatureAlgorithm:   "Dilithium3",

		// New defaults
		KyberSecurityLevel:     1024,
		DilithiumSecurityLevel: 3,
		EnableKeyRotation:      true,
		KeyRotationInterval:    6 * time.Hour,
		MinKeyGenerationTime:   100 * time.Millisecond,
		MaxKeyGenerationRetry:  3,
	}
}

// Environment represents different deployment environments
type Environment string

const (
	Development Environment = "development"
	Staging     Environment = "staging"
	Production  Environment = "production"
)

// DatabaseConfig holds database-specific configuration
type DatabaseConfig struct {
	// CouchDB Configuration
	CouchDB struct {
		URL            string        `mapstructure:"url" validate:"required,url"`
		Database       string        `mapstructure:"database" validate:"required"`
		Username       string        `mapstructure:"username"`
		Password       string        `mapstructure:"password"`
		MaxConnections int           `mapstructure:"max_connections" validate:"required,min=1"`
		Timeout        time.Duration `mapstructure:"timeout" validate:"required"`
		RetryLimit     int           `mapstructure:"retry_limit" validate:"required,min=1"`
		RetryDelay     time.Duration `mapstructure:"retry_delay" validate:"required"`
	} `mapstructure:"couchdb"`

	// PostgreSQL Configuration
	Postgres struct {
		Host            string        `mapstructure:"host" validate:"required"`
		Port            int           `mapstructure:"port" validate:"required,min=1"`
		Database        string        `mapstructure:"database" validate:"required"`
		Username        string        `mapstructure:"username" validate:"required"`
		Password        string        `mapstructure:"password" validate:"required"`
		MaxConnections  int           `mapstructure:"max_connections" validate:"required,min=1"`
		MaxIdleConns    int           `mapstructure:"max_idle_connections" validate:"required,min=1"`
		ConnMaxLifetime time.Duration `mapstructure:"conn_max_lifetime" validate:"required"`
		SSLMode         string        `mapstructure:"ssl_mode" validate:"oneof=disable require verify-full"`
	} `mapstructure:"postgres"`
}

// SyncConfig holds synchronization-specific configuration
type SyncConfig struct {
	BatchSize        int           `mapstructure:"batch_size" validate:"required,min=1"`
	SyncInterval     time.Duration `mapstructure:"sync_interval" validate:"required"`
	WorkerCount      int           `mapstructure:"worker_count" validate:"required,min=1"`
	RetryLimit       int           `mapstructure:"retry_limit" validate:"required,min=1"`
	RetryDelay       time.Duration `mapstructure:"retry_delay" validate:"required"`
	ArchivalAge      uint64        `mapstructure:"archival_age" validate:"required,min=1"`
	CheckpointSize   uint64        `mapstructure:"checkpoint_size" validate:"required,min=1"`
	CompressionLevel int           `mapstructure:"compression_level" validate:"min=0,max=9"`
}

// CacheConfig holds caching-specific configuration
type CacheConfig struct {
	Enabled      bool          `mapstructure:"enabled"`
	Type         string        `mapstructure:"type" validate:"oneof=memory redis"`
	Size         int           `mapstructure:"size" validate:"required_if=Enabled true,min=1"`
	TTL          time.Duration `mapstructure:"ttl" validate:"required_if=Enabled true"`
	RedisURL     string        `mapstructure:"redis_url" validate:"required_if=Type redis,omitempty,url"`
	RedisDB      int           `mapstructure:"redis_db" validate:"min=0"`
	MaxRetries   int           `mapstructure:"max_retries" validate:"required,min=1"`
	DialTimeout  time.Duration `mapstructure:"dial_timeout" validate:"required"`
	ReadTimeout  time.Duration `mapstructure:"read_timeout" validate:"required"`
	WriteTimeout time.Duration `mapstructure:"write_timeout" validate:"required"`
}

// OptimizerConfig holds AI optimizer-specific configuration
type OptimizerConfig struct {
	Enabled bool `mapstructure:"enabled"`

	// IOPS Configuration
	IOPS struct {
		MaxRead    int     `mapstructure:"max_read" validate:"required,min=1"`
		MaxWrite   int     `mapstructure:"max_write" validate:"required,min=1"`
		TargetUtil float64 `mapstructure:"target_utilization" validate:"required,min=0,max=1"`
	} `mapstructure:"iops"`

	// Rate Limiting
	RateLimiter struct {
		Enabled      bool    `mapstructure:"enabled"`
		InitialLimit int     `mapstructure:"initial_limit" validate:"required,min=1"`
		BurstLimit   int     `mapstructure:"burst_limit" validate:"required,min=1"`
		ScaleFactor  float64 `mapstructure:"scale_factor" validate:"required,min=0.1"`
	} `mapstructure:"rate_limiter"`

	// Sharding Configuration
	Sharding struct {
		Enabled    bool    `mapstructure:"enabled"`
		ShardCount int     `mapstructure:"shard_count" validate:"required_if=Enabled true,min=1"`
		ReplicaSet int     `mapstructure:"replica_set" validate:"required_if=Enabled true,min=1"`
		ShardSize  int64   `mapstructure:"shard_size" validate:"required_if=Enabled true,min=1"`
		LoadFactor float64 `mapstructure:"load_factor" validate:"required,min=0,max=1"`
	} `mapstructure:"sharding"`

	// High Availability Configuration
	HighAvailability struct {
		Enabled           bool          `mapstructure:"enabled"`
		HeartbeatInterval time.Duration `mapstructure:"heartbeat_interval" validate:"required_if=Enabled true"`
		FailoverTimeout   time.Duration `mapstructure:"failover_timeout" validate:"required_if=Enabled true"`
		MinReplicas       int           `mapstructure:"min_replicas" validate:"required_if=Enabled true,min=1"`
	} `mapstructure:"high_availability"`

	// Learning Parameters
	Learning struct {
		Interval   time.Duration `mapstructure:"interval" validate:"required"`
		WindowSize int           `mapstructure:"window_size" validate:"required,min=1"`
		Threshold  float64       `mapstructure:"threshold" validate:"required,min=0,max=1"`
		MaxAdjust  float64       `mapstructure:"max_adjustment" validate:"required,min=0,max=1"`
	} `mapstructure:"learning"`
}

// Config represents the complete configuration
type Config struct {
	Environment Environment     `mapstructure:"environment" validate:"required,oneof=development staging production"`
	Database    DatabaseConfig  `mapstructure:"database"`
	Sync        SyncConfig      `mapstructure:"sync"`
	Cache       CacheConfig     `mapstructure:"cache"`
	Optimizer   OptimizerConfig `mapstructure:"optimizer"`

	mu             sync.RWMutex
	changeHandlers []func(*Config)
}

// Manager handles configuration loading and dynamic updates
type Manager struct {
	config     *Config
	configPath string
	viper      *viper.Viper
	mu         sync.RWMutex
}

// NewManager creates a new configuration manager
func NewManager(configPath string) (*Manager, error) {
	v := viper.New()
	v.SetConfigFile(configPath)

	manager := &Manager{
		configPath: configPath,
		viper:      v,
	}

	if err := manager.load(); err != nil {
		return nil, fmt.Errorf("failed to load config: %w", err)
	}

	// Setup file watcher for dynamic updates
	if err := manager.setupWatcher(); err != nil {
		log.Printf("Warning: Config file watching disabled: %v", err)
		// Continue without file watching
	}

	return manager, nil
}

// load reads and validates the configuration
func (m *Manager) load() error {
	if err := m.viper.ReadInConfig(); err != nil {
		return fmt.Errorf("failed to read config file: %w", err)
	}

	var config Config
	if err := m.viper.Unmarshal(&config, func(dc *mapstructure.DecoderConfig) {
		dc.TagName = "mapstructure"
	}); err != nil {
		return fmt.Errorf("failed to unmarshal config: %w", err)
	}

	if err := validateConfig(&config); err != nil {
		return fmt.Errorf("config validation failed: %w", err)
	}

	m.mu.Lock()
	m.config = &config
	m.mu.Unlock()

	return nil
}

// setupWatcher configures file watching for dynamic updates
func (m *Manager) setupWatcher() error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("failed to create watcher: %w", err)
	}

	go func() {
		for {
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}
				if event.Op&fsnotify.Write == fsnotify.Write {
					if err := m.load(); err != nil {
						log.Printf("Error reloading config: %v", err)
						continue
					}
					m.notifyChangeHandlers()
				}
			case err, ok := <-watcher.Errors:
				if !ok {
					return
				}
				log.Printf("Config watcher error: %v", err)
			}
		}
	}()

	return watcher.Add(filepath.Dir(m.configPath))
}

// GetConfig returns the current configuration
func (m *Manager) GetConfig() *Config {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.config
}

// RegisterChangeHandler registers a handler function for configuration changes
func (m *Manager) RegisterChangeHandler(handler func(*Config)) {
	m.config.mu.Lock()
	defer m.config.mu.Unlock()
	m.config.changeHandlers = append(m.config.changeHandlers, handler)
}

// notifyChangeHandlers notifies all registered handlers of configuration changes
func (m *Manager) notifyChangeHandlers() {
	m.config.mu.RLock()
	handlers := m.config.changeHandlers
	config := m.config
	m.config.mu.RUnlock()

	for _, handler := range handlers {
		go handler(config)
	}
}

// validateConfig performs validation of the configuration values
func validateConfig(config *Config) error {
	validate := validator.New()

	// Register custom validation functions
	if err := registerCustomValidations(validate); err != nil {
		return fmt.Errorf("failed to register custom validations: %w", err)
	}

	if err := validate.Struct(config); err != nil {
		return processValidationErrors(err)
	}

	// Additional custom validations
	if err := performCustomValidations(config); err != nil {
		return err
	}

	return nil
}

// registerCustomValidations registers any custom validation functions
func registerCustomValidations(validate *validator.Validate) error {
	// Register custom validation for URLs if needed
	if err := validate.RegisterValidation("url", validateURL); err != nil {
		return fmt.Errorf("failed to register URL validator: %w", err)
	}

	return nil
}

// validateURL implements custom URL validation
func validateURL(fl validator.FieldLevel) bool {
	// Add URL validation logic if needed
	url := fl.Field().String()
	return len(url) > 0 // Basic check, enhance as needed
}

// processValidationErrors formats validation errors nicely
func processValidationErrors(err error) error {
	if validationErrors, ok := err.(validator.ValidationErrors); ok {
		var errorMessages []string
		for _, e := range validationErrors {
			errorMessages = append(errorMessages, fmt.Sprintf(
				"field: %s, tag: %s, value: %s",
				e.Field(),
				e.Tag(),
				e.Value(),
			))
		}
		return fmt.Errorf("validation errors: %v", errorMessages)
	}
	return err
}

// performCustomValidations implements additional custom validation logic
func performCustomValidations(config *Config) error {
	if config.Sync.BatchSize > 10000 {
		return fmt.Errorf("batch size cannot exceed 10000")
	}

	if config.Cache.Enabled && config.Cache.Type == "redis" && config.Cache.RedisURL == "" {
		return fmt.Errorf("redis URL is required when redis cache is enabled")
	}

	if config.Optimizer.Enabled {
		if err := validateOptimizer(&config.Optimizer); err != nil {
			return fmt.Errorf("optimizer validation failed: %w", err)
		}
	}

	return nil
}

// validateOptimizer performs specific optimizer validations
func validateOptimizer(opt *OptimizerConfig) error {
	if opt.IOPS.TargetUtil <= 0 || opt.IOPS.TargetUtil > 1 {
		return fmt.Errorf("IOPS target utilization must be between 0 and 1")
	}

	if opt.RateLimiter.Enabled && opt.RateLimiter.InitialLimit >= opt.RateLimiter.BurstLimit {
		return fmt.Errorf("rate limiter initial limit must be less than burst limit")
	}

	if opt.Sharding.Enabled && opt.Sharding.LoadFactor <= 0 {
		return fmt.Errorf("sharding load factor must be positive")
	}

	if opt.HighAvailability.Enabled && opt.HighAvailability.MinReplicas < 2 {
		return fmt.Errorf("high availability requires at least 2 replicas")
	}

	return nil
}

// GetDatabaseConfig returns the database configuration
func (m *Manager) GetDatabaseConfig() DatabaseConfig {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.config.Database
}

// GetSyncConfig returns the sync configuration
func (m *Manager) GetSyncConfig() SyncConfig {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.config.Sync
}

// GetCacheConfig returns the cache configuration
func (m *Manager) GetCacheConfig() CacheConfig {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.config.Cache
}

// GetOptimizerConfig returns the optimizer configuration
func (m *Manager) GetOptimizerConfig() OptimizerConfig {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.config.Optimizer
}

// GetEnvironment returns the current environment
func (m *Manager) GetEnvironment() Environment {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.config.Environment
}

// ////crypto.go//
func (c *CryptoConfig) GetKyberSecurityLevel() int {
	if c.KeyExchangeAlgorithm == "" {
		return c.KyberSecurityLevel // Use new field if old one is not set
	}
	// Parse from existing field for backward compatibility
	switch c.KeyExchangeAlgorithm {
	case "Kyber512":
		return 512
	case "Kyber768":
		return 768
	case "Kyber1024":
		return 1024
	default:
		return 1024 // Default to highest security
	}
}

// GetDilithiumSecurityLevel returns the numeric security level for Dilithium
func (c *CryptoConfig) GetDilithiumSecurityLevel() int {
	if c.SignatureAlgorithm == "" {
		return c.DilithiumSecurityLevel // Use new field if old one is not set
	}
	// Parse from existing field for backward compatibility
	switch c.SignatureAlgorithm {
	case "Dilithium2":
		return 2
	case "Dilithium3":
		return 3
	case "Dilithium5":
		return 5
	default:
		return 3 // Default to NIST Level 3
	}
}

// GetKeyExpiration returns the key expiration duration
func (c *CryptoConfig) GetKeyExpiration() time.Duration {
	return c.KeyExpirationTime
}

// IsKeyRotationDue checks if key rotation is due based on current configuration
func (c *CryptoConfig) IsKeyRotationDue(lastRotation time.Time) bool {
	if !c.EnableKeyRotation {
		return false
	}
	return time.Since(lastRotation) >= c.KeyRotationInterval
}
