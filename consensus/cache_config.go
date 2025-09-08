// consensus/cache_config.go

package consensus

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"time"
)

// CacheConfigProfile defines a set of cache configurations for a specific environment
type CacheConfigProfile struct {
	// Profile name (e.g., "development", "testing", "production")
	Name string `json:"name"`

	// Default configuration for all caches
	DefaultConfig CacheConfig `json:"default_config"`

	// Specific configurations for different cache types
	EventCacheConfig             *CacheConfig `json:"event_cache_config,omitempty"`
	EventValidationCacheConfig   *CacheConfig `json:"event_validation_cache_config,omitempty"`
	EventFinalizationCacheConfig *CacheConfig `json:"event_finalization_cache_config,omitempty"`
	BlockCacheConfig             *CacheConfig `json:"block_cache_config,omitempty"`
	BlockValidationCacheConfig   *CacheConfig `json:"block_validation_cache_config,omitempty"`
	ValidatorCacheConfig         *CacheConfig `json:"validator_cache_config,omitempty"`
	ValidatorStakeCacheConfig    *CacheConfig `json:"validator_stake_cache_config,omitempty"`
	ValidatorStatusCacheConfig   *CacheConfig `json:"validator_status_cache_config,omitempty"`
	StateCacheConfig             *CacheConfig `json:"state_cache_config,omitempty"`
	SignatureCacheConfig         *CacheConfig `json:"signature_cache_config,omitempty"`
}

// GetCacheConfig returns the cache configuration for a specific cache type
func (p *CacheConfigProfile) GetCacheConfig(cacheName string) *CacheConfig {
	switch cacheName {
	case EventCache:
		if p.EventCacheConfig != nil {
			return p.EventCacheConfig
		}
	case EventValidationCache:
		if p.EventValidationCacheConfig != nil {
			return p.EventValidationCacheConfig
		}
	case EventFinalizationCache:
		if p.EventFinalizationCacheConfig != nil {
			return p.EventFinalizationCacheConfig
		}
	case BlockCache:
		if p.BlockCacheConfig != nil {
			return p.BlockCacheConfig
		}
	case BlockValidationCache:
		if p.BlockValidationCacheConfig != nil {
			return p.BlockValidationCacheConfig
		}
	case ValidatorCache:
		if p.ValidatorCacheConfig != nil {
			return p.ValidatorCacheConfig
		}
	case ValidatorStakeCache:
		if p.ValidatorStakeCacheConfig != nil {
			return p.ValidatorStakeCacheConfig
		}
	case ValidatorStatusCache:
		if p.ValidatorStatusCacheConfig != nil {
			return p.ValidatorStatusCacheConfig
		}
	case StateCache:
		if p.StateCacheConfig != nil {
			return p.StateCacheConfig
		}
	case SignatureCache:
		if p.SignatureCacheConfig != nil {
			return p.SignatureCacheConfig
		}
	}

	// Return a copy of the default config
	config := p.DefaultConfig
	return &config
}

// CacheConfigManager manages cache configurations
type CacheConfigManager struct {
	profiles map[string]*CacheConfigProfile
	active   string
	logger   *hybridConsensusLogger
}

// NewCacheConfigManager creates a new CacheConfigManager
func NewCacheConfigManager(logger *hybridConsensusLogger) *CacheConfigManager {
	return &CacheConfigManager{
		profiles: make(map[string]*CacheConfigProfile),
		active:   "default",
		logger:   logger,
	}
}

// RegisterProfile registers a cache configuration profile
func (m *CacheConfigManager) RegisterProfile(profile *CacheConfigProfile) {
	m.profiles[profile.Name] = profile
	m.logger.Info("Registered cache configuration profile", LogKeyValue{Key: "name", Value: profile.Name})
}

// SetActiveProfile sets the active cache configuration profile
func (m *CacheConfigManager) SetActiveProfile(name string) error {
	if _, ok := m.profiles[name]; !ok {
		return fmt.Errorf("cache configuration profile not found: %s", name)
	}
	m.active = name
	m.logger.Info("Set active cache configuration profile", LogKeyValue{Key: "name", Value: name})
	return nil
}

// GetActiveProfile returns the active cache configuration profile
func (m *CacheConfigManager) GetActiveProfile() *CacheConfigProfile {
	return m.profiles[m.active]
}

// GetCacheConfig returns the cache configuration for a specific cache type
func (m *CacheConfigManager) GetCacheConfig(cacheName string) *CacheConfig {
	profile := m.GetActiveProfile()
	if profile == nil {
		// Return a default configuration if no profile is active
		return &CacheConfig{
			Type:         "lru",
			Capacity:     1000,
			TTL:          0,
			TrackMetrics: true,
		}
	}
	return profile.GetCacheConfig(cacheName)
}

// LoadProfileFromFile loads a cache configuration profile from a JSON file
func (m *CacheConfigManager) LoadProfileFromFile(filePath string) error {
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("failed to read cache configuration file: %w", err)
	}

	var profile CacheConfigProfile
	if err := json.Unmarshal(data, &profile); err != nil {
		return fmt.Errorf("failed to parse cache configuration file: %w", err)
	}

	m.RegisterProfile(&profile)
	return nil
}

// SaveProfileToFile saves a cache configuration profile to a JSON file
func (m *CacheConfigManager) SaveProfileToFile(name string, filePath string) error {
	profile, ok := m.profiles[name]
	if !ok {
		return fmt.Errorf("cache configuration profile not found: %s", name)
	}

	data, err := json.MarshalIndent(profile, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal cache configuration profile: %w", err)
	}

	if err := ioutil.WriteFile(filePath, data, 0644); err != nil {
		return fmt.Errorf("failed to write cache configuration file: %w", err)
	}

	m.logger.Info("Saved cache configuration profile to file",
		LogKeyValue{Key: "name", Value: name},
		LogKeyValue{Key: "file", Value: filePath})
	return nil
}

// ApplyProfileToConsensus applies a cache configuration profile to a CachedConsensus instance
func (m *CacheConfigManager) ApplyProfileToConsensus(cachedConsensus *CachedConsensus) {
	profile := m.GetActiveProfile()
	if profile == nil {
		m.logger.Warn("No active cache configuration profile")
		return
	}

	cacheManager := cachedConsensus.GetCacheManager()

	// Apply configurations to all cache types
	cacheManager.GetCache(EventCache, profile.GetCacheConfig(EventCache))
	cacheManager.GetCache(EventValidationCache, profile.GetCacheConfig(EventValidationCache))
	cacheManager.GetCache(EventFinalizationCache, profile.GetCacheConfig(EventFinalizationCache))
	cacheManager.GetCache(BlockCache, profile.GetCacheConfig(BlockCache))
	cacheManager.GetCache(BlockValidationCache, profile.GetCacheConfig(BlockValidationCache))
	cacheManager.GetCache(ValidatorCache, profile.GetCacheConfig(ValidatorCache))
	cacheManager.GetCache(ValidatorStakeCache, profile.GetCacheConfig(ValidatorStakeCache))
	cacheManager.GetCache(ValidatorStatusCache, profile.GetCacheConfig(ValidatorStatusCache))
	cacheManager.GetCache(StateCache, profile.GetCacheConfig(StateCache))
	cacheManager.GetCache(SignatureCache, profile.GetCacheConfig(SignatureCache))

	m.logger.Info("Applied cache configuration profile to consensus", LogKeyValue{Key: "name", Value: profile.Name})
}

// CreateDefaultProfiles creates default cache configuration profiles
func (m *CacheConfigManager) CreateDefaultProfiles() {
	// Development profile - optimized for development environment
	m.RegisterProfile(&CacheConfigProfile{
		Name: "development",
		DefaultConfig: CacheConfig{
			Type:         "lru",
			Capacity:     1000,
			TTL:          10 * time.Minute,
			TrackMetrics: true,
		},
		// Override specific cache configurations
		EventCacheConfig: &CacheConfig{
			Type:         "lru",
			Capacity:     5000,
			TTL:          30 * time.Minute,
			TrackMetrics: true,
		},
		SignatureCacheConfig: &CacheConfig{
			Type:         "lru",
			Capacity:     10000,
			TTL:          1 * time.Hour,
			TrackMetrics: true,
		},
	})

	// Testing profile - optimized for testing environment
	m.RegisterProfile(&CacheConfigProfile{
		Name: "testing",
		DefaultConfig: CacheConfig{
			Type:         "lru",
			Capacity:     100,
			TTL:          1 * time.Minute,
			TrackMetrics: true,
		},
		// Override specific cache configurations
		EventCacheConfig: &CacheConfig{
			Type:         "lru",
			Capacity:     500,
			TTL:          5 * time.Minute,
			TrackMetrics: true,
		},
		SignatureCacheConfig: &CacheConfig{
			Type:         "lru",
			Capacity:     1000,
			TTL:          10 * time.Minute,
			TrackMetrics: true,
		},
	})

	// Production profile - optimized for production environment
	m.RegisterProfile(&CacheConfigProfile{
		Name: "production",
		DefaultConfig: CacheConfig{
			Type:         "lru",
			Capacity:     10000,
			TTL:          1 * time.Hour,
			TrackMetrics: true,
		},
		// Override specific cache configurations
		EventCacheConfig: &CacheConfig{
			Type:         "lru",
			Capacity:     50000,
			TTL:          2 * time.Hour,
			TrackMetrics: true,
		},
		BlockCacheConfig: &CacheConfig{
			Type:         "lru",
			Capacity:     5000,
			TTL:          3 * time.Hour,
			TrackMetrics: true,
		},
		SignatureCacheConfig: &CacheConfig{
			Type:         "lru",
			Capacity:     100000,
			TTL:          24 * time.Hour,
			TrackMetrics: true,
		},
	})

	// High-performance profile - optimized for high-performance environment
	m.RegisterProfile(&CacheConfigProfile{
		Name: "high-performance",
		DefaultConfig: CacheConfig{
			Type:         "lru",
			Capacity:     50000,
			TTL:          2 * time.Hour,
			TrackMetrics: true,
		},
		// Override specific cache configurations
		EventCacheConfig: &CacheConfig{
			Type:         "lru",
			Capacity:     200000,
			TTL:          4 * time.Hour,
			TrackMetrics: true,
		},
		BlockCacheConfig: &CacheConfig{
			Type:         "lru",
			Capacity:     20000,
			TTL:          6 * time.Hour,
			TrackMetrics: true,
		},
		SignatureCacheConfig: &CacheConfig{
			Type:         "lru",
			Capacity:     500000,
			TTL:          48 * time.Hour,
			TrackMetrics: true,
		},
	})

	// Low-memory profile - optimized for low-memory environment
	m.RegisterProfile(&CacheConfigProfile{
		Name: "low-memory",
		DefaultConfig: CacheConfig{
			Type:         "lru",
			Capacity:     500,
			TTL:          30 * time.Minute,
			TrackMetrics: true,
		},
		// Override specific cache configurations
		EventCacheConfig: &CacheConfig{
			Type:         "lru",
			Capacity:     2000,
			TTL:          1 * time.Hour,
			TrackMetrics: true,
		},
		BlockCacheConfig: &CacheConfig{
			Type:         "lru",
			Capacity:     1000,
			TTL:          2 * time.Hour,
			TrackMetrics: true,
		},
		SignatureCacheConfig: &CacheConfig{
			Type:         "lru",
			Capacity:     5000,
			TTL:          6 * time.Hour,
			TrackMetrics: true,
		},
	})
}

// Example of using the CacheConfigManager
func ExampleCacheConfigManager() {
	// Create a logger
	logger := &hybridConsensusLogger{
		logger: nil, // Use a real logger in production
	}

	// Create a CacheConfigManager
	configManager := NewCacheConfigManager(logger)

	// Create default profiles
	configManager.CreateDefaultProfiles()

	// Set the active profile
	configManager.SetActiveProfile("production")

	// Create a HybridConsensus instance
	hc := NewHybridConsensusWithConfig(DefaultHybridConfig())

	// Create a CachedConsensus instance
	cachedConsensus := NewCachedConsensus(hc, nil)

	// Apply the active profile to the CachedConsensus instance
	configManager.ApplyProfileToConsensus(cachedConsensus)

	// Save the active profile to a file
	configManager.SaveProfileToFile("production", "cache_config_production.json")

	// Load a profile from a file
	configManager.LoadProfileFromFile("cache_config_custom.json")

	// Set the loaded profile as active
	configManager.SetActiveProfile("custom")

	// Apply the new active profile to the CachedConsensus instance
	configManager.ApplyProfileToConsensus(cachedConsensus)
}

// GetEnvironmentProfile returns the cache configuration profile based on the environment
func GetEnvironmentProfile() string {
	env := os.Getenv("DIAMANTE_ENV")
	if env == "" {
		env = "development" // Default to development
	}
	return env
}

// InitializeCacheConfigManager initializes the CacheConfigManager with default profiles
// and sets the active profile based on the environment
func InitializeCacheConfigManager(logger *hybridConsensusLogger) *CacheConfigManager {
	configManager := NewCacheConfigManager(logger)
	configManager.CreateDefaultProfiles()

	// Set the active profile based on the environment
	env := GetEnvironmentProfile()
	if err := configManager.SetActiveProfile(env); err != nil {
		logger.Warn("Failed to set active cache configuration profile", LogKeyValue{Key: "error", Value: err.Error()})
		// Fall back to development profile
		configManager.SetActiveProfile("development")
	}

	return configManager
}
