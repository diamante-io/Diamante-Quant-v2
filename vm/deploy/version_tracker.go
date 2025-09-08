// Package deploy provides version tracking for deployed contracts
package deploy

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"diamante/storage"

	"github.com/sirupsen/logrus"
)

// DefaultVersionTracker is the default implementation of VersionTracker
type DefaultVersionTracker struct {
	store  storage.LedgerStore
	logger *logrus.Logger
	mu     sync.RWMutex
	cache  map[string][]*ContractVersion // contractID -> versions
}

// NewVersionTracker creates a new version tracker
func NewVersionTracker(store storage.LedgerStore, logger *logrus.Logger) VersionTracker {
	return &DefaultVersionTracker{
		store:  store,
		logger: logger,
		cache:  make(map[string][]*ContractVersion),
	}
}

// AddVersion adds a new version
func (vt *DefaultVersionTracker) AddVersion(version *ContractVersion) error {
	vt.mu.Lock()
	defer vt.mu.Unlock()

	// Validate version
	if version.ContractID == "" || version.Version == "" {
		return fmt.Errorf("contract ID and version are required")
	}

	// Check if version already exists
	existing, err := vt.getVersionFromStorage(version.ContractID, version.Version)
	if err == nil && existing != nil {
		return fmt.Errorf("version %s already exists for contract %s", version.Version, version.ContractID)
	}

	// Store version
	key := vt.versionKey(version.ContractID, version.Version)
	data, err := json.Marshal(version)
	if err != nil {
		return fmt.Errorf("failed to marshal version: %w", err)
	}

	if err := vt.store.SaveState([]byte(key), data); err != nil {
		return fmt.Errorf("failed to store version: %w", err)
	}

	// Update index for efficient lookups
	if err := vt.addVersionToIndex(version.ContractID, version.Version); err != nil {
		return fmt.Errorf("failed to update version index: %w", err)
	}

	// Update cache
	vt.cache[version.ContractID] = append(vt.cache[version.ContractID], version)

	// If this is the active version, update the current version pointer
	if version.Active {
		if err := vt.setCurrentVersion(version.ContractID, version.Version); err != nil {
			return fmt.Errorf("failed to set current version: %w", err)
		}
	}

	vt.logger.WithFields(logrus.Fields{
		"contractID": version.ContractID,
		"version":    version.Version,
		"active":     version.Active,
	}).Info("Version added")

	return nil
}

// GetVersion retrieves a specific version
func (vt *DefaultVersionTracker) GetVersion(contractID, version string) (*ContractVersion, error) {
	vt.mu.RLock()
	defer vt.mu.RUnlock()

	// Check cache first
	if versions, exists := vt.cache[contractID]; exists {
		for _, v := range versions {
			if v.Version == version {
				return v, nil
			}
		}
	}

	// Get from storage
	return vt.getVersionFromStorage(contractID, version)
}

// GetCurrentVersion retrieves the current active version
func (vt *DefaultVersionTracker) GetCurrentVersion(contractID string) (*ContractVersion, error) {
	vt.mu.RLock()
	defer vt.mu.RUnlock()

	// Get current version pointer
	currentKey := vt.currentVersionKey(contractID)
	versionData, err := vt.store.GetState([]byte(currentKey))
	if err != nil {
		return nil, fmt.Errorf("failed to get current version: %w", err)
	}

	if len(versionData) == 0 {
		return nil, fmt.Errorf("no current version found for contract %s", contractID)
	}

	currentVersion := string(versionData)
	return vt.GetVersion(contractID, currentVersion)
}

// GetVersionHistory retrieves all versions for a contract
func (vt *DefaultVersionTracker) GetVersionHistory(contractID string) ([]*ContractVersion, error) {
	vt.mu.RLock()
	defer vt.mu.RUnlock()

	// Check cache first
	if versions, exists := vt.cache[contractID]; exists && len(versions) > 0 {
		return versions, nil
	}

	// Load from storage
	versions, err := vt.loadVersionHistory(contractID)
	if err != nil {
		return nil, err
	}

	// Update cache
	vt.cache[contractID] = versions

	return versions, nil
}

// DeactivateVersion deactivates a version
func (vt *DefaultVersionTracker) DeactivateVersion(contractID, version string) error {
	vt.mu.Lock()
	defer vt.mu.Unlock()

	// Get version
	v, err := vt.getVersionFromStorage(contractID, version)
	if err != nil {
		return fmt.Errorf("failed to get version: %w", err)
	}

	if v == nil {
		return fmt.Errorf("version %s not found for contract %s", version, contractID)
	}

	// Update version
	v.Active = false

	// Store updated version
	key := vt.versionKey(contractID, version)
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("failed to marshal version: %w", err)
	}

	if err := vt.store.SaveState([]byte(key), data); err != nil {
		return fmt.Errorf("failed to store version: %w", err)
	}

	// Update cache
	if versions, exists := vt.cache[contractID]; exists {
		for i, cached := range versions {
			if cached.Version == version {
				vt.cache[contractID][i] = v
				break
			}
		}
	}

	vt.logger.WithFields(logrus.Fields{
		"contractID": contractID,
		"version":    version,
	}).Info("Version deactivated")

	return nil
}

// Helper methods

func (vt *DefaultVersionTracker) versionKey(contractID, version string) string {
	return fmt.Sprintf("contract:%s:version:%s", contractID, version)
}

func (vt *DefaultVersionTracker) currentVersionKey(contractID string) string {
	return fmt.Sprintf("contract:%s:current", contractID)
}

func (vt *DefaultVersionTracker) versionIndexKey(contractID string) string {
	return fmt.Sprintf("contract:%s:versions", contractID)
}

func (vt *DefaultVersionTracker) getVersionFromStorage(contractID, version string) (*ContractVersion, error) {
	key := vt.versionKey(contractID, version)
	data, err := vt.store.GetState([]byte(key))
	if err != nil {
		return nil, err
	}

	if len(data) == 0 {
		return nil, nil
	}

	var v ContractVersion
	if err := json.Unmarshal(data, &v); err != nil {
		return nil, fmt.Errorf("failed to unmarshal version: %w", err)
	}

	return &v, nil
}

func (vt *DefaultVersionTracker) setCurrentVersion(contractID, version string) error {
	key := vt.currentVersionKey(contractID)
	return vt.store.SaveState([]byte(key), []byte(version))
}

func (vt *DefaultVersionTracker) addVersionToIndex(contractID, version string) error {
	indexKey := vt.versionIndexKey(contractID)
	var versions []string

	data, err := vt.store.GetState([]byte(indexKey))
	if err != nil && !errors.Is(err, storage.ErrNotFound) {
		return err
	}
	if len(data) > 0 {
		if err := json.Unmarshal(data, &versions); err != nil {
			return err
		}
	}
	for _, v := range versions {
		if v == version {
			return nil
		}
	}
	versions = append(versions, version)
	encoded, err := json.Marshal(versions)
	if err != nil {
		return err
	}
	return vt.store.SaveState([]byte(indexKey), encoded)
}

func (vt *DefaultVersionTracker) loadVersionHistory(contractID string) ([]*ContractVersion, error) {
	indexKey := vt.versionIndexKey(contractID)

	data, err := vt.store.GetState([]byte(indexKey))
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return []*ContractVersion{}, nil
		}
		return nil, fmt.Errorf("failed to load version index: %w", err)
	}

	if len(data) == 0 {
		return []*ContractVersion{}, nil
	}

	var versionIDs []string
	if err := json.Unmarshal(data, &versionIDs); err != nil {
		return nil, fmt.Errorf("failed to unmarshal version index: %w", err)
	}

	versions := make([]*ContractVersion, 0, len(versionIDs))
	for _, vID := range versionIDs {
		v, err := vt.getVersionFromStorage(contractID, vID)
		if err != nil {
			return nil, fmt.Errorf("failed to load version %s: %w", vID, err)
		}
		if v != nil {
			versions = append(versions, v)
		}
	}

	return versions, nil
}
