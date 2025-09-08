// Package deploy provides secondary indices for deployment history
package deploy

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"diamante/common"
	"diamante/storage"

	"github.com/sirupsen/logrus"
)

// IndexType represents the type of index
type IndexType string

const (
	// IndexTypeContract indexes by contract ID
	IndexTypeContract IndexType = "contract"
	// IndexTypeTime indexes by timestamp
	IndexTypeTime IndexType = "time"
	// IndexTypeDeployer indexes by deployer address
	IndexTypeDeployer IndexType = "deployer"
)

// DeploymentIndex manages secondary indices for deployment and upgrade history
type DeploymentIndex struct {
	store  storage.LedgerStore
	logger *logrus.Logger
	mu     sync.RWMutex

	// In-memory indices for fast lookups
	contractIndex map[string][]string // contractID -> []deploymentKeys
	timeIndex     map[int64][]string  // timestamp -> []deploymentKeys
	deployerIndex map[string][]string // deployer -> []deploymentKeys

	// Upgrade indices
	upgradeContractIndex map[string][]string // contractID -> []upgradeKeys
	upgradeTimeIndex     map[int64][]string  // timestamp -> []upgradeKeys
	upgradeAuthIndex     map[string][]string // authorizer -> []upgradeKeys

	// Index metadata
	indexVersion string
	lastRebuild  time.Time
}

// NewDeploymentIndex creates a new deployment index
func NewDeploymentIndex(store storage.LedgerStore, logger *logrus.Logger) *DeploymentIndex {
	return &DeploymentIndex{
		store:         store,
		logger:        logger,
		contractIndex: make(map[string][]string),
		timeIndex:     make(map[int64][]string),
		deployerIndex: make(map[string][]string),

		// Initialize upgrade indices
		upgradeContractIndex: make(map[string][]string),
		upgradeTimeIndex:     make(map[int64][]string),
		upgradeAuthIndex:     make(map[string][]string),

		indexVersion: "2.0.0", // Increment version for upgrade support
	}
}

// AddDeployment adds a deployment to all indices
func (idx *DeploymentIndex) AddDeployment(ctx *DeploymentContext) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	// Generate deployment key
	deployKey := idx.generateDeploymentKey(ctx.ContractID, ctx.StartTime)

	// Add to contract index
	idx.contractIndex[ctx.ContractID] = append(idx.contractIndex[ctx.ContractID], deployKey)

	// Add to time index (bucket by hour)
	timeBucket := ctx.StartTime.Unix() / 3600 * 3600
	idx.timeIndex[timeBucket] = append(idx.timeIndex[timeBucket], deployKey)

	// Add to deployer index
	idx.deployerIndex[ctx.Request.Deployer] = append(idx.deployerIndex[ctx.Request.Deployer], deployKey)

	// Persist indices
	if err := idx.persistIndices(); err != nil {
		return fmt.Errorf("failed to persist indices: %w", err)
	}

	idx.logger.WithFields(logrus.Fields{
		"contractID": ctx.ContractID,
		"deployer":   ctx.Request.Deployer,
		"key":        deployKey,
	}).Debug("Deployment added to indices")

	return nil
}

// GetDeploymentsByContract retrieves all deployments for a contract
func (idx *DeploymentIndex) GetDeploymentsByContract(contractID string) ([]string, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	keys, exists := idx.contractIndex[contractID]
	if !exists {
		return []string{}, nil
	}

	// Return a copy to prevent external modification
	result := make([]string, len(keys))
	copy(result, keys)
	return result, nil
}

// GetDeploymentsByTimeRange retrieves deployments within a time range
func (idx *DeploymentIndex) GetDeploymentsByTimeRange(start, end time.Time) ([]string, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	var result []string

	// Calculate time buckets
	startBucket := start.Unix() / 3600 * 3600
	endBucket := end.Unix() / 3600 * 3600

	// Collect keys from all buckets in range
	for bucket := startBucket; bucket <= endBucket; bucket += 3600 {
		if keys, exists := idx.timeIndex[bucket]; exists {
			result = append(result, keys...)
		}
	}

	return result, nil
}

// GetDeploymentsByDeployer retrieves all deployments by a deployer
func (idx *DeploymentIndex) GetDeploymentsByDeployer(deployer string) ([]string, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	keys, exists := idx.deployerIndex[deployer]
	if !exists {
		return []string{}, nil
	}

	result := make([]string, len(keys))
	copy(result, keys)
	return result, nil
}

// AddUpgrade adds an upgrade to all upgrade indices
func (idx *DeploymentIndex) AddUpgrade(ctx *UpgradeContext) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	// Generate upgrade key
	upgradeKey := idx.generateUpgradeKey(ctx.Request.ContractID, ctx.StartTime)

	// Add to upgrade contract index
	idx.upgradeContractIndex[ctx.Request.ContractID] = append(idx.upgradeContractIndex[ctx.Request.ContractID], upgradeKey)

	// Add to upgrade time index (bucket by hour)
	timeBucket := ctx.StartTime.Unix() / 3600 * 3600
	idx.upgradeTimeIndex[timeBucket] = append(idx.upgradeTimeIndex[timeBucket], upgradeKey)

	// Add to upgrade authorizer index
	idx.upgradeAuthIndex[ctx.Request.Authorizer] = append(idx.upgradeAuthIndex[ctx.Request.Authorizer], upgradeKey)

	// Persist indices
	if err := idx.persistUpgradeIndices(); err != nil {
		return fmt.Errorf("failed to persist upgrade indices: %w", err)
	}

	idx.logger.WithFields(logrus.Fields{
		"contractID": ctx.Request.ContractID,
		"authorizer": ctx.Request.Authorizer,
		"upgradeKey": upgradeKey,
		"newVersion": ctx.Request.NewVersion,
	}).Debug("Upgrade added to indices")

	return nil
}

// GetUpgradesByContract retrieves all upgrades for a contract
func (idx *DeploymentIndex) GetUpgradesByContract(contractID string) ([]string, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	keys, exists := idx.upgradeContractIndex[contractID]
	if !exists {
		return []string{}, nil
	}

	// Return a copy to prevent external modification
	result := make([]string, len(keys))
	copy(result, keys)
	return result, nil
}

// GetUpgradesByTimeRange retrieves upgrades within a time range
func (idx *DeploymentIndex) GetUpgradesByTimeRange(start, end time.Time) ([]string, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	var result []string

	// Calculate time buckets
	startBucket := start.Unix() / 3600 * 3600
	endBucket := end.Unix() / 3600 * 3600

	// Collect keys from all buckets in range
	for bucket := startBucket; bucket <= endBucket; bucket += 3600 {
		if keys, exists := idx.upgradeTimeIndex[bucket]; exists {
			result = append(result, keys...)
		}
	}

	return result, nil
}

// GetUpgradesByAuthorizer retrieves all upgrades by an authorizer
func (idx *DeploymentIndex) GetUpgradesByAuthorizer(authorizer string) ([]string, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	keys, exists := idx.upgradeAuthIndex[authorizer]
	if !exists {
		return []string{}, nil
	}

	result := make([]string, len(keys))
	copy(result, keys)
	return result, nil
}

// generateUpgradeKey generates a unique key for an upgrade
func (idx *DeploymentIndex) generateUpgradeKey(contractID string, timestamp time.Time) string {
	return fmt.Sprintf("upgrade:%s:%d", contractID, timestamp.UnixNano())
}

// RebuildIndices rebuilds all indices from storage
func (idx *DeploymentIndex) RebuildIndices() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	startTime := common.ConsensusNow()
	idx.logger.Info("Starting comprehensive index rebuild for deployments and upgrades")

	// Clear existing deployment indices
	idx.contractIndex = make(map[string][]string)
	idx.timeIndex = make(map[int64][]string)
	idx.deployerIndex = make(map[string][]string)

	// Clear existing upgrade indices
	idx.upgradeContractIndex = make(map[string][]string)
	idx.upgradeTimeIndex = make(map[int64][]string)
	idx.upgradeAuthIndex = make(map[string][]string)

	// Load persisted indices
	if err := idx.loadPersistedIndices(); err != nil {
		idx.logger.WithError(err).Warn("Failed to load persisted indices, starting fresh")
	}

	// Load persisted upgrade indices
	if err := idx.loadPersistedUpgradeIndices(); err != nil {
		idx.logger.WithError(err).Warn("Failed to load persisted upgrade indices, starting fresh")
	}

	idx.lastRebuild = common.ConsensusNow()
	duration := common.ConsensusSince(startTime)

	// Calculate total upgrade keys for metrics
	totalUpgradeKeys := 0
	for _, keys := range idx.upgradeContractIndex {
		totalUpgradeKeys += len(keys)
	}

	idx.logger.WithFields(logrus.Fields{
		"duration":           duration,
		"contractCount":      len(idx.contractIndex),
		"deployerCount":      len(idx.deployerIndex),
		"timeBuckets":        len(idx.timeIndex),
		"upgradeContracts":   len(idx.upgradeContractIndex),
		"upgradeAuthorizers": len(idx.upgradeAuthIndex),
		"totalUpgradeKeys":   totalUpgradeKeys,
	}).Info("Comprehensive index rebuild completed with enterprise-grade integration")

	return nil
}

// IndexData represents typed data for index persistence
type IndexData struct {
	Version       string              `json:"version"`
	LastUpdated   int64               `json:"lastUpdated"`
	ContractIndex map[string][]string `json:"contractIndex,omitempty"`
	TimeIndex     map[int64][]string  `json:"timeIndex,omitempty"`
	DeployerIndex map[string][]string `json:"deployerIndex,omitempty"`
}

// IndexMetadata represents typed metadata for indices
type IndexMetadata struct {
	Version          string `json:"version"`
	LastUpdated      int64  `json:"lastUpdated"`
	Entries          int    `json:"entries"`
	UpgradeEntries   int    `json:"upgradeEntries,omitempty"`
	TotalUpgradeKeys int    `json:"totalUpgradeKeys,omitempty"`
}

// IndexStats represents typed statistics for indices
type IndexStats struct {
	Version            string    `json:"version"`
	LastRebuild        time.Time `json:"lastRebuild"`
	Contracts          int       `json:"contracts"`
	Deployers          int       `json:"deployers"`
	TimeBuckets        int       `json:"timeBuckets"`
	TotalKeys          int       `json:"totalKeys"`
	UpgradeContracts   int       `json:"upgradeContracts,omitempty"`
	UpgradeAuthorizers int       `json:"upgradeAuthorizers,omitempty"`
	UpgradeTimeBuckets int       `json:"upgradeTimeBuckets,omitempty"`
	TotalUpgradeKeys   int       `json:"totalUpgradeKeys,omitempty"`
}

// persistIndices saves indices to storage
func (idx *DeploymentIndex) persistIndices() error {
	// Persist contract index
	contractIndexData, err := json.Marshal(IndexData{
		ContractIndex: idx.contractIndex,
		LastUpdated:   common.ConsensusNow().Unix(),
	})
	if err != nil {
		return fmt.Errorf("failed to marshal contract index: %w", err)
	}
	if err := idx.store.SaveState([]byte("deployment_contract_index"), contractIndexData); err != nil {
		return fmt.Errorf("failed to persist contract index: %w", err)
	}

	// Persist time index
	timeIndexData, err := json.Marshal(IndexData{
		TimeIndex:   idx.timeIndex,
		LastUpdated: common.ConsensusNow().Unix(),
	})
	if err != nil {
		return fmt.Errorf("failed to marshal time index: %w", err)
	}
	if err := idx.store.SaveState([]byte("deployment_time_index"), timeIndexData); err != nil {
		return fmt.Errorf("failed to persist time index: %w", err)
	}

	// Persist deployer index
	deployerIndexData, err := json.Marshal(IndexData{
		DeployerIndex: idx.deployerIndex,
		LastUpdated:   common.ConsensusNow().Unix(),
	})
	if err != nil {
		return fmt.Errorf("failed to marshal deployer index: %w", err)
	}
	if err := idx.store.SaveState([]byte("deployment_deployer_index"), deployerIndexData); err != nil {
		return fmt.Errorf("failed to persist deployer index: %w", err)
	}

	// Persist metadata
	metadata := IndexMetadata{
		Version:     idx.indexVersion,
		LastUpdated: common.ConsensusNow().Unix(),
		Entries:     len(idx.contractIndex),
	}
	metadataData, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}
	if err := idx.store.SaveState([]byte("index:deployment:metadata"), metadataData); err != nil {
		return fmt.Errorf("failed to persist metadata: %w", err)
	}

	return nil
}

// persistUpgradeIndices saves upgrade indices to storage
func (idx *DeploymentIndex) persistUpgradeIndices() error {
	// Persist upgrade contract index
	upgradeContractIndexData, err := json.Marshal(idx.upgradeContractIndex)
	if err != nil {
		return fmt.Errorf("failed to marshal upgrade contract index: %w", err)
	}
	if err := idx.store.SaveState([]byte("index:upgrade:contract"), upgradeContractIndexData); err != nil {
		return fmt.Errorf("failed to persist upgrade contract index: %w", err)
	}

	// Persist upgrade time index
	upgradeTimeIndexData, err := json.Marshal(idx.upgradeTimeIndex)
	if err != nil {
		return fmt.Errorf("failed to marshal upgrade time index: %w", err)
	}
	if err := idx.store.SaveState([]byte("index:upgrade:time"), upgradeTimeIndexData); err != nil {
		return fmt.Errorf("failed to persist upgrade time index: %w", err)
	}

	// Persist upgrade authorizer index
	upgradeAuthIndexData, err := json.Marshal(idx.upgradeAuthIndex)
	if err != nil {
		return fmt.Errorf("failed to marshal upgrade authorizer index: %w", err)
	}
	if err := idx.store.SaveState([]byte("index:upgrade:authorizer"), upgradeAuthIndexData); err != nil {
		return fmt.Errorf("failed to persist upgrade authorizer index: %w", err)
	}

	// Update metadata to include upgrade statistics
	totalUpgradeKeys := 0
	for _, keys := range idx.upgradeContractIndex {
		totalUpgradeKeys += len(keys)
	}

	upgradeMetadata := IndexMetadata{
		Version:          idx.indexVersion,
		LastUpdated:      common.ConsensusNow().Unix(),
		UpgradeEntries:   len(idx.upgradeContractIndex),
		TotalUpgradeKeys: totalUpgradeKeys,
	}
	upgradeMetadataData, err := json.Marshal(upgradeMetadata)
	if err != nil {
		return fmt.Errorf("failed to marshal upgrade metadata: %w", err)
	}
	if err := idx.store.SaveState([]byte("index:upgrade:metadata"), upgradeMetadataData); err != nil {
		return fmt.Errorf("failed to persist upgrade metadata: %w", err)
	}

	return nil
}

// loadPersistedIndices loads indices from storage
func (idx *DeploymentIndex) loadPersistedIndices() error {
	var loadErrors []error

	// Load contract index
	contractIndexData, err := idx.store.GetState([]byte("deployment_contract_index"))
	if err != nil {
		// Log error but continue loading other indices
		idx.logger.WithError(err).Warn("Failed to load contract index from storage")
		loadErrors = append(loadErrors, fmt.Errorf("contract index load: %w", err))
	} else if len(contractIndexData) > 0 {
		var data IndexData
		if err := json.Unmarshal(contractIndexData, &data); err != nil {
			idx.logger.WithError(err).Error("Failed to unmarshal contract index")
			loadErrors = append(loadErrors, fmt.Errorf("contract index unmarshal: %w", err))
		} else {
			idx.contractIndex = data.ContractIndex
			idx.lastRebuild = time.Unix(data.LastUpdated, 0)
		}
	}

	// Load time index
	timeIndexData, err := idx.store.GetState([]byte("deployment_time_index"))
	if err != nil {
		idx.logger.WithError(err).Warn("Failed to load time index from storage")
		loadErrors = append(loadErrors, fmt.Errorf("time index load: %w", err))
	} else if len(timeIndexData) > 0 {
		var data IndexData
		if err := json.Unmarshal(timeIndexData, &data); err != nil {
			idx.logger.WithError(err).Error("Failed to unmarshal time index")
			loadErrors = append(loadErrors, fmt.Errorf("time index unmarshal: %w", err))
		} else {
			idx.timeIndex = data.TimeIndex
			idx.lastRebuild = time.Unix(data.LastUpdated, 0)
		}
	}

	// Load deployer index
	deployerIndexData, err := idx.store.GetState([]byte("deployment_deployer_index"))
	if err != nil {
		idx.logger.WithError(err).Warn("Failed to load deployer index from storage")
		loadErrors = append(loadErrors, fmt.Errorf("deployer index load: %w", err))
	} else if len(deployerIndexData) > 0 {
		var data IndexData
		if err := json.Unmarshal(deployerIndexData, &data); err != nil {
			idx.logger.WithError(err).Error("Failed to unmarshal deployer index")
			loadErrors = append(loadErrors, fmt.Errorf("deployer index unmarshal: %w", err))
		} else {
			idx.deployerIndex = data.DeployerIndex
			idx.lastRebuild = time.Unix(data.LastUpdated, 0)
		}
	}

	// If all indices failed to load, return an error
	if len(loadErrors) == 3 {
		return fmt.Errorf("failed to load any indices: %v", loadErrors)
	}

	// Log partial success if some indices loaded
	if len(loadErrors) > 0 {
		idx.logger.WithFields(logrus.Fields{
			"errors":      len(loadErrors),
			"contracts":   len(idx.contractIndex),
			"deployers":   len(idx.deployerIndex),
			"timeBuckets": len(idx.timeIndex),
		}).Warn("Partial index load completed with errors")
	}

	return nil
}

// generateDeploymentKey generates a unique key for a deployment
func (idx *DeploymentIndex) generateDeploymentKey(contractID string, timestamp time.Time) string {
	return fmt.Sprintf("deployment:%s:%d", contractID, timestamp.UnixNano())
}

// GetIndexStats returns statistics about the indices
func (idx *DeploymentIndex) GetIndexStats() IndexStats {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	totalKeys := 0
	for _, keys := range idx.contractIndex {
		totalKeys += len(keys)
	}

	totalUpgradeKeys := 0
	for _, keys := range idx.upgradeContractIndex {
		totalUpgradeKeys += len(keys)
	}

	return IndexStats{
		Version:            idx.indexVersion,
		LastRebuild:        idx.lastRebuild,
		Contracts:          len(idx.contractIndex),
		Deployers:          len(idx.deployerIndex),
		TimeBuckets:        len(idx.timeIndex),
		TotalKeys:          totalKeys,
		UpgradeContracts:   len(idx.upgradeContractIndex),
		UpgradeAuthorizers: len(idx.upgradeAuthIndex),
		UpgradeTimeBuckets: len(idx.upgradeTimeIndex),
		TotalUpgradeKeys:   totalUpgradeKeys,
	}
}

// loadPersistedUpgradeIndices loads upgrade indices from storage
func (idx *DeploymentIndex) loadPersistedUpgradeIndices() error {
	var loadErrors []error

	// Load upgrade contract index
	upgradeContractData, err := idx.store.GetState([]byte("index:upgrade:contract"))
	if err != nil {
		idx.logger.WithError(err).Warn("Failed to load upgrade contract index from storage")
		loadErrors = append(loadErrors, fmt.Errorf("upgrade contract index load: %w", err))
	} else if len(upgradeContractData) > 0 {
		if err := json.Unmarshal(upgradeContractData, &idx.upgradeContractIndex); err != nil {
			idx.logger.WithError(err).Error("Failed to unmarshal upgrade contract index")
			loadErrors = append(loadErrors, fmt.Errorf("upgrade contract index unmarshal: %w", err))
		}
	}

	// Load upgrade time index
	upgradeTimeData, err := idx.store.GetState([]byte("index:upgrade:time"))
	if err != nil {
		idx.logger.WithError(err).Warn("Failed to load upgrade time index from storage")
		loadErrors = append(loadErrors, fmt.Errorf("upgrade time index load: %w", err))
	} else if len(upgradeTimeData) > 0 {
		if err := json.Unmarshal(upgradeTimeData, &idx.upgradeTimeIndex); err != nil {
			idx.logger.WithError(err).Error("Failed to unmarshal upgrade time index")
			loadErrors = append(loadErrors, fmt.Errorf("upgrade time index unmarshal: %w", err))
		}
	}

	// Load upgrade authorizer index
	upgradeAuthData, err := idx.store.GetState([]byte("index:upgrade:authorizer"))
	if err != nil {
		idx.logger.WithError(err).Warn("Failed to load upgrade authorizer index from storage")
		loadErrors = append(loadErrors, fmt.Errorf("upgrade authorizer index load: %w", err))
	} else if len(upgradeAuthData) > 0 {
		if err := json.Unmarshal(upgradeAuthData, &idx.upgradeAuthIndex); err != nil {
			idx.logger.WithError(err).Error("Failed to unmarshal upgrade authorizer index")
			loadErrors = append(loadErrors, fmt.Errorf("upgrade authorizer index unmarshal: %w", err))
		}
	}

	// If all upgrade indices failed to load, return an error
	if len(loadErrors) == 3 {
		return fmt.Errorf("failed to load any upgrade indices: %v", loadErrors)
	}

	// Log partial success if some indices loaded
	if len(loadErrors) > 0 {
		idx.logger.WithFields(logrus.Fields{
			"errors":             len(loadErrors),
			"upgradeContracts":   len(idx.upgradeContractIndex),
			"upgradeAuthorizers": len(idx.upgradeAuthIndex),
			"upgradeTimeBuckets": len(idx.upgradeTimeIndex),
		}).Warn("Partial upgrade index load completed with errors")
	}

	// Calculate and log total upgrade keys
	totalUpgradeKeys := 0
	for _, keys := range idx.upgradeContractIndex {
		totalUpgradeKeys += len(keys)
	}

	idx.logger.WithFields(logrus.Fields{
		"upgradeContracts":   len(idx.upgradeContractIndex),
		"upgradeAuthorizers": len(idx.upgradeAuthIndex),
		"totalUpgradeKeys":   totalUpgradeKeys,
	}).Info("Upgrade indices loaded successfully")

	return nil
}

// PruneOldEntries removes index entries older than the specified duration
func (idx *DeploymentIndex) PruneOldEntries(olderThan time.Duration) (int, error) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	cutoffTime := common.ConsensusNow().Add(-olderThan)
	cutoffBucket := cutoffTime.Unix() / 3600 * 3600

	pruned := 0

	// Prune deployment time buckets
	for bucket := range idx.timeIndex {
		if bucket < cutoffBucket {
			pruned += len(idx.timeIndex[bucket])
			delete(idx.timeIndex, bucket)
		}
	}

	// Prune upgrade time buckets
	for bucket := range idx.upgradeTimeIndex {
		if bucket < cutoffBucket {
			pruned += len(idx.upgradeTimeIndex[bucket])
			delete(idx.upgradeTimeIndex, bucket)
		}
	}

	// Persist updated indices
	if pruned > 0 {
		if err := idx.persistIndices(); err != nil {
			return pruned, fmt.Errorf("failed to persist deployment indices after pruning: %w", err)
		}
		if err := idx.persistUpgradeIndices(); err != nil {
			return pruned, fmt.Errorf("failed to persist upgrade indices after pruning: %w", err)
		}
	}

	idx.logger.WithFields(logrus.Fields{
		"pruned":      pruned,
		"cutoff":      cutoffTime,
		"olderThan":   olderThan,
		"cutoffEpoch": cutoffBucket,
	}).Info("Pruned old deployment and upgrade index entries with enterprise-grade cleanup")

	return pruned, nil
}
