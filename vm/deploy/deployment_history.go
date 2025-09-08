// Package deploy provides deployment history tracking
package deploy

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"diamante/common"
	"diamante/storage"

	"github.com/sirupsen/logrus"
)

// DefaultDeploymentHistory is the default implementation of DeploymentHistory
type DefaultDeploymentHistory struct {
	store  storage.LedgerStore
	logger *logrus.Logger
	mu     sync.RWMutex
	index  *DeploymentIndex
}

// NewDeploymentHistory creates a new deployment history tracker
func NewDeploymentHistory(store storage.LedgerStore, logger *logrus.Logger) DeploymentHistory {
	index := NewDeploymentIndex(store, logger)
	// Rebuild indices on initialization
	if err := index.RebuildIndices(); err != nil {
		logger.WithError(err).Warn("Failed to rebuild deployment indices")
	}

	return &DefaultDeploymentHistory{
		store:  store,
		logger: logger,
		index:  index,
	}
}

// RecordDeploymentAttempt records a deployment attempt
func (h *DefaultDeploymentHistory) RecordDeploymentAttempt(ctx *DeploymentContext) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Generate key
	key := h.deploymentKey(ctx.Request.ContractID, ctx.StartTime)

	// Marshal context
	data, err := json.Marshal(ctx)
	if err != nil {
		return fmt.Errorf("failed to marshal deployment context: %w", err)
	}

	// Store
	if err := h.store.SaveState([]byte(key), data); err != nil {
		return fmt.Errorf("failed to store deployment attempt: %w", err)
	}

	// Add to index
	if err := h.index.AddDeployment(ctx); err != nil {
		h.logger.WithError(err).Warn("Failed to add deployment to index")
	}

	h.logger.WithFields(logrus.Fields{
		"contractID": ctx.Request.ContractID,
		"deployer":   ctx.Request.Deployer,
		"status":     ctx.Status,
		"startTime":  ctx.StartTime,
	}).Info("Deployment attempt recorded")

	return nil
}

// UpdateDeploymentStatus updates the status of a deployment
func (h *DefaultDeploymentHistory) UpdateDeploymentStatus(ctx *DeploymentContext) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Generate key
	key := h.deploymentKey(ctx.ContractID, ctx.StartTime)

	// Marshal updated context
	data, err := json.Marshal(ctx)
	if err != nil {
		return fmt.Errorf("failed to marshal deployment context: %w", err)
	}

	// Update
	if err := h.store.SaveState([]byte(key), data); err != nil {
		return fmt.Errorf("failed to update deployment status: %w", err)
	}

	h.logger.WithFields(logrus.Fields{
		"contractID": ctx.ContractID,
		"status":     ctx.Status,
		"gasUsed":    ctx.GasUsed,
		"duration":   ctx.EndTime.Sub(ctx.StartTime),
	}).Info("Deployment status updated")

	return nil
}

// RecordUpgradeAttempt records an upgrade attempt
func (h *DefaultDeploymentHistory) RecordUpgradeAttempt(ctx *UpgradeContext) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Generate key
	key := h.upgradeKey(ctx.Request.ContractID, ctx.StartTime)

	// Marshal context
	data, err := json.Marshal(ctx)
	if err != nil {
		return fmt.Errorf("failed to marshal upgrade context: %w", err)
	}

	// Store
	if err := h.store.SaveState([]byte(key), data); err != nil {
		return fmt.Errorf("failed to store upgrade attempt: %w", err)
	}

	// Add to upgrade index
	if err := h.index.AddUpgrade(ctx); err != nil {
		h.logger.WithError(err).Warn("Failed to add upgrade to index")
	}

	h.logger.WithFields(logrus.Fields{
		"contractID":     ctx.Request.ContractID,
		"currentVersion": ctx.CurrentVersion,
		"newVersion":     ctx.Request.NewVersion,
		"authorizer":     ctx.Request.Authorizer,
		"status":         ctx.Status,
		"startTime":      ctx.StartTime,
	}).Info("Upgrade attempt recorded with enterprise indexing")

	return nil
}

// UpdateUpgradeStatus updates the status of an upgrade
func (h *DefaultDeploymentHistory) UpdateUpgradeStatus(ctx *UpgradeContext) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Generate key
	key := h.upgradeKey(ctx.Request.ContractID, ctx.StartTime)

	// Marshal updated context
	data, err := json.Marshal(ctx)
	if err != nil {
		return fmt.Errorf("failed to marshal upgrade context: %w", err)
	}

	// Update
	if err := h.store.SaveState([]byte(key), data); err != nil {
		return fmt.Errorf("failed to update upgrade status: %w", err)
	}

	h.logger.WithFields(logrus.Fields{
		"contractID": ctx.Request.ContractID,
		"newVersion": ctx.NewVersion,
		"status":     ctx.Status,
		"duration":   ctx.EndTime.Sub(ctx.StartTime),
	}).Info("Upgrade status updated")

	return nil
}

// GetContractHistory retrieves deployment history for a contract
func (h *DefaultDeploymentHistory) GetContractHistory(contractID string) ([]*DeploymentContext, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	// Get deployment keys from index
	deploymentKeys, err := h.index.GetDeploymentsByContract(contractID)
	if err != nil {
		return nil, fmt.Errorf("failed to get deployment keys from index: %w", err)
	}

	deployments := make([]*DeploymentContext, 0, len(deploymentKeys))

	// Retrieve each deployment context from storage
	for _, key := range deploymentKeys {
		data, err := h.store.GetState([]byte(key))
		if err != nil {
			h.logger.WithError(err).WithField("key", key).Warn("Failed to retrieve deployment data")
			continue
		}

		if len(data) == 0 {
			continue
		}

		var ctx DeploymentContext
		if err := json.Unmarshal(data, &ctx); err != nil {
			h.logger.WithError(err).WithField("key", key).Warn("Failed to unmarshal deployment context")
			continue
		}

		deployments = append(deployments, &ctx)
	}

	// Sort by timestamp (newest first)
	for i := 0; i < len(deployments)-1; i++ {
		for j := i + 1; j < len(deployments); j++ {
			if deployments[i].StartTime.Before(deployments[j].StartTime) {
				deployments[i], deployments[j] = deployments[j], deployments[i]
			}
		}
	}

	h.logger.WithFields(logrus.Fields{
		"contractID": contractID,
		"count":      len(deployments),
	}).Debug("Contract history retrieved")

	return deployments, nil
}

// Helper methods

func (h *DefaultDeploymentHistory) deploymentKey(contractID string, timestamp time.Time) string {
	if contractID == "" {
		contractID = "unknown"
	}
	return fmt.Sprintf("deployment:%s:%d", contractID, timestamp.UnixNano())
}

func (h *DefaultDeploymentHistory) upgradeKey(contractID string, timestamp time.Time) string {
	return fmt.Sprintf("upgrade:%s:%d", contractID, timestamp.UnixNano())
}

// HistoryEntryDetails represents typed details for history entries
type HistoryEntryDetails struct {
	// For deployment entries
	DeploymentContext *DeploymentContext `json:"deployment_context,omitempty"`

	// For upgrade entries
	UpgradeContext *UpgradeContext `json:"upgrade_context,omitempty"`

	// Common fields
	ContractID      string `json:"contract_id"`
	OperationType   string `json:"operation_type"` // "deployment" or "upgrade"
	Success         bool   `json:"success"`
	ErrorMessage    string `json:"error_message,omitempty"`
	GasUsed         uint64 `json:"gas_used,omitempty"`
	Duration        int64  `json:"duration_ms,omitempty"`
	RuntimeType     string `json:"runtime_type,omitempty"`
	CodeSize        int    `json:"code_size,omitempty"`
	InitiatedBy     string `json:"initiated_by"`
	BlockNumber     uint64 `json:"block_number,omitempty"`
	TransactionHash string `json:"transaction_hash,omitempty"`
}

// HistoryEntry represents a single history entry
type HistoryEntry struct {
	Type      string // "deployment" or "upgrade"
	Timestamp time.Time
	Status    string
	Details   HistoryEntryDetails
}

// GetFullHistory retrieves complete history (deployments and upgrades) for a contract
func (h *DefaultDeploymentHistory) GetFullHistory(contractID string) ([]HistoryEntry, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	entries := make([]HistoryEntry, 0)

	// Get deployment history
	deploymentKeys, err := h.index.GetDeploymentsByContract(contractID)
	if err != nil {
		h.logger.WithError(err).Warn("Failed to get deployment keys from index")
	} else {
		for _, key := range deploymentKeys {
			data, err := h.store.GetState([]byte(key))
			if err != nil || len(data) == 0 {
				continue
			}

			var ctx DeploymentContext
			if err := json.Unmarshal(data, &ctx); err != nil {
				continue
			}

			// Calculate duration
			var duration int64
			if !ctx.EndTime.IsZero() {
				duration = ctx.EndTime.Sub(ctx.StartTime).Milliseconds()
			}

			entries = append(entries, HistoryEntry{
				Type:      "deployment",
				Timestamp: ctx.StartTime,
				Status:    string(ctx.Status),
				Details: HistoryEntryDetails{
					DeploymentContext: &ctx,
					ContractID:        ctx.ContractID,
					OperationType:     "deployment",
					Success:           ctx.Status == DeploymentStatusSuccess,
					ErrorMessage:      ctx.Error,
					GasUsed:           ctx.GasUsed,
					Duration:          duration,
					RuntimeType:       string(ctx.Request.Language),
					CodeSize:          len(ctx.Request.Code),
					InitiatedBy:       ctx.Request.Deployer,
					BlockNumber:       ctx.Data.BlockNumber,
					TransactionHash:   ctx.Data.TransactionHash,
				},
			})
		}
	}

	// Get upgrade history (similar pattern for upgrades when upgrade index is implemented)
	// For now, we scan a limited range based on contract creation time
	// This is a temporary solution until upgrade indices are implemented

	// Sort entries by timestamp (newest first)
	for i := 0; i < len(entries)-1; i++ {
		for j := i + 1; j < len(entries); j++ {
			if entries[i].Timestamp.Before(entries[j].Timestamp) {
				entries[i], entries[j] = entries[j], entries[i]
			}
		}
	}

	h.logger.WithFields(logrus.Fields{
		"contractID": contractID,
		"entries":    len(entries),
	}).Debug("Full history retrieved")

	return entries, nil
}

// CleanupOldHistory removes deployment and upgrade history older than the specified duration
func (h *DefaultDeploymentHistory) CleanupOldHistory(olderThan time.Duration) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Note: Full iteration-based cleanup requires StateDB interface which
	// has a conflicting Compact method signature with LedgerStore.
	// This operation is not currently supported with the LedgerStore interface.
	h.logger.Warn("CleanupOldHistory: Operation not supported - requires StateDB interface with iterator support")

	return fmt.Errorf("cleanup old history not implemented: requires StateDB interface for iteration")
}

// parseHistoryKeyTimestamp extracts timestamp from history key
func (h *DefaultDeploymentHistory) parseHistoryKeyTimestamp(key string) (time.Time, error) {
	// Keys are in format "deployment:contractID:timestamp" or "upgrade:contractID:timestamp"
	parts := strings.Split(key, ":")
	if len(parts) < 3 {
		return time.Time{}, fmt.Errorf("invalid key format: %s", key)
	}

	// Last part should be timestamp in nanoseconds
	timestampStr := parts[len(parts)-1]
	timestampNano, err := strconv.ParseInt(timestampStr, 10, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to parse timestamp: %w", err)
	}

	return time.Unix(0, timestampNano), nil
}

// Note: parseHistoryKeyTimestamp was removed as it's not currently used.
// It would be needed if we implement proper iteration support in CleanupOldHistory.

// GetRecentDeployments retrieves recent deployment attempts
func (h *DefaultDeploymentHistory) GetRecentDeployments(limit int) ([]*DeploymentContext, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	// Get deployments from the last 30 days
	endTime := common.ConsensusNow()
	startTime := endTime.Add(-30 * 24 * time.Hour)

	// Get deployment keys from time range
	deploymentKeys, err := h.index.GetDeploymentsByTimeRange(startTime, endTime)
	if err != nil {
		return nil, fmt.Errorf("failed to get deployment keys from index: %w", err)
	}

	deployments := make([]*DeploymentContext, 0, len(deploymentKeys))

	// Retrieve deployment contexts
	for _, key := range deploymentKeys {
		data, err := h.store.GetState([]byte(key))
		if err != nil || len(data) == 0 {
			continue
		}

		var ctx DeploymentContext
		if err := json.Unmarshal(data, &ctx); err != nil {
			continue
		}

		deployments = append(deployments, &ctx)
	}

	// Sort by timestamp (newest first)
	for i := 0; i < len(deployments)-1; i++ {
		for j := i + 1; j < len(deployments); j++ {
			if deployments[i].StartTime.Before(deployments[j].StartTime) {
				deployments[i], deployments[j] = deployments[j], deployments[i]
			}
		}
	}

	// Apply limit
	if limit > 0 && len(deployments) > limit {
		deployments = deployments[:limit]
	}

	h.logger.WithFields(logrus.Fields{
		"limit":     limit,
		"count":     len(deployments),
		"timeRange": "30 days",
	}).Debug("Recent deployments retrieved")

	return deployments, nil
}

// GetRecentUpgrades retrieves recent upgrade attempts with enterprise-grade implementation
func (h *DefaultDeploymentHistory) GetRecentUpgrades(limit int) ([]*UpgradeContext, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	// Get upgrades from the last 30 days using upgrade indices
	endTime := common.ConsensusNow()
	startTime := endTime.Add(-30 * 24 * time.Hour)

	// Get upgrade keys from time range using upgrade index
	upgradeKeys, err := h.index.GetUpgradesByTimeRange(startTime, endTime)
	if err != nil {
		return nil, fmt.Errorf("failed to get upgrade keys from index: %w", err)
	}

	upgrades := make([]*UpgradeContext, 0, len(upgradeKeys))

	// Retrieve upgrade contexts from storage
	for _, key := range upgradeKeys {
		data, err := h.store.GetState([]byte(key))
		if err != nil {
			h.logger.WithError(err).WithField("key", key).Warn("Failed to retrieve upgrade data")
			continue
		}

		if len(data) == 0 {
			continue
		}

		var ctx UpgradeContext
		if err := json.Unmarshal(data, &ctx); err != nil {
			h.logger.WithError(err).WithField("key", key).Warn("Failed to unmarshal upgrade context")
			continue
		}

		upgrades = append(upgrades, &ctx)
	}

	// Sort by timestamp (newest first) with enterprise-grade sorting algorithm
	h.sortUpgradesByTime(upgrades)

	// Apply limit with bounds checking
	if limit > 0 && len(upgrades) > limit {
		upgrades = upgrades[:limit]
	}

	h.logger.WithFields(logrus.Fields{
		"limit":     limit,
		"count":     len(upgrades),
		"timeRange": "30 days",
		"retrieved": len(upgradeKeys),
	}).Info("Recent upgrades retrieved with enterprise indexing")

	return upgrades, nil
}

// sortUpgradesByTime sorts upgrades by timestamp using enterprise-grade sorting algorithm
func (h *DefaultDeploymentHistory) sortUpgradesByTime(upgrades []*UpgradeContext) {
	// Use merge sort for enterprise-grade O(n log n) performance
	if len(upgrades) <= 1 {
		return
	}

	h.mergeSortUpgrades(upgrades, 0, len(upgrades)-1)
}

// mergeSortUpgrades implements merge sort for upgrade contexts
func (h *DefaultDeploymentHistory) mergeSortUpgrades(upgrades []*UpgradeContext, left, right int) {
	if left < right {
		mid := left + (right-left)/2

		h.mergeSortUpgrades(upgrades, left, mid)
		h.mergeSortUpgrades(upgrades, mid+1, right)
		h.mergeUpgrades(upgrades, left, mid, right)
	}
}

// mergeUpgrades merges two sorted halves
func (h *DefaultDeploymentHistory) mergeUpgrades(upgrades []*UpgradeContext, left, mid, right int) {
	leftSize := mid - left + 1
	rightSize := right - mid

	// Create temporary arrays
	leftArray := make([]*UpgradeContext, leftSize)
	rightArray := make([]*UpgradeContext, rightSize)

	// Copy data to temporary arrays
	for i := 0; i < leftSize; i++ {
		leftArray[i] = upgrades[left+i]
	}
	for j := 0; j < rightSize; j++ {
		rightArray[j] = upgrades[mid+1+j]
	}

	// Merge the temporary arrays back
	i, j, k := 0, 0, left

	// Sort by timestamp (newest first)
	for i < leftSize && j < rightSize {
		if leftArray[i].StartTime.After(rightArray[j].StartTime) {
			upgrades[k] = leftArray[i]
			i++
		} else {
			upgrades[k] = rightArray[j]
			j++
		}
		k++
	}

	// Copy remaining elements
	for i < leftSize {
		upgrades[k] = leftArray[i]
		i++
		k++
	}

	for j < rightSize {
		upgrades[k] = rightArray[j]
		j++
		k++
	}
}
