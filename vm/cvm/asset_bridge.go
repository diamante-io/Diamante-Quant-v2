package cvm

import (
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

// AssetBridge manages cross-VM asset transfers
type AssetBridge struct {
	lockedAssets  map[string]*LockInfo          // key: assetID + amount + txID
	wrappedAssets map[VMType]map[string]Address // VM -> assetID -> wrapped contract address
	validators    map[VMType]AssetValidator
	mu            sync.RWMutex
	logger        *logrus.Logger

	// Configuration
	lockTimeout time.Duration
}

// AssetValidator validates asset operations for a specific VM
type AssetValidator interface {
	ValidateAsset(assetID AssetID, amount uint64) error
	GetAssetInfo(assetID AssetID) (AssetInfo, error)
	VerifyOwnership(assetID AssetID, owner Address) error
}

// AssetInfo contains information about an asset
type AssetInfo struct {
	ID            AssetID
	Name          string
	Symbol        string
	Decimals      uint8
	TotalSupply   uint64
	OriginVM      VMType
	IsWrapped     bool
	OriginalAsset *AssetID // For wrapped assets
}

// NewAssetBridge creates a new asset bridge
func NewAssetBridge(logger *logrus.Logger) *AssetBridge {
	return &AssetBridge{
		lockedAssets:  make(map[string]*LockInfo),
		wrappedAssets: make(map[VMType]map[string]Address),
		validators:    make(map[VMType]AssetValidator),
		logger:        logger,
		lockTimeout:   5 * time.Minute,
	}
}

// RegisterValidator registers an asset validator for a VM type
func (b *AssetBridge) RegisterValidator(vmType VMType, validator AssetValidator) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.validators[vmType]; exists {
		return fmt.Errorf("validator already registered for VM %s", vmType)
	}

	b.validators[vmType] = validator
	b.wrappedAssets[vmType] = make(map[string]Address)

	return nil
}

// LockAsset locks an asset for cross-VM transfer
func (b *AssetBridge) LockAsset(assetID AssetID, amount uint64, from Address, to Address, txID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Validate asset
	validator, exists := b.validators[from.VM]
	if !exists {
		return fmt.Errorf("no validator for source VM %s", from.VM)
	}

	if err := validator.ValidateAsset(assetID, amount); err != nil {
		return fmt.Errorf("asset validation failed: %w", err)
	}

	// Verify ownership
	if err := validator.VerifyOwnership(assetID, from); err != nil {
		return fmt.Errorf("ownership verification failed: %w", err)
	}

	// Create lock key
	lockKey := b.createLockKey(assetID, amount, txID)

	// Check if already locked
	if _, exists := b.lockedAssets[lockKey]; exists {
		return fmt.Errorf("asset already locked for transaction %s", txID)
	}

	// Create lock
	lock := &LockInfo{
		AssetID:       assetID,
		Amount:        amount,
		LockedBy:      from,
		LockedFor:     to,
		LockTime:      time.Now(),
		UnlockTime:    time.Now().Add(b.lockTimeout),
		TransactionID: txID,
	}

	b.lockedAssets[lockKey] = lock

	b.logger.Infof("Locked asset %s amount %d for transaction %s", hex.EncodeToString(assetID[:]), amount, txID)

	// Start lock timeout monitor
	go b.monitorLockTimeout(lockKey)

	return nil
}

// TransferAsset completes the asset transfer
func (b *AssetBridge) TransferAsset(assetID AssetID, amount uint64, from Address, to Address, txID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Find and verify lock
	lockKey := b.createLockKey(assetID, amount, txID)
	lock, exists := b.lockedAssets[lockKey]
	if !exists {
		return fmt.Errorf("no lock found for asset transfer in transaction %s", txID)
	}

	// Verify lock matches transfer
	if !addressEquals(lock.LockedBy, from) || !addressEquals(lock.LockedFor, to) {
		return fmt.Errorf("lock mismatch: locked by %s for %s, but transfer from %s to %s",
			lock.LockedBy, lock.LockedFor, from, to)
	}

	// Perform the transfer based on VM types
	if from.VM == to.VM {
		// Same VM transfer - simple case
		b.logger.Infof("Same-VM transfer of asset %s from %s to %s",
			hex.EncodeToString(assetID[:]), from, to)
	} else {
		// Cross-VM transfer - need to handle wrapping/unwrapping
		if err := b.handleCrossVMTransfer(assetID, amount, from, to); err != nil {
			return fmt.Errorf("cross-VM transfer failed: %w", err)
		}
	}

	// Remove lock
	delete(b.lockedAssets, lockKey)

	b.logger.Infof("Completed asset transfer for transaction %s", txID)
	return nil
}

// UnlockAsset unlocks an asset (used during rollback)
func (b *AssetBridge) UnlockAsset(assetID AssetID, amount uint64, txID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	lockKey := b.createLockKey(assetID, amount, txID)
	_, exists := b.lockedAssets[lockKey]
	if !exists {
		return fmt.Errorf("no lock found for asset in transaction %s", txID)
	}

	// Remove lock
	delete(b.lockedAssets, lockKey)

	b.logger.Infof("Unlocked asset %s for transaction %s", hex.EncodeToString(assetID[:]), txID)
	return nil
}

// handleCrossVMTransfer handles the complexity of cross-VM asset transfers
func (b *AssetBridge) handleCrossVMTransfer(assetID AssetID, amount uint64, from Address, to Address) error {
	// Get asset info
	sourceValidator, exists := b.validators[from.VM]
	if !exists {
		return fmt.Errorf("no validator for source VM %s", from.VM)
	}

	assetInfo, err := sourceValidator.GetAssetInfo(assetID)
	if err != nil {
		return fmt.Errorf("failed to get asset info: %w", err)
	}

	// Check if target VM has a wrapped version of this asset
	wrappedAddr, hasWrapped := b.getWrappedAsset(to.VM, assetID)

	if !hasWrapped {
		// Need to create wrapped asset in target VM
		wrappedAddr, err = b.createWrappedAsset(to.VM, assetInfo)
		if err != nil {
			return fmt.Errorf("failed to create wrapped asset: %w", err)
		}

		// Store wrapped asset mapping
		if b.wrappedAssets[to.VM] == nil {
			b.wrappedAssets[to.VM] = make(map[string]Address)
		}
		b.wrappedAssets[to.VM][hex.EncodeToString(assetID[:])] = wrappedAddr
	}

	b.logger.Infof("Cross-VM transfer: burning in %s, minting in %s (wrapped: %s)",
		from.VM, to.VM, wrappedAddr)

	// The actual burn/mint operations would be handled by the VM executors
	// This is just the bridge logic to track and coordinate

	return nil
}

// createWrappedAsset creates a wrapped version of an asset in target VM
func (b *AssetBridge) createWrappedAsset(targetVM VMType, assetInfo AssetInfo) (Address, error) {
	// This would interact with the target VM to deploy a wrapped asset contract
	// For now, return a placeholder address

	// Generate deterministic wrapped asset address
	wrappedAddr := Address{
		VM:      targetVM,
		Address: []byte(fmt.Sprintf("wrapped_%s_%s", assetInfo.OriginVM, hex.EncodeToString(assetInfo.ID[:8]))),
	}

	b.logger.Infof("Created wrapped asset %s in VM %s", assetInfo.Name, targetVM)

	return wrappedAddr, nil
}

// getWrappedAsset returns the wrapped asset address if it exists
func (b *AssetBridge) getWrappedAsset(vm VMType, assetID AssetID) (Address, bool) {
	if wrappedAssets, exists := b.wrappedAssets[vm]; exists {
		if addr, exists := wrappedAssets[hex.EncodeToString(assetID[:])]; exists {
			return addr, true
		}
	}
	return Address{}, false
}

// createLockKey creates a unique key for asset locks
func (b *AssetBridge) createLockKey(assetID AssetID, amount uint64, txID string) string {
	return fmt.Sprintf("%s:%d:%s", hex.EncodeToString(assetID[:]), amount, txID)
}

// monitorLockTimeout monitors and releases expired locks
func (b *AssetBridge) monitorLockTimeout(lockKey string) {
	time.Sleep(b.lockTimeout)

	b.mu.Lock()
	defer b.mu.Unlock()

	if lock, exists := b.lockedAssets[lockKey]; exists {
		if time.Now().After(lock.UnlockTime) {
			delete(b.lockedAssets, lockKey)
			b.logger.Warnf("Released expired lock for asset %s in transaction %s",
				hex.EncodeToString(lock.AssetID[:]), lock.TransactionID)
		}
	}
}

// GetLockedAssets returns all currently locked assets
func (b *AssetBridge) GetLockedAssets() []*LockInfo {
	b.mu.RLock()
	defer b.mu.RUnlock()

	locks := make([]*LockInfo, 0, len(b.lockedAssets))
	for _, lock := range b.lockedAssets {
		lockCopy := *lock
		locks = append(locks, &lockCopy)
	}

	return locks
}

// GetLockedAssetCount returns the number of locked assets
func (b *AssetBridge) GetLockedAssetCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return len(b.lockedAssets)
}

// GetWrappedAssets returns all wrapped asset mappings
func (b *AssetBridge) GetWrappedAssets() map[VMType]map[string]Address {
	b.mu.RLock()
	defer b.mu.RUnlock()

	// Deep copy to prevent external modification
	result := make(map[VMType]map[string]Address)
	for vm, assets := range b.wrappedAssets {
		result[vm] = make(map[string]Address)
		for assetID, addr := range assets {
			result[vm][assetID] = addr
		}
	}

	return result
}

// BridgeMetrics provides asset bridge statistics
type BridgeMetrics struct {
	TotalLocks     int64
	ActiveLocks    int
	TotalTransfers int64
	WrappedAssets  int
	ExpiredLocks   int64
}

// GetMetrics returns bridge metrics
func (b *AssetBridge) GetMetrics() BridgeMetrics {
	b.mu.RLock()
	defer b.mu.RUnlock()

	metrics := BridgeMetrics{
		ActiveLocks: len(b.lockedAssets),
	}

	// Count wrapped assets across all VMs
	for _, wrappedInVM := range b.wrappedAssets {
		metrics.WrappedAssets += len(wrappedInVM)
	}

	// TODO: Track total locks, transfers, and expired locks

	return metrics
}
