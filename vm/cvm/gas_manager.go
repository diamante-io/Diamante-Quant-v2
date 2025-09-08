package cvm

import (
	"fmt"
	"sync"
	"time"
)

// GasManager manages gas allocation and consumption across VMs
type GasManager struct {
	allocations map[string]*GasAllocation
	prices      map[VMType]GasPrice
	mu          sync.RWMutex

	// Configuration
	defaultGasPrice uint64
	maxGasPerTx     uint64
}

// GasAllocation tracks gas for a transaction
type GasAllocation struct {
	TransactionID string
	TotalLimit    uint64
	Consumed      uint64
	Reserved      uint64
	VMBreakdown   map[VMType]uint64 // Gas used per VM
	StartTime     time.Time
	LastUpdate    time.Time
}

// GasPrice defines gas pricing for a VM
type GasPrice struct {
	BasePrice         uint64  // Base price per gas unit
	ComputeMultiplier float64 // Multiplier for compute operations
	StorageMultiplier float64 // Multiplier for storage operations
	CrossVMMultiplier float64 // Multiplier for cross-VM calls
}

// NewGasManager creates a new gas manager
func NewGasManager() *GasManager {
	gm := &GasManager{
		allocations:     make(map[string]*GasAllocation),
		prices:          make(map[VMType]GasPrice),
		defaultGasPrice: 1,
		maxGasPerTx:     10_000_000, // 10M gas limit per transaction
	}

	// Initialize default gas prices
	gm.initializeGasPrices()

	return gm
}

// initializeGasPrices sets up default gas prices for each VM
func (gm *GasManager) initializeGasPrices() {
	// zkEVM - Most expensive due to proof generation
	gm.prices[VMTypeZKEVM] = GasPrice{
		BasePrice:         2,
		ComputeMultiplier: 1.5,
		StorageMultiplier: 2.0,
		CrossVMMultiplier: 1.2,
	}

	// Chaincode - Moderate cost
	gm.prices[VMTypeChaincode] = GasPrice{
		BasePrice:         1,
		ComputeMultiplier: 1.0,
		StorageMultiplier: 1.5,
		CrossVMMultiplier: 1.1,
	}

	// Native - Most efficient
	gm.prices[VMTypeNative] = GasPrice{
		BasePrice:         1,
		ComputeMultiplier: 0.8,
		StorageMultiplier: 1.0,
		CrossVMMultiplier: 1.0,
	}
}

// AllocateGas allocates gas for a transaction
func (gm *GasManager) AllocateGas(txID string, gasLimit uint64) error {
	gm.mu.Lock()
	defer gm.mu.Unlock()

	// Check if allocation already exists
	if _, exists := gm.allocations[txID]; exists {
		return fmt.Errorf("gas already allocated for transaction %s", txID)
	}

	// Validate gas limit
	if gasLimit == 0 {
		return fmt.Errorf("gas limit must be greater than zero")
	}

	if gasLimit > gm.maxGasPerTx {
		return fmt.Errorf("gas limit %d exceeds maximum %d", gasLimit, gm.maxGasPerTx)
	}

	// Create allocation
	allocation := &GasAllocation{
		TransactionID: txID,
		TotalLimit:    gasLimit,
		Consumed:      0,
		Reserved:      0,
		VMBreakdown:   make(map[VMType]uint64),
		StartTime:     time.Now(),
		LastUpdate:    time.Now(),
	}

	gm.allocations[txID] = allocation
	return nil
}

// ConsumeGas consumes gas from an allocation
func (gm *GasManager) ConsumeGas(txID string, amount uint64) error {
	gm.mu.Lock()
	defer gm.mu.Unlock()

	allocation, exists := gm.allocations[txID]
	if !exists {
		return fmt.Errorf("no gas allocation found for transaction %s", txID)
	}

	// Check if enough gas remains
	totalUsed := allocation.Consumed + allocation.Reserved + amount
	if totalUsed > allocation.TotalLimit {
		return CVMError{
			Code:    ErrCodeInsufficientGas,
			Message: fmt.Sprintf("insufficient gas: need %d, have %d", totalUsed, allocation.TotalLimit),
		}
	}

	allocation.Consumed += amount
	allocation.LastUpdate = time.Now()

	return nil
}

// ConsumeGasForVM consumes gas and tracks per-VM usage
func (gm *GasManager) ConsumeGasForVM(txID string, vm VMType, amount uint64) error {
	gm.mu.Lock()
	defer gm.mu.Unlock()

	allocation, exists := gm.allocations[txID]
	if !exists {
		return fmt.Errorf("no gas allocation found for transaction %s", txID)
	}

	// Apply VM-specific pricing
	price, exists := gm.prices[vm]
	if !exists {
		price = GasPrice{BasePrice: gm.defaultGasPrice}
	}

	adjustedAmount := amount * price.BasePrice

	// Check if enough gas remains
	totalUsed := allocation.Consumed + allocation.Reserved + adjustedAmount
	if totalUsed > allocation.TotalLimit {
		return CVMError{
			Code:    ErrCodeInsufficientGas,
			Message: fmt.Sprintf("insufficient gas: need %d, have %d", totalUsed, allocation.TotalLimit),
		}
	}

	allocation.Consumed += adjustedAmount
	allocation.VMBreakdown[vm] += adjustedAmount
	allocation.LastUpdate = time.Now()

	return nil
}

// ReserveGas reserves gas for future operations
func (gm *GasManager) ReserveGas(txID string, amount uint64) error {
	gm.mu.Lock()
	defer gm.mu.Unlock()

	allocation, exists := gm.allocations[txID]
	if !exists {
		return fmt.Errorf("no gas allocation found for transaction %s", txID)
	}

	// Check if enough gas remains
	totalUsed := allocation.Consumed + allocation.Reserved + amount
	if totalUsed > allocation.TotalLimit {
		return CVMError{
			Code:    ErrCodeInsufficientGas,
			Message: fmt.Sprintf("insufficient gas for reservation: need %d, have %d", totalUsed, allocation.TotalLimit),
		}
	}

	allocation.Reserved += amount
	allocation.LastUpdate = time.Now()

	return nil
}

// ReleaseReservedGas releases previously reserved gas
func (gm *GasManager) ReleaseReservedGas(txID string, amount uint64) error {
	gm.mu.Lock()
	defer gm.mu.Unlock()

	allocation, exists := gm.allocations[txID]
	if !exists {
		return fmt.Errorf("no gas allocation found for transaction %s", txID)
	}

	if amount > allocation.Reserved {
		return fmt.Errorf("cannot release more gas than reserved: have %d, releasing %d",
			allocation.Reserved, amount)
	}

	allocation.Reserved -= amount
	allocation.LastUpdate = time.Now()

	return nil
}

// EstimateGas estimates gas cost for a cross-VM call
func (gm *GasManager) EstimateGas(msg CVMMessage) (uint64, error) {
	gm.mu.RLock()
	defer gm.mu.RUnlock()

	// Base cost for cross-VM communication
	baseCost := uint64(21000)

	// Add cost for source VM
	sourcePrice, exists := gm.prices[msg.SourceVM]
	if !exists {
		sourcePrice = GasPrice{BasePrice: gm.defaultGasPrice}
	}
	baseCost = uint64(float64(baseCost) * sourcePrice.CrossVMMultiplier)

	// Add cost for target VM
	targetPrice, exists := gm.prices[msg.TargetVM]
	if !exists {
		targetPrice = GasPrice{BasePrice: gm.defaultGasPrice}
	}
	baseCost = baseCost * targetPrice.BasePrice

	// Add cost for data
	dataSize := len(msg.Arguments)
	dataCost := uint64(dataSize * 16) // 16 gas per byte

	// Add cost for assets
	assetCost := uint64(len(msg.Assets) * 5000) // 5000 gas per asset transfer

	totalEstimate := baseCost + dataCost + assetCost

	// Apply safety margin
	return uint64(float64(totalEstimate) * 1.2), nil
}

// GetAllocation returns gas allocation for a transaction
func (gm *GasManager) GetAllocation(txID string) (*GasAllocation, error) {
	gm.mu.RLock()
	defer gm.mu.RUnlock()

	allocation, exists := gm.allocations[txID]
	if !exists {
		return nil, fmt.Errorf("no gas allocation found for transaction %s", txID)
	}

	// Return a copy
	copy := *allocation
	copy.VMBreakdown = make(map[VMType]uint64)
	for vm, gas := range allocation.VMBreakdown {
		copy.VMBreakdown[vm] = gas
	}

	return &copy, nil
}

// ReleaseAllocation releases gas allocation after transaction completion
func (gm *GasManager) ReleaseAllocation(txID string) error {
	gm.mu.Lock()
	defer gm.mu.Unlock()

	if _, exists := gm.allocations[txID]; !exists {
		return fmt.Errorf("no gas allocation found for transaction %s", txID)
	}

	delete(gm.allocations, txID)
	return nil
}

// SetGasPrice updates gas price for a VM
func (gm *GasManager) SetGasPrice(vm VMType, price GasPrice) {
	gm.mu.Lock()
	defer gm.mu.Unlock()

	gm.prices[vm] = price
}

// GetGasPrice returns gas price for a VM
func (gm *GasManager) GetGasPrice(vm VMType) GasPrice {
	gm.mu.RLock()
	defer gm.mu.RUnlock()

	price, exists := gm.prices[vm]
	if !exists {
		return GasPrice{BasePrice: gm.defaultGasPrice}
	}

	return price
}

// GasMetrics provides gas usage statistics
type GasMetrics struct {
	ActiveAllocations int
	TotalConsumed     uint64
	TotalReserved     uint64
	VMUsage           map[string]uint64
	AverageGasPerTx   uint64
}

// GetMetrics returns gas usage metrics
func (gm *GasManager) GetMetrics() GasMetrics {
	gm.mu.RLock()
	defer gm.mu.RUnlock()

	metrics := GasMetrics{
		ActiveAllocations: len(gm.allocations),
		VMUsage:           make(map[string]uint64),
	}

	txCount := 0
	for _, allocation := range gm.allocations {
		metrics.TotalConsumed += allocation.Consumed
		metrics.TotalReserved += allocation.Reserved

		if allocation.Consumed > 0 {
			txCount++
		}

		for vm, gas := range allocation.VMBreakdown {
			metrics.VMUsage[vm.String()] += gas
		}
	}

	if txCount > 0 {
		metrics.AverageGasPerTx = metrics.TotalConsumed / uint64(txCount)
	}

	return metrics
}

// CleanupStaleAllocations removes allocations older than the given duration
func (gm *GasManager) CleanupStaleAllocations(maxAge time.Duration) int {
	gm.mu.Lock()
	defer gm.mu.Unlock()

	now := time.Now()
	removed := 0

	for txID, allocation := range gm.allocations {
		if now.Sub(allocation.LastUpdate) > maxAge {
			delete(gm.allocations, txID)
			removed++
		}
	}

	return removed
}
