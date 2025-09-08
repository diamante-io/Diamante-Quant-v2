package cvm

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// ContractRegistry manages cross-VM contract registrations
type ContractRegistry struct {
	contracts map[string]*ContractMetadata // key: address string
	aliases   map[string]Address           // human-readable name -> address
	vmRouting map[string]VMType            // address string -> VM type
	mu        sync.RWMutex
}

// NewContractRegistry creates a new contract registry
func NewContractRegistry() *ContractRegistry {
	return &ContractRegistry{
		contracts: make(map[string]*ContractMetadata),
		aliases:   make(map[string]Address),
		vmRouting: make(map[string]VMType),
	}
}

// RegisterContract registers a new contract in the registry
func (r *ContractRegistry) RegisterContract(metadata *ContractMetadata) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	addrStr := metadata.Address.String()

	// Check if contract already exists
	if _, exists := r.contracts[addrStr]; exists {
		return fmt.Errorf("contract already registered at address %s", addrStr)
	}

	// Validate metadata
	if err := r.validateMetadata(metadata); err != nil {
		return fmt.Errorf("invalid contract metadata: %w", err)
	}

	// Set timestamps
	now := time.Now()
	metadata.CreatedAt = now
	metadata.LastModified = now

	// Register contract
	r.contracts[addrStr] = metadata
	r.vmRouting[addrStr] = metadata.VM

	// Register alias if provided
	if metadata.Name != "" {
		r.aliases[metadata.Name] = metadata.Address
	}

	return nil
}

// GetContract retrieves contract metadata by address
func (r *ContractRegistry) GetContract(addr Address) (*ContractMetadata, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	addrStr := addr.String()
	contract, exists := r.contracts[addrStr]
	if !exists {
		return nil, fmt.Errorf("contract not found at address %s", addrStr)
	}

	// Return a copy to prevent external modification
	copy := *contract
	return &copy, nil
}

// GetContractByAlias retrieves contract metadata by alias
func (r *ContractRegistry) GetContractByAlias(alias string) (*ContractMetadata, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	addr, exists := r.aliases[alias]
	if !exists {
		return nil, fmt.Errorf("no contract found with alias %s", alias)
	}

	return r.GetContract(addr)
}

// UpdateContract updates contract metadata
func (r *ContractRegistry) UpdateContract(addr Address, updates map[string]interface{}) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	addrStr := addr.String()
	contract, exists := r.contracts[addrStr]
	if !exists {
		return fmt.Errorf("contract not found at address %s", addrStr)
	}

	// Apply updates
	for key, value := range updates {
		switch key {
		case "version":
			if v, ok := value.(string); ok {
				contract.Version = v
			}
		case "abi":
			if v, ok := value.(json.RawMessage); ok {
				contract.ABI = v
			}
		case "permissions":
			if v, ok := value.(CrossVMPermissions); ok {
				contract.Permissions = v
			}
		case "owner":
			if v, ok := value.(Address); ok {
				contract.Owner = v
			}
		default:
			return fmt.Errorf("unknown update field: %s", key)
		}
	}

	contract.LastModified = time.Now()
	return nil
}

// RemoveContract removes a contract from the registry
func (r *ContractRegistry) RemoveContract(addr Address) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	addrStr := addr.String()
	contract, exists := r.contracts[addrStr]
	if !exists {
		return fmt.Errorf("contract not found at address %s", addrStr)
	}

	// Remove from all maps
	delete(r.contracts, addrStr)
	delete(r.vmRouting, addrStr)

	// Remove alias if exists
	if contract.Name != "" {
		delete(r.aliases, contract.Name)
	}

	return nil
}

// ListContracts returns all registered contracts
func (r *ContractRegistry) ListContracts() []*ContractMetadata {
	r.mu.RLock()
	defer r.mu.RUnlock()

	contracts := make([]*ContractMetadata, 0, len(r.contracts))
	for _, contract := range r.contracts {
		copy := *contract
		contracts = append(contracts, &copy)
	}

	return contracts
}

// ListContractsByVM returns contracts for a specific VM type
func (r *ContractRegistry) ListContractsByVM(vmType VMType) []*ContractMetadata {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var contracts []*ContractMetadata
	for _, contract := range r.contracts {
		if contract.VM == vmType {
			copy := *contract
			contracts = append(contracts, &copy)
		}
	}

	return contracts
}

// GetVMForAddress returns the VM type for a given address
func (r *ContractRegistry) GetVMForAddress(addr Address) (VMType, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	addrStr := addr.String()
	vmType, exists := r.vmRouting[addrStr]
	if !exists {
		return VMTypeUnknown, fmt.Errorf("no VM routing found for address %s", addrStr)
	}

	return vmType, nil
}

// ResolveAddress resolves an alias to an address
func (r *ContractRegistry) ResolveAddress(aliasOrAddr string) (Address, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// First check if it's an alias
	if addr, exists := r.aliases[aliasOrAddr]; exists {
		return addr, nil
	}

	// If not an alias, assume it's a direct address
	// This is a simplified implementation - in production, you'd parse the address format
	return Address{}, fmt.Errorf("address or alias not found: %s", aliasOrAddr)
}

// validateMetadata validates contract metadata
func (r *ContractRegistry) validateMetadata(metadata *ContractMetadata) error {
	if metadata.VM == VMTypeUnknown {
		return fmt.Errorf("invalid VM type")
	}

	if len(metadata.Address.Address) == 0 {
		return fmt.Errorf("empty contract address")
	}

	if metadata.Version == "" {
		metadata.Version = "1.0.0"
	}

	// Initialize permissions if not set
	if metadata.Permissions.RateLimit == 0 {
		metadata.Permissions.RateLimit = 1000 // Default: 1000 calls per minute
	}

	return nil
}

// ExportRegistry exports the registry to JSON
func (r *ContractRegistry) ExportRegistry() ([]byte, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	data := struct {
		Contracts map[string]*ContractMetadata `json:"contracts"`
		Aliases   map[string]Address           `json:"aliases"`
		Timestamp time.Time                    `json:"timestamp"`
	}{
		Contracts: r.contracts,
		Aliases:   r.aliases,
		Timestamp: time.Now(),
	}

	return json.MarshalIndent(data, "", "  ")
}

// ImportRegistry imports registry data from JSON
func (r *ContractRegistry) ImportRegistry(data []byte) error {
	var imported struct {
		Contracts map[string]*ContractMetadata `json:"contracts"`
		Aliases   map[string]Address           `json:"aliases"`
		Timestamp time.Time                    `json:"timestamp"`
	}

	if err := json.Unmarshal(data, &imported); err != nil {
		return fmt.Errorf("failed to unmarshal registry data: %w", err)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// Clear existing data
	r.contracts = make(map[string]*ContractMetadata)
	r.aliases = make(map[string]Address)
	r.vmRouting = make(map[string]VMType)

	// Import contracts
	for addrStr, contract := range imported.Contracts {
		r.contracts[addrStr] = contract
		r.vmRouting[addrStr] = contract.VM
	}

	// Import aliases
	for alias, addr := range imported.Aliases {
		r.aliases[alias] = addr
	}

	return nil
}

// GetStats returns registry statistics
func (r *ContractRegistry) GetStats() map[string]interface{} {
	r.mu.RLock()
	defer r.mu.RUnlock()

	vmCounts := make(map[string]int)
	for _, contract := range r.contracts {
		vmCounts[contract.VM.String()]++
	}

	return map[string]interface{}{
		"total_contracts": len(r.contracts),
		"total_aliases":   len(r.aliases),
		"contracts_by_vm": vmCounts,
	}
}
