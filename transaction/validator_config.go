package transaction

import (
	"errors"
	"math/big"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

// ValidatorConfig holds configuration for transaction validation
type ValidatorConfig struct {
	MaxTransactionSize          int
	MinGasPrice                 *big.Int
	MaxGasLimit                 uint64
	EnableSignatureVerification bool
	EnableReplayProtection      bool
	EnableStrictMode            bool
}

// DefaultValidatorConfig returns default validator configuration
func DefaultValidatorConfig() *ValidatorConfig {
	return &ValidatorConfig{
		MaxTransactionSize:          1024 * 1024,            // 1MB
		MinGasPrice:                 big.NewInt(1000000000), // 1 Gwei
		MaxGasLimit:                 uint64(10000000),
		EnableSignatureVerification: true,
		EnableReplayProtection:      true,
		EnableStrictMode:            false,
	}
}

// ValidationResult contains the result of transaction validation
type ValidationResult struct {
	Valid              bool
	TransactionType    TransactionType
	IsContractCreation bool
	EstimatedGas       uint64
	Errors             []string
	Warnings           []string
	ValidationDuration time.Duration
}

// TransactionType represents different types of transactions
type TransactionType int

const (
	TypeTransfer TransactionType = iota
	TypeContractCall
	TypeContractCreation
)

// ValidatorStats holds statistics about validation operations
type ValidatorStats struct {
	TotalValidations    uint64
	ValidTransactions   uint64
	InvalidTransactions uint64
	LastValidationTime  time.Time
}

// Validator handles transaction validation with configurable rules
type Validator struct {
	config  *ValidatorConfig
	logger  *logrus.Logger
	mu      sync.RWMutex
	running bool
	seenTxs map[string]bool // For replay protection
	stats   ValidatorStats
	stopCh  chan struct{}
}

// NewValidator creates a new transaction validator
func NewValidator(config *ValidatorConfig, logger *logrus.Logger) *Validator {
	if config == nil {
		config = DefaultValidatorConfig()
	}
	if logger == nil {
		logger = logrus.New()
	}

	return &Validator{
		config:  config,
		logger:  logger,
		seenTxs: make(map[string]bool),
		stopCh:  make(chan struct{}),
	}
}

// Start starts the validator
func (v *Validator) Start() error {
	v.mu.Lock()
	defer v.mu.Unlock()

	if v.running {
		return ErrAlreadyRunning
	}

	v.running = true
	v.logger.Info("Transaction validator started")
	return nil
}

// Stop stops the validator
func (v *Validator) Stop() error {
	v.mu.Lock()
	defer v.mu.Unlock()

	if !v.running {
		return ErrNotRunning
	}

	v.running = false
	close(v.stopCh)
	v.logger.Info("Transaction validator stopped")
	return nil
}

// Validate validates a single transaction
func (v *Validator) Validate(tx *Transaction) (*ValidationResult, error) {
	v.mu.RLock()
	if !v.running {
		v.mu.RUnlock()
		return nil, ErrNotRunning
	}
	v.mu.RUnlock()

	start := time.Now()
	result := &ValidationResult{
		Valid:    true,
		Errors:   []string{},
		Warnings: []string{},
	}

	// Determine transaction type
	if tx.To == "" && len(tx.Data) > 0 {
		result.TransactionType = TypeContractCreation
		result.IsContractCreation = true
	} else if len(tx.Data) > 0 {
		result.TransactionType = TypeContractCall
	} else {
		result.TransactionType = TypeTransfer
	}

	// Size validation
	if v.config.MaxTransactionSize > 0 {
		// Estimate size (simplified)
		size := len(tx.ID) + len(tx.From) + len(tx.To) + len(tx.Data) + 200 // overhead
		if size > v.config.MaxTransactionSize {
			result.Valid = false
			result.Errors = append(result.Errors, "transaction size exceeds maximum limit")
		}
	}

	// Gas validation
	if v.config.MinGasPrice != nil && tx.GasPrice != nil {
		if tx.GasPrice.Cmp(v.config.MinGasPrice) < 0 {
			result.Valid = false
			result.Errors = append(result.Errors, "gas price below minimum")
		}
	}

	if v.config.MaxGasLimit > 0 && tx.GasLimit > v.config.MaxGasLimit {
		result.Valid = false
		result.Errors = append(result.Errors, "gas limit exceeds maximum")
	}

	// Address validation
	if !isValidAddress(tx.From) {
		result.Valid = false
		result.Errors = append(result.Errors, "invalid from address")
	}

	if tx.To != "" && !isValidAddress(tx.To) {
		result.Valid = false
		result.Errors = append(result.Errors, "invalid to address")
	}

	// Signature verification
	if v.config.EnableSignatureVerification {
		if tx.V == nil || tx.R == nil || tx.S == nil {
			result.Valid = false
			result.Errors = append(result.Errors, "missing signature")
		}
	}

	// Replay protection
	if v.config.EnableReplayProtection {
		v.mu.Lock()
		if v.seenTxs[tx.ID] {
			result.Valid = false
			result.Errors = append(result.Errors, "transaction replay detected")
		} else if result.Valid {
			v.seenTxs[tx.ID] = true
		}
		v.mu.Unlock()
	}

	// Estimate gas
	result.EstimatedGas = estimateGas(tx)

	// Update stats
	v.mu.Lock()
	v.stats.TotalValidations++
	if result.Valid {
		v.stats.ValidTransactions++
	} else {
		v.stats.InvalidTransactions++
	}
	v.stats.LastValidationTime = time.Now()
	v.mu.Unlock()

	result.ValidationDuration = time.Since(start)
	return result, nil
}

// ValidateBatch validates multiple transactions
func (v *Validator) ValidateBatch(txs []*Transaction) ([]*ValidationResult, error) {
	results := make([]*ValidationResult, len(txs))

	for i, tx := range txs {
		result, err := v.Validate(tx)
		if err != nil {
			return nil, err
		}
		results[i] = result
	}

	return results, nil
}

// GetStats returns validator statistics
func (v *Validator) GetStats() *ValidatorStats {
	v.mu.RLock()
	defer v.mu.RUnlock()

	stats := v.stats
	return &stats
}

// Transaction represents a transaction structure compatible with tests
type Transaction struct {
	ID        string
	From      string
	To        string
	Value     *big.Int
	GasPrice  *big.Int
	GasLimit  uint64
	Nonce     uint64
	Data      []byte
	Timestamp time.Time
	V         *big.Int
	R         *big.Int
	S         *big.Int
}

// Helper functions
func isValidAddress(addr string) bool {
	if len(addr) != 42 || addr[:2] != "0x" {
		return false
	}
	// Simple hex check
	for i := 2; i < len(addr); i++ {
		c := addr[i]
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return false
		}
	}
	return true
}

func estimateGas(tx *Transaction) uint64 {
	baseGas := uint64(21000)

	// Add gas for data
	if len(tx.Data) > 0 {
		dataGas := uint64(0)
		for _, b := range tx.Data {
			if b == 0 {
				dataGas += 4
			} else {
				dataGas += 16
			}
		}
		baseGas += dataGas
	}

	// Contract creation needs more gas
	if tx.To == "" && len(tx.Data) > 0 {
		baseGas += uint64(32000)
	}

	return baseGas
}

// Error definitions
var (
	ErrAlreadyRunning = errors.New("validator already running")
	ErrNotRunning     = errors.New("validator not running")
)
