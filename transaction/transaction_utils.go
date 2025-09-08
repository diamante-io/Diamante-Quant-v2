package transaction

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"diamante/common"
	"diamante/consensus"
	"diamante/crypto"
	"github.com/sirupsen/logrus"
)

var (
	// logger is the package-level logger for transaction utilities
	logger = logrus.WithField("package", "transaction")

	// ErrNonceReuse indicates an attempt to reuse a nonce value
	ErrNonceReuse = errors.New("nonce reuse detected")

	// ErrNonceSequenceViolation indicates a gap in the nonce sequence
	ErrNonceSequenceViolation = errors.New("nonce sequence violation")

	// ErrInvalidSignature indicates a transaction signature is invalid
	ErrInvalidSignature = errors.New("invalid transaction signature")

	// ErrMissingPublicKey indicates no public key found for signature verification
	ErrMissingPublicKey = errors.New("missing public key for signature verification")
)

// TransactionValidationError is a custom error for transaction validation.
type TransactionValidationError struct {
	Message string
	Code    string
}

func (e *TransactionValidationError) Error() string {
	if e.Code != "" {
		return fmt.Sprintf("[%s] %s", e.Code, e.Message)
	}
	return e.Message
}

// ValidationErrorCode defines standard error codes for transaction validation
type ValidationErrorCode string

const (
	ErrCodeInsufficientFunds  ValidationErrorCode = "INSUFFICIENT_FUNDS"
	ErrCodeInvalidAmount      ValidationErrorCode = "INVALID_AMOUNT"
	ErrCodeInvalidFee         ValidationErrorCode = "INVALID_FEE"
	ErrCodeInvalidSender      ValidationErrorCode = "INVALID_SENDER"
	ErrCodeInvalidReceiver    ValidationErrorCode = "INVALID_RECEIVER"
	ErrCodeInvalidSignature   ValidationErrorCode = "INVALID_SIGNATURE"
	ErrCodeInvalidNonce       ValidationErrorCode = "INVALID_NONCE"
	ErrCodeTransactionExpired ValidationErrorCode = "TRANSACTION_EXPIRED"
)

// NewValidationError creates a transaction validation error with a standard error code
func NewValidationError(message string, code ValidationErrorCode) *TransactionValidationError {
	return &TransactionValidationError{
		Message: message,
		Code:    string(code),
	}
}

// NonceTracker is an interface for tracking account nonces (replay protection).
type NonceTracker interface {
	GetNonce(accountID string) int64
	SetNonce(accountID string, nonce int64)
	ValidateNonce(accountID string, nonce int64) error
	Reset(accountID string)
	GetAllNonces() map[string]int64
}

// DefaultNonceTracker is a basic in-memory map version.
type DefaultNonceTracker struct {
	nonces map[string]int64
	mu     sync.RWMutex
}

// NewDefaultNonceTracker creates a new nonce tracker
func NewDefaultNonceTracker() *DefaultNonceTracker {
	return &DefaultNonceTracker{
		nonces: make(map[string]int64),
		mu:     sync.RWMutex{},
	}
}

// GetNonce retrieves the current nonce for an account
func (nt *DefaultNonceTracker) GetNonce(accountID string) int64 {
	nt.mu.RLock()
	defer nt.mu.RUnlock()
	return nt.nonces[accountID]
}

// SetNonce updates the nonce for an account
func (nt *DefaultNonceTracker) SetNonce(accountID string, nonce int64) {
	nt.mu.Lock()
	defer nt.mu.Unlock()
	nt.nonces[accountID] = nonce
}

// ValidateNonce checks if a nonce is valid for the account
func (nt *DefaultNonceTracker) ValidateNonce(accountID string, nonce int64) error {
	nt.mu.RLock()
	defer nt.mu.RUnlock()

	currentNonce, exists := nt.nonces[accountID]

	// If account doesn't exist yet, nonce must be 1
	if !exists {
		if nonce != 1 {
			return fmt.Errorf("%w: first transaction nonce must be 1, got %d",
				ErrNonceSequenceViolation, nonce)
		}
		return nil
	}

	// Nonce must be greater than current nonce
	if nonce <= currentNonce {
		return fmt.Errorf("%w: got %d, expected > %d",
			ErrNonceReuse, nonce, currentNonce)
	}

	// For now, allow gaps in nonce sequence
	// The transaction manager will enforce maxFutureNonce limit
	// This allows for out-of-order transaction submission

	return nil
}

// Reset clears the nonce for an account
func (nt *DefaultNonceTracker) Reset(accountID string) {
	nt.mu.Lock()
	defer nt.mu.Unlock()
	delete(nt.nonces, accountID)
}

// GetAllNonces returns a copy of all tracked nonces
func (nt *DefaultNonceTracker) GetAllNonces() map[string]int64 {
	nt.mu.RLock()
	defer nt.mu.RUnlock()

	result := make(map[string]int64)
	for acc, nonce := range nt.nonces {
		result[acc] = nonce
	}
	return result
}

// PersistentNonceTracker is an advanced nonce tracker that can recover state
type PersistentNonceTracker struct {
	DefaultNonceTracker
	ledgerAPI   common.LedgerAPI
	initialized bool
}

// NewPersistentNonceTracker creates a nonce tracker that can recover from ledger
func NewPersistentNonceTracker(ledger common.LedgerAPI) *PersistentNonceTracker {
	return &PersistentNonceTracker{
		DefaultNonceTracker: *NewDefaultNonceTracker(),
		ledgerAPI:           ledger,
		initialized:         false,
	}
}

// lazyInitialize performs one-time initialization with ledger data
func (nt *PersistentNonceTracker) lazyInitialize() {
	nt.mu.Lock()
	defer nt.mu.Unlock()

	if nt.initialized {
		return
	}

	// Query the ledger for account nonces
	if nt.ledgerAPI != nil {
		// Get stats from ledger to understand scale
		_, err := nt.ledgerAPI.GetStats()
		if err == nil {
			// Load nonces for accounts with recent activity
			// This would need to be implemented differently as stats doesn't have recentAccounts
			// For now, skip automatic loading
		}
	}

	nt.initialized = true
}

// loadAccountNonce loads the nonce for a specific account from the ledger
func (nt *PersistentNonceTracker) loadAccountNonce(accountID string) {
	if nt.ledgerAPI == nil {
		return
	}

	// Get recent transactions for the account to find the highest nonce
	txs, err := nt.ledgerAPI.GetAccountTransactions(accountID, 10, 0)
	if err != nil {
		return
	}

	var highestNonce int64
	for _, tx := range txs {
		if tx.Sender == accountID && int64(tx.Nonce) > highestNonce {
			highestNonce = int64(tx.Nonce)
		}
	}

	// Only update if we found transactions
	if highestNonce > 0 {
		nt.nonces[accountID] = highestNonce
	}
}

// GetNonce returns the current nonce, initializing from ledger if needed
func (nt *PersistentNonceTracker) GetNonce(accountID string) int64 {
	nt.lazyInitialize()

	nt.mu.RLock()
	nonce, exists := nt.nonces[accountID]
	nt.mu.RUnlock()

	// If we don't have the nonce cached, try to load it from ledger
	if !exists && nt.ledgerAPI != nil {
		nt.mu.Lock()
		// Double-check after acquiring write lock
		if _, stillMissing := nt.nonces[accountID]; stillMissing {
			nt.loadAccountNonce(accountID)
			nonce = nt.nonces[accountID]
		}
		nt.mu.Unlock()
	}

	return nonce
}

// SyncWithLedger synchronizes the nonce tracker with the ledger
func (nt *PersistentNonceTracker) SyncWithLedger() error {
	if nt.ledgerAPI == nil {
		return errors.New("ledger API not available")
	}

	nt.mu.Lock()
	defer nt.mu.Unlock()

	// Get all accounts we're tracking
	accountsToSync := make([]string, 0, len(nt.nonces))
	for accountID := range nt.nonces {
		accountsToSync = append(accountsToSync, accountID)
	}

	// Sync each account
	var syncErrors []error
	for _, accountID := range accountsToSync {
		// Get recent transactions
		txs, err := nt.ledgerAPI.GetAccountTransactions(accountID, 5, 0)
		if err != nil {
			syncErrors = append(syncErrors, fmt.Errorf("failed to sync account %s: %w", accountID, err))
			continue
		}

		// Find highest nonce
		var highestNonce int64
		for _, tx := range txs {
			if tx.Sender == accountID && int64(tx.Nonce) > highestNonce {
				highestNonce = int64(tx.Nonce)
			}
		}

		// Update if ledger has higher nonce
		if highestNonce > nt.nonces[accountID] {
			nt.nonces[accountID] = highestNonce
		}
	}

	if len(syncErrors) > 0 {
		return fmt.Errorf("sync completed with %d errors: %w", len(syncErrors), syncErrors[0])
	}

	return nil
}

// StartPeriodicSync starts periodic synchronization with the ledger
func (nt *PersistentNonceTracker) StartPeriodicSync(interval time.Duration, stopChan <-chan struct{}) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := nt.SyncWithLedger(); err != nil {
				// Log error but continue syncing
				logger.WithError(err).Error("Nonce sync error")
			}
		case <-stopChan:
			return
		}
	}
}

// ValidateNonce checks if a nonce is valid, loading from ledger if needed
func (nt *PersistentNonceTracker) ValidateNonce(accountID string, nonce int64) error {
	// Ensure we have the latest nonce from ledger
	currentNonce := nt.GetNonce(accountID)

	// If account doesn't exist yet, nonce must be 1
	if currentNonce == 0 && nonce != 1 {
		return fmt.Errorf("%w: first transaction nonce must be 1, got %d", ErrNonceSequenceViolation, nonce)
	}

	// Nonce must be greater than current nonce
	if nonce <= currentNonce {
		return fmt.Errorf("%w: got %d, expected > %d", ErrNonceReuse, nonce, currentNonce)
	}

	// For now, allow gaps in nonce sequence
	// The transaction manager will enforce maxFutureNonce limit
	// This allows for out-of-order transaction submission

	return nil
}

// ValidateTransaction performs comprehensive checks on a transaction
func ValidateTransaction(tx common.Transaction, minFee float64, nonceTracker NonceTracker) error {
	// 1) Check amounts
	if tx.Amount <= 0 {
		logger.WithField("txID", tx.ID).Debug("Tx Validation: Amount <= 0")
		return NewValidationError("transaction amount must be positive", ErrCodeInvalidAmount)
	}

	if tx.Fee < minFee {
		return NewValidationError(
			fmt.Sprintf("fee %f below minimum %f", tx.Fee, minFee),
			ErrCodeInvalidFee,
		)
	}

	// 2) Check sender/receiver
	if tx.Sender == "" || tx.Receiver == "" {
		logger.WithField("txID", tx.ID).Debug("Tx Validation: missing sender or receiver")
		return NewValidationError("sender and receiver required",
			ErrCodeInvalidSender)
	}

	// 3) Balance check
	if !common.CheckAccountBalance(tx.Sender, tx.Amount+tx.Fee) {
		logger.WithFields(logrus.Fields{
			"txID":   tx.ID,
			"sender": tx.Sender,
		}).Debug("Tx Validation: insufficient balance")
		return NewValidationError("insufficient funds for transaction",
			ErrCodeInsufficientFunds)
	}

	// 4) Nonce validation
	if err := nonceTracker.ValidateNonce(tx.Sender, int64(tx.Nonce)); err != nil {
		logger.WithFields(logrus.Fields{
			"txID":   tx.ID,
			"sender": tx.Sender,
			"nonce":  tx.Nonce,
			"error":  err,
		}).Debug("Tx Validation: nonce error")
		return NewValidationError(fmt.Sprintf("invalid nonce: %v", err),
			ErrCodeInvalidNonce)
	}

	// 5) Expiration check
	if tx.ExpiryTime > 0 && tx.ExpiryTime < consensus.ConsensusUnix() {
		return NewValidationError("transaction has expired",
			ErrCodeTransactionExpired)
	}

	// 6) Signature verification
	if len(tx.Signature) > 0 {
		// Get sender's public key
		pubKey, err := common.GetPublicKey(tx.Sender)
		if err != nil {
			logger.WithFields(logrus.Fields{
				"txID":   tx.ID,
				"sender": tx.Sender,
				"error":  err,
			}).Debug("Tx Validation: error retrieving pubKey")
			return NewValidationError("sender public key not found",
				ErrCodeInvalidSender)
		}

		if len(pubKey) == 0 {
			return NewValidationError("empty public key for sender",
				ErrCodeInvalidSender)
		}

		// Verify signature
		ok, err := crypto.VerifySignature(pubKey, []byte(tx.ID), tx.Signature)
		if err != nil {
			logger.WithFields(logrus.Fields{
				"txID":  tx.ID,
				"error": err,
			}).Debug("Tx Validation: verifySignature error")
			return NewValidationError(fmt.Sprintf("signature verification error: %v", err),
				ErrCodeInvalidSignature)
		}

		if !ok {
			logger.WithField("txID", tx.ID).Debug("Tx Validation: invalid signature")
			return NewValidationError("invalid signature",
				ErrCodeInvalidSignature)
		}
	}

	logger.WithField("txID", tx.ID).Debug("Transaction validated")
	return nil
}

// GenerateTransactionID creates a deterministic hash for transaction identification
func GenerateTransactionID(sender, receiver string, amount float64) string {
	// Add more entropy with timestamp
	timestamp := consensus.ConsensusUnixNano()

	// Create unique string combining all transaction elements
	raw := fmt.Sprintf("%s:%s:%.8f:%d:%d",
		sender, receiver, amount, timestamp, consensus.ConsensusUnixNano())

	// Hash the combined string with SHA-256
	hash := sha256.Sum256([]byte(raw))

	// Return the first 32 bytes as hex string
	return hex.EncodeToString(hash[:])
}

// ReplayProtectionMiddleware validates transaction nonce to prevent replay attacks
func ReplayProtectionMiddleware(tx common.Transaction, tracker NonceTracker) error {
	currentNonce := tracker.GetNonce(tx.Sender)

	// Nonce must be exactly currentNonce + 1
	if int64(tx.Nonce) != currentNonce+1 {
		return fmt.Errorf("invalid nonce: expected %d, got %d",
			currentNonce+1, tx.Nonce)
	}

	// Update nonce tracker
	tracker.SetNonce(tx.Sender, int64(tx.Nonce))
	return nil
}

// VerifyTransactionSignature verifies the signature on a transaction
func VerifyTransactionSignature(tx *common.Transaction) (bool, error) {
	if tx == nil {
		return false, errors.New("transaction cannot be nil")
	}

	if len(tx.Signature) == 0 {
		return false, errors.New("transaction has no signature")
	}

	// Get the public key for the sender
	pubKey, err := common.GetPublicKey(tx.Sender)
	if err != nil {
		return false, fmt.Errorf("failed to get public key: %w", err)
	}

	if len(pubKey) == 0 {
		return false, ErrMissingPublicKey
	}

	// Verify the signature
	return crypto.VerifySignature(pubKey, []byte(tx.ID), tx.Signature)
}

// EstimateTransactionFee calculates a recommended fee based on transaction size and priority
func EstimateTransactionFee(tx *common.Transaction, baseFee float64, urgent bool) float64 {
	// Basic size estimation
	sizeEstimate := estimateTransactionSizeForPriority(tx)

	// Base fee per KB
	feePerKB := baseFee

	// Apply urgency multiplier
	if urgent {
		feePerKB *= 1.5
	}

	// Calculate fee based on size
	fee := feePerKB * float64(sizeEstimate) / 1024.0

	// Add premium for smart contract interactions
	if tx.SmartContractID != "" {
		fee += baseFee * 0.5
	}

	// Ensure minimum fee
	if fee < baseFee {
		fee = baseFee
	}

	return fee
}

// ValidateTransactionBatch validates multiple transactions as a group
func ValidateTransactionBatch(txs []*common.Transaction, minFee float64, tracker NonceTracker) (map[string]error, []*common.Transaction) {
	if len(txs) == 0 {
		return nil, nil
	}

	results := make(map[string]error)
	validTxs := make([]*common.Transaction, 0, len(txs))

	// Create a temporary nonce tracker for the batch
	tempTracker := NewDefaultNonceTracker()

	// First initialize with current nonces
	allNonces := tracker.GetAllNonces()
	for acc, nonce := range allNonces {
		tempTracker.SetNonce(acc, nonce)
	}

	// Validate each transaction
	for _, tx := range txs {
		err := ValidateTransaction(*tx, minFee, tempTracker)
		if err != nil {
			results[tx.ID] = err
		} else {
			// If valid, update the temp tracker and add to valid list
			tempTracker.SetNonce(tx.Sender, int64(tx.Nonce))
			validTxs = append(validTxs, tx)
		}
	}

	return results, validTxs
}

// SortTransactionsByPriority sorts transactions by their priority score
func SortTransactionsByPriority(txs []*common.Transaction) []*common.Transaction {
	if len(txs) <= 1 {
		return txs
	}

	// Create a copy to avoid modifying the original slice
	sorted := make([]*common.Transaction, len(txs))
	copy(sorted, txs)

	// Sort using multi-criteria priority
	sort.SliceStable(sorted, func(i, j int) bool {
		return compareTransactionPriority(sorted[i], sorted[j])
	})

	return sorted
}

// compareTransactionPriority compares two transactions for priority ordering
// Returns true if tx1 has higher priority than tx2
func compareTransactionPriority(tx1, tx2 *common.Transaction) bool {
	// 1. Transaction type priority (system > governance > contracts > transfers)
	priority1 := getTransactionTypePriority(tx1)
	priority2 := getTransactionTypePriority(tx2)

	if priority1 != priority2 {
		return priority1 > priority2
	}

	// 2. Fee per byte calculation
	fee1 := calculateFeePerByte(tx1)
	fee2 := calculateFeePerByte(tx2)

	// Higher fee per byte gets priority
	if fee1 != fee2 {
		return fee1 > fee2
	}

	// 3. Transaction age (prevent starvation)
	// Older transactions get slight priority boost
	age1 := consensus.ConsensusUnix() - tx1.Timestamp
	age2 := consensus.ConsensusUnix() - tx2.Timestamp

	// If one transaction is significantly older (>1 hour), prioritize it
	if age1 > 3600 && age2 <= 3600 {
		return true
	}
	if age2 > 3600 && age1 <= 3600 {
		return false
	}

	// 4. Nonce ordering for same account
	if tx1.Sender == tx2.Sender {
		return tx1.Nonce < tx2.Nonce
	}

	// 5. Fall back to timestamp for deterministic ordering
	return tx1.Timestamp < tx2.Timestamp
}

// getTransactionTypePriority returns a priority score based on transaction type
func getTransactionTypePriority(tx *common.Transaction) int {
	// Check metadata for transaction type
	if tx.Metadata != nil {
		// Check category field for transaction type
		switch tx.Metadata.Category {
		case "system", "system_update":
			return 4 // Highest priority
		case "governance", "proposal", "vote":
			return 3
		case "contract", "deploy", "execute", "upgrade":
			return 2
		default:
			// Check purpose field as fallback
			if strings.Contains(strings.ToLower(tx.Metadata.Purpose), "system") {
				return 4
			}
			if strings.Contains(strings.ToLower(tx.Metadata.Purpose), "governance") {
				return 3
			}
			if strings.Contains(strings.ToLower(tx.Metadata.Purpose), "contract") {
				return 2
			}
			return 1 // Standard transfers
		}
	}

	// Infer type from transaction properties
	if tx.SmartContractID != "" {
		return 2 // Contract interaction
	}

	// Check if it's a governance transaction based on receiver
	if tx.Receiver != "" && isGovernanceAddress(tx.Receiver) {
		return 3
	}

	return 1 // Default to standard transfer
}

// calculateFeePerByte calculates the fee per byte for a transaction
func calculateFeePerByte(tx *common.Transaction) float64 {
	// Estimate transaction size
	size := estimateTransactionSizeForPriority(tx)
	if size == 0 {
		size = 1 // Prevent division by zero
	}

	return tx.Fee / float64(size)
}

// estimateTransactionSizeForPriority estimates transaction size for priority calculation
func estimateTransactionSizeForPriority(tx *common.Transaction) int {
	// Base size for all transactions
	baseSize := 200

	// Add data size
	dataSize := len(tx.Data)

	// Add metadata size estimate
	metadataSize := 0
	if tx.Metadata != nil {
		// Calculate actual size based on metadata fields
		metadataSize += len(tx.Metadata.Category)
		metadataSize += len(tx.Metadata.Description)
		metadataSize += len(tx.Metadata.Reference)
		metadataSize += len(tx.Metadata.Source)
		metadataSize += len(tx.Metadata.Destination)
		metadataSize += len(tx.Metadata.Purpose)
		for _, tag := range tx.Metadata.Tags {
			metadataSize += len(tag)
		}
	}

	// Add signature size
	signatureSize := len(tx.Signature)

	return baseSize + dataSize + metadataSize + signatureSize
}

// isGovernanceAddress checks if an address is a known governance contract
func isGovernanceAddress(address string) bool {
	// In a real implementation, this would check against known governance addresses
	// For now, use a simple heuristic
	return strings.Contains(strings.ToLower(address), "governance") ||
		strings.Contains(strings.ToLower(address), "voting") ||
		strings.Contains(strings.ToLower(address), "proposal")
}

// SortTransactionBatch sorts a batch of transactions with batching optimization
func SortTransactionBatch(txs []*common.Transaction, maxBatchSize int) [][]*common.Transaction {
	if len(txs) == 0 {
		return nil
	}

	// First, sort all transactions by priority
	sorted := SortTransactionsByPriority(txs)

	// Group by sender to maintain nonce ordering
	senderGroups := make(map[string][]*common.Transaction)
	for _, tx := range sorted {
		senderGroups[tx.Sender] = append(senderGroups[tx.Sender], tx)
	}

	// Sort transactions within each sender group by nonce
	for sender, group := range senderGroups {
		sort.SliceStable(group, func(i, j int) bool {
			return group[i].Nonce < group[j].Nonce
		})
		senderGroups[sender] = group
	}

	// Create batches while maintaining dependencies
	var batches [][]*common.Transaction
	currentBatch := make([]*common.Transaction, 0, maxBatchSize)
	processedNonces := make(map[string]int)

	for len(processedNonces) < len(senderGroups) {
		batchFull := false

		// Try to add one transaction from each sender
		for sender, group := range senderGroups {
			if batchFull {
				break
			}

			// Find the next transaction for this sender
			processed := processedNonces[sender]
			if processed >= len(group) {
				continue // All transactions from this sender are processed
			}

			// Add the next transaction if batch has space
			if len(currentBatch) < maxBatchSize {
				currentBatch = append(currentBatch, group[processed])
				processedNonces[sender] = processed + 1
			} else {
				batchFull = true
			}
		}

		// If we added transactions to the batch, save it
		if len(currentBatch) > 0 {
			batches = append(batches, currentBatch)
			currentBatch = make([]*common.Transaction, 0, maxBatchSize)
		} else {
			// No more transactions to process
			break
		}
	}

	return batches
}

// IsValidTransactionID checks if a string has the correct format for a transaction ID
func IsValidTransactionID(id string) bool {
	// Transaction ID should be a 64-character hex string (32 bytes)
	if len(id) != 64 {
		return false
	}

	// Verify it's valid hex
	_, err := hex.DecodeString(id)
	return err == nil
}
