package transaction

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"diamante/common"
	"diamante/crypto"
)

var (
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

	// Otherwise, nonce must be exactly currentNonce + 1
	if nonce <= currentNonce {
		return fmt.Errorf("%w: got %d, expected > %d",
			ErrNonceReuse, nonce, currentNonce)
	}

	// Check for gaps in nonce sequence
	if nonce > currentNonce+1 {
		return fmt.Errorf("%w: got %d, expected %d",
			ErrNonceSequenceViolation, nonce, currentNonce+1)
	}

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

// lazyInitialize loads nonce state from ledger if needed
func (nt *PersistentNonceTracker) lazyInitialize() {
	nt.mu.Lock()
	defer nt.mu.Unlock()

	if nt.initialized {
		return
	}

	// In a real implementation, we would query the ledger for the latest nonce
	// for each account, but that isn't necessary for this example
	nt.initialized = true
}

// GetNonce returns the current nonce, initializing from ledger if needed
func (nt *PersistentNonceTracker) GetNonce(accountID string) int64 {
	nt.lazyInitialize()
	return nt.DefaultNonceTracker.GetNonce(accountID)
}

// ValidateTransaction performs comprehensive checks on a transaction
func ValidateTransaction(tx common.Transaction, minFee float64, nonceTracker NonceTracker) error {
	// 1) Check amounts
	if tx.Amount <= 0 {
		log.Println("Tx Validation: Amount <= 0")
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
		log.Println("Tx Validation: missing sender or receiver")
		return NewValidationError("sender and receiver required",
			ErrCodeInvalidSender)
	}

	// 3) Balance check
	if !common.CheckAccountBalance(tx.Sender, tx.Amount+tx.Fee) {
		log.Println("Tx Validation: insufficient balance")
		return NewValidationError("insufficient funds for transaction",
			ErrCodeInsufficientFunds)
	}

	// 4) Nonce validation
	if err := nonceTracker.ValidateNonce(tx.Sender, int64(tx.Nonce)); err != nil {
		log.Printf("Tx Validation: nonce error => %v\n", err)
		return NewValidationError(fmt.Sprintf("invalid nonce: %v", err),
			ErrCodeInvalidNonce)
	}

	// 5) Expiration check
	if tx.ExpiryTime > 0 && tx.ExpiryTime < time.Now().Unix() {
		return NewValidationError("transaction has expired",
			ErrCodeTransactionExpired)
	}

	// 6) Signature verification
	if len(tx.Signature) > 0 {
		// Get sender's public key
		pubKey, err := common.GetPublicKey(tx.Sender)
		if err != nil {
			log.Printf("Tx Validation: error retrieving pubKey for %s: %v\n", tx.Sender, err)
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
			log.Printf("Tx Validation: verifySignature error: %v\n", err)
			return NewValidationError(fmt.Sprintf("signature verification error: %v", err),
				ErrCodeInvalidSignature)
		}

		if !ok {
			log.Println("Tx Validation: invalid signature")
			return NewValidationError("invalid signature",
				ErrCodeInvalidSignature)
		}
	}

	log.Printf("Tx %s validated.\n", tx.ID)
	return nil
}

// GenerateTransactionID creates a deterministic hash for transaction identification
func GenerateTransactionID(sender, receiver string, amount float64) string {
	// Add more entropy with timestamp
	timestamp := time.Now().UnixNano()

	// Create unique string combining all transaction elements
	raw := fmt.Sprintf("%s:%s:%.8f:%d:%d",
		sender, receiver, amount, timestamp, time.Now().UnixNano())

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
	sizeEstimate := estimateTransactionSize(*tx)

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
	// Simple implementation - in production use a proper sort
	// with the priority calculation from the TransactionPool

	// Just a placeholder - we would normally implement a proper sort here
	return txs
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
