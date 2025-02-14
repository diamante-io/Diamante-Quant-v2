package transaction

import (
	"fmt"
	"log"
	"time"

	"diamante/common"
	"diamante/crypto"
)

// TransactionValidationError is a custom error for transaction validation.
type TransactionValidationError struct {
	Message string
}

func (e *TransactionValidationError) Error() string {
	return e.Message
}

// NonceTracker is an interface for tracking account nonces (replay protection).
type NonceTracker interface {
	GetNonce(accountID string) int
	SetNonce(accountID string, nonce int)
}

// DefaultNonceTracker is a basic in-memory map version.
type DefaultNonceTracker struct {
	nonces map[string]int
}

func NewDefaultNonceTracker() *DefaultNonceTracker {
	return &DefaultNonceTracker{
		nonces: make(map[string]int),
	}
}
func (nt *DefaultNonceTracker) GetNonce(accountID string) int {
	return nt.nonces[accountID]
}
func (nt *DefaultNonceTracker) SetNonce(accountID string, nonce int) {
	nt.nonces[accountID] = nonce
}

// ValidateTransaction does a full set of checks: amount > 0, fee, signature, nonce, etc.
func ValidateTransaction(tx common.Transaction, minFee float64, nonceTracker NonceTracker) error {
	// 1) Check amounts
	if tx.Amount <= 0 {
		log.Println("Tx Validation: Amount <= 0")
		return &TransactionValidationError{"Amount must be > 0"}
	}
	if tx.Fee < minFee {
		return &TransactionValidationError{
			fmt.Sprintf("fee %f < minFee %f", tx.Fee, minFee),
		}
	}
	// 2) Check sender/receiver
	if tx.Sender == "" || tx.Receiver == "" {
		log.Println("Tx Validation: missing sender or receiver")
		return &TransactionValidationError{"Missing sender/receiver"}
	}
	// 3) Balance check
	if !common.CheckAccountBalance(tx.Sender, tx.Amount+tx.Fee) {
		log.Println("Tx Validation: insufficient balance")
		return &TransactionValidationError{"Insufficient balance"}
	}
	// 4) Signature
	pubKey, err := common.GetPublicKey(tx.Sender)
	if err != nil {
		log.Printf("Tx Validation: error retrieving pubKey for %s: %v\n", tx.Sender, err)
		return &TransactionValidationError{"Sender public key not found"}
	}
	ok, err := crypto.VerifySignature(pubKey, []byte(tx.ID), tx.Signature)
	if err != nil {
		log.Printf("Tx Validation: verifySignature error: %v\n", err)
		return &TransactionValidationError{"Signature verification error"}
	}
	if !ok {
		log.Println("Tx Validation: invalid signature")
		return &TransactionValidationError{"Invalid signature"}
	}
	// 5) Nonce
	if err := validateNonce(tx.Sender, tx.Nonce, nonceTracker); err != nil {
		log.Printf("Tx Validation: nonce error => %v\n", err)
		return &TransactionValidationError{fmt.Sprintf("Nonce error: %v", err)}
	}
	log.Printf("Tx %s validated.\n", tx.ID)
	return nil
}

// validateNonce => ensure tx.Nonce > currentNonce
func validateNonce(sender string, nonce int, tracker NonceTracker) error {
	curr := tracker.GetNonce(sender)
	if nonce <= curr {
		return fmt.Errorf("nonce %d <= current %d for %s", nonce, curr, sender)
	}
	tracker.SetNonce(sender, nonce)
	return nil
}

// GenerateTransactionID => simple hash of sender+receiver+amount+timestamp
func GenerateTransactionID(sender, receiver string, amount float64) string {
	t := time.Now().Unix()
	raw := fmt.Sprintf("%s:%s:%f:%d", sender, receiver, amount, t)
	return common.HashData([]byte(raw)) // returns hex-encoded string
}

// ReplayProtectionMiddleware => re-check nonce at finalization time.
func ReplayProtectionMiddleware(tx common.Transaction, tracker NonceTracker) error {
	return validateNonce(tx.Sender, tx.Nonce, tracker)
}
