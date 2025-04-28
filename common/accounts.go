// common/account.go
package common

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"
)

// Account represents a user or entity on the blockchain network.
// In production, the private key is stored encrypted.
type Account struct {
	ID                  string  `json:"id"`
	PublicKey           []byte  `json:"publicKey"`
	EncryptedPrivateKey []byte  `json:"encryptedPrivateKey"` // Encrypted form
	Balance             float64 `json:"balance"`
	MultiSig            bool    `json:"multiSig"`
	HDWalletKey         string  `json:"hdWalletKey"`
	Role                string  `json:"role"`
	KYCStatus           bool    `json:"kycStatus"`
	VotingWeight        int     `json:"votingWeight"`
	Nonce               int     `json:"nonce"` // Used for replay protection and transaction ordering

	// Added fields for enhanced functionality
	CreatedAt          int64   `json:"createdAt"`          // Unix timestamp of account creation
	LastActive         int64   `json:"lastActive"`         // Last transaction timestamp
	StakedAmount       float64 `json:"stakedAmount"`       // Amount currently staked
	DelegatedValidator string  `json:"delegatedValidator"` // ID of validator this account delegates to

	// Mutex for safe concurrent access to account fields
	mu sync.RWMutex `json:"-"`
}

// Validate performs basic validation on account data
func (a *Account) Validate() error {
	if a.ID == "" {
		return errors.New("account ID cannot be empty")
	}

	if len(a.PublicKey) == 0 {
		return errors.New("account must have a public key")
	}

	if a.Balance < 0 {
		return errors.New("account balance cannot be negative")
	}

	if a.Nonce < 0 {
		return errors.New("account nonce cannot be negative")
	}

	return nil
}

// GetBalance returns the current account balance in a thread-safe manner
func (a *Account) GetBalance() float64 {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.Balance
}

// IncrementNonce increments the account nonce and returns the new value
func (a *Account) IncrementNonce() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.Nonce++
	return a.Nonce
}

// UpdateBalance safely updates the account balance
func (a *Account) UpdateBalance(amount float64) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.Balance+amount < 0 {
		return ErrInsufficientFunds
	}

	a.Balance += amount
	a.LastActive = time.Now().Unix()
	return nil
}

// Stake sets an amount as staked (reduces available balance)
func (a *Account) Stake(amount float64) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if amount <= 0 {
		return errors.New("stake amount must be positive")
	}

	if a.Balance < amount {
		return ErrInsufficientFunds
	}

	a.Balance -= amount
	a.StakedAmount += amount
	return nil
}

// Unstake returns staked amount to available balance
func (a *Account) Unstake(amount float64) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if amount <= 0 {
		return errors.New("unstake amount must be positive")
	}

	if a.StakedAmount < amount {
		return errors.New("insufficient staked amount")
	}

	a.StakedAmount -= amount
	a.Balance += amount
	return nil
}

// EncryptPrivateKey encrypts a plaintext private key using a provided 32-byte key.
func EncryptPrivateKey(plain []byte, key []byte) ([]byte, error) {
	if len(key) != 32 {
		return nil, errors.New("encryption key must be 32 bytes for AES-256")
	}

	if len(plain) == 0 {
		return nil, errors.New("private key cannot be empty")
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("aes NewCipher error: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("cipher NewGCM error: %w", err)
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("nonce generation error: %w", err)
	}
	ciphertext := aead.Seal(nonce, nonce, plain, nil)
	return ciphertext, nil
}

// DecryptPrivateKey decrypts an encrypted private key using a provided 32-byte key.
func DecryptPrivateKey(ciphertext []byte, key []byte) ([]byte, error) {
	if len(key) != 32 {
		return nil, errors.New("decryption key must be 32 bytes for AES-256")
	}

	if len(ciphertext) == 0 {
		return nil, errors.New("ciphertext cannot be empty")
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("aes NewCipher error: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("cipher NewGCM error: %w", err)
	}
	nonceSize := aead.NonceSize()
	if len(ciphertext) < nonceSize {
		return nil, errors.New("ciphertext too short")
	}
	nonce, data := ciphertext[:nonceSize], ciphertext[nonceSize:]
	plain, err := aead.Open(nil, nonce, data, nil)
	if err != nil {
		return nil, fmt.Errorf("aes GCM open error: %w", err)
	}
	return plain, nil
}

// SetPrivateKey encrypts and sets the account's private key using the provided hex-encoded key.
func (a *Account) SetPrivateKey(plain []byte, hexKey string) error {
	if len(plain) == 0 {
		return errors.New("private key cannot be empty")
	}

	if hexKey == "" {
		return errors.New("encryption key cannot be empty")
	}

	key, err := hex.DecodeString(hexKey)
	if err != nil {
		return fmt.Errorf("failed to decode hex key: %w", err)
	}
	encrypted, err := EncryptPrivateKey(plain, key)
	if err != nil {
		return err
	}

	a.mu.Lock()
	a.EncryptedPrivateKey = encrypted
	a.mu.Unlock()

	return nil
}

// GetDecryptedPrivateKey decrypts and returns the account's private key using the provided hex-encoded key.
func (a *Account) GetDecryptedPrivateKey(hexKey string) ([]byte, error) {
	if hexKey == "" {
		return nil, errors.New("decryption key cannot be empty")
	}

	a.mu.RLock()
	encryptedKey := a.EncryptedPrivateKey
	a.mu.RUnlock()

	if len(encryptedKey) == 0 {
		return nil, errors.New("no encrypted private key available")
	}

	key, err := hex.DecodeString(hexKey)
	if err != nil {
		return nil, fmt.Errorf("failed to decode hex key: %w", err)
	}
	return DecryptPrivateKey(encryptedKey, key)
}

// Clone creates a deep copy of the account
func (a *Account) Clone() *Account {
	a.mu.RLock()
	defer a.mu.RUnlock()

	clone := &Account{
		ID:                 a.ID,
		Balance:            a.Balance,
		MultiSig:           a.MultiSig,
		HDWalletKey:        a.HDWalletKey,
		Role:               a.Role,
		KYCStatus:          a.KYCStatus,
		VotingWeight:       a.VotingWeight,
		Nonce:              a.Nonce,
		CreatedAt:          a.CreatedAt,
		LastActive:         a.LastActive,
		StakedAmount:       a.StakedAmount,
		DelegatedValidator: a.DelegatedValidator,
	}

	// Deep copy byte slices
	if len(a.PublicKey) > 0 {
		clone.PublicKey = make([]byte, len(a.PublicKey))
		copy(clone.PublicKey, a.PublicKey)
	}

	if len(a.EncryptedPrivateKey) > 0 {
		clone.EncryptedPrivateKey = make([]byte, len(a.EncryptedPrivateKey))
		copy(clone.EncryptedPrivateKey, a.EncryptedPrivateKey)
	}

	return clone
}

// NewAccount creates a new account with the given ID and public key
func NewAccount(id string, publicKey []byte) (*Account, error) {
	if id == "" {
		return nil, errors.New("account ID cannot be empty")
	}

	if len(publicKey) == 0 {
		return nil, errors.New("public key cannot be empty")
	}

	return &Account{
		ID:         id,
		PublicKey:  publicKey,
		Balance:    0,
		Nonce:      0,
		CreatedAt:  time.Now().Unix(),
		LastActive: time.Now().Unix(),
	}, nil
}
