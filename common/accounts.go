// common/account.go
package common

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"io"
	"sync"

	"golang.org/x/crypto/pbkdf2"
)

// Constants for key derivation
const (
	// PBKDF2 parameters for key derivation
	pbkdf2Iterations = 100000 // NIST recommended minimum for PBKDF2
	saltSize         = 32     // 256-bit salt
	keySize          = 32     // 256-bit key for AES-256
)

// Account represents a user or entity on the blockchain network.
// In production, the private key is stored encrypted with proper key derivation.
// Now supports both classical and quantum-resistant cryptography.
type Account struct {
	ID                  string  `json:"id"`
	PublicKey           []byte  `json:"publicKey"`           // Dilithium public key (quantum-resistant)
	EncryptedPrivateKey []byte  `json:"encryptedPrivateKey"` // Encrypted Dilithium private key
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

	// Security metadata
	KeyDerivationAlgo string `json:"keyDerivationAlgo"` // Algorithm used for key derivation
	KDFParams         string `json:"kdfParams"`         // Parameters for key derivation (iterations, etc.)

	// Quantum-resistant key fields
	DilithiumPubKey           []byte `json:"dilithiumPubKey,omitempty"`  // Dilithium public key
	EncryptedDilithiumPrivKey []byte `json:"dilithiumPrivKey,omitempty"` // Encrypted Dilithium private key
	KyberPubKey               []byte `json:"kyberPubKey,omitempty"`      // Kyber public key for KEM
	EncryptedKyberPrivKey     []byte `json:"kyberPrivKey,omitempty"`     // Encrypted Kyber private key

	// Legacy ECDSA keys (for transition period)
	ECDSAPubKey           []byte `json:"ecdsaPubKey,omitempty"`  // Legacy ECDSA public key
	EncryptedECDSAPrivKey []byte `json:"ecdsaPrivKey,omitempty"` // Encrypted ECDSA private key

	// Quantum transition metadata
	QuantumEnabled       bool  `json:"quantumEnabled"`       // Whether account has quantum keys
	QuantumMigrationDate int64 `json:"quantumMigrationDate"` // When account migrated to quantum

	// Mutex for safe concurrent access to account fields
	mu sync.RWMutex `json:"-"`
}

// Validate performs comprehensive validation on account data
func (a *Account) Validate() error {
	if a.ID == "" {
		return fmt.Errorf("account validation failed: ID cannot be empty")
	}

	if len(a.PublicKey) == 0 {
		return fmt.Errorf("account validation failed: public key cannot be empty")
	}

	// Validate public key length (should be appropriate for the crypto algorithm)
	if len(a.PublicKey) < 32 {
		return fmt.Errorf("account validation failed: public key too short (minimum 32 bytes)")
	}

	if a.Balance < 0 {
		return fmt.Errorf("account validation failed: balance cannot be negative (current: %f)", a.Balance)
	}

	if a.Nonce < 0 {
		return fmt.Errorf("account validation failed: nonce cannot be negative (current: %d)", a.Nonce)
	}

	if a.StakedAmount < 0 {
		return fmt.Errorf("account validation failed: staked amount cannot be negative")
	}

	if a.StakedAmount > a.Balance+a.StakedAmount {
		return fmt.Errorf("account validation failed: staked amount exceeds total holdings")
	}

	// Validate role if specified
	if a.Role != "" {
		validRoles := map[string]bool{
			"user":      true,
			"validator": true,
			"admin":     true,
			"observer":  true,
		}
		if !validRoles[a.Role] {
			return fmt.Errorf("account validation failed: invalid role '%s'", a.Role)
		}
	}

	return nil
}

// GetBalance returns the current account balance in a thread-safe manner
func (a *Account) GetBalance() float64 {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.Balance
}

// GetTotalBalance returns the sum of available balance and staked amount
func (a *Account) GetTotalBalance() float64 {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.Balance + a.StakedAmount
}

// IncrementNonce increments the account nonce and returns the new value
func (a *Account) IncrementNonce() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.Nonce++
	a.LastActive = ConsensusUnix()
	return a.Nonce
}

// UpdateBalance safely updates the account balance with transaction atomicity
func (a *Account) UpdateBalance(amount float64) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	newBalance := a.Balance + amount
	if newBalance < 0 {
		return fmt.Errorf("insufficient funds: current balance %f, attempted change %f", a.Balance, amount)
	}

	a.Balance = newBalance
	a.LastActive = ConsensusUnix()
	return nil
}

// Stake sets an amount as staked (reduces available balance)
func (a *Account) Stake(amount float64) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if amount <= 0 {
		return fmt.Errorf("stake amount must be positive, got %f", amount)
	}

	if a.Balance < amount {
		return fmt.Errorf("insufficient funds for staking: balance %f, requested stake %f", a.Balance, amount)
	}

	a.Balance -= amount
	a.StakedAmount += amount
	a.LastActive = ConsensusUnix()
	return nil
}

// Unstake returns staked amount to available balance
func (a *Account) Unstake(amount float64) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if amount <= 0 {
		return fmt.Errorf("unstake amount must be positive, got %f", amount)
	}

	if a.StakedAmount < amount {
		return fmt.Errorf("insufficient staked amount: staked %f, requested unstake %f", a.StakedAmount, amount)
	}

	a.StakedAmount -= amount
	a.Balance += amount
	a.LastActive = ConsensusUnix()
	return nil
}

// deriveKey derives an encryption key from a password using PBKDF2
func deriveKey(password []byte, salt []byte) []byte {
	return pbkdf2.Key(password, salt, pbkdf2Iterations, keySize, sha256.New)
}

// EncryptPrivateKey encrypts a plaintext private key using password-based encryption with PBKDF2
func EncryptPrivateKey(plain []byte, password []byte) ([]byte, error) {
	if len(plain) == 0 {
		return nil, fmt.Errorf("private key encryption failed: private key cannot be empty")
	}

	if len(password) == 0 {
		return nil, fmt.Errorf("private key encryption failed: password cannot be empty")
	}

	// Generate random salt
	salt := make([]byte, saltSize)
	if _, err := io.ReadFull(rand.Reader, salt); err != nil {
		return nil, fmt.Errorf("private key encryption failed: salt generation error: %w", err)
	}

	// Derive key from password using PBKDF2
	key := deriveKey(password, salt)

	// Create AES cipher
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("private key encryption failed: AES cipher creation error: %w", err)
	}

	// Create GCM mode
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("private key encryption failed: GCM mode creation error: %w", err)
	}

	// Generate nonce
	nonce := make([]byte, aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("private key encryption failed: nonce generation error: %w", err)
	}

	// Encrypt the private key
	ciphertext := aead.Seal(nil, nonce, plain, nil)

	// Prepend salt and nonce to ciphertext for storage
	// Format: [salt(32)][nonce(12)][ciphertext]
	result := make([]byte, len(salt)+len(nonce)+len(ciphertext))
	copy(result, salt)
	copy(result[len(salt):], nonce)
	copy(result[len(salt)+len(nonce):], ciphertext)

	// Clear sensitive data from memory
	for i := range key {
		key[i] = 0
	}

	return result, nil
}

// DecryptPrivateKey decrypts an encrypted private key using password-based decryption with PBKDF2
func DecryptPrivateKey(encrypted []byte, password []byte) ([]byte, error) {
	if len(encrypted) < saltSize+12 { // 12 is GCM nonce size
		return nil, fmt.Errorf("private key decryption failed: encrypted data too short")
	}

	if len(password) == 0 {
		return nil, fmt.Errorf("private key decryption failed: password cannot be empty")
	}

	// Extract salt, nonce, and ciphertext
	salt := encrypted[:saltSize]
	nonce := encrypted[saltSize : saltSize+12]
	ciphertext := encrypted[saltSize+12:]

	// Derive key from password using PBKDF2
	key := deriveKey(password, salt)
	defer func() {
		// Clear sensitive data from memory
		for i := range key {
			key[i] = 0
		}
	}()

	// Create AES cipher
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("private key decryption failed: AES cipher creation error: %w", err)
	}

	// Create GCM mode
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("private key decryption failed: GCM mode creation error: %w", err)
	}

	// Decrypt the private key
	plain, err := aead.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("private key decryption failed: decryption error: %w", err)
	}

	return plain, nil
}

// SetPrivateKey encrypts and sets the account's private key using password-based encryption
func (a *Account) SetPrivateKey(plain []byte, password string) error {
	if len(plain) == 0 {
		return fmt.Errorf("set private key failed: private key cannot be empty")
	}

	if password == "" {
		return fmt.Errorf("set private key failed: password cannot be empty")
	}

	encrypted, err := EncryptPrivateKey(plain, []byte(password))
	if err != nil {
		return fmt.Errorf("set private key failed: %w", err)
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	a.EncryptedPrivateKey = encrypted
	a.KeyDerivationAlgo = "PBKDF2-SHA256"
	a.KDFParams = fmt.Sprintf("iterations=%d,saltSize=%d", pbkdf2Iterations, saltSize)

	return nil
}

// GetDecryptedPrivateKey decrypts and returns the account's private key using the provided password
func (a *Account) GetDecryptedPrivateKey(password string) ([]byte, error) {
	if password == "" {
		return nil, fmt.Errorf("get private key failed: password cannot be empty")
	}

	a.mu.RLock()
	encryptedKey := make([]byte, len(a.EncryptedPrivateKey))
	copy(encryptedKey, a.EncryptedPrivateKey)
	a.mu.RUnlock()

	if len(encryptedKey) == 0 {
		return nil, fmt.Errorf("get private key failed: no encrypted private key available")
	}

	decrypted, err := DecryptPrivateKey(encryptedKey, []byte(password))
	if err != nil {
		return nil, fmt.Errorf("get private key failed: %w", err)
	}

	return decrypted, nil
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
		KeyDerivationAlgo:  a.KeyDerivationAlgo,
		KDFParams:          a.KDFParams,
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

	// Copy quantum-resistant keys
	if len(a.DilithiumPubKey) > 0 {
		clone.DilithiumPubKey = make([]byte, len(a.DilithiumPubKey))
		copy(clone.DilithiumPubKey, a.DilithiumPubKey)
	}

	if len(a.EncryptedDilithiumPrivKey) > 0 {
		clone.EncryptedDilithiumPrivKey = make([]byte, len(a.EncryptedDilithiumPrivKey))
		copy(clone.EncryptedDilithiumPrivKey, a.EncryptedDilithiumPrivKey)
	}

	if len(a.KyberPubKey) > 0 {
		clone.KyberPubKey = make([]byte, len(a.KyberPubKey))
		copy(clone.KyberPubKey, a.KyberPubKey)
	}

	if len(a.EncryptedKyberPrivKey) > 0 {
		clone.EncryptedKyberPrivKey = make([]byte, len(a.EncryptedKyberPrivKey))
		copy(clone.EncryptedKyberPrivKey, a.EncryptedKyberPrivKey)
	}

	// Copy legacy ECDSA keys
	if len(a.ECDSAPubKey) > 0 {
		clone.ECDSAPubKey = make([]byte, len(a.ECDSAPubKey))
		copy(clone.ECDSAPubKey, a.ECDSAPubKey)
	}

	if len(a.EncryptedECDSAPrivKey) > 0 {
		clone.EncryptedECDSAPrivKey = make([]byte, len(a.EncryptedECDSAPrivKey))
		copy(clone.EncryptedECDSAPrivKey, a.EncryptedECDSAPrivKey)
	}

	clone.QuantumEnabled = a.QuantumEnabled
	clone.QuantumMigrationDate = a.QuantumMigrationDate

	return clone
}

// SetQuantumKeys sets the quantum-resistant keys for the account
func (a *Account) SetQuantumKeys(dilithiumPubKey, encryptedDilithiumPrivKey, kyberPubKey, encryptedKyberPrivKey []byte) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if len(dilithiumPubKey) == 0 {
		return fmt.Errorf("Dilithium public key cannot be empty")
	}

	if len(encryptedDilithiumPrivKey) == 0 {
		return fmt.Errorf("encrypted Dilithium private key cannot be empty")
	}

	if len(kyberPubKey) == 0 {
		return fmt.Errorf("Kyber public key cannot be empty")
	}

	if len(encryptedKyberPrivKey) == 0 {
		return fmt.Errorf("encrypted Kyber private key cannot be empty")
	}

	// Set quantum keys
	a.DilithiumPubKey = make([]byte, len(dilithiumPubKey))
	copy(a.DilithiumPubKey, dilithiumPubKey)

	a.EncryptedDilithiumPrivKey = make([]byte, len(encryptedDilithiumPrivKey))
	copy(a.EncryptedDilithiumPrivKey, encryptedDilithiumPrivKey)

	a.KyberPubKey = make([]byte, len(kyberPubKey))
	copy(a.KyberPubKey, kyberPubKey)

	a.EncryptedKyberPrivKey = make([]byte, len(encryptedKyberPrivKey))
	copy(a.EncryptedKyberPrivKey, encryptedKyberPrivKey)

	a.QuantumEnabled = true
	a.QuantumMigrationDate = ConsensusUnix()

	return nil
}

// MigrateToQuantum migrates the account from ECDSA to quantum-resistant keys
// This preserves the legacy ECDSA keys for backward compatibility during transition
func (a *Account) MigrateToQuantum(dilithiumPubKey, encryptedDilithiumPrivKey, kyberPubKey, encryptedKyberPrivKey []byte) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Ensure account has ECDSA keys to migrate from
	if len(a.PublicKey) == 0 && len(a.ECDSAPubKey) == 0 {
		return fmt.Errorf("no existing keys to migrate from")
	}

	// If PublicKey is set but ECDSAPubKey is not, move it to legacy fields
	if len(a.PublicKey) > 0 && len(a.ECDSAPubKey) == 0 {
		a.ECDSAPubKey = make([]byte, len(a.PublicKey))
		copy(a.ECDSAPubKey, a.PublicKey)

		if len(a.EncryptedPrivateKey) > 0 {
			a.EncryptedECDSAPrivKey = make([]byte, len(a.EncryptedPrivateKey))
			copy(a.EncryptedECDSAPrivKey, a.EncryptedPrivateKey)
		}
	}

	// Set quantum keys
	a.DilithiumPubKey = make([]byte, len(dilithiumPubKey))
	copy(a.DilithiumPubKey, dilithiumPubKey)

	a.EncryptedDilithiumPrivKey = make([]byte, len(encryptedDilithiumPrivKey))
	copy(a.EncryptedDilithiumPrivKey, encryptedDilithiumPrivKey)

	a.KyberPubKey = make([]byte, len(kyberPubKey))
	copy(a.KyberPubKey, kyberPubKey)

	a.EncryptedKyberPrivKey = make([]byte, len(encryptedKyberPrivKey))
	copy(a.EncryptedKyberPrivKey, encryptedKyberPrivKey)

	// Update primary key fields to use Dilithium
	a.PublicKey = make([]byte, len(dilithiumPubKey))
	copy(a.PublicKey, dilithiumPubKey)

	a.EncryptedPrivateKey = make([]byte, len(encryptedDilithiumPrivKey))
	copy(a.EncryptedPrivateKey, encryptedDilithiumPrivKey)

	a.QuantumEnabled = true
	a.QuantumMigrationDate = ConsensusUnix()

	return nil
}

// GetSigningPublicKey returns the appropriate public key based on quantum transition status
// During transition: returns ECDSA key if available, otherwise Dilithium
// After transition: always returns Dilithium key
func (a *Account) GetSigningPublicKey(quantumOnly bool) ([]byte, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	if quantumOnly || a.QuantumEnabled {
		// Return Dilithium key
		if len(a.DilithiumPubKey) > 0 {
			key := make([]byte, len(a.DilithiumPubKey))
			copy(key, a.DilithiumPubKey)
			return key, nil
		}
		if len(a.PublicKey) > 0 {
			// PublicKey might be Dilithium if account was created post-quantum
			key := make([]byte, len(a.PublicKey))
			copy(key, a.PublicKey)
			return key, nil
		}
		return nil, fmt.Errorf("no quantum public key available")
	}

	// During transition, prefer ECDSA if available
	if len(a.ECDSAPubKey) > 0 {
		key := make([]byte, len(a.ECDSAPubKey))
		copy(key, a.ECDSAPubKey)
		return key, nil
	}

	// Fallback to primary key (could be either type)
	if len(a.PublicKey) > 0 {
		key := make([]byte, len(a.PublicKey))
		copy(key, a.PublicKey)
		return key, nil
	}

	return nil, fmt.Errorf("no public key available")
}

// GetDecryptedSigningKey returns the decrypted private key appropriate for signing
// During transition: returns ECDSA key if available, otherwise Dilithium
// After transition: always returns Dilithium key
func (a *Account) GetDecryptedSigningKey(password string, quantumOnly bool) ([]byte, error) {
	if password == "" {
		return nil, fmt.Errorf("password cannot be empty")
	}

	a.mu.RLock()
	defer a.mu.RUnlock()

	var encryptedKey []byte

	if quantumOnly || a.QuantumEnabled {
		// Use Dilithium key
		if len(a.EncryptedDilithiumPrivKey) > 0 {
			encryptedKey = make([]byte, len(a.EncryptedDilithiumPrivKey))
			copy(encryptedKey, a.EncryptedDilithiumPrivKey)
		} else if len(a.EncryptedPrivateKey) > 0 {
			// EncryptedPrivateKey might be Dilithium if account was created post-quantum
			encryptedKey = make([]byte, len(a.EncryptedPrivateKey))
			copy(encryptedKey, a.EncryptedPrivateKey)
		} else {
			return nil, fmt.Errorf("no quantum private key available")
		}
	} else {
		// During transition, prefer ECDSA if available
		if len(a.EncryptedECDSAPrivKey) > 0 {
			encryptedKey = make([]byte, len(a.EncryptedECDSAPrivKey))
			copy(encryptedKey, a.EncryptedECDSAPrivKey)
		} else if len(a.EncryptedPrivateKey) > 0 {
			// Fallback to primary key
			encryptedKey = make([]byte, len(a.EncryptedPrivateKey))
			copy(encryptedKey, a.EncryptedPrivateKey)
		} else {
			return nil, fmt.Errorf("no private key available")
		}
	}

	return DecryptPrivateKey(encryptedKey, []byte(password))
}

// HasQuantumKeys returns whether the account has quantum-resistant keys set
func (a *Account) HasQuantumKeys() bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.QuantumEnabled && len(a.DilithiumPubKey) > 0 && len(a.KyberPubKey) > 0
}

// HasLegacyKeys returns whether the account has legacy ECDSA keys
func (a *Account) HasLegacyKeys() bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return len(a.ECDSAPubKey) > 0 || (len(a.PublicKey) > 0 && !a.QuantumEnabled)
}

// NewAccount creates a new account with the given ID and public key
func NewAccount(id string, publicKey []byte) (*Account, error) {
	if id == "" {
		return nil, fmt.Errorf("account creation failed: ID cannot be empty")
	}

	if len(publicKey) == 0 {
		return nil, fmt.Errorf("account creation failed: public key cannot be empty")
	}

	if len(publicKey) < 32 {
		return nil, fmt.Errorf("account creation failed: public key too short (minimum 32 bytes)")
	}

	account := &Account{
		ID:                id,
		PublicKey:         publicKey,
		Balance:           0,
		Nonce:             0,
		CreatedAt:         ConsensusUnix(),
		LastActive:        ConsensusUnix(),
		KeyDerivationAlgo: "PBKDF2-SHA256",
		KDFParams:         fmt.Sprintf("iterations=%d,saltSize=%d", pbkdf2Iterations, saltSize),
	}

	// Validate the new account
	if err := account.Validate(); err != nil {
		return nil, fmt.Errorf("account creation failed: %w", err)
	}

	return account, nil
}

// ClearSensitiveData overwrites sensitive data in memory
func (a *Account) ClearSensitiveData() {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Overwrite encrypted private key
	if len(a.EncryptedPrivateKey) > 0 {
		for i := range a.EncryptedPrivateKey {
			a.EncryptedPrivateKey[i] = 0
		}
		a.EncryptedPrivateKey = nil
	}

	// Overwrite quantum private keys
	if len(a.EncryptedDilithiumPrivKey) > 0 {
		for i := range a.EncryptedDilithiumPrivKey {
			a.EncryptedDilithiumPrivKey[i] = 0
		}
		a.EncryptedDilithiumPrivKey = nil
	}

	if len(a.EncryptedKyberPrivKey) > 0 {
		for i := range a.EncryptedKyberPrivKey {
			a.EncryptedKyberPrivKey[i] = 0
		}
		a.EncryptedKyberPrivKey = nil
	}

	// Overwrite legacy ECDSA private key
	if len(a.EncryptedECDSAPrivKey) > 0 {
		for i := range a.EncryptedECDSAPrivKey {
			a.EncryptedECDSAPrivKey[i] = 0
		}
		a.EncryptedECDSAPrivKey = nil
	}

	// Clear HD wallet key
	if a.HDWalletKey != "" {
		a.HDWalletKey = ""
	}
}
