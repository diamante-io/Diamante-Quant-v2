package wallet

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"

	"diamante/common"
	"diamante/consensus"
	"diamante/crypto"
	"diamante/transaction"

	"github.com/sirupsen/logrus"
)

// Wallet represents a user wallet that manages keys and provides methods to create, sign, and submit transactions.
type Wallet struct {
	// ID is the unique identifier for the wallet/account.
	ID string `json:"id"`

	// Nonce is maintained locally to help with replay protection when creating transactions.
	Nonce int `json:"nonce"`

	// KEMKeyPair holds the Kyber (KEM) key pair used for encryption and key encapsulation.
	KEMKeyPair *crypto.KyberKeyPair `json:"kemKeyPair"`

	// SigKeyPair holds the Dilithium key pair used for digital signatures.
	SigKeyPair *crypto.DilithiumKeyPair `json:"sigKeyPair"`

	// CryptoManager is the instance used for all cryptographic operations.
	// This field is not serialized and will be re‑initialized upon import.
	CryptoManager *crypto.CryptoManager `json:"-"`

	// mu protects wallet fields during concurrent access.
	mu sync.Mutex
}

// WalletFile is the structure used for persisting a wallet.
type WalletFile struct {
	ID         string                             `json:"id"`
	Nonce      int                                `json:"nonce"`
	KEMKeyPair *crypto.KyberKeyPairSerialized     `json:"kemKeyPair"`
	SigKeyPair *crypto.DilithiumKeyPairSerialized `json:"sigKeyPair"`
}

// Config holds wallet configuration settings
type Config struct {
	// EncryptionKey is used to encrypt wallet private keys
	EncryptionKey []byte
	// KyberLevel determines the security level for Kyber encryption
	KyberLevel int
	// DilithiumLevel determines the security level for Dilithium signatures
	DilithiumLevel int
}

// DefaultConfig returns the default wallet configuration with strong security checks
func DefaultConfig() (*Config, error) {
	// Production detection - any of these env vars set to production means we're in production
	productionEnvs := []string{"DIAMANTE_ENV", "NODE_ENV", "GO_ENV", "APP_ENV", "ENVIRONMENT"}
	isProduction := false
	for _, env := range productionEnvs {
		if val := os.Getenv(env); val == "production" || val == "prod" {
			isProduction = true
			break
		}
	}

	// Get encryption key from environment
	encKeyHex := os.Getenv("DIAMANTE_WALLET_ENCRYPTION_KEY")

	// List of known weak/development keys that must never be used in production
	weakKeys := []string{
		"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		"0000000000000000000000000000000000000000000000000000000000000000",
		"ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
		"deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
	}

	// Check if the provided key is a known weak key
	isWeakKey := false
	for _, weakKey := range weakKeys {
		if encKeyHex == weakKey {
			isWeakKey = true
			break
		}
	}

	// Production environment checks
	if isProduction {
		if encKeyHex == "" {
			return nil, fmt.Errorf("DIAMANTE_WALLET_ENCRYPTION_KEY must be set in production environment")
		}
		if isWeakKey {
			return nil, fmt.Errorf("weak/development encryption key detected in production environment - this is a security risk")
		}
	}

	// Development environment handling
	if encKeyHex == "" {
		// Only allow empty key in development with explicit flags
		isDevelopment := os.Getenv("DIAMANTE_ENV") == "development" ||
			os.Getenv("NODE_ENV") == "development" ||
			os.Getenv("GO_ENV") == "development"
		allowDevKey := os.Getenv("DIAMANTE_ALLOW_DEV_KEY") == "true"

		if !isDevelopment || !allowDevKey {
			return nil, fmt.Errorf("DIAMANTE_WALLET_ENCRYPTION_KEY not set (set DIAMANTE_ENV=development and DIAMANTE_ALLOW_DEV_KEY=true for development)")
		}

		// Use development key with strong warning
		logger := logrus.StandardLogger()
		if logger != nil {
			logger.Warn("SECURITY WARNING: Using development encryption key - NEVER use in production!")
		}
		encKeyHex = weakKeys[0] // Use the first weak key for development
	}

	// Decode and validate the key
	keyBytes, err := hex.DecodeString(encKeyHex)
	if err != nil {
		return nil, fmt.Errorf("failed to decode DIAMANTE_WALLET_ENCRYPTION_KEY: %w", err)
	}
	if len(keyBytes) != 32 {
		return nil, fmt.Errorf("DIAMANTE_WALLET_ENCRYPTION_KEY must be exactly 64 hex characters (32 bytes)")
	}

	// Always validate key entropy for non-development keys
	if !isWeakKey {
		if err := validateKeyEntropy(keyBytes); err != nil {
			return nil, fmt.Errorf("encryption key validation failed: %w", err)
		}
	}

	return &Config{
		EncryptionKey:  keyBytes,
		KyberLevel:     crypto.KyberLevel1024,
		DilithiumLevel: crypto.DilithiumLevel3,
	}, nil
}

// validateKeyEntropy ensures the encryption key has sufficient entropy
func validateKeyEntropy(key []byte) error {
	// Check for obvious patterns
	allSame := true
	for i := 1; i < len(key); i++ {
		if key[i] != key[0] {
			allSame = false
			break
		}
	}
	if allSame {
		return errors.New("encryption key has insufficient entropy: all bytes are the same")
	}

	// Check for sequential patterns
	sequential := true
	for i := 1; i < len(key); i++ {
		if key[i] != key[i-1]+1 {
			sequential = false
			break
		}
	}
	if sequential {
		return errors.New("encryption key has insufficient entropy: sequential pattern detected")
	}

	// Calculate byte distribution
	byteCounts := make(map[byte]int)
	for _, b := range key {
		byteCounts[b]++
	}

	// Check for low entropy (too many repeated bytes)
	maxCount := 0
	for _, count := range byteCounts {
		if count > maxCount {
			maxCount = count
		}
	}

	// If any byte appears more than 25% of the time, consider it low entropy
	if float64(maxCount)/float64(len(key)) > 0.25 {
		return errors.New("encryption key has insufficient entropy: uneven byte distribution")
	}

	return nil
}

// NewWallet creates a new wallet with fresh key pairs and a unique account ID.
func NewWallet(logger *logrus.Logger) (*Wallet, error) {
	config, err := DefaultConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to get wallet config: %w", err)
	}

	return NewWalletWithConfig(config, logger)
}

// NewWalletWithConfig creates a new wallet with the provided configuration
func NewWalletWithConfig(config *Config, logger *logrus.Logger) (*Wallet, error) {
	if config == nil {
		return nil, errors.New("config cannot be nil")
	}

	// Instantiate a new crypto manager.
	cm, err := crypto.NewCryptoManager(config.KyberLevel, config.DilithiumLevel, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create crypto manager: %w", err)
	}

	// Generate Kyber (KEM) key pair.
	kemKP, err := cm.GenerateKEMKeyPair()
	if err != nil {
		return nil, fmt.Errorf("failed to generate Kyber key pair: %w", err)
	}

	// Generate Dilithium (signature) key pair.
	sigKP, err := cm.GenerateSignatureKeyPair()
	if err != nil {
		return nil, fmt.Errorf("failed to generate Dilithium key pair: %w", err)
	}

	// Generate a unique account ID.
	accountID := common.GenerateUniqueID()

	wallet := &Wallet{
		ID:            accountID,
		Nonce:         0,
		KEMKeyPair:    kemKP,
		SigKeyPair:    sigKP,
		CryptoManager: cm,
	}

	logger.WithField("walletID", accountID).Info("New wallet created successfully")
	return wallet, nil
}

// RegisterAccount registers the wallet's account in the global account store.
// In production, the account's private key is stored encrypted.
func (w *Wallet) RegisterAccount() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	config, err := DefaultConfig()
	if err != nil {
		return fmt.Errorf("failed to get wallet config: %w", err)
	}

	// Create account using the new common.Account which stores an encrypted private key.
	account := &common.Account{
		ID:        w.ID,
		PublicKey: w.SigKeyPair.PublicKey,
		Balance:   0,
	}
	// Encrypt the private key using the provided encryption key.
	if err := account.SetPrivateKey(w.SigKeyPair.PrivateKey, hex.EncodeToString(config.EncryptionKey)); err != nil {
		return fmt.Errorf("failed to encrypt private key: %w", err)
	}

	if err := common.RegisterAccount(account); err != nil {
		return fmt.Errorf("failed to register account: %w", err)
	}
	return nil
}

// GetBalance returns the current balance for the wallet's account from the global store.
func (w *Wallet) GetBalance() (float64, error) {
	account := common.GetAccount(w.ID)
	if account == nil {
		return 0, fmt.Errorf("account %s not found", w.ID)
	}
	return account.Balance, nil
}

// FundWallet credits the wallet's account with a given amount.
func (w *Wallet) FundWallet(amount float64) error {
	if amount < 0 {
		return fmt.Errorf("amount cannot be negative")
	}
	return common.UpdateAccountBalance(w.ID, amount)
}

// CreateTransaction constructs a new transaction using the wallet as the sender.
// It generates a unique transaction ID, increments the nonce, signs the transaction,
// and returns the transaction object.
func (w *Wallet) CreateTransaction(receiver string, amount float64, fee float64, data []byte) (*common.Transaction, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Validate inputs
	if receiver == "" {
		return nil, fmt.Errorf("receiver cannot be empty")
	}
	if amount < 0 {
		return nil, fmt.Errorf("amount cannot be negative")
	}
	if fee < 0 {
		return nil, fmt.Errorf("fee cannot be negative")
	}

	txID := common.GenerateUniqueID()
	w.Nonce++

	tx := &common.Transaction{
		ID:        txID,
		Sender:    w.ID,
		Receiver:  receiver,
		Amount:    amount,
		Fee:       fee,
		Timestamp: consensus.ConsensusUnix(),
		Nonce:     w.Nonce,
		Data:      data,
	}

	sig, err := crypto.SignDataWithDilithium(w.SigKeyPair.PrivateKey, []byte(tx.ID))
	if err != nil {
		// Rollback nonce on error
		w.Nonce--
		return nil, fmt.Errorf("failed to sign transaction: %w", err)
	}
	tx.Signature = sig

	return tx, nil
}

// SubmitTransaction creates and submits a transaction via the provided TransactionManager.
// It returns the submitted transaction with updated fields from the transaction manager.
func (w *Wallet) SubmitTransaction(receiver string, amount float64, fee float64, data []byte, txManager *transaction.TransactionManager) (*common.Transaction, error) {
	// Create a local transaction to get signature and nonce
	localTx, err := w.CreateTransaction(receiver, amount, fee, data)
	if err != nil {
		return nil, fmt.Errorf("failed to create transaction: %w", err)
	}

	// Submit to transaction manager - it may update certain fields like ID
	submittedTx, err := txManager.CreateTransaction(localTx.Sender, localTx.Receiver, localTx.Amount, localTx.Fee, localTx.Data)
	if err != nil {
		// Rollback nonce on submission failure
		w.mu.Lock()
		w.Nonce--
		w.mu.Unlock()
		return nil, fmt.Errorf("failed to submit transaction via TransactionManager: %w", err)
	}

	// Preserve the signature and nonce from our local transaction
	submittedTx.Signature = localTx.Signature
	submittedTx.Nonce = localTx.Nonce

	return submittedTx, nil
}

// SignMessage signs an arbitrary message using the wallet's signature key.
func (w *Wallet) SignMessage(message string) ([]byte, error) {
	if message == "" {
		return nil, fmt.Errorf("message cannot be empty")
	}
	return crypto.SignDataWithDilithium(w.SigKeyPair.PrivateKey, []byte(message))
}

// VerifySignature verifies a signature for a given message using the wallet's public key.
func (w *Wallet) VerifySignature(message string, signature []byte) (bool, error) {
	if message == "" {
		return false, fmt.Errorf("message cannot be empty")
	}
	if len(signature) == 0 {
		return false, fmt.Errorf("signature cannot be empty")
	}
	return crypto.VerifySignature(w.SigKeyPair.PublicKey, []byte(message), signature)
}

// GetPublicKeyHex returns the wallet's public key as a hex-encoded string.
func (w *Wallet) GetPublicKeyHex() string {
	return hex.EncodeToString(w.SigKeyPair.PublicKey)
}

// Export writes the wallet's data to a JSON file.
func (w *Wallet) Export(filePath string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	config, err := DefaultConfig()
	if err != nil {
		return fmt.Errorf("failed to get wallet config: %w", err)
	}

	kemSerialized, err := crypto.SerializeKyberKeyPair(w.KEMKeyPair)
	if err != nil {
		return fmt.Errorf("failed to serialize Kyber key pair: %w", err)
	}
	sigSerialized, err := crypto.SerializeDilithiumKeyPair(w.SigKeyPair)
	if err != nil {
		return fmt.Errorf("failed to serialize Dilithium key pair: %w", err)
	}

	kemSerialized.PrivateKey, err = common.EncryptPrivateKey(kemSerialized.PrivateKey, config.EncryptionKey)
	if err != nil {
		return fmt.Errorf("failed to encrypt Kyber private key: %w", err)
	}
	sigSerialized.PrivateKey, err = common.EncryptPrivateKey(sigSerialized.PrivateKey, config.EncryptionKey)
	if err != nil {
		return fmt.Errorf("failed to encrypt Dilithium private key: %w", err)
	}

	wf := WalletFile{
		ID:         w.ID,
		Nonce:      w.Nonce,
		KEMKeyPair: kemSerialized,
		SigKeyPair: sigSerialized,
	}

	data, err := json.MarshalIndent(wf, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal wallet to JSON: %w", err)
	}

	if err := os.WriteFile(filePath, data, 0600); err != nil {
		return fmt.Errorf("failed to write wallet file: %w", err)
	}
	return nil
}

// ImportWallet reads a wallet file and returns a new Wallet instance.
func ImportWallet(filePath string, logger *logrus.Logger) (*Wallet, error) {
	config, err := DefaultConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to get wallet config: %w", err)
	}

	return ImportWalletWithConfig(filePath, config, logger)
}

// ImportWalletWithConfig reads a wallet file with the provided configuration
func ImportWalletWithConfig(filePath string, config *Config, logger *logrus.Logger) (*Wallet, error) {
	if config == nil {
		return nil, errors.New("config cannot be nil")
	}

	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read wallet file: %w", err)
	}

	var wf WalletFile
	if err := json.Unmarshal(data, &wf); err != nil {
		return nil, fmt.Errorf("failed to unmarshal wallet JSON: %w", err)
	}

	cm, err := crypto.NewCryptoManager(config.KyberLevel, config.DilithiumLevel, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to re-create crypto manager: %w", err)
	}

	if wf.KEMKeyPair == nil || wf.SigKeyPair == nil {
		return nil, errors.New("wallet file missing key pairs")
	}

	wf.KEMKeyPair.PrivateKey, err = common.DecryptPrivateKey(wf.KEMKeyPair.PrivateKey, config.EncryptionKey)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt Kyber private key: %w", err)
	}
	wf.SigKeyPair.PrivateKey, err = common.DecryptPrivateKey(wf.SigKeyPair.PrivateKey, config.EncryptionKey)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt Dilithium private key: %w", err)
	}

	kemKP, err := crypto.DeserializeKyberKeyPair(wf.KEMKeyPair)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize Kyber key pair: %w", err)
	}
	sigKP, err := crypto.DeserializeDilithiumKeyPair(wf.SigKeyPair)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize Dilithium key pair: %w", err)
	}

	wallet := &Wallet{
		ID:            wf.ID,
		Nonce:         wf.Nonce,
		KEMKeyPair:    kemKP,
		SigKeyPair:    sigKP,
		CryptoManager: cm,
	}

	logger.WithField("walletID", wallet.ID).Info("Wallet imported successfully")
	return wallet, nil
}
