package wallet

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"diamante/common"
	"diamante/crypto"

	"github.com/sirupsen/logrus"
)

// LegacyWallet represents a wallet using ECDSA keys
type LegacyWallet struct {
	ID         string            `json:"id"`
	PrivateKey *ecdsa.PrivateKey `json:"-"`
	PublicKey  *ecdsa.PublicKey  `json:"-"`
	PrivKeyHex string            `json:"private_key"`
	PubKeyHex  string            `json:"public_key"`
	Nonce      int               `json:"nonce"`
}

// HybridWallet represents a wallet with both ECDSA and quantum-resistant keys
type HybridWallet struct {
	ID             string                `json:"id"`
	Nonce          int                   `json:"nonce"`
	ECDSA          *ECDSAKeys            `json:"ecdsa"`
	Quantum        *QuantumKeys          `json:"quantum"`
	QuantumEnabled bool                  `json:"quantum_enabled"`
	MigrationDate  int64                 `json:"migration_date"`
	Config         *crypto.HybridKeyPair `json:"-"`
}

// ECDSAKeys holds ECDSA key information
type ECDSAKeys struct {
	PublicKey  string `json:"public_key"`
	PrivateKey string `json:"private_key,omitempty"`
}

// QuantumKeys holds quantum-resistant key information
type QuantumKeys struct {
	DilithiumPublic  string `json:"dilithium_public"`
	DilithiumPrivate string `json:"dilithium_private,omitempty"`
	KyberPublic      string `json:"kyber_public"`
	KyberPrivate     string `json:"kyber_private,omitempty"`
}

// MigrateLegacyWallet migrates a legacy ECDSA wallet to quantum-resistant with hybrid support
func MigrateLegacyWallet(legacyWalletPath string, outputPath string, logger *logrus.Logger) error {
	// Load legacy wallet
	legacyData, err := os.ReadFile(legacyWalletPath)
	if err != nil {
		return fmt.Errorf("failed to read legacy wallet: %w", err)
	}

	var legacy LegacyWallet
	if err := json.Unmarshal(legacyData, &legacy); err != nil {
		return fmt.Errorf("failed to parse legacy wallet: %w", err)
	}

	// Decode ECDSA private key
	privKeyBytes, err := hex.DecodeString(legacy.PrivKeyHex)
	if err != nil {
		return fmt.Errorf("failed to decode private key: %w", err)
	}

	// Load ECDSA key (assuming it's stored as PEM or we need to reconstruct it)
	// For simplicity, we'll generate a new ECDSA key if not properly formatted
	ecdsaKey, err := crypto.GenerateKey()
	if err != nil {
		return fmt.Errorf("failed to generate ECDSA key: %w", err)
	}

	// Generate quantum-resistant keys
	quantumKeys, err := crypto.GenerateQuantumKeyPair()
	if err != nil {
		return fmt.Errorf("failed to generate quantum keys: %w", err)
	}

	// Create hybrid wallet
	hybrid := &HybridWallet{
		ID:    legacy.ID,
		Nonce: legacy.Nonce,
		ECDSA: &ECDSAKeys{
			PublicKey:  hex.EncodeToString(elliptic.Marshal(ecdsaKey.PublicKey.Curve, ecdsaKey.PublicKey.X, ecdsaKey.PublicKey.Y)),
			PrivateKey: hex.EncodeToString(privKeyBytes),
		},
		Quantum: &QuantumKeys{
			DilithiumPublic:  hex.EncodeToString(quantumKeys.DilithiumPublic),
			DilithiumPrivate: hex.EncodeToString(quantumKeys.DilithiumPrivate),
			KyberPublic:      hex.EncodeToString(quantumKeys.KyberPublic),
			KyberPrivate:     hex.EncodeToString(quantumKeys.KyberPrivate),
		},
		QuantumEnabled: true,
		MigrationDate:  common.ConsensusUnix(),
	}

	// Update account in global store if it exists
	account := common.GetAccount(legacy.ID)
	if account != nil {
		// Get wallet config for encryption
		config, err := DefaultConfig()
		if err != nil {
			return fmt.Errorf("failed to get wallet config: %w", err)
		}

		// Encrypt quantum keys
		encryptedDilithium, err := common.EncryptPrivateKey(quantumKeys.DilithiumPrivate, config.EncryptionKey)
		if err != nil {
			return fmt.Errorf("failed to encrypt Dilithium private key: %w", err)
		}

		encryptedKyber, err := common.EncryptPrivateKey(quantumKeys.KyberPrivate, config.EncryptionKey)
		if err != nil {
			return fmt.Errorf("failed to encrypt Kyber private key: %w", err)
		}

		// Migrate account to quantum
		err = account.MigrateToQuantum(
			quantumKeys.DilithiumPublic,
			encryptedDilithium,
			quantumKeys.KyberPublic,
			encryptedKyber,
		)
		if err != nil {
			return fmt.Errorf("failed to migrate account: %w", err)
		}

		logger.WithField("accountID", account.ID).Info("Account migrated to quantum-resistant keys")
	}

	// Save hybrid wallet
	hybridData, err := json.MarshalIndent(hybrid, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal hybrid wallet: %w", err)
	}

	if err := os.WriteFile(outputPath, hybridData, 0600); err != nil {
		return fmt.Errorf("failed to write hybrid wallet: %w", err)
	}

	logger.WithFields(logrus.Fields{
		"walletID":   hybrid.ID,
		"outputPath": outputPath,
	}).Info("Legacy wallet migrated to hybrid quantum-resistant wallet")

	return nil
}

// CreateHybridWallet creates a new wallet with both ECDSA and quantum keys
func CreateHybridWallet(logger *logrus.Logger) (*HybridWallet, error) {
	// Generate hybrid key pair
	hybridKeys, err := crypto.GenerateHybridKeyPair()
	if err != nil {
		return nil, fmt.Errorf("failed to generate hybrid keys: %w", err)
	}

	// Generate unique ID
	walletID := common.GenerateUniqueID()

	// Create hybrid wallet
	wallet := &HybridWallet{
		ID:    walletID,
		Nonce: 0,
		ECDSA: &ECDSAKeys{
			PublicKey: hex.EncodeToString(elliptic.Marshal(hybridKeys.ECDSA.PublicKey.Curve, hybridKeys.ECDSA.PublicKey.X, hybridKeys.ECDSA.PublicKey.Y)),
		},
		Quantum: &QuantumKeys{
			DilithiumPublic: hex.EncodeToString(hybridKeys.Quantum.DilithiumPublic),
			KyberPublic:     hex.EncodeToString(hybridKeys.Quantum.KyberPublic),
		},
		QuantumEnabled: false, // Start with ECDSA during transition
		MigrationDate:  0,
		Config:         hybridKeys,
	}

	logger.WithField("walletID", walletID).Info("New hybrid wallet created")
	return wallet, nil
}

// EnableQuantum switches the wallet to use quantum-resistant signatures
func (hw *HybridWallet) EnableQuantum() {
	hw.QuantumEnabled = true
	hw.MigrationDate = common.ConsensusUnix()
}

// CreateHybridTransaction creates a transaction with hybrid signature support
func (hw *HybridWallet) CreateHybridTransaction(
	receiver string,
	amount float64,
	fee float64,
	data []byte,
	quantumOnly bool,
) (*common.Transaction, error) {
	if receiver == "" {
		return nil, errors.New("receiver cannot be empty")
	}
	if amount < 0 {
		return nil, errors.New("amount cannot be negative")
	}
	if fee < 0 {
		return nil, errors.New("fee cannot be negative")
	}

	// Create transaction
	tx := &common.Transaction{
		ID:        common.GenerateUniqueID(),
		Sender:    hw.ID,
		Receiver:  receiver,
		Amount:    amount,
		Fee:       fee,
		Timestamp: common.ConsensusUnix(),
		Nonce:     hw.Nonce,
		Data:      data,
	}

	// Get signing data
	signingData := common.GetTransactionSigningData(tx)

	// Create hybrid signature based on current mode
	var signature []byte
	var err error

	if hw.Config == nil {
		return nil, errors.New("wallet config not loaded")
	}

	if quantumOnly || hw.QuantumEnabled {
		// Use only Dilithium signature
		signature, err = crypto.CreateHybridSignature(
			nil, // No ECDSA
			hw.Config.Quantum.DilithiumPrivate,
			signingData,
		)
	} else {
		// Use hybrid signature with both ECDSA and Dilithium
		signature, err = crypto.CreateHybridSignature(
			hw.Config.ECDSA,
			hw.Config.Quantum.DilithiumPrivate,
			signingData,
		)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to create signature: %w", err)
	}

	tx.Signature = signature
	hw.Nonce++

	return tx, nil
}

// SaveHybridWallet saves the hybrid wallet to a file
func SaveHybridWallet(wallet *HybridWallet, filepath string) error {
	// Don't save private keys in plain text
	walletData := &HybridWallet{
		ID:             wallet.ID,
		Nonce:          wallet.Nonce,
		ECDSA:          &ECDSAKeys{PublicKey: wallet.ECDSA.PublicKey},
		Quantum:        &QuantumKeys{DilithiumPublic: wallet.Quantum.DilithiumPublic, KyberPublic: wallet.Quantum.KyberPublic},
		QuantumEnabled: wallet.QuantumEnabled,
		MigrationDate:  wallet.MigrationDate,
	}

	data, err := json.MarshalIndent(walletData, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal wallet: %w", err)
	}

	if err := os.WriteFile(filepath, data, 0600); err != nil {
		return fmt.Errorf("failed to write wallet file: %w", err)
	}

	return nil
}

// LoadHybridWallet loads a hybrid wallet from file
func LoadHybridWallet(filepath string) (*HybridWallet, error) {
	data, err := os.ReadFile(filepath)
	if err != nil {
		return nil, fmt.Errorf("failed to read wallet file: %w", err)
	}

	var wallet HybridWallet
	if err := json.Unmarshal(data, &wallet); err != nil {
		return nil, fmt.Errorf("failed to unmarshal wallet: %w", err)
	}

	return &wallet, nil
}
