package wallet

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/sha512"
	"errors"
	"fmt"
	"strings"

	"diamante/crypto"

	"github.com/sirupsen/logrus"
	"golang.org/x/crypto/pbkdf2"
)

// MnemonicGenerator handles BIP39-like mnemonic generation and wallet recovery
type MnemonicGenerator struct {
	wordList []string
	logger   *logrus.Logger
}

// NewMnemonicGenerator creates a new mnemonic generator
func NewMnemonicGenerator(logger *logrus.Logger) (*MnemonicGenerator, error) {
	if logger == nil {
		logger = logrus.New()
		logger.SetLevel(logrus.InfoLevel)
	}

	return &MnemonicGenerator{
		wordList: BIP39EnglishWordList,
		logger:   logger,
	}, nil
}

// GenerateMnemonic creates a new 24-word mnemonic phrase
func (mg *MnemonicGenerator) GenerateMnemonic() (string, error) {
	// Generate 256 bits of entropy for 24 words
	entropy := make([]byte, 32)
	if _, err := rand.Read(entropy); err != nil {
		return "", fmt.Errorf("failed to generate entropy: %w", err)
	}

	return mg.entropyToMnemonic(entropy)
}

// GenerateMnemonicWithStrength creates a mnemonic with specified strength
// strength must be 128, 160, 192, 224, or 256 bits
func (mg *MnemonicGenerator) GenerateMnemonicWithStrength(bitSize int) (string, error) {
	if bitSize%32 != 0 || bitSize < 128 || bitSize > 256 {
		return "", fmt.Errorf("invalid bit size: must be 128, 160, 192, 224, or 256")
	}

	entropy := make([]byte, bitSize/8)
	if _, err := rand.Read(entropy); err != nil {
		return "", fmt.Errorf("failed to generate entropy: %w", err)
	}

	return mg.entropyToMnemonic(entropy)
}

// entropyToMnemonic converts entropy to mnemonic words
func (mg *MnemonicGenerator) entropyToMnemonic(entropy []byte) (string, error) {
	// Add checksum
	checksum := sha256.Sum256(entropy)
	checksumBits := len(entropy) / 4 // 1 bit per 4 bytes of entropy

	// Combine entropy and checksum bits
	combined := make([]byte, len(entropy)+1)
	copy(combined, entropy)
	combined[len(entropy)] = checksum[0]

	// Convert to word indices
	wordCount := (len(entropy)*8 + checksumBits) / 11
	words := make([]string, wordCount)

	for i := 0; i < wordCount; i++ {
		// Extract 11 bits for word index
		bitOffset := i * 11
		byteOffset := bitOffset / 8
		bitShift := uint(bitOffset % 8)

		var index uint16
		if byteOffset+2 < len(combined) {
			index = uint16(combined[byteOffset])<<8 | uint16(combined[byteOffset+1])
			index = index >> (5 - bitShift)
		} else if byteOffset+1 < len(combined) {
			index = uint16(combined[byteOffset])<<8 | uint16(combined[byteOffset+1])
			index = index >> (5 - bitShift)
		} else {
			index = uint16(combined[byteOffset]) << (3 + bitShift)
		}
		index &= 0x7FF // Keep only 11 bits

		if int(index) >= len(mg.wordList) {
			index = index % uint16(len(mg.wordList))
		}
		words[i] = mg.wordList[index]
	}

	mnemonic := strings.Join(words, " ")
	mg.logger.WithField("wordCount", len(words)).Debug("Generated mnemonic phrase")
	return mnemonic, nil
}

// ValidateMnemonic checks if a mnemonic phrase is valid
func (mg *MnemonicGenerator) ValidateMnemonic(mnemonic string) error {
	words := strings.Fields(mnemonic)

	// Validate word count
	validWordCounts := map[int]bool{12: true, 15: true, 18: true, 21: true, 24: true}
	if !validWordCounts[len(words)] {
		return fmt.Errorf("invalid word count: %d", len(words))
	}

	// Validate each word
	wordMap := make(map[string]int)
	for i, word := range mg.wordList {
		wordMap[word] = i
	}

	for _, word := range words {
		if _, exists := wordMap[word]; !exists {
			return fmt.Errorf("invalid word: %s", word)
		}
	}

	return nil
}

// MnemonicToSeed converts a mnemonic phrase to a seed using PBKDF2
func (mg *MnemonicGenerator) MnemonicToSeed(mnemonic, passphrase string) ([]byte, error) {
	if err := mg.ValidateMnemonic(mnemonic); err != nil {
		return nil, fmt.Errorf("invalid mnemonic: %w", err)
	}

	salt := "mnemonic" + passphrase
	seed := pbkdf2.Key([]byte(mnemonic), []byte(salt), 2048, 64, sha512.New)

	mg.logger.Debug("Generated seed from mnemonic")
	return seed, nil
}

// GenerateWalletFromMnemonic creates a new wallet from a mnemonic phrase
func GenerateWalletFromMnemonic(mnemonic, passphrase string, config *Config, logger *logrus.Logger) (*Wallet, error) {
	if config == nil {
		defaultConfig, err := DefaultConfig()
		if err != nil {
			return nil, fmt.Errorf("failed to get default config: %w", err)
		}
		config = defaultConfig
	}

	mg, err := NewMnemonicGenerator(logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create mnemonic generator: %w", err)
	}

	// Generate seed from mnemonic
	seed, err := mg.MnemonicToSeed(mnemonic, passphrase)
	if err != nil {
		return nil, fmt.Errorf("failed to generate seed: %w", err)
	}

	// Create deterministic keys from seed
	return generateWalletFromSeed(seed, config, logger)
}

// generateWalletFromSeed creates a wallet with deterministic keys from a seed
func generateWalletFromSeed(seed []byte, config *Config, logger *logrus.Logger) (*Wallet, error) {
	// Use first 32 bytes for KEM key derivation
	kemSeed := seed[:32]
	// Use last 32 bytes for signature key derivation
	sigSeed := seed[32:]

	// Create crypto manager
	cm, err := crypto.NewCryptoManager(config.KyberLevel, config.DilithiumLevel, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create crypto manager: %w", err)
	}

	// Generate deterministic KEM key pair
	kemKP, err := generateDeterministicKEMKeyPair(kemSeed, config.KyberLevel, cm)
	if err != nil {
		return nil, fmt.Errorf("failed to generate KEM key pair: %w", err)
	}

	// Generate deterministic signature key pair
	sigKP, err := generateDeterministicSigKeyPair(sigSeed, config.DilithiumLevel)
	if err != nil {
		return nil, fmt.Errorf("failed to generate signature key pair: %w", err)
	}

	// Generate deterministic account ID from public keys
	accountID := GenerateDeterministicAccountID(kemKP.PublicKey, sigKP.PublicKey)

	wallet := &Wallet{
		ID:            accountID,
		Nonce:         0,
		KEMKeyPair:    kemKP,
		SigKeyPair:    sigKP,
		CryptoManager: cm,
	}

	logger.WithField("walletID", accountID).Info("Generated wallet from mnemonic")
	return wallet, nil
}

// generateDeterministicKEMKeyPair generates a deterministic Kyber key pair from a seed
func generateDeterministicKEMKeyPair(seed []byte, kyberLevel int, cm *crypto.CryptoManager) (*crypto.KyberKeyPair, error) {
	// Use the deterministic key generation from deterministic_keys.go
	return GenerateDeterministicKyberKeyPair(seed, kyberLevel)
}

// generateDeterministicSigKeyPair generates a deterministic Dilithium key pair from a seed
func generateDeterministicSigKeyPair(seed []byte, dilithiumLevel int) (*crypto.DilithiumKeyPair, error) {
	// Use the deterministic key generation from deterministic_keys.go
	return GenerateDeterministicDilithiumKeyPair(seed, dilithiumLevel)
}

// GenerateDeterministicAccountID generates a deterministic account ID from public keys
func GenerateDeterministicAccountID(kemPubKey, sigPubKey []byte) string {
	// Use SHA256 to generate a deterministic hash
	h := sha256.New()
	h.Write([]byte("diamante-wallet-id"))
	h.Write(kemPubKey)
	h.Write(sigPubKey)
	hash := h.Sum(nil)

	// Convert first 16 bytes to hex for account ID
	return fmt.Sprintf("%x", hash[:16])
}

// WalletWithMnemonic extends Wallet to include mnemonic phrase
type WalletWithMnemonic struct {
	*Wallet
	Mnemonic string
}

// GenerateWalletWithMnemonic creates a new wallet and returns it with the mnemonic
func GenerateWalletWithMnemonic(config *Config, logger *logrus.Logger) (*WalletWithMnemonic, error) {
	mg, err := NewMnemonicGenerator(logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create mnemonic generator: %w", err)
	}

	// Generate 24-word mnemonic
	mnemonic, err := mg.GenerateMnemonic()
	if err != nil {
		return nil, fmt.Errorf("failed to generate mnemonic: %w", err)
	}

	// Generate wallet from mnemonic with empty passphrase
	wallet, err := GenerateWalletFromMnemonic(mnemonic, "", config, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to generate wallet from mnemonic: %w", err)
	}

	return &WalletWithMnemonic{
		Wallet:   wallet,
		Mnemonic: mnemonic,
	}, nil
}

// ExportMnemonicEncrypted exports an encrypted mnemonic phrase
func ExportMnemonicEncrypted(mnemonic string, password []byte) ([]byte, error) {
	if len(password) < 8 {
		return nil, errors.New("password must be at least 8 characters")
	}

	// Derive encryption key from password
	salt := make([]byte, 32)
	if _, err := rand.Read(salt); err != nil {
		return nil, fmt.Errorf("failed to generate salt: %w", err)
	}

	key := pbkdf2.Key(password, salt, 10000, 32, sha256.New)

	// Encrypt mnemonic
	encrypted, err := crypto.EncryptWithShared([]byte(mnemonic), key)
	if err != nil {
		return nil, fmt.Errorf("failed to encrypt mnemonic: %w", err)
	}

	// Combine salt and encrypted data
	result := make([]byte, 32+len(encrypted))
	copy(result[:32], salt)
	copy(result[32:], encrypted)

	return result, nil
}

// ImportMnemonicEncrypted decrypts and imports an encrypted mnemonic phrase
func ImportMnemonicEncrypted(encryptedData []byte, password []byte) (string, error) {
	if len(encryptedData) < 32 {
		return "", errors.New("invalid encrypted data")
	}

	// Extract salt and encrypted mnemonic
	salt := encryptedData[:32]
	encrypted := encryptedData[32:]

	// Derive decryption key
	key := pbkdf2.Key(password, salt, 10000, 32, sha256.New)

	// Decrypt mnemonic
	decrypted, err := crypto.DecryptWithShared(encrypted, key)
	if err != nil {
		return "", fmt.Errorf("failed to decrypt mnemonic: %w", err)
	}

	return string(decrypted), nil
}

// SecureMnemonicStorage provides secure storage for mnemonic phrases
type SecureMnemonicStorage struct {
	encryptedMnemonic []byte
	salt              []byte
}

// NewSecureMnemonicStorage creates a new secure storage for a mnemonic
func NewSecureMnemonicStorage(mnemonic string, password []byte) (*SecureMnemonicStorage, error) {
	encrypted, err := ExportMnemonicEncrypted(mnemonic, password)
	if err != nil {
		return nil, err
	}

	return &SecureMnemonicStorage{
		encryptedMnemonic: encrypted[32:],
		salt:              encrypted[:32],
	}, nil
}

// Unlock retrieves the mnemonic using the password
func (sms *SecureMnemonicStorage) Unlock(password []byte) (string, error) {
	data := make([]byte, 32+len(sms.encryptedMnemonic))
	copy(data[:32], sms.salt)
	copy(data[32:], sms.encryptedMnemonic)

	return ImportMnemonicEncrypted(data, password)
}

// Clear securely clears the encrypted mnemonic from memory
func (sms *SecureMnemonicStorage) Clear() {
	for i := range sms.encryptedMnemonic {
		sms.encryptedMnemonic[i] = 0
	}
	for i := range sms.salt {
		sms.salt[i] = 0
	}
}

// MnemonicMetadata stores metadata about a mnemonic-based wallet
type MnemonicMetadata struct {
	WordCount     int    `json:"wordCount"`
	HasPassphrase bool   `json:"hasPassphrase"`
	CreatedAt     int64  `json:"createdAt"`
	Algorithm     string `json:"algorithm"`
}
