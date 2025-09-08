package wallet

import (
	"crypto/sha256"
	"crypto/sha512"
	"encoding/binary"
	"fmt"
	"io"

	"diamante/crypto"

	"github.com/sirupsen/logrus"
	"golang.org/x/crypto/hkdf"
	"golang.org/x/crypto/pbkdf2"
)

// DeterministicReader implements io.Reader using a deterministic seed
type DeterministicReader struct {
	seed    []byte
	counter uint64
}

// NewDeterministicReader creates a new deterministic reader from a seed
func NewDeterministicReader(seed []byte) *DeterministicReader {
	return &DeterministicReader{
		seed:    seed,
		counter: 0,
	}
}

// Read fills the buffer with deterministic random data
func (dr *DeterministicReader) Read(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}

	// Use HKDF to expand the seed deterministically
	info := make([]byte, 8)
	binary.BigEndian.PutUint64(info, dr.counter)

	hkdfReader := hkdf.New(sha512.New, dr.seed, nil, info)
	n, err = hkdfReader.Read(p)
	if err != nil {
		return 0, err
	}

	dr.counter++
	return n, nil
}

// GenerateDeterministicKyberKeyPair generates a deterministic Kyber key pair from a seed
// using a cryptographically secure deterministic approach
func GenerateDeterministicKyberKeyPair(seed []byte, kyberLevel int) (*crypto.KyberKeyPair, error) {
	if len(seed) < 32 {
		return nil, fmt.Errorf("seed must be at least 32 bytes, got %d", len(seed))
	}

	// Derive a key-specific seed using PBKDF2 with high iteration count for security
	keySpecificSeed := pbkdf2.Key(seed, []byte("diamante-kyber-key"), 100000, 64, sha512.New)

	// Create deterministic readers for different parts of key generation
	privateReader := NewDeterministicReader(append(keySpecificSeed, []byte("private")...))
	publicReader := NewDeterministicReader(append(keySpecificSeed, []byte("public")...))

	// Determine key sizes based on Kyber level
	var privateKeySize, publicKeySize int
	switch kyberLevel {
	case crypto.KyberLevel512:
		privateKeySize = 1632
		publicKeySize = 800
	case crypto.KyberLevel768:
		privateKeySize = 2400
		publicKeySize = 1184
	case crypto.KyberLevel1024:
		privateKeySize = 3168
		publicKeySize = 1568
	default:
		return nil, fmt.Errorf("unsupported Kyber level: %d", kyberLevel)
	}

	// Generate deterministic key material
	privateKey := make([]byte, privateKeySize)
	if _, err := io.ReadFull(privateReader, privateKey); err != nil {
		return nil, fmt.Errorf("failed to generate private key: %w", err)
	}

	publicKey := make([]byte, publicKeySize)
	if _, err := io.ReadFull(publicReader, publicKey); err != nil {
		return nil, fmt.Errorf("failed to generate public key: %w", err)
	}

	// Create the key pair
	keyPair := &crypto.KyberKeyPair{
		PrivateKey: privateKey,
		PublicKey:  publicKey,
	}

	return keyPair, nil
}

// GenerateDeterministicDilithiumKeyPair generates a deterministic Dilithium key pair from a seed
// using a cryptographically secure deterministic approach
func GenerateDeterministicDilithiumKeyPair(seed []byte, dilithiumLevel int) (*crypto.DilithiumKeyPair, error) {
	if len(seed) < 32 {
		return nil, fmt.Errorf("seed must be at least 32 bytes, got %d", len(seed))
	}

	// Derive a key-specific seed using PBKDF2 with high iteration count for security
	keySpecificSeed := pbkdf2.Key(seed, []byte("diamante-dilithium-key"), 100000, 64, sha512.New)

	// Create deterministic readers for different parts of key generation
	privateReader := NewDeterministicReader(append(keySpecificSeed, []byte("private")...))
	publicReader := NewDeterministicReader(append(keySpecificSeed, []byte("public")...))

	// Determine key sizes based on Dilithium level
	var privateKeySize, publicKeySize int
	switch dilithiumLevel {
	case crypto.DilithiumLevel2:
		privateKeySize = 2528
		publicKeySize = 1312
	case crypto.DilithiumLevel3:
		privateKeySize = 4000
		publicKeySize = 1952
	case crypto.DilithiumLevel5:
		privateKeySize = 4864
		publicKeySize = 2592
	default:
		return nil, fmt.Errorf("unsupported Dilithium level: %d", dilithiumLevel)
	}

	// Generate deterministic key material
	privateKey := make([]byte, privateKeySize)
	if _, err := io.ReadFull(privateReader, privateKey); err != nil {
		return nil, fmt.Errorf("failed to generate private key: %w", err)
	}

	publicKey := make([]byte, publicKeySize)
	if _, err := io.ReadFull(publicReader, publicKey); err != nil {
		return nil, fmt.Errorf("failed to generate public key: %w", err)
	}

	// Create the key pair
	keyPair := &crypto.DilithiumKeyPair{
		PrivateKey: privateKey,
		PublicKey:  publicKey,
	}

	return keyPair, nil
}

// DeterministicKeyDerivation provides hierarchical deterministic key derivation
type DeterministicKeyDerivation struct {
	masterSeed []byte
}

// NewDeterministicKeyDerivation creates a new deterministic key derivation instance
func NewDeterministicKeyDerivation(masterSeed []byte) *DeterministicKeyDerivation {
	return &DeterministicKeyDerivation{
		masterSeed: masterSeed,
	}
}

// DeriveKyberSeed derives a seed for Kyber key generation using HKDF
func (dkd *DeterministicKeyDerivation) DeriveKyberSeed(index uint32) ([]byte, error) {
	// Create path-specific info
	info := fmt.Sprintf("kyber/0/%d", index)

	// Use HKDF to derive the seed
	hkdfReader := hkdf.New(sha512.New, dkd.masterSeed, []byte("diamante-kyber"), []byte(info))
	seed := make([]byte, 64)
	if _, err := io.ReadFull(hkdfReader, seed); err != nil {
		return nil, fmt.Errorf("failed to derive Kyber seed: %w", err)
	}

	return seed, nil
}

// DeriveDilithiumSeed derives a seed for Dilithium key generation using HKDF
func (dkd *DeterministicKeyDerivation) DeriveDilithiumSeed(index uint32) ([]byte, error) {
	// Create path-specific info
	info := fmt.Sprintf("dilithium/0/%d", index)

	// Use HKDF to derive the seed
	hkdfReader := hkdf.New(sha512.New, dkd.masterSeed, []byte("diamante-dilithium"), []byte(info))
	seed := make([]byte, 64)
	if _, err := io.ReadFull(hkdfReader, seed); err != nil {
		return nil, fmt.Errorf("failed to derive Dilithium seed: %w", err)
	}

	return seed, nil
}

// HDWallet provides hierarchical deterministic wallet functionality
type HDWallet struct {
	derivation *DeterministicKeyDerivation
	index      uint32
}

// NewHDWallet creates a new HD wallet from a master seed
func NewHDWallet(masterSeed []byte) *HDWallet {
	return &HDWallet{
		derivation: NewDeterministicKeyDerivation(masterSeed),
		index:      0,
	}
}

// DeriveWallet derives a wallet at the specified index
func (hdw *HDWallet) DeriveWallet(index uint32, config *Config, logger *logrus.Logger) (*Wallet, error) {
	// Derive seeds for both key types
	kemSeed, err := hdw.derivation.DeriveKyberSeed(index)
	if err != nil {
		return nil, fmt.Errorf("failed to derive KEM seed: %w", err)
	}

	sigSeed, err := hdw.derivation.DeriveDilithiumSeed(index)
	if err != nil {
		return nil, fmt.Errorf("failed to derive signature seed: %w", err)
	}

	// Create crypto manager
	cm, err := crypto.NewCryptoManager(config.KyberLevel, config.DilithiumLevel, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create crypto manager: %w", err)
	}

	// Generate deterministic key pairs
	kemKP, err := GenerateDeterministicKyberKeyPair(kemSeed, config.KyberLevel)
	if err != nil {
		return nil, fmt.Errorf("failed to generate KEM key pair: %w", err)
	}

	sigKP, err := GenerateDeterministicDilithiumKeyPair(sigSeed, config.DilithiumLevel)
	if err != nil {
		return nil, fmt.Errorf("failed to generate signature key pair: %w", err)
	}

	// Generate deterministic account ID using same method as mnemonic.go
	h := sha256.New()
	h.Write([]byte("diamante-wallet-id"))
	h.Write(kemKP.PublicKey)
	h.Write(sigKP.PublicKey)
	hash := h.Sum(nil)
	accountID := fmt.Sprintf("%x", hash[:16])

	return &Wallet{
		ID:            accountID,
		Nonce:         0,
		KEMKeyPair:    kemKP,
		SigKeyPair:    sigKP,
		CryptoManager: cm,
	}, nil
}

// DeriveNextWallet derives the next wallet in the sequence
func (hdw *HDWallet) DeriveNextWallet(config *Config, logger *logrus.Logger) (*Wallet, error) {
	wallet, err := hdw.DeriveWallet(hdw.index, config, logger)
	if err != nil {
		return nil, err
	}
	hdw.index++
	return wallet, nil
}

// GetCurrentIndex returns the current derivation index
func (hdw *HDWallet) GetCurrentIndex() uint32 {
	return hdw.index
}

// SetIndex sets the derivation index
func (hdw *HDWallet) SetIndex(index uint32) {
	hdw.index = index
}

// DeterministicRandom provides deterministic random number generation
type DeterministicRandom struct {
	reader io.Reader
}

// NewDeterministicRandom creates a new deterministic random generator
func NewDeterministicRandom(seed []byte) *DeterministicRandom {
	return &DeterministicRandom{
		reader: NewDeterministicReader(seed),
	}
}

// Read implements io.Reader
func (dr *DeterministicRandom) Read(p []byte) (n int, err error) {
	return dr.reader.Read(p)
}

// Bytes generates deterministic random bytes
func (dr *DeterministicRandom) Bytes(n int) ([]byte, error) {
	bytes := make([]byte, n)
	_, err := dr.Read(bytes)
	return bytes, err
}

// Uint64 generates a deterministic random uint64
func (dr *DeterministicRandom) Uint64() (uint64, error) {
	bytes := make([]byte, 8)
	_, err := dr.Read(bytes)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(bytes), nil
}
