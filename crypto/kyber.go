// kyber.go
package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/cloudflare/circl/kem"
	"github.com/cloudflare/circl/kem/kyber/kyber1024"
	"github.com/cloudflare/circl/kem/kyber/kyber512"
	"github.com/cloudflare/circl/kem/kyber/kyber768"
	"github.com/sirupsen/logrus"
)

var (
	// ErrInvalidKeyberScheme indicates an unsupported Kyber scheme
	ErrInvalidKyberScheme = errors.New("invalid or unsupported Kyber scheme")

	// ErrNilKyberScheme indicates a nil Kyber scheme
	ErrNilKyberScheme = errors.New("kyber scheme is nil")

	// ErrUnmarshalPublicKey indicates a failure to unmarshal a public key
	ErrUnmarshalPublicKey = errors.New("failed to unmarshal Kyber public key")

	// ErrUnmarshalPrivateKey indicates a failure to unmarshal a private key
	ErrUnmarshalPrivateKey = errors.New("failed to unmarshal Kyber private key")

	// ErrEncapsulationFailed indicates a failure during KEM encapsulation
	ErrEncapsulationFailed = errors.New("kyber encapsulation failed")

	// ErrDecapsulationFailed indicates a failure during KEM decapsulation
	ErrDecapsulationFailed = errors.New("kyber decapsulation failed")

	// ErrInvalidCiphertext indicates an invalid ciphertext structure
	ErrInvalidCiphertext = errors.New("invalid ciphertext format")
)

// Constants for the recognized security levels for Circl's Kyber subpackages.
const (
	KyberLevel512  = 512
	KyberLevel768  = 768
	KyberLevel1024 = 1024
)

// KyberSchemeFromLevel picks the correct Circl-Kyber scheme based on security level.
func KyberSchemeFromLevel(level int) kem.Scheme {
	switch level {
	case KyberLevel512:
		return kyber512.Scheme()
	case KyberLevel768:
		return kyber768.Scheme()
	case KyberLevel1024:
		return kyber1024.Scheme()
	default:
		return nil // invalid or not recognized
	}
}

// KyberKeyPair holds the marshaled public/private keys.
type KyberKeyPair struct {
	PublicKey  []byte
	PrivateKey []byte
}

// Validate checks that the key pair contains valid keys
func (kp *KyberKeyPair) Validate() error {
	if kp == nil {
		return errors.New("nil key pair")
	}

	if len(kp.PublicKey) == 0 {
		return errors.New("empty public key")
	}

	if len(kp.PrivateKey) == 0 {
		return errors.New("empty private key")
	}

	return nil
}

// KyberCrypto is a wrapper around the chosen Circl-Kyber scheme.
type KyberCrypto struct {
	scheme       kem.Scheme
	logger       *logrus.Logger
	mu           sync.RWMutex
	securityBits int // security bits for the scheme (128, 192, 256)

	// Performance stats
	stats struct {
		keyGenCount   uint64
		encapsCount   uint64
		decapsCount   uint64
		avgKeyGenTime time.Duration
		avgEncapsTime time.Duration
		avgDecapsTime time.Duration
	}
}

// KyberKeyPairSerialized is the serializable representation of a KyberKeyPair.
type KyberKeyPairSerialized struct {
	PublicKey  []byte `json:"publicKey"`
	PrivateKey []byte `json:"privateKey"`
}

// SerializeKyberKeyPair converts a KyberKeyPair into its serializable form.
func SerializeKyberKeyPair(kp *KyberKeyPair) (*KyberKeyPairSerialized, error) {
	if kp == nil {
		return nil, fmt.Errorf("nil KyberKeyPair")
	}

	if err := kp.Validate(); err != nil {
		return nil, fmt.Errorf("invalid KyberKeyPair: %w", err)
	}

	return &KyberKeyPairSerialized{
		PublicKey:  kp.PublicKey,
		PrivateKey: kp.PrivateKey,
	}, nil
}

// DeserializeKyberKeyPair converts the serialized form back to a KyberKeyPair.
func DeserializeKyberKeyPair(serialized *KyberKeyPairSerialized) (*KyberKeyPair, error) {
	if serialized == nil {
		return nil, fmt.Errorf("nil serialized KyberKeyPair")
	}

	if len(serialized.PublicKey) == 0 {
		return nil, errors.New("empty public key in serialized KyberKeyPair")
	}

	if len(serialized.PrivateKey) == 0 {
		return nil, errors.New("empty private key in serialized KyberKeyPair")
	}

	// Create key pair and validate
	kp := &KyberKeyPair{
		PublicKey:  serialized.PublicKey,
		PrivateKey: serialized.PrivateKey,
	}

	if err := kp.Validate(); err != nil {
		return nil, err
	}

	return kp, nil
}

// NewKyberCrypto constructs a KyberCrypto from the given scheme.
func NewKyberCrypto(scheme kem.Scheme, logger *logrus.Logger) *KyberCrypto {
	if logger == nil {
		logger = logrus.New()
		logger.SetLevel(logrus.InfoLevel)
	}

	// Determine security bits based on the scheme
	securityBits := 0
	if scheme != nil {
		switch scheme.Name() {
		case "Kyber512":
			securityBits = 128 // AES-128 equivalent security
		case "Kyber768":
			securityBits = 192 // AES-192 equivalent security
		case "Kyber1024":
			securityBits = 256 // AES-256 equivalent security
		}
	}

	logger.WithFields(logrus.Fields{
		"scheme":       scheme.Name(),
		"securityBits": securityBits,
	}).Info("Initializing KyberCrypto")

	return &KyberCrypto{
		scheme:       scheme,
		logger:       logger,
		securityBits: securityBits,
	}
}

// GetSecurityLevel returns the security level in bits (128, 192, or 256)
func (kc *KyberCrypto) GetSecurityLevel() int {
	kc.mu.RLock()
	defer kc.mu.RUnlock()
	return kc.securityBits
}

// GenerateKeyPair uses the Circl scheme to produce a new key pair (serialized).
func (kc *KyberCrypto) GenerateKeyPair() (*KyberKeyPair, error) {
	if kc.scheme == nil {
		return nil, ErrNilKyberScheme
	}

	startTime := time.Now()
	pub, priv, err := kc.scheme.GenerateKeyPair()
	if err != nil {
		kc.logger.WithError(err).Error("Kyber GenerateKeyPair failed")
		return nil, fmt.Errorf("kyber generate keypair: %w", err)
	}

	pubBytes, err := pub.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("kyber public key marshaling failed: %w", err)
	}

	privBytes, err := priv.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("kyber private key marshaling failed: %w", err)
	}

	// Update stats
	kc.mu.Lock()
	kc.stats.keyGenCount++
	duration := time.Since(startTime)
	if kc.stats.avgKeyGenTime == 0 {
		kc.stats.avgKeyGenTime = duration
	} else {
		kc.stats.avgKeyGenTime = (kc.stats.avgKeyGenTime*9 + duration) / 10
	}
	kc.mu.Unlock()

	kc.logger.WithFields(logrus.Fields{
		"pubKeySize":  len(pubBytes),
		"privKeySize": len(privBytes),
		"duration":    duration,
	}).Debug("Kyber key pair generated")

	return &KyberKeyPair{PublicKey: pubBytes, PrivateKey: privBytes}, nil
}

// EncapsulateFromBytes loads a Circl public key from pubKeyBytes, then does Encapsulate => (ct, ss).
func (kc *KyberCrypto) EncapsulateFromBytes(pubKeyBytes []byte) ([]byte, []byte, error) {
	if kc.scheme == nil {
		return nil, nil, ErrNilKyberScheme
	}

	if len(pubKeyBytes) == 0 {
		return nil, nil, ErrEmptyInput
	}

	startTime := time.Now()

	pubKey, err := kc.scheme.UnmarshalBinaryPublicKey(pubKeyBytes)
	if err != nil {
		kc.logger.WithError(err).Error("Failed to unmarshal Kyber public key")
		return nil, nil, fmt.Errorf("%w: %v", ErrUnmarshalPublicKey, err)
	}

	ct, ss, err := kc.scheme.Encapsulate(pubKey)
	if err != nil {
		kc.logger.WithError(err).Error("Kyber encapsulation failed")
		return nil, nil, fmt.Errorf("%w: %v", ErrEncapsulationFailed, err)
	}

	// Update stats
	kc.mu.Lock()
	kc.stats.encapsCount++
	duration := time.Since(startTime)
	if kc.stats.avgEncapsTime == 0 {
		kc.stats.avgEncapsTime = duration
	} else {
		kc.stats.avgEncapsTime = (kc.stats.avgEncapsTime*9 + duration) / 10
	}
	kc.mu.Unlock()

	kc.logger.WithFields(logrus.Fields{
		"ciphertextSize":   len(ct),
		"sharedSecretSize": len(ss),
		"duration":         duration,
	}).Debug("Kyber encapsulation completed")

	return ct, ss, nil
}

// DecapsulateFromBytes decapsulates a shared secret using a marshaled private key
func (kc *KyberCrypto) DecapsulateFromBytes(privKeyBytes, ciphertext []byte) ([]byte, error) {
	if kc.scheme == nil {
		return nil, ErrNilKyberScheme
	}

	if len(privKeyBytes) == 0 || len(ciphertext) == 0 {
		return nil, ErrEmptyInput
	}

	startTime := time.Now()

	privKey, err := kc.scheme.UnmarshalBinaryPrivateKey(privKeyBytes)
	if err != nil {
		kc.logger.WithError(err).Error("Failed to unmarshal Kyber private key")
		return nil, fmt.Errorf("%w: %v", ErrUnmarshalPrivateKey, err)
	}

	ss, err := kc.scheme.Decapsulate(privKey, ciphertext)
	if err != nil {
		kc.logger.WithError(err).Error("Kyber decapsulation failed")
		return nil, fmt.Errorf("%w: %v", ErrDecapsulationFailed, err)
	}

	// Update stats
	kc.mu.Lock()
	kc.stats.decapsCount++
	duration := time.Since(startTime)
	if kc.stats.avgDecapsTime == 0 {
		kc.stats.avgDecapsTime = duration
	} else {
		kc.stats.avgDecapsTime = (kc.stats.avgDecapsTime*9 + duration) / 10
	}
	kc.mu.Unlock()

	kc.logger.WithFields(logrus.Fields{
		"sharedSecretSize": len(ss),
		"duration":         duration,
	}).Debug("Kyber decapsulation completed")

	return ss, nil
}

// GetPerformanceStats returns performance statistics for the Kyber operations
func (kc *KyberCrypto) GetPerformanceStats() map[string]interface{} {
	kc.mu.RLock()
	defer kc.mu.RUnlock()

	return map[string]interface{}{
		"keyGenerations": kc.stats.keyGenCount,
		"encapsulations": kc.stats.encapsCount,
		"decapsulations": kc.stats.decapsCount,
		"avgKeyGenTime":  kc.stats.avgKeyGenTime.String(),
		"avgEncapsTime":  kc.stats.avgEncapsTime.String(),
		"avgDecapsTime":  kc.stats.avgDecapsTime.String(),
		"schemeName":     kc.scheme.Name(),
		"securityBits":   kc.securityBits,
	}
}

// deriveSessionKey uses SHA-256 on the sharedSecret => 256-bit key.
func deriveSessionKey(sharedSecret []byte) []byte {
	if len(sharedSecret) == 0 {
		return nil
	}

	h := sha256.Sum256(sharedSecret)
	return h[:]
}

// EncryptWithShared uses AES-GCM with the derived key => ciphertext
func EncryptWithShared(plaintext, sharedSecret []byte) ([]byte, error) {
	if len(plaintext) == 0 || len(sharedSecret) == 0 {
		return nil, ErrEmptyInput
	}

	key := deriveSessionKey(sharedSecret)
	if key == nil {
		return nil, errors.New("failed to derive session key")
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("AES cipher creation failed: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("GCM mode initialization failed: %w", err)
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("nonce generation failed: %w", err)
	}

	ciphertext := gcm.Seal(nonce, nonce, plaintext, nil)
	return ciphertext, nil
}

// DecryptWithShared uses AES-GCM with derived key => plaintext
func DecryptWithShared(ciphertext, sharedSecret []byte) ([]byte, error) {
	if len(ciphertext) == 0 || len(sharedSecret) == 0 {
		return nil, ErrEmptyInput
	}

	key := deriveSessionKey(sharedSecret)
	if key == nil {
		return nil, errors.New("failed to derive session key")
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("AES cipher creation failed: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("GCM mode initialization failed: %w", err)
	}

	nsz := gcm.NonceSize()
	if len(ciphertext) < nsz {
		return nil, fmt.Errorf("%w: too short for nonce", ErrInvalidCiphertext)
	}

	nonce, enc := ciphertext[:nsz], ciphertext[nsz:]
	plaintext, err := gcm.Open(nil, nonce, enc, nil)
	if err != nil {
		return nil, fmt.Errorf("AES-GCM decryption failed: %w", err)
	}

	return plaintext, nil
}

// ValidateKyberKeyPair checks that a key pair can properly encapsulate and decapsulate
func ValidateKyberKeyPair(scheme kem.Scheme, keyPair *KyberKeyPair) error {
	if scheme == nil {
		return ErrNilKyberScheme
	}

	if err := keyPair.Validate(); err != nil {
		return err
	}

	// Try to unmarshal keys
	pubKey, err := scheme.UnmarshalBinaryPublicKey(keyPair.PublicKey)
	if err != nil {
		return fmt.Errorf("public key unmarshaling failed: %w", err)
	}

	privKey, err := scheme.UnmarshalBinaryPrivateKey(keyPair.PrivateKey)
	if err != nil {
		return fmt.Errorf("private key unmarshaling failed: %w", err)
	}

	// Test encapsulation and decapsulation
	ct, ss1, err := scheme.Encapsulate(pubKey)
	if err != nil {
		return fmt.Errorf("encapsulation test failed: %w", err)
	}

	ss2, err := scheme.Decapsulate(privKey, ct)
	if err != nil {
		return fmt.Errorf("decapsulation test failed: %w", err)
	}

	// Check that shared secrets match
	if !compareByteSlices(ss1, ss2) {
		return errors.New("shared secret mismatch in validation")
	}

	return nil
}

// compareByteSlices compares two byte slices in constant time
func compareByteSlices(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}

	var result byte = 0
	for i := 0; i < len(a); i++ {
		result |= a[i] ^ b[i]
	}

	return result == 0
}

// KyberLevelToBytes returns the security level in bytes (16, 24, or 32)
func KyberLevelToBytes(level int) int {
	switch level {
	case KyberLevel512:
		return 16 // 128 bits = 16 bytes
	case KyberLevel768:
		return 24 // 192 bits = 24 bytes
	case KyberLevel1024:
		return 32 // 256 bits = 32 bytes
	default:
		return 32 // Default to maximum security
	}
}
