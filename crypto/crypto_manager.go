// crypto_manager.go
package crypto

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"golang.org/x/crypto/sha3"
)

var (
	// ErrInvalidKyberLevel indicates an unsupported Kyber security level
	ErrInvalidKyberLevel = errors.New("invalid kyber security level")

	// ErrInvalidDilithiumLevel indicates an unsupported Dilithium security level
	ErrInvalidDilithiumLevel = errors.New("invalid dilithium security level")

	// ErrNilInput indicates a required input parameter was nil
	ErrNilInput = errors.New("required input parameter is nil")

	// ErrEmptyInput indicates a required byte array was empty
	ErrEmptyInput = errors.New("required byte array is empty")

	// ErrSigningTimeout indicates a signing operation timed out
	ErrSigningTimeout = errors.New("signing operation timed out")

	// ErrInvalidKeyLength indicates a key with an incorrect length
	ErrInvalidKeyLength = errors.New("invalid key length")
)

// KeyManager is an alias for CryptoManager for backward compatibility
type KeyManager = CryptoManager

// CryptoManager orchestrates:
//   - Kyber (KEM) for encryption/key encapsulation
//   - Dilithium for signatures
type CryptoManager struct {
	logger *logrus.Logger
	mu     sync.RWMutex

	// KEM
	kyberLevel  int
	kyberCrypto *KyberCrypto

	// Sig
	dilithiumLevel int

	// Performance metrics
	metrics cryptoMetrics
}

// cryptoMetrics tracks performance and operation counts
type cryptoMetrics struct {
	encapsulationCount   uint64
	decapsulationCount   uint64
	signatureCount       uint64
	verificationCount    uint64
	lastKeyRotation      time.Time
	avgEncapsulationTime time.Duration
	avgDecapsulationTime time.Duration
	avgSignTime          time.Duration
	avgVerifyTime        time.Duration
}

// CryptoManagerOption defines functional options for CryptoManager
type CryptoManagerOption func(*CryptoManager)

// WithLogger sets a custom logger for the CryptoManager
func WithLogger(logger *logrus.Logger) CryptoManagerOption {
	return func(cm *CryptoManager) {
		cm.logger = logger
	}
}

// WithCustomTimeouts sets custom timeouts for cryptographic operations
func WithCustomTimeouts(signingTimeout time.Duration) CryptoManagerOption {
	return func(cm *CryptoManager) {
		// Store timeouts in the manager if needed
		// This is just a placeholder for the pattern
	}
}

// NewCryptoManager builds the manager with chosen Kyber & Dilithium levels.
func NewCryptoManager(kyberLevel, dilLevel int, logger *logrus.Logger, opts ...CryptoManagerOption) (*CryptoManager, error) {
	if logger == nil {
		logger = logrus.New()
		logger.SetLevel(logrus.InfoLevel)
	}

	logger.WithFields(logrus.Fields{
		"KyberLevel":     kyberLevel,
		"DilithiumLevel": dilLevel,
	}).Info("Initializing CryptoManager")

	kyberScheme := KyberSchemeFromLevel(kyberLevel)
	if kyberScheme == nil {
		return nil, fmt.Errorf("%w: %d", ErrInvalidKyberLevel, kyberLevel)
	}

	// Validate Dilithium level
	if !isValidDilithiumLevel(dilLevel) {
		return nil, fmt.Errorf("%w: %d", ErrInvalidDilithiumLevel, dilLevel)
	}

	kyb := NewKyberCrypto(kyberScheme, logger)

	cm := &CryptoManager{
		logger:         logger,
		kyberLevel:     kyberLevel,
		kyberCrypto:    kyb,
		dilithiumLevel: dilLevel,
		metrics: cryptoMetrics{
			lastKeyRotation: time.Now(),
		},
	}

	// Apply functional options
	for _, opt := range opts {
		opt(cm)
	}

	return cm, nil
}

// isValidDilithiumLevel validates the provided Dilithium security level
func isValidDilithiumLevel(level int) bool {
	return level == DilithiumLevel2 || level == DilithiumLevel3 || level == DilithiumLevel5
}

// GenerateKEMKeyPair produces a new Kyber key pair with proper error handling
func (cm *CryptoManager) GenerateKEMKeyPair() (*KyberKeyPair, error) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	startTime := time.Now()
	keyPair, err := cm.kyberCrypto.GenerateKeyPair()

	if err != nil {
		cm.logger.WithError(err).Error("Failed to generate Kyber key pair")
		return nil, fmt.Errorf("failed to generate Kyber key pair: %w", err)
	}

	// Validate key pair
	if len(keyPair.PublicKey) == 0 || len(keyPair.PrivateKey) == 0 {
		return nil, errors.New("generated key pair contains empty keys")
	}

	cm.logger.WithFields(logrus.Fields{
		"pubKeySize":  len(keyPair.PublicKey),
		"privKeySize": len(keyPair.PrivateKey),
		"duration":    time.Since(startTime),
	}).Debug("Generated Kyber key pair")

	return keyPair, nil
}

// EncryptKEM encapsulates a shared secret using a public key (wrapper for EncapsulateFromBytes)
func (cm *CryptoManager) EncryptKEM(pubKeyBytes []byte) ([]byte, []byte, error) {
	if len(pubKeyBytes) == 0 {
		return nil, nil, ErrEmptyInput
	}

	startTime := time.Now()
	ciphertext, sharedSecret, err := cm.kyberCrypto.EncapsulateFromBytes(pubKeyBytes)

	cm.mu.Lock()
	cm.metrics.encapsulationCount++
	// Update average time calculation
	if cm.metrics.avgEncapsulationTime == 0 {
		cm.metrics.avgEncapsulationTime = time.Since(startTime)
	} else {
		// Exponential moving average
		cm.metrics.avgEncapsulationTime = (cm.metrics.avgEncapsulationTime*9 + time.Since(startTime)) / 10
	}
	cm.mu.Unlock()

	if err != nil {
		cm.logger.WithError(err).Error("KEM encapsulation failed")
		return nil, nil, fmt.Errorf("KEM encapsulation failed: %w", err)
	}

	cm.logger.WithFields(logrus.Fields{
		"ciphertextSize":   len(ciphertext),
		"sharedSecretSize": len(sharedSecret),
		"duration":         time.Since(startTime),
	}).Debug("KEM encapsulation completed")

	return ciphertext, sharedSecret, nil
}

// DecryptKEM decapsulates a shared secret using a private key (wrapper for DecapsulateFromBytes)
func (cm *CryptoManager) DecryptKEM(privKeyBytes, ciphertext []byte) ([]byte, error) {
	if len(privKeyBytes) == 0 || len(ciphertext) == 0 {
		return nil, ErrEmptyInput
	}

	startTime := time.Now()
	sharedSecret, err := cm.kyberCrypto.DecapsulateFromBytes(privKeyBytes, ciphertext)

	cm.mu.Lock()
	cm.metrics.decapsulationCount++
	// Update average time calculation
	if cm.metrics.avgDecapsulationTime == 0 {
		cm.metrics.avgDecapsulationTime = time.Since(startTime)
	} else {
		// Exponential moving average
		cm.metrics.avgDecapsulationTime = (cm.metrics.avgDecapsulationTime*9 + time.Since(startTime)) / 10
	}
	cm.mu.Unlock()

	if err != nil {
		cm.logger.WithError(err).Error("KEM decapsulation failed")
		return nil, fmt.Errorf("KEM decapsulation failed: %w", err)
	}

	cm.logger.WithFields(logrus.Fields{
		"sharedSecretSize": len(sharedSecret),
		"duration":         time.Since(startTime),
	}).Debug("KEM decapsulation completed")

	return sharedSecret, nil
}

// GenerateSignatureKeyPair produces a new Dilithium key pair
func (cm *CryptoManager) GenerateSignatureKeyPair() (*DilithiumKeyPair, error) {
	startTime := time.Now()
	keyPair, err := GenerateDilithiumKeyPair(cm.dilithiumLevel)

	if err != nil {
		cm.logger.WithError(err).Error("Failed to generate Dilithium key pair")
		return nil, fmt.Errorf("failed to generate Dilithium key pair: %w", err)
	}

	// Validate key pair
	if len(keyPair.PublicKey) == 0 || len(keyPair.PrivateKey) == 0 {
		return nil, errors.New("generated Dilithium key pair contains empty keys")
	}

	cm.logger.WithFields(logrus.Fields{
		"pubKeySize":     len(keyPair.PublicKey),
		"privKeySize":    len(keyPair.PrivateKey),
		"dilithiumLevel": cm.dilithiumLevel,
		"duration":       time.Since(startTime),
	}).Debug("Generated Dilithium key pair")

	return keyPair, nil
}

// Sign signs a message with Dilithium with a timeout context
func (cm *CryptoManager) Sign(priv *DilithiumKeyPair, message []byte) ([]byte, error) {
	return cm.SignWithContext(context.Background(), priv, message)
}

// SignWithContext signs with Dilithium but allows context-based cancellation/timeout
func (cm *CryptoManager) SignWithContext(ctx context.Context, priv *DilithiumKeyPair, message []byte) ([]byte, error) {
	if priv == nil {
		return nil, ErrNilInput
	}
	if len(message) == 0 {
		return nil, ErrEmptyInput
	}

	// Validate private key
	if len(priv.PrivateKey) == 0 {
		return nil, errors.New("private key is empty")
	}

	type result struct {
		sig []byte
		err error
	}
	done := make(chan result, 1)
	startTime := time.Now()

	go func() {
		sig, err := SignDilithium(cm.dilithiumLevel, priv.PrivateKey, message)
		select {
		case done <- result{sig, err}:
			// Result sent successfully
		case <-ctx.Done():
			// Context cancelled, but signing finished, return result is discarded
		}
	}()

	select {
	case r := <-done:
		cm.mu.Lock()
		cm.metrics.signatureCount++
		// Update avg signing time metric
		if cm.metrics.avgSignTime == 0 {
			cm.metrics.avgSignTime = time.Since(startTime)
		} else {
			cm.metrics.avgSignTime = (cm.metrics.avgSignTime*9 + time.Since(startTime)) / 10
		}
		cm.mu.Unlock()

		if r.err != nil {
			cm.logger.WithError(r.err).Error("Dilithium signing failed")
			return nil, fmt.Errorf("dilithium signing failed: %w", r.err)
		}

		cm.logger.WithFields(logrus.Fields{
			"signatureSize": len(r.sig),
			"messageSize":   len(message),
			"duration":      time.Since(startTime),
		}).Debug("Dilithium signing completed successfully")

		return r.sig, nil

	case <-ctx.Done():
		cm.logger.Error("Dilithium signing timed out or cancelled")
		return nil, fmt.Errorf("%w: %v", ErrSigningTimeout, ctx.Err())
	}
}

// Verify checks a Dilithium signature
func (cm *CryptoManager) Verify(pub *DilithiumKeyPair, msg, sig []byte) (bool, error) {
	if pub == nil {
		return false, ErrNilInput
	}
	if len(msg) == 0 || len(sig) == 0 {
		return false, ErrEmptyInput
	}

	// Validate public key
	if len(pub.PublicKey) == 0 {
		return false, errors.New("public key is empty")
	}

	startTime := time.Now()
	ok, err := VerifyDilithium(cm.dilithiumLevel, pub.PublicKey, msg, sig)

	cm.mu.Lock()
	cm.metrics.verificationCount++
	// Update avg verification time
	if cm.metrics.avgVerifyTime == 0 {
		cm.metrics.avgVerifyTime = time.Since(startTime)
	} else {
		cm.metrics.avgVerifyTime = (cm.metrics.avgVerifyTime*9 + time.Since(startTime)) / 10
	}
	cm.mu.Unlock()

	if err != nil {
		cm.logger.WithError(err).Error("Dilithium verification error")
		return false, fmt.Errorf("dilithium verification failed: %w", err)
	}

	cm.logger.WithFields(logrus.Fields{
		"signatureSize": len(sig),
		"messageSize":   len(msg),
		"valid":         ok,
		"duration":      time.Since(startTime),
	}).Debug("Dilithium verification completed")

	return ok, nil
}

// CombinedEncryptAndSign performs KEM encapsulation followed by Dilithium signing
func (cm *CryptoManager) CombinedEncryptAndSign(pubKeyKEM []byte, dilPriv *DilithiumKeyPair, message []byte) (ciphertext []byte, encrypted []byte, signature []byte, err error) {
	if len(pubKeyKEM) == 0 || dilPriv == nil || len(message) == 0 {
		return nil, nil, nil, ErrEmptyInput
	}

	// 1) Encaps => ephemeral sharedSecret
	ct, ss, err := cm.EncryptKEM(pubKeyKEM)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("encapsulation error: %w", err)
	}

	// 2) AES-GCM
	enc, err := EncryptWithShared(message, ss)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("AES encryption error: %w", err)
	}

	// 3) Sign with timeout context
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	sig, err := cm.SignWithContext(ctx, dilPriv, message)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("dilithium sign error: %w", err)
	}

	cm.logger.WithFields(logrus.Fields{
		"ciphertextSize": len(ct),
		"encryptedSize":  len(enc),
		"signatureSize":  len(sig),
		"messageSize":    len(message),
	}).Debug("Combined encrypt and sign completed")

	return ct, enc, sig, nil
}

// CombinedDecryptAndVerify decapsulates a shared secret, decrypts data, and verifies a signature
func (cm *CryptoManager) CombinedDecryptAndVerify(privKeyKEM []byte, dilPub *DilithiumKeyPair, ciphertext, encData, signature []byte) ([]byte, bool, error) {
	if len(privKeyKEM) == 0 || dilPub == nil || len(ciphertext) == 0 || len(encData) == 0 || len(signature) == 0 {
		return nil, false, ErrEmptyInput
	}

	// 1) Decapsulate shared secret
	ss, err := cm.DecryptKEM(privKeyKEM, ciphertext)
	if err != nil {
		return nil, false, fmt.Errorf("decapsulation error: %w", err)
	}

	// 2) Decrypt data
	pt, err := DecryptWithShared(encData, ss)
	if err != nil {
		return nil, false, fmt.Errorf("AES decryption error: %w", err)
	}

	// 3) Verify signature
	ok, err := cm.Verify(dilPub, pt, signature)
	if err != nil {
		return nil, false, fmt.Errorf("signature verification error: %w", err)
	}

	cm.logger.WithFields(logrus.Fields{
		"decryptedSize":  len(pt),
		"signatureValid": ok,
	}).Debug("Combined decrypt and verify completed")

	return pt, ok, nil
}

// GenerateRandomBytes creates a secure random byte slice of specified length
func (cm *CryptoManager) GenerateRandomBytes(n int) ([]byte, error) {
	if n <= 0 {
		return nil, errors.New("invalid length: must be positive")
	}

	buf := make([]byte, n)
	_, err := rand.Read(buf)
	if err != nil {
		return nil, fmt.Errorf("random read failed: %w", err)
	}
	return buf, nil
}

// DeriveKey implements a key derivation function using SHAKE256 (XOF)
func (cm *CryptoManager) DeriveKey(seed []byte, length int) ([]byte, error) {
	if len(seed) == 0 {
		return nil, ErrEmptyInput
	}

	if length <= 0 {
		return nil, errors.New("invalid length: must be positive")
	}

	h := sha3.NewShake256()
	_, err := h.Write(seed)
	if err != nil {
		return nil, fmt.Errorf("SHAKE256 write failed: %w", err)
	}

	out := make([]byte, length)
	_, err = h.Read(out)
	if err != nil {
		return nil, fmt.Errorf("SHAKE256 read failed: %w", err)
	}

	return out, nil
}

// SerializeKyberPublicKey formats a Kyber public key with length prefix
func (cm *CryptoManager) SerializeKyberPublicKey(pub []byte) ([]byte, error) {
	if len(pub) == 0 {
		return nil, ErrEmptyInput
	}

	sz := len(pub)
	out := make([]byte, sz+4)
	out[0] = byte(sz >> 24)
	out[1] = byte(sz >> 16)
	out[2] = byte(sz >> 8)
	out[3] = byte(sz)
	copy(out[4:], pub)
	return out, nil
}

// DeserializeKyberPublicKey parses a length-prefixed Kyber public key
func (cm *CryptoManager) DeserializeKyberPublicKey(data []byte) ([]byte, error) {
	if len(data) < 4 {
		return nil, errors.New("input too short for length prefix")
	}

	sz := (int(data[0]) << 24) | (int(data[1]) << 16) | (int(data[2]) << 8) | int(data[3])
	if sz <= 0 || len(data) != sz+4 {
		return nil, errors.New("invalid data format: size mismatch")
	}

	return data[4:], nil
}

// GetMetrics returns current crypto operation metrics
func (cm *CryptoManager) GetMetrics() map[string]interface{} {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	return map[string]interface{}{
		"encapsulations":       cm.metrics.encapsulationCount,
		"decapsulations":       cm.metrics.decapsulationCount,
		"signatures":           cm.metrics.signatureCount,
		"verifications":        cm.metrics.verificationCount,
		"avgEncapsulationTime": cm.metrics.avgEncapsulationTime.String(),
		"avgDecapsulationTime": cm.metrics.avgDecapsulationTime.String(),
		"avgSignTime":          cm.metrics.avgSignTime.String(),
		"avgVerifyTime":        cm.metrics.avgVerifyTime.String(),
		"lastKeyRotation":      cm.metrics.lastKeyRotation,
		"kyberLevel":           cm.kyberLevel,
		"dilithiumLevel":       cm.dilithiumLevel,
	}
}

// RotateKeys implements key rotation functionality (placeholder)
// In a real implementation, this would generate new keys and handle
// key transition securely
func (cm *CryptoManager) RotateKeys() error {
	cm.mu.Lock()
	cm.metrics.lastKeyRotation = time.Now()
	cm.mu.Unlock()

	cm.logger.Info("Crypto keys rotated")
	return nil
}
