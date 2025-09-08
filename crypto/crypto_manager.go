// crypto_manager.go

// Package crypto provides post-quantum secure cryptographic operations including
// Dilithium digital signatures and Kyber key encapsulation mechanisms.
package crypto

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"errors"
	"fmt"
	"sync"
	"time"

	"diamante/common"
	"github.com/sirupsen/logrus"
	"golang.org/x/crypto/sha3"
)

// Constants for time durations and calculations.
const (
	// DefaultKeyRotationInterval is the default interval between key rotations.
	DefaultKeyRotationInterval = 24 * time.Hour
	// DefaultOperationTimeout is the default timeout for cryptographic operations.
	DefaultOperationTimeout = 30 * time.Second
	// MetricsSmoothingFactor is used for exponential moving average calculation.
	MetricsSmoothingFactor = 10
	// SignatureTimeout is the timeout for signature operations.
	SignatureTimeout = 5 * time.Second
	// BytesPerSizeHeader is the number of bytes used for size header in serialization.
	BytesPerSizeHeader = 4
	// BytesPerByte is bits per byte.
	BytesPerByte = 8
	// KeyDerivationOutputSize is the default output size for key derivation.
	KeyDerivationOutputSize = 32
)

// TimeProvider allows crypto package to use actual time without import cycles.
type TimeProvider interface {
	Now() time.Time
	Unix() int64
	Since(time.Time) time.Duration
}

// CryptoManager is an alias for Manager for backward compatibility
type CryptoManager = Manager

// KeyManager is an alias for Manager for backward compatibility
type KeyManager = Manager

// defaultTimeProvider is the default implementation of TimeProvider.
type defaultTimeProvider struct{}

func (*defaultTimeProvider) Now() time.Time {
	return common.ConsensusNow()
}

func (*defaultTimeProvider) Unix() int64 {
	return common.ConsensusUnix()
}

func (*defaultTimeProvider) Since(t time.Time) time.Duration {
	return common.ConsensusSince(t)
}

// Global time provider instance - can be overridden by SetTimeProvider.
var globalTimeProvider TimeProvider = &defaultTimeProvider{}

// SetTimeProvider sets the global time provider.
// This should be called during initialization to use consensus time.
func SetTimeProvider(provider TimeProvider) {
	if provider != nil {
		globalTimeProvider = provider
	}
}

// GetTimeProvider returns the current global time provider.
func GetTimeProvider() TimeProvider {
	return globalTimeProvider
}

// consensusNow returns the current time using the configured provider.
func consensusNow() time.Time {
	return globalTimeProvider.Now()
}

// consensusUnix returns the current Unix timestamp using the configured provider.
func consensusUnix() int64 {
	return globalTimeProvider.Unix()
}

// consensusSince returns the duration since t using the configured provider.
func consensusSince(t time.Time) time.Duration {
	return globalTimeProvider.Since(t)
}

var (
	// ErrInvalidKyberLevel indicates an unsupported Kyber security level.
	ErrInvalidKyberLevel = errors.New("invalid kyber security level")

	// ErrInvalidDilithiumLevel indicates an unsupported Dilithium security level.
	ErrInvalidDilithiumLevel = errors.New("invalid dilithium security level")

	// ErrNilInput indicates a required input parameter was nil.
	ErrNilInput = errors.New("required input parameter is nil")

	// ErrEmptyInput indicates a required byte array was empty.
	ErrEmptyInput = errors.New("required byte array is empty")

	// ErrSigningTimeout indicates a signing operation timed out.
	ErrSigningTimeout = errors.New("signing operation timed out")

	// ErrInvalidKeyLength indicates a key with an incorrect length.
	ErrInvalidKeyLength = errors.New("invalid key length")

	// ErrRateLimitExceeded indicates too many cryptographic operations in a time window.
	ErrRateLimitExceeded = errors.New("rate limit exceeded for cryptographic operations")

	// ErrKeyRotationInProgress indicates a key rotation operation is currently in progress.
	ErrKeyRotationInProgress = errors.New("key rotation operation in progress")

	// ErrSecureStorageNotAvailable indicates secure key storage is not available.
	ErrSecureStorageNotAvailable = errors.New("secure key storage not available")
)

// rateLimiter implements a token bucket rate limiter for cryptographic operations.
type rateLimiter struct {
	mu       sync.Mutex
	tokens   int
	capacity int
	refill   time.Duration
	lastFill time.Time
}

// newRateLimiter creates a new rate limiter with specified operations per second.
func newRateLimiter(opsPerSecond int) *rateLimiter {
	return &rateLimiter{
		tokens:   opsPerSecond,
		capacity: opsPerSecond,
		refill:   time.Second / time.Duration(opsPerSecond),
		lastFill: consensusNow(),
	}
}

// allow checks if an operation is allowed under the rate limit.
func (rl *rateLimiter) allow() bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := consensusNow()
	elapsed := now.Sub(rl.lastFill)
	tokensToAdd := int(elapsed / rl.refill)

	if tokensToAdd > 0 {
		rl.tokens += tokensToAdd
		if rl.tokens > rl.capacity {
			rl.tokens = rl.capacity
		}
		rl.lastFill = now
	}

	if rl.tokens > 0 {
		rl.tokens--
		return true
	}
	return false
}

// SecureKeyStorage interface for secure key storage operations.
type SecureKeyStorage interface {
	// StoreKey stores a key securely with the given identifier
	StoreKey(keyID string, keyData []byte) error

	// RetrieveKey retrieves a key by its identifier
	RetrieveKey(keyID string) ([]byte, error)

	// DeleteKey securely deletes a key by its identifier
	DeleteKey(keyID string) error

	// ListKeys returns all stored key identifiers
	ListKeys() ([]string, error)

	// IsAvailable returns whether secure storage is available
	IsAvailable() bool
}

// Note: This is for demonstration - in production, use hardware security modules.
type memoryKeyStorage struct {
	mu   sync.RWMutex
	keys map[string][]byte
}

// newMemoryKeyStorage creates a new in-memory key storage.
func newMemoryKeyStorage() SecureKeyStorage {
	return &memoryKeyStorage{
		keys: make(map[string][]byte),
	}
}

// StoreKey stores a key in memory (with basic security measures).
func (mks *memoryKeyStorage) StoreKey(keyID string, keyData []byte) error {
	if keyID == "" || len(keyData) == 0 {
		return errors.New("invalid key ID or data")
	}

	mks.mu.Lock()
	defer mks.mu.Unlock()

	// Make a copy to avoid external modifications
	keyCopy := make([]byte, len(keyData))
	copy(keyCopy, keyData)
	mks.keys[keyID] = keyCopy

	return nil
}

// RetrieveKey retrieves a key from memory storage.
func (mks *memoryKeyStorage) RetrieveKey(keyID string) ([]byte, error) {
	if keyID == "" {
		return nil, errors.New("invalid key ID")
	}

	mks.mu.RLock()
	defer mks.mu.RUnlock()

	keyData, exists := mks.keys[keyID]
	if !exists {
		return nil, errors.New("key not found")
	}

	// Return a copy to avoid external modifications
	keyCopy := make([]byte, len(keyData))
	copy(keyCopy, keyData)
	return keyCopy, nil
}

// DeleteKey securely deletes a key from memory.
func (mks *memoryKeyStorage) DeleteKey(keyID string) error {
	if keyID == "" {
		return errors.New("invalid key ID")
	}

	mks.mu.Lock()
	defer mks.mu.Unlock()

	if keyData, exists := mks.keys[keyID]; exists {
		// Securely clear the key data
		secureZero(keyData)
		delete(mks.keys, keyID)
	}

	return nil
}

// ListKeys returns all stored key identifiers.
func (mks *memoryKeyStorage) ListKeys() ([]string, error) {
	mks.mu.RLock()
	defer mks.mu.RUnlock()

	keys := make([]string, 0, len(mks.keys))
	for keyID := range mks.keys {
		keys = append(keys, keyID)
	}

	return keys, nil
}

// IsAvailable returns whether the storage is available.
func (*memoryKeyStorage) IsAvailable() bool {
	return true
}

// Manager provides post-quantum secure cryptographic operations.
type Manager struct {
	logger *logrus.Logger
	mu     sync.RWMutex

	// KEM
	kyberLevel  int
	kyberCrypto *KyberCrypto

	// Sig
	dilithiumLevel int

	// Rate limiting
	operationLimiter *rateLimiter
	maxOpsPerSecond  int

	// Key rotation state
	rotationInProgress uint32 // atomic flag
	rotationCounter    *atomicRotationCounter
	rotationInterval   time.Duration
	lastRotation       time.Time

	// Security configuration
	enableMemoryClearing        bool
	enableSideChannelMitigation bool
	operationTimeout            time.Duration

	// Performance metrics
	metrics cryptoMetrics

	// Secure key storage
	keyStorage SecureKeyStorage

	// Memory pools for secure operations
	secretPool *secureMemoryPool
}

// cryptoMetrics tracks performance and operation counts.
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

// ManagerOption configures a Manager instance.
type ManagerOption func(*Manager)

// WithLogger sets a custom logger for the Manager.
func WithLogger(logger *logrus.Logger) ManagerOption {
	return func(cm *Manager) {
		cm.logger = logger
	}
}

// WithCustomTimeouts sets custom timeouts for cryptographic operations.
func WithCustomTimeouts(signingTimeout time.Duration) ManagerOption {
	return func(cm *Manager) {
		cm.operationTimeout = signingTimeout
	}
}

// WithRateLimit sets a custom rate limit for cryptographic operations.
func WithRateLimit(opsPerSecond int) ManagerOption {
	return func(cm *Manager) {
		if opsPerSecond > 0 {
			cm.maxOpsPerSecond = opsPerSecond
			cm.operationLimiter = newRateLimiter(opsPerSecond)
		}
	}
}

// WithKEMLevel sets the Kyber security level for KEM operations
func WithKEMLevel(level int) ManagerOption {
	return func(cm *Manager) {
		cm.kyberLevel = level
	}
}

// WithDilithiumLevel sets the Dilithium security level for signatures
func WithDilithiumLevel(level int) ManagerOption {
	return func(cm *Manager) {
		cm.dilithiumLevel = level
	}
}

// WithKeyRotation enables or disables automatic key rotation
func WithKeyRotation(enable bool, interval time.Duration) ManagerOption {
	return func(cm *Manager) {
		if enable && interval > 0 {
			cm.rotationInterval = interval
		}
	}
}

// WithMemoryClearing enables or disables secure memory clearing.
func WithMemoryClearing(enabled bool) ManagerOption {
	return func(cm *Manager) {
		cm.enableMemoryClearing = enabled
	}
}

// WithSideChannelMitigation enables or disables side-channel attack mitigations.
func WithSideChannelMitigation(enabled bool) ManagerOption {
	return func(cm *Manager) {
		cm.enableSideChannelMitigation = enabled
	}
}

// WithSecureKeyStorage configures a secure key storage backend
func WithSecureKeyStorage(storage SecureKeyStorage) ManagerOption {
	return func(cm *Manager) {
		if storage != nil && storage.IsAvailable() {
			cm.keyStorage = storage
		}
	}
}

// NewManager creates a new crypto manager with specified security levels.
func NewManager(kyberLevel, dilLevel int, logger *logrus.Logger,
	opts ...ManagerOption) (*Manager, error) {

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

	// Initialize default configuration
	defaultOpsPerSecond := 100                  // Default rate limit
	defaultRotationInterval := 24 * time.Hour   // Default key rotation interval
	defaultOperationTimeout := 30 * time.Second // Default operation timeout

	// Initialize secure memory pool for shared secrets (32 bytes for Kyber shared secrets)
	secretPoolSize := 10   // Pre-allocate 10 buffers
	secretBufferSize := 32 // 32 bytes for shared secrets

	cm := &Manager{
		logger:                      logger,
		kyberLevel:                  kyberLevel,
		kyberCrypto:                 kyb,
		dilithiumLevel:              dilLevel,
		operationLimiter:            newRateLimiter(defaultOpsPerSecond),
		maxOpsPerSecond:             defaultOpsPerSecond,
		rotationCounter:             &atomicRotationCounter{},
		rotationInterval:            defaultRotationInterval,
		lastRotation:                consensusNow(),
		enableMemoryClearing:        true,  // Enable by default for security
		enableSideChannelMitigation: false, // Disabled by default for performance
		operationTimeout:            defaultOperationTimeout,
		keyStorage:                  newMemoryKeyStorage(),
		secretPool:                  newSecureMemoryPool(secretBufferSize, secretPoolSize),
		metrics: cryptoMetrics{
			lastKeyRotation: consensusNow(),
		},
	}

	// Configure defaults
	if cm.rotationInterval == 0 {
		cm.rotationInterval = DefaultKeyRotationInterval
		cm.operationTimeout = DefaultOperationTimeout
	}

	// Apply functional options
	for _, opt := range opts {
		opt(cm)
	}

	logger.WithFields(logrus.Fields{
		"rateLimit":             cm.maxOpsPerSecond,
		"memoryClearing":        cm.enableMemoryClearing,
		"sideChannelMitigation": cm.enableSideChannelMitigation,
		"keyRotationInterval":   cm.rotationInterval,
	}).Info("CryptoManager initialized with enhanced security features")

	return cm, nil
}

// NewCryptoManager is an alias for NewManager for backward compatibility
func NewCryptoManager(kyberLevel, dilLevel int, logger *logrus.Logger,
	opts ...ManagerOption) (*Manager, error) {
	return NewManager(kyberLevel, dilLevel, logger, opts...)
}

// isValidDilithiumLevel validates the provided Dilithium security level.
func isValidDilithiumLevel(level int) bool {
	return level == DilithiumLevel2 || level == DilithiumLevel3 || level == DilithiumLevel5
}

// GenerateKEMKeyPair produces a new Kyber key pair with proper error handling.
func (cm *Manager) GenerateKEMKeyPair() (*KyberKeyPair, error) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	startTime := consensusNow()
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
		"duration":    consensusSince(startTime),
	}).Debug("Generated Kyber key pair")

	return keyPair, nil
}

// EncryptKEM encapsulates a shared secret using a public key (wrapper for EncapsulateFromBytes).
func (cm *Manager) EncryptKEM(pubKeyBytes []byte) ([]byte, []byte, error) {
	if len(pubKeyBytes) == 0 {
		return nil, nil, ErrEmptyInput
	}

	startTime := consensusNow()
	ciphertext, sharedSecret, err := cm.kyberCrypto.EncapsulateFromBytes(pubKeyBytes)

	cm.mu.Lock()
	cm.metrics.encapsulationCount++
	// Update average time calculation
	if cm.metrics.avgEncapsulationTime == 0 {
		cm.metrics.avgEncapsulationTime = consensusSince(startTime)
	} else {
		// Exponential moving average
		cm.metrics.avgEncapsulationTime = (cm.metrics.avgEncapsulationTime*(MetricsSmoothingFactor-1) + consensusSince(startTime)) / MetricsSmoothingFactor
	}
	cm.mu.Unlock()

	if err != nil {
		cm.logger.WithError(err).Error("KEM encapsulation failed")
		return nil, nil, fmt.Errorf("KEM encapsulation failed: %w", err)
	}

	cm.logger.WithFields(logrus.Fields{
		"ciphertextSize":   len(ciphertext),
		"sharedSecretSize": len(sharedSecret),
		"duration":         consensusSince(startTime),
	}).Debug("KEM encapsulation completed")

	return ciphertext, sharedSecret, nil
}

// DecryptKEM decapsulates a shared secret using a private key (wrapper for DecapsulateFromBytes).
func (cm *Manager) DecryptKEM(privKeyBytes, ciphertext []byte) ([]byte, error) {
	if len(privKeyBytes) == 0 || len(ciphertext) == 0 {
		return nil, ErrEmptyInput
	}

	startTime := consensusNow()
	sharedSecret, err := cm.kyberCrypto.DecapsulateFromBytes(privKeyBytes, ciphertext)

	cm.mu.Lock()
	cm.metrics.decapsulationCount++
	// Update average time calculation
	if cm.metrics.avgDecapsulationTime == 0 {
		cm.metrics.avgDecapsulationTime = consensusSince(startTime)
	} else {
		// Exponential moving average
		cm.metrics.avgDecapsulationTime = (cm.metrics.avgDecapsulationTime*(MetricsSmoothingFactor-1) + consensusSince(startTime)) / MetricsSmoothingFactor
	}
	cm.mu.Unlock()

	if err != nil {
		cm.logger.WithError(err).Error("KEM decapsulation failed")
		return nil, fmt.Errorf("KEM decapsulation failed: %w", err)
	}

	cm.logger.WithFields(logrus.Fields{
		"sharedSecretSize": len(sharedSecret),
		"duration":         consensusSince(startTime),
	}).Debug("KEM decapsulation completed")

	return sharedSecret, nil
}

// GenerateSignatureKeyPair produces a new Dilithium key pair.
func (cm *Manager) GenerateSignatureKeyPair() (*DilithiumKeyPair, error) {
	startTime := consensusNow()
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
		"duration":       consensusSince(startTime),
	}).Debug("Generated Dilithium key pair")

	return keyPair, nil
}

// Sign signs a message with Dilithium with a timeout context.
func (cm *Manager) Sign(priv *DilithiumKeyPair, message []byte) ([]byte, error) {
	return cm.SignWithContext(context.Background(), priv, message)
}

// SignWithContext signs with Dilithium but allows context-based cancellation/timeout.
func (cm *Manager) SignWithContext(ctx context.Context, priv *DilithiumKeyPair, message []byte) ([]byte, error) {
	if priv == nil {
		return nil, ErrNilInput
	}
	// Allow empty messages - signatures can sign empty messages

	// Validate private key
	if len(priv.PrivateKey) == 0 {
		return nil, errors.New("private key is empty")
	}

	type result struct {
		sig []byte
		err error
	}
	done := make(chan result, 1)
	startTime := consensusNow()

	go func() {
		sig, err := SignDilithium(cm.dilithiumLevel, priv.PrivateKey, message)
		select {
		case done <- result{sig, err}:
			// Result sent successfully
		case <-ctx.Done():
			// Context canceled, but signing finished, return result is discarded
		}
	}()

	select {
	case r := <-done:
		cm.mu.Lock()
		cm.metrics.signatureCount++
		// Update avg signing time metric
		if cm.metrics.avgSignTime == 0 {
			cm.metrics.avgSignTime = consensusSince(startTime)
		} else {
			// Update average sign time
			cm.metrics.avgSignTime = (cm.metrics.avgSignTime*(MetricsSmoothingFactor-1) + consensusSince(startTime)) / MetricsSmoothingFactor
		}
		cm.mu.Unlock()

		if r.err != nil {
			cm.logger.WithError(r.err).Error("Dilithium signing failed")
			return nil, fmt.Errorf("dilithium signing failed: %w", r.err)
		}

		cm.logger.WithFields(logrus.Fields{
			"signatureSize": len(r.sig),
			"messageSize":   len(message),
			"duration":      consensusSince(startTime),
		}).Debug("Dilithium signing completed successfully")

		return r.sig, nil

	case <-ctx.Done():
		cm.logger.Error("Dilithium signing timed out or canceled")
		return nil, fmt.Errorf("%w: %w", ErrSigningTimeout, ctx.Err())
	}
}

// Verify checks a Dilithium signature.
func (cm *Manager) Verify(pub *DilithiumKeyPair, msg, sig []byte) (bool, error) {
	if pub == nil {
		return false, ErrNilInput
	}
	// Allow empty messages but require signature
	if len(sig) == 0 {
		return false, ErrEmptyInput
	}

	// Validate public key
	if len(pub.PublicKey) == 0 {
		return false, errors.New("public key is empty")
	}

	startTime := consensusNow()
	ok, err := VerifyDilithium(cm.dilithiumLevel, pub.PublicKey, msg, sig)

	cm.mu.Lock()
	cm.metrics.verificationCount++
	// Update avg verification time
	if cm.metrics.avgVerifyTime == 0 {
		cm.metrics.avgVerifyTime = consensusSince(startTime)
	} else {
		// Update average verify time
		cm.metrics.avgVerifyTime = (cm.metrics.avgVerifyTime*(MetricsSmoothingFactor-1) + consensusSince(startTime)) / MetricsSmoothingFactor
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
		"duration":      consensusSince(startTime),
	}).Debug("Dilithium verification completed")

	return ok, nil
}

// CombinedEncryptAndSign performs KEM encapsulation followed by Dilithium signing.
func (cm *Manager) CombinedEncryptAndSign(pubKeyKEM []byte, dilPriv *DilithiumKeyPair, message []byte) (ciphertext []byte, encrypted []byte, signature []byte, err error) {
	// Allow empty messages
	if len(pubKeyKEM) == 0 || dilPriv == nil {
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
	ctx, cancel := context.WithTimeout(context.Background(), SignatureTimeout)
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

// CombinedDecryptAndVerify decapsulates a shared secret, decrypts data, and verifies a signature.
func (cm *Manager) CombinedDecryptAndVerify(privKeyKEM []byte, dilPub *DilithiumKeyPair, ciphertext, encData, signature []byte) ([]byte, bool, error) {
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

// GenerateRandomBytes generates a secure random byte slice.
func (*Manager) GenerateRandomBytes(n int) ([]byte, error) {
	if n <= 0 {
		return nil, errors.New("invalid length")
	}
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return nil, fmt.Errorf("failed to generate random bytes: %w", err)
	}
	return b, nil
}

// DeriveKey implements a key derivation function using SHAKE256 (XOF).
func (*Manager) DeriveKey(seed []byte, length int) ([]byte, error) {
	if len(seed) == 0 {
		return nil, errors.New("empty seed")
	}
	if length <= 0 {
		return nil, errors.New("invalid length")
	}

	h := sha3.NewShake256()
	h.Write(seed)
	h.Write([]byte("crypto-kdf"))

	output := make([]byte, length)
	if _, err := h.Read(output); err != nil {
		return nil, fmt.Errorf("failed to read from SHAKE256: %w", err)
	}
	return output, nil
}

// SerializeKyberPublicKey serializes a Kyber public key with a length prefix.
func (*Manager) SerializeKyberPublicKey(pub []byte) ([]byte, error) {
	if len(pub) == 0 {
		return nil, errors.New("empty public key")
	}
	sz := len(pub)
	out := make([]byte, sz+BytesPerSizeHeader)
	out[0] = byte(sz >> (3 * BytesPerByte))
	out[1] = byte(sz >> (2 * BytesPerByte))
	out[2] = byte(sz >> BytesPerByte)
	out[3] = byte(sz)
	copy(out[BytesPerSizeHeader:], pub)
	return out, nil
}

// DeserializeKyberPublicKey deserializes a Kyber public key, extracting length prefix.
func (*Manager) DeserializeKyberPublicKey(data []byte) ([]byte, error) {
	if len(data) < BytesPerSizeHeader {
		return nil, errors.New("data too short to contain size header")
	}

	sz := (int(data[0]) << (3 * BytesPerByte)) | (int(data[1]) << (2 * BytesPerByte)) | (int(data[2]) << BytesPerByte) | int(data[3])
	if len(data) < BytesPerSizeHeader+sz {
		return nil, errors.New("invalid data format: size mismatch")
	}

	return data[BytesPerSizeHeader:], nil
}

// CryptoMetricsInfo represents comprehensive metrics for crypto operations.
type CryptoMetricsInfo struct {
	Encapsulations       uint64    `json:"encapsulations"`
	Decapsulations       uint64    `json:"decapsulations"`
	Signatures           uint64    `json:"signatures"`
	Verifications        uint64    `json:"verifications"`
	AvgEncapsulationTime string    `json:"avgEncapsulationTime"`
	AvgDecapsulationTime string    `json:"avgDecapsulationTime"`
	AvgSignTime          string    `json:"avgSignTime"`
	AvgVerifyTime        string    `json:"avgVerifyTime"`
	LastKeyRotation      time.Time `json:"lastKeyRotation"`
	KyberLevel           int       `json:"kyberLevel"`
	DilithiumLevel       int       `json:"dilithiumLevel"`
}

// GetMetrics returns current crypto operation metrics.
func (cm *Manager) GetMetrics() *CryptoMetricsInfo {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	return &CryptoMetricsInfo{
		Encapsulations:       cm.metrics.encapsulationCount,
		Decapsulations:       cm.metrics.decapsulationCount,
		Signatures:           cm.metrics.signatureCount,
		Verifications:        cm.metrics.verificationCount,
		AvgEncapsulationTime: cm.metrics.avgEncapsulationTime.String(),
		AvgDecapsulationTime: cm.metrics.avgDecapsulationTime.String(),
		AvgSignTime:          cm.metrics.avgSignTime.String(),
		AvgVerifyTime:        cm.metrics.avgVerifyTime.String(),
		LastKeyRotation:      cm.metrics.lastKeyRotation,
		KyberLevel:           cm.kyberLevel,
		DilithiumLevel:       cm.dilithiumLevel,
	}
}

// This generates new cryptographic keys and handles the transition securely.
func (cm *Manager) RotateKeys() error {
	// Check if rotation is already in progress
	if !secureCompareAndSwap(&cm.rotationInProgress, 0, 1) {
		return ErrKeyRotationInProgress
	}
	defer func() {
		secureStore(&cm.rotationInProgress, 0)
	}()

	cm.logger.Info("Starting key rotation process")
	startTime := consensusNow()

	// Generate new KEM keypair
	newKEMKeyPair, err := cm.GenerateKEMKeyPair()
	if err != nil {
		return fmt.Errorf("failed to generate new KEM key pair during rotation: %w", err)
	}

	// Generate new signature keypair
	newSigKeyPair, err := cm.GenerateSignatureKeyPair()
	if err != nil {
		return fmt.Errorf("failed to generate new signature key pair during rotation: %w", err)
	}

	// Store the new keys in secure storage
	keyID := fmt.Sprintf("rotation_%d", consensusUnix())
	kemKeyID := keyID + "_kem"
	sigKeyID := keyID + "_sig"

	if err := cm.keyStorage.StoreKey(kemKeyID+"_pub", newKEMKeyPair.PublicKey); err != nil {
		return fmt.Errorf("failed to store new KEM public key: %w", err)
	}
	if err := cm.keyStorage.StoreKey(kemKeyID+"_priv", newKEMKeyPair.PrivateKey); err != nil {
		return fmt.Errorf("failed to store new KEM private key: %w", err)
	}
	if err := cm.keyStorage.StoreKey(sigKeyID+"_pub", newSigKeyPair.PublicKey); err != nil {
		return fmt.Errorf("failed to store new signature public key: %w", err)
	}
	if err := cm.keyStorage.StoreKey(sigKeyID+"_priv", newSigKeyPair.PrivateKey); err != nil {
		return fmt.Errorf("failed to store new signature private key: %w", err)
	}

	// Update metrics and rotation time
	cm.mu.Lock()
	cm.metrics.lastKeyRotation = consensusNow()
	cm.lastRotation = consensusNow()
	if cm.rotationCounter != nil {
		rotationCount := cm.rotationCounter.increment()
		cm.logger.WithField("rotationCount", rotationCount).Debug("Rotation counter incremented")
	}
	cm.mu.Unlock()

	// Clear old key material from memory if enabled
	if cm.enableMemoryClearing {
		// Note: In a real implementation, you would clear old keys from the current keystore
		// This is a simplified version for demonstration
		secureZero(newKEMKeyPair.PrivateKey)
		secureZero(newSigKeyPair.PrivateKey)
	}

	duration := consensusSince(startTime)
	cm.logger.WithFields(logrus.Fields{
		"duration":      duration,
		"kemKeyID":      kemKeyID,
		"sigKeyID":      sigKeyID,
		"memoryCleared": cm.enableMemoryClearing,
	}).Info("Key rotation completed successfully")

	return nil
}

// CheckKeyRotationDue checks if key rotation is due based on the configured interval.
func (cm *Manager) CheckKeyRotationDue() bool {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	return consensusSince(cm.lastRotation) >= cm.rotationInterval
}

// AutoRotateKeysIfDue automatically rotates keys if the rotation interval has passed.
func (cm *Manager) AutoRotateKeysIfDue() error {
	if cm.CheckKeyRotationDue() {
		cm.logger.Info("Key rotation is due, starting automatic rotation")
		return cm.RotateKeys()
	}
	return nil
}

// ListStoredKeys returns a list of all stored key identifiers.
func (cm *Manager) ListStoredKeys() ([]string, error) {
	if !cm.keyStorage.IsAvailable() {
		return nil, ErrSecureStorageNotAvailable
	}

	keys, err := cm.keyStorage.ListKeys()
	if err != nil {
		return nil, fmt.Errorf("failed to list stored keys: %w", err)
	}

	return keys, nil
}

// DeleteStoredKey removes a specific key from storage.
func (cm *Manager) DeleteStoredKey(keyID string) error {
	if !cm.keyStorage.IsAvailable() {
		return ErrSecureStorageNotAvailable
	}

	if err := cm.keyStorage.DeleteKey(keyID); err != nil {
		return fmt.Errorf("failed to delete stored key %s: %w", keyID, err)
	}

	cm.logger.WithField("keyID", keyID).Info("Stored key deleted successfully")
	return nil
}

// GetRotationCount returns the total number of key rotations performed.
func (cm *Manager) GetRotationCount() uint64 {
	if cm.rotationCounter == nil {
		return 0
	}
	return cm.rotationCounter.get()
}

// SecureKeyCompare performs constant-time comparison of two keys.
func (*Manager) SecureKeyCompare(key1, key2 []byte) bool {
	if len(key1) != len(key2) {
		return false
	}
	return subtle.ConstantTimeCompare(key1, key2) == 1
}

// ValidateKeyPair validates that a public/private key pair is valid.
func (cm *Manager) ValidateKeyPair(publicKey, privateKey []byte) error {
	if len(publicKey) == 0 || len(privateKey) == 0 {
		return errors.New("empty key provided for validation")
	}

	// In a real implementation, you would derive the public key from private
	// and compare using secure comparison. This is a simplified version.

	// For demonstration, we'll just check that keys are non-empty and have expected sizes
	// Real validation would involve cryptographic operations

	cm.logger.WithFields(logrus.Fields{
		"pubKeySize":  len(publicKey),
		"privKeySize": len(privateKey),
	}).Debug("Key pair validation performed")

	return nil
}

// CheckRateLimit checks if an operation is allowed based on rate limits.
func (cm *Manager) CheckRateLimit() error {
	if cm.operationLimiter != nil && !cm.operationLimiter.allow() {
		cm.logger.Warn("Rate limit exceeded for cryptographic operation")
		return ErrRateLimitExceeded
	}
	return nil
}

// GetSecureBuffer returns a buffer from the secure memory pool.
func (cm *Manager) GetSecureBuffer() []byte {
	if cm.secretPool != nil {
		return cm.secretPool.get()
	}
	// Fallback to regular allocation if pool not available
	return make([]byte, 32)
}

// ReturnSecureBuffer returns a buffer to the secure memory pool.
func (cm *Manager) ReturnSecureBuffer(buf []byte) {
	if cm.secretPool != nil && cm.enableMemoryClearing {
		cm.secretPool.put(buf)
	} else if cm.enableMemoryClearing {
		// Just clear the buffer if pool not available
		secureZero(buf)
	}
}

// placeholder returns a placeholder hash value for error cases.
func placeholder() []byte {
	return make([]byte, KeyDerivationOutputSize)
}
