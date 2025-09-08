// Package crypto provides post-quantum secure cryptographic operations including
// Dilithium digital signatures and Kyber key encapsulation mechanisms.
package crypto

import (
	"crypto"
	"crypto/subtle"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"time"

	"diamante/common"

	"github.com/cloudflare/circl/sign"
	"github.com/cloudflare/circl/sign/dilithium/mode2"
	"github.com/cloudflare/circl/sign/dilithium/mode3"
	"github.com/cloudflare/circl/sign/dilithium/mode5"
	"github.com/sirupsen/logrus"
)

// Dilithium security levels.
const (
	// DilithiumLevel2 provides NIST Level 2 security (roughly AES-128 equivalent).
	DilithiumLevel2 = 2

	// DilithiumLevel3 provides NIST Level 3 security (roughly AES-192 equivalent).
	DilithiumLevel3 = 3

	// DilithiumLevel5 provides NIST Level 5 security (roughly AES-256 equivalent).
	DilithiumLevel5 = 5

	// DilithiumKeyGenTestSize is the size of test messages for key generation.
	DilithiumKeyGenTestSize = 32

	// DilithiumKeySize2 is the approximate size for mode 2.
	DilithiumKeySize2 = 2420

	// DilithiumKeySize3 is the approximate size for mode 3.
	DilithiumKeySize3 = 3293

	// DilithiumKeySize5 is the approximate size for mode 5.
	DilithiumKeySize5 = 4595

	// Dilithium3 key sizes (exact values from NIST specification)
	Dilithium3PublicKeySize  = 1952
	Dilithium3PrivateKeySize = 4000
	Dilithium3SignatureSize  = 3293
)

var (
	// ErrInvalidDilithiumScheme indicates an invalid or unsupported Dilithium scheme.
	ErrInvalidDilithiumScheme = errors.New("invalid or unsupported Dilithium scheme")

	// ErrDilithiumKeyGenFailed indicates a failure during key generation.
	ErrDilithiumKeyGenFailed = errors.New("dilithium key generation failed")

	// ErrDilithiumSignFailed indicates a failure during signing.
	ErrDilithiumSignFailed = errors.New("dilithium signing failed")

	// ErrDilithiumPublicKeyRecover indicates a failure to recover public key from private key.
	ErrDilithiumPublicKeyRecover = errors.New("failed to recover Dilithium public key")
)

// dilithiumStats tracks performance metrics for Dilithium operations.
type dilithiumStats struct {
	mu                sync.RWMutex
	keyGenCount       uint64
	signCount         uint64
	verifyCount       uint64
	avgKeyGenTime     time.Duration
	avgSignTime       time.Duration
	avgVerifyTime     time.Duration
	lastOperationTime time.Time
}

var stats = dilithiumStats{
	lastOperationTime: common.ConsensusNow().UTC(), // Use consensus time for deterministic behavior
}

// DilithiumStatsInfo represents performance statistics for Dilithium operations.
type DilithiumStatsInfo struct {
	KeyGenerations    uint64    `json:"keyGenerations"`
	Signatures        uint64    `json:"signatures"`
	Verifications     uint64    `json:"verifications"`
	AvgKeyGenTime     string    `json:"avgKeyGenTime"`
	AvgSignTime       string    `json:"avgSignTime"`
	AvgVerifyTime     string    `json:"avgVerifyTime"`
	LastOperationTime time.Time `json:"lastOperationTime"`
}

// GetDilithiumStats returns performance statistics.
func GetDilithiumStats() *DilithiumStatsInfo {
	stats.mu.RLock()
	defer stats.mu.RUnlock()

	return &DilithiumStatsInfo{
		KeyGenerations:    stats.keyGenCount,
		Signatures:        stats.signCount,
		Verifications:     stats.verifyCount,
		AvgKeyGenTime:     stats.avgKeyGenTime.String(),
		AvgSignTime:       stats.avgSignTime.String(),
		AvgVerifyTime:     stats.avgVerifyTime.String(),
		LastOperationTime: stats.lastOperationTime,
	}
}

// DilithiumKeyPairSerialized is the serializable representation of a DilithiumKeyPair.
type DilithiumKeyPairSerialized struct {
	PublicKey  []byte `json:"publicKey"`
	PrivateKey []byte `json:"privateKey"`
	Level      int    `json:"level"` // Added security level
}

// SerializeDilithiumKeyPair converts a DilithiumKeyPair into its serializable form.
func SerializeDilithiumKeyPair(kp *DilithiumKeyPair) (*DilithiumKeyPairSerialized, error) {
	if kp == nil {
		return nil, errors.New("nil DilithiumKeyPair")
	}

	if err := kp.Validate(); err != nil {
		return nil, fmt.Errorf("invalid DilithiumKeyPair: %w", err)
	}

	return &DilithiumKeyPairSerialized{
		PublicKey:  kp.PublicKey,
		PrivateKey: kp.PrivateKey,
		Level:      kp.Level,
	}, nil
}

// DeserializeDilithiumKeyPair converts the serialized form back to a DilithiumKeyPair.
func DeserializeDilithiumKeyPair(serialized *DilithiumKeyPairSerialized) (*DilithiumKeyPair, error) {
	if serialized == nil {
		return nil, errors.New("nil serialized DilithiumKeyPair")
	}

	if len(serialized.PublicKey) == 0 {
		return nil, errors.New("empty public key in serialized DilithiumKeyPair")
	}

	if len(serialized.PrivateKey) == 0 {
		return nil, errors.New("empty private key in serialized DilithiumKeyPair")
	}

	// Validate level if provided, otherwise use default
	level := serialized.Level
	if level == 0 {
		level = defaultDilithiumLevel
	}

	if !isValidDilithiumLevel(level) {
		return nil, fmt.Errorf("invalid Dilithium level: %d", level)
	}

	// Create and validate key pair
	kp := &DilithiumKeyPair{
		PublicKey:  serialized.PublicKey,
		PrivateKey: serialized.PrivateKey,
		Level:      level,
	}

	if err := kp.Validate(); err != nil {
		return nil, err
	}

	return kp, nil
}

// getDilithiumScheme picks the correct subpackage scheme (mode2, 3, or 5).
func getDilithiumScheme(level int) (sign.Scheme, error) {
	switch level {
	case DilithiumLevel2:
		return mode2.Scheme(), nil
	case DilithiumLevel3:
		return mode3.Scheme(), nil
	case DilithiumLevel5:
		return mode5.Scheme(), nil
	default:
		return nil, fmt.Errorf("%w: %d", ErrInvalidDilithiumScheme, level)
	}
}

// DilithiumKeyPair holds marshaled pub/priv keys for a chosen Dilithium mode.
type DilithiumKeyPair struct {
	PublicKey  []byte
	PrivateKey []byte
	Level      int // Added security level for context
}

// Validate checks that the key pair contains valid data.
func (kp *DilithiumKeyPair) Validate() error {
	if kp == nil {
		return errors.New("nil key pair")
	}

	if len(kp.PublicKey) == 0 {
		return errors.New("empty public key")
	}

	if len(kp.PrivateKey) == 0 {
		return errors.New("empty private key")
	}

	// Validate security level
	if !isValidDilithiumLevel(kp.Level) && kp.Level != 0 {
		return fmt.Errorf("invalid Dilithium level: %d", kp.Level)
	}

	return nil
}

// GetSignatureSize returns the signature size in bytes for this key pair's level.
func (kp *DilithiumKeyPair) GetSignatureSize() int {
	level := kp.Level
	if level == 0 {
		level = defaultDilithiumLevel
	}

	return DilithiumSignatureSize(level)
}

// DilithiumSignatureSize returns the signature size in bytes for a given security level.
func DilithiumSignatureSize(level int) int {
	switch level {
	case DilithiumLevel2:
		return DilithiumKeySize2 // approximate size for mode2
	case DilithiumLevel3:
		return DilithiumKeySize3 // approximate size for mode3
	case DilithiumLevel5:
		return DilithiumKeySize5 // approximate size for mode5
	default:
		return DilithiumKeySize3 // default to level 3
	}
}

// GenerateDilithiumKeyPair generates a new Dilithium keypair for the given security level.
func GenerateDilithiumKeyPair(level int) (*DilithiumKeyPair, error) {
	if !isValidDilithiumLevel(level) {
		return nil, fmt.Errorf("%w: %d", ErrInvalidDilithiumLevel, level)
	}

	startTime := GetTimeProvider().Now()

	scheme, err := getDilithiumScheme(level)
	if err != nil {
		return nil, err
	}

	// Generate key pair
	pubKey, privKey, err := scheme.GenerateKey()
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrDilithiumKeyGenFailed, err)
	}

	// Marshal keys to bytes
	pubKeyBytes, err := pubKey.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("failed to marshal Dilithium public key: %w", err)
	}

	privKeyBytes, err := privKey.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("failed to marshal Dilithium private key: %w", err)
	}

	// Update stats
	stats.mu.Lock()
	stats.keyGenCount++
	duration := GetTimeProvider().Since(startTime)
	stats.avgKeyGenTime = (stats.avgKeyGenTime*(MetricsSmoothingFactor-1) + duration) / MetricsSmoothingFactor
	stats.lastOperationTime = GetTimeProvider().Now()
	stats.mu.Unlock()

	// Log with a default logger if needed
	logger := getLogger()
	logger.WithFields(logrus.Fields{
		"level":       level,
		"pubKeySize":  len(pubKeyBytes),
		"privKeySize": len(privKeyBytes),
		"duration":    duration,
	}).Debug("Dilithium key pair generated")

	return &DilithiumKeyPair{
		PublicKey:  pubKeyBytes,
		PrivateKey: privKeyBytes,
		Level:      level,
	}, nil
}

// getLogger returns a default logger if needed.
func getLogger() *logrus.Logger {
	logger := logrus.New()
	logger.SetLevel(logrus.InfoLevel)
	return logger
}

// SignDilithium signs a message with a Dilithium private key.
func SignDilithium(level int, privKeyBytes, message []byte) ([]byte, error) {
	if !isValidDilithiumLevel(level) {
		return nil, fmt.Errorf("%w: %d", ErrInvalidDilithiumScheme, level)
	}

	// Allow empty messages - signatures can sign empty messages
	if len(privKeyBytes) == 0 {
		return nil, ErrEmptyInput
	}

	startTime := GetTimeProvider().Now()

	scheme, err := getDilithiumScheme(level)
	if err != nil {
		return nil, err
	}

	sk, err := scheme.UnmarshalBinaryPrivateKey(privKeyBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal Dilithium private key: %w", err)
	}

	// Add optional salt parameter for non-deterministic signatures if needed
	// (nil means deterministic signatures)
	signature := scheme.Sign(sk, message, nil)

	// Update stats
	stats.mu.Lock()
	stats.signCount++
	duration := GetTimeProvider().Since(startTime)
	stats.avgSignTime = (stats.avgSignTime*(MetricsSmoothingFactor-1) + duration) / MetricsSmoothingFactor
	stats.lastOperationTime = GetTimeProvider().Now()
	stats.mu.Unlock()

	// Log with a default logger if needed
	logger := getLogger()
	logger.WithFields(logrus.Fields{
		"level":         level,
		"messageSize":   len(message),
		"signatureSize": len(signature),
		"duration":      duration,
	}).Debug("Dilithium signature created")

	return signature, nil
}

// VerifyDilithium verifies a Dilithium signature.
func VerifyDilithium(level int, pubKeyBytes, message, signature []byte) (bool, error) {
	if !isValidDilithiumLevel(level) {
		return false, fmt.Errorf("%w: %d", ErrInvalidDilithiumScheme, level)
	}

	// Allow empty messages but require signature and public key
	if len(pubKeyBytes) == 0 || len(signature) == 0 {
		return false, ErrEmptyInput
	}

	startTime := GetTimeProvider().Now()

	scheme, err := getDilithiumScheme(level)
	if err != nil {
		return false, err
	}

	pk, err := scheme.UnmarshalBinaryPublicKey(pubKeyBytes)
	if err != nil {
		return false, fmt.Errorf("failed to unmarshal Dilithium public key: %w", err)
	}

	// Verify the signature
	valid := scheme.Verify(pk, message, signature, nil)

	// Update stats
	stats.mu.Lock()
	stats.verifyCount++
	duration := GetTimeProvider().Since(startTime)
	stats.avgVerifyTime = (stats.avgVerifyTime*(MetricsSmoothingFactor-1) + duration) / MetricsSmoothingFactor
	stats.lastOperationTime = GetTimeProvider().Now()
	stats.mu.Unlock()

	// Log with a default logger if needed
	logger := getLogger()
	logger.WithFields(logrus.Fields{
		"level":         level,
		"messageSize":   len(message),
		"signatureSize": len(signature),
		"valid":         valid,
		"duration":      duration,
	}).Debug("Dilithium verification completed")

	return valid, nil
}

// DilithiumPrivateKeyToPub recovers the publicKey from a privateKey. Not always needed, but can help.
func DilithiumPrivateKeyToPub(level int, privKeyBytes []byte) ([]byte, error) {
	if !isValidDilithiumLevel(level) {
		return nil, fmt.Errorf("%w: %d", ErrInvalidDilithiumScheme, level)
	}

	if len(privKeyBytes) == 0 {
		return nil, ErrEmptyInput
	}

	scheme, err := getDilithiumScheme(level)
	if err != nil {
		return nil, err
	}

	sk, err := scheme.UnmarshalBinaryPrivateKey(privKeyBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal Dilithium private key: %w", err)
	}

	// sk.Public() returns a crypto.PublicKey, but we need the Circl sign.PublicKey:
	pubIfc := sk.Public()
	pubKey, ok := pubIfc.(sign.PublicKey)
	if !ok {
		return nil, ErrDilithiumPublicKeyRecover
	}

	// Now we can marshal:
	pubBytes, err := pubKey.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("failed to marshal Dilithium public key: %w", err)
	}
	return pubBytes, nil
}

// Provide a method to get a crypto.Signer if you want to use the standard library's abstractions.
func (kp *DilithiumKeyPair) CryptoSigner() (crypto.Signer, error) {
	level := kp.Level
	if level == 0 {
		level = defaultDilithiumLevel
	}

	return kp.CryptoSignerWithLevel(level)
}

// CryptoSignerWithLevel returns a crypto.Signer for the specified security level.
func (kp *DilithiumKeyPair) CryptoSignerWithLevel(level int) (crypto.Signer, error) {
	if err := kp.Validate(); err != nil {
		return nil, err
	}

	scheme, err := getDilithiumScheme(level)
	if err != nil {
		return nil, err
	}

	sk, err := scheme.UnmarshalBinaryPrivateKey(kp.PrivateKey)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal Dilithium private key: %w", err)
	}

	// Circl private key implements crypto.Signer, so we can return sk directly.
	return sk, nil
}

const defaultDilithiumLevel = DilithiumLevel3 // or set to 2 or 5 if you prefer

// SignDataWithDilithium signs 'data' using the given 'privKey' at a default Dilithium level.
func SignDataWithDilithium(privKey, data []byte) ([]byte, error) {
	return SignDilithium(defaultDilithiumLevel, privKey, data)
}

// VerifySignature verifies 'data' and 'sig' against 'pubKey' at a default Dilithium level.
func VerifySignature(pubKey, data, sig []byte) (bool, error) {
	return VerifyDilithium(defaultDilithiumLevel, pubKey, data, sig)
}

// ValidateDilithiumKeyPair verifies that a key pair can sign and verify correctly.
func ValidateDilithiumKeyPair(kp *DilithiumKeyPair) error {
	if err := kp.Validate(); err != nil {
		return err
	}

	// Use level from key pair or default
	level := kp.Level
	if level == 0 {
		level = defaultDilithiumLevel
	}

	// Generate test message
	testMsg := make([]byte, DilithiumKeyGenTestSize)
	binary.LittleEndian.PutUint64(testMsg, uint64(GetTimeProvider().Unix()))

	// Sign test message
	sig, err := SignDilithium(level, kp.PrivateKey, testMsg)
	if err != nil {
		return fmt.Errorf("validation signing failed: %w", err)
	}

	// Verify signature
	valid, err := VerifyDilithium(level, kp.PublicKey, testMsg, sig)
	if err != nil {
		return fmt.Errorf("validation verification failed: %w", err)
	}

	if !valid {
		return errors.New("key pair validation failed: signature verification failed")
	}

	return nil
}

// GetSecurityLevel returns the NIST security level equivalent for each Dilithium level.
func GetSecurityLevel(dilithiumLevel int) int {
	switch dilithiumLevel {
	case DilithiumLevel2:
		return 2 // NIST Level 2
	case DilithiumLevel3:
		return 3 // NIST Level 3
	case DilithiumLevel5:
		return 5 // NIST Level 5
	default:
		return 0 // Unknown
	}
}

// This is a more secure alternative to the regular VerifySignature function.
func VerifySignatureConstantTime(pubKey, data, sig []byte) (bool, error) {
	valid, err := VerifyDilithium(defaultDilithiumLevel, pubKey, data, sig)

	// Use subtle.ConstantTimeSelect to avoid timing side channels in the boolean result
	result := subtle.ConstantTimeSelect(
		subtle.ConstantTimeByteEq(byte(boolToInt(valid)), byte(1)),
		1, 0,
	)

	return result == 1, err
}

// boolToInt converts a boolean to 1 (true) or 0 (false).
func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
