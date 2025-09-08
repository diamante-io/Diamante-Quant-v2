// consensus/common/signature_verification.go

package common

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sync"
	"time"
)

// SignatureAlgorithm represents the type of signature algorithm
type SignatureAlgorithm int

const (
	// SigAlgoDilithium represents Dilithium quantum-resistant signature
	SigAlgoDilithium SignatureAlgorithm = iota
	// SigAlgoKyber represents Kyber quantum-resistant encryption
	SigAlgoKyber
	// SigAlgoEd25519 represents Ed25519 signature (for compatibility)
	SigAlgoEd25519
)

// Signature represents a cryptographic signature with metadata
type Signature struct {
	Algorithm     SignatureAlgorithm
	PublicKey     []byte
	SignatureData []byte
	Timestamp     Time // Using consensus time
	Nonce         uint64
}

// SignedMessage represents a message with its signature
type SignedMessage struct {
	Message   []byte
	Signature Signature
	MessageID [32]byte
}

// SignatureVerifier provides signature verification with replay protection
type SignatureVerifier struct {
	mu sync.RWMutex

	// Replay protection
	seenNonces  map[[32]byte]map[uint64]Time // validator -> nonce -> timestamp
	nonceExpiry time.Duration

	// Signature verification interfaces
	dilithiumVerifier DilithiumVerifier
	kyberVerifier     KyberVerifier
	ed25519Verifier   Ed25519Verifier

	// Consensus time source
	consensusTime ConsensusTime

	// Metrics
	verificationCount   uint64
	replayAttempts      uint64
	failedVerifications uint64
}

// DilithiumVerifier interface for Dilithium signature verification
type DilithiumVerifier interface {
	Verify(publicKey, message, signature []byte) bool
	PublicKeySize() int
	SignatureSize() int
}

// KyberVerifier interface for Kyber encryption/decryption
type KyberVerifier interface {
	Verify(publicKey, message, signature []byte) bool
	PublicKeySize() int
	SignatureSize() int
}

// Ed25519Verifier interface for Ed25519 signature verification
type Ed25519Verifier interface {
	Verify(publicKey, message, signature []byte) bool
	PublicKeySize() int
	SignatureSize() int
}

// NewSignatureVerifier creates a new signature verifier
func NewSignatureVerifier(
	consensusTime ConsensusTime,
	dilithiumVerifier DilithiumVerifier,
	kyberVerifier KyberVerifier,
	ed25519Verifier Ed25519Verifier,
) *SignatureVerifier {
	return &SignatureVerifier{
		seenNonces:        make(map[[32]byte]map[uint64]Time),
		nonceExpiry:       24 * time.Hour, // Nonces expire after 24 hours
		dilithiumVerifier: dilithiumVerifier,
		kyberVerifier:     kyberVerifier,
		ed25519Verifier:   ed25519Verifier,
		consensusTime:     consensusTime,
	}
}

// VerifySignature verifies a signature with replay protection
func (sv *SignatureVerifier) VerifySignature(message []byte, sig Signature) error {
	sv.mu.Lock()
	sv.verificationCount++
	sv.mu.Unlock()

	// Validate signature fields
	if err := sv.validateSignature(sig); err != nil {
		sv.mu.Lock()
		sv.failedVerifications++
		sv.mu.Unlock()
		return fmt.Errorf("invalid signature: %w", err)
	}

	// Check replay protection
	if err := sv.checkReplayProtection(sig); err != nil {
		sv.mu.Lock()
		sv.replayAttempts++
		sv.failedVerifications++
		sv.mu.Unlock()
		return fmt.Errorf("replay protection failed: %w", err)
	}

	// Verify the actual signature
	verified := false
	switch sig.Algorithm {
	case SigAlgoDilithium:
		if sv.dilithiumVerifier == nil {
			return fmt.Errorf("Dilithium verifier not configured")
		}
		verified = sv.dilithiumVerifier.Verify(sig.PublicKey, message, sig.SignatureData)

	case SigAlgoKyber:
		if sv.kyberVerifier == nil {
			return fmt.Errorf("Kyber verifier not configured")
		}
		verified = sv.kyberVerifier.Verify(sig.PublicKey, message, sig.SignatureData)

	case SigAlgoEd25519:
		if sv.ed25519Verifier == nil {
			return fmt.Errorf("Ed25519 verifier not configured")
		}
		verified = sv.ed25519Verifier.Verify(sig.PublicKey, message, sig.SignatureData)

	default:
		return fmt.Errorf("unsupported signature algorithm: %d", sig.Algorithm)
	}

	if !verified {
		sv.mu.Lock()
		sv.failedVerifications++
		sv.mu.Unlock()
		return fmt.Errorf("signature verification failed")
	}

	// Record the nonce to prevent replay
	sv.recordNonce(sig)

	return nil
}

// VerifySignedMessage verifies a signed message
func (sv *SignatureVerifier) VerifySignedMessage(msg SignedMessage) error {
	// Verify message ID matches
	expectedID := sv.calculateMessageID(msg.Message, msg.Signature)
	if expectedID != msg.MessageID {
		return fmt.Errorf("message ID mismatch")
	}

	// Verify signature
	return sv.VerifySignature(msg.Message, msg.Signature)
}

// validateSignature validates signature fields
func (sv *SignatureVerifier) validateSignature(sig Signature) error {
	// Check public key
	if len(sig.PublicKey) == 0 {
		return fmt.Errorf("empty public key")
	}

	// Check signature data
	if len(sig.SignatureData) == 0 {
		return fmt.Errorf("empty signature data")
	}

	// Validate key and signature sizes based on algorithm
	switch sig.Algorithm {
	case SigAlgoDilithium:
		if sv.dilithiumVerifier != nil {
			if len(sig.PublicKey) != sv.dilithiumVerifier.PublicKeySize() {
				return fmt.Errorf("invalid Dilithium public key size: got %d, want %d",
					len(sig.PublicKey), sv.dilithiumVerifier.PublicKeySize())
			}
			if len(sig.SignatureData) != sv.dilithiumVerifier.SignatureSize() {
				return fmt.Errorf("invalid Dilithium signature size: got %d, want %d",
					len(sig.SignatureData), sv.dilithiumVerifier.SignatureSize())
			}
		}

	case SigAlgoKyber:
		if sv.kyberVerifier != nil {
			if len(sig.PublicKey) != sv.kyberVerifier.PublicKeySize() {
				return fmt.Errorf("invalid Kyber public key size: got %d, want %d",
					len(sig.PublicKey), sv.kyberVerifier.PublicKeySize())
			}
			if len(sig.SignatureData) != sv.kyberVerifier.SignatureSize() {
				return fmt.Errorf("invalid Kyber signature size: got %d, want %d",
					len(sig.SignatureData), sv.kyberVerifier.SignatureSize())
			}
		}

	case SigAlgoEd25519:
		if sv.ed25519Verifier != nil {
			if len(sig.PublicKey) != sv.ed25519Verifier.PublicKeySize() {
				return fmt.Errorf("invalid Ed25519 public key size: got %d, want %d",
					len(sig.PublicKey), sv.ed25519Verifier.PublicKeySize())
			}
			if len(sig.SignatureData) != sv.ed25519Verifier.SignatureSize() {
				return fmt.Errorf("invalid Ed25519 signature size: got %d, want %d",
					len(sig.SignatureData), sv.ed25519Verifier.SignatureSize())
			}
		}
	}

	// Validate timestamp
	currentTime := sv.consensusTime.Now()
	if sig.Timestamp.After(currentTime.Add(5 * time.Minute)) {
		return fmt.Errorf("signature timestamp too far in future")
	}

	if sig.Timestamp.Before(currentTime.Add(-sv.nonceExpiry)) {
		return fmt.Errorf("signature timestamp too old")
	}

	return nil
}

// checkReplayProtection checks if this signature has been seen before
func (sv *SignatureVerifier) checkReplayProtection(sig Signature) error {
	sv.mu.Lock()
	defer sv.mu.Unlock()

	// Get validator ID from public key
	validatorID := sha256.Sum256(sig.PublicKey)

	// Check if we've seen this nonce from this validator
	if nonces, exists := sv.seenNonces[validatorID]; exists {
		if seenTime, seen := nonces[sig.Nonce]; seen {
			return fmt.Errorf("nonce %d already used at %v", sig.Nonce, seenTime)
		}
	} else {
		sv.seenNonces[validatorID] = make(map[uint64]Time)
	}

	// Clean expired nonces
	sv.cleanExpiredNonces()

	return nil
}

// recordNonce records a nonce to prevent replay
func (sv *SignatureVerifier) recordNonce(sig Signature) {
	sv.mu.Lock()
	defer sv.mu.Unlock()

	validatorID := sha256.Sum256(sig.PublicKey)
	sv.seenNonces[validatorID][sig.Nonce] = sig.Timestamp
}

// cleanExpiredNonces removes old nonces to prevent unbounded growth
func (sv *SignatureVerifier) cleanExpiredNonces() {
	currentTime := sv.consensusTime.Now()
	expiryTime := currentTime.Add(-sv.nonceExpiry)

	for validatorID, nonces := range sv.seenNonces {
		for nonce, timestamp := range nonces {
			if timestamp.Before(expiryTime) {
				delete(nonces, nonce)
			}
		}

		// Remove validator entry if no nonces left
		if len(nonces) == 0 {
			delete(sv.seenNonces, validatorID)
		}
	}
}

// calculateMessageID calculates a deterministic message ID
func (sv *SignatureVerifier) calculateMessageID(message []byte, sig Signature) [32]byte {
	h := sha256.New()

	// Add message
	h.Write(message)

	// Add signature algorithm
	algBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(algBytes, uint32(sig.Algorithm))
	h.Write(algBytes)

	// Add public key
	h.Write(sig.PublicKey)

	// Add timestamp
	h.Write(TimeToBytes(sig.Timestamp))

	// Add nonce
	nonceBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(nonceBytes, sig.Nonce)
	h.Write(nonceBytes)

	var id [32]byte
	copy(id[:], h.Sum(nil))
	return id
}

// GetMetrics returns verification metrics
func (sv *SignatureVerifier) GetMetrics() map[string]uint64 {
	sv.mu.RLock()
	defer sv.mu.RUnlock()

	return map[string]uint64{
		"verification_count":   sv.verificationCount,
		"replay_attempts":      sv.replayAttempts,
		"failed_verifications": sv.failedVerifications,
		"active_validators":    uint64(len(sv.seenNonces)),
	}
}

// BatchVerifier provides batch signature verification
type BatchVerifier struct {
	verifier *SignatureVerifier
	workers  int
}

// NewBatchVerifier creates a new batch verifier
func NewBatchVerifier(verifier *SignatureVerifier, workers int) *BatchVerifier {
	if workers <= 0 {
		workers = 1
	}
	return &BatchVerifier{
		verifier: verifier,
		workers:  workers,
	}
}

// VerifyBatch verifies multiple signatures in parallel
func (bv *BatchVerifier) VerifyBatch(messages []SignedMessage) ([]error, error) {
	if len(messages) == 0 {
		return nil, nil
	}

	// Create channels for work distribution
	workChan := make(chan int, len(messages))
	resultChan := make(chan struct {
		index int
		err   error
	}, len(messages))

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < bv.workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for index := range workChan {
				err := bv.verifier.VerifySignedMessage(messages[index])
				resultChan <- struct {
					index int
					err   error
				}{index, err}
			}
		}()
	}

	// Send work
	for i := range messages {
		workChan <- i
	}
	close(workChan)

	// Wait for completion
	wg.Wait()
	close(resultChan)

	// Collect results
	results := make([]error, len(messages))
	for result := range resultChan {
		results[result.index] = result.err
	}

	return results, nil
}

// SignatureCache provides caching for verified signatures
type SignatureCache struct {
	mu       sync.RWMutex
	cache    map[[32]byte]Time // message ID -> verification time
	maxSize  int
	ttl      time.Duration
	verifier *SignatureVerifier
}

// NewSignatureCache creates a new signature cache
func NewSignatureCache(verifier *SignatureVerifier, maxSize int, ttl time.Duration) *SignatureCache {
	return &SignatureCache{
		cache:    make(map[[32]byte]Time),
		maxSize:  maxSize,
		ttl:      ttl,
		verifier: verifier,
	}
}

// VerifyWithCache verifies a signature using cache
func (sc *SignatureCache) VerifyWithCache(msg SignedMessage) error {
	sc.mu.RLock()
	if verifyTime, exists := sc.cache[msg.MessageID]; exists {
		currentTime := sc.verifier.consensusTime.Now()
		if currentTime.Sub(verifyTime) < sc.ttl {
			sc.mu.RUnlock()
			return nil // Already verified recently
		}
	}
	sc.mu.RUnlock()

	// Verify the signature
	if err := sc.verifier.VerifySignedMessage(msg); err != nil {
		return err
	}

	// Cache the result
	sc.mu.Lock()
	defer sc.mu.Unlock()

	// Evict old entries if cache is full
	if len(sc.cache) >= sc.maxSize {
		sc.evictOldest()
	}

	sc.cache[msg.MessageID] = sc.verifier.consensusTime.Now()
	return nil
}

// evictOldest removes the oldest cache entry
func (sc *SignatureCache) evictOldest() {
	var oldestID [32]byte
	var oldestTime Time
	first := true

	for id, t := range sc.cache {
		if first || t.Before(oldestTime) {
			oldestID = id
			oldestTime = t
			first = false
		}
	}

	delete(sc.cache, oldestID)
}
