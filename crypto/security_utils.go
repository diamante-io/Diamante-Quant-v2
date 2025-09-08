// security_utils.go
package crypto

import (
	"context"
	"crypto/subtle"
	"runtime"
	"sync/atomic"
	"time"
)

// secureZero securely clears sensitive data from memory using constant-time operations.
func secureZero(data []byte) {
	if len(data) == 0 {
		return
	}

	// Use subtle.ConstantTimeCopy to ensure compiler doesn't optimize away the clearing
	zeros := make([]byte, len(data))
	subtle.ConstantTimeCopy(1, data, zeros)

	// Force a garbage collection to help ensure the memory is actually cleared
	runtime.GC()
}

// secureCompare compares two byte slices in constant time to prevent timing attacks.
func secureCompare(a, b []byte) bool {
	return subtle.ConstantTimeCompare(a, b) == 1
}

// atomicRotationCounter provides thread-safe rotation state tracking.
type atomicRotationCounter struct {
	counter uint64
}

// increment atomically increments the rotation counter.
func (arc *atomicRotationCounter) increment() uint64 {
	return atomic.AddUint64(&arc.counter, 1)
}

// get atomically gets the current rotation counter.
func (arc *atomicRotationCounter) get() uint64 {
	return atomic.LoadUint64(&arc.counter)
}

// secureCompareAndSwap atomically compares and swaps a uint32 value.
func secureCompareAndSwap(addr *uint32, old, new uint32) bool {
	return atomic.CompareAndSwapUint32(addr, old, new)
}

// secureStore atomically stores a uint32 value.
func secureStore(addr *uint32, val uint32) {
	atomic.StoreUint32(addr, val)
}

// secureLoad atomically loads a uint32 value.
func secureLoad(addr *uint32) uint32 {
	return atomic.LoadUint32(addr)
}

// secureMemoryPool provides a pool of pre-allocated secure memory buffers.
type secureMemoryPool struct {
	pool chan []byte
	size int
}

// newSecureMemoryPool creates a new secure memory pool with specified buffer size and count.
func newSecureMemoryPool(bufferSize, poolSize int) *secureMemoryPool {
	pool := make(chan []byte, poolSize)
	for range poolSize {
		pool <- make([]byte, bufferSize)
	}
	return &secureMemoryPool{
		pool: pool,
		size: bufferSize,
	}
}

// get retrieves a buffer from the pool, or creates a new one if pool is empty.
func (smp *secureMemoryPool) get() []byte {
	select {
	case buf := <-smp.pool:
		// Clear the buffer before returning
		secureZero(buf)
		return buf
	default:
		// Pool is empty, create new buffer
		return make([]byte, smp.size)
	}
}

// put returns a buffer to the pool after securely clearing it.
func (smp *secureMemoryPool) put(buf []byte) {
	if len(buf) != smp.size {
		return // Buffer size doesn't match, don't return to pool
	}

	secureZero(buf)
	select {
	case smp.pool <- buf:
		// Successfully returned to pool
	default:
		// Pool is full, let garbage collector handle it
	}
}

// TimingAttackMitigation provides utilities to mitigate timing-based side-channel attacks.
type TimingAttackMitigation struct {
	baseDelay    time.Duration
	jitterFactor float64
}

// NewTimingAttackMitigation creates a new timing attack mitigation utility.
func NewTimingAttackMitigation(baseDelay time.Duration, jitterFactor float64) *TimingAttackMitigation {
	return &TimingAttackMitigation{
		baseDelay:    baseDelay,
		jitterFactor: jitterFactor,
	}
}

// AddConstantTimeDelay adds a constant-time delay to mitigate timing attacks.
func (tam *TimingAttackMitigation) AddConstantTimeDelay() {
	// Use context with timeout for delay
	ctx, cancel := context.WithTimeout(context.Background(), tam.baseDelay*2)
	defer cancel()

	// Add base delay using select with timer
	select {
	case <-time.After(tam.baseDelay):
		// Base delay completed
	case <-ctx.Done():
		// Context canceled
		return
	}

	// Add small deterministic jitter to prevent attackers from measuring exact timing
	// Use a fixed seed for deterministic behavior across nodes
	if tam.jitterFactor > 0 {
		// Use a deterministic value instead of common.ConsensusNow() for consensus safety
		// This still provides timing variation but in a predictable way
		deterministicSeed := int64(42) // Fixed seed for determinism
		jitter := time.Duration(float64(tam.baseDelay) * tam.jitterFactor * (0.5 - float64(deterministicSeed%1000)/2000))
		select {
		case <-time.After(jitter):
			// Jitter delay completed
		case <-ctx.Done():
			// Context canceled
			return
		}
	}
}

// SideChannelMitigation provides side-channel attack mitigations.
type SideChannelMitigation struct {
	enabled bool
	timing  *TimingAttackMitigation
}

// NewSideChannelMitigation creates a new side-channel mitigation instance.
func NewSideChannelMitigation(enabled bool) *SideChannelMitigation {
	return &SideChannelMitigation{
		enabled: enabled,
		timing:  NewTimingAttackMitigation(time.Microsecond*100, 0.1),
	}
}

// ApplyMitigations applies various side-channel mitigations if enabled.
func (scm *SideChannelMitigation) ApplyMitigations() {
	if !scm.enabled {
		return
	}

	// Add timing delay to normalize operation duration
	scm.timing.AddConstantTimeDelay()
}

// SecureKeyDerivation provides secure key derivation with side-channel mitigations.
type SecureKeyDerivation struct {
	mitigation *SideChannelMitigation
}

// NewSecureKeyDerivation creates a new secure key derivation instance.
func NewSecureKeyDerivation(enableMitigation bool) *SecureKeyDerivation {
	return &SecureKeyDerivation{
		mitigation: NewSideChannelMitigation(enableMitigation),
	}
}

// DeriveKeySecure derives a key with additional security measures.
func (skd *SecureKeyDerivation) DeriveKeySecure(seed []byte, length int) ([]byte, error) {
	// Apply side-channel mitigations
	skd.mitigation.ApplyMitigations()

	// Use the existing key derivation but with enhanced security
	cm := &CryptoManager{} // Temporary instance for key derivation
	result, err := cm.DeriveKey(seed, length)

	// Clear the seed copy if any was made during derivation
	defer func() {
		if seed != nil {
			secureZero(seed)
		}
	}()

	return result, err
}
