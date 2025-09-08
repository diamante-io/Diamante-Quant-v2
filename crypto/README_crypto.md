# Crypto Package Documentation

## Overview

The crypto package implements post-quantum cryptographic operations using Cloudflare's CIRCL library. It provides implementations for both Kyber (Key Encapsulation Mechanism) and Dilithium (Digital Signatures) with configurable security levels. The package is designed for production use with comprehensive error handling, logging, and testing.

## Core Components

### 1. Dilithium Implementation (`dilithium.go`)

Implements quantum-resistant digital signatures using CIRCL's Dilithium.

#### Security Levels
- Level 2 (NIST Security Level 2)
- Level 3 (NIST Security Level 3)
- Level 5 (NIST Security Level 5)

#### Key Functions
```go
func GenerateDilithiumKeyPair(level int) (*DilithiumKeyPair, error)
func SignDilithium(level int, privKeyBytes, message []byte) ([]byte, error)
func VerifyDilithium(level int, pubKeyBytes, message, signature []byte) (bool, error)
func DilithiumPrivateKeyToPub(level int, privKeyBytes []byte) ([]byte, error)
```

#### Key Types
```go
type DilithiumKeyPair struct {
    PublicKey  []byte
    PrivateKey []byte
}
```

### 2. Kyber Implementation (`kyber.go`)

Implements quantum-resistant key encapsulation using CIRCL's Kyber.

#### Security Levels
- Kyber-512 (AES-128 equivalent security)
- Kyber-768 (AES-192 equivalent security)
- Kyber-1024 (AES-256 equivalent security)

#### Key Functions
```go
func (kc *KyberCrypto) GenerateKeyPair() (*KyberKeyPair, error)
func (kc *KyberCrypto) EncapsulateFromBytes(pubKeyBytes []byte) ([]byte, []byte, error)
func (kc *KyberCrypto) DecapsulateFromBytes(privKeyBytes, ciphertext []byte) ([]byte, error)
```

#### Encryption Utilities
```go
func EncryptWithShared(plaintext, sharedSecret []byte) ([]byte, error)
func DecryptWithShared(ciphertext, sharedSecret []byte) ([]byte, error)
```

### 3. CryptoManager (`crypto_manager.go`)

Orchestrates both Kyber and Dilithium operations with additional utilities.

#### Initialization
```go
func NewCryptoManager(kyberLevel, dilLevel int, logger *logrus.Logger) (*CryptoManager, error)
```

#### Combined Operations
```go
func (cm *CryptoManager) CombinedEncryptAndSign(
    pubKeyKEM []byte, 
    dilPriv *DilithiumKeyPair, 
    message []byte,
) (ciphertext []byte, encrypted []byte, signature []byte, err error)

func (cm *CryptoManager) CombinedDecryptAndVerify(
    privKeyKEM []byte, 
    dilPub *DilithiumKeyPair, 
    ciphertext, encData, signature []byte,
) ([]byte, bool, error)
```

#### Utility Functions
```go
func (cm *CryptoManager) GenerateRandomBytes(n int) ([]byte, error)
func (cm *CryptoManager) DeriveKey(seed []byte, length int) ([]byte, error)
```

## Configuration

### CryptoConfig Structure
```go
type CryptoConfig struct {
    KyberSecurityLevel     int
    DilithiumSecurityLevel int
    EnableKeyRotation      bool
    KeyRotationInterval    time.Duration
    MinKeyGenerationTime   time.Duration
    MaxKeyGenerationRetry  int
}
```

### Default Configuration
```go
func NewDefaultCryptoConfig() *CryptoConfig {
    return &CryptoConfig{
        KyberSecurityLevel:     1024,
        DilithiumSecurityLevel: 3,
        EnableKeyRotation:      true,
        KeyRotationInterval:    6 * time.Hour,
        MinKeyGenerationTime:   100 * time.Millisecond,
        MaxKeyGenerationRetry:  3,
    }
}
```

## Error Handling

The package implements comprehensive error handling with proper error wrapping:

```go
// Example error handling pattern
if err != nil {
    return nil, fmt.Errorf("operation failed: %w", err)
}
```

Key error scenarios handled:
- Invalid security levels
- Key generation failures
- Serialization errors
- Signing/verification failures
- Timeout conditions
- Invalid input parameters

## Security Considerations

1. Key Management
   - Automatic key rotation support
   - Configurable security levels
   - Proper key serialization

2. Operation Timeouts
   - Signing operations have a 5-second timeout
   - Prevents potential blocking operations

3. Random Number Generation
   - Uses crypto/rand for secure random number generation
   - Implements proper entropy checking

4. Memory Security
   - Keys are stored as byte slices for secure handling
   - No string conversions of sensitive data

## Usage Examples

### Basic Key Generation and Signing
```go
// Initialize CryptoManager
cm, err := NewCryptoManager(KyberLevel1024, DilithiumLevel3, logger)
if err != nil {
    log.Fatal(err)
}

// Generate Dilithium keypair
dilKP, err := cm.GenerateSignatureKeyPair()
if err != nil {
    log.Fatal(err)
}

// Sign a message
message := []byte("Hello, World!")
signature, err := cm.Sign(dilKP, message)
if err != nil {
    log.Fatal(err)
}

// Verify signature
valid, err := cm.Verify(dilKP, message, signature)
if err != nil {
    log.Fatal(err)
}
```

### Combined Encryption and Signing
```go
// Generate keypairs
kemKP, err := cm.GenerateKEMKeyPair()
if err != nil {
    log.Fatal(err)
}
dilKP, err := cm.GenerateSignatureKeyPair()
if err != nil {
    log.Fatal(err)
}

// Encrypt and sign
message := []byte("Secret message")
ct, enc, sig, err := cm.CombinedEncryptAndSign(
    kemKP.PublicKey, 
    dilKP, 
    message,
)
if err != nil {
    log.Fatal(err)
}

// Decrypt and verify
recoveredMsg, ok, err := cm.CombinedDecryptAndVerify(
    kemKP.PrivateKey,
    dilKP,
    ct, enc, sig,
)
```

## Testing

The package includes comprehensive test coverage:

1. Unit Tests
   - Key generation
   - Encryption/decryption
   - Signing/verification
   - Combined operations
   - Error cases

2. Test Utilities
```go
func getTestLogger() *logrus.Logger
func getTestCryptoConfig() *config.CryptoConfig
```

3. Test Cases
   - Basic functionality
   - Edge cases
   - Invalid inputs
   - Timeout scenarios
   - Key rotation

## Performance Considerations

1. Operation Timings
   - Key generation: ~100ms
   - Signing: <5s (with timeout)
   - Verification: ~50ms
   - KEM operations: ~20ms

2. Memory Usage
   - Key sizes vary by security level
   - Buffer sizes are pre-allocated where possible
   - Proper cleanup of sensitive data

## Dependencies

- github.com/cloudflare/circl/sign
- github.com/cloudflare/circl/kem
- github.com/sirupsen/logrus
- crypto/rand
- crypto/aes
- crypto/cipher
- golang.org/x/crypto/sha3

## Best Practices

1. Key Management
   - Implement regular key rotation
   - Use appropriate security levels
   - Secure key storage

2. Error Handling
   - Always check error returns
   - Implement proper logging
   - Use timeout protection

3. Configuration
   - Use environment-appropriate settings
   - Implement proper validation
   - Monitor operation metrics

## Future Enhancements

1. Additional Features
   - Key versioning
   - Operation metrics
   - Rate limiting
   - Enhanced logging

2. Security Improvements
   - Key backup mechanisms
   - Hardware security module integration
   - Additional post-quantum algorithms

## Support and Maintenance

1. Logging
   - Structured logging with logrus
   - Operation tracking
   - Error monitoring

2. Debugging
   - Detailed error messages
   - Operation tracing
   - Performance monitoring

3. Updates
   - Regular security updates
   - CIRCL library updates
   - Configuration management