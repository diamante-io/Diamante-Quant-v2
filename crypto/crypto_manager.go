// crypto_manager.go
package crypto

import (
	"crypto/rand"
	"errors"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
	"golang.org/x/crypto/sha3"
)

type KeyManager = CryptoManager

// CryptoManager orchestrates:
//   - Kyber (KEM) for encryption/key encaps
//   - Dilithium for signatures
type CryptoManager struct {
	logger *logrus.Logger

	// KEM
	kyberLevel  int
	kyberCrypto *KyberCrypto

	// Sig
	dilithiumLevel int
}

// NewCryptoManager builds the manager with chosen Kyber & Dilithium levels.
func NewCryptoManager(kyberLevel, dilLevel int, logger *logrus.Logger) (*CryptoManager, error) {
	logger.WithFields(logrus.Fields{
		"KyberLevel":     kyberLevel,
		"DilithiumLevel": dilLevel,
	}).Info("Initializing CryptoManager")

	kyberScheme := KyberSchemeFromLevel(kyberLevel)
	if kyberScheme == nil {
		return nil, fmt.Errorf("invalid kyber security level %d", kyberLevel)
	}
	kyb := NewKyberCrypto(kyberScheme, logger)

	return &CryptoManager{
		logger:         logger,
		kyberLevel:     kyberLevel,
		kyberCrypto:    kyb,
		dilithiumLevel: dilLevel,
	}, nil
}

// GenerateKEMKeyPair => produce new Kyber key pair
func (cm *CryptoManager) GenerateKEMKeyPair() (*KyberKeyPair, error) {
	return cm.kyberCrypto.GenerateKeyPair()
}

// EncryptKEM => EncapsulateFromBytes
func (cm *CryptoManager) EncryptKEM(pubKeyBytes []byte) ([]byte, []byte, error) {
	return cm.kyberCrypto.EncapsulateFromBytes(pubKeyBytes)
}

// DecryptKEM => DecapsulateFromBytes
func (cm *CryptoManager) DecryptKEM(privKeyBytes, ciphertext []byte) ([]byte, error) {
	return cm.kyberCrypto.DecapsulateFromBytes(privKeyBytes, ciphertext)
}

// GenerateSignatureKeyPair => produce new Dilithium key pair
func (cm *CryptoManager) GenerateSignatureKeyPair() (*DilithiumKeyPair, error) {
	return GenerateDilithiumKeyPair(cm.dilithiumLevel)
}

// Sign => sign with Dilithium
func (cm *CryptoManager) Sign(priv *DilithiumKeyPair, message []byte) ([]byte, error) {
	type result struct {
		sig []byte
		err error
	}
	done := make(chan result, 1)

	go func() {
		sig, err := SignDilithium(cm.dilithiumLevel, priv.PrivateKey, message)
		done <- result{sig, err}
	}()

	select {
	case r := <-done:
		if r.err != nil {
			cm.logger.WithError(r.err).Error("Dilithium signing failed")
			return nil, r.err
		}
		cm.logger.Info("Dilithium signing completed successfully")
		return r.sig, nil
	case <-time.After(5 * time.Second):
		cm.logger.Error("Dilithium signing timed out")
		return nil, errors.New("signing timeout")
	}
}

// Verify => Dilithium verify
func (cm *CryptoManager) Verify(pub *DilithiumKeyPair, msg, sig []byte) (bool, error) {
	ok, err := VerifyDilithium(cm.dilithiumLevel, pub.PublicKey, msg, sig)
	if err != nil {
		cm.logger.WithError(err).Error("dilithium verify error")
		return false, err
	}
	return ok, nil
}

// CombinedEncryptAndSign => KEM encaps => AES-GCM => Dilithium sign.
func (cm *CryptoManager) CombinedEncryptAndSign(pubKeyKEM []byte, dilPriv *DilithiumKeyPair, message []byte) (ciphertext []byte, encrypted []byte, signature []byte, err error) {
	// 1) Encaps => ephemeral sharedSecret
	ct, ss, err := cm.EncryptKEM(pubKeyKEM)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("encaps error: %w", err)
	}
	// 2) AES-GCM
	enc, err := EncryptWithShared(message, ss)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("encryptWithShared: %w", err)
	}
	// 3) Sign
	sig, err := cm.Sign(dilPriv, message)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("dilithium sign error: %w", err)
	}
	return ct, enc, sig, nil
}

// CombinedDecryptAndVerify => get ephemeral sharedSecret => decrypt => verify signature.
func (cm *CryptoManager) CombinedDecryptAndVerify(privKeyKEM []byte, dilPub *DilithiumKeyPair, ciphertext, encData, signature []byte) ([]byte, bool, error) {
	ss, err := cm.DecryptKEM(privKeyKEM, ciphertext)
	if err != nil {
		return nil, false, fmt.Errorf("decaps error: %w", err)
	}
	pt, err := DecryptWithShared(encData, ss)
	if err != nil {
		return nil, false, fmt.Errorf("decryptWithShared error: %w", err)
	}
	ok, err := cm.Verify(dilPub, pt, signature)
	if err != nil {
		return nil, false, err
	}
	return pt, ok, nil
}

// Extra utility: random bytes
func (cm *CryptoManager) GenerateRandomBytes(n int) ([]byte, error) {
	buf := make([]byte, n)
	_, err := rand.Read(buf)
	if err != nil {
		return nil, fmt.Errorf("random read: %w", err)
	}
	return buf, nil
}

// Extra utility: simple KDF using SHAKE256
func (cm *CryptoManager) DeriveKey(seed []byte, length int) ([]byte, error) {
	if length <= 0 {
		return nil, errors.New("invalid length")
	}
	h := sha3.NewShake256()
	_, _ = h.Write(seed)
	out := make([]byte, length)
	_, err := h.Read(out)
	if err != nil {
		return nil, fmt.Errorf("shake read: %w", err)
	}
	return out, nil
}

// SerializeKyberPublicKey => length prefix + bytes
func (cm *CryptoManager) SerializeKyberPublicKey(pub []byte) ([]byte, error) {
	sz := len(pub)
	if sz == 0 {
		return nil, errors.New("empty kyber pubkey")
	}
	out := make([]byte, sz+4)
	out[0] = byte(sz >> 24)
	out[1] = byte(sz >> 16)
	out[2] = byte(sz >> 8)
	out[3] = byte(sz)
	copy(out[4:], pub)
	return out, nil
}

// DeserializeKyberPublicKey => parse length
func (cm *CryptoManager) DeserializeKyberPublicKey(data []byte) ([]byte, error) {
	if len(data) < 4 {
		return nil, errors.New("input too short for length prefix")
	}
	sz := (int(data[0]) << 24) | (int(data[1]) << 16) | (int(data[2]) << 8) | int(data[3])
	if len(data) != sz+4 {
		return nil, errors.New("size mismatch in kyber pubkey")
	}
	return data[4:], nil
}
