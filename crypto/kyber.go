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

	"github.com/cloudflare/circl/kem"
	"github.com/cloudflare/circl/kem/kyber/kyber1024"
	"github.com/cloudflare/circl/kem/kyber/kyber512"
	"github.com/cloudflare/circl/kem/kyber/kyber768"
	"github.com/sirupsen/logrus"
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

// KyberCrypto is a small wrapper around the chosen Circl-Kyber scheme.
type KyberCrypto struct {
	scheme kem.Scheme
	logger *logrus.Logger
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
	// Assuming KyberKeyPair is defined as:
	return &KyberKeyPair{
		PublicKey:  serialized.PublicKey,
		PrivateKey: serialized.PrivateKey,
	}, nil
}

// NewKyberCrypto constructs a KyberCrypto from the given scheme.
func NewKyberCrypto(scheme kem.Scheme, logger *logrus.Logger) *KyberCrypto {
	return &KyberCrypto{scheme: scheme, logger: logger}
}

// GenerateKeyPair uses the Circl scheme to produce a new key pair (serialized).
func (kc *KyberCrypto) GenerateKeyPair() (*KyberKeyPair, error) {
	if kc.scheme == nil {
		return nil, errors.New("kyber scheme is nil")
	}
	pub, priv, err := kc.scheme.GenerateKeyPair()
	if err != nil {
		kc.logger.WithError(err).Error("Kyber GenerateKeyPair failed")
		return nil, fmt.Errorf("kyber generate keypair: %w", err)
	}
	pubBytes, err := pub.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("kyber pub marshal: %w", err)
	}
	privBytes, err := priv.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("kyber priv marshal: %w", err)
	}
	return &KyberKeyPair{PublicKey: pubBytes, PrivateKey: privBytes}, nil
}

// EncapsulateFromBytes loads a Circl public key from pubKeyBytes, then does Encapsulate => (ct, ss).
func (kc *KyberCrypto) EncapsulateFromBytes(pubKeyBytes []byte) ([]byte, []byte, error) {
	if kc.scheme == nil {
		return nil, nil, errors.New("kyber scheme is nil")
	}
	pubKey, err := kc.scheme.UnmarshalBinaryPublicKey(pubKeyBytes) // Use UnmarshalBinaryPublicKey
	if err != nil {
		return nil, nil, fmt.Errorf("failed to unmarshal Kyber public key: %w", err)
	}
	ct, ss, err := kc.scheme.Encapsulate(pubKey)
	if err != nil {
		return nil, nil, fmt.Errorf("kyber encapsulation failed: %w", err)
	}
	return ct, ss, nil
}

func (kc *KyberCrypto) DecapsulateFromBytes(privKeyBytes, ciphertext []byte) ([]byte, error) {
	if kc.scheme == nil {
		return nil, errors.New("kyber scheme is nil")
	}
	privKey, err := kc.scheme.UnmarshalBinaryPrivateKey(privKeyBytes) // Use UnmarshalBinaryPrivateKey
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal Kyber private key: %w", err)
	}
	ss, err := kc.scheme.Decapsulate(privKey, ciphertext)
	if err != nil {
		return nil, fmt.Errorf("kyber decapsulation failed: %w", err)
	}
	return ss, nil
}

// deriveSessionKey uses SHA-256 on the sharedSecret => 256-bit key.
func deriveSessionKey(sharedSecret []byte) []byte {
	h := sha256.Sum256(sharedSecret)
	return h[:]
}

// EncryptWithShared uses AES-GCM with the derived key => ciphertext
func EncryptWithShared(plaintext, sharedSecret []byte) ([]byte, error) {
	key := deriveSessionKey(sharedSecret)
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("aes newCipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("aes newGCM: %w", err)
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("read nonce: %w", err)
	}
	ciphertext := gcm.Seal(nonce, nonce, plaintext, nil)
	return ciphertext, nil
}

// DecryptWithShared uses AES-GCM with derived key => plaintext
func DecryptWithShared(ciphertext, sharedSecret []byte) ([]byte, error) {
	key := deriveSessionKey(sharedSecret)
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("aes newCipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("aes newGCM: %w", err)
	}
	nsz := gcm.NonceSize()
	if len(ciphertext) < nsz {
		return nil, errors.New("ciphertext too short for nonce")
	}
	nonce, enc := ciphertext[:nsz], ciphertext[nsz:]
	plaintext, err := gcm.Open(nil, nonce, enc, nil)
	if err != nil {
		return nil, fmt.Errorf("gcm open: %w", err)
	}
	return plaintext, nil
}
