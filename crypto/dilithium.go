// dilithium.go
package crypto

import (
	"crypto"
	"errors"
	"fmt"

	// Circl sign
	"github.com/cloudflare/circl/sign"
	"github.com/cloudflare/circl/sign/dilithium/mode2"
	"github.com/cloudflare/circl/sign/dilithium/mode3"
	"github.com/cloudflare/circl/sign/dilithium/mode5"
)

// We define constants for recognized Dilithium levels.
const (
	DilithiumLevel2 = 2
	DilithiumLevel3 = 3
	DilithiumLevel5 = 5
)

// DilithiumKeyPairSerialized is the serializable representation of a DilithiumKeyPair.
type DilithiumKeyPairSerialized struct {
	PublicKey  []byte `json:"publicKey"`
	PrivateKey []byte `json:"privateKey"`
}

// SerializeDilithiumKeyPair converts a DilithiumKeyPair into its serializable form.
func SerializeDilithiumKeyPair(kp *DilithiumKeyPair) (*DilithiumKeyPairSerialized, error) {
	if kp == nil {
		return nil, fmt.Errorf("nil DilithiumKeyPair")
	}
	return &DilithiumKeyPairSerialized{
		PublicKey:  kp.PublicKey,
		PrivateKey: kp.PrivateKey,
	}, nil
}

// DeserializeDilithiumKeyPair converts the serialized form back to a DilithiumKeyPair.
func DeserializeDilithiumKeyPair(serialized *DilithiumKeyPairSerialized) (*DilithiumKeyPair, error) {
	if serialized == nil {
		return nil, fmt.Errorf("nil serialized DilithiumKeyPair")
	}
	return &DilithiumKeyPair{
		PublicKey:  serialized.PublicKey,
		PrivateKey: serialized.PrivateKey,
	}, nil
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
		return nil, fmt.Errorf("unsupported dilithium level: %d", level)
	}
}

// DilithiumKeyPair holds marshaled pub/priv keys for a chosen Dilithium mode.
type DilithiumKeyPair struct {
	PublicKey  []byte
	PrivateKey []byte
}

// GenerateDilithiumKeyPair obtains scheme via getDilithiumScheme and calls GenerateKey().
func GenerateDilithiumKeyPair(level int) (*DilithiumKeyPair, error) {
	scheme, err := getDilithiumScheme(level)
	if err != nil {
		return nil, err
	}
	pub, priv, err := scheme.GenerateKey()
	if err != nil {
		return nil, fmt.Errorf("dilithium generateKey: %w", err)
	}
	pubBytes, err := pub.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("dilithium pub marshal: %w", err)
	}
	privBytes, err := priv.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("dilithium priv marshal: %w", err)
	}
	return &DilithiumKeyPair{
		PublicKey:  pubBytes,
		PrivateKey: privBytes,
	}, nil
}

// SignDilithium signs message with the privateKey bytes at the chosen level.
func SignDilithium(level int, privKeyBytes, message []byte) ([]byte, error) {
	scheme, err := getDilithiumScheme(level)
	if err != nil {
		return nil, err
	}
	sk, err := scheme.UnmarshalBinaryPrivateKey(privKeyBytes)
	if err != nil {
		return nil, fmt.Errorf("dilithium unmarshal priv: %w", err)
	}
	// sign => returns []byte
	signature := scheme.Sign(sk, message, nil)
	return signature, nil
}

// VerifyDilithium verifies signature under publicKey bytes at the chosen level.
func VerifyDilithium(level int, pubKeyBytes, message, signature []byte) (bool, error) {
	scheme, err := getDilithiumScheme(level)
	if err != nil {
		return false, err
	}
	pk, err := scheme.UnmarshalBinaryPublicKey(pubKeyBytes)
	if err != nil {
		return false, fmt.Errorf("dilithium unmarshal pub: %w", err)
	}
	ok := scheme.Verify(pk, message, signature, nil)
	return ok, nil
}

// DilithiumPrivateKeyToPub recovers the publicKey from a privateKey. Not always needed, but can help.
func DilithiumPrivateKeyToPub(level int, privKeyBytes []byte) ([]byte, error) {
	scheme, err := getDilithiumScheme(level)
	if err != nil {
		return nil, err
	}
	sk, err := scheme.UnmarshalBinaryPrivateKey(privKeyBytes)
	if err != nil {
		return nil, fmt.Errorf("unmarshal Dilithium private key: %w", err)
	}

	// sk.Public() returns a crypto.PublicKey, but we need the Circl sign.PublicKey:
	pubIfc := sk.Public()
	pubKey, ok := pubIfc.(sign.PublicKey)
	if !ok {
		return nil, errors.New("error: the returned public key is not a Circl sign.PublicKey")
	}

	// Now we can marshal:
	pubBytes, err := pubKey.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("failed to marshal Dilithium public key: %w", err)
	}
	return pubBytes, nil
}

// Provide a method to get a crypto.Signer if you want to use the standard library's abstractions.
func (kp *DilithiumKeyPair) CryptoSigner(level int) (crypto.Signer, error) {
	scheme, err := getDilithiumScheme(level)
	if err != nil {
		return nil, err
	}
	sk, err := scheme.UnmarshalBinaryPrivateKey(kp.PrivateKey)
	if err != nil {
		return nil, fmt.Errorf("unmarshal private key: %w", err)
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
