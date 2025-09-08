package crypto

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math/big"

	"diamante/common"
)

// Signature types for identification
const (
	SigTypeECDSA     byte = 0x01 // Legacy ECDSA signatures
	SigTypeDilithium byte = 0x02 // Quantum-resistant Dilithium signatures
	SigTypeHybrid    byte = 0x03 // Both ECDSA and Dilithium (transition period)
)

var (
	// ErrInvalidSignatureType indicates an unsupported signature type
	ErrInvalidSignatureType = errors.New("invalid signature type")

	// ErrMalformedHybridSignature indicates a malformed hybrid signature
	ErrMalformedHybridSignature = errors.New("malformed hybrid signature")
)

// HybridSignature represents a signature that can be either classical or quantum-resistant
type HybridSignature struct {
	Type               byte
	ECDSASignature     []byte // 64 bytes for ECDSA (r||s)
	DilithiumSignature []byte // Variable size for Dilithium
}

// Serialize converts a HybridSignature to bytes
// Format: [type:1][ecdsa_len:4][ecdsa_sig][dilithium_len:4][dilithium_sig]
func (hs *HybridSignature) Serialize() []byte {
	var buf bytes.Buffer

	// Write signature type
	buf.WriteByte(hs.Type)

	// Write ECDSA signature if present
	ecdsaLen := uint32(len(hs.ECDSASignature))
	binary.Write(&buf, binary.BigEndian, ecdsaLen)
	if ecdsaLen > 0 {
		buf.Write(hs.ECDSASignature)
	}

	// Write Dilithium signature if present
	dilithiumLen := uint32(len(hs.DilithiumSignature))
	binary.Write(&buf, binary.BigEndian, dilithiumLen)
	if dilithiumLen > 0 {
		buf.Write(hs.DilithiumSignature)
	}

	return buf.Bytes()
}

// DeserializeHybridSignature parses a byte array into a HybridSignature
func DeserializeHybridSignature(data []byte) (*HybridSignature, error) {
	if len(data) < 9 { // Minimum: type(1) + 2 length fields(8)
		return nil, ErrMalformedHybridSignature
	}

	hs := &HybridSignature{}
	buf := bytes.NewReader(data)

	// Read signature type
	sigType, err := buf.ReadByte()
	if err != nil {
		return nil, err
	}
	hs.Type = sigType

	// Read ECDSA signature
	var ecdsaLen uint32
	if err := binary.Read(buf, binary.BigEndian, &ecdsaLen); err != nil {
		return nil, err
	}
	if ecdsaLen > 0 {
		hs.ECDSASignature = make([]byte, ecdsaLen)
		if _, err := buf.Read(hs.ECDSASignature); err != nil {
			return nil, err
		}
	}

	// Read Dilithium signature
	var dilithiumLen uint32
	if err := binary.Read(buf, binary.BigEndian, &dilithiumLen); err != nil {
		return nil, err
	}
	if dilithiumLen > 0 {
		hs.DilithiumSignature = make([]byte, dilithiumLen)
		if _, err := buf.Read(hs.DilithiumSignature); err != nil {
			return nil, err
		}
	}

	return hs, nil
}

// CreateHybridSignature creates a signature using both ECDSA and Dilithium during transition
func CreateHybridSignature(ecdsaPrivKey *ecdsa.PrivateKey, dilithiumPrivKey []byte, message []byte) ([]byte, error) {
	hs := &HybridSignature{Type: SigTypeHybrid}

	// Create ECDSA signature if key provided
	if ecdsaPrivKey != nil {
		ecdsaSig, err := SignECDSA(ecdsaPrivKey, message)
		if err != nil {
			return nil, fmt.Errorf("ECDSA signing failed: %w", err)
		}
		hs.ECDSASignature = ecdsaSig
	}

	// Create Dilithium signature if key provided
	if len(dilithiumPrivKey) > 0 {
		dilithiumSig, err := SignDataWithDilithium(dilithiumPrivKey, message)
		if err != nil {
			return nil, fmt.Errorf("Dilithium signing failed: %w", err)
		}
		hs.DilithiumSignature = dilithiumSig
	}

	// Ensure at least one signature is present
	if len(hs.ECDSASignature) == 0 && len(hs.DilithiumSignature) == 0 {
		return nil, errors.New("no keys provided for signing")
	}

	// Update type based on what signatures are present
	if len(hs.ECDSASignature) > 0 && len(hs.DilithiumSignature) > 0 {
		hs.Type = SigTypeHybrid
	} else if len(hs.ECDSASignature) > 0 {
		hs.Type = SigTypeECDSA
	} else {
		hs.Type = SigTypeDilithium
	}

	return hs.Serialize(), nil
}

// VerifyHybridSignature verifies a hybrid signature against the message
// During transition, it accepts if EITHER signature is valid
// After transition (quantumOnly=true), it requires Dilithium signature
func VerifyHybridSignature(signature []byte, message []byte, ecdsaPubKey *ecdsa.PublicKey, dilithiumPubKey []byte, quantumOnly bool) (bool, error) {
	// Deserialize the signature
	hs, err := DeserializeHybridSignature(signature)
	if err != nil {
		return false, fmt.Errorf("failed to deserialize signature: %w", err)
	}

	// Check signature type
	switch hs.Type {
	case SigTypeECDSA:
		if quantumOnly {
			return false, errors.New("ECDSA signatures not accepted in quantum-only mode")
		}
		if ecdsaPubKey == nil {
			return false, errors.New("ECDSA public key required for ECDSA signature")
		}
		return VerifyECDSA(ecdsaPubKey, message, hs.ECDSASignature)

	case SigTypeDilithium:
		if len(dilithiumPubKey) == 0 {
			return false, errors.New("Dilithium public key required for Dilithium signature")
		}
		return VerifySignature(dilithiumPubKey, message, hs.DilithiumSignature)

	case SigTypeHybrid:
		// During transition, accept if EITHER signature is valid
		var ecdsaValid, dilithiumValid bool
		var ecdsaErr, dilithiumErr error

		// Verify ECDSA if present and not in quantum-only mode
		if len(hs.ECDSASignature) > 0 && ecdsaPubKey != nil && !quantumOnly {
			ecdsaValid, ecdsaErr = VerifyECDSA(ecdsaPubKey, message, hs.ECDSASignature)
		}

		// Verify Dilithium if present
		if len(hs.DilithiumSignature) > 0 && len(dilithiumPubKey) > 0 {
			dilithiumValid, dilithiumErr = VerifySignature(dilithiumPubKey, message, hs.DilithiumSignature)
		}

		// In quantum-only mode, require valid Dilithium signature
		if quantumOnly {
			if dilithiumErr != nil {
				return false, dilithiumErr
			}
			return dilithiumValid, nil
		}

		// During transition, accept if either is valid
		if ecdsaValid || dilithiumValid {
			return true, nil
		}

		// Both failed, return appropriate error
		if ecdsaErr != nil && dilithiumErr != nil {
			return false, fmt.Errorf("both signatures invalid: ECDSA: %v, Dilithium: %v", ecdsaErr, dilithiumErr)
		} else if ecdsaErr != nil {
			return false, ecdsaErr
		} else {
			return false, dilithiumErr
		}

	default:
		return false, fmt.Errorf("%w: %d", ErrInvalidSignatureType, hs.Type)
	}
}

// SignECDSA creates an ECDSA signature
func SignECDSA(privKey *ecdsa.PrivateKey, message []byte) ([]byte, error) {
	hash := sha256.Sum256(message)
	r, s, err := ecdsa.Sign(rand.Reader, privKey, hash[:])
	if err != nil {
		return nil, err
	}

	// Encode r||s as 64 bytes
	signature := make([]byte, 64)
	rBytes := r.Bytes()
	sBytes := s.Bytes()

	// Pad with zeros if necessary
	copy(signature[32-len(rBytes):32], rBytes)
	copy(signature[64-len(sBytes):], sBytes)

	return signature, nil
}

// VerifyECDSA verifies an ECDSA signature
func VerifyECDSA(pubKey *ecdsa.PublicKey, message []byte, signature []byte) (bool, error) {
	if len(signature) != 64 {
		return false, fmt.Errorf("invalid ECDSA signature length: expected 64, got %d", len(signature))
	}

	hash := sha256.Sum256(message)
	r := new(big.Int).SetBytes(signature[:32])
	s := new(big.Int).SetBytes(signature[32:])

	return ecdsa.Verify(pubKey, hash[:], r, s), nil
}

// UpdateTransactionSignature updates the common.VerifySignature function to support hybrid signatures
func UpdateTransactionSignature(tx *common.Transaction, ecdsaPubKey *ecdsa.PublicKey, dilithiumPubKey []byte, quantumOnly bool) error {
	if tx == nil {
		return errors.New("transaction is nil")
	}

	if len(tx.Signature) == 0 {
		return errors.New("transaction has no signature")
	}

	// Get the signing data
	signingData := common.GetTransactionSigningData(tx)

	// Try to verify as hybrid signature first
	valid, err := VerifyHybridSignature(tx.Signature, signingData, ecdsaPubKey, dilithiumPubKey, quantumOnly)
	if err == nil && valid {
		return nil
	}

	// Fallback: try as raw Dilithium signature (for backward compatibility)
	if len(dilithiumPubKey) > 0 {
		dilithiumValid, dilithiumErr := VerifySignature(dilithiumPubKey, signingData, tx.Signature)
		if dilithiumErr == nil && dilithiumValid {
			return nil
		}
	}

	// Fallback: try as raw ECDSA signature (for backward compatibility)
	if ecdsaPubKey != nil && !quantumOnly && len(tx.Signature) == 64 {
		ecdsaValid, ecdsaErr := VerifyECDSA(ecdsaPubKey, signingData, tx.Signature)
		if ecdsaErr == nil && ecdsaValid {
			return nil
		}
	}

	return fmt.Errorf("signature verification failed: %v", err)
}
