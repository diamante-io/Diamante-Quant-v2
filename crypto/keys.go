package crypto

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"os"
	"path/filepath"
)

// GenerateKey generates a new ECDSA private key
func GenerateKey() (*ecdsa.PrivateKey, error) {
	return ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
}

// SaveKey saves a private key to a file
func SaveKey(filename string, key *ecdsa.PrivateKey) error {
	// Create directory if it doesn't exist
	dir := filepath.Dir(filename)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// Marshal private key
	keyBytes, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return fmt.Errorf("failed to marshal private key: %w", err)
	}

	// Create PEM block
	block := &pem.Block{
		Type:  "EC PRIVATE KEY",
		Bytes: keyBytes,
	}

	// Write to file
	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return fmt.Errorf("failed to open key file: %w", err)
	}
	defer file.Close()

	if err := pem.Encode(file, block); err != nil {
		return fmt.Errorf("failed to write key: %w", err)
	}

	return nil
}

// SavePublicKey saves a public key to a file
func SavePublicKey(filename string, key *ecdsa.PublicKey) error {
	// Create directory if it doesn't exist
	dir := filepath.Dir(filename)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// Marshal public key
	keyBytes, err := x509.MarshalPKIXPublicKey(key)
	if err != nil {
		return fmt.Errorf("failed to marshal public key: %w", err)
	}

	// Create PEM block
	block := &pem.Block{
		Type:  "PUBLIC KEY",
		Bytes: keyBytes,
	}

	// Write to file
	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to open key file: %w", err)
	}
	defer file.Close()

	if err := pem.Encode(file, block); err != nil {
		return fmt.Errorf("failed to write key: %w", err)
	}

	return nil
}

// LoadKey loads a private key from a file
func LoadKey(filename string) (*ecdsa.PrivateKey, error) {
	// Read file
	keyPEM, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read key file: %w", err)
	}

	// Decode PEM
	block, _ := pem.Decode(keyPEM)
	if block == nil {
		return nil, fmt.Errorf("failed to parse PEM block")
	}

	// Parse private key
	key, err := x509.ParseECPrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse private key: %w", err)
	}

	return key, nil
}

// LoadPublicKey loads a public key from a file
func LoadPublicKey(filename string) (*ecdsa.PublicKey, error) {
	// Read file
	keyPEM, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read key file: %w", err)
	}

	// Decode PEM
	block, _ := pem.Decode(keyPEM)
	if block == nil {
		return nil, fmt.Errorf("failed to parse PEM block")
	}

	// Parse public key
	pubInterface, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse public key: %w", err)
	}

	// Type assert to ECDSA public key
	pubKey, ok := pubInterface.(*ecdsa.PublicKey)
	if !ok {
		return nil, fmt.Errorf("not an ECDSA public key")
	}

	return pubKey, nil
}

// QuantumKeyPair represents a pair of quantum-resistant keys
type QuantumKeyPair struct {
	DilithiumPublic  []byte `json:"dilithium_public"`
	DilithiumPrivate []byte `json:"dilithium_private"`
	KyberPublic      []byte `json:"kyber_public"`
	KyberPrivate     []byte `json:"kyber_private"`
}

// HybridKeyPair represents both classical and quantum keys for transition period
type HybridKeyPair struct {
	ECDSA    *ecdsa.PrivateKey `json:"-"` // Not serialized directly
	Quantum  *QuantumKeyPair   `json:"quantum"`
	ECDSAPEM string            `json:"ecdsa_pem"` // PEM encoded ECDSA key
}

// GenerateQuantumKeyPair generates a new quantum-resistant key pair
func GenerateQuantumKeyPair() (*QuantumKeyPair, error) {
	// Generate Dilithium key pair at default security level
	dilithiumKeyPair, err := GenerateDilithiumKeyPair(DilithiumLevel3)
	if err != nil {
		return nil, fmt.Errorf("failed to generate Dilithium key pair: %w", err)
	}

	// Generate Kyber key pair at default security level
	// We need to create a KyberCrypto instance first
	kyberScheme := KyberSchemeFromLevel(KyberLevel768)
	kyberCrypto := NewKyberCrypto(kyberScheme, nil)

	kyberKeyPair, err := kyberCrypto.GenerateKeyPair()
	if err != nil {
		return nil, fmt.Errorf("failed to generate Kyber key pair: %w", err)
	}

	return &QuantumKeyPair{
		DilithiumPublic:  dilithiumKeyPair.PublicKey,
		DilithiumPrivate: dilithiumKeyPair.PrivateKey,
		KyberPublic:      kyberKeyPair.PublicKey,
		KyberPrivate:     kyberKeyPair.PrivateKey,
	}, nil
}

// GenerateHybridKeyPair generates both classical and quantum key pairs
func GenerateHybridKeyPair() (*HybridKeyPair, error) {
	// Generate ECDSA key
	ecdsaKey, err := GenerateKey()
	if err != nil {
		return nil, fmt.Errorf("failed to generate ECDSA key: %w", err)
	}

	// Generate quantum keys
	quantumKeys, err := GenerateQuantumKeyPair()
	if err != nil {
		return nil, fmt.Errorf("failed to generate quantum keys: %w", err)
	}

	// Encode ECDSA key to PEM for storage
	keyBytes, err := x509.MarshalECPrivateKey(ecdsaKey)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal ECDSA key: %w", err)
	}
	block := &pem.Block{
		Type:  "EC PRIVATE KEY",
		Bytes: keyBytes,
	}
	ecdsaPEM := string(pem.EncodeToMemory(block))

	return &HybridKeyPair{
		ECDSA:    ecdsaKey,
		Quantum:  quantumKeys,
		ECDSAPEM: ecdsaPEM,
	}, nil
}

// SaveQuantumKeys saves quantum keys to a JSON file
func SaveQuantumKeys(filename string, keys *QuantumKeyPair) error {
	// Create directory if it doesn't exist
	dir := filepath.Dir(filename)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// Encode keys to JSON
	data, err := json.MarshalIndent(keys, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal quantum keys: %w", err)
	}

	// Write to file with restricted permissions
	if err := os.WriteFile(filename, data, 0600); err != nil {
		return fmt.Errorf("failed to write quantum keys: %w", err)
	}

	return nil
}

// LoadQuantumKeys loads quantum keys from a JSON file
func LoadQuantumKeys(filename string) (*QuantumKeyPair, error) {
	// Read file
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read quantum keys file: %w", err)
	}

	// Decode JSON
	var keys QuantumKeyPair
	if err := json.Unmarshal(data, &keys); err != nil {
		return nil, fmt.Errorf("failed to unmarshal quantum keys: %w", err)
	}

	return &keys, nil
}

// SaveHybridKeys saves hybrid keys to a JSON file
func SaveHybridKeys(filename string, keys *HybridKeyPair) error {
	// Create directory if it doesn't exist
	dir := filepath.Dir(filename)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// Encode keys to JSON
	data, err := json.MarshalIndent(keys, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal hybrid keys: %w", err)
	}

	// Write to file with restricted permissions
	if err := os.WriteFile(filename, data, 0600); err != nil {
		return fmt.Errorf("failed to write hybrid keys: %w", err)
	}

	return nil
}

// LoadHybridKeys loads hybrid keys from a JSON file
func LoadHybridKeys(filename string) (*HybridKeyPair, error) {
	// Read file
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read hybrid keys file: %w", err)
	}

	// Decode JSON
	var keys HybridKeyPair
	if err := json.Unmarshal(data, &keys); err != nil {
		return nil, fmt.Errorf("failed to unmarshal hybrid keys: %w", err)
	}

	// Decode ECDSA key from PEM
	if keys.ECDSAPEM != "" {
		block, _ := pem.Decode([]byte(keys.ECDSAPEM))
		if block == nil {
			return nil, fmt.Errorf("failed to parse ECDSA PEM block")
		}

		ecdsaKey, err := x509.ParseECPrivateKey(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("failed to parse ECDSA private key: %w", err)
		}
		keys.ECDSA = ecdsaKey
	}

	return &keys, nil
}

// ExportQuantumPublicKeys exports only the public components of quantum keys
func ExportQuantumPublicKeys(keys *QuantumKeyPair) map[string]string {
	return map[string]string{
		"dilithium_public": base64.StdEncoding.EncodeToString(keys.DilithiumPublic),
		"kyber_public":     base64.StdEncoding.EncodeToString(keys.KyberPublic),
	}
}

// ImportQuantumPublicKeys imports public quantum keys from base64 encoded strings
func ImportQuantumPublicKeys(dilithiumPubB64, kyberPubB64 string) (*QuantumKeyPair, error) {
	dilithiumPub, err := base64.StdEncoding.DecodeString(dilithiumPubB64)
	if err != nil {
		return nil, fmt.Errorf("failed to decode Dilithium public key: %w", err)
	}

	kyberPub, err := base64.StdEncoding.DecodeString(kyberPubB64)
	if err != nil {
		return nil, fmt.Errorf("failed to decode Kyber public key: %w", err)
	}

	return &QuantumKeyPair{
		DilithiumPublic: dilithiumPub,
		KyberPublic:     kyberPub,
	}, nil
}
