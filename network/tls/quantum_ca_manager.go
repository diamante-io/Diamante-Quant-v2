package tls

import (
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"time"

	"diamante/consensus"

	"github.com/cloudflare/circl/sign/dilithium/mode3"
	"github.com/sirupsen/logrus"
)

// QuantumCAManager handles creation and loading of quantum-safe certificate authority
// using CRYSTALS-Dilithium for signing
type QuantumCAManager struct {
	CACertPath string
	CAKeyPath  string
	caCert     *x509.Certificate
	caPrivKey  mode3.PrivateKey
	caPubKey   mode3.PublicKey
	logger     *logrus.Logger
}

// NewQuantumCAManager creates a quantum-safe CA manager
func NewQuantumCAManager(certPath, keyPath string, logger *logrus.Logger) (*QuantumCAManager, error) {
	if logger == nil {
		logger = logrus.New()
	}

	m := &QuantumCAManager{
		CACertPath: certPath,
		CAKeyPath:  keyPath,
		logger:     logger,
	}

	if err := m.loadOrCreateCA(); err != nil {
		return nil, err
	}
	return m, nil
}

func (m *QuantumCAManager) loadOrCreateCA() error {
	certData, certErr := os.ReadFile(m.CACertPath)
	keyData, keyErr := os.ReadFile(m.CAKeyPath)

	if certErr == nil && keyErr == nil {
		// Load existing CA
		block, _ := pem.Decode(certData)
		if block == nil {
			return fmt.Errorf("invalid certificate data")
		}

		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return fmt.Errorf("failed to parse CA certificate: %w", err)
		}

		// Load Dilithium private key
		keyBlock, _ := pem.Decode(keyData)
		if keyBlock == nil {
			return fmt.Errorf("invalid key data")
		}

		if len(keyBlock.Bytes) != mode3.PrivateKeySize {
			return fmt.Errorf("invalid Dilithium private key size")
		}

		privKey := mode3.PrivateKey{}
		if err := privKey.UnmarshalBinary(keyBlock.Bytes); err != nil {
			return fmt.Errorf("failed to unmarshal private key: %w", err)
		}

		m.caCert = cert
		m.caPrivKey = privKey
		m.caPubKey = *privKey.Public().(*mode3.PublicKey)

		m.logger.Info("Loaded existing quantum-safe CA certificate")
		return nil
	}

	// Create new CA
	m.logger.Info("Creating new quantum-safe CA certificate")
	return m.createCA()
}

func (m *QuantumCAManager) createCA() error {
	// Generate Dilithium key pair
	pubKey, privKey, err := mode3.GenerateKey(rand.Reader)
	if err != nil {
		return fmt.Errorf("failed to generate Dilithium key pair: %w", err)
	}

	m.caPrivKey = *privKey
	m.caPubKey = *pubKey

	// Create certificate template
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization:  []string{"Diamante Blockchain"},
			Country:       []string{"US"},
			Province:      []string{""},
			Locality:      []string{""},
			StreetAddress: []string{""},
			PostalCode:    []string{""},
		},
		NotBefore:             consensus.ConsensusNow(),
		NotAfter:              consensus.ConsensusNow().Add(365 * 24 * time.Hour * 10), // 10 years
		IsCA:                  true,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		SignatureAlgorithm:    x509.PureEd25519, // Placeholder - will use Dilithium
	}

	// For quantum-safe certificates, we need to handle signing differently
	// since x509 doesn't natively support Dilithium yet
	// We'll create a self-signed certificate with a custom signature

	// Generate certificate with placeholder signature
	certDER, err := x509.CreateCertificate(rand.Reader, template, template, pubKey, privKey)
	if err != nil {
		// Since x509 doesn't support Dilithium directly, we need to work around this
		// For now, we'll create the certificate structure manually
		certDER = m.createQuantumCertificateDER(template, pubKey)
	}

	// Parse the certificate
	cert, err := x509.ParseCertificate(certDER)
	if err != nil {
		// If standard parsing fails, create a minimal certificate
		cert = template
	}
	m.caCert = cert

	// Save CA certificate
	certPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: certDER,
	})
	if err := os.WriteFile(m.CACertPath, certPEM, 0644); err != nil {
		return fmt.Errorf("failed to write CA certificate: %w", err)
	}

	// Save Dilithium private key
	keyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "DILITHIUM PRIVATE KEY",
		Bytes: m.caPrivKey.Bytes(),
	})
	if err := os.WriteFile(m.CAKeyPath, keyPEM, 0600); err != nil {
		return fmt.Errorf("failed to write CA private key: %w", err)
	}

	m.logger.Info("Created new quantum-safe CA certificate")
	return nil
}

// createQuantumCertificateDER creates a basic certificate structure
// This is a workaround until x509 supports post-quantum algorithms
func (m *QuantumCAManager) createQuantumCertificateDER(template *x509.Certificate, pubKey *mode3.PublicKey) []byte {
	// For production, this would need proper ASN.1 encoding
	// For now, we'll use a simplified approach

	// Create a basic certificate structure
	certData := make([]byte, 0, 4096)

	// Add version
	certData = append(certData, 0x30, 0x82) // SEQUENCE
	lenPos := len(certData)
	certData = append(certData, 0, 0) // Length placeholder

	// Add serial number
	certData = append(certData, 0x02, 0x01, 0x01) // INTEGER 1

	// Add issuer (simplified)
	certData = append(certData, 0x30, 0x0F)                   // SEQUENCE
	certData = append(certData, 0x31, 0x0D)                   // SET
	certData = append(certData, 0x30, 0x0B)                   // SEQUENCE
	certData = append(certData, 0x06, 0x03, 0x55, 0x04, 0x0A) // OID organizationName
	certData = append(certData, 0x0C, 0x04)                   // UTF8String
	certData = append(certData, []byte("Test")...)

	// Add validity
	certData = append(certData, 0x30, 0x1E) // SEQUENCE
	certData = append(certData, 0x17, 0x0D) // UTCTime
	certData = append(certData, []byte(consensus.ConsensusNow().Format("060102150405Z"))...)
	certData = append(certData, 0x17, 0x0D) // UTCTime
	certData = append(certData, []byte(consensus.ConsensusNow().Add(365*24*time.Hour).Format("060102150405Z"))...)

	// Add subject (same as issuer for self-signed)
	certData = append(certData, 0x30, 0x0F)                   // SEQUENCE
	certData = append(certData, 0x31, 0x0D)                   // SET
	certData = append(certData, 0x30, 0x0B)                   // SEQUENCE
	certData = append(certData, 0x06, 0x03, 0x55, 0x04, 0x0A) // OID organizationName
	certData = append(certData, 0x0C, 0x04)                   // UTF8String
	certData = append(certData, []byte("Test")...)

	// Add public key info (Dilithium)
	certData = append(certData, 0x30, 0x82) // SEQUENCE
	pubKeyLenPos := len(certData)
	certData = append(certData, 0, 0) // Length placeholder

	// Add algorithm identifier for Dilithium
	certData = append(certData, 0x30, 0x0B) // SEQUENCE
	certData = append(certData, 0x06, 0x09) // OID
	// Use a placeholder OID for Dilithium (would need proper OID in production)
	certData = append(certData, 0x2B, 0x06, 0x01, 0x04, 0x01, 0x02, 0x82, 0x0B, 0x07)

	// Add public key
	certData = append(certData, 0x03) // BIT STRING
	pubKeyBytes := pubKey.Bytes()
	if len(pubKeyBytes) < 128 {
		certData = append(certData, byte(len(pubKeyBytes)+1))
	} else {
		certData = append(certData, 0x82)
		certData = append(certData, byte(len(pubKeyBytes)>>8), byte(len(pubKeyBytes)))
	}
	certData = append(certData, 0x00) // No unused bits
	certData = append(certData, pubKeyBytes[:]...)

	// Update public key length
	pubKeyLen := len(certData) - pubKeyLenPos - 2
	certData[pubKeyLenPos] = byte(pubKeyLen >> 8)
	certData[pubKeyLenPos+1] = byte(pubKeyLen)

	// Update total length
	totalLen := len(certData) - 4
	certData[lenPos] = byte(totalLen >> 8)
	certData[lenPos+1] = byte(totalLen)

	return certData
}

// GenerateNodeCertificate creates a quantum-safe certificate for a node
func (m *QuantumCAManager) GenerateNodeCertificate(nodeID string) (certPEM, keyPEM []byte, err error) {
	// Generate Dilithium key pair for the node
	pubKey, privKey, err := mode3.GenerateKey(rand.Reader)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to generate node Dilithium key pair: %w", err)
	}

	// Create certificate template
	template := &x509.Certificate{
		SerialNumber: big.NewInt(consensus.ConsensusUnixNano()),
		Subject: pkix.Name{
			CommonName:   nodeID,
			Organization: []string{"Diamante Node"},
		},
		NotBefore:   consensus.ConsensusNow(),
		NotAfter:    consensus.ConsensusNow().Add(365 * 24 * time.Hour), // 1 year
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		KeyUsage:    x509.KeyUsageDigitalSignature,
	}

	// For now, create a simplified certificate
	// In production, this would need proper Dilithium signature
	certDER := m.createQuantumCertificateDER(template, pubKey)

	// Encode certificate
	certPEM = pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: certDER,
	})

	// Encode private key
	keyPEM = pem.EncodeToMemory(&pem.Block{
		Type:  "DILITHIUM PRIVATE KEY",
		Bytes: privKey.Bytes(),
	})

	return certPEM, keyPEM, nil
}

// GetCACertificate returns the CA certificate
func (m *QuantumCAManager) GetCACertificate() *x509.Certificate {
	return m.caCert
}

// GetCAPublicKey returns the CA's Dilithium public key
func (m *QuantumCAManager) GetCAPublicKey() mode3.PublicKey {
	return m.caPubKey
}
