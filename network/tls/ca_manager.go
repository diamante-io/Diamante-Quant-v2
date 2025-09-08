package tls

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"time"

	"diamante/consensus"

	"github.com/sirupsen/logrus"
)

// CAManager handles creation and loading of a simple certificate authority and
// node certificates. It is intentionally lightweight to keep the example short.
type CAManager struct {
	CACertPath string
	CAKeyPath  string
	caCert     *x509.Certificate
	caKey      *rsa.PrivateKey
	logger     *logrus.Logger
}

// NewCAManager creates a CA manager and loads or generates the CA certificate
// and key if they do not exist.
func NewCAManager(certPath, keyPath string) (*CAManager, error) {
	m := &CAManager{CACertPath: certPath, CAKeyPath: keyPath}
	if err := m.loadOrCreateCA(); err != nil {
		return nil, err
	}
	return m, nil
}

func (m *CAManager) loadOrCreateCA() error {
	certData, certErr := os.ReadFile(m.CACertPath)
	keyData, keyErr := os.ReadFile(m.CAKeyPath)
	if certErr == nil && keyErr == nil {
		block, _ := pem.Decode(certData)
		if block == nil {
			return fmt.Errorf("invalid certificate data: %w", os.ErrInvalid)
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return fmt.Errorf("failed to parse CA certificate: %w", err)
		}
		block, keyRemainder := pem.Decode(keyData)
		_ = keyRemainder // Remainder is expected to be empty for single key files
		if block == nil {
			return fmt.Errorf("invalid key data: %w", os.ErrInvalid)
		}
		key, err := x509.ParsePKCS1PrivateKey(block.Bytes)
		if err != nil {
			return fmt.Errorf("failed to parse CA private key: %w", err)
		}
		m.caCert = cert
		m.caKey = key
		return nil
	}

	// Create new CA
	key, err := rsa.GenerateKey(rand.Reader, 4096)
	if err != nil {
		return fmt.Errorf("failed to generate CA private key: %w", err)
	}
	template := x509.Certificate{
		SerialNumber:          big.NewInt(consensus.ConsensusUnixNano()),
		Subject:               pkix.Name{CommonName: "Diamante CA"},
		NotBefore:             consensus.ConsensusNow(),
		NotAfter:              consensus.ConsensusNow().Add(10 * 365 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		MaxPathLen:            0,
		MaxPathLenZero:        true,
	}
	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		return fmt.Errorf("failed to create CA certificate: %w", err)
	}
	cert, err := x509.ParseCertificate(certDER)
	if err != nil {
		return fmt.Errorf("failed to parse CA certificate: %w", err)
	}
	m.caCert = cert
	m.caKey = key

	if err := m.saveCertificate(m.CACertPath, cert); err != nil {
		return fmt.Errorf("failed to save CA certificate: %w", err)
	}

	if err := m.savePrivateKey(m.CAKeyPath, key); err != nil {
		return fmt.Errorf("failed to save CA private key: %w", err)
	}

	return nil
}

// GenerateNodeCert issues a certificate for the given node ID signed by the CA.
func (m *CAManager) GenerateNodeCert(nodeID, certPath, keyPath string) error {
	if m.caCert == nil || m.caKey == nil {
		return fmt.Errorf("CA not available")
	}
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return fmt.Errorf("failed to generate node private key: %w", err)
	}
	template := x509.Certificate{
		SerialNumber: big.NewInt(consensus.ConsensusUnixNano()),
		Subject:      pkix.Name{CommonName: nodeID},
		NotBefore:    consensus.ConsensusNow().Add(-time.Hour),
		NotAfter:     consensus.ConsensusNow().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	certDER, err := x509.CreateCertificate(rand.Reader, &template, m.caCert, &key.PublicKey, m.caKey)
	if err != nil {
		return fmt.Errorf("failed to create node certificate: %w", err)
	}
	cert, err := x509.ParseCertificate(certDER)
	if err != nil {
		return fmt.Errorf("failed to parse node certificate: %w", err)
	}

	if err := m.saveCertificate(certPath, cert); err != nil {
		return fmt.Errorf("failed to save node certificate: %w", err)
	}

	if err := m.savePrivateKey(keyPath, key); err != nil {
		return fmt.Errorf("failed to save node private key: %w", err)
	}

	return nil
}

func (m *CAManager) CACertificate() *x509.Certificate {
	return m.caCert
}

func (m *CAManager) saveCertificate(path string, cert *x509.Certificate) error {
	certOut, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("failed to create certificate file: %w", err)
	}
	defer certOut.Close()

	if err := pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw}); err != nil {
		return fmt.Errorf("failed to encode certificate: %w", err)
	}
	return nil
}

func (m *CAManager) savePrivateKey(path string, key *rsa.PrivateKey) error {
	keyOut, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("failed to create private key file: %w", err)
	}
	defer keyOut.Close()

	if err := pem.Encode(keyOut, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}); err != nil {
		return fmt.Errorf("failed to encode private key: %w", err)
	}
	return nil
}
