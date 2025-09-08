package tls

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"diamante/common"

	"github.com/sirupsen/logrus"
)

// EnhancedCAManager provides enterprise-grade certificate authority functionality
// with advanced security features, certificate tracking, and revocation support.
type EnhancedCAManager struct {
	CACertPath       string
	CAKeyPath        string
	CertDir          string
	CRLPath          string // Certificate Revocation List
	caCert           *x509.Certificate
	caKey            *rsa.PrivateKey
	revokedCerts     map[string]*RevokedCert // Serial number -> revocation info
	certCache        map[string]*CertInfo    // Node ID -> certificate info
	mu               sync.RWMutex
	logger           *logrus.Logger
	keySize          int
	certValidityDays int
	crlNumber        *big.Int
}

// CertInfo holds comprehensive certificate metadata
type CertInfo struct {
	Certificate *x509.Certificate
	PrivateKey  *rsa.PrivateKey
	NodeID      string
	IssuedAt    time.Time
	ExpiresAt   time.Time
	SerialNum   string
	Fingerprint string
	IPAddresses []net.IP
	DNSNames    []string
	KeyUsage    x509.KeyUsage
	ExtKeyUsage []x509.ExtKeyUsage
}

// RevokedCert holds information about revoked certificates
type RevokedCert struct {
	SerialNumber   *big.Int
	RevocationTime time.Time
	Reason         int // CRL reason code
	NodeID         string
}

// CAConfig holds configuration for the Enhanced Certificate Authority
type CAConfig struct {
	CACertPath       string
	CAKeyPath        string
	CertDir          string
	CRLPath          string
	KeySize          int
	CertValidityDays int
	Logger           *logrus.Logger
	Organization     string
	Country          string
	Province         string
	Locality         string
}

// DefaultEnhancedCAConfig returns a production-ready CA configuration
func DefaultEnhancedCAConfig() *CAConfig {
	return &CAConfig{
		CACertPath:       "certs/ca.crt",
		CAKeyPath:        "certs/ca.key",
		CertDir:          "certs/nodes",
		CRLPath:          "certs/ca.crl",
		KeySize:          4096, // Enhanced security with 4096-bit keys
		CertValidityDays: 365,
		Logger:           logrus.New(),
		Organization:     "Diamante Blockchain Network",
		Country:          "US",
		Province:         "California",
		Locality:         "San Francisco",
	}
}

// NewEnhancedCAManager creates an enhanced CA manager with production-grade security
func NewEnhancedCAManager(config *CAConfig) (*EnhancedCAManager, error) {
	if config == nil {
		config = DefaultEnhancedCAConfig()
	}

	// Ensure certificate directories exist
	if err := os.MkdirAll(filepath.Dir(config.CACertPath), 0700); err != nil {
		return nil, fmt.Errorf("failed to create CA cert directory: %w", err)
	}
	if err := os.MkdirAll(config.CertDir, 0700); err != nil {
		return nil, fmt.Errorf("failed to create node cert directory: %w", err)
	}

	m := &EnhancedCAManager{
		CACertPath:       config.CACertPath,
		CAKeyPath:        config.CAKeyPath,
		CertDir:          config.CertDir,
		CRLPath:          config.CRLPath,
		revokedCerts:     make(map[string]*RevokedCert),
		certCache:        make(map[string]*CertInfo),
		logger:           config.Logger,
		keySize:          config.KeySize,
		certValidityDays: config.CertValidityDays,
		crlNumber:        big.NewInt(1),
	}

	if err := m.loadOrCreateCA(config); err != nil {
		return nil, fmt.Errorf("failed to initialize CA: %w", err)
	}

	// Load existing CRL if it exists
	if err := m.loadCRL(); err != nil {
		m.logger.Warnf("Failed to load existing CRL: %v", err)
	}

	m.logger.Info("Enhanced CA Manager initialized successfully")
	return m, nil
}

// loadOrCreateCA loads existing CA or creates a new one with enhanced security
func (m *EnhancedCAManager) loadOrCreateCA(config *CAConfig) error {
	certData, certErr := os.ReadFile(m.CACertPath)
	keyData, keyErr := os.ReadFile(m.CAKeyPath)

	if certErr == nil && keyErr == nil {
		// Load existing CA
		if err := m.loadExistingCA(certData, keyData); err != nil {
			return fmt.Errorf("failed to load existing CA: %w", err)
		}
		m.logger.Info("Loaded existing CA certificate")
		return nil
	}

	// Create new CA with enhanced security
	return m.createNewCA(config)
}

// loadExistingCA loads an existing CA certificate and key
func (m *EnhancedCAManager) loadExistingCA(certData, keyData []byte) error {
	// Parse certificate
	block, _ := pem.Decode(certData)
	if block == nil {
		return fmt.Errorf("failed to decode CA certificate PEM")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return fmt.Errorf("failed to parse CA certificate: %w", err)
	}

	// Parse private key
	block, keyRemainder := pem.Decode(keyData)
	_ = keyRemainder // Remainder is expected to be empty for single key files
	if block == nil {
		return fmt.Errorf("failed to decode CA private key PEM")
	}
	key, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	if err != nil {
		return fmt.Errorf("failed to parse CA private key: %w", err)
	}

	// Validate certificate
	if common.ConsensusNow().After(cert.NotAfter) {
		return fmt.Errorf("CA certificate has expired")
	}
	if common.ConsensusNow().Add(30 * 24 * time.Hour).After(cert.NotAfter) {
		m.logger.Warn("CA certificate will expire within 30 days")
	}

	m.caCert = cert
	m.caKey = key
	return nil
}

// createNewCA creates a new CA certificate with enhanced security features
func (m *EnhancedCAManager) createNewCA(config *CAConfig) error {
	m.logger.Info("Creating new CA certificate with enhanced security")

	// Generate strong private key
	key, err := rsa.GenerateKey(rand.Reader, m.keySize)
	if err != nil {
		return fmt.Errorf("failed to generate CA private key: %w", err)
	}

	// Create CA certificate template with enhanced security
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName:         "Diamante Blockchain CA",
			Organization:       []string{config.Organization},
			Country:            []string{config.Country},
			Province:           []string{config.Province},
			Locality:           []string{config.Locality},
			OrganizationalUnit: []string{"Certificate Authority"},
		},
		NotBefore:             common.ConsensusNow().Add(-5 * time.Minute),          // 5 minute clock skew tolerance
		NotAfter:              common.ConsensusNow().Add(10 * 365 * 24 * time.Hour), // 10 years
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
		MaxPathLen:            0, // No intermediate CAs allowed
		MaxPathLenZero:        true,
	}

	// Create the certificate
	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return fmt.Errorf("failed to create CA certificate: %w", err)
	}

	// Save certificate
	if err := m.saveCertificatePEM(m.CACertPath, certDER); err != nil {
		return fmt.Errorf("failed to save CA certificate: %w", err)
	}

	// Save private key with restricted permissions
	if err := m.savePrivateKeyPEM(m.CAKeyPath, key); err != nil {
		return fmt.Errorf("failed to save CA private key: %w", err)
	}

	// Parse the created certificate
	cert, err := x509.ParseCertificate(certDER)
	if err != nil {
		return fmt.Errorf("failed to parse created CA certificate: %w", err)
	}

	m.caCert = cert
	m.caKey = key

	m.logger.WithFields(logrus.Fields{
		"serial":     cert.SerialNumber.String(),
		"subject":    cert.Subject.String(),
		"not_before": cert.NotBefore,
		"not_after":  cert.NotAfter,
		"key_size":   m.keySize,
	}).Info("Created new CA certificate")

	return nil
}

// GenerateNodeCertificate creates a certificate for a blockchain node with enhanced security
func (m *EnhancedCAManager) GenerateNodeCertificate(nodeID string, ipAddresses []net.IP, dnsNames []string) (*CertInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.caCert == nil || m.caKey == nil {
		return nil, fmt.Errorf("CA not initialized")
	}

	// Check if certificate already exists and is valid
	if existing, exists := m.certCache[nodeID]; exists {
		if common.ConsensusNow().Add(24 * time.Hour).Before(existing.ExpiresAt) {
			m.logger.WithField("node_id", nodeID).Debug("Using existing valid certificate")
			return existing, nil
		}
		// Certificate is expiring soon, revoke it
		m.revokeCertificateInternal(existing.SerialNum, 4) // superseded
	}

	// Generate new private key
	key, err := rsa.GenerateKey(rand.Reader, m.keySize)
	if err != nil {
		return nil, fmt.Errorf("failed to generate node private key: %w", err)
	}

	// Create certificate template
	serialNumber, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, fmt.Errorf("failed to generate serial number: %w", err)
	}

	template := &x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			CommonName:         nodeID,
			Organization:       []string{"Diamante Blockchain Network"},
			OrganizationalUnit: []string{"Blockchain Node"},
		},
		NotBefore:   common.ConsensusNow().Add(-5 * time.Minute), // 5 minute clock skew tolerance
		NotAfter:    common.ConsensusNow().Add(time.Duration(m.certValidityDays) * 24 * time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		IPAddresses: ipAddresses,
		DNSNames:    dnsNames,
	}

	// Create the certificate
	certDER, err := x509.CreateCertificate(rand.Reader, template, m.caCert, &key.PublicKey, m.caKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create node certificate: %w", err)
	}

	// Parse the created certificate
	cert, err := x509.ParseCertificate(certDER)
	if err != nil {
		return nil, fmt.Errorf("failed to parse created certificate: %w", err)
	}

	// Calculate fingerprint
	fingerprint := sha256.Sum256(cert.Raw)
	fingerprintHex := hex.EncodeToString(fingerprint[:])

	// Create certificate info
	certInfo := &CertInfo{
		Certificate: cert,
		PrivateKey:  key,
		NodeID:      nodeID,
		IssuedAt:    cert.NotBefore,
		ExpiresAt:   cert.NotAfter,
		SerialNum:   cert.SerialNumber.String(),
		Fingerprint: fingerprintHex,
		IPAddresses: ipAddresses,
		DNSNames:    dnsNames,
		KeyUsage:    cert.KeyUsage,
		ExtKeyUsage: cert.ExtKeyUsage,
	}

	// Save certificate and key files
	certPath := filepath.Join(m.CertDir, fmt.Sprintf("%s.crt", nodeID))
	keyPath := filepath.Join(m.CertDir, fmt.Sprintf("%s.key", nodeID))

	if err := m.saveCertificatePEM(certPath, certDER); err != nil {
		return nil, fmt.Errorf("failed to save node certificate: %w", err)
	}

	if err := m.savePrivateKeyPEM(keyPath, key); err != nil {
		return nil, fmt.Errorf("failed to save node private key: %w", err)
	}

	// Cache the certificate info
	m.certCache[nodeID] = certInfo

	m.logger.WithFields(logrus.Fields{
		"node_id":     nodeID,
		"serial":      cert.SerialNumber.String(),
		"fingerprint": fingerprintHex[:16] + "...",
		"expires":     cert.NotAfter,
		"ip_count":    len(ipAddresses),
		"dns_count":   len(dnsNames),
	}).Info("Generated new node certificate")

	return certInfo, nil
}

// RevokeCertificate revokes a certificate and updates the CRL
func (m *EnhancedCAManager) RevokeCertificate(nodeID string, reason int) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	certInfo, exists := m.certCache[nodeID]
	if !exists {
		return fmt.Errorf("certificate for node %s not found", nodeID)
	}

	return m.revokeCertificateInternal(certInfo.SerialNum, reason)
}

// revokeCertificateInternal performs the actual revocation (must be called with lock held)
func (m *EnhancedCAManager) revokeCertificateInternal(serialNum string, reason int) error {
	serial, ok := new(big.Int).SetString(serialNum, 10)
	if !ok {
		return fmt.Errorf("invalid serial number: %s", serialNum)
	}

	revokedCert := &RevokedCert{
		SerialNumber:   serial,
		RevocationTime: common.ConsensusNow(),
		Reason:         reason,
	}

	m.revokedCerts[serialNum] = revokedCert

	// Update CRL
	if err := m.generateCRL(); err != nil {
		return fmt.Errorf("failed to update CRL: %w", err)
	}

	m.logger.WithFields(logrus.Fields{
		"serial": serialNum,
		"reason": reason,
	}).Info("Certificate revoked")

	return nil
}

// generateCRL creates a new Certificate Revocation List
func (m *EnhancedCAManager) generateCRL() error {
	var revokedCerts []pkix.RevokedCertificate
	for _, revoked := range m.revokedCerts {
		revokedCerts = append(revokedCerts, pkix.RevokedCertificate{
			SerialNumber:   revoked.SerialNumber,
			RevocationTime: revoked.RevocationTime,
		})
	}

	crlTemplate := &x509.RevocationList{
		SignatureAlgorithm:  x509.SHA256WithRSA,
		RevokedCertificates: revokedCerts,
		Number:              m.crlNumber,
		ThisUpdate:          common.ConsensusNow(),
		NextUpdate:          common.ConsensusNow().Add(24 * time.Hour), // Update daily
		ExtraExtensions:     []pkix.Extension{},
	}

	crlDER, err := x509.CreateRevocationList(rand.Reader, crlTemplate, m.caCert, m.caKey)
	if err != nil {
		return fmt.Errorf("failed to create CRL: %w", err)
	}

	// Save CRL
	crlOut, err := os.OpenFile(m.CRLPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to create CRL file: %w", err)
	}
	defer crlOut.Close()

	if err := pem.Encode(crlOut, &pem.Block{Type: "X509 CRL", Bytes: crlDER}); err != nil {
		return fmt.Errorf("failed to encode CRL: %w", err)
	}

	m.crlNumber.Add(m.crlNumber, big.NewInt(1))
	return nil
}

// loadCRL loads an existing Certificate Revocation List
func (m *EnhancedCAManager) loadCRL() error {
	crlData, err := os.ReadFile(m.CRLPath)
	if err != nil {
		return err // File might not exist yet
	}

	block, _ := pem.Decode(crlData)
	if block == nil {
		return fmt.Errorf("failed to decode CRL PEM")
	}

	crl, err := x509.ParseRevocationList(block.Bytes)
	if err != nil {
		return fmt.Errorf("failed to parse CRL: %w", err)
	}

	// Load revoked certificates into memory
	for _, revoked := range crl.RevokedCertificates {
		m.revokedCerts[revoked.SerialNumber.String()] = &RevokedCert{
			SerialNumber:   revoked.SerialNumber,
			RevocationTime: revoked.RevocationTime,
		}
	}

	m.logger.WithField("revoked_count", len(crl.RevokedCertificates)).Info("Loaded CRL")
	return nil
}

// ValidateCertificate validates a certificate against the CA and CRL
func (m *EnhancedCAManager) ValidateCertificate(cert *x509.Certificate) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Check if certificate is revoked
	if _, revoked := m.revokedCerts[cert.SerialNumber.String()]; revoked {
		return fmt.Errorf("certificate is revoked")
	}

	// Verify certificate against CA
	roots := x509.NewCertPool()
	roots.AddCert(m.caCert)

	opts := x509.VerifyOptions{
		Roots:       roots,
		CurrentTime: common.ConsensusNow(),
	}

	_, err := cert.Verify(opts)
	return err
}

// GetCertificateInfo returns certificate information for a node
func (m *EnhancedCAManager) GetCertificateInfo(nodeID string) (*CertInfo, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	info, exists := m.certCache[nodeID]
	return info, exists
}

// ListCertificates returns all cached certificate information
func (m *EnhancedCAManager) ListCertificates() map[string]*CertInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make(map[string]*CertInfo)
	for k, v := range m.certCache {
		result[k] = v
	}
	return result
}

// GetCACertificate returns the CA certificate
func (m *EnhancedCAManager) GetCACertificate() *x509.Certificate {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.caCert
}

// saveCertificatePEM saves a certificate in PEM format
func (m *EnhancedCAManager) saveCertificatePEM(path string, certDER []byte) error {
	certOut, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer certOut.Close()

	return pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: certDER})
}

// savePrivateKeyPEM saves a private key in PEM format with restricted permissions
func (m *EnhancedCAManager) savePrivateKeyPEM(path string, key *rsa.PrivateKey) error {
	keyOut, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600) // Restricted permissions
	if err != nil {
		return err
	}
	defer keyOut.Close()

	return pem.Encode(keyOut, &pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(key),
	})
}

// CleanupExpiredCertificates removes expired certificates from cache and files
func (m *EnhancedCAManager) CleanupExpiredCertificates() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := common.ConsensusNow()
	var expiredNodes []string

	for nodeID, certInfo := range m.certCache {
		if now.After(certInfo.ExpiresAt) {
			expiredNodes = append(expiredNodes, nodeID)
		}
	}

	for _, nodeID := range expiredNodes {
		delete(m.certCache, nodeID)

		// Remove certificate files
		certPath := filepath.Join(m.CertDir, fmt.Sprintf("%s.crt", nodeID))
		keyPath := filepath.Join(m.CertDir, fmt.Sprintf("%s.key", nodeID))

		os.Remove(certPath)
		os.Remove(keyPath)

		m.logger.WithField("node_id", nodeID).Info("Cleaned up expired certificate")
	}

	if len(expiredNodes) > 0 {
		m.logger.WithField("count", len(expiredNodes)).Info("Cleaned up expired certificates")
	}

	return nil
}
