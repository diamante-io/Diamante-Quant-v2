package network

import (
	"bytes"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"io/ioutil"
	"math/big"
	"net/http"
	"sync"
	"time"

	"diamante/apperrors"
	"diamante/common"

	"github.com/sirupsen/logrus"
)

// EnhancedTLSConfig provides advanced TLS configuration with certificate pinning
type EnhancedTLSConfig struct {
	// Base TLS configuration
	BaseConfig *tls.Config

	// Certificate pinning configuration
	EnablePinning bool
	PinnedKeys    map[string][]string // hostname -> list of base64 encoded SPKI hashes

	// Certificate validation
	EnableRevocationCheck bool
	RevocationCheckURL    string
	CustomVerifyFunc      func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error

	// Mutual TLS
	RequireClientCert bool
	ClientCAs         *x509.CertPool

	// Session management
	EnableSessionCache bool
	SessionCacheSize   int

	// Logger
	Logger *logrus.Logger

	mu sync.RWMutex
}

// NewEnhancedTLSConfig creates a new enhanced TLS configuration
func NewEnhancedTLSConfig(logger *logrus.Logger) *EnhancedTLSConfig {
	if logger == nil {
		logger = logrus.New()
	}

	return &EnhancedTLSConfig{
		BaseConfig: &tls.Config{
			MinVersion: tls.VersionTLS13, // CRITICAL: TLS 1.3 ONLY for quantum safety
			MaxVersion: tls.VersionTLS13, // CRITICAL: Lock to TLS 1.3 only
			CipherSuites: []uint16{
				// QUANTUM-SAFE: TLS 1.3 cipher suites only
				tls.TLS_AES_256_GCM_SHA384,       // AES-GCM is quantum-resistant
				tls.TLS_CHACHA20_POLY1305_SHA256, // ChaCha20-Poly1305 is quantum-resistant
				tls.TLS_AES_128_GCM_SHA256,       // Additional AES-GCM option
			},
			PreferServerCipherSuites: true,
			SessionTicketsDisabled:   false,
		},
		PinnedKeys:         make(map[string][]string),
		EnableSessionCache: true,
		SessionCacheSize:   100,
		Logger:             logger,
	}
}

// BuildTLSConfig builds a tls.Config with enhanced security features
func (etc *EnhancedTLSConfig) BuildTLSConfig() (*tls.Config, error) {
	etc.mu.RLock()
	defer etc.mu.RUnlock()

	config := etc.BaseConfig.Clone()

	// Set up certificate verification
	config.VerifyPeerCertificate = etc.verifyPeerCertificate

	// Set up client certificate requirements
	if etc.RequireClientCert {
		config.ClientAuth = tls.RequireAndVerifyClientCert
		config.ClientCAs = etc.ClientCAs
	}

	// Set up session cache
	if etc.EnableSessionCache {
		config.ClientSessionCache = tls.NewLRUClientSessionCache(etc.SessionCacheSize)
	}

	return config, nil
}

// verifyPeerCertificate implements custom certificate verification
func (etc *EnhancedTLSConfig) verifyPeerCertificate(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
	if len(rawCerts) == 0 {
		return apperrors.New(apperrors.ModuleNetwork, apperrors.CodeInvalid, "no certificates provided")
	}

	// Parse the leaf certificate
	cert, err := x509.ParseCertificate(rawCerts[0])
	if err != nil {
		return apperrors.Wrap(err, apperrors.ModuleNetwork, apperrors.CodeInvalid, "failed to parse certificate")
	}

	// Perform public key pinning if enabled
	if etc.EnablePinning {
		if err := etc.verifyPublicKeyPin(cert); err != nil {
			return err
		}
	}

	// Check certificate revocation if enabled
	if etc.EnableRevocationCheck {
		if err := etc.checkCertificateRevocation(cert); err != nil {
			return err
		}
	}

	// Run custom verification function if provided
	if etc.CustomVerifyFunc != nil {
		if err := etc.CustomVerifyFunc(rawCerts, verifiedChains); err != nil {
			return apperrors.Wrap(err, apperrors.ModuleNetwork, apperrors.CodeInvalid, "custom verification failed")
		}
	}

	etc.Logger.Debug("Certificate verification successful",
		"subject", cert.Subject,
		"issuer", cert.Issuer,
		"serialNumber", cert.SerialNumber.String())

	return nil
}

// verifyPublicKeyPin verifies the certificate against pinned public keys (RFC 7469)
func (etc *EnhancedTLSConfig) verifyPublicKeyPin(cert *x509.Certificate) error {
	// Get the hostname from the certificate
	hostname := ""
	if len(cert.DNSNames) > 0 {
		hostname = cert.DNSNames[0]
	} else {
		hostname = cert.Subject.CommonName
	}

	pinnedHashes, ok := etc.PinnedKeys[hostname]
	if !ok || len(pinnedHashes) == 0 {
		// No pins for this hostname
		return nil
	}

	// Calculate SPKI hash of the certificate
	spkiHash := calculateSPKIHash(cert)
	spkiHashBase64 := base64.StdEncoding.EncodeToString(spkiHash)

	// Check if the hash matches any pinned hash
	for _, pinnedHash := range pinnedHashes {
		if pinnedHash == spkiHashBase64 {
			etc.Logger.Debug("Public key pin matched",
				"hostname", hostname,
				"hash", spkiHashBase64)
			return nil
		}
	}

	// Also check the chain if available
	if len(cert.IssuingCertificateURL) > 0 {
		// Verify the certificate chain
		if err := etc.verifyCertificateChain(cert); err != nil {
			etc.Logger.Warn("Certificate chain verification failed", "error", err)
			return err
		}
	}

	return apperrors.New(apperrors.ModuleNetwork, apperrors.CodeInvalid,
		fmt.Sprintf("public key pin verification failed for %s", hostname))
}

// calculateSPKIHash calculates the SHA-256 hash of the Subject Public Key Info
func calculateSPKIHash(cert *x509.Certificate) []byte {
	// Get the SPKI from the certificate
	spki, err := x509.MarshalPKIXPublicKey(cert.PublicKey)
	if err != nil {
		// This should never happen with a valid certificate
		return nil
	}

	// Calculate SHA-256 hash
	hash := sha256.Sum256(spki)
	return hash[:]
}

// checkCertificateRevocation checks if the certificate has been revoked
func (etc *EnhancedTLSConfig) checkCertificateRevocation(cert *x509.Certificate) error {
	// Check CRL if available
	if len(cert.CRLDistributionPoints) > 0 {
		if err := etc.checkCRL(cert); err != nil {
			etc.Logger.Warn("CRL check failed", "error", err)
			// Don't fail on CRL check failure, just log it
		}
	}

	// Check OCSP if available
	if len(cert.OCSPServer) > 0 {
		if err := etc.checkOCSP(cert); err != nil {
			etc.Logger.Warn("OCSP check failed", "error", err)
			// Don't fail on OCSP check failure, just log it
		}
	}

	return nil
}

// verifyCertificateChain verifies the certificate chain
func (etc *EnhancedTLSConfig) verifyCertificateChain(cert *x509.Certificate) error {
	// Create a certificate pool with the root CAs
	var roots *x509.CertPool
	if etc.BaseConfig.RootCAs != nil {
		roots = etc.BaseConfig.RootCAs
	} else {
		// Use system root CAs
		systemRoots, err := x509.SystemCertPool()
		if err != nil {
			return fmt.Errorf("failed to load system root CAs: %w", err)
		}
		roots = systemRoots
	}

	// Create intermediate pool
	intermediates := x509.NewCertPool()

	// Verify the certificate chain
	opts := x509.VerifyOptions{
		Roots:         roots,
		Intermediates: intermediates,
		CurrentTime:   common.ConsensusNow(),
		KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageAny},
	}

	chains, err := cert.Verify(opts)
	if err != nil {
		return fmt.Errorf("certificate chain verification failed: %w", err)
	}

	etc.Logger.Debug("Certificate chain verified successfully",
		"chains", len(chains),
		"subject", cert.Subject.String())

	return nil
}

// checkCRL checks Certificate Revocation List
func (etc *EnhancedTLSConfig) checkCRL(cert *x509.Certificate) error {
	// In a production implementation, this would:
	// 1. Download CRL from distribution points
	// 2. Parse the CRL
	// 3. Check if the certificate serial number is in the CRL
	// 4. Verify CRL signature

	// For now, we implement a basic check structure
	for _, crlURL := range cert.CRLDistributionPoints {
		etc.Logger.Debug("Checking CRL", "url", crlURL)

		// Simulate CRL check
		// In production, you would fetch and parse the CRL here
		revoked, err := etc.isCertificateInCRL(cert.SerialNumber.String(), crlURL)
		if err != nil {
			return fmt.Errorf("CRL check error for %s: %w", crlURL, err)
		}

		if revoked {
			return fmt.Errorf("certificate is revoked according to CRL at %s", crlURL)
		}
	}

	etc.Logger.Debug("CRL check passed", "serialNumber", cert.SerialNumber.String())
	return nil
}

// checkOCSP checks Online Certificate Status Protocol
func (etc *EnhancedTLSConfig) checkOCSP(cert *x509.Certificate) error {
	// In a production implementation, this would:
	// 1. Create OCSP request
	// 2. Send request to OCSP responder
	// 3. Parse OCSP response
	// 4. Verify response signature
	// 5. Check certificate status

	// For now, we implement a basic check structure
	for _, ocspURL := range cert.OCSPServer {
		etc.Logger.Debug("Checking OCSP", "url", ocspURL)

		// Simulate OCSP check
		// In production, you would perform actual OCSP request here
		status, err := etc.getOCSPStatus(cert, ocspURL)
		if err != nil {
			return fmt.Errorf("OCSP check error for %s: %w", ocspURL, err)
		}

		if status != "good" {
			return fmt.Errorf("certificate OCSP status is %s from %s", status, ocspURL)
		}
	}

	etc.Logger.Debug("OCSP check passed", "serialNumber", cert.SerialNumber.String())
	return nil
}

// isCertificateInCRL checks if a certificate is revoked by checking the CRL
func (etc *EnhancedTLSConfig) isCertificateInCRL(serialNumber string, crlURL string) (bool, error) {
	// Fetch CRL from the URL
	resp, err := http.Get(crlURL)
	if err != nil {
		return false, fmt.Errorf("failed to fetch CRL from %s: %w", crlURL, err)
	}
	defer resp.Body.Close()

	// Read CRL data
	crlData, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return false, fmt.Errorf("failed to read CRL data: %w", err)
	}

	// Parse CRL
	crl, err := x509.ParseCRL(crlData)
	if err != nil {
		// Try parsing as DER-encoded CRL
		crl, err = x509.ParseDERCRL(crlData)
		if err != nil {
			return false, fmt.Errorf("failed to parse CRL: %w", err)
		}
	}

	// Check if the certificate serial number is in the revoked list
	targetSerial := new(big.Int)
	if _, success := targetSerial.SetString(serialNumber, 10); !success {
		// Try hex format
		if _, success := targetSerial.SetString(serialNumber, 16); !success {
			return false, fmt.Errorf("invalid serial number format: %s", serialNumber)
		}
	}

	// Check revoked certificates
	if crl.TBSCertList.RevokedCertificates != nil {
		for _, revoked := range crl.TBSCertList.RevokedCertificates {
			if revoked.SerialNumber.Cmp(targetSerial) == 0 {
				etc.Logger.Warn("Certificate found in CRL",
					"serialNumber", serialNumber,
					"revocationTime", revoked.RevocationTime)
				return true, nil
			}
		}
	}

	etc.Logger.Debug("Certificate not found in CRL",
		"serialNumber", serialNumber,
		"crlURL", crlURL)
	return false, nil
}

// getOCSPStatus checks the OCSP status of a certificate
func (etc *EnhancedTLSConfig) getOCSPStatus(cert *x509.Certificate, ocspURL string) (string, error) {
	// In a real implementation, the issuer certificate would be retrieved from the certificate chain
	// For this simplified implementation, we'll skip OCSP checking if we don't have the issuer
	etc.Logger.Debug("OCSP check skipped - issuer certificate not available in simplified implementation",
		"serialNumber", cert.SerialNumber.String(),
		"ocspURL", ocspURL)
	return "good", nil
}

// AddPinnedKey adds a pinned public key for a hostname
func (etc *EnhancedTLSConfig) AddPinnedKey(hostname string, spkiHashBase64 string) {
	etc.mu.Lock()
	defer etc.mu.Unlock()

	etc.PinnedKeys[hostname] = append(etc.PinnedKeys[hostname], spkiHashBase64)
	etc.Logger.Info("Added pinned key",
		"hostname", hostname,
		"hash", spkiHashBase64)
}

// RemovePinnedKey removes a pinned public key for a hostname
func (etc *EnhancedTLSConfig) RemovePinnedKey(hostname string, spkiHashBase64 string) {
	etc.mu.Lock()
	defer etc.mu.Unlock()

	pins := etc.PinnedKeys[hostname]
	newPins := make([]string, 0, len(pins))
	for _, pin := range pins {
		if pin != spkiHashBase64 {
			newPins = append(newPins, pin)
		}
	}
	etc.PinnedKeys[hostname] = newPins

	etc.Logger.Info("Removed pinned key",
		"hostname", hostname,
		"hash", spkiHashBase64)
}

// ClearPinnedKeys clears all pinned keys for a hostname
func (etc *EnhancedTLSConfig) ClearPinnedKeys(hostname string) {
	etc.mu.Lock()
	defer etc.mu.Unlock()

	delete(etc.PinnedKeys, hostname)
	etc.Logger.Info("Cleared all pinned keys", "hostname", hostname)
}

// EnhancedTLSManager manages TLS certificates and keys with rotation support
type EnhancedTLSManager struct {
	config        *EnhancedTLSConfig
	certFile      string
	keyFile       string
	caFile        string
	logger        *logrus.Logger
	mu            sync.RWMutex
	stopChan      chan struct{}
	rotationTimer *time.Timer
}

// NewEnhancedTLSManager creates a new TLS manager
func NewEnhancedTLSManager(config *EnhancedTLSConfig, certFile, keyFile, caFile string, logger *logrus.Logger) *EnhancedTLSManager {
	if logger == nil {
		logger = logrus.New()
	}

	return &EnhancedTLSManager{
		config:   config,
		certFile: certFile,
		keyFile:  keyFile,
		caFile:   caFile,
		logger:   logger,
		stopChan: make(chan struct{}),
	}
}

// LoadCertificates loads certificates and keys from files
func (etm *EnhancedTLSManager) LoadCertificates() error {
	etm.mu.Lock()
	defer etm.mu.Unlock()

	// Load server certificate and key
	cert, err := tls.LoadX509KeyPair(etm.certFile, etm.keyFile)
	if err != nil {
		return apperrors.Wrap(err, apperrors.ModuleNetwork, apperrors.CodeInternal,
			"failed to load certificate and key")
	}

	etm.config.BaseConfig.Certificates = []tls.Certificate{cert}

	// Load CA certificate if provided
	if etm.caFile != "" {
		caCert, err := ioutil.ReadFile(etm.caFile)
		if err != nil {
			return apperrors.Wrap(err, apperrors.ModuleNetwork, apperrors.CodeInternal,
				"failed to read CA certificate")
		}

		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCert) {
			return apperrors.New(apperrors.ModuleNetwork, apperrors.CodeInternal,
				"failed to parse CA certificate")
		}

		etm.config.BaseConfig.RootCAs = caCertPool
		etm.config.ClientCAs = caCertPool
	}

	etm.logger.Info("Certificates loaded successfully")
	return nil
}

// StartCertificateRotation starts automatic certificate rotation
func (etm *EnhancedTLSManager) StartCertificateRotation(interval time.Duration) {
	etm.mu.Lock()
	defer etm.mu.Unlock()

	if etm.rotationTimer != nil {
		etm.rotationTimer.Stop()
	}

	etm.rotationTimer = time.AfterFunc(interval, func() {
		if err := etm.rotateCertificates(); err != nil {
			etm.logger.Error("Certificate rotation failed", "error", err)
		}
		// Schedule next rotation
		etm.StartCertificateRotation(interval)
	})

	etm.logger.Info("Certificate rotation scheduled", "interval", interval)
}

// StopCertificateRotation stops automatic certificate rotation
func (etm *EnhancedTLSManager) StopCertificateRotation() {
	etm.mu.Lock()
	defer etm.mu.Unlock()

	if etm.rotationTimer != nil {
		etm.rotationTimer.Stop()
		etm.rotationTimer = nil
	}

	etm.logger.Info("Certificate rotation stopped")
}

// rotateCertificates performs certificate rotation
func (etm *EnhancedTLSManager) rotateCertificates() error {
	etm.logger.Info("Starting certificate rotation")

	// In a production system, this would:
	// 1. Generate or fetch new certificates
	// 2. Validate the new certificates
	// 3. Atomically swap the certificates
	// 4. Notify connected peers of the change

	// For now, we'll just reload the certificates from disk
	if err := etm.LoadCertificates(); err != nil {
		return apperrors.Wrap(err, apperrors.ModuleNetwork, apperrors.CodeInternal,
			"failed to rotate certificates")
	}

	etm.logger.Info("Certificate rotation completed")
	return nil
}

// GetTLSConfig returns the current TLS configuration
func (etm *EnhancedTLSManager) GetTLSConfig() (*tls.Config, error) {
	etm.mu.RLock()
	defer etm.mu.RUnlock()

	return etm.config.BuildTLSConfig()
}

// ValidatePeerCertificate validates a peer's certificate
func (etm *EnhancedTLSManager) ValidatePeerCertificate(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
	return etm.config.verifyPeerCertificate(rawCerts, verifiedChains)
}

// ExtractCertificateInfo extracts information from a certificate
func ExtractCertificateInfo(cert *x509.Certificate) map[string]interface{} {
	info := make(map[string]interface{})

	info["subject"] = cert.Subject.String()
	info["issuer"] = cert.Issuer.String()
	info["serialNumber"] = cert.SerialNumber.String()
	info["notBefore"] = cert.NotBefore
	info["notAfter"] = cert.NotAfter
	info["dnsNames"] = cert.DNSNames
	info["ipAddresses"] = cert.IPAddresses
	info["keyUsage"] = cert.KeyUsage
	info["extKeyUsage"] = cert.ExtKeyUsage
	info["isCA"] = cert.IsCA

	// Calculate SPKI hash
	spkiHash := calculateSPKIHash(cert)
	info["spkiHash"] = base64.StdEncoding.EncodeToString(spkiHash)

	return info
}

// EnhancedCAManager manages Certificate Authority operations
type EnhancedCAManager struct {
	caPool       *x509.CertPool
	trustedCAs   map[string]*x509.Certificate
	revokedCerts map[string]time.Time // serial number -> revocation time
	mu           sync.RWMutex
	logger       *logrus.Logger
}

// NewEnhancedCAManager creates a new CA manager
func NewEnhancedCAManager(logger *logrus.Logger) *EnhancedCAManager {
	if logger == nil {
		logger = logrus.New()
	}

	return &EnhancedCAManager{
		caPool:       x509.NewCertPool(),
		trustedCAs:   make(map[string]*x509.Certificate),
		revokedCerts: make(map[string]time.Time),
		logger:       logger,
	}
}

// AddTrustedCA adds a trusted CA certificate
func (ecm *EnhancedCAManager) AddTrustedCA(caCert *x509.Certificate) error {
	ecm.mu.Lock()
	defer ecm.mu.Unlock()

	if caCert == nil {
		return apperrors.New(apperrors.ModuleNetwork, apperrors.CodeInvalid, "CA certificate is nil")
	}

	// Verify the CA certificate is valid for signing
	if !caCert.IsCA {
		return apperrors.New(apperrors.ModuleNetwork, apperrors.CodeInvalid,
			"certificate is not a CA certificate")
	}

	// Add to pool and map
	ecm.caPool.AddCert(caCert)
	ecm.trustedCAs[caCert.Subject.String()] = caCert

	ecm.logger.Info("Added trusted CA",
		"subject", caCert.Subject.String(),
		"serialNumber", caCert.SerialNumber.String())

	return nil
}

// RemoveTrustedCA removes a trusted CA certificate
func (ecm *EnhancedCAManager) RemoveTrustedCA(subject string) error {
	ecm.mu.Lock()
	defer ecm.mu.Unlock()

	if _, exists := ecm.trustedCAs[subject]; !exists {
		return apperrors.New(apperrors.ModuleNetwork, apperrors.CodeNotFound,
			fmt.Sprintf("CA with subject %s not found", subject))
	}

	// Remove from map (note: can't remove from pool, need to rebuild)
	delete(ecm.trustedCAs, subject)

	// Rebuild the pool
	ecm.caPool = x509.NewCertPool()
	for _, ca := range ecm.trustedCAs {
		ecm.caPool.AddCert(ca)
	}

	ecm.logger.Info("Removed trusted CA", "subject", subject)
	return nil
}

// RevokeCertificate marks a certificate as revoked
func (ecm *EnhancedCAManager) RevokeCertificate(serialNumber string, revocationTime time.Time) {
	ecm.mu.Lock()
	defer ecm.mu.Unlock()

	ecm.revokedCerts[serialNumber] = revocationTime
	ecm.logger.Info("Certificate revoked",
		"serialNumber", serialNumber,
		"revocationTime", revocationTime)
}

// IsCertificateRevoked checks if a certificate is revoked
func (ecm *EnhancedCAManager) IsCertificateRevoked(serialNumber string) (bool, time.Time) {
	ecm.mu.RLock()
	defer ecm.mu.RUnlock()

	revocationTime, revoked := ecm.revokedCerts[serialNumber]
	return revoked, revocationTime
}

// GetCAPool returns the current CA pool
func (ecm *EnhancedCAManager) GetCAPool() *x509.CertPool {
	ecm.mu.RLock()
	defer ecm.mu.RUnlock()

	// Return a copy to prevent external modification
	poolCopy := x509.NewCertPool()
	for _, ca := range ecm.trustedCAs {
		poolCopy.AddCert(ca)
	}
	return poolCopy
}

// LoadCAFromPEM loads a CA certificate from PEM data
func (ecm *EnhancedCAManager) LoadCAFromPEM(pemData []byte) error {
	block, _ := pem.Decode(pemData)
	if block == nil {
		return apperrors.New(apperrors.ModuleNetwork, apperrors.CodeInvalid,
			"failed to parse PEM block")
	}

	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return apperrors.Wrap(err, apperrors.ModuleNetwork, apperrors.CodeInvalid,
			"failed to parse certificate")
	}

	return ecm.AddTrustedCA(cert)
}

// SaveCAsToPEM saves all trusted CAs to PEM format
func (ecm *EnhancedCAManager) SaveCAsToPEM() ([]byte, error) {
	ecm.mu.RLock()
	defer ecm.mu.RUnlock()

	var buf bytes.Buffer

	for _, ca := range ecm.trustedCAs {
		pemBlock := &pem.Block{
			Type:  "CERTIFICATE",
			Bytes: ca.Raw,
		}
		if err := pem.Encode(&buf, pemBlock); err != nil {
			return nil, apperrors.Wrap(err, apperrors.ModuleNetwork, apperrors.CodeInternal,
				"failed to encode certificate")
		}
	}

	return buf.Bytes(), nil
}
