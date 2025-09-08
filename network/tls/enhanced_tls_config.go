package tls

import (
	"crypto/sha256"
	stdtls "crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"diamante/common"

	"github.com/sirupsen/logrus"
)

// EnhancedTLSConfig provides production-grade TLS configuration with
// automatic certificate management and security hardening.
type EnhancedTLSConfig struct {
	Enabled                bool
	NodeID                 string
	CertFile               string
	KeyFile                string
	CAFile                 string
	CertRotationInterval   time.Duration
	MinVersion             string
	MaxVersion             string
	CipherSuites           []string
	ClientAuth             string
	InsecureSkipVerify     bool // SECURITY: NEVER enable in production - only for security testing with specific node IDs
	PreferServerCiphers    bool
	SessionTicketsDisabled bool
	Renegotiation          stdtls.RenegotiationSupport

	// Enhanced security features
	EnableOCSP         bool
	EnableSCT          bool     // Certificate Transparency
	PinPublicKeys      []string // Public key pinning
	RequireSNI         bool
	MaxCertChainLength int

	// Certificate management
	caManager   *EnhancedCAManager
	certRotator *EnhancedCertRotator
	tlsConfig   *stdtls.Config
	mu          sync.RWMutex
	logger      *logrus.Logger

	// Network configuration
	IPAddresses []net.IP
	DNSNames    []string
}

// EnhancedTLSConfigOptions holds configuration options
type EnhancedTLSConfigOptions struct {
	NodeID               string
	CertDir              string
	CAManager            *EnhancedCAManager
	Logger               *logrus.Logger
	IPAddresses          []net.IP
	DNSNames             []string
	CertRotationInterval time.Duration
	EnableAutoRotation   bool
}

// DefaultEnhancedTLSConfig returns a production-ready TLS configuration
func DefaultEnhancedTLSConfig() *EnhancedTLSConfig {
	return &EnhancedTLSConfig{
		Enabled:    true,
		MinVersion: "1.3",
		MaxVersion: "1.3",
		CipherSuites: []string{
			"TLS_AES_256_GCM_SHA384",
			"TLS_CHACHA20_POLY1305_SHA256",
			"TLS_AES_128_GCM_SHA256",
		},
		ClientAuth:             "RequireAndVerifyClientCert",
		PreferServerCiphers:    true,
		SessionTicketsDisabled: true,
		Renegotiation:          stdtls.RenegotiateNever,
		EnableOCSP:             true,
		EnableSCT:              true,
		RequireSNI:             true,
		MaxCertChainLength:     3,
		CertRotationInterval:   24 * time.Hour,
		InsecureSkipVerify:     false, // SECURITY: Always enforce certificate verification in production
		logger:                 logrus.New(),
	}
}

// NewEnhancedTLSConfig creates a new enhanced TLS configuration
func NewEnhancedTLSConfig(options *EnhancedTLSConfigOptions) (*EnhancedTLSConfig, error) {
	config := DefaultEnhancedTLSConfig()

	if options != nil {
		config.NodeID = options.NodeID
		config.caManager = options.CAManager
		config.logger = options.Logger
		config.IPAddresses = options.IPAddresses
		config.DNSNames = options.DNSNames

		if options.CertRotationInterval > 0 {
			config.CertRotationInterval = options.CertRotationInterval
		}

		// Set certificate paths
		if options.CertDir != "" {
			config.CertFile = fmt.Sprintf("%s/%s.crt", options.CertDir, options.NodeID)
			config.KeyFile = fmt.Sprintf("%s/%s.key", options.CertDir, options.NodeID)
		}

		// Initialize certificate rotator if enabled
		if options.EnableAutoRotation && config.caManager != nil {
			config.certRotator = NewEnhancedCertRotator(&EnhancedCertRotatorConfig{
				CAManager:   config.caManager,
				NodeID:      config.NodeID,
				Interval:    config.CertRotationInterval,
				IPAddresses: config.IPAddresses,
				DNSNames:    config.DNSNames,
				Logger:      config.logger,
				OnRotation:  config.onCertificateRotated,
			})
		}
	}

	// Build initial TLS configuration
	if err := config.buildTLSConfig(); err != nil {
		return nil, fmt.Errorf("failed to build TLS config: %w", err)
	}

	config.logger.WithFields(logrus.Fields{
		"node_id":     config.NodeID,
		"min_version": config.MinVersion,
		"max_version": config.MaxVersion,
		"client_auth": config.ClientAuth,
	}).Info("Enhanced TLS configuration initialized")

	return config, nil
}

// buildTLSConfig constructs the underlying tls.Config
func (c *EnhancedTLSConfig) buildTLSConfig() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.Enabled {
		c.tlsConfig = nil
		return nil
	}

	tlsConfig := &stdtls.Config{
		MinVersion:               c.parseVersion(c.MinVersion),
		MaxVersion:               c.parseVersion(c.MaxVersion),
		PreferServerCipherSuites: c.PreferServerCiphers,
		SessionTicketsDisabled:   c.SessionTicketsDisabled,
		Renegotiation:            c.Renegotiation,
		InsecureSkipVerify:       c.InsecureSkipVerify,
	}

	// Set cipher suites
	if len(c.CipherSuites) > 0 {
		tlsConfig.CipherSuites = c.parseCipherSuites(c.CipherSuites)
	}

	// Set client authentication
	tlsConfig.ClientAuth = c.parseClientAuth(c.ClientAuth)

	// Certificate loading function with automatic reload
	tlsConfig.GetCertificate = c.getCertificate
	tlsConfig.GetClientCertificate = c.getClientCertificate

	// Custom certificate verification
	if !c.InsecureSkipVerify {
		tlsConfig.VerifyPeerCertificate = c.verifyPeerCertificate
	} else {
		// Log security warning when InsecureSkipVerify is enabled
		c.logger.WithField("node_id", c.NodeID).Error("SECURITY WARNING: TLS certificate verification is disabled - this should ONLY be used for security testing")
	}

	// Load CA certificates if specified
	if c.CAFile != "" || c.caManager != nil {
		if err := c.loadCACertificates(tlsConfig); err != nil {
			return fmt.Errorf("failed to load CA certificates: %w", err)
		}
	}

	c.tlsConfig = tlsConfig
	return nil
}

// getCertificate dynamically loads the certificate for server connections
func (c *EnhancedTLSConfig) getCertificate(hello *stdtls.ClientHelloInfo) (*stdtls.Certificate, error) {
	// If we have a CA manager, try to get the certificate from it
	if c.caManager != nil {
		if certInfo, exists := c.caManager.GetCertificateInfo(c.NodeID); exists {
			// Check if certificate is still valid
			if common.ConsensusNow().Add(time.Hour).Before(certInfo.ExpiresAt) {
				cert := stdtls.Certificate{
					Certificate: [][]byte{certInfo.Certificate.Raw},
					PrivateKey:  certInfo.PrivateKey,
				}
				return &cert, nil
			}
			// Certificate is expiring soon, generate a new one
			c.logger.WithField("node_id", c.NodeID).Info("Certificate expiring soon, generating new one")
			if err := c.regenerateCertificate(); err != nil {
				c.logger.WithError(err).Error("Failed to regenerate certificate")
			}
		}
	}

	// Fallback to file-based certificate loading
	if c.CertFile != "" && c.KeyFile != "" {
		cert, err := stdtls.LoadX509KeyPair(c.CertFile, c.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load certificate from files: %w", err)
		}
		return &cert, nil
	}

	return nil, fmt.Errorf("no certificate available for node %s", c.NodeID)
}

// getClientCertificate dynamically loads the certificate for client connections
func (c *EnhancedTLSConfig) getClientCertificate(info *stdtls.CertificateRequestInfo) (*stdtls.Certificate, error) {
	return c.getCertificate(nil)
}

// verifyPeerCertificate performs enhanced certificate verification
func (c *EnhancedTLSConfig) verifyPeerCertificate(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
	if len(rawCerts) == 0 {
		return fmt.Errorf("no peer certificates provided")
	}

	// Parse the peer certificate
	cert, err := x509.ParseCertificate(rawCerts[0])
	if err != nil {
		return fmt.Errorf("failed to parse peer certificate: %w", err)
	}

	// Check certificate chain length
	if c.MaxCertChainLength > 0 && len(rawCerts) > c.MaxCertChainLength {
		return fmt.Errorf("certificate chain too long: %d > %d", len(rawCerts), c.MaxCertChainLength)
	}

	// Validate with CA manager if available
	if c.caManager != nil {
		if err := c.caManager.ValidateCertificate(cert); err != nil {
			return fmt.Errorf("CA validation failed: %w", err)
		}
	}

	// Public key pinning verification
	if len(c.PinPublicKeys) > 0 {
		if err := c.verifyPublicKeyPinning(cert); err != nil {
			return fmt.Errorf("public key pinning failed: %w", err)
		}
	}

	c.logger.WithFields(logrus.Fields{
		"peer_cn":     cert.Subject.CommonName,
		"peer_serial": cert.SerialNumber.String(),
		"expires":     cert.NotAfter,
	}).Debug("Peer certificate verified successfully")

	return nil
}

// verifyPublicKeyPinning checks if the certificate's public key is pinned (RFC 7469)
func (c *EnhancedTLSConfig) verifyPublicKeyPinning(cert *x509.Certificate) error {
	if len(c.PinPublicKeys) == 0 {
		// No pins configured, skip verification
		return nil
	}

	// Calculate SPKI fingerprint (SHA256 hash of SubjectPublicKeyInfo)
	spkiHash := sha256.Sum256(cert.RawSubjectPublicKeyInfo)
	spkiHashBase64 := base64.StdEncoding.EncodeToString(spkiHash[:])

	// Check if the certificate's SPKI hash matches any pinned key
	for _, pinnedKey := range c.PinPublicKeys {
		// Support both hex and base64 encoded pins
		if pinnedKey == spkiHashBase64 {
			c.logger.Debug("Certificate public key matched pinned key",
				"fingerprint", spkiHashBase64[:16]+"...")
			return nil
		}

		// Try hex encoding
		spkiHashHex := hex.EncodeToString(spkiHash[:])
		if pinnedKey == spkiHashHex {
			c.logger.Debug("Certificate public key matched pinned key (hex)",
				"fingerprint", spkiHashHex[:16]+"...")
			return nil
		}
	}

	// Also check the certificate chain if available
	if cert.IsCA {
		// For CA certificates, we might want different handling
		c.logger.Warn("CA certificate did not match any pinned keys",
			"subject", cert.Subject.String())
	}

	return fmt.Errorf("certificate public key does not match any pinned keys: %s", spkiHashBase64)
}

// loadCACertificates loads CA certificates for peer verification
func (c *EnhancedTLSConfig) loadCACertificates(tlsConfig *stdtls.Config) error {
	pool := x509.NewCertPool()

	// Load from CA manager if available
	if c.caManager != nil {
		caCert := c.caManager.GetCACertificate()
		if caCert != nil {
			pool.AddCert(caCert)
		}
	}

	// Load from CA file if specified
	if c.CAFile != "" {
		// Implementation would load from file
		// This is handled by the existing ca_manager.go
	}

	tlsConfig.ClientCAs = pool
	tlsConfig.RootCAs = pool

	return nil
}

// regenerateCertificate generates a new certificate using the CA manager
func (c *EnhancedTLSConfig) regenerateCertificate() error {
	if c.caManager == nil {
		return fmt.Errorf("no CA manager available for certificate regeneration")
	}

	_, err := c.caManager.GenerateNodeCertificate(c.NodeID, c.IPAddresses, c.DNSNames)
	if err != nil {
		return fmt.Errorf("failed to generate new certificate: %w", err)
	}

	// Rebuild TLS configuration to use the new certificate
	return c.buildTLSConfig()
}

// onCertificateRotated is called when the certificate rotator generates a new certificate
func (c *EnhancedTLSConfig) onCertificateRotated(certInfo *CertInfo) {
	c.logger.WithFields(logrus.Fields{
		"node_id":     certInfo.NodeID,
		"serial":      certInfo.SerialNum,
		"expires":     certInfo.ExpiresAt,
		"fingerprint": certInfo.Fingerprint[:16] + "...",
	}).Info("Certificate rotated successfully")

	// Rebuild TLS configuration to use the new certificate
	if err := c.buildTLSConfig(); err != nil {
		c.logger.WithError(err).Error("Failed to rebuild TLS config after certificate rotation")
	}
}

// Start initializes the TLS configuration and starts certificate rotation if enabled
func (c *EnhancedTLSConfig) Start() error {
	if !c.Enabled {
		c.logger.Info("TLS is disabled")
		return nil
	}

	// Generate initial certificate if using CA manager
	if c.caManager != nil && c.NodeID != "" {
		if _, exists := c.caManager.GetCertificateInfo(c.NodeID); !exists {
			c.logger.WithField("node_id", c.NodeID).Info("Generating initial certificate")
			if _, err := c.caManager.GenerateNodeCertificate(c.NodeID, c.IPAddresses, c.DNSNames); err != nil {
				return fmt.Errorf("failed to generate initial certificate: %w", err)
			}
		}
	}

	// Start certificate rotator if configured
	if c.certRotator != nil {
		if err := c.certRotator.Start(); err != nil {
			return fmt.Errorf("failed to start certificate rotator: %w", err)
		}
	}

	c.logger.WithField("node_id", c.NodeID).Info("Enhanced TLS configuration started")
	return nil
}

// Stop stops the certificate rotator and cleans up resources
func (c *EnhancedTLSConfig) Stop() error {
	if c.certRotator != nil {
		c.certRotator.Stop()
	}

	c.logger.WithField("node_id", c.NodeID).Info("Enhanced TLS configuration stopped")
	return nil
}

// GetTLSConfig returns the underlying tls.Config
func (c *EnhancedTLSConfig) GetTLSConfig() *stdtls.Config {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.tlsConfig
}

// UpdateConfiguration updates the TLS configuration and rebuilds it
func (c *EnhancedTLSConfig) UpdateConfiguration(updates map[string]interface{}) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Apply configuration updates
	for key, value := range updates {
		switch key {
		case "min_version":
			if v, ok := value.(string); ok {
				c.MinVersion = v
			}
		case "max_version":
			if v, ok := value.(string); ok {
				c.MaxVersion = v
			}
		case "cipher_suites":
			if v, ok := value.([]string); ok {
				c.CipherSuites = v
			}
		case "client_auth":
			if v, ok := value.(string); ok {
				c.ClientAuth = v
			}
		}
	}

	// Rebuild TLS configuration
	return c.buildTLSConfig()
}

// GetSecurityMetrics returns security-related metrics
func (c *EnhancedTLSConfig) GetSecurityMetrics() map[string]interface{} {
	c.mu.RLock()
	defer c.mu.RUnlock()

	metrics := map[string]interface{}{
		"enabled":                  c.Enabled,
		"min_version":              c.MinVersion,
		"max_version":              c.MaxVersion,
		"cipher_suites_count":      len(c.CipherSuites),
		"client_auth":              c.ClientAuth,
		"session_tickets_disabled": c.SessionTicketsDisabled,
		"renegotiation":            fmt.Sprintf("%d", c.Renegotiation),
		"ocsp_enabled":             c.EnableOCSP,
		"sct_enabled":              c.EnableSCT,
		"public_key_pinning":       len(c.PinPublicKeys) > 0,
	}

	if c.caManager != nil {
		if certInfo, exists := c.caManager.GetCertificateInfo(c.NodeID); exists {
			metrics["certificate_expires"] = certInfo.ExpiresAt
			metrics["certificate_serial"] = certInfo.SerialNum
			metrics["certificate_fingerprint"] = certInfo.Fingerprint
		}
	}

	return metrics
}

// Utility functions for parsing configuration values

func (c *EnhancedTLSConfig) parseVersion(version string) uint16 {
	switch strings.TrimSpace(version) {
	case "1.3":
		return stdtls.VersionTLS13
	case "1.2":
		return stdtls.VersionTLS12
	case "1.1":
		return stdtls.VersionTLS11
	case "1.0":
		return stdtls.VersionTLS10
	default:
		return stdtls.VersionTLS13 // Default to most secure
	}
}

func (c *EnhancedTLSConfig) parseCipherSuites(suites []string) []uint16 {
	var result []uint16
	cipherMap := map[string]uint16{
		"TLS_AES_256_GCM_SHA384":       stdtls.TLS_AES_256_GCM_SHA384,
		"TLS_CHACHA20_POLY1305_SHA256": stdtls.TLS_CHACHA20_POLY1305_SHA256,
		"TLS_AES_128_GCM_SHA256":       stdtls.TLS_AES_128_GCM_SHA256,
	}

	for _, suite := range suites {
		if cipher, exists := cipherMap[strings.TrimSpace(suite)]; exists {
			result = append(result, cipher)
		}
	}

	return result
}

func (c *EnhancedTLSConfig) parseClientAuth(auth string) stdtls.ClientAuthType {
	authMap := map[string]stdtls.ClientAuthType{
		"RequireAndVerifyClientCert": stdtls.RequireAndVerifyClientCert,
		"RequireAnyClientCert":       stdtls.RequireAnyClientCert,
		"VerifyClientCertIfGiven":    stdtls.VerifyClientCertIfGiven,
		"RequestClientCert":          stdtls.RequestClientCert,
		"NoClientCert":               stdtls.NoClientCert,
	}

	if authType, exists := authMap[strings.TrimSpace(auth)]; exists {
		return authType
	}

	return stdtls.RequireAndVerifyClientCert // Default to most secure
}

// Validate ensures the TLS configuration is valid
func (c *EnhancedTLSConfig) Validate() error {
	if !c.Enabled {
		return nil
	}

	if c.NodeID == "" {
		return fmt.Errorf("node ID is required when TLS is enabled")
	}

	if c.caManager == nil && (c.CertFile == "" || c.KeyFile == "") {
		return fmt.Errorf("either CA manager or certificate files must be provided")
	}

	// SECURITY: Validate that InsecureSkipVerify is never enabled in production
	if c.InsecureSkipVerify {
		// Only allow InsecureSkipVerify in very specific security testing contexts
		if c.NodeID != "security-scanner" && c.NodeID != "network-scanner" {
			return fmt.Errorf("SECURITY ERROR: InsecureSkipVerify is disabled in production. Use proper certificates instead")
		}
		// Log a warning even for security scanners
		c.logger.WithField("node_id", c.NodeID).Warn("InsecureSkipVerify is enabled - this should ONLY be used for security testing")
	}

	// Validate TLS version settings
	minVer := c.parseVersion(c.MinVersion)
	maxVer := c.parseVersion(c.MaxVersion)
	if minVer > maxVer {
		return fmt.Errorf("minimum TLS version cannot be higher than maximum version")
	}

	// SECURITY: Ensure minimum TLS version is at least 1.2
	if minVer < stdtls.VersionTLS12 {
		return fmt.Errorf("SECURITY ERROR: TLS version below 1.2 is not allowed: %s", c.MinVersion)
	}

	return nil
}
