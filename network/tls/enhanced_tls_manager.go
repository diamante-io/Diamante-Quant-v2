package tls

import (
	"crypto/sha256"
	stdtls "crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"sync"
	"time"

	"diamante/common"

	"github.com/sirupsen/logrus"
)

// EnhancedTLSManager provides centralized management of TLS infrastructure
// including CA management, certificate rotation, and TLS configuration.
type EnhancedTLSManager struct {
	caManager *EnhancedCAManager
	tlsConfig *EnhancedTLSConfig
	nodeID    string
	logger    *logrus.Logger
	mu        sync.RWMutex
	isStarted bool

	// Network configuration
	ipAddresses []net.IP
	dnsNames    []string

	// Security metrics
	connectionCount    int64
	handshakeFailures  int64
	certificateErrors  int64
	lastHandshakeError error
}

// EnhancedTLSManagerConfig holds configuration for the TLS manager
type EnhancedTLSManagerConfig struct {
	NodeID               string
	CertDir              string
	Logger               *logrus.Logger
	IPAddresses          []net.IP
	DNSNames             []string
	CertRotationInterval time.Duration
	EnableAutoRotation   bool
	CAConfig             *CAConfig
	TLSEnabled           bool
}

// NewEnhancedTLSManager creates a new enhanced TLS manager
func NewEnhancedTLSManager(config *EnhancedTLSManagerConfig) (*EnhancedTLSManager, error) {
	if config == nil {
		return nil, fmt.Errorf("TLS manager configuration is required")
	}

	if config.Logger == nil {
		config.Logger = logrus.New()
	}

	if config.NodeID == "" {
		return nil, fmt.Errorf("node ID is required")
	}

	manager := &EnhancedTLSManager{
		nodeID:      config.NodeID,
		logger:      config.Logger,
		ipAddresses: config.IPAddresses,
		dnsNames:    config.DNSNames,
	}

	// Initialize CA manager if TLS is enabled
	if config.TLSEnabled {
		caConfig := config.CAConfig
		if caConfig == nil {
			caConfig = DefaultEnhancedCAConfig()
			caConfig.Logger = config.Logger
		}

		var err error
		manager.caManager, err = NewEnhancedCAManager(caConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize CA manager: %w", err)
		}

		// Initialize TLS configuration
		tlsOptions := &EnhancedTLSConfigOptions{
			NodeID:               config.NodeID,
			CertDir:              config.CertDir,
			CAManager:            manager.caManager,
			Logger:               config.Logger,
			IPAddresses:          config.IPAddresses,
			DNSNames:             config.DNSNames,
			CertRotationInterval: config.CertRotationInterval,
			EnableAutoRotation:   config.EnableAutoRotation,
		}

		manager.tlsConfig, err = NewEnhancedTLSConfig(tlsOptions)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize TLS config: %w", err)
		}
	}

	manager.logger.WithFields(logrus.Fields{
		"node_id":     config.NodeID,
		"tls_enabled": config.TLSEnabled,
		"cert_dir":    config.CertDir,
	}).Info("Enhanced TLS manager initialized")

	return manager, nil
}

// Start initializes and starts the TLS infrastructure
func (m *EnhancedTLSManager) Start() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.isStarted {
		return fmt.Errorf("TLS manager is already started")
	}

	// Start TLS configuration if enabled
	if m.tlsConfig != nil {
		if err := m.tlsConfig.Start(); err != nil {
			return fmt.Errorf("failed to start TLS configuration: %w", err)
		}
	}

	m.isStarted = true
	m.logger.WithField("node_id", m.nodeID).Info("Enhanced TLS manager started")

	return nil
}

// Stop stops the TLS infrastructure and cleans up resources
func (m *EnhancedTLSManager) Stop() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.isStarted {
		return nil
	}

	// Stop TLS configuration
	if m.tlsConfig != nil {
		if err := m.tlsConfig.Stop(); err != nil {
			m.logger.WithError(err).Error("Failed to stop TLS configuration")
		}
	}

	// Clean up expired certificates
	if m.caManager != nil {
		if err := m.caManager.CleanupExpiredCertificates(); err != nil {
			m.logger.WithError(err).Error("Failed to cleanup expired certificates")
		}
	}

	m.isStarted = false
	m.logger.WithField("node_id", m.nodeID).Info("Enhanced TLS manager stopped")

	return nil
}

// GetTLSConfig returns the TLS configuration for use with network connections
func (m *EnhancedTLSManager) GetTLSConfig() *stdtls.Config {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.tlsConfig == nil {
		return nil
	}

	return m.tlsConfig.GetTLSConfig()
}

// IsEnabled returns whether TLS is enabled
func (m *EnhancedTLSManager) IsEnabled() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.tlsConfig != nil && m.tlsConfig.Enabled
}

// GenerateNodeCertificate generates a new certificate for a specific node
func (m *EnhancedTLSManager) GenerateNodeCertificate(nodeID string, ipAddresses []net.IP, dnsNames []string) (*CertInfo, error) {
	if m.caManager == nil {
		return nil, fmt.Errorf("CA manager not available")
	}

	return m.caManager.GenerateNodeCertificate(nodeID, ipAddresses, dnsNames)
}

// RevokeCertificate revokes a certificate for a specific node
func (m *EnhancedTLSManager) RevokeCertificate(nodeID string, reason int) error {
	if m.caManager == nil {
		return fmt.Errorf("CA manager not available")
	}

	return m.caManager.RevokeCertificate(nodeID, reason)
}

// ValidatePeerCertificate validates a peer's certificate
func (m *EnhancedTLSManager) ValidatePeerCertificate(certDER []byte) error {
	if m.caManager == nil {
		return fmt.Errorf("CA manager not available")
	}

	if len(certDER) == 0 {
		return fmt.Errorf("empty certificate data")
	}

	// Parse the certificate from DER format
	cert, err := x509.ParseCertificate(certDER)
	if err != nil {
		return fmt.Errorf("failed to parse certificate: %w", err)
	}

	// Validate the certificate
	if err := m.caManager.ValidateCertificate(cert); err != nil {
		return fmt.Errorf("certificate validation failed: %w", err)
	}

	m.logger.WithFields(logrus.Fields{
		"subject":     cert.Subject.String(),
		"serial":      cert.SerialNumber.String(),
		"not_before":  cert.NotBefore,
		"not_after":   cert.NotAfter,
		"fingerprint": fmt.Sprintf("%x", sha256.Sum256(cert.Raw))[:16],
	}).Debug("Peer certificate validated successfully")

	return nil
}

// GetCertificateInfo returns certificate information for a node
func (m *EnhancedTLSManager) GetCertificateInfo(nodeID string) (*CertInfo, bool) {
	if m.caManager == nil {
		return nil, false
	}

	return m.caManager.GetCertificateInfo(nodeID)
}

// ListCertificates returns all managed certificates
func (m *EnhancedTLSManager) ListCertificates() map[string]*CertInfo {
	if m.caManager == nil {
		return make(map[string]*CertInfo)
	}

	return m.caManager.ListCertificates()
}

// GetCACertificate returns the CA certificate
func (m *EnhancedTLSManager) GetCACertificate() []byte {
	if m.caManager == nil {
		return nil
	}

	caCert := m.caManager.GetCACertificate()
	if caCert == nil {
		return nil
	}

	return caCert.Raw
}

// UpdateNetworkConfiguration updates the network configuration for certificate generation
func (m *EnhancedTLSManager) UpdateNetworkConfiguration(ipAddresses []net.IP, dnsNames []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.ipAddresses = ipAddresses
	m.dnsNames = dnsNames

	// Update TLS configuration if available
	if m.tlsConfig != nil {
		m.tlsConfig.IPAddresses = ipAddresses
		m.tlsConfig.DNSNames = dnsNames

		// Update certificate rotator configuration
		if m.tlsConfig.certRotator != nil {
			return m.tlsConfig.certRotator.UpdateConfiguration(0, ipAddresses, dnsNames)
		}
	}

	m.logger.WithFields(logrus.Fields{
		"node_id":   m.nodeID,
		"ip_count":  len(ipAddresses),
		"dns_count": len(dnsNames),
	}).Info("Network configuration updated")

	return nil
}

// ForceRotateCertificate forces immediate certificate rotation
func (m *EnhancedTLSManager) ForceRotateCertificate() error {
	if m.tlsConfig == nil || m.tlsConfig.certRotator == nil {
		return fmt.Errorf("certificate rotator not available")
	}

	return m.tlsConfig.certRotator.ForceRotation()
}

// GetSecurityMetrics returns comprehensive security metrics
func (m *EnhancedTLSManager) GetSecurityMetrics() map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()

	metrics := map[string]interface{}{
		"node_id":            m.nodeID,
		"tls_enabled":        m.IsEnabled(),
		"is_started":         m.isStarted,
		"connection_count":   m.connectionCount,
		"handshake_failures": m.handshakeFailures,
		"certificate_errors": m.certificateErrors,
	}

	if m.lastHandshakeError != nil {
		metrics["last_handshake_error"] = m.lastHandshakeError.Error()
	}

	// Add TLS configuration metrics
	if m.tlsConfig != nil {
		tlsMetrics := m.tlsConfig.GetSecurityMetrics()
		for k, v := range tlsMetrics {
			metrics["tls_"+k] = v
		}
	}

	// Add certificate rotation metrics
	if m.tlsConfig != nil && m.tlsConfig.certRotator != nil {
		rotationMetrics := m.tlsConfig.certRotator.GetRotationMetrics()
		for k, v := range rotationMetrics {
			metrics["rotation_"+k] = v
		}
	}

	// Add CA manager metrics
	if m.caManager != nil {
		certificates := m.caManager.ListCertificates()
		metrics["managed_certificates"] = len(certificates)

		// Count certificates by status
		var validCerts, expiringSoon, expired int
		now := common.ConsensusNow()
		for _, cert := range certificates {
			if now.After(cert.ExpiresAt) {
				expired++
			} else if now.Add(7 * 24 * time.Hour).After(cert.ExpiresAt) {
				expiringSoon++
			} else {
				validCerts++
			}
		}

		metrics["valid_certificates"] = validCerts
		metrics["expiring_soon"] = expiringSoon
		metrics["expired_certificates"] = expired
	}

	return metrics
}

// RecordConnectionMetrics records metrics for TLS connections
func (m *EnhancedTLSManager) RecordConnectionMetrics(success bool, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.connectionCount++

	if !success {
		m.handshakeFailures++
		if err != nil {
			m.lastHandshakeError = err
			m.logger.WithError(err).Warn("TLS handshake failed")
		}
	}
}

// RecordCertificateError records certificate-related errors
func (m *EnhancedTLSManager) RecordCertificateError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.certificateErrors++
	m.logger.WithError(err).Error("Certificate error occurred")
}

// GetHealthStatus returns the health status of the TLS infrastructure
func (m *EnhancedTLSManager) GetHealthStatus() map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()

	status := map[string]interface{}{
		"healthy":    true,
		"node_id":    m.nodeID,
		"is_started": m.isStarted,
	}

	var issues []string

	// Check if TLS is properly configured
	if m.tlsConfig == nil {
		issues = append(issues, "TLS configuration not available")
	} else if !m.tlsConfig.Enabled {
		issues = append(issues, "TLS is disabled")
	}

	// Check CA manager health
	if m.caManager == nil {
		issues = append(issues, "CA manager not available")
	} else {
		// Check if CA certificate is expiring soon
		caCert := m.caManager.GetCACertificate()
		if caCert != nil && common.ConsensusNow().Add(30*24*time.Hour).After(caCert.NotAfter) {
			issues = append(issues, "CA certificate expiring within 30 days")
		}
	}

	// Check certificate rotator health
	if m.tlsConfig != nil && m.tlsConfig.certRotator != nil {
		if !m.tlsConfig.certRotator.IsRunning() {
			issues = append(issues, "Certificate rotator not running")
		}
	}

	// Check error rates
	if m.connectionCount > 0 {
		failureRate := float64(m.handshakeFailures) / float64(m.connectionCount)
		if failureRate > 0.1 { // More than 10% failure rate
			issues = append(issues, fmt.Sprintf("High handshake failure rate: %.2f%%", failureRate*100))
		}
	}

	if len(issues) > 0 {
		status["healthy"] = false
		status["issues"] = issues
	}

	return status
}

// Validate validates the TLS manager configuration
func (m *EnhancedTLSManager) Validate() error {
	if m.nodeID == "" {
		return fmt.Errorf("node ID is required")
	}

	if m.tlsConfig != nil {
		if err := m.tlsConfig.Validate(); err != nil {
			return fmt.Errorf("TLS configuration validation failed: %w", err)
		}
	}

	return nil
}

// GetNodeID returns the node ID
func (m *EnhancedTLSManager) GetNodeID() string {
	return m.nodeID
}

// IsStarted returns whether the TLS manager is started
func (m *EnhancedTLSManager) IsStarted() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.isStarted
}
