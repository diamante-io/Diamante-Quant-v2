package network

import (
	stdtls "crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"fmt"

	"diamante/apperrors"

	"github.com/sirupsen/logrus"
)

// TLSPeer wraps Peer and uses a tls.Conn for communication with enhanced security.
type TLSPeer struct {
	*Peer
	RemoteID        string
	CertificateInfo map[string]interface{}
	tlsConn         *stdtls.Conn
	logger          *logrus.Logger
}

// NewTLSPeer constructs a TLS-enabled peer with certificate validation.
func NewTLSPeer(addr string, conn *stdtls.Conn, nm *NetworkManager) *TLSPeer {
	logger := logrus.New()

	// Perform TLS handshake with proper error handling
	if err := conn.Handshake(); err != nil {
		logger.WithError(err).Error("TLS handshake failed for peer", "addr", addr)
		// Close the connection on handshake failure
		if closeErr := conn.Close(); closeErr != nil {
			logger.WithError(closeErr).Warn("Failed to close connection after handshake failure")
		}
		return nil
	}

	// Get connection state and validate peer certificate
	state := conn.ConnectionState()
	if !state.HandshakeComplete {
		logger.Error("TLS handshake incomplete for peer", "addr", addr)
		if closeErr := conn.Close(); closeErr != nil {
			logger.WithError(closeErr).Warn("Failed to close connection after incomplete handshake")
		}
		return nil
	}

	// Extract and validate peer certificate information
	var remoteID string
	var certInfo map[string]interface{}

	if len(state.PeerCertificates) > 0 {
		peerCert := state.PeerCertificates[0]

		// Extract certificate information
		certInfo = ExtractCertificateInfo(peerCert)

		// Set remote ID from certificate
		if peerCert.Subject.CommonName != "" {
			remoteID = peerCert.Subject.CommonName
		} else if len(peerCert.DNSNames) > 0 {
			remoteID = peerCert.DNSNames[0]
		} else {
			remoteID = fmt.Sprintf("CN=%s", peerCert.Subject)
		}

		// Log certificate details
		logger.Info("TLS peer connected",
			"addr", addr,
			"remoteID", remoteID,
			"subject", peerCert.Subject,
			"issuer", peerCert.Issuer,
			"serialNumber", peerCert.SerialNumber.String())
	} else {
		logger.Warn("No peer certificate provided", "addr", addr)
		remoteID = fmt.Sprintf("uncertified-%s", addr)
	}

	// Create the base peer
	p := NewPeer(addr, conn, nm)
	if p == nil {
		logger.Error("Failed to create base peer", "addr", addr)
		if closeErr := conn.Close(); closeErr != nil {
			logger.WithError(closeErr).Warn("Failed to close connection after peer creation failure")
		}
		return nil
	}

	return &TLSPeer{
		Peer:            p,
		RemoteID:        remoteID,
		CertificateInfo: certInfo,
		tlsConn:         conn,
		logger:          logger,
	}
}

// ValidatePeerCertificate validates the peer's certificate chain
func (tp *TLSPeer) ValidatePeerCertificate(tlsManager *EnhancedTLSManager) error {
	if tp.tlsConn == nil {
		return apperrors.New(apperrors.ModuleNetwork, apperrors.CodeInvalid, "TLS connection is nil")
	}

	state := tp.tlsConn.ConnectionState()
	if len(state.PeerCertificates) == 0 {
		return apperrors.New(apperrors.ModuleNetwork, apperrors.CodeInvalid, "no peer certificates available")
	}

	// Convert certificates to raw format
	rawCerts := make([][]byte, len(state.PeerCertificates))
	for i, cert := range state.PeerCertificates {
		rawCerts[i] = cert.Raw
	}

	// Validate using the TLS manager
	if tlsManager != nil {
		return tlsManager.ValidatePeerCertificate(rawCerts, state.VerifiedChains)
	}

	return nil
}

// GetCertificateHash returns the SPKI hash of the peer's certificate
func (tp *TLSPeer) GetCertificateHash() (string, error) {
	state := tp.tlsConn.ConnectionState()
	if len(state.PeerCertificates) == 0 {
		return "", apperrors.New(apperrors.ModuleNetwork, apperrors.CodeInvalid, "no peer certificate available")
	}

	// Calculate SPKI hash
	spkiHash := calculateSPKIHash(state.PeerCertificates[0])
	return base64.StdEncoding.EncodeToString(spkiHash), nil
}

// VerifyAgainstCA verifies the peer's certificate against a CA manager
func (tp *TLSPeer) VerifyAgainstCA(caManager *EnhancedCAManager) error {
	state := tp.tlsConn.ConnectionState()
	if len(state.PeerCertificates) == 0 {
		return apperrors.New(apperrors.ModuleNetwork, apperrors.CodeInvalid, "no peer certificate available")
	}

	peerCert := state.PeerCertificates[0]

	// Check if certificate is revoked
	if revoked, revocationTime := caManager.IsCertificateRevoked(peerCert.SerialNumber.String()); revoked {
		return apperrors.New(apperrors.ModuleNetwork, apperrors.CodeInvalid,
			fmt.Sprintf("certificate is revoked (revoked at %v)", revocationTime))
	}

	// Verify certificate chain against CA pool
	opts := x509.VerifyOptions{
		Roots:         caManager.GetCAPool(),
		Intermediates: x509.NewCertPool(),
		CurrentTime:   peerCert.NotBefore.Add(1), // Ensure we're within validity period
	}

	// Add intermediate certificates if available
	for i := 1; i < len(state.PeerCertificates); i++ {
		opts.Intermediates.AddCert(state.PeerCertificates[i])
	}

	// Verify the certificate chain
	chains, err := peerCert.Verify(opts)
	if err != nil {
		return apperrors.Wrap(err, apperrors.ModuleNetwork, apperrors.CodeInvalid,
			"certificate chain verification failed")
	}

	tp.logger.Debug("Certificate chain verified",
		"peer", tp.RemoteID,
		"chains", len(chains))

	return nil
}

// RenegotiateTLS triggers a TLS renegotiation
func (tp *TLSPeer) RenegotiateTLS() error {
	if tp.tlsConn == nil {
		return apperrors.New(apperrors.ModuleNetwork, apperrors.CodeInvalid, "TLS connection is nil")
	}

	tp.logger.Info("Initiating TLS renegotiation", "peer", tp.RemoteID)

	// Perform renegotiation
	if err := tp.tlsConn.Handshake(); err != nil {
		return apperrors.Wrap(err, apperrors.ModuleNetwork, apperrors.CodeInternal,
			"TLS renegotiation failed")
	}

	// Update certificate information
	state := tp.tlsConn.ConnectionState()
	if len(state.PeerCertificates) > 0 {
		tp.CertificateInfo = ExtractCertificateInfo(state.PeerCertificates[0])
	}

	tp.logger.Info("TLS renegotiation completed", "peer", tp.RemoteID)
	return nil
}

// GetTLSVersion returns the negotiated TLS version
func (tp *TLSPeer) GetTLSVersion() string {
	if tp.tlsConn == nil {
		return "unknown"
	}

	state := tp.tlsConn.ConnectionState()
	switch state.Version {
	case stdtls.VersionTLS10:
		return "TLS 1.0"
	case stdtls.VersionTLS11:
		return "TLS 1.1"
	case stdtls.VersionTLS12:
		return "TLS 1.2"
	case stdtls.VersionTLS13:
		return "TLS 1.3"
	default:
		return fmt.Sprintf("0x%04x", state.Version)
	}
}

// GetCipherSuite returns the negotiated cipher suite
func (tp *TLSPeer) GetCipherSuite() string {
	if tp.tlsConn == nil {
		return "unknown"
	}

	state := tp.tlsConn.ConnectionState()
	return stdtls.CipherSuiteName(state.CipherSuite)
}

// GetSecurityInfo returns detailed security information about the connection
func (tp *TLSPeer) GetSecurityInfo() map[string]interface{} {
	info := make(map[string]interface{})

	if tp.tlsConn == nil {
		info["status"] = "no TLS connection"
		return info
	}

	state := tp.tlsConn.ConnectionState()
	info["handshakeComplete"] = state.HandshakeComplete
	info["version"] = tp.GetTLSVersion()
	info["cipherSuite"] = tp.GetCipherSuite()
	info["negotiatedProtocol"] = state.NegotiatedProtocol
	info["serverName"] = state.ServerName
	info["peerCertificates"] = len(state.PeerCertificates)
	info["verifiedChains"] = len(state.VerifiedChains)

	if tp.CertificateInfo != nil {
		info["certificateInfo"] = tp.CertificateInfo
	}

	return info
}
