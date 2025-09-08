package scanners

import (
	"context"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"net/url"
	"strings"
	"sync"
	"time"

	"diamante/common"
	"diamante/security"
)

// AllowedNetworkTools defines the allowed network scanning tools
var AllowedNetworkTools = map[string]bool{
	"nmap":    true,
	"masscan": true,
	"netstat": true,
	"ss":      true,
}

// NetworkScanner implements secure network vulnerability scanning
type NetworkScanner struct {
	mu           sync.RWMutex
	findings     []security.Finding
	lastScanTime time.Time
	scannerID    string
	timeout      time.Duration
}

// NewNetworkScanner creates a new network scanner instance
func NewNetworkScanner() *NetworkScanner {
	return &NetworkScanner{
		scannerID: fmt.Sprintf("net-scanner-%d", common.ConsensusNow().Unix()),
		findings:  make([]security.Finding, 0),
		timeout:   30 * time.Second,
	}
}

// Scan performs a network security scan on the target
func (ns *NetworkScanner) Scan(ctx context.Context, target string) (*security.ScanResult, error) {
	if err := ns.ValidateTarget(target); err != nil {
		return nil, fmt.Errorf("target validation failed: %w", err)
	}

	startTime := common.ConsensusNow()
	result := &security.ScanResult{
		ID:          fmt.Sprintf("scan-%s-%d", ns.scannerID, common.ConsensusNow().Unix()),
		ScannerType: security.ScannerTypeNetwork,
		Target:      target,
		StartTime:   startTime,
		Success:     true,
		Findings:    []security.Finding{},
	}

	// Perform the network scan
	findings, err := ns.performScan(ctx, target)
	if err != nil {
		result.Success = false
		result.ErrorMessage = err.Error()
		result.EndTime = common.ConsensusNow()
		return result, fmt.Errorf("network scan failed: %w", err)
	}

	result.Findings = findings
	result.EndTime = common.ConsensusNow()
	result.Summary = ns.generateSummary(findings, result.EndTime.Sub(startTime))

	// Update internal state
	ns.mu.Lock()
	ns.findings = findings
	ns.lastScanTime = common.ConsensusNow()
	ns.mu.Unlock()

	return result, nil
}

// ValidateTarget validates if the target is safe to scan
func (ns *NetworkScanner) ValidateTarget(target string) error {
	if target == "" {
		return fmt.Errorf("target cannot be empty")
	}

	// Check if it's a valid IP address
	if ip := net.ParseIP(target); ip != nil {
		// Check for private/local addresses
		if ip.IsLoopback() || ip.IsPrivate() {
			return nil // Allow local/private scanning
		}
		// Check for special addresses
		if ip.IsUnspecified() || ip.IsMulticast() {
			return fmt.Errorf("invalid target IP: %s", target)
		}
		return nil
	}

	// Check if it's a valid hostname/URL
	if strings.Contains(target, "://") {
		parsedURL, err := url.Parse(target)
		if err != nil {
			return fmt.Errorf("invalid URL: %w", err)
		}
		target = parsedURL.Host
	}

	// Validate hostname
	if _, err := net.LookupHost(target); err != nil {
		return fmt.Errorf("cannot resolve target: %w", err)
	}

	// Check for suspicious patterns
	if strings.Contains(target, ";") || strings.Contains(target, "&") ||
		strings.Contains(target, "|") || strings.Contains(target, "$") {
		return fmt.Errorf("target contains suspicious characters")
	}

	return nil
}

// GetScannerType returns the scanner type
func (ns *NetworkScanner) GetScannerType() security.ScannerType {
	return security.ScannerTypeNetwork
}

// performScan executes the actual network scanning
func (ns *NetworkScanner) performScan(ctx context.Context, target string) ([]security.Finding, error) {
	findings := []security.Finding{}

	// Perform port scanning
	portFindings := ns.scanCommonPorts(ctx, target)
	findings = append(findings, portFindings...)

	// Check for common network vulnerabilities
	vulnFindings := ns.checkNetworkVulnerabilities(ctx, target)
	findings = append(findings, vulnFindings...)

	// Check SSL/TLS configuration if applicable
	if sslFindings := ns.checkSSLConfiguration(ctx, target); sslFindings != nil {
		findings = append(findings, sslFindings...)
	}

	return findings, nil
}

// scanCommonPorts scans common ports for security issues
func (ns *NetworkScanner) scanCommonPorts(ctx context.Context, target string) []security.Finding {
	findings := []security.Finding{}

	// Common ports to check
	commonPorts := map[int]string{
		21:    "FTP",
		22:    "SSH",
		23:    "Telnet",
		25:    "SMTP",
		80:    "HTTP",
		110:   "POP3",
		143:   "IMAP",
		443:   "HTTPS",
		445:   "SMB",
		1433:  "MSSQL",
		3306:  "MySQL",
		3389:  "RDP",
		5432:  "PostgreSQL",
		5900:  "VNC",
		6379:  "Redis",
		8080:  "HTTP-Alt",
		8443:  "HTTPS-Alt",
		27017: "MongoDB",
	}

	// Scan each port
	for port, service := range commonPorts {
		select {
		case <-ctx.Done():
			return findings
		default:
			if finding := ns.checkPort(target, port, service); finding != nil {
				findings = append(findings, *finding)
			}
		}
	}

	return findings
}

// checkPort checks a specific port for security issues
func (ns *NetworkScanner) checkPort(target string, port int, service string) *security.Finding {
	address := fmt.Sprintf("%s:%d", target, port)

	// Try to connect with timeout
	conn, err := net.DialTimeout("tcp", address, ns.timeout)
	if err != nil {
		return nil // Port is closed or filtered
	}
	defer conn.Close()

	// Port is open - check for security issues
	severity := security.SeverityLow
	description := fmt.Sprintf("Port %d (%s) is open", port, service)
	remediation := "Review if this port needs to be exposed"

	// Check for insecure services
	insecureServices := map[string]bool{
		"Telnet": true,
		"FTP":    true,
		"POP3":   true,
		"IMAP":   true,
		"SMTP":   true,
	}

	if insecureServices[service] {
		severity = security.SeverityHigh
		description = fmt.Sprintf("Insecure service %s running on port %d", service, port)
		remediation = fmt.Sprintf("Replace %s with a secure alternative (e.g., SSH instead of Telnet, SFTP instead of FTP)", service)
	}

	// Check for database ports
	databasePorts := map[int]bool{
		1433:  true, // MSSQL
		3306:  true, // MySQL
		5432:  true, // PostgreSQL
		6379:  true, // Redis
		27017: true, // MongoDB
	}

	if databasePorts[port] {
		severity = security.SeverityMedium
		description = fmt.Sprintf("Database service %s exposed on port %d", service, port)
		remediation = "Database ports should not be exposed to the internet. Use VPN or SSH tunneling for remote access"
	}

	return &security.Finding{
		ID:          fmt.Sprintf("net-port-%d-%d", port, common.ConsensusNow().Unix()),
		Title:       fmt.Sprintf("Open Port Detected: %d (%s)", port, service),
		Description: description,
		Severity:    severity,
		Category:    "network",
		Location:    address,
		Evidence:    fmt.Sprintf("Successfully connected to %s", address),
		Remediation: remediation,
		References:  []string{"https://www.iana.org/assignments/service-names-port-numbers/"},
		FoundAt:     common.ConsensusNow(),
		Verified:    true,
		Metadata: map[string]interface{}{
			"port":    port,
			"service": service,
			"target":  target,
		},
	}
}

// checkNetworkVulnerabilities checks for common network vulnerabilities
func (ns *NetworkScanner) checkNetworkVulnerabilities(ctx context.Context, target string) []security.Finding {
	findings := []security.Finding{}

	// Check for common vulnerabilities
	// 1. Check if HTTP is available without HTTPS
	if finding := ns.checkHTTPWithoutHTTPS(target); finding != nil {
		findings = append(findings, *finding)
	}

	// 2. Check for open management interfaces
	if finding := ns.checkManagementInterfaces(target); finding != nil {
		findings = append(findings, *finding)
	}

	return findings
}

// checkHTTPWithoutHTTPS checks if HTTP is available without HTTPS
func (ns *NetworkScanner) checkHTTPWithoutHTTPS(target string) *security.Finding {
	httpAddr := fmt.Sprintf("%s:80", target)
	httpsAddr := fmt.Sprintf("%s:443", target)

	httpConn, httpErr := net.DialTimeout("tcp", httpAddr, ns.timeout)
	if httpErr == nil {
		httpConn.Close()
	}

	httpsConn, httpsErr := net.DialTimeout("tcp", httpsAddr, ns.timeout)
	if httpsErr == nil {
		httpsConn.Close()
	}

	// If HTTP is open but HTTPS is not
	if httpErr == nil && httpsErr != nil {
		return &security.Finding{
			ID:          fmt.Sprintf("net-http-no-https-%d", common.ConsensusNow().Unix()),
			Title:       "HTTP Available Without HTTPS",
			Description: "The server accepts HTTP connections but HTTPS is not available",
			Severity:    security.SeverityMedium,
			Category:    "network",
			Location:    target,
			Evidence:    "Port 80 is open but port 443 is closed",
			Remediation: "Enable HTTPS and redirect all HTTP traffic to HTTPS",
			References:  []string{"https://developer.mozilla.org/en-US/docs/Web/Security/Mixed_content"},
			FoundAt:     common.ConsensusNow(),
			Verified:    true,
			Metadata: map[string]interface{}{
				"http_available":  true,
				"https_available": false,
			},
		}
	}

	return nil
}

// checkManagementInterfaces checks for exposed management interfaces
func (ns *NetworkScanner) checkManagementInterfaces(target string) *security.Finding {
	// Common management interface ports
	mgmtPorts := map[int]string{
		8080:  "HTTP Management",
		8443:  "HTTPS Management",
		9090:  "Web Console",
		10000: "Webmin",
	}

	for port, desc := range mgmtPorts {
		addr := fmt.Sprintf("%s:%d", target, port)
		conn, err := net.DialTimeout("tcp", addr, ns.timeout)
		if err == nil {
			conn.Close()
			return &security.Finding{
				ID:          fmt.Sprintf("net-mgmt-exposed-%d-%d", port, common.ConsensusNow().Unix()),
				Title:       "Management Interface Exposed",
				Description: fmt.Sprintf("%s interface exposed on port %d", desc, port),
				Severity:    security.SeverityHigh,
				Category:    "network",
				Location:    addr,
				Evidence:    fmt.Sprintf("Port %d is accessible from external network", port),
				Remediation: "Restrict management interface access to trusted networks only",
				References:  []string{"https://owasp.org/www-project-top-ten/"},
				FoundAt:     common.ConsensusNow(),
				Verified:    true,
				Metadata: map[string]interface{}{
					"port":      port,
					"interface": desc,
				},
			}
		}
	}

	return nil
}

// checkSSLConfiguration checks SSL/TLS configuration
func (ns *NetworkScanner) checkSSLConfiguration(ctx context.Context, target string) []security.Finding {
	findings := []security.Finding{}

	// Common HTTPS ports to check
	httpsPorts := []int{443, 8443}

	for _, port := range httpsPorts {
		httpsAddr := fmt.Sprintf("%s:%d", target, port)

		// First check if port is open
		conn, err := net.DialTimeout("tcp", httpsAddr, ns.timeout)
		if err != nil {
			continue // Port not available
		}
		conn.Close()

		// Perform TLS handshake analysis
		tlsFindings := ns.analyzeTLSConnection(httpsAddr, port)
		findings = append(findings, tlsFindings...)
	}

	return findings
}

// analyzeTLSConnection performs detailed TLS/SSL analysis
func (ns *NetworkScanner) analyzeTLSConnection(address string, port int) []security.Finding {
	findings := []security.Finding{}

	// Try to establish TLS connection
	// Note: InsecureSkipVerify is intentionally set to true here because
	// this is a security scanner that needs to analyze potentially
	// misconfigured certificates. This is NOT production connection code.
	tlsConfig := &tls.Config{
		InsecureSkipVerify: true, // Required for security scanning - DO NOT USE IN PRODUCTION CODE
		MinVersion:         0,    // Accept any version to test vulnerabilities
		MaxVersion:         0,    // Accept any version to test vulnerabilities
	}

	dialer := &net.Dialer{Timeout: ns.timeout}
	conn, err := tls.DialWithDialer(dialer, "tcp", address, tlsConfig)
	if err != nil {
		// TLS handshake failed
		finding := security.Finding{
			ID:          fmt.Sprintf("net-ssl-fail-%d-%d", port, common.ConsensusNow().Unix()),
			Title:       "TLS/SSL Handshake Failed",
			Description: fmt.Sprintf("Failed to establish TLS connection on port %d: %v", port, err),
			Severity:    security.SeverityCritical,
			Category:    "network-ssl",
			Location:    address,
			Evidence:    err.Error(),
			Remediation: "Verify SSL/TLS configuration and certificate validity",
			References:  []string{"https://tools.ietf.org/html/rfc5246"},
			FoundAt:     common.ConsensusNow(),
			Verified:    true,
			Metadata: map[string]interface{}{
				"port":  port,
				"error": err.Error(),
			},
		}
		findings = append(findings, finding)
		return findings
	}
	defer conn.Close()

	// Get connection state
	state := conn.ConnectionState()

	// Check TLS version
	if versionFinding := ns.checkTLSVersion(state.Version, address, port); versionFinding != nil {
		findings = append(findings, *versionFinding)
	}

	// Check cipher suite
	if cipherFinding := ns.checkCipherSuite(state.CipherSuite, address, port); cipherFinding != nil {
		findings = append(findings, *cipherFinding)
	}

	// Check certificates
	certFindings := ns.checkCertificates(state.PeerCertificates, address, port)
	findings = append(findings, certFindings...)

	return findings
}

// checkTLSVersion checks if the TLS version is secure
func (ns *NetworkScanner) checkTLSVersion(version uint16, address string, port int) *security.Finding {
	versionName := getTLSVersionName(version)

	// Check for outdated TLS versions
	if version < tls.VersionTLS12 {
		return &security.Finding{
			ID:          fmt.Sprintf("net-ssl-version-%d-%d", port, common.ConsensusNow().Unix()),
			Title:       "Outdated TLS Version",
			Description: fmt.Sprintf("Server supports outdated TLS version: %s", versionName),
			Severity:    security.SeverityHigh,
			Category:    "network-ssl",
			Location:    address,
			Evidence:    fmt.Sprintf("TLS version: %s (0x%04x)", versionName, version),
			Remediation: "Disable TLS versions below 1.2 and enable TLS 1.3",
			References: []string{
				"https://tools.ietf.org/html/rfc8446",
				"https://www.ssl.com/article/deprecating-early-tls/",
			},
			FoundAt:  common.ConsensusNow(),
			Verified: true,
			Metadata: map[string]interface{}{
				"port":         port,
				"tls_version":  versionName,
				"version_code": version,
			},
		}
	}

	return nil
}

// checkCipherSuite checks if the cipher suite is secure
func (ns *NetworkScanner) checkCipherSuite(suite uint16, address string, port int) *security.Finding {
	suiteName := tls.CipherSuiteName(suite)

	// List of weak cipher suites
	weakCiphers := map[string]bool{
		"TLS_RSA_WITH_RC4_128_SHA":            true,
		"TLS_RSA_WITH_3DES_EDE_CBC_SHA":       true,
		"TLS_RSA_WITH_AES_128_CBC_SHA256":     true,
		"TLS_ECDHE_ECDSA_WITH_RC4_128_SHA":    true,
		"TLS_ECDHE_RSA_WITH_RC4_128_SHA":      true,
		"TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA": true,
		"TLS_RSA_WITH_AES_128_CBC_SHA":        true,
		"TLS_RSA_WITH_AES_256_CBC_SHA":        true,
	}

	// Check for NULL cipher
	if strings.Contains(suiteName, "NULL") {
		return &security.Finding{
			ID:          fmt.Sprintf("net-ssl-null-cipher-%d-%d", port, common.ConsensusNow().Unix()),
			Title:       "NULL Cipher Suite Detected",
			Description: fmt.Sprintf("Server accepts NULL cipher suite: %s", suiteName),
			Severity:    security.SeverityCritical,
			Category:    "network-ssl",
			Location:    address,
			Evidence:    fmt.Sprintf("Cipher suite: %s", suiteName),
			Remediation: "Disable all NULL cipher suites immediately",
			References:  []string{"https://ciphersuite.info/cs/"},
			FoundAt:     common.ConsensusNow(),
			Verified:    true,
			Metadata: map[string]interface{}{
				"port":         port,
				"cipher_suite": suiteName,
				"suite_id":     suite,
			},
		}
	}

	// Check for weak ciphers
	if weakCiphers[suiteName] || strings.Contains(suiteName, "RC4") ||
		strings.Contains(suiteName, "DES") || strings.Contains(suiteName, "MD5") {
		return &security.Finding{
			ID:          fmt.Sprintf("net-ssl-weak-cipher-%d-%d", port, common.ConsensusNow().Unix()),
			Title:       "Weak Cipher Suite",
			Description: fmt.Sprintf("Server supports weak cipher suite: %s", suiteName),
			Severity:    security.SeverityHigh,
			Category:    "network-ssl",
			Location:    address,
			Evidence:    fmt.Sprintf("Cipher suite: %s", suiteName),
			Remediation: "Use only strong cipher suites with AEAD (e.g., AES-GCM, ChaCha20-Poly1305)",
			References: []string{
				"https://wiki.mozilla.org/Security/Server_Side_TLS",
				"https://ciphersuite.info/cs/",
			},
			FoundAt:  common.ConsensusNow(),
			Verified: true,
			Metadata: map[string]interface{}{
				"port":         port,
				"cipher_suite": suiteName,
				"suite_id":     suite,
			},
		}
	}

	return nil
}

// checkCertificates checks certificate validity and configuration
func (ns *NetworkScanner) checkCertificates(certs []*x509.Certificate, address string, port int) []security.Finding {
	findings := []security.Finding{}

	if len(certs) == 0 {
		finding := security.Finding{
			ID:          fmt.Sprintf("net-ssl-no-cert-%d-%d", port, common.ConsensusNow().Unix()),
			Title:       "No Certificate Presented",
			Description: "Server did not present any certificates",
			Severity:    security.SeverityCritical,
			Category:    "network-ssl",
			Location:    address,
			Evidence:    "No certificates in TLS handshake",
			Remediation: "Configure server with valid SSL/TLS certificate",
			References:  []string{"https://letsencrypt.org/"},
			FoundAt:     common.ConsensusNow(),
			Verified:    true,
			Metadata: map[string]interface{}{
				"port": port,
			},
		}
		findings = append(findings, finding)
		return findings
	}

	// Check the server certificate (first in chain)
	cert := certs[0]
	now := common.ConsensusNow()

	// Check certificate validity period
	if now.Before(cert.NotBefore) {
		finding := security.Finding{
			ID:          fmt.Sprintf("net-ssl-cert-notyet-%d-%d", port, common.ConsensusNow().Unix()),
			Title:       "Certificate Not Yet Valid",
			Description: fmt.Sprintf("Certificate is not valid until %s", cert.NotBefore.Format(time.RFC3339)),
			Severity:    security.SeverityCritical,
			Category:    "network-ssl",
			Location:    address,
			Evidence:    fmt.Sprintf("Current time: %s, Valid from: %s", now.Format(time.RFC3339), cert.NotBefore.Format(time.RFC3339)),
			Remediation: "Replace with a certificate that is currently valid",
			References:  []string{"https://tools.ietf.org/html/rfc5280"},
			FoundAt:     common.ConsensusNow(),
			Verified:    true,
			Metadata: map[string]interface{}{
				"port":       port,
				"not_before": cert.NotBefore,
				"subject":    cert.Subject.String(),
			},
		}
		findings = append(findings, finding)
	}

	if now.After(cert.NotAfter) {
		finding := security.Finding{
			ID:          fmt.Sprintf("net-ssl-cert-expired-%d-%d", port, common.ConsensusNow().Unix()),
			Title:       "Certificate Expired",
			Description: fmt.Sprintf("Certificate expired on %s", cert.NotAfter.Format(time.RFC3339)),
			Severity:    security.SeverityCritical,
			Category:    "network-ssl",
			Location:    address,
			Evidence:    fmt.Sprintf("Current time: %s, Expired: %s", now.Format(time.RFC3339), cert.NotAfter.Format(time.RFC3339)),
			Remediation: "Renew the certificate immediately",
			References:  []string{"https://tools.ietf.org/html/rfc5280"},
			FoundAt:     common.ConsensusNow(),
			Verified:    true,
			Metadata: map[string]interface{}{
				"port":      port,
				"not_after": cert.NotAfter,
				"subject":   cert.Subject.String(),
			},
		}
		findings = append(findings, finding)
	} else if cert.NotAfter.Sub(now) < 30*24*time.Hour {
		// Certificate expires within 30 days
		finding := security.Finding{
			ID:          fmt.Sprintf("net-ssl-cert-expiring-%d-%d", port, common.ConsensusNow().Unix()),
			Title:       "Certificate Expiring Soon",
			Description: fmt.Sprintf("Certificate expires in %.0f days", cert.NotAfter.Sub(now).Hours()/24),
			Severity:    security.SeverityMedium,
			Category:    "network-ssl",
			Location:    address,
			Evidence:    fmt.Sprintf("Expires: %s", cert.NotAfter.Format(time.RFC3339)),
			Remediation: "Plan certificate renewal before expiration",
			References:  []string{"https://tools.ietf.org/html/rfc5280"},
			FoundAt:     common.ConsensusNow(),
			Verified:    true,
			Metadata: map[string]interface{}{
				"port":           port,
				"not_after":      cert.NotAfter,
				"days_remaining": int(cert.NotAfter.Sub(now).Hours() / 24),
				"subject":        cert.Subject.String(),
			},
		}
		findings = append(findings, finding)
	}

	// Check for self-signed certificates
	if cert.Issuer.String() == cert.Subject.String() {
		finding := security.Finding{
			ID:          fmt.Sprintf("net-ssl-self-signed-%d-%d", port, common.ConsensusNow().Unix()),
			Title:       "Self-Signed Certificate",
			Description: "Server is using a self-signed certificate",
			Severity:    security.SeverityHigh,
			Category:    "network-ssl",
			Location:    address,
			Evidence:    fmt.Sprintf("Issuer: %s, Subject: %s", cert.Issuer, cert.Subject),
			Remediation: "Use a certificate signed by a trusted Certificate Authority",
			References:  []string{"https://letsencrypt.org/"},
			FoundAt:     common.ConsensusNow(),
			Verified:    true,
			Metadata: map[string]interface{}{
				"port":    port,
				"issuer":  cert.Issuer.String(),
				"subject": cert.Subject.String(),
			},
		}
		findings = append(findings, finding)
	}

	// Check key size
	if cert.PublicKeyAlgorithm == x509.RSA {
		if rsaKey, ok := cert.PublicKey.(*rsa.PublicKey); ok {
			keySize := rsaKey.N.BitLen()
			if keySize < 2048 {
				finding := security.Finding{
					ID:          fmt.Sprintf("net-ssl-weak-key-%d-%d", port, common.ConsensusNow().Unix()),
					Title:       "Weak RSA Key Size",
					Description: fmt.Sprintf("RSA key size is only %d bits", keySize),
					Severity:    security.SeverityHigh,
					Category:    "network-ssl",
					Location:    address,
					Evidence:    fmt.Sprintf("RSA key size: %d bits", keySize),
					Remediation: "Use RSA keys of at least 2048 bits, preferably 4096 bits",
					References:  []string{"https://www.keylength.com/"},
					FoundAt:     common.ConsensusNow(),
					Verified:    true,
					Metadata: map[string]interface{}{
						"port":      port,
						"key_size":  keySize,
						"algorithm": "RSA",
					},
				}
				findings = append(findings, finding)
			}
		}
	}

	// Check for weak signature algorithms
	weakSignatureAlgos := map[x509.SignatureAlgorithm]bool{
		x509.MD5WithRSA:    true,
		x509.SHA1WithRSA:   true,
		x509.DSAWithSHA1:   true,
		x509.ECDSAWithSHA1: true,
	}

	if weakSignatureAlgos[cert.SignatureAlgorithm] {
		finding := security.Finding{
			ID:          fmt.Sprintf("net-ssl-weak-sig-%d-%d", port, common.ConsensusNow().Unix()),
			Title:       "Weak Signature Algorithm",
			Description: fmt.Sprintf("Certificate uses weak signature algorithm: %s", cert.SignatureAlgorithm),
			Severity:    security.SeverityHigh,
			Category:    "network-ssl",
			Location:    address,
			Evidence:    fmt.Sprintf("Signature algorithm: %s", cert.SignatureAlgorithm),
			Remediation: "Use SHA-256 or stronger signature algorithms",
			References:  []string{"https://tools.ietf.org/html/rfc6151"},
			FoundAt:     common.ConsensusNow(),
			Verified:    true,
			Metadata: map[string]interface{}{
				"port":      port,
				"algorithm": cert.SignatureAlgorithm.String(),
			},
		}
		findings = append(findings, finding)
	}

	return findings
}

// getTLSVersionName returns the human-readable name for a TLS version
func getTLSVersionName(version uint16) string {
	switch version {
	case tls.VersionSSL30:
		return "SSL 3.0"
	case tls.VersionTLS10:
		return "TLS 1.0"
	case tls.VersionTLS11:
		return "TLS 1.1"
	case tls.VersionTLS12:
		return "TLS 1.2"
	case tls.VersionTLS13:
		return "TLS 1.3"
	default:
		return fmt.Sprintf("Unknown (0x%04x)", version)
	}
}

// generateSummary creates a scan summary from findings
func (ns *NetworkScanner) generateSummary(findings []security.Finding, duration time.Duration) security.ScanSummary {
	summary := security.ScanSummary{
		TotalFindings: len(findings),
		ScanDuration:  duration,
	}

	// Count findings by severity
	for _, finding := range findings {
		switch finding.Severity {
		case security.SeverityCritical:
			summary.CriticalCount++
		case security.SeverityHigh:
			summary.HighCount++
		case security.SeverityMedium:
			summary.MediumCount++
		case security.SeverityLow:
			summary.LowCount++
		case security.SeverityInfo:
			summary.InfoCount++
		}
	}

	// Calculate risk score
	summary.RiskScore = float64(summary.CriticalCount*10 + summary.HighCount*5 +
		summary.MediumCount*3 + summary.LowCount*1)

	return summary
}

// NetworkScan provides backward compatibility with the old function signature
func NetworkScan(tool, target string) (string, error) {
	// For backward compatibility, we ignore the tool parameter and use built-in scanning
	scanner := NewNetworkScanner()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	result, err := scanner.Scan(ctx, target)
	if err != nil {
		return "", err
	}

	// Format result as string for backward compatibility
	output := fmt.Sprintf("Network Scan Results:\n")
	output += fmt.Sprintf("Target: %s\n", result.Target)
	output += fmt.Sprintf("Scanner: %s\n", result.ScannerType)
	output += fmt.Sprintf("Duration: %s\n", result.Summary.ScanDuration)
	output += fmt.Sprintf("Total Findings: %d\n", result.Summary.TotalFindings)
	output += fmt.Sprintf("Critical: %d, High: %d, Medium: %d, Low: %d, Info: %d\n",
		result.Summary.CriticalCount, result.Summary.HighCount,
		result.Summary.MediumCount, result.Summary.LowCount, result.Summary.InfoCount)
	output += fmt.Sprintf("Risk Score: %.2f\n", result.Summary.RiskScore)

	for _, finding := range result.Findings {
		output += fmt.Sprintf("\n[%s] %s\n", finding.Severity, finding.Title)
		output += fmt.Sprintf("  Description: %s\n", finding.Description)
		output += fmt.Sprintf("  Location: %s\n", finding.Location)
		output += fmt.Sprintf("  Remediation: %s\n", finding.Remediation)
	}

	return output, nil
}
