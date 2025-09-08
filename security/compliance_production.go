package security

import (
	"bufio"
	"diamante/common"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ProductionComplianceChecker implements the production-ready compliance checking logic
type ProductionComplianceChecker struct {
	mu            sync.RWMutex
	stateCache    map[string]interface{}
	cacheExpiry   time.Duration
	lastCacheTime time.Time
}

// NewProductionComplianceChecker creates a new production compliance checker
func NewProductionComplianceChecker() *ProductionComplianceChecker {
	return &ProductionComplianceChecker{
		stateCache:  make(map[string]interface{}),
		cacheExpiry: 5 * time.Minute,
	}
}

// EnhanceComplianceChecker enhances the existing compliance checker with production capabilities
func EnhanceComplianceChecker(cc *ComplianceCheckerImpl) {
	// Add production checker to the existing implementation
	cc.productionChecker = NewProductionComplianceChecker()
}

// ProductionCheckAccessControlRule performs real access control validation
func (pc *ProductionComplianceChecker) CheckAccessControlRule(rule PolicyRule) string {
	switch rule.Condition {
	case "least_privilege":
		return pc.checkLeastPrivilege(rule)
	case "role_based_access":
		return pc.checkRBACImplementation(rule)
	case "file_permissions":
		return pc.checkFilePermissions(rule)
	case "user_privileges":
		return pc.checkUserPrivileges(rule)
	default:
		return ""
	}
}

// checkLeastPrivilege performs actual least privilege checks
func (pc *ProductionComplianceChecker) checkLeastPrivilege(rule PolicyRule) string {
	requiredLevel, _ := rule.Parameters["required_level"].(string)

	// Check sudo configuration
	sudoersPath := "/etc/sudoers"
	if content, err := os.ReadFile(sudoersPath); err == nil {
		// Check for dangerous sudo configurations
		dangerousPatterns := []string{
			`ALL\s*=\s*\(\s*ALL\s*\)\s*NOPASSWD\s*:\s*ALL`,
			`ALL\s*=\s*\(\s*ALL\s*\)\s*ALL`,
		}

		for _, pattern := range dangerousPatterns {
			if matched, _ := regexp.MatchString(pattern, string(content)); matched {
				return fmt.Sprintf("Overly permissive sudo configuration detected for rule %s", rule.ID)
			}
		}
	}

	// Check for setuid/setgid binaries
	if requiredLevel == "strict" {
		if violations := pc.findSetuidBinaries(); len(violations) > 0 {
			return fmt.Sprintf("Found %d setuid/setgid binaries that may violate least privilege for rule %s", len(violations), rule.ID)
		}
	}

	return ""
}

// findSetuidBinaries finds setuid/setgid binaries on the system
func (pc *ProductionComplianceChecker) findSetuidBinaries() []string {
	// Check cache first
	pc.mu.RLock()
	if cached, ok := pc.stateCache["setuid_binaries"]; ok {
		pc.mu.RUnlock()
		if violations, ok := cached.([]string); ok {
			return violations
		}
	}
	pc.mu.RUnlock()

	violations := []string{}

	// Common paths to check
	paths := []string{"/usr/bin", "/usr/sbin", "/bin", "/sbin"}

	allowedSetuid := map[string]bool{
		"passwd": true,
		"sudo":   true,
		"su":     true,
		"ping":   true,
	}

	for _, path := range paths {
		filepath.Walk(path, func(filePath string, info os.FileInfo, err error) error {
			if err != nil {
				return nil
			}

			if info.Mode()&os.ModeSetuid != 0 || info.Mode()&os.ModeSetgid != 0 {
				basename := filepath.Base(filePath)
				if !allowedSetuid[basename] {
					violations = append(violations, filePath)
				}
			}
			return nil
		})
	}

	// Cache results
	pc.mu.Lock()
	pc.stateCache["setuid_binaries"] = violations
	pc.lastCacheTime = common.ConsensusNow()
	pc.mu.Unlock()

	return violations
}

// checkRBACImplementation checks role-based access control
func (pc *ProductionComplianceChecker) checkRBACImplementation(rule PolicyRule) string {
	minRoles, _ := rule.Parameters["min_roles"].(int)

	// Check for RBAC configuration files
	rbacConfigPaths := []string{
		"./config/rbac.json",
		"./config/roles.yaml",
		"/etc/security/rbac.conf",
	}

	rbacFound := false
	roleCount := 0

	for _, configPath := range rbacConfigPaths {
		if info, err := os.Stat(configPath); err == nil && !info.IsDir() {
			rbacFound = true

			// Parse and count roles
			content, err := os.ReadFile(configPath)
			if err == nil {
				// Simple role counting
				roleCount = strings.Count(string(content), "role:")
				if roleCount == 0 {
					roleCount = strings.Count(string(content), `"role"`)
				}
			}
			break
		}
	}

	if !rbacFound {
		return fmt.Sprintf("No RBAC configuration found for rule %s", rule.ID)
	}

	if minRoles > 0 && roleCount < minRoles {
		return fmt.Sprintf("Insufficient roles defined (%d < %d) for rule %s", roleCount, minRoles, rule.ID)
	}

	return ""
}

// checkFilePermissions checks file permission security
func (pc *ProductionComplianceChecker) checkFilePermissions(rule PolicyRule) string {
	criticalFiles, _ := rule.Parameters["critical_files"].([]string)
	if len(criticalFiles) == 0 {
		criticalFiles = []string{
			"/etc/passwd",
			"/etc/shadow",
			"/etc/ssh/sshd_config",
			"./config/config.yaml",
		}
	}

	violations := []string{}

	for _, file := range criticalFiles {
		if info, err := os.Stat(file); err == nil {
			mode := info.Mode()

			// Check for world-writable files
			if mode&0002 != 0 {
				violations = append(violations, fmt.Sprintf("%s is world-writable", file))
			}

			// Check for appropriate ownership (cross-platform approach)
			// On Unix-like systems, check if shadow files are owned by root
			// This is a simplified check that works across platforms
			if strings.Contains(file, "shadow") {
				// For shadow files, we expect restrictive permissions
				// On Windows, this check is not applicable, so we skip it
				if mode.Perm() > 0o600 {
					violations = append(violations, fmt.Sprintf("%s has overly permissive permissions", file))
				}
			}
		}
	}

	if len(violations) > 0 {
		return fmt.Sprintf("File permission violations for rule %s: %s", rule.ID, strings.Join(violations, "; "))
	}

	return ""
}

// checkUserPrivileges checks user privilege configurations
func (pc *ProductionComplianceChecker) checkUserPrivileges(rule PolicyRule) string {
	// Check for users with UID 0 (root privileges)
	passwdFile := "/etc/passwd"
	if content, err := os.ReadFile(passwdFile); err == nil {
		scanner := bufio.NewScanner(strings.NewReader(string(content)))
		rootUsers := 0

		for scanner.Scan() {
			fields := strings.Split(scanner.Text(), ":")
			if len(fields) >= 3 {
				if uid, err := strconv.Atoi(fields[2]); err == nil && uid == 0 {
					rootUsers++
				}
			}
		}

		if rootUsers > 1 {
			return fmt.Sprintf("Multiple users with UID 0 detected for rule %s", rule.ID)
		}
	}

	return ""
}

// ProductionCheckDataProtectionRule performs real data protection validation
func (pc *ProductionComplianceChecker) CheckDataProtectionRule(rule PolicyRule) string {
	switch rule.Condition {
	case "encryption_at_rest":
		return pc.checkEncryptionAtRest(rule)
	case "encryption_in_transit":
		return pc.checkEncryptionInTransit(rule)
	case "key_management":
		return pc.checkKeyManagement(rule)
	default:
		return ""
	}
}

// checkEncryptionAtRest verifies encryption at rest
func (pc *ProductionComplianceChecker) checkEncryptionAtRest(rule PolicyRule) string {
	requiredAlgo, _ := rule.Parameters["algorithm"].(string)

	// Check for encrypted filesystems
	output, err := exec.Command("lsblk", "-o", "NAME,FSTYPE,MOUNTPOINT", "-J").Output()
	if err == nil {
		var blockDevices map[string]interface{}
		if err := json.Unmarshal(output, &blockDevices); err == nil {
			// Parse block devices to check for encryption
			if devices, ok := blockDevices["blockdevices"].([]interface{}); ok {
				encryptedCount := 0
				for _, device := range devices {
					if dev, ok := device.(map[string]interface{}); ok {
						if fstype, ok := dev["fstype"].(string); ok && strings.Contains(fstype, "crypt") {
							encryptedCount++
						}
					}
				}

				if encryptedCount == 0 {
					return fmt.Sprintf("No encrypted volumes found for rule %s", rule.ID)
				}
			}
		}
	}

	// Check database encryption settings
	dbConfigPaths := []string{
		"./config/database.yaml",
		"/etc/mysql/my.cnf",
		"/etc/postgresql/postgresql.conf",
	}

	encryptionFound := false
	for _, configPath := range dbConfigPaths {
		if content, err := os.ReadFile(configPath); err == nil {
			contentStr := string(content)
			// Check for encryption keywords
			if strings.Contains(contentStr, "encrypt") ||
				strings.Contains(contentStr, requiredAlgo) ||
				strings.Contains(contentStr, "tde_enabled") {
				encryptionFound = true

				// Verify it's the correct algorithm
				if requiredAlgo != "" && !strings.Contains(contentStr, requiredAlgo) {
					return fmt.Sprintf("Encryption algorithm %s not found in configuration for rule %s", requiredAlgo, rule.ID)
				}
			}
		}
	}

	if !encryptionFound {
		return fmt.Sprintf("Database encryption configuration not found for rule %s", rule.ID)
	}

	return ""
}

// checkEncryptionInTransit verifies encryption in transit
func (pc *ProductionComplianceChecker) checkEncryptionInTransit(rule PolicyRule) string {
	minTLSVersion, _ := rule.Parameters["min_tls_version"].(string)

	violations := []string{}

	// Check SSH configuration
	sshdConfig := "/etc/ssh/sshd_config"
	if content, err := os.ReadFile(sshdConfig); err == nil {
		if !strings.Contains(string(content), "Protocol 2") {
			violations = append(violations, "SSH not configured for Protocol 2")
		}
	}

	// Check web server TLS configuration
	nginxConfig := "/etc/nginx/nginx.conf"
	apacheConfig := "/etc/apache2/apache2.conf"

	for _, configPath := range []string{nginxConfig, apacheConfig} {
		if content, err := os.ReadFile(configPath); err == nil {
			contentStr := string(content)

			// Check for weak protocols
			if strings.Contains(contentStr, "SSLv2") || strings.Contains(contentStr, "SSLv3") {
				violations = append(violations, "Weak SSL protocols enabled")
			}

			// Check minimum TLS version
			if minTLSVersion != "" && !strings.Contains(contentStr, minTLSVersion) {
				violations = append(violations, fmt.Sprintf("Minimum TLS version %s not enforced", minTLSVersion))
			}
		}
	}

	// Check for unencrypted services
	unencryptedPorts := pc.checkUnencryptedServices()
	if len(unencryptedPorts) > 0 {
		violations = append(violations, fmt.Sprintf("Unencrypted services on ports: %v", unencryptedPorts))
	}

	if len(violations) > 0 {
		return fmt.Sprintf("Encryption in transit violations for rule %s: %s", rule.ID, strings.Join(violations, "; "))
	}

	return ""
}

// checkUnencryptedServices checks for services without encryption
func (pc *ProductionComplianceChecker) checkUnencryptedServices() []int {
	unencryptedPorts := []int{}

	// Common unencrypted service ports
	checkPorts := map[int]string{
		21:  "FTP",
		23:  "Telnet",
		25:  "SMTP",
		80:  "HTTP",
		110: "POP3",
		143: "IMAP",
	}

	for port := range checkPorts {
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("localhost:%d", port), 2*time.Second)
		if err == nil {
			conn.Close()
			unencryptedPorts = append(unencryptedPorts, port)
		}
	}

	return unencryptedPorts
}

// checkKeyManagement checks key management practices
func (pc *ProductionComplianceChecker) checkKeyManagement(rule PolicyRule) string {
	// Check for hardcoded keys in common locations
	keyPatterns := []string{
		`-----BEGIN.*PRIVATE KEY-----`,
		`[Aa][Pp][Ii][-_]?[Kk][Ee][Yy]\s*[:=]\s*['"]?[A-Za-z0-9+/]{20,}`,
		`[Ss][Ee][Cc][Rr][Ee][Tt]\s*[:=]\s*['"]?[A-Za-z0-9+/]{20,}`,
	}

	configDirs := []string{"./config", "./src", "."}
	violations := []string{}

	for _, dir := range configDirs {
		filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
			if err != nil || info.IsDir() {
				return nil
			}

			// Skip binary files and specific extensions
			ext := filepath.Ext(path)
			if ext == ".bin" || ext == ".exe" || ext == ".so" {
				return nil
			}

			content, err := os.ReadFile(path)
			if err != nil {
				return nil
			}

			for _, pattern := range keyPatterns {
				if matched, _ := regexp.MatchString(pattern, string(content)); matched {
					violations = append(violations, fmt.Sprintf("Potential hardcoded key in %s", path))
					break
				}
			}

			return nil
		})
	}

	if len(violations) > 0 {
		return fmt.Sprintf("Key management violations for rule %s: %s", rule.ID, strings.Join(violations, "; "))
	}

	return ""
}

// ProductionCheckNetworkSecurityRule performs real network security validation
func (pc *ProductionComplianceChecker) CheckNetworkSecurityRule(rule PolicyRule) string {
	switch rule.Condition {
	case "firewall_configured":
		return pc.checkFirewallConfiguration(rule)
	case "ports_restricted":
		return pc.checkPortRestrictions(rule)
	case "network_segmentation":
		return pc.checkNetworkSegmentation(rule)
	default:
		return ""
	}
}

// checkFirewallConfiguration verifies firewall setup
func (pc *ProductionComplianceChecker) checkFirewallConfiguration(rule PolicyRule) string {
	defaultDeny, _ := rule.Parameters["default_deny"].(bool)

	// Check iptables rules
	output, err := exec.Command("iptables", "-L", "-n").Output()
	if err != nil {
		// Try firewalld
		output, err = exec.Command("firewall-cmd", "--list-all").Output()
		if err != nil {
			return fmt.Sprintf("No firewall configuration found for rule %s", rule.ID)
		}
	}

	outputStr := string(output)

	// Check for default deny policy
	if defaultDeny {
		if !strings.Contains(outputStr, "policy DROP") && !strings.Contains(outputStr, "policy REJECT") {
			return fmt.Sprintf("Default deny policy not configured for rule %s", rule.ID)
		}
	}

	// Check for any allow-all rules
	if strings.Contains(outputStr, "ACCEPT     all") && strings.Contains(outputStr, "0.0.0.0/0") {
		return fmt.Sprintf("Overly permissive firewall rules detected for rule %s", rule.ID)
	}

	return ""
}

// checkPortRestrictions verifies port access restrictions
func (pc *ProductionComplianceChecker) checkPortRestrictions(rule PolicyRule) string {
	allowedPorts, _ := rule.Parameters["allowed_ports"].([]interface{})

	// Get listening ports
	output, err := exec.Command("netstat", "-tlnp").Output()
	if err != nil {
		output, err = exec.Command("ss", "-tlnp").Output()
		if err != nil {
			return ""
		}
	}

	// Parse listening ports
	listeningPorts := []int{}
	scanner := bufio.NewScanner(strings.NewReader(string(output)))
	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, "LISTEN") {
			// Extract port from address
			parts := strings.Fields(line)
			for _, part := range parts {
				if strings.Contains(part, ":") {
					portStr := part[strings.LastIndex(part, ":")+1:]
					if port, err := strconv.Atoi(portStr); err == nil {
						listeningPorts = append(listeningPorts, port)
					}
				}
			}
		}
	}

	// Check against allowed ports
	violations := []int{}
	if len(allowedPorts) > 0 {
		allowedMap := make(map[int]bool)
		for _, p := range allowedPorts {
			if port, ok := p.(int); ok {
				allowedMap[port] = true
			}
		}

		for _, port := range listeningPorts {
			if !allowedMap[port] {
				violations = append(violations, port)
			}
		}
	}

	if len(violations) > 0 {
		return fmt.Sprintf("Unauthorized ports open for rule %s: %v", rule.ID, violations)
	}

	return ""
}

// checkNetworkSegmentation checks network segmentation
func (pc *ProductionComplianceChecker) checkNetworkSegmentation(rule PolicyRule) string {
	// Check for VLAN configuration
	output, err := exec.Command("ip", "link", "show").Output()
	if err != nil {
		return ""
	}

	vlanCount := strings.Count(string(output), "vlan")
	minVlans, _ := rule.Parameters["min_vlans"].(int)

	if minVlans > 0 && vlanCount < minVlans {
		return fmt.Sprintf("Insufficient network segmentation (VLANs: %d < %d) for rule %s", vlanCount, minVlans, rule.ID)
	}

	return ""
}

// ProductionCheckAuthenticationRule performs real authentication validation
func (pc *ProductionComplianceChecker) CheckAuthenticationRule(rule PolicyRule) string {
	switch rule.Condition {
	case "multi_factor_auth":
		return pc.checkMultiFactorAuth(rule)
	case "password_policy":
		return pc.checkPasswordPolicy(rule)
	case "account_lockout":
		return pc.checkAccountLockout(rule)
	case "session_management":
		return pc.checkSessionManagement(rule)
	default:
		return ""
	}
}

// checkMultiFactorAuth verifies MFA configuration
func (pc *ProductionComplianceChecker) checkMultiFactorAuth(rule PolicyRule) string {
	required, _ := rule.Parameters["required"].(bool)

	if !required {
		return ""
	}

	// Check PAM configuration for MFA
	pamConfigs := []string{
		"/etc/pam.d/sshd",
		"/etc/pam.d/login",
		"/etc/pam.d/sudo",
	}

	mfaFound := false
	for _, pamConfig := range pamConfigs {
		if content, err := os.ReadFile(pamConfig); err == nil {
			contentStr := string(content)
			// Check for common MFA modules
			if strings.Contains(contentStr, "pam_google_authenticator") ||
				strings.Contains(contentStr, "pam_duo") ||
				strings.Contains(contentStr, "pam_yubico") ||
				strings.Contains(contentStr, "pam_u2f") {
				mfaFound = true
				break
			}
		}
	}

	if !mfaFound {
		return fmt.Sprintf("Multi-factor authentication not configured for rule %s", rule.ID)
	}

	return ""
}

// checkPasswordPolicy verifies password policy configuration
func (pc *ProductionComplianceChecker) checkPasswordPolicy(rule PolicyRule) string {
	minLength, _ := rule.Parameters["min_length"].(int)
	requireComplexity, _ := rule.Parameters["require_complexity"].(bool)

	violations := []string{}

	// Check PAM password quality settings
	pamPwquality := "/etc/pam.d/common-password"
	if _, err := os.Stat(pamPwquality); err != nil {
		pamPwquality = "/etc/pam.d/system-auth"
	}

	if content, err := os.ReadFile(pamPwquality); err == nil {
		contentStr := string(content)

		// Check minimum length
		if minLength > 0 {
			minlenPattern := regexp.MustCompile(`minlen=(\d+)`)
			if matches := minlenPattern.FindStringSubmatch(contentStr); len(matches) > 1 {
				if configuredLen, _ := strconv.Atoi(matches[1]); configuredLen < minLength {
					violations = append(violations, fmt.Sprintf("Password minimum length %d < %d", configuredLen, minLength))
				}
			} else {
				violations = append(violations, "Password minimum length not configured")
			}
		}

		// Check complexity requirements
		if requireComplexity {
			complexityChecks := []string{"dcredit", "ucredit", "lcredit", "ocredit"}
			for _, check := range complexityChecks {
				if !strings.Contains(contentStr, check) {
					violations = append(violations, fmt.Sprintf("Password complexity check %s not configured", check))
				}
			}
		}
	} else {
		violations = append(violations, "Password policy configuration not found")
	}

	if len(violations) > 0 {
		return fmt.Sprintf("Password policy violations for rule %s: %s", rule.ID, strings.Join(violations, "; "))
	}

	return ""
}

// checkAccountLockout verifies account lockout configuration
func (pc *ProductionComplianceChecker) checkAccountLockout(rule PolicyRule) string {
	maxAttempts, _ := rule.Parameters["max_attempts"].(int)

	// Check PAM tally/faillock configuration
	pamConfig := "/etc/pam.d/common-auth"
	if _, err := os.Stat(pamConfig); err != nil {
		pamConfig = "/etc/pam.d/system-auth"
	}

	if content, err := os.ReadFile(pamConfig); err == nil {
		contentStr := string(content)

		// Check for faillock or tally2
		if !strings.Contains(contentStr, "pam_faillock") && !strings.Contains(contentStr, "pam_tally2") {
			return fmt.Sprintf("Account lockout not configured for rule %s", rule.ID)
		}

		// Check max attempts
		if maxAttempts > 0 {
			denyPattern := regexp.MustCompile(`deny=(\d+)`)
			if matches := denyPattern.FindStringSubmatch(contentStr); len(matches) > 1 {
				if configuredAttempts, _ := strconv.Atoi(matches[1]); configuredAttempts > maxAttempts {
					return fmt.Sprintf("Account lockout threshold too high (%d > %d) for rule %s", configuredAttempts, maxAttempts, rule.ID)
				}
			}
		}
	}

	return ""
}

// checkSessionManagement verifies session management configuration
func (pc *ProductionComplianceChecker) checkSessionManagement(rule PolicyRule) string {
	maxIdleTime, _ := rule.Parameters["max_idle_time"].(int)

	// Check SSH session timeout
	sshdConfig := "/etc/ssh/sshd_config"
	if content, err := os.ReadFile(sshdConfig); err == nil {
		contentStr := string(content)

		// Check ClientAliveInterval
		intervalPattern := regexp.MustCompile(`ClientAliveInterval\s+(\d+)`)
		if matches := intervalPattern.FindStringSubmatch(contentStr); len(matches) > 1 {
			if interval, _ := strconv.Atoi(matches[1]); maxIdleTime > 0 && (interval == 0 || interval > maxIdleTime) {
				return fmt.Sprintf("SSH session timeout not properly configured for rule %s", rule.ID)
			}
		} else if maxIdleTime > 0 {
			return fmt.Sprintf("SSH session timeout not configured for rule %s", rule.ID)
		}
	}

	return ""
}

// ProductionCheckLoggingRule performs real logging validation
func (pc *ProductionComplianceChecker) CheckLoggingRule(rule PolicyRule) string {
	switch rule.Condition {
	case "audit_logging_enabled":
		return pc.checkAuditLogging(rule)
	case "log_retention":
		return pc.checkLogRetention(rule)
	case "log_forwarding":
		return pc.checkLogForwarding(rule)
	default:
		return ""
	}
}

// checkAuditLogging verifies audit logging is enabled
func (pc *ProductionComplianceChecker) checkAuditLogging(rule PolicyRule) string {
	// Check if auditd is running
	output, err := exec.Command("systemctl", "is-active", "auditd").Output()
	if err != nil || strings.TrimSpace(string(output)) != "active" {
		return fmt.Sprintf("Audit logging service not active for rule %s", rule.ID)
	}

	// Check audit rules
	rulesOutput, err := exec.Command("auditctl", "-l").Output()
	if err != nil || len(rulesOutput) == 0 {
		return fmt.Sprintf("No audit rules configured for rule %s", rule.ID)
	}

	return ""
}

// checkLogRetention verifies log retention policies
func (pc *ProductionComplianceChecker) checkLogRetention(rule PolicyRule) string {
	minDays, _ := rule.Parameters["min_days"].(int)

	// Check logrotate configuration
	logrotateConfigs := []string{
		"/etc/logrotate.conf",
		"/etc/logrotate.d/rsyslog",
		"/etc/logrotate.d/audit",
	}

	for _, config := range logrotateConfigs {
		if content, err := os.ReadFile(config); err == nil {
			// Check for rotate directive
			rotatePattern := regexp.MustCompile(`rotate\s+(\d+)`)
			if matches := rotatePattern.FindStringSubmatch(string(content)); len(matches) > 1 {
				if retention, _ := strconv.Atoi(matches[1]); retention < minDays/7 { // Convert days to weeks
					return fmt.Sprintf("Insufficient log retention (%d weeks < %d days) for rule %s", retention, minDays, rule.ID)
				}
			}
		}
	}

	return ""
}

// checkLogForwarding verifies log forwarding configuration
func (pc *ProductionComplianceChecker) checkLogForwarding(rule PolicyRule) string {
	// Check rsyslog configuration for remote logging
	rsyslogConfig := "/etc/rsyslog.conf"
	if content, err := os.ReadFile(rsyslogConfig); err == nil {
		// Check for remote syslog server configuration
		if !strings.Contains(string(content), "@@") && !strings.Contains(string(content), "@") {
			return fmt.Sprintf("No remote log forwarding configured for rule %s", rule.ID)
		}
	}

	return ""
}
