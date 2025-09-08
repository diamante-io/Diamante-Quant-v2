package scanners

import (
	"context"
	"diamante/common"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"diamante/security"
)

// AllowedStaticTools defines the allowed static analysis tools
var AllowedStaticTools = map[string]bool{
	"gosec":       true,
	"staticcheck": true,
	"golint":      true,
	"ineffassign": true,
	"errcheck":    true,
}

// StaticScanner implements secure static code analysis
type StaticScanner struct {
	mu           sync.RWMutex
	findings     []security.Finding
	lastScanTime time.Time
	scannerID    string
}

// NewStaticScanner creates a new static scanner instance
func NewStaticScanner() *StaticScanner {
	return &StaticScanner{
		scannerID: fmt.Sprintf("static-scanner-%d", common.ConsensusNow().Unix()),
		findings:  make([]security.Finding, 0),
	}
}

// Scan performs a static analysis scan on the target
func (ss *StaticScanner) Scan(ctx context.Context, target string) (*security.ScanResult, error) {
	if err := ss.ValidateTarget(target); err != nil {
		return nil, fmt.Errorf("target validation failed: %w", err)
	}

	startTime := common.ConsensusNow()
	result := &security.ScanResult{
		ID:          fmt.Sprintf("scan-%s-%d", ss.scannerID, common.ConsensusNow().Unix()),
		ScannerType: security.ScannerTypeStatic,
		Target:      target,
		StartTime:   startTime,
		Success:     true,
		Findings:    []security.Finding{},
	}

	// Perform the static analysis scan
	findings, err := ss.performScan(ctx, target)
	if err != nil {
		result.Success = false
		result.ErrorMessage = err.Error()
		result.EndTime = common.ConsensusNow()
		return result, fmt.Errorf("static analysis failed: %w", err)
	}

	result.Findings = findings
	result.EndTime = common.ConsensusNow()
	result.Summary = ss.generateSummary(findings, result.EndTime.Sub(startTime))

	// Update internal state
	ss.mu.Lock()
	ss.findings = findings
	ss.lastScanTime = common.ConsensusNow()
	ss.mu.Unlock()

	return result, nil
}

// ValidateTarget validates if the target path is safe to scan
func (ss *StaticScanner) ValidateTarget(target string) error {
	if target == "" {
		return fmt.Errorf("target path cannot be empty")
	}

	// Clean and validate the path
	cleanPath := filepath.Clean(target)

	// Check if path contains suspicious patterns
	if strings.Contains(cleanPath, "..") {
		return fmt.Errorf("path traversal attempt detected")
	}

	// Verify the path exists
	info, err := os.Stat(cleanPath)
	if err != nil {
		return fmt.Errorf("invalid target path: %w", err)
	}

	// Ensure it's a directory or a Go file
	if !info.IsDir() && !strings.HasSuffix(cleanPath, ".go") {
		return fmt.Errorf("target must be a directory or Go source file")
	}

	return nil
}

// GetScannerType returns the scanner type
func (ss *StaticScanner) GetScannerType() security.ScannerType {
	return security.ScannerTypeStatic
}

// performScan executes the actual static analysis
func (ss *StaticScanner) performScan(ctx context.Context, target string) ([]security.Finding, error) {
	findings := []security.Finding{}

	// Use gosec as the primary tool if available
	gosecFindings, err := ss.runGosec(ctx, target)
	if err == nil {
		findings = append(findings, gosecFindings...)
	}

	// Perform built-in static analysis
	builtinFindings, err := ss.performBuiltinAnalysis(ctx, target)
	if err == nil {
		findings = append(findings, builtinFindings...)
	}

	return findings, nil
}

// runGosec runs the gosec tool securely
func (ss *StaticScanner) runGosec(ctx context.Context, target string) ([]security.Finding, error) {
	// Check if gosec is available
	if _, err := exec.LookPath("gosec"); err != nil {
		// gosec not available, use built-in analysis
		return []security.Finding{}, nil
	}

	// Create command with context for timeout
	cmd := exec.CommandContext(ctx, "gosec", "-fmt=json", "-quiet", "./...")
	cmd.Dir = filepath.Clean(target)

	// Set secure environment
	cmd.Env = append(os.Environ(),
		"GOSEC_NO_UPDATE=1", // Prevent auto-updates
		"GO111MODULE=on",
	)

	output, err := cmd.Output()
	if err != nil {
		// gosec returns non-zero exit code when issues are found
		if exitErr, ok := err.(*exec.ExitError); ok {
			output = exitErr.Stderr
		} else {
			return nil, fmt.Errorf("gosec execution failed: %w", err)
		}
	}

	// Parse output and convert to findings
	findings := ss.parseGosecOutput(output)
	return findings, nil
}

// parseGosecOutput parses gosec JSON output
func (ss *StaticScanner) parseGosecOutput(output []byte) []security.Finding {
	findings := []security.Finding{}

	// Handle empty output
	if len(output) == 0 {
		return findings
	}

	// Try to parse as JSON first
	var gosecReport GosecReport
	if err := json.Unmarshal(output, &gosecReport); err == nil {
		// Successfully parsed JSON output
		for _, issue := range gosecReport.Issues {
			severity := mapGosecSeverity(issue.Severity)

			finding := security.Finding{
				ID:          fmt.Sprintf("gosec-%s-%d", issue.RuleID, common.ConsensusNow().UnixNano()),
				Title:       issue.Details,
				Description: fmt.Sprintf("%s: %s", issue.RuleID, issue.Details),
				Severity:    severity,
				Category:    categorizeGosecRule(issue.RuleID),
				Location:    fmt.Sprintf("%s:%s", issue.File, issue.Line),
				Evidence:    issue.Code,
				Remediation: getRemediationForRule(issue.RuleID),
				References: []string{
					fmt.Sprintf("https://securego.io/docs/rules/%s.html", strings.ToLower(issue.RuleID)),
					issue.CWE.URL,
				},
				FoundAt:  common.ConsensusNow(),
				Verified: true,
				Metadata: map[string]interface{}{
					"scanner":    "gosec",
					"rule_id":    issue.RuleID,
					"confidence": issue.Confidence,
					"cwe_id":     issue.CWE.ID,
					"column":     issue.Column,
				},
			}
			findings = append(findings, finding)
		}
	} else {
		// Fallback to text parsing if JSON parsing fails
		lines := strings.Split(string(output), "\n")
		for _, line := range lines {
			if strings.Contains(line, "Severity:") && strings.Contains(line, "Confidence:") {
				finding := ss.parseGosecTextLine(line)
				if finding != nil {
					findings = append(findings, *finding)
				}
			}
		}
	}

	return findings
}

// GosecReport represents the JSON structure of gosec output
type GosecReport struct {
	Issues []GosecIssue `json:"Issues"`
	Stats  GosecStats   `json:"Stats"`
}

// GosecIssue represents a single issue found by gosec
type GosecIssue struct {
	Severity   string   `json:"severity"`
	Confidence string   `json:"confidence"`
	RuleID     string   `json:"rule_id"`
	Details    string   `json:"details"`
	File       string   `json:"file"`
	Code       string   `json:"code"`
	Line       string   `json:"line"`
	Column     string   `json:"column"`
	CWE        GosecCWE `json:"cwe"`
}

// GosecCWE represents CWE information
type GosecCWE struct {
	ID  string `json:"id"`
	URL string `json:"url"`
}

// GosecStats represents scan statistics
type GosecStats struct {
	Files int `json:"files"`
	Lines int `json:"lines"`
	Nosec int `json:"nosec"`
	Found int `json:"found"`
}

// mapGosecSeverity maps gosec severity to our severity levels
func mapGosecSeverity(gosecSeverity string) security.SeverityLevel {
	switch strings.ToUpper(gosecSeverity) {
	case "HIGH":
		return security.SeverityHigh
	case "MEDIUM":
		return security.SeverityMedium
	case "LOW":
		return security.SeverityLow
	default:
		return security.SeverityInfo
	}
}

// categorizeGosecRule categorizes gosec rules into security categories
func categorizeGosecRule(ruleID string) string {
	ruleCategories := map[string]string{
		"G101": "hardcoded-credential",
		"G102": "network-binding",
		"G103": "audit-log",
		"G104": "error-handling",
		"G106": "ssh-security",
		"G107": "url-request",
		"G108": "profiling-endpoint",
		"G109": "integer-overflow",
		"G110": "integer-overflow",
		"G201": "sql-injection",
		"G202": "sql-injection",
		"G203": "xss",
		"G204": "command-injection",
		"G301": "file-permissions",
		"G302": "file-permissions",
		"G303": "file-permissions",
		"G304": "path-traversal",
		"G305": "path-traversal",
		"G306": "file-permissions",
		"G307": "error-handling",
		"G401": "weak-crypto",
		"G402": "weak-crypto",
		"G403": "weak-crypto",
		"G404": "weak-crypto",
		"G501": "weak-crypto",
		"G502": "weak-crypto",
		"G503": "weak-crypto",
		"G504": "weak-crypto",
		"G505": "weak-crypto",
		"G601": "implicit-aliasing",
	}

	if category, exists := ruleCategories[ruleID]; exists {
		return category
	}
	return "security-issue"
}

// getRemediationForRule provides remediation advice for specific rules
func getRemediationForRule(ruleID string) string {
	remediations := map[string]string{
		"G101": "Store credentials in environment variables or secure vaults, never in source code",
		"G102": "Bind to specific interfaces instead of 0.0.0.0",
		"G103": "Use audit logging for security-relevant events",
		"G104": "Always check and handle errors appropriately",
		"G106": "Use SSH with proper host key verification",
		"G107": "Validate and sanitize URLs before making requests",
		"G108": "Secure or disable profiling endpoints in production",
		"G109": "Use appropriate integer types to prevent overflow",
		"G110": "Use appropriate integer types to prevent underflow",
		"G201": "Use parameterized queries to prevent SQL injection",
		"G202": "Use parameterized queries and avoid string concatenation in SQL",
		"G203": "Properly escape output to prevent XSS attacks",
		"G204": "Avoid command execution or use secure alternatives",
		"G301": "Use restrictive file permissions (e.g., 0600 or 0644)",
		"G302": "Use restrictive file permissions when creating files",
		"G303": "Use secure methods for creating directories",
		"G304": "Validate and sanitize file paths to prevent traversal",
		"G305": "Use secure methods for file extraction",
		"G306": "Use proper file permissions when writing files",
		"G307": "Handle errors from deferred function calls",
		"G401": "Use strong cryptographic algorithms (e.g., AES, SHA-256)",
		"G402": "Use TLS 1.2 or higher",
		"G403": "Use strong RSA keys (2048 bits or more)",
		"G404": "Use cryptographically secure random number generators",
		"G501": "Avoid using weak hash algorithms like MD5",
		"G502": "Avoid using weak hash algorithms like SHA-1",
		"G503": "Avoid using weak encryption algorithms like RC4",
		"G504": "Avoid using weak encryption algorithms like DES",
		"G505": "Avoid using weak hash algorithms",
		"G601": "Be careful with implicit memory aliasing in loops",
	}

	if remediation, exists := remediations[ruleID]; exists {
		return remediation
	}
	return "Review and fix the identified security issue following security best practices"
}

// parseGosecTextLine parses a single line of gosec text output
func (ss *StaticScanner) parseGosecTextLine(line string) *security.Finding {
	// Example line format: "[/path/to/file:123] - G104 (CWE-703): Errors unhandled. (Confidence: HIGH, Severity: LOW)"

	// Extract file and line number
	fileMatch := regexp.MustCompile(`\[(.*?):(\d+)\]`).FindStringSubmatch(line)
	if len(fileMatch) < 3 {
		return nil
	}

	file := fileMatch[1]
	lineNum := fileMatch[2]

	// Extract rule ID
	ruleMatch := regexp.MustCompile(`G\d+`).FindString(line)
	if ruleMatch == "" {
		return nil
	}

	// Extract severity
	severityMatch := regexp.MustCompile(`Severity:\s*(\w+)`).FindStringSubmatch(line)
	severity := security.SeverityInfo
	if len(severityMatch) > 1 {
		severity = mapGosecSeverity(severityMatch[1])
	}

	// Extract description
	descMatch := regexp.MustCompile(`:\s*(.+?)\s*\(Confidence:`).FindStringSubmatch(line)
	description := "Security issue detected"
	if len(descMatch) > 1 {
		description = descMatch[1]
	}

	return &security.Finding{
		ID:          fmt.Sprintf("gosec-%s-%d", ruleMatch, common.ConsensusNow().UnixNano()),
		Title:       fmt.Sprintf("%s: %s", ruleMatch, description),
		Description: description,
		Severity:    severity,
		Category:    categorizeGosecRule(ruleMatch),
		Location:    fmt.Sprintf("%s:%s", file, lineNum),
		Evidence:    line,
		Remediation: getRemediationForRule(ruleMatch),
		References:  []string{fmt.Sprintf("https://securego.io/docs/rules/%s.html", strings.ToLower(ruleMatch))},
		FoundAt:     common.ConsensusNow(),
		Verified:    true,
		Metadata: map[string]interface{}{
			"scanner": "gosec",
			"rule_id": ruleMatch,
			"line":    lineNum,
		},
	}
}

// performBuiltinAnalysis performs built-in static analysis checks
func (ss *StaticScanner) performBuiltinAnalysis(ctx context.Context, target string) ([]security.Finding, error) {
	findings := []security.Finding{}

	info, err := os.Stat(target)
	if err != nil {
		return nil, fmt.Errorf("failed to stat target: %w", err)
	}

	if info.IsDir() {
		// Scan all Go files in directory
		err := filepath.Walk(target, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			// Check context cancellation
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			if strings.HasSuffix(path, ".go") && !strings.Contains(path, "vendor/") {
				fileFindings, err := ss.analyzeGoFile(path)
				if err != nil {
					// Log error but continue scanning
					return nil
				}
				findings = append(findings, fileFindings...)
			}
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("directory walk failed: %w", err)
		}
	} else {
		// Single file analysis
		fileFindings, err := ss.analyzeGoFile(target)
		if err != nil {
			return nil, fmt.Errorf("file analysis failed: %w", err)
		}
		findings = append(findings, fileFindings...)
	}

	return findings, nil
}

// analyzeGoFile performs static analysis on a single Go file
func (ss *StaticScanner) analyzeGoFile(filename string) ([]security.Finding, error) {
	findings := []security.Finding{}

	// Parse the Go file
	fset := token.NewFileSet()
	node, err := parser.ParseFile(fset, filename, nil, parser.ParseComments)
	if err != nil {
		return nil, fmt.Errorf("failed to parse file: %w", err)
	}

	// Check for security issues
	ast.Inspect(node, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.CallExpr:
			// Check for dangerous function calls
			if finding := ss.checkDangerousFunctions(x, fset, filename); finding != nil {
				findings = append(findings, *finding)
			}
		case *ast.BasicLit:
			// Check for hardcoded credentials
			if finding := ss.checkHardcodedCredentials(x, fset, filename); finding != nil {
				findings = append(findings, *finding)
			}
		}
		return true
	})

	// Check for additional issues
	if finding := ss.checkFilePermissions(filename); finding != nil {
		findings = append(findings, *finding)
	}

	if finding := ss.checkErrorHandling(node, fset, filename); finding != nil {
		findings = append(findings, *finding)
	}

	return findings, nil
}

// checkDangerousFunctions checks for use of dangerous functions
func (ss *StaticScanner) checkDangerousFunctions(call *ast.CallExpr, fset *token.FileSet, filename string) *security.Finding {
	dangerousFuncs := map[string]string{
		"exec.Command":       "Command injection risk",
		"os/exec.Command":    "Command injection risk",
		"sql.Query":          "SQL injection risk",
		"database/sql.Query": "SQL injection risk",
		"fmt.Sprintf":        "Format string vulnerability if used with SQL",
		"template.HTML":      "XSS vulnerability risk",
		"template.JS":        "XSS vulnerability risk",
	}

	if ident, ok := call.Fun.(*ast.Ident); ok {
		if risk, found := dangerousFuncs[ident.Name]; found {
			pos := fset.Position(call.Pos())
			return &security.Finding{
				ID:          fmt.Sprintf("static-func-%s-%d", ident.Name, common.ConsensusNow().Unix()),
				Title:       fmt.Sprintf("Potentially Dangerous Function: %s", ident.Name),
				Description: fmt.Sprintf("Use of %s detected: %s", ident.Name, risk),
				Severity:    security.SeverityMedium,
				Category:    "dangerous-function",
				Location:    fmt.Sprintf("%s:%d:%d", filename, pos.Line, pos.Column),
				Evidence:    fmt.Sprintf("Function call to %s", ident.Name),
				Remediation: "Validate all inputs and use parameterized queries or safe alternatives",
				References:  []string{"https://owasp.org/www-project-top-ten/"},
				FoundAt:     common.ConsensusNow(),
				Verified:    true,
				Metadata: map[string]interface{}{
					"function": ident.Name,
					"line":     pos.Line,
				},
			}
		}
	}

	return nil
}

// checkHardcodedCredentials checks for hardcoded credentials
func (ss *StaticScanner) checkHardcodedCredentials(lit *ast.BasicLit, fset *token.FileSet, filename string) *security.Finding {
	if lit.Kind != token.STRING {
		return nil
	}

	value := strings.ToLower(lit.Value)

	// Check for common credential patterns
	credentialPatterns := []string{
		"password",
		"passwd",
		"secret",
		"api_key",
		"apikey",
		"access_token",
		"private_key",
		"auth_token",
	}

	for _, pattern := range credentialPatterns {
		if strings.Contains(value, pattern) && len(lit.Value) > 10 {
			pos := fset.Position(lit.Pos())
			return &security.Finding{
				ID:          fmt.Sprintf("static-cred-%d", common.ConsensusNow().Unix()),
				Title:       "Potential Hardcoded Credential",
				Description: "Possible hardcoded credential detected in source code",
				Severity:    security.SeverityHigh,
				Category:    "hardcoded-credential",
				Location:    fmt.Sprintf("%s:%d:%d", filename, pos.Line, pos.Column),
				Evidence:    "String literal containing credential-related keyword",
				Remediation: "Use environment variables or secure configuration management for credentials",
				References:  []string{"https://cwe.mitre.org/data/definitions/798.html"},
				FoundAt:     common.ConsensusNow(),
				Verified:    false,
				Metadata: map[string]interface{}{
					"line": pos.Line,
				},
			}
		}
	}

	return nil
}

// checkFilePermissions checks for insecure file permissions
func (ss *StaticScanner) checkFilePermissions(filename string) *security.Finding {
	content, err := os.ReadFile(filename)
	if err != nil {
		return nil
	}

	// Check for insecure file permission patterns
	insecurePatterns := []string{
		"0777",
		"0666",
		"os.ModePerm",
	}

	for _, pattern := range insecurePatterns {
		if strings.Contains(string(content), pattern) {
			return &security.Finding{
				ID:          fmt.Sprintf("static-perm-%d", common.ConsensusNow().Unix()),
				Title:       "Insecure File Permissions",
				Description: fmt.Sprintf("Insecure file permission %s detected", pattern),
				Severity:    security.SeverityMedium,
				Category:    "file-permissions",
				Location:    filename,
				Evidence:    fmt.Sprintf("Found pattern: %s", pattern),
				Remediation: "Use restrictive file permissions (e.g., 0600 or 0644)",
				References:  []string{"https://cwe.mitre.org/data/definitions/732.html"},
				FoundAt:     common.ConsensusNow(),
				Verified:    true,
				Metadata: map[string]interface{}{
					"pattern": pattern,
				},
			}
		}
	}

	return nil
}

// checkErrorHandling checks for ignored errors
func (ss *StaticScanner) checkErrorHandling(file *ast.File, fset *token.FileSet, filename string) *security.Finding {
	ignoredErrors := 0

	ast.Inspect(file, func(n ast.Node) bool {
		if assign, ok := n.(*ast.AssignStmt); ok {
			// Check for pattern: _, err := someFunc()
			if len(assign.Lhs) == 2 {
				if ident, ok := assign.Lhs[0].(*ast.Ident); ok && ident.Name == "_" {
					if errIdent, ok := assign.Lhs[1].(*ast.Ident); ok && errIdent.Name == "err" {
						ignoredErrors++
					}
				}
			}
		}
		return true
	})

	if ignoredErrors > 0 {
		return &security.Finding{
			ID:          fmt.Sprintf("static-err-%d", common.ConsensusNow().Unix()),
			Title:       "Ignored Error Handling",
			Description: fmt.Sprintf("Found %d instances of ignored errors", ignoredErrors),
			Severity:    security.SeverityLow,
			Category:    "error-handling",
			Location:    filename,
			Evidence:    fmt.Sprintf("%d errors are being ignored with underscore", ignoredErrors),
			Remediation: "Properly handle all errors instead of ignoring them",
			References:  []string{"https://go.dev/blog/error-handling-and-go"},
			FoundAt:     common.ConsensusNow(),
			Verified:    true,
			Metadata: map[string]interface{}{
				"count": ignoredErrors,
			},
		}
	}

	return nil
}

// generateSummary creates a scan summary from findings
func (ss *StaticScanner) generateSummary(findings []security.Finding, duration time.Duration) security.ScanSummary {
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

// StaticScan provides backward compatibility with the old function signature
func StaticScan(tool, path string) (string, error) {
	// Validate tool is in allowed list
	if tool != "" && !AllowedStaticTools[tool] {
		return "", fmt.Errorf("tool '%s' is not in the allowed list", tool)
	}

	// Create a new scanner and perform scan
	scanner := NewStaticScanner()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	result, err := scanner.Scan(ctx, path)
	if err != nil {
		return "", err
	}

	// Format result as string for backward compatibility
	output := fmt.Sprintf("Static Analysis Results:\n")
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
		output += fmt.Sprintf("  Location: %s\n", finding.Location)
		output += fmt.Sprintf("  Description: %s\n", finding.Description)
		output += fmt.Sprintf("  Remediation: %s\n", finding.Remediation)
	}

	return output, nil
}
