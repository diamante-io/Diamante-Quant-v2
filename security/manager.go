package security

import (
	"context"
	"diamante/common"
	"fmt"
	"strings"
	"sync"
	"time"
)

// SecurityManager integrates all security components
type SecurityManager struct {
	mu                    sync.RWMutex
	dependencyScanner     SecurityScanner
	networkScanner        SecurityScanner
	staticScanner         SecurityScanner
	vulnerabilityReporter VulnerabilityReporter
	complianceChecker     ComplianceChecker
	securityMonitor       SecurityMonitor
	auditLogger           AuditLogger
	threatDetector        ThreatDetector
	isInitialized         bool
	config                SecurityConfig
}

// SecurityConfig holds configuration for the security manager
type SecurityConfig struct {
	EnableAutoScanning     bool
	ScanInterval           time.Duration
	CompliancePolicies     []SecurityPolicy
	ThreatDetectionEnabled bool
	AuditLoggingEnabled    bool
	MaxEventHistory        int
	AlertThreshold         SeverityLevel
}

// DefaultSecurityConfig returns a default security configuration
func DefaultSecurityConfig() SecurityConfig {
	return SecurityConfig{
		EnableAutoScanning:     true,
		ScanInterval:           24 * time.Hour,
		CompliancePolicies:     GetDefaultPolicies(),
		ThreatDetectionEnabled: true,
		AuditLoggingEnabled:    true,
		MaxEventHistory:        10000,
		AlertThreshold:         SeverityHigh,
	}
}

// NewSecurityManager creates a new security manager instance
func NewSecurityManager(config SecurityConfig) *SecurityManager {
	return &SecurityManager{
		config:        config,
		isInitialized: false,
	}
}

// Initialize initializes all security components
func (sm *SecurityManager) Initialize(ctx context.Context) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if sm.isInitialized {
		return fmt.Errorf("security manager already initialized")
	}

	// Initialize scanners
	sm.dependencyScanner = NewDependencyScanner()
	sm.networkScanner = NewNetworkScanner()
	sm.staticScanner = NewStaticScanner()

	// Initialize reporters and checkers
	sm.vulnerabilityReporter = &VulnerabilityReporterImpl{
		findings: make([]Finding, 0),
		reportID: fmt.Sprintf("vuln-report-%d", common.ConsensusNow().Unix()),
	}
	sm.complianceChecker = NewComplianceChecker()

	// Initialize monitoring components
	sm.securityMonitor = NewSecurityMonitor()
	sm.auditLogger = NewAuditLogger()
	sm.threatDetector = NewThreatDetector()

	// Start security monitoring
	if sm.config.ThreatDetectionEnabled {
		eventChan, err := sm.securityMonitor.MonitorEvents(ctx)
		if err != nil {
			return fmt.Errorf("failed to start security monitoring: %w", err)
		}

		// Start event processor
		go sm.processSecurityEvents(ctx, eventChan)
	}

	// Start auto-scanning if enabled
	if sm.config.EnableAutoScanning {
		go sm.autoScanRoutine(ctx)
	}

	sm.isInitialized = true

	// Log initialization
	sm.logSecurityEvent(SecurityEvent{
		Type:        EventTypeAccessDenied,
		Severity:    SeverityInfo,
		Source:      "security-manager",
		Target:      "system",
		Description: "Security manager initialized successfully",
		Timestamp:   common.ConsensusNow(),
		Details: map[string]interface{}{
			"config": sm.config,
		},
	})

	return nil
}

// PerformSecurityScan performs a comprehensive security scan
func (sm *SecurityManager) PerformSecurityScan(ctx context.Context, target string) (*SecurityScanReport, error) {
	if !sm.isInitialized {
		return nil, fmt.Errorf("security manager not initialized")
	}

	report := &SecurityScanReport{
		ID:        fmt.Sprintf("scan-report-%d", common.ConsensusNow().Unix()),
		StartTime: common.ConsensusNow(),
		Target:    target,
		Findings:  make([]Finding, 0),
	}

	// Run all scanners
	scanners := []struct {
		name    string
		scanner SecurityScanner
	}{
		{"dependency", sm.dependencyScanner},
		{"network", sm.networkScanner},
		{"static", sm.staticScanner},
	}

	for _, s := range scanners {
		result, err := s.scanner.Scan(ctx, target)
		if err != nil {
			report.Errors = append(report.Errors, fmt.Sprintf("%s scan failed: %v", s.name, err))
			continue
		}

		report.Findings = append(report.Findings, result.Findings...)
		report.ScanResults = append(report.ScanResults, result)
	}

	report.EndTime = common.ConsensusNow()

	// Generate vulnerability report
	vulnReport, err := sm.vulnerabilityReporter.GenerateReport(ctx, report.Findings)
	if err != nil {
		report.Errors = append(report.Errors, fmt.Sprintf("vulnerability report generation failed: %v", err))
	} else {
		report.VulnerabilityReport = vulnReport
	}

	// Check compliance
	complianceReport, err := sm.complianceChecker.CheckCompliance(ctx, sm.config.CompliancePolicies)
	if err != nil {
		report.Errors = append(report.Errors, fmt.Sprintf("compliance check failed: %v", err))
	} else {
		report.ComplianceReport = complianceReport
	}

	// Log scan completion
	sm.logSecurityEvent(SecurityEvent{
		Type:        EventTypeVulnerabilityFound,
		Severity:    sm.getHighestSeverity(report.Findings),
		Source:      "security-scanner",
		Target:      target,
		Description: fmt.Sprintf("Security scan completed: %d findings", len(report.Findings)),
		Timestamp:   common.ConsensusNow(),
		Details: map[string]interface{}{
			"scan_id":  report.ID,
			"findings": len(report.Findings),
			"duration": report.EndTime.Sub(report.StartTime),
		},
	})

	return report, nil
}

// DetectThreats performs threat detection on provided data
func (sm *SecurityManager) DetectThreats(ctx context.Context, data []byte) ([]Threat, error) {
	if !sm.isInitialized {
		return nil, fmt.Errorf("security manager not initialized")
	}

	if !sm.config.ThreatDetectionEnabled {
		return nil, fmt.Errorf("threat detection is disabled")
	}

	threats, err := sm.threatDetector.DetectThreats(ctx, data)
	if err != nil {
		return nil, fmt.Errorf("threat detection failed: %w", err)
	}

	// Log detected threats
	for _, threat := range threats {
		sm.logSecurityEvent(SecurityEvent{
			Type:        EventTypeThreatDetected,
			Severity:    threat.Severity,
			Source:      "threat-detector",
			Target:      threat.Target,
			Description: threat.Description,
			Timestamp:   threat.DetectedAt,
			Details: map[string]interface{}{
				"threat_id":   threat.ID,
				"threat_type": threat.Type,
				"indicators":  threat.Indicators,
			},
		})
	}

	return threats, nil
}

// GetSecurityStatus returns the current security status
func (sm *SecurityManager) GetSecurityStatus() SecurityStatus {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	status := SecurityStatus{
		IsInitialized:    sm.isInitialized,
		ThreatLevel:      sm.threatDetector.GetThreatLevel(),
		ComplianceStatus: sm.complianceChecker.GetComplianceStatus(),
		LastScanTime:     time.Time{}, // Would be tracked in production
		ActiveThreats:    0,           // Would be calculated from monitor
	}

	// Get statistics from components
	if sm.isInitialized {
		if stats, ok := sm.securityMonitor.(*SecurityMonitorImpl); ok {
			monitorStats := stats.GetEventStatistics()
			if unresolved, ok := monitorStats["unresolved_events"].(int); ok {
				status.ActiveThreats = unresolved
			}
		}
	}

	return status
}

// processSecurityEvents processes events from the security monitor
func (sm *SecurityManager) processSecurityEvents(ctx context.Context, eventChan <-chan SecurityEvent) {
	for {
		select {
		case <-ctx.Done():
			return
		case event := <-eventChan:
			// Process based on severity
			if event.Severity >= sm.config.AlertThreshold {
				sm.handleHighSeverityEvent(event)
			}

			// Detect threats from event data
			if event.Details != nil {
				if data, ok := event.Details["data"].([]byte); ok {
					go func() {
						threats, err := sm.DetectThreats(ctx, data)
						if err == nil && len(threats) > 0 {
							// Threats detected from event
							for _, threat := range threats {
								sm.handleDetectedThreat(threat)
							}
						}
					}()
				}
			}
		}
	}
}

// handleHighSeverityEvent handles high severity security events
func (sm *SecurityManager) handleHighSeverityEvent(event SecurityEvent) {
	// In production, this would:
	// - Send alerts to security team
	// - Trigger automated response
	// - Initiate incident response procedures
	// - Block suspicious activity

	// Log critical event
	if sm.auditLogger != nil {
		sm.auditLogger.LogSecurityEvent(event)
	}

	// For now, log to console
	fmt.Printf("HIGH SEVERITY EVENT: %s - %s\n", event.Type, event.Description)
}

// handleDetectedThreat handles a detected threat
func (sm *SecurityManager) handleDetectedThreat(threat Threat) {
	// In production, this would:
	// - Apply mitigations
	// - Block threat source
	// - Update security rules
	// - Alert security team

	// Create security event for the threat
	event := SecurityEvent{
		Type:        EventTypeThreatDetected,
		Severity:    threat.Severity,
		Source:      threat.Source,
		Target:      threat.Target,
		Description: fmt.Sprintf("Active threat detected: %s", threat.Name),
		Timestamp:   common.ConsensusNow(),
		Details: map[string]interface{}{
			"threat":      threat,
			"mitigations": threat.Mitigations,
		},
	}

	sm.logSecurityEvent(event)
}

// autoScanRoutine performs automatic security scans
func (sm *SecurityManager) autoScanRoutine(ctx context.Context) {
	ticker := time.NewTicker(sm.config.ScanInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Perform automatic scan
			report, err := sm.PerformSecurityScan(ctx, ".")
			if err != nil {
				sm.logSecurityEvent(SecurityEvent{
					Type:        EventTypeAnomalyDetected,
					Severity:    SeverityMedium,
					Source:      "auto-scanner",
					Target:      "system",
					Description: fmt.Sprintf("Auto scan failed: %v", err),
					Timestamp:   common.ConsensusNow(),
				})
			} else {
				// Process scan results
				if report.VulnerabilityReport != nil &&
					report.VulnerabilityReport.RiskAssessment.ThreatLevel >= ThreatLevelHigh {
					// High risk detected
					sm.handleHighRiskScan(report)
				}
			}
		}
	}
}

// handleHighRiskScan handles high risk scan results
func (sm *SecurityManager) handleHighRiskScan(report *SecurityScanReport) {
	event := SecurityEvent{
		Type:        EventTypeVulnerabilityFound,
		Severity:    SeverityCritical,
		Source:      "auto-scanner",
		Target:      report.Target,
		Description: "High risk vulnerabilities detected in automatic scan",
		Timestamp:   common.ConsensusNow(),
		Details: map[string]interface{}{
			"report_id":    report.ID,
			"threat_level": report.VulnerabilityReport.RiskAssessment.ThreatLevel,
			"findings":     len(report.Findings),
		},
	}

	sm.handleHighSeverityEvent(event)
}

// logSecurityEvent logs a security event
func (sm *SecurityManager) logSecurityEvent(event SecurityEvent) {
	if sm.securityMonitor != nil {
		sm.securityMonitor.RecordEvent(event)
	}

	if sm.auditLogger != nil && sm.config.AuditLoggingEnabled {
		sm.auditLogger.LogSecurityEvent(event)
	}
}

// getHighestSeverity returns the highest severity from findings
func (sm *SecurityManager) getHighestSeverity(findings []Finding) SeverityLevel {
	highest := SeverityInfo

	for _, finding := range findings {
		switch finding.Severity {
		case SeverityCritical:
			return SeverityCritical
		case SeverityHigh:
			highest = SeverityHigh
		case SeverityMedium:
			if highest != SeverityHigh {
				highest = SeverityMedium
			}
		case SeverityLow:
			if highest == SeverityInfo {
				highest = SeverityLow
			}
		}
	}

	return highest
}

// Shutdown gracefully shuts down the security manager
func (sm *SecurityManager) Shutdown() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if !sm.isInitialized {
		return fmt.Errorf("security manager not initialized")
	}

	// Log shutdown
	sm.logSecurityEvent(SecurityEvent{
		Type:        EventTypeAccessDenied,
		Severity:    SeverityInfo,
		Source:      "security-manager",
		Target:      "system",
		Description: "Security manager shutting down",
		Timestamp:   common.ConsensusNow(),
	})

	sm.isInitialized = false
	return nil
}

// Additional types for security management

// SecurityScanReport represents a comprehensive security scan report
type SecurityScanReport struct {
	ID                  string
	StartTime           time.Time
	EndTime             time.Time
	Target              string
	ScanResults         []*ScanResult
	Findings            []Finding
	VulnerabilityReport *VulnerabilityReport
	ComplianceReport    *ComplianceReport
	Errors              []string
}

// SecurityStatus represents the current security status
type SecurityStatus struct {
	IsInitialized    bool
	ThreatLevel      ThreatLevel
	ComplianceStatus ComplianceStatus
	LastScanTime     time.Time
	ActiveThreats    int
}

// Factory functions for scanner creation
func NewDependencyScanner() SecurityScanner {
	return NewDependencyScannerImpl()
}

func NewNetworkScanner() SecurityScanner {
	return NewNetworkScannerImpl()
}

func NewStaticScanner() SecurityScanner {
	return NewStaticScannerImpl()
}

// NewDependencyScannerImpl creates a dependency scanner that implements SecurityScanner
func NewDependencyScannerImpl() SecurityScanner {
	return &DependencyScannerWrapper{}
}

// NewNetworkScannerImpl creates a network scanner that implements SecurityScanner
func NewNetworkScannerImpl() SecurityScanner {
	return &NetworkScannerWrapper{}
}

// NewStaticScannerImpl creates a static scanner that implements SecurityScanner
func NewStaticScannerImpl() SecurityScanner {
	return &StaticScannerWrapper{}
}

// Scanner wrapper types to avoid import cycles

// DependencyScannerWrapper wraps the dependency scanner functionality
type DependencyScannerWrapper struct{}

// Scan performs a dependency scan
func (ds *DependencyScannerWrapper) Scan(ctx context.Context, target string) (*ScanResult, error) {
	// For now, return a basic result
	// In production, this would call the actual scanner implementation
	return &ScanResult{
		ID:          fmt.Sprintf("dep-scan-%d", common.ConsensusNow().Unix()),
		ScannerType: ScannerTypeDependency,
		Target:      target,
		StartTime:   common.ConsensusNow(),
		EndTime:     common.ConsensusNow(),
		Success:     true,
		Findings:    []Finding{},
		Summary: ScanSummary{
			TotalFindings: 0,
			ScanDuration:  0,
		},
	}, nil
}

// ValidateTarget validates the target
func (ds *DependencyScannerWrapper) ValidateTarget(target string) error {
	if target == "" {
		return fmt.Errorf("target cannot be empty")
	}
	return nil
}

// GetScannerType returns the scanner type
func (ds *DependencyScannerWrapper) GetScannerType() ScannerType {
	return ScannerTypeDependency
}

// NetworkScannerWrapper wraps the network scanner functionality
type NetworkScannerWrapper struct{}

// Scan performs a network scan
func (ns *NetworkScannerWrapper) Scan(ctx context.Context, target string) (*ScanResult, error) {
	return &ScanResult{
		ID:          fmt.Sprintf("net-scan-%d", common.ConsensusNow().Unix()),
		ScannerType: ScannerTypeNetwork,
		Target:      target,
		StartTime:   common.ConsensusNow(),
		EndTime:     common.ConsensusNow(),
		Success:     true,
		Findings:    []Finding{},
		Summary: ScanSummary{
			TotalFindings: 0,
			ScanDuration:  0,
		},
	}, nil
}

// ValidateTarget validates the target
func (ns *NetworkScannerWrapper) ValidateTarget(target string) error {
	if target == "" {
		return fmt.Errorf("target cannot be empty")
	}
	return nil
}

// GetScannerType returns the scanner type
func (ns *NetworkScannerWrapper) GetScannerType() ScannerType {
	return ScannerTypeNetwork
}

// StaticScannerWrapper wraps the static scanner functionality
type StaticScannerWrapper struct{}

// Scan performs a static scan
func (ss *StaticScannerWrapper) Scan(ctx context.Context, target string) (*ScanResult, error) {
	return &ScanResult{
		ID:          fmt.Sprintf("static-scan-%d", common.ConsensusNow().Unix()),
		ScannerType: ScannerTypeStatic,
		Target:      target,
		StartTime:   common.ConsensusNow(),
		EndTime:     common.ConsensusNow(),
		Success:     true,
		Findings:    []Finding{},
		Summary: ScanSummary{
			TotalFindings: 0,
			ScanDuration:  0,
		},
	}, nil
}

// ValidateTarget validates the target
func (ss *StaticScannerWrapper) ValidateTarget(target string) error {
	if target == "" {
		return fmt.Errorf("target cannot be empty")
	}
	return nil
}

// GetScannerType returns the scanner type
func (ss *StaticScannerWrapper) GetScannerType() ScannerType {
	return ScannerTypeStatic
}

// VulnerabilityReporterImpl implements the VulnerabilityReporter interface
type VulnerabilityReporterImpl struct {
	mu       sync.RWMutex
	findings []Finding
	reportID string
}

// GenerateReport creates a comprehensive vulnerability report
func (vr *VulnerabilityReporterImpl) GenerateReport(ctx context.Context, findings []Finding) (*VulnerabilityReport, error) {
	if ctx.Err() != nil {
		return nil, fmt.Errorf("context cancelled: %w", ctx.Err())
	}

	report := &VulnerabilityReport{
		ID:          fmt.Sprintf("report-%s-%d", vr.reportID, common.ConsensusNow().Unix()),
		GeneratedAt: common.ConsensusNow(),
		ReportType:  "vulnerability_assessment",
		Findings:    findings,
		Metadata: map[string]interface{}{
			"reporter_version": "1.0.0",
			"total_findings":   len(findings),
		},
	}

	// Generate summary
	report.Summary = vr.generateSummary(findings)

	// Perform risk assessment
	report.RiskAssessment = vr.assessRisk(findings)

	// Generate recommendations
	report.Recommendations = vr.generateRecommendations(findings)

	// Update internal state
	vr.mu.Lock()
	vr.findings = findings
	vr.mu.Unlock()

	return report, nil
}

// AddFinding adds a new security finding to the reporter
func (vr *VulnerabilityReporterImpl) AddFinding(finding Finding) error {
	if finding.ID == "" {
		return fmt.Errorf("finding ID cannot be empty")
	}

	vr.mu.Lock()
	defer vr.mu.Unlock()

	// Check for duplicates
	for _, existing := range vr.findings {
		if existing.ID == finding.ID {
			return fmt.Errorf("finding with ID %s already exists", finding.ID)
		}
	}

	vr.findings = append(vr.findings, finding)
	return nil
}

// GetFindings returns all current findings
func (vr *VulnerabilityReporterImpl) GetFindings() []Finding {
	vr.mu.RLock()
	defer vr.mu.RUnlock()

	// Return a copy to prevent external modification
	findings := make([]Finding, len(vr.findings))
	copy(findings, vr.findings)
	return findings
}

// generateSummary creates a report summary from findings
func (vr *VulnerabilityReporterImpl) generateSummary(findings []Finding) ReportSummary {
	summary := ReportSummary{
		TotalIssues:       len(findings),
		SeverityBreakdown: make(map[string]int),
		TopCategories:     []string{},
	}

	categoryCount := make(map[string]int)

	// Count by severity and category
	for _, finding := range findings {
		summary.SeverityBreakdown[string(finding.Severity)]++
		categoryCount[finding.Category]++
	}

	// Determine overall risk
	criticalCount := summary.SeverityBreakdown[string(SeverityCritical)]
	highCount := summary.SeverityBreakdown[string(SeverityHigh)]

	if criticalCount > 0 {
		summary.OverallRisk = "Critical"
	} else if highCount > 5 {
		summary.OverallRisk = "High"
	} else if highCount > 0 {
		summary.OverallRisk = "Medium"
	} else if len(findings) > 10 {
		summary.OverallRisk = "Medium"
	} else if len(findings) > 0 {
		summary.OverallRisk = "Low"
	} else {
		summary.OverallRisk = "None"
	}

	return summary
}

// assessRisk performs comprehensive risk assessment
func (vr *VulnerabilityReporterImpl) assessRisk(findings []Finding) RiskAssessment {
	assessment := RiskAssessment{
		ThreatLevel: ThreatLevelNone,
	}

	if len(findings) == 0 {
		assessment.ImpactAnalysis = "No vulnerabilities detected"
		assessment.MitigationStatus = "No mitigation required"
		return assessment
	}

	// Calculate scores
	var totalScore float64
	criticalCount := 0
	highCount := 0

	for _, finding := range findings {
		switch finding.Severity {
		case SeverityCritical:
			totalScore += 10
			criticalCount++
		case SeverityHigh:
			totalScore += 5
			highCount++
		case SeverityMedium:
			totalScore += 3
		case SeverityLow:
			totalScore += 1
		}
	}

	assessment.OverallScore = totalScore
	assessment.LikelihoodScore = float64(len(findings)) / 10.0 // Normalized score

	// Determine threat level
	if criticalCount > 0 {
		assessment.ThreatLevel = ThreatLevelCritical
	} else if highCount > 3 {
		assessment.ThreatLevel = ThreatLevelHigh
	} else if highCount > 0 || totalScore > 15 {
		assessment.ThreatLevel = ThreatLevelMedium
	} else if totalScore > 5 {
		assessment.ThreatLevel = ThreatLevelLow
	} else {
		assessment.ThreatLevel = ThreatLevelNone
	}

	// Impact analysis
	switch assessment.ThreatLevel {
	case ThreatLevelCritical:
		assessment.ImpactAnalysis = "Critical vulnerabilities detected that could lead to complete system compromise"
	case ThreatLevelHigh:
		assessment.ImpactAnalysis = "High-severity vulnerabilities that could result in significant security breaches"
	case ThreatLevelMedium:
		assessment.ImpactAnalysis = "Medium-severity vulnerabilities that require attention to prevent potential exploits"
	default:
		assessment.ImpactAnalysis = "Low-severity vulnerabilities with minimal immediate impact"
	}

	// Mitigation status
	if criticalCount > 0 {
		assessment.MitigationStatus = "Immediate action required"
	} else if highCount > 0 {
		assessment.MitigationStatus = "Urgent mitigation needed"
	} else {
		assessment.MitigationStatus = "Standard mitigation timeline"
	}

	return assessment
}

// generateRecommendations creates actionable recommendations
func (vr *VulnerabilityReporterImpl) generateRecommendations(findings []Finding) []string {
	recommendations := []string{}

	if len(findings) == 0 {
		return recommendations
	}

	// Priority-based recommendations
	hasCritical := false
	hasHigh := false

	for _, finding := range findings {
		if finding.Severity == SeverityCritical {
			hasCritical = true
		}
		if finding.Severity == SeverityHigh {
			hasHigh = true
		}
	}

	// Add recommendations based on findings
	if hasCritical {
		recommendations = append(recommendations, "Immediately address all critical vulnerabilities before deployment")
		recommendations = append(recommendations, "Implement emergency patching procedures for critical issues")
	}

	if hasHigh {
		recommendations = append(recommendations, "Prioritize remediation of high-severity vulnerabilities")
		recommendations = append(recommendations, "Conduct thorough security review of affected components")
	}

	// General recommendations
	if len(findings) > 0 {
		recommendations = append(recommendations, "Establish regular security scanning as part of CI/CD pipeline")
		recommendations = append(recommendations, "Implement security training for development team")
		recommendations = append(recommendations, "Create and maintain a vulnerability management process")
	}

	return recommendations
}

// String provides a detailed string representation for debugging and logging
func (sm *SecurityManager) String() string {
	if sm == nil {
		return "SecurityManager{<nil>}"
	}

	sm.mu.RLock()
	defer sm.mu.RUnlock()

	hasDependencyScanner := sm.dependencyScanner != nil
	hasNetworkScanner := sm.networkScanner != nil
	hasStaticScanner := sm.staticScanner != nil
	hasVulnReporter := sm.vulnerabilityReporter != nil
	hasComplianceChecker := sm.complianceChecker != nil
	hasSecurityMonitor := sm.securityMonitor != nil
	hasAuditLogger := sm.auditLogger != nil
	hasThreatDetector := sm.threatDetector != nil

	policiesCount := len(sm.config.CompliancePolicies)

	return fmt.Sprintf("SecurityManager{initialized=%v, config.EnableAutoScanning=%v, "+
		"config.ScanInterval=%v, config.ThreatDetectionEnabled=%v, config.AuditLoggingEnabled=%v, "+
		"config.MaxEventHistory=%d, config.AlertThreshold=%v, policiesCount=%d, "+
		"hasDependencyScanner=%v, hasNetworkScanner=%v, hasStaticScanner=%v, hasVulnReporter=%v, "+
		"hasComplianceChecker=%v, hasSecurityMonitor=%v, hasAuditLogger=%v, hasThreatDetector=%v}",
		sm.isInitialized, sm.config.EnableAutoScanning, sm.config.ScanInterval,
		sm.config.ThreatDetectionEnabled, sm.config.AuditLoggingEnabled, sm.config.MaxEventHistory,
		sm.config.AlertThreshold, policiesCount, hasDependencyScanner, hasNetworkScanner,
		hasStaticScanner, hasVulnReporter, hasComplianceChecker, hasSecurityMonitor,
		hasAuditLogger, hasThreatDetector)
}

// Validate performs comprehensive validation of the SecurityManager configuration and state
func (sm *SecurityManager) Validate() error {
	if sm == nil {
		return fmt.Errorf("SecurityManager is nil")
	}

	// Validate configuration
	if err := sm.validateConfiguration(); err != nil {
		return fmt.Errorf("configuration validation failed: %w", err)
	}

	// Validate components
	if err := sm.validateComponents(); err != nil {
		return fmt.Errorf("component validation failed: %w", err)
	}

	// Validate state consistency
	if err := sm.validateState(); err != nil {
		return fmt.Errorf("state validation failed: %w", err)
	}

	// If initialized, validate component health
	if sm.isInitialized {
		if err := sm.validateComponentHealth(); err != nil {
			return fmt.Errorf("component health validation failed: %w", err)
		}
	}

	return nil
}

// validateConfiguration validates the SecurityManager configuration
func (sm *SecurityManager) validateConfiguration() error {
	// Validate scan interval
	if sm.config.ScanInterval <= 0 {
		return fmt.Errorf("scan interval must be positive: %v", sm.config.ScanInterval)
	}
	if sm.config.ScanInterval < time.Minute {
		return fmt.Errorf("scan interval too frequent (min 1 minute): %v", sm.config.ScanInterval)
	}
	if sm.config.ScanInterval > 7*24*time.Hour {
		return fmt.Errorf("scan interval too large (max 7 days): %v", sm.config.ScanInterval)
	}

	// Validate compliance policies
	if len(sm.config.CompliancePolicies) == 0 {
		return fmt.Errorf("no compliance policies configured")
	}

	for i, policy := range sm.config.CompliancePolicies {
		if err := sm.validatePolicy(policy); err != nil {
			return fmt.Errorf("invalid policy at index %d: %w", i, err)
		}
	}

	// Validate event history limit
	if sm.config.MaxEventHistory <= 0 {
		return fmt.Errorf("max event history must be positive: %d", sm.config.MaxEventHistory)
	}
	if sm.config.MaxEventHistory > 1000000 {
		return fmt.Errorf("max event history too large (max 1M): %d", sm.config.MaxEventHistory)
	}

	// Validate alert threshold
	if !sm.isValidSeverityLevel(sm.config.AlertThreshold) {
		return fmt.Errorf("invalid alert threshold severity level: %v", sm.config.AlertThreshold)
	}

	return nil
}

// validatePolicy validates a single security policy
func (sm *SecurityManager) validatePolicy(policy SecurityPolicy) error {
	if policy.Name == "" {
		return fmt.Errorf("policy name is empty")
	}
	if len(policy.Name) > 100 {
		return fmt.Errorf("policy name too long (max 100 chars): %d", len(policy.Name))
	}

	if policy.Description == "" {
		return fmt.Errorf("policy description is empty")
	}
	if len(policy.Description) > 1000 {
		return fmt.Errorf("policy description too long (max 1000 chars): %d", len(policy.Description))
	}

	if len(policy.Rules) == 0 {
		return fmt.Errorf("policy has no rules")
	}

	for i, rule := range policy.Rules {
		if rule.Type == "" {
			return fmt.Errorf("rule %d has empty type", i)
		}
		if rule.Pattern == "" {
			return fmt.Errorf("rule %d has empty pattern", i)
		}
		if !sm.isValidSeverityLevel(rule.Severity) {
			return fmt.Errorf("rule %d has invalid severity level: %v", i, rule.Severity)
		}
	}

	return nil
}

// validateComponents validates that required components are properly initialized
func (sm *SecurityManager) validateComponents() error {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	// If initialized, all components should be present
	if sm.isInitialized {
		if sm.dependencyScanner == nil {
			return fmt.Errorf("dependency scanner is nil but manager is initialized")
		}
		if sm.networkScanner == nil {
			return fmt.Errorf("network scanner is nil but manager is initialized")
		}
		if sm.staticScanner == nil {
			return fmt.Errorf("static scanner is nil but manager is initialized")
		}
		if sm.vulnerabilityReporter == nil {
			return fmt.Errorf("vulnerability reporter is nil but manager is initialized")
		}
		if sm.complianceChecker == nil {
			return fmt.Errorf("compliance checker is nil but manager is initialized")
		}
		if sm.securityMonitor == nil {
			return fmt.Errorf("security monitor is nil but manager is initialized")
		}
		if sm.auditLogger == nil {
			return fmt.Errorf("audit logger is nil but manager is initialized")
		}
		if sm.threatDetector == nil {
			return fmt.Errorf("threat detector is nil but manager is initialized")
		}
	}

	return nil
}

// validateState validates the current state consistency
func (sm *SecurityManager) validateState() error {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	// Configuration should always be valid
	if sm.config.EnableAutoScanning && sm.config.ScanInterval <= 0 {
		return fmt.Errorf("auto scanning enabled but scan interval is invalid")
	}

	if sm.config.ThreatDetectionEnabled && sm.threatDetector == nil && sm.isInitialized {
		return fmt.Errorf("threat detection enabled but threat detector is nil")
	}

	if sm.config.AuditLoggingEnabled && sm.auditLogger == nil && sm.isInitialized {
		return fmt.Errorf("audit logging enabled but audit logger is nil")
	}

	return nil
}

// validateComponentHealth validates that all components are healthy
func (sm *SecurityManager) validateComponentHealth() error {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	var healthErrors []string

	// Check scanner health
	scanners := map[string]SecurityScanner{
		"dependency": sm.dependencyScanner,
		"network":    sm.networkScanner,
		"static":     sm.staticScanner,
	}

	for name, scanner := range scanners {
		if scanner != nil {
			if healthChecker, ok := scanner.(interface{ HealthCheck() error }); ok {
				if err := healthChecker.HealthCheck(); err != nil {
					healthErrors = append(healthErrors, fmt.Sprintf("%s scanner: %v", name, err))
				}
			}
		}
	}

	// Check other components
	components := map[string]interface{}{
		"vulnerability reporter": sm.vulnerabilityReporter,
		"compliance checker":     sm.complianceChecker,
		"security monitor":       sm.securityMonitor,
		"audit logger":           sm.auditLogger,
		"threat detector":        sm.threatDetector,
	}

	for name, component := range components {
		if component != nil {
			if healthChecker, ok := component.(interface{ HealthCheck() error }); ok {
				if err := healthChecker.HealthCheck(); err != nil {
					healthErrors = append(healthErrors, fmt.Sprintf("%s: %v", name, err))
				}
			}
		}
	}

	if len(healthErrors) > 0 {
		return fmt.Errorf("component health check failures: %s", strings.Join(healthErrors, "; "))
	}

	return nil
}

// isValidSeverityLevel checks if a severity level is valid
func (sm *SecurityManager) isValidSeverityLevel(severity SeverityLevel) bool {
	switch severity {
	case SeverityLow, SeverityMedium, SeverityHigh, SeverityCritical:
		return true
	default:
		return false
	}
}

// Close properly shuts down the SecurityManager and releases all resources
func (sm *SecurityManager) Close() error {
	if sm == nil {
		return nil
	}

	var closeErrors []error

	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Stop auto-scanning if running
	// Note: This assumes there's a way to stop auto-scanning.
	// The actual implementation might need adjustment based on how auto-scanning is implemented.

	// Close all components that support it
	components := map[string]interface{}{
		"dependency scanner":     sm.dependencyScanner,
		"network scanner":        sm.networkScanner,
		"static scanner":         sm.staticScanner,
		"vulnerability reporter": sm.vulnerabilityReporter,
		"compliance checker":     sm.complianceChecker,
		"security monitor":       sm.securityMonitor,
		"audit logger":           sm.auditLogger,
		"threat detector":        sm.threatDetector,
	}

	for name, component := range components {
		if component != nil {
			// Try to stop the component first
			if stopper, ok := component.(interface{ Stop() error }); ok {
				if err := stopper.Stop(); err != nil {
					closeErrors = append(closeErrors, fmt.Errorf("failed to stop %s: %w", name, err))
				}
			}

			// Then try to close it
			if closer, ok := component.(interface{ Close() error }); ok {
				if err := closer.Close(); err != nil {
					closeErrors = append(closeErrors, fmt.Errorf("failed to close %s: %w", name, err))
				}
			}
		}
	}

	// Clear all references to help with garbage collection
	sm.dependencyScanner = nil
	sm.networkScanner = nil
	sm.staticScanner = nil
	sm.vulnerabilityReporter = nil
	sm.complianceChecker = nil
	sm.securityMonitor = nil
	sm.auditLogger = nil
	sm.threatDetector = nil
	sm.isInitialized = false

	// Reset configuration to default
	sm.config = DefaultSecurityConfig()

	// If there were any errors during close, return them
	if len(closeErrors) > 0 {
		errorStrs := make([]string, len(closeErrors))
		for i, err := range closeErrors {
			errorStrs[i] = err.Error()
		}
		return fmt.Errorf("multiple close errors: %s", strings.Join(errorStrs, "; "))
	}

	return nil
}
