package security

import (
	"context"
	"time"
)

// SecurityScanner defines the interface for all security scanning operations
type SecurityScanner interface {
	// Scan performs a security scan on the given target
	Scan(ctx context.Context, target string) (*ScanResult, error)
	// ValidateTarget validates if the target is safe to scan
	ValidateTarget(target string) error
	// GetScannerType returns the type of scanner
	GetScannerType() ScannerType
}

// VulnerabilityReporter defines the interface for vulnerability reporting
type VulnerabilityReporter interface {
	// GenerateReport creates a comprehensive vulnerability report
	GenerateReport(ctx context.Context, findings []Finding) (*VulnerabilityReport, error)
	// AddFinding adds a new security finding to the reporter
	AddFinding(finding Finding) error
	// GetFindings returns all current findings
	GetFindings() []Finding
}

// ComplianceChecker defines the interface for compliance validation
type ComplianceChecker interface {
	// CheckCompliance validates compliance against security policies
	CheckCompliance(ctx context.Context, policies []SecurityPolicy) (*ComplianceReport, error)
	// ValidatePolicy validates a single security policy
	ValidatePolicy(policy SecurityPolicy) error
	// GetComplianceStatus returns current compliance status
	GetComplianceStatus() ComplianceStatus
}

// SecurityMonitor defines the interface for security event monitoring
type SecurityMonitor interface {
	// MonitorEvents monitors security events in real-time
	MonitorEvents(ctx context.Context) (<-chan SecurityEvent, error)
	// RecordEvent records a security event
	RecordEvent(event SecurityEvent) error
	// GetEventHistory returns historical security events
	GetEventHistory(since time.Time) ([]SecurityEvent, error)
}

// AuditLogger defines the interface for security audit logging
type AuditLogger interface {
	// LogSecurityEvent logs a security-related event
	LogSecurityEvent(event SecurityEvent) error
	// LogAccessAttempt logs an access attempt
	LogAccessAttempt(attempt AccessAttempt) error
	// GetAuditLogs retrieves audit logs for a time period
	GetAuditLogs(start, end time.Time) ([]AuditLog, error)
}

// ThreatDetector defines the interface for threat detection
type ThreatDetector interface {
	// DetectThreats analyzes for potential security threats
	DetectThreats(ctx context.Context, data []byte) ([]Threat, error)
	// AnalyzePattern analyzes patterns for anomalies
	AnalyzePattern(pattern Pattern) (*ThreatAnalysis, error)
	// GetThreatLevel returns current threat level
	GetThreatLevel() ThreatLevel
}
