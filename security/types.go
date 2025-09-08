package security

import (
	"time"
)

// ScannerType represents different types of security scanners
type ScannerType string

const (
	// ScannerTypeDependency represents dependency vulnerability scanning
	ScannerTypeDependency ScannerType = "dependency"
	// ScannerTypeStatic represents static code analysis
	ScannerTypeStatic ScannerType = "static"
	// ScannerTypeNetwork represents network security scanning
	ScannerTypeNetwork ScannerType = "network"
	// ScannerTypeDynamic represents dynamic application security testing
	ScannerTypeDynamic ScannerType = "dynamic"
	// ScannerTypeContainer represents container security scanning
	ScannerTypeContainer ScannerType = "container"
)

// SeverityLevel represents the severity of a security finding
type SeverityLevel string

const (
	// SeverityCritical represents critical severity findings
	SeverityCritical SeverityLevel = "critical"
	// SeverityHigh represents high severity findings
	SeverityHigh SeverityLevel = "high"
	// SeverityMedium represents medium severity findings
	SeverityMedium SeverityLevel = "medium"
	// SeverityLow represents low severity findings
	SeverityLow SeverityLevel = "low"
	// SeverityInfo represents informational findings
	SeverityInfo SeverityLevel = "info"
)

// ComplianceStatus represents the compliance validation status
type ComplianceStatus string

const (
	// ComplianceStatusCompliant represents full compliance
	ComplianceStatusCompliant ComplianceStatus = "compliant"
	// ComplianceStatusPartial represents partial compliance
	ComplianceStatusPartial ComplianceStatus = "partial"
	// ComplianceStatusNonCompliant represents non-compliance
	ComplianceStatusNonCompliant ComplianceStatus = "non_compliant"
	// ComplianceStatusUnknown represents unknown compliance status
	ComplianceStatusUnknown ComplianceStatus = "unknown"
)

// ThreatLevel represents the current threat level
type ThreatLevel string

const (
	// ThreatLevelCritical represents critical threat level
	ThreatLevelCritical ThreatLevel = "critical"
	// ThreatLevelHigh represents high threat level
	ThreatLevelHigh ThreatLevel = "high"
	// ThreatLevelMedium represents medium threat level
	ThreatLevelMedium ThreatLevel = "medium"
	// ThreatLevelLow represents low threat level
	ThreatLevelLow ThreatLevel = "low"
	// ThreatLevelNone represents no detected threats
	ThreatLevelNone ThreatLevel = "none"
)

// SecurityEventType represents different types of security events
type SecurityEventType string

const (
	// EventTypeAccessDenied represents access denial events
	EventTypeAccessDenied SecurityEventType = "access_denied"
	// EventTypeAuthFailure represents authentication failure events
	EventTypeAuthFailure SecurityEventType = "auth_failure"
	// EventTypeThreatDetected represents threat detection events
	EventTypeThreatDetected SecurityEventType = "threat_detected"
	// EventTypeVulnerabilityFound represents vulnerability discovery events
	EventTypeVulnerabilityFound SecurityEventType = "vulnerability_found"
	// EventTypePolicyViolation represents policy violation events
	EventTypePolicyViolation SecurityEventType = "policy_violation"
	// EventTypeAnomalyDetected represents anomaly detection events
	EventTypeAnomalyDetected SecurityEventType = "anomaly_detected"
	// EventTypeUnauthorizedAccess represents unauthorized access attempts
	EventTypeUnauthorizedAccess SecurityEventType = "unauthorized_access"
	// EventTypeResponseAction represents automated response actions
	EventTypeResponseAction SecurityEventType = "response_action"
	// EventTypeAlert represents security alerts
	EventTypeAlert SecurityEventType = "alert"
	// EventTypeLog represents log events
	EventTypeLog SecurityEventType = "log"
	// EventTypeIncident represents security incidents
	EventTypeIncident SecurityEventType = "incident"
)

// ScanResult represents the result of a security scan
type ScanResult struct {
	ID           string      `json:"id"`
	ScannerType  ScannerType `json:"scanner_type"`
	Target       string      `json:"target"`
	StartTime    time.Time   `json:"start_time"`
	EndTime      time.Time   `json:"end_time"`
	Findings     []Finding   `json:"findings"`
	Summary      ScanSummary `json:"summary"`
	Success      bool        `json:"success"`
	ErrorMessage string      `json:"error_message,omitempty"`
}

// Finding represents a security finding or vulnerability
type Finding struct {
	ID          string                 `json:"id"`
	Title       string                 `json:"title"`
	Description string                 `json:"description"`
	Severity    SeverityLevel          `json:"severity"`
	Category    string                 `json:"category"`
	Location    string                 `json:"location"`
	Evidence    string                 `json:"evidence"`
	Remediation string                 `json:"remediation"`
	References  []string               `json:"references"`
	Metadata    map[string]interface{} `json:"metadata"`
	FoundAt     time.Time              `json:"found_at"`
	Verified    bool                   `json:"verified"`
}

// ScanSummary provides a summary of scan results
type ScanSummary struct {
	TotalFindings int           `json:"total_findings"`
	CriticalCount int           `json:"critical_count"`
	HighCount     int           `json:"high_count"`
	MediumCount   int           `json:"medium_count"`
	LowCount      int           `json:"low_count"`
	InfoCount     int           `json:"info_count"`
	RiskScore     float64       `json:"risk_score"`
	ScanDuration  time.Duration `json:"scan_duration"`
}

// VulnerabilityReport represents a comprehensive vulnerability report
type VulnerabilityReport struct {
	ID              string                 `json:"id"`
	GeneratedAt     time.Time              `json:"generated_at"`
	ReportType      string                 `json:"report_type"`
	Summary         ReportSummary          `json:"summary"`
	Findings        []Finding              `json:"findings"`
	RiskAssessment  RiskAssessment         `json:"risk_assessment"`
	Recommendations []string               `json:"recommendations"`
	Metadata        map[string]interface{} `json:"metadata"`
}

// ReportSummary provides a summary for reports
type ReportSummary struct {
	TotalIssues       int            `json:"total_issues"`
	SeverityBreakdown map[string]int `json:"severity_breakdown"`
	TopCategories     []string       `json:"top_categories"`
	OverallRisk       string         `json:"overall_risk"`
}

// RiskAssessment represents security risk assessment
type RiskAssessment struct {
	OverallScore     float64     `json:"overall_score"`
	ThreatLevel      ThreatLevel `json:"threat_level"`
	ImpactAnalysis   string      `json:"impact_analysis"`
	LikelihoodScore  float64     `json:"likelihood_score"`
	MitigationStatus string      `json:"mitigation_status"`
}

// SecurityPolicy represents a security policy definition
type SecurityPolicy struct {
	ID          string                 `json:"id"`
	Name        string                 `json:"name"`
	Description string                 `json:"description"`
	Category    string                 `json:"category"`
	Rules       []PolicyRule           `json:"rules"`
	Enabled     bool                   `json:"enabled"`
	CreatedAt   time.Time              `json:"created_at"`
	UpdatedAt   time.Time              `json:"updated_at"`
	Metadata    map[string]interface{} `json:"metadata"`
}

// PolicyRule represents a single rule within a security policy
type PolicyRule struct {
	ID         string                 `json:"id"`
	Name       string                 `json:"name"`
	Type       string                 `json:"type"`
	Pattern    string                 `json:"pattern"`
	Condition  string                 `json:"condition"`
	Action     string                 `json:"action"`
	Severity   SeverityLevel          `json:"severity"`
	Parameters map[string]interface{} `json:"parameters"`
}

// ComplianceReport represents a compliance validation report
type ComplianceReport struct {
	ID              string           `json:"id"`
	GeneratedAt     time.Time        `json:"generated_at"`
	Status          ComplianceStatus `json:"status"`
	PolicyResults   []PolicyResult   `json:"policy_results"`
	ComplianceScore float64          `json:"compliance_score"`
	FailedPolicies  []string         `json:"failed_policies"`
	Recommendations []string         `json:"recommendations"`
}

// PolicyResult represents the result of a policy validation
type PolicyResult struct {
	PolicyID   string           `json:"policy_id"`
	PolicyName string           `json:"policy_name"`
	Status     ComplianceStatus `json:"status"`
	Violations []string         `json:"violations"`
	CheckedAt  time.Time        `json:"checked_at"`
}

// SecurityEvent represents a security-related event
type SecurityEvent struct {
	ID          string                 `json:"id"`
	Type        SecurityEventType      `json:"type"`
	Severity    SeverityLevel          `json:"severity"`
	Source      string                 `json:"source"`
	Target      string                 `json:"target"`
	Description string                 `json:"description"`
	Timestamp   time.Time              `json:"timestamp"`
	UserID      string                 `json:"user_id,omitempty"`
	IPAddress   string                 `json:"ip_address,omitempty"`
	Details     map[string]interface{} `json:"details"`
	Resolved    bool                   `json:"resolved"`
}

// AccessAttempt represents an access attempt event
type AccessAttempt struct {
	ID          string    `json:"id"`
	UserID      string    `json:"user_id"`
	Resource    string    `json:"resource"`
	Action      string    `json:"action"`
	IPAddress   string    `json:"ip_address"`
	UserAgent   string    `json:"user_agent"`
	Success     bool      `json:"success"`
	Reason      string    `json:"reason,omitempty"`
	AttemptedAt time.Time `json:"attempted_at"`
}

// AuditLog represents an audit log entry
type AuditLog struct {
	ID        string                 `json:"id"`
	EventType string                 `json:"event_type"`
	UserID    string                 `json:"user_id"`
	Action    string                 `json:"action"`
	Resource  string                 `json:"resource"`
	Result    string                 `json:"result"`
	IPAddress string                 `json:"ip_address"`
	UserAgent string                 `json:"user_agent"`
	Details   map[string]interface{} `json:"details"`
	Timestamp time.Time              `json:"timestamp"`
}

// Threat represents a detected security threat
type Threat struct {
	ID          string                 `json:"id"`
	Type        string                 `json:"type"`
	Name        string                 `json:"name"`
	Description string                 `json:"description"`
	Severity    SeverityLevel          `json:"severity"`
	Source      string                 `json:"source"`
	Target      string                 `json:"target"`
	Indicators  []string               `json:"indicators"`
	Mitigations []string               `json:"mitigations"`
	DetectedAt  time.Time              `json:"detected_at"`
	Metadata    map[string]interface{} `json:"metadata"`
}

// Pattern represents a pattern for threat analysis
type Pattern struct {
	ID         string                 `json:"id"`
	Name       string                 `json:"name"`
	Type       string                 `json:"type"`
	Data       []byte                 `json:"data"`
	Frequency  int                    `json:"frequency"`
	LastSeen   time.Time              `json:"last_seen"`
	Attributes map[string]interface{} `json:"attributes"`
}

// ThreatAnalysis represents the result of threat pattern analysis
type ThreatAnalysis struct {
	ID             string      `json:"id"`
	PatternID      string      `json:"pattern_id"`
	ThreatLevel    ThreatLevel `json:"threat_level"`
	Confidence     float64     `json:"confidence"`
	Anomalies      []string    `json:"anomalies"`
	RiskIndicators []string    `json:"risk_indicators"`
	AnalyzedAt     time.Time   `json:"analyzed_at"`
}
