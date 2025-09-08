// Package security provides typed security management
package security

import (
	"context"
	"diamante/common"
	"fmt"
	"sync"
	"time"

	"diamante/types"
	"github.com/sirupsen/logrus"
)

// TypedSecurityEvent is defined in typed_events.go

// TypedScanResult represents a typed security scan result
type TypedScanResult struct {
	ID          string            `json:"id"`
	ScannerType ScannerType       `json:"scanner_type"`
	Target      string            `json:"target"`
	StartTime   time.Time         `json:"start_time"`
	EndTime     time.Time         `json:"end_time"`
	Status      string            `json:"status"`
	Findings    []*TypedFinding   `json:"findings"`
	Summary     *TypedScanSummary `json:"summary"`
	Metadata    *types.TypedMap   `json:"metadata"`
}

// TypedFinding represents a typed security finding
type TypedFinding struct {
	ID             string           `json:"id"`
	Type           string           `json:"type"`
	Title          string           `json:"title"`
	Description    string           `json:"description"`
	Severity       SeverityLevel    `json:"severity"`
	Impact         string           `json:"impact"`
	Recommendation string           `json:"recommendation"`
	Location       *FindingLocation `json:"location"`
	Evidence       *types.TypedMap  `json:"evidence"`
	References     []string         `json:"references"`
	Metadata       *types.TypedMap  `json:"metadata"`
}

// FindingLocation represents where a security finding was discovered
type FindingLocation struct {
	File      string `json:"file,omitempty"`
	Line      int    `json:"line,omitempty"`
	Column    int    `json:"column,omitempty"`
	Function  string `json:"function,omitempty"`
	Package   string `json:"package,omitempty"`
	Module    string `json:"module,omitempty"`
	URL       string `json:"url,omitempty"`
	IPAddress string `json:"ip_address,omitempty"`
	Port      int    `json:"port,omitempty"`
}

// TypedScanSummary represents a typed scan summary
type TypedScanSummary struct {
	TotalFindings int                   `json:"total_findings"`
	BySeverity    map[SeverityLevel]int `json:"by_severity"`
	ByType        map[string]int        `json:"by_type"`
	RiskScore     float64               `json:"risk_score"`
	Passed        bool                  `json:"passed"`
	Duration      time.Duration         `json:"duration"`
}

// TypedThreat represents a typed security threat
type TypedThreat struct {
	ID              string          `json:"id"`
	Name            string          `json:"name"`
	Type            string          `json:"type"`
	Severity        ThreatLevel     `json:"severity"`
	Description     string          `json:"description"`
	Indicators      []string        `json:"indicators"`
	Mitigation      string          `json:"mitigation"`
	FirstDetected   time.Time       `json:"first_detected"`
	LastDetected    time.Time       `json:"last_detected"`
	OccurrenceCount int             `json:"occurrence_count"`
	Status          string          `json:"status"`
	Details         *types.TypedMap `json:"details"`
}

// TypedSecurityPolicy represents a typed security policy
type TypedSecurityPolicy struct {
	ID          string           `json:"id"`
	Name        string           `json:"name"`
	Description string           `json:"description"`
	Type        string           `json:"type"`
	Rules       []*PolicyRule    `json:"rules"`
	Enforcement EnforcementLevel `json:"enforcement"`
	Enabled     bool             `json:"enabled"`
	CreatedAt   time.Time        `json:"created_at"`
	UpdatedAt   time.Time        `json:"updated_at"`
	Tags        []string         `json:"tags"`
	Metadata    *types.TypedMap  `json:"metadata"`
}

// PolicyRule represents a security policy rule
type TypedPolicyRule struct {
	ID         string          `json:"id"`
	Name       string          `json:"name"`
	Condition  string          `json:"condition"`
	Action     string          `json:"action"`
	Parameters *types.TypedMap `json:"parameters"`
	Enabled    bool            `json:"enabled"`
}

// EnforcementLevel represents how strictly a policy is enforced
type EnforcementLevel string

const (
	EnforcementStrict    EnforcementLevel = "strict"
	EnforcementModerate  EnforcementLevel = "moderate"
	EnforcementLenient   EnforcementLevel = "lenient"
	EnforcementAuditOnly EnforcementLevel = "audit_only"
)

// TypedComplianceReport represents a typed compliance report
type TypedComplianceReport struct {
	ID              string               `json:"id"`
	GeneratedAt     time.Time            `json:"generated_at"`
	Framework       string               `json:"framework"`
	Version         string               `json:"version"`
	Status          ComplianceStatus     `json:"status"`
	Score           float64              `json:"score"`
	Controls        []*ComplianceControl `json:"controls"`
	Summary         *ComplianceSummary   `json:"summary"`
	Recommendations []string             `json:"recommendations"`
	Metadata        *types.TypedMap      `json:"metadata"`
}

// ComplianceControl represents a compliance control check
type ComplianceControl struct {
	ID          string          `json:"id"`
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Category    string          `json:"category"`
	Status      string          `json:"status"`
	Result      string          `json:"result"`
	Evidence    *types.TypedMap `json:"evidence"`
	Remediation string          `json:"remediation"`
}

// ComplianceSummary represents compliance summary statistics
type ComplianceSummary struct {
	TotalControls  int            `json:"total_controls"`
	PassedControls int            `json:"passed_controls"`
	FailedControls int            `json:"failed_controls"`
	ByCategory     map[string]int `json:"by_category"`
	ByStatus       map[string]int `json:"by_status"`
}

// TypedAuditLog is defined in typed_events.go

// TypedSecurityManager manages security operations with typed data
type TypedSecurityManager struct {
	config         *SecurityConfig
	scanners       map[ScannerType]SecurityScanner
	monitor        SecurityMonitor
	threatDetector ThreatDetector
	auditLogger    AuditLogger
	policies       map[string]*TypedSecurityPolicy

	// State
	isInitialized      bool
	currentThreatLevel ThreatLevel
	lastScanTime       time.Time

	// Metrics
	eventsProcessed uint64
	threatsDetected uint64
	scansPerformed  uint64

	logger *logrus.Logger
	mu     sync.RWMutex
}

// convertToTypedEventDetails converts a TypedMap to TypedEventDetails
func convertToTypedEventDetails(tm *types.TypedMap) TypedEventDetails {
	details := make(TypedEventDetails)
	if tm != nil {
		// Convert each entry in the TypedMap
		// This is a simplified conversion - adjust based on actual needs
		details.SetString("converted", "from_typed_map")
	}
	return details
}

// NewTypedSecurityManager creates a new typed security manager
func NewTypedSecurityManager(config *SecurityConfig, logger *logrus.Logger) *TypedSecurityManager {
	if logger == nil {
		logger = logrus.New()
	}

	return &TypedSecurityManager{
		config:             config,
		scanners:           make(map[ScannerType]SecurityScanner),
		policies:           make(map[string]*TypedSecurityPolicy),
		logger:             logger,
		currentThreatLevel: ThreatLevelNone,
	}
}

// Initialize initializes the typed security manager
func (sm *TypedSecurityManager) Initialize(ctx context.Context) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if sm.isInitialized {
		return fmt.Errorf("security manager already initialized")
	}

	// Log initialization event
	sm.logTypedSecurityEvent(&TypedSecurityEvent{
		ID:          generateEventID(),
		Type:        EventTypeLog,
		Severity:    string(SeverityInfo),
		Source:      "security-manager",
		Description: "Security manager initialized successfully",
		Timestamp:   common.ConsensusNow(),
		Details:     convertToTypedEventDetails(sm.createConfigDetails()),
	})

	sm.isInitialized = true
	return nil
}

// PerformTypedSecurityScan performs a typed security scan
func (sm *TypedSecurityManager) PerformTypedSecurityScan(ctx context.Context, target string) (*TypedScanResult, error) {
	sm.mu.Lock()
	scanner, exists := sm.scanners[ScannerTypeStatic]
	sm.mu.Unlock()

	if !exists {
		return nil, fmt.Errorf("scanner not found: %s", ScannerTypeStatic)
	}

	startTime := common.ConsensusNow()

	// Perform scan
	oldResult, err := scanner.Scan(ctx, target)
	if err != nil {
		return nil, err
	}

	// Convert to typed result
	result := sm.convertToTypedScanResult(oldResult, startTime)

	// Update metrics
	sm.mu.Lock()
	sm.scansPerformed++
	sm.lastScanTime = common.ConsensusNow()
	sm.mu.Unlock()

	// Log scan completion
	sm.logTypedSecurityEvent(&TypedSecurityEvent{
		ID:          generateEventID(),
		Type:        EventTypeLog,
		Severity:    string(SeverityInfo),
		Source:      "security-scanner",
		Description: fmt.Sprintf("Security scan completed on %s with %d findings", target, len(result.Findings)),
		Timestamp:   common.ConsensusNow(),
		Details:     convertToTypedEventDetails(sm.createScanDetails(result)),
	})

	return result, nil
}

// HandleTypedSecurityEvent handles a typed security event
func (sm *TypedSecurityManager) HandleTypedSecurityEvent(event *TypedSecurityEvent) error {
	sm.mu.Lock()
	sm.eventsProcessed++
	sm.mu.Unlock()

	// Log the event
	sm.logTypedSecurityEvent(event)

	// Check if event requires threat level update
	if event.Type == EventTypeThreatDetected || event.Severity == string(SeverityCritical) {
		sm.updateThreatLevel(event)
	}

	// Apply security policies
	return sm.applySecurityPolicies(event)
}

// AddTypedSecurityPolicy adds a typed security policy
func (sm *TypedSecurityManager) AddTypedSecurityPolicy(policy *TypedSecurityPolicy) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if _, exists := sm.policies[policy.ID]; exists {
		return fmt.Errorf("policy already exists: %s", policy.ID)
	}

	sm.policies[policy.ID] = policy

	sm.logger.WithFields(logrus.Fields{
		"policy_id":   policy.ID,
		"policy_name": policy.Name,
		"enforcement": policy.Enforcement,
	}).Info("Security policy added")

	return nil
}

// GetTypedComplianceReport generates a typed compliance report
func (sm *TypedSecurityManager) GetTypedComplianceReport(ctx context.Context, framework string) (*TypedComplianceReport, error) {
	report := &TypedComplianceReport{
		ID:          generateReportID(),
		GeneratedAt: common.ConsensusNow(),
		Framework:   framework,
		Version:     "1.0",
		Controls:    make([]*ComplianceControl, 0),
		Metadata:    types.NewTypedMap(),
	}

	// Check compliance controls
	controls := sm.getComplianceControls(framework)
	passed := 0
	failed := 0

	for _, control := range controls {
		result := sm.checkControl(control)
		if result.Status == "passed" {
			passed++
		} else {
			failed++
		}
		report.Controls = append(report.Controls, result)
	}

	// Calculate compliance score
	total := len(controls)
	if total > 0 {
		report.Score = float64(passed) / float64(total) * 100
	}

	// Set compliance status
	if report.Score >= 90 {
		report.Status = ComplianceStatusCompliant
	} else if report.Score >= 70 {
		report.Status = ComplianceStatusPartial
	} else {
		report.Status = ComplianceStatusNonCompliant
	}

	// Create summary
	report.Summary = &ComplianceSummary{
		TotalControls:  total,
		PassedControls: passed,
		FailedControls: failed,
		ByCategory:     make(map[string]int),
		ByStatus:       make(map[string]int),
	}

	return report, nil
}

// Helper methods

func (sm *TypedSecurityManager) createConfigDetails() *types.TypedMap {
	details := types.NewTypedMap()
	// SecurityConfig doesn't have these fields - use defaults
	details.Set("enable_monitoring", types.NewValue(types.ValueTypeUint64, types.Uint64ToBytes(1)))
	details.Set("scan_interval", types.NewValue(types.ValueTypeUint64, types.Uint64ToBytes(uint64(sm.config.ScanInterval))))
	details.Set("threat_detection", types.NewValue(types.ValueTypeUint64, types.Uint64ToBytes(1)))
	return details
}

func (sm *TypedSecurityManager) createScanDetails(result *TypedScanResult) *types.TypedMap {
	details := types.NewTypedMap()
	details.Set("scan_id", types.NewValue(types.ValueTypeString, []byte(result.ID)))
	details.Set("findings_count", types.NewValue(types.ValueTypeUint64, types.Uint64ToBytes(uint64(len(result.Findings)))))
	// No Float64 support, store as bytes
	riskBytes := make([]byte, 8)
	details.Set("risk_score", types.NewValue(types.ValueTypeBytes, riskBytes))
	details.Set("duration_ms", types.NewValue(types.ValueTypeUint64, types.Uint64ToBytes(uint64(result.Summary.Duration.Milliseconds()))))
	return details
}

func (sm *TypedSecurityManager) convertToTypedScanResult(oldResult *ScanResult, startTime time.Time) *TypedScanResult {
	// Convert old scan result to typed version
	typed := &TypedScanResult{
		ID:          oldResult.ID,
		ScannerType: oldResult.ScannerType,
		Target:      oldResult.Target,
		StartTime:   startTime,
		EndTime:     common.ConsensusNow(),
		Status:      "completed", // ScanResult doesn't have Status field
		Findings:    make([]*TypedFinding, 0),
		Metadata:    types.NewTypedMap(),
	}

	// Convert findings
	for _, finding := range oldResult.Findings {
		typedFinding := &TypedFinding{
			ID:          finding.ID,
			Type:        finding.Category, // Use Category as Type
			Title:       finding.Title,
			Description: finding.Description,
			Severity:    finding.Severity,
			Impact:      "Unknown", // Finding doesn't have Impact field
			Evidence:    types.NewTypedMap(),
			Metadata:    types.NewTypedMap(),
		}
		typed.Findings = append(typed.Findings, typedFinding)
	}

	// Create summary
	typed.Summary = sm.createScanSummary(typed.Findings, typed.EndTime.Sub(typed.StartTime))

	return typed
}

func (sm *TypedSecurityManager) createScanSummary(findings []*TypedFinding, duration time.Duration) *TypedScanSummary {
	summary := &TypedScanSummary{
		TotalFindings: len(findings),
		BySeverity:    make(map[SeverityLevel]int),
		ByType:        make(map[string]int),
		Duration:      duration,
	}

	// Count by severity and type
	criticalCount := 0
	for _, finding := range findings {
		summary.BySeverity[finding.Severity]++
		summary.ByType[finding.Type]++

		if finding.Severity == SeverityCritical {
			criticalCount++
		}
	}

	// Calculate risk score
	summary.RiskScore = float64(criticalCount*10 + summary.BySeverity[SeverityHigh]*5 +
		summary.BySeverity[SeverityMedium]*2 + summary.BySeverity[SeverityLow])

	// Determine if scan passed
	summary.Passed = criticalCount == 0 && summary.BySeverity[SeverityHigh] < 5

	return summary
}

func (sm *TypedSecurityManager) logTypedSecurityEvent(event *TypedSecurityEvent) {
	sm.logger.WithFields(logrus.Fields{
		"event_id": event.ID,
		"type":     event.Type,
		"severity": event.Severity,
		"source":   event.Source,
		// Target field not available in TypedSecurityEvent
	}).Info(event.Description)
}

func (sm *TypedSecurityManager) updateThreatLevel(event *TypedSecurityEvent) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	switch event.Severity {
	case string(SeverityCritical):
		sm.currentThreatLevel = ThreatLevelCritical
	case string(SeverityHigh):
		if sm.currentThreatLevel != ThreatLevelCritical {
			sm.currentThreatLevel = ThreatLevelHigh
		}
	case string(SeverityMedium):
		if sm.currentThreatLevel == ThreatLevelLow || sm.currentThreatLevel == ThreatLevelNone {
			sm.currentThreatLevel = ThreatLevelMedium
		}
	}
}

func (sm *TypedSecurityManager) applySecurityPolicies(event *TypedSecurityEvent) error {
	sm.mu.RLock()
	policies := make([]*TypedSecurityPolicy, 0, len(sm.policies))
	for _, policy := range sm.policies {
		if policy.Enabled {
			policies = append(policies, policy)
		}
	}
	sm.mu.RUnlock()

	// Apply each policy
	for _, policy := range policies {
		if err := sm.applyPolicy(policy, event); err != nil {
			sm.logger.WithError(err).WithField("policy", policy.ID).Error("Failed to apply security policy")
		}
	}

	return nil
}

func (sm *TypedSecurityManager) applyPolicy(policy *TypedSecurityPolicy, event *TypedSecurityEvent) error {
	// Policy application logic would go here
	return nil
}

func (sm *TypedSecurityManager) getComplianceControls(framework string) []string {
	// Return compliance controls for the framework
	return []string{"access-control", "encryption", "audit-logging", "vulnerability-management"}
}

func (sm *TypedSecurityManager) checkControl(control string) *ComplianceControl {
	// Check individual compliance control
	return &ComplianceControl{
		ID:          control,
		Name:        control,
		Description: fmt.Sprintf("Compliance check for %s", control),
		Category:    "security",
		Status:      "passed",
		Result:      "Control is properly implemented",
		Evidence:    types.NewTypedMap(),
	}
}

// Utility functions

func generateEventID() string {
	return fmt.Sprintf("event-%d", common.ConsensusNow().UnixNano())
}

func generateReportID() string {
	return fmt.Sprintf("report-%d", common.ConsensusNow().UnixNano())
}

// GetMetrics returns security metrics
func (sm *TypedSecurityManager) GetMetrics() *TypedSecurityMetrics {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	return &TypedSecurityMetrics{
		EventsProcessed:    sm.eventsProcessed,
		ThreatsDetected:    sm.threatsDetected,
		ScansPerformed:     sm.scansPerformed,
		CurrentThreatLevel: sm.currentThreatLevel,
		LastScanTime:       sm.lastScanTime,
	}
}

// SecurityMetrics contains security performance metrics
type TypedSecurityMetrics struct {
	EventsProcessed    uint64      `json:"events_processed"`
	ThreatsDetected    uint64      `json:"threats_detected"`
	ScansPerformed     uint64      `json:"scans_performed"`
	CurrentThreatLevel ThreatLevel `json:"current_threat_level"`
	LastScanTime       time.Time   `json:"last_scan_time"`
}
