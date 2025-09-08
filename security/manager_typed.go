// Package security provides a refactored security manager using typed structures
package security

import (
	"context"
	"diamante/common"
	"fmt"
	"log"
	"sync"
	"time"

	"diamante/monitoring"
	"diamante/types"

	"github.com/sirupsen/logrus"
)

// Incident is an alias for SecurityIncident
type Incident = SecurityIncident

// IncidentStatus represents the status of an incident
type IncidentStatus string

const (
	IncidentStatusOpen       IncidentStatus = "open"
	IncidentStatusInProgress IncidentStatus = "in_progress"
	IncidentStatusResolved   IncidentStatus = "resolved"
	IncidentStatusClosed     IncidentStatus = "closed"
)

// ScannerTypeVulnerability scanner type for vulnerability scanning
const ScannerTypeVulnerability ScannerType = "vulnerability"

// NewVulnerabilityScanner creates a new vulnerability scanner
func NewVulnerabilityScanner() SecurityScanner {
	return NewStaticScanner() // Reuse static scanner for now
}

// ComplianceMonitor monitors compliance with security policies
type ComplianceMonitor struct {
	checker ComplianceChecker
	logger  *logrus.Logger
}

// NewComplianceMonitor creates a new compliance monitor
func NewComplianceMonitor(logger *logrus.Logger) *ComplianceMonitor {
	return &ComplianceMonitor{
		checker: NewComplianceChecker(),
		logger:  logger,
	}
}

// TypedSecurityManagerV2 is the refactored security manager without interface{}
type TypedSecurityManagerV2 struct {
	config            *SecurityConfig
	logger            *logrus.Logger
	monitor           SecurityMonitor
	threatDetector    ThreatDetector
	incidentHandler   *IncidentHandler
	complianceMonitor *ComplianceMonitor
	scanners          map[ScannerType]SecurityScanner
	isInitialized     bool
	metricsCollector  monitoring.MetricsCollector

	// Typed event handling
	eventHandlers map[SecurityEventType][]TypedEventHandler
	policies      map[string]*TypedSecurityPolicy

	// State management
	state *SecurityState

	mu sync.RWMutex
}

// TypedEventHandler handles typed security events
type TypedEventHandler func(event *TypedSecurityEvent) error

// SecurityState represents the current security state
type SecurityState struct {
	ThreatLevel     ThreatLevel
	LastScanTime    time.Time
	ActiveIncidents int
	ComplianceScore float64
	EventsProcessed uint64
	ThreatsDetected uint64
	mu              sync.RWMutex
}

// NewTypedSecurityManagerV2 creates a new typed security manager
func NewTypedSecurityManagerV2(config *SecurityConfig) (*TypedSecurityManagerV2, error) {
	logger := logrus.New()
	logger = logger.WithField("component", "security-manager-v2").Logger

	return &TypedSecurityManagerV2{
		config:        config,
		logger:        logger,
		scanners:      make(map[ScannerType]SecurityScanner),
		eventHandlers: make(map[SecurityEventType][]TypedEventHandler),
		policies:      make(map[string]*TypedSecurityPolicy),
		state: &SecurityState{
			ThreatLevel: ThreatLevelNone,
		},
	}, nil
}

// Initialize initializes the typed security manager
func (sm *TypedSecurityManagerV2) Initialize(ctx context.Context) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if sm.isInitialized {
		return fmt.Errorf("security manager already initialized")
	}

	// Initialize components
	if err := sm.initializeComponents(ctx); err != nil {
		return fmt.Errorf("failed to initialize components: %w", err)
	}

	// Register default event handlers
	sm.registerDefaultHandlers()

	// Load security policies
	if err := sm.loadSecurityPolicies(); err != nil {
		return fmt.Errorf("failed to load security policies: %w", err)
	}

	// Log initialization
	sm.logTypedEvent(CreateTypedSecurityEvent(
		EventTypeLog,
		SeverityInfo,
		"security-manager",
		"system",
		"Security manager initialized successfully",
	))

	sm.isInitialized = true
	return nil
}

// initializeComponents initializes security components
func (sm *TypedSecurityManagerV2) initializeComponents(ctx context.Context) error {
	// Initialize security monitor (always enabled)
	sm.monitor = NewSecurityMonitor()

	// Initialize threat detector
	if sm.config.ThreatDetectionEnabled {
		sm.threatDetector = NewThreatDetector()
	}

	// Initialize incident handler
	incidentConfig := &IncidentConfig{
		AutoResponse: true,
		MaxIncidents: 1000,
	}
	standardLogger := log.New(sm.logger.Out, "", log.LstdFlags)
	sm.incidentHandler = NewIncidentHandler(incidentConfig, standardLogger)

	// Initialize compliance monitor
	sm.complianceMonitor = NewComplianceMonitor(sm.logger)

	// Initialize scanners
	if err := sm.initializeScanners(); err != nil {
		return fmt.Errorf("failed to initialize scanners: %w", err)
	}

	// Metrics collector will be initialized externally if needed

	return nil
}

// initializeScanners initializes security scanners
func (sm *TypedSecurityManagerV2) initializeScanners() error {
	// Initialize all scanners
	if sm.config.EnableAutoScanning {
		sm.scanners[ScannerTypeDependency] = NewDependencyScanner()
		sm.scanners[ScannerTypeStatic] = NewStaticScanner()
		sm.scanners[ScannerTypeNetwork] = NewNetworkScanner()
		sm.scanners[ScannerTypeVulnerability] = NewVulnerabilityScanner()
	}

	return nil
}

// registerDefaultHandlers registers default event handlers
func (sm *TypedSecurityManagerV2) registerDefaultHandlers() {
	// Register threat detection handler
	sm.RegisterEventHandler(EventTypeThreatDetected, func(event *TypedSecurityEvent) error {
		return sm.handleThreatDetection(event)
	})

	// Register policy violation handler
	sm.RegisterEventHandler(EventTypePolicyViolation, func(event *TypedSecurityEvent) error {
		return sm.handlePolicyViolation(event)
	})

	// Register incident handler
	sm.RegisterEventHandler(EventTypeIncident, func(event *TypedSecurityEvent) error {
		return sm.handleIncident(event)
	})

	// Register anomaly detection handler
	sm.RegisterEventHandler(EventTypeAnomalyDetected, func(event *TypedSecurityEvent) error {
		return sm.handleAnomaly(event)
	})
}

// RegisterEventHandler registers a typed event handler
func (sm *TypedSecurityManagerV2) RegisterEventHandler(eventType SecurityEventType, handler TypedEventHandler) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.eventHandlers[eventType] = append(sm.eventHandlers[eventType], handler)
}

// HandleSecurityEvent handles a typed security event
func (sm *TypedSecurityManagerV2) HandleSecurityEvent(event *TypedSecurityEvent) error {
	// Update state
	sm.state.mu.Lock()
	sm.state.EventsProcessed++
	sm.state.mu.Unlock()

	// Log event
	sm.logTypedEvent(event)

	// Record metrics
	// if sm.metricsCollector != nil {
	// 	sm.recordEventMetrics(event)
	// }

	// Get handlers for this event type
	sm.mu.RLock()
	handlers := sm.eventHandlers[event.Type]
	sm.mu.RUnlock()

	// Execute handlers
	for _, handler := range handlers {
		if err := handler(event); err != nil {
			sm.logger.WithError(err).WithField("event_type", event.Type).Error("Event handler failed")
		}
	}

	// Check if threat level needs updating
	if event.Type == EventTypeThreatDetected || event.Severity == string(SeverityCritical) {
		sm.updateThreatLevel(event)
	}

	// Apply security policies
	return sm.applyPolicies(event)
}

// PerformSecurityScan performs a comprehensive security scan
func (sm *TypedSecurityManagerV2) PerformSecurityScan(ctx context.Context, target string) (*TypedScanResult, error) {
	if !sm.isInitialized {
		return nil, fmt.Errorf("security manager not initialized")
	}

	// Create scan result
	result := &TypedScanResult{
		ID:        generateScanID(),
		Target:    target,
		StartTime: common.ConsensusNow(),
		Findings:  make([]*TypedFinding, 0),
		Metadata:  types.NewTypedMap(),
	}

	// Run all enabled scanners
	var wg sync.WaitGroup
	findingsChan := make(chan []*TypedFinding, len(sm.scanners))
	errorsChan := make(chan error, len(sm.scanners))

	for scannerType, scanner := range sm.scanners {
		wg.Add(1)
		go func(sType ScannerType, s SecurityScanner) {
			defer wg.Done()

			scanResult, err := s.Scan(ctx, target)
			if err != nil {
				errorsChan <- fmt.Errorf("%s scanner failed: %w", sType, err)
				return
			}

			// Convert findings to typed format
			typedFindings := sm.convertFindings(scanResult.Findings)
			findingsChan <- typedFindings
		}(scannerType, scanner)
	}

	// Wait for all scanners to complete
	wg.Wait()
	close(findingsChan)
	close(errorsChan)

	// Collect all findings
	for findings := range findingsChan {
		result.Findings = append(result.Findings, findings...)
	}

	// Check for errors
	var scanErrors []error
	for err := range errorsChan {
		scanErrors = append(scanErrors, err)
	}

	// Complete scan result
	result.EndTime = common.ConsensusNow()
	result.Status = "completed"
	if len(scanErrors) > 0 {
		result.Status = "completed_with_errors"
		// Add errors to metadata
		errorDetails := types.NewTypedMap()
		for i, err := range scanErrors {
			errorDetails.Set(fmt.Sprintf("error_%d", i), types.NewValue(types.ValueTypeString, []byte(err.Error())))
		}
		result.Metadata.Set("error_count", types.NewValue(types.ValueTypeUint64, types.Uint64ToBytes(uint64(len(scanErrors)))))
	}

	// Create summary
	result.Summary = sm.createScanSummary(result.Findings, result.EndTime.Sub(result.StartTime))

	// Update state
	sm.state.mu.Lock()
	sm.state.LastScanTime = common.ConsensusNow()
	sm.state.mu.Unlock()

	// Log scan completion
	sm.logTypedEvent(CreateTypedSecurityEventWithDetails(
		EventTypeLog,
		SeverityInfo,
		"security-scanner",
		target,
		fmt.Sprintf("Security scan completed with %d findings", len(result.Findings)),
		map[string]interface{}{
			"scan_id":        result.ID,
			"total_findings": len(result.Findings),
			"risk_score":     result.Summary.RiskScore,
			"duration_ms":    result.Summary.Duration.Milliseconds(),
		},
	))

	return result, nil
}

// Event handlers

func (sm *TypedSecurityManagerV2) handleThreatDetection(event *TypedSecurityEvent) error {
	sm.state.mu.Lock()
	sm.state.ThreatsDetected++
	sm.state.mu.Unlock()

	// Create incident if severity is high enough
	if event.Severity == string(SeverityCritical) || event.Severity == string(SeverityHigh) {
		incident := sm.createIncidentFromEvent(event)

		// Process incident through the incident handler
		if sm.incidentHandler != nil {
			// Convert incident to SecurityAlert for HandleAlert
			alert := SecurityAlert{
				Type:        incident.Type,
				Severity:    string(incident.Severity),
				Source:      incident.Source,
				Description: incident.Description,
				Timestamp:   incident.Timestamp,
				Metadata:    incident.Metadata,
			}
			if err := sm.incidentHandler.HandleAlert(context.Background(), alert); err != nil {
				sm.logger.WithError(err).WithField("incident_id", incident.ID).Error("Failed to handle incident")
				return fmt.Errorf("failed to handle incident: %w", err)
			}
		} else {
			sm.logger.WithField("incident", incident).Warn("Incident handler not initialized, logging incident only")
		}
	}

	return nil
}

func (sm *TypedSecurityManagerV2) handlePolicyViolation(event *TypedSecurityEvent) error {
	// Log policy violation
	policyDetail, hasPolicyDetail := event.Details["policy_id"]
	actionDetail, hasActionDetail := event.Details["action"]
	policyVal := ""
	actionVal := ""
	if hasPolicyDetail {
		policyVal = policyDetail.String()
	}
	if hasActionDetail {
		actionVal = actionDetail.String()
	}
	sm.logger.WithFields(logrus.Fields{
		"policy":   policyVal,
		"violator": event.Source,
		"action":   actionVal,
	}).Warn("Security policy violation detected")

	// Take enforcement action based on policy
	if policyVal != "" {
		if policy, exists := sm.policies[policyVal]; exists {
			return sm.enforcePolicy(policy, event)
		}
	}

	return nil
}

func (sm *TypedSecurityManagerV2) handleIncident(event *TypedSecurityEvent) error {
	sm.state.mu.Lock()
	sm.state.ActiveIncidents++
	sm.state.mu.Unlock()

	// Escalate to incident handler
	incident := sm.createIncidentFromEvent(event)
	// IncidentHandler processes incidents via channels
	sm.logger.WithField("incident", incident).Error("Security incident detected")
	return nil
}

func (sm *TypedSecurityManagerV2) handleAnomaly(event *TypedSecurityEvent) error {
	// Analyze anomaly
	if sm.threatDetector != nil {
		// ThreatDetector doesn't have AnalyzeAnomaly method
		// Log anomaly for now
		sm.logger.WithField("anomaly", event).Warn("Anomaly detected")
	}

	return nil
}

// Helper methods

func (sm *TypedSecurityManagerV2) convertFindings(findings []Finding) []*TypedFinding {
	typed := make([]*TypedFinding, 0, len(findings))

	for _, finding := range findings {
		typedFinding := &TypedFinding{
			ID:             finding.ID,
			Type:           finding.Category, // Use Category as Type
			Title:          finding.Title,
			Description:    finding.Description,
			Severity:       finding.Severity,
			Impact:         "Unknown", // Not in Finding struct
			Recommendation: finding.Remediation,
			Evidence:       types.NewTypedMap(),
			References:     finding.References,
			Metadata:       types.NewTypedMap(),
		}

		// Location is a string in Finding struct, create simple location
		if finding.Location != "" {
			typedFinding.Location = &FindingLocation{
				File:     finding.Location,
				Line:     0,
				Column:   0,
				Function: "",
			}
		}

		typed = append(typed, typedFinding)
	}

	return typed
}

func (sm *TypedSecurityManagerV2) createScanSummary(findings []*TypedFinding, duration time.Duration) *TypedScanSummary {
	summary := &TypedScanSummary{
		TotalFindings: len(findings),
		BySeverity:    make(map[SeverityLevel]int),
		ByType:        make(map[string]int),
		Duration:      duration,
	}

	// Count by severity and type
	criticalCount := 0
	highCount := 0

	for _, finding := range findings {
		summary.BySeverity[finding.Severity]++
		summary.ByType[finding.Type]++

		switch finding.Severity {
		case SeverityCritical:
			criticalCount++
		case SeverityHigh:
			highCount++
		}
	}

	// Calculate risk score (0-100)
	summary.RiskScore = float64(criticalCount*20 + highCount*10 +
		summary.BySeverity[SeverityMedium]*5 + summary.BySeverity[SeverityLow]*2)

	if summary.RiskScore > 100 {
		summary.RiskScore = 100
	}

	// Determine if scan passed
	summary.Passed = criticalCount == 0 && highCount < 3

	return summary
}

func (sm *TypedSecurityManagerV2) updateThreatLevel(event *TypedSecurityEvent) {
	sm.state.mu.Lock()
	defer sm.state.mu.Unlock()

	currentLevel := sm.state.ThreatLevel
	newLevel := currentLevel

	switch event.Severity {
	case string(SeverityCritical):
		newLevel = ThreatLevelCritical
	case string(SeverityHigh):
		if currentLevel != ThreatLevelCritical {
			newLevel = ThreatLevelHigh
		}
	case string(SeverityMedium):
		if currentLevel == ThreatLevelLow || currentLevel == ThreatLevelNone {
			newLevel = ThreatLevelMedium
		}
	}

	if newLevel != currentLevel {
		sm.state.ThreatLevel = newLevel
		sm.logger.WithFields(logrus.Fields{
			"old_level": currentLevel,
			"new_level": newLevel,
		}).Warn("Threat level changed")
	}
}

func (sm *TypedSecurityManagerV2) applyPolicies(event *TypedSecurityEvent) error {
	applicablePolicies := sm.getApplicablePolicies(event)

	for _, policy := range applicablePolicies {
		if err := sm.enforcePolicy(policy, event); err != nil {
			sm.logger.WithError(err).WithField("policy", policy.ID).Error("Failed to enforce policy")
		}
	}

	return nil
}

func (sm *TypedSecurityManagerV2) getApplicablePolicies(event *TypedSecurityEvent) []*TypedSecurityPolicy {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	applicable := make([]*TypedSecurityPolicy, 0)

	for _, policy := range sm.policies {
		if policy.Enabled && sm.isPolicyApplicable(policy, event) {
			applicable = append(applicable, policy)
		}
	}

	return applicable
}

func (sm *TypedSecurityManagerV2) isPolicyApplicable(policy *TypedSecurityPolicy, event *TypedSecurityEvent) bool {
	// Check if policy applies to this event type
	for _, tag := range policy.Tags {
		if tag == string(event.Type) || tag == string(event.Severity) {
			return true
		}
	}

	return false
}

func (sm *TypedSecurityManagerV2) enforcePolicy(policy *TypedSecurityPolicy, event *TypedSecurityEvent) error {
	sm.logger.WithFields(logrus.Fields{
		"policy":      policy.ID,
		"enforcement": policy.Enforcement,
	}).Debug("Enforcing security policy")

	// Execute policy rules
	for _, rule := range policy.Rules {
		if err := sm.executeRule(rule, event); err != nil {
			return err
		}
	}

	return nil
}

func (sm *TypedSecurityManagerV2) executeRule(rule *PolicyRule, event *TypedSecurityEvent) error {
	// Rule execution logic would go here
	return nil
}

func (sm *TypedSecurityManagerV2) createIncidentFromEvent(event *TypedSecurityEvent) *Incident {
	return &Incident{
		ID:          generateIncidentID(),
		Type:        string(event.Type),
		Description: event.Description,
		Severity:    string(event.Severity),
		Status:      string(IncidentStatusOpen),
		Source:      event.Source,
		Timestamp:   event.Timestamp,
		Metadata:    make(map[string]interface{}),
		Actions:     []IncidentAction{},
		Response:    nil,
	}
}

func (sm *TypedSecurityManagerV2) logTypedEvent(event *TypedSecurityEvent) {
	sm.logger.WithFields(logrus.Fields{
		"event_id": event.ID,
		"type":     event.Type,
		"severity": event.Severity,
		"source":   event.Source,
		// Target field not available in TypedSecurityEvent
	}).Info(event.Description)
}

func (sm *TypedSecurityManagerV2) recordEventMetrics(event *TypedSecurityEvent) {
	// Metrics recording disabled
	// labels := map[string]string{
	// 	"type":     string(event.Type),
	// 	"severity": string(event.Severity),
	// }

	// sm.metricsCollector.IncrementCounter("security_events_total", labels)
}

func (sm *TypedSecurityManagerV2) registerMetrics() {
	// Metrics registration disabled - MetricsCollector interface doesn't have these methods
	// sm.metricsCollector.RegisterCounter("security_events_total", "Total security events", []string{"type", "severity"})
	// sm.metricsCollector.RegisterGauge("security_threat_level", "Current threat level", []string{})
	// sm.metricsCollector.RegisterGauge("security_active_incidents", "Active security incidents", []string{})
	// sm.metricsCollector.RegisterHistogram("security_scan_duration", "Security scan duration", []string{"scanner_type"})
}

func (sm *TypedSecurityManagerV2) loadSecurityPolicies() error {
	// Load default security policies
	policies := []*TypedSecurityPolicy{
		{
			ID:          "auth-failure-policy",
			Name:        "Authentication Failure Policy",
			Description: "Handles repeated authentication failures",
			Type:        "authentication",
			Enforcement: EnforcementStrict,
			Enabled:     true,
			Tags:        []string{string(EventTypeAuthFailure)},
			Rules: []*PolicyRule{
				{
					ID:        "block-after-failures",
					Name:      "Block after repeated failures",
					Condition: "failure_count > 5",
					Action:    "block_ip",
				},
			},
			Metadata: types.NewTypedMap(),
		},
		{
			ID:          "threat-response-policy",
			Name:        "Threat Response Policy",
			Description: "Automated threat response",
			Type:        "threat_response",
			Enforcement: EnforcementModerate,
			Enabled:     true,
			Tags:        []string{string(EventTypeThreatDetected), string(SeverityCritical)},
			Rules: []*PolicyRule{
				{
					ID:        "isolate-threat",
					Name:      "Isolate threat source",
					Condition: "threat_level == critical",
					Action:    "isolate",
				},
			},
			Metadata: types.NewTypedMap(),
		},
	}

	for _, policy := range policies {
		sm.policies[policy.ID] = policy
	}

	return nil
}

// Utility functions

func generateScanID() string {
	return fmt.Sprintf("scan-%d", common.ConsensusNow().UnixNano())
}

func generateIncidentID() string {
	return fmt.Sprintf("incident-%d", common.ConsensusNow().UnixNano())
}

// GetState returns the current security state
func (sm *TypedSecurityManagerV2) GetState() SecurityState {
	sm.state.mu.RLock()
	defer sm.state.mu.RUnlock()

	return SecurityState{
		ThreatLevel:     sm.state.ThreatLevel,
		LastScanTime:    sm.state.LastScanTime,
		ActiveIncidents: sm.state.ActiveIncidents,
		ComplianceScore: sm.state.ComplianceScore,
		EventsProcessed: sm.state.EventsProcessed,
		ThreatsDetected: sm.state.ThreatsDetected,
	}
}

// Shutdown gracefully shuts down the security manager
func (sm *TypedSecurityManagerV2) Shutdown() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if !sm.isInitialized {
		return nil
	}

	// Shutdown components
	// Monitor and ThreatDetector don't have Stop/Shutdown methods

	sm.isInitialized = false
	sm.logger.Info("Security manager shut down")

	return nil
}
