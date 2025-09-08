package security

import (
	"context"
	"diamante/common"
	"fmt"
	"sync"
	"time"
)

// SecurityMonitorImpl implements real-time security event monitoring
type SecurityMonitorImpl struct {
	mu              sync.RWMutex
	events          []SecurityEvent
	eventChannel    chan SecurityEvent
	subscribers     []chan<- SecurityEvent
	monitorID       string
	isMonitoring    bool
	maxEvents       int
	eventProcessors map[SecurityEventType]EventProcessor
}

// EventProcessor processes specific types of security events
type EventProcessor func(event SecurityEvent) error

// NewSecurityMonitor creates a new security monitor instance
func NewSecurityMonitor() SecurityMonitor {
	return &SecurityMonitorImpl{
		monitorID:       fmt.Sprintf("monitor-%d", common.ConsensusNow().Unix()),
		events:          make([]SecurityEvent, 0),
		eventChannel:    make(chan SecurityEvent, 1000),
		subscribers:     make([]chan<- SecurityEvent, 0),
		maxEvents:       10000, // Keep last 10k events in memory
		eventProcessors: make(map[SecurityEventType]EventProcessor),
	}
}

// MonitorEvents starts monitoring security events in real-time
func (sm *SecurityMonitorImpl) MonitorEvents(ctx context.Context) (<-chan SecurityEvent, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if sm.isMonitoring {
		return nil, fmt.Errorf("monitoring already active")
	}

	// Create subscriber channel
	subscriberChan := make(chan SecurityEvent, 100)
	sm.subscribers = append(sm.subscribers, subscriberChan)

	// Start monitoring if not already running
	if !sm.isMonitoring {
		sm.isMonitoring = true
		go sm.processEvents(ctx)
	}

	return subscriberChan, nil
}

// RecordEvent records a security event
func (sm *SecurityMonitorImpl) RecordEvent(event SecurityEvent) error {
	if event.ID == "" {
		event.ID = fmt.Sprintf("event-%d-%d", common.ConsensusNow().Unix(), common.ConsensusNow().Nanosecond())
	}

	if event.Timestamp.IsZero() {
		event.Timestamp = common.ConsensusNow()
	}

	// Validate event
	if err := sm.validateEvent(event); err != nil {
		return fmt.Errorf("invalid event: %w", err)
	}

	// Send to event channel
	select {
	case sm.eventChannel <- event:
		return nil
	default:
		return fmt.Errorf("event channel full, dropping event")
	}
}

// GetEventHistory returns historical security events
func (sm *SecurityMonitorImpl) GetEventHistory(since time.Time) ([]SecurityEvent, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	history := make([]SecurityEvent, 0)
	for _, event := range sm.events {
		if event.Timestamp.After(since) || event.Timestamp.Equal(since) {
			history = append(history, event)
		}
	}

	return history, nil
}

// processEvents processes events from the event channel
func (sm *SecurityMonitorImpl) processEvents(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			sm.mu.Lock()
			sm.isMonitoring = false
			sm.mu.Unlock()
			return
		case event := <-sm.eventChannel:
			// Store event
			sm.storeEvent(event)

			// Process event with specific processor if available
			if processor, exists := sm.eventProcessors[event.Type]; exists {
				if err := processor(event); err != nil {
					// Log processing error but continue
					fmt.Printf("Error processing event %s: %v\n", event.ID, err)
				}
			}

			// Distribute to subscribers
			sm.distributeEvent(event)

			// Check for critical events
			if event.Severity == SeverityCritical {
				sm.handleCriticalEvent(event)
			}
		}
	}
}

// storeEvent stores an event in memory
func (sm *SecurityMonitorImpl) storeEvent(event SecurityEvent) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.events = append(sm.events, event)

	// Maintain max events limit
	if len(sm.events) > sm.maxEvents {
		sm.events = sm.events[len(sm.events)-sm.maxEvents:]
	}
}

// distributeEvent sends event to all subscribers
func (sm *SecurityMonitorImpl) distributeEvent(event SecurityEvent) {
	sm.mu.RLock()
	subscribers := make([]chan<- SecurityEvent, len(sm.subscribers))
	copy(subscribers, sm.subscribers)
	sm.mu.RUnlock()

	for _, subscriber := range subscribers {
		select {
		case subscriber <- event:
			// Event sent successfully
		default:
			// Subscriber channel full, skip
		}
	}
}

// handleCriticalEvent handles critical security events
func (sm *SecurityMonitorImpl) handleCriticalEvent(event SecurityEvent) {
	// Create comprehensive alert event
	alertEvent := SecurityEvent{
		ID:          fmt.Sprintf("alert-%s", event.ID),
		Type:        EventTypeThreatDetected,
		Severity:    SeverityCritical,
		Source:      "security-monitor",
		Target:      event.Target,
		Description: fmt.Sprintf("CRITICAL ALERT: %s", event.Description),
		Timestamp:   common.ConsensusNow(),
		Details: map[string]interface{}{
			"original_event_id": event.ID,
			"alert_type":        "critical_event",
			"threat_level":      sm.detectThreatLevel(event),
			"response_actions":  sm.getResponseActions(event),
			"affected_systems":  sm.identifyAffectedSystems(event),
		},
		Resolved: false,
	}

	// Store the alert
	sm.storeEvent(alertEvent)

	// Trigger automated response
	sm.triggerAutomatedResponse(event)

	// Send alerts to security team
	sm.sendSecurityAlerts(event)

	// Log to external SIEM systems
	sm.logToSIEM(event)

	// Initiate incident response workflow
	sm.initiateIncidentResponse(event)
}

// detectThreatLevel analyzes event to determine threat level
func (sm *SecurityMonitorImpl) detectThreatLevel(event SecurityEvent) string {
	// Analyze event characteristics to determine threat level
	threatScore := 0.0

	// Factor 1: Event severity
	switch event.Severity {
	case SeverityCritical:
		threatScore += 0.4
	case SeverityHigh:
		threatScore += 0.3
	case SeverityMedium:
		threatScore += 0.2
	case SeverityLow:
		threatScore += 0.1
	}

	// Factor 2: Event type
	switch event.Type {
	case EventTypeThreatDetected:
		threatScore += 0.3
	case EventTypeUnauthorizedAccess:
		threatScore += 0.25
	case EventTypePolicyViolation:
		threatScore += 0.2
	case EventTypeAnomalyDetected:
		threatScore += 0.15
	}

	// Factor 3: Target criticality
	if sm.isCriticalTarget(event.Target) {
		threatScore += 0.2
	}

	// Factor 4: Event frequency (check for similar events)
	recentSimilarEvents := sm.countRecentSimilarEvents(event, time.Hour)
	if recentSimilarEvents > 10 {
		threatScore += 0.1
	} else if recentSimilarEvents > 5 {
		threatScore += 0.05
	}

	// Determine threat level based on score
	if threatScore >= 0.8 {
		return "critical"
	} else if threatScore >= 0.6 {
		return "high"
	} else if threatScore >= 0.4 {
		return "medium"
	} else if threatScore >= 0.2 {
		return "low"
	}
	return "minimal"
}

// getResponseActions determines appropriate response actions
func (sm *SecurityMonitorImpl) getResponseActions(event SecurityEvent) []string {
	actions := []string{}

	switch event.Type {
	case EventTypeUnauthorizedAccess:
		actions = append(actions,
			"Block source IP address",
			"Disable affected user account",
			"Force password reset",
			"Review access logs",
		)
	case EventTypeThreatDetected:
		actions = append(actions,
			"Isolate affected systems",
			"Initiate threat containment",
			"Collect forensic evidence",
			"Run malware scans",
		)
	case EventTypePolicyViolation:
		actions = append(actions,
			"Notify policy owner",
			"Review policy configuration",
			"Apply policy enforcement",
			"Generate compliance report",
		)
	case EventTypeAnomalyDetected:
		actions = append(actions,
			"Increase monitoring frequency",
			"Analyze behavior patterns",
			"Check for false positives",
			"Update detection baselines",
		)
	}

	// Add general actions for critical events
	if event.Severity == SeverityCritical {
		actions = append(actions,
			"Activate incident response team",
			"Enable enhanced logging",
			"Backup critical data",
			"Prepare incident report",
		)
	}

	return actions
}

// identifyAffectedSystems determines which systems are affected
func (sm *SecurityMonitorImpl) identifyAffectedSystems(event SecurityEvent) []string {
	systems := []string{event.Target}

	// Check if event details contain system information
	if relatedSystems, exists := event.Details["related_systems"]; exists {
		if sysList, ok := relatedSystems.([]string); ok {
			systems = append(systems, sysList...)
		}
	}

	// Add connected systems based on target
	connectedSystems := sm.getConnectedSystems(event.Target)
	systems = append(systems, connectedSystems...)

	// Remove duplicates
	uniqueSystems := make(map[string]bool)
	for _, sys := range systems {
		uniqueSystems[sys] = true
	}

	result := make([]string, 0, len(uniqueSystems))
	for sys := range uniqueSystems {
		result = append(result, sys)
	}

	return result
}

// isCriticalTarget checks if the target is a critical system
func (sm *SecurityMonitorImpl) isCriticalTarget(target string) bool {
	criticalTargets := []string{
		"authentication_service",
		"payment_gateway",
		"user_database",
		"api_gateway",
		"admin_panel",
		"wallet_service",
		"consensus_engine",
	}

	for _, critical := range criticalTargets {
		if target == critical || contains(target, critical) {
			return true
		}
	}

	return false
}

// countRecentSimilarEvents counts similar events in the given duration
func (sm *SecurityMonitorImpl) countRecentSimilarEvents(event SecurityEvent, duration time.Duration) int {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	count := 0
	threshold := common.ConsensusNow().Add(-duration)

	for _, e := range sm.events {
		if e.Timestamp.After(threshold) &&
			e.Type == event.Type &&
			e.Target == event.Target {
			count++
		}
	}

	return count
}

// getConnectedSystems returns systems connected to the target
func (sm *SecurityMonitorImpl) getConnectedSystems(target string) []string {
	// In production, this would query a service dependency graph
	systemDependencies := map[string][]string{
		"api_gateway":      {"authentication_service", "user_database", "wallet_service"},
		"payment_gateway":  {"wallet_service", "transaction_service", "notification_service"},
		"consensus_engine": {"validator_service", "block_service", "network_service"},
	}

	if deps, exists := systemDependencies[target]; exists {
		return deps
	}

	return []string{}
}

// triggerAutomatedResponse executes automated response actions
func (sm *SecurityMonitorImpl) triggerAutomatedResponse(event SecurityEvent) {
	// Execute response actions based on event type and severity
	switch event.Type {
	case EventTypeUnauthorizedAccess:
		// Block the source IP
		if sourceIP, exists := event.Details["source_ip"]; exists {
			if ipAddr, ok := sourceIP.(string); ok {
				sm.blockIPAddress(ipAddr)
			}
		}
		// Disable the user account
		if userID := event.UserID; userID != "" {
			sm.disableUserAccount(userID)
		}

	case EventTypeThreatDetected:
		// Isolate affected systems
		affectedSystems := sm.identifyAffectedSystems(event)
		for _, system := range affectedSystems {
			sm.isolateSystem(system)
		}

	case EventTypePolicyViolation:
		// Apply policy enforcement
		sm.enforcePolicyRestrictions(event)
	}

	// Log response actions
	responseLog := SecurityEvent{
		ID:          fmt.Sprintf("response-%s", event.ID),
		Type:        EventTypeResponseAction,
		Severity:    SeverityInfo,
		Source:      "automated-response",
		Target:      event.Target,
		Description: fmt.Sprintf("Automated response initiated for event: %s", event.ID),
		Timestamp:   common.ConsensusNow(),
		Details: map[string]interface{}{
			"original_event": event.ID,
			"actions_taken":  sm.getResponseActions(event),
		},
		Resolved: false,
	}
	sm.storeEvent(responseLog)
}

// sendSecurityAlerts sends alerts to security team
func (sm *SecurityMonitorImpl) sendSecurityAlerts(event SecurityEvent) {
	// In production, this would integrate with alerting services
	// For now, we'll create alert records

	alertChannels := []string{"email", "sms", "slack", "pagerduty"}

	for _, channel := range alertChannels {
		alertRecord := SecurityEvent{
			ID:          fmt.Sprintf("alert-%s-%s", channel, event.ID),
			Type:        EventTypeAlert,
			Severity:    event.Severity,
			Source:      "alert-system",
			Target:      channel,
			Description: fmt.Sprintf("Security alert sent via %s for event: %s", channel, event.Description),
			Timestamp:   common.ConsensusNow(),
			Details: map[string]interface{}{
				"channel":        channel,
				"event_id":       event.ID,
				"sent_to":        sm.getAlertRecipients(channel, event.Severity),
				"alert_priority": sm.getAlertPriority(event),
			},
			Resolved: false,
		}
		sm.storeEvent(alertRecord)
	}
}

// logToSIEM logs event to external SIEM systems
func (sm *SecurityMonitorImpl) logToSIEM(event SecurityEvent) {
	// Format event for SIEM consumption
	siemEvent := map[string]interface{}{
		"timestamp":    event.Timestamp.Unix(),
		"event_id":     event.ID,
		"type":         string(event.Type),
		"severity":     string(event.Severity),
		"source":       event.Source,
		"target":       event.Target,
		"description":  event.Description,
		"user_id":      event.UserID,
		"ip_address":   event.IPAddress,
		"details":      event.Details,
		"threat_level": sm.detectThreatLevel(event),
	}

	// In production, this would send to actual SIEM endpoints
	// For now, we create a log record
	siemLog := SecurityEvent{
		ID:          fmt.Sprintf("siem-%s", event.ID),
		Type:        EventTypeLog,
		Severity:    SeverityInfo,
		Source:      "siem-connector",
		Target:      "external-siem",
		Description: "Event forwarded to SIEM",
		Timestamp:   common.ConsensusNow(),
		Details:     siemEvent,
		Resolved:    true,
	}
	sm.storeEvent(siemLog)
}

// initiateIncidentResponse starts the incident response workflow
func (sm *SecurityMonitorImpl) initiateIncidentResponse(event SecurityEvent) {
	// Create incident ticket
	incident := map[string]interface{}{
		"incident_id":      fmt.Sprintf("INC-%d", common.ConsensusNow().Unix()),
		"severity":         event.Severity,
		"status":           "open",
		"assigned_to":      "security-team",
		"created_at":       common.ConsensusNow(),
		"event_id":         event.ID,
		"description":      event.Description,
		"affected_systems": sm.identifyAffectedSystems(event),
		"response_plan":    sm.getIncidentResponsePlan(event),
	}

	// Record incident creation
	incidentEvent := SecurityEvent{
		ID:          fmt.Sprintf("incident-%s", event.ID),
		Type:        EventTypeIncident,
		Severity:    event.Severity,
		Source:      "incident-response",
		Target:      event.Target,
		Description: fmt.Sprintf("Incident created for event: %s", event.ID),
		Timestamp:   common.ConsensusNow(),
		Details:     incident,
		Resolved:    false,
	}
	sm.storeEvent(incidentEvent)
}

// Helper methods for automated response

func (sm *SecurityMonitorImpl) blockIPAddress(ip string) {
	// In production, this would update firewall rules
	fmt.Printf("Blocking IP address: %s\n", ip)
}

func (sm *SecurityMonitorImpl) disableUserAccount(userID string) {
	// In production, this would disable the user account
	fmt.Printf("Disabling user account: %s\n", userID)
}

func (sm *SecurityMonitorImpl) isolateSystem(system string) {
	// In production, this would isolate the system from network
	fmt.Printf("Isolating system: %s\n", system)
}

func (sm *SecurityMonitorImpl) enforcePolicyRestrictions(event SecurityEvent) {
	// In production, this would apply policy restrictions
	fmt.Printf("Enforcing policy restrictions for event: %s\n", event.ID)
}

func (sm *SecurityMonitorImpl) getAlertRecipients(channel string, severity SeverityLevel) []string {
	// In production, this would lookup actual recipients
	recipients := map[string][]string{
		"email":     {"security@company.com", "oncall@company.com"},
		"sms":       {"+1234567890", "+0987654321"},
		"slack":     {"#security-alerts", "#incidents"},
		"pagerduty": {"security-team", "ops-team"},
	}

	if severity == SeverityCritical {
		// Add executives for critical alerts
		recipients["email"] = append(recipients["email"], "ciso@company.com", "cto@company.com")
	}

	return recipients[channel]
}

func (sm *SecurityMonitorImpl) getAlertPriority(event SecurityEvent) string {
	switch event.Severity {
	case SeverityCritical:
		return "P0"
	case SeverityHigh:
		return "P1"
	case SeverityMedium:
		return "P2"
	case SeverityLow:
		return "P3"
	default:
		return "P4"
	}
}

func (sm *SecurityMonitorImpl) getIncidentResponsePlan(event SecurityEvent) []string {
	plan := []string{
		"1. Assess the situation and confirm the threat",
		"2. Contain the threat to prevent spread",
		"3. Eradicate the threat from affected systems",
		"4. Recover normal operations",
		"5. Document lessons learned",
	}

	// Add specific steps based on event type
	switch event.Type {
	case EventTypeThreatDetected:
		plan = append([]string{
			"0. Activate threat response team",
			"0.1. Enable enhanced monitoring",
		}, plan...)
	case EventTypeUnauthorizedAccess:
		plan = append([]string{
			"0. Verify unauthorized access",
			"0.1. Check for data exfiltration",
		}, plan...)
	}

	return plan
}

// Helper function
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr ||
		(len(s) > len(substr) && (s[:len(substr)] == substr || s[len(s)-len(substr):] == substr)))
}

// validateEvent validates a security event
func (sm *SecurityMonitorImpl) validateEvent(event SecurityEvent) error {
	if event.Type == "" {
		return fmt.Errorf("event type cannot be empty")
	}

	if event.Severity == "" {
		return fmt.Errorf("event severity cannot be empty")
	}

	if event.Source == "" {
		return fmt.Errorf("event source cannot be empty")
	}

	if event.Description == "" {
		return fmt.Errorf("event description cannot be empty")
	}

	return nil
}

// RegisterEventProcessor registers a processor for specific event types
func (sm *SecurityMonitorImpl) RegisterEventProcessor(eventType SecurityEventType, processor EventProcessor) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.eventProcessors[eventType] = processor
}

// GetEventStatistics returns statistics about security events
func (sm *SecurityMonitorImpl) GetEventStatistics() map[string]interface{} {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	stats := make(map[string]interface{})

	// Count by type
	typeCount := make(map[SecurityEventType]int)
	severityCount := make(map[SeverityLevel]int)
	resolvedCount := 0

	for _, event := range sm.events {
		typeCount[event.Type]++
		severityCount[event.Severity]++
		if event.Resolved {
			resolvedCount++
		}
	}

	stats["total_events"] = len(sm.events)
	stats["events_by_type"] = typeCount
	stats["events_by_severity"] = severityCount
	stats["resolved_events"] = resolvedCount
	stats["unresolved_events"] = len(sm.events) - resolvedCount
	stats["monitoring_active"] = sm.isMonitoring

	return stats
}

// AuditLoggerImpl implements security audit logging
type AuditLoggerImpl struct {
	mu           sync.RWMutex
	logs         []AuditLog
	maxLogs      int
	loggerID     string
	outputWriter AuditLogWriter
}

// AuditLogWriter interface for writing audit logs to external systems
type AuditLogWriter interface {
	Write(log AuditLog) error
}

// NewAuditLogger creates a new audit logger instance
func NewAuditLogger() AuditLogger {
	return &AuditLoggerImpl{
		loggerID: fmt.Sprintf("audit-logger-%d", common.ConsensusNow().Unix()),
		logs:     make([]AuditLog, 0),
		maxLogs:  100000, // Keep last 100k logs in memory
	}
}

// LogSecurityEvent logs a security-related event
func (al *AuditLoggerImpl) LogSecurityEvent(event SecurityEvent) error {
	auditLog := AuditLog{
		ID:        fmt.Sprintf("audit-%s", event.ID),
		EventType: string(event.Type),
		UserID:    event.UserID,
		Action:    "security_event",
		Resource:  event.Target,
		Result:    "logged",
		IPAddress: event.IPAddress,
		UserAgent: "",
		Details: map[string]interface{}{
			"event_id":    event.ID,
			"severity":    event.Severity,
			"description": event.Description,
			"source":      event.Source,
			"resolved":    event.Resolved,
		},
		Timestamp: event.Timestamp,
	}

	return al.storeLog(auditLog)
}

// LogAccessAttempt logs an access attempt
func (al *AuditLoggerImpl) LogAccessAttempt(attempt AccessAttempt) error {
	result := "success"
	if !attempt.Success {
		result = "failure"
	}

	auditLog := AuditLog{
		ID:        fmt.Sprintf("audit-access-%s", attempt.ID),
		EventType: "access_attempt",
		UserID:    attempt.UserID,
		Action:    attempt.Action,
		Resource:  attempt.Resource,
		Result:    result,
		IPAddress: attempt.IPAddress,
		UserAgent: attempt.UserAgent,
		Details: map[string]interface{}{
			"attempt_id": attempt.ID,
			"success":    attempt.Success,
			"reason":     attempt.Reason,
		},
		Timestamp: attempt.AttemptedAt,
	}

	return al.storeLog(auditLog)
}

// GetAuditLogs retrieves audit logs for a time period
func (al *AuditLoggerImpl) GetAuditLogs(start, end time.Time) ([]AuditLog, error) {
	al.mu.RLock()
	defer al.mu.RUnlock()

	if start.After(end) {
		return nil, fmt.Errorf("start time must be before end time")
	}

	logs := make([]AuditLog, 0)
	for _, log := range al.logs {
		if (log.Timestamp.Equal(start) || log.Timestamp.After(start)) &&
			(log.Timestamp.Equal(end) || log.Timestamp.Before(end)) {
			logs = append(logs, log)
		}
	}

	return logs, nil
}

// storeLog stores an audit log
func (al *AuditLoggerImpl) storeLog(log AuditLog) error {
	if log.ID == "" {
		log.ID = fmt.Sprintf("audit-%d-%d", common.ConsensusNow().Unix(), common.ConsensusNow().Nanosecond())
	}

	if log.Timestamp.IsZero() {
		log.Timestamp = common.ConsensusNow()
	}

	al.mu.Lock()
	defer al.mu.Unlock()

	al.logs = append(al.logs, log)

	// Maintain max logs limit
	if len(al.logs) > al.maxLogs {
		al.logs = al.logs[len(al.logs)-al.maxLogs:]
	}

	// Write to external system if configured
	if al.outputWriter != nil {
		if err := al.outputWriter.Write(log); err != nil {
			// Log error but don't fail the operation
			fmt.Printf("Failed to write audit log to external system: %v\n", err)
		}
	}

	return nil
}

// SetOutputWriter sets the external audit log writer
func (al *AuditLoggerImpl) SetOutputWriter(writer AuditLogWriter) {
	al.mu.Lock()
	defer al.mu.Unlock()
	al.outputWriter = writer
}

// GetAuditStatistics returns statistics about audit logs
func (al *AuditLoggerImpl) GetAuditStatistics() map[string]interface{} {
	al.mu.RLock()
	defer al.mu.RUnlock()

	stats := make(map[string]interface{})

	// Count by event type and result
	eventTypeCount := make(map[string]int)
	resultCount := make(map[string]int)
	userCount := make(map[string]int)

	for _, log := range al.logs {
		eventTypeCount[log.EventType]++
		resultCount[log.Result]++
		if log.UserID != "" {
			userCount[log.UserID]++
		}
	}

	stats["total_logs"] = len(al.logs)
	stats["logs_by_event_type"] = eventTypeCount
	stats["logs_by_result"] = resultCount
	stats["unique_users"] = len(userCount)
	stats["most_active_users"] = al.getMostActiveUsers(userCount, 5)

	return stats
}

// getMostActiveUsers returns the top N most active users
func (al *AuditLoggerImpl) getMostActiveUsers(userCount map[string]int, topN int) []map[string]interface{} {
	type userActivity struct {
		UserID string
		Count  int
	}

	activities := make([]userActivity, 0, len(userCount))
	for userID, count := range userCount {
		activities = append(activities, userActivity{UserID: userID, Count: count})
	}

	// Sort by count
	for i := 0; i < len(activities)-1; i++ {
		for j := i + 1; j < len(activities); j++ {
			if activities[j].Count > activities[i].Count {
				activities[i], activities[j] = activities[j], activities[i]
			}
		}
	}

	// Return top N
	result := make([]map[string]interface{}, 0, topN)
	for i := 0; i < topN && i < len(activities); i++ {
		result = append(result, map[string]interface{}{
			"user_id": activities[i].UserID,
			"count":   activities[i].Count,
		})
	}

	return result
}
