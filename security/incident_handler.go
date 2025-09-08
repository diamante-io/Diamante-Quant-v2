package security

import (
	"context"
	"diamante/common"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"
)

// IncidentHandler manages security incidents and responses
type IncidentHandler struct {
	mu           sync.RWMutex
	incidents    map[string]*SecurityIncident
	config       *IncidentConfig
	alertChan    chan SecurityAlert
	responseChan chan IncidentResponse
	logger       *log.Logger
}

// SecurityIncident represents a security incident
type SecurityIncident struct {
	ID          string
	Type        string
	Severity    string
	Source      string
	Description string
	Timestamp   time.Time
	Status      string
	Metadata    map[string]interface{}
	Actions     []IncidentAction
	Response    *IncidentResponse
}

// IncidentAction represents an action taken in response to an incident
type IncidentAction struct {
	Type        string
	Timestamp   time.Time
	Description string
	Success     bool
	Error       string
}

// IncidentResponse represents a response to an incident
type IncidentResponse struct {
	IncidentID string
	Action     string
	Success    bool
	Message    string
	Timestamp  time.Time
	Metadata   map[string]interface{}
}

// IncidentConfig contains configuration for incident handling
type IncidentConfig struct {
	AutoResponse      bool
	ResponseTimeout   time.Duration
	MaxIncidents      int
	RetentionPeriod   time.Duration
	EscalationRules   map[string]EscalationRule
	NotificationRules map[string]NotificationRule
}

// EscalationRule defines when and how to escalate incidents
type EscalationRule struct {
	Severity   string
	Threshold  int
	TimeWindow time.Duration
	Action     string
	Recipients []string
}

// NotificationRule defines how to notify about incidents
type NotificationRule struct {
	Severity string
	Channels []string
	Template string
}

// NewIncidentHandler creates a new incident handler
func NewIncidentHandler(config *IncidentConfig, logger *log.Logger) *IncidentHandler {
	if config == nil {
		config = &IncidentConfig{
			AutoResponse:      true,
			ResponseTimeout:   time.Minute * 5,
			MaxIncidents:      1000,
			RetentionPeriod:   time.Hour * 24,
			EscalationRules:   make(map[string]EscalationRule),
			NotificationRules: make(map[string]NotificationRule),
		}
	}

	if logger == nil {
		logger = log.New(log.Writer(), "[SECURITY] ", log.LstdFlags)
	}

	ih := &IncidentHandler{
		incidents:    make(map[string]*SecurityIncident),
		config:       config,
		alertChan:    make(chan SecurityAlert, 100),
		responseChan: make(chan IncidentResponse, 100),
		logger:       logger,
	}

	// Start incident processor
	go ih.processIncidents()

	return ih
}

// Start starts the incident handler
func (ih *IncidentHandler) Start() error {
	// Already started in constructor
	return nil
}

// HandleSuspiciousActivity handles suspicious activity detection
func (ih *IncidentHandler) HandleSuspiciousActivity(clientIP, pattern string, r *http.Request) {
	alert := SecurityAlert{
		Type:        "SUSPICIOUS_ACTIVITY",
		Severity:    "medium",
		Source:      clientIP,
		Description: fmt.Sprintf("Suspicious activity detected: %s", pattern),
		Timestamp:   common.ConsensusNow(),
		Metadata: map[string]interface{}{
			"pattern":    pattern,
			"path":       r.URL.Path,
			"method":     r.Method,
			"user_agent": r.Header.Get("User-Agent"),
		},
	}

	ih.HandleAlert(context.Background(), alert)
}

// HandleAlert handles a security alert and potentially creates an incident
func (ih *IncidentHandler) HandleAlert(ctx context.Context, alert SecurityAlert) error {
	ih.mu.Lock()
	defer ih.mu.Unlock()

	// Check if this is a new incident or part of an existing one
	incidentID := ih.generateIncidentID(alert)

	incident, exists := ih.incidents[incidentID]
	if !exists {
		incident = &SecurityIncident{
			ID:          incidentID,
			Type:        alert.Type,
			Severity:    alert.Severity,
			Source:      alert.Source,
			Description: alert.Description,
			Timestamp:   alert.Timestamp,
			Status:      "open",
			Metadata:    alert.Metadata,
			Actions:     make([]IncidentAction, 0),
		}
		ih.incidents[incidentID] = incident
		ih.logger.Printf("New security incident created: %s (Type: %s, Severity: %s)",
			incidentID, alert.Type, alert.Severity)
	}

	// Handle the incident based on severity and type
	if ih.config.AutoResponse {
		go ih.handleIncidentResponse(incident)
	}

	return nil
}

// handleIncidentResponse handles the response to an incident
func (ih *IncidentHandler) handleIncidentResponse(incident *SecurityIncident) {
	ctx, cancel := context.WithTimeout(context.Background(), ih.config.ResponseTimeout)
	defer cancel()

	var response IncidentResponse

	switch incident.Type {
	case "HIGH_FREQUENCY":
		response = ih.handleHighFrequencyIncident(ctx, incident)
	case "SQL_INJECTION":
		response = ih.handleSQLInjectionIncident(ctx, incident)
	case "XSS_ATTEMPT":
		response = ih.handleXSSIncident(ctx, incident)
	case "UNAUTHORIZED_ACCESS":
		response = ih.handleUnauthorizedAccessIncident(ctx, incident)
	default:
		response = ih.handleGenericIncident(ctx, incident)
	}

	// Update incident with response
	ih.mu.Lock()
	incident.Response = &response
	incident.Actions = append(incident.Actions, IncidentAction{
		Type:        response.Action,
		Timestamp:   response.Timestamp,
		Description: response.Message,
		Success:     response.Success,
	})
	ih.mu.Unlock()

	// Send response to channel
	select {
	case ih.responseChan <- response:
	default:
		ih.logger.Printf("Response channel full, dropping response for incident %s", incident.ID)
	}
}

// handleHighFrequencyIncident handles high frequency request incidents
func (ih *IncidentHandler) handleHighFrequencyIncident(ctx context.Context, incident *SecurityIncident) IncidentResponse {
	// Extract IP from metadata
	ip := ""
	if incident.Metadata != nil {
		if ipVal, ok := incident.Metadata["ip"]; ok {
			ip = fmt.Sprintf("%v", ipVal)
		}
	}

	// Block IP temporarily
	success := ih.blockIP(ip, time.Minute*30)

	return IncidentResponse{
		IncidentID: incident.ID,
		Action:     "BLOCK_IP",
		Success:    success,
		Message:    fmt.Sprintf("Temporarily blocked IP %s for 30 minutes", ip),
		Timestamp:  common.ConsensusNow(),
		Metadata: map[string]interface{}{
			"ip":       ip,
			"duration": "30m",
		},
	}
}

// handleSQLInjectionIncident handles SQL injection incidents
func (ih *IncidentHandler) handleSQLInjectionIncident(ctx context.Context, incident *SecurityIncident) IncidentResponse {
	// Extract IP from metadata
	ip := ""
	if incident.Metadata != nil {
		if ipVal, ok := incident.Metadata["ip"]; ok {
			ip = fmt.Sprintf("%v", ipVal)
		}
	}

	// Block IP for longer period
	success := ih.blockIP(ip, time.Hour*24)

	return IncidentResponse{
		IncidentID: incident.ID,
		Action:     "BLOCK_IP_EXTENDED",
		Success:    success,
		Message:    fmt.Sprintf("Blocked IP %s for 24 hours due to SQL injection attempt", ip),
		Timestamp:  common.ConsensusNow(),
		Metadata: map[string]interface{}{
			"ip":       ip,
			"duration": "24h",
			"reason":   "sql_injection",
		},
	}
}

// handleXSSIncident handles XSS incidents
func (ih *IncidentHandler) handleXSSIncident(ctx context.Context, incident *SecurityIncident) IncidentResponse {
	// Extract IP from metadata
	ip := ""
	if incident.Metadata != nil {
		if ipVal, ok := incident.Metadata["ip"]; ok {
			ip = fmt.Sprintf("%v", ipVal)
		}
	}

	// Block IP for extended period
	success := ih.blockIP(ip, time.Hour*12)

	return IncidentResponse{
		IncidentID: incident.ID,
		Action:     "BLOCK_IP_EXTENDED",
		Success:    success,
		Message:    fmt.Sprintf("Blocked IP %s for 12 hours due to XSS attempt", ip),
		Timestamp:  common.ConsensusNow(),
		Metadata: map[string]interface{}{
			"ip":       ip,
			"duration": "12h",
			"reason":   "xss_attempt",
		},
	}
}

// handleUnauthorizedAccessIncident handles unauthorized access incidents
func (ih *IncidentHandler) handleUnauthorizedAccessIncident(ctx context.Context, incident *SecurityIncident) IncidentResponse {
	// Extract IP from metadata
	ip := ""
	if incident.Metadata != nil {
		if ipVal, ok := incident.Metadata["ip"]; ok {
			ip = fmt.Sprintf("%v", ipVal)
		}
	}

	// Block IP temporarily
	success := ih.blockIP(ip, time.Hour*2)

	return IncidentResponse{
		IncidentID: incident.ID,
		Action:     "BLOCK_IP",
		Success:    success,
		Message:    fmt.Sprintf("Blocked IP %s for 2 hours due to unauthorized access", ip),
		Timestamp:  common.ConsensusNow(),
		Metadata: map[string]interface{}{
			"ip":       ip,
			"duration": "2h",
			"reason":   "unauthorized_access",
		},
	}
}

// handleGenericIncident handles generic incidents
func (ih *IncidentHandler) handleGenericIncident(ctx context.Context, incident *SecurityIncident) IncidentResponse {
	return IncidentResponse{
		IncidentID: incident.ID,
		Action:     "LOG_ONLY",
		Success:    true,
		Message:    fmt.Sprintf("Logged security incident of type %s", incident.Type),
		Timestamp:  common.ConsensusNow(),
		Metadata: map[string]interface{}{
			"incident_type": incident.Type,
		},
	}
}

// blockIP simulates blocking an IP address
func (ih *IncidentHandler) blockIP(ip string, duration time.Duration) bool {
	// In a real implementation, this would interface with a firewall or proxy
	ih.logger.Printf("Blocking IP %s for %v", ip, duration)
	return true
}

// generateIncidentID generates a unique incident ID
func (ih *IncidentHandler) generateIncidentID(alert SecurityAlert) string {
	return fmt.Sprintf("%s-%s-%d", alert.Type, alert.Source, alert.Timestamp.Unix())
}

// processIncidents processes incidents in the background
func (ih *IncidentHandler) processIncidents() {
	ticker := time.NewTicker(time.Minute * 5)
	defer ticker.Stop()

	for range ticker.C {
		ih.mu.Lock()
		now := common.ConsensusNow()

		// Clean up old incidents
		for id, incident := range ih.incidents {
			if now.Sub(incident.Timestamp) > ih.config.RetentionPeriod {
				delete(ih.incidents, id)
			}
		}

		ih.mu.Unlock()
	}
}

// GetIncidents returns all incidents
func (ih *IncidentHandler) GetIncidents() map[string]*SecurityIncident {
	ih.mu.RLock()
	defer ih.mu.RUnlock()

	incidents := make(map[string]*SecurityIncident)
	for id, incident := range ih.incidents {
		incidents[id] = incident
	}

	return incidents
}

// GetIncident returns a specific incident
func (ih *IncidentHandler) GetIncident(id string) *SecurityIncident {
	ih.mu.RLock()
	defer ih.mu.RUnlock()

	return ih.incidents[id]
}

// GetStats returns incident handler statistics
func (ih *IncidentHandler) GetStats() map[string]interface{} {
	ih.mu.RLock()
	defer ih.mu.RUnlock()

	stats := map[string]interface{}{
		"total_incidents":   len(ih.incidents),
		"pending_responses": len(ih.responseChan),
	}

	// Count by severity
	severityCount := make(map[string]int)
	statusCount := make(map[string]int)

	for _, incident := range ih.incidents {
		severityCount[incident.Severity]++
		statusCount[incident.Status]++
	}

	stats["by_severity"] = severityCount
	stats["by_status"] = statusCount

	return stats
}

// GetResponseChannel returns the response channel
func (ih *IncidentHandler) GetResponseChannel() <-chan IncidentResponse {
	return ih.responseChan
}

// Stop stops the incident handler
func (ih *IncidentHandler) Stop() {
	close(ih.alertChan)
	close(ih.responseChan)
}
