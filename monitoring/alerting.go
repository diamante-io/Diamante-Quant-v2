// Package monitoring provides alerting capabilities
package monitoring

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"diamante/common"

	"github.com/sirupsen/logrus"
)

// AlertManager manages alerts and notifications
type AlertManager struct {
	rules    map[string]*AlertRule
	channels map[string]AlertChannel
	config   *AlertConfig
	logger   *logrus.Logger

	// State
	activeAlerts map[string]*Alert
	alertHistory []Alert

	mu        sync.RWMutex
	isRunning bool
}

// AlertConfig contains alerting configuration
type AlertConfig struct {
	CheckInterval   time.Duration `json:"check_interval"`
	DefaultSeverity AlertSeverity `json:"default_severity"`
	MaxAlerts       int           `json:"max_alerts"`
	RetentionPeriod time.Duration `json:"retention_period"`
	EnableWebhooks  bool          `json:"enable_webhooks"`
	WebhookURL      string        `json:"webhook_url"`
}

// AlertRule defines conditions for triggering alerts
type AlertRule struct {
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	Description string            `json:"description"`
	Query       string            `json:"query"`
	Condition   AlertCondition    `json:"condition"`
	Threshold   float64           `json:"threshold"`
	Duration    time.Duration     `json:"duration"`
	Severity    AlertSeverity     `json:"severity"`
	Labels      map[string]string `json:"labels"`
	Annotations map[string]string `json:"annotations"`
	Channels    []string          `json:"channels"`
	Enabled     bool              `json:"enabled"`
}

// AlertCondition represents alert conditions
type AlertCondition string

const (
	AlertConditionGreaterThan AlertCondition = "gt"
	AlertConditionLessThan    AlertCondition = "lt"
	AlertConditionEquals      AlertCondition = "eq"
	AlertConditionNotEquals   AlertCondition = "ne"
)

// AlertSeverity represents alert severity levels
type AlertSeverity string

const (
	AlertSeverityCritical AlertSeverity = "critical"
	AlertSeverityWarning  AlertSeverity = "warning"
	AlertSeverityInfo     AlertSeverity = "info"
)

// AlertStatus represents alert status
type AlertStatus string

const (
	AlertStatusFiring   AlertStatus = "firing"
	AlertStatusResolved AlertStatus = "resolved"
	AlertStatusSilenced AlertStatus = "silenced"
)

// Alert represents an active alert
type Alert struct {
	ID          string                 `json:"id"`
	RuleID      string                 `json:"rule_id"`
	RuleName    string                 `json:"rule_name"`
	Status      AlertStatus            `json:"status"`
	Severity    AlertSeverity          `json:"severity"`
	Message     string                 `json:"message"`
	Description string                 `json:"description"`
	Labels      map[string]string      `json:"labels"`
	Annotations map[string]string      `json:"annotations"`
	Value       float64                `json:"value"`
	Threshold   float64                `json:"threshold"`
	StartsAt    time.Time              `json:"starts_at"`
	EndsAt      *time.Time             `json:"ends_at,omitempty"`
	UpdatedAt   time.Time              `json:"updated_at"`
	Details     map[string]interface{} `json:"details,omitempty"`
}

// AlertChannel defines notification channels
type AlertChannel interface {
	Name() string
	Send(ctx context.Context, alert *Alert) error
	SupportedSeverities() []AlertSeverity
}

// NewAlertManager creates a new alert manager
func NewAlertManager(config *AlertConfig, logger *logrus.Logger) *AlertManager {
	if logger == nil {
		logger = logrus.New()
	}

	if config == nil {
		config = DefaultAlertConfig()
	}

	manager := &AlertManager{
		rules:        make(map[string]*AlertRule),
		channels:     make(map[string]AlertChannel),
		config:       config,
		logger:       logger,
		activeAlerts: make(map[string]*Alert),
		alertHistory: make([]Alert, 0),
	}

	// Register default alert rules
	manager.registerDefaultRules()

	// Register default channels
	manager.registerDefaultChannels()

	return manager
}

// DefaultAlertConfig returns default alert configuration
func DefaultAlertConfig() *AlertConfig {
	return &AlertConfig{
		CheckInterval:   30 * time.Second,
		DefaultSeverity: AlertSeverityWarning,
		MaxAlerts:       1000,
		RetentionPeriod: 24 * time.Hour,
		EnableWebhooks:  false,
	}
}

// Start starts the alert manager
func (am *AlertManager) Start(ctx context.Context) error {
	am.mu.Lock()
	defer am.mu.Unlock()

	if am.isRunning {
		return fmt.Errorf("alert manager already running")
	}

	// Start alert evaluation loop
	go am.evaluateRules(ctx)

	// Start cleanup routine
	go am.cleanupAlerts(ctx)

	am.isRunning = true
	am.logger.Info("Alert manager started")

	return nil
}

// Stop stops the alert manager
func (am *AlertManager) Stop() error {
	am.mu.Lock()
	defer am.mu.Unlock()

	am.isRunning = false
	am.logger.Info("Alert manager stopped")

	return nil
}

// RegisterRule registers an alert rule
func (am *AlertManager) RegisterRule(rule *AlertRule) error {
	am.mu.Lock()
	defer am.mu.Unlock()

	if _, exists := am.rules[rule.ID]; exists {
		return fmt.Errorf("rule already exists: %s", rule.ID)
	}

	am.rules[rule.ID] = rule
	am.logger.WithField("rule", rule.ID).Debug("Alert rule registered")

	return nil
}

// RegisterChannel registers an alert channel
func (am *AlertManager) RegisterChannel(channel AlertChannel) {
	am.mu.Lock()
	defer am.mu.Unlock()

	am.channels[channel.Name()] = channel
	am.logger.WithField("channel", channel.Name()).Debug("Alert channel registered")
}

// FireAlert fires an alert
func (am *AlertManager) FireAlert(ruleID string, value float64, labels map[string]string) error {
	am.mu.RLock()
	rule, exists := am.rules[ruleID]
	am.mu.RUnlock()

	if !exists {
		return fmt.Errorf("rule not found: %s", ruleID)
	}

	alertID := generateAlertID(ruleID, labels)

	am.mu.Lock()
	existingAlert, isActive := am.activeAlerts[alertID]
	am.mu.Unlock()

	if isActive {
		// Update existing alert
		existingAlert.Value = value
		existingAlert.UpdatedAt = common.ConsensusNow()
		return nil
	}

	// Create new alert
	alert := &Alert{
		ID:          alertID,
		RuleID:      ruleID,
		RuleName:    rule.Name,
		Status:      AlertStatusFiring,
		Severity:    rule.Severity,
		Message:     am.formatAlertMessage(rule, value),
		Description: rule.Description,
		Labels:      mergeLabels(rule.Labels, labels),
		Annotations: rule.Annotations,
		Value:       value,
		Threshold:   rule.Threshold,
		StartsAt:    common.ConsensusNow(),
		UpdatedAt:   common.ConsensusNow(),
	}

	am.mu.Lock()
	am.activeAlerts[alertID] = alert
	am.alertHistory = append(am.alertHistory, *alert)
	am.mu.Unlock()

	// Send notifications
	go am.sendNotifications(alert, rule.Channels)

	am.logger.WithFields(logrus.Fields{
		"alert":    alertID,
		"rule":     ruleID,
		"severity": rule.Severity,
		"value":    value,
	}).Warn("Alert fired")

	return nil
}

// ResolveAlert resolves an alert
func (am *AlertManager) ResolveAlert(ruleID string, labels map[string]string) error {
	alertID := generateAlertID(ruleID, labels)

	am.mu.Lock()
	defer am.mu.Unlock()

	alert, exists := am.activeAlerts[alertID]
	if !exists {
		return nil // Already resolved
	}

	// Mark as resolved
	now := common.ConsensusNow()
	alert.Status = AlertStatusResolved
	alert.EndsAt = &now
	alert.UpdatedAt = now

	// Remove from active alerts
	delete(am.activeAlerts, alertID)

	// Add to history
	am.alertHistory = append(am.alertHistory, *alert)

	am.logger.WithField("alert", alertID).Info("Alert resolved")

	return nil
}

// registerDefaultRules registers default alert rules
func (am *AlertManager) registerDefaultRules() {
	rules := []*AlertRule{
		{
			ID:          "high_tps_drop",
			Name:        "High TPS Drop",
			Description: "Transaction rate has dropped significantly",
			Query:       "rate(diamante_transactions_total[5m])",
			Condition:   AlertConditionLessThan,
			Threshold:   1000, // Less than 1000 TPS
			Duration:    2 * time.Minute,
			Severity:    AlertSeverityWarning,
			Labels:      map[string]string{"component": "transaction"},
			Annotations: map[string]string{
				"summary":     "Transaction processing rate is low",
				"description": "The transaction processing rate has dropped below {{ .Threshold }} TPS",
			},
			Channels: []string{"log", "webhook"},
			Enabled:  true,
		},
		{
			ID:          "consensus_failure",
			Name:        "Consensus Failure",
			Description: "Consensus rounds are failing",
			Query:       "rate(diamante_consensus_rounds_total{result='failed'}[5m])",
			Condition:   AlertConditionGreaterThan,
			Threshold:   0.1, // More than 10% failure rate
			Duration:    1 * time.Minute,
			Severity:    AlertSeverityCritical,
			Labels:      map[string]string{"component": "consensus"},
			Annotations: map[string]string{
				"summary":     "Consensus rounds are failing",
				"description": "Consensus failure rate is {{ .Value }}% which exceeds {{ .Threshold }}%",
			},
			Channels: []string{"log", "webhook"},
			Enabled:  true,
		},
		{
			ID:          "high_memory_usage",
			Name:        "High Memory Usage",
			Description: "Memory usage is critically high",
			Query:       "diamante_system_memory_usage",
			Condition:   AlertConditionGreaterThan,
			Threshold:   2147483648, // 2GB
			Duration:    5 * time.Minute,
			Severity:    AlertSeverityWarning,
			Labels:      map[string]string{"component": "system"},
			Annotations: map[string]string{
				"summary":     "Memory usage is high",
				"description": "Memory usage is {{ .Value | humanizeBytes }} which exceeds {{ .Threshold | humanizeBytes }}",
			},
			Channels: []string{"log"},
			Enabled:  true,
		},
		{
			ID:          "peer_count_low",
			Name:        "Low Peer Count",
			Description: "Number of connected peers is low",
			Query:       "diamante_peer_count",
			Condition:   AlertConditionLessThan,
			Threshold:   5,
			Duration:    2 * time.Minute,
			Severity:    AlertSeverityWarning,
			Labels:      map[string]string{"component": "network"},
			Annotations: map[string]string{
				"summary":     "Peer count is low",
				"description": "Only {{ .Value }} peers connected, minimum recommended is {{ .Threshold }}",
			},
			Channels: []string{"log"},
			Enabled:  true,
		},
		{
			ID:          "security_threat_detected",
			Name:        "Security Threat Detected",
			Description: "Security threat level is elevated",
			Query:       "diamante_threat_level",
			Condition:   AlertConditionGreaterThan,
			Threshold:   2, // Above medium threat level
			Duration:    0, // Immediate
			Severity:    AlertSeverityCritical,
			Labels:      map[string]string{"component": "security"},
			Annotations: map[string]string{
				"summary":     "Security threat detected",
				"description": "Threat level is {{ .Value }} which indicates elevated security risk",
			},
			Channels: []string{"log", "webhook"},
			Enabled:  true,
		},
	}

	for _, rule := range rules {
		am.RegisterRule(rule)
	}
}

// registerDefaultChannels registers default alert channels
func (am *AlertManager) registerDefaultChannels() {
	am.RegisterChannel(&LogChannel{logger: am.logger})

	if am.config.EnableWebhooks && am.config.WebhookURL != "" {
		am.RegisterChannel(&WebhookChannel{
			url:    am.config.WebhookURL,
			logger: am.logger,
		})
	}
}

// evaluateRules evaluates alert rules periodically
func (am *AlertManager) evaluateRules(ctx context.Context) {
	ticker := time.NewTicker(am.config.CheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			am.evaluateAllRules(ctx)
		}
	}
}

// evaluateAllRules evaluates all enabled rules
func (am *AlertManager) evaluateAllRules(ctx context.Context) {
	am.mu.RLock()
	rules := make([]*AlertRule, 0, len(am.rules))
	for _, rule := range am.rules {
		if rule.Enabled {
			rules = append(rules, rule)
		}
	}
	am.mu.RUnlock()

	for _, rule := range rules {
		am.evaluateRule(ctx, rule)
	}
}

// evaluateRule evaluates a single rule
func (am *AlertManager) evaluateRule(ctx context.Context, rule *AlertRule) {
	// Mock metric evaluation - in real implementation, this would query metrics
	value := am.mockEvaluateQuery(rule.Query)

	shouldFire := false
	switch rule.Condition {
	case AlertConditionGreaterThan:
		shouldFire = value > rule.Threshold
	case AlertConditionLessThan:
		shouldFire = value < rule.Threshold
	case AlertConditionEquals:
		shouldFire = value == rule.Threshold
	case AlertConditionNotEquals:
		shouldFire = value != rule.Threshold
	}

	if shouldFire {
		am.FireAlert(rule.ID, value, rule.Labels)
	} else {
		am.ResolveAlert(rule.ID, rule.Labels)
	}
}

// mockEvaluateQuery mock metric evaluation
func (am *AlertManager) mockEvaluateQuery(query string) float64 {
	// Mock values for different queries
	switch {
	case query == "rate(diamante_transactions_total[5m])":
		return 50000.0 // Mock TPS
	case query == "rate(diamante_consensus_rounds_total{result='failed'}[5m])":
		return 0.05 // 5% failure rate
	case query == "diamante_system_memory_usage":
		return 1.5 * 1024 * 1024 * 1024 // 1.5GB
	case query == "diamante_peer_count":
		return 8.0 // 8 peers
	case query == "diamante_threat_level":
		return 1.0 // Low threat level
	default:
		return 0.0
	}
}

// sendNotifications sends alert notifications to configured channels
func (am *AlertManager) sendNotifications(alert *Alert, channelNames []string) {
	for _, channelName := range channelNames {
		am.mu.RLock()
		channel, exists := am.channels[channelName]
		am.mu.RUnlock()

		if !exists {
			am.logger.WithField("channel", channelName).Warn("Alert channel not found")
			continue
		}

		// Check if channel supports this severity
		if !am.channelSupportsSeverity(channel, alert.Severity) {
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		if err := channel.Send(ctx, alert); err != nil {
			am.logger.WithError(err).WithField("channel", channelName).Error("Failed to send alert notification")
		}
		cancel()
	}
}

// channelSupportsSeverity checks if channel supports alert severity
func (am *AlertManager) channelSupportsSeverity(channel AlertChannel, severity AlertSeverity) bool {
	supported := channel.SupportedSeverities()
	if len(supported) == 0 {
		return true // No restrictions
	}

	for _, s := range supported {
		if s == severity {
			return true
		}
	}

	return false
}

// cleanupAlerts cleans up old alerts
func (am *AlertManager) cleanupAlerts(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			am.performCleanup()
		}
	}
}

// performCleanup removes old alerts from history
func (am *AlertManager) performCleanup() {
	am.mu.Lock()
	defer am.mu.Unlock()

	cutoff := common.ConsensusNow().Add(-am.config.RetentionPeriod)
	newHistory := make([]Alert, 0)

	for _, alert := range am.alertHistory {
		if alert.StartsAt.After(cutoff) {
			newHistory = append(newHistory, alert)
		}
	}

	removed := len(am.alertHistory) - len(newHistory)
	am.alertHistory = newHistory

	if removed > 0 {
		am.logger.WithField("removed", removed).Debug("Cleaned up old alerts")
	}
}

// formatAlertMessage formats alert message
func (am *AlertManager) formatAlertMessage(rule *AlertRule, value float64) string {
	return fmt.Sprintf("%s: %.2f (threshold: %.2f)", rule.Name, value, rule.Threshold)
}

// GetActiveAlerts returns all active alerts
func (am *AlertManager) GetActiveAlerts() []*Alert {
	am.mu.RLock()
	defer am.mu.RUnlock()

	alerts := make([]*Alert, 0, len(am.activeAlerts))
	for _, alert := range am.activeAlerts {
		alerts = append(alerts, alert)
	}

	return alerts
}

// GetAlertHistory returns alert history
func (am *AlertManager) GetAlertHistory(limit int) []Alert {
	am.mu.RLock()
	defer am.mu.RUnlock()

	if limit <= 0 || limit > len(am.alertHistory) {
		limit = len(am.alertHistory)
	}

	// Return most recent alerts
	start := len(am.alertHistory) - limit
	return am.alertHistory[start:]
}

// Helper functions

func generateAlertID(ruleID string, labels map[string]string) string {
	// Simple alert ID generation - in real implementation, use proper hashing
	return fmt.Sprintf("%s-%d", ruleID, common.ConsensusNow().UnixNano())
}

func mergeLabels(base, additional map[string]string) map[string]string {
	result := make(map[string]string)

	for k, v := range base {
		result[k] = v
	}

	for k, v := range additional {
		result[k] = v
	}

	return result
}

// Default alert channels

// LogChannel logs alerts to the logger
type LogChannel struct {
	logger *logrus.Logger
}

func (c *LogChannel) Name() string                         { return "log" }
func (c *LogChannel) SupportedSeverities() []AlertSeverity { return nil } // All severities

func (c *LogChannel) Send(ctx context.Context, alert *Alert) error {
	logLevel := logrus.WarnLevel
	if alert.Severity == AlertSeverityCritical {
		logLevel = logrus.ErrorLevel
	} else if alert.Severity == AlertSeverityInfo {
		logLevel = logrus.InfoLevel
	}

	c.logger.WithFields(logrus.Fields{
		"alert_id":  alert.ID,
		"rule":      alert.RuleName,
		"severity":  alert.Severity,
		"value":     alert.Value,
		"threshold": alert.Threshold,
		"labels":    alert.Labels,
	}).Log(logLevel, alert.Message)

	return nil
}

// WebhookChannel sends alerts via HTTP webhook
type WebhookChannel struct {
	url    string
	logger *logrus.Logger
}

func (c *WebhookChannel) Name() string                         { return "webhook" }
func (c *WebhookChannel) SupportedSeverities() []AlertSeverity { return nil } // All severities

func (c *WebhookChannel) Send(ctx context.Context, alert *Alert) error {
	payload, err := json.Marshal(alert)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", c.url, bytes.NewBuffer(payload))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("webhook returned status %d", resp.StatusCode)
	}

	c.logger.WithField("alert", alert.ID).Debug("Alert sent via webhook")
	return nil
}
