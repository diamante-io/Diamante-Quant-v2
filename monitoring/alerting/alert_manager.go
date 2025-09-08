package alerting

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/smtp"
	"strings"
	"sync"
	"time"

	"diamante/consensus"

	"github.com/sirupsen/logrus"
)

// AlertContext contains typed context information for alerts
type AlertContext struct {
	// Core alert information
	AlertID   string    `json:"alert_id"`
	Timestamp time.Time `json:"timestamp"`
	Severity  string    `json:"severity"`
	Source    string    `json:"source"`
	Component string    `json:"component"`

	// Metric values
	StringMetrics map[string]string  `json:"string_metrics,omitempty"`
	IntMetrics    map[string]int64   `json:"int_metrics,omitempty"`
	FloatMetrics  map[string]float64 `json:"float_metrics,omitempty"`
	BoolMetrics   map[string]bool    `json:"bool_metrics,omitempty"`

	// Additional context
	Tags        []string          `json:"tags,omitempty"`
	Labels      map[string]string `json:"labels,omitempty"`
	Annotations map[string]string `json:"annotations,omitempty"`

	// Thresholds and current values
	Thresholds    map[string]float64 `json:"thresholds,omitempty"`
	CurrentValues map[string]float64 `json:"current_values,omitempty"`

	// Related information
	RelatedAlerts []string `json:"related_alerts,omitempty"`
	RunbookURL    string   `json:"runbook_url,omitempty"`
	DashboardURL  string   `json:"dashboard_url,omitempty"`
}

// MetricsSnapshot contains typed metrics for rule evaluation
type MetricsSnapshot struct {
	// System metrics
	SystemMetrics SystemMetrics `json:"system_metrics"`

	// Business metrics
	BusinessMetrics BusinessMetrics `json:"business_metrics"`

	// Network metrics
	NetworkMetrics NetworkMetrics `json:"network_metrics"`

	// Consensus metrics
	ConsensusMetrics ConsensusMetrics `json:"consensus_metrics"`

	// Custom metrics
	CustomStringMetrics map[string]string  `json:"custom_string_metrics,omitempty"`
	CustomIntMetrics    map[string]int64   `json:"custom_int_metrics,omitempty"`
	CustomFloatMetrics  map[string]float64 `json:"custom_float_metrics,omitempty"`
	CustomBoolMetrics   map[string]bool    `json:"custom_bool_metrics,omitempty"`
}

// SystemMetrics contains system-level metrics
type SystemMetrics struct {
	CPUUsagePercent     float64 `json:"cpu_usage_percent"`
	MemoryUsageBytes    int64   `json:"memory_usage_bytes"`
	MemoryUsagePercent  float64 `json:"memory_usage_percent"`
	DiskUsageBytes      int64   `json:"disk_usage_bytes"`
	DiskUsagePercent    float64 `json:"disk_usage_percent"`
	LoadAverage         float64 `json:"load_average"`
	OpenFileDescriptors int64   `json:"open_file_descriptors"`
	GoroutineCount      int64   `json:"goroutine_count"`
}

// BusinessMetrics contains business-level metrics
type BusinessMetrics struct {
	TransactionsProcessed int64   `json:"transactions_processed"`
	TransactionsFailed    int64   `json:"transactions_failed"`
	TransactionsPerSecond float64 `json:"transactions_per_second"`
	BlocksProduced        int64   `json:"blocks_produced"`
	BlocksValidated       int64   `json:"blocks_validated"`
	ContractsDeployed     int64   `json:"contracts_deployed"`
	ContractCalls         int64   `json:"contract_calls"`
	FeesCollected         int64   `json:"fees_collected"`
}

// NetworkMetrics contains network-level metrics
type NetworkMetrics struct {
	PeerCount           int64   `json:"peer_count"`
	InboundConnections  int64   `json:"inbound_connections"`
	OutboundConnections int64   `json:"outbound_connections"`
	BytesSent           int64   `json:"bytes_sent"`
	BytesReceived       int64   `json:"bytes_received"`
	MessagesSent        int64   `json:"messages_sent"`
	MessagesReceived    int64   `json:"messages_received"`
	NetworkLatencyMs    float64 `json:"network_latency_ms"`
	PacketLossPercent   float64 `json:"packet_loss_percent"`
}

// ConsensusMetrics contains consensus-level metrics
type ConsensusMetrics struct {
	BlockHeight        int64   `json:"block_height"`
	ConsensusRounds    int64   `json:"consensus_rounds"`
	ValidatorCount     int64   `json:"validator_count"`
	ActiveValidators   int64   `json:"active_validators"`
	ConsensusLatencyMs float64 `json:"consensus_latency_ms"`
	ParticipationRate  float64 `json:"participation_rate"`
	SlashingEvents     int64   `json:"slashing_events"`
	MissedBlocks       int64   `json:"missed_blocks"`
}

// AlertCallback defines a function triggered when a rule fires.
type AlertCallback func(message string, ctx AlertContext)

// Rule defines an alerting rule evaluated on a metrics snapshot.
type Rule struct {
	Name     string
	Check    func(MetricsSnapshot) bool
	Callback AlertCallback
}

// Manager evaluates rules and dispatches alerts.
type Manager struct {
	rules      []Rule
	logger     *logrus.Logger
	httpClient *http.Client
	mu         sync.RWMutex
}

// NewManager creates an Alert Manager with no rules and configured HTTP client.
func NewManager(logger *logrus.Logger) *Manager {
	if logger == nil {
		logger = logrus.New()
	}

	// Configure HTTP client with timeout and connection pooling
	httpClient := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 10,
			IdleConnTimeout:     90 * time.Second,
			DisableCompression:  true,
		},
	}

	return &Manager{
		logger:     logger,
		httpClient: httpClient,
		rules:      make([]Rule, 0),
	}
}

// AddRule registers a new alerting rule.
func (m *Manager) AddRule(r Rule) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rules = append(m.rules, r)
}

// Evaluate checks rules against the provided metrics snapshot.
func (m *Manager) Evaluate(metrics MetricsSnapshot) {
	m.mu.RLock()
	rules := make([]Rule, len(m.rules))
	copy(rules, m.rules)
	m.mu.RUnlock()

	for _, r := range rules {
		if r.Check != nil && r.Check(metrics) {
			m.logger.WithFields(logrus.Fields{
				"rule":    r.Name,
				"metrics": metrics,
			}).Warn("alert triggered")

			if r.Callback != nil {
				// Create alert context from metrics
				alertCtx := m.createAlertContext(r.Name, metrics)

				// Execute callback asynchronously with timeout
				go func(rule Rule, ctx AlertContext) {
					done := make(chan struct{})
					go func() {
						defer close(done)
						rule.Callback(rule.Name, ctx)
					}()

					select {
					case <-done:
						// Callback completed successfully
					case <-time.After(5 * time.Minute):
						m.logger.WithField("rule", rule.Name).Error("alert callback timed out")
					}
				}(r, alertCtx)
			}
		}
	}
}

// createAlertContext creates a typed alert context from metrics
func (m *Manager) createAlertContext(ruleName string, metrics MetricsSnapshot) AlertContext {
	return AlertContext{
		AlertID:   fmt.Sprintf("%s-%d", ruleName, consensus.ConsensusUnix()),
		Timestamp: consensus.ConsensusNow(),
		Severity:  "warning",
		Source:    "diamante-monitoring",
		Component: "alert-manager",
		StringMetrics: map[string]string{
			"rule_name": ruleName,
		},
		FloatMetrics: map[string]float64{
			"cpu_usage_percent":       metrics.SystemMetrics.CPUUsagePercent,
			"memory_usage_percent":    metrics.SystemMetrics.MemoryUsagePercent,
			"disk_usage_percent":      metrics.SystemMetrics.DiskUsagePercent,
			"transactions_per_second": metrics.BusinessMetrics.TransactionsPerSecond,
			"network_latency_ms":      metrics.NetworkMetrics.NetworkLatencyMs,
			"consensus_latency_ms":    metrics.ConsensusMetrics.ConsensusLatencyMs,
		},
		IntMetrics: map[string]int64{
			"peer_count":             metrics.NetworkMetrics.PeerCount,
			"transactions_processed": metrics.BusinessMetrics.TransactionsProcessed,
			"transactions_failed":    metrics.BusinessMetrics.TransactionsFailed,
			"block_height":           metrics.ConsensusMetrics.BlockHeight,
			"validator_count":        metrics.ConsensusMetrics.ValidatorCount,
		},
	}
}

// SlackCallback creates a callback that sends alerts to Slack with error handling.
func SlackCallback(webhookURL, channel string, logger *logrus.Logger) AlertCallback {
	if logger == nil {
		logger = logrus.New()
	}

	return func(msg string, ctx AlertContext) {
		// Define Slack-specific types
		type SlackAttachment struct {
			Text  string `json:"text"`
			Color string `json:"color"`
		}

		type SlackPayload struct {
			Channel     string            `json:"channel"`
			Text        string            `json:"text"`
			Username    string            `json:"username"`
			IconEmoji   string            `json:"icon_emoji"`
			Attachments []SlackAttachment `json:"attachments,omitempty"`
		}

		payload := SlackPayload{
			Channel:   channel,
			Text:      msg,
			Username:  "Diamante Alert",
			IconEmoji: ":warning:",
		}

		// Add context as attachment if present
		if len(ctx.FloatMetrics) > 0 || len(ctx.IntMetrics) > 0 {
			contextStr, err := json.MarshalIndent(ctx, "", "  ")
			if err != nil {
				logger.WithError(err).Error("failed to marshal alert context")
			} else {
				payload.Attachments = []SlackAttachment{
					{
						Text:  fmt.Sprintf("```\n%s\n```", string(contextStr)),
						Color: "danger",
					},
				}
			}
		}

		body, err := json.Marshal(payload)
		if err != nil {
			logger.WithError(err).Error("failed to marshal slack payload")
			// Fallback to simple message
			simplePayload := fmt.Sprintf(`{"channel":"%s","text":"%s"}`, channel, msg)
			body = []byte(simplePayload)
		}

		req, err := http.NewRequest("POST", webhookURL, bytes.NewBuffer(body))
		if err != nil {
			logger.WithError(err).Error("failed to create slack request")
			return
		}
		req.Header.Set("Content-Type", "application/json")

		client := &http.Client{Timeout: 10 * time.Second}
		resp, err := client.Do(req)
		if err != nil {
			logger.WithError(err).Error("failed to send slack alert")
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			logger.WithField("status", resp.StatusCode).Error("slack webhook returned non-200 status")
		}

		logger.WithFields(logrus.Fields{
			"channel": channel,
			"webhook": webhookURL,
		}).Info("slack alert sent successfully")
	}
}

// SMTPConfig holds SMTP server configuration
type SMTPConfig struct {
	Host     string
	Port     int
	Username string
	Password string
	From     string
	TLS      bool
	StartTLS bool
}

// EmailCallback creates a callback that sends email alerts with proper SMTP authentication.
func EmailCallback(config SMTPConfig, recipients []string, logger *logrus.Logger) AlertCallback {
	if logger == nil {
		logger = logrus.New()
	}

	return func(msg string, ctx AlertContext) {
		// Format email body
		body := formatEmailBody(msg, ctx)

		// Prepare email message
		subject := fmt.Sprintf("Diamante Alert: %s", msg)
		emailMsg := formatEmailMessage(config.From, recipients, subject, body)

		// Send email with retry logic
		var lastErr error
		for attempt := 0; attempt < 3; attempt++ {
			if err := sendEmail(config, recipients, emailMsg); err != nil {
				lastErr = fmt.Errorf("attempt %d failed: %w", attempt+1, err)
				logger.WithError(lastErr).Warn("email send attempt failed")

				// Use context-aware timing for retry delay
				if attempt < 2 { // Don't delay after the last attempt
					select {
					case <-time.After(time.Duration(attempt+1) * time.Second):
						// Retry delay completed
					default:
						// Non-blocking, proceed immediately if needed
					}
				}
				continue
			}

			logger.WithFields(logrus.Fields{
				"recipients": recipients,
				"subject":    subject,
			}).Info("email alert sent successfully")
			return
		}

		logger.WithError(lastErr).Error("failed to send email alert after all retries")
	}
}

// sendEmail handles the actual SMTP communication with proper authentication
func sendEmail(config SMTPConfig, recipients []string, message []byte) error {
	// Establish connection
	addr := fmt.Sprintf("%s:%d", config.Host, config.Port)

	var conn net.Conn
	var err error

	if config.TLS {
		// Direct TLS connection
		tlsConfig := &tls.Config{
			ServerName: config.Host,
		}
		conn, err = tls.Dial("tcp", addr, tlsConfig)
	} else {
		// Plain connection (may upgrade to TLS later)
		conn, err = net.Dial("tcp", addr)
	}

	if err != nil {
		return fmt.Errorf("failed to connect to SMTP server: %w", err)
	}
	defer conn.Close()

	// Create SMTP client
	client, err := smtp.NewClient(conn, config.Host)
	if err != nil {
		return fmt.Errorf("failed to create SMTP client: %w", err)
	}
	defer client.Quit()

	// STARTTLS if required and not already using TLS
	if config.StartTLS && !config.TLS {
		tlsConfig := &tls.Config{
			ServerName: config.Host,
		}
		if err := client.StartTLS(tlsConfig); err != nil {
			return fmt.Errorf("STARTTLS failed: %w", err)
		}
	}

	// Authenticate if credentials provided
	if config.Username != "" && config.Password != "" {
		auth := smtp.PlainAuth("", config.Username, config.Password, config.Host)
		if err := client.Auth(auth); err != nil {
			return fmt.Errorf("SMTP authentication failed: %w", err)
		}
	}

	// Send email
	if err := client.Mail(config.From); err != nil {
		return fmt.Errorf("failed to set sender: %w", err)
	}

	for _, recipient := range recipients {
		if err := client.Rcpt(recipient); err != nil {
			return fmt.Errorf("failed to set recipient %s: %w", recipient, err)
		}
	}

	writer, err := client.Data()
	if err != nil {
		return fmt.Errorf("failed to get data writer: %w", err)
	}

	if _, err := writer.Write(message); err != nil {
		return fmt.Errorf("failed to write message: %w", err)
	}

	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to close data writer: %w", err)
	}

	return nil
}

// formatEmailBody formats the email body with typed context
func formatEmailBody(msg string, ctx AlertContext) string {
	var body strings.Builder
	body.WriteString(fmt.Sprintf("Alert: %s\n\n", msg))
	body.WriteString(fmt.Sprintf("Timestamp: %s\n", ctx.Timestamp.Format(time.RFC3339)))
	body.WriteString(fmt.Sprintf("Alert ID: %s\n", ctx.AlertID))
	body.WriteString(fmt.Sprintf("Severity: %s\n", ctx.Severity))
	body.WriteString(fmt.Sprintf("Component: %s\n\n", ctx.Component))

	if len(ctx.FloatMetrics) > 0 {
		body.WriteString("Float Metrics:\n")
		for key, value := range ctx.FloatMetrics {
			body.WriteString(fmt.Sprintf("  %s: %.2f\n", key, value))
		}
		body.WriteString("\n")
	}

	if len(ctx.IntMetrics) > 0 {
		body.WriteString("Integer Metrics:\n")
		for key, value := range ctx.IntMetrics {
			body.WriteString(fmt.Sprintf("  %s: %d\n", key, value))
		}
		body.WriteString("\n")
	}

	if len(ctx.StringMetrics) > 0 {
		body.WriteString("String Metrics:\n")
		for key, value := range ctx.StringMetrics {
			body.WriteString(fmt.Sprintf("  %s: %s\n", key, value))
		}
		body.WriteString("\n")
	}

	if len(ctx.Thresholds) > 0 {
		body.WriteString("Thresholds:\n")
		for key, value := range ctx.Thresholds {
			currentVal := ctx.CurrentValues[key]
			body.WriteString(fmt.Sprintf("  %s: %.2f (threshold: %.2f)\n", key, currentVal, value))
		}
		body.WriteString("\n")
	}

	body.WriteString("---\nThis is an automated alert from Diamante monitoring system.")
	return body.String()
}

// formatEmailMessage formats a complete email message with headers
func formatEmailMessage(from string, to []string, subject, body string) []byte {
	var msg strings.Builder
	msg.WriteString(fmt.Sprintf("From: %s\r\n", from))
	msg.WriteString(fmt.Sprintf("To: %s\r\n", strings.Join(to, ", ")))
	msg.WriteString(fmt.Sprintf("Subject: %s\r\n", subject))
	msg.WriteString(fmt.Sprintf("Date: %s\r\n", consensus.ConsensusNow().Format(time.RFC1123Z)))
	msg.WriteString("MIME-Version: 1.0\r\n")
	msg.WriteString("Content-Type: text/plain; charset=UTF-8\r\n")
	msg.WriteString("\r\n")
	msg.WriteString(body)
	return []byte(msg.String())
}

// PagerDutyCallback creates a callback that sends alerts to PagerDuty with error handling.
func PagerDutyCallback(integrationKey string, logger *logrus.Logger) AlertCallback {
	if logger == nil {
		logger = logrus.New()
	}

	return func(msg string, ctx AlertContext) {
		// Create dedupe key from message and timestamp
		dedupeKey := fmt.Sprintf("%s-%d", msg, consensus.ConsensusUnix()/300) // 5-minute window

		// Define PagerDuty-specific types
		type PagerDutyPayloadDetails struct {
			Summary       string       `json:"summary"`
			Source        string       `json:"source"`
			Severity      string       `json:"severity"`
			Timestamp     string       `json:"timestamp"`
			CustomDetails AlertContext `json:"custom_details"`
		}

		type PagerDutyEvent struct {
			RoutingKey  string                  `json:"routing_key"`
			EventAction string                  `json:"event_action"`
			DedupKey    string                  `json:"dedup_key"`
			Client      string                  `json:"client"`
			ClientURL   string                  `json:"client_url"`
			Payload     PagerDutyPayloadDetails `json:"payload"`
		}

		payload := PagerDutyEvent{
			RoutingKey:  integrationKey,
			EventAction: "trigger",
			DedupKey:    dedupeKey,
			Client:      "Diamante Monitoring",
			ClientURL:   "https://diamante.io",
			Payload: PagerDutyPayloadDetails{
				Summary:       msg,
				Source:        "diamante",
				Severity:      "error",
				Timestamp:     ctx.Timestamp.Format(time.RFC3339),
				CustomDetails: ctx,
			},
		}

		body, err := json.Marshal(payload)
		if err != nil {
			logger.WithError(err).Error("failed to marshal pagerduty payload")
			// Create minimal fallback payload
			fallbackPayload := fmt.Sprintf(`{"routing_key":"%s","event_action":"trigger","payload":{"summary":"%s","source":"diamante","severity":"error"}}`,
				integrationKey, msg)
			body = []byte(fallbackPayload)
		}

		req, err := http.NewRequest("POST", "https://events.pagerduty.com/v2/enqueue", bytes.NewBuffer(body))
		if err != nil {
			logger.WithError(err).Error("failed to create pagerduty request")
			return
		}
		req.Header.Set("Content-Type", "application/json")

		client := &http.Client{Timeout: 10 * time.Second}
		resp, err := client.Do(req)
		if err != nil {
			logger.WithError(err).Error("failed to send pagerduty alert")
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusAccepted {
			var respBody bytes.Buffer
			respBody.ReadFrom(resp.Body)
			logger.WithFields(logrus.Fields{
				"status": resp.StatusCode,
				"body":   respBody.String(),
			}).Error("pagerduty API returned error")
			return
		}

		logger.WithFields(logrus.Fields{
			"dedup_key": dedupeKey,
			"summary":   msg,
		}).Info("pagerduty alert sent successfully")
	}
}

// GetRuleCount returns the number of registered rules
func (m *Manager) GetRuleCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.rules)
}

// ClearRules removes all registered rules
func (m *Manager) ClearRules() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rules = []Rule{}
}

// Close cleans up resources used by the manager
func (m *Manager) Close() error {
	if m.httpClient != nil {
		m.httpClient.CloseIdleConnections()
	}
	return nil
}
