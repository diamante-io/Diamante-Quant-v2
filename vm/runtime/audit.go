// Package runtime provides audit logging for runtime registry operations
package runtime

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"diamante/common"
	"diamante/consensus"

	"github.com/sirupsen/logrus"
)

// AuditAction represents the type of action being audited
type AuditAction string

const (
	// Registry operations
	AuditActionRegisterRuntime     AuditAction = "REGISTER_RUNTIME"
	AuditActionUnregisterRuntime   AuditAction = "UNREGISTER_RUNTIME"
	AuditActionRegisterMetadata    AuditAction = "REGISTER_METADATA"
	AuditActionRegisterValidation  AuditAction = "REGISTER_VALIDATION"
	AuditActionRegisterHealthCheck AuditAction = "REGISTER_HEALTH_CHECK"

	// Runtime operations
	AuditActionRuntimeInitialize AuditAction = "RUNTIME_INITIALIZE"
	AuditActionRuntimeStart      AuditAction = "RUNTIME_START"
	AuditActionRuntimeStop       AuditAction = "RUNTIME_STOP"
	AuditActionRuntimeExecute    AuditAction = "RUNTIME_EXECUTE"
	AuditActionRuntimeDeploy     AuditAction = "RUNTIME_DEPLOY"
	AuditActionRuntimeUpgrade    AuditAction = "RUNTIME_UPGRADE"

	// Query operations
	AuditActionQueryRuntime    AuditAction = "QUERY_RUNTIME"
	AuditActionQueryMetadata   AuditAction = "QUERY_METADATA"
	AuditActionQueryCapability AuditAction = "QUERY_CAPABILITY"
	AuditActionHealthCheck     AuditAction = "HEALTH_CHECK"
)

// AuditSeverity represents the severity level of an audit event
type AuditSeverity string

const (
	AuditSeverityInfo     AuditSeverity = "INFO"
	AuditSeverityWarning  AuditSeverity = "WARNING"
	AuditSeverityError    AuditSeverity = "ERROR"
	AuditSeverityCritical AuditSeverity = "CRITICAL"
)

// AuditEntry represents a single audit log entry
type AuditEntry struct {
	ID           string                 `json:"id"`
	Timestamp    time.Time              `json:"timestamp"`
	Action       AuditAction            `json:"action"`
	Severity     AuditSeverity          `json:"severity"`
	RuntimeType  RuntimeType            `json:"runtime_type,omitempty"`
	User         string                 `json:"user,omitempty"`
	Source       string                 `json:"source,omitempty"`
	Success      bool                   `json:"success"`
	ErrorMessage string                 `json:"error_message,omitempty"`
	Duration     time.Duration          `json:"duration,omitempty"`
	Details      map[string]interface{} `json:"details,omitempty"`
	StackTrace   string                 `json:"stack_trace,omitempty"`
}

// AuditLogger handles audit logging for runtime operations
type AuditLogger struct {
	mu           sync.RWMutex
	logger       *logrus.Logger          // Legacy logger for backward compatibility
	structLogger common.StructuredLogger // New structured logger
	fileLogger   *logrus.Logger
	logFile      *os.File
	buffer       []AuditEntry
	bufferSize   int
	flushSize    int
	flushTicker  *time.Ticker
	stopChan     chan struct{}
	filters      []AuditFilter
	retention    time.Duration
}

// AuditFilter defines a filter for audit entries
type AuditFilter func(entry AuditEntry) bool

// AuditLoggerConfig contains configuration for audit logger
type AuditLoggerConfig struct {
	LogDir        string
	LogFile       string
	BufferSize    int
	FlushSize     int
	FlushInterval time.Duration
	Retention     time.Duration
	Filters       []AuditFilter
}

// NewAuditLogger creates a new audit logger
func NewAuditLogger(config AuditLoggerConfig) (*AuditLogger, error) {
	// Set defaults
	if config.LogDir == "" {
		config.LogDir = "/var/log/diamante/audit"
	}
	if config.LogFile == "" {
		config.LogFile = fmt.Sprintf("runtime-audit-%s.log", consensus.ConsensusNow().Format("2006-01-02"))
	}
	if config.BufferSize == 0 {
		config.BufferSize = 1000
	}
	if config.FlushSize == 0 {
		config.FlushSize = 100
	}
	if config.FlushInterval == 0 {
		config.FlushInterval = 10 * time.Second
	}
	if config.Retention == 0 {
		config.Retention = 30 * 24 * time.Hour // 30 days
	}

	// Create structured logger
	structLogger := common.NewStructuredLogger("vm-runtime-audit")

	// Create log directory
	if err := os.MkdirAll(config.LogDir, 0755); err != nil {
		structLogger.Error("Failed to create audit log directory",
			common.StringField("logDir", config.LogDir),
			common.ErrorField(err))
		return nil, fmt.Errorf("failed to create audit log directory: %w", err)
	}

	// Open log file
	logPath := filepath.Join(config.LogDir, config.LogFile)
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		structLogger.Error("Failed to open audit log file",
			common.StringField("logPath", logPath),
			common.ErrorField(err))
		return nil, fmt.Errorf("failed to open audit log file: %w", err)
	}

	// Create file logger
	fileLogger := logrus.New()
	fileLogger.SetOutput(logFile)
	fileLogger.SetFormatter(&logrus.JSONFormatter{
		TimestampFormat: time.RFC3339Nano,
	})

	// Create console logger
	consoleLogger := logrus.New()
	consoleLogger.SetFormatter(&logrus.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: "2006-01-02 15:04:05.000",
	})

	al := &AuditLogger{
		logger:       consoleLogger,
		structLogger: structLogger,
		fileLogger:   fileLogger,
		logFile:      logFile,
		buffer:       make([]AuditEntry, 0, config.BufferSize),
		bufferSize:   config.BufferSize,
		flushSize:    config.FlushSize,
		flushTicker:  time.NewTicker(config.FlushInterval),
		stopChan:     make(chan struct{}),
		filters:      config.Filters,
		retention:    config.Retention,
	}

	// Start background flusher
	go al.backgroundFlusher()

	// Start retention cleaner
	go al.retentionCleaner(config.LogDir)

	structLogger.Info("Audit logger initialized",
		common.StringField("logDir", config.LogDir),
		common.StringField("logFile", config.LogFile),
		common.IntField("bufferSize", config.BufferSize),
		common.IntField("flushSize", config.FlushSize),
		common.DurationField("flushInterval", config.FlushInterval),
		common.DurationField("retention", config.Retention))

	return al, nil
}

// LogEntry logs an audit entry
func (al *AuditLogger) LogEntry(entry AuditEntry) {
	// Set ID if not provided
	if entry.ID == "" {
		entry.ID = generateUniqueID()
	}

	// Apply filters
	for _, filter := range al.filters {
		if !filter(entry) {
			return // Skip this entry
		}
	}

	// Add to buffer
	al.mu.Lock()
	al.buffer = append(al.buffer, entry)

	// Check if we need to flush
	if len(al.buffer) >= al.flushSize {
		entries := al.buffer
		al.buffer = make([]AuditEntry, 0, al.bufferSize)
		al.mu.Unlock()

		al.flush(entries)
	} else {
		al.mu.Unlock()
	}

	// Log to console based on severity
	al.logToConsole(entry)
}

// LogRegistration logs a runtime registration event
func (al *AuditLogger) LogRegistration(runtimeType RuntimeType, success bool, err error, duration time.Duration) {
	entry := AuditEntry{
		Timestamp:   consensus.ConsensusNow(),
		Action:      AuditActionRegisterRuntime,
		Severity:    AuditSeverityInfo,
		RuntimeType: runtimeType,
		Success:     success,
		Duration:    duration,
		Details: map[string]interface{}{
			"operation": "runtime_registration",
		},
	}

	if !success && err != nil {
		entry.Severity = AuditSeverityError
		entry.ErrorMessage = err.Error()
	}

	al.structLogger.Info("Runtime registration audit",
		common.StringField("runtimeType", string(runtimeType)),
		common.BoolField("success", success),
		common.DurationField("duration", duration),
		common.StringField("auditID", entry.ID))

	if err != nil {
		al.structLogger.Error("Runtime registration failed",
			common.StringField("runtimeType", string(runtimeType)),
			common.ErrorField(err))
	}

	al.LogEntry(entry)
}

// LogExecution logs a runtime execution event
func (al *AuditLogger) LogExecution(runtimeType RuntimeType, contractID string, function string, success bool, gasUsed uint64, duration time.Duration) {
	entry := AuditEntry{
		Timestamp:   consensus.ConsensusNow(),
		Action:      AuditActionRuntimeExecute,
		Severity:    AuditSeverityInfo,
		RuntimeType: runtimeType,
		Success:     success,
		Duration:    duration,
		Details: map[string]interface{}{
			"contract_id": contractID,
			"function":    function,
			"gas_used":    gasUsed,
		},
	}

	if !success {
		entry.Severity = AuditSeverityWarning
	}

	al.structLogger.Info("Runtime execution audit",
		common.StringField("runtimeType", string(runtimeType)),
		common.StringField("contractID", contractID),
		common.StringField("function", function),
		common.BoolField("success", success),
		common.GasUsedField(gasUsed),
		common.DurationField("duration", duration),
		common.StringField("auditID", entry.ID))

	if !success {
		al.structLogger.Warn("Runtime execution failed",
			common.StringField("runtimeType", string(runtimeType)),
			common.StringField("contractID", contractID),
			common.StringField("function", function))
	}

	al.LogEntry(entry)
}

// LogHealthCheck logs a health check event
func (al *AuditLogger) LogHealthCheck(runtimeType RuntimeType, success bool, err error, duration time.Duration) {
	entry := AuditEntry{
		Timestamp:   consensus.ConsensusNow(),
		Action:      AuditActionHealthCheck,
		Severity:    AuditSeverityInfo,
		RuntimeType: runtimeType,
		Success:     success,
		Duration:    duration,
	}

	if !success && err != nil {
		entry.Severity = AuditSeverityWarning
		entry.ErrorMessage = err.Error()
	}

	al.structLogger.Debug("Runtime health check audit",
		common.StringField("runtimeType", string(runtimeType)),
		common.BoolField("success", success),
		common.DurationField("duration", duration),
		common.StringField("auditID", entry.ID))

	if !success && err != nil {
		al.structLogger.Warn("Runtime health check failed",
			common.StringField("runtimeType", string(runtimeType)),
			common.ErrorField(err))
	}

	al.LogEntry(entry)
}

// LogSecurityEvent logs a security-related event
func (al *AuditLogger) LogSecurityEvent(action AuditAction, runtimeType RuntimeType, details map[string]interface{}) {
	entry := AuditEntry{
		Timestamp:   consensus.ConsensusNow(),
		Action:      action,
		Severity:    AuditSeverityCritical,
		RuntimeType: runtimeType,
		Success:     false,
		Details:     details,
	}

	al.structLogger.Error("Security event audit",
		common.StringField("action", string(action)),
		common.StringField("runtimeType", string(runtimeType)),
		common.StringField("auditID", entry.ID),
		common.SecurityAuditField("security_event", "critical"))

	// Log details as separate fields
	for key, value := range details {
		al.structLogger.Error("Security event detail",
			common.StringField("key", key),
			common.StringField("value", fmt.Sprintf("%v", value)))
	}

	al.LogEntry(entry)
}

// Query retrieves audit entries based on criteria
func (al *AuditLogger) Query(criteria AuditQueryCriteria) ([]AuditEntry, error) {
	al.structLogger.Debug("Querying audit entries",
		common.IntField("limit", criteria.Limit))

	var results []AuditEntry

	// First, check buffered entries
	al.mu.RLock()
	for _, entry := range al.buffer {
		if criteria.Matches(entry) {
			results = append(results, entry)
		}
	}
	al.mu.RUnlock()

	// Then query from persistent storage (log files)
	persistentResults, err := al.queryPersistentStorage(criteria)
	if err != nil {
		al.structLogger.Warn("Failed to query persistent storage, returning buffer results only",
			common.ErrorField(err))
		return results, nil
	}

	// Combine results
	results = append(results, persistentResults...)

	// Sort by timestamp (newest first)
	sort.Slice(results, func(i, j int) bool {
		return results[i].Timestamp.After(results[j].Timestamp)
	})

	// Apply limit if specified
	if criteria.Limit > 0 && len(results) > criteria.Limit {
		results = results[:criteria.Limit]
	}

	al.structLogger.Debug("Audit query completed",
		common.IntField("resultCount", len(results)))

	return results, nil
}

// queryPersistentStorage queries audit entries from log files
func (al *AuditLogger) queryPersistentStorage(criteria AuditQueryCriteria) ([]AuditEntry, error) {
	var results []AuditEntry

	// Get log directory
	logDir := filepath.Dir(al.logFile.Name())

	// Read all audit log files
	files, err := os.ReadDir(logDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read log directory: %w", err)
	}

	// Process each log file
	for _, file := range files {
		if file.IsDir() || !strings.HasPrefix(file.Name(), "runtime-audit-") {
			continue
		}

		// Check if file is within time range
		info, err := file.Info()
		if err != nil {
			continue
		}

		// Skip files that are definitely outside the time range
		if criteria.EndTime != nil {
			adjustedTime := criteria.EndTime.Add(-24 * time.Hour)
			if info.ModTime().Before(adjustedTime) {
				continue
			}
		}

		// Read and parse log file
		filePath := filepath.Join(logDir, file.Name())
		fileResults, err := al.parseLogFile(filePath, criteria)
		if err != nil {
			al.structLogger.Warn("Failed to parse log file",
				common.ErrorField(err),
				common.StringField("file", filePath))
			continue
		}

		results = append(results, fileResults...)
	}

	return results, nil
}

// parseLogFile parses a single audit log file
func (al *AuditLogger) parseLogFile(filePath string, criteria AuditQueryCriteria) ([]AuditEntry, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var results []AuditEntry
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Text()

		// Extract JSON from log line
		// Log format: time="..." level=info msg="AUDIT" audit="{...}"
		auditIndex := strings.Index(line, `audit="`)
		if auditIndex == -1 {
			continue
		}

		// Find the end of the JSON
		jsonStart := auditIndex + 7
		jsonEnd := strings.LastIndex(line, `"`)
		if jsonEnd <= jsonStart {
			continue
		}

		jsonData := line[jsonStart:jsonEnd]
		// Unescape the JSON
		jsonData = strings.ReplaceAll(jsonData, `\"`, `"`)

		// Parse the audit entry
		var entry AuditEntry
		if err := json.Unmarshal([]byte(jsonData), &entry); err != nil {
			continue
		}

		// Check if entry matches criteria
		if criteria.Matches(entry) {
			results = append(results, entry)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading file: %w", err)
	}

	return results, nil
}

// Close closes the audit logger
func (al *AuditLogger) Close() error {
	al.structLogger.Info("Closing audit logger")

	// Stop background workers
	close(al.stopChan)
	al.flushTicker.Stop()

	// Flush remaining entries
	al.mu.Lock()
	entries := al.buffer
	al.buffer = nil
	al.mu.Unlock()

	if len(entries) > 0 {
		al.structLogger.Info("Flushing remaining audit entries",
			common.IntField("entryCount", len(entries)))
		al.flush(entries)
	}

	// Close log file
	err := al.logFile.Close()
	if err != nil {
		al.structLogger.Error("Failed to close audit log file",
			common.ErrorField(err))
	} else {
		al.structLogger.Info("Audit logger closed successfully")
	}

	return err
}

// backgroundFlusher periodically flushes the buffer
func (al *AuditLogger) backgroundFlusher() {
	for {
		select {
		case <-al.flushTicker.C:
			al.mu.Lock()
			if len(al.buffer) > 0 {
				entries := al.buffer
				al.buffer = make([]AuditEntry, 0, al.bufferSize)
				al.mu.Unlock()

				al.structLogger.Debug("Flushing audit buffer",
					common.IntField("entryCount", len(entries)))
				al.flush(entries)
			} else {
				al.mu.Unlock()
			}

		case <-al.stopChan:
			al.structLogger.Debug("Stopping audit background flusher")
			return
		}
	}
}

// flush writes entries to persistent storage
func (al *AuditLogger) flush(entries []AuditEntry) {
	var flushErrors []error

	for _, entry := range entries {
		// Write to file
		data, err := json.Marshal(entry)
		if err != nil {
			al.structLogger.Error("Failed to marshal audit entry",
				common.StringField("entryID", entry.ID),
				common.ErrorField(err))
			flushErrors = append(flushErrors, fmt.Errorf("failed to marshal entry %s: %w", entry.ID, err))
			continue
		}

		al.fileLogger.WithField("audit", string(data)).Info("AUDIT")
	}

	// Sync file
	if err := al.logFile.Sync(); err != nil {
		al.structLogger.Error("Failed to sync audit log file",
			common.ErrorField(err))
		flushErrors = append(flushErrors, fmt.Errorf("failed to sync log file: %w", err))
	}

	// Log summary of flush errors if any occurred
	if len(flushErrors) > 0 {
		al.structLogger.Error("Audit log flush completed with errors",
			common.IntField("errorCount", len(flushErrors)),
			common.IntField("totalEntries", len(entries)))
	} else {
		al.structLogger.Debug("Audit log flush completed successfully",
			common.IntField("totalEntries", len(entries)))
	}
}

// retentionCleaner periodically cleans old audit logs
func (al *AuditLogger) retentionCleaner(logDir string) {
	ticker := time.NewTicker(24 * time.Hour) // Clean daily
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			al.structLogger.Debug("Starting audit log retention cleanup")
			al.cleanOldLogs(logDir)
		case <-al.stopChan:
			al.structLogger.Debug("Stopping audit retention cleaner")
			return
		}
	}
}

// cleanOldLogs removes old audit log files
func (al *AuditLogger) cleanOldLogs(logDir string) {
	cutoff := consensus.ConsensusNow().Add(-al.retention)

	al.structLogger.Debug("Cleaning old audit logs",
		common.StringField("logDir", logDir),
		common.TimeField("cutoff", cutoff))

	files, err := os.ReadDir(logDir)
	if err != nil {
		al.structLogger.Error("Failed to read audit log directory",
			common.StringField("logDir", logDir),
			common.ErrorField(err))
		return
	}

	removedCount := 0
	for _, file := range files {
		if file.IsDir() || !strings.HasPrefix(file.Name(), "runtime-audit-") {
			continue
		}

		info, err := file.Info()
		if err != nil {
			continue
		}

		if info.ModTime().Before(cutoff) {
			filePath := filepath.Join(logDir, file.Name())
			if err := os.Remove(filePath); err != nil {
				al.structLogger.Warn("Failed to remove old audit log file",
					common.StringField("filePath", filePath),
					common.ErrorField(err))
			} else {
				removedCount++
				al.structLogger.Debug("Removed old audit log file",
					common.StringField("filePath", filePath))
			}
		}
	}

	al.structLogger.Info("Audit log cleanup completed",
		common.IntField("removedCount", removedCount))
}

// logToConsole logs entry to console based on severity
func (al *AuditLogger) logToConsole(entry AuditEntry) {
	// Ensure logger is available
	if al.logger == nil {
		return
	}

	fields := logrus.Fields{
		"action":       entry.Action,
		"runtime_type": entry.RuntimeType,
		"success":      entry.Success,
	}

	if entry.Duration > 0 {
		fields["duration_ms"] = entry.Duration.Milliseconds()
	}

	switch entry.Severity {
	case AuditSeverityError:
		if entry.ErrorMessage != "" {
			al.logger.WithFields(fields).Error(entry.ErrorMessage)
		} else {
			al.logger.WithFields(fields).Error("Unknown error")
		}
	case AuditSeverityCritical:
		if entry.ErrorMessage != "" {
			al.logger.WithFields(fields).Error(entry.ErrorMessage) // Changed from Fatal to Error to avoid process termination
		} else {
			al.logger.WithFields(fields).Error("Critical error")
		}
	case AuditSeverityWarning:
		al.logger.WithFields(fields).Warn(string(entry.Action))
	default:
		al.logger.WithFields(fields).Info(string(entry.Action))
	}
}

// AuditQueryCriteria defines criteria for querying audit logs
type AuditQueryCriteria struct {
	StartTime   *time.Time
	EndTime     *time.Time
	Actions     []AuditAction
	RuntimeType *RuntimeType
	Severity    *AuditSeverity
	Success     *bool
	Limit       int
}

// Matches checks if an entry matches the criteria
func (c AuditQueryCriteria) Matches(entry AuditEntry) bool {
	if c.StartTime != nil && entry.Timestamp.Before(*c.StartTime) {
		return false
	}
	if c.EndTime != nil && entry.Timestamp.After(*c.EndTime) {
		return false
	}
	if len(c.Actions) > 0 {
		found := false
		for _, action := range c.Actions {
			if entry.Action == action {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	if c.RuntimeType != nil && entry.RuntimeType != *c.RuntimeType {
		return false
	}
	if c.Severity != nil && entry.Severity != *c.Severity {
		return false
	}
	if c.Success != nil && entry.Success != *c.Success {
		return false
	}
	return true
}

// Helper functions

func generateUniqueID() string {
	return fmt.Sprintf("audit-%d-%d", consensus.ConsensusUnixNano(), os.Getpid())
}

// Common audit filters

// NewSeverityFilter creates a filter based on minimum severity
func NewSeverityFilter(minSeverity AuditSeverity) AuditFilter {
	severityOrder := map[AuditSeverity]int{
		AuditSeverityInfo:     0,
		AuditSeverityWarning:  1,
		AuditSeverityError:    2,
		AuditSeverityCritical: 3,
	}

	return func(entry AuditEntry) bool {
		return severityOrder[entry.Severity] >= severityOrder[minSeverity]
	}
}

// NewActionFilter creates a filter for specific actions
func NewActionFilter(actions ...AuditAction) AuditFilter {
	actionMap := make(map[AuditAction]bool)
	for _, action := range actions {
		actionMap[action] = true
	}

	return func(entry AuditEntry) bool {
		return actionMap[entry.Action]
	}
}

// NewRuntimeFilter creates a filter for specific runtime types
func NewRuntimeFilter(runtimeTypes ...RuntimeType) AuditFilter {
	rtMap := make(map[RuntimeType]bool)
	for _, rt := range runtimeTypes {
		rtMap[rt] = true
	}

	return func(entry AuditEntry) bool {
		return rtMap[entry.RuntimeType]
	}
}
