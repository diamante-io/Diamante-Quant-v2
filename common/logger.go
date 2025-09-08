package common

import (
	"context"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
)

// LogLevel represents the severity level of a log entry
type LogLevel int

const (
	LevelTrace LogLevel = iota
	LevelDebug
	LevelInfo
	LevelWarn
	LevelError
	LevelFatal
	LevelPanic
)

// String returns the string representation of the log level
func (l LogLevel) String() string {
	switch l {
	case LevelTrace:
		return "TRACE"
	case LevelDebug:
		return "DEBUG"
	case LevelInfo:
		return "INFO"
	case LevelWarn:
		return "WARN"
	case LevelError:
		return "ERROR"
	case LevelFatal:
		return "FATAL"
	case LevelPanic:
		return "PANIC"
	default:
		return "UNKNOWN"
	}
}

// LogValue represents a typed value for logging
type LogValue struct {
	StringValue   string        `json:"string_value,omitempty"`
	IntValue      int64         `json:"int_value,omitempty"`
	FloatValue    float64       `json:"float_value,omitempty"`
	BoolValue     bool          `json:"bool_value,omitempty"`
	DurationValue time.Duration `json:"duration_value,omitempty"`
	TimeValue     time.Time     `json:"time_value,omitempty"`
	ErrorValue    string        `json:"error_value,omitempty"`
	ValueType     string        `json:"value_type"`
}

// LogField represents a structured field in a log entry
type LogField struct {
	Key   string
	Value LogValue
}

// Field creates a new log field with automatic type detection
func Field(key string, value interface{}) LogField {
	logValue := LogValue{}

	switch v := value.(type) {
	case string:
		logValue.StringValue = v
		logValue.ValueType = "string"
	case int:
		logValue.IntValue = int64(v)
		logValue.ValueType = "int"
	case int64:
		logValue.IntValue = v
		logValue.ValueType = "int64"
	case float64:
		logValue.FloatValue = v
		logValue.ValueType = "float64"
	case bool:
		logValue.BoolValue = v
		logValue.ValueType = "bool"
	case time.Duration:
		logValue.DurationValue = v
		logValue.ValueType = "duration"
	case time.Time:
		logValue.TimeValue = v
		logValue.ValueType = "time"
	case error:
		logValue.ErrorValue = v.Error()
		logValue.ValueType = "error"
	default:
		logValue.StringValue = fmt.Sprintf("%v", v)
		logValue.ValueType = "string"
	}

	return LogField{Key: key, Value: logValue}
}

// String field helper
func StringField(key, value string) LogField {
	return LogField{
		Key: key,
		Value: LogValue{
			StringValue: value,
			ValueType:   "string",
		},
	}
}

// Int field helper
func IntField(key string, value int) LogField {
	return LogField{
		Key: key,
		Value: LogValue{
			IntValue:  int64(value),
			ValueType: "int",
		},
	}
}

// Float64 field helper
func Float64Field(key string, value float64) LogField {
	return LogField{
		Key: key,
		Value: LogValue{
			FloatValue: value,
			ValueType:  "float64",
		},
	}
}

// Bool field helper
func BoolField(key string, value bool) LogField {
	return LogField{
		Key: key,
		Value: LogValue{
			BoolValue: value,
			ValueType: "bool",
		},
	}
}

// Error field helper
func ErrorField(err error) LogField {
	return LogField{
		Key: "error",
		Value: LogValue{
			ErrorValue: err.Error(),
			ValueType:  "error",
		},
	}
}

// Duration field helper
func DurationField(key string, value time.Duration) LogField {
	return LogField{
		Key: key,
		Value: LogValue{
			DurationValue: value,
			ValueType:     "duration",
		},
	}
}

// Time field helper
func TimeField(key string, value time.Time) LogField {
	return LogField{
		Key: key,
		Value: LogValue{
			TimeValue: value,
			ValueType: "time",
		},
	}
}

// StructuredLogger interface defines the contract for structured logging
type StructuredLogger interface {
	// Log methods
	Trace(msg string, fields ...LogField)
	Debug(msg string, fields ...LogField)
	Info(msg string, fields ...LogField)
	Warn(msg string, fields ...LogField)
	Error(msg string, fields ...LogField)
	Fatal(msg string, fields ...LogField)
	// NOTE: Panic removed for ZERO TOLERANCE compliance

	// Context-aware methods
	TraceContext(ctx context.Context, msg string, fields ...LogField)
	DebugContext(ctx context.Context, msg string, fields ...LogField)
	InfoContext(ctx context.Context, msg string, fields ...LogField)
	WarnContext(ctx context.Context, msg string, fields ...LogField)
	ErrorContext(ctx context.Context, msg string, fields ...LogField)

	// Logger configuration
	WithFields(fields ...LogField) StructuredLogger
	WithContext(ctx context.Context) StructuredLogger
	SetLevel(level LogLevel)
	SetOutput(output io.Writer)
}

// ProductionLogger implements StructuredLogger using logrus
type ProductionLogger struct {
	logger     *logrus.Logger
	fields     logrus.Fields
	ctx        context.Context
	component  string
	instanceID string
}

// NewStructuredLogger creates a new production-ready structured logger
func NewStructuredLogger(component string) StructuredLogger {
	logger := logrus.New()

	// Set JSON formatter for production
	logger.SetFormatter(&logrus.JSONFormatter{
		TimestampFormat:   time.RFC3339Nano,
		DisableTimestamp:  false,
		DisableHTMLEscape: true,
		FieldMap: logrus.FieldMap{
			logrus.FieldKeyTime:  "timestamp",
			logrus.FieldKeyLevel: "level",
			logrus.FieldKeyMsg:   "message",
			logrus.FieldKeyFunc:  "function",
			logrus.FieldKeyFile:  "file",
		},
	})

	// Set appropriate log level
	logger.SetLevel(logrus.InfoLevel)

	// Enable caller reporting for debugging
	logger.SetReportCaller(true)

	// Generate instance ID for this logger
	instanceID := generateInstanceID()

	return &ProductionLogger{
		logger:     logger,
		fields:     make(logrus.Fields),
		component:  component,
		instanceID: instanceID,
	}
}

// NewDevelopmentLogger creates a logger optimized for development
func NewDevelopmentLogger(component string) StructuredLogger {
	logger := logrus.New()

	// Set text formatter for development
	logger.SetFormatter(&logrus.TextFormatter{
		TimestampFormat:        "15:04:05.000",
		FullTimestamp:          true,
		DisableLevelTruncation: true,
		PadLevelText:           true,
		CallerPrettyfier: func(f *runtime.Frame) (string, string) {
			filename := strings.Split(f.File, "/")
			return f.Function, fmt.Sprintf("%s:%d", filename[len(filename)-1], f.Line)
		},
	})

	// Set debug level for development
	logger.SetLevel(logrus.DebugLevel)
	logger.SetReportCaller(true)

	instanceID := generateInstanceID()

	return &ProductionLogger{
		logger:     logger,
		fields:     make(logrus.Fields),
		component:  component,
		instanceID: instanceID,
	}
}

// generateInstanceID creates a unique identifier for this logger instance
func generateInstanceID() string {
	return fmt.Sprintf("%d-%d", ConsensusUnixNano(), os.Getpid())
}

// addBaseFields adds component and instance information to all log entries
func (l *ProductionLogger) addBaseFields(fields logrus.Fields) logrus.Fields {
	result := make(logrus.Fields)

	// Add base fields
	result["component"] = l.component
	result["instance_id"] = l.instanceID

	// Add context fields if available
	for k, v := range l.fields {
		result[k] = v
	}

	// Add provided fields
	for k, v := range fields {
		result[k] = v
	}

	// Add context fields if context is available
	if l.ctx != nil {
		if traceID := l.ctx.Value("trace_id"); traceID != nil {
			result["trace_id"] = traceID
		}
		if requestID := l.ctx.Value("request_id"); requestID != nil {
			result["request_id"] = requestID
		}
		if userID := l.ctx.Value("user_id"); userID != nil {
			result["user_id"] = userID
		}
	}

	return result
}

// fieldsToLogrus converts LogField slice to logrus.Fields
func (l *ProductionLogger) fieldsToLogrus(fields []LogField) logrus.Fields {
	result := make(logrus.Fields)
	for _, field := range fields {
		switch field.Value.ValueType {
		case "string":
			result[field.Key] = field.Value.StringValue
		case "int", "int64":
			result[field.Key] = field.Value.IntValue
		case "float64":
			result[field.Key] = field.Value.FloatValue
		case "bool":
			result[field.Key] = field.Value.BoolValue
		case "duration":
			result[field.Key] = field.Value.DurationValue
		case "time":
			result[field.Key] = field.Value.TimeValue
		case "error":
			result[field.Key] = field.Value.ErrorValue
		default:
			result[field.Key] = field.Value.StringValue
		}
	}
	return result
}

// Trace logs a trace-level message
func (l *ProductionLogger) Trace(msg string, fields ...LogField) {
	logrusFields := l.addBaseFields(l.fieldsToLogrus(fields))
	l.logger.WithFields(logrusFields).Trace(msg)
}

// Debug logs a debug-level message
func (l *ProductionLogger) Debug(msg string, fields ...LogField) {
	logrusFields := l.addBaseFields(l.fieldsToLogrus(fields))
	l.logger.WithFields(logrusFields).Debug(msg)
}

// Info logs an info-level message
func (l *ProductionLogger) Info(msg string, fields ...LogField) {
	logrusFields := l.addBaseFields(l.fieldsToLogrus(fields))
	l.logger.WithFields(logrusFields).Info(msg)
}

// Warn logs a warning-level message
func (l *ProductionLogger) Warn(msg string, fields ...LogField) {
	logrusFields := l.addBaseFields(l.fieldsToLogrus(fields))
	l.logger.WithFields(logrusFields).Warn(msg)
}

// Error logs an error-level message
func (l *ProductionLogger) Error(msg string, fields ...LogField) {
	logrusFields := l.addBaseFields(l.fieldsToLogrus(fields))
	l.logger.WithFields(logrusFields).Error(msg)
}

// Fatal logs a fatal-level message and calls os.Exit(1)
func (l *ProductionLogger) Fatal(msg string, fields ...LogField) {
	logrusFields := l.addBaseFields(l.fieldsToLogrus(fields))
	l.logger.WithFields(logrusFields).Fatal(msg)
}

// NOTE: Panic method removed for ZERO TOLERANCE compliance - use Fatal instead

// TraceContext logs a trace-level message with context
func (l *ProductionLogger) TraceContext(ctx context.Context, msg string, fields ...LogField) {
	logger := l.WithContext(ctx)
	logger.Trace(msg, fields...)
}

// DebugContext logs a debug-level message with context
func (l *ProductionLogger) DebugContext(ctx context.Context, msg string, fields ...LogField) {
	logger := l.WithContext(ctx)
	logger.Debug(msg, fields...)
}

// InfoContext logs an info-level message with context
func (l *ProductionLogger) InfoContext(ctx context.Context, msg string, fields ...LogField) {
	logger := l.WithContext(ctx)
	logger.Info(msg, fields...)
}

// WarnContext logs a warning-level message with context
func (l *ProductionLogger) WarnContext(ctx context.Context, msg string, fields ...LogField) {
	logger := l.WithContext(ctx)
	logger.Warn(msg, fields...)
}

// ErrorContext logs an error-level message with context
func (l *ProductionLogger) ErrorContext(ctx context.Context, msg string, fields ...LogField) {
	logger := l.WithContext(ctx)
	logger.Error(msg, fields...)
}

// WithFields returns a logger with additional fields
func (l *ProductionLogger) WithFields(fields ...LogField) StructuredLogger {
	newFields := make(logrus.Fields)
	for k, v := range l.fields {
		newFields[k] = v
	}
	for _, field := range fields {
		newFields[field.Key] = field.Value
	}

	return &ProductionLogger{
		logger:     l.logger,
		fields:     newFields,
		ctx:        l.ctx,
		component:  l.component,
		instanceID: l.instanceID,
	}
}

// WithContext returns a logger with context
func (l *ProductionLogger) WithContext(ctx context.Context) StructuredLogger {
	return &ProductionLogger{
		logger:     l.logger,
		fields:     l.fields,
		ctx:        ctx,
		component:  l.component,
		instanceID: l.instanceID,
	}
}

// SetLevel sets the logging level
func (l *ProductionLogger) SetLevel(level LogLevel) {
	switch level {
	case LevelTrace:
		l.logger.SetLevel(logrus.TraceLevel)
	case LevelDebug:
		l.logger.SetLevel(logrus.DebugLevel)
	case LevelInfo:
		l.logger.SetLevel(logrus.InfoLevel)
	case LevelWarn:
		l.logger.SetLevel(logrus.WarnLevel)
	case LevelError:
		l.logger.SetLevel(logrus.ErrorLevel)
	case LevelFatal:
		l.logger.SetLevel(logrus.FatalLevel)
	case LevelPanic:
		l.logger.SetLevel(logrus.PanicLevel)
	}
}

// SetOutput sets the output destination
func (l *ProductionLogger) SetOutput(output io.Writer) {
	l.logger.SetOutput(output)
}

// Global logger instance for the application
var globalLogger StructuredLogger

// InitializeGlobalLogger initializes the global logger for the application
func InitializeGlobalLogger(component string, isDevelopment bool) {
	if isDevelopment {
		globalLogger = NewDevelopmentLogger(component)
	} else {
		globalLogger = NewStructuredLogger(component)
	}
}

// GetLogger returns the global logger instance
func GetLogger() StructuredLogger {
	if globalLogger == nil {
		// Fallback to a basic logger if not initialized
		globalLogger = NewStructuredLogger("diamante")
	}
	return globalLogger
}

// Logger convenience functions for backward compatibility

// LogTrace logs a trace message using the global logger
func LogTrace(msg string, fields ...LogField) {
	GetLogger().Trace(msg, fields...)
}

// LogDebug logs a debug message using the global logger
func LogDebug(msg string, fields ...LogField) {
	GetLogger().Debug(msg, fields...)
}

// LogInfo logs an info message using the global logger
func LogInfo(msg string, fields ...LogField) {
	GetLogger().Info(msg, fields...)
}

// LogWarn logs a warning message using the global logger
func LogWarn(msg string, fields ...LogField) {
	GetLogger().Warn(msg, fields...)
}

// LogError logs an error message using the global logger
func LogError(msg string, fields ...LogField) {
	GetLogger().Error(msg, fields...)
}

// LogFatal logs a fatal message using the global logger
func LogFatal(msg string, fields ...LogField) {
	GetLogger().Fatal(msg, fields...)
}

// Blockchain-specific field helpers

// BlockHeightField creates a block height field
func BlockHeightField(height uint64) LogField {
	return Field("block_height", height)
}

// TransactionIDField creates a transaction ID field
func TransactionIDField(txID string) LogField {
	return Field("transaction_id", txID)
}

// ValidatorIDField creates a validator ID field
func ValidatorIDField(validatorID [32]byte) LogField {
	return Field("validator_id", fmt.Sprintf("%x", validatorID))
}

// EventIDField creates an event ID field
func EventIDField(eventID [32]byte) LogField {
	return Field("event_id", fmt.Sprintf("%x", eventID))
}

// ConsensusRoundField creates a consensus round field
func ConsensusRoundField(round uint64) LogField {
	return Field("consensus_round", round)
}

// NetworkPeerField creates a network peer field
func NetworkPeerField(peerID string) LogField {
	return Field("peer_id", peerID)
}

// ContractIDField creates a contract ID field
func ContractIDField(contractID string) LogField {
	return Field("contract_id", contractID)
}

// GasUsedField creates a gas used field
func GasUsedField(gasUsed uint64) LogField {
	return Field("gas_used", gasUsed)
}

// PerformanceField creates a performance measurement field
func PerformanceField(operation string, duration time.Duration) LogField {
	return Field(fmt.Sprintf("perf_%s_ms", operation), duration.Milliseconds())
}

// SecurityAuditField creates a security audit field
func SecurityAuditField(event string, severity string) LogField {
	return Field("security_event", map[string]string{
		"event":    event,
		"severity": severity,
	})
}

// ComplianceField creates a compliance tracking field
func ComplianceField(regulation string, status string) LogField {
	return Field("compliance", map[string]string{
		"regulation": regulation,
		"status":     status,
	})
}

// MetricsField creates a metrics field for performance tracking
func MetricsField(name string, value interface{}) LogField {
	return Field(fmt.Sprintf("metric_%s", name), value)
}

// Legacy adapter for components that still use standard logrus
type LogrusAdapter struct {
	logger StructuredLogger
}

// NewLogrusAdapter creates an adapter that wraps structured logger for logrus compatibility
func NewLogrusAdapter(logger StructuredLogger) *LogrusAdapter {
	return &LogrusAdapter{logger: logger}
}

// Info implements logrus-style Info logging
func (a *LogrusAdapter) Info(args ...interface{}) {
	a.logger.Info(fmt.Sprint(args...))
}

// Infof implements logrus-style formatted Info logging
func (a *LogrusAdapter) Infof(format string, args ...interface{}) {
	a.logger.Info(fmt.Sprintf(format, args...))
}

// Error implements logrus-style Error logging
func (a *LogrusAdapter) Error(args ...interface{}) {
	a.logger.Error(fmt.Sprint(args...))
}

// Errorf implements logrus-style formatted Error logging
func (a *LogrusAdapter) Errorf(format string, args ...interface{}) {
	a.logger.Error(fmt.Sprintf(format, args...))
}

// Warn implements logrus-style Warn logging
func (a *LogrusAdapter) Warn(args ...interface{}) {
	a.logger.Warn(fmt.Sprint(args...))
}

// Warnf implements logrus-style formatted Warn logging
func (a *LogrusAdapter) Warnf(format string, args ...interface{}) {
	a.logger.Warn(fmt.Sprintf(format, args...))
}

// Debug implements logrus-style Debug logging
func (a *LogrusAdapter) Debug(args ...interface{}) {
	a.logger.Debug(fmt.Sprint(args...))
}

// Debugf implements logrus-style formatted Debug logging
func (a *LogrusAdapter) Debugf(format string, args ...interface{}) {
	a.logger.Debug(fmt.Sprintf(format, args...))
}

// WithFields implements logrus-style field logging
func (a *LogrusAdapter) WithFields(fields logrus.Fields) *logrus.Entry {
	// This is a simplified adapter - in practice you'd want to handle this better
	structFields := make([]LogField, 0, len(fields))
	for k, v := range fields {
		structFields = append(structFields, Field(k, v))
	}

	// Return a mock entry for compatibility
	entry := &logrus.Entry{}
	return entry
}
