// Package common provides shared utilities for cmd services
package common

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"diamante/consensus"
)

// Logger interface for structured logging
type Logger interface {
	Info(msg string, args ...interface{})
	Error(msg string, args ...interface{})
	Warn(msg string, args ...interface{})
	Debug(msg string, args ...interface{})
}

// StructuredLogger implements the Logger interface with structured logging
type StructuredLogger struct {
	logger *log.Logger
	level  LogLevel
	format LogFormat
}

// LogLevel represents the logging level
type LogLevel int

const (
	// LogLevelDebug debug level
	LogLevelDebug LogLevel = iota
	// LogLevelInfo info level
	LogLevelInfo
	// LogLevelWarn warn level
	LogLevelWarn
	// LogLevelError error level
	LogLevelError
)

// LogFormat represents the logging format
type LogFormat int

const (
	// LogFormatJSON JSON format
	LogFormatJSON LogFormat = iota
	// LogFormatText plain text format
	LogFormatText
)

// LogEntry represents a structured log entry
type LogEntry struct {
	Timestamp time.Time              `json:"timestamp"`
	Level     string                 `json:"level"`
	Message   string                 `json:"message"`
	Fields    map[string]interface{} `json:"fields,omitempty"`
}

// NewStructuredLogger creates a new structured logger
func NewStructuredLogger(level string, format string) (Logger, error) {
	logLevel, err := parseLogLevel(level)
	if err != nil {
		return nil, fmt.Errorf("invalid log level: %w", err)
	}

	logFormat, err := parseLogFormat(format)
	if err != nil {
		return nil, fmt.Errorf("invalid log format: %w", err)
	}

	return &StructuredLogger{
		logger: log.New(os.Stdout, "", 0),
		level:  logLevel,
		format: logFormat,
	}, nil
}

// Info logs an info message
func (sl *StructuredLogger) Info(msg string, args ...interface{}) {
	if sl.level <= LogLevelInfo {
		sl.log("INFO", msg, args...)
	}
}

// Error logs an error message
func (sl *StructuredLogger) Error(msg string, args ...interface{}) {
	if sl.level <= LogLevelError {
		sl.log("ERROR", msg, args...)
	}
}

// Warn logs a warning message
func (sl *StructuredLogger) Warn(msg string, args ...interface{}) {
	if sl.level <= LogLevelWarn {
		sl.log("WARN", msg, args...)
	}
}

// Debug logs a debug message
func (sl *StructuredLogger) Debug(msg string, args ...interface{}) {
	if sl.level <= LogLevelDebug {
		sl.log("DEBUG", msg, args...)
	}
}

// log formats and writes the log entry
func (sl *StructuredLogger) log(level, msg string, args ...interface{}) {
	entry := LogEntry{
		Level:     level,
		Message:   msg,
		Timestamp: consensus.ConsensusNow().UTC(),
		Fields:    make(map[string]interface{}),
	}

	// Parse args as key-value pairs
	for i := 0; i < len(args)-1; i += 2 {
		if key, ok := args[i].(string); ok {
			entry.Fields[key] = args[i+1]
		}
	}

	logData, _ := json.Marshal(entry)
	fmt.Println(string(logData))
}

// parseLogLevel parses log level from string
func parseLogLevel(level string) (LogLevel, error) {
	switch level {
	case "debug":
		return LogLevelDebug, nil
	case "info":
		return LogLevelInfo, nil
	case "warn", "warning":
		return LogLevelWarn, nil
	case "error":
		return LogLevelError, nil
	default:
		return LogLevelInfo, fmt.Errorf("unknown log level: %s", level)
	}
}

// parseLogFormat parses log format from string
func parseLogFormat(format string) (LogFormat, error) {
	switch format {
	case "json":
		return LogFormatJSON, nil
	case "text":
		return LogFormatText, nil
	default:
		return LogFormatJSON, fmt.Errorf("unknown log format: %s", format)
	}
}

func logError(msg string, err error) {
	fmt.Printf("[ERROR] %s %s: %v\n",
		consensus.ConsensusNow().UTC().Format(time.RFC3339), msg, err)
}
