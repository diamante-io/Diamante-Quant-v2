package main

import (
	"github.com/sirupsen/logrus"
)

// LoggerAdapter adapts logrus.Logger to the Logger interfaces required by various packages
type LoggerAdapter struct {
	logger *logrus.Logger
}

// NewLoggerAdapter creates a new LoggerAdapter
func NewLoggerAdapter(logger *logrus.Logger) *LoggerAdapter {
	return &LoggerAdapter{logger: logger}
}

// Info implements the Info method required by the Logger interfaces
func (la *LoggerAdapter) Info(msg string, keyvals ...interface{}) {
	la.logger.WithFields(convertToFields(keyvals)).Info(msg)
}

// Error implements the Error method required by the Logger interfaces
func (la *LoggerAdapter) Error(msg string, keyvals ...interface{}) {
	la.logger.WithFields(convertToFields(keyvals)).Error(msg)
}

// convertToFields converts a list of key-value pairs to logrus.Fields
func convertToFields(keyvals []interface{}) logrus.Fields {
	fields := logrus.Fields{}

	// Process key-value pairs
	for i := 0; i < len(keyvals); i += 2 {
		if i+1 < len(keyvals) {
			key, ok := keyvals[i].(string)
			if ok {
				fields[key] = keyvals[i+1]
			}
		}
	}

	return fields
}
