package security

import (
	"context"
	"diamante/common"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"
)

// ThreatDetectorImpl implements comprehensive threat detection
type ThreatDetectorImpl struct {
	mu                sync.RWMutex
	detectedThreats   []Threat
	patterns          []Pattern
	threatLevel       ThreatLevel
	detectorID        string
	anomalyThreshold  float64
	behaviorBaselines map[string]BehaviorBaseline
}

// BehaviorBaseline represents normal behavior patterns
type BehaviorBaseline struct {
	UserID             string
	AverageRequestRate float64
	CommonIPAddresses  []string
	CommonResources    []string
	CommonActions      []string
	LastUpdated        time.Time
	AnomalyScore       float64
}

// NewThreatDetector creates a new threat detector instance
func NewThreatDetector() ThreatDetector {
	return &ThreatDetectorImpl{
		detectorID:        fmt.Sprintf("threat-detector-%d", common.ConsensusNow().Unix()),
		detectedThreats:   make([]Threat, 0),
		patterns:          make([]Pattern, 0),
		threatLevel:       ThreatLevelNone,
		anomalyThreshold:  0.7, // 70% deviation from baseline
		behaviorBaselines: make(map[string]BehaviorBaseline),
	}
}

// DetectThreats analyzes data for potential security threats
func (td *ThreatDetectorImpl) DetectThreats(ctx context.Context, data []byte) ([]Threat, error) {
	if ctx.Err() != nil {
		return nil, fmt.Errorf("context cancelled: %w", ctx.Err())
	}

	if len(data) == 0 {
		return nil, fmt.Errorf("no data provided for threat detection")
	}

	threats := make([]Threat, 0)

	// Perform various threat detection analyses
	// 1. Pattern matching for known threats
	if patternThreats := td.detectPatternThreats(data); len(patternThreats) > 0 {
		threats = append(threats, patternThreats...)
	}

	// 2. Anomaly detection
	if anomalyThreats := td.detectAnomalies(data); len(anomalyThreats) > 0 {
		threats = append(threats, anomalyThreats...)
	}

	// 3. Behavioral analysis
	if behaviorThreats := td.detectBehavioralThreats(data); len(behaviorThreats) > 0 {
		threats = append(threats, behaviorThreats...)
	}

	// 4. Signature-based detection
	if signatureThreats := td.detectSignatureThreats(data); len(signatureThreats) > 0 {
		threats = append(threats, signatureThreats...)
	}

	// Update threat level based on findings
	td.updateThreatLevel(threats)

	// Store detected threats
	td.mu.Lock()
	td.detectedThreats = append(td.detectedThreats, threats...)
	td.mu.Unlock()

	return threats, nil
}

// AnalyzePattern analyzes patterns for anomalies
func (td *ThreatDetectorImpl) AnalyzePattern(pattern Pattern) (*ThreatAnalysis, error) {
	if pattern.ID == "" {
		return nil, fmt.Errorf("pattern ID cannot be empty")
	}

	analysis := &ThreatAnalysis{
		ID:             fmt.Sprintf("analysis-%s-%d", pattern.ID, common.ConsensusNow().Unix()),
		PatternID:      pattern.ID,
		ThreatLevel:    ThreatLevelNone,
		Confidence:     0.0,
		Anomalies:      make([]string, 0),
		RiskIndicators: make([]string, 0),
		AnalyzedAt:     common.ConsensusNow(),
	}

	// Analyze pattern frequency
	if pattern.Frequency > 100 {
		analysis.Anomalies = append(analysis.Anomalies, "Unusually high pattern frequency")
		analysis.RiskIndicators = append(analysis.RiskIndicators, "Potential automated attack")
		analysis.Confidence += 0.3
	}

	// Analyze pattern data
	dataStr := string(pattern.Data)

	// Check for SQL injection patterns
	if td.containsSQLInjectionPattern(dataStr) {
		analysis.Anomalies = append(analysis.Anomalies, "SQL injection pattern detected")
		analysis.RiskIndicators = append(analysis.RiskIndicators, "Database attack attempt")
		analysis.Confidence += 0.4
	}

	// Check for XSS patterns
	if td.containsXSSPattern(dataStr) {
		analysis.Anomalies = append(analysis.Anomalies, "Cross-site scripting pattern detected")
		analysis.RiskIndicators = append(analysis.RiskIndicators, "XSS attack attempt")
		analysis.Confidence += 0.4
	}

	// Check for command injection patterns
	if td.containsCommandInjectionPattern(dataStr) {
		analysis.Anomalies = append(analysis.Anomalies, "Command injection pattern detected")
		analysis.RiskIndicators = append(analysis.RiskIndicators, "System command execution attempt")
		analysis.Confidence += 0.5
	}

	// Determine threat level based on confidence
	if analysis.Confidence >= 0.8 {
		analysis.ThreatLevel = ThreatLevelCritical
	} else if analysis.Confidence >= 0.6 {
		analysis.ThreatLevel = ThreatLevelHigh
	} else if analysis.Confidence >= 0.4 {
		analysis.ThreatLevel = ThreatLevelMedium
	} else if analysis.Confidence >= 0.2 {
		analysis.ThreatLevel = ThreatLevelLow
	}

	// Store pattern for future reference
	td.mu.Lock()
	td.patterns = append(td.patterns, pattern)
	td.mu.Unlock()

	return analysis, nil
}

// GetThreatLevel returns current threat level
func (td *ThreatDetectorImpl) GetThreatLevel() ThreatLevel {
	td.mu.RLock()
	defer td.mu.RUnlock()
	return td.threatLevel
}

// detectPatternThreats detects threats based on known patterns
func (td *ThreatDetectorImpl) detectPatternThreats(data []byte) []Threat {
	threats := make([]Threat, 0)
	dataStr := string(data)

	// Known malicious patterns
	maliciousPatterns := []struct {
		name        string
		pattern     string
		threatType  string
		severity    SeverityLevel
		description string
	}{
		{
			name:        "SQL Injection",
			pattern:     "' OR '1'='1",
			threatType:  "sql_injection",
			severity:    SeverityCritical,
			description: "SQL injection attempt detected",
		},
		{
			name:        "XSS Script Tag",
			pattern:     "<script>",
			threatType:  "xss",
			severity:    SeverityHigh,
			description: "Cross-site scripting attempt detected",
		},
		{
			name:        "Directory Traversal",
			pattern:     "../",
			threatType:  "path_traversal",
			severity:    SeverityHigh,
			description: "Directory traversal attempt detected",
		},
		{
			name:        "Command Injection",
			pattern:     "; rm -rf",
			threatType:  "command_injection",
			severity:    SeverityCritical,
			description: "Command injection attempt detected",
		},
	}

	for _, mp := range maliciousPatterns {
		if strings.Contains(dataStr, mp.pattern) {
			threat := Threat{
				ID:          fmt.Sprintf("threat-pattern-%d", common.ConsensusNow().UnixNano()),
				Type:        mp.threatType,
				Name:        mp.name,
				Description: mp.description,
				Severity:    mp.severity,
				Source:      "pattern_detection",
				Target:      "unknown",
				Indicators:  []string{mp.pattern},
				Mitigations: td.getMitigations(mp.threatType),
				DetectedAt:  common.ConsensusNow(),
				Metadata: map[string]interface{}{
					"pattern":     mp.pattern,
					"data_sample": dataStr[:min(100, len(dataStr))],
				},
			}
			threats = append(threats, threat)
		}
	}

	return threats
}

// detectAnomalies detects anomalous patterns
func (td *ThreatDetectorImpl) detectAnomalies(data []byte) []Threat {
	threats := make([]Threat, 0)

	// Analyze data characteristics
	dataStr := string(data)

	// Check for unusual character distributions
	if td.hasUnusualCharacterDistribution(dataStr) {
		threat := Threat{
			ID:          fmt.Sprintf("threat-anomaly-%d", common.ConsensusNow().UnixNano()),
			Type:        "anomaly",
			Name:        "Unusual Character Distribution",
			Description: "Data contains unusual character patterns that may indicate encoded malicious content",
			Severity:    SeverityMedium,
			Source:      "anomaly_detection",
			Target:      "data_analysis",
			Indicators:  []string{"high_entropy", "unusual_chars"},
			Mitigations: []string{"Inspect data for encoded payloads", "Apply input validation"},
			DetectedAt:  common.ConsensusNow(),
			Metadata: map[string]interface{}{
				"entropy": td.calculateEntropy(dataStr),
			},
		}
		threats = append(threats, threat)
	}

	// Check for excessive repetition (potential DoS)
	if td.hasExcessiveRepetition(dataStr) {
		threat := Threat{
			ID:          fmt.Sprintf("threat-dos-%d", common.ConsensusNow().UnixNano()),
			Type:        "dos_attempt",
			Name:        "Potential DoS Pattern",
			Description: "Data contains excessive repetition that may indicate a denial of service attempt",
			Severity:    SeverityHigh,
			Source:      "anomaly_detection",
			Target:      "resource_consumption",
			Indicators:  []string{"repetitive_pattern", "large_payload"},
			Mitigations: []string{"Implement rate limiting", "Set payload size limits"},
			DetectedAt:  common.ConsensusNow(),
			Metadata: map[string]interface{}{
				"data_size": len(data),
			},
		}
		threats = append(threats, threat)
	}

	return threats
}

// detectBehavioralThreats detects threats based on behavior analysis
func (td *ThreatDetectorImpl) detectBehavioralThreats(data []byte) []Threat {
	threats := make([]Threat, 0)

	// Parse request data to extract behavioral indicators
	dataStr := string(data)

	// Extract user ID from data (simplified for demo)
	userID := td.extractUserID(dataStr)
	if userID == "" {
		return threats
	}

	// Get or create baseline for user
	td.mu.RLock()
	baseline, exists := td.behaviorBaselines[userID]
	td.mu.RUnlock()

	if !exists {
		// First time seeing this user, create baseline
		baseline = BehaviorBaseline{
			UserID:             userID,
			AverageRequestRate: 10.0, // requests per minute
			CommonIPAddresses:  []string{},
			CommonResources:    []string{},
			CommonActions:      []string{},
			LastUpdated:        common.ConsensusNow(),
			AnomalyScore:       0.0,
		}
	}

	// Analyze current behavior
	currentBehavior := td.analyzeCurrentBehavior(dataStr)

	// Calculate anomaly score
	anomalyScore := td.calculateAnomalyScore(baseline, currentBehavior)

	// Check for specific behavioral anomalies
	if anomalyScore > td.anomalyThreshold {
		threat := Threat{
			ID:          fmt.Sprintf("threat-behavior-%d", common.ConsensusNow().UnixNano()),
			Type:        "behavioral_anomaly",
			Name:        "Anomalous User Behavior",
			Description: fmt.Sprintf("User %s showing abnormal behavior patterns (score: %.2f)", userID, anomalyScore),
			Severity:    td.severityFromAnomalyScore(anomalyScore),
			Source:      "behavior_analysis",
			Target:      userID,
			Indicators:  td.getAnomalyIndicators(baseline, currentBehavior),
			Mitigations: []string{
				"Monitor user activity closely",
				"Consider temporary access restrictions",
				"Verify user identity through additional means",
				"Review recent user actions for malicious intent",
			},
			DetectedAt: common.ConsensusNow(),
			Metadata: map[string]interface{}{
				"user_id":       userID,
				"anomaly_score": anomalyScore,
				"baseline":      baseline,
				"current":       currentBehavior,
			},
		}
		threats = append(threats, threat)
	}

	// Check for velocity attacks (rapid requests)
	if velocityThreat := td.checkVelocityAttack(userID, currentBehavior); velocityThreat != nil {
		threats = append(threats, *velocityThreat)
	}

	// Check for privilege escalation attempts
	if escalationThreat := td.checkPrivilegeEscalation(userID, currentBehavior); escalationThreat != nil {
		threats = append(threats, *escalationThreat)
	}

	// Update baseline with current behavior (with decay)
	td.updateUserBaseline(userID, baseline, currentBehavior)

	return threats
}

// detectSignatureThreats detects threats based on known signatures
func (td *ThreatDetectorImpl) detectSignatureThreats(data []byte) []Threat {
	threats := make([]Threat, 0)

	// Known malware/attack signatures (simplified)
	signatures := []struct {
		sig      []byte
		name     string
		severity SeverityLevel
	}{
		{
			sig:      []byte{0x4d, 0x5a}, // MZ header (potential executable)
			name:     "Executable File Upload",
			severity: SeverityHigh,
		},
		{
			sig:      []byte{0x1f, 0x8b}, // Gzip header (potential compressed malware)
			name:     "Compressed Payload",
			severity: SeverityMedium,
		},
	}

	for _, sig := range signatures {
		if len(data) >= len(sig.sig) && bytesEqual(data[:len(sig.sig)], sig.sig) {
			threat := Threat{
				ID:          fmt.Sprintf("threat-signature-%d", common.ConsensusNow().UnixNano()),
				Type:        "malware_signature",
				Name:        sig.name,
				Description: fmt.Sprintf("Known malicious signature detected: %s", sig.name),
				Severity:    sig.severity,
				Source:      "signature_detection",
				Target:      "file_upload",
				Indicators:  []string{fmt.Sprintf("%x", sig.sig)},
				Mitigations: []string{"Block file upload", "Scan with antivirus", "Quarantine suspicious files"},
				DetectedAt:  common.ConsensusNow(),
				Metadata: map[string]interface{}{
					"signature": fmt.Sprintf("%x", sig.sig),
				},
			}
			threats = append(threats, threat)
		}
	}

	return threats
}

// updateThreatLevel updates the overall threat level
func (td *ThreatDetectorImpl) updateThreatLevel(threats []Threat) {
	td.mu.Lock()
	defer td.mu.Unlock()

	if len(threats) == 0 {
		return
	}

	// Determine highest severity
	highestSeverity := SeverityInfo
	criticalCount := 0
	highCount := 0

	for _, threat := range threats {
		switch threat.Severity {
		case SeverityCritical:
			criticalCount++
			highestSeverity = SeverityCritical
		case SeverityHigh:
			highCount++
			if highestSeverity != SeverityCritical {
				highestSeverity = SeverityHigh
			}
		case SeverityMedium:
			if highestSeverity != SeverityCritical && highestSeverity != SeverityHigh {
				highestSeverity = SeverityMedium
			}
		}
	}

	// Update threat level
	if criticalCount > 0 {
		td.threatLevel = ThreatLevelCritical
	} else if highCount > 2 {
		td.threatLevel = ThreatLevelCritical
	} else if highCount > 0 {
		td.threatLevel = ThreatLevelHigh
	} else if highestSeverity == SeverityMedium {
		td.threatLevel = ThreatLevelMedium
	} else {
		td.threatLevel = ThreatLevelLow
	}
}

// Helper functions for pattern detection
func (td *ThreatDetectorImpl) containsSQLInjectionPattern(data string) bool {
	sqlPatterns := []string{
		"' OR '",
		"'; DROP TABLE",
		"UNION SELECT",
		"1=1",
		"' OR 1=1",
		"admin'--",
		"' OR 'a'='a",
	}

	dataLower := strings.ToLower(data)
	for _, pattern := range sqlPatterns {
		if strings.Contains(dataLower, strings.ToLower(pattern)) {
			return true
		}
	}
	return false
}

func (td *ThreatDetectorImpl) containsXSSPattern(data string) bool {
	xssPatterns := []string{
		"<script",
		"javascript:",
		"onerror=",
		"onload=",
		"onclick=",
		"<iframe",
		"<embed",
		"<object",
	}

	dataLower := strings.ToLower(data)
	for _, pattern := range xssPatterns {
		if strings.Contains(dataLower, pattern) {
			return true
		}
	}
	return false
}

func (td *ThreatDetectorImpl) containsCommandInjectionPattern(data string) bool {
	cmdPatterns := []string{
		"; rm ",
		"&& rm ",
		"| rm ",
		"`rm ",
		"$(rm ",
		"; cat /etc/passwd",
		"&& whoami",
		"| nc ",
		"; wget ",
	}

	for _, pattern := range cmdPatterns {
		if strings.Contains(data, pattern) {
			return true
		}
	}
	return false
}

func (td *ThreatDetectorImpl) hasUnusualCharacterDistribution(data string) bool {
	// Simple entropy check
	entropy := td.calculateEntropy(data)
	return entropy > 7.5 // High entropy might indicate encoded/encrypted content
}

func (td *ThreatDetectorImpl) calculateEntropy(data string) float64 {
	if len(data) == 0 {
		return 0
	}

	// Character frequency
	frequency := make(map[rune]int)
	for _, char := range data {
		frequency[char]++
	}

	// Calculate entropy
	var entropy float64
	dataLen := float64(len(data))

	for _, count := range frequency {
		probability := float64(count) / dataLen
		if probability > 0 {
			entropy -= probability * math.Log2(probability)
		}
	}

	return entropy
}

func (td *ThreatDetectorImpl) hasExcessiveRepetition(data string) bool {
	if len(data) < 100 {
		return false
	}

	// Check for repeated substrings
	for i := 0; i < len(data)-10; i++ {
		substring := data[i:min(i+10, len(data))]
		count := strings.Count(data, substring)
		if count > 10 {
			return true
		}
	}

	return false
}

func (td *ThreatDetectorImpl) getMitigations(threatType string) []string {
	mitigations := map[string][]string{
		"sql_injection": {
			"Use parameterized queries",
			"Implement input validation",
			"Apply least privilege database access",
			"Enable SQL query logging",
		},
		"xss": {
			"Encode user input before display",
			"Implement Content Security Policy",
			"Use HTTP-only cookies",
			"Validate and sanitize all inputs",
		},
		"path_traversal": {
			"Validate file paths",
			"Use chroot jails",
			"Implement access controls",
			"Sanitize user input",
		},
		"command_injection": {
			"Never pass user input to system commands",
			"Use safe APIs instead of shell commands",
			"Implement strict input validation",
			"Apply principle of least privilege",
		},
	}

	if m, exists := mitigations[threatType]; exists {
		return m
	}

	return []string{"Investigate and remediate the threat", "Implement security best practices"}
}

// Helper functions
func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// UpdateBehaviorBaseline updates the behavior baseline for a user
func (td *ThreatDetectorImpl) UpdateBehaviorBaseline(userID string, baseline BehaviorBaseline) {
	td.mu.Lock()
	defer td.mu.Unlock()
	baseline.LastUpdated = common.ConsensusNow()
	td.behaviorBaselines[userID] = baseline
}

// GetThreatStatistics returns statistics about detected threats
func (td *ThreatDetectorImpl) GetThreatStatistics() map[string]interface{} {
	td.mu.RLock()
	defer td.mu.RUnlock()

	stats := make(map[string]interface{})

	// Count by type and severity
	typeCount := make(map[string]int)
	severityCount := make(map[SeverityLevel]int)

	for _, threat := range td.detectedThreats {
		typeCount[threat.Type]++
		severityCount[threat.Severity]++
	}

	stats["total_threats"] = len(td.detectedThreats)
	stats["threats_by_type"] = typeCount
	stats["threats_by_severity"] = severityCount
	stats["current_threat_level"] = td.threatLevel
	stats["patterns_stored"] = len(td.patterns)
	stats["baselines_stored"] = len(td.behaviorBaselines)

	return stats
}

// extractUserID extracts user ID from request data
func (td *ThreatDetectorImpl) extractUserID(data string) string {
	// Look for common user ID patterns in request data
	// This is a simplified implementation - in production would parse actual request format

	// Check for user_id parameter
	if idx := strings.Index(data, "user_id="); idx != -1 {
		endIdx := strings.IndexAny(data[idx+8:], "&\n\r ")
		if endIdx == -1 {
			return data[idx+8:]
		}
		return data[idx+8 : idx+8+endIdx]
	}

	// Check for userId in JSON
	if idx := strings.Index(data, `"userId":`); idx != -1 {
		startIdx := idx + 9
		// Skip whitespace and quotes
		for startIdx < len(data) && (data[startIdx] == ' ' || data[startIdx] == '"') {
			startIdx++
		}
		endIdx := startIdx
		for endIdx < len(data) && data[endIdx] != '"' && data[endIdx] != ',' && data[endIdx] != '}' {
			endIdx++
		}
		if endIdx > startIdx {
			return data[startIdx:endIdx]
		}
	}

	// Check for user in path
	if idx := strings.Index(data, "/users/"); idx != -1 {
		startIdx := idx + 7
		endIdx := strings.IndexAny(data[startIdx:], "/?& \n\r")
		if endIdx == -1 {
			return data[startIdx:]
		}
		return data[startIdx : startIdx+endIdx]
	}

	return ""
}

// CurrentBehavior represents current user behavior metrics
type CurrentBehavior struct {
	RequestRate float64
	IPAddress   string
	Resources   []string
	Actions     []string
	RequestTime time.Time
	PayloadSize int
	UserAgent   string
	GeoLocation string
}

// analyzeCurrentBehavior extracts behavior metrics from request data
func (td *ThreatDetectorImpl) analyzeCurrentBehavior(data string) CurrentBehavior {
	behavior := CurrentBehavior{
		RequestTime: common.ConsensusNow(),
		Resources:   make([]string, 0),
		Actions:     make([]string, 0),
	}

	// Extract IP address
	if idx := strings.Index(data, "IP:"); idx != -1 {
		endIdx := strings.IndexAny(data[idx+3:], " \n\r")
		if endIdx > 0 {
			behavior.IPAddress = strings.TrimSpace(data[idx+3 : idx+3+endIdx])
		}
	}

	// Extract resources accessed
	resourcePatterns := []string{"/api/", "/admin/", "/user/", "/data/"}
	for _, pattern := range resourcePatterns {
		if idx := strings.Index(data, pattern); idx != -1 {
			endIdx := strings.IndexAny(data[idx:], " ?&\n\r")
			if endIdx > 0 {
				behavior.Resources = append(behavior.Resources, data[idx:idx+endIdx])
			}
		}
	}

	// Extract HTTP methods as actions
	httpMethods := []string{"GET", "POST", "PUT", "DELETE", "PATCH"}
	for _, method := range httpMethods {
		if strings.Contains(data, method+" ") {
			behavior.Actions = append(behavior.Actions, method)
		}
	}

	// Extract user agent
	if idx := strings.Index(data, "User-Agent:"); idx != -1 {
		endIdx := strings.Index(data[idx:], "\n")
		if endIdx > 0 {
			behavior.UserAgent = strings.TrimSpace(data[idx+11 : idx+endIdx])
		}
	}

	// Calculate request rate (simplified - in production would track over time)
	behavior.RequestRate = 1.0 // Single request

	// Payload size
	behavior.PayloadSize = len(data)

	return behavior
}

// calculateAnomalyScore calculates how anomalous the current behavior is
func (td *ThreatDetectorImpl) calculateAnomalyScore(baseline BehaviorBaseline, current CurrentBehavior) float64 {
	score := 0.0
	factors := 0

	// Check request rate deviation
	if baseline.AverageRequestRate > 0 {
		rateDeviation := math.Abs(current.RequestRate-baseline.AverageRequestRate) / baseline.AverageRequestRate
		score += math.Min(rateDeviation, 1.0)
		factors++
	}

	// Check if IP is unusual
	if len(baseline.CommonIPAddresses) > 0 {
		ipFound := false
		for _, ip := range baseline.CommonIPAddresses {
			if ip == current.IPAddress {
				ipFound = true
				break
			}
		}
		if !ipFound {
			score += 0.8
		}
		factors++
	}

	// Check for unusual resources
	if len(baseline.CommonResources) > 0 && len(current.Resources) > 0 {
		unusualResources := 0
		for _, resource := range current.Resources {
			found := false
			for _, common := range baseline.CommonResources {
				if resource == common {
					found = true
					break
				}
			}
			if !found {
				unusualResources++
			}
		}
		if len(current.Resources) > 0 {
			score += float64(unusualResources) / float64(len(current.Resources))
			factors++
		}
	}

	// Check for unusual actions
	if len(baseline.CommonActions) > 0 && len(current.Actions) > 0 {
		unusualActions := 0
		for _, action := range current.Actions {
			found := false
			for _, common := range baseline.CommonActions {
				if action == common {
					found = true
					break
				}
			}
			if !found {
				unusualActions++
			}
		}
		if len(current.Actions) > 0 {
			score += float64(unusualActions) / float64(len(current.Actions))
			factors++
		}
	}

	// Time-based anomaly (accessing at unusual hours)
	hour := current.RequestTime.Hour()
	if hour < 6 || hour > 22 { // Outside business hours
		score += 0.3
		factors++
	}

	// Large payload anomaly
	if current.PayloadSize > 10000 { // 10KB threshold
		score += 0.5
		factors++
	}

	if factors == 0 {
		return 0
	}

	return score / float64(factors)
}

// severityFromAnomalyScore converts anomaly score to severity level
func (td *ThreatDetectorImpl) severityFromAnomalyScore(score float64) SeverityLevel {
	if score >= 0.9 {
		return SeverityCritical
	} else if score >= 0.75 {
		return SeverityHigh
	} else if score >= 0.6 {
		return SeverityMedium
	} else if score >= 0.4 {
		return SeverityLow
	}
	return SeverityInfo
}

// getAnomalyIndicators returns specific indicators of anomalous behavior
func (td *ThreatDetectorImpl) getAnomalyIndicators(baseline BehaviorBaseline, current CurrentBehavior) []string {
	indicators := make([]string, 0)

	// Check request rate
	if baseline.AverageRequestRate > 0 {
		rateRatio := current.RequestRate / baseline.AverageRequestRate
		if rateRatio > 2.0 {
			indicators = append(indicators, fmt.Sprintf("Request rate %.1fx normal", rateRatio))
		}
	}

	// Check IP
	if len(baseline.CommonIPAddresses) > 0 {
		ipKnown := false
		for _, ip := range baseline.CommonIPAddresses {
			if ip == current.IPAddress {
				ipKnown = true
				break
			}
		}
		if !ipKnown {
			indicators = append(indicators, fmt.Sprintf("Unknown IP address: %s", current.IPAddress))
		}
	}

	// Check resources
	for _, resource := range current.Resources {
		if strings.Contains(resource, "/admin") || strings.Contains(resource, "/config") {
			indicators = append(indicators, fmt.Sprintf("Accessing sensitive resource: %s", resource))
		}
	}

	// Check actions
	for _, action := range current.Actions {
		if action == "DELETE" || action == "PUT" {
			indicators = append(indicators, fmt.Sprintf("Performing sensitive action: %s", action))
		}
	}

	// Time-based
	hour := current.RequestTime.Hour()
	if hour < 6 || hour > 22 {
		indicators = append(indicators, fmt.Sprintf("Activity outside business hours: %02d:00", hour))
	}

	// Payload size
	if current.PayloadSize > 10000 {
		indicators = append(indicators, fmt.Sprintf("Large payload: %d bytes", current.PayloadSize))
	}

	return indicators
}

// checkVelocityAttack checks for rapid request patterns
func (td *ThreatDetectorImpl) checkVelocityAttack(userID string, current CurrentBehavior) *Threat {
	// In production, this would track request timestamps and calculate actual velocity
	// For now, we'll use a threshold-based approach

	td.mu.RLock()
	baseline, exists := td.behaviorBaselines[userID]
	td.mu.RUnlock()

	if !exists {
		return nil
	}

	// Check if request rate is significantly higher than baseline
	if baseline.AverageRequestRate > 0 && current.RequestRate > baseline.AverageRequestRate*5 {
		return &Threat{
			ID:          fmt.Sprintf("threat-velocity-%d", common.ConsensusNow().UnixNano()),
			Type:        "velocity_attack",
			Name:        "Velocity Attack Detected",
			Description: fmt.Sprintf("User %s making requests at %dx normal rate", userID, int(current.RequestRate/baseline.AverageRequestRate)),
			Severity:    SeverityHigh,
			Source:      "behavior_analysis",
			Target:      userID,
			Indicators: []string{
				fmt.Sprintf("Request rate: %.1f/min", current.RequestRate),
				fmt.Sprintf("Normal rate: %.1f/min", baseline.AverageRequestRate),
				"Potential automated attack or account compromise",
			},
			Mitigations: []string{
				"Implement rate limiting",
				"Enable CAPTCHA verification",
				"Consider temporary account suspension",
				"Verify account ownership",
			},
			DetectedAt: common.ConsensusNow(),
			Metadata: map[string]interface{}{
				"user_id":      userID,
				"request_rate": current.RequestRate,
				"baseline":     baseline.AverageRequestRate,
			},
		}
	}

	return nil
}

// checkPrivilegeEscalation checks for privilege escalation attempts
func (td *ThreatDetectorImpl) checkPrivilegeEscalation(userID string, current CurrentBehavior) *Threat {
	// Check for access to admin resources or sensitive operations
	suspiciousPatterns := []string{
		"/admin",
		"/config",
		"/system",
		"/root",
		"/../",
		"sudo",
		"admin=true",
		"role=admin",
	}

	suspiciousActions := []string{}

	// Check resources
	for _, resource := range current.Resources {
		for _, pattern := range suspiciousPatterns {
			if strings.Contains(resource, pattern) {
				suspiciousActions = append(suspiciousActions, fmt.Sprintf("Accessing %s", resource))
			}
		}
	}

	// Check for privilege-related parameters in request
	if len(suspiciousActions) > 0 {
		return &Threat{
			ID:          fmt.Sprintf("threat-privesc-%d", common.ConsensusNow().UnixNano()),
			Type:        "privilege_escalation",
			Name:        "Privilege Escalation Attempt",
			Description: fmt.Sprintf("User %s attempting to access privileged resources", userID),
			Severity:    SeverityCritical,
			Source:      "behavior_analysis",
			Target:      userID,
			Indicators:  suspiciousActions,
			Mitigations: []string{
				"Review user permissions immediately",
				"Block access to administrative resources",
				"Audit recent user activities",
				"Verify authentication status",
				"Consider account suspension pending review",
			},
			DetectedAt: common.ConsensusNow(),
			Metadata: map[string]interface{}{
				"user_id":            userID,
				"suspicious_actions": suspiciousActions,
				"resources":          current.Resources,
			},
		}
	}

	return nil
}

// updateUserBaseline updates the baseline with new behavior data
func (td *ThreatDetectorImpl) updateUserBaseline(userID string, baseline BehaviorBaseline, current CurrentBehavior) {
	// Apply exponential decay to incorporate new behavior
	decayFactor := 0.9

	// Update request rate with moving average
	baseline.AverageRequestRate = baseline.AverageRequestRate*decayFactor + current.RequestRate*(1-decayFactor)

	// Update IP addresses
	if current.IPAddress != "" {
		ipExists := false
		for _, ip := range baseline.CommonIPAddresses {
			if ip == current.IPAddress {
				ipExists = true
				break
			}
		}
		if !ipExists {
			baseline.CommonIPAddresses = append(baseline.CommonIPAddresses, current.IPAddress)
			// Keep only last 10 IPs
			if len(baseline.CommonIPAddresses) > 10 {
				baseline.CommonIPAddresses = baseline.CommonIPAddresses[1:]
			}
		}
	}

	// Update resources
	for _, resource := range current.Resources {
		resourceExists := false
		for _, r := range baseline.CommonResources {
			if r == resource {
				resourceExists = true
				break
			}
		}
		if !resourceExists {
			baseline.CommonResources = append(baseline.CommonResources, resource)
			// Keep only last 20 resources
			if len(baseline.CommonResources) > 20 {
				baseline.CommonResources = baseline.CommonResources[1:]
			}
		}
	}

	// Update actions
	for _, action := range current.Actions {
		actionExists := false
		for _, a := range baseline.CommonActions {
			if a == action {
				actionExists = true
				break
			}
		}
		if !actionExists {
			baseline.CommonActions = append(baseline.CommonActions, action)
			// Keep only last 10 actions
			if len(baseline.CommonActions) > 10 {
				baseline.CommonActions = baseline.CommonActions[1:]
			}
		}
	}

	baseline.LastUpdated = common.ConsensusNow()

	// Update stored baseline
	td.mu.Lock()
	td.behaviorBaselines[userID] = baseline
	td.mu.Unlock()
}
