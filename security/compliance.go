package security

import (
	"context"
	"diamante/common"
	"fmt"
	"strings"
	"sync"
	"time"
)

// ComplianceChecker implements comprehensive compliance validation
type ComplianceCheckerImpl struct {
	mu                sync.RWMutex
	policies          []SecurityPolicy
	lastCheckTime     time.Time
	complianceStatus  ComplianceStatus
	checkerID         string
	productionChecker *ProductionComplianceChecker
}

// NewComplianceChecker creates a new compliance checker instance
func NewComplianceChecker() ComplianceChecker {
	return &ComplianceCheckerImpl{
		checkerID:         fmt.Sprintf("compliance-checker-%d", common.ConsensusNow().Unix()),
		policies:          make([]SecurityPolicy, 0),
		complianceStatus:  ComplianceStatusUnknown,
		productionChecker: NewProductionComplianceChecker(),
	}
}

// CheckCompliance validates compliance against security policies
func (cc *ComplianceCheckerImpl) CheckCompliance(ctx context.Context, policies []SecurityPolicy) (*ComplianceReport, error) {
	if ctx.Err() != nil {
		return nil, fmt.Errorf("context cancelled: %w", ctx.Err())
	}

	if len(policies) == 0 {
		return nil, fmt.Errorf("no policies provided for compliance check")
	}

	// Validate all policies first
	for _, policy := range policies {
		if err := cc.ValidatePolicy(policy); err != nil {
			return nil, fmt.Errorf("invalid policy %s: %w", policy.ID, err)
		}
	}

	report := &ComplianceReport{
		ID:              fmt.Sprintf("compliance-%s-%d", cc.checkerID, common.ConsensusNow().Unix()),
		GeneratedAt:     common.ConsensusNow(),
		Status:          ComplianceStatusCompliant,
		PolicyResults:   make([]PolicyResult, 0),
		FailedPolicies:  make([]string, 0),
		Recommendations: make([]string, 0),
	}

	// Check each policy
	totalScore := 0.0
	compliantCount := 0

	for _, policy := range policies {
		result := cc.checkPolicy(ctx, policy)
		report.PolicyResults = append(report.PolicyResults, result)

		switch result.Status {
		case ComplianceStatusCompliant:
			compliantCount++
			totalScore += 1.0
		case ComplianceStatusPartial:
			totalScore += 0.5
		default:
			report.FailedPolicies = append(report.FailedPolicies, policy.ID)
		}
	}

	// Calculate compliance score
	if len(policies) > 0 {
		report.ComplianceScore = (totalScore / float64(len(policies))) * 100
	}

	// Determine overall status
	if compliantCount == len(policies) {
		report.Status = ComplianceStatusCompliant
	} else if compliantCount > 0 {
		report.Status = ComplianceStatusPartial
	} else {
		report.Status = ComplianceStatusNonCompliant
	}

	// Generate recommendations
	report.Recommendations = cc.generateRecommendations(report)

	// Update internal state
	cc.mu.Lock()
	cc.policies = policies
	cc.lastCheckTime = common.ConsensusNow()
	cc.complianceStatus = report.Status
	cc.mu.Unlock()

	return report, nil
}

// ValidatePolicy validates a single security policy
func (cc *ComplianceCheckerImpl) ValidatePolicy(policy SecurityPolicy) error {
	if policy.ID == "" {
		return fmt.Errorf("policy ID cannot be empty")
	}

	if policy.Name == "" {
		return fmt.Errorf("policy name cannot be empty")
	}

	if len(policy.Rules) == 0 {
		return fmt.Errorf("policy must have at least one rule")
	}

	// Validate each rule
	for _, rule := range policy.Rules {
		if rule.ID == "" {
			return fmt.Errorf("rule ID cannot be empty")
		}
		if rule.Condition == "" {
			return fmt.Errorf("rule condition cannot be empty")
		}
		if rule.Action == "" {
			return fmt.Errorf("rule action cannot be empty")
		}
	}

	return nil
}

// GetComplianceStatus returns current compliance status
func (cc *ComplianceCheckerImpl) GetComplianceStatus() ComplianceStatus {
	cc.mu.RLock()
	defer cc.mu.RUnlock()
	return cc.complianceStatus
}

// checkPolicy checks a single policy for compliance
func (cc *ComplianceCheckerImpl) checkPolicy(ctx context.Context, policy SecurityPolicy) PolicyResult {
	result := PolicyResult{
		PolicyID:   policy.ID,
		PolicyName: policy.Name,
		Status:     ComplianceStatusCompliant,
		Violations: make([]string, 0),
		CheckedAt:  common.ConsensusNow(),
	}

	if !policy.Enabled {
		result.Status = ComplianceStatusUnknown
		result.Violations = append(result.Violations, "Policy is disabled")
		return result
	}

	// Check each rule in the policy
	violationCount := 0
	for _, rule := range policy.Rules {
		if violation := cc.checkRule(ctx, rule, policy.Category); violation != "" {
			result.Violations = append(result.Violations, violation)
			violationCount++
		}
	}

	// Determine policy compliance status
	if violationCount == 0 {
		result.Status = ComplianceStatusCompliant
	} else if violationCount < len(policy.Rules)/2 {
		result.Status = ComplianceStatusPartial
	} else {
		result.Status = ComplianceStatusNonCompliant
	}

	return result
}

// checkRule checks a single rule for compliance
func (cc *ComplianceCheckerImpl) checkRule(_ context.Context, rule PolicyRule, category string) string {
	// This is a simplified rule checking implementation
	// In production, this would integrate with actual system state

	switch category {
	case "access_control":
		return cc.checkAccessControlRule(rule)
	case "data_protection":
		return cc.checkDataProtectionRule(rule)
	case "network_security":
		return cc.checkNetworkSecurityRule(rule)
	case "authentication":
		return cc.checkAuthenticationRule(rule)
	case "logging":
		return cc.checkLoggingRule(rule)
	default:
		// Generic rule checking
		if rule.Severity == SeverityCritical {
			// For demo purposes, assume critical rules need special attention
			return fmt.Sprintf("Rule %s requires manual verification", rule.ID)
		}
	}

	return "" // No violation
}

// checkAccessControlRule checks access control rules
func (cc *ComplianceCheckerImpl) checkAccessControlRule(rule PolicyRule) string {
	// Use production checker if available
	if cc.productionChecker != nil {
		return cc.productionChecker.CheckAccessControlRule(rule)
	}

	// Fallback to simplified check
	switch rule.Condition {
	case "least_privilege":
		// Check if least privilege is enforced
		if params, ok := rule.Parameters["required_level"]; ok {
			if level, ok := params.(string); ok && level == "strict" {
				// In production, would check actual permissions
				return ""
			}
		}
		return fmt.Sprintf("Least privilege not properly enforced for rule %s", rule.ID)
	case "role_based_access":
		// Check RBAC implementation
		return ""
	default:
		return ""
	}
}

// checkDataProtectionRule checks data protection rules
func (cc *ComplianceCheckerImpl) checkDataProtectionRule(rule PolicyRule) string {
	// Use production checker if available
	if cc.productionChecker != nil {
		return cc.productionChecker.CheckDataProtectionRule(rule)
	}

	// Fallback to simplified check
	switch rule.Condition {
	case "encryption_at_rest":
		// Check if data is encrypted at rest
		if params, ok := rule.Parameters["algorithm"]; ok {
			if algo, ok := params.(string); ok && algo != "AES-256" {
				return fmt.Sprintf("Insufficient encryption algorithm for rule %s", rule.ID)
			}
		}
		return ""
	case "encryption_in_transit":
		// Check TLS configuration
		return ""
	default:
		return ""
	}
}

// checkNetworkSecurityRule checks network security rules
func (cc *ComplianceCheckerImpl) checkNetworkSecurityRule(rule PolicyRule) string {
	// Use production checker if available
	if cc.productionChecker != nil {
		return cc.productionChecker.CheckNetworkSecurityRule(rule)
	}

	// Fallback to simplified check
	switch rule.Condition {
	case "firewall_configured":
		// Check firewall configuration
		return ""
	case "ports_restricted":
		// Check for exposed ports
		if _, ok := rule.Parameters["allowed_ports"]; ok {
			// In production, would check actual open ports
			return ""
		}
		return fmt.Sprintf("Unrestricted ports detected for rule %s", rule.ID)
	default:
		return ""
	}
}

// checkAuthenticationRule checks authentication rules
func (cc *ComplianceCheckerImpl) checkAuthenticationRule(rule PolicyRule) string {
	// Use production checker if available
	if cc.productionChecker != nil {
		return cc.productionChecker.CheckAuthenticationRule(rule)
	}

	// Fallback to simplified check
	switch rule.Condition {
	case "multi_factor_auth":
		// Check if MFA is enabled
		if params, ok := rule.Parameters["required"]; ok {
			if required, ok := params.(bool); ok && required {
				// In production, would check MFA configuration
				return ""
			}
		}
		return fmt.Sprintf("Multi-factor authentication not enabled for rule %s", rule.ID)
	case "password_policy":
		// Check password policy compliance
		return ""
	default:
		return ""
	}
}

// checkLoggingRule checks logging rules
func (cc *ComplianceCheckerImpl) checkLoggingRule(rule PolicyRule) string {
	// Use production checker if available
	if cc.productionChecker != nil {
		return cc.productionChecker.CheckLoggingRule(rule)
	}

	// Fallback to simplified check
	switch rule.Condition {
	case "audit_logging_enabled":
		// Check if audit logging is enabled
		return ""
	case "log_retention":
		// Check log retention policy
		if params, ok := rule.Parameters["min_days"]; ok {
			if days, ok := params.(int); ok && days < 90 {
				return fmt.Sprintf("Insufficient log retention period for rule %s", rule.ID)
			}
		}
		return ""
	default:
		return ""
	}
}

// generateRecommendations generates recommendations based on compliance report
func (cc *ComplianceCheckerImpl) generateRecommendations(report *ComplianceReport) []string {
	recommendations := []string{}

	// Based on compliance score
	if report.ComplianceScore < 50 {
		recommendations = append(recommendations, "Immediate action required to address critical compliance failures")
		recommendations = append(recommendations, "Conduct comprehensive security audit and remediation")
	} else if report.ComplianceScore < 80 {
		recommendations = append(recommendations, "Address non-compliant policies to improve security posture")
		recommendations = append(recommendations, "Implement automated compliance monitoring")
	}

	// Based on specific violations
	hasAccessControl := false
	hasDataProtection := false
	hasNetworkSecurity := false

	for _, result := range report.PolicyResults {
		for _, violation := range result.Violations {
			if !hasAccessControl && containsKeyword(violation, "access", "privilege", "rbac") {
				hasAccessControl = true
				recommendations = append(recommendations, "Review and strengthen access control mechanisms")
			}
			if !hasDataProtection && containsKeyword(violation, "encryption", "data protection") {
				hasDataProtection = true
				recommendations = append(recommendations, "Implement comprehensive data encryption strategy")
			}
			if !hasNetworkSecurity && containsKeyword(violation, "network", "firewall", "ports") {
				hasNetworkSecurity = true
				recommendations = append(recommendations, "Enhance network security configurations")
			}
		}
	}

	// General recommendations
	if report.Status != ComplianceStatusCompliant {
		recommendations = append(recommendations, "Establish regular compliance assessment schedule")
		recommendations = append(recommendations, "Document all security policies and procedures")
		recommendations = append(recommendations, "Provide security awareness training to all staff")
	}

	return recommendations
}

// containsKeyword checks if text contains any of the keywords
func containsKeyword(text string, keywords ...string) bool {
	if text == "" {
		return false
	}

	textLower := strings.ToLower(text)
	for _, keyword := range keywords {
		if strings.Contains(textLower, strings.ToLower(keyword)) {
			return true
		}
	}
	return false
}

// GetDefaultPolicies returns a set of default security policies
func GetDefaultPolicies() []SecurityPolicy {
	return []SecurityPolicy{
		{
			ID:          "pol-access-001",
			Name:        "Access Control Policy",
			Description: "Ensures proper access control mechanisms are in place",
			Category:    "access_control",
			Rules: []PolicyRule{
				{
					ID:        "rule-ac-001",
					Name:      "Least Privilege",
					Condition: "least_privilege",
					Action:    "enforce",
					Severity:  SeverityHigh,
					Parameters: map[string]interface{}{
						"required_level": "strict",
					},
				},
				{
					ID:        "rule-ac-002",
					Name:      "Role-Based Access",
					Condition: "role_based_access",
					Action:    "enforce",
					Severity:  SeverityMedium,
					Parameters: map[string]interface{}{
						"min_roles": 3,
					},
				},
			},
			Enabled:   true,
			CreatedAt: common.ConsensusNow(),
			UpdatedAt: common.ConsensusNow(),
			Metadata:  map[string]interface{}{"version": "1.0"},
		},
		{
			ID:          "pol-data-001",
			Name:        "Data Protection Policy",
			Description: "Ensures data is properly protected at rest and in transit",
			Category:    "data_protection",
			Rules: []PolicyRule{
				{
					ID:        "rule-dp-001",
					Name:      "Encryption at Rest",
					Condition: "encryption_at_rest",
					Action:    "enforce",
					Severity:  SeverityCritical,
					Parameters: map[string]interface{}{
						"algorithm": "AES-256",
					},
				},
				{
					ID:        "rule-dp-002",
					Name:      "Encryption in Transit",
					Condition: "encryption_in_transit",
					Action:    "enforce",
					Severity:  SeverityCritical,
					Parameters: map[string]interface{}{
						"min_tls_version": "1.2",
					},
				},
			},
			Enabled:   true,
			CreatedAt: common.ConsensusNow(),
			UpdatedAt: common.ConsensusNow(),
			Metadata:  map[string]interface{}{"version": "1.0"},
		},
		{
			ID:          "pol-net-001",
			Name:        "Network Security Policy",
			Description: "Ensures network is properly secured",
			Category:    "network_security",
			Rules: []PolicyRule{
				{
					ID:        "rule-ns-001",
					Name:      "Firewall Configuration",
					Condition: "firewall_configured",
					Action:    "enforce",
					Severity:  SeverityHigh,
					Parameters: map[string]interface{}{
						"default_deny": true,
					},
				},
				{
					ID:        "rule-ns-002",
					Name:      "Port Restrictions",
					Condition: "ports_restricted",
					Action:    "enforce",
					Severity:  SeverityMedium,
					Parameters: map[string]interface{}{
						"allowed_ports": []int{22, 443, 8080},
					},
				},
			},
			Enabled:   true,
			CreatedAt: common.ConsensusNow(),
			UpdatedAt: common.ConsensusNow(),
			Metadata:  map[string]interface{}{"version": "1.0"},
		},
	}
}
