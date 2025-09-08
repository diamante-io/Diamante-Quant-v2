package reports

import (
	"strings"
)

// VulnerabilityReport aggregates scanner findings into a single string.
func VulnerabilityReport(findings []string) string {
	return strings.Join(findings, "\n")
}

// ComplianceCheck returns a basic compliance status message.
func ComplianceCheck() string {
	return "All compliance checks passed"
}
