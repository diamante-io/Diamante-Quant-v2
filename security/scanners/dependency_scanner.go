package scanners

import (
	"fmt"
	"os/exec"
	"strings"
)

// DependencyScan runs a dependency scanning tool, such as govulncheck, on the given path.
func DependencyScan(tool, path string) (string, error) {
	// Validate tool to prevent command injection
	allowedTools := map[string]bool{
		"govulncheck": true,
		"nancy":       true,
		"gosec":       true,
	}

	if !allowedTools[tool] {
		return "", fmt.Errorf("tool '%s' is not allowed", tool)
	}

	// Validate path to prevent directory traversal
	if strings.Contains(path, "..") || strings.Contains(path, ";") || strings.Contains(path, "|") {
		return "", fmt.Errorf("invalid path: %s", path)
	}

	cmd := exec.Command(tool, path)
	out, err := cmd.CombinedOutput()
	return string(out), err
}
