// Package chaincode provides Docker client integration for chaincode runtime
package chaincode

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"diamante/consensus"

	dockercontainer "github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
	"github.com/sirupsen/logrus"
)

// DockerClient manages Docker containers for chaincode execution
type DockerClient struct {
	client          *client.Client
	logger          *logrus.Logger
	networkID       string
	maxContainers   int
	containerPrefix string
	mu              sync.RWMutex

	// Container tracking
	containers map[string]*ContainerInfo
	portPool   *PortPool

	// Metrics
	containersCreated int64
	containersRemoved int64
}

// PortPool manages available ports for containers
type PortPool struct {
	mu        sync.Mutex
	basePort  int
	maxPorts  int
	available []int
	allocated map[int]string // port -> containerID
}

// NewDockerClient creates a new Docker client for chaincode management
func NewDockerClient(config *ChaincodeConfig, logger *logrus.Logger) (*DockerClient, error) {
	// Create Docker client
	cli, err := client.NewClientWithOpts(
		client.WithHost(config.DockerEndpoint),
		client.WithAPIVersionNegotiation(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create Docker client: %w", err)
	}

	// Verify Docker daemon is accessible
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := cli.Ping(ctx); err != nil {
		return nil, fmt.Errorf("failed to connect to Docker daemon: %w", err)
	}

	dc := &DockerClient{
		client:          cli,
		logger:          logger,
		maxContainers:   config.MaxContainers,
		containerPrefix: "diamante-chaincode-",
		containers:      make(map[string]*ContainerInfo),
		portPool:        NewPortPool(7050, 1000), // Base port 7050, 1000 ports available
	}

	// Create or get chaincode network
	if err := dc.ensureNetwork(config.NetworkMode); err != nil {
		return nil, fmt.Errorf("failed to ensure network: %w", err)
	}

	// Clean up any orphaned containers
	if err := dc.cleanupOrphanedContainers(); err != nil {
		logger.WithError(err).Warn("Failed to cleanup orphaned containers")
	}

	return dc, nil
}

// NewPortPool creates a new port pool
func NewPortPool(basePort, maxPorts int) *PortPool {
	pp := &PortPool{
		basePort:  basePort,
		maxPorts:  maxPorts,
		available: make([]int, maxPorts),
		allocated: make(map[int]string),
	}

	// Initialize available ports
	for i := 0; i < maxPorts; i++ {
		pp.available[i] = basePort + i
	}

	return pp
}

// AllocatePort allocates a port for a container
func (pp *PortPool) AllocatePort(containerID string) (int, error) {
	pp.mu.Lock()
	defer pp.mu.Unlock()

	if len(pp.available) == 0 {
		return 0, fmt.Errorf("no available ports")
	}

	// Take the first available port
	port := pp.available[0]
	pp.available = pp.available[1:]
	pp.allocated[port] = containerID

	return port, nil
}

// ReleasePort releases a port back to the pool
func (pp *PortPool) ReleasePort(port int) {
	pp.mu.Lock()
	defer pp.mu.Unlock()

	if _, exists := pp.allocated[port]; exists {
		delete(pp.allocated, port)
		pp.available = append(pp.available, port)
	}
}

// CreateContainer creates a new chaincode container
func (dc *DockerClient) CreateContainer(chaincodeID, imageName string, env []string) (*ContainerInfo, error) {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	// Check container limit
	if len(dc.containers) >= dc.maxContainers {
		return nil, fmt.Errorf("maximum container limit reached: %d", dc.maxContainers)
	}

	// Allocate port
	port, err := dc.portPool.AllocatePort(chaincodeID)
	if err != nil {
		return nil, fmt.Errorf("failed to allocate port: %w", err)
	}

	containerName := dc.containerPrefix + chaincodeID[:8]
	ctx := context.Background()

	// Container configuration
	containerConfig := &dockercontainer.Config{
		Image: imageName,
		Env:   env,
		ExposedPorts: nat.PortSet{
			"7052/tcp": struct{}{},
		},
		Labels: map[string]string{
			"diamante.chaincode.id":   chaincodeID,
			"diamante.chaincode.type": "chaincode",
			"diamante.created":        consensus.ConsensusNow().Format(time.RFC3339),
		},
	}

	// Host configuration
	hostConfig := &dockercontainer.HostConfig{
		PortBindings: nat.PortMap{
			"7052/tcp": []nat.PortBinding{
				{
					HostIP:   "0.0.0.0",
					HostPort: fmt.Sprintf("%d", port),
				},
			},
		},
		Resources: dockercontainer.Resources{
			Memory:     512 * 1024 * 1024, // 512MB
			MemorySwap: 512 * 1024 * 1024, // No swap
			CPUShares:  512,               // 0.5 CPU
		},
		RestartPolicy: dockercontainer.RestartPolicy{
			Name: "unless-stopped",
		},
		AutoRemove: false,
	}

	// Network configuration
	networkConfig := &network.NetworkingConfig{
		EndpointsConfig: map[string]*network.EndpointSettings{
			dc.networkID: {},
		},
	}

	// Create container
	resp, err := dc.client.ContainerCreate(
		ctx,
		containerConfig,
		hostConfig,
		networkConfig,
		nil,
		containerName,
	)
	if err != nil {
		dc.portPool.ReleasePort(port)
		return nil, fmt.Errorf("failed to create container: %w", err)
	}

	// Start container
	if err := dc.client.ContainerStart(ctx, resp.ID, dockercontainer.StartOptions{}); err != nil {
		// Clean up on failure
		dc.client.ContainerRemove(ctx, resp.ID, dockercontainer.RemoveOptions{Force: true})
		dc.portPool.ReleasePort(port)
		return nil, fmt.Errorf("failed to start container: %w", err)
	}

	// Wait for container to be healthy
	if err := dc.waitForContainerHealth(resp.ID, 30*time.Second); err != nil {
		// Clean up on failure
		dc.StopContainer(resp.ID)
		dc.RemoveContainer(resp.ID)
		return nil, fmt.Errorf("container failed health check: %w", err)
	}

	containerInfo := &ContainerInfo{
		ID:          resp.ID,
		ChaincodeID: chaincodeID,
		Status:      "running",
		StartedAt:   consensus.ConsensusNow(),
		Port:        port,
	}

	dc.containers[resp.ID] = containerInfo
	dc.containersCreated++

	dc.logger.WithFields(logrus.Fields{
		"containerID": resp.ID[:12],
		"chaincodeID": chaincodeID,
		"port":        port,
	}).Info("Chaincode container created")

	return containerInfo, nil
}

// StopContainer stops a running container
func (dc *DockerClient) StopContainer(containerID string) error {
	ctx := context.Background()

	// The Docker SDK ContainerStop expects a container.StopOptions with timeout in seconds
	timeout := 10 // 10 seconds
	stopOptions := dockercontainer.StopOptions{
		Timeout: &timeout,
	}

	if err := dc.client.ContainerStop(ctx, containerID, stopOptions); err != nil {
		return fmt.Errorf("failed to stop container: %w", err)
	}

	dc.logger.WithField("containerID", containerID[:12]).Info("Container stopped")
	return nil
}

// RemoveContainer removes a stopped container
func (dc *DockerClient) RemoveContainer(containerID string) error {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	ctx := context.Background()

	// Get container info before removal
	containerInfo, exists := dc.containers[containerID]
	if exists {
		// Release port
		dc.portPool.ReleasePort(containerInfo.Port)
		delete(dc.containers, containerID)
	}

	// Remove container
	if err := dc.client.ContainerRemove(ctx, containerID, dockercontainer.RemoveOptions{
		Force:         true,
		RemoveVolumes: true,
	}); err != nil {
		return fmt.Errorf("failed to remove container: %w", err)
	}

	dc.containersRemoved++
	dc.logger.WithField("containerID", containerID[:12]).Info("Container removed")

	return nil
}

// GetContainerStatus gets the status of a container
func (dc *DockerClient) GetContainerStatus(containerID string) (string, error) {
	ctx := context.Background()

	inspect, err := dc.client.ContainerInspect(ctx, containerID)
	if err != nil {
		return "", fmt.Errorf("failed to inspect container: %w", err)
	}

	return inspect.State.Status, nil
}

// GetContainerLogs retrieves container logs
func (dc *DockerClient) GetContainerLogs(containerID string, tail int) (string, error) {
	ctx := context.Background()

	options := dockercontainer.LogsOptions{
		ShowStdout: true,
		ShowStderr: true,
		Tail:       fmt.Sprintf("%d", tail),
		Timestamps: true,
	}

	reader, err := dc.client.ContainerLogs(ctx, containerID, options)
	if err != nil {
		return "", fmt.Errorf("failed to get container logs: %w", err)
	}
	defer reader.Close()

	logs, err := io.ReadAll(reader)
	if err != nil {
		return "", fmt.Errorf("failed to read logs: %w", err)
	}

	return string(logs), nil
}

// MonitorContainerHealth monitors the health of all containers
func (dc *DockerClient) MonitorContainerHealth() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		dc.mu.RLock()
		containerIDs := make([]string, 0, len(dc.containers))
		for id := range dc.containers {
			containerIDs = append(containerIDs, id)
		}
		dc.mu.RUnlock()

		for _, id := range containerIDs {
			status, err := dc.GetContainerStatus(id)
			if err != nil {
				dc.logger.WithError(err).WithField("containerID", id[:12]).Warn("Failed to check container status")
				continue
			}

			if status != "running" {
				dc.logger.WithFields(logrus.Fields{
					"containerID": id[:12],
					"status":      status,
				}).Warn("Container not running")

				// Update container info
				dc.mu.Lock()
				if info, exists := dc.containers[id]; exists {
					info.Status = status
				}
				dc.mu.Unlock()
			}
		}
	}
}

// ensureNetwork ensures the chaincode network exists
func (dc *DockerClient) ensureNetwork(networkMode string) error {
	if dc.client == nil {
		return fmt.Errorf("docker client is not initialized")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	networkName := "diamante-chaincode-net"

	// List existing networks
	networks, err := dc.client.NetworkList(ctx, network.ListOptions{
		Filters: filters.NewArgs(filters.Arg("name", networkName)),
	})
	if err != nil {
		return fmt.Errorf("failed to list networks: %w", err)
	}

	// Network already exists
	if len(networks) > 0 {
		dc.networkID = networks[0].ID
		dc.logger.WithField("networkID", dc.networkID).Debug("Using existing chaincode network")

		// Verify network is usable
		networkResource, err := dc.client.NetworkInspect(ctx, dc.networkID, network.InspectOptions{})
		if err != nil {
			return fmt.Errorf("failed to inspect existing network: %w", err)
		}

		if networkResource.Driver != networkMode {
			dc.logger.WithFields(logrus.Fields{
				"expected": networkMode,
				"actual":   networkResource.Driver,
			}).Warn("Network driver mismatch")
		}

		return nil
	}

	// Create network
	enableIPv6 := false
	resp, err := dc.client.NetworkCreate(ctx, networkName, network.CreateOptions{
		Driver: networkMode,
		Labels: map[string]string{
			"diamante.network.type": "chaincode",
			"created":               consensus.ConsensusNow().Format(time.RFC3339),
		},
		EnableIPv6: &enableIPv6,
	})
	if err != nil {
		// Check if it's a duplicate error (race condition)
		if strings.Contains(err.Error(), "already exists") {
			// Try to get the network again
			networks, listErr := dc.client.NetworkList(ctx, network.ListOptions{
				Filters: filters.NewArgs(filters.Arg("name", networkName)),
			})
			if listErr != nil {
				return fmt.Errorf("failed to list networks after create error: %w", listErr)
			}
			if len(networks) > 0 {
				dc.networkID = networks[0].ID
				dc.logger.WithField("networkID", dc.networkID).Info("Using network created by another process")
				return nil
			}
		}
		return fmt.Errorf("failed to create network: %w", err)
	}

	dc.networkID = resp.ID
	dc.logger.WithField("networkID", dc.networkID).Info("Created chaincode network")

	return nil
}

// cleanupOrphanedContainers removes any orphaned chaincode containers
func (dc *DockerClient) cleanupOrphanedContainers() error {
	if dc.client == nil {
		return fmt.Errorf("docker client is not initialized")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// List all containers with our label
	containers, err := dc.client.ContainerList(ctx, dockercontainer.ListOptions{
		All: true,
		Filters: filters.NewArgs(
			filters.Arg("label", "diamante.chaincode.type=chaincode"),
		),
	})
	if err != nil {
		return fmt.Errorf("failed to list containers: %w", err)
	}

	var cleanupErrors []error
	removedCount := 0

	// Remove orphaned containers
	for _, container := range containers {
		// Check if container is tracked
		dc.mu.RLock()
		_, tracked := dc.containers[container.ID]
		dc.mu.RUnlock()

		if !tracked {
			// Get container age
			created := container.Created
			age := time.Since(time.Unix(created, 0))

			dc.logger.WithFields(logrus.Fields{
				"containerID": container.ID[:12],
				"age":         age.String(),
				"status":      container.Status,
			}).Info("Found orphaned container")

			// Stop container if running
			if container.State == "running" {
				stopCtx, stopCancel := context.WithTimeout(context.Background(), 10*time.Second)
				// The Docker SDK ContainerStop expects a container.StopOptions with timeout in seconds
				timeout := 5 // 5 seconds
				stopOptions := dockercontainer.StopOptions{
					Timeout: &timeout,
				}
				if err := dc.client.ContainerStop(stopCtx, container.ID, stopOptions); err != nil {
					dc.logger.WithError(err).WithField("containerID", container.ID[:12]).Warn("Failed to stop orphaned container")
					cleanupErrors = append(cleanupErrors, fmt.Errorf("stop container %s: %w", container.ID[:12], err))
				}
				stopCancel()
			}

			// Remove container
			removeCtx, removeCancel := context.WithTimeout(context.Background(), 10*time.Second)
			if err := dc.client.ContainerRemove(removeCtx, container.ID, dockercontainer.RemoveOptions{
				Force:         true,
				RemoveVolumes: true,
			}); err != nil {
				dc.logger.WithError(err).WithField("containerID", container.ID[:12]).Error("Failed to remove orphaned container")
				cleanupErrors = append(cleanupErrors, fmt.Errorf("remove container %s: %w", container.ID[:12], err))
			} else {
				removedCount++
				dc.containersRemoved++
			}
			removeCancel()
		}
	}

	if removedCount > 0 {
		dc.logger.WithField("count", removedCount).Info("Cleaned up orphaned containers")
	}

	// Return aggregated error if any cleanup failed
	if len(cleanupErrors) > 0 {
		return fmt.Errorf("cleanup completed with %d errors: %v", len(cleanupErrors), cleanupErrors)
	}

	return nil
}

// ContainerHealthStatus represents the health status of a container
type ContainerHealthStatus struct {
	ContainerID     string
	Status          string
	Healthy         bool
	HealthChecks    map[string]HealthCheckResult
	ResourceUsage   ContainerResourceUsage
	StartTime       time.Time
	LastChecked     time.Time
	ErrorMessages   []string
	WarningMessages []string
}

// HealthCheckResult represents the result of a health check
type HealthCheckResult struct {
	Name        string
	Passed      bool
	Duration    time.Duration
	Message     string
	LastChecked time.Time
}

// ContainerResourceUsage represents container resource usage metrics
type ContainerResourceUsage struct {
	CPUPercent    float64
	MemoryUsageMB int64
	MemoryLimitMB int64
	NetworkRxMB   float64
	NetworkTxMB   float64
	DiskUsageMB   int64
}

// waitForContainerHealth waits for a container to become healthy
func (dc *DockerClient) waitForContainerHealth(containerID string, timeout time.Duration) error {
	startTime := consensus.ConsensusNow()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	for {
		// Check if timeout exceeded
		if consensus.ConsensusSince(startTime) > timeout {
			return fmt.Errorf("timeout waiting for container health")
		}

		// Get container status
		status, err := dc.GetContainerStatus(containerID)
		if err != nil {
			return fmt.Errorf("failed to check container status: %w", err)
		}

		if status == "running" {
			return nil
		}

		// Wait before next check using context-based timer
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(1 * time.Second):
			// Continue to next iteration
		}
	}
}

// performComprehensiveHealthCheck performs a comprehensive health check on a container
func (dc *DockerClient) performComprehensiveHealthCheck(containerID string) ContainerHealthStatus {
	status := ContainerHealthStatus{
		ContainerID:     containerID,
		HealthChecks:    make(map[string]HealthCheckResult),
		StartTime:       consensus.ConsensusNow(),
		LastChecked:     consensus.ConsensusNow(),
		ErrorMessages:   []string{},
		WarningMessages: []string{},
	}

	// Perform various health checks
	dc.checkContainerStatus(&status)
	dc.checkResourceUsage(&status)
	dc.checkNetworkConnectivity(&status)
	dc.checkApplicationHealth(&status)
	dc.analyzeContainerLogs(&status)
	dc.validateContainerConfiguration(&status)

	// Evaluate overall health
	status.Healthy = dc.evaluateOverallHealth(status)

	return status
}

// checkContainerStatus checks the basic status of the container
func (dc *DockerClient) checkContainerStatus(status *ContainerHealthStatus) {
	start := consensus.ConsensusNow()

	containerStatus, err := dc.GetContainerStatus(status.ContainerID)
	if err != nil {
		status.HealthChecks["container_status"] = HealthCheckResult{
			Name:        "Container Status",
			Passed:      false,
			Duration:    consensus.ConsensusSince(start),
			Message:     fmt.Sprintf("Failed to get status: %v", err),
			LastChecked: consensus.ConsensusNow(),
		}
		status.ErrorMessages = append(status.ErrorMessages, err.Error())
		return
	}

	status.Status = containerStatus
	passed := containerStatus == "running"

	status.HealthChecks["container_status"] = HealthCheckResult{
		Name:        "Container Status",
		Passed:      passed,
		Duration:    consensus.ConsensusSince(start),
		Message:     fmt.Sprintf("Container status: %s", containerStatus),
		LastChecked: consensus.ConsensusNow(),
	}

	if !passed {
		status.ErrorMessages = append(status.ErrorMessages, fmt.Sprintf("Container not running: %s", containerStatus))
	}
}

// checkResourceUsage checks the resource usage of the container
func (dc *DockerClient) checkResourceUsage(status *ContainerHealthStatus) {
	start := consensus.ConsensusNow()

	ctx := context.Background()
	stats, err := dc.client.ContainerStats(ctx, status.ContainerID, false)
	if err != nil {
		status.HealthChecks["resource_usage"] = HealthCheckResult{
			Name:        "Resource Usage",
			Passed:      false,
			Duration:    consensus.ConsensusSince(start),
			Message:     fmt.Sprintf("Failed to get stats: %v", err),
			LastChecked: consensus.ConsensusNow(),
		}
		status.ErrorMessages = append(status.ErrorMessages, err.Error())
		return
	}
	defer stats.Body.Close()

	// Parse stats
	var statsData struct {
		CPUStats struct {
			CPUUsage struct {
				TotalUsage  uint64   `json:"total_usage"`
				PercpuUsage []uint64 `json:"percpu_usage"`
			} `json:"cpu_usage"`
			SystemUsage uint64 `json:"system_cpu_usage"`
		} `json:"cpu_stats"`
		PreCPUStats struct {
			CPUUsage struct {
				TotalUsage uint64 `json:"total_usage"`
			} `json:"cpu_usage"`
			SystemUsage uint64 `json:"system_cpu_usage"`
		} `json:"precpu_stats"`
		MemoryStats struct {
			Usage uint64 `json:"usage"`
			Limit uint64 `json:"limit"`
		} `json:"memory_stats"`
		Networks map[string]struct {
			RxBytes uint64 `json:"rx_bytes"`
			TxBytes uint64 `json:"tx_bytes"`
		} `json:"networks"`
	}

	if err := json.NewDecoder(stats.Body).Decode(&statsData); err != nil {
		status.HealthChecks["resource_usage"] = HealthCheckResult{
			Name:        "Resource Usage",
			Passed:      false,
			Duration:    consensus.ConsensusSince(start),
			Message:     fmt.Sprintf("Failed to parse stats: %v", err),
			LastChecked: consensus.ConsensusNow(),
		}
		status.ErrorMessages = append(status.ErrorMessages, err.Error())
		return
	}

	// Calculate CPU percentage
	cpuPercent := calculateCPUPercent(&statsData)
	memoryUsageMB := int64(statsData.MemoryStats.Usage / (1024 * 1024))
	memoryLimitMB := int64(statsData.MemoryStats.Limit / (1024 * 1024))

	status.ResourceUsage = ContainerResourceUsage{
		CPUPercent:    cpuPercent,
		MemoryUsageMB: memoryUsageMB,
		MemoryLimitMB: memoryLimitMB,
	}

	// Check if resource usage is within acceptable limits
	passed := cpuPercent < 80.0 && memoryUsageMB < memoryLimitMB*8/10 // 80% memory threshold

	status.HealthChecks["resource_usage"] = HealthCheckResult{
		Name:        "Resource Usage",
		Passed:      passed,
		Duration:    consensus.ConsensusSince(start),
		Message:     fmt.Sprintf("CPU: %.1f%%, Memory: %dMB/%dMB", cpuPercent, memoryUsageMB, memoryLimitMB),
		LastChecked: consensus.ConsensusNow(),
	}

	if !passed {
		if cpuPercent >= 80.0 {
			status.WarningMessages = append(status.WarningMessages, fmt.Sprintf("High CPU usage: %.1f%%", cpuPercent))
		}
		if memoryUsageMB >= memoryLimitMB*8/10 {
			status.WarningMessages = append(status.WarningMessages, fmt.Sprintf("High memory usage: %dMB", memoryUsageMB))
		}
	}
}

// calculateCPUPercent calculates CPU usage percentage from Docker stats
func calculateCPUPercent(stats *struct {
	CPUStats struct {
		CPUUsage struct {
			TotalUsage  uint64   `json:"total_usage"`
			PercpuUsage []uint64 `json:"percpu_usage"`
		} `json:"cpu_usage"`
		SystemUsage uint64 `json:"system_cpu_usage"`
	} `json:"cpu_stats"`
	PreCPUStats struct {
		CPUUsage struct {
			TotalUsage uint64 `json:"total_usage"`
		} `json:"cpu_usage"`
		SystemUsage uint64 `json:"system_cpu_usage"`
	} `json:"precpu_stats"`
	MemoryStats struct {
		Usage uint64 `json:"usage"`
		Limit uint64 `json:"limit"`
	} `json:"memory_stats"`
	Networks map[string]struct {
		RxBytes uint64 `json:"rx_bytes"`
		TxBytes uint64 `json:"tx_bytes"`
	} `json:"networks"`
}) float64 {
	cpuDelta := float64(stats.CPUStats.CPUUsage.TotalUsage) - float64(stats.PreCPUStats.CPUUsage.TotalUsage)
	systemDelta := float64(stats.CPUStats.SystemUsage) - float64(stats.PreCPUStats.SystemUsage)

	if systemDelta > 0 && cpuDelta > 0 {
		return (cpuDelta / systemDelta) * float64(len(stats.CPUStats.CPUUsage.PercpuUsage)) * 100.0
	}
	return 0.0
}

// checkNetworkConnectivity checks network connectivity
func (dc *DockerClient) checkNetworkConnectivity(status *ContainerHealthStatus) {
	start := consensus.ConsensusNow()

	ctx := context.Background()
	inspect, err := dc.client.ContainerInspect(ctx, status.ContainerID)
	if err != nil {
		status.HealthChecks["network_connectivity"] = HealthCheckResult{
			Name:        "Network Connectivity",
			Passed:      false,
			Duration:    consensus.ConsensusSince(start),
			Message:     fmt.Sprintf("Failed to inspect container: %v", err),
			LastChecked: consensus.ConsensusNow(),
		}
		status.ErrorMessages = append(status.ErrorMessages, err.Error())
		return
	}

	// Check if container is connected to the chaincode network
	connected := false
	for networkName := range inspect.NetworkSettings.Networks {
		if strings.Contains(networkName, "chaincode") {
			connected = true
			break
		}
	}

	status.HealthChecks["network_connectivity"] = HealthCheckResult{
		Name:        "Network Connectivity",
		Passed:      connected,
		Duration:    consensus.ConsensusSince(start),
		Message:     fmt.Sprintf("Connected to chaincode network: %v", connected),
		LastChecked: consensus.ConsensusNow(),
	}

	if !connected {
		status.ErrorMessages = append(status.ErrorMessages, "Container not connected to chaincode network")
	}
}

// checkApplicationHealth checks application-specific health
func (dc *DockerClient) checkApplicationHealth(status *ContainerHealthStatus) {
	start := consensus.ConsensusNow()

	// For now, we'll just check if the container is responding
	// In a real implementation, this would make HTTP requests to health endpoints
	passed := status.Status == "running"

	status.HealthChecks["application_health"] = HealthCheckResult{
		Name:        "Application Health",
		Passed:      passed,
		Duration:    consensus.ConsensusSince(start),
		Message:     "Basic application health check",
		LastChecked: consensus.ConsensusNow(),
	}
}

// analyzeContainerLogs analyzes container logs for errors
func (dc *DockerClient) analyzeContainerLogs(status *ContainerHealthStatus) {
	start := consensus.ConsensusNow()

	logs, err := dc.GetContainerLogs(status.ContainerID, 100)
	if err != nil {
		status.HealthChecks["log_analysis"] = HealthCheckResult{
			Name:        "Log Analysis",
			Passed:      false,
			Duration:    consensus.ConsensusSince(start),
			Message:     fmt.Sprintf("Failed to get logs: %v", err),
			LastChecked: consensus.ConsensusNow(),
		}
		status.ErrorMessages = append(status.ErrorMessages, err.Error())
		return
	}

	// Simple log analysis - count error occurrences
	errorCount := strings.Count(strings.ToLower(logs), "error")
	fatalCount := strings.Count(strings.ToLower(logs), "fatal")

	passed := errorCount < 5 && fatalCount == 0

	status.HealthChecks["log_analysis"] = HealthCheckResult{
		Name:        "Log Analysis",
		Passed:      passed,
		Duration:    consensus.ConsensusSince(start),
		Message:     fmt.Sprintf("Errors: %d, Fatal: %d", errorCount, fatalCount),
		LastChecked: consensus.ConsensusNow(),
	}

	if !passed {
		status.WarningMessages = append(status.WarningMessages, fmt.Sprintf("High error count in logs: %d errors, %d fatal", errorCount, fatalCount))
	}
}

// validateContainerConfiguration validates container configuration
func (dc *DockerClient) validateContainerConfiguration(status *ContainerHealthStatus) {
	start := consensus.ConsensusNow()

	ctx := context.Background()
	inspect, err := dc.client.ContainerInspect(ctx, status.ContainerID)
	if err != nil {
		status.HealthChecks["config_validation"] = HealthCheckResult{
			Name:        "Configuration Validation",
			Passed:      false,
			Duration:    consensus.ConsensusSince(start),
			Message:     fmt.Sprintf("Failed to inspect container: %v", err),
			LastChecked: consensus.ConsensusNow(),
		}
		status.ErrorMessages = append(status.ErrorMessages, err.Error())
		return
	}

	// Check if container has required labels
	hasRequiredLabels := true
	requiredLabels := []string{"diamante.chaincode.id", "diamante.chaincode.type"}

	for _, label := range requiredLabels {
		if _, exists := inspect.Config.Labels[label]; !exists {
			hasRequiredLabels = false
			break
		}
	}

	status.HealthChecks["config_validation"] = HealthCheckResult{
		Name:        "Configuration Validation",
		Passed:      hasRequiredLabels,
		Duration:    consensus.ConsensusSince(start),
		Message:     fmt.Sprintf("Required labels present: %v", hasRequiredLabels),
		LastChecked: consensus.ConsensusNow(),
	}

	if !hasRequiredLabels {
		status.ErrorMessages = append(status.ErrorMessages, "Container missing required labels")
	}
}

// evaluateOverallHealth determines the overall health status
func (dc *DockerClient) evaluateOverallHealth(status ContainerHealthStatus) bool {
	// Critical checks that must pass
	criticalChecks := []string{"container_status", "application_health"}

	for _, checkName := range criticalChecks {
		if check, exists := status.HealthChecks[checkName]; exists && !check.Passed {
			return false
		}
	}

	// Count passed checks
	passedCount := 0
	totalCount := len(status.HealthChecks)

	for _, check := range status.HealthChecks {
		if check.Passed {
			passedCount++
		}
	}

	// Require at least 80% of checks to pass
	healthRatio := float64(passedCount) / float64(totalCount)
	return healthRatio >= 0.8
}

// getDetailedHealthStatus retrieves comprehensive health information
func (dc *DockerClient) getDetailedHealthStatus(containerID string) ContainerHealthStatus {
	return dc.performComprehensiveHealthCheck(containerID)
}

// calculateHealthScore computes a numeric health score (0-100)
func (dc *DockerClient) calculateHealthScore(status ContainerHealthStatus) int {
	if len(status.HealthChecks) == 0 {
		return 0
	}

	passedCount := dc.countPassedChecks(status.HealthChecks)
	baseScore := (passedCount * 100) / len(status.HealthChecks)

	// Deduct points for errors and warnings
	errorPenalty := len(status.ErrorMessages) * 10
	warningPenalty := len(status.WarningMessages) * 2

	score := baseScore - errorPenalty - warningPenalty
	if score < 0 {
		score = 0
	}
	if score > 100 {
		score = 100
	}

	return score
}

// countPassedChecks counts the number of passed health checks
func (dc *DockerClient) countPassedChecks(checks map[string]HealthCheckResult) int {
	count := 0
	for _, check := range checks {
		if check.Passed {
			count++
		}
	}
	return count
}

// DockerClientMetrics represents docker client metrics
type DockerClientMetrics struct {
	ContainerCount    int    `json:"container_count"`
	RunningContainers int    `json:"running_containers"`
	StoppedContainers int    `json:"stopped_containers"`
	FailedContainers  int    `json:"failed_containers"`
	ImageCount        int    `json:"image_count"`
	NetworkCount      int    `json:"network_count"`
	VolumeCount       int    `json:"volume_count"`
	TotalCPU          int64  `json:"total_cpu_usage"`
	TotalMemory       int64  `json:"total_memory_usage_bytes"`
	HealthStatus      string `json:"health_status"`
}

// GetMetrics returns docker client metrics
func (dc *DockerClient) GetMetrics() DockerClientMetrics {
	dc.mu.RLock()
	defer dc.mu.RUnlock()

	// Count containers by state
	running, stopped, failed := 0, 0, 0
	for _, container := range dc.containers {
		switch container.Status {
		case "running":
			running++
		case "exited", "stopped":
			stopped++
		case "failed", "error":
			failed++
		}
	}

	return DockerClientMetrics{
		ContainerCount:    len(dc.containers),
		RunningContainers: running,
		StoppedContainers: stopped,
		FailedContainers:  failed,
		ImageCount:        0,         // Not tracked in current implementation
		NetworkCount:      0,         // Not tracked in current implementation
		VolumeCount:       0,         // Not tracked in current implementation
		TotalCPU:          0,         // Would need container stats API
		TotalMemory:       0,         // Would need container stats API
		HealthStatus:      "healthy", // Simplified health status
	}
}

// Close closes the Docker client
func (dc *DockerClient) Close() error {
	// Stop monitoring
	// Clean up all containers
	dc.mu.Lock()
	containerIDs := make([]string, 0, len(dc.containers))
	for id := range dc.containers {
		containerIDs = append(containerIDs, id)
	}
	dc.mu.Unlock()

	// Stop and remove all containers
	for _, id := range containerIDs {
		if err := dc.StopContainer(id); err != nil {
			dc.logger.WithError(err).Warn("Failed to stop container during cleanup")
		}
		if err := dc.RemoveContainer(id); err != nil {
			dc.logger.WithError(err).Warn("Failed to remove container during cleanup")
		}
	}

	// Close Docker client
	if err := dc.client.Close(); err != nil {
		return fmt.Errorf("failed to close Docker client: %w", err)
	}

	dc.logger.Info("Docker client closed")
	return nil
}

// PruneContainers removes old containers
func (dc *DockerClient) PruneContainers(olderThan time.Duration) (int, error) {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	cutoff := consensus.ConsensusNow().Add(-olderThan)

	ctx := context.Background()
	containers, err := dc.client.ContainerList(ctx, dockercontainer.ListOptions{
		All: true,
		Filters: filters.NewArgs(
			filters.Arg("label", "diamante.chaincode.type=chaincode"),
		),
	})
	if err != nil {
		return 0, fmt.Errorf("failed to list containers: %w", err)
	}

	removedCount := 0
	for _, container := range containers {
		created := time.Unix(container.Created, 0)
		if created.Before(cutoff) {
			if err := dc.RemoveContainer(container.ID); err != nil {
				dc.logger.WithError(err).WithField("containerID", container.ID[:12]).Warn("Failed to remove old container")
				continue
			}
			removedCount++
		}
	}

	return removedCount, nil
}

// RestartContainer restarts a container
func (dc *DockerClient) RestartContainer(containerID string) error {
	ctx := context.Background()

	// The Docker SDK ContainerRestart expects a container.StopOptions with timeout in seconds
	timeout := 10 // 10 seconds
	stopOptions := dockercontainer.StopOptions{
		Timeout: &timeout,
	}

	if err := dc.client.ContainerRestart(ctx, containerID, stopOptions); err != nil {
		return fmt.Errorf("failed to restart container: %w", err)
	}

	// Wait for container to be healthy
	if err := dc.waitForContainerHealth(containerID, 30*time.Second); err != nil {
		return fmt.Errorf("container failed health check after restart: %w", err)
	}

	dc.logger.WithField("containerID", containerID[:12]).Info("Container restarted")
	return nil
}

// GetContainerByChaincode finds a container by chaincode ID
func (dc *DockerClient) GetContainerByChaincode(chaincodeID string) (*ContainerInfo, error) {
	dc.mu.RLock()
	defer dc.mu.RUnlock()

	for _, info := range dc.containers {
		if info.ChaincodeID == chaincodeID {
			return info, nil
		}
	}

	return nil, fmt.Errorf("container not found for chaincode: %s", chaincodeID)
}

// StreamContainerLogs streams container logs
func (dc *DockerClient) StreamContainerLogs(containerID string, follow bool) (io.ReadCloser, error) {
	ctx := context.Background()

	options := dockercontainer.LogsOptions{
		ShowStdout: true,
		ShowStderr: true,
		Follow:     follow,
		Timestamps: true,
	}

	reader, err := dc.client.ContainerLogs(ctx, containerID, options)
	if err != nil {
		return nil, fmt.Errorf("failed to stream container logs: %w", err)
	}

	return reader, nil
}

// ExecInContainer executes a command in a running container
func (dc *DockerClient) ExecInContainer(containerID string, cmd []string) (string, error) {
	ctx := context.Background()

	// Create exec instance
	execConfig := dockercontainer.ExecOptions{
		Cmd:          cmd,
		AttachStdout: true,
		AttachStderr: true,
	}

	execID, err := dc.client.ContainerExecCreate(ctx, containerID, execConfig)
	if err != nil {
		return "", fmt.Errorf("failed to create exec: %w", err)
	}

	// Start exec
	resp, err := dc.client.ContainerExecAttach(ctx, execID.ID, dockercontainer.ExecStartOptions{})
	if err != nil {
		return "", fmt.Errorf("failed to start exec: %w", err)
	}
	defer resp.Close()

	// Read output
	output, err := io.ReadAll(resp.Reader)
	if err != nil {
		return "", fmt.Errorf("failed to read exec output: %w", err)
	}

	// Check exec result
	inspect, err := dc.client.ContainerExecInspect(ctx, execID.ID)
	if err != nil {
		return "", fmt.Errorf("failed to inspect exec: %w", err)
	}

	if inspect.ExitCode != 0 {
		return "", fmt.Errorf("exec failed with exit code %d: %s", inspect.ExitCode, string(output))
	}

	return strings.TrimSpace(string(output)), nil
}
