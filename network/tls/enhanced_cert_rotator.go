package tls

import (
	"fmt"
	"net"
	"sync"
	"time"

	"diamante/common"

	"github.com/sirupsen/logrus"
)

// EnhancedCertRotator provides automatic certificate rotation with enhanced features
type EnhancedCertRotator struct {
	caManager   *EnhancedCAManager
	nodeID      string
	interval    time.Duration
	ipAddresses []net.IP
	dnsNames    []string
	logger      *logrus.Logger
	onRotation  func(*CertInfo)

	// Rotation control
	stopChan  chan struct{}
	ticker    *time.Ticker
	mu        sync.RWMutex
	isRunning bool

	// Rotation metrics
	rotationCount     int64
	lastRotation      time.Time
	nextRotation      time.Time
	rotationErrors    int64
	lastRotationError error
}

// EnhancedCertRotatorConfig holds configuration for the certificate rotator
type EnhancedCertRotatorConfig struct {
	CAManager   *EnhancedCAManager
	NodeID      string
	Interval    time.Duration
	IPAddresses []net.IP
	DNSNames    []string
	Logger      *logrus.Logger
	OnRotation  func(*CertInfo)
}

// NewEnhancedCertRotator creates a new enhanced certificate rotator
func NewEnhancedCertRotator(config *EnhancedCertRotatorConfig) *EnhancedCertRotator {
	if config == nil {
		return nil
	}

	if config.Logger == nil {
		config.Logger = logrus.New()
	}

	if config.Interval == 0 {
		config.Interval = 24 * time.Hour // Default to daily rotation
	}

	return &EnhancedCertRotator{
		caManager:   config.CAManager,
		nodeID:      config.NodeID,
		interval:    config.Interval,
		ipAddresses: config.IPAddresses,
		dnsNames:    config.DNSNames,
		logger:      config.Logger,
		onRotation:  config.OnRotation,
		stopChan:    make(chan struct{}),
	}
}

// Start begins the certificate rotation process
func (r *EnhancedCertRotator) Start() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.isRunning {
		return fmt.Errorf("certificate rotator is already running")
	}

	if r.caManager == nil {
		return fmt.Errorf("CA manager is required for certificate rotation")
	}

	if r.nodeID == "" {
		return fmt.Errorf("node ID is required for certificate rotation")
	}

	r.isRunning = true
	r.nextRotation = common.ConsensusNow().Add(r.interval)
	r.ticker = time.NewTicker(r.interval)

	// Perform initial rotation if no certificate exists
	if _, exists := r.caManager.GetCertificateInfo(r.nodeID); !exists {
		r.logger.WithField("node_id", r.nodeID).Info("Performing initial certificate generation")
		if err := r.rotateCertificate(); err != nil {
			r.logger.WithError(err).Error("Failed to perform initial certificate generation")
			r.rotationErrors++
			r.lastRotationError = err
		}
	}

	// Start the rotation goroutine
	go r.rotationLoop()

	r.logger.WithFields(logrus.Fields{
		"node_id":       r.nodeID,
		"interval":      r.interval,
		"next_rotation": r.nextRotation,
	}).Info("Certificate rotator started")

	return nil
}

// Stop stops the certificate rotation process
func (r *EnhancedCertRotator) Stop() {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.isRunning {
		return
	}

	r.isRunning = false
	close(r.stopChan)

	if r.ticker != nil {
		r.ticker.Stop()
		r.ticker = nil
	}

	r.logger.WithField("node_id", r.nodeID).Info("Certificate rotator stopped")
}

// rotationLoop runs the certificate rotation timer
func (r *EnhancedCertRotator) rotationLoop() {
	for {
		select {
		case <-r.ticker.C:
			r.logger.WithField("node_id", r.nodeID).Info("Performing scheduled certificate rotation")
			if err := r.rotateCertificate(); err != nil {
				r.logger.WithError(err).Error("Scheduled certificate rotation failed")
				r.mu.Lock()
				r.rotationErrors++
				r.lastRotationError = err
				r.mu.Unlock()
			}

		case <-r.stopChan:
			r.logger.WithField("node_id", r.nodeID).Debug("Certificate rotation loop stopped")
			return
		}
	}
}

// rotateCertificate performs the actual certificate rotation
func (r *EnhancedCertRotator) rotateCertificate() error {
	start := common.ConsensusNow()

	// Generate new certificate
	certInfo, err := r.caManager.GenerateNodeCertificate(r.nodeID, r.ipAddresses, r.dnsNames)
	if err != nil {
		return fmt.Errorf("failed to generate new certificate: %w", err)
	}

	// Update rotation metrics
	r.mu.Lock()
	r.rotationCount++
	r.lastRotation = common.ConsensusNow()
	r.nextRotation = r.lastRotation.Add(r.interval)
	r.mu.Unlock()

	duration := time.Since(start)

	r.logger.WithFields(logrus.Fields{
		"node_id":     r.nodeID,
		"serial":      certInfo.SerialNum,
		"expires":     certInfo.ExpiresAt,
		"duration":    duration,
		"fingerprint": certInfo.Fingerprint[:16] + "...",
	}).Info("Certificate rotated successfully")

	// Call rotation callback if provided
	if r.onRotation != nil {
		r.onRotation(certInfo)
	}

	return nil
}

// ForceRotation forces an immediate certificate rotation
func (r *EnhancedCertRotator) ForceRotation() error {
	r.mu.RLock()
	if !r.isRunning {
		r.mu.RUnlock()
		return fmt.Errorf("certificate rotator is not running")
	}
	r.mu.RUnlock()

	r.logger.WithField("node_id", r.nodeID).Info("Forcing certificate rotation")
	return r.rotateCertificate()
}

// GetRotationMetrics returns metrics about certificate rotation
func (r *EnhancedCertRotator) GetRotationMetrics() map[string]interface{} {
	r.mu.RLock()
	defer r.mu.RUnlock()

	metrics := map[string]interface{}{
		"node_id":         r.nodeID,
		"is_running":      r.isRunning,
		"interval":        r.interval.String(),
		"rotation_count":  r.rotationCount,
		"rotation_errors": r.rotationErrors,
		"last_rotation":   r.lastRotation,
		"next_rotation":   r.nextRotation,
	}

	if r.lastRotationError != nil {
		metrics["last_error"] = r.lastRotationError.Error()
	}

	// Add current certificate info if available
	if r.caManager != nil {
		if certInfo, exists := r.caManager.GetCertificateInfo(r.nodeID); exists {
			metrics["current_cert_serial"] = certInfo.SerialNum
			metrics["current_cert_expires"] = certInfo.ExpiresAt
			metrics["current_cert_fingerprint"] = certInfo.Fingerprint

			// Calculate time until expiration
			timeUntilExpiry := time.Until(certInfo.ExpiresAt)
			metrics["time_until_expiry"] = timeUntilExpiry.String()
			metrics["expires_soon"] = timeUntilExpiry < 7*24*time.Hour // Expires within 7 days
		}
	}

	return metrics
}

// UpdateConfiguration updates the rotator configuration
func (r *EnhancedCertRotator) UpdateConfiguration(interval time.Duration, ipAddresses []net.IP, dnsNames []string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if interval > 0 && interval != r.interval {
		r.interval = interval
		r.nextRotation = common.ConsensusNow().Add(interval)

		// Update ticker if running
		if r.isRunning && r.ticker != nil {
			r.ticker.Stop()
			r.ticker = time.NewTicker(interval)
		}

		r.logger.WithFields(logrus.Fields{
			"node_id":       r.nodeID,
			"new_interval":  interval,
			"next_rotation": r.nextRotation,
		}).Info("Certificate rotation interval updated")
	}

	if ipAddresses != nil {
		r.ipAddresses = ipAddresses
		r.logger.WithFields(logrus.Fields{
			"node_id":  r.nodeID,
			"ip_count": len(ipAddresses),
		}).Info("Certificate IP addresses updated")
	}

	if dnsNames != nil {
		r.dnsNames = dnsNames
		r.logger.WithFields(logrus.Fields{
			"node_id":   r.nodeID,
			"dns_count": len(dnsNames),
		}).Info("Certificate DNS names updated")
	}

	return nil
}

// IsRunning returns whether the rotator is currently running
func (r *EnhancedCertRotator) IsRunning() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.isRunning
}

// GetNextRotationTime returns when the next rotation is scheduled
func (r *EnhancedCertRotator) GetNextRotationTime() time.Time {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.nextRotation
}

// GetRotationInterval returns the current rotation interval
func (r *EnhancedCertRotator) GetRotationInterval() time.Duration {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.interval
}

// CheckCertificateExpiry checks if the current certificate is expiring soon
func (r *EnhancedCertRotator) CheckCertificateExpiry(threshold time.Duration) (bool, time.Duration, error) {
	if r.caManager == nil {
		return false, 0, fmt.Errorf("CA manager not available")
	}

	certInfo, exists := r.caManager.GetCertificateInfo(r.nodeID)
	if !exists {
		return false, 0, fmt.Errorf("certificate not found for node %s", r.nodeID)
	}

	timeUntilExpiry := time.Until(certInfo.ExpiresAt)
	isExpiringSoon := timeUntilExpiry < threshold

	if isExpiringSoon {
		r.logger.WithFields(logrus.Fields{
			"node_id":           r.nodeID,
			"expires":           certInfo.ExpiresAt,
			"time_until_expiry": timeUntilExpiry,
			"threshold":         threshold,
		}).Warn("Certificate is expiring soon")
	}

	return isExpiringSoon, timeUntilExpiry, nil
}

// RotateIfExpiringSoon rotates the certificate if it's expiring within the threshold
func (r *EnhancedCertRotator) RotateIfExpiringSoon(threshold time.Duration) error {
	isExpiring, timeUntilExpiry, err := r.CheckCertificateExpiry(threshold)
	if err != nil {
		return err
	}

	if isExpiring {
		r.logger.WithFields(logrus.Fields{
			"node_id":           r.nodeID,
			"time_until_expiry": timeUntilExpiry,
			"threshold":         threshold,
		}).Info("Certificate is expiring soon, forcing rotation")

		return r.ForceRotation()
	}

	return nil
}

// GetRotationHistory returns a summary of recent rotations
func (r *EnhancedCertRotator) GetRotationHistory() map[string]interface{} {
	r.mu.RLock()
	defer r.mu.RUnlock()

	history := map[string]interface{}{
		"total_rotations":   r.rotationCount,
		"total_errors":      r.rotationErrors,
		"last_rotation":     r.lastRotation,
		"next_rotation":     r.nextRotation,
		"rotation_interval": r.interval.String(),
	}

	if r.rotationCount > 0 {
		successRate := float64(r.rotationCount-r.rotationErrors) / float64(r.rotationCount) * 100
		history["success_rate"] = fmt.Sprintf("%.2f%%", successRate)
	}

	if r.lastRotationError != nil {
		history["last_error"] = r.lastRotationError.Error()
	}

	return history
}
