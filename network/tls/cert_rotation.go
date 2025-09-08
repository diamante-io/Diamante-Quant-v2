package tls

import (
	"time"

	"github.com/sirupsen/logrus"
)

// CertRotator periodically rotates node certificates using the provided CAManager.
type CertRotator struct {
	Manager  *CAManager
	Interval time.Duration
	NodeID   string
	CertPath string
	KeyPath  string
	stopChan chan struct{}
}

// Rotate triggers a single certificate rotation.
func (r *CertRotator) Rotate() {
	if r.Manager == nil {
		return
	}
	if err := r.Manager.GenerateNodeCert(r.NodeID, r.CertPath, r.KeyPath); err != nil {
		// Log the error but don't interrupt the rotation loop
		// This is expected behavior as cert rotation should be resilient
		logrus.WithError(err).WithFields(logrus.Fields{
			"nodeID":   r.NodeID,
			"certPath": r.CertPath,
			"keyPath":  r.KeyPath,
		}).Warn("Certificate rotation failed, will retry on next interval")
	}
}

// Start begins periodic certificate rotation.
func (r *CertRotator) Start() {
	if r.Interval == 0 || r.Manager == nil {
		return
	}
	r.stopChan = make(chan struct{})
	r.Rotate()
	go r.loop()
}

// Stop halts certificate rotation.
func (r *CertRotator) Stop() {
	if r.stopChan != nil {
		close(r.stopChan)
	}
}

func (r *CertRotator) loop() {
	ticker := time.NewTicker(r.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			r.Rotate()
		case <-r.stopChan:
			return
		}
	}
}
