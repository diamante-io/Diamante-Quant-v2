package crypto

import (
	"testing"
	"time"

	"github.com/sirupsen/logrus"
)

func TestTimeProviderNotFixedEpoch(t *testing.T) {
	// Test that consensusNow() doesn't return a fixed epoch time
	time1 := consensusNow()
	time.Sleep(10 * time.Millisecond)
	time2 := consensusNow()

	if time1.Equal(time2) {
		t.Error("consensusNow() returns fixed time, not advancing")
	}

	// Test that the time is not the fixed epoch (2024-01-01)
	fixedEpoch := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	if time1.Equal(fixedEpoch) {
		t.Error("consensusNow() returns the fixed epoch time (2024-01-01)")
	}

	// Test that consensusUnix() returns reasonable Unix timestamp
	unixTime := consensusUnix()
	// Should be after year 2020
	if unixTime < 1577836800 { // Jan 1, 2020
		t.Errorf("consensusUnix() returns unreasonable timestamp: %d", unixTime)
	}

	// Test that consensusSince() works correctly
	start := consensusNow()
	time.Sleep(50 * time.Millisecond)
	duration := consensusSince(start)

	if duration < 40*time.Millisecond || duration > 100*time.Millisecond {
		t.Errorf("consensusSince() returned unexpected duration: %v", duration)
	}
}

func TestRateLimiterWithRealTime(t *testing.T) {
	// Test that rate limiter works with real time progression
	rl := newRateLimiter(10) // 10 ops per second

	// Should allow initial operations
	allowed := 0
	for range 20 {
		if rl.allow() {
			allowed++
		}
	}

	// Should have allowed approximately 10 operations
	if allowed < 8 || allowed > 12 {
		t.Errorf("Rate limiter allowed %d operations, expected ~10", allowed)
	}

	// Wait for refill
	time.Sleep(200 * time.Millisecond)

	// Should allow more operations after refill
	allowed2 := 0
	for range 5 {
		if rl.allow() {
			allowed2++
		}
	}

	if allowed2 == 0 {
		t.Error("Rate limiter not refilling tokens over time")
	}
}

func TestCryptoManagerKeyRotationTiming(t *testing.T) {
	logger := logrus.New()

	// Create manager with short rotation interval
	cm, err := NewCryptoManager(
		KyberLevel1024,
		DilithiumLevel3,
		logger,
		WithKeyRotation(true, 24*time.Hour),
	)
	if err != nil {
		t.Fatalf("Failed to create CryptoManager: %v", err)
	}

	// Initially, rotation should not be due
	if cm.CheckKeyRotationDue() {
		t.Error("Key rotation should not be due immediately after creation")
	}

	// Wait for rotation interval
	time.Sleep(150 * time.Millisecond)

	// Now rotation should be due
	if !cm.CheckKeyRotationDue() {
		t.Error("Key rotation should be due after interval has passed")
	}

	// Perform rotation
	if err := cm.RotateKeys(); err != nil {
		t.Fatalf("Key rotation failed: %v", err)
	}

	// After rotation, it should not be due again immediately
	if cm.CheckKeyRotationDue() {
		t.Error("Key rotation should not be due immediately after rotation")
	}
}
