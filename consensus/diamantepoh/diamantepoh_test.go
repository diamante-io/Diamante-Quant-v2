//consensus/diamantepoh/diamantepoh_test.go

package diamantepoh_test

import (
	"bytes"
	"crypto/sha256"
	"testing"
	"time"

	"diamante/consensus/diamantepoh"
)

type mockLogger struct{}

func (m *mockLogger) Info(msg string, keyvals ...interface{})  {}
func (m *mockLogger) Error(msg string, keyvals ...interface{}) {}

func TestPoH_Tick(t *testing.T) {
	logger := &mockLogger{}
	initial := sha256.Sum256([]byte("init"))
	poh := diamantepoh.NewPoH(initial, 10*time.Millisecond, logger)

	time.Sleep(15 * time.Millisecond) // Enough time to exceed 10ms
	poh.Tick()                        // Should advance state once
	if poh.GetCount() == 0 {
		t.Errorf("expected PoH count to increment after tick")
	}
}

func TestPoH_SetTickDelay(t *testing.T) {
	poh := diamantepoh.NewPoH([32]byte{}, 10*time.Millisecond, nil)

	err := poh.SetTickDelay(0)
	if err == nil {
		t.Error("expected error on zero/negative tick delay, got nil")
	}

	if err2 := poh.SetTickDelay(20 * time.Millisecond); err2 != nil {
		t.Errorf("unexpected error: %v", err2)
	}
	if poh.GetTickDelay() != 20*time.Millisecond {
		t.Errorf("expected tickDelay=20ms, got %v", poh.GetTickDelay())
	}
}

func TestPoH_Record(t *testing.T) {
	logger := &mockLogger{}
	poh := diamantepoh.NewPoH([32]byte{}, 1*time.Second, logger)

	data := []byte("hello")
	proof := poh.Record(data)
	if bytes.Equal(proof[:], make([]byte, 32)) {
		t.Error("expected a non-zero proof after Record")
	}
	if poh.GetCount() != 1 {
		t.Errorf("expected count=1 after 1 record, got %d", poh.GetCount())
	}
}

func TestPoH_Verify(t *testing.T) {
	logger := &mockLogger{}
	poh := diamantepoh.NewPoH([32]byte{}, time.Second, logger)

	// Record some data
	data := []byte("verify me")
	prevState := poh.GetState()
	proof := poh.Record(data)
	count := poh.GetCount()

	if !poh.Verify(prevState, data, proof, count) {
		t.Error("expected verification to succeed, got false")
	}
}

func TestPoH_Synchronize(t *testing.T) {
	logger := &mockLogger{}
	poh := diamantepoh.NewPoH([32]byte{}, 10*time.Millisecond, logger)

	// Advance it a bit
	poh.AdvanceState(10)
	oldCount := poh.GetCount()

	targetCount := oldCount + 500
	syncState := sha256.Sum256([]byte("syncTest"))
	if err := poh.Synchronize(syncState, targetCount); err != nil {
		t.Errorf("unexpected error in Synchronize: %v", err)
	}

	if poh.GetCount() != targetCount {
		t.Errorf("expected count=%d, got %d", targetCount, poh.GetCount())
	}

	if poh.GetState() != syncState {
		t.Errorf("poh state mismatch after sync")
	}
}

func TestPoH_GenerateProof(t *testing.T) {
	logger := &mockLogger{}
	poh := diamantepoh.NewPoH([32]byte{}, time.Millisecond, logger)

	data := []byte("some data")
	proof, startState, startCount, err := poh.GenerateProof(data, 100)
	if err != nil {
		t.Fatalf("GenerateProof error: %v", err)
	}
	if bytes.Equal(proof[:], make([]byte, 32)) {
		t.Error("expected non-zero proof")
	}
	if startCount != 0 {
		t.Errorf("expected startCount=0, got %d", startCount)
	}
	if startState == ([32]byte{}) {
		t.Error("expected non-empty startState")
	}
}

func TestPoH_VerifyProof(t *testing.T) {
	logger := &mockLogger{}
	poh := diamantepoh.NewPoH([32]byte{}, time.Millisecond, logger)

	data := []byte("some data for proof")
	proof, startSt, startCnt, err := poh.GenerateProof(data, 50)
	if err != nil {
		t.Fatalf("GenerateProof error: %v", err)
	}

	ok, verifyErr := poh.VerifyProof(startSt, data, proof, startCnt, 50)
	if verifyErr != nil {
		t.Errorf("VerifyProof returned an error: %v", verifyErr)
	}
	if !ok {
		t.Error("expected proof verification to succeed, got false")
	}
}

func TestPoH_BatchRecord(t *testing.T) {
	logger := &mockLogger{}
	poh := diamantepoh.NewPoH([32]byte{}, time.Millisecond, logger)

	dataBatch := [][]byte{
		[]byte("one"),
		[]byte("two"),
		[]byte("three"),
	}
	hashes := poh.BatchRecord(dataBatch)
	if len(hashes) != 3 {
		t.Fatalf("expected 3 returned hashes, got %d", len(hashes))
	}
	if poh.GetCount() != 3 {
		t.Errorf("expected count=3 after batch record, got %d", poh.GetCount())
	}
}

func TestPoH_EstimateTime(t *testing.T) {
	poh := diamantepoh.NewPoH([32]byte{}, 10*time.Millisecond, nil)
	poh.AdvanceState(5)
	// Count=5
	d, err := poh.EstimateTime(10) // 10 - 5 => 5
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if d < 40*time.Millisecond || d > 70*time.Millisecond {
		t.Errorf("expected ~50ms, got %v", d)
	}

	// If targetCount < current, error
	if _, err2 := poh.EstimateTime(3); err2 == nil {
		t.Error("expected error with targetCount < current count, got nil")
	}
}

func TestPoH_RecoverFromError(t *testing.T) {
	logger := &mockLogger{}
	poh := diamantepoh.NewPoH([32]byte{}, 10*time.Millisecond, logger)

	poh.AdvanceState(10)
	curCount := poh.GetCount()

	lastKnownState := poh.GetState()
	lastKnownCount := curCount - 5
	if err := poh.RecoverFromError(lastKnownState, lastKnownCount); err != nil {
		t.Fatalf("RecoverFromError error: %v", err)
	}
	if poh.GetCount() != lastKnownCount {
		t.Errorf("expected count=%d after recovery, got %d", lastKnownCount, poh.GetCount())
	}

	// Attempt to recover to future count => error
	if err2 := poh.RecoverFromError(lastKnownState, lastKnownCount+10); err2 == nil {
		t.Error("expected error recovering to future count, got nil")
	}
}
