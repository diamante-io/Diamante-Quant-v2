package ledger

import (
	"testing"
	"time"

	"diamante/common"
)

// fakeDilithiumPubKey returns a dummy public key of size 1952 bytes (Dilithium mode3).
func fakeDilithiumPubKey() []byte {
	return make([]byte, 1952) // all zeros, but correct length
}

// fakeSignature returns a minimal, non-empty byte slice to pass "non-empty" checks.
func fakeSignature() []byte {
	// This doesn't have to be large; just not empty
	return []byte{0x01, 0x02, 0x03}
}

func TestLedger_NewLedger(t *testing.T) {
	l := NewLedger()
	if l == nil {
		t.Fatal("expected a non-nil ledger instance")
	}
}

func TestLedger_CreateAccount(t *testing.T) {
	l := NewLedger()

	acc := &common.Account{
		ID:        "testAccount",
		Balance:   50.0,
		PublicKey: fakeDilithiumPubKey(),
	}
	err := l.CreateAccount(acc)
	if err != nil {
		t.Fatalf("CreateAccount failed: %v", err)
	}

	// Try creating the same account => should fail
	err2 := l.CreateAccount(acc)
	if err2 == nil {
		t.Fatal("expected error for creating duplicate account, got nil")
	}
}

func TestLedger_UpdateAccount(t *testing.T) {
	l := NewLedger()

	acc := &common.Account{
		ID:        "updateMe",
		Balance:   100.0,
		PublicKey: fakeDilithiumPubKey(),
	}
	if err := l.CreateAccount(acc); err != nil {
		t.Fatalf("CreateAccount failed: %v", err)
	}
	// Modify the account
	acc.Balance = 200.0
	err := l.UpdateAccount(acc)
	if err != nil {
		t.Fatalf("UpdateAccount failed: %v", err)
	}

	// Try updating a non-existent account
	fakeAcc := &common.Account{ID: "nonExistent", PublicKey: fakeDilithiumPubKey()}
	err2 := l.UpdateAccount(fakeAcc)
	if err2 == nil {
		t.Fatal("expected error for non-existent account, got nil")
	}
}

func TestLedger_GetBalance(t *testing.T) {
	l := NewLedger()

	acc := &common.Account{
		ID:        "balCheck",
		Balance:   75.0,
		PublicKey: fakeDilithiumPubKey(),
	}
	if err := l.CreateAccount(acc); err != nil {
		t.Fatalf("CreateAccount failed: %v", err)
	}
	bal, err := l.GetBalance("balCheck")
	if err != nil {
		t.Fatalf("GetBalance error: %v", err)
	}
	if bal != 75.0 {
		t.Fatalf("expected 75.0, got %.2f", bal)
	}
}

func TestLedger_UpdateAccountBalance(t *testing.T) {
	l := NewLedger()

	acc := &common.Account{
		ID:        "updateBal",
		Balance:   20.0,
		PublicKey: fakeDilithiumPubKey(),
	}
	if err := l.CreateAccount(acc); err != nil {
		t.Fatalf("CreateAccount failed: %v", err)
	}
	// Add 15 => new total 35
	err := l.UpdateAccountBalance("updateBal", 15)
	if err != nil {
		t.Fatalf("UpdateAccountBalance failed: %v", err)
	}
	newBal, _ := l.GetBalance("updateBal")
	if newBal != 35.0 {
		t.Fatalf("expected 35.0, got %.2f", newBal)
	}
}

func TestLedger_AddTransaction(t *testing.T) {
	l := NewLedger()

	sender := &common.Account{
		ID:        "sender",
		Balance:   100.0,
		PublicKey: fakeDilithiumPubKey(),
	}
	receiver := &common.Account{
		ID:        "receiver",
		Balance:   10.0,
		PublicKey: fakeDilithiumPubKey(),
	}
	_ = l.CreateAccount(sender)
	_ = l.CreateAccount(receiver)

	tx := common.Transaction{
		ID:        "tx-add-1",
		Sender:    "sender",
		Receiver:  "receiver",
		Amount:    40.0,
		Fee:       1.0,
		Signature: fakeSignature(), // minimal dummy signature
	}
	err := l.AddTransaction(tx)
	if err != nil {
		t.Fatalf("AddTransaction failed unexpectedly: %v", err)
	}

	// Check if ledger stored it
	if !l.IsTransactionCommitted("tx-add-1") {
		t.Fatal("expected transaction to be in ledger, but IsTransactionCommitted returned false")
	}
}

func TestLedger_IsTransactionCommitted(t *testing.T) {
	l := NewLedger()

	acc1 := &common.Account{
		ID:        "sender2",
		Balance:   50.0,
		PublicKey: fakeDilithiumPubKey(),
	}
	acc2 := &common.Account{
		ID:        "receiver2",
		Balance:   5.0,
		PublicKey: fakeDilithiumPubKey(),
	}
	_ = l.CreateAccount(acc1)
	_ = l.CreateAccount(acc2)

	tx := common.Transaction{
		ID:        "tx-check-1",
		Sender:    "sender2",
		Receiver:  "receiver2",
		Amount:    10.0,
		Fee:       0.5,
		Signature: fakeSignature(),
	}
	// Not yet added => must be false
	if l.IsTransactionCommitted("tx-check-1") {
		t.Fatal("expected false, got true")
	}
	// Add it
	err := l.AddTransaction(tx)
	if err != nil {
		t.Fatalf("AddTransaction failed: %v", err)
	}
	// Now must be true
	if !l.IsTransactionCommitted("tx-check-1") {
		t.Fatal("expected true after adding transaction, got false")
	}
}

func TestLedger_CommitBlock(t *testing.T) {
	l := NewLedger()

	// Create two accounts
	blockSender := &common.Account{
		ID:        "blockSender",
		Balance:   200.0,
		PublicKey: fakeDilithiumPubKey(),
	}
	blockReceiver := &common.Account{
		ID:        "blockReceiver",
		Balance:   20.0,
		PublicKey: fakeDilithiumPubKey(),
	}
	_ = l.CreateAccount(blockSender)
	_ = l.CreateAccount(blockReceiver)

	// create a valid transaction so it doesn't fail signature checks
	tx := common.Transaction{
		ID:        "btx1",
		Sender:    "blockSender",
		Receiver:  "blockReceiver",
		Amount:    50.0,
		Fee:       1.0,
		Signature: fakeSignature(),
	}
	errTx := l.AddTransaction(tx)
	if errTx != nil {
		t.Fatalf("AddTransaction failed: %v", errTx)
	}

	block := common.Block{
		Number:       1,
		PreviousHash: "some-genesis-hash",
		// Include our transaction
		Transactions: []common.Transaction{tx},
		Timestamp:    time.Now().Unix(),
		Hash:         "hash-of-this-block", // minimal; let ledger recompute
	}

	// The ledger will do re-check, so let's fix the block's Hash to match common.ComputeBlockHash
	computed := common.ComputeBlockHash(block)
	block.Hash = computed

	err := l.CommitBlock(block)
	if err != nil {
		t.Fatalf("unexpected error committing block: %v", err)
	}

	// Now the ledger must have block #1
	if l.currentHeight != 1 {
		t.Fatalf("expected currentHeight=1, got %d", l.currentHeight)
	}
}

func TestLedger_GetLastBlockHash(t *testing.T) {
	l := NewLedger()

	// no block => must fail
	_, err0 := l.GetLastBlockHash()
	if err0 == nil {
		t.Fatal("expected error for no blocks, got nil")
	}

	// commit 1 block => last block #1
	// create accounts so transaction passes
	acc1 := &common.Account{
		ID:        "gblh-sender",
		Balance:   200.0,
		PublicKey: fakeDilithiumPubKey(),
	}
	acc2 := &common.Account{
		ID:        "gblh-recv",
		Balance:   10.0,
		PublicKey: fakeDilithiumPubKey(),
	}
	_ = l.CreateAccount(acc1)
	_ = l.CreateAccount(acc2)

	tx := common.Transaction{
		ID:        "gblh-tx1",
		Sender:    "gblh-sender",
		Receiver:  "gblh-recv",
		Amount:    20.0,
		Fee:       0.5,
		Signature: fakeSignature(),
	}
	if errAdd := l.AddTransaction(tx); errAdd != nil {
		t.Fatalf("AddTransaction failed: %v", errAdd)
	}
	block := common.Block{
		Number:       1,
		PreviousHash: "some-hash",
		Transactions: []common.Transaction{tx},
		Timestamp:    time.Now().Unix(),
	}
	// compute + set final block.Hash
	block.Hash = common.ComputeBlockHash(block)
	if errCommit := l.CommitBlock(block); errCommit != nil {
		t.Fatalf("CommitBlock error: %v", errCommit)
	}

	lastHash, errLast := l.GetLastBlockHash()
	if errLast != nil {
		t.Fatalf("unexpected error: %v", errLast)
	}
	if lastHash != block.Hash {
		t.Fatalf("expected lastHash=%s, got %s", block.Hash, lastHash)
	}
}

func TestLedger_GetBlockByNumber(t *testing.T) {
	l := NewLedger()

	// Commit block #1
	acS := &common.Account{
		ID:        "blockNumSender",
		Balance:   99.0,
		PublicKey: fakeDilithiumPubKey(),
	}
	acR := &common.Account{
		ID:        "blockNumReceiver",
		Balance:   1.0,
		PublicKey: fakeDilithiumPubKey(),
	}
	_ = l.CreateAccount(acS)
	_ = l.CreateAccount(acR)

	tx := common.Transaction{
		ID:        "gblock-1",
		Sender:    "blockNumSender",
		Receiver:  "blockNumReceiver",
		Amount:    10.0,
		Fee:       0.1,
		Signature: fakeSignature(),
	}
	_ = l.AddTransaction(tx)

	block := common.Block{
		Number:       1,
		PreviousHash: "blockNum0",
		Transactions: []common.Transaction{tx},
		Timestamp:    time.Now().Unix(),
	}
	block.Hash = common.ComputeBlockHash(block)
	if err := l.CommitBlock(block); err != nil {
		t.Fatalf("CommitBlock error: %v", err)
	}

	// now we can get block #1
	got, found := l.GetBlockByNumber(1)
	if !found {
		t.Fatal("expected block #1 to exist, got false")
	}
	if got.Number != 1 {
		t.Fatalf("mismatch: expected block number=1, got %d", got.Number)
	}

	// block #2 doesn’t exist => found==false
	_, found2 := l.GetBlockByNumber(2)
	if found2 {
		t.Fatal("expected block #2 not to exist")
	}
}

func TestLedger_CreateSnapshotAndRestore(t *testing.T) {
	l := NewLedger()

	// create an account
	acc := &common.Account{
		ID:        "snapAcc",
		Balance:   999.0,
		PublicKey: fakeDilithiumPubKey(),
	}
	if err := l.CreateAccount(acc); err != nil {
		t.Fatalf("CreateAccount error: %v", err)
	}

	// Must commit a block #1 so snapshot(1) is valid
	block := common.Block{
		Number:       1,
		PreviousHash: "genesis",
		Timestamp:    time.Now().Unix(),
		Hash:         "fake-hash-block1",
	}
	// Let ledger compute final hash
	block.Hash = common.ComputeBlockHash(block)
	if errCb := l.CommitBlock(block); errCb != nil {
		t.Fatalf("commit block #1 failed: %v", errCb)
	}

	// Now create snapshot at height=1
	errSnap := l.CreateSnapshot(1)
	if errSnap != nil {
		t.Fatalf("snapshot at 'height=1' failed: %v", errSnap)
	}

	// Adjust the account => e.g. subtract 100
	errBal := l.UpdateAccountBalance("snapAcc", -100)
	if errBal != nil {
		t.Fatalf("failed to update snapAcc -100: %v", errBal)
	}
	newBal, _ := l.GetBalance("snapAcc")
	if newBal != 899.0 {
		t.Fatalf("expected 899 after sub 100, got %.2f", newBal)
	}

	// Restore snapshot => expect revert
	errRestore := l.RestoreSnapshot(1)
	if errRestore != nil {
		t.Fatalf("RestoreSnapshot(1) error: %v", errRestore)
	}
	balRest, _ := l.GetBalance("snapAcc")
	if balRest != 999.0 {
		t.Fatalf("expected snapAcc=999 after restore, got %.2f", balRest)
	}
}

func TestLedger_IntegrityCheck(t *testing.T) {
	l := NewLedger()
	// By default, l.checkpoints is empty => no error
	if err := l.IntegrityCheck(); err != nil {
		t.Fatalf("IntegrityCheck failed: %v", err)
	}

	// Let’s commit a block => get a checkpoint
	acc := &common.Account{
		ID:        "intCheckSender",
		Balance:   500.0,
		PublicKey: fakeDilithiumPubKey(),
	}
	l.CreateAccount(acc)

	block := common.Block{
		Number:       1,
		PreviousHash: "xyz",
		Timestamp:    time.Now().Unix(),
	}
	block.Hash = common.ComputeBlockHash(block)
	if errCb := l.CommitBlock(block); errCb != nil {
		t.Fatalf("commit block error: %v", errCb)
	}

	// If ledger code updates checkpoint => now IntegrityCheck sees a single checkpoint
	if errCheck := l.IntegrityCheck(); errCheck != nil {
		t.Fatalf("IntegrityCheck failed after block #1: %v", errCheck)
	}
}
