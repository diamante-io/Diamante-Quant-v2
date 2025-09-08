package ledger

import (
	"context"
	"testing"
	"time"

	"diamante/common"
	"diamante/storage"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// setupTestMongoDB sets up a MongoDB connection for testing
func setupTestMongoDB(t *testing.T) *storage.MongoLedger {
	// Use a test-specific database name to avoid conflicts
	mongoURI := "mongodb://localhost:27017"
	dbName := "diamante_test_" + time.Now().Format("20060102150405")

	// Try to connect to MongoDB
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		t.Skipf("Skipping MongoDB tests: %v", err)
		return nil
	}

	// Check if MongoDB is actually running
	err = client.Ping(ctx, nil)
	if err != nil {
		t.Skipf("Skipping MongoDB tests: MongoDB not available: %v", err)
		return nil
	}

	// Create a new MongoLedger
	ledger, err := storage.NewMongoLedger(mongoURI, dbName)
	if err != nil {
		t.Fatalf("Failed to create MongoLedger: %v", err)
	}

	// Register cleanup to drop the test database
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		client.Database(dbName).Drop(ctx)
		client.Disconnect(ctx)
	})

	return ledger
}

// fakeDilithiumPubKey returns a dummy public key of length 1952 bytes (typical for Dilithium mode3).
func fakeDilithiumPubKey() []byte {
	return make([]byte, 1952)
}

// fakeSignature returns a minimal non-empty signature (which our ledger code treats specially).
func fakeSignature() []byte {
	return []byte{0x01, 0x02, 0x03}
}

func TestMongoLedger_NewLedger(t *testing.T) {
	// Skip if MongoDB is not available
	ldb := setupTestMongoDB(t)
	if ldb == nil {
		t.Skip("MongoDB not available")
	}
}

func TestMongoLedger_CreateAccount(t *testing.T) {
	// Skip if MongoDB is not available
	ldb := setupTestMongoDB(t)
	if ldb == nil {
		t.Skip("MongoDB not available")
	}

	acc := &common.Account{
		ID:        "testAccount",
		Balance:   50.0,
		PublicKey: fakeDilithiumPubKey(),
	}
	if err := ldb.CreateAccount(acc); err != nil {
		t.Fatalf("CreateAccount failed: %v", err)
	}
	// Attempt to create the same account again; should return an error.
	if err := ldb.CreateAccount(acc); err == nil {
		t.Fatal("expected error when creating duplicate account, got nil")
	}
}

func TestMongoLedger_UpdateAccount(t *testing.T) {
	// Skip if MongoDB is not available
	ldb := setupTestMongoDB(t)
	if ldb == nil {
		t.Skip("MongoDB not available")
	}

	acc := &common.Account{
		ID:        "updateMe",
		Balance:   100.0,
		PublicKey: fakeDilithiumPubKey(),
	}
	if err := ldb.CreateAccount(acc); err != nil {
		t.Fatalf("CreateAccount failed: %v", err)
	}
	// Modify the account's balance.
	acc.Balance = 200.0
	if err := ldb.UpdateAccount(acc); err != nil {
		t.Fatalf("UpdateAccount failed: %v", err)
	}
	// Try updating a non-existent account.
	fakeAcc := &common.Account{ID: "nonExistent", PublicKey: fakeDilithiumPubKey()}
	if err := ldb.UpdateAccount(fakeAcc); err == nil {
		t.Fatal("expected error when updating a non-existent account, got nil")
	}
}

func TestMongoLedger_GetBalance(t *testing.T) {
	// Skip if MongoDB is not available
	ldb := setupTestMongoDB(t)
	if ldb == nil {
		t.Skip("MongoDB not available")
	}

	acc := &common.Account{
		ID:        "balCheck",
		Balance:   75.0,
		PublicKey: fakeDilithiumPubKey(),
	}
	if err := ldb.CreateAccount(acc); err != nil {
		t.Fatalf("CreateAccount failed: %v", err)
	}
	bal, err := ldb.GetBalance("balCheck")
	if err != nil {
		t.Fatalf("GetBalance error: %v", err)
	}
	if bal != 75.0 {
		t.Fatalf("expected balance 75.0, got %.2f", bal)
	}
}

func TestMongoLedger_UpdateAccountBalance(t *testing.T) {
	// Skip if MongoDB is not available
	ldb := setupTestMongoDB(t)
	if ldb == nil {
		t.Skip("MongoDB not available")
	}

	acc := &common.Account{
		ID:        "updateBal",
		Balance:   20.0,
		PublicKey: fakeDilithiumPubKey(),
	}
	if err := ldb.CreateAccount(acc); err != nil {
		t.Fatalf("CreateAccount failed: %v", err)
	}
	// Add 15; expected new total is 35.
	if err := ldb.UpdateAccountBalance("updateBal", 15); err != nil {
		t.Fatalf("UpdateAccountBalance failed: %v", err)
	}
	newBal, err := ldb.GetBalance("updateBal")
	if err != nil {
		t.Fatalf("GetBalance failed: %v", err)
	}
	if newBal != 35.0 {
		t.Fatalf("expected balance 35.0, got %.2f", newBal)
	}
}

func TestMongoLedger_AddTransaction(t *testing.T) {
	// Skip if MongoDB is not available
	ldb := setupTestMongoDB(t)
	if ldb == nil {
		t.Skip("MongoDB not available")
	}

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
	if err := ldb.CreateAccount(sender); err != nil {
		t.Fatalf("CreateAccount for sender failed: %v", err)
	}
	if err := ldb.CreateAccount(receiver); err != nil {
		t.Fatalf("CreateAccount for receiver failed: %v", err)
	}

	tx := common.Transaction{
		ID:        "tx-add-1",
		Sender:    "sender",
		Receiver:  "receiver",
		Amount:    40.0,
		Fee:       1.0,
		Signature: fakeSignature(),
	}
	if err := ldb.AddTransaction(tx); err != nil {
		t.Fatalf("AddTransaction failed: %v", err)
	}
	if !ldb.IsTransactionCommitted("tx-add-1") {
		t.Fatal("expected transaction to be committed, but IsTransactionCommitted returned false")
	}
}

func TestMongoLedger_IsTransactionCommitted(t *testing.T) {
	// Skip if MongoDB is not available
	ldb := setupTestMongoDB(t)
	if ldb == nil {
		t.Skip("MongoDB not available")
	}

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
	if err := ldb.CreateAccount(acc1); err != nil {
		t.Fatalf("CreateAccount for sender2 failed: %v", err)
	}
	if err := ldb.CreateAccount(acc2); err != nil {
		t.Fatalf("CreateAccount for receiver2 failed: %v", err)
	}

	tx := common.Transaction{
		ID:        "tx-check-1",
		Sender:    "sender2",
		Receiver:  "receiver2",
		Amount:    10.0,
		Fee:       0.5,
		Signature: fakeSignature(),
	}
	// Before adding, the transaction should not be committed.
	if ldb.IsTransactionCommitted("tx-check-1") {
		t.Fatal("expected IsTransactionCommitted to return false, got true")
	}
	if err := ldb.AddTransaction(tx); err != nil {
		t.Fatalf("AddTransaction failed: %v", err)
	}
	// Now it should be committed.
	if !ldb.IsTransactionCommitted("tx-check-1") {
		t.Fatal("expected IsTransactionCommitted to return true after adding transaction, got false")
	}
}

func TestMongoLedger_CommitBlock(t *testing.T) {
	// Skip if MongoDB is not available
	ldb := setupTestMongoDB(t)
	if ldb == nil {
		t.Skip("MongoDB not available")
	}

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
	if err := ldb.CreateAccount(blockSender); err != nil {
		t.Fatalf("CreateAccount for blockSender failed: %v", err)
	}
	if err := ldb.CreateAccount(blockReceiver); err != nil {
		t.Fatalf("CreateAccount for blockReceiver failed: %v", err)
	}

	tx := common.Transaction{
		ID:        "btx1",
		Sender:    "blockSender",
		Receiver:  "blockReceiver",
		Amount:    50.0,
		Fee:       1.0,
		Signature: fakeSignature(),
	}
	if err := ldb.AddTransaction(tx); err != nil {
		t.Fatalf("AddTransaction failed: %v", err)
	}

	block := common.Block{
		Number:       1,
		PreviousHash: "some-genesis-hash",
		Transactions: []common.Transaction{tx},
		Timestamp:    time.Now().Unix(),
		Hash:         "hash-of-this-block", // placeholder; ledger will recompute.
	}
	// Compute final hash.
	block.Hash = common.ComputeBlockHash(block)

	if err := ldb.CommitBlock(block); err != nil {
		t.Fatalf("unexpected error committing block: %v", err)
	}
}

func TestMongoLedger_GetBlock(t *testing.T) {
	// Skip if MongoDB is not available
	ldb := setupTestMongoDB(t)
	if ldb == nil {
		t.Skip("MongoDB not available")
	}

	// Create accounts.
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
	if err := ldb.CreateAccount(acc1); err != nil {
		t.Fatalf("CreateAccount for gblh-sender failed: %v", err)
	}
	if err := ldb.CreateAccount(acc2); err != nil {
		t.Fatalf("CreateAccount for gblh-recv failed: %v", err)
	}

	tx := common.Transaction{
		ID:        "gblh-tx1",
		Sender:    "gblh-sender",
		Receiver:  "gblh-recv",
		Amount:    20.0,
		Fee:       0.5,
		Signature: fakeSignature(),
	}
	if err := ldb.AddTransaction(tx); err != nil {
		t.Fatalf("AddTransaction failed: %v", err)
	}
	block := common.Block{
		Number:       1,
		PreviousHash: "some-hash",
		Transactions: []common.Transaction{tx},
		Timestamp:    time.Now().Unix(),
	}
	block.Hash = common.ComputeBlockHash(block)
	if err := ldb.CommitBlock(block); err != nil {
		t.Fatalf("CommitBlock error: %v", err)
	}

	// Get the block by number
	retrievedBlock, found := ldb.GetBlockByNumber(1)
	if !found {
		t.Fatal("expected block #1 to exist, got false")
	}
	if retrievedBlock.Hash != block.Hash {
		t.Fatalf("expected hash=%s, got %s", block.Hash, retrievedBlock.Hash)
	}
}

func TestMongoLedger_GetBlockByNumber(t *testing.T) {
	// Skip if MongoDB is not available
	ldb := setupTestMongoDB(t)
	if ldb == nil {
		t.Skip("MongoDB not available")
	}

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
	if err := ldb.CreateAccount(acS); err != nil {
		t.Fatalf("CreateAccount for blockNumSender failed: %v", err)
	}
	if err := ldb.CreateAccount(acR); err != nil {
		t.Fatalf("CreateAccount for blockNumReceiver failed: %v", err)
	}

	tx := common.Transaction{
		ID:        "gblock-1",
		Sender:    "blockNumSender",
		Receiver:  "blockNumReceiver",
		Amount:    10.0,
		Fee:       0.1,
		Signature: fakeSignature(),
	}
	if err := ldb.AddTransaction(tx); err != nil {
		t.Fatalf("AddTransaction failed: %v", err)
	}

	block := common.Block{
		Number:       1,
		PreviousHash: "blockNum0",
		Transactions: []common.Transaction{tx},
		Timestamp:    time.Now().Unix(),
	}
	block.Hash = common.ComputeBlockHash(block)
	if err := ldb.CommitBlock(block); err != nil {
		t.Fatalf("CommitBlock error: %v", err)
	}

	got, found := ldb.GetBlockByNumber(1)
	if !found {
		t.Fatal("expected block #1 to exist, got false")
	}
	if got.Number != 1 {
		t.Fatalf("mismatch: expected block number=1, got %d", got.Number)
	}

	_, found2 := ldb.GetBlockByNumber(2)
	if found2 {
		t.Fatal("expected block #2 not to exist")
	}
}

func TestMongoLedger_CreateSnapshotAndRestore(t *testing.T) {
	t.Skip("Snapshot and restore are not implemented in MongoLedger")
}

func TestMongoLedger_IntegrityCheck(t *testing.T) {
	// Skip if MongoDB is not available
	ldb := setupTestMongoDB(t)
	if ldb == nil {
		t.Skip("MongoDB not available")
	}

	// Initially, since no block is committed, IntegrityCheck should fail.
	if err := ldb.IntegrityCheck(); err == nil {
		t.Fatal("expected IntegrityCheck to fail due to missing metadata, but it passed")
	}

	// Commit a block to update metadata.
	acc := &common.Account{
		ID:        "intCheckSender",
		Balance:   500.0,
		PublicKey: fakeDilithiumPubKey(),
	}
	if err := ldb.CreateAccount(acc); err != nil {
		t.Fatalf("CreateAccount failed: %v", err)
	}
	block := common.Block{
		Number:       1,
		PreviousHash: "xyz",
		Timestamp:    time.Now().Unix(),
	}
	block.Hash = common.ComputeBlockHash(block)
	if err := ldb.CommitBlock(block); err != nil {
		t.Fatalf("CommitBlock error: %v", err)
	}

	if err := ldb.IntegrityCheck(); err != nil {
		t.Fatalf("IntegrityCheck failed after committing block: %v", err)
	}
}
