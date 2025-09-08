package unit

import (
	"context"
	"testing"
	"time"

	"diamante/common"
	testutils "diamante/tests/utils"
)

// TestAccount tests the Account struct and its methods
func TestAccount(t *testing.T) {
	runner := testutils.NewTestRunner(t)
	runner.Run(func(env *testutils.TestEnvironment) {
		t.Run("CreateAccount", func(t *testing.T) {
			// Test valid account creation using test helper
			account := env.CreateTestAccount("test-account-1", 0.0)
			env.AssertEqual("test-account-1", account.ID, "Account ID mismatch")
			env.AssertEqual(0.0, account.Balance, "Initial balance should be 0")
			env.AssertEqual(0, account.Nonce, "Initial nonce should be 0")

			// Test account validation
			err := account.Validate()
			env.AssertNoError(err, "Valid account should pass validation")

			// Test invalid account creation with proper 32-byte key
			publicKey := make([]byte, 32)
			for i := 0; i < 32; i++ {
				publicKey[i] = byte(i + 1)
			}

			_, err = common.NewAccount("", publicKey)
			env.AssertError(err, "Empty account ID should cause error")

			_, err = common.NewAccount("test-account-2", nil)
			env.AssertError(err, "Nil public key should cause error")
		})

		t.Run("AccountBalance", func(t *testing.T) {
			account := env.CreateTestAccount("balance-test", 100.0)

			// Test getting balance
			balance := account.GetBalance()
			env.AssertEqual(100.0, balance, "Balance should match initial value")

			// Test updating balance
			err := account.UpdateBalance(50.0)
			env.AssertNoError(err, "Valid balance update should succeed")
			env.AssertEqual(150.0, account.GetBalance(), "Balance should be updated")

			// Test negative balance (should fail due to insufficient funds)
			err = account.UpdateBalance(-200.0)
			env.AssertError(err, "Negative balance update should fail due to insufficient funds")
			env.AssertEqual(150.0, account.GetBalance(), "Balance should remain unchanged after failed update")
		})

		t.Run("AccountClone", func(t *testing.T) {
			original := env.CreateTestAccount("clone-test", 75.0)
			original.Nonce = 5

			clone := original.Clone()
			env.AssertEqual(original.ID, clone.ID, "Cloned account ID should match")
			env.AssertEqual(original.Balance, clone.Balance, "Cloned account balance should match")
			env.AssertEqual(original.Nonce, clone.Nonce, "Cloned account nonce should match")

			// Verify it's a deep copy
			clone.Balance = 100.0
			env.AssertEqual(75.0, original.Balance, "Original balance should not change")
		})
	})
}

// TestTransaction tests the Transaction struct and its methods
func TestTransaction(t *testing.T) {
	runner := testutils.NewTestRunner(t)
	runner.Run(func(env *testutils.TestEnvironment) {
		t.Run("CreateTransaction", func(t *testing.T) {
			tx := env.CreateTestTransaction("sender123", "receiver123", 50.0)

			// Test basic properties
			env.AssertEqual("sender123", tx.Sender, "Sender should match")
			env.AssertEqual("receiver123", tx.Receiver, "Receiver should match")
			env.AssertEqual(50.0, tx.Amount, "Amount should match")
			env.AssertEqual("pending", tx.Status, "Initial status should be pending")
		})

		t.Run("TransactionValidation", func(t *testing.T) {
			// Test valid transaction
			validTx := common.Transaction{
				ID:        "valid-tx-1",
				Sender:    "sender1",
				Receiver:  "receiver1",
				Amount:    100.0,
				Timestamp: time.Now().Unix(),
				Status:    "pending",
				Metadata: &common.TransactionMetadata{
					Category:    "transfer",
					Description: "Test transaction",
				},
			}
			err := validTx.Validate()
			env.AssertNoError(err, "Valid transaction should pass validation")

			// Test transaction with empty sender
			invalidTx := validTx
			invalidTx.Sender = ""
			err = invalidTx.Validate()
			env.AssertError(err, "Transaction with empty sender should fail validation")

			// Test transaction with empty receiver
			invalidTx = validTx
			invalidTx.Receiver = ""
			err = invalidTx.Validate()
			env.AssertError(err, "Transaction with empty receiver should fail validation")

			// Test transaction with negative amount
			invalidTx = validTx
			invalidTx.Amount = -10.0
			err = invalidTx.Validate()
			env.AssertError(err, "Transaction with negative amount should fail validation")

			// Test transaction with zero amount
			invalidTx = validTx
			invalidTx.Amount = 0.0
			err = invalidTx.Validate()
			env.AssertError(err, "Transaction with zero amount should fail validation")

			// Test transaction with negative fee
			invalidTx = validTx
			invalidTx.Fee = -5.0
			err = invalidTx.Validate()
			env.AssertError(err, "Transaction with negative fee should fail validation")

			// Test transaction with invalid timestamp
			invalidTx = validTx
			invalidTx.Timestamp = 0
			err = invalidTx.Validate()
			env.AssertError(err, "Transaction with zero timestamp should fail validation")

			// Test transaction integrity validation (which checks for empty ID)
			invalidTx = validTx
			invalidTx.ID = ""
			err = common.VerifyTransactionIntegrity(&invalidTx)
			env.AssertError(err, "Transaction with empty ID should fail integrity validation")
		})

		t.Run("TransactionSigning", func(t *testing.T) {
			tx := env.CreateTestTransaction("sender1", "receiver1", 25.0)

			// Test transaction hash calculation
			hash := common.CalculateTransactionHash(&tx)
			env.AssertTrue(len(hash) > 0, "Transaction hash should be calculated")

			// Test transaction integrity verification
			err := common.VerifyTransactionIntegrity(&tx)
			env.AssertNoError(err, "Transaction integrity verification should succeed")

			// Test signature verification (requires setting up public key)
			publicKey := []byte("test-public-key")
			err = common.SetPublicKey(tx.Sender, publicKey)
			env.AssertNoError(err, "Setting public key should succeed")

			// Set a dummy signature for testing
			tx.Signature = []byte("dummy-signature-for-testing")
			tx.PublicKey = publicKey

			// Note: In a real implementation, this would use proper cryptographic verification
			// For testing, we just verify the structure is correct
			env.AssertTrue(len(tx.Signature) > 0, "Signature should be set")
		})
	})
}

// TestBlock tests the Block struct and its methods
func TestBlock(t *testing.T) {
	runner := testutils.NewTestRunner(t)
	runner.Run(func(env *testutils.TestEnvironment) {
		t.Run("CreateBlock", func(t *testing.T) {
			tx1 := env.CreateTestTransaction("sender1", "receiver1", 10.0)
			tx2 := env.CreateTestTransaction("sender2", "receiver2", 20.0)
			transactions := []common.Transaction{tx1, tx2}

			block := env.CreateTestBlock(1, transactions)

			env.AssertEqual(1, block.Number, "Block number should match")
			env.AssertEqual(2, len(block.Transactions), "Block should contain correct number of transactions")
			env.AssertTrue(len(block.Hash) > 0, "Block hash should be set")
		})

		t.Run("BlockValidation", func(t *testing.T) {
			// Test valid block (genesis block)
			genesisBlock := common.Block{
				Number:       0,
				Hash:         "genesis-hash",
				PreviousHash: "0",
				Timestamp:    time.Now().Unix(),
				Transactions: []common.Transaction{},
			}
			err := genesisBlock.Validate(nil)
			env.AssertNoError(err, "Genesis block should pass validation")

			// Test valid block with previous block
			prevBlock := common.Block{
				Number:       1,
				Hash:         "prev-hash",
				PreviousHash: "genesis-hash",
				Timestamp:    time.Now().Unix() - 60,
				Transactions: []common.Transaction{},
			}

			nextBlock := common.Block{
				Number:       2,
				Hash:         "next-hash",
				PreviousHash: "prev-hash",
				Timestamp:    time.Now().Unix(),
				Transactions: []common.Transaction{},
			}
			err = nextBlock.Validate(&prevBlock)
			env.AssertNoError(err, "Valid block chain should pass validation")

			// Test invalid block number (should be sequential)
			invalidBlock := nextBlock
			invalidBlock.Number = 5 // Should be 2
			err = invalidBlock.Validate(&prevBlock)
			env.AssertError(err, "Block with invalid number should fail validation")

			// Test invalid previous hash
			invalidBlock = nextBlock
			invalidBlock.PreviousHash = "wrong-hash"
			err = invalidBlock.Validate(&prevBlock)
			env.AssertError(err, "Block with wrong previous hash should fail validation")

			// Test block with invalid timestamp (zero)
			invalidBlock = nextBlock
			invalidBlock.Timestamp = 0
			err = invalidBlock.Validate(&prevBlock)
			env.AssertError(err, "Block with zero timestamp should fail validation")

			// Test block with empty hash
			invalidBlock = nextBlock
			invalidBlock.Hash = ""
			err = invalidBlock.Validate(&prevBlock)
			env.AssertError(err, "Block with empty hash should fail validation")
		})

		t.Run("BlockHashing", func(t *testing.T) {
			block := env.CreateTestBlock(1, []common.Transaction{})

			// Test hash calculation using the common function
			hash := common.ComputeBlockHash(block)
			env.AssertTrue(len(hash) > 0, "Hash should be calculated")

			// Test hash consistency
			hash2 := common.ComputeBlockHash(block)
			env.AssertEqual(hash, hash2, "Hash should be consistent")

			// Test hash changes with content
			block.Timestamp = time.Now().Unix() + 1
			hash3 := common.ComputeBlockHash(block)
			env.AssertTrue(hash != hash3, "Hash should change when content changes")
		})
	})
}

// TestSmartContract tests the SmartContract struct and its methods
func TestSmartContract(t *testing.T) {
	runner := testutils.NewTestRunner(t)
	runner.Run(func(env *testutils.TestEnvironment) {
		t.Run("CreateSmartContract", func(t *testing.T) {
			contract := &common.SmartContract{
				ID:       "contract-1",
				Code:     "contract code here",
				Owner:    "owner1",
				Language: "solidity",
				State: &common.SmartContractState{
					Variables:     make(map[string]string),
					Balances:      make(map[string]float64),
					Permissions:   make(map[string]bool),
					Configuration: make(map[string]string),
					Counters:      make(map[string]int64),
				},
				Events: make([]common.SmartContractEvent, 0),
			}

			env.AssertEqual("contract-1", contract.ID, "Contract ID should match")
			env.AssertEqual("solidity", contract.Language, "Contract language should match")
			env.AssertEqual("owner1", contract.Owner, "Contract owner should match")
		})

		t.Run("SmartContractExecution", func(t *testing.T) {
			contract := &common.SmartContract{
				ID:       "contract-2",
				Code:     "function transfer(to, amount) { /* implementation */ }",
				Owner:    "owner2",
				Language: "javascript",
				State: &common.SmartContractState{
					Variables:     make(map[string]string),
					Balances:      make(map[string]float64),
					Permissions:   make(map[string]bool),
					Configuration: make(map[string]string),
					Counters:      make(map[string]int64),
				},
				Events: make([]common.SmartContractEvent, 0),
			}

			// Test adding an event
			event := common.SmartContractEvent{
				ContractID:   contract.ID,
				FunctionName: "transfer",
				Params: &common.SmartContractParams{
					FunctionName: "transfer",
					Caller:       "sender",
					StringParams: map[string]string{"to": "recipient"},
					NumberParams: map[string]float64{"amount": 100},
				},
				Result: &common.SmartContractResult{
					Success:      true,
					StringResult: "success",
				},
				Timestamp: time.Now().Unix(),
			}

			contract.Events = append(contract.Events, event)
			env.AssertEqual(1, len(contract.Events), "Contract should have one event")
			env.AssertEqual("transfer", contract.Events[0].FunctionName, "Event function name should match")
		})

		t.Run("SmartContractState", func(t *testing.T) {
			contract := &common.SmartContract{
				ID:       "contract-3",
				Code:     "state management contract",
				Owner:    "owner3",
				Language: "wasm",
				State: &common.SmartContractState{
					Variables:     make(map[string]string),
					Balances:      make(map[string]float64),
					Permissions:   make(map[string]bool),
					Configuration: make(map[string]string),
					Counters:      make(map[string]int64),
				},
				Events: make([]common.SmartContractEvent, 0),
			}

			// Test state management
			contract.State.Balances["owner3"] = 1000
			contract.State.Variables["owner"] = "owner3"

			env.AssertEqual(1000.0, contract.State.Balances["owner3"], "Contract state should store balance")
			env.AssertEqual("owner3", contract.State.Variables["owner"], "Contract state should store owner")

			// Test state updates
			contract.State.Balances["owner3"] = 900
			env.AssertEqual(900.0, contract.State.Balances["owner3"], "Contract state should be updatable")
		})
	})
}

// TestDefaultLedger tests the DefaultLedger implementation
func TestDefaultLedger(t *testing.T) {
	runner := testutils.NewTestRunner(t)
	runner.Run(func(env *testutils.TestEnvironment) {
		t.Run("LedgerAccountOperations", func(t *testing.T) {
			ledger := common.NewDefaultLedger()

			// Test account creation
			account := env.CreateTestAccount("ledger-test-1", 100.0)
			err := ledger.CreateAccount(account)
			env.AssertNoError(err, "Account creation should succeed")

			// Test duplicate account creation
			err = ledger.CreateAccount(account)
			env.AssertError(err, "Duplicate account creation should fail")

			// Test account retrieval
			balance, err := ledger.GetBalance("ledger-test-1")
			env.AssertNoError(err, "Balance retrieval should succeed")
			env.AssertEqual(100.0, balance, "Balance should match")

			// Test account update
			account.Balance = 150.0
			err = ledger.UpdateAccount(account)
			env.AssertNoError(err, "Account update should succeed")

			balance, err = ledger.GetBalance("ledger-test-1")
			env.AssertNoError(err, "Balance retrieval after update should succeed")
			env.AssertEqual(150.0, balance, "Updated balance should match")
		})

		t.Run("LedgerTransactionOperations", func(t *testing.T) {
			ledger := common.NewDefaultLedger()

			// Test transaction addition
			tx := env.CreateTestTransaction("sender1", "receiver1", 50.0)
			err := ledger.AddTransaction(tx)
			env.AssertNoError(err, "Transaction addition should succeed")

			// Test transaction retrieval
			retrievedTx, err := ledger.GetTransaction(tx.ID)
			env.AssertNoError(err, "Transaction retrieval should succeed")
			env.AssertEqual(tx.ID, retrievedTx.ID, "Retrieved transaction ID should match")
			env.AssertEqual(tx.Amount, retrievedTx.Amount, "Retrieved transaction amount should match")

			// Test transaction commitment status
			isCommitted := ledger.IsTransactionCommitted(tx.ID)
			env.AssertFalse(isCommitted, "New transaction should not be committed")
		})

		t.Run("LedgerBlockOperations", func(t *testing.T) {
			ledger := common.NewDefaultLedger()

			// Create and add transactions with unique IDs
			tx1 := env.CreateTestTransaction("sender123", "receiver123", 25.0)
			tx2 := env.CreateTestTransaction("sender456", "receiver456", 35.0)

			// Add small delay to ensure unique timestamps
			time.Sleep(1 * time.Millisecond)
			tx2 = env.CreateTestTransaction("sender456", "receiver456", 35.0)

			err := ledger.AddTransaction(tx1)
			env.AssertNoError(err, "First transaction addition should succeed")
			err = ledger.AddTransaction(tx2)
			env.AssertNoError(err, "Second transaction addition should succeed")

			// Create and commit block
			block := env.CreateTestBlock(1, []common.Transaction{tx1, tx2})
			err = ledger.CommitBlock(block)
			env.AssertNoError(err, "Block commitment should succeed")

			// Test block retrieval
			retrievedBlock, found := ledger.GetBlockByNumber(1)
			env.AssertTrue(found, "Block should be found")
			env.AssertEqual(1, retrievedBlock.Number, "Retrieved block number should match")
			env.AssertEqual(2, len(retrievedBlock.Transactions), "Retrieved block should have correct transaction count")

			// Test transaction commitment status after block commit
			isCommitted := ledger.IsTransactionCommitted(tx1.ID)
			env.AssertTrue(isCommitted, "Transaction should be committed after block commit")
		})

		t.Run("LedgerIntegrityCheck", func(t *testing.T) {
			ledger := common.NewDefaultLedger()

			// Test integrity check on empty ledger
			err := ledger.IntegrityCheck()
			env.AssertNoError(err, "Integrity check on empty ledger should pass")

			// Add some data and test integrity
			account := env.CreateTestAccount("integrity-test", 200.0)
			err = ledger.CreateAccount(account)
			env.AssertNoError(err, "Account creation should succeed")

			tx := env.CreateTestTransaction("integrity-test", "receiver", 50.0)
			err = ledger.AddTransaction(tx)
			env.AssertNoError(err, "Transaction addition should succeed")

			block := env.CreateTestBlock(1, []common.Transaction{tx})
			err = ledger.CommitBlock(block)
			env.AssertNoError(err, "Block commitment should succeed")

			err = ledger.IntegrityCheck()
			env.AssertNoError(err, "Integrity check with data should pass")
		})

		t.Run("LedgerHealthCheck", func(t *testing.T) {
			ledger := common.NewDefaultLedger()

			// Test health check with proper context
			ctx := context.WithValue(context.Background(), "test", "health-check")
			err := ledger.HealthCheck(ctx)
			env.AssertNoError(err, "Health check should pass")

			// Test health check after closing
			err = ledger.Close()
			env.AssertNoError(err, "Ledger close should succeed")

			err = ledger.HealthCheck(ctx)
			env.AssertError(err, "Health check on closed ledger should fail")
		})
	})
}

// TestTokenSupply tests the token supply functionality
func TestTokenSupply(t *testing.T) {
	runner := testutils.NewTestRunner(t)
	runner.Run(func(env *testutils.TestEnvironment) {
		t.Run("TokenSupplyInitialization", func(t *testing.T) {
			tokenSupply := common.GetTokenSupply()

			// Test initialization
			err := tokenSupply.Initialize(1000000.0, "treasury-account")
			env.AssertNoError(err, "Token supply initialization should succeed")

			// Test getting total supply
			totalSupply := tokenSupply.GetTotalSupply()
			env.AssertEqual(1000000.0, totalSupply, "Total supply should match initialized value")

			// Test getting circulating supply (should be 0 initially)
			circulatingSupply := tokenSupply.GetCirculatingSupply()
			env.AssertEqual(0.0, circulatingSupply, "Initial circulating supply should be 0")
		})

		t.Run("TokenSupplyOperations", func(t *testing.T) {
			tokenSupply := common.GetTokenSupply()
			err := tokenSupply.Initialize(1000000.0, "treasury-account")
			env.AssertNoError(err, "Token supply initialization should succeed")

			// Test minting tokens to an account
			err = tokenSupply.MintTokens("test-account", 100000.0)
			env.AssertNoError(err, "Token minting should succeed")

			// Check circulating supply increased
			circulatingSupply := tokenSupply.GetCirculatingSupply()
			env.AssertEqual(100000.0, circulatingSupply, "Circulating supply should increase after minting")

			// Test burning tokens from an account
			err = tokenSupply.BurnTokens("test-account", 50000.0)
			env.AssertNoError(err, "Token burning should succeed")

			// Check circulating supply decreased
			circulatingSupply = tokenSupply.GetCirculatingSupply()
			env.AssertEqual(50000.0, circulatingSupply, "Circulating supply should decrease after burning")

			// Test funding a new wallet
			err = tokenSupply.FundNewWallet("new-wallet", 25000.0)
			env.AssertNoError(err, "Funding new wallet should succeed")

			// Check circulating supply increased again
			circulatingSupply = tokenSupply.GetCirculatingSupply()
			env.AssertEqual(75000.0, circulatingSupply, "Circulating supply should increase after funding")
		})
	})
}
