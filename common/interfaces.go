package common

// LedgerAPI defines the methods that your transaction manager or smart contracts expect from the ledger.
type LedgerAPI interface {
	// Existing methods from snippet
	DeploySmartContract(sc *SmartContract) error
	ExecuteSmartContract(scID, function, sender string, params map[string]interface{}) (interface{}, error)
	UpdateAccountBalance(accountID string, amount float64) error
	RemoveSmartContract(contractID string) error

	// Additional methods required by your TransactionManager tests:
	AddTransaction(tx Transaction) error
	IsTransactionCommitted(txID string) bool
}

// SmartContractManagerAPI defines the methods that the ledger expects from the smart contract manager.
type SmartContractManagerAPI interface {
	DeploySmartContract(contractCode, owner string) (string, error)
	ExecuteContractFunction(contractID, functionName string, params map[string]interface{}, sender string) (interface{}, error)
	TerminateSmartContract(contractID, requester string) error
}
