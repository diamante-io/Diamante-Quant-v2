// File: wallet/wallet.go
package wallet

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"sync"
	"time"

	"diamante/common"
	"diamante/crypto"
	"diamante/transaction"

	"github.com/sirupsen/logrus"
)

// Wallet represents a user wallet that manages keys and provides methods to create and sign transactions.
type Wallet struct {
	// ID is the unique identifier for the wallet/account.
	ID string `json:"id"`

	// Nonce is maintained locally to help with replay protection when creating transactions.
	Nonce int `json:"nonce"`

	// KEMKeyPair holds the Kyber (KEM) key pair used for encryption and key encapsulation.
	KEMKeyPair *crypto.KyberKeyPair `json:"kemKeyPair"`

	// SigKeyPair holds the Dilithium key pair used for digital signatures.
	SigKeyPair *crypto.DilithiumKeyPair `json:"sigKeyPair"`

	// CryptoManager is the instance used for all cryptographic operations.
	// This field is not serialized and will be re‑initialized upon import.
	CryptoManager *crypto.CryptoManager `json:"-"`

	// mu protects wallet fields during concurrent access.
	mu sync.Mutex
}

// WalletFile is the structure used for persisting a wallet.
type WalletFile struct {
	ID         string                             `json:"id"`
	Nonce      int                                `json:"nonce"`
	KEMKeyPair *crypto.KyberKeyPairSerialized     `json:"kemKeyPair"`
	SigKeyPair *crypto.DilithiumKeyPairSerialized `json:"sigKeyPair"`
}

// NewWallet creates a new wallet with fresh key pairs and a unique account ID.
func NewWallet(logger *logrus.Logger) (*Wallet, error) {
	const defaultKyberLevel = crypto.KyberLevel1024
	const defaultDilithiumLevel = crypto.DilithiumLevel3

	// Instantiate a new crypto manager.
	cm, err := crypto.NewCryptoManager(defaultKyberLevel, defaultDilithiumLevel, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create crypto manager: %w", err)
	}

	// Generate Kyber (KEM) key pair.
	kemKP, err := cm.GenerateKEMKeyPair()
	if err != nil {
		return nil, fmt.Errorf("failed to generate Kyber key pair: %w", err)
	}

	// Generate Dilithium (signature) key pair.
	sigKP, err := cm.GenerateSignatureKeyPair()
	if err != nil {
		return nil, fmt.Errorf("failed to generate Dilithium key pair: %w", err)
	}

	// Generate a unique account ID.
	accountID := common.GenerateUniqueID()

	wallet := &Wallet{
		ID:            accountID,
		Nonce:         0,
		KEMKeyPair:    kemKP,
		SigKeyPair:    sigKP,
		CryptoManager: cm,
	}

	logger.WithField("walletID", accountID).Info("New wallet created successfully")
	return wallet, nil
}

// RegisterAccount registers the wallet’s account in the global account store.
func (w *Wallet) RegisterAccount() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	account := &common.Account{
		ID:         w.ID,
		PublicKey:  w.SigKeyPair.PublicKey,
		PrivateKey: w.SigKeyPair.PrivateKey,
		Balance:    0,
	}

	// Use the common package function to register the account.
	if err := common.RegisterAccount(account); err != nil {
		return fmt.Errorf("failed to register account: %w", err)
	}
	return nil
}

// Export writes the wallet’s data to a JSON file.
func (w *Wallet) Export(filePath string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	kemSerialized, err := crypto.SerializeKyberKeyPair(w.KEMKeyPair)
	if err != nil {
		return fmt.Errorf("failed to serialize Kyber key pair: %w", err)
	}
	sigSerialized, err := crypto.SerializeDilithiumKeyPair(w.SigKeyPair)
	if err != nil {
		return fmt.Errorf("failed to serialize Dilithium key pair: %w", err)
	}

	wf := WalletFile{
		ID:         w.ID,
		Nonce:      w.Nonce,
		KEMKeyPair: kemSerialized,
		SigKeyPair: sigSerialized,
	}

	data, err := json.MarshalIndent(wf, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal wallet to JSON: %w", err)
	}

	if err := ioutil.WriteFile(filePath, data, 0600); err != nil {
		return fmt.Errorf("failed to write wallet file: %w", err)
	}
	return nil
}

// ImportWallet reads a wallet file and returns a new Wallet instance.
func ImportWallet(filePath string, logger *logrus.Logger) (*Wallet, error) {
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read wallet file: %w", err)
	}

	var wf WalletFile
	if err := json.Unmarshal(data, &wf); err != nil {
		return nil, fmt.Errorf("failed to unmarshal wallet JSON: %w", err)
	}

	const defaultKyberLevel = crypto.KyberLevel1024
	const defaultDilithiumLevel = crypto.DilithiumLevel3
	cm, err := crypto.NewCryptoManager(defaultKyberLevel, defaultDilithiumLevel, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to re-create crypto manager: %w", err)
	}

	kemKP, err := crypto.DeserializeKyberKeyPair(wf.KEMKeyPair)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize Kyber key pair: %w", err)
	}
	sigKP, err := crypto.DeserializeDilithiumKeyPair(wf.SigKeyPair)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize Dilithium key pair: %w", err)
	}

	wallet := &Wallet{
		ID:            wf.ID,
		Nonce:         wf.Nonce,
		KEMKeyPair:    kemKP,
		SigKeyPair:    sigKP,
		CryptoManager: cm,
	}

	logger.WithField("walletID", wallet.ID).Info("Wallet imported successfully")
	return wallet, nil
}

// CreateTransaction constructs a new transaction using the wallet as the sender.
// It generates a unique transaction ID, increments the nonce, and signs the transaction ID.
func (w *Wallet) CreateTransaction(receiver string, amount float64, fee float64, data []byte) (*common.Transaction, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	txID := common.GenerateUniqueID()
	w.Nonce++

	// Note: common.Transaction expects Timestamp as an int64.
	tx := &common.Transaction{
		ID:        txID,
		Sender:    w.ID,
		Receiver:  receiver,
		Amount:    amount,
		Fee:       fee,
		Timestamp: time.Now().Unix(),
		Nonce:     w.Nonce,
		Data:      data,
	}

	sig, err := crypto.SignDataWithDilithium(w.SigKeyPair.PrivateKey, []byte(tx.ID))
	if err != nil {
		return nil, fmt.Errorf("failed to sign transaction: %w", err)
	}
	tx.Signature = sig

	return tx, nil
}

// SubmitTransaction submits a transaction via the provided TransactionManager.
// It creates a transaction, then calls the TransactionManager to add it to the pool.
func (w *Wallet) SubmitTransaction(receiver string, amount float64, fee float64, data []byte, txManager *transaction.TransactionManager) (*common.Transaction, error) {
	tx, err := w.CreateTransaction(receiver, amount, fee, data)
	if err != nil {
		return nil, err
	}

	// Call the TransactionManager's CreateTransaction and ignore the returned transaction.
	_, err = txManager.CreateTransaction(tx.Sender, tx.Receiver, tx.Amount, tx.Fee, tx.Data)
	if err != nil {
		return nil, fmt.Errorf("failed to submit transaction via TransactionManager: %w", err)
	}

	return tx, nil
}
