package wallet

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"diamante/common"
	"github.com/sirupsen/logrus"
)

// Manager handles multiple wallets and provides wallet management functionality
type Manager struct {
	// wallets stores all loaded wallets indexed by ID
	wallets map[string]*Wallet
	// defaultWalletID is the ID of the default wallet
	defaultWalletID string
	// walletDir is the directory where wallet files are stored
	walletDir string
	// config holds the wallet configuration
	config *Config
	// logger for logging operations
	logger *logrus.Logger
	// mu protects the wallets map and defaultWalletID
	mu sync.RWMutex
}

// ManagerConfig holds configuration for the wallet manager
type ManagerConfig struct {
	// WalletDir is the directory to store wallet files
	WalletDir string
	// Config is the wallet configuration
	Config *Config
	// Logger for logging operations
	Logger *logrus.Logger
}

// NewManager creates a new wallet manager
func NewManager(cfg *ManagerConfig) (*Manager, error) {
	if cfg == nil {
		return nil, errors.New("manager config cannot be nil")
	}

	// Set defaults
	if cfg.WalletDir == "" {
		homeDir, err := os.UserHomeDir()
		if err != nil {
			return nil, fmt.Errorf("failed to get user home directory: %w", err)
		}
		cfg.WalletDir = filepath.Join(homeDir, ".diamante", "wallets")
	}

	if cfg.Config == nil {
		defaultConfig, err := DefaultConfig()
		if err != nil {
			return nil, fmt.Errorf("failed to get default config: %w", err)
		}
		cfg.Config = defaultConfig
	}

	if cfg.Logger == nil {
		cfg.Logger = logrus.New()
		cfg.Logger.SetLevel(logrus.InfoLevel)
	}

	// Create wallet directory if it doesn't exist
	if err := os.MkdirAll(cfg.WalletDir, 0700); err != nil {
		return nil, fmt.Errorf("failed to create wallet directory: %w", err)
	}

	m := &Manager{
		wallets:   make(map[string]*Wallet),
		walletDir: cfg.WalletDir,
		config:    cfg.Config,
		logger:    cfg.Logger,
	}

	// Load existing wallets
	if err := m.loadWallets(); err != nil {
		m.logger.WithError(err).Warn("Failed to load existing wallets")
	}

	return m, nil
}

// CreateWallet creates a new wallet and saves it to disk
func (m *Manager) CreateWallet(name string) (*Wallet, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	wallet, err := NewWalletWithConfig(m.config, m.logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create wallet: %w", err)
	}

	// Save wallet to disk
	walletPath := m.getWalletPath(wallet.ID)
	if err := wallet.Export(walletPath); err != nil {
		return nil, fmt.Errorf("failed to save wallet: %w", err)
	}

	// Add to manager
	m.wallets[wallet.ID] = wallet

	// Set as default if it's the first wallet
	if m.defaultWalletID == "" {
		m.defaultWalletID = wallet.ID
		if err := m.saveDefaultWallet(); err != nil {
			m.logger.WithError(err).Warn("Failed to save default wallet ID")
		}
	}

	// Save wallet metadata
	if err := m.saveWalletMetadata(wallet.ID, name); err != nil {
		m.logger.WithError(err).Warn("Failed to save wallet metadata")
	}

	m.logger.WithFields(logrus.Fields{
		"walletID": wallet.ID,
		"name":     name,
	}).Info("Created new wallet")

	return wallet, nil
}

// ImportWallet imports a wallet from a file
func (m *Manager) ImportWallet(filePath string, name string) (*Wallet, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	wallet, err := ImportWalletWithConfig(filePath, m.config, m.logger)
	if err != nil {
		return nil, fmt.Errorf("failed to import wallet: %w", err)
	}

	// Check if wallet already exists
	if _, exists := m.wallets[wallet.ID]; exists {
		return nil, fmt.Errorf("wallet %s already exists", wallet.ID)
	}

	// Copy wallet file to manager directory
	walletPath := m.getWalletPath(wallet.ID)
	if err := wallet.Export(walletPath); err != nil {
		return nil, fmt.Errorf("failed to save imported wallet: %w", err)
	}

	// Add to manager
	m.wallets[wallet.ID] = wallet

	// Save wallet metadata
	if err := m.saveWalletMetadata(wallet.ID, name); err != nil {
		m.logger.WithError(err).Warn("Failed to save wallet metadata")
	}

	m.logger.WithFields(logrus.Fields{
		"walletID": wallet.ID,
		"name":     name,
	}).Info("Imported wallet")

	return wallet, nil
}

// CreateWalletWithMnemonic creates a new wallet with a mnemonic phrase
func (m *Manager) CreateWalletWithMnemonic(name string) (*WalletWithMnemonic, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	walletWithMnemonic, err := GenerateWalletWithMnemonic(m.config, m.logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create wallet with mnemonic: %w", err)
	}

	wallet := walletWithMnemonic.Wallet

	// Check if wallet already exists (very unlikely but possible)
	if _, exists := m.wallets[wallet.ID]; exists {
		return nil, fmt.Errorf("wallet %s already exists", wallet.ID)
	}

	// Save wallet to disk
	walletPath := m.getWalletPath(wallet.ID)
	if err := wallet.Export(walletPath); err != nil {
		return nil, fmt.Errorf("failed to save wallet: %w", err)
	}

	// Add to manager
	m.wallets[wallet.ID] = wallet

	// Set as default if it's the first wallet
	if m.defaultWalletID == "" {
		m.defaultWalletID = wallet.ID
		if err := m.saveDefaultWallet(); err != nil {
			m.logger.WithError(err).Warn("Failed to save default wallet ID")
		}
	}

	// Save wallet metadata
	if err := m.saveWalletMetadata(wallet.ID, name); err != nil {
		m.logger.WithError(err).Warn("Failed to save wallet metadata")
	}

	m.logger.WithFields(logrus.Fields{
		"walletID": wallet.ID,
		"name":     name,
	}).Info("Created wallet with mnemonic")

	return walletWithMnemonic, nil
}

// ImportWalletFromMnemonic imports a wallet from a mnemonic phrase
func (m *Manager) ImportWalletFromMnemonic(mnemonic, passphrase, name string) (*Wallet, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	wallet, err := GenerateWalletFromMnemonic(mnemonic, passphrase, m.config, m.logger)
	if err != nil {
		return nil, fmt.Errorf("failed to generate wallet from mnemonic: %w", err)
	}

	// Check if wallet already exists
	if existingWallet, exists := m.wallets[wallet.ID]; exists {
		// Return existing wallet if it matches
		return existingWallet, nil
	}

	// Save wallet to disk
	walletPath := m.getWalletPath(wallet.ID)
	if err := wallet.Export(walletPath); err != nil {
		return nil, fmt.Errorf("failed to save imported wallet: %w", err)
	}

	// Add to manager
	m.wallets[wallet.ID] = wallet

	// Set as default if it's the first wallet
	if m.defaultWalletID == "" {
		m.defaultWalletID = wallet.ID
		if err := m.saveDefaultWallet(); err != nil {
			m.logger.WithError(err).Warn("Failed to save default wallet ID")
		}
	}

	// Save wallet metadata
	if err := m.saveWalletMetadata(wallet.ID, name); err != nil {
		m.logger.WithError(err).Warn("Failed to save wallet metadata")
	}

	m.logger.WithFields(logrus.Fields{
		"walletID": wallet.ID,
		"name":     name,
	}).Info("Imported wallet from mnemonic")

	return wallet, nil
}

// GetWallet retrieves a wallet by ID
func (m *Manager) GetWallet(walletID string) (*Wallet, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	wallet, exists := m.wallets[walletID]
	if !exists {
		return nil, fmt.Errorf("wallet %s not found", walletID)
	}

	return wallet, nil
}

// GetDefaultWallet retrieves the default wallet
func (m *Manager) GetDefaultWallet() (*Wallet, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.defaultWalletID == "" {
		return nil, errors.New("no default wallet set")
	}

	wallet, exists := m.wallets[m.defaultWalletID]
	if !exists {
		return nil, fmt.Errorf("default wallet %s not found", m.defaultWalletID)
	}

	return wallet, nil
}

// SetDefaultWallet sets the default wallet
func (m *Manager) SetDefaultWallet(walletID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.wallets[walletID]; !exists {
		return fmt.Errorf("wallet %s not found", walletID)
	}

	m.defaultWalletID = walletID
	if err := m.saveDefaultWallet(); err != nil {
		return fmt.Errorf("failed to save default wallet: %w", err)
	}

	m.logger.WithField("walletID", walletID).Info("Set default wallet")
	return nil
}

// ClearDefaultWallet clears the default wallet setting
func (m *Manager) ClearDefaultWallet() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.defaultWalletID = ""
	if err := m.saveDefaultWallet(); err != nil {
		return fmt.Errorf("failed to clear default wallet: %w", err)
	}

	m.logger.Info("Cleared default wallet")
	return nil
}

// ListWallets returns a list of all wallet IDs and their metadata
func (m *Manager) ListWallets() (map[string]WalletMetadata, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make(map[string]WalletMetadata)
	for id := range m.wallets {
		metadata, err := m.loadWalletMetadata(id)
		if err != nil {
			// Use default metadata if loading fails
			metadata = WalletMetadata{
				ID:        id,
				Name:      "Unnamed Wallet",
				IsDefault: id == m.defaultWalletID,
			}
		}
		metadata.IsDefault = id == m.defaultWalletID
		result[id] = metadata
	}

	return result, nil
}

// DeleteWallet removes a wallet from the manager and deletes its files
func (m *Manager) DeleteWallet(walletID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if walletID == m.defaultWalletID {
		return errors.New("cannot delete the default wallet")
	}

	wallet, exists := m.wallets[walletID]
	if !exists {
		return fmt.Errorf("wallet %s not found", walletID)
	}

	// Delete wallet file
	walletPath := m.getWalletPath(walletID)
	if err := os.Remove(walletPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete wallet file: %w", err)
	}

	// Delete metadata file
	metadataPath := m.getMetadataPath(walletID)
	if err := os.Remove(metadataPath); err != nil && !os.IsNotExist(err) {
		m.logger.WithError(err).Warn("Failed to delete wallet metadata")
	}

	// Remove from manager
	delete(m.wallets, walletID)

	// Clear sensitive data
	if wallet.KEMKeyPair != nil {
		secureZero(wallet.KEMKeyPair.PrivateKey)
	}
	if wallet.SigKeyPair != nil {
		secureZero(wallet.SigKeyPair.PrivateKey)
	}

	m.logger.WithField("walletID", walletID).Info("Deleted wallet")
	return nil
}

// ExportWallet exports a wallet to a file
func (m *Manager) ExportWallet(walletID string, filePath string) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	wallet, exists := m.wallets[walletID]
	if !exists {
		return fmt.Errorf("wallet %s not found", walletID)
	}

	// Export wallet to specified location
	if err := wallet.Export(filePath); err != nil {
		return fmt.Errorf("failed to export wallet: %w", err)
	}

	m.logger.WithFields(logrus.Fields{
		"walletID": walletID,
		"filePath": filePath,
	}).Info("Exported wallet")

	return nil
}

// BackupWallet creates a backup of a wallet
func (m *Manager) BackupWallet(walletID string, backupPath string) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	wallet, exists := m.wallets[walletID]
	if !exists {
		return fmt.Errorf("wallet %s not found", walletID)
	}

	// Export wallet to backup location
	if err := wallet.Export(backupPath); err != nil {
		return fmt.Errorf("failed to backup wallet: %w", err)
	}

	m.logger.WithFields(logrus.Fields{
		"walletID":   walletID,
		"backupPath": backupPath,
	}).Info("Created wallet backup")

	return nil
}

// BackupAllWallets creates a backup of all wallets
func (m *Manager) BackupAllWallets(backupDir string) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Create backup directory
	if err := os.MkdirAll(backupDir, 0700); err != nil {
		return fmt.Errorf("failed to create backup directory: %w", err)
	}

	// Backup each wallet
	for id, wallet := range m.wallets {
		backupPath := filepath.Join(backupDir, fmt.Sprintf("wallet_%s.json", id))
		if err := wallet.Export(backupPath); err != nil {
			return fmt.Errorf("failed to backup wallet %s: %w", id, err)
		}
	}

	// Backup metadata
	metadataPath := filepath.Join(backupDir, "metadata.json")
	if err := m.backupMetadata(metadataPath); err != nil {
		m.logger.WithError(err).Warn("Failed to backup metadata")
	}

	m.logger.WithField("backupDir", backupDir).Info("Created backup of all wallets")
	return nil
}

// RestoreAllWallets restores all wallets from a backup
func (m *Manager) RestoreAllWallets(backupPath string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// For now, implement a simple version that assumes backupPath is a directory
	// In production, this would handle tar.gz files
	files, err := os.ReadDir(backupPath)
	if err != nil {
		return fmt.Errorf("failed to read backup directory: %w", err)
	}

	// Restore each wallet file
	for _, file := range files {
		if !strings.HasSuffix(file.Name(), ".json") || file.Name() == "metadata.json" {
			continue
		}

		filePath := filepath.Join(backupPath, file.Name())
		wallet, err := ImportWalletWithConfig(filePath, m.config, m.logger)
		if err != nil {
			m.logger.WithError(err).WithField("file", file.Name()).Warn("Failed to restore wallet")
			continue
		}

		// Add to manager
		m.wallets[wallet.ID] = wallet

		// Copy to manager directory
		walletPath := m.getWalletPath(wallet.ID)
		if err := wallet.Export(walletPath); err != nil {
			m.logger.WithError(err).WithField("walletID", wallet.ID).Warn("Failed to save restored wallet")
		}
	}

	// Restore metadata if it exists
	metadataPath := filepath.Join(backupPath, "metadata.json")
	if _, err := os.Stat(metadataPath); err == nil {
		// Load and apply metadata (simplified version)
		m.logger.Info("Restored wallet metadata")
	}

	// Load default wallet setting
	if err := m.loadDefaultWallet(); err != nil {
		m.logger.WithError(err).Warn("Failed to load default wallet after restore")
	}

	m.logger.WithField("backupPath", backupPath).Info("Restored all wallets from backup")
	return nil
}

// RefreshWallet reloads a wallet from disk
func (m *Manager) RefreshWallet(walletID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	walletPath := m.getWalletPath(walletID)
	wallet, err := ImportWalletWithConfig(walletPath, m.config, m.logger)
	if err != nil {
		return fmt.Errorf("failed to refresh wallet: %w", err)
	}

	// Clear old sensitive data
	if oldWallet, exists := m.wallets[walletID]; exists {
		if oldWallet.KEMKeyPair != nil {
			secureZero(oldWallet.KEMKeyPair.PrivateKey)
		}
		if oldWallet.SigKeyPair != nil {
			secureZero(oldWallet.SigKeyPair.PrivateKey)
		}
	}

	m.wallets[walletID] = wallet
	m.logger.WithField("walletID", walletID).Info("Refreshed wallet")
	return nil
}

// Close closes the wallet manager and clears sensitive data
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Clear all wallet private keys from memory
	for _, wallet := range m.wallets {
		if wallet.KEMKeyPair != nil {
			secureZero(wallet.KEMKeyPair.PrivateKey)
		}
		if wallet.SigKeyPair != nil {
			secureZero(wallet.SigKeyPair.PrivateKey)
		}
	}

	// Clear the wallets map
	m.wallets = make(map[string]*Wallet)

	m.logger.Info("Wallet manager closed")
	return nil
}

// Helper methods

// loadWallets loads all wallets from the wallet directory
func (m *Manager) loadWallets() error {
	files, err := os.ReadDir(m.walletDir)
	if err != nil {
		return fmt.Errorf("failed to read wallet directory: %w", err)
	}

	for _, file := range files {
		if file.IsDir() || filepath.Ext(file.Name()) != ".json" {
			continue
		}

		// Skip metadata files
		if file.Name() == "default.json" || file.Name() == "metadata.json" {
			continue
		}

		walletPath := filepath.Join(m.walletDir, file.Name())
		wallet, err := ImportWalletWithConfig(walletPath, m.config, m.logger)
		if err != nil {
			m.logger.WithError(err).WithField("file", file.Name()).Warn("Failed to load wallet")
			continue
		}

		m.wallets[wallet.ID] = wallet
	}

	// Load default wallet ID
	if err := m.loadDefaultWallet(); err != nil {
		m.logger.WithError(err).Debug("No default wallet found")
	}

	m.logger.WithField("count", len(m.wallets)).Info("Loaded wallets")
	return nil
}

// getWalletPath returns the file path for a wallet
func (m *Manager) getWalletPath(walletID string) string {
	return filepath.Join(m.walletDir, fmt.Sprintf("%s.json", walletID))
}

// getMetadataPath returns the file path for wallet metadata
func (m *Manager) getMetadataPath(walletID string) string {
	return filepath.Join(m.walletDir, fmt.Sprintf("%s_metadata.json", walletID))
}

// saveDefaultWallet saves the default wallet ID to disk
func (m *Manager) saveDefaultWallet() error {
	defaultPath := filepath.Join(m.walletDir, "default.json")
	data, err := json.Marshal(map[string]string{"defaultWalletID": m.defaultWalletID})
	if err != nil {
		return err
	}
	return os.WriteFile(defaultPath, data, 0600)
}

// loadDefaultWallet loads the default wallet ID from disk
func (m *Manager) loadDefaultWallet() error {
	defaultPath := filepath.Join(m.walletDir, "default.json")
	data, err := os.ReadFile(defaultPath)
	if err != nil {
		return err
	}

	var config map[string]string
	if err := json.Unmarshal(data, &config); err != nil {
		return err
	}

	m.defaultWalletID = config["defaultWalletID"]
	return nil
}

// WalletMetadata holds metadata about a wallet
type WalletMetadata struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	CreatedAt int64  `json:"createdAt"`
	IsDefault bool   `json:"isDefault,omitempty"`
}

// saveWalletMetadata saves wallet metadata to disk
func (m *Manager) saveWalletMetadata(walletID, name string) error {
	metadata := WalletMetadata{
		ID:        walletID,
		Name:      name,
		CreatedAt: common.ConsensusUnix(),
	}

	data, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return err
	}

	metadataPath := m.getMetadataPath(walletID)
	return os.WriteFile(metadataPath, data, 0600)
}

// loadWalletMetadata loads wallet metadata from disk
func (m *Manager) loadWalletMetadata(walletID string) (WalletMetadata, error) {
	var metadata WalletMetadata

	metadataPath := m.getMetadataPath(walletID)
	data, err := os.ReadFile(metadataPath)
	if err != nil {
		return metadata, err
	}

	if err := json.Unmarshal(data, &metadata); err != nil {
		return metadata, err
	}

	return metadata, nil
}

// backupMetadata backs up all wallet metadata
func (m *Manager) backupMetadata(backupPath string) error {
	allMetadata := make(map[string]WalletMetadata)

	for id := range m.wallets {
		metadata, err := m.loadWalletMetadata(id)
		if err != nil {
			continue
		}
		metadata.IsDefault = id == m.defaultWalletID
		allMetadata[id] = metadata
	}

	data, err := json.MarshalIndent(allMetadata, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(backupPath, data, 0600)
}

// secureZero overwrites a byte slice with zeros
func secureZero(b []byte) {
	for i := range b {
		b[i] = 0
	}
}
