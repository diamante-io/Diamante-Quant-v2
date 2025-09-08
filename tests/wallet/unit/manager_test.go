package unit

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"diamante/wallet"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestManagerCreation(t *testing.T) {
	tempDir := t.TempDir()
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)

	// Set up test environment
	os.Setenv("DIAMANTE_ENV", "development")
	os.Setenv("DIAMANTE_ALLOW_DEV_KEY", "true")
	defer func() {
		os.Unsetenv("DIAMANTE_ENV")
		os.Unsetenv("DIAMANTE_ALLOW_DEV_KEY")
	}()

	t.Run("Create manager with custom config", func(t *testing.T) {
		config, err := wallet.DefaultConfig()
		require.NoError(t, err)

		cfg := &wallet.ManagerConfig{
			WalletDir: tempDir,
			Config:    config,
			Logger:    logger,
		}

		manager, err := wallet.NewManager(cfg)
		assert.NoError(t, err)
		assert.NotNil(t, manager)

		// Verify wallet directory was created
		_, err = os.Stat(tempDir)
		assert.NoError(t, err)
	})

	t.Run("Create manager with nil config", func(t *testing.T) {
		_, err := wallet.NewManager(nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "manager config cannot be nil")
	})

	t.Run("Create manager with defaults", func(t *testing.T) {
		cfg := &wallet.ManagerConfig{}

		manager, err := wallet.NewManager(cfg)
		assert.NoError(t, err)
		assert.NotNil(t, manager)

		// Should use default wallet directory
		homeDir, _ := os.UserHomeDir()
		expectedDir := filepath.Join(homeDir, ".diamante", "wallets")
		_, err = os.Stat(expectedDir)
		assert.NoError(t, err)

		// Clean up
		os.RemoveAll(filepath.Join(homeDir, ".diamante"))
	})
}

func TestManagerWalletOperations(t *testing.T) {
	tempDir := t.TempDir()
	logger := logrus.New()

	// Set up test environment
	os.Setenv("DIAMANTE_ENV", "development")
	os.Setenv("DIAMANTE_ALLOW_DEV_KEY", "true")
	defer func() {
		os.Unsetenv("DIAMANTE_ENV")
		os.Unsetenv("DIAMANTE_ALLOW_DEV_KEY")
	}()

	config, err := wallet.DefaultConfig()
	require.NoError(t, err)

	cfg := &wallet.ManagerConfig{
		WalletDir: tempDir,
		Config:    config,
		Logger:    logger,
	}

	t.Run("Create and list wallets", func(t *testing.T) {
		manager, err := wallet.NewManager(cfg)
		require.NoError(t, err)

		// Initially no wallets
		wallets, err := manager.ListWallets()
		require.NoError(t, err)
		assert.Equal(t, 0, len(wallets))

		// Create wallet
		w1, err := manager.CreateWallet("wallet1")
		assert.NoError(t, err)
		assert.NotNil(t, w1)

		// List should show one wallet
		wallets, err = manager.ListWallets()
		require.NoError(t, err)
		assert.Equal(t, 1, len(wallets))
		assert.Contains(t, wallets, w1.ID)

		// Create another wallet
		w2, err := manager.CreateWallet("wallet2")
		assert.NoError(t, err)
		assert.NotNil(t, w2)
		assert.NotEqual(t, w1.ID, w2.ID)

		// List should show two wallets
		wallets, err = manager.ListWallets()
		require.NoError(t, err)
		assert.Equal(t, 2, len(wallets))
	})

	t.Run("Get wallet by ID", func(t *testing.T) {
		manager, err := wallet.NewManager(cfg)
		require.NoError(t, err)

		// Create wallet
		w1, err := manager.CreateWallet("test")
		require.NoError(t, err)

		// Get existing wallet
		retrieved, err := manager.GetWallet(w1.ID)
		assert.NoError(t, err)
		assert.NotNil(t, retrieved)
		assert.Equal(t, w1.ID, retrieved.ID)

		// Get non-existent wallet
		_, err = manager.GetWallet("non-existent-id")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "wallet not found")
	})

	t.Run("Default wallet operations", func(t *testing.T) {
		manager, err := wallet.NewManager(cfg)
		require.NoError(t, err)

		// No default wallet initially
		_, err = manager.GetDefaultWallet()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no default wallet set")

		// Create and set default wallet
		w1, err := manager.CreateWallet("default")
		require.NoError(t, err)

		err = manager.SetDefaultWallet(w1.ID)
		assert.NoError(t, err)

		// Get default wallet
		defaultWallet, err := manager.GetDefaultWallet()
		assert.NoError(t, err)
		assert.Equal(t, w1.ID, defaultWallet.ID)

		// Set non-existent wallet as default
		err = manager.SetDefaultWallet("non-existent")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "wallet not found")
	})

	t.Run("Delete wallet", func(t *testing.T) {
		manager, err := wallet.NewManager(cfg)
		require.NoError(t, err)

		// Create wallet
		w1, err := manager.CreateWallet("to-delete")
		require.NoError(t, err)

		// Verify it exists
		wallets, err := manager.ListWallets()
		require.NoError(t, err)
		assert.Contains(t, wallets, w1.ID)

		// Delete wallet
		err = manager.DeleteWallet(w1.ID)
		assert.NoError(t, err)

		// Verify it's gone
		wallets, err = manager.ListWallets()
		require.NoError(t, err)
		assert.NotContains(t, wallets, w1.ID)

		// Try to get deleted wallet
		_, err = manager.GetWallet(w1.ID)
		assert.Error(t, err)

		// Delete non-existent wallet
		err = manager.DeleteWallet("non-existent")
		assert.Error(t, err)
	})

	t.Run("Delete default wallet", func(t *testing.T) {
		manager, err := wallet.NewManager(cfg)
		require.NoError(t, err)

		// Create and set default wallet
		w1, err := manager.CreateWallet("default")
		require.NoError(t, err)

		err = manager.SetDefaultWallet(w1.ID)
		require.NoError(t, err)

		// Try to delete default wallet
		err = manager.DeleteWallet(w1.ID)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "cannot delete default wallet")

		// Clear default wallet
		err = manager.ClearDefaultWallet()
		assert.NoError(t, err)

		// Now delete should work
		err = manager.DeleteWallet(w1.ID)
		assert.NoError(t, err)
	})
}

func TestManagerImportExport(t *testing.T) {
	tempDir := t.TempDir()
	logger := logrus.New()

	// Set up test environment
	os.Setenv("DIAMANTE_ENV", "development")
	os.Setenv("DIAMANTE_ALLOW_DEV_KEY", "true")
	defer func() {
		os.Unsetenv("DIAMANTE_ENV")
		os.Unsetenv("DIAMANTE_ALLOW_DEV_KEY")
	}()

	config, err := wallet.DefaultConfig()
	require.NoError(t, err)

	cfg := &wallet.ManagerConfig{
		WalletDir: tempDir,
		Config:    config,
		Logger:    logger,
	}

	t.Run("Export and import wallet", func(t *testing.T) {
		manager, err := wallet.NewManager(cfg)
		require.NoError(t, err)

		// Create wallet
		w1, err := manager.CreateWallet("export-test")
		require.NoError(t, err)

		// Export to file
		exportPath := filepath.Join(tempDir, "exported_wallet.json")
		err = manager.ExportWallet(w1.ID, exportPath)
		assert.NoError(t, err)

		// Verify file exists
		_, err = os.Stat(exportPath)
		assert.NoError(t, err)

		// Create new manager
		newManager, err := wallet.NewManager(&wallet.ManagerConfig{
			WalletDir: filepath.Join(tempDir, "new"),
			Config:    config,
			Logger:    logger,
		})
		require.NoError(t, err)

		// Import wallet
		imported, err := newManager.ImportWallet(exportPath, "imported")
		assert.NoError(t, err)
		assert.NotNil(t, imported)
		assert.Equal(t, w1.ID, imported.ID)

		// Verify imported wallet is in manager
		wallets, err := newManager.ListWallets()
		require.NoError(t, err)
		assert.Contains(t, wallets, imported.ID)
	})

	t.Run("Export non-existent wallet", func(t *testing.T) {
		manager, err := wallet.NewManager(cfg)
		require.NoError(t, err)

		err = manager.ExportWallet("non-existent", filepath.Join(tempDir, "export.json"))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "wallet not found")
	})

	t.Run("Import invalid file", func(t *testing.T) {
		manager, err := wallet.NewManager(cfg)
		require.NoError(t, err)

		// Create invalid wallet file
		invalidPath := filepath.Join(tempDir, "invalid.json")
		err = os.WriteFile(invalidPath, []byte("invalid json"), 0600)
		require.NoError(t, err)

		_, err = manager.ImportWallet(invalidPath, "invalid")
		assert.Error(t, err)
	})
}

func TestManagerMnemonicOperations(t *testing.T) {
	tempDir := t.TempDir()
	logger := logrus.New()

	// Set up test environment
	os.Setenv("DIAMANTE_ENV", "development")
	os.Setenv("DIAMANTE_ALLOW_DEV_KEY", "true")
	defer func() {
		os.Unsetenv("DIAMANTE_ENV")
		os.Unsetenv("DIAMANTE_ALLOW_DEV_KEY")
	}()

	config, err := wallet.DefaultConfig()
	require.NoError(t, err)

	cfg := &wallet.ManagerConfig{
		WalletDir: tempDir,
		Config:    config,
		Logger:    logger,
	}

	t.Run("Create wallet with mnemonic", func(t *testing.T) {
		manager, err := wallet.NewManager(cfg)
		require.NoError(t, err)

		// Create wallet with mnemonic
		walletWithMnemonic, err := manager.CreateWalletWithMnemonic("mnemonic-wallet")
		assert.NoError(t, err)
		assert.NotNil(t, walletWithMnemonic)
		assert.NotEmpty(t, walletWithMnemonic.Mnemonic)
		assert.NotNil(t, walletWithMnemonic.Wallet)

		// Verify wallet is in manager
		wallets, err := manager.ListWallets()
		require.NoError(t, err)
		assert.Contains(t, wallets, walletWithMnemonic.Wallet.ID)

		// Verify mnemonic is valid
		mg, _ := wallet.NewMnemonicGenerator(logger)
		err = mg.ValidateMnemonic(walletWithMnemonic.Mnemonic)
		assert.NoError(t, err)
	})

	t.Run("Import wallet from mnemonic", func(t *testing.T) {
		manager, err := wallet.NewManager(cfg)
		require.NoError(t, err)

		// Generate mnemonic
		mg, err := wallet.NewMnemonicGenerator(logger)
		require.NoError(t, err)

		mnemonic, err := mg.GenerateMnemonic()
		require.NoError(t, err)

		// Import wallet from mnemonic
		imported, err := manager.ImportWalletFromMnemonic(mnemonic, "", "mnemonic-import")
		assert.NoError(t, err)
		assert.NotNil(t, imported)

		// Verify wallet is in manager
		wallets, err := manager.ListWallets()
		require.NoError(t, err)
		assert.Contains(t, wallets, imported.ID)

		// Import same mnemonic again - should produce same wallet
		imported2, err := manager.ImportWalletFromMnemonic(mnemonic, "", "mnemonic-import-2")
		assert.NoError(t, err)
		assert.Equal(t, imported.ID, imported2.ID)
	})

	t.Run("Import invalid mnemonic", func(t *testing.T) {
		manager, err := wallet.NewManager(cfg)
		require.NoError(t, err)

		_, err = manager.ImportWalletFromMnemonic("invalid mnemonic words", "", "invalid")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid mnemonic")
	})
}

func TestManagerPersistence(t *testing.T) {
	tempDir := t.TempDir()
	logger := logrus.New()

	// Set up test environment
	os.Setenv("DIAMANTE_ENV", "development")
	os.Setenv("DIAMANTE_ALLOW_DEV_KEY", "true")
	defer func() {
		os.Unsetenv("DIAMANTE_ENV")
		os.Unsetenv("DIAMANTE_ALLOW_DEV_KEY")
	}()

	config, err := wallet.DefaultConfig()
	require.NoError(t, err)

	cfg := &wallet.ManagerConfig{
		WalletDir: tempDir,
		Config:    config,
		Logger:    logger,
	}

	t.Run("Persistence across manager instances", func(t *testing.T) {
		// Create first manager and add wallets
		manager1, err := wallet.NewManager(cfg)
		require.NoError(t, err)

		w1, err := manager1.CreateWallet("wallet1")
		require.NoError(t, err)

		w2, err := manager1.CreateWallet("wallet2")
		require.NoError(t, err)

		err = manager1.SetDefaultWallet(w1.ID)
		require.NoError(t, err)

		// Create second manager with same directory
		manager2, err := wallet.NewManager(cfg)
		require.NoError(t, err)

		// Verify wallets are loaded
		wallets, err := manager2.ListWallets()
		require.NoError(t, err)
		assert.Equal(t, 2, len(wallets))
		assert.Contains(t, wallets, w1.ID)
		assert.Contains(t, wallets, w2.ID)

		// Verify default wallet is preserved
		defaultWallet, err := manager2.GetDefaultWallet()
		assert.NoError(t, err)
		assert.Equal(t, w1.ID, defaultWallet.ID)
	})

	t.Run("Manager state file", func(t *testing.T) {
		manager, err := wallet.NewManager(cfg)
		require.NoError(t, err)

		// Create wallet and set as default
		w1, err := manager.CreateWallet("state-test")
		require.NoError(t, err)

		err = manager.SetDefaultWallet(w1.ID)
		require.NoError(t, err)

		// Check state file exists
		stateFile := filepath.Join(tempDir, "manager_state.json")
		_, err = os.Stat(stateFile)
		assert.NoError(t, err)

		// Read and verify state file
		data, err := os.ReadFile(stateFile)
		require.NoError(t, err)

		var state map[string]interface{}
		err = json.Unmarshal(data, &state)
		assert.NoError(t, err)
		assert.Equal(t, w1.ID, state["defaultWalletID"])
	})
}

func TestManagerConcurrency(t *testing.T) {
	tempDir := t.TempDir()
	logger := logrus.New()
	logger.SetLevel(logrus.WarnLevel) // Reduce log noise

	// Set up test environment
	os.Setenv("DIAMANTE_ENV", "development")
	os.Setenv("DIAMANTE_ALLOW_DEV_KEY", "true")
	defer func() {
		os.Unsetenv("DIAMANTE_ENV")
		os.Unsetenv("DIAMANTE_ALLOW_DEV_KEY")
	}()

	config, err := wallet.DefaultConfig()
	require.NoError(t, err)

	cfg := &wallet.ManagerConfig{
		WalletDir: tempDir,
		Config:    config,
		Logger:    logger,
	}

	t.Run("Concurrent wallet creation", func(t *testing.T) {
		manager, err := wallet.NewManager(cfg)
		require.NoError(t, err)

		numGoroutines := 10
		var wg sync.WaitGroup
		wg.Add(numGoroutines)

		errors := make(chan error, numGoroutines)
		walletIDs := make(chan string, numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			go func(index int) {
				defer wg.Done()

				w, err := manager.CreateWallet(fmt.Sprintf("concurrent-%d", index))
				if err != nil {
					errors <- err
					return
				}
				walletIDs <- w.ID
			}(i)
		}

		wg.Wait()
		close(errors)
		close(walletIDs)

		// Check for errors
		for err := range errors {
			assert.NoError(t, err)
		}

		// Collect wallet IDs
		ids := make(map[string]bool)
		for id := range walletIDs {
			ids[id] = true
		}

		// Verify all wallets were created
		assert.Equal(t, numGoroutines, len(ids))

		// Verify all wallets are in manager
		wallets, err := manager.ListWallets()
		require.NoError(t, err)
		assert.Equal(t, numGoroutines, len(wallets))
	})

	t.Run("Concurrent read operations", func(t *testing.T) {
		manager, err := wallet.NewManager(cfg)
		require.NoError(t, err)

		// Create test wallet
		w, err := manager.CreateWallet("read-test")
		require.NoError(t, err)

		numReaders := 20
		var wg sync.WaitGroup
		wg.Add(numReaders)

		for i := 0; i < numReaders; i++ {
			go func() {
				defer wg.Done()

				// Perform multiple read operations
				for j := 0; j < 10; j++ {
					// Get wallet
					retrieved, err := manager.GetWallet(w.ID)
					assert.NoError(t, err)
					assert.NotNil(t, retrieved)

					// List wallets
					wallets, err := manager.ListWallets()
					assert.NoError(t, err)
					assert.Contains(t, wallets, w.ID)
				}
			}()
		}

		wg.Wait()
	})
}

func TestManagerBackupRestore(t *testing.T) {
	tempDir := t.TempDir()
	logger := logrus.New()

	// Set up test environment
	os.Setenv("DIAMANTE_ENV", "development")
	os.Setenv("DIAMANTE_ALLOW_DEV_KEY", "true")
	defer func() {
		os.Unsetenv("DIAMANTE_ENV")
		os.Unsetenv("DIAMANTE_ALLOW_DEV_KEY")
	}()

	config, err := wallet.DefaultConfig()
	require.NoError(t, err)

	cfg := &wallet.ManagerConfig{
		WalletDir: tempDir,
		Config:    config,
		Logger:    logger,
	}

	t.Run("Backup and restore all wallets", func(t *testing.T) {
		manager, err := wallet.NewManager(cfg)
		require.NoError(t, err)

		// Create multiple wallets
		w1, err := manager.CreateWallet("backup1")
		require.NoError(t, err)

		w2, err := manager.CreateWallet("backup2")
		require.NoError(t, err)

		err = manager.SetDefaultWallet(w1.ID)
		require.NoError(t, err)

		// Backup all wallets
		backupPath := filepath.Join(tempDir, "backup.tar.gz")
		err = manager.BackupAllWallets(backupPath)
		assert.NoError(t, err)

		// Verify backup file exists
		_, err = os.Stat(backupPath)
		assert.NoError(t, err)

		// Create new manager with different directory
		restoreDir := filepath.Join(tempDir, "restored")
		restoreCfg := &wallet.ManagerConfig{
			WalletDir: restoreDir,
			Config:    config,
			Logger:    logger,
		}

		restoredManager, err := wallet.NewManager(restoreCfg)
		require.NoError(t, err)

		// Restore wallets
		err = restoredManager.RestoreAllWallets(backupPath)
		assert.NoError(t, err)

		// Verify wallets are restored
		wallets, err := restoredManager.ListWallets()
		require.NoError(t, err)
		assert.Equal(t, 2, len(wallets))
		assert.Contains(t, wallets, w1.ID)
		assert.Contains(t, wallets, w2.ID)

		// Verify default wallet is restored
		defaultWallet, err := restoredManager.GetDefaultWallet()
		assert.NoError(t, err)
		assert.Equal(t, w1.ID, defaultWallet.ID)
	})
}

// TestManagerEdgeCases tests edge cases and error scenarios
func TestManagerEdgeCases(t *testing.T) {
	tempDir := t.TempDir()
	logger := logrus.New()

	// Set up test environment
	os.Setenv("DIAMANTE_ENV", "development")
	os.Setenv("DIAMANTE_ALLOW_DEV_KEY", "true")
	defer func() {
		os.Unsetenv("DIAMANTE_ENV")
		os.Unsetenv("DIAMANTE_ALLOW_DEV_KEY")
	}()

	config, err := wallet.DefaultConfig()
	require.NoError(t, err)

	t.Run("Invalid wallet directory permissions", func(t *testing.T) {
		// Create directory with no write permissions
		restrictedDir := filepath.Join(tempDir, "restricted")
		err := os.Mkdir(restrictedDir, 0444)
		require.NoError(t, err)

		cfg := &wallet.ManagerConfig{
			WalletDir: filepath.Join(restrictedDir, "wallets"),
			Config:    config,
			Logger:    logger,
		}

		_, err = wallet.NewManager(cfg)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to create wallet directory")
	})

	t.Run("Corrupted wallet file", func(t *testing.T) {
		cfg := &wallet.ManagerConfig{
			WalletDir: tempDir,
			Config:    config,
			Logger:    logger,
		}

		manager, err := wallet.NewManager(cfg)
		require.NoError(t, err)

		// Create a wallet
		w, err := manager.CreateWallet("test")
		require.NoError(t, err)

		// Corrupt the wallet file
		walletFile := filepath.Join(tempDir, w.ID+".json")
		err = os.WriteFile(walletFile, []byte("corrupted data"), 0600)
		require.NoError(t, err)

		// Create new manager - should handle corrupted file gracefully
		newManager, err := wallet.NewManager(cfg)
		assert.NoError(t, err) // Should not fail completely

		// Corrupted wallet should not be loaded
		wallets, err := newManager.ListWallets()
		require.NoError(t, err)
		assert.NotContains(t, wallets, w.ID)
	})
}
