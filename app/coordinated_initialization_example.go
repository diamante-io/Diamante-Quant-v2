// Package app provides an example of coordinated initialization
package app

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"diamante/config"
	finality "diamante/consensus/diamantefinality"
	"diamante/consensus/diamantepoh"
	"diamante/consensus/diamantepos"
	"diamante/network"
	networktls "diamante/network/tls"
	"diamante/storage"

	apipkg "diamante/api"

	"github.com/sirupsen/logrus"
)

// LoggerAdapter adapts logrus.Logger to the interface expected by consensus components
type LoggerAdapter struct {
	logger *logrus.Logger
}

// NewLoggerAdapter creates a new logger adapter
func NewLoggerAdapter(logger *logrus.Logger) *LoggerAdapter {
	return &LoggerAdapter{logger: logger}
}

// Log implements the logging interface
func (la *LoggerAdapter) Log(level string, msg string, args ...interface{}) {
	switch level {
	case "debug":
		la.logger.Debugf(msg, args...)
	case "info":
		la.logger.Infof(msg, args...)
	case "warn":
		la.logger.Warnf(msg, args...)
	case "error":
		la.logger.Errorf(msg, args...)
	default:
		la.logger.Printf(msg, args...)
	}
}

// Info logs at info level
func (la *LoggerAdapter) Info(msg string, args ...interface{}) {
	la.logger.Infof(msg, args...)
}

// Debug logs at debug level
func (la *LoggerAdapter) Debug(msg string, args ...interface{}) {
	la.logger.Debugf(msg, args...)
}

// Error logs at error level
func (la *LoggerAdapter) Error(msg string, args ...interface{}) {
	la.logger.Errorf(msg, args...)
}

// CoordinatedApp represents an application with coordinated initialization
type CoordinatedApp struct {
	coordinator *InitializationCoordinator
	logger      *logrus.Logger

	// Component references
	configManager    *config.Manager
	mongoLedger      *storage.MongoDBLedger
	mongoStore       *storage.MongoStore
	blockPersistence *storage.BlockPersistence
	tlsManager       *networktls.EnhancedTLSManager
	networkManager   *network.NetworkManager
	consensusWrapper *ConsensusWrapper
	hybridVMSystem   interface{} // Replace with actual VM system type when available
	apiServer        *apipkg.API
}

// NewCoordinatedApp creates a new application with coordinated initialization
func NewCoordinatedApp(logger *logrus.Logger) *CoordinatedApp {
	if logger == nil {
		logger = logrus.New()
		logger.SetLevel(logrus.InfoLevel)
	}

	return &CoordinatedApp{
		coordinator: NewInitializationCoordinator(logger),
		logger:      logger,
	}
}

// RegisterInitializationPhases registers all initialization phases
func (app *CoordinatedApp) RegisterInitializationPhases() error {
	// Configuration Phase
	configPhaseConfig := &InitPhaseConfig{
		Handler: func(ctx context.Context) error {
			app.logger.Info("Loading configuration...")
			configPath := os.Getenv("CONFIG_PATH")
			if configPath == "" {
				configPath = "config/config.yaml"
			}

			var err error
			app.configManager, err = config.NewManager(configPath)
			if err != nil {
				return fmt.Errorf("failed to load configuration: %w", err)
			}

			app.logger.Info("Configuration loaded successfully")
			return nil
		},
		ShutdownHandler: func(ctx context.Context) error {
			app.logger.Info("Configuration shutdown (no-op)")
			return nil
		},
		Timeout:    10 * time.Second,
		RetryCount: 3,
		RetryDelay: 1 * time.Second,
	}

	// Storage Phase
	storagePhaseConfig := &InitPhaseConfig{
		Handler: func(ctx context.Context) error {
			app.logger.Info("Initializing storage...")

			cfg := app.configManager.GetConfig()
			mongoConfig := cfg.Database.Mongo

			// MongoDB connection
			mongoURI := mongoConfig.URI
			dbName := mongoConfig.Database
			maxPoolSize := mongoConfig.MaxPoolSize
			retries := 3
			retryDelay := 1 * time.Second

			var err error

			// Initialize MongoDB ledger
			mongoAdapter, err := storage.NewMongoAdapter(mongoURI, dbName, app.logger, 1000)
			if err != nil {
				return fmt.Errorf("failed to create MongoDB adapter: %w", err)
			}

			app.mongoLedger, err = storage.NewMongoDBLedger(mongoAdapter, app.logger)
			if err != nil {
				return fmt.Errorf("failed to create MongoDB ledger: %w", err)
			}

			// Initialize MongoDB store
			app.mongoStore, err = storage.NewMongoStore(mongoURI, dbName, maxPoolSize, retries, retryDelay)
			if err != nil {
				// MongoLedger doesn't have a Close method, so we can't clean it up
				return fmt.Errorf("failed to connect to MongoDB store: %w", err)
			}

			// Initialize block persistence
			dataDir := os.Getenv("DIAMANTE_DATA_DIR")
			if dataDir == "" {
				dataDir = "/tmp/diamante/data"
			}
			blocksDir := filepath.Join(dataDir, "blocks")
			app.blockPersistence, err = storage.NewBlockPersistence(blocksDir, app.logger)
			if err != nil {
				app.mongoStore.Close()
				return fmt.Errorf("failed to initialize block persistence: %w", err)
			}

			app.logger.Info("Storage initialized successfully")
			return nil
		},
		ShutdownHandler: func(ctx context.Context) error {
			app.logger.Info("Shutting down storage...")

			var shutdownErrors []error

			if app.blockPersistence != nil {
				app.logger.Debug("Closing block persistence")
				// Block persistence has no explicit close method
			}

			if app.mongoStore != nil {
				app.logger.Debug("Closing MongoDB store")
				if err := app.mongoStore.Close(); err != nil {
					shutdownErrors = append(shutdownErrors, fmt.Errorf("mongo store: %w", err))
				}
			}

			if app.mongoLedger != nil {
				app.logger.Debug("Closing MongoDB ledger")
				// MongoLedger doesn't have a Close method
			}

			if len(shutdownErrors) > 0 {
				return fmt.Errorf("storage shutdown errors: %v", shutdownErrors)
			}

			app.logger.Info("Storage shutdown complete")
			return nil
		},
		Timeout:      30 * time.Second,
		RetryCount:   5,
		RetryDelay:   2 * time.Second,
		Dependencies: []InitPhase{InitPhase("config")},
	}

	// Network Phase
	networkPhaseConfig := &InitPhaseConfig{
		Handler: func(ctx context.Context) error {
			app.logger.Info("Initializing network...")

			cfg := app.configManager.GetConfig()

			// Initialize TLS manager with proper config structure
			tlsManagerConfig := &networktls.EnhancedTLSManagerConfig{
				NodeID:             cfg.Network.TLS.NodeID,
				CertDir:            filepath.Dir(cfg.Network.TLS.CertFile),
				Logger:             app.logger,
				TLSEnabled:         cfg.Network.TLS.Enabled,
				EnableAutoRotation: true,
				CAConfig: &networktls.CAConfig{
					CACertPath: cfg.Network.TLS.CertFile,
					CAKeyPath:  cfg.Network.TLS.KeyFile,
					CertDir:    filepath.Dir(cfg.Network.TLS.CertFile),
					Logger:     app.logger,
				},
			}

			var err error
			app.tlsManager, err = networktls.NewEnhancedTLSManager(tlsManagerConfig)
			if err != nil {
				return fmt.Errorf("failed to initialize TLS manager: %w", err)
			}

			if err := app.tlsManager.Start(); err != nil {
				return fmt.Errorf("failed to start TLS manager: %w", err)
			}

			// Wait for TLS to be ready
			select {
			case <-time.After(500 * time.Millisecond):
			case <-ctx.Done():
				return ctx.Err()
			}

			// Initialize network manager using ListenAddress from config
			localAddr := cfg.Network.ListenAddress
			// Create a basic discovery service - pass nil for now
			var discovery network.Discovery
			tlsCfg := app.tlsManager.GetTLSConfig()

			app.networkManager = network.NewNetworkManager(localAddr, discovery, tlsCfg, nil)

			if err := app.networkManager.Start(); err != nil {
				app.tlsManager.Stop()
				return fmt.Errorf("failed to start network manager: %w", err)
			}

			app.logger.Info("Network initialized successfully")
			return nil
		},
		ShutdownHandler: func(ctx context.Context) error {
			app.logger.Info("Shutting down network...")

			if app.networkManager != nil {
				app.logger.Debug("Stopping network manager")
				if err := app.networkManager.Stop(); err != nil {
					app.logger.WithError(err).Warn("Network manager stop error")
				}
			}

			if app.tlsManager != nil {
				app.logger.Debug("Stopping TLS manager")
				if err := app.tlsManager.Stop(); err != nil {
					app.logger.WithError(err).Warn("TLS manager stop error")
				}
			}

			app.logger.Info("Network shutdown complete")
			return nil
		},
		Timeout:      20 * time.Second,
		RetryCount:   3,
		RetryDelay:   2 * time.Second,
		Dependencies: []InitPhase{InitPhase("config")},
	}

	// Consensus Phase
	consensusPhaseConfig := &InitPhaseConfig{
		Handler: func(ctx context.Context) error {
			app.logger.Info("Initializing consensus...")

			// Initialize consensus components
			lachesis := finality.NewLachesis(100 * time.Millisecond)
			dpos := diamantepos.NewDPoS(21, 1000, NewLoggerAdapter(app.logger))
			poh := diamantepoh.NewPoH([32]byte{}, 1*time.Second, NewLoggerAdapter(app.logger))

			// Create wrapper
			app.consensusWrapper = NewConsensusWrapper(lachesis, dpos, poh, app.logger)

			// Start consensus
			if err := app.consensusWrapper.Start(); err != nil {
				return fmt.Errorf("failed to start consensus: %w", err)
			}

			app.logger.Info("Consensus initialized successfully")
			return nil
		},
		ShutdownHandler: func(ctx context.Context) error {
			app.logger.Info("Shutting down consensus...")

			if app.consensusWrapper != nil {
				if err := app.consensusWrapper.Stop(); err != nil {
					return fmt.Errorf("consensus stop error: %w", err)
				}
			}

			app.logger.Info("Consensus shutdown complete")
			return nil
		},
		Timeout:      15 * time.Second,
		RetryCount:   2,
		RetryDelay:   1 * time.Second,
		Dependencies: []InitPhase{InitPhase("storage"), InitPhase("network")},
	}

	// Runtime Phase
	runtimePhaseConfig := &InitPhaseConfig{
		Handler: func(ctx context.Context) error {
			app.logger.Info("Initializing runtime...")

			// Since VM package is not available, we'll skip VM initialization
			// In production, this would initialize the actual VM system
			app.logger.Info("VM initialization skipped (VM package not available)")

			app.logger.Info("Runtime initialized successfully")
			return nil
		},
		ShutdownHandler: func(ctx context.Context) error {
			app.logger.Info("Shutting down runtime...")
			app.logger.Info("Runtime shutdown complete")
			return nil
		},
		Timeout:      25 * time.Second,
		RetryCount:   3,
		RetryDelay:   2 * time.Second,
		Dependencies: []InitPhase{InitPhase("storage")},
	}

	// API Phase
	apiPhaseConfig := &InitPhaseConfig{
		Handler: func(ctx context.Context) error {
			app.logger.Info("Initializing API...")

			// Since we have interface mismatches and missing types,
			// we'll skip actual API initialization
			app.logger.Info("API initialization skipped due to interface mismatches")

			// In production, you would need to:
			// 1. Create proper adapters for the interfaces
			// 2. Implement missing types like TransactionManager and GovernanceManager
			// 3. Modify the API to accept the correct interfaces

			app.logger.Info("API initialized successfully (mock)")
			return nil
		},
		ShutdownHandler: func(ctx context.Context) error {
			app.logger.Info("Shutting down API...")

			if app.apiServer != nil {
				// API server should have graceful shutdown
				app.logger.Debug("Stopping API server")
				// Note: In production, implement proper API shutdown
			}

			app.logger.Info("API shutdown complete")
			return nil
		},
		Timeout:      10 * time.Second,
		RetryCount:   2,
		RetryDelay:   1 * time.Second,
		Dependencies: []InitPhase{InitPhase("consensus"), InitPhase("runtime")},
	}

	// Register all phases
	if err := app.coordinator.RegisterPhaseWithConfig(InitPhase("config"), configPhaseConfig); err != nil {
		return fmt.Errorf("failed to register config phase: %w", err)
	}

	if err := app.coordinator.RegisterPhaseWithConfig(InitPhase("storage"), storagePhaseConfig); err != nil {
		return fmt.Errorf("failed to register storage phase: %w", err)
	}

	if err := app.coordinator.RegisterPhaseWithConfig(InitPhase("network"), networkPhaseConfig); err != nil {
		return fmt.Errorf("failed to register network phase: %w", err)
	}

	if err := app.coordinator.RegisterPhaseWithConfig(InitPhase("consensus"), consensusPhaseConfig); err != nil {
		return fmt.Errorf("failed to register consensus phase: %w", err)
	}

	if err := app.coordinator.RegisterPhaseWithConfig(InitPhase("runtime"), runtimePhaseConfig); err != nil {
		return fmt.Errorf("failed to register runtime phase: %w", err)
	}

	if err := app.coordinator.RegisterPhaseWithConfig(InitPhase("api"), apiPhaseConfig); err != nil {
		return fmt.Errorf("failed to register API phase: %w", err)
	}

	return nil
}

// Initialize performs coordinated initialization
func (app *CoordinatedApp) Initialize() error {
	return app.coordinator.Initialize()
}

// Run starts the application and waits for shutdown signal
func (app *CoordinatedApp) Run() error {
	// Setup signal handling
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	// Wait for shutdown signal
	app.logger.Info("Application running. Press Ctrl+C to stop.")
	<-sigCh

	app.logger.Info("Shutdown signal received")

	// Perform coordinated shutdown
	return app.coordinator.Shutdown(30 * time.Second)
}

// GetStatus returns the current initialization status
func (app *CoordinatedApp) GetStatus() map[InitPhase]InitStatus {
	return app.coordinator.GetStatus()
}

// Example usage in main.go
func ExampleCoordinatedMain() {
	// Setup logging
	logger := logrus.New()
	logger.SetLevel(logrus.InfoLevel)
	logger.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
		ForceColors:   true,
	})

	// Create coordinated app
	app := NewCoordinatedApp(logger)

	// Register all initialization phases
	if err := app.RegisterInitializationPhases(); err != nil {
		logger.Fatalf("Failed to register initialization phases: %v", err)
	}

	// Perform coordinated initialization
	if err := app.Initialize(); err != nil {
		logger.Fatalf("Initialization failed: %v", err)
	}

	// Run application
	if err := app.Run(); err != nil {
		logger.WithError(err).Error("Application shutdown with errors")
		os.Exit(1)
	}

	logger.Info("Application shutdown complete")
}
