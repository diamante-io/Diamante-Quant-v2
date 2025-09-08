package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"diamante/common"
	"diamante/migration"
	"diamante/migration/handlers"
	"diamante/types"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	configFile string
	workingDir string
	backupDir  string
	dryRun     bool
	skipBackup bool
	verbose    bool
	timeout    time.Duration
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "migration-tool",
		Short: "Diamante blockchain migration tool",
		Long:  "A comprehensive tool for managing blockchain state migrations, upgrades, and data transformations.",
	}

	// Global flags
	rootCmd.PersistentFlags().StringVar(&configFile, "config", "", "Configuration file path")
	rootCmd.PersistentFlags().StringVar(&workingDir, "work-dir", ".", "Working directory for migrations")
	rootCmd.PersistentFlags().StringVar(&backupDir, "backup-dir", "./backups", "Backup directory")
	rootCmd.PersistentFlags().BoolVar(&dryRun, "dry-run", false, "Execute migration in dry run mode")
	rootCmd.PersistentFlags().BoolVar(&skipBackup, "skip-backup", false, "Skip creating backups")
	rootCmd.PersistentFlags().BoolVar(&verbose, "verbose", false, "Enable verbose logging")
	rootCmd.PersistentFlags().DurationVar(&timeout, "timeout", 30*time.Minute, "Migration timeout")

	// Add subcommands
	rootCmd.AddCommand(createPlanCmd())
	rootCmd.AddCommand(listPlansCmd())
	rootCmd.AddCommand(showPlanCmd())
	rootCmd.AddCommand(executePlanCmd())
	rootCmd.AddCommand(validatePlanCmd())
	rootCmd.AddCommand(deletePlanCmd())
	rootCmd.AddCommand(statusCmd())
	rootCmd.AddCommand(cancelCmd())
	rootCmd.AddCommand(backupCmd())
	rootCmd.AddCommand(restoreCmd())
	rootCmd.AddCommand(genesisCmd())

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func createPlanCmd() *cobra.Command {
	var (
		name        string
		description string
		fromVersion string
		toVersion   string
	)

	cmd := &cobra.Command{
		Use:   "create-plan",
		Short: "Create a new migration plan",
		RunE: func(cmd *cobra.Command, args []string) error {
			manager, err := createMigrationManager()
			if err != nil {
				return err
			}

			plan := manager.CreatePlan(name, description, fromVersion, toVersion)

			if err := manager.SavePlan(plan); err != nil {
				return fmt.Errorf("failed to save plan: %w", err)
			}

			fmt.Printf("Migration plan created successfully: %s\n", plan.ID)
			return nil
		},
	}

	cmd.Flags().StringVar(&name, "name", "", "Migration plan name (required)")
	cmd.Flags().StringVar(&description, "description", "", "Migration plan description")
	cmd.Flags().StringVar(&fromVersion, "from", "", "Source version (required)")
	cmd.Flags().StringVar(&toVersion, "to", "", "Target version (required)")

	cmd.MarkFlagRequired("name")
	cmd.MarkFlagRequired("from")
	cmd.MarkFlagRequired("to")

	return cmd
}

func listPlansCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list-plans",
		Short: "List all migration plans",
		RunE: func(cmd *cobra.Command, args []string) error {
			manager, err := createMigrationManager()
			if err != nil {
				return err
			}

			plans, err := manager.ListPlans()
			if err != nil {
				return fmt.Errorf("failed to list plans: %w", err)
			}

			if len(plans) == 0 {
				fmt.Println("No migration plans found")
				return nil
			}

			fmt.Printf("%-20s %-30s %-12s %-12s %-15s\n", "ID", "Name", "From", "To", "Status")
			fmt.Println(string(make([]byte, 90, 90))) // Separator line

			for _, plan := range plans {
				fmt.Printf("%-20s %-30s %-12s %-12s %-15s\n",
					plan.ID,
					plan.Name,
					plan.FromVersion,
					plan.ToVersion,
					plan.Status)
			}

			return nil
		},
	}
}

func showPlanCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "show-plan [plan-id]",
		Short: "Show detailed information about a migration plan",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			planID := args[0]

			manager, err := createMigrationManager()
			if err != nil {
				return err
			}

			plan, err := manager.LoadPlan(planID)
			if err != nil {
				return fmt.Errorf("failed to load plan: %w", err)
			}

			printPlanDetails(plan)
			return nil
		},
	}
}

func executePlanCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "execute [plan-id]",
		Short: "Execute a migration plan",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			planID := args[0]

			manager, err := createMigrationManager()
			if err != nil {
				return err
			}

			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()

			options := &migration.ExecutionOptions{
				Config:     types.NewTypedMap(),
				DryRun:     dryRun,
				SkipBackup: skipBackup,
			}

			fmt.Printf("Executing migration plan: %s\n", planID)
			if dryRun {
				fmt.Println("DRY RUN MODE - No changes will be made")
			}

			if err := manager.ExecutePlan(ctx, planID, options); err != nil {
				return fmt.Errorf("migration execution failed: %w", err)
			}

			fmt.Println("Migration execution started successfully")
			return nil
		},
	}
}

func validatePlanCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "validate [plan-id]",
		Short: "Validate a migration plan",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			planID := args[0]

			manager, err := createMigrationManager()
			if err != nil {
				return err
			}

			plan, err := manager.LoadPlan(planID)
			if err != nil {
				return fmt.Errorf("failed to load plan: %w", err)
			}

			fmt.Printf("Validating migration plan: %s\n", plan.Name)

			// Validate each migration step
			registry := manager.GetRegistry()
			ctx := &migration.MigrationContext{
				Plan:   plan,
				Config: types.NewTypedMap(),
				State:  types.NewTypedMap(),
				DryRun: true,
			}

			validationErrors := 0
			for i, step := range plan.MigrationSteps {
				fmt.Printf("Validating step %d: %s... ", i+1, step.Name)

				handler, exists := registry.GetHandler(step.Handler)
				if !exists {
					fmt.Printf("FAILED - Handler '%s' not found\n", step.Handler)
					validationErrors++
					continue
				}

				if err := handler.Validate(ctx, step); err != nil {
					fmt.Printf("FAILED - %v\n", err)
					validationErrors++
				} else {
					fmt.Println("OK")
				}
			}

			// Validate validation steps
			for i, step := range plan.ValidationSteps {
				fmt.Printf("Validating validation step %d: %s... ", i+1, step.Name)

				_, exists := registry.GetValidationHandler(step.Handler)
				if !exists {
					fmt.Printf("FAILED - Validation handler '%s' not found\n", step.Handler)
					validationErrors++
					continue
				}

				fmt.Println("OK")
			}

			if validationErrors > 0 {
				return fmt.Errorf("validation failed with %d errors", validationErrors)
			}

			fmt.Println("Validation completed successfully!")
			return nil
		},
	}
}

func deletePlanCmd() *cobra.Command {
	var force bool

	cmd := &cobra.Command{
		Use:   "delete [plan-id]",
		Short: "Delete a migration plan",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			planID := args[0]

			manager, err := createMigrationManager()
			if err != nil {
				return err
			}

			if !force {
				fmt.Printf("Are you sure you want to delete plan '%s'? (y/N): ", planID)
				var response string
				fmt.Scanln(&response)
				if response != "y" && response != "Y" {
					fmt.Println("Operation cancelled")
					return nil
				}
			}

			// Delete the plan using MigrationManager
			if err := manager.DeletePlan(planID); err != nil {
				return fmt.Errorf("failed to delete plan: %w", err)
			}

			fmt.Printf("Migration plan '%s' deleted successfully\n", planID)
			return nil
		},
	}

	cmd.Flags().BoolVar(&force, "force", false, "Skip confirmation prompt")
	return cmd
}

func statusCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "status [plan-id]",
		Short: "Check migration status",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			planID := args[0]

			manager, err := createMigrationManager()
			if err != nil {
				return err
			}

			status, err := manager.GetMigrationStatus(planID)
			if err != nil {
				return fmt.Errorf("failed to get status: %w", err)
			}

			fmt.Printf("Migration Status: %s\n", *status)

			// Try to get more detailed information
			if plan, err := manager.LoadPlan(planID); err == nil {
				printMigrationProgress(plan)
			}

			return nil
		},
	}
}

func cancelCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "cancel [plan-id]",
		Short: "Cancel a running migration",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			planID := args[0]

			manager, err := createMigrationManager()
			if err != nil {
				return err
			}

			if err := manager.CancelMigration(planID); err != nil {
				return fmt.Errorf("failed to cancel migration: %w", err)
			}

			fmt.Printf("Migration '%s' cancelled successfully\n", planID)
			return nil
		},
	}
}

func backupCmd() *cobra.Command {
	var version string

	cmd := &cobra.Command{
		Use:   "backup",
		Short: "Create a backup of current state",
		RunE: func(cmd *cobra.Command, args []string) error {
			manager, err := createMigrationManager()
			if err != nil {
				return err
			}

			backupManager := manager.GetBackupManager()

			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()

			fmt.Printf("Creating backup for version %s...\n", version)

			backupInfo, err := backupManager.CreateBackup(ctx, version)
			if err != nil {
				return fmt.Errorf("backup failed: %w", err)
			}

			fmt.Printf("Backup created successfully:\n")
			fmt.Printf("  ID: %s\n", backupInfo.ID)
			fmt.Printf("  Path: %s\n", backupInfo.Path)
			fmt.Printf("  Size: %d bytes\n", backupInfo.Size)
			fmt.Printf("  Checksum: %s\n", backupInfo.Checksum)

			return nil
		},
	}

	cmd.Flags().StringVar(&version, "version", "current", "Version to backup")
	return cmd
}

func restoreCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "restore [backup-path]",
		Short: "Restore from a backup",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			backupPath := args[0]

			manager, err := createMigrationManager()
			if err != nil {
				return err
			}

			backupManager := manager.GetBackupManager()

			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()

			fmt.Printf("Restoring from backup: %s\n", backupPath)

			if err := backupManager.RestoreBackup(ctx, backupPath); err != nil {
				return fmt.Errorf("restore failed: %w", err)
			}

			fmt.Println("Restore completed successfully")
			return nil
		},
	}
}

func genesisCmd() *cobra.Command {
	genesisCmd := &cobra.Command{
		Use:   "genesis",
		Short: "Genesis file migration tools",
	}

	genesisCmd.AddCommand(migrateGenesisCmd())
	genesisCmd.AddCommand(validateGenesisCmd())

	return genesisCmd
}

func migrateGenesisCmd() *cobra.Command {
	var (
		source     string
		target     string
		chainID    string
		updateTime bool
	)

	cmd := &cobra.Command{
		Use:   "migrate",
		Short: "Migrate genesis file",
		RunE: func(cmd *cobra.Command, args []string) error {
			if source == "" {
				source = "./config/genesis.json"
			}
			if target == "" {
				target = "./config/genesis_migrated.json"
			}

			// Create a quick genesis migration
			genesisHandler := handlers.NewGenesisHandler(source)

			// Create migration step
			step := &migration.MigrationStep{
				ID:      "genesis_migration",
				Name:    "Genesis Migration",
				Type:    migration.MigrationTypeGenesis,
				Handler: "genesis",
				Config:  types.NewTypedMap(),
			}

			step.Config.Set("source_genesis", types.NewValue(types.ValueTypeString, []byte(source)))
			step.Config.Set("target_genesis", types.NewValue(types.ValueTypeString, []byte(target)))
			updateTimeBytes := []byte{0}
			if updateTime {
				updateTimeBytes = []byte{1}
			}
			step.Config.Set("update_genesis_time", types.NewValue(types.ValueTypeBool, updateTimeBytes))

			if chainID != "" {
				// Create transformation map
				transformMap := types.NewTypedMap()
				transformMap.Set("type", types.NewValue(types.ValueTypeString, []byte("update_chain_id")))
				transformMap.Set("chain_id", types.NewValue(types.ValueTypeString, []byte(chainID)))

				// Create transformations array as JSON
				transformations := []map[string]string{{
					"type":     "update_chain_id",
					"chain_id": chainID,
				}}
				transformData, _ := json.Marshal(transformations)
				step.Config.Set("transformations", types.NewValue(types.ValueTypeJSON, transformData))
			}

			// Create migration context
			ctx := &migration.MigrationContext{
				Config: types.NewTypedMap(),
				State:  types.NewTypedMap(),
				DryRun: dryRun,
			}

			fmt.Printf("Migrating genesis from %s to %s\n", source, target)
			if dryRun {
				fmt.Println("DRY RUN MODE - No files will be modified")
			}

			result, err := genesisHandler.Execute(ctx, step)
			if err != nil {
				return fmt.Errorf("genesis migration failed: %w", err)
			}

			fmt.Printf("Genesis migration completed: %s\n", result.Message)
			return nil
		},
	}

	cmd.Flags().StringVar(&source, "source", "", "Source genesis file")
	cmd.Flags().StringVar(&target, "target", "", "Target genesis file")
	cmd.Flags().StringVar(&chainID, "chain-id", "", "New chain ID")
	cmd.Flags().BoolVar(&updateTime, "update-time", false, "Update genesis time to current time")

	return cmd
}

func validateGenesisCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "validate [genesis-file]",
		Short: "Validate genesis file",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			genesisFile := args[0]

			genesisHandler := handlers.NewGenesisHandler(genesisFile)

			stepConfig := types.NewTypedMap()
			stepConfig.Set("source_genesis", types.NewValue(types.ValueTypeString, []byte(genesisFile)))
			step := &migration.MigrationStep{
				Config: stepConfig,
			}

			ctx := &migration.MigrationContext{
				DryRun: true,
			}

			if err := genesisHandler.Validate(ctx, step); err != nil {
				return fmt.Errorf("genesis validation failed: %w", err)
			}

			fmt.Printf("Genesis file %s is valid\n", genesisFile)
			return nil
		},
	}
}

// Helper functions

func createMigrationManager() (*migration.MigrationManager, error) {
	// Create logger
	logLevel := "info"
	if verbose {
		logLevel = "debug"
	}

	// Create logger adapter that implements common.StructuredLogger
	logrusLogger := logrus.New()
	level, err := logrus.ParseLevel(logLevel)
	if err != nil {
		level = logrus.InfoLevel
	}
	logrusLogger.SetLevel(level)

	// Create adapter
	log := &loggerAdapter{logger: logrusLogger}

	// Create storage
	storageDir := filepath.Join(workingDir, "migrations")
	storage, err := migration.NewFileMigrationStorage(storageDir)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage: %w", err)
	}

	// Create configuration
	config := &migration.MigrationConfig{
		WorkingDirectory: workingDir,
		BackupDirectory:  backupDir,
		MaxConcurrent:    1,
		DefaultTimeout:   timeout,
		EnableBackups:    !skipBackup,
		EnableDryRun:     dryRun,
		RetryPolicy: &migration.RetryPolicy{
			MaxRetries:   3,
			InitialDelay: 1 * time.Second,
			MaxDelay:     30 * time.Second,
			Multiplier:   2.0,
		},
		ValidationMode: "strict",
	}

	// Create migration manager
	manager := migration.NewMigrationManager(config, storage, log)

	// Register handlers
	registerHandlers(manager.GetRegistry())

	return manager, nil
}

func registerHandlers(registry *migration.MigrationRegistry) {
	// Register genesis handler
	genesisHandler := handlers.NewGenesisHandler("./config/genesis.json")
	registry.RegisterHandler("genesis", genesisHandler)

	// Register state handler (would need actual database implementation)
	// stateHandler := handlers.NewStateHandler("./data", nil, nil)
	// registry.RegisterHandler("state", stateHandler)
}

func printPlanDetails(plan *migration.MigrationPlan) {
	fmt.Printf("Migration Plan Details:\n")
	fmt.Printf("  ID: %s\n", plan.ID)
	fmt.Printf("  Name: %s\n", plan.Name)
	fmt.Printf("  Description: %s\n", plan.Description)
	fmt.Printf("  From Version: %s\n", plan.FromVersion)
	fmt.Printf("  To Version: %s\n", plan.ToVersion)
	fmt.Printf("  Status: %s\n", plan.Status)
	fmt.Printf("  Created: %s\n", plan.CreatedAt.Format(time.RFC3339))
	fmt.Printf("  Updated: %s\n", plan.UpdatedAt.Format(time.RFC3339))

	if len(plan.MigrationSteps) > 0 {
		fmt.Printf("\nMigration Steps (%d):\n", len(plan.MigrationSteps))
		for i, step := range plan.MigrationSteps {
			fmt.Printf("  %d. %s (%s) - %s\n", i+1, step.Name, step.Type, step.Status)
		}
	}

	if len(plan.ValidationSteps) > 0 {
		fmt.Printf("\nValidation Steps (%d):\n", len(plan.ValidationSteps))
		for i, step := range plan.ValidationSteps {
			fmt.Printf("  %d. %s (%s) - %s\n", i+1, step.Name, step.Type, step.Status)
		}
	}
}

func printMigrationProgress(plan *migration.MigrationPlan) {
	completed := 0
	total := len(plan.MigrationSteps)

	for _, step := range plan.MigrationSteps {
		if step.Status == migration.StepStatusCompleted {
			completed++
		}
	}

	if total > 0 {
		progress := float64(completed) / float64(total) * 100
		fmt.Printf("Progress: %d/%d steps completed (%.1f%%)\n", completed, total, progress)
	}

	// Show current step details
	for i, step := range plan.MigrationSteps {
		statusSymbol := ""
		switch step.Status {
		case migration.StepStatusCompleted:
			statusSymbol = "✓"
		case migration.StepStatusRunning:
			statusSymbol = "⏳"
		case migration.StepStatusFailed:
			statusSymbol = "✗"
		case migration.StepStatusPending:
			statusSymbol = "○"
		default:
			statusSymbol = "?"
		}

		fmt.Printf("  %s Step %d: %s\n", statusSymbol, i+1, step.Name)

		if step.Status == migration.StepStatusFailed && step.Error != "" {
			fmt.Printf("    Error: %s\n", step.Error)
		}
	}
}

// loggerAdapter adapts logrus.Logger to common.StructuredLogger interface
type loggerAdapter struct {
	logger *logrus.Logger
}

func (l *loggerAdapter) Trace(msg string, fields ...common.LogField) {
	l.logger.Trace(msg)
}

func (l *loggerAdapter) Debug(msg string, fields ...common.LogField) {
	l.logger.Debug(msg)
}

func (l *loggerAdapter) Info(msg string, fields ...common.LogField) {
	l.logger.Info(msg)
}

func (l *loggerAdapter) Warn(msg string, fields ...common.LogField) {
	l.logger.Warn(msg)
}

func (l *loggerAdapter) Error(msg string, fields ...common.LogField) {
	l.logger.Error(msg)
}

func (l *loggerAdapter) Fatal(msg string, fields ...common.LogField) {
	l.logger.Fatal(msg)
}

func (l *loggerAdapter) TraceContext(ctx context.Context, msg string, fields ...common.LogField) {
	l.logger.Trace(msg)
}

func (l *loggerAdapter) DebugContext(ctx context.Context, msg string, fields ...common.LogField) {
	l.logger.Debug(msg)
}

func (l *loggerAdapter) InfoContext(ctx context.Context, msg string, fields ...common.LogField) {
	l.logger.Info(msg)
}

func (l *loggerAdapter) WarnContext(ctx context.Context, msg string, fields ...common.LogField) {
	l.logger.Warn(msg)
}

func (l *loggerAdapter) ErrorContext(ctx context.Context, msg string, fields ...common.LogField) {
	l.logger.Error(msg)
}

func (l *loggerAdapter) FatalContext(ctx context.Context, msg string, fields ...common.LogField) {
	l.logger.Fatal(msg)
}

func (l *loggerAdapter) WithFields(fields ...common.LogField) common.StructuredLogger {
	// Convert LogFields to logrus fields
	logrusFields := make(logrus.Fields)
	for _, field := range fields {
		logrusFields[field.Key] = field.Value
	}
	return &loggerAdapter{logger: l.logger.WithFields(logrusFields).Logger}
}

func (l *loggerAdapter) WithContext(ctx context.Context) common.StructuredLogger {
	return l
}

func (l *loggerAdapter) SetLevel(level common.LogLevel) {
	var logrusLevel logrus.Level
	switch level {
	case common.LevelTrace:
		logrusLevel = logrus.TraceLevel
	case common.LevelDebug:
		logrusLevel = logrus.DebugLevel
	case common.LevelInfo:
		logrusLevel = logrus.InfoLevel
	case common.LevelWarn:
		logrusLevel = logrus.WarnLevel
	case common.LevelError:
		logrusLevel = logrus.ErrorLevel
	case common.LevelFatal:
		logrusLevel = logrus.FatalLevel
	default:
		logrusLevel = logrus.InfoLevel
	}
	l.logger.SetLevel(logrusLevel)
}

func (l *loggerAdapter) SetOutput(output io.Writer) {
	l.logger.SetOutput(output)
}
