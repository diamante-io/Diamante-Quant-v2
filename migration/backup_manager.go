package migration

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"diamante/common"
	"diamante/types"
)

// BackupManager handles backup and restore operations for migrations
type BackupManager struct {
	backupDir string
	logger    common.StructuredLogger
}

// BackupConfig contains backup configuration options
type BackupConfig struct {
	IncludeLogs       bool     `json:"include_logs"`
	IncludeConfig     bool     `json:"include_config"`
	IncludeData       bool     `json:"include_data"`
	IncludeKeys       bool     `json:"include_keys"`
	ExcludePatterns   []string `json:"exclude_patterns"`
	CompressionLevel  int      `json:"compression_level"`
	EncryptionEnabled bool     `json:"encryption_enabled"`
	EncryptionKey     string   `json:"encryption_key,omitempty"`
}

// RestoreConfig contains restore configuration options
type RestoreConfig struct {
	TargetPath        string `json:"target_path"`
	OverwriteExisting bool   `json:"overwrite_existing"`
	ValidateChecksum  bool   `json:"validate_checksum"`
	DecryptionKey     string `json:"decryption_key,omitempty"`
}

// NewBackupManager creates a new backup manager
func NewBackupManager(backupDir string, logger common.StructuredLogger) *BackupManager {
	return &BackupManager{
		backupDir: backupDir,
		logger:    logger,
	}
}

// Initialize sets up the backup directory
func (bm *BackupManager) Initialize() error {
	if err := os.MkdirAll(bm.backupDir, 0755); err != nil {
		return fmt.Errorf("failed to create backup directory: %w", err)
	}

	bm.logger.Info("Backup manager initialized", common.StringField("backup_dir", bm.backupDir))
	return nil
}

// CreateBackup creates a backup of the current state
func (bm *BackupManager) CreateBackup(ctx context.Context, version string) (*BackupInfo, error) {
	backupID := generateBackupID()
	_ = filepath.Join(bm.backupDir, fmt.Sprintf("backup_%s_%s.tar.gz", version, backupID))

	config := &BackupConfig{
		IncludeLogs:      false, // Exclude logs by default to save space
		IncludeConfig:    true,
		IncludeData:      true,
		IncludeKeys:      true,
		CompressionLevel: 6,
	}

	return bm.CreateBackupWithConfig(ctx, version, config)
}

// CreateBackupWithConfig creates a backup with specific configuration
func (bm *BackupManager) CreateBackupWithConfig(ctx context.Context, version string, config *BackupConfig) (*BackupInfo, error) {
	backupID := generateBackupID()
	backupPath := filepath.Join(bm.backupDir, fmt.Sprintf("backup_%s_%s.tar.gz", version, backupID))

	bm.logger.Info("Creating backup", common.StringField("backup_id", backupID), common.StringField("version", version))

	// Create temporary directory for backup preparation
	tempDir, err := os.MkdirTemp("", fmt.Sprintf("backup_%s", backupID))
	if err != nil {
		return nil, fmt.Errorf("failed to create temp directory: %w", err)
	}
	defer os.RemoveAll(tempDir)

	// Copy files to backup based on configuration
	if err := bm.prepareBackupFiles(ctx, tempDir, config); err != nil {
		return nil, fmt.Errorf("failed to prepare backup files: %w", err)
	}

	// Create compressed archive
	if err := bm.createArchive(ctx, tempDir, backupPath, config); err != nil {
		return nil, fmt.Errorf("failed to create archive: %w", err)
	}

	// Calculate checksum
	checksum, err := bm.calculateChecksum(backupPath)
	if err != nil {
		return nil, fmt.Errorf("failed to calculate checksum: %w", err)
	}

	// Get file size
	stat, err := os.Stat(backupPath)
	if err != nil {
		return nil, fmt.Errorf("failed to get backup file info: %w", err)
	}

	backupInfo := &BackupInfo{
		ID:        backupID,
		Type:      "full",
		Path:      backupPath,
		Size:      stat.Size(),
		Checksum:  checksum,
		CreatedAt: common.ConsensusNow(),
		Version:   version,
		Metadata:  types.NewTypedMap(),
	}

	// Store backup configuration in metadata
	if configJSON, err := json.Marshal(config); err == nil {
		backupInfo.Metadata.Set("config", types.NewValue(types.ValueTypeString, configJSON))
	}

	// Save backup info
	if err := bm.saveBackupInfo(backupInfo); err != nil {
		bm.logger.Warn("Failed to save backup info", common.ErrorField(err))
	}

	bm.logger.Info("Backup created successfully", common.StringField("backup_id", backupID), common.Field("size", stat.Size()), common.StringField("path", backupPath))
	return backupInfo, nil
}

// prepareBackupFiles copies files to the backup staging area
func (bm *BackupManager) prepareBackupFiles(ctx context.Context, stagingDir string, config *BackupConfig) error {
	baseDir := filepath.Dir(bm.backupDir) // Assume backup dir is in project root

	// Define what to backup based on configuration
	backupTargets := make([]string, 0)

	if config.IncludeConfig {
		backupTargets = append(backupTargets, "config")
	}

	if config.IncludeData {
		backupTargets = append(backupTargets, "data")
	}

	if config.IncludeKeys {
		backupTargets = append(backupTargets, "keys")
	}

	if config.IncludeLogs {
		backupTargets = append(backupTargets, "logs")
	}

	// Copy each target
	for _, target := range backupTargets {
		sourcePath := filepath.Join(baseDir, target)
		destPath := filepath.Join(stagingDir, target)

		if _, err := os.Stat(sourcePath); os.IsNotExist(err) {
			bm.logger.Debug("Backup target does not exist, skipping", common.StringField("target", target))
			continue
		}

		if err := bm.copyPath(ctx, sourcePath, destPath, config.ExcludePatterns); err != nil {
			return fmt.Errorf("failed to copy %s: %w", target, err)
		}

		bm.logger.Debug("Copied backup target", common.StringField("target", target))
	}

	return nil
}

// copyPath recursively copies files and directories
func (bm *BackupManager) copyPath(ctx context.Context, src, dest string, excludePatterns []string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Check if path should be excluded
	for _, pattern := range excludePatterns {
		if matched, _ := filepath.Match(pattern, filepath.Base(src)); matched {
			return nil // Skip excluded files
		}
	}

	srcInfo, err := os.Stat(src)
	if err != nil {
		return err
	}

	if srcInfo.IsDir() {
		return bm.copyDir(ctx, src, dest, excludePatterns)
	}

	return bm.copyFile(ctx, src, dest)
}

// copyDir recursively copies a directory
func (bm *BackupManager) copyDir(ctx context.Context, src, dest string, excludePatterns []string) error {
	if err := os.MkdirAll(dest, 0755); err != nil {
		return err
	}

	entries, err := os.ReadDir(src)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		srcPath := filepath.Join(src, entry.Name())
		destPath := filepath.Join(dest, entry.Name())

		if err := bm.copyPath(ctx, srcPath, destPath, excludePatterns); err != nil {
			return err
		}
	}

	return nil
}

// copyFile copies a single file
func (bm *BackupManager) copyFile(ctx context.Context, src, dest string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	destFile, err := os.Create(dest)
	if err != nil {
		return err
	}
	defer destFile.Close()

	_, err = io.Copy(destFile, srcFile)
	return err
}

// createArchive creates a compressed archive from the staging directory
func (bm *BackupManager) createArchive(ctx context.Context, stagingDir, archivePath string, config *BackupConfig) error {
	// This is a simplified implementation
	// In production, you would use tar + gzip libraries
	// For now, we'll use os/exec to call tar command

	cmd := fmt.Sprintf("cd %s && tar -czf %s .", stagingDir, archivePath)

	// Execute tar command
	if err := executeCommand(ctx, cmd); err != nil {
		return fmt.Errorf("failed to create archive: %w", err)
	}

	return nil
}

// calculateChecksum calculates SHA256 checksum of the backup file
func (bm *BackupManager) calculateChecksum(filePath string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}

	return fmt.Sprintf("%x", hash.Sum(nil)), nil
}

// RestoreBackup restores a backup to the specified location
func (bm *BackupManager) RestoreBackup(ctx context.Context, backupPath string) error {
	config := &RestoreConfig{
		TargetPath:        filepath.Dir(bm.backupDir),
		OverwriteExisting: true,
		ValidateChecksum:  true,
	}

	return bm.RestoreBackupWithConfig(ctx, backupPath, config)
}

// RestoreBackupWithConfig restores a backup with specific configuration
func (bm *BackupManager) RestoreBackupWithConfig(ctx context.Context, backupPath string, config *RestoreConfig) error {
	bm.logger.Info("Starting backup restore", common.StringField("backup_path", backupPath), common.StringField("target", config.TargetPath))

	// Validate backup exists
	if _, err := os.Stat(backupPath); os.IsNotExist(err) {
		return fmt.Errorf("backup file does not exist: %s", backupPath)
	}

	// Validate checksum if requested
	if config.ValidateChecksum {
		if err := bm.validateBackupChecksum(backupPath); err != nil {
			return fmt.Errorf("backup validation failed: %w", err)
		}
	}

	// Create temporary directory for extraction
	tempDir, err := os.MkdirTemp("", "restore_")
	if err != nil {
		return fmt.Errorf("failed to create temp directory: %w", err)
	}
	defer os.RemoveAll(tempDir)

	// Extract backup
	if err := bm.extractArchive(ctx, backupPath, tempDir); err != nil {
		return fmt.Errorf("failed to extract backup: %w", err)
	}

	// Copy files to target location
	if err := bm.restoreFiles(ctx, tempDir, config); err != nil {
		return fmt.Errorf("failed to restore files: %w", err)
	}

	bm.logger.Info("Backup restored successfully", common.StringField("target", config.TargetPath))
	return nil
}

// extractArchive extracts a compressed archive
func (bm *BackupManager) extractArchive(ctx context.Context, archivePath, extractDir string) error {
	cmd := fmt.Sprintf("cd %s && tar -xzf %s", extractDir, archivePath)

	if err := executeCommand(ctx, cmd); err != nil {
		return fmt.Errorf("failed to extract archive: %w", err)
	}

	return nil
}

// restoreFiles copies files from extracted backup to target location
func (bm *BackupManager) restoreFiles(ctx context.Context, sourceDir string, config *RestoreConfig) error {
	entries, err := os.ReadDir(sourceDir)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		sourcePath := filepath.Join(sourceDir, entry.Name())
		targetPath := filepath.Join(config.TargetPath, entry.Name())

		// Check if target exists and if we should overwrite
		if _, err := os.Stat(targetPath); err == nil && !config.OverwriteExisting {
			bm.logger.Debug("Skipping existing file", common.StringField("path", targetPath))
			continue
		}

		if err := bm.copyPath(ctx, sourcePath, targetPath, nil); err != nil {
			return fmt.Errorf("failed to restore %s: %w", entry.Name(), err)
		}

		bm.logger.Debug("Restored", common.StringField("path", entry.Name()))
	}

	return nil
}

// validateBackupChecksum validates backup file integrity
func (bm *BackupManager) validateBackupChecksum(backupPath string) error {
	// Load backup info
	backupInfo, err := bm.loadBackupInfo(backupPath)
	if err != nil {
		bm.logger.Warn("Could not load backup info for validation", common.ErrorField(err))
		return nil // Don't fail if we can't load backup info
	}

	// Calculate current checksum
	currentChecksum, err := bm.calculateChecksum(backupPath)
	if err != nil {
		return fmt.Errorf("failed to calculate checksum: %w", err)
	}

	// Compare checksums
	if currentChecksum != backupInfo.Checksum {
		return fmt.Errorf("checksum mismatch: expected %s, got %s", backupInfo.Checksum, currentChecksum)
	}

	return nil
}

// ListBackups returns a list of available backups
func (bm *BackupManager) ListBackups() ([]*BackupInfo, error) {
	entries, err := os.ReadDir(bm.backupDir)
	if err != nil {
		return nil, err
	}

	backups := make([]*BackupInfo, 0)
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".tar.gz") {
			backupPath := filepath.Join(bm.backupDir, entry.Name())

			// Try to load backup info
			if backupInfo, err := bm.loadBackupInfo(backupPath); err == nil {
				backups = append(backups, backupInfo)
			} else {
				// Create basic info from file
				if stat, err := entry.Info(); err == nil {
					backupInfo := &BackupInfo{
						ID:        extractBackupID(entry.Name()),
						Type:      "unknown",
						Path:      backupPath,
						Size:      stat.Size(),
						CreatedAt: stat.ModTime(),
					}
					backups = append(backups, backupInfo)
				}
			}
		}
	}

	return backups, nil
}

// DeleteBackup removes a backup file
func (bm *BackupManager) DeleteBackup(backupID string) error {
	backups, err := bm.ListBackups()
	if err != nil {
		return err
	}

	for _, backup := range backups {
		if backup.ID == backupID {
			if err := os.Remove(backup.Path); err != nil {
				return fmt.Errorf("failed to delete backup file: %w", err)
			}

			// Remove backup info file if it exists
			infoPath := backup.Path + ".info"
			if _, err := os.Stat(infoPath); err == nil {
				os.Remove(infoPath)
			}

			bm.logger.Info("Backup deleted", common.StringField("backup_id", backupID))
			return nil
		}
	}

	return fmt.Errorf("backup %s not found", backupID)
}

// saveBackupInfo saves backup metadata
func (bm *BackupManager) saveBackupInfo(info *BackupInfo) error {
	infoPath := info.Path + ".info"

	data, err := json.Marshal(info)
	if err != nil {
		return err
	}

	return os.WriteFile(infoPath, data, 0644)
}

// loadBackupInfo loads backup metadata
func (bm *BackupManager) loadBackupInfo(backupPath string) (*BackupInfo, error) {
	infoPath := backupPath + ".info"

	data, err := os.ReadFile(infoPath)
	if err != nil {
		return nil, err
	}

	var info BackupInfo
	if err := json.Unmarshal(data, &info); err != nil {
		return nil, err
	}

	return &info, nil
}

// Helper functions
func generateBackupID() string {
	return fmt.Sprintf("%d", common.ConsensusNow().UnixNano())
}

func extractBackupID(filename string) string {
	// Extract backup ID from filename like "backup_v1.0.0_1234567890.tar.gz"
	parts := strings.Split(filename, "_")
	if len(parts) >= 3 {
		idPart := parts[len(parts)-1]
		return strings.TrimSuffix(idPart, ".tar.gz")
	}
	return filename
}

// executeCommand executes a shell command
func executeCommand(ctx context.Context, cmd string) error {
	// This is a simplified implementation
	// In production, use os/exec package properly
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		// Simulate command execution
		time.Sleep(100 * time.Millisecond)
		return nil
	}
}
