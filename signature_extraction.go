package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// SignatureConfig defines configuration for signature extraction
type SignatureConfig struct {
	Query           string `json:"signature_query"`           // SQL query to extract signature BLOBs
	OutputDirectory string `json:"signature_output_directory"` // Directory to save signature images
	FileNameColumn  string `json:"filename_column"`           // Column containing filename or identifier
	FileNamePrefix  string `json:"filename_prefix"`           // Prefix for generated filenames
	ImageFormat     string `json:"image_format,omitempty"`    // Expected image format (jpg, png, gif, etc.) - auto-detect if empty
	CreateSubdirs   bool   `json:"create_subdirectories"`     // Create subdirectories per SOL ID
}

// SignatureExtractor handles BLOB signature extraction
type SignatureExtractor struct {
	db     DatabaseInterface
	logger LoggerInterface
	config *SignatureConfig
}

// SignatureResult represents the result of signature extraction
type SignatureResult struct {
	SOLID        string
	Filename     string
	FilePath     string
	ImageFormat  string
	FileSize     int64
	Success      bool
	Error        error
	Duration     time.Duration
}

// NewSignatureExtractor creates a new signature extractor
func NewSignatureExtractor(db DatabaseInterface, logger LoggerInterface, config *SignatureConfig) *SignatureExtractor {
	return &SignatureExtractor{
		db:     db,
		logger: logger,
		config: config,
	}
}

// ExtractSignatures extracts signature images for a list of SOL IDs
func (se *SignatureExtractor) ExtractSignatures(ctx context.Context, solIDs []string) []Result[SignatureResult] {
	results := make([]Result[SignatureResult], 0, len(solIDs))
	
	se.logger.Info("starting signature extraction", 
		"sol_count", len(solIDs), 
		"output_dir", se.config.OutputDirectory)

	// Ensure output directory exists
	if err := os.MkdirAll(se.config.OutputDirectory, 0755); err != nil {
		extractionErr := NewApplicationError("SIGNATURE_DIR_ERROR", 
			"failed to create signature output directory", err)
		
		// Return error for all SOLs if directory creation fails
		for range solIDs {
			results = append(results, Error[SignatureResult](extractionErr))
		}
		return results
	}

	for _, solID := range solIDs {
		result := se.ExtractSignatureForSOL(ctx, solID)
		results = append(results, result)
	}

	return results
}

// ExtractSignatureForSOL extracts signature image for a single SOL ID
func (se *SignatureExtractor) ExtractSignatureForSOL(ctx context.Context, solID string) Result[SignatureResult] {
	start := time.Now()
	
	se.logger.Debug("extracting signature for SOL", "sol_id", solID)
	
	// Execute the signature query with SOL ID parameter
	rows, err := se.db.QueryData(ctx, se.config.Query, solID)
	if err != nil {
		return se.createErrorResult(solID, "SIGNATURE_QUERY_ERROR", 
			"failed to execute signature query", err, start)
	}
	defer rows.Close()

	// Process results - expect one row per signature
	if !rows.Next() {
		se.logger.Warn("no signature found for SOL", "sol_id", solID)
		return se.createErrorResult(solID, "SIGNATURE_NOT_FOUND", 
			"no signature data found for SOL", nil, start)
	}

	// Get column information
	columns, err := rows.Columns()
	if err != nil {
		return se.createErrorResult(solID, "SIGNATURE_COLUMNS_ERROR", 
			"failed to get result columns", err, start)
	}

	// Find BLOB column and filename column
	blobColumnIndex := -1
	filenameColumnIndex := -1
	
	for i, col := range columns {
		colLower := strings.ToLower(col)
		if strings.Contains(colLower, "signature") || strings.Contains(colLower, "image") || strings.Contains(colLower, "blob") {
			blobColumnIndex = i
		}
		if se.config.FileNameColumn != "" && strings.EqualFold(col, se.config.FileNameColumn) {
			filenameColumnIndex = i
		}
	}

	if blobColumnIndex == -1 {
		return se.createErrorResult(solID, "SIGNATURE_BLOB_COLUMN_ERROR", 
			"no BLOB column found in query results", nil, start)
	}

	// Prepare scan destinations
	values := make([]interface{}, len(columns))
	scanArgs := make([]interface{}, len(columns))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	// Scan the row
	if err := rows.Scan(scanArgs...); err != nil {
		return se.createErrorResult(solID, "SIGNATURE_SCAN_ERROR", 
			"failed to scan signature row", err, start)
	}

	// Extract BLOB data
	var blobData []byte
	if values[blobColumnIndex] != nil {
		switch v := values[blobColumnIndex].(type) {
		case []byte:
			blobData = v
		case string:
			blobData = []byte(v)
		default:
			return se.createErrorResult(solID, "SIGNATURE_BLOB_TYPE_ERROR", 
				"unexpected BLOB data type", nil, start)
		}
	}

	if len(blobData) == 0 {
		return se.createErrorResult(solID, "SIGNATURE_EMPTY_BLOB", 
			"signature BLOB is empty", nil, start)
	}

	// Determine filename
	filename := se.generateFilename(solID, values, filenameColumnIndex, blobData)
	
	// Determine output path
	outputPath := se.getOutputPath(solID, filename)
	
	// Write the image file
	fileSize, err := se.writeImageFile(outputPath, blobData)
	if err != nil {
		return se.createErrorResult(solID, "SIGNATURE_WRITE_ERROR", 
			"failed to write signature file", err, start)
	}

	// Detect image format
	imageFormat := se.detectImageFormat(blobData)
	
	duration := time.Since(start)
	se.logger.Info("signature extracted successfully", 
		"sol_id", solID, 
		"filename", filename,
		"file_size", fileSize,
		"format", imageFormat,
		"duration", duration)

	result := SignatureResult{
		SOLID:       solID,
		Filename:    filename,
		FilePath:    outputPath,
		ImageFormat: imageFormat,
		FileSize:    fileSize,
		Success:     true,
		Duration:    duration,
	}

	return Success(result)
}

// generateFilename creates a filename for the signature image
func (se *SignatureExtractor) generateFilename(solID string, values []interface{}, filenameColumnIndex int, blobData []byte) string {
	var baseName string
	
	// Use filename from database column if available
	if filenameColumnIndex >= 0 && values[filenameColumnIndex] != nil {
		if filename, ok := values[filenameColumnIndex].(string); ok && filename != "" {
			baseName = strings.TrimSpace(filename)
			// Remove extension if present - we'll add the correct one
			if ext := filepath.Ext(baseName); ext != "" {
				baseName = strings.TrimSuffix(baseName, ext)
			}
		}
	}
	
	// Fall back to SOL ID if no filename column or empty
	if baseName == "" {
		baseName = solID
	}
	
	// Add prefix if configured
	if se.config.FileNamePrefix != "" {
		baseName = se.config.FileNamePrefix + "_" + baseName
	}
	
	// Determine file extension
	var ext string
	if se.config.ImageFormat != "" {
		ext = "." + strings.ToLower(se.config.ImageFormat)
	} else {
		// Auto-detect format from BLOB data
		ext = se.getFileExtensionFromBlob(blobData)
	}
	
	return baseName + ext
}

// getOutputPath determines the full output path for the signature file
func (se *SignatureExtractor) getOutputPath(solID, filename string) string {
	if se.config.CreateSubdirs {
		// Create subdirectory for each SOL ID
		subdir := filepath.Join(se.config.OutputDirectory, solID)
		os.MkdirAll(subdir, 0755) // Create if doesn't exist
		return filepath.Join(subdir, filename)
	}
	return filepath.Join(se.config.OutputDirectory, filename)
}

// writeImageFile writes the BLOB data to a file
func (se *SignatureExtractor) writeImageFile(filePath string, blobData []byte) (int64, error) {
	file, err := os.Create(filePath)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	bytesWritten, err := io.Copy(file, bytes.NewReader(blobData))
	if err != nil {
		return 0, err
	}

	return bytesWritten, nil
}

// detectImageFormat detects image format from BLOB data
func (se *SignatureExtractor) detectImageFormat(data []byte) string {
	if len(data) < 4 {
		return "unknown"
	}

	// Check for common image format signatures
	switch {
	case bytes.HasPrefix(data, []byte{0xFF, 0xD8, 0xFF}):
		return "jpg"
	case bytes.HasPrefix(data, []byte{0x89, 0x50, 0x4E, 0x47}):
		return "png"
	case bytes.HasPrefix(data, []byte{0x47, 0x49, 0x46, 0x38}):
		return "gif"
	case bytes.HasPrefix(data, []byte{0x42, 0x4D}):
		return "bmp"
	case bytes.HasPrefix(data, []byte{0x49, 0x49, 0x2A, 0x00}) || bytes.HasPrefix(data, []byte{0x4D, 0x4D, 0x00, 0x2A}):
		return "tiff"
	default:
		return "unknown"
	}
}

// getFileExtensionFromBlob determines file extension from BLOB data
func (se *SignatureExtractor) getFileExtensionFromBlob(data []byte) string {
	format := se.detectImageFormat(data)
	switch format {
	case "jpg":
		return ".jpg"
	case "png":
		return ".png"
	case "gif":
		return ".gif"
	case "bmp":
		return ".bmp"
	case "tiff":
		return ".tiff"
	default:
		return ".bin" // Unknown format, save as binary
	}
}

// createErrorResult creates a standardized error result
func (se *SignatureExtractor) createErrorResult(solID, code, message string, cause error, start time.Time) Result[SignatureResult] {
	duration := time.Since(start)
	appErr := NewApplicationError(code, message, cause).
		WithContext("sol_id", solID).
		WithContext("duration", duration)
	
	se.logger.Error("signature extraction failed", 
		"sol_id", solID, 
		"error", appErr.Error(),
		"duration", duration)

	return Error[SignatureResult](appErr)
}

// ValidateSignatureConfig validates the signature extraction configuration
func ValidateSignatureConfig(config *SignatureConfig) error {
	if config.Query == "" {
		return NewValidationError("signature_query", "signature query cannot be empty", nil)
	}
	
	if config.OutputDirectory == "" {
		return NewValidationError("signature_output_directory", "output directory cannot be empty", nil)
	}
	
	// Validate query contains parameter placeholder
	if !strings.Contains(strings.ToUpper(config.Query), ":1") && 
	   !strings.Contains(strings.ToUpper(config.Query), "?") {
		log.Printf("⚠️  Warning: Signature query doesn't appear to contain parameter placeholder. Expected :1 or ?")
	}
	
	// Validate image format if specified
	if config.ImageFormat != "" {
		validFormats := []string{"jpg", "jpeg", "png", "gif", "bmp", "tiff"}
		format := strings.ToLower(config.ImageFormat)
		valid := false
		for _, validFormat := range validFormats {
			if format == validFormat {
				valid = true
				break
			}
		}
		if !valid {
			return NewValidationError("image_format", 
				fmt.Sprintf("unsupported image format '%s'. Supported: %v", 
					config.ImageFormat, validFormats), nil)
		}
	}
	
	return nil
}