package main

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// DatabaseInterface defines the contract for database operations
type DatabaseInterface interface {
	Connect(ctx context.Context, connectionString string) error
	Close() error
	ExecuteProcedure(ctx context.Context, packageName, procedureName, solID string) error
	QueryData(ctx context.Context, query string, params ...interface{}) (*sql.Rows, error)
	GetStats() DatabaseStats
}

// ConfigurationInterface defines the contract for configuration management
type ConfigurationInterface interface {
	LoadMainConfig(path string) (MainConfig, error)
	LoadExtractionConfig(path string) (ExtractionConfig, error)
	ValidateConfig() error
	GetConnectionString() string
}

// LoggerInterface defines the contract for logging operations
type LoggerInterface interface {
	Info(msg string, args ...any)
	Debug(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
	With(args ...any) LoggerInterface
	WithContext(ctx context.Context) LoggerInterface
}

// ProcessorInterface defines the contract for data processing operations
type ProcessorInterface interface {
	ProcessSOL(ctx context.Context, solID string) Result[ProcessingResult]
	ProcessBatch(ctx context.Context, solIDs []string) []Result[ProcessingResult]
	GetMetrics() ProcessingMetrics
}

// ExtractionInterface defines the contract for data extraction operations
type ExtractionInterface interface {
	ExtractData(ctx context.Context, solID, procedure string) Result[ExtractionResult]
	MergeFiles(procedures []string) error
	ValidateExtraction(solID, procedure string) error
}

// FileWriterInterface defines the contract for file writing operations
type FileWriterInterface interface {
	WriteRecord(record DatabaseRecord) error
	WriteRecords(records []DatabaseRecord) error
	Flush() error
	Close() error
}

// ProgressTrackerInterface defines the contract for progress tracking
type ProgressTrackerInterface interface {
	Start(total int, description string)
	Update(completed int)
	Finish()
	GetProgress() ProgressInfo
}

// MetricsCollectorInterface defines the contract for metrics collection
type MetricsCollectorInterface interface {
	RecordProcedureCall(duration time.Duration, success bool)
	RecordSOLComplete()
	UpdateConcurrency(current int)
	GetMetrics() MetricsSnapshot
}

// Supporting types for interfaces

// ProcessingResult represents the result of processing a SOL
type ProcessingResult struct {
	SOLID       string
	Procedures  []string
	RecordsProcessed int
	Duration    time.Duration
	Success     bool
	Errors      []error
}

// ExtractionResult represents the result of data extraction
type ExtractionResult struct {
	SOLID        string
	Procedure    string
	RecordsExtracted int
	OutputFile   string
	Duration     time.Duration
}

// ProcessingMetrics contains metrics about processing operations
type ProcessingMetrics struct {
	TotalSOLs        int
	CompletedSOLs    int
	FailedSOLs       int
	AverageDuration  time.Duration
	ThroughputPerMin float64
}

// ProgressInfo contains information about current progress
type ProgressInfo struct {
	Total       int
	Completed   int
	Percentage  float64
	ETA         time.Duration
	Description string
}

// MetricsSnapshot represents a point-in-time view of metrics
type MetricsSnapshot struct {
	Timestamp        time.Time
	SOLsProcessed    int64
	ProceduresCalled int64
	ErrorCount       int64
	AverageResponse  time.Duration
	ThroughputPerMin float64
	ErrorRatePercent float64
}

// Real implementations of interfaces

// DatabaseManager implements DatabaseInterface
type DatabaseManager struct {
	db           *sql.DB
	logger       LoggerInterface
	connectionString string
}

// NewDatabaseManager creates a new database manager
func NewDatabaseManager(logger LoggerInterface) *DatabaseManager {
	return &DatabaseManager{
		logger: logger,
	}
}

// Connect implements DatabaseInterface.Connect
func (dm *DatabaseManager) Connect(ctx context.Context, connectionString string) error {
	db, err := sql.Open("godror", connectionString)
	if err != nil {
		return NewDatabaseError("connect", "failed to open database connection", err)
	}
	
	// Test the connection
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return NewDatabaseError("ping", "failed to ping database", err)
	}
	
	dm.db = db
	dm.connectionString = connectionString
	dm.logger.Info("database connected successfully")
	
	return nil
}

// Close implements DatabaseInterface.Close
func (dm *DatabaseManager) Close() error {
	if dm.db != nil {
		err := dm.db.Close()
		if err != nil {
			return NewDatabaseError("close", "failed to close database connection", err)
		}
		dm.logger.Info("database connection closed")
	}
	return nil
}

// ExecuteProcedure implements DatabaseInterface.ExecuteProcedure
func (dm *DatabaseManager) ExecuteProcedure(ctx context.Context, packageName, procedureName, solID string) error {
	if dm.db == nil {
		return NewDatabaseError("execute", "database connection is nil", nil)
	}
	
	query := fmt.Sprintf("BEGIN %s.%s(:1); END;", packageName, procedureName)
	start := time.Now()
	
	_, err := dm.db.ExecContext(ctx, query, solID)
	duration := time.Since(start)
	
	if err != nil {
		dm.logger.Error("procedure execution failed",
			"package", packageName,
			"procedure", procedureName,
			"sol_id", solID,
			"duration", duration,
			"error", err.Error())
		return NewDatabaseError("execute_procedure", 
			fmt.Sprintf("failed to execute %s.%s for SOL %s", packageName, procedureName, solID), err)
	}
	
	dm.logger.Debug("procedure executed successfully",
		"package", packageName,
		"procedure", procedureName,
		"sol_id", solID,
		"duration", duration)
	
	return nil
}

// QueryData implements DatabaseInterface.QueryData
func (dm *DatabaseManager) QueryData(ctx context.Context, query string, params ...interface{}) (*sql.Rows, error) {
	if dm.db == nil {
		return nil, NewDatabaseError("query", "database connection is nil", nil)
	}
	
	rows, err := dm.db.QueryContext(ctx, query, params...)
	if err != nil {
		return nil, NewDatabaseError("query", "failed to execute query", err)
	}
	
	return rows, nil
}

// GetStats implements DatabaseInterface.GetStats
func (dm *DatabaseManager) GetStats() DatabaseStats {
	if dm.db == nil {
		return DatabaseStats{}
	}
	
	stats := dm.db.Stats()
	return DatabaseStats{
		MaxOpenConnections: stats.MaxOpenConnections,
		OpenConnections:    stats.OpenConnections,
		InUse:             stats.InUse,
		Idle:              stats.Idle,
		WaitCount:         stats.WaitCount,
		WaitDuration:      stats.WaitDuration,
	}
}

// SOLProcessor implements ProcessorInterface
type SOLProcessor struct {
	db       DatabaseInterface
	extractor ExtractionInterface
	logger   LoggerInterface
	config   *ExtractionConfig
}

// NewSOLProcessor creates a new SOL processor
func NewSOLProcessor(db DatabaseInterface, extractor ExtractionInterface, logger LoggerInterface, config *ExtractionConfig) *SOLProcessor {
	return &SOLProcessor{
		db:       db,
		extractor: extractor,
		logger:   logger,
		config:   config,
	}
}

// ProcessSOL implements ProcessorInterface.ProcessSOL
func (sp *SOLProcessor) ProcessSOL(ctx context.Context, solID string) Result[ProcessingResult] {
	start := time.Now()
	result := ProcessingResult{
		SOLID:      solID,
		Procedures: sp.config.Procedures,
		Success:    true,
		Errors:     make([]error, 0),
	}
	
	sp.logger.Info("processing SOL", "sol_id", solID)
	
	// Process each procedure for the SOL
	for _, procedure := range sp.config.Procedures {
		extractResult := sp.extractor.ExtractData(ctx, solID, procedure)
		if extractResult.IsError() {
			result.Success = false
			result.Errors = append(result.Errors, extractResult.Error)
			sp.logger.Error("procedure failed", 
				"sol_id", solID, 
				"procedure", procedure, 
				"error", extractResult.Error.Error())
		} else {
			result.RecordsProcessed += extractResult.Value.RecordsExtracted
		}
	}
	
	result.Duration = time.Since(start)
	
	if result.Success {
		sp.logger.Info("SOL processed successfully", 
			"sol_id", solID, 
			"duration", result.Duration,
			"records", result.RecordsProcessed)
		return Success(result)
	} else {
		sp.logger.Error("SOL processing failed", 
			"sol_id", solID, 
			"duration", result.Duration,
			"error_count", len(result.Errors))
		return Error[ProcessingResult](NewProcessingError("process_sol", solID, "", result.Errors[0]))
	}
}

// ProcessBatch implements ProcessorInterface.ProcessBatch
func (sp *SOLProcessor) ProcessBatch(ctx context.Context, solIDs []string) []Result[ProcessingResult] {
	results := make([]Result[ProcessingResult], len(solIDs))
	
	for i, solID := range solIDs {
		results[i] = sp.ProcessSOL(ctx, solID)
	}
	
	return results
}

// GetMetrics implements ProcessorInterface.GetMetrics
func (sp *SOLProcessor) GetMetrics() ProcessingMetrics {
	// This would typically aggregate metrics from a metrics collector
	return ProcessingMetrics{}
}