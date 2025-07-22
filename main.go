package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "github.com/godror/godror"
)

var (
	appCfgFile = new(string)
	runCfgFile = new(string)
	mode       string
	verbose    = new(bool)
	quiet      = new(bool)
)

func init() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Claude Extract - Oracle Database Extraction/Insertion Tool\n\n")
		fmt.Fprintf(os.Stderr, "USAGE:\n")
		fmt.Fprintf(os.Stderr, "  %s -appCfg <config.json> -runCfg <run.json> -mode <E|I> [OPTIONS]\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "REQUIRED FLAGS:\n")
		fmt.Fprintf(os.Stderr, "  -appCfg string\n        Path to the main application configuration file\n")
		fmt.Fprintf(os.Stderr, "  -runCfg string\n        Path to the extraction configuration file\n")
		fmt.Fprintf(os.Stderr, "  -mode string\n        Mode of operation: E (Extract), I (Insert), or S (Signature)\n\n")
		fmt.Fprintf(os.Stderr, "OPTIONS:\n")
		fmt.Fprintf(os.Stderr, "  -verbose\n        Enable verbose logging output\n")
		fmt.Fprintf(os.Stderr, "  -quiet\n        Suppress non-essential output\n")
		fmt.Fprintf(os.Stderr, "  -h, -help\n        Show this help message\n\n")
		fmt.Fprintf(os.Stderr, "EXAMPLES:\n")
		fmt.Fprintf(os.Stderr, "  # Extract data\n")
		fmt.Fprintf(os.Stderr, "  %s -appCfg app.json -runCfg extract.json -mode E\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  # Insert data with verbose output\n")
		fmt.Fprintf(os.Stderr, "  %s -appCfg app.json -runCfg insert.json -mode I -verbose\n\n", os.Args[0])
	}
	
	flag.StringVar(appCfgFile, "appCfg", "", "Path to the main application configuration file")
	flag.StringVar(runCfgFile, "runCfg", "", "Path to the extraction configuration file")
	flag.StringVar(&mode, "mode", "", "Mode of operation: E - Extract, I - Insert, S - Signature")
	flag.BoolVar(verbose, "verbose", false, "Enable verbose logging output")
	flag.BoolVar(quiet, "quiet", false, "Suppress non-essential output")
}

func parseAndValidateFlags() {
	flag.Parse()

	// Validate CLI arguments
	if mode != "E" && mode != "I" && mode != "S" {
		ExitWithError(ExitUsageError, "Invalid mode. Valid values are 'E' for Extract, 'I' for Insert, and 'S' for Signature extraction.")
	}
	if *appCfgFile == "" || *runCfgFile == "" {
		ExitWithError(ExitUsageError, "Both -appCfg and -runCfg must be specified. Use -help for usage information.")
	}
	if *verbose && *quiet {
		ExitWithError(ExitUsageError, "Cannot use both -verbose and -quiet flags simultaneously")
	}
	if _, err := os.Stat(*appCfgFile); os.IsNotExist(err) {
		ExitWithError(ExitConfigError, "Application configuration file does not exist: %s", *appCfgFile)
	}
	if _, err := os.Stat(*runCfgFile); os.IsNotExist(err) {
		ExitWithError(ExitConfigError, "Extraction configuration file does not exist: %s", *runCfgFile)
	}
}

func main() {
	parseAndValidateFlags()
	PrintBanner()
	
	// Initialize structured logger
	logger := NewLogger(*verbose, *quiet)
	
	// Initialize CLI logger for progress/banners
	cliLogger := NewCLILogger(*verbose, *quiet)
	
	cliLogger.Info("Loading configuration files...")
	logger.Info("starting claude_extract", slog.String("mode", mode))
	appCfg, err := loadMainConfig(*appCfgFile)
	if err != nil {
		ExitWithError(ExitConfigError, "Failed to load main config: %v", err)
	}
	cliLogger.Verbose("Main config loaded: %s", *appCfgFile)
	
	runCfg, err := loadExtractionConfig(*runCfgFile)
	if err != nil {
		ExitWithError(ExitConfigError, "Failed to load extraction config: %v", err)
	}
	cliLogger.Verbose("Run config loaded: %s", *runCfgFile)
	
	// Set chunked processing defaults
	setChunkedDefaults(&runCfg)
	if len(runCfg.ChunkedProcedures) > 0 {
		cliLogger.Info("Chunked procedures configured: %v (chunk size: %d)", runCfg.ChunkedProcedures, runCfg.ChunkSize)
	}

	// Load templates
	cliLogger.Info("Loading procedure templates...")
	templates := make(map[string][]ColumnConfig)
	for _, proc := range runCfg.Procedures {
		tmplPath := filepath.Join(runCfg.TemplatePath, fmt.Sprintf("%s.csv", proc))
		cols, err := readColumnsFromCSV(tmplPath)
		if err != nil {
			ExitWithError(ExitFileError, "Failed to read template for %s: %v", proc, err)
		}
		templates[proc] = cols
		cliLogger.Verbose("Template loaded for procedure: %s", proc)
	}
	cliLogger.Success("All %d procedure templates loaded", len(templates))

	// Build actual connection string with password for database connection
	actualConnString := fmt.Sprintf(`user="%s" password="%s" connectString="%s:%d/%s"`,
		appCfg.DBUser, appCfg.DBPassword, appCfg.DBHost, appCfg.DBPort, appCfg.DBSid)
	
	cliLogger.Info("Connecting to database: %s@%s:%d/%s", 
		appCfg.DBUser, appCfg.DBHost, appCfg.DBPort, appCfg.DBSid)
	logger.LogDatabaseConnection(appCfg.DBUser, appCfg.DBHost, appCfg.DBPort, appCfg.DBSid)

	db, err := sql.Open("godror", actualConnString)
	if err != nil {
		dbErr := NewDatabaseError("connection", "failed to open database connection", err)
		logger.Error("database connection failed", "error", dbErr.Error())
		ExitWithError(ExitDatabaseError, "Failed to connect to database: %v", err)
	}
	defer db.Close()

	cliLogger.Info("Reading SOL IDs from: %s", appCfg.SolFilePath)
	sols, err := readSols(appCfg.SolFilePath)
	if err != nil {
		ExitWithError(ExitFileError, "Failed to read SOL IDs: %v", err)
	}
	cliLogger.Success("Loaded %d SOL IDs", len(sols))

	procCount := len(runCfg.Procedures)
	// Optimize connection pool for high concurrency with smart scaling
	maxConns := calculateOptimalConnections(appCfg.Concurrency, procCount, len(sols))
	cliLogger.Verbose("Configuring connection pool: max=%d, idle=%d, procedures=%d, SOLs=%d", 
		maxConns, maxConns/2, procCount, len(sols))
	
	db.SetMaxOpenConns(maxConns)
	db.SetMaxIdleConns(maxConns / 2) // Reduce idle connections
	db.SetConnMaxLifetime(15 * time.Minute) // Shorter lifetime for high throughput
	db.SetConnMaxIdleTime(5 * time.Minute)  // Close idle connections faster

	// Scale buffer size based on expected load
	bufferSize := max(1000, min(50000, len(sols)*len(runCfg.Procedures)))
	procLogCh := make(chan ProcLog, bufferSize)
	var summaryMu sync.Mutex
	procSummary := make(map[string]ProcSummary)

	if (mode == "I" && !runCfg.RunInsertionParallel) || (mode == "E" && !runCfg.RunExtractionParallel) {
		cliLogger.Warning("Running procedures sequentially - parallel execution is disabled")
		appCfg.Concurrency = 1
	}

	var LogFile, LogFileSummary string
	if mode == "I" {
		LogFile = runCfg.PackageName + "_insert.csv"
		LogFileSummary = runCfg.PackageName + "_insert_summary.csv"
	} else if mode == "E" {
		LogFile = runCfg.PackageName + "_extract.csv"
		LogFileSummary = runCfg.PackageName + "_extract_summary.csv"
	} else if mode == "S" {
		LogFile = runCfg.PackageName + "_signature.csv"
		LogFileSummary = runCfg.PackageName + "_signature_summary.csv"
	}

	go writeLog(filepath.Join(appCfg.LogFilePath, LogFile), procLogCh)

	sem := make(chan struct{}, appCfg.Concurrency)
	// Setup graceful shutdown handling
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	
	// Listen for interrupt signals
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		log.Printf("🛑 Received signal %v - initiating graceful shutdown", sig)
		cancel()
	}()

	// Initialize performance metrics tracking
	metrics := NewPerformanceMetrics()
	
	// Start metrics monitoring goroutine
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				throughput := metrics.GetThroughput()
				errorRate := metrics.GetErrorRate()
				_, _, _, avgResponse, _ := metrics.GetMetrics()
				log.Printf("📊 Metrics: %.2f SOLs/min, %.2f%% error rate, %s avg response", 
					throughput, errorRate, avgResponse.Round(time.Millisecond))
			case <-ctx.Done():
				return
			}
		}
	}()

	var wg sync.WaitGroup
	totalSols := len(sols)
	overallStart := time.Now()
	var mu sync.Mutex
	completed := 0
	errorCount := 0
	
	// Initialize progress bar
	operation := "Extraction"
	if mode == "I" {
		operation = "Insertion"
	} else if mode == "S" {
		operation = "Signature Extraction"
	}
	progressBar := NewProgressBar(totalSols, fmt.Sprintf("%s Progress", operation))
	
	cliLogger.Info("Starting %s for %d SOL IDs using %d concurrent workers", 
		strings.ToLower(operation), totalSols, appCfg.Concurrency)

	if mode == "E" {
		for _, sol := range sols {
			wg.Add(1)
			sem <- struct{}{}
			go func(solID string) {
				defer wg.Done()
				defer func() { <-sem }()
				log.Printf("➡️ Starting SOL %s", solID)

				runExtractionForSol(ctx, db, solID, &runCfg, templates, procLogCh, &summaryMu, procSummary)

				mu.Lock()
				completed++
				progressBar.Update(completed)
				mu.Unlock()
			}(sol)
		}
		wg.Wait()
	} else if mode == "I" {
		if runCfg.UseProcLevelParallel {
			log.Printf("🚀 Starting procedure-level parallel execution for %d SOLs with %d procedures", totalSols, len(runCfg.Procedures))
			totalTasks := totalSols * len(runCfg.Procedures)
			log.Printf("📊 Total tasks to execute: %d (SOL-Procedure combinations)", totalTasks)
			log.Printf("🔌 Connection pool: %d max connections, %d idle connections", maxConns, maxConns/2)
			
			// Monitor connection pool stats
			go func() {
				ticker := time.NewTicker(30 * time.Second)
				defer ticker.Stop()
				for {
					select {
					case <-ticker.C:
						stats := db.Stats()
						log.Printf("📊 DB Stats: Open=%d, InUse=%d, Idle=%d, WaitCount=%d, WaitDuration=%s",
							stats.OpenConnections, stats.InUse, stats.Idle, stats.WaitCount, stats.WaitDuration)
					case <-ctx.Done():
						return
					}
				}
			}()
			
			runProceduresWithProcLevelParallelism(ctx, db, sols, &runCfg, procLogCh, &summaryMu, procSummary, appCfg.Concurrency)
			log.Printf("✅ Completed all %d tasks", totalTasks)
		} else {
			log.Printf("🚀 Starting SOL-level parallel execution (legacy mode) for %d SOLs", totalSols)
			for _, sol := range sols {
				wg.Add(1)
				sem <- struct{}{}
				go func(solID string) {
					defer wg.Done()
					defer func() { <-sem }()
					log.Printf("➡️ Starting SOL %s", solID)

					runProceduresForSol(ctx, db, solID, &runCfg, procLogCh, &summaryMu, procSummary)

					mu.Lock()
					completed++
					if completed%100 == 0 || completed == totalSols {
						elapsed := time.Since(overallStart)
						estimatedTotal := time.Duration(float64(elapsed) / float64(completed) * float64(totalSols))
						eta := estimatedTotal - elapsed
						log.Printf("✅ Progress: %d/%d (%.2f%%) | Elapsed: %s | ETA: %s",
							completed, totalSols, float64(completed)*100/float64(totalSols),
							elapsed.Round(time.Second), eta.Round(time.Second))
					}
					mu.Unlock()
				}(sol)
			}
			wg.Wait()
		}
	} else if mode == "S" {
		// Signature extraction mode
		if runCfg.SignatureExtraction == nil {
			ExitWithError(ExitConfigError, "Signature extraction configuration is required when mode is 'S'")
		}

		cliLogger.Info("Starting signature extraction for %d SOL IDs", totalSols)
		
		// Create database manager for signature extraction
		dbManager := NewDatabaseManager(logger)
		if err := dbManager.Connect(ctx, actualConnString); err != nil {
			ExitWithError(ExitDatabaseError, "Failed to connect database manager: %v", err)
		}
		defer dbManager.Close()

		// Create signature extractor
		signatureExtractor := NewSignatureExtractor(dbManager, logger, runCfg.SignatureExtraction)
		
		// Extract signatures in batches to avoid memory issues
		batchSize := min(appCfg.Concurrency*10, 100) // Process in batches
		for i := 0; i < len(sols); i += batchSize {
			end := min(i+batchSize, len(sols))
			batch := sols[i:end]
			
			cliLogger.Info("Processing signature batch %d-%d of %d", i+1, end, totalSols)
			
			// Extract signatures for this batch
			results := signatureExtractor.ExtractSignatures(ctx, batch)
			
			// Process results
			for j, result := range results {
				solID := batch[j]
				
				mu.Lock()
				completed++
				if result.IsSuccess() {
					signatureResult := result.Value
					cliLogger.Verbose("Signature extracted: %s -> %s (%s, %d bytes)", 
						solID, signatureResult.Filename, signatureResult.ImageFormat, signatureResult.FileSize)
				} else {
					errorCount++
					cliLogger.Error("Signature extraction failed for SOL %s: %v", solID, result.Error)
				}
				progressBar.Update(completed)
				mu.Unlock()
			}
		}
		
		cliLogger.Success("Signature extraction completed: %d successful, %d failed", 
			completed-errorCount, errorCount)
	}
	close(procLogCh)
	
	// Ensure progress bar is complete
	progressBar.Finish()

	cliLogger.Info("Writing execution summary...")
	writeSummary(filepath.Join(appCfg.LogFilePath, LogFileSummary), procSummary)
	
	if mode == "E" {
		cliLogger.Info("Merging output files...")
		mergeFiles(&runCfg)
	}
	
	// Final performance summary
	if *verbose {
		finalStats := db.Stats()
		cliLogger.Verbose("Final DB Stats: MaxOpen=%d, Open=%d, InUse=%d, Idle=%d, WaitCount=%d, WaitDuration=%s",
			finalStats.MaxOpenConnections, finalStats.OpenConnections, finalStats.InUse, finalStats.Idle, finalStats.WaitCount, finalStats.WaitDuration)
	}
	
	// Print execution summary
	PrintSummary(overallStart, totalSols, mode, errorCount)
	
	// Exit with appropriate code
	if errorCount > 0 {
		os.Exit(ExitGeneralError)
	}
	os.Exit(ExitSuccess)
}

// calculateOptimalConnections determines optimal database connection pool size
func calculateOptimalConnections(concurrency, procCount, solCount int) int {
	// Base calculation: concurrency * procedures
	baseConns := concurrency * procCount
	
	// Apply scaling factors based on workload
	if solCount > 10000 {
		// For large datasets, cap connections to prevent database overload
		baseConns = min(baseConns, 150)
	} else if solCount < 100 {
		// For small datasets, reduce connections to save resources
		baseConns = min(baseConns, 50)
	}
	
	// Absolute limits
	maxConns := min(baseConns, 200) // Never exceed 200 connections
	maxConns = max(maxConns, 10)    // Always have at least 10 connections
	
	if maxConns > 150 {
		log.Printf("⚠️ Warning: High connection count (%d) - monitor database performance", maxConns)
	}
	
	return maxConns
}
