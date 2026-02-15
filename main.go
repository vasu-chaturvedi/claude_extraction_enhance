package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	_ "github.com/godror/godror"
)

var (
	appCfgFile = new(string)
	runCfgFile = new(string)
	mode       string
)

func init() {
	// Setup structured logging
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	flag.StringVar(appCfgFile, "appCfg", "", "Path to the main application configuration file")
	flag.StringVar(runCfgFile, "runCfg", "", "Path to the extraction configuration file")
	flag.StringVar(&mode, "mode", "", "Mode of operation: E - Extract, I - Insert, S - Signature")
	flag.Parse()

	if !slices.Contains([]string{"E", "I", "S"}, mode) {
		slog.Error("Invalid mode specified", "mode", mode, "valid_modes", []string{"E", "I", "S"})
		os.Exit(1)
	}
	if *appCfgFile == "" || *runCfgFile == "" {
		slog.Error("Configuration files required", "app_cfg", *appCfgFile, "run_cfg", *runCfgFile)
		os.Exit(1)
	}
	if _, err := os.Stat(*appCfgFile); os.IsNotExist(err) {
		slog.Error("Application configuration file not found", "path", *appCfgFile, "error", err)
		os.Exit(1)
	}
	if _, err := os.Stat(*runCfgFile); os.IsNotExist(err) {
		slog.Error("Extraction configuration file not found", "path", *runCfgFile, "error", err)
		os.Exit(1)
	}
}

func main() {
	slog.Info("Starting claude_extract", "mode", mode, "app_config", *appCfgFile, "run_config", *runCfgFile)

	appCfg, err := loadMainConfig(*appCfgFile)
	if err != nil {
		slog.Error("Failed to load main config", "path", *appCfgFile, "error", err)
		os.Exit(1)
	}
	runCfg, err := loadExtractionConfig(*runCfgFile)
	if err != nil {
		slog.Error("Failed to load extraction config", "path", *runCfgFile, "error", err)
		os.Exit(1)
	}

	// Set chunked processing defaults
	setChunkedDefaults(&runCfg)
	if len(runCfg.ChunkedProcedures) > 0 {
		slog.Info("Chunked procedures configured",
			"procedures", runCfg.ChunkedProcedures,
			"chunk_size", runCfg.ChunkSize)
	}

	// Load templates
	templates := make(map[string][]ColumnConfig)
	for _, proc := range runCfg.Procedures {
		tmplPath := filepath.Join(runCfg.TemplatePath, fmt.Sprintf("%s.csv", proc))
		cols, err := readColumnsFromCSV(tmplPath)
		if err != nil {
			slog.Error("Failed to read template", "procedure", proc, "path", tmplPath, "error", err)
			os.Exit(1)
		}
		templates[proc] = cols
	}
	slog.Info("Templates loaded", "count", len(templates), "procedures", runCfg.Procedures)

	connString := fmt.Sprintf(`user="%s" password="%s" connectString="%s:%d/%s"`,
		appCfg.DBUser, appCfg.DBPassword, appCfg.DBHost, appCfg.DBPort, appCfg.DBSid)

	db, err := sql.Open("godror", connString)
	if err != nil {
		slog.Error("Failed to connect to database",
			"host", appCfg.DBHost,
			"port", appCfg.DBPort,
			"sid", appCfg.DBSid,
			"error", err)
		os.Exit(1)
	}
	defer db.Close()
	slog.Info("Database connection established",
		"host", appCfg.DBHost,
		"port", appCfg.DBPort,
		"sid", appCfg.DBSid)

	procCount := len(runCfg.Procedures)
	// Optimize connection pool for high concurrency with improved settings
	maxConns := appCfg.Concurrency * procCount
	if maxConns > 200 {
		slog.Warn("Connection pool size may exceed database limits",
			"calculated_size", maxConns,
			"capped_size", 200)
		maxConns = 200 // Cap at reasonable limit
	}

	// Enhanced connection pool configuration for better performance
	db.SetMaxOpenConns(maxConns)
	db.SetMaxIdleConns(maxConns / 2)        // Keep more idle connections to avoid reconnect cost
	db.SetConnMaxLifetime(10 * time.Minute) // Shorter lifetime for high throughput
	db.SetConnMaxIdleTime(3 * time.Minute)  // Close idle connections after inactivity

	slog.Info("Connection pool configured",
		"max_connections", maxConns,
		"idle_connections", maxConns/2,
		"concurrency", appCfg.Concurrency,
		"procedure_count", procCount)

	sols, err := readSols(appCfg.SolFilePath)
	if err != nil {
		slog.Error("Failed to read SOL IDs", "path", appCfg.SolFilePath, "error", err)
		os.Exit(1)
	}
	slog.Info("SOL IDs loaded", "count", len(sols), "path", appCfg.SolFilePath)

	// Scale buffer size based on expected load
	bufferSize := max(1000, min(50000, len(sols)*len(runCfg.Procedures)))
	procLogCh := make(chan ProcLog, bufferSize)
	var summaryMu sync.Mutex
	procSummary := make(map[string]ProcSummary)

	if (mode == "I" && !runCfg.RunInsertionParallel) || (mode == "E" && !runCfg.RunExtractionParallel) || (mode == "S" && !runCfg.RunExtractionParallel) {
		slog.Info("Running procedures sequentially", "reason", "parallel execution disabled")
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

	logFilePath := filepath.Join(appCfg.LogFilePath, LogFile)
	summaryFilePath := filepath.Join(appCfg.LogFilePath, LogFileSummary)
	slog.Info("Log files configured",
		"procedure_log", logFilePath,
		"summary_log", summaryFilePath)

	go writeLog(logFilePath, procLogCh)

	sem := make(chan struct{}, appCfg.Concurrency)
	var wg sync.WaitGroup
	ctx := context.Background()
	totalSols := len(sols)
	overallStart := time.Now()
	var mu sync.Mutex
	completed := 0

	slog.Info("Starting processing",
		"mode", mode,
		"total_sols", totalSols,
		"concurrency", appCfg.Concurrency,
		"buffer_size", bufferSize)

	if mode == "E" {
		slog.Info("Starting extraction mode", "total_sols", totalSols)
		for _, sol := range sols {
			wg.Add(1)
			sem <- struct{}{}
			go func(solID string) {
				defer wg.Done()
				defer func() { <-sem }()
				slog.Debug("Starting SOL extraction", "sol_id", solID)

				runExtractionForSol(ctx, db, solID, &runCfg, templates, procLogCh, &summaryMu, procSummary)

				mu.Lock()
				completed++
				if completed%100 == 0 || completed == totalSols {
					elapsed := time.Since(overallStart)
					estimatedTotal := time.Duration(float64(elapsed) / float64(completed) * float64(totalSols))
					eta := estimatedTotal - elapsed
					progress := float64(completed) * 100 / float64(totalSols)
					slog.Info("Extraction progress",
						"completed", completed,
						"total", totalSols,
						"progress_percent", fmt.Sprintf("%.2f", progress),
						"elapsed", elapsed.Round(time.Second).String(),
						"eta", eta.Round(time.Second).String())
				}
				mu.Unlock()
			}(sol)
		}
		wg.Wait()
	} else if mode == "I" {
		if runCfg.UseProcLevelParallel {
			totalTasks := totalSols * len(runCfg.Procedures)
			slog.Info("Starting procedure-level parallel execution",
				"total_sols", totalSols,
				"procedures", len(runCfg.Procedures),
				"total_tasks", totalTasks,
				"max_connections", maxConns,
				"idle_connections", maxConns/2)

			// Monitor connection pool stats
			go func() {
				ticker := time.NewTicker(30 * time.Second)
				defer ticker.Stop()
				for {
					select {
					case <-ticker.C:
						stats := db.Stats()
						slog.Info("Database connection stats",
							"open_connections", stats.OpenConnections,
							"in_use", stats.InUse,
							"idle", stats.Idle,
							"wait_count", stats.WaitCount,
							"wait_duration", stats.WaitDuration.String())
					case <-ctx.Done():
						return
					}
				}
			}()

			runProceduresWithProcLevelParallelism(ctx, db, sols, &runCfg, procLogCh, &summaryMu, procSummary, appCfg.Concurrency)
			slog.Info("Completed all procedure-level tasks", "total_tasks", totalTasks)
		} else {
			slog.Info("Starting SOL-level parallel execution (legacy mode)", "total_sols", totalSols)
			for _, sol := range sols {
				wg.Add(1)
				sem <- struct{}{}
				go func(solID string) {
					defer wg.Done()
					defer func() { <-sem }()
					slog.Debug("Starting SOL procedures", "sol_id", solID)

					runProceduresForSol(ctx, db, solID, &runCfg, procLogCh, &summaryMu, procSummary)

					mu.Lock()
					completed++
					if completed%100 == 0 || completed == totalSols {
						elapsed := time.Since(overallStart)
						estimatedTotal := time.Duration(float64(elapsed) / float64(completed) * float64(totalSols))
						eta := estimatedTotal - elapsed
						progress := float64(completed) * 100 / float64(totalSols)
						slog.Info("SOL-level progress",
							"completed", completed,
							"total", totalSols,
							"progress_percent", fmt.Sprintf("%.2f", progress),
							"elapsed", elapsed.Round(time.Second).String(),
							"eta", eta.Round(time.Second).String())
					}
					mu.Unlock()
				}(sol)
			}
			wg.Wait()
		}
	} else if mode == "S" {
		if runCfg.UseProcLevelParallel {
			slog.Info("Starting procedure-level parallel signature extraction",
				"total_sols", totalSols,
				"max_connections", maxConns)

			runSignatureExtractionWithProcLevelParallelism(ctx, db, sols, &runCfg, procLogCh, &summaryMu, procSummary, appCfg.Concurrency)
			slog.Info("Completed procedure-level signature extraction", "total_sols", totalSols)
		} else {
			slog.Info("Starting SOL-level parallel signature extraction", "total_sols", totalSols)
			runSignatureExtraction(ctx, db, sols, &runCfg, procLogCh, &summaryMu, procSummary, appCfg.Concurrency)
		}
	}
	close(procLogCh)

	slog.Info("Writing summary files", "summary_path", summaryFilePath)
	writeSummary(summaryFilePath, procSummary)
	if mode == "E" {
		slog.Info("Merging extraction files")
		mergeFiles(&runCfg)
	}

	// Clean up prepared statements
	globalStmtCache.Close()
	procStmtCache.Close()
	slog.Info("Cleaned up prepared statement caches")

	// Final performance summary
	finalStats := db.Stats()
	slog.Info("Final database stats",
		"max_open", finalStats.MaxOpenConnections,
		"open", finalStats.OpenConnections,
		"in_use", finalStats.InUse,
		"idle", finalStats.Idle,
		"wait_count", finalStats.WaitCount,
		"wait_duration", finalStats.WaitDuration.String())

	// Performance metrics summary
	totalQueries, avgDuration, cacheHitRate, slowQueries := globalMetrics.GetStats()
	totalRowsProcessed := globalMetrics.TotalRowsProcessed
	totalBytesWritten := globalMetrics.TotalBytesWritten
	totalDuration := time.Since(overallStart)

	slog.Info("Performance summary",
		"total_queries", totalQueries,
		"avg_query_time", avgDuration.Round(time.Millisecond).String(),
		"cache_hit_rate_percent", fmt.Sprintf("%.1f", cacheHitRate),
		"slow_queries", slowQueries,
		"rows_processed", totalRowsProcessed,
		"bytes_written_mb", fmt.Sprintf("%.2f", float64(totalBytesWritten)/(1024*1024)),
		"total_duration", totalDuration.Round(time.Second).String(),
		"sols_processed", totalSols)

	slog.Info("Processing completed successfully",
		"mode", mode,
		"total_sols", totalSols,
		"duration", totalDuration.Round(time.Second).String())
}
