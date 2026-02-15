package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Memory pools for performance optimization
var (
	scanArgsPool = sync.Pool{
		New: func() interface{} {
			return make([]interface{}, 0, 50) // Pre-allocate capacity for 50 columns
		},
	}
	valuesPool = sync.Pool{
		New: func() interface{} {
			return make([]sql.NullString, 0, 50) // Pre-allocate capacity for 50 columns
		},
	}
	stringBuilderPool = sync.Pool{
		New: func() interface{} {
			return &strings.Builder{}
		},
	}
)

// Legacy prepared statement cache - replaced by generic cache in cache.go

var globalStmtCache = NewPreparedStmtCache()

func runExtractionForSol(ctx context.Context, db *sql.DB, solID string, procConfig *ExtractionConfig, templates map[string][]ColumnConfig, logCh chan<- ProcLog, mu *sync.Mutex, summary map[string]ProcSummary) {
	var wg sync.WaitGroup
	procCh := make(chan string)

	numWorkers := runtime.NumCPU() * 2
	for range numWorkers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for proc := range procCh {
				start := time.Now()

				// Check if this procedure uses chunked logic
				if isChunkedProcedure(proc, procConfig.ChunkedProcedures) {
					slog.Debug("Starting chunked extraction", "procedure", proc, "sol_id", solID)
					chunkResultsCh := make(chan ChunkResult, 500)
					runChunkedExtractionForSol(ctx, db, solID, proc, procConfig, templates, logCh, chunkResultsCh)
					close(chunkResultsCh)

					// Process chunk results for summary
					for result := range chunkResultsCh {
						mu.Lock()
						s, exists := summary[result.Procedure]
						if !exists {
							s = ProcSummary{
								Procedure: result.Procedure,
								StartTime: result.StartTime,
								EndTime:   result.EndTime,
								Status:    result.Status,
							}
						} else {
							if result.StartTime.Before(s.StartTime) {
								s.StartTime = result.StartTime
							}
							if result.EndTime.After(s.EndTime) {
								s.EndTime = result.EndTime
							}
							if s.Status != "FAIL" && result.Status == "FAIL" {
								s.Status = "FAIL"
							}
						}
						summary[result.Procedure] = s
						mu.Unlock()
					}
				} else {
					// Regular extraction logic
					slog.Debug("Starting extraction", "procedure", proc, "sol_id", solID)
					err := extractData(ctx, db, proc, solID, procConfig, templates)
					end := time.Now()

					plog := ProcLog{
						SolID:         solID,
						Procedure:     proc,
						StartTime:     start,
						EndTime:       end,
						ExecutionTime: end.Sub(start),
					}
					if err != nil {
						plog.Status = "FAIL"
						plog.ErrorDetails = err.Error()
					} else {
						plog.Status = "SUCCESS"
					}
					logCh <- plog

					mu.Lock()
					s, exists := summary[proc]
					if !exists {
						s = ProcSummary{Procedure: proc, StartTime: start, EndTime: end, Status: plog.Status}
					} else {
						if start.Before(s.StartTime) {
							s.StartTime = start
						}
						if end.After(s.EndTime) {
							s.EndTime = end
						}
						if s.Status != "FAIL" && plog.Status == "FAIL" {
							s.Status = "FAIL"
						}
					}
					summary[proc] = s
					mu.Unlock()
					slog.Debug("Completed extraction",
						"procedure", proc,
						"sol_id", solID,
						"duration", end.Sub(start).Round(time.Millisecond).String())
				}
			}
		}()
	}

	for _, proc := range procConfig.Procedures {
		procCh <- proc
	}
	close(procCh)
	wg.Wait()
}

func extractData(ctx context.Context, db *sql.DB, procName, solID string, cfg *ExtractionConfig, templates map[string][]ColumnConfig) error {
	cols, ok := templates[procName]
	if !ok {
		return fmt.Errorf("missing template for procedure %s", procName)
	}

	colNames := make([]string, len(cols))
	for i, col := range cols {
		colNames[i] = col.Name
	}

	query := fmt.Sprintf("SELECT %s FROM %s WHERE SOL_ID = :1", strings.Join(colNames, ", "), procName)
	start := time.Now()

	// Use prepared statement cache for better performance
	stmt, err := globalStmtCache.GetOrPrepare(db, query)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}

	rows, err := stmt.QueryContext(ctx, solID)
	if err != nil {
		return fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()
	slog.Debug("Query executed",
		"procedure", procName,
		"sol_id", solID,
		"duration", time.Since(start).Round(time.Millisecond).String())

	spoolPath := filepath.Join(cfg.SpoolOutputPath, fmt.Sprintf("%s_%s.spool", procName, solID))
	f, err := os.Create(spoolPath)
	if err != nil {
		return err
	}
	defer f.Close()

	// Use larger buffer for better I/O performance (128KB)
	buf := bufio.NewWriterSize(f, 128*1024)
	defer buf.Flush()

	rowCount := int64(0)
	totalBytes := int64(0)

	// Pre-allocate reusable slices outside the loop
	values := valuesPool.Get().([]sql.NullString)
	scanArgs := scanArgsPool.Get().([]interface{})
	if cap(values) < len(cols) {
		values = make([]sql.NullString, len(cols))
	} else {
		values = values[:len(cols)]
	}
	if cap(scanArgs) < len(cols) {
		scanArgs = make([]interface{}, len(cols))
	} else {
		scanArgs = scanArgs[:len(cols)]
	}
	for i := range values {
		scanArgs[i] = &values[i]
	}
	strValues := make([]string, len(cols))

	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			valuesPool.Put(values[:0])
			scanArgsPool.Put(scanArgs[:0])
			return err
		}

		for i, v := range values {
			if v.Valid {
				strValues[i] = v.String
			} else {
				strValues[i] = ""
			}
		}

		formattedRow := formatRow(cfg, cols, strValues) + "\n"
		buf.WriteString(formattedRow)

		rowCount++
		totalBytes += int64(len(formattedRow))
	}

	// Return objects to pools after the loop
	valuesPool.Put(values[:0])
	scanArgsPool.Put(scanArgs[:0])

	// Record performance metrics
	queryDuration := time.Since(start)
	globalMetrics.RecordQuery(queryDuration, rowCount, totalBytes)

	return nil
}

func mergeFiles(cfg *ExtractionConfig) error {
	// Set chunked defaults before processing
	setChunkedDefaults(cfg)

	for _, proc := range cfg.Procedures {
		// Skip merging for chunked procedures - they handle their own file output
		if isChunkedProcedure(proc, cfg.ChunkedProcedures) {
			slog.Info("Skipping merge for chunked procedure", "procedure", proc, "reason", "files already in final format")
			continue
		}

		slog.Info("Starting merge for procedure", "procedure", proc)

		pattern := filepath.Join(cfg.SpoolOutputPath, fmt.Sprintf("%s_*.spool", proc))
		finalFile := filepath.Join(cfg.SpoolOutputPath, fmt.Sprintf("%s.txt", proc))

		files, err := filepath.Glob(pattern)
		if err != nil {
			return fmt.Errorf("glob failed: %w", err)
		}
		slices.Sort(files)

		outFile, err := os.Create(finalFile)
		if err != nil {
			return err
		}
		defer outFile.Close()

		// Use larger buffer for merge operations (256KB)
		writer := bufio.NewWriterSize(outFile, 256*1024)
		start := time.Now()

		for _, file := range files {
			in, err := os.Open(file)
			if err != nil {
				return err
			}
			if _, err := io.Copy(writer, in); err != nil {
				in.Close()
				return fmt.Errorf("failed to copy spool file %s: %w", file, err)
			}
			in.Close()
			os.Remove(file)
		}
		writer.Flush()
		slog.Info("Merge completed",
			"procedure", proc,
			"file_count", len(files),
			"output_file", finalFile,
			"duration", time.Since(start).Round(time.Second).String())
	}
	return nil
}

func readColumnsFromCSV(path string) ([]ColumnConfig, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r := bufio.NewReader(f)
	csvr := csv.NewReader(r)
	headers, err := csvr.Read()
	if err != nil {
		return nil, err
	}
	index := make(map[string]int)
	for i, h := range headers {
		index[strings.ToLower(h)] = i
	}
	var cols []ColumnConfig
	for {
		row, err := csvr.Read()
		if err != nil {
			break
		}
		col := ColumnConfig{Name: row[index["name"]]}
		if i, ok := index["length"]; ok && i < len(row) {
			col.Length, _ = strconv.Atoi(row[i])
		}
		if i, ok := index["align"]; ok && i < len(row) {
			col.Align = row[i]
		}
		cols = append(cols, col)
	}
	return cols, nil
}

func sanitize(s string) string {
	return strings.ReplaceAll(strings.ReplaceAll(s, "\n", " "), "\r", " ")
}

func formatRow(cfg *ExtractionConfig, cols []ColumnConfig, values []string) string {
	switch cfg.Format {
	case "delimited":
		// Use string builder from pool for better performance
		builder := stringBuilderPool.Get().(*strings.Builder)
		builder.Reset()
		defer stringBuilderPool.Put(builder)

		for i, v := range values {
			if i > 0 {
				builder.WriteString(cfg.Delimiter)
			}
			builder.WriteString(sanitize(v))
		}
		return builder.String()

	case "fixed":
		// Use string builder from pool for better performance
		builder := stringBuilderPool.Get().(*strings.Builder)
		builder.Reset()
		defer stringBuilderPool.Put(builder)

		for i, col := range cols {
			var val string
			if i < len(values) && values[i] != "" {
				val = sanitize(values[i])
			} else {
				val = ""
			}

			if len(val) > col.Length {
				val = val[:col.Length]
			}

			padLen := col.Length - len(val)
			if col.Align == "right" {
				if padLen > 0 {
					builder.WriteString(strings.Repeat(" ", padLen))
				}
				builder.WriteString(val)
			} else {
				builder.WriteString(val)
				if padLen > 0 {
					builder.WriteString(strings.Repeat(" ", padLen))
				}
			}
		}
		return builder.String()

	default:
		return ""
	}
}
