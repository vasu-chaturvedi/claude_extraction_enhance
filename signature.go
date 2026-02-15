package main

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"
)

// dirCache caches created directories to avoid repeated MkdirAll calls
var dirCache = make(map[string]bool)
var dirCacheMu sync.RWMutex

// blobPool reuses byte slices for blob data to reduce memory allocations
var blobPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, 0, 64*1024) // 64KB initial capacity
	},
}

// blobWriteTask represents a file writing task
type blobWriteTask struct {
	filename string
	data     []byte
	done     chan error
}

// runSignatureExtraction executes the signature extraction for all SOLs
func runSignatureExtraction(ctx context.Context, db *sql.DB, sols []string, procConfig *ExtractionConfig, procLogCh chan<- ProcLog, mu *sync.Mutex, procSummary map[string]ProcSummary, concurrency int) {
	var wg sync.WaitGroup
	sem := make(chan struct{}, concurrency)
	overallStart := time.Now()
	var progressMu sync.Mutex
	completed := 0
	totalSols := len(sols)

	slog.Info("Starting signature extraction",
		"total_sols", totalSols,
		"concurrency", concurrency,
		"mode", "signature")

	for _, sol := range sols {
		wg.Add(1)
		sem <- struct{}{}
		go func(solID string) {
			defer wg.Done()
			defer func() { <-sem }()

			start := time.Now()
			slog.Debug("Starting signature extraction", "sol_id", solID)

			err := extractSignaturesForSol(ctx, db, solID)
			end := time.Now()

			plog := ProcLog{
				SolID:         solID,
				Procedure:     "SIGNATURE_EXTRACT",
				StartTime:     start,
				EndTime:       end,
				ExecutionTime: end.Sub(start),
			}

			if err != nil {
				plog.Status = "FAIL"
				plog.ErrorDetails = err.Error()
				slog.Error("Signature extraction failed", "sol_id", solID, "error", err)
			} else {
				plog.Status = "SUCCESS"
				slog.Debug("Signature extraction completed", "sol_id", solID, "duration", end.Sub(start).Round(time.Millisecond).String())
			}
			procLogCh <- plog

			// Update summary
			mu.Lock()
			s, exists := procSummary["SIGNATURE_EXTRACT"]
			if !exists {
				s = ProcSummary{
					Procedure: "SIGNATURE_EXTRACT",
					StartTime: start,
					EndTime:   end,
					Status:    plog.Status,
				}
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
			procSummary["SIGNATURE_EXTRACT"] = s
			mu.Unlock()

			// Progress reporting
			progressMu.Lock()
			completed++
			localCompleted := completed
			progressMu.Unlock()

			if localCompleted%50 == 0 || localCompleted == totalSols {
				elapsed := time.Since(overallStart)
				estimatedTotal := time.Duration(float64(elapsed) / float64(localCompleted) * float64(totalSols))
				eta := estimatedTotal - elapsed
				progress := float64(localCompleted) * 100 / float64(totalSols)
				slog.Info("Signature extraction progress",
					"completed", localCompleted,
					"total", totalSols,
					"progress_percent", fmt.Sprintf("%.2f", progress),
					"elapsed", elapsed.Round(time.Second).String(),
					"eta", eta.Round(time.Second).String())
			}
		}(sol)
	}
	wg.Wait()

	slog.Info("Signature extraction completed",
		"total_sols", totalSols,
		"duration", time.Since(overallStart).Round(time.Second).String())
}

// extractSignaturesForSol extracts signature blobs for a specific SOL
func extractSignaturesForSol(ctx context.Context, db *sql.DB, solID string) error {
	// Hardcoded query that returns filename and image blob columns
	query := `
		SELECT filename, image
		FROM signature_table
		WHERE sol_id = :1
		AND image IS NOT NULL
		ORDER BY filename`

	start := time.Now()

	// Use prepared statement cache for better performance
	stmt, err := globalStmtCache.GetOrPrepare(db, query)
	if err != nil {
		return fmt.Errorf("failed to prepare signature query: %w", err)
	}

	rows, err := stmt.QueryContext(ctx, solID)
	if err != nil {
		return fmt.Errorf("signature query failed for SOL %s: %w", solID, err)
	}
	defer rows.Close()

	slog.Debug("Signature query executed",
		"sol_id", solID,
		"duration", time.Since(start).Round(time.Millisecond).String())

	// First pass: collect all blobs and directories for batch processing
	type blobData struct {
		filename string
		data     []byte
	}
	var blobs []blobData
	dirSet := make(map[string]bool)

	for rows.Next() {
		var filename string
		// Use pooled buffer for blob data
		buf := blobPool.Get().([]byte)
		var imageBlob []byte

		if err := rows.Scan(&filename, &imageBlob); err != nil {
			blobPool.Put(buf[:0])
			return fmt.Errorf("failed to scan signature row for SOL %s: %w", solID, err)
		}

		if len(imageBlob) == 0 {
			blobPool.Put(buf[:0])
			slog.Warn("Empty blob found", "sol_id", solID, "filename", filename)
			continue
		}

		// Copy blob data to pooled buffer
		buf = buf[:0]
		buf = append(buf, imageBlob...)
		blobs = append(blobs, blobData{filename: filename, data: buf})
		dirSet[filepath.Dir(filename)] = true
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating signature rows for SOL %s: %w", solID, err)
	}

	// Batch create all directories
	if err := batchCreateDirectories(dirSet); err != nil {
		// Return buffers to pool on error
		for _, blob := range blobs {
			blobPool.Put(blob.data[:0])
		}
		return fmt.Errorf("failed to create directories for SOL %s: %w", solID, err)
	}

	// Create file writer workers
	fileWriteCh := make(chan blobWriteTask, min(len(blobs), 100))
	var writerWg sync.WaitGroup
	workerCount := min(4, len(blobs))

	for i := 0; i < workerCount; i++ {
		writerWg.Add(1)
		go func() {
			defer writerWg.Done()
			for task := range fileWriteCh {
				err := writeBlobToFileOptimized(task.filename, task.data)
				task.done <- err
			}
		}()
	}

	// Dispatch all blob writes concurrently, then collect results
	signatureCount := 0
	totalBytes := int64(0)
	doneChans := make([]chan error, len(blobs))

	for i, blob := range blobs {
		done := make(chan error, 1)
		doneChans[i] = done
		fileWriteCh <- blobWriteTask{
			filename: blob.filename,
			data:     blob.data,
			done:     done,
		}
	}
	close(fileWriteCh)

	// Collect results after all tasks are dispatched
	var firstErr error
	for i, blob := range blobs {
		if err := <-doneChans[i]; err != nil && firstErr == nil {
			firstErr = fmt.Errorf("failed to write signature file %s for SOL %s: %w", blob.filename, solID, err)
		}
		signatureCount++
		totalBytes += int64(len(blob.data))
		// Return buffer to pool
		blobPool.Put(blob.data[:0])
	}

	writerWg.Wait()

	if firstErr != nil {
		return firstErr
	}

	// Record performance metrics
	queryDuration := time.Since(start)
	globalMetrics.RecordQuery(queryDuration, int64(signatureCount), totalBytes)

	slog.Info("Signature extraction completed for SOL",
		"sol_id", solID,
		"signature_count", signatureCount,
		"total_bytes", totalBytes,
		"duration", queryDuration.Round(time.Millisecond).String())

	return nil
}

// batchCreateDirectories creates all directories in the set efficiently
func batchCreateDirectories(dirSet map[string]bool) error {
	for dir := range dirSet {
		dirCacheMu.RLock()
		exists := dirCache[dir]
		dirCacheMu.RUnlock()

		if !exists {
			if err := os.MkdirAll(dir, 0755); err != nil {
				return fmt.Errorf("failed to create directory %s: %w", dir, err)
			}
			dirCacheMu.Lock()
			dirCache[dir] = true
			dirCacheMu.Unlock()
		}
	}
	return nil
}

// writeBlobToFileOptimized writes a blob to the specified file path with optimized buffering
func writeBlobToFileOptimized(filename string, blob []byte) error {
	// Create the file
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create file %s: %w", filename, err)
	}
	defer file.Close()

	// Use dynamic buffer sizing based on blob size
	bufSize := min(len(blob), 8*1024) // Cap at 8KB for small files
	if bufSize < 1024 {
		bufSize = 1024 // Minimum 1KB buffer
	}
	buf := bufio.NewWriterSize(file, bufSize)
	defer buf.Flush()

	// Write the blob data
	_, err = buf.Write(blob)
	if err != nil {
		return fmt.Errorf("failed to write blob data to file %s: %w", filename, err)
	}

	return nil
}

// writeBlobToFile writes a blob to the specified file path with buffered I/O (legacy function)
func writeBlobToFile(filename string, blob []byte) error {
	// Ensure the directory exists - use cache to avoid repeated MkdirAll calls
	dir := filepath.Dir(filename)
	dirCacheMu.RLock()
	dirExists := dirCache[dir]
	dirCacheMu.RUnlock()

	if !dirExists {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("failed to create directory %s: %w", dir, err)
		}
		dirCacheMu.Lock()
		dirCache[dir] = true
		dirCacheMu.Unlock()
	}

	return writeBlobToFileOptimized(filename, blob)
}

// runSignatureExtractionWithProcLevelParallelism runs signature extraction with procedure-level parallelism
func runSignatureExtractionWithProcLevelParallelism(ctx context.Context, db *sql.DB, sols []string, procConfig *ExtractionConfig, procLogCh chan<- ProcLog, mu *sync.Mutex, procSummary map[string]ProcSummary, concurrency int) {
	// For signature extraction, we treat each SOL as a separate task
	taskCh := make(chan string, len(sols))
	var wg sync.WaitGroup
	totalTasks := len(sols)
	overallStart := time.Now()
	var progressMu sync.Mutex
	completed := 0

	slog.Info("Starting procedure-level parallel signature extraction",
		"total_sols", totalTasks,
		"concurrency", concurrency)

	// Start workers
	maxProcs := runtime.GOMAXPROCS(0)
	for range concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for solID := range taskCh {
				start := time.Now()

				if maxProcs <= 4 {
					slog.Debug("Starting signature extraction task",
						"sol_id", solID,
						"queue_remaining", len(taskCh))
				}

				err := extractSignaturesForSol(ctx, db, solID)
				end := time.Now()
				duration := end.Sub(start)

				plog := ProcLog{
					SolID:         solID,
					Procedure:     "SIGNATURE_EXTRACT",
					StartTime:     start,
					EndTime:       end,
					ExecutionTime: duration,
				}

				if err != nil {
					plog.Status = "FAIL"
					plog.ErrorDetails = err.Error()
					if maxProcs <= 4 {
						slog.Error("Signature extraction task failed",
							"sol_id", solID,
							"duration", duration.Round(time.Millisecond).String(),
							"error", err.Error())
					}
				} else {
					plog.Status = "SUCCESS"
					if maxProcs <= 4 {
						slog.Debug("Signature extraction task completed",
							"sol_id", solID,
							"duration", duration.Round(time.Millisecond).String())
					}
				}
				procLogCh <- plog

				// Update summary
				mu.Lock()
				s, exists := procSummary["SIGNATURE_EXTRACT"]
				if !exists {
					s = ProcSummary{
						Procedure: "SIGNATURE_EXTRACT",
						StartTime: start,
						EndTime:   end,
						Status:    plog.Status,
					}
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
				procSummary["SIGNATURE_EXTRACT"] = s
				mu.Unlock()

				// Progress reporting
				progressMu.Lock()
				completed++
				localCompleted := completed
				progressMu.Unlock()

				if localCompleted%50 == 0 || localCompleted == totalTasks {
					elapsed := time.Since(overallStart)
					rate := float64(localCompleted) / elapsed.Seconds()
					eta := time.Duration(float64(totalTasks-localCompleted)/rate) * time.Second

					slog.Info("Signature extraction progress",
						"completed", localCompleted,
						"total", totalTasks,
						"progress_percent", fmt.Sprintf("%.1f", float64(localCompleted)*100/float64(totalTasks)),
						"rate_per_second", fmt.Sprintf("%.1f", rate),
						"eta", eta.Round(time.Second).String())
				}
			}
		}()
	}

	// Send all SOL IDs to the task channel
	for _, sol := range sols {
		taskCh <- sol
	}
	close(taskCh)
	wg.Wait()

	slog.Info("Signature extraction with procedure-level parallelism completed",
		"total_tasks", totalTasks,
		"duration", time.Since(overallStart).Round(time.Second).String())
}
