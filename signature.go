package main

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// dirCache using sync.Map for lock-free reads under high concurrency
var dirCache sync.Map

// blobWriteTask represents a file writing task for the worker pool
type blobWriteTask struct {
	filename string
	data     []byte
}

// signatureWritePool is a shared pool of file-write workers across all SOLs.
// Workers read from taskCh, write blobs directly via os.WriteFile, and return
// pooled buffers. Errors are tracked atomically via firstErr.
type signatureWritePool struct {
	taskCh   chan blobWriteTask
	wg       sync.WaitGroup
	firstErr atomic.Value // stores first error as *error
	metrics  struct {
		filesWritten atomic.Int64
		bytesWritten atomic.Int64
	}
}

const signatureWriteWorkers = 12

func newSignatureWritePool() *signatureWritePool {
	p := &signatureWritePool{
		taskCh: make(chan blobWriteTask, 256),
	}
	for range signatureWriteWorkers {
		p.wg.Add(1)
		go p.worker()
	}
	return p
}

func (p *signatureWritePool) worker() {
	defer p.wg.Done()
	for task := range p.taskCh {
		if err := os.WriteFile(task.filename, task.data, 0644); err != nil {
			// Store first error only
			wrappedErr := fmt.Errorf("failed to write signature file %s: %w", task.filename, err)
			p.firstErr.CompareAndSwap(nil, &wrappedErr)
		}
		p.metrics.filesWritten.Add(1)
		p.metrics.bytesWritten.Add(int64(len(task.data)))
		// Return buffer to pool
		blobPool.Put(task.data[:0])
	}
}

func (p *signatureWritePool) submit(filename string, data []byte) {
	p.taskCh <- blobWriteTask{filename: filename, data: data}
}

func (p *signatureWritePool) close() error {
	close(p.taskCh)
	p.wg.Wait()
	if v := p.firstErr.Load(); v != nil {
		return *(v.(*error))
	}
	return nil
}

// blobPool reuses byte slices for blob data to reduce memory allocations
var blobPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, 0, 64*1024) // 64KB initial capacity
	},
}

// ensureDir creates a directory if not already cached, using sync.Map for lock-free reads
func ensureDir(dir string) error {
	if _, ok := dirCache.Load(dir); ok {
		return nil
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	dirCache.Store(dir, true)
	return nil
}

// runSignatureExtraction executes the signature extraction for all SOLs
func runSignatureExtraction(ctx context.Context, db *sql.DB, sols []string, procConfig *ExtractionConfig, procLogCh chan<- ProcLog, mu *sync.Mutex, procSummary map[string]ProcSummary, concurrency int) {
	var wg sync.WaitGroup
	sem := make(chan struct{}, concurrency)
	overallStart := time.Now()
	var completed atomic.Int64
	totalSols := len(sols)

	// Shared write pool across all SOL goroutines
	writePool := newSignatureWritePool()

	slog.Info("Starting signature extraction",
		"total_sols", totalSols,
		"concurrency", concurrency,
		"write_workers", signatureWriteWorkers,
		"mode", "signature")

	for _, sol := range sols {
		wg.Add(1)
		sem <- struct{}{}
		go func(solID string) {
			defer wg.Done()
			defer func() { <-sem }()

			start := time.Now()

			err := extractSignaturesForSol(ctx, db, solID, writePool)
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
			}
			procLogCh <- plog

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

			localCompleted := int(completed.Add(1))
			if localCompleted%50 == 0 || localCompleted == totalSols {
				elapsed := time.Since(overallStart)
				estimatedTotal := time.Duration(float64(elapsed) / float64(localCompleted) * float64(totalSols))
				eta := estimatedTotal - elapsed
				slog.Info("Signature extraction progress",
					"completed", localCompleted,
					"total", totalSols,
					"progress_percent", fmt.Sprintf("%.2f", float64(localCompleted)*100/float64(totalSols)),
					"elapsed", elapsed.Round(time.Second).String(),
					"eta", eta.Round(time.Second).String())
			}
		}(sol)
	}
	wg.Wait()

	// Close the shared write pool and check for write errors
	if err := writePool.close(); err != nil {
		slog.Error("Signature file write errors occurred", "error", err)
	}

	slog.Info("Signature extraction completed",
		"total_sols", totalSols,
		"files_written", writePool.metrics.filesWritten.Load(),
		"bytes_written_mb", fmt.Sprintf("%.2f", float64(writePool.metrics.bytesWritten.Load())/(1024*1024)),
		"duration", time.Since(overallStart).Round(time.Second).String())
}

// extractSignaturesForSol streams signature blobs from Oracle and writes each file
// immediately as it's scanned — no buffering of all blobs in memory.
func extractSignaturesForSol(ctx context.Context, db *sql.DB, solID string, writePool *signatureWritePool) error {
	// No ORDER BY — Oracle can start streaming rows immediately
	query := `
		SELECT filename, image
		FROM signature_table
		WHERE sol_id = :1
		AND image IS NOT NULL`

	start := time.Now()

	stmt, err := globalStmtCache.GetOrPrepare(db, query)
	if err != nil {
		return fmt.Errorf("failed to prepare signature query: %w", err)
	}

	rows, err := stmt.QueryContext(ctx, solID)
	if err != nil {
		return fmt.Errorf("signature query failed for SOL %s: %w", solID, err)
	}
	defer rows.Close()

	// Stream rows: scan each blob and dispatch write immediately
	signatureCount := int64(0)
	totalBytes := int64(0)

	for rows.Next() {
		var filename string
		var imageBlob []byte

		if err := rows.Scan(&filename, &imageBlob); err != nil {
			return fmt.Errorf("failed to scan signature row for SOL %s: %w", solID, err)
		}

		if len(imageBlob) == 0 {
			continue
		}

		// Ensure directory exists (lock-free fast path via sync.Map)
		dir := filepath.Dir(filename)
		if err := ensureDir(dir); err != nil {
			return fmt.Errorf("failed to create directory %s for SOL %s: %w", dir, solID, err)
		}

		// Copy blob data to pooled buffer and submit to shared write pool
		buf := blobPool.Get().([]byte)
		buf = buf[:0]
		buf = append(buf, imageBlob...)

		writePool.submit(filename, buf)

		signatureCount++
		totalBytes += int64(len(imageBlob))
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating signature rows for SOL %s: %w", solID, err)
	}

	queryDuration := time.Since(start)
	globalMetrics.RecordQuery(queryDuration, signatureCount, totalBytes)

	return nil
}

// runSignatureExtractionWithProcLevelParallelism runs signature extraction with procedure-level parallelism
func runSignatureExtractionWithProcLevelParallelism(ctx context.Context, db *sql.DB, sols []string, procConfig *ExtractionConfig, procLogCh chan<- ProcLog, mu *sync.Mutex, procSummary map[string]ProcSummary, concurrency int) {
	taskCh := make(chan string, len(sols))
	var wg sync.WaitGroup
	totalTasks := len(sols)
	overallStart := time.Now()
	var completed atomic.Int64

	// Shared write pool across all worker goroutines
	writePool := newSignatureWritePool()

	slog.Info("Starting procedure-level parallel signature extraction",
		"total_sols", totalTasks,
		"concurrency", concurrency,
		"write_workers", signatureWriteWorkers)

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

				err := extractSignaturesForSol(ctx, db, solID, writePool)
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

				localCompleted := int(completed.Add(1))
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

	for _, sol := range sols {
		taskCh <- sol
	}
	close(taskCh)
	wg.Wait()

	// Close the shared write pool and check for write errors
	if err := writePool.close(); err != nil {
		slog.Error("Signature file write errors occurred", "error", err)
	}

	slog.Info("Signature extraction with procedure-level parallelism completed",
		"total_tasks", totalTasks,
		"files_written", writePool.metrics.filesWritten.Load(),
		"bytes_written_mb", fmt.Sprintf("%.2f", float64(writePool.metrics.bytesWritten.Load())/(1024*1024)),
		"duration", time.Since(overallStart).Round(time.Second).String())
}
