package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"runtime"
	"sync"
	"time"
)

type ProcTask struct {
	SolID string
	Proc  string
}

func runProceduresForSol(ctx context.Context, db *sql.DB, solID string, procConfig *ExtractionConfig, logCh chan<- ProcLog, mu *sync.Mutex, summary map[string]ProcSummary) {
	for _, proc := range procConfig.Procedures {
		start := time.Now()
		log.Printf("🔁 Inserting: %s.%s for SOL %s", procConfig.PackageName, proc, solID)
		err := callProcedure(ctx, db, procConfig.PackageName, proc, solID)
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

		updateSummary(mu, summary, proc, start, end, plog.Status)
	}
}

func runProceduresWithProcLevelParallelism(ctx context.Context, db *sql.DB, sols []string, procConfig *ExtractionConfig, logCh chan<- ProcLog, mu *sync.Mutex, summary map[string]ProcSummary, concurrency int) {
	// Scale task channel buffer based on workload
	taskBufferSize := len(sols) * len(procConfig.Procedures)
	if taskBufferSize < 1000 {
		taskBufferSize = 1000
	}
	if taskBufferSize > 10000 {
		taskBufferSize = 10000 // Cap to prevent excessive memory usage
	}
	taskCh := make(chan ProcTask, taskBufferSize)
	var wg sync.WaitGroup
	totalTasks := len(sols) * len(procConfig.Procedures)
	overallStart := time.Now()
	var progressMu sync.Mutex
	completed := 0

	for range concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for task := range taskCh {
				start := time.Now()
							// Reduce verbose logging to improve performance
				if runtime.GOMAXPROCS(0) <= 4 {
					log.Printf("🔁 Inserting: %s.%s for SOL %s", procConfig.PackageName, task.Proc, task.SolID)
				}
				err := callProcedure(ctx, db, procConfig.PackageName, task.Proc, task.SolID)
				end := time.Now()

				plog := ProcLog{
					SolID:         task.SolID,
					Procedure:     task.Proc,
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

				// Update summary with reduced mutex contention
				updateSummary(mu, summary, task.Proc, start, end, plog.Status)

				// Optimize progress reporting to reduce mutex contention
				progressMu.Lock()
				completed++
				localCompleted := completed
				progressMu.Unlock()
				
				// Only log progress at intervals to reduce lock contention
				if localCompleted%100 == 0 || localCompleted == totalTasks {
					elapsed := time.Since(overallStart)
					estimatedTotal := time.Duration(float64(elapsed) / float64(localCompleted) * float64(totalTasks))
					eta := estimatedTotal - elapsed
					log.Printf("✅ Progress: %d/%d (%.2f%%) | Elapsed: %s | ETA: %s",
						localCompleted, totalTasks, float64(localCompleted)*100/float64(totalTasks),
						elapsed.Round(time.Second), eta.Round(time.Second))
				}
			}
		}()
	}

	for _, sol := range sols {
		for _, proc := range procConfig.Procedures {
			taskCh <- ProcTask{SolID: sol, Proc: proc}
		}
	}
	close(taskCh)
	wg.Wait()
}

func callProcedure(ctx context.Context, db *sql.DB, pkgName, procName, solID string) error {
	query := fmt.Sprintf("BEGIN %s.%s(:1); END;", pkgName, procName)
	start := time.Now()
	_, err := db.ExecContext(ctx, query, solID)
	
	if err != nil {
		return fmt.Errorf("failed to execute procedure %s.%s for SOL %s: %w", pkgName, procName, solID, err)
	}
	
	// Reduce verbose logging for performance - only log slow procedures
	duration := time.Since(start)
	if duration > 5*time.Second {
		log.Printf("⚠️ Slow procedure: %s.%s for SOL %s took %s", pkgName, procName, solID, duration.Round(time.Millisecond))
	} else if duration > 1*time.Second {
		log.Printf("✅ Finished: %s.%s for SOL %s in %s", pkgName, procName, solID, duration.Round(time.Millisecond))
	}
	return err
}

// updateSummary centralizes summary update logic and reduces mutex contention
func updateSummary(mu *sync.Mutex, summary map[string]ProcSummary, proc string, start, end time.Time, status string) {
	mu.Lock()
	defer mu.Unlock()
	
	s, exists := summary[proc]
	if !exists {
		s = ProcSummary{Procedure: proc, StartTime: start, EndTime: end, Status: status}
	} else {
		if start.Before(s.StartTime) {
			s.StartTime = start
		}
		if end.After(s.EndTime) {
			s.EndTime = end
		}
		if s.Status != "FAIL" && status == "FAIL" {
			s.Status = "FAIL"
		}
	}
	summary[proc] = s
}
