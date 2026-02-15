package main

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type ProcTask struct {
	SolID string
	Proc  string
}

func runProceduresForSol(ctx context.Context, db *sql.DB, solID string, procConfig *ExtractionConfig, logCh chan<- ProcLog, mu *sync.Mutex, summary map[string]ProcSummary) {
	for _, proc := range procConfig.Procedures {
		start := time.Now()
		slog.Debug("Starting procedure insertion",
			"package", procConfig.PackageName,
			"procedure", proc,
			"sol_id", solID)
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
	}
}

// TaskTracker provides enhanced tracking for SOL-procedure combinations
type TaskTracker struct {
	mu                sync.RWMutex
	totalTasks        int
	completedTasks    int
	successfulTasks   int
	failedTasks       int
	procedureCounts   map[string]int // completed per procedure
	procedureFailures map[string]int // failures per procedure
	procedureTotals   map[string]int // total per procedure
	startTime         time.Time
	lastReportTime    time.Time
}

func NewTaskTracker(sols []string, procedures []string) *TaskTracker {
	totalTasks := len(sols) * len(procedures)
	procedureTotals := make(map[string]int)
	procedureCounts := make(map[string]int)
	procedureFailures := make(map[string]int)

	for _, proc := range procedures {
		procedureTotals[proc] = len(sols)
		procedureCounts[proc] = 0
		procedureFailures[proc] = 0
	}

	return &TaskTracker{
		totalTasks:        totalTasks,
		procedureTotals:   procedureTotals,
		procedureCounts:   procedureCounts,
		procedureFailures: procedureFailures,
		startTime:         time.Now(),
		lastReportTime:    time.Now(),
	}
}

func (tt *TaskTracker) UpdateTask(procedure string, success bool) {
	tt.mu.Lock()
	defer tt.mu.Unlock()

	tt.completedTasks++
	if success {
		tt.successfulTasks++
		tt.procedureCounts[procedure]++
	} else {
		tt.failedTasks++
		tt.procedureFailures[procedure]++
	}
}

func (tt *TaskTracker) GetStats() (int, int, int, int, float64, time.Duration) {
	tt.mu.RLock()
	defer tt.mu.RUnlock()

	elapsed := time.Since(tt.startTime)
	rate := float64(tt.completedTasks) / elapsed.Seconds()

	return tt.completedTasks, tt.totalTasks, tt.successfulTasks, tt.failedTasks, rate, elapsed
}

func (tt *TaskTracker) GetProcedureStats() map[string][3]int {
	tt.mu.RLock()
	defer tt.mu.RUnlock()

	stats := make(map[string][3]int) // [completed, failed, total]
	for proc := range tt.procedureTotals {
		stats[proc] = [3]int{
			tt.procedureCounts[proc],
			tt.procedureFailures[proc],
			tt.procedureTotals[proc],
		}
	}
	return stats
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
	var completed atomic.Int64

	// Initialize enhanced task tracker
	tracker := NewTaskTracker(sols, procConfig.Procedures)

	// Start dashboard monitoring goroutine
	go func() {
		dashboardTicker := time.NewTicker(30 * time.Second)
		defer dashboardTicker.Stop()

		for range dashboardTicker.C {
			completedTasks, totalTasks, successful, failed, rate, elapsed := tracker.GetStats()
			if completedTasks > 0 {
				remaining := totalTasks - completedTasks
				eta := time.Duration(float64(remaining)/rate) * time.Second

				slog.Info("Task execution dashboard",
					"completed_tasks", completedTasks,
					"total_tasks", totalTasks,
					"progress_percent", fmt.Sprintf("%.1f", float64(completedTasks)*100/float64(totalTasks)),
					"successful", successful,
					"failed", failed,
					"rate_per_second", fmt.Sprintf("%.1f", rate),
					"elapsed", elapsed.Round(time.Second).String(),
					"eta", eta.Round(time.Second).String(),
					"sol_count", len(sols),
					"procedure_count", len(procConfig.Procedures))

				// Show database connection stats
				dbStats := db.Stats()
				slog.Info("Database connection stats",
					"active", dbStats.InUse,
					"idle", dbStats.Idle,
					"waiting", dbStats.WaitCount,
					"max_connections", dbStats.MaxOpenConnections)

				// Show performance metrics if available
				if globalMetrics != nil {
					totalQueries, avgDuration, cacheHitRate, slowQueries := globalMetrics.GetStats()
					if totalQueries > 0 {
						slog.Info("Performance metrics",
							"cache_hit_rate_percent", fmt.Sprintf("%.1f", cacheHitRate),
							"avg_query_ms", avgDuration.Milliseconds(),
							"slow_queries", slowQueries)
					}
				}

				// Show per-procedure breakdown
				procStats := tracker.GetProcedureStats()
				slog.Info("Procedure breakdown summary")
				for _, proc := range procConfig.Procedures {
					if stats, exists := procStats[proc]; exists {
						completed := stats[0] + stats[1] // success + failed
						total := stats[2]
						successCount := stats[0]
						failedCount := stats[1]
						progress := float64(completed) * 100 / float64(total)

						status := "🟢"
						if failedCount > 0 {
							status = "🟡"
							if failedCount > successCount {
								status = "🔴"
							}
						}

						slog.Info("Procedure stats",
							"procedure", proc,
							"completed", completed,
							"total", total,
							"progress_percent", fmt.Sprintf("%.1f", progress),
							"successful", successCount,
							"failed", failedCount,
							"status", status)
					}
				}
				slog.Info("Dashboard update complete")
			}
		}
	}()

	maxProcs := runtime.GOMAXPROCS(0)
	for range concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for task := range taskCh {
				start := time.Now()

				// Enhanced task logging with queue info
				queueRemaining := len(taskCh)
				completedSoFar, _, _, _, _, _ := tracker.GetStats()
				taskNumber := completedSoFar + 1

				if maxProcs <= 4 {
					slog.Debug("Starting task",
						"task_num", taskNumber,
						"total_tasks", totalTasks,
						"package", procConfig.PackageName,
						"procedure", task.Proc,
						"sol_id", task.SolID,
						"queue_remaining", queueRemaining)
				}

				err := callProcedure(ctx, db, procConfig.PackageName, task.Proc, task.SolID)
				end := time.Now()
				duration := end.Sub(start)

				plog := ProcLog{
					SolID:         task.SolID,
					Procedure:     task.Proc,
					StartTime:     start,
					EndTime:       end,
					ExecutionTime: duration,
				}

				success := err == nil
				if err != nil {
					plog.Status = "FAIL"
					plog.ErrorDetails = err.Error()
					if maxProcs <= 4 {
						slog.Error("Task failed",
							"task_num", taskNumber,
							"total_tasks", totalTasks,
							"package", procConfig.PackageName,
							"procedure", task.Proc,
							"sol_id", task.SolID,
							"duration", duration.Round(time.Millisecond).String(),
							"error", err.Error())
					}
				} else {
					plog.Status = "SUCCESS"
					if maxProcs <= 4 {
						slog.Debug("Task completed successfully",
							"task_num", taskNumber,
							"total_tasks", totalTasks,
							"package", procConfig.PackageName,
							"procedure", task.Proc,
							"sol_id", task.SolID,
							"duration", duration.Round(time.Millisecond).String())
					}
				}
				logCh <- plog

				// Update enhanced tracker
				tracker.UpdateTask(task.Proc, success)

				// Batch summary updates to reduce mutex contention
				mu.Lock()
				s, exists := summary[task.Proc]
				if !exists {
					s = ProcSummary{Procedure: task.Proc, StartTime: start, EndTime: end, Status: plog.Status}
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
				summary[task.Proc] = s
				mu.Unlock()

				// Enhanced progress reporting using atomic counter
				localCompleted := int(completed.Add(1))

				// More frequent progress updates with enhanced info
				if localCompleted%100 == 0 || localCompleted == totalTasks {
					elapsed := time.Since(overallStart)
					rate := float64(localCompleted) / elapsed.Seconds()
					eta := time.Duration(float64(totalTasks-localCompleted)/rate) * time.Second

					slog.Info("Task progress",
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
		for _, proc := range procConfig.Procedures {
			taskCh <- ProcTask{SolID: sol, Proc: proc}
		}
	}
	close(taskCh)
	wg.Wait()
}

// Prepared statement cache for procedure calls
var procStmtCache = NewPreparedStmtCache()

func callProcedure(ctx context.Context, db *sql.DB, pkgName, procName, solID string) error {
	query := fmt.Sprintf("BEGIN %s.%s(:1); END;", pkgName, procName)
	start := time.Now()

	// Use prepared statement cache for procedure calls too
	stmt, err := procStmtCache.GetOrPrepare(db, query)
	if err != nil {
		return fmt.Errorf("failed to prepare procedure statement: %w", err)
	}

	_, err = stmt.ExecContext(ctx, solID)

	// Reduce verbose logging for performance - only log slow procedures
	duration := time.Since(start)
	if duration > 5*time.Second {
		slog.Warn("Slow procedure detected",
			"package", pkgName,
			"procedure", procName,
			"sol_id", solID,
			"duration", duration.Round(time.Millisecond).String())
	} else if duration > 1*time.Second {
		slog.Debug("Procedure completed",
			"package", pkgName,
			"procedure", procName,
			"sol_id", solID,
			"duration", duration.Round(time.Millisecond).String())
	}
	return err
}
