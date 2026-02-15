package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"log/slog"
	"os"
	"slices"
)

// Optimized async logging with batching to reduce I/O bottlenecks
func writeLog(path string, logCh <-chan ProcLog) {
	file, err := os.Create(path)
	if err != nil {
		slog.Error("Failed to create procedure log file", "path", path, "error", err)
		return
	}
	defer file.Close()
	slog.Info("Procedure log file created", "path", path)

	// Use buffered writer for better I/O performance
	bufWriter := bufio.NewWriterSize(file, 64*1024) // 64KB buffer
	defer bufWriter.Flush()

	writer := csv.NewWriter(bufWriter)
	defer writer.Flush()

	// Write header
	writer.Write([]string{"SOL_ID", "PROCEDURE", "START_TIME", "END_TIME", "EXECUTION_SECONDS", "STATUS", "ERROR_DETAILS"})

	// Batch writes to reduce I/O overhead (reduced for more frequent updates)
	batchSize := 10
	batch := make([][]string, 0, batchSize)
	timeFormat := "02-01-2006 15:04:05"

	for plog := range logCh {
		errDetails := plog.ErrorDetails
		if errDetails == "" {
			errDetails = "-"
		}
		record := []string{
			plog.SolID,
			plog.Procedure,
			plog.StartTime.Format(timeFormat),
			plog.EndTime.Format(timeFormat),
			fmt.Sprintf("%.3f", plog.ExecutionTime.Seconds()),
			plog.Status,
			errDetails,
		}
		batch = append(batch, record)

		// Write batch when full
		if len(batch) >= batchSize {
			writer.WriteAll(batch)
			writer.Flush()
			batch = batch[:0] // Reset batch
		}
	}

	// Write remaining records
	if len(batch) > 0 {
		writer.WriteAll(batch)
	}
}

// Write procedure summary CSV after all executions
func writeSummary(path string, summary map[string]ProcSummary) {
	file, err := os.Create(path)
	if err != nil {
		slog.Error("Failed to create procedure summary file", "path", path, "error", err)
		return
	}
	defer file.Close()
	slog.Info("Writing procedure summary", "path", path, "procedure_count", len(summary))

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Header
	writer.Write([]string{"PROCEDURE", "EARLIEST_START_TIME", "LATEST_END_TIME", "EXECUTION_SECONDS", "STATUS"})

	// Sort procedures alphabetically
	var procs []string
	for p := range summary {
		procs = append(procs, p)
	}
	slices.Sort(procs)

	for _, p := range procs {
		s := summary[p]
		execSeconds := s.EndTime.Sub(s.StartTime).Seconds()
		timeFormat := "02-01-2006 15:04:05"
		writer.Write([]string{
			p,
			s.StartTime.Format(timeFormat),
			s.EndTime.Format(timeFormat),
			fmt.Sprintf("%.3f", execSeconds),
			s.Status,
		})
	}
}
