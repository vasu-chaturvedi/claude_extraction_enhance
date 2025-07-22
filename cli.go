package main

import (
	"fmt"
	"os"
	"strings"
	"time"
)

// ProgressBar represents a simple CLI progress indicator
type ProgressBar struct {
	total     int
	current   int
	width     int
	prefix    string
	startTime time.Time
}

// NewProgressBar creates a new progress bar
func NewProgressBar(total int, prefix string) *ProgressBar {
	return &ProgressBar{
		total:     total,
		current:   0,
		width:     50,
		prefix:    prefix,
		startTime: time.Now(),
	}
}

// Update updates the progress bar
func (pb *ProgressBar) Update(current int) {
	if *quiet {
		return // Don't show progress in quiet mode
	}
	
	pb.current = current
	percent := float64(current) / float64(pb.total) * 100
	filled := int(float64(pb.width) * float64(current) / float64(pb.total))
	
	bar := strings.Repeat("█", filled) + strings.Repeat("░", pb.width-filled)
	
	elapsed := time.Since(pb.startTime)
	var eta string
	if current > 0 {
		remaining := time.Duration(float64(elapsed) * float64(pb.total-current) / float64(current))
		eta = fmt.Sprintf("ETA: %s", remaining.Round(time.Second))
	} else {
		eta = "ETA: calculating..."
	}
	
	fmt.Printf("\r%s [%s] %d/%d (%.1f%%) %s", 
		pb.prefix, bar, current, pb.total, percent, eta)
	
	if current >= pb.total {
		fmt.Printf("\n✅ %s completed in %s\n", pb.prefix, elapsed.Round(time.Second))
	}
}

// Finish completes the progress bar
func (pb *ProgressBar) Finish() {
	pb.Update(pb.total)
}

// CLILogger provides CLI-appropriate logging
type CLILogger struct {
	verbose bool
	quiet   bool
}

// NewCLILogger creates a new CLI logger
func NewCLILogger(verbose, quiet bool) *CLILogger {
	return &CLILogger{
		verbose: verbose,
		quiet:   quiet,
	}
}

// Info logs informational messages
func (l *CLILogger) Info(format string, args ...interface{}) {
	if l.quiet {
		return
	}
	fmt.Printf("ℹ️  "+format+"\n", args...)
}

// Success logs success messages
func (l *CLILogger) Success(format string, args ...interface{}) {
	if l.quiet {
		return
	}
	fmt.Printf("✅ "+format+"\n", args...)
}

// Warning logs warning messages
func (l *CLILogger) Warning(format string, args ...interface{}) {
	fmt.Printf("⚠️  "+format+"\n", args...)
}

// Error logs error messages
func (l *CLILogger) Error(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, "❌ "+format+"\n", args...)
}

// Verbose logs verbose messages (only when verbose mode is enabled)
func (l *CLILogger) Verbose(format string, args ...interface{}) {
	if !l.verbose || l.quiet {
		return
	}
	fmt.Printf("🔍 "+format+"\n", args...)
}

// Debug logs debug messages (only when verbose mode is enabled)
func (l *CLILogger) Debug(format string, args ...interface{}) {
	if !l.verbose || l.quiet {
		return
	}
	fmt.Printf("🐛 "+format+"\n", args...)
}

// ExitCode constants for CLI application
const (
	ExitSuccess       = 0
	ExitGeneralError  = 1
	ExitUsageError    = 2
	ExitConfigError   = 3
	ExitDatabaseError = 4
	ExitFileError     = 5
)

// ExitWithError exits the application with an error code and message
func ExitWithError(code int, format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, "❌ "+format+"\n", args...)
	os.Exit(code)
}

// PrintBanner prints a CLI banner for the application
func PrintBanner() {
	if *quiet {
		return
	}
	
	banner := `
╔══════════════════════════════════════════════════════╗
║                 Claude Extract v1.0                  ║
║           Oracle Database Extraction Tool            ║
╚══════════════════════════════════════════════════════╝
`
	fmt.Println(banner)
}

// PrintSummary prints execution summary
func PrintSummary(startTime time.Time, totalSOLs int, mode string, errors int) {
	if *quiet {
		return
	}
	
	elapsed := time.Since(startTime)
	operation := "Extraction"
	if mode == "I" {
		operation = "Insertion"
	}
	
	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Printf("📊 %s Summary\n", operation)
	fmt.Println(strings.Repeat("=", 60))
	fmt.Printf("Total SOLs processed: %d\n", totalSOLs)
	fmt.Printf("Execution time: %s\n", elapsed.Round(time.Second))
	fmt.Printf("Average time per SOL: %s\n", (elapsed/time.Duration(totalSOLs)).Round(time.Millisecond))
	
	if errors > 0 {
		fmt.Printf("Errors encountered: %d\n", errors)
		fmt.Printf("Success rate: %.2f%%\n", float64(totalSOLs-errors)/float64(totalSOLs)*100)
	} else {
		fmt.Printf("✅ All operations completed successfully!\n")
	}
	fmt.Println(strings.Repeat("=", 60))
}