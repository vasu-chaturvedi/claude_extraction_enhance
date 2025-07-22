package main

import (
	"context"
	"io"
	"log/slog"
	"os"
)

// Logger wraps slog with application-specific functionality
type Logger struct {
	*slog.Logger
	verbose bool
	quiet   bool
}

// NewLogger creates a new structured logger with appropriate configuration
func NewLogger(verbose, quiet bool) *Logger {
	var level slog.Level
	var output io.Writer = os.Stdout

	// Set log level based on verbosity
	switch {
	case quiet:
		level = slog.LevelError
	case verbose:
		level = slog.LevelDebug
	default:
		level = slog.LevelInfo
	}

	// Create structured logger with JSON formatting for production
	opts := &slog.HandlerOptions{
		Level: level,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			// Customize timestamp format
			if a.Key == slog.TimeKey {
				a.Value = slog.StringValue(a.Value.Time().Format("2006-01-02 15:04:05"))
			}
			return a
		},
	}

	var handler slog.Handler
	if verbose {
		// Use text handler for verbose/debug mode (more readable)
		handler = slog.NewTextHandler(output, opts)
	} else {
		// Use JSON handler for production (structured logging)
		handler = slog.NewJSONHandler(output, opts)
	}

	logger := slog.New(handler)

	return &Logger{
		Logger:  logger,
		verbose: verbose,
		quiet:   quiet,
	}
}

// Info logs informational messages with structured data
func (l *Logger) Info(msg string, args ...any) {
	if l.quiet {
		return
	}
	l.Logger.Info(msg, args...)
}

// Debug logs debug messages (only when verbose mode is enabled)
func (l *Logger) Debug(msg string, args ...any) {
	if !l.verbose || l.quiet {
		return
	}
	l.Logger.Debug(msg, args...)
}

// Warn logs warning messages
func (l *Logger) Warn(msg string, args ...any) {
	l.Logger.Warn(msg, args...)
}

// Error logs error messages
func (l *Logger) Error(msg string, args ...any) {
	l.Logger.Error(msg, args...)
}

// With returns a logger with additional context fields
func (l *Logger) With(args ...any) LoggerInterface {
	return &Logger{
		Logger:  l.Logger.With(args...),
		verbose: l.verbose,
		quiet:   l.quiet,
	}
}

// WithContext returns a logger that includes context values
func (l *Logger) WithContext(ctx context.Context) LoggerInterface {
	return &Logger{
		Logger:  l.Logger.With(slog.Any("trace_id", ctx.Value("trace_id"))),
		verbose: l.verbose,
		quiet:   l.quiet,
	}
}

// LogDatabaseConnection logs database connection attempts
func (l *Logger) LogDatabaseConnection(user, host string, port int, sid string) {
	l.Info("connecting to database",
		slog.String("user", user),
		slog.String("host", host),
		slog.Int("port", port),
		slog.String("sid", sid),
	)
}

// LogProcedureExecution logs procedure execution with timing
func (l *Logger) LogProcedureExecution(solID, procedure, status string, duration int64, err error) {
	attrs := []any{
		slog.String("sol_id", solID),
		slog.String("procedure", procedure),
		slog.String("status", status),
		slog.Int64("duration_ms", duration),
	}
	
	if err != nil {
		attrs = append(attrs, slog.String("error", err.Error()))
		l.Error("procedure execution failed", attrs...)
	} else {
		l.Debug("procedure executed", attrs...)
	}
}

// LogProgress logs processing progress with metrics
func (l *Logger) LogProgress(completed, total int, throughput float64, errorRate float64) {
	if l.quiet {
		return
	}
	
	l.Info("processing progress",
		slog.Int("completed", completed),
		slog.Int("total", total),
		slog.Float64("completion_percent", float64(completed)/float64(total)*100),
		slog.Float64("throughput_per_min", throughput),
		slog.Float64("error_rate_percent", errorRate),
	)
}

// LogPerformanceMetrics logs performance statistics
func (l *Logger) LogPerformanceMetrics(metrics PerformanceMetrics) {
	if !l.verbose {
		return
	}
	
	sols, procs, errors, avgResponse, _ := metrics.GetMetrics()
	l.Debug("performance metrics",
		slog.Int64("sols_processed", sols),
		slog.Int64("procedures_called", procs),
		slog.Int64("errors", errors),
		slog.Duration("avg_response_time", avgResponse),
		slog.Float64("throughput_per_min", metrics.GetThroughput()),
		slog.Float64("error_rate_percent", metrics.GetErrorRate()),
	)
}