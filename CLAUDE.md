# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build Commands

```bash
# Build the binary
go build -o claude_extract

# Build using vendored dependencies (no network required)
go build -mod=vendor -o claude_extract

# Run
./claude_extract -appCfg /path/to/app_config.json -runCfg /path/to/extraction_config.json -mode E|I|S
```

There are no tests, linting, or CI configurations in this project.

## Architecture

This is a Go CLI tool for Oracle Database data extraction and insertion, built on the `godror` Oracle driver. It operates in three modes:

- **E (Extract):** Pulls data from Oracle stored procedures into formatted output files (delimited or fixed-width)
- **I (Insert):** Calls Oracle stored procedures to insert/process data with configurable parallelism
- **S (Signature):** Extracts signature/blob data in parallel

### Execution Flow

1. Load two JSON config files (`MainConfig` for DB/logging, `ExtractionConfig` for procedures/output)
2. Load column definitions from CSV templates and SOL identifiers from a text file
3. Configure Oracle connection pool (adaptive sizing up to 200 connections)
4. Execute mode-specific pipeline with semaphore-based concurrency control
5. Async CSV logging of all procedure executions, final summary output

### Key Files

| File | Role |
|------|------|
| `main.go` | Entry point, mode orchestration, connection pool setup, progress/ETA tracking |
| `config.go` | JSON config loading (`MainConfig`, `ExtractionConfig`) |
| `extract.go` | Core extraction: query execution, delimited/fixed-width formatting, file merging |
| `chunkextract.go` | Chunked extraction for large datasets (e.g., debit-credit balancing) |
| `runproc.go` | Procedure execution with two parallelism strategies (SOL-level vs procedure-level) |
| `signature.go` | Signature/blob extraction with parallel SOL processing |
| `cache.go` | Generic thread-safe cache and `PreparedStmtCache` with double-check locking |
| `generics.go` | Generic types: `Record[T]`, `Result[T]`, `Optional[T]`, `Batch[T]`, functional helpers |
| `types.go` | Core structs (`ProcLog`, `ColumnConfig`, `PerformanceMetrics` with atomic counters) |
| `writeLog.go` | Async CSV logging with 10-record batching |

### Performance Patterns

- `sync.Pool` for memory reuse (row buffers, string builders, byte slices)
- Pre-allocated, reused scan slices (`values`, `scanArgs`, `strValues`) outside row loops to avoid per-row allocations
- Prepared statement caching (`globalStmtCache`, `procStmtCache`) used across all query paths including chunked extraction
- Large I/O buffers: 128KB for extraction/chunk writes, 256KB for merge operations
- `io.Copy` for file merging instead of line-by-line scanning
- Buffered I/O (`bufio.Writer`) on all file write paths including chunked output
- Manual string padding in fixed-width formatting (avoids `fmt.Sprintf` in hot path)
- Truly parallel blob file writes in signature extraction (dispatch-all-then-collect pattern)
- Directory creation caching to avoid repeated `MkdirAll` calls
- Atomic counters for lock-free progress tracking and performance metrics
- Cached `runtime.GOMAXPROCS` value to avoid repeated syscalls in worker loops

### Parallelism Strategies (runproc.go)

- **SOL-level (legacy):** Each SOL processed independently with all its procedures
- **Procedure-level (modern):** All SOL-procedure combinations dispatched as independent tasks via `TaskTracker`

Controlled by `use_proc_level_parallel` in `ExtractionConfig`.
