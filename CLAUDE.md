# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

`claude_extract` is a high-performance Go application for Oracle database data extraction and insertion operations. It processes SOL (Solution) IDs through configurable database procedures with advanced concurrency, connection pooling, and chunked processing capabilities.

## Key Architecture Components

### Core Processing Modes
- **Extract Mode (-mode E)**: Extracts data from Oracle procedures, outputs to CSV files
- **Insert Mode (-mode I)**: Executes Oracle procedures for data insertion with two parallelism strategies:
  - SOL-level parallel: Legacy mode processing one SOL at a time
  - Procedure-level parallel: Modern mode with fine-grained task distribution

### Configuration System
The application uses a dual-configuration approach:
- **Main Config** (`-appCfg`): Database connection, concurrency settings, file paths
- **Extraction Config** (`-runCfg`): Procedure definitions, parallelism controls, chunked processing

### Performance Architecture
- **Connection Pooling**: Dynamic pool sizing based on concurrency × procedure count (capped at 200)
- **Prepared Statement Caching**: Global cache with hit/miss tracking (`globalStmtCache`, `procStmtCache`)
- **Memory Pools**: Object pooling for scan arguments, values, and string builders
- **Chunked Processing**: Handles large datasets through Oracle cursor-based chunking with `_EXTRACT` suffixed procedures

### Concurrency Models
- **TaskTracker**: Enhanced tracking for SOL-procedure combinations with per-procedure metrics
- **Worker Pools**: CPU-based worker allocation (NumCPU × 2 for extraction)
- **Channel-based Communication**: Buffered channels with dynamic sizing based on workload

## Development Commands

### Build and Run
```bash
# Build the application
go build -o claude_extract

# Run extraction mode
./claude_extract -appCfg path/to/app.json -runCfg path/to/extraction.json -mode E

# Run insertion mode (SOL-level parallel)
./claude_extract -appCfg path/to/app.json -runCfg path/to/extraction.json -mode I

# Run insertion mode (procedure-level parallel - recommended)
# Set "use_proc_level_parallel": true in extraction config
```

### Testing and Development
```bash
# Run tests
go test ./...

# Run with race detection
go run -race main.go -appCfg config.json -runCfg extraction.json -mode E

# Build with optimizations
go build -ldflags="-s -w" -o claude_extract

# Check dependencies
go mod tidy
go mod verify
```

## Configuration Structure

### Main Configuration (`app.json`)
```json
{
  "db_user": "username",
  "db_password": "password", 
  "db_host": "hostname",
  "db_port": 1521,
  "db_sid": "ORCL",
  "concurrency": 10,
  "log_path": "/path/to/logs",
  "sol_list_path": "/path/to/sol_ids.txt"
}
```

### Extraction Configuration (`extraction.json`)
```json
{
  "package_name": "PKG_NAME",
  "procedures": ["PROC1", "PROC2"],
  "spool_output_path": "/output/path",
  "run_insertion_parallel": true,
  "run_extraction_parallel": true,
  "use_proc_level_parallel": true,
  "template_path": "/templates",
  "chunked_procedures": ["LARGE_PROC"],
  "chunk_size": 5000
}
```

## Key Implementation Details

### Modern Go Features (Go 1.24.5)
- **Structured Logging**: Uses `log/slog` for structured application logs with contextual information
- **Generics**: Type-safe caching system with `Cache[K, V]` and specialized `PreparedStmtCache`
- **Error Wrapping**: Proper error context with `fmt.Errorf("%w", err)` throughout
- **Modern Packages**: Uses `slices` package for sorting and slice operations

### Oracle Integration
- Uses `github.com/godror/godror` driver for Oracle connectivity
- Connection string format: `user="x" password="y" connectString="host:port/sid"`
- Generic prepared statement cache with type safety and performance metrics

### Chunked Processing Logic
- Procedures in `chunked_procedures` array use `_EXTRACT` suffixed variants
- Chunks processed sequentially per SOL until `hasMore` returns false
- Each chunk limited by `chunk_size` parameter (default: 5000 records)

### Performance Monitoring & Logging
- **Structured Logging**: JSON-formatted logs with contextual metadata for production monitoring
- **Real-time Metrics**: Performance tracking via `PerformanceMetrics` struct
- **Connection Pool Stats**: Database connection monitoring with slog integration
- **Cache Performance**: Hit/miss ratios tracked with generic cache implementation

### File Operations
- CSV templates define column formatting from `template_path`
- Extraction outputs individual procedure files, merged at completion
- Async logging with batched writes (10 records per batch) to reduce I/O overhead
- **Separate Concerns**: CSV files for business analytics, slog for operational monitoring

## Error Handling Patterns

- Graceful degradation with connection pool warnings when exceeding 200 connections
- Statement cache miss/hit tracking for performance optimization
- Per-procedure status tracking with detailed error logging
- SOL processing continues despite individual procedure failures

## Dependencies

- `github.com/godror/godror v0.49.1`: Oracle database driver
- Go 1.24.5 minimum required version
- Vendor directory included for dependency management