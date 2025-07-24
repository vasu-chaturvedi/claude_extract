package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sort"
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

// Prepared statement cache for performance optimization
type PreparedStmtCache struct {
	mu    sync.RWMutex
	stmts map[string]*sql.Stmt
}

func NewPreparedStmtCache() *PreparedStmtCache {
	return &PreparedStmtCache{
		stmts: make(map[string]*sql.Stmt),
	}
}

func (c *PreparedStmtCache) GetOrPrepare(db *sql.DB, query string) (*sql.Stmt, error) {
	c.mu.RLock()
	if stmt, exists := c.stmts[query]; exists {
		c.mu.RUnlock()
		globalMetrics.RecordCacheHit()
		return stmt, nil
	}
	c.mu.RUnlock()

	c.mu.Lock()
	defer c.mu.Unlock()
	
	// Double-check pattern to avoid race condition
	if stmt, exists := c.stmts[query]; exists {
		globalMetrics.RecordCacheHit()
		return stmt, nil
	}

	globalMetrics.RecordCacheMiss()
	stmt, err := db.Prepare(query)
	if err != nil {
		return nil, err
	}
	
	c.stmts[query] = stmt
	return stmt, nil
}

func (c *PreparedStmtCache) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	for _, stmt := range c.stmts {
		stmt.Close()
	}
	c.stmts = make(map[string]*sql.Stmt)
}

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
					log.Printf("🧩 Starting chunked extraction for %s, SOL %s", proc, solID)
					chunkResultsCh := make(chan ChunkResult, 100)
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
					log.Printf("📥 Extracting %s for SOL %s", proc, solID)
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
					log.Printf("✅ Completed %s for SOL %s in %s", proc, solID, end.Sub(start).Round(time.Millisecond))
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
	log.Printf("🧮 Query executed for %s (SOL %s) in %s", procName, solID, time.Since(start).Round(time.Millisecond))

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
	
	for rows.Next() {
		// Get objects from pools to reduce allocations
		values := valuesPool.Get().([]sql.NullString)
		scanArgs := scanArgsPool.Get().([]interface{})
		
		// Resize slices if needed
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
		if err := rows.Scan(scanArgs...); err != nil {
			// Return objects to pools before error return
			valuesPool.Put(values[:0])
			scanArgsPool.Put(scanArgs[:0])
			return err
		}
		
		var strValues []string
		for _, v := range values {
			if v.Valid {
				strValues = append(strValues, v.String)
			} else {
				strValues = append(strValues, "")
			}
		}
		
		formattedRow := formatRow(cfg, cols, strValues) + "\n"
		buf.WriteString(formattedRow)
		
		rowCount++
		totalBytes += int64(len(formattedRow))
		
		// Return objects to pools for reuse
		valuesPool.Put(values[:0])
		scanArgsPool.Put(scanArgs[:0])
	}
	
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
			log.Printf("📦 Skipping merge for chunked procedure: %s (files already in final format)", proc)
			continue
		}

		log.Printf("📦 Starting merge for procedure: %s", proc)

		pattern := filepath.Join(cfg.SpoolOutputPath, fmt.Sprintf("%s_*.spool", proc))
		finalFile := filepath.Join(cfg.SpoolOutputPath, fmt.Sprintf("%s.txt", proc))

		files, err := filepath.Glob(pattern)
		if err != nil {
			return fmt.Errorf("glob failed: %w", err)
		}
		sort.Strings(files)

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
			// Use larger scanner buffer for reading (128KB)
			scanner := bufio.NewScanner(in)
			scanBuf := make([]byte, 0, 128*1024)
			scanner.Buffer(scanBuf, 1024*1024) // Max 1MB per line
			
			for scanner.Scan() {
				writer.WriteString(scanner.Text() + "\n")
			}
			in.Close()
			os.Remove(file)
		}
		writer.Flush()
		log.Printf("📑 Merged %d files into %s in %s", len(files), finalFile, time.Since(start).Round(time.Second))
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

			if col.Align == "right" {
				builder.WriteString(fmt.Sprintf("%*s", col.Length, val))
			} else {
				builder.WriteString(fmt.Sprintf("%-*s", col.Length, val))
			}
		}
		return builder.String()

	default:
		return ""
	}
}
