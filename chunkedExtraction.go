package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// ChunkResult represents the result of a chunk extraction
type ChunkResult struct {
	SolID      string
	Procedure  string
	ChunkNum   int
	TotalChunks int
	Records    int
	StartTime  time.Time
	EndTime    time.Time
	Status     string
	Error      error
}

// runChunkedExtractionForSol performs chunked extraction for a single SOL with debit-credit balancing
func runChunkedExtractionForSol(ctx context.Context, db *sql.DB, solID string, procedure string, config *ExtractionConfig, templates map[string][]ColumnConfig, logCh chan<- ProcLog, chunkResultsCh chan<- ChunkResult) {
	startTime := time.Now()
	log.Printf("🧩 Starting chunked extraction for SOL %s, procedure %s", solID, procedure)
	
	// Get chunk procedure name - automatically add "_EXTRACT" suffix
	chunkProcName := procedure + "_EXTRACT"
	
	chunkNum := 1
	totalRecords := 0
	
	for {
		chunkStart := time.Now()
		log.Printf("🔄 Processing chunk %d for SOL %s, procedure %s", chunkNum, solID, procedure)
		
		// Call the chunk procedure to get debit records and generate credit legs
		records, hasMore, err := callChunkProcedure(ctx, db, config.PackageName, chunkProcName, solID, chunkNum, config.ChunkSize)
		chunkEnd := time.Now()
		
		if err != nil {
			log.Printf("❌ Chunk %d failed for SOL %s, procedure %s: %v", chunkNum, solID, procedure, err)
			
			// Log the failure
			plog := ProcLog{
				SolID:         solID,
				Procedure:     procedure,
				StartTime:     chunkStart,
				EndTime:       chunkEnd,
				ExecutionTime: chunkEnd.Sub(chunkStart),
				Status:        "FAIL",
				ErrorDetails:  fmt.Sprintf("Chunk %d failed: %v", chunkNum, err),
			}
			logCh <- plog
			
			// Send chunk result
			chunkResultsCh <- ChunkResult{
				SolID:     solID,
				Procedure: procedure,
				ChunkNum:  chunkNum,
				Records:   0,
				StartTime: chunkStart,
				EndTime:   chunkEnd,
				Status:    "FAIL",
				Error:     err,
			}
			return
		}
		
		if len(records) == 0 && chunkNum == 1 {
			log.Printf("📄 No records found for SOL %s, procedure %s", solID, procedure)
			
			// Create empty file for consistency
			fileName := generateChunkFileName(solID, procedure, 1, 1, config.SpoolOutputPath)
			if err := createEmptyFile(fileName); err != nil {
				log.Printf("❌ Failed to create empty file %s: %v", fileName, err)
			}
			
			// Log success with no records
			plog := ProcLog{
				SolID:         solID,
				Procedure:     procedure,
				StartTime:     chunkStart,
				EndTime:       chunkEnd,
				ExecutionTime: chunkEnd.Sub(chunkStart),
				Status:        "SUCCESS",
				ErrorDetails:  "",
			}
			logCh <- plog
			
			chunkResultsCh <- ChunkResult{
				SolID:      solID,
				Procedure:  procedure,
				ChunkNum:   1,
				TotalChunks: 1,
				Records:    0,
				StartTime:  chunkStart,
				EndTime:    chunkEnd,
				Status:     "SUCCESS",
				Error:      nil,
			}
			return
		}
		
		if len(records) > 0 {
			// Generate file name for this chunk
			fileName := generateChunkFileName(solID, procedure, chunkNum, -1, config.SpoolOutputPath) // -1 means we don't know total yet
			
			// Write chunk to file
			if err := writeChunkToFile(fileName, records, templates[procedure], config); err != nil {
				log.Printf("❌ Failed to write chunk %d for SOL %s, procedure %s: %v", chunkNum, solID, procedure, err)
				
				plog := ProcLog{
					SolID:         solID,
					Procedure:     procedure,
					StartTime:     chunkStart,
					EndTime:       chunkEnd,
					ExecutionTime: chunkEnd.Sub(chunkStart),
					Status:        "FAIL",
					ErrorDetails:  fmt.Sprintf("Failed to write chunk %d: %v", chunkNum, err),
				}
				logCh <- plog
				
				chunkResultsCh <- ChunkResult{
					SolID:     solID,
					Procedure: procedure,
					ChunkNum:  chunkNum,
					Records:   len(records),
					StartTime: chunkStart,
					EndTime:   chunkEnd,
					Status:    "FAIL",
					Error:     err,
				}
				return
			}
			
			totalRecords += len(records)
			log.Printf("✅ Chunk %d completed for SOL %s, procedure %s: %d records", chunkNum, solID, procedure, len(records))
			
			// Log successful chunk
			plog := ProcLog{
				SolID:         solID,
				Procedure:     procedure,
				StartTime:     chunkStart,
				EndTime:       chunkEnd,
				ExecutionTime: chunkEnd.Sub(chunkStart),
				Status:        "SUCCESS",
				ErrorDetails:  "",
			}
			logCh <- plog
			
			chunkResultsCh <- ChunkResult{
				SolID:     solID,
				Procedure: procedure,
				ChunkNum:  chunkNum,
				Records:   len(records),
				StartTime: chunkStart,
				EndTime:   chunkEnd,
				Status:    "SUCCESS",
				Error:     nil,
			}
		}
		
		// Check if there are more chunks
		if !hasMore {
			break
		}
		
		chunkNum++
	}
	
	// Rename files to include total chunk count if multiple chunks
	if chunkNum > 1 {
		renameChunkFiles(solID, procedure, chunkNum, config.SpoolOutputPath)
	}
	
	log.Printf("🎯 Completed chunked extraction for SOL %s, procedure %s: %d chunks, %d total records in %s", 
		solID, procedure, chunkNum, totalRecords, time.Since(startTime).Round(time.Millisecond))
}

// callChunkProcedure calls the Oracle procedure to get a chunk of debit records and their credit legs
func callChunkProcedure(ctx context.Context, db *sql.DB, pkgName, procName, solID string, chunkNum, chunkSize int) ([]map[string]interface{}, bool, error) {
	// This procedure should return:
	// 1. Up to N debit records for the chunk
	// 2. All required credit legs for those debits (using mapping tables)
	// 3. A flag indicating if there are more chunks
	
	query := fmt.Sprintf(`
		DECLARE
			v_has_more NUMBER;
		BEGIN
			%s.%s(
				p_sol_id => :1,
				p_chunk_num => :2,
				p_chunk_size => :3,
				p_has_more => v_has_more
			);
			:4 := v_has_more;
		END;`, pkgName, procName)
	
	var hasMoreFlag int
	_, err := db.ExecContext(ctx, query, solID, chunkNum, chunkSize, sql.Out{Dest: &hasMoreFlag})
	if err != nil {
		return nil, false, fmt.Errorf("failed to execute chunk procedure: %w", err)
	}
	
	// Now fetch the results from the procedure's output (assuming it writes to a temp table or returns a cursor)
	// This is a simplified example - in practice, you'd need to fetch from the procedure's output
	resultQuery := fmt.Sprintf(`
		SELECT * FROM %s_CHUNK_RESULTS 
		WHERE sol_id = :1 AND chunk_num = :2 
		ORDER BY record_type, record_id`, pkgName)
	
	rows, err := db.QueryContext(ctx, resultQuery, solID, chunkNum)
	if err != nil {
		return nil, false, fmt.Errorf("failed to fetch chunk results: %w", err)
	}
	defer rows.Close()
	
	// Convert rows to map for flexible handling
	records, err := rowsToMaps(rows)
	if err != nil {
		return nil, false, fmt.Errorf("failed to convert rows to maps: %w", err)
	}
	
	return records, hasMoreFlag == 1, nil
}

// generateChunkFileName generates the appropriate file name for a chunk
func generateChunkFileName(solID, procedure string, chunkNum, totalChunks int, outputPath string) string {
	if totalChunks == 1 || totalChunks == -1 {
		// Single chunk or unknown total yet
		if chunkNum == 1 && totalChunks == 1 {
			return filepath.Join(outputPath, fmt.Sprintf("%s_%s.txt", solID, procedure))
		}
		return filepath.Join(outputPath, fmt.Sprintf("%s_%s_%d.txt", solID, procedure, chunkNum))
	}
	
	// Multiple chunks
	return filepath.Join(outputPath, fmt.Sprintf("%s_%s_%d.txt", solID, procedure, chunkNum))
}

// writeChunkToFile writes a chunk of records to a file
func writeChunkToFile(fileName string, records []map[string]interface{}, columns []ColumnConfig, config *ExtractionConfig) error {
	file, err := os.Create(fileName)
	if err != nil {
		return fmt.Errorf("failed to create file %s: %w", fileName, err)
	}
	defer file.Close()
	
	// Write records based on format
	if config.Format == "delimited" {
		return writeDelimitedChunk(file, records, columns, config.Delimiter)
	} else {
		return writeFixedWidthChunk(file, records, columns)
	}
}

// writeDelimitedChunk writes records in delimited format
func writeDelimitedChunk(file *os.File, records []map[string]interface{}, columns []ColumnConfig, delimiter string) error {
	for _, record := range records {
		var values []string
		for _, col := range columns {
			value := ""
			if val, exists := record[col.Name]; exists && val != nil {
				value = fmt.Sprintf("%v", val)
			}
			values = append(values, value)
		}
		
		var line strings.Builder
		for i, value := range values {
			if i > 0 {
				line.WriteString(delimiter)
			}
			line.WriteString(value)
		}
		line.WriteString("\n")
		
		if _, err := file.WriteString(line.String()); err != nil {
			return err
		}
	}
	return nil
}

// writeFixedWidthChunk writes records in fixed-width format
func writeFixedWidthChunk(file *os.File, records []map[string]interface{}, columns []ColumnConfig) error {
	for _, record := range records {
		line := ""
		for _, col := range columns {
			value := ""
			if val, exists := record[col.Name]; exists && val != nil {
				value = fmt.Sprintf("%v", val)
			}
			
			// Apply fixed-width formatting
			if len(value) > col.Length {
				value = value[:col.Length]
			}
			
			if col.Align == "right" {
				value = fmt.Sprintf("%*s", col.Length, value)
			} else {
				value = fmt.Sprintf("%-*s", col.Length, value)
			}
			
			line += value
		}
		line += "\n"
		
		if _, err := file.WriteString(line); err != nil {
			return err
		}
	}
	return nil
}

// renameChunkFiles renames chunk files to include total chunk count
func renameChunkFiles(solID, procedure string, totalChunks int, outputPath string) {
	for i := range totalChunks {
		oldName := filepath.Join(outputPath, fmt.Sprintf("%s_%s_%d.txt", solID, procedure, i+1))
		newName := filepath.Join(outputPath, fmt.Sprintf("%s_%s_%d.txt", solID, procedure, i+1))
		
		// In this case, the naming is already correct, but this function exists
		// for potential future enhancements where we might want to rename based on total chunks
		_ = oldName
		_ = newName
	}
}

// createEmptyFile creates an empty file for SOLs with no records
func createEmptyFile(fileName string) error {
	file, err := os.Create(fileName)
	if err != nil {
		return err
	}
	defer file.Close()
	return nil
}

// rowsToMaps converts SQL rows to a slice of maps for flexible handling
func rowsToMaps(rows *sql.Rows) ([]map[string]interface{}, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	
	var results []map[string]interface{}
	
	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		
		for i := range values {
			valuePtrs[i] = &values[i]
		}
		
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}
		
		record := make(map[string]interface{})
		for i, col := range columns {
			record[col] = values[i]
		}
		
		results = append(results, record)
	}
	
	return results, rows.Err()
}

// runChunkedExtractionForProcedure handles chunked extraction for all SOLs for a given procedure
func runChunkedExtractionForProcedure(ctx context.Context, db *sql.DB, sols []string, procedure string, config *ExtractionConfig, templates map[string][]ColumnConfig, logCh chan<- ProcLog, mu *sync.Mutex, summary map[string]ProcSummary, concurrency int) {
	chunkResultsCh := make(chan ChunkResult, len(sols)*10) // Buffer for chunk results
	defer close(chunkResultsCh)
	
	// Start result collector
	go func() {
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
	}()
	
	// Process SOLs with limited concurrency
	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup
	
	for _, sol := range sols {
		wg.Add(1)
		sem <- struct{}{}
		
		go func(solID string) {
			defer wg.Done()
			defer func() { <-sem }()
			
			runChunkedExtractionForSol(ctx, db, solID, procedure, config, templates, logCh, chunkResultsCh)
		}(sol)
	}
	
	wg.Wait()
}