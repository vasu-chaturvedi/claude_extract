package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/godror/godror"
)

var (
	appCfgFile = new(string)
	runCfgFile = new(string)
	mode       string
)

// WorkItem represents a single unit of work: one procedure for one SOL.
type WorkItem struct {
	SolID     string
	Procedure string
}

// ProcStats tracks execution metrics for procedures
type ProcStats struct {
	TotalExecutions int64
	TotalDuration   time.Duration
	FailureCount    int64
	LastExecution   time.Time
	AvgDuration     time.Duration
}

// CircuitBreaker manages procedure health
type CircuitBreaker struct {
	FailureThreshold int
	TimeoutThreshold time.Duration
	ResetTimeout     time.Duration
	State            string // "CLOSED", "OPEN", "HALF_OPEN"
	FailureCount     int
	LastFailTime     time.Time
}

// Pipeline represents different execution lanes
type Pipeline struct {
	FastLane   chan WorkItem // < 5s procedures
	MediumLane chan WorkItem // 5-30s procedures  
	SlowLane   chan WorkItem // > 30s procedures
	ErrorLane  chan WorkItem // Failed/circuit-broken procedures
}

// WorkerPool manages the hybrid pipeline architecture
type WorkerPool struct {
	Pipeline       *Pipeline
	ProcStats      map[string]*ProcStats
	CircuitBreakers map[string]*CircuitBreaker
	FastWorkers    int
	MediumWorkers  int
	SlowWorkers    int
	ErrorWorkers   int
	StatsMutex     sync.RWMutex
}

func init() {
	flag.StringVar(appCfgFile, "appCfg", "", "Path to the main application configuration file")
	flag.StringVar(runCfgFile, "runCfg", "", "Path to the extraction configuration file")
	flag.StringVar(&mode, "mode", "", "Mode of operation: E - Extract, I - Insert")
	flag.Parse()

	if mode != "E" && mode != "I" {
		log.Fatal("Invalid mode. Valid values are 'E' for Extract and 'I' for Insert.")
	}
	if *appCfgFile == "" || *runCfgFile == "" {
		log.Fatal("Both appCfg and runCfg must be specified")
	}
	if _, err := os.Stat(*appCfgFile); os.IsNotExist(err) {
		log.Fatalf("Application configuration file does not exist: %s", *appCfgFile)
	}
	if _, err := os.Stat(*runCfgFile); os.IsNotExist(err) {
		log.Fatalf("Extraction configuration file does not exist: %s", *runCfgFile)
	}
}

// createWorkerPool initializes the hybrid pipeline architecture
func createWorkerPool(concurrency int, procedures []string) *WorkerPool {
	pipeline := &Pipeline{
		FastLane:   make(chan WorkItem, 1000),
		MediumLane: make(chan WorkItem, 1000),
		SlowLane:   make(chan WorkItem, 1000),
		ErrorLane:  make(chan WorkItem, 100),
	}
	
	// Distribute workers: 50% fast, 30% medium, 15% slow, 5% error
	fastWorkers := int(float64(concurrency) * 0.5)
	mediumWorkers := int(float64(concurrency) * 0.3)
	slowWorkers := int(float64(concurrency) * 0.15)
	errorWorkers := concurrency - fastWorkers - mediumWorkers - slowWorkers
	
	if fastWorkers == 0 { fastWorkers = 1 }
	if mediumWorkers == 0 { mediumWorkers = 1 }
	if slowWorkers == 0 { slowWorkers = 1 }
	if errorWorkers == 0 { errorWorkers = 1 }
	
	wp := &WorkerPool{
		Pipeline:        pipeline,
		ProcStats:       make(map[string]*ProcStats),
		CircuitBreakers: make(map[string]*CircuitBreaker),
		FastWorkers:     fastWorkers,
		MediumWorkers:   mediumWorkers,
		SlowWorkers:     slowWorkers,
		ErrorWorkers:    errorWorkers,
	}
	
	// Initialize stats and circuit breakers for each procedure
	for _, proc := range procedures {
		wp.ProcStats[proc] = &ProcStats{}
		wp.CircuitBreakers[proc] = &CircuitBreaker{
			FailureThreshold: 5,
			TimeoutThreshold: 60 * time.Second,
			ResetTimeout:     5 * time.Minute,
			State:           "CLOSED",
		}
	}
	
	return wp
}

// categorizeWorkItem determines which lane a work item should go to
func (wp *WorkerPool) categorizeWorkItem(item WorkItem) chan WorkItem {
	wp.StatsMutex.RLock()
	defer wp.StatsMutex.RUnlock()
	
	stats := wp.ProcStats[item.Procedure]
	breaker := wp.CircuitBreakers[item.Procedure]
	
	// Check circuit breaker state
	if breaker.State == "OPEN" {
		if time.Since(breaker.LastFailTime) > breaker.ResetTimeout {
			breaker.State = "HALF_OPEN"
		} else {
			return wp.Pipeline.ErrorLane
		}
	}
	
	// No historical data - start with fast lane
	if stats.TotalExecutions == 0 {
		return wp.Pipeline.FastLane
	}
	
	// Categorize based on average execution time
	avgDuration := stats.AvgDuration
	if avgDuration < 5*time.Second {
		return wp.Pipeline.FastLane
	} else if avgDuration < 30*time.Second {
		return wp.Pipeline.MediumLane
	} else {
		return wp.Pipeline.SlowLane
	}
}

// updateStats updates procedure execution statistics
func (wp *WorkerPool) updateStats(procedure string, duration time.Duration, success bool) {
	wp.StatsMutex.Lock()
	defer wp.StatsMutex.Unlock()
	
	stats := wp.ProcStats[procedure]
	breaker := wp.CircuitBreakers[procedure]
	
	stats.TotalExecutions++
	stats.TotalDuration += duration
	stats.LastExecution = time.Now()
	stats.AvgDuration = time.Duration(int64(stats.TotalDuration) / stats.TotalExecutions)
	
	if !success {
		stats.FailureCount++
		breaker.FailureCount++
		breaker.LastFailTime = time.Now()
		
		if breaker.FailureCount >= breaker.FailureThreshold {
			breaker.State = "OPEN"
			log.Printf("🚨 Circuit breaker OPEN for procedure %s (failures: %d)", procedure, breaker.FailureCount)
		}
	} else {
		// Reset circuit breaker on success
		if breaker.State == "HALF_OPEN" {
			breaker.State = "CLOSED"
			breaker.FailureCount = 0
			log.Printf("✅ Circuit breaker CLOSED for procedure %s", procedure)
		}
	}
}

func processWork(ctx context.Context, db *sql.DB, preparedStmts map[string]*sql.Stmt, 
	runCfg ExtractionConfig, templates map[string][]ColumnConfig, 
	procedure, solID, mode string, procLogCh chan ProcLog, 
	summaryMu *sync.Mutex, procSummary map[string]ProcSummary, 
	completed *int64, totalItems int, overallStart time.Time, 
	workerPool *WorkerPool) {
	
	start := time.Now()
	var err error

	if mode == "E" {
		log.Printf("📥 Extracting %s for SOL %s", procedure, solID)
		stmt := preparedStmts[procedure]
		err = extractData(ctx, stmt, procedure, solID, &runCfg, templates[procedure])
	} else if mode == "I" {
		log.Printf("🔁 Inserting: %s.%s for SOL %s", runCfg.PackageName, procedure, solID)
		err = callProcedure(ctx, db, runCfg.PackageName, procedure, solID)
	}
	end := time.Now()

	plog := ProcLog{
		SolID:         solID,
		Procedure:     procedure,
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
	procLogCh <- plog
	
	// Update worker pool stats
	workerPool.updateStats(procedure, plog.ExecutionTime, err == nil)

	summaryMu.Lock()
	s, exists := procSummary[procedure]
	if !exists {
		s = ProcSummary{Procedure: procedure, StartTime: start, EndTime: end, Status: plog.Status}
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
	procSummary[procedure] = s
	summaryMu.Unlock()

	atomic.AddInt64(completed, 1)
	currentCompleted := atomic.LoadInt64(completed)
	if currentCompleted%100 == 0 || int(currentCompleted) == totalItems {
		elapsed := time.Since(overallStart)
		estimatedTotal := time.Duration(float64(elapsed) / float64(currentCompleted) * float64(totalItems))
		eta := estimatedTotal - elapsed
		log.Printf("✅ Progress: %d/%d (%.2f%%) | Elapsed: %s | ETA: %s",
			currentCompleted, totalItems, float64(currentCompleted)*100/float64(totalItems),
			elapsed.Round(time.Second), eta.Round(time.Second))
	}
}

func main() {
	appCfg, err := loadMainConfig(*appCfgFile)
	if err != nil {
		log.Fatalf("Failed to load main config: %v", err)
	}
	runCfg, err := loadExtractionConfig(*runCfgFile)
	if err != nil {
		log.Fatalf("Failed to load extraction config: %v", err)
	}

	templates := make(map[string][]ColumnConfig)
	if mode == "E" {
		for _, proc := range runCfg.Procedures {
			tmplPath := filepath.Join(runCfg.TemplatePath, fmt.Sprintf("%s.csv", proc))
			cols, err := readColumnsFromCSV(tmplPath)
			if err != nil {
				log.Fatalf("Failed to read template for %s: %v", proc, err)
			}
			templates[proc] = cols
		}
	}

	connString := fmt.Sprintf(`user="%s" password="%s" connectString="%s:%d/%s"`,
		appCfg.DBUser, appCfg.DBPassword, appCfg.DBHost, appCfg.DBPort, appCfg.DBSid)

	db, err := sql.Open("godror", connString)
	if err != nil {
		log.Fatalf("Failed to connect to DB: %v", err)
	}
	defer db.Close()

	db.SetMaxOpenConns(appCfg.Concurrency)
	db.SetMaxIdleConns(appCfg.Concurrency)
	db.SetConnMaxLifetime(30 * time.Minute)

	sols, err := readSols(appCfg.SolFilePath)
	if err != nil {
		log.Fatalf("Failed to read SOL IDs: %v", err)
	}

	ctx := context.Background()

	// --- Prepare statements once if in Extract mode ---
	preparedStmts := make(map[string]*sql.Stmt)
	if mode == "E" {
		log.Println("⚙️ Preparing database statements for extraction...")
		for _, proc := range runCfg.Procedures {
			cols := templates[proc]
			colNames := make([]string, len(cols))
			for i, col := range cols {
				colNames[i] = col.Name
			}
			query := fmt.Sprintf("SELECT %s FROM %s WHERE SOL_ID = :1", strings.Join(colNames, ", "), proc)

			stmt, err := db.PrepareContext(ctx, query)
			if err != nil {
				log.Fatalf("Failed to prepare statement for %s: %v", proc, err)
			}
			preparedStmts[proc] = stmt
		}
		// Defer closing of all prepared statements
		defer func() {
			for _, stmt := range preparedStmts {
				stmt.Close()
			}
		}()
	}

	procLogCh := make(chan ProcLog, 1000)
	var summaryMu sync.Mutex
	procSummary := make(map[string]ProcSummary)

	var LogFile, LogFileSummary string
	if mode == "I" {
		LogFile = runCfg.PackageName + "_insert.csv"
		LogFileSummary = runCfg.PackageName + "_insert_summary.csv"
	} else if mode == "E" {
		LogFile = runCfg.PackageName + "_extract.csv"
		LogFileSummary = runCfg.PackageName + "_extract_summary.csv"
	}

	go writeLog(filepath.Join(appCfg.LogFilePath, LogFile), procLogCh)

	totalItems := len(sols) * len(runCfg.Procedures)
	overallStart := time.Now()
	var completed int64

	// Create hybrid pipeline worker pool
	workerPool := createWorkerPool(appCfg.Concurrency, runCfg.Procedures)
	
	log.Printf("🏭 Starting hybrid pipeline: Fast(%d) Medium(%d) Slow(%d) Error(%d) workers", 
		workerPool.FastWorkers, workerPool.MediumWorkers, workerPool.SlowWorkers, workerPool.ErrorWorkers)

	// Populate work items and categorize them
	log.Printf("📦 Categorizing %d work items into pipeline lanes...", totalItems)
	for _, proc := range runCfg.Procedures {
		for _, sol := range sols {
			item := WorkItem{Procedure: proc, SolID: sol}
			lane := workerPool.categorizeWorkItem(item)
			lane <- item
		}
	}

	// Close all lanes
	close(workerPool.Pipeline.FastLane)
	close(workerPool.Pipeline.MediumLane)
	close(workerPool.Pipeline.SlowLane)
	close(workerPool.Pipeline.ErrorLane)

	var wg sync.WaitGroup

	// Start Fast Lane Workers
	for i := 0; i < workerPool.FastWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Process fast lane
			for item := range workerPool.Pipeline.FastLane {
				processWork(ctx, db, preparedStmts, runCfg, templates, item.Procedure, item.SolID,
					mode, procLogCh, &summaryMu, procSummary, &completed, totalItems, overallStart, workerPool)
			}
			// Help medium lane when fast is done
			for item := range workerPool.Pipeline.MediumLane {
				processWork(ctx, db, preparedStmts, runCfg, templates, item.Procedure, item.SolID,
					mode, procLogCh, &summaryMu, procSummary, &completed, totalItems, overallStart, workerPool)
			}
		}()
	}

	// Start Medium Lane Workers
	for i := 0; i < workerPool.MediumWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Process medium lane
			for item := range workerPool.Pipeline.MediumLane {
				processWork(ctx, db, preparedStmts, runCfg, templates, item.Procedure, item.SolID,
					mode, procLogCh, &summaryMu, procSummary, &completed, totalItems, overallStart, workerPool)
			}
			// Help slow lane when medium is done
			for item := range workerPool.Pipeline.SlowLane {
				processWork(ctx, db, preparedStmts, runCfg, templates, item.Procedure, item.SolID,
					mode, procLogCh, &summaryMu, procSummary, &completed, totalItems, overallStart, workerPool)
			}
		}()
	}

	// Start Slow Lane Workers
	for i := 0; i < workerPool.SlowWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Process slow lane
			for item := range workerPool.Pipeline.SlowLane {
				processWork(ctx, db, preparedStmts, runCfg, templates, item.Procedure, item.SolID,
					mode, procLogCh, &summaryMu, procSummary, &completed, totalItems, overallStart, workerPool)
			}
		}()
	}

	// Start Error Lane Workers
	for i := 0; i < workerPool.ErrorWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Process error lane with retry logic
			for item := range workerPool.Pipeline.ErrorLane {
				log.Printf("🔄 Retrying failed procedure %s for SOL %s", item.Procedure, item.SolID)
				processWork(ctx, db, preparedStmts, runCfg, templates, item.Procedure, item.SolID,
					mode, procLogCh, &summaryMu, procSummary, &completed, totalItems, overallStart, workerPool)
			}
		}()
	}

	wg.Wait()
	close(procLogCh)

	// Print final statistics
	log.Printf("📊 Final Procedure Statistics:")
	workerPool.StatsMutex.RLock()
	for proc, stats := range workerPool.ProcStats {
		breaker := workerPool.CircuitBreakers[proc]
		log.Printf("  %s: Executions=%d, AvgTime=%s, Failures=%d, CircuitState=%s", 
			proc, stats.TotalExecutions, stats.AvgDuration.Round(time.Millisecond), 
			stats.FailureCount, breaker.State)
	}
	workerPool.StatsMutex.RUnlock()

	writeSummary(filepath.Join(appCfg.LogFilePath, LogFileSummary), procSummary)
	if mode == "E" {
		mergeFiles(&runCfg)
	}
	log.Printf("🎯 All done! Processed %d SOLs across %d procedures in %s", len(sols), len(runCfg.Procedures), time.Since(overallStart).Round(time.Second))
}