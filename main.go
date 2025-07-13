package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
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

func processWork(ctx context.Context, workerCtx *WorkerContext, db *sql.DB,
	runCfg ExtractionConfig, templates map[string][]ColumnConfig,
	procedure, solID, mode string, batchLogger *BatchLogger,
	summaryMu *sync.Mutex, procSummary map[string]ProcSummary,
	completed *int64, totalItems int, overallStart time.Time,
	workerPool *WorkerPool) {

	start := time.Now()
	var err error

	if mode == "E" {
		log.Printf("📥 [W%d] Extracting %s for SOL %s", workerCtx.WorkerID, procedure, solID)
		stmt := workerCtx.PreparedStmts[procedure]
		err = extractData(ctx, stmt, procedure, solID, &runCfg, templates[procedure])
	} else if mode == "I" {
		log.Printf("🔁 [W%d] Inserting: %s.%s for SOL %s", workerCtx.WorkerID, runCfg.PackageName, procedure, solID)
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

	// Send to batch logger
	batchLogger.LogCh <- plog

	// Update worker pool stats
	workerPool.updateStats(procedure, plog.ExecutionTime, err == nil)

	currentCompleted := atomic.AddInt64(completed, 1)

	// Update summary less frequently to reduce lock contention
	if currentCompleted%100 == 0 || plog.Status == "FAIL" || int(currentCompleted) == totalItems {
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
	}

	// Reduce progress logging frequency to reduce contention
	if currentCompleted%500 == 0 || int(currentCompleted) == totalItems {
		elapsed := time.Since(overallStart)
		estimatedTotal := time.Duration(float64(elapsed) / float64(currentCompleted) * float64(totalItems))
		eta := estimatedTotal - elapsed
		log.Printf("✅ Progress: %d/%d (%.2f%%) | Elapsed: %s | ETA: %s",
			currentCompleted, totalItems, float64(currentCompleted)*100/float64(totalItems),
			elapsed.Round(time.Second), eta.Round(time.Second))
	}
}

func main() {
	// Load configurations
	appCfg, err := loadMainConfig(*appCfgFile)
	if err != nil {
		log.Fatalf("Failed to load main config: %v", err)
	}
	runCfg, err := loadExtractionConfig(*runCfgFile)
	if err != nil {
		log.Fatalf("Failed to load extraction config: %v", err)
	}

	// Load templates for extraction mode
	var templates map[string][]ColumnConfig
	if mode == "E" {
		templates, err = loadTemplates(runCfg.Procedures, runCfg.TemplatePath)
		if err != nil {
			log.Fatalf("Failed to load templates: %v", err)
		}
	}

	// Create database connection string
	connString := fmt.Sprintf(`user="%s" password="%s" connectString="%s:%d/%s"`,
		appCfg.DBUser, appCfg.DBPassword, appCfg.DBHost, appCfg.DBPort, appCfg.DBSid)

	// Create optimized database pools per lane
	dbPools, err := createDatabasePools(connString, appCfg.Concurrency)
	if err != nil {
		log.Fatalf("Failed to create database pools: %v", err)
	}
	defer dbPools.Close()

	log.Printf("🏊 Database pools created: Fast(%d) Medium(%d) Slow(%d) Error(%d) connections",
		int(float64(appCfg.Concurrency)*0.5), int(float64(appCfg.Concurrency)*0.3),
		int(float64(appCfg.Concurrency)*0.15), appCfg.Concurrency-int(float64(appCfg.Concurrency)*0.95))

	// Warm up database connections
	log.Printf("🔥 Warming up database connections...")
	ctx := context.Background()
	if err := dbPools.warmupConnections(ctx); err != nil {
		log.Fatalf("Failed to warm up connections: %v", err)
	}

	// Load SOL IDs
	sols, err := readSols(appCfg.SolFilePath)
	if err != nil {
		log.Fatalf("Failed to read SOL IDs: %v", err)
	}

	// Determine log file paths
	var LogFile, LogFileSummary string
	if mode == "I" {
		LogFile = runCfg.PackageName + "_insert.csv"
		LogFileSummary = runCfg.PackageName + "_insert_summary.csv"
	} else if mode == "E" {
		LogFile = runCfg.PackageName + "_extract.csv"
		LogFileSummary = runCfg.PackageName + "_extract_summary.csv"
	}

	// Create optimized batch logger
	batchLogger := createBatchLogger(filepath.Join(appCfg.LogFilePath, LogFile))
	defer batchLogger.Close()

	// Initialize tracking variables
	var summaryMu sync.Mutex
	procSummary := make(map[string]ProcSummary)
	totalItems := len(sols) * len(runCfg.Procedures)
	overallStart := time.Now()
	var completed int64

	// Create hybrid pipeline worker pool
	workerPool := createWorkerPool(appCfg.Concurrency, runCfg.Procedures, dbPools)

	log.Printf("🏭 Starting optimized hybrid pipeline: Fast(%d) Medium(%d) Slow(%d) Error(%d) workers",
		workerPool.FastWorkers, workerPool.MediumWorkers, workerPool.SlowWorkers, workerPool.ErrorWorkers)

	// Initialize worker contexts with per-worker prepared statements
	log.Printf("⚙️ Initializing %d worker contexts with prepared statements...",
		workerPool.FastWorkers+workerPool.MediumWorkers+workerPool.SlowWorkers+workerPool.ErrorWorkers)

	// Populate work items and categorize them
	log.Printf("📦 Categorizing %d work items into pipeline lanes...", totalItems)
	workerPool.populateWorkItems(runCfg.Procedures, sols)

	// Create worker manager and start all workers
	workerManager := createWorkerManager(workerPool, dbPools, batchLogger, templates, runCfg, mode, ctx)
	workerManager.startAllWorkers(&summaryMu, procSummary, &completed, totalItems, overallStart)

	// Print final statistics
	workerPool.printFinalStats()

	// Write summary and merge files if needed
	if err := writeSummary(filepath.Join(appCfg.LogFilePath, LogFileSummary), procSummary); err != nil {
		log.Printf("Failed to write summary: %v", err)
	}
	
	if mode == "E" {
		mergeFiles(&runCfg)
	}
	
	log.Printf("🎯 All done! Processed %d SOLs across %d procedures in %s", 
		len(sols), len(runCfg.Procedures), time.Since(overallStart).Round(time.Second))
}