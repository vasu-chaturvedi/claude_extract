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

// ProcWorkerPool manages workers for a specific procedure
type ProcWorkerPool struct {
	Procedure string
	WorkQueue chan string // SOL IDs only
	Workers   int
	Done      chan bool
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

func processWork(ctx context.Context, db *sql.DB, preparedStmts map[string]*sql.Stmt, 
	runCfg ExtractionConfig, templates map[string][]ColumnConfig, 
	procedure, solID, mode string, procLogCh chan ProcLog, 
	summaryMu *sync.Mutex, procSummary map[string]ProcSummary, 
	completed *int64, totalItems int, overallStart time.Time) {
	
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

	// Create procedure-specific worker pools
	procPools := make([]*ProcWorkerPool, len(runCfg.Procedures))
	workersPerProc := appCfg.Concurrency / len(runCfg.Procedures)
	if workersPerProc == 0 {
		workersPerProc = 1
	}
	remainingWorkers := appCfg.Concurrency - (workersPerProc * len(runCfg.Procedures))

	log.Printf("📦 Creating %d procedure-specific worker pools...", len(runCfg.Procedures))
	for i, proc := range runCfg.Procedures {
		workers := workersPerProc
		if i < remainingWorkers {
			workers++
		}
		
		pool := &ProcWorkerPool{
			Procedure: proc,
			WorkQueue: make(chan string, len(sols)),
			Workers:   workers,
			Done:      make(chan bool),
		}
		
		log.Printf("📥 Populating work queue for %s with %d SOLs, %d workers", proc, len(sols), workers)
		for _, sol := range sols {
			pool.WorkQueue <- sol
		}
		close(pool.WorkQueue)
		
		procPools[i] = pool
	}

	var wg sync.WaitGroup
	
	// Start workers for each procedure pool
	for _, pool := range procPools {
		for i := 0; i < pool.Workers; i++ {
			wg.Add(1)
			go func(p *ProcWorkerPool) {
				defer wg.Done()
				
				// Process own procedure's work
				for solID := range p.WorkQueue {
					processWork(ctx, db, preparedStmts, runCfg, templates, p.Procedure, solID, 
						mode, procLogCh, &summaryMu, procSummary, &completed, totalItems, overallStart)
				}
				
				// Signal this procedure is done
				select {
				case p.Done <- true:
				default:
				}
				
				// Work stealing: help other procedures
				for _, otherPool := range procPools {
					if otherPool == p {
						continue
					}
					
					// Try to steal work from other procedures
					for {
						select {
						case solID := <-otherPool.WorkQueue:
							processWork(ctx, db, preparedStmts, runCfg, templates, otherPool.Procedure, solID,
								mode, procLogCh, &summaryMu, procSummary, &completed, totalItems, overallStart)
						default:
							goto nextPool
						}
					}
					nextPool:
				}
			}(pool)
		}
	}

	wg.Wait()
	close(procLogCh)

	writeSummary(filepath.Join(appCfg.LogFilePath, LogFileSummary), procSummary)
	if mode == "E" {
		mergeFiles(&runCfg)
	}
	log.Printf("🎯 All done! Processed %d SOLs across %d procedures in %s", len(sols), len(runCfg.Procedures), time.Since(overallStart).Round(time.Second))
}