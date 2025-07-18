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
	"time"

	_ "github.com/godror/godror"
)

var (
	appCfgFile = new(string)
	runCfgFile = new(string)
	mode       string
)

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

func main() {
	appCfg, err := loadMainConfig(*appCfgFile)
	if err != nil {
		log.Fatalf("Failed to load main config: %v", err)
	}
	runCfg, err := loadExtractionConfig(*runCfgFile)
	if err != nil {
		log.Fatalf("Failed to load extraction config: %v", err)
	}
	
	// Set chunked processing defaults
	setChunkedDefaults(&runCfg)
	if len(runCfg.ChunkedProcedures) > 0 {
		log.Printf("🧩 Chunked procedures configured: %v (chunk size: %d)", runCfg.ChunkedProcedures, runCfg.ChunkSize)
	}

	// Load templates
	templates := make(map[string][]ColumnConfig)
	for _, proc := range runCfg.Procedures {
		tmplPath := filepath.Join(runCfg.TemplatePath, fmt.Sprintf("%s.csv", proc))
		cols, err := readColumnsFromCSV(tmplPath)
		if err != nil {
			log.Fatalf("Failed to read template for %s: %v", proc, err)
		}
		templates[proc] = cols
	}

	connString := fmt.Sprintf(`user="%s" password="%s" connectString="%s:%d/%s"`,
		appCfg.DBUser, appCfg.DBPassword, appCfg.DBHost, appCfg.DBPort, appCfg.DBSid)

	db, err := sql.Open("godror", connString)
	if err != nil {
		log.Fatalf("Failed to connect to DB: %v", err)
	}
	defer db.Close()

	procCount := len(runCfg.Procedures)
	// Optimize connection pool for high concurrency
	maxConns := appCfg.Concurrency * procCount
	if maxConns > 200 {
		log.Printf("⚠️ Warning: Connection pool size (%d) may exceed database limits", maxConns)
	}
	db.SetMaxOpenConns(maxConns)
	db.SetMaxIdleConns(maxConns / 2) // Reduce idle connections
	db.SetConnMaxLifetime(15 * time.Minute) // Shorter lifetime for high throughput
	db.SetConnMaxIdleTime(5 * time.Minute)  // Close idle connections faster

	sols, err := readSols(appCfg.SolFilePath)
	if err != nil {
		log.Fatalf("Failed to read SOL IDs: %v", err)
	}

	// Scale buffer size based on expected load
	bufferSize := max(1000, min(50000, len(sols)*len(runCfg.Procedures)))
	procLogCh := make(chan ProcLog, bufferSize)
	var summaryMu sync.Mutex
	procSummary := make(map[string]ProcSummary)

	if (mode == "I" && !runCfg.RunInsertionParallel) || (mode == "E" && !runCfg.RunExtractionParallel) {
		log.Println("Running procedures sequentially as parallel execution is disabled")
		appCfg.Concurrency = 1
	}

	var LogFile, LogFileSummary string
	if mode == "I" {
		LogFile = runCfg.PackageName + "_insert.csv"
		LogFileSummary = runCfg.PackageName + "_insert_summary.csv"
	} else if mode == "E" {
		LogFile = runCfg.PackageName + "_extract.csv"
		LogFileSummary = runCfg.PackageName + "_extract_summary.csv"
	}

	go writeLog(filepath.Join(appCfg.LogFilePath, LogFile), procLogCh)

	sem := make(chan struct{}, appCfg.Concurrency)
	var wg sync.WaitGroup
	ctx := context.Background()
	totalSols := len(sols)
	overallStart := time.Now()
	var mu sync.Mutex
	completed := 0

	if mode == "E" {
		for _, sol := range sols {
			wg.Add(1)
			sem <- struct{}{}
			go func(solID string) {
				defer wg.Done()
				defer func() { <-sem }()
				log.Printf("➡️ Starting SOL %s", solID)

				runExtractionForSol(ctx, db, solID, &runCfg, templates, procLogCh, &summaryMu, procSummary)

				mu.Lock()
				completed++
				if completed%100 == 0 || completed == totalSols {
					elapsed := time.Since(overallStart)
					estimatedTotal := time.Duration(float64(elapsed) / float64(completed) * float64(totalSols))
					eta := estimatedTotal - elapsed
					log.Printf("✅ Progress: %d/%d (%.2f%%) | Elapsed: %s | ETA: %s",
						completed, totalSols, float64(completed)*100/float64(totalSols),
						elapsed.Round(time.Second), eta.Round(time.Second))
				}
				mu.Unlock()
			}(sol)
		}
		wg.Wait()
	} else if mode == "I" {
		if runCfg.UseProcLevelParallel {
			log.Printf("🚀 Starting procedure-level parallel execution for %d SOLs with %d procedures", totalSols, len(runCfg.Procedures))
			totalTasks := totalSols * len(runCfg.Procedures)
			log.Printf("📊 Total tasks to execute: %d (SOL-Procedure combinations)", totalTasks)
			log.Printf("🔌 Connection pool: %d max connections, %d idle connections", maxConns, maxConns/2)
			
			// Monitor connection pool stats
			go func() {
				ticker := time.NewTicker(30 * time.Second)
				defer ticker.Stop()
				for {
					select {
					case <-ticker.C:
						stats := db.Stats()
						log.Printf("📊 DB Stats: Open=%d, InUse=%d, Idle=%d, WaitCount=%d, WaitDuration=%s",
							stats.OpenConnections, stats.InUse, stats.Idle, stats.WaitCount, stats.WaitDuration)
					case <-ctx.Done():
						return
					}
				}
			}()
			
			runProceduresWithProcLevelParallelism(ctx, db, sols, &runCfg, procLogCh, &summaryMu, procSummary, appCfg.Concurrency)
			log.Printf("✅ Completed all %d tasks", totalTasks)
		} else {
			log.Printf("🚀 Starting SOL-level parallel execution (legacy mode) for %d SOLs", totalSols)
			for _, sol := range sols {
				wg.Add(1)
				sem <- struct{}{}
				go func(solID string) {
					defer wg.Done()
					defer func() { <-sem }()
					log.Printf("➡️ Starting SOL %s", solID)

					runProceduresForSol(ctx, db, solID, &runCfg, procLogCh, &summaryMu, procSummary)

					mu.Lock()
					completed++
					if completed%100 == 0 || completed == totalSols {
						elapsed := time.Since(overallStart)
						estimatedTotal := time.Duration(float64(elapsed) / float64(completed) * float64(totalSols))
						eta := estimatedTotal - elapsed
						log.Printf("✅ Progress: %d/%d (%.2f%%) | Elapsed: %s | ETA: %s",
							completed, totalSols, float64(completed)*100/float64(totalSols),
							elapsed.Round(time.Second), eta.Round(time.Second))
					}
					mu.Unlock()
				}(sol)
			}
			wg.Wait()
		}
	}
	close(procLogCh)

	writeSummary(filepath.Join(appCfg.LogFilePath, LogFileSummary), procSummary)
	if mode == "E" {
		mergeFiles(&runCfg)
	}
	// Final performance summary
	finalStats := db.Stats()
	log.Printf("📊 Final DB Stats: MaxOpen=%d, Open=%d, InUse=%d, Idle=%d, WaitCount=%d, WaitDuration=%s",
		finalStats.MaxOpenConnections, finalStats.OpenConnections, finalStats.InUse, finalStats.Idle, finalStats.WaitCount, finalStats.WaitDuration)
	log.Printf("🎯 All done! Processed %d SOLs in %s", totalSols, time.Since(overallStart).Round(time.Second))
}
