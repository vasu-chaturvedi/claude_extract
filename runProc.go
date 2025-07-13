package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"sync"
	"time"
)

type WorkItem struct {
	SolID     string
	Procedure string
}

func runProceduresForSol(ctx context.Context, db *sql.DB, solID string, procConfig *ExtractionConfig, logCh chan<- ProcLog, mu *sync.Mutex, summary map[string]ProcSummary) {
	for _, proc := range procConfig.Procedures {
		start := time.Now()
		log.Printf("🔁 Inserting: %s.%s for SOL %s", procConfig.PackageName, proc, solID)
		err := callProcedure(ctx, db, procConfig.PackageName, proc, solID)
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
	}
}

func runProceduresGlobal(ctx context.Context, db *sql.DB, sols []string, procConfig *ExtractionConfig, logCh chan<- ProcLog, mu *sync.Mutex, summary map[string]ProcSummary, concurrency int) {
	workCh := make(chan WorkItem, 1000)
	var wg sync.WaitGroup
	
	totalWork := len(sols) * len(procConfig.Procedures)
	completed := 0
	var progressMu sync.Mutex
	overallStart := time.Now()

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for work := range workCh {
				start := time.Now()
				log.Printf("🔁 Worker %d: %s.%s for SOL %s", workerID, procConfig.PackageName, work.Procedure, work.SolID)
				err := callProcedure(ctx, db, procConfig.PackageName, work.Procedure, work.SolID)
				end := time.Now()

				plog := ProcLog{
					SolID:         work.SolID,
					Procedure:     work.Procedure,
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
				s, exists := summary[work.Procedure]
				if !exists {
					s = ProcSummary{Procedure: work.Procedure, StartTime: start, EndTime: end, Status: plog.Status}
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
				summary[work.Procedure] = s
				mu.Unlock()

				progressMu.Lock()
				completed++
				if completed%500 == 0 || completed == totalWork {
					elapsed := time.Since(overallStart)
					estimatedTotal := time.Duration(float64(elapsed) / float64(completed) * float64(totalWork))
					eta := estimatedTotal - elapsed
					log.Printf("📊 Progress: %d/%d (%.2f%%) | Elapsed: %s | ETA: %s",
						completed, totalWork, float64(completed)*100/float64(totalWork),
						elapsed.Round(time.Second), eta.Round(time.Second))
				}
				progressMu.Unlock()
			}
		}(i)
	}

	for _, sol := range sols {
		for _, proc := range procConfig.Procedures {
			workCh <- WorkItem{SolID: sol, Procedure: proc}
		}
	}
	close(workCh)
	wg.Wait()
	
	log.Printf("🎯 Insert mode completed! Processed %d operations in %s", totalWork, time.Since(overallStart).Round(time.Second))
}

func callProcedure(ctx context.Context, db *sql.DB, pkgName, procName, solID string) error {
	query := fmt.Sprintf("BEGIN %s.%s(:1); END;", pkgName, procName)
	start := time.Now()
	_, err := db.ExecContext(ctx, query, solID)
	log.Printf("✅ Finished: %s.%s for SOL %s in %s", pkgName, procName, solID, time.Since(start).Round(time.Millisecond))
	return err
}
