package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"runtime"
	"sync"
	"time"
)

type ProcTask struct {
	SolID string
	Proc  string
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

func runProceduresWithProcLevelParallelism(ctx context.Context, db *sql.DB, sols []string, procConfig *ExtractionConfig, logCh chan<- ProcLog, mu *sync.Mutex, summary map[string]ProcSummary, concurrency int) {
	// Scale task channel buffer based on workload
	taskBufferSize := len(sols) * len(procConfig.Procedures)
	if taskBufferSize < 1000 {
		taskBufferSize = 1000
	}
	if taskBufferSize > 10000 {
		taskBufferSize = 10000 // Cap to prevent excessive memory usage
	}
	taskCh := make(chan ProcTask, taskBufferSize)
	var wg sync.WaitGroup
	totalTasks := len(sols) * len(procConfig.Procedures)
	overallStart := time.Now()
	var progressMu sync.Mutex
	completed := 0

	for range concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for task := range taskCh {
				start := time.Now()
							// Reduce verbose logging to improve performance
				if runtime.GOMAXPROCS(0) <= 4 {
					log.Printf("🔁 Inserting: %s.%s for SOL %s", procConfig.PackageName, task.Proc, task.SolID)
				}
				err := callProcedure(ctx, db, procConfig.PackageName, task.Proc, task.SolID)
				end := time.Now()

				plog := ProcLog{
					SolID:         task.SolID,
					Procedure:     task.Proc,
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

				// Batch summary updates to reduce mutex contention
				mu.Lock()
				s, exists := summary[task.Proc]
				if !exists {
					s = ProcSummary{Procedure: task.Proc, StartTime: start, EndTime: end, Status: plog.Status}
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
				summary[task.Proc] = s
				mu.Unlock()

				// Optimize progress reporting to reduce mutex contention
				progressMu.Lock()
				completed++
				localCompleted := completed
				progressMu.Unlock()
				
				// Only log progress at intervals to reduce lock contention
				if localCompleted%100 == 0 || localCompleted == totalTasks {
					elapsed := time.Since(overallStart)
					estimatedTotal := time.Duration(float64(elapsed) / float64(localCompleted) * float64(totalTasks))
					eta := estimatedTotal - elapsed
					log.Printf("✅ Progress: %d/%d (%.2f%%) | Elapsed: %s | ETA: %s",
						localCompleted, totalTasks, float64(localCompleted)*100/float64(totalTasks),
						elapsed.Round(time.Second), eta.Round(time.Second))
				}
			}
		}()
	}

	for _, sol := range sols {
		for _, proc := range procConfig.Procedures {
			taskCh <- ProcTask{SolID: sol, Proc: proc}
		}
	}
	close(taskCh)
	wg.Wait()
}

// Prepared statement cache for procedure calls
var procStmtCache = NewPreparedStmtCache()

func callProcedure(ctx context.Context, db *sql.DB, pkgName, procName, solID string) error {
	query := fmt.Sprintf("BEGIN %s.%s(:1); END;", pkgName, procName)
	start := time.Now()
	
	// Use prepared statement cache for procedure calls too
	stmt, err := procStmtCache.GetOrPrepare(db, query)
	if err != nil {
		return fmt.Errorf("failed to prepare procedure statement: %w", err)
	}
	
	_, err = stmt.ExecContext(ctx, solID)
	
	// Reduce verbose logging for performance - only log slow procedures
	duration := time.Since(start)
	if duration > 5*time.Second {
		log.Printf("⚠️ Slow procedure: %s.%s for SOL %s took %s", pkgName, procName, solID, duration.Round(time.Millisecond))
	} else if duration > 1*time.Second {
		log.Printf("✅ Finished: %s.%s for SOL %s in %s", pkgName, procName, solID, duration.Round(time.Millisecond))
	}
	return err
}
