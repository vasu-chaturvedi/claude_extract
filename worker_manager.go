package main

import (
	"context"
	"log"
	"sync"
	"time"
)

// WorkerManager handles worker lifecycle and coordination
type WorkerManager struct {
	WorkerPool   *WorkerPool
	DBPools      *DatabasePools
	BatchLogger  *BatchLogger
	Templates    map[string][]ColumnConfig
	RunConfig    ExtractionConfig
	Mode         string
	Context      context.Context
}

// createWorkerManager initializes the worker management system
func createWorkerManager(workerPool *WorkerPool, dbPools *DatabasePools, batchLogger *BatchLogger,
	templates map[string][]ColumnConfig, runConfig ExtractionConfig, mode string, ctx context.Context) *WorkerManager {
	
	return &WorkerManager{
		WorkerPool:  workerPool,
		DBPools:     dbPools,
		BatchLogger: batchLogger,
		Templates:   templates,
		RunConfig:   runConfig,
		Mode:        mode,
		Context:     ctx,
	}
}

// startAllWorkers initializes and starts all worker lanes
func (wm *WorkerManager) startAllWorkers(summaryMu *sync.Mutex, procSummary map[string]ProcSummary,
	completed *int64, totalItems int, overallStart time.Time) {

	var wg sync.WaitGroup
	workerID := 0

	// Start Fast Lane Workers
	wm.startFastLaneWorkers(&wg, &workerID, summaryMu, procSummary, completed, totalItems, overallStart)

	// Start Medium Lane Workers  
	wm.startMediumLaneWorkers(&wg, &workerID, summaryMu, procSummary, completed, totalItems, overallStart)

	// Start Slow Lane Workers
	wm.startSlowLaneWorkers(&wg, &workerID, summaryMu, procSummary, completed, totalItems, overallStart)

	// Start Error Lane Workers
	wm.startErrorLaneWorkers(&wg, &workerID, summaryMu, procSummary, completed, totalItems, overallStart)

	wg.Wait()
}

// startFastLaneWorkers initializes and starts fast lane workers
func (wm *WorkerManager) startFastLaneWorkers(wg *sync.WaitGroup, workerID *int,
	summaryMu *sync.Mutex, procSummary map[string]ProcSummary,
	completed *int64, totalItems int, overallStart time.Time) {

	for i := 0; i < wm.WorkerPool.FastWorkers; i++ {
		workerCtx, err := createWorkerContext(*workerID, wm.RunConfig.Procedures, wm.DBPools.FastDB, wm.Templates, wm.Mode)
		if err != nil {
			log.Fatalf("Failed to create fast worker context %d: %v", *workerID, err)
		}
		wm.WorkerPool.WorkerContexts[*workerID] = workerCtx

		wg.Add(1)
		go func(wCtx *WorkerContext) {
			defer wg.Done()
			defer wm.cleanupWorkerContext(wCtx)

			// Process fast lane
			for item := range wm.WorkerPool.Pipeline.FastLane {
				processWork(wm.Context, wCtx, wm.DBPools.FastDB, wm.RunConfig, wm.Templates, item.Procedure, item.SolID,
					wm.Mode, wm.BatchLogger, summaryMu, procSummary, completed, totalItems, overallStart, wm.WorkerPool)
			}
			// Help medium lane when fast is done
			for item := range wm.WorkerPool.Pipeline.MediumLane {
				processWork(wm.Context, wCtx, wm.DBPools.MediumDB, wm.RunConfig, wm.Templates, item.Procedure, item.SolID,
					wm.Mode, wm.BatchLogger, summaryMu, procSummary, completed, totalItems, overallStart, wm.WorkerPool)
			}
		}(workerCtx)
		(*workerID)++
	}
}

// startMediumLaneWorkers initializes and starts medium lane workers
func (wm *WorkerManager) startMediumLaneWorkers(wg *sync.WaitGroup, workerID *int,
	summaryMu *sync.Mutex, procSummary map[string]ProcSummary,
	completed *int64, totalItems int, overallStart time.Time) {

	for i := 0; i < wm.WorkerPool.MediumWorkers; i++ {
		workerCtx, err := createWorkerContext(*workerID, wm.RunConfig.Procedures, wm.DBPools.MediumDB, wm.Templates, wm.Mode)
		if err != nil {
			log.Fatalf("Failed to create medium worker context %d: %v", *workerID, err)
		}
		wm.WorkerPool.WorkerContexts[*workerID] = workerCtx

		wg.Add(1)
		go func(wCtx *WorkerContext) {
			defer wg.Done()
			defer wm.cleanupWorkerContext(wCtx)

			// Process medium lane
			for item := range wm.WorkerPool.Pipeline.MediumLane {
				processWork(wm.Context, wCtx, wm.DBPools.MediumDB, wm.RunConfig, wm.Templates, item.Procedure, item.SolID,
					wm.Mode, wm.BatchLogger, summaryMu, procSummary, completed, totalItems, overallStart, wm.WorkerPool)
			}
			// Help slow lane when medium is done
			for item := range wm.WorkerPool.Pipeline.SlowLane {
				processWork(wm.Context, wCtx, wm.DBPools.SlowDB, wm.RunConfig, wm.Templates, item.Procedure, item.SolID,
					wm.Mode, wm.BatchLogger, summaryMu, procSummary, completed, totalItems, overallStart, wm.WorkerPool)
			}
		}(workerCtx)
		(*workerID)++
	}
}

// startSlowLaneWorkers initializes and starts slow lane workers
func (wm *WorkerManager) startSlowLaneWorkers(wg *sync.WaitGroup, workerID *int,
	summaryMu *sync.Mutex, procSummary map[string]ProcSummary,
	completed *int64, totalItems int, overallStart time.Time) {

	for i := 0; i < wm.WorkerPool.SlowWorkers; i++ {
		workerCtx, err := createWorkerContext(*workerID, wm.RunConfig.Procedures, wm.DBPools.SlowDB, wm.Templates, wm.Mode)
		if err != nil {
			log.Fatalf("Failed to create slow worker context %d: %v", *workerID, err)
		}
		wm.WorkerPool.WorkerContexts[*workerID] = workerCtx

		wg.Add(1)
		go func(wCtx *WorkerContext) {
			defer wg.Done()
			defer wm.cleanupWorkerContext(wCtx)

			// Process slow lane
			for item := range wm.WorkerPool.Pipeline.SlowLane {
				processWork(wm.Context, wCtx, wm.DBPools.SlowDB, wm.RunConfig, wm.Templates, item.Procedure, item.SolID,
					wm.Mode, wm.BatchLogger, summaryMu, procSummary, completed, totalItems, overallStart, wm.WorkerPool)
			}
		}(workerCtx)
		(*workerID)++
	}
}

// startErrorLaneWorkers initializes and starts error lane workers
func (wm *WorkerManager) startErrorLaneWorkers(wg *sync.WaitGroup, workerID *int,
	summaryMu *sync.Mutex, procSummary map[string]ProcSummary,
	completed *int64, totalItems int, overallStart time.Time) {

	for i := 0; i < wm.WorkerPool.ErrorWorkers; i++ {
		workerCtx, err := createWorkerContext(*workerID, wm.RunConfig.Procedures, wm.DBPools.ErrorDB, wm.Templates, wm.Mode)
		if err != nil {
			log.Fatalf("Failed to create error worker context %d: %v", *workerID, err)
		}
		wm.WorkerPool.WorkerContexts[*workerID] = workerCtx

		wg.Add(1)
		go func(wCtx *WorkerContext) {
			defer wg.Done()
			defer wm.cleanupWorkerContext(wCtx)

			// Process error lane with retry logic
			for item := range wm.WorkerPool.Pipeline.ErrorLane {
				log.Printf("🔄 [W%d] Retrying failed procedure %s for SOL %s", wCtx.WorkerID, item.Procedure, item.SolID)
				processWork(wm.Context, wCtx, wm.DBPools.ErrorDB, wm.RunConfig, wm.Templates, item.Procedure, item.SolID,
					wm.Mode, wm.BatchLogger, summaryMu, procSummary, completed, totalItems, overallStart, wm.WorkerPool)
			}
		}(workerCtx)
		(*workerID)++
	}
}

// cleanupWorkerContext closes prepared statements for a worker
func (wm *WorkerManager) cleanupWorkerContext(wCtx *WorkerContext) {
	for _, stmt := range wCtx.PreparedStmts {
		stmt.Close()
	}
}