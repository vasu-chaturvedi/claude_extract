package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"
)

// WorkerContext contains worker-specific resources
type WorkerContext struct {
	WorkerID      int
	PreparedStmts map[string]*sql.Stmt
	LogBuffer     []ProcLog
	BufferMutex   sync.Mutex
	LastFlush     time.Time
	StringPool    *sync.Pool // Reusable string slices for CSV records
}

// WorkerPool manages the hybrid pipeline architecture
type WorkerPool struct {
	Pipeline       *Pipeline
	StatsManager   *StatsManager
	FastWorkers    int
	MediumWorkers  int
	SlowWorkers    int
	ErrorWorkers   int
	DBPools        *DatabasePools
	WorkerContexts []*WorkerContext
}

// createWorkerPool initializes the hybrid pipeline architecture
func createWorkerPool(concurrency int, procedures []string, dbPools *DatabasePools) *WorkerPool {
	pipeline := createPipeline()
	statsManager := createStatsManager(procedures)

	// Distribute workers: 50% fast, 30% medium, 15% slow, 5% error
	fastWorkers := int(float64(concurrency) * 0.5)
	mediumWorkers := int(float64(concurrency) * 0.3)
	slowWorkers := int(float64(concurrency) * 0.15)
	errorWorkers := concurrency - fastWorkers - mediumWorkers - slowWorkers

	if fastWorkers == 0 {
		fastWorkers = 1
	}
	if mediumWorkers == 0 {
		mediumWorkers = 1
	}
	if slowWorkers == 0 {
		slowWorkers = 1
	}
	if errorWorkers == 0 {
		errorWorkers = 1
	}

	// Create worker contexts with individual prepared statements
	totalWorkers := fastWorkers + mediumWorkers + slowWorkers + errorWorkers
	workerContexts := make([]*WorkerContext, totalWorkers)

	wp := &WorkerPool{
		Pipeline:       pipeline,
		StatsManager:   statsManager,
		FastWorkers:    fastWorkers,
		MediumWorkers:  mediumWorkers,
		SlowWorkers:    slowWorkers,
		ErrorWorkers:   errorWorkers,
		DBPools:        dbPools,
		WorkerContexts: workerContexts,
	}

	return wp
}

// createWorkerContext initializes a worker with its own prepared statements
func createWorkerContext(workerID int, procedures []string, db *sql.DB, templates map[string][]ColumnConfig, mode string) (*WorkerContext, error) {
	// Create string pool for memory-efficient CSV record allocation
	stringPool := &sync.Pool{
		New: func() interface{} {
			return make([]string, 7) // Pre-allocate for CSV record fields
		},
	}

	ctx := &WorkerContext{
		WorkerID:      workerID,
		PreparedStmts: make(map[string]*sql.Stmt),
		LogBuffer:     make([]ProcLog, 0, 50),
		LastFlush:     time.Now(),
		StringPool:    stringPool,
	}

	// Create prepared statements for this worker (only in Extract mode)
	if mode == "E" {
		for _, proc := range procedures {
			cols := templates[proc]
			colNames := make([]string, len(cols))
			for i, col := range cols {
				colNames[i] = col.Name
			}
			query := fmt.Sprintf("SELECT %s FROM %s WHERE SOL_ID = :1", strings.Join(colNames, ", "), proc)

			stmt, err := db.PrepareContext(context.Background(), query)
			if err != nil {
				// Close any already prepared statements
				for _, s := range ctx.PreparedStmts {
					s.Close()
				}
				return nil, fmt.Errorf("failed to prepare statement for %s: %v", proc, err)
			}
			ctx.PreparedStmts[proc] = stmt
		}
	}

	return ctx, nil
}

// populateWorkItems distributes work items across pipeline lanes
func (wp *WorkerPool) populateWorkItems(procedures []string, sols []string) {
	// Group SOLs by procedure for better cache locality
	procSolGroups := make(map[string][]string)
	for _, proc := range procedures {
		procSolGroups[proc] = make([]string, 0, len(sols))
		for _, sol := range sols {
			procSolGroups[proc] = append(procSolGroups[proc], sol)
		}
	}

	// Distribute work items by procedure groups
	for _, proc := range procedures {
		for _, sol := range procSolGroups[proc] {
			item := WorkItem{Procedure: proc, SolID: sol}
			lane := wp.Pipeline.categorizeWorkItem(item, wp.StatsManager)
			lane <- item
		}
	}

	// Close all lanes
	wp.Pipeline.closeAllLanes()
}

// getLaneDB returns the appropriate database for a given lane
func (wp *WorkerPool) getLaneDB(lane chan WorkItem) *sql.DB {
	return wp.Pipeline.getLaneSpecificDB(lane, wp.DBPools)
}

// updateStats updates procedure execution statistics
func (wp *WorkerPool) updateStats(procedure string, duration time.Duration, success bool) {
	wp.StatsManager.updateStats(procedure, duration, success)
}

// printFinalStats displays final procedure statistics
func (wp *WorkerPool) printFinalStats() {
	wp.StatsManager.printFinalStats()
}