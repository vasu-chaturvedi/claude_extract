package main

import (
	"database/sql"
	"time"
)

// Pipeline represents different execution lanes
type Pipeline struct {
	FastLane   chan WorkItem // < 5s procedures
	MediumLane chan WorkItem // 5-30s procedures  
	SlowLane   chan WorkItem // > 30s procedures
	ErrorLane  chan WorkItem // Failed/circuit-broken procedures
}

// createPipeline initializes the pipeline with buffered channels
func createPipeline() *Pipeline {
	return &Pipeline{
		FastLane:   make(chan WorkItem, 20000),
		MediumLane: make(chan WorkItem, 20000),
		SlowLane:   make(chan WorkItem, 20000),
		ErrorLane:  make(chan WorkItem, 5000),
	}
}

// categorizeWorkItem determines which lane a work item should go to
func (p *Pipeline) categorizeWorkItem(item WorkItem, statsManager *StatsManager) chan WorkItem {
	circuitState := statsManager.getCircuitBreakerState(item.Procedure)
	
	// Check circuit breaker state
	if circuitState == "OPEN" {
		return p.ErrorLane
	}
	
	// No historical data - start with fast lane
	totalExecutions := statsManager.getTotalExecutions(item.Procedure)
	if totalExecutions == 0 {
		return p.FastLane
	}
	
	// Categorize based on average execution time
	avgDuration := statsManager.getAvgDuration(item.Procedure)
	if avgDuration < 5*time.Second {
		return p.FastLane
	} else if avgDuration < 30*time.Second {
		return p.MediumLane
	} else {
		return p.SlowLane
	}
}

// getLaneDB returns the appropriate database for a given lane
func (p *Pipeline) getLaneDB(lane chan WorkItem, dbPools *DatabasePools) *DatabasePools {
	switch lane {
	case p.FastLane:
		return dbPools
	case p.MediumLane:
		return dbPools
	case p.SlowLane:
		return dbPools
	case p.ErrorLane:
		return dbPools
	default:
		return dbPools // Fallback
	}
}

// getLaneSpecificDB returns the specific database connection for a lane
func (p *Pipeline) getLaneSpecificDB(lane chan WorkItem, dbPools *DatabasePools) *sql.DB {
	switch lane {
	case p.FastLane:
		return dbPools.FastDB
	case p.MediumLane:
		return dbPools.MediumDB
	case p.SlowLane:
		return dbPools.SlowDB
	case p.ErrorLane:
		return dbPools.ErrorDB
	default:
		return dbPools.FastDB // Fallback
	}
}

// closeAllLanes closes all pipeline channels
func (p *Pipeline) closeAllLanes() {
	close(p.FastLane)
	close(p.MediumLane)
	close(p.SlowLane)
	close(p.ErrorLane)
}