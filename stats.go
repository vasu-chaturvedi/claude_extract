package main

import (
	"log"
	"sync"
	"time"
)

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

// StatsManager handles procedure statistics and circuit breakers
type StatsManager struct {
	ProcStats       map[string]*ProcStats
	CircuitBreakers map[string]*CircuitBreaker
	Mutex           sync.RWMutex
}

// createStatsManager initializes the stats tracking system
func createStatsManager(procedures []string) *StatsManager {
	sm := &StatsManager{
		ProcStats:       make(map[string]*ProcStats),
		CircuitBreakers: make(map[string]*CircuitBreaker),
	}

	// Initialize stats and circuit breakers for each procedure
	for _, proc := range procedures {
		sm.ProcStats[proc] = &ProcStats{}
		sm.CircuitBreakers[proc] = &CircuitBreaker{
			FailureThreshold: 5,
			TimeoutThreshold: 60 * time.Second,
			ResetTimeout:     5 * time.Minute,
			State:            "CLOSED",
		}
	}

	return sm
}

// updateStats updates procedure execution statistics
func (sm *StatsManager) updateStats(procedure string, duration time.Duration, success bool) {
	sm.Mutex.Lock()
	defer sm.Mutex.Unlock()

	stats := sm.ProcStats[procedure]
	breaker := sm.CircuitBreakers[procedure]

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

// getCircuitBreakerState returns the current state of a procedure's circuit breaker
func (sm *StatsManager) getCircuitBreakerState(procedure string) string {
	sm.Mutex.RLock()
	defer sm.Mutex.RUnlock()

	breaker := sm.CircuitBreakers[procedure]

	// Check if circuit breaker should transition from OPEN to HALF_OPEN
	if breaker.State == "OPEN" {
		if time.Since(breaker.LastFailTime) > breaker.ResetTimeout {
			breaker.State = "HALF_OPEN"
		}
	}

	return breaker.State
}

// getAvgDuration returns the average execution time for a procedure
func (sm *StatsManager) getAvgDuration(procedure string) time.Duration {
	sm.Mutex.RLock()
	defer sm.Mutex.RUnlock()

	stats := sm.ProcStats[procedure]
	return stats.AvgDuration
}

// getTotalExecutions returns the total execution count for a procedure
func (sm *StatsManager) getTotalExecutions(procedure string) int64 {
	sm.Mutex.RLock()
	defer sm.Mutex.RUnlock()

	stats := sm.ProcStats[procedure]
	return stats.TotalExecutions
}

// printFinalStats displays final procedure statistics
func (sm *StatsManager) printFinalStats() {
	log.Printf("📊 Final Procedure Statistics:")
	sm.Mutex.RLock()
	defer sm.Mutex.RUnlock()

	for proc, stats := range sm.ProcStats {
		breaker := sm.CircuitBreakers[proc]
		log.Printf("  %s: Executions=%d, AvgTime=%s, Failures=%d, CircuitState=%s",
			proc, stats.TotalExecutions, stats.AvgDuration.Round(time.Millisecond),
			stats.FailureCount, breaker.State)
	}
}