package main

import (
	"testing"
	"time"
)

func TestPipelineCategorization(t *testing.T) {
	// Create a test worker pool
	procedures := []string{"FAST_PROC", "MEDIUM_PROC", "SLOW_PROC"}
	wp := createWorkerPool(10, procedures)

	// Test initial categorization (should go to fast lane)
	item := WorkItem{Procedure: "FAST_PROC", SolID: "TEST001"}
	lane := wp.categorizeWorkItem(item)
	
	if lane != wp.Pipeline.FastLane {
		t.Error("New procedures should start in fast lane")
	}

	// Simulate fast procedure stats
	wp.updateStats("FAST_PROC", 2*time.Second, true)
	lane = wp.categorizeWorkItem(WorkItem{Procedure: "FAST_PROC", SolID: "TEST002"})
	if lane != wp.Pipeline.FastLane {
		t.Error("Fast procedures should stay in fast lane")
	}

	// Simulate medium procedure stats
	wp.updateStats("MEDIUM_PROC", 15*time.Second, true)
	lane = wp.categorizeWorkItem(WorkItem{Procedure: "MEDIUM_PROC", SolID: "TEST003"})
	if lane != wp.Pipeline.MediumLane {
		t.Error("Medium procedures should go to medium lane")
	}

	// Simulate slow procedure stats
	wp.updateStats("SLOW_PROC", 60*time.Second, true)
	lane = wp.categorizeWorkItem(WorkItem{Procedure: "SLOW_PROC", SolID: "TEST004"})
	if lane != wp.Pipeline.SlowLane {
		t.Error("Slow procedures should go to slow lane")
	}
}

func TestCircuitBreaker(t *testing.T) {
	procedures := []string{"FAILING_PROC"}
	wp := createWorkerPool(4, procedures)

	// Simulate multiple failures
	for i := 0; i < 5; i++ {
		wp.updateStats("FAILING_PROC", 1*time.Second, false)
	}

	// Circuit breaker should be OPEN
	breaker := wp.CircuitBreakers["FAILING_PROC"]
	if breaker.State != "OPEN" {
		t.Error("Circuit breaker should be OPEN after 5 failures")
	}

	// Work should go to error lane
	item := WorkItem{Procedure: "FAILING_PROC", SolID: "TEST005"}
	lane := wp.categorizeWorkItem(item)
	if lane != wp.Pipeline.ErrorLane {
		t.Error("Failed procedures should go to error lane when circuit is OPEN")
	}
}

func TestWorkerDistribution(t *testing.T) {
	procedures := []string{"PROC_A", "PROC_B"}
	wp := createWorkerPool(10, procedures)

	// Check worker distribution
	total := wp.FastWorkers + wp.MediumWorkers + wp.SlowWorkers + wp.ErrorWorkers
	if total != 10 {
		t.Errorf("Total workers should be 10, got %d", total)
	}

	// Fast workers should get the most (50%)
	if wp.FastWorkers < wp.MediumWorkers || wp.FastWorkers < wp.SlowWorkers {
		t.Error("Fast workers should get the largest allocation")
	}
}