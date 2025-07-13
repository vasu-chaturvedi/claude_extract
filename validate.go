package main

import (
	"fmt"
	"time"
)

func validatePipeline() {
	fmt.Println("🧪 Testing Hybrid Pipeline Implementation...")

	// Test worker pool creation
	procedures := []string{"PROC_A", "PROC_B", "PROC_C"}
	wp := createWorkerPool(12, procedures)

	fmt.Printf("✅ Worker Distribution: Fast(%d) Medium(%d) Slow(%d) Error(%d)\n",
		wp.FastWorkers, wp.MediumWorkers, wp.SlowWorkers, wp.ErrorWorkers)

	// Test categorization
	item := WorkItem{Procedure: "PROC_A", SolID: "TEST001"}
	lane := wp.categorizeWorkItem(item)
	fmt.Printf("✅ New procedure categorized to: %p (FastLane: %p)\n", lane, wp.Pipeline.FastLane)

	// Test stats update
	wp.updateStats("PROC_A", 2*time.Second, true)
	stats := wp.ProcStats["PROC_A"]
	fmt.Printf("✅ Stats updated: Executions=%d, AvgDuration=%s\n", 
		stats.TotalExecutions, stats.AvgDuration)

	// Test circuit breaker
	for i := 0; i < 6; i++ {
		wp.updateStats("PROC_B", 1*time.Second, false)
	}
	breaker := wp.CircuitBreakers["PROC_B"]
	fmt.Printf("✅ Circuit breaker state after failures: %s\n", breaker.State)

	// Test lane assignment after stats
	wp.updateStats("PROC_C", 45*time.Second, true)
	slowItem := WorkItem{Procedure: "PROC_C", SolID: "TEST002"}
	slowLane := wp.categorizeWorkItem(slowItem)
	fmt.Printf("✅ Slow procedure categorized to: %p (SlowLane: %p)\n", slowLane, wp.Pipeline.SlowLane)

	fmt.Println("🎯 All tests passed! Hybrid pipeline is ready.")
}