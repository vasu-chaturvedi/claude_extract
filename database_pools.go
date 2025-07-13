package main

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	_ "github.com/godror/godror"
)

// DatabasePools manages separate connection pools per lane
type DatabasePools struct {
	FastDB   *sql.DB
	MediumDB *sql.DB
	SlowDB   *sql.DB
	ErrorDB  *sql.DB
}

// createDatabasePools creates optimized connection pools per lane
func createDatabasePools(connString string, totalConcurrency int) (*DatabasePools, error) {
	// Fast pool: 50% connections, short idle timeout
	fastDB, err := sql.Open("godror", connString)
	if err != nil {
		return nil, fmt.Errorf("failed to create fast DB pool: %v", err)
	}
	fastConns := int(float64(totalConcurrency) * 0.5)
	if fastConns == 0 {
		fastConns = 1
	}
	fastDB.SetMaxOpenConns(fastConns)
	fastDB.SetMaxIdleConns(fastConns)
	fastDB.SetConnMaxLifetime(5 * time.Minute)
	fastDB.SetConnMaxIdleTime(1 * time.Minute)

	// Medium pool: 30% connections, medium idle timeout
	mediumDB, err := sql.Open("godror", connString)
	if err != nil {
		return nil, fmt.Errorf("failed to create medium DB pool: %v", err)
	}
	mediumConns := int(float64(totalConcurrency) * 0.3)
	if mediumConns == 0 {
		mediumConns = 1
	}
	mediumDB.SetMaxOpenConns(mediumConns)
	mediumDB.SetMaxIdleConns(mediumConns / 2)
	mediumDB.SetConnMaxLifetime(15 * time.Minute)
	mediumDB.SetConnMaxIdleTime(5 * time.Minute)

	// Slow pool: 15% connections, long idle timeout
	slowDB, err := sql.Open("godror", connString)
	if err != nil {
		return nil, fmt.Errorf("failed to create slow DB pool: %v", err)
	}
	slowConns := int(float64(totalConcurrency) * 0.15)
	if slowConns == 0 {
		slowConns = 1
	}
	slowDB.SetMaxOpenConns(slowConns)
	slowDB.SetMaxIdleConns(slowConns / 3)
	slowDB.SetConnMaxLifetime(30 * time.Minute)
	slowDB.SetConnMaxIdleTime(15 * time.Minute)

	// Error pool: 5% connections, minimal resources
	errorDB, err := sql.Open("godror", connString)
	if err != nil {
		return nil, fmt.Errorf("failed to create error DB pool: %v", err)
	}
	errorConns := totalConcurrency - fastConns - mediumConns - slowConns
	if errorConns == 0 {
		errorConns = 1
	}
	errorDB.SetMaxOpenConns(errorConns)
	errorDB.SetMaxIdleConns(1)
	errorDB.SetConnMaxLifetime(10 * time.Minute)
	errorDB.SetConnMaxIdleTime(2 * time.Minute)

	return &DatabasePools{
		FastDB:   fastDB,
		MediumDB: mediumDB,
		SlowDB:   slowDB,
		ErrorDB:  errorDB,
	}, nil
}

// warmupConnections pre-establishes database connections for all pools
func (dbPools *DatabasePools) warmupConnections(ctx context.Context) error {
	warmupCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(4)

	var warmupError error
	var mu sync.Mutex

	go func() {
		defer wg.Done()
		if err := dbPools.FastDB.PingContext(warmupCtx); err != nil {
			mu.Lock()
			warmupError = fmt.Errorf("fast DB warmup failed: %v", err)
			mu.Unlock()
		}
	}()
	go func() {
		defer wg.Done()
		if err := dbPools.MediumDB.PingContext(warmupCtx); err != nil {
			mu.Lock()
			warmupError = fmt.Errorf("medium DB warmup failed: %v", err)
			mu.Unlock()
		}
	}()
	go func() {
		defer wg.Done()
		if err := dbPools.SlowDB.PingContext(warmupCtx); err != nil {
			mu.Lock()
			warmupError = fmt.Errorf("slow DB warmup failed: %v", err)
			mu.Unlock()
		}
	}()
	go func() {
		defer wg.Done()
		if err := dbPools.ErrorDB.PingContext(warmupCtx); err != nil {
			mu.Lock()
			warmupError = fmt.Errorf("error DB warmup failed: %v", err)
			mu.Unlock()
		}
	}()

	wg.Wait()
	return warmupError
}

// Close closes all database connections
func (dbPools *DatabasePools) Close() error {
	var errs []error
	
	if err := dbPools.FastDB.Close(); err != nil {
		errs = append(errs, fmt.Errorf("fast DB close error: %v", err))
	}
	if err := dbPools.MediumDB.Close(); err != nil {
		errs = append(errs, fmt.Errorf("medium DB close error: %v", err))
	}
	if err := dbPools.SlowDB.Close(); err != nil {
		errs = append(errs, fmt.Errorf("slow DB close error: %v", err))
	}
	if err := dbPools.ErrorDB.Close(); err != nil {
		errs = append(errs, fmt.Errorf("error DB close error: %v", err))
	}
	
	if len(errs) > 0 {
		return fmt.Errorf("database close errors: %v", errs)
	}
	return nil
}