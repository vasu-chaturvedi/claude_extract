package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"sync"
	"time"
)

// BatchLogger manages efficient log writing
type BatchLogger struct {
	LogCh         chan ProcLog
	BatchSize     int
	FlushInterval time.Duration
	FilePath      string
	Buffer        []ProcLog
	LastFlush     time.Time
	Mutex         sync.Mutex
}

// createBatchLogger initializes the buffered logging system
func createBatchLogger(filePath string) *BatchLogger {
	logger := &BatchLogger{
		LogCh:         make(chan ProcLog, 1000),
		BatchSize:     100,
		FlushInterval: 5 * time.Second,
		FilePath:      filePath,
		Buffer:        make([]ProcLog, 0, 100),
		LastFlush:     time.Now(),
	}

	go logger.run()
	return logger
}

// run processes logs in batches
func (bl *BatchLogger) run() {
	ticker := time.NewTicker(bl.FlushInterval)
	defer ticker.Stop()

	var file *os.File
	var writer *csv.Writer

	for {
		select {
		case log, ok := <-bl.LogCh:
			if !ok {
				// Channel closed, flush remaining logs
				bl.flushBuffer(&file, &writer)
				if file != nil {
					file.Close()
				}
				return
			}

			bl.Mutex.Lock()
			bl.Buffer = append(bl.Buffer, log)
			shouldFlush := len(bl.Buffer) >= bl.BatchSize
			bl.Mutex.Unlock()

			if shouldFlush {
				bl.flushBuffer(&file, &writer)
			}

		case <-ticker.C:
			// Periodic flush
			bl.Mutex.Lock()
			shouldFlush := len(bl.Buffer) > 0 && time.Since(bl.LastFlush) >= bl.FlushInterval
			bl.Mutex.Unlock()

			if shouldFlush {
				bl.flushBuffer(&file, &writer)
			}
		}
	}
}

// flushBuffer writes accumulated logs to file
func (bl *BatchLogger) flushBuffer(file **os.File, writer **csv.Writer) {
	bl.Mutex.Lock()
	defer bl.Mutex.Unlock()

	if len(bl.Buffer) == 0 {
		return
	}

	// Create file and writer if needed
	if *file == nil {
		var err error
		*file, err = os.Create(bl.FilePath)
		if err != nil {
			fmt.Printf("Failed to create log file: %v\n", err)
			return
		}

		*writer = csv.NewWriter(*file)
		// Write header
		(*writer).Write([]string{"SOL_ID", "PROCEDURE", "START_TIME", "END_TIME", "EXECUTION_SECONDS", "STATUS", "ERROR_DETAILS"})
	}

	// Write all buffered logs with pre-allocated string slice
	timeFormat := "02-01-2006 15:04:05"
	record := make([]string, 7) // Pre-allocate once for all records
	for _, plog := range bl.Buffer {
		errDetails := plog.ErrorDetails
		if errDetails == "" {
			errDetails = "-"
		}

		// Reuse the same slice to reduce allocations
		record[0] = plog.SolID
		record[1] = plog.Procedure
		record[2] = plog.StartTime.Format(timeFormat)
		record[3] = plog.EndTime.Format(timeFormat)
		record[4] = fmt.Sprintf("%.3f", plog.ExecutionTime.Seconds())
		record[5] = plog.Status
		record[6] = errDetails

		(*writer).Write(record)
	}

	(*writer).Flush()
	bl.Buffer = bl.Buffer[:0] // Reset buffer
	bl.LastFlush = time.Now()
}

// Close finalizes the batch logger
func (bl *BatchLogger) Close() {
	close(bl.LogCh)
	// Give some time for final flush
	time.Sleep(1 * time.Second)
}