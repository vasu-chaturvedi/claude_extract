package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// readSols reads SOL IDs from a file
func readSols(filepath string) ([]string, error) {
	file, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var sols []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" {
			sols = append(sols, line)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return sols, nil
}

// readColumnsFromCSV is implemented in ExtractData.go to avoid duplication

// loadTemplates loads all CSV templates for the given procedures
func loadTemplates(procedures []string, templatePath string) (map[string][]ColumnConfig, error) {
	templates := make(map[string][]ColumnConfig)
	
	for _, proc := range procedures {
		tmplPath := filepath.Join(templatePath, fmt.Sprintf("%s.csv", proc))
		cols, err := readColumnsFromCSV(tmplPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read template for %s: %v", proc, err)
		}
		templates[proc] = cols
	}
	
	return templates, nil
}

// writeSummary writes procedure summary to CSV file
func writeSummary(path string, summary map[string]ProcSummary) error {
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("failed to create procedure summary file: %v", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Header
	writer.Write([]string{"PROCEDURE", "EARLIEST_START_TIME", "LATEST_END_TIME", "EXECUTION_SECONDS", "STATUS"})

	// Sort procedures alphabetically
	var procs []string
	for p := range summary {
		procs = append(procs, p)
	}
	sort.Strings(procs)

	timeFormat := "02-01-2006 15:04:05"
	for _, p := range procs {
		s := summary[p]
		execSeconds := s.EndTime.Sub(s.StartTime).Seconds()
		writer.Write([]string{
			p,
			s.StartTime.Format(timeFormat),
			s.EndTime.Format(timeFormat),
			fmt.Sprintf("%.3f", execSeconds),
			s.Status,
		})
	}
	
	return nil
}