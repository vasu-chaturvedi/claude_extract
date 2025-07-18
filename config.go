package main

import (
	"bufio"
	"encoding/json"
	"os"
	"slices"
)

type MainConfig struct {
	DBUser      string `json:"db_user"`
	DBPassword  string `json:"db_password"`
	DBHost      string `json:"db_host"`
	DBPort      int    `json:"db_port"`
	DBSid       string `json:"db_sid"`
	Concurrency int    `json:"concurrency"`
	LogFilePath string `json:"log_path"`
	SolFilePath string `json:"sol_list_path"`
}

type ExtractionConfig struct {
	PackageName           string   `json:"package_name"`
	Procedures            []string `json:"procedures"`
	SpoolOutputPath       string   `json:"spool_output_path"`
	RunInsertionParallel  bool     `json:"run_insertion_parallel"`
	RunExtractionParallel bool     `json:"run_extraction_parallel"`
	UseProcLevelParallel  bool     `json:"use_proc_level_parallel"`
	TemplatePath          string   `json:"template_path"`
	Format                string   `json:"format"`
	Delimiter             string   `json:"delimiter"`
	// Chunked debit-credit processing
	ChunkedProcedures     []string `json:"chunked_procedures,omitempty"`     // Procedures that use chunked logic
	ChunkSize             int      `json:"chunk_size,omitempty"`             // Default: 5000 records per chunk
	ChunkProcedureSuffix  string   `json:"chunk_procedure_suffix,omitempty"` // Suffix for chunk procedures (e.g., "_CHUNK")
}

func loadMainConfig(path string) (MainConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return MainConfig{}, err
	}

	var cfg MainConfig
	err = json.Unmarshal(data, &cfg)
	return cfg, err
}

func loadExtractionConfig(path string) (ExtractionConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return ExtractionConfig{}, err
	}

	var cfg ExtractionConfig
	err = json.Unmarshal(data, &cfg)
	return cfg, err
}

func readSols(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var sols []string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if line != "" {
			sols = append(sols, line)
		}
	}
	return sols, scanner.Err()
}

// Check if a procedure uses chunked logic
func isChunkedProcedure(proc string, chunkedProcs []string) bool {
	return slices.Contains(chunkedProcs, proc)
}

// Set default values for chunked configuration
func setChunkedDefaults(config *ExtractionConfig) {
	if config.ChunkSize == 0 {
		config.ChunkSize = 5000 // Default chunk size
	}
	// Note: ChunkProcedureSuffix is not used - hardcoded "_EXTRACT" in chunkedExtraction.go
}
