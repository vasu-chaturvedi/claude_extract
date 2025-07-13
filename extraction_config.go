package main

import (
	"encoding/json"
	"os"
)

// ExtractionConfig represents the extraction/insertion configuration
type ExtractionConfig struct {
	PackageName       string   `json:"package_name"`
	Procedures        []string `json:"procedures"`
	Format            string   `json:"format"`
	Delimiter         string   `json:"delimiter,omitempty"`
	TemplatePath      string   `json:"template_path"`
	OutputPath        string   `json:"output_path"`
	SpoolOutputPath   string   `json:"spool_output_path"`
	CombinedOutput    string   `json:"combined_output"`
}

// loadExtractionConfig loads the extraction configuration from a JSON file
func loadExtractionConfig(filepath string) (ExtractionConfig, error) {
	var config ExtractionConfig
	
	file, err := os.Open(filepath)
	if err != nil {
		return config, err
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	err = decoder.Decode(&config)
	return config, err
}