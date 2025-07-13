package main

import (
	"encoding/json"
	"os"
)

// MainConfig represents the main application configuration
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

// loadMainConfig loads the main application configuration from a JSON file
func loadMainConfig(filepath string) (MainConfig, error) {
	var config MainConfig
	
	file, err := os.Open(filepath)
	if err != nil {
		return config, err
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	err = decoder.Decode(&config)
	return config, err
}