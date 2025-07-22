package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strings"
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
	// Signature extraction configuration
	SignatureExtraction   *SignatureConfig `json:"signature_extraction,omitempty"` // Configuration for BLOB signature extraction
}

func loadMainConfig(path string) (MainConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return MainConfig{}, err
	}

	var cfg MainConfig
	err = json.Unmarshal(data, &cfg)
	if err != nil {
		return MainConfig{}, err
	}

	// Validate configuration
	if err := validateMainConfig(&cfg); err != nil {
		return MainConfig{}, err
	}

	return cfg, nil
}

func loadExtractionConfig(path string) (ExtractionConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return ExtractionConfig{}, err
	}

	var cfg ExtractionConfig
	err = json.Unmarshal(data, &cfg)
	if err != nil {
		return ExtractionConfig{}, err
	}

	// Validate configuration
	if err := validateExtractionConfig(&cfg); err != nil {
		return ExtractionConfig{}, err
	}

	return cfg, nil
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

// validateMainConfig validates main configuration parameters
func validateMainConfig(cfg *MainConfig) error {
	if cfg.DBUser == "" {
		return fmt.Errorf("db_user is required")
	}
	if cfg.DBPassword == "" {
		return fmt.Errorf("db_password is required")
	}
	if cfg.DBHost == "" {
		return fmt.Errorf("db_host is required")
	}
	if cfg.DBPort <= 0 || cfg.DBPort > 65535 {
		return fmt.Errorf("db_port must be between 1 and 65535")
	}
	if cfg.DBSid == "" {
		return fmt.Errorf("db_sid is required")
	}
	if cfg.Concurrency <= 0 {
		return fmt.Errorf("concurrency must be greater than 0")
	}
	if cfg.Concurrency > 500 {
		return fmt.Errorf("concurrency should not exceed 500 to avoid resource exhaustion")
	}
	if cfg.LogFilePath == "" {
		return fmt.Errorf("log_path is required")
	}
	if cfg.SolFilePath == "" {
		return fmt.Errorf("sol_list_path is required")
	}
	return nil
}

// validateExtractionConfig validates extraction configuration parameters
func validateExtractionConfig(cfg *ExtractionConfig) error {
	if cfg.PackageName == "" {
		return fmt.Errorf("package_name is required")
	}
	if len(cfg.Procedures) == 0 {
		return fmt.Errorf("at least one procedure must be specified")
	}
	if cfg.SpoolOutputPath == "" {
		return fmt.Errorf("spool_output_path is required")
	}
	if cfg.TemplatePath == "" {
		return fmt.Errorf("template_path is required")
	}
	if cfg.Format != "" && cfg.Format != "delimited" && cfg.Format != "fixed" {
		return fmt.Errorf("format must be either 'delimited' or 'fixed'")
	}
	if cfg.Format == "delimited" && cfg.Delimiter == "" {
		return fmt.Errorf("delimiter is required when format is 'delimited'")
	}
	if cfg.ChunkSize < 0 {
		return fmt.Errorf("chunk_size cannot be negative")
	}
	if cfg.ChunkSize > 100000 {
		return fmt.Errorf("chunk_size should not exceed 100000 to avoid memory issues")
	}
	
	// Validate procedure names
	for _, proc := range cfg.Procedures {
		if strings.TrimSpace(proc) == "" {
			return fmt.Errorf("procedure names cannot be empty")
		}
	}
	
	// Validate chunked procedures are subset of main procedures
	for _, chunkedProc := range cfg.ChunkedProcedures {
		if !slices.Contains(cfg.Procedures, chunkedProc) {
			return fmt.Errorf("chunked procedure '%s' is not in the main procedures list", chunkedProc)
		}
	}
	
	// Validate signature extraction configuration if present
	if cfg.SignatureExtraction != nil {
		if err := ValidateSignatureConfig(cfg.SignatureExtraction); err != nil {
			return fmt.Errorf("signature extraction configuration error: %w", err)
		}
	}
	
	return nil
}
