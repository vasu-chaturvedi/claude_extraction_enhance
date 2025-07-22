package main

import (
	"errors"
	"fmt"
	"time"
)

// ApplicationError represents a custom error with additional context
type ApplicationError struct {
	Code       string
	Message    string
	Cause      error
	Timestamp  time.Time
	Context    map[string]interface{}
	Retryable  bool
	Severity   ErrorSeverity
}

// ErrorSeverity represents the severity level of an error
type ErrorSeverity int

const (
	SeverityInfo ErrorSeverity = iota
	SeverityWarning
	SeverityError
	SeverityCritical
	SeverityFatal
)

// String returns the string representation of error severity
func (s ErrorSeverity) String() string {
	switch s {
	case SeverityInfo:
		return "INFO"
	case SeverityWarning:
		return "WARNING"
	case SeverityError:
		return "ERROR"
	case SeverityCritical:
		return "CRITICAL"
	case SeverityFatal:
		return "FATAL"
	default:
		return "UNKNOWN"
	}
}

// NewApplicationError creates a new application error with context
func NewApplicationError(code, message string, cause error) *ApplicationError {
	return &ApplicationError{
		Code:      code,
		Message:   message,
		Cause:     cause,
		Timestamp: time.Now(),
		Context:   make(map[string]interface{}),
		Retryable: false,
		Severity:  SeverityError,
	}
}

// Error implements the error interface
func (e *ApplicationError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("%s: %s (caused by: %v)", e.Code, e.Message, e.Cause)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

// Unwrap returns the wrapped error for errors.Is and errors.As
func (e *ApplicationError) Unwrap() error {
	return e.Cause
}

// WithContext adds context information to the error
func (e *ApplicationError) WithContext(key string, value interface{}) *ApplicationError {
	if e.Context == nil {
		e.Context = make(map[string]interface{})
	}
	e.Context[key] = value
	return e
}

// WithSeverity sets the error severity
func (e *ApplicationError) WithSeverity(severity ErrorSeverity) *ApplicationError {
	e.Severity = severity
	return e
}

// WithRetryable marks the error as retryable or not
func (e *ApplicationError) WithRetryable(retryable bool) *ApplicationError {
	e.Retryable = retryable
	return e
}

// IsRetryable checks if the error is retryable
func (e *ApplicationError) IsRetryable() bool {
	return e.Retryable
}

// GetSeverity returns the error severity
func (e *ApplicationError) GetSeverity() ErrorSeverity {
	return e.Severity
}

// GetContext returns the error context
func (e *ApplicationError) GetContext() map[string]interface{} {
	return e.Context
}

// Database-specific errors
var (
	ErrDatabaseConnection = &ApplicationError{
		Code:     "DB_CONNECTION_FAILED",
		Message:  "Failed to establish database connection",
		Severity: SeverityCritical,
	}
	
	ErrDatabaseTimeout = &ApplicationError{
		Code:      "DB_TIMEOUT",
		Message:   "Database operation timed out",
		Retryable: true,
		Severity:  SeverityError,
	}
	
	ErrProcedureExecution = &ApplicationError{
		Code:     "PROCEDURE_FAILED",
		Message:  "Stored procedure execution failed",
		Severity: SeverityError,
	}
	
	ErrInvalidQuery = &ApplicationError{
		Code:     "INVALID_QUERY",
		Message:  "Database query is invalid",
		Severity: SeverityError,
	}
)

// Configuration-specific errors
var (
	ErrConfigNotFound = &ApplicationError{
		Code:     "CONFIG_NOT_FOUND",
		Message:  "Configuration file not found",
		Severity: SeverityCritical,
	}
	
	ErrConfigInvalid = &ApplicationError{
		Code:     "CONFIG_INVALID",
		Message:  "Configuration is invalid",
		Severity: SeverityCritical,
	}
	
	ErrMissingParameter = &ApplicationError{
		Code:     "MISSING_PARAMETER",
		Message:  "Required parameter is missing",
		Severity: SeverityError,
	}
)

// File operation errors
var (
	ErrFileNotFound = &ApplicationError{
		Code:     "FILE_NOT_FOUND",
		Message:  "File not found",
		Severity: SeverityError,
	}
	
	ErrFilePermission = &ApplicationError{
		Code:     "FILE_PERMISSION",
		Message:  "Insufficient file permissions",
		Severity: SeverityError,
	}
	
	ErrFileWrite = &ApplicationError{
		Code:      "FILE_WRITE_FAILED",
		Message:   "Failed to write to file",
		Retryable: true,
		Severity:  SeverityError,
	}
	
	ErrFileRead = &ApplicationError{
		Code:      "FILE_READ_FAILED",
		Message:   "Failed to read from file",
		Retryable: true,
		Severity:  SeverityError,
	}
)

// Processing errors
var (
	ErrProcessingFailed = &ApplicationError{
		Code:     "PROCESSING_FAILED",
		Message:  "Data processing failed",
		Severity: SeverityError,
	}
	
	ErrValidationFailed = &ApplicationError{
		Code:     "VALIDATION_FAILED",
		Message:  "Data validation failed",
		Severity: SeverityWarning,
	}
	
	ErrTimeoutExceeded = &ApplicationError{
		Code:      "TIMEOUT_EXCEEDED",
		Message:   "Operation timeout exceeded",
		Retryable: true,
		Severity:  SeverityError,
	}
)

// Helper functions for creating errors with context

// NewDatabaseError creates a database-related error with context
func NewDatabaseError(operation, details string, cause error) *ApplicationError {
	return ErrDatabaseConnection.
		WithContext("operation", operation).
		WithContext("details", details).
		WithContext("cause", cause)
}

// NewConfigurationError creates a configuration-related error
func NewConfigurationError(configFile, parameter string, cause error) *ApplicationError {
	return ErrConfigInvalid.
		WithContext("config_file", configFile).
		WithContext("parameter", parameter).
		WithContext("cause", cause)
}

// NewFileError creates a file operation error
func NewFileError(operation, filename string, cause error) *ApplicationError {
	err := &ApplicationError{
		Code:      fmt.Sprintf("FILE_%s_FAILED", operation),
		Message:   fmt.Sprintf("File %s operation failed", operation),
		Cause:     cause,
		Timestamp: time.Now(),
		Context:   make(map[string]interface{}),
		Severity:  SeverityError,
	}
	
	return err.
		WithContext("operation", operation).
		WithContext("filename", filename)
}

// NewProcessingError creates a processing-related error
func NewProcessingError(stage, solID, procedure string, cause error) *ApplicationError {
	return ErrProcessingFailed.
		WithContext("stage", stage).
		WithContext("sol_id", solID).
		WithContext("procedure", procedure).
		WithContext("cause", cause)
}

// NewValidationError creates a validation-specific error
func NewValidationError(field, message string, cause error) *ApplicationError {
	return NewApplicationError("VALIDATION_ERROR", 
		fmt.Sprintf("validation failed for field '%s': %s", field, message), cause).
		WithContext("field", field).
		WithSeverity(SeverityError)
}

// IsError checks if an error matches a specific application error type
func IsError(err error, target *ApplicationError) bool {
	var appErr *ApplicationError
	if errors.As(err, &appErr) {
		return appErr.Code == target.Code
	}
	return false
}

// IsDatabaseError checks if an error is database-related
func IsDatabaseError(err error) bool {
	var appErr *ApplicationError
	if errors.As(err, &appErr) {
		return appErr.Code == "DB_CONNECTION_FAILED" || 
			   appErr.Code == "DB_TIMEOUT" || 
			   appErr.Code == "PROCEDURE_FAILED" ||
			   appErr.Code == "INVALID_QUERY"
	}
	return false
}

// IsRetryableError checks if an error is retryable
func IsRetryableError(err error) bool {
	var appErr *ApplicationError
	if errors.As(err, &appErr) {
		return appErr.IsRetryable()
	}
	return false
}

// GetErrorSeverity returns the severity of an error
func GetErrorSeverity(err error) ErrorSeverity {
	var appErr *ApplicationError
	if errors.As(err, &appErr) {
		return appErr.GetSeverity()
	}
	return SeverityError // Default severity for unknown errors
}

// ErrorChain represents a chain of related errors
type ErrorChain struct {
	errors []error
}

// NewErrorChain creates a new error chain
func NewErrorChain() *ErrorChain {
	return &ErrorChain{
		errors: make([]error, 0),
	}
}

// Add adds an error to the chain
func (ec *ErrorChain) Add(err error) *ErrorChain {
	if err != nil {
		ec.errors = append(ec.errors, err)
	}
	return ec
}

// HasErrors returns true if the chain contains any errors
func (ec *ErrorChain) HasErrors() bool {
	return len(ec.errors) > 0
}

// Count returns the number of errors in the chain
func (ec *ErrorChain) Count() int {
	return len(ec.errors)
}

// Errors returns all errors in the chain
func (ec *ErrorChain) Errors() []error {
	return ec.errors
}

// Error implements the error interface
func (ec *ErrorChain) Error() string {
	if len(ec.errors) == 0 {
		return "no errors"
	}
	
	if len(ec.errors) == 1 {
		return ec.errors[0].Error()
	}
	
	return fmt.Sprintf("multiple errors occurred (%d total): %v", len(ec.errors), ec.errors[0])
}

// FirstError returns the first error in the chain
func (ec *ErrorChain) FirstError() error {
	if len(ec.errors) > 0 {
		return ec.errors[0]
	}
	return nil
}

// LastError returns the last error in the chain
func (ec *ErrorChain) LastError() error {
	if len(ec.errors) > 0 {
		return ec.errors[len(ec.errors)-1]
	}
	return nil
}