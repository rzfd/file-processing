package validator

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/rs/zerolog/log"
)

// ValidationRule defines a validation rule for a field
type ValidationRule struct {
	Field       string
	Required    bool
	DataType    string // "string", "int", "float", "email", "date"
	MinLength   int
	MaxLength   int
	MinValue    float64
	MaxValue    float64
	Pattern     string // regex pattern
	CustomError string
}

// ValidationResult contains validation results
type ValidationResult struct {
	Valid    bool
	Errors   []ValidationError
	Warnings []ValidationWarning
}

// ValidationError represents a validation error
type ValidationError struct {
	Row     int
	Field   string
	Value   string
	Message string
}

// ValidationWarning represents a validation warning
type ValidationWarning struct {
	Row     int
	Field   string
	Value   string
	Message string
}

// Validator handles data validation
type Validator struct {
	Rules []ValidationRule
}

// NewValidator creates a new validator with rules
func NewValidator(rules []ValidationRule) *Validator {
	return &Validator{
		Rules: rules,
	}
}

// ValidateRecord validates a single record
func (v *Validator) ValidateRecord(row int, record map[string]interface{}) []ValidationError {
	var errors []ValidationError

	log.Debug().
		Int("row", row).
		Int("rule_count", len(v.Rules)).
		Msg("Validating record")

	for _, rule := range v.Rules {
		value, exists := record[rule.Field]

		// Check if required field exists
		if rule.Required && !exists {
			err := ValidationError{
				Row:     row,
				Field:   rule.Field,
				Value:   "",
				Message: fmt.Sprintf("Required field '%s' is missing", rule.Field),
			}
			errors = append(errors, err)

			log.Warn().
				Int("row", row).
				Str("field", rule.Field).
				Msg("Required field missing")
			continue
		}

		if !exists {
			continue // Skip validation if field doesn't exist and not required
		}

		// Convert value to string for validation
		valueStr := fmt.Sprintf("%v", value)

		// Validate data type
		if err := v.validateDataType(row, rule, valueStr); err != nil {
			errors = append(errors, *err)
		}

		// Validate length
		if err := v.validateLength(row, rule, valueStr); err != nil {
			errors = append(errors, *err)
		}

		// Validate range
		if err := v.validateRange(row, rule, valueStr); err != nil {
			errors = append(errors, *err)
		}

		// Validate pattern
		if err := v.validatePattern(row, rule, valueStr); err != nil {
			errors = append(errors, *err)
		}
	}

	if len(errors) > 0 {
		log.Warn().
			Int("row", row).
			Int("error_count", len(errors)).
			Msg("Record validation failed")
	} else {
		log.Debug().
			Int("row", row).
			Msg("Record validation passed")
	}

	return errors
}

// validateDataType validates the data type of a value
func (v *Validator) validateDataType(row int, rule ValidationRule, value string) *ValidationError {
	if rule.DataType == "" {
		return nil
	}

	switch rule.DataType {
	case "int":
		if _, err := strconv.Atoi(value); err != nil {
			log.Warn().
				Int("row", row).
				Str("field", rule.Field).
				Str("value", value).
				Msg("Invalid integer value")

			return &ValidationError{
				Row:     row,
				Field:   rule.Field,
				Value:   value,
				Message: fmt.Sprintf("Field '%s' must be an integer", rule.Field),
			}
		}

	case "float":
		if _, err := strconv.ParseFloat(value, 64); err != nil {
			log.Warn().
				Int("row", row).
				Str("field", rule.Field).
				Str("value", value).
				Msg("Invalid float value")

			return &ValidationError{
				Row:     row,
				Field:   rule.Field,
				Value:   value,
				Message: fmt.Sprintf("Field '%s' must be a number", rule.Field),
			}
		}

	case "email":
		emailRegex := regexp.MustCompile(`^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$`)
		if !emailRegex.MatchString(value) {
			log.Warn().
				Int("row", row).
				Str("field", rule.Field).
				Str("value", value).
				Msg("Invalid email format")

			return &ValidationError{
				Row:     row,
				Field:   rule.Field,
				Value:   value,
				Message: fmt.Sprintf("Field '%s' must be a valid email", rule.Field),
			}
		}

	case "date":
		// Simple date format validation (YYYY-MM-DD)
		dateRegex := regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`)
		if !dateRegex.MatchString(value) {
			log.Warn().
				Int("row", row).
				Str("field", rule.Field).
				Str("value", value).
				Msg("Invalid date format")

			return &ValidationError{
				Row:     row,
				Field:   rule.Field,
				Value:   value,
				Message: fmt.Sprintf("Field '%s' must be in YYYY-MM-DD format", rule.Field),
			}
		}
	}

	return nil
}

// validateLength validates the length of a string value
func (v *Validator) validateLength(row int, rule ValidationRule, value string) *ValidationError {
	length := len(value)

	if rule.MinLength > 0 && length < rule.MinLength {
		log.Warn().
			Int("row", row).
			Str("field", rule.Field).
			Int("length", length).
			Int("min_length", rule.MinLength).
			Msg("Value too short")

		return &ValidationError{
			Row:     row,
			Field:   rule.Field,
			Value:   value,
			Message: fmt.Sprintf("Field '%s' must be at least %d characters", rule.Field, rule.MinLength),
		}
	}

	if rule.MaxLength > 0 && length > rule.MaxLength {
		log.Warn().
			Int("row", row).
			Str("field", rule.Field).
			Int("length", length).
			Int("max_length", rule.MaxLength).
			Msg("Value too long")

		return &ValidationError{
			Row:     row,
			Field:   rule.Field,
			Value:   value,
			Message: fmt.Sprintf("Field '%s' must be at most %d characters", rule.Field, rule.MaxLength),
		}
	}

	return nil
}

// validateRange validates numeric range
func (v *Validator) validateRange(row int, rule ValidationRule, value string) *ValidationError {
	// Only validate range for numeric types
	if rule.DataType != "int" && rule.DataType != "float" {
		return nil
	}

	numValue, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return nil // Already handled by data type validation
	}

	if rule.MinValue != 0 && numValue < rule.MinValue {
		log.Warn().
			Int("row", row).
			Str("field", rule.Field).
			Float64("value", numValue).
			Float64("min_value", rule.MinValue).
			Msg("Value below minimum")

		return &ValidationError{
			Row:     row,
			Field:   rule.Field,
			Value:   value,
			Message: fmt.Sprintf("Field '%s' must be at least %.2f", rule.Field, rule.MinValue),
		}
	}

	if rule.MaxValue != 0 && numValue > rule.MaxValue {
		log.Warn().
			Int("row", row).
			Str("field", rule.Field).
			Float64("value", numValue).
			Float64("max_value", rule.MaxValue).
			Msg("Value above maximum")

		return &ValidationError{
			Row:     row,
			Field:   rule.Field,
			Value:   value,
			Message: fmt.Sprintf("Field '%s' must be at most %.2f", rule.Field, rule.MaxValue),
		}
	}

	return nil
}

// validatePattern validates against a regex pattern
func (v *Validator) validatePattern(row int, rule ValidationRule, value string) *ValidationError {
	if rule.Pattern == "" {
		return nil
	}

	matched, err := regexp.MatchString(rule.Pattern, value)
	if err != nil {
		log.Error().
			Err(err).
			Str("pattern", rule.Pattern).
			Msg("Invalid regex pattern")
		return nil
	}

	if !matched {
		log.Warn().
			Int("row", row).
			Str("field", rule.Field).
			Str("value", value).
			Str("pattern", rule.Pattern).
			Msg("Value doesn't match pattern")

		message := fmt.Sprintf("Field '%s' has invalid format", rule.Field)
		if rule.CustomError != "" {
			message = rule.CustomError
		}

		return &ValidationError{
			Row:     row,
			Field:   rule.Field,
			Value:   value,
			Message: message,
		}
	}

	return nil
}

// ValidateHeaders validates that required headers exist
func (v *Validator) ValidateHeaders(headers []string) []ValidationError {
	var errors []ValidationError
	headerMap := make(map[string]bool)

	for _, header := range headers {
		headerMap[strings.TrimSpace(header)] = true
	}

	log.Info().
		Int("header_count", len(headers)).
		Strs("headers", headers).
		Msg("Validating file headers")

	for _, rule := range v.Rules {
		if rule.Required && !headerMap[rule.Field] {
			err := ValidationError{
				Row:     0,
				Field:   rule.Field,
				Value:   "",
				Message: fmt.Sprintf("Required column '%s' is missing from file", rule.Field),
			}
			errors = append(errors, err)

			log.Error().
				Str("field", rule.Field).
				Msg("Required column missing from headers")
		}
	}

	if len(errors) > 0 {
		log.Error().
			Int("error_count", len(errors)).
			Msg("Header validation failed")
	} else {
		log.Info().Msg("Header validation passed")
	}

	return errors
}

// ValidateDuplicates checks for duplicate records based on specified key fields
func ValidateDuplicates(records []map[string]interface{}, keyFields []string) []ValidationError {
	var errors []ValidationError
	seen := make(map[string][]int) // hash -> row numbers

	log.Info().
		Int("record_count", len(records)).
		Strs("key_fields", keyFields).
		Msg("Checking for duplicate records")

	for i, record := range records {
		rowNum := i + 1

		// Build a hash key from the specified fields
		var keyParts []string
		for _, field := range keyFields {
			if val, exists := record[field]; exists {
				keyParts = append(keyParts, fmt.Sprintf("%v", val))
			} else {
				keyParts = append(keyParts, "")
			}
		}
		key := strings.Join(keyParts, "|")

		// Check if we've seen this combination before
		if rows, exists := seen[key]; exists {
			err := ValidationError{
				Row:     rowNum,
				Field:   strings.Join(keyFields, "+"),
				Value:   key,
				Message: fmt.Sprintf("Duplicate record found (same as row %v)", rows),
			}
			errors = append(errors, err)

			log.Warn().
				Int("row", rowNum).
				Ints("duplicate_of", rows).
				Str("key", key).
				Msg("Duplicate record detected")
		} else {
			seen[key] = []int{rowNum}
		}

		// Add current row to the list for this key
		if rows, exists := seen[key]; exists && len(rows) > 0 && rows[0] != rowNum {
			seen[key] = append(rows, rowNum)
		}
	}

	if len(errors) > 0 {
		log.Warn().
			Int("duplicate_count", len(errors)).
			Msg("Duplicate records found")
	} else {
		log.Info().Msg("No duplicate records found")
	}

	return errors
}

// GetValidationSummary returns a summary of validation results
func (v *ValidationResult) GetValidationSummary() string {
	if v.Valid {
		return "All records passed validation"
	}

	return fmt.Sprintf("Validation failed: %d errors, %d warnings",
		len(v.Errors), len(v.Warnings))
}
