package logger

import (
	"os"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// InitLogger initializes the global logger with JSON output
func InitLogger() {
	// Set global log level to INFO (hide DEBUG logs)
	zerolog.SetGlobalLevel(zerolog.InfoLevel)

	// Use RFC3339 format for timestamps (ISO 8601 format for better compatibility)
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnixMs

	// Output JSON to stdout for structured logging
	// This will be captured by Fluent Bit and sent to Loki
	log.Logger = zerolog.New(os.Stdout).With().Timestamp().Logger()
}

// GetLogger returns a logger with a specific service name
func GetLogger(service string) zerolog.Logger {
	return log.With().Str("service", service).Logger()
}
