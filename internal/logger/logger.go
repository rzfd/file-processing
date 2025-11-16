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

	// Use RFC3339 format for timestamps
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix

	// Output JSON to stdout for structured logging
	log.Logger = zerolog.New(os.Stdout).With().Timestamp().Logger()
}

// GetLogger returns a logger with a specific service name
func GetLogger(service string) zerolog.Logger {
	return log.With().Str("service", service).Logger()
}
