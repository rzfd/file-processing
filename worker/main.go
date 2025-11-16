package main

import (
	"context"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog/log"
	"github.com/rzfd/file-processing-system/internal/config"
	"github.com/rzfd/file-processing-system/internal/database"
	"github.com/rzfd/file-processing-system/internal/kafka"
	"github.com/rzfd/file-processing-system/internal/logger"
	"github.com/rzfd/file-processing-system/internal/minio"
	"github.com/rzfd/file-processing-system/internal/models"
	"github.com/rzfd/file-processing-system/internal/processor"
	"github.com/rzfd/file-processing-system/internal/validator"
)

var (
	processedFilesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "processed_files_total",
			Help: "Total number of processed files",
		},
		[]string{"status"},
	)

	processedRecordsTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "processed_records_total",
			Help: "Total number of processed records",
		},
	)

	processingDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "file_processing_duration_seconds",
			Help:    "File processing duration in seconds",
			Buckets: prometheus.DefBuckets,
		},
	)
)

func init() {
	prometheus.MustRegister(processedFilesTotal)
	prometheus.MustRegister(processedRecordsTotal)
	prometheus.MustRegister(processingDuration)
}

type Worker struct {
	db     *database.DB
	minio  *minio.Client
	kafka  *kafka.Consumer
	config *config.Config
}

func main() {
	// Setup logger
	logger.InitLogger()

	cfg := config.LoadConfig()

	log.Info().Msg("🔧 Starting Worker Service")

	// Initialize database
	db, err := database.NewDB(cfg)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize database")
	}
	defer db.Close()

	// Initialize MinIO
	minioClient, err := minio.NewClient(cfg)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize MinIO")
	}

	// Initialize Kafka consumer
	kafkaConsumer, err := kafka.NewConsumer(cfg)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize Kafka consumer")
	}
	defer kafkaConsumer.Close()

	worker := &Worker{
		db:     db,
		minio:  minioClient,
		kafka:  kafkaConsumer,
		config: cfg,
	}

	// Start metrics server
	go func() {
		metricsMux := http.NewServeMux()
		metricsMux.Handle("/metrics", promhttp.Handler())
		log.Info().Str("port", cfg.MetricsPort).Msg("Metrics server starting")
		if err := http.ListenAndServe(":"+cfg.MetricsPort, metricsMux); err != nil {
			log.Error().Err(err).Msg("Metrics server error")
		}
	}()

	log.Info().Msg("Worker service started - waiting for messages from Kafka")

	// Start consuming messages
	err = kafkaConsumer.ConsumeMessages(worker.processFile)
	if err != nil {
		log.Fatal().Err(err).Msg("Error consuming messages")
	}
}

func (w *Worker) processFile(event *models.FileProcessingEvent) error {
	start := time.Now()
	defer func() {
		duration := time.Since(start).Seconds()
		processingDuration.Observe(duration)
		log.Info().
			Float64("duration_seconds", duration).
			Msg("Processing completed")
	}()

	log.Info().Msg("========================================")
	log.Info().
		Str("filename", event.FileName).
		Int64("file_id", event.FileID).
		Str("bucket", event.BucketName).
		Str("object", event.ObjectName).
		Msg("Processing file")

	// Update status to processing
	log.Info().
		Int64("file_id", event.FileID).
		Msg("Updating status to 'processing'")
	if err := w.db.UpdateFileStatus(event.FileID, models.StatusProcessing); err != nil {
		log.Error().
			Err(err).
			Int64("file_id", event.FileID).
			Msg("Failed to update status to processing")
		processedFilesTotal.WithLabelValues("failed").Inc()
		return err
	}

	ctx := context.Background()

	// Download file from MinIO
	log.Info().
		Str("object", event.ObjectName).
		Msg("Downloading file from MinIO")
	reader, err := w.minio.DownloadFile(ctx, event.ObjectName)
	if err != nil {
		log.Error().
			Err(err).
			Str("object", event.ObjectName).
			Msg("Failed to download file from MinIO")
		w.db.UpdateFileStatus(event.FileID, models.StatusFailed)
		processedFilesTotal.WithLabelValues("failed").Inc()
		return err
	}
	defer reader.Close()
	log.Info().
		Str("object", event.ObjectName).
		Msg("File downloaded successfully")

	// Process file based on extension
	ext := strings.ToLower(filepath.Ext(event.FileName))
	log.Info().
		Str("extension", ext).
		Msg("Detecting file type")
	var records []models.ProcessedRecord

	switch ext {
	case ".csv":
		log.Info().Msg("Using CSV processor")
		records, err = processor.ProcessCSV(reader)
	case ".xlsx", ".xls":
		log.Info().Msg("Using XLSX processor")
		records, err = processor.ProcessXLSX(reader)
	default:
		err = fmt.Errorf("unsupported file type: %s", ext)
		log.Error().
			Str("extension", ext).
			Msg("Unsupported file type")
	}

	if err != nil {
		log.Error().
			Err(err).
			Str("filename", event.FileName).
			Msg("Failed to process file")
		w.db.UpdateFileStatus(event.FileID, models.StatusFailed)
		processedFilesTotal.WithLabelValues("failed").Inc()
		return err
	}

	log.Info().
		Int("record_count", len(records)).
		Str("filename", event.FileName).
		Msg("Successfully parsed records")

	// Validate records
	log.Info().
		Int("record_count", len(records)).
		Msg("Starting record validation")

	validationErrors := w.validateRecords(event.FileID, records)

	if len(validationErrors) > 0 {
		log.Error().
			Int("error_count", len(validationErrors)).
			Int("total_records", len(records)).
			Msg("Validation failed")

		// Log first few errors for debugging
		for i, verr := range validationErrors {
			if i >= 5 {
				break // Only log first 5 errors
			}
			log.Warn().
				Int("row", verr.Row).
				Str("field", verr.Field).
				Str("value", verr.Value).
				Str("error", verr.Message).
				Msg("Validation error")
		}

		w.db.UpdateFileStatus(event.FileID, models.StatusFailed)
		processedFilesTotal.WithLabelValues("failed").Inc()
		return fmt.Errorf("validation failed: %d errors out of %d records", len(validationErrors), len(records))
	}

	log.Info().
		Int("record_count", len(records)).
		Msg("All records passed validation")

	// Batch insert records
	if len(records) > 0 {
		// Process in batches
		batchSize := w.config.BatchSize
		totalBatches := (len(records) + batchSize - 1) / batchSize
		log.Info().
			Int("total_records", len(records)).
			Int("total_batches", totalBatches).
			Int("batch_size", batchSize).
			Msg("Starting batch insert")

		for i := 0; i < len(records); i += batchSize {
			end := i + batchSize
			if end > len(records) {
				end = len(records)
			}

			batchNum := (i / batchSize) + 1
			batch := records[i:end]

			log.Info().
				Int("batch_num", batchNum).
				Int("total_batches", totalBatches).
				Int("batch_records", len(batch)).
				Msg("Inserting batch")
			if err := w.db.BatchInsertProcessedRecords(event.FileID, batch); err != nil {
				log.Error().
					Err(err).
					Int("batch_num", batchNum).
					Msg("Failed to insert batch")
				w.db.UpdateFileStatus(event.FileID, models.StatusFailed)
				processedFilesTotal.WithLabelValues("failed").Inc()
				return err
			}

			processedRecordsTotal.Add(float64(len(batch)))
			log.Info().
				Int("batch_num", batchNum).
				Int("total_batches", totalBatches).
				Msg("Batch inserted successfully")
		}
		log.Info().Msg("All batches inserted successfully")
	} else {
		log.Warn().Msg("No records to insert")
	}

	// Update status to completed
	log.Info().
		Int64("file_id", event.FileID).
		Msg("Updating status to 'completed'")
	if err := w.db.UpdateFileStatusWithProcessedAt(event.FileID, models.StatusCompleted); err != nil {
		log.Error().
			Err(err).
			Int64("file_id", event.FileID).
			Msg("Failed to update status to completed")
		processedFilesTotal.WithLabelValues("failed").Inc()
		return err
	}

	log.Info().
		Int64("file_id", event.FileID).
		Str("filename", event.FileName).
		Int("records", len(records)).
		Str("status", "completed").
		Msg("✅ File processed successfully")
	log.Info().Msg("========================================")

	processedFilesTotal.WithLabelValues("completed").Inc()
	return nil
}

// validateRecords validates all records using validation rules
func (w *Worker) validateRecords(fileID int64, records []models.ProcessedRecord) []validator.ValidationError {
	// Define validation rules
	// You can customize these rules based on your business requirements
	rules := []validator.ValidationRule{
		{
			Field:    "id",
			Required: true,
			DataType: "int",
			MinValue: 1,
		},
		{
			Field:     "name",
			Required:  true,
			DataType:  "string",
			MinLength: 2,
			MaxLength: 100,
		},
		{
			Field:    "email",
			Required: false,
			DataType: "email",
		},
		{
			Field:    "age",
			Required: false,
			DataType: "int",
			MinValue: 0,
			MaxValue: 150,
		},
	}

	v := validator.NewValidator(rules)
	var allErrors []validator.ValidationError

	log.Info().
		Int("file_id", int(fileID)).
		Int("record_count", len(records)).
		Int("rule_count", len(rules)).
		Msg("Initializing validation")

	// Validate each record
	for i, record := range records {
		errors := v.ValidateRecord(i+1, record.Data)
		allErrors = append(allErrors, errors...)
	}

	if len(allErrors) > 0 {
		log.Warn().
			Int("file_id", int(fileID)).
			Int("total_errors", len(allErrors)).
			Float64("error_rate", float64(len(allErrors))/float64(len(records))*100).
			Msg("Validation completed with errors")
	} else {
		log.Info().
			Int("file_id", int(fileID)).
			Int("record_count", len(records)).
			Msg("All records validated successfully")
	}

	return allErrors
}
