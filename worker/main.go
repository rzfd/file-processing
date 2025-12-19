package main

import (
	"context"
	"fmt"
	"net/http"
	"path/filepath"
	"strconv"
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
	"github.com/rzfd/file-processing-system/internal/pdf"
	"github.com/rzfd/file-processing-system/internal/processor"
	"github.com/rzfd/file-processing-system/internal/tracing"
	"github.com/rzfd/file-processing-system/internal/validator"

	"go.opentelemetry.io/otel/attribute"
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

	// Initialize Jaeger tracer
	shutdownTracer, err := tracing.InitTracer(cfg, "file-processing-worker")
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize tracer")
	}
	defer func() {
		if err := shutdownTracer(context.Background()); err != nil {
			log.Error().Err(err).Msg("Failed to shutdown tracer")
		}
	}()

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

	// Start scheduler for scheduled files
	go worker.startScheduler()

	// Start consuming messages
	err = kafkaConsumer.ConsumeMessages(worker.processFile)
	if err != nil {
		log.Fatal().Err(err).Msg("Error consuming messages")
	}
}

// startScheduler starts a background scheduler that checks for scheduled files
func (w *Worker) startScheduler() {
	// Check every 1 minute for scheduled files
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	log.Info().Msg("Scheduler started - checking scheduled files every 1 minute")

	for {
		select {
		case <-ticker.C:
			w.processScheduledFiles()
		}
	}
}

// processScheduledFiles processes files that are scheduled and ready to be processed
func (w *Worker) processScheduledFiles() {
	log.Info().Msg("Checking for scheduled files ready to process")

	// Get scheduled files that are ready (status = pending, scheduled_at <= now)
	files, err := w.db.GetScheduledFiles(100) // Process up to 100 files at a time
	if err != nil {
		log.Error().
			Err(err).
			Msg("Failed to get scheduled files")
		return
	}

	if len(files) == 0 {
		log.Debug().Msg("No scheduled files ready to process")
		return
	}

	log.Info().
		Int("count", len(files)).
		Msg("Found scheduled files ready to process")

	// Process each scheduled file
	for _, file := range files {
		// Create processing event
		event := &models.FileProcessingEvent{
			FileID:     file.ID,
			FileName:   file.FileName,
			BucketName: file.BucketName,
			ObjectName: file.ObjectName,
			EventType:  "file_scheduled",
			TraceID:    fmt.Sprintf("scheduler-%d-%d", file.ID, time.Now().UnixNano()),
		}

		// Process file in goroutine to avoid blocking
		go func(f models.FileMetadata, e *models.FileProcessingEvent) {
			log.Info().
				Int64("file_id", f.ID).
				Str("filename", f.FileName).
				Time("scheduled_at", *f.ScheduledAt).
				Msg("Processing scheduled file")

			if err := w.processFile(e); err != nil {
				log.Error().
					Int64("file_id", f.ID).
					Err(err).
					Msg("Failed to process scheduled file")
			}
		}(file, event)
	}
}

func (w *Worker) processFile(event *models.FileProcessingEvent) error {
	start := time.Now()

	// Create root span for file processing with trace context from event
	ctx := context.Background()
	ctx, span := tracing.StartSpan(ctx, "process_file")
	defer span.End()

	// Extract trace_id from event for end-to-end tracing
	traceID := event.TraceID
	if traceID == "" {
		traceID = event.RequestID // fallback to request_id
	}
	if traceID == "" {
		// Generate a new trace_id if not provided (for backward compatibility)
		traceID = fmt.Sprintf("file-%d-%d", event.FileID, time.Now().UnixNano())
	}

	// Add attributes to span
	span.SetAttributes(
		attribute.Int64("file.id", event.FileID),
		attribute.String("file.name", event.FileName),
		attribute.String("file.bucket", event.BucketName),
		attribute.String("file.object", event.ObjectName),
		attribute.String("request_id", event.RequestID),
		attribute.String("trace_id", traceID),
		attribute.String("event_type", event.EventType),
	)

	defer func() {
		duration := time.Since(start).Seconds()
		processingDuration.Observe(duration)
		span.SetAttributes(attribute.Float64("processing.duration_seconds", duration))
		log.Info().
			Str("trace_id", traceID).
			Float64("duration_seconds", duration).
			Msg("Processing completed")
	}()

	log.Info().
		Str("trace_id", traceID).
		Msg("========================================")
	log.Info().
		Str("trace_id", traceID).
		Str("filename", event.FileName).
		Int64("file_id", event.FileID).
		Str("bucket", event.BucketName).
		Str("object", event.ObjectName).
		Msg("Processing file")

	// Get file metadata to check schedule
	fileMetadata, err := w.db.GetFileMetadata(fmt.Sprintf("%d", event.FileID))
	if err != nil {
		log.Error().
			Str("trace_id", traceID).
			Err(err).
			Int64("file_id", event.FileID).
			Msg("Failed to get file metadata")
		return err
	}

	// Check if file is scheduled and if it's time to process
	if fileMetadata.ScheduleType == "scheduled" {
		if fileMetadata.ScheduledAt == nil {
			log.Warn().
				Str("trace_id", traceID).
				Int64("file_id", event.FileID).
				Msg("File is scheduled but scheduled_at is not set, skipping")
			return nil // Skip processing
		}

		// scheduled_at from database is already converted to UTC in GetFileMetadata
		// So we can directly compare with time.Now() (which is in UTC)
		now := time.Now()
		if fileMetadata.ScheduledAt.After(now) {
			log.Info().
				Str("trace_id", traceID).
				Int64("file_id", event.FileID).
				Time("scheduled_at", *fileMetadata.ScheduledAt).
				Time("now", now).
				Msg("File is scheduled but not yet time to process, skipping")
			return nil // Skip processing, will be processed later
		}

		log.Info().
			Str("trace_id", traceID).
			Int64("file_id", event.FileID).
			Time("scheduled_at", *fileMetadata.ScheduledAt).
			Time("now", now).
			Msg("File scheduled time has arrived, proceeding with processing")
	}

	// Get status codes from database
	processingStatus, err := w.db.GetStatusByCode("processing")
	if err != nil {
		log.Error().
			Str("trace_id", traceID).
			Err(err).
			Msg("Failed to get processing status")
		return err
	}

	failedStatus, err := w.db.GetStatusByCode("failed")
	if err != nil {
		log.Error().
			Str("trace_id", traceID).
			Err(err).
			Msg("Failed to get failed status")
		return err
	}

	completedStatus, err := w.db.GetStatusByCode("completed")
	if err != nil {
		log.Error().
			Str("trace_id", traceID).
			Err(err).
			Msg("Failed to get completed status")
		return err
	}

	// Get validation status codes
	validationNotStartedStatus, err := w.db.GetStatusByCode("validation_not_started")
	if err != nil {
		log.Error().
			Str("trace_id", traceID).
			Err(err).
			Msg("Failed to get validation_not_started status")
		return err
	}

	validationInProgressStatus, err := w.db.GetStatusByCode("validation_in_progress")
	if err != nil {
		log.Error().
			Str("trace_id", traceID).
			Err(err).
			Msg("Failed to get validation_in_progress status")
		return err
	}

	validationSuccessStatus, err := w.db.GetStatusByCode("validation_success")
	if err != nil {
		log.Error().
			Str("trace_id", traceID).
			Err(err).
			Msg("Failed to get validation_success status")
		return err
	}

	validationFailedStatus, err := w.db.GetStatusByCode("validation_failed")
	if err != nil {
		log.Error().
			Str("trace_id", traceID).
			Err(err).
			Msg("Failed to get validation_failed status")
		return err
	}

	// Update status to processing
	log.Info().
		Str("trace_id", traceID).
		Int64("file_id", event.FileID).
		Msg("Updating status to 'processing'")
	if err := w.db.UpdateFileStatus(event.FileID, processingStatus.Code); err != nil {
		log.Error().
			Str("trace_id", traceID).
			Err(err).
			Int64("file_id", event.FileID).
			Msg("Failed to update status to processing")
		processedFilesTotal.WithLabelValues(failedStatus.Code).Inc()
		return err
	}

	// Download file from MinIO
	log.Info().
		Str("trace_id", traceID).
		Str("object", event.ObjectName).
		Msg("Downloading file from MinIO")

	// Create child span for MinIO download
	_, downloadSpan := tracing.StartSpan(ctx, "minio_download")
	downloadSpan.SetAttributes(
		attribute.String("minio.object", event.ObjectName),
		attribute.String("minio.bucket", event.BucketName),
	)
	reader, err := w.minio.DownloadFile(ctx, event.ObjectName)
	downloadSpan.End()

	if err != nil {
		log.Error().
			Str("trace_id", traceID).
			Err(err).
			Str("object", event.ObjectName).
			Msg("Failed to download file from MinIO")
		span.RecordError(err)
		span.SetAttributes(attribute.Bool("error", true))
		w.db.UpdateFileStatus(event.FileID, failedStatus.Code)
		processedFilesTotal.WithLabelValues(failedStatus.Code).Inc()
		return err
	}
	defer reader.Close()
	log.Info().
		Str("trace_id", traceID).
		Str("object", event.ObjectName).
		Msg("File downloaded successfully")

	// Process file based on extension
	ext := strings.ToLower(filepath.Ext(event.FileName))
	log.Info().
		Str("trace_id", traceID).
		Str("extension", ext).
		Msg("Detecting file type")

	// Create child span for file parsing
	_, parseSpan := tracing.StartSpan(ctx, "parse_file")
	parseSpan.SetAttributes(
		attribute.String("file.extension", ext),
		attribute.String("file.name", event.FileName),
	)

	var result *models.ProcessingResult

	switch ext {
	case ".csv":
		log.Info().
			Str("trace_id", traceID).
			Msg("Using CSV processor")
		result, err = processor.ProcessCSVWithHeaders(reader)
	case ".xlsx", ".xls":
		log.Info().
			Str("trace_id", traceID).
			Msg("Using XLSX processor")
		result, err = processor.ProcessXLSXWithHeaders(reader)
	default:
		err = fmt.Errorf("unsupported file type: %s", ext)
		log.Error().
			Str("trace_id", traceID).
			Str("extension", ext).
			Msg("Unsupported file type")
	}
	parseSpan.End()

	if err != nil {
		log.Error().
			Str("trace_id", traceID).
			Err(err).
			Str("filename", event.FileName).
			Msg("Failed to process file")
		span.RecordError(err)
		span.SetAttributes(attribute.Bool("error", true))
		w.db.UpdateFileStatus(event.FileID, failedStatus.Code)
		processedFilesTotal.WithLabelValues(failedStatus.Code).Inc()
		return err
	}

	records := result.Records
	headers := result.Headers

	parseSpan.SetAttributes(attribute.Int("records.count", len(records)))
	span.SetAttributes(attribute.Int("records.count", len(records)))

	log.Info().
		Str("trace_id", traceID).
		Int("record_count", len(records)).
		Str("filename", event.FileName).
		Msg("Successfully parsed records")

	// Validate records
	log.Info().
		Str("trace_id", traceID).
		Int("record_count", len(records)).
		Msg("Starting record validation")

	// Create child span for validation
	_, validationSpan := tracing.StartSpan(ctx, "validate_records")
	validationSpan.SetAttributes(
		attribute.Int("validation.record_count", len(records)),
	)
	validationErrors := w.validateRecords(event.FileID, traceID, headers, records)
	validationSpan.SetAttributes(
		attribute.Int("validation.error_count", len(validationErrors)),
	)
	validationSpan.End()

	if len(validationErrors) > 0 {
		log.Error().
			Str("trace_id", traceID).
			Int("error_count", len(validationErrors)).
			Int("total_records", len(records)).
			Msg("Validation failed")
		span.SetAttributes(
			attribute.Bool("validation.failed", true),
			attribute.Int("validation.error_count", len(validationErrors)),
		)

		// Log first few errors for debugging
		for i, verr := range validationErrors {
			if i >= 5 {
				break // Only log first 5 errors
			}
			log.Warn().
				Str("trace_id", traceID).
				Int("row", verr.Row).
				Str("field", verr.Field).
				Str("value", verr.Value).
				Str("error", verr.Message).
				Msg("Validation error")
		}

		w.db.UpdateFileStatus(event.FileID, failedStatus.Code)
		processedFilesTotal.WithLabelValues(failedStatus.Code).Inc()
		return fmt.Errorf("validation failed: %d errors out of %d records", len(validationErrors), len(records))
	}

	log.Info().
		Str("trace_id", traceID).
		Int("record_count", len(records)).
		Msg("All records passed validation")

	// Create batches and items
	if len(records) > 0 {
		// Process in batches
		batchSize := w.config.BatchSize
		totalBatches := (len(records) + batchSize - 1) / batchSize
		log.Info().
			Str("trace_id", traceID).
			Int("total_records", len(records)).
			Int("total_batches", totalBatches).
			Int("batch_size", batchSize).
			Msg("Starting batch and items creation")

		// Create child span for batch processing
		_, batchSpan := tracing.StartSpan(ctx, "process_batches")
		batchSpan.SetAttributes(
			attribute.Int("batches.total_records", len(records)),
			attribute.Int("batches.total_batches", totalBatches),
			attribute.Int("batches.batch_size", batchSize),
		)
		defer batchSpan.End()

		for i := 0; i < len(records); i += batchSize {
			end := i + batchSize
			if end > len(records) {
				end = len(records)
			}

			batchNum := (i / batchSize) + 1
			batchRecords := records[i:end]

			// Create batch
			batch := &models.Batch{
				FileID:           event.FileID,
				BatchNumber:      batchNum,
				TotalItems:       len(batchRecords),
				ValidationStatus: validationNotStartedStatus.Code,
			}

			log.Info().
				Str("trace_id", traceID).
				Int("batch_num", batchNum).
				Int("total_batches", totalBatches).
				Int("batch_records", len(batchRecords)).
				Msg("Creating batch")
			if err := w.db.CreateBatch(batch); err != nil {
				log.Error().
					Str("trace_id", traceID).
					Err(err).
					Int("batch_num", batchNum).
					Msg("Failed to create batch")
				w.db.UpdateFileStatus(event.FileID, failedStatus.Code)
				processedFilesTotal.WithLabelValues(failedStatus.Code).Inc()
				return err
			}

			// Convert records to items (one item per row with name and nominal columns)
			items := make([]models.Item, 0, len(batchRecords))
			for _, record := range batchRecords {
				item := models.Item{
					BatchID:          batch.ID,
					FileID:           event.FileID,
					RowNumber:        record.RowNumber,
					ValidationStatus: validationNotStartedStatus.Code,
				}

				// Extract name and nominal from record data
				if nameVal, ok := record.Data["name"]; ok {
					if nameStr, ok := nameVal.(string); ok {
						item.Name = &nameStr
					} else {
						s := fmt.Sprintf("%v", nameVal)
						item.Name = &s
					}
				}

				if nominalVal, ok := record.Data["nominal"]; ok {
					switch v := nominalVal.(type) {
					case float64:
						item.Nominal = &v
					case int:
						f := float64(v)
						item.Nominal = &f
					case int64:
						f := float64(v)
						item.Nominal = &f
					case string:
						// Try to parse string as number
						if num, err := strconv.ParseFloat(v, 64); err == nil {
							item.Nominal = &num
						}
					}
				}

				items = append(items, item)
			}

			// Insert items
			log.Info().
				Str("trace_id", traceID).
				Int64("batch_id", batch.ID).
				Int("item_count", len(items)).
				Msg("Inserting items")
			if err := w.db.BatchInsertItems(batch.ID, event.FileID, items); err != nil {
				log.Error().
					Str("trace_id", traceID).
					Err(err).
					Int64("batch_id", batch.ID).
					Msg("Failed to insert items")
				w.db.UpdateFileStatus(event.FileID, failedStatus.Code)
				processedFilesTotal.WithLabelValues(failedStatus.Code).Inc()
				return err
			}

			// Update batch validation status to in progress
			if err := w.db.UpdateBatchValidationStatus(batch.ID, validationInProgressStatus.Code); err != nil {
				log.Warn().
					Str("trace_id", traceID).
					Err(err).
					Int64("batch_id", batch.ID).
					Msg("Failed to update batch validation status")
			}

			// Get items after insert to get their IDs
			insertedItems, err := w.db.GetItemsByBatchID(batch.ID)
			if err != nil {
				log.Warn().
					Str("trace_id", traceID).
					Int64("batch_id", batch.ID).
					Err(err).
					Msg("Failed to get inserted items, skipping validation")
			} else {
				// Validate items in batch
				w.validateBatchItems(traceID, batch.ID, insertedItems, validationInProgressStatus.Code, validationSuccessStatus.Code, validationFailedStatus.Code)
			}

			processedRecordsTotal.Add(float64(len(batchRecords)))
			log.Info().
				Str("trace_id", traceID).
				Int64("batch_id", batch.ID).
				Int("batch_num", batchNum).
				Int("total_batches", totalBatches).
				Msg("Batch and items created successfully")
		}
		log.Info().
			Str("trace_id", traceID).
			Msg("All batches and items created successfully")
	} else {
		log.Warn().
			Str("trace_id", traceID).
			Msg("No records to insert")
	}

	// Update status to completed
	log.Info().
		Str("trace_id", traceID).
		Int64("file_id", event.FileID).
		Msg("Updating status to 'completed'")
	if err := w.db.UpdateFileStatusWithProcessedAt(event.FileID, completedStatus.Code); err != nil {
		log.Error().
			Str("trace_id", traceID).
			Err(err).
			Int64("file_id", event.FileID).
			Msg("Failed to update status to completed")
		processedFilesTotal.WithLabelValues(failedStatus.Code).Inc()
		return err
	}

	// Generate PDF with watermark after status is completed
	log.Info().
		Str("trace_id", traceID).
		Int64("file_id", event.FileID).
		Msg("Generating PDF with watermark")

	// Get all items for the file
	allItems, err := w.db.GetItemsByFileID(event.FileID)
	if err != nil {
		log.Warn().
			Str("trace_id", traceID).
			Err(err).
			Int64("file_id", event.FileID).
			Msg("Failed to get items for PDF generation, continuing without PDF")
	} else if len(allItems) > 0 {
		// Generate PDF
		pdfGen := pdf.NewGenerator()
		pdfData, err := pdfGen.GeneratePDF(allItems, fileMetadata.CreatedAt, event.FileName)
		if err != nil {
			log.Warn().
				Str("trace_id", traceID).
				Err(err).
				Int64("file_id", event.FileID).
				Msg("Failed to generate PDF, continuing without PDF")
		} else {
			// Generate PDF filename
			pdfFileName := fmt.Sprintf("%d_%s.pdf", event.FileID, strings.TrimSuffix(event.FileName, filepath.Ext(event.FileName)))

			// Upload PDF to MinIO
			if err := w.minio.UploadPDF(ctx, pdfFileName, pdfData); err != nil {
				log.Warn().
					Str("trace_id", traceID).
					Err(err).
					Int64("file_id", event.FileID).
					Msg("Failed to upload PDF to MinIO, continuing without PDF")
			} else {
				// Update PDF path in database
				pdfPath := fmt.Sprintf("download/%s", pdfFileName)
				if err := w.db.UpdatePDFPath(event.FileID, pdfPath); err != nil {
					log.Warn().
						Str("trace_id", traceID).
						Err(err).
						Int64("file_id", event.FileID).
						Str("pdf_path", pdfPath).
						Msg("Failed to update PDF path in database")
				} else {
					log.Info().
						Str("trace_id", traceID).
						Int64("file_id", event.FileID).
						Str("pdf_path", pdfPath).
						Msg("PDF generated and uploaded successfully")
				}
			}
		}
	}

	log.Info().
		Str("trace_id", traceID).
		Int64("file_id", event.FileID).
		Str("filename", event.FileName).
		Int("records", len(records)).
		Str("status", completedStatus.Code).
		Msg("✅ File processed successfully")
	log.Info().
		Str("trace_id", traceID).
		Msg("========================================")

	processedFilesTotal.WithLabelValues(completedStatus.Code).Inc()
	return nil
}

// validateBatchItems validates items in a batch and updates their validation status
func (w *Worker) validateBatchItems(traceID string, batchID int64, items []models.Item, inProgressCode, successCode, failedCode string) {
	log.Info().
		Str("trace_id", traceID).
		Int64("batch_id", batchID).
		Int("item_count", len(items)).
		Msg("Starting batch items validation")

	allSuccess := true
	hasFailed := false
	successCount := 0
	failedCount := 0
	processedCount := 0

	for _, item := range items {
		// Basic validation per item: ensure name is present
		validationErrors := make(map[string]interface{})
		isValid := true
		if item.Name == nil || *item.Name == "" {
			isValid = false
			validationErrors["name"] = "Name is required"
		}
		if item.Nominal == nil || *item.Nominal <= 0 {
			isValid = false
			validationErrors["nominal"] = "Nominal must be greater than 0"
		}

		// Update item validation status
		if isValid {
			successCount++
			if err := w.db.UpdateItemValidationStatus(item.ID, successCode, nil); err != nil {
				log.Warn().
					Str("trace_id", traceID).
					Int64("item_id", item.ID).
					Err(err).
					Msg("Failed to update item validation status to success")
			}
		} else {
			hasFailed = true
			allSuccess = false
			failedCount++
			if err := w.db.UpdateItemValidationStatus(item.ID, failedCode, validationErrors); err != nil {
				log.Warn().
					Str("trace_id", traceID).
					Int64("item_id", item.ID).
					Err(err).
					Msg("Failed to update item validation status to failed")
			}
		}
		processedCount++
	}

	// Update batch validation status based on items
	batchStatus := successCode
	if hasFailed {
		batchStatus = failedCode
	}

	if err := w.db.UpdateBatchValidationStatus(batchID, batchStatus); err != nil {
		log.Warn().
			Str("trace_id", traceID).
			Int64("batch_id", batchID).
			Err(err).
			Msg("Failed to update batch validation status")
	}

	// Update batch counters
	if err := w.db.UpdateBatchCounters(batchID, processedCount, successCount, failedCount); err != nil {
		log.Warn().
			Str("trace_id", traceID).
			Int64("batch_id", batchID).
			Err(err).
			Msg("Failed to update batch counters")
	}

	log.Info().
		Str("trace_id", traceID).
		Int64("batch_id", batchID).
		Str("status", batchStatus).
		Int("processed", processedCount).
		Int("success", successCount).
		Int("failed", failedCount).
		Bool("all_success", allSuccess).
		Msg("Batch items validation completed")
}

// validateRecords validates all records using validation rules
func (w *Worker) validateRecords(fileID int64, traceID string, headers []string, records []models.ProcessedRecord) []validator.ValidationError {
	// Define validation rules
	// You can customize these rules based on your business requirements
	rules := []validator.ValidationRule{
		{
			Field:     "name",
			Required:  true,
			DataType:  "string",
			MinLength: 2,
			MaxLength: 100,
		},
		{
			Field:    "nominal",
			Required: true,
			DataType: "float",
			MinValue: 0,
		},
	}

	v := validator.NewValidator(rules)
	var allErrors []validator.ValidationError

	log.Info().
		Str("trace_id", traceID).
		Int("file_id", int(fileID)).
		Int("record_count", len(records)).
		Int("rule_count", len(rules)).
		Msg("Initializing validation")

	// 1. Validate headers
	log.Info().
		Str("trace_id", traceID).
		Msg("Validating file headers")
	headerErrors := v.ValidateHeaders(headers)
	if len(headerErrors) > 0 {
		log.Error().
			Str("trace_id", traceID).
			Int("header_error_count", len(headerErrors)).
			Msg("Header validation failed")
		allErrors = append(allErrors, headerErrors...)
		// Return early if headers are invalid
		return allErrors
	}
	log.Info().
		Str("trace_id", traceID).
		Msg("Header validation passed")

	// 2. Validate each record (field validation)
	log.Info().
		Str("trace_id", traceID).
		Msg("Validating record fields")
	for i, record := range records {
		errors := v.ValidateRecord(i+1, record.Data)
		allErrors = append(allErrors, errors...)
	}

	// 3. Check for duplicate records
	log.Info().
		Str("trace_id", traceID).
		Msg("Checking for duplicate records")

	// Extract data maps from records
	recordData := make([]map[string]interface{}, len(records))
	for i, record := range records {
		recordData[i] = record.Data
	}

	// Define key fields for duplicate detection
	// You can customize these based on your business requirements
	duplicateKeyFields := []string{"name", "nominal"}
	duplicateErrors := validator.ValidateDuplicates(recordData, duplicateKeyFields)
	allErrors = append(allErrors, duplicateErrors...)

	if len(allErrors) > 0 {
		log.Warn().
			Str("trace_id", traceID).
			Int("file_id", int(fileID)).
			Int("total_errors", len(allErrors)).
			Int("header_errors", len(headerErrors)).
			Int("duplicate_errors", len(duplicateErrors)).
			Float64("error_rate", float64(len(allErrors))/float64(len(records))*100).
			Msg("Validation completed with errors")
	} else {
		log.Info().
			Str("trace_id", traceID).
			Int("file_id", int(fileID)).
			Int("record_count", len(records)).
			Msg("All records validated successfully")
	}

	return allErrors
}
