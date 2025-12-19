package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog/log"
	"github.com/rzfd/file-processing-system/internal/config"
	"github.com/rzfd/file-processing-system/internal/database"
	"github.com/rzfd/file-processing-system/internal/kafka"
	"github.com/rzfd/file-processing-system/internal/logger"
	"github.com/rzfd/file-processing-system/internal/minio"
	"github.com/rzfd/file-processing-system/internal/models"
	"github.com/rzfd/file-processing-system/internal/tracing"

	_ "github.com/rzfd/file-processing-system/docs" // swagger docs
	httpSwagger "github.com/swaggo/http-swagger"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/attribute"
)

// @title File Processing System API
// @version 1.0
// @description API for file processing system with batch and item management
// @termsOfService http://swagger.io/terms/

// @contact.name API Support
// @contact.email support@fileprocessing.com

// @license.name Apache 2.0
// @license.url http://www.apache.org/licenses/LICENSE-2.0.html

// @host localhost:8080
// @BasePath /
// @schemes http

var (
	httpRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "Total number of HTTP requests",
		},
		[]string{"method", "endpoint", "status"},
	)

	httpRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "HTTP request duration in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method", "endpoint"},
	)
)

func init() {
	prometheus.MustRegister(httpRequestsTotal)
	prometheus.MustRegister(httpRequestDuration)
}

type Server struct {
	db     *database.DB
	minio  *minio.Client
	kafka  *kafka.Producer
	config *config.Config
	router *mux.Router
}

func main() {
	logger.InitLogger()

	cfg := config.LoadConfig()

	log.Info().Msg("🚀 Starting Backend Service")

	// Initialize Jaeger tracer
	shutdownTracer, err := tracing.InitTracer(cfg, "file-processing-backend")
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize tracer")
	}
	defer func() {
		if err := shutdownTracer(context.Background()); err != nil {
			log.Error().Err(err).Msg("Failed to shutdown tracer")
		}
	}()

	db, err := database.NewDB(cfg)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize database")
	}
	defer db.Close()

	minioClient, err := minio.NewClient(cfg)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize MinIO")
	}

	kafkaProducer, err := kafka.NewProducer(cfg)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize Kafka producer")
	}
	defer kafkaProducer.Close()

	server := &Server{
		db:     db,
		minio:  minioClient,
		kafka:  kafkaProducer,
		config: cfg,
		router: mux.NewRouter(),
	}

	server.setupRoutes()

	// Wrap router with OpenTelemetry HTTP instrumentation
	otelHandler := otelhttp.NewHandler(server.router, "file-processing-backend",
		otelhttp.WithMessageEvents(otelhttp.ReadEvents, otelhttp.WriteEvents),
	)

	go func() {
		metricsMux := http.NewServeMux()
		metricsMux.Handle("/metrics", promhttp.Handler())
		log.Info().Str("port", cfg.MetricsPort).Msg("Metrics server starting")
		if err := http.ListenAndServe(":"+cfg.MetricsPort, metricsMux); err != nil {
			log.Error().Err(err).Msg("Metrics server error")
		}
	}()

	log.Info().Str("port", cfg.ServerPort).Msg("Backend server starting")
	log.Fatal().Err(http.ListenAndServe(":"+cfg.ServerPort, otelHandler)).Msg("Server stopped")
}

func (s *Server) setupRoutes() {
	s.router.HandleFunc("/health", s.healthHandler).Methods("GET")
	s.router.HandleFunc("/upload", s.requestIDMiddleware(s.prometheusMiddleware(s.uploadHandler))).Methods("POST")
	s.router.HandleFunc("/files", s.requestIDMiddleware(s.prometheusMiddleware(s.listFilesHandler))).Methods("GET")
	s.router.HandleFunc("/files/{id}", s.requestIDMiddleware(s.prometheusMiddleware(s.getFileStatusHandler))).Methods("GET")
	s.router.HandleFunc("/files/{id}/download", s.requestIDMiddleware(s.prometheusMiddleware(s.downloadPDFHandler))).Methods("GET")
	s.router.HandleFunc("/scheduled", s.requestIDMiddleware(s.prometheusMiddleware(s.listScheduledFilesHandler))).Methods("GET")
	s.router.Handle("/metrics", promhttp.Handler())

	// Swagger documentation
	s.router.PathPrefix("/swagger/").Handler(httpSwagger.WrapHandler)
}

// requestIDMiddleware adds request_id to context and response header
func (s *Server) requestIDMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Generate or get request ID from header
		requestID := r.Header.Get("X-Request-ID")
		if requestID == "" {
			requestID = uuid.New().String()
		}

		// Set to response header for client tracking
		w.Header().Set("X-Request-ID", requestID)

		// Add to context for downstream usage
		ctx := context.WithValue(r.Context(), "request_id", requestID)
		next(w, r.WithContext(ctx))
	}
}

func (s *Server) healthHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

func (s *Server) prometheusMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		ww := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}

		next(ww, r)

		duration := time.Since(start).Seconds()
		httpRequestDuration.WithLabelValues(r.Method, r.URL.Path).Observe(duration)
		httpRequestsTotal.WithLabelValues(r.Method, r.URL.Path, fmt.Sprintf("%d", ww.statusCode)).Inc()
	}
}

// uploadHandler handles file upload requests
// @Summary Upload a file
// @Description Upload a CSV or XLSX file for processing. Can be processed immediately or scheduled for later.
// @Tags files
// @Accept multipart/form-data
// @Produce json
// @Param file formData file true "File to upload"
// @Param schedule_type formData string false "Schedule type: immediate or scheduled" default(immediate) Enums(immediate, scheduled)
// @Param scheduled_at formData string false "Scheduled time in RFC3339 format (e.g., 2025-12-05T15:00:00Z). Required if schedule_type=scheduled, must be in the future"
// @Success 200 {object} map[string]interface{} "id, file_name, status, schedule_type, scheduled_at"
// @Failure 400 {string} string "Bad request"
// @Failure 409 {string} string "File with this name already exists"
// @Failure 500 {string} string "Internal server error"
// @Router /upload [post]
func (s *Server) uploadHandler(w http.ResponseWriter, r *http.Request) {
	// Start tracing span for upload operation
	ctx, span := tracing.StartSpan(r.Context(), "upload_file")
	defer span.End()

	// Get request ID from context
	requestID, _ := r.Context().Value("request_id").(string)

	// Add tracing attributes
	traceID := tracing.TraceIDFromContext(ctx)
	span.SetAttributes(
		attribute.String("request_id", requestID),
		attribute.String("trace_id", traceID),
		attribute.String("http.method", r.Method),
		attribute.String("http.path", r.URL.Path),
		attribute.String("remote_addr", r.RemoteAddr),
	)

	log.Info().
		Str("request_id", requestID).
		Str("trace_id", traceID).
		Str("remote_addr", r.RemoteAddr).
		Str("method", r.Method).
		Msg("New upload request")

	// Parse multipart form
	err := r.ParseMultipartForm(s.config.MaxFileSize)
	if err != nil {
		log.Error().
			Str("request_id", requestID).
			Err(err).
			Msg("Failed to parse multipart form")
		http.Error(w, "Failed to parse multipart form", http.StatusBadRequest)
		return
	}

	file, header, err := r.FormFile("file")
	if err != nil {
		log.Error().
			Str("request_id", requestID).
			Err(err).
			Msg("Failed to get file from form")
		http.Error(w, "Failed to get file from form", http.StatusBadRequest)
		return
	}
	defer file.Close()

	log.Info().
		Str("request_id", requestID).
		Str("filename", header.Filename).
		Int64("size", header.Size).
		Msg("File received")

	// Add file attributes to span
	span.SetAttributes(
		attribute.String("file.name", header.Filename),
		attribute.Int64("file.size", header.Size),
	)

	// Validate file extension
	ext := strings.ToLower(filepath.Ext(header.Filename))
	allowed := false
	for _, allowedExt := range s.config.AllowedExtensions {
		if ext == strings.ToLower(allowedExt) {
			allowed = true
			break
		}
	}

	if !allowed {
		log.Warn().
			Str("request_id", requestID).
			Str("extension", ext).
			Str("filename", header.Filename).
			Msg("File type not allowed")
		http.Error(w, "File type not allowed", http.StatusBadRequest)
		return
	}

	// Validate file size
	if header.Size > s.config.MaxFileSize {
		log.Warn().
			Str("request_id", requestID).
			Int64("file_size", header.Size).
			Int64("max_size", s.config.MaxFileSize).
			Msg("File size exceeds limit")
		http.Error(w, "File size exceeds limit", http.StatusBadRequest)
		return
	}

	// Validate duplicate filename
	isDuplicate, err := s.db.CheckDuplicateFilename(header.Filename)
	if err != nil {
		log.Error().
			Str("request_id", requestID).
			Err(err).
			Str("filename", header.Filename).
			Msg("Failed to check duplicate filename")
		http.Error(w, "Failed to validate filename", http.StatusInternalServerError)
		return
	}
	if isDuplicate {
		log.Warn().
			Str("request_id", requestID).
			Str("filename", header.Filename).
			Msg("Duplicate filename detected")
		http.Error(w, "File with this name already exists", http.StatusConflict)
		return
	}

	// Generate unique object name
	objectName := fmt.Sprintf("%d_%s", time.Now().UnixNano(), header.Filename)
	log.Debug().
		Str("request_id", requestID).
		Str("object_name", objectName).
		Msg("Generated object name")

	// Upload to MinIO
	log.Info().
		Str("request_id", requestID).
		Str("bucket", s.config.MinIOBucketName).
		Str("object", objectName).
		Msg("Uploading to MinIO")

	// Create child span for MinIO upload
	_, minioSpan := tracing.StartSpan(ctx, "minio_upload")
	minioSpan.SetAttributes(
		attribute.String("minio.object", objectName),
		attribute.String("minio.bucket", s.config.MinIOBucketName),
		attribute.Int64("minio.size", header.Size),
	)
	err = s.minio.UploadFile(ctx, objectName, file, header.Size, header.Header.Get("Content-Type"))
	minioSpan.End()

	if err != nil {
		log.Error().
			Str("request_id", requestID).
			Err(err).
			Str("object", objectName).
			Msg("Failed to upload file to MinIO")
		span.RecordError(err)
		span.SetAttributes(attribute.Bool("error", true))
		http.Error(w, "Failed to upload file", http.StatusInternalServerError)
		return
	}
	log.Info().
		Str("request_id", requestID).
		Str("object", objectName).
		Msg("Successfully uploaded to MinIO")

	// Get schedule parameters
	scheduleType := r.FormValue("schedule_type")
	if scheduleType == "" {
		scheduleType = "immediate" // default
	}

	// Validate schedule_type
	if scheduleType != "immediate" && scheduleType != "scheduled" {
		log.Warn().
			Str("request_id", requestID).
			Str("schedule_type", scheduleType).
			Msg("Invalid schedule_type")
		http.Error(w, "Invalid schedule_type. Must be 'immediate' or 'scheduled'", http.StatusBadRequest)
		return
	}

	var scheduledAt *time.Time
	if scheduleType == "scheduled" {
		scheduledAtStr := r.FormValue("scheduled_at")
		if scheduledAtStr == "" {
			log.Warn().
				Str("request_id", requestID).
				Msg("scheduled_at is required when schedule_type is 'scheduled'")
			http.Error(w, "scheduled_at is required when schedule_type is 'scheduled'", http.StatusBadRequest)
			return
		}
		parsedTime, err := time.Parse(time.RFC3339, scheduledAtStr)
		if err != nil {
			log.Warn().
				Str("request_id", requestID).
				Str("scheduled_at", scheduledAtStr).
				Err(err).
				Msg("Invalid scheduled_at format")
			http.Error(w, "Invalid scheduled_at format. Use RFC3339 (e.g., 2025-12-05T15:00:00Z)", http.StatusBadRequest)
			return
		}
		// Validate that scheduled_at is in the future
		if parsedTime.Before(time.Now()) {
			log.Warn().
				Str("request_id", requestID).
				Time("scheduled_at", parsedTime).
				Msg("scheduled_at must be in the future")
			http.Error(w, "scheduled_at must be in the future", http.StatusBadRequest)
			return
		}
		scheduledAt = &parsedTime
	}

	// Get pending status code from database
	pendingStatus, err := s.db.GetStatusByCode("pending")
	if err != nil {
		log.Error().
			Str("request_id", requestID).
			Err(err).
			Msg("Failed to get pending status")
		http.Error(w, "Failed to get status", http.StatusInternalServerError)
		return
	}

	// Save metadata to PostgreSQL
	fileMetadata := &models.FileMetadata{
		FileName:     header.Filename,
		FileSize:     header.Size,
		ContentType:  header.Header.Get("Content-Type"),
		BucketName:   s.config.MinIOBucketName,
		ObjectName:   objectName,
		Status:       pendingStatus.Code,
		ScheduleType: scheduleType,
		ScheduledAt:  scheduledAt,
	}

	log.Info().
		Str("request_id", requestID).
		Msg("Saving metadata to PostgreSQL")
	err = s.db.CreateFileMetadata(fileMetadata)
	if err != nil {
		log.Error().
			Str("request_id", requestID).
			Err(err).
			Str("filename", fileMetadata.FileName).
			Msg("Failed to save metadata")
		http.Error(w, "Failed to save metadata", http.StatusInternalServerError)
		return
	}
	log.Info().
		Str("request_id", requestID).
		Int64("file_id", fileMetadata.ID).
		Str("filename", fileMetadata.FileName).
		Msg("Metadata saved successfully")

	// Get traceID from span context for distributed tracing
	traceID = tracing.TraceIDFromContext(ctx)
	if traceID == "" {
		traceID = requestID // fallback to request_id
	}

	// Publish event to Kafka with trace information
	event := &models.FileProcessingEvent{
		FileID:     fileMetadata.ID,
		FileName:   fileMetadata.FileName,
		BucketName: fileMetadata.BucketName,
		ObjectName: fileMetadata.ObjectName,
		EventType:  "file_uploaded",
		RequestID:  requestID,
		TraceID:    traceID, // Use OpenTelemetry trace_id for distributed tracing
	}

	log.Info().
		Str("request_id", requestID).
		Str("trace_id", traceID).
		Int64("file_id", event.FileID).
		Str("filename", event.FileName).
		Msg("Publishing event to Kafka")

	// Create child span for Kafka publish
	_, kafkaSpan := tracing.StartSpan(ctx, "kafka_publish")
	kafkaSpan.SetAttributes(
		attribute.String("kafka.topic", s.config.KafkaTopic),
		attribute.Int64("kafka.file_id", event.FileID),
		attribute.String("kafka.event_type", event.EventType),
	)
	err = s.kafka.PublishFileEvent(event)
	kafkaSpan.End()

	if err != nil {
		log.Error().
			Str("request_id", requestID).
			Err(err).
			Int64("file_id", event.FileID).
			Msg("Failed to publish event to Kafka")
		span.RecordError(err)
		// Don't fail the request, just log the error
	} else {
		log.Info().
			Str("request_id", requestID).
			Int64("file_id", event.FileID).
			Msg("Event published successfully")
	}

	log.Info().
		Str("request_id", requestID).
		Int64("file_id", fileMetadata.ID).
		Str("filename", fileMetadata.FileName).
		Str("status", fileMetadata.Status).
		Msg("File uploaded successfully")

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	// Build response with schedule info
	response := map[string]interface{}{
		"id":            fileMetadata.ID,
		"file_name":     fileMetadata.FileName,
		"status":        fileMetadata.Status,
		"schedule_type": fileMetadata.ScheduleType,
	}
	if fileMetadata.ScheduledAt != nil {
		response["scheduled_at"] = fileMetadata.ScheduledAt.Format(time.RFC3339)
	}
	json.NewEncoder(w).Encode(response)
}

// getFileStatusHandler handles get file status requests
// @Summary Get file status
// @Description Get the processing status and details of a file
// @Tags files
// @Produce json
// @Param id path string true "File ID"
// @Success 200 {object} models.FileMetadata
// @Failure 404 {string} string "File not found"
// @Failure 500 {string} string "Internal server error"
// @Router /files/{id} [get]
func (s *Server) getFileStatusHandler(w http.ResponseWriter, r *http.Request) {
	requestID, _ := r.Context().Value("request_id").(string)
	vars := mux.Vars(r)
	fileID := vars["id"]

	log.Info().
		Str("request_id", requestID).
		Str("file_id", fileID).
		Msg("Get file status request")

	// Get file metadata from database
	fileMetadata, err := s.db.GetFileMetadata(fileID)
	if err != nil {
		log.Warn().
			Str("request_id", requestID).
			Err(err).
			Str("file_id", fileID).
			Msg("File not found")
		http.Error(w, `{"error": "File not found"}`, http.StatusNotFound)
		return
	}

	// Get completed status code
	completedStatus, err := s.db.GetStatusByCode("completed")
	if err != nil {
		log.Error().
			Str("request_id", requestID).
			Err(err).
			Msg("Failed to get completed status")
		http.Error(w, "Failed to get status", http.StatusInternalServerError)
		return
	}

	// Get record count if completed
	var recordCount int64
	if fileMetadata.Status == completedStatus.Code {
		recordCount, _ = s.db.GetRecordCount(fileMetadata.ID)
	}

	log.Info().
		Str("request_id", requestID).
		Int64("id", fileMetadata.ID).
		Str("status", fileMetadata.Status).
		Int64("records", recordCount).
		Msg("File status retrieved")

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	response := map[string]interface{}{
		"id":           fileMetadata.ID,
		"file_name":    fileMetadata.FileName,
		"file_size":    fileMetadata.FileSize,
		"content_type": fileMetadata.ContentType,
		"status":       fileMetadata.Status,
		"created_at":   fileMetadata.CreatedAt,
		"updated_at":   fileMetadata.UpdatedAt,
	}

	if fileMetadata.ProcessedAt != nil {
		response["processed_at"] = fileMetadata.ProcessedAt
	}

	if recordCount > 0 {
		response["record_count"] = recordCount
	}

	json.NewEncoder(w).Encode(response)
}

// listFilesHandler handles list files requests
// @Summary List all files
// @Description Get a list of all uploaded files
// @Tags files
// @Produce json
// @Success 200 {object} map[string]interface{} "files, count"
// @Failure 500 {string} string "Internal server error"
// @Router /files [get]
func (s *Server) listFilesHandler(w http.ResponseWriter, r *http.Request) {
	requestID, _ := r.Context().Value("request_id").(string)

	log.Info().
		Str("request_id", requestID).
		Msg("List files request")

	// Get query parameters
	status := r.URL.Query().Get("status")
	limitStr := r.URL.Query().Get("limit")

	limit := 10
	if limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 && l <= 100 {
			limit = l
		}
	}

	log.Info().
		Str("request_id", requestID).
		Str("status", status).
		Int("limit", limit).
		Msg("Query parameters")

	// Get files from database
	files, err := s.db.ListFiles(status, limit)
	if err != nil {
		log.Error().
			Str("request_id", requestID).
			Err(err).
			Msg("Failed to list files")
		http.Error(w, `{"error": "Failed to list files"}`, http.StatusInternalServerError)
		return
	}

	log.Info().
		Str("request_id", requestID).
		Int("count", len(files)).
		Msg("Files retrieved")

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	response := map[string]interface{}{
		"files": files,
		"count": len(files),
	}

	json.NewEncoder(w).Encode(response)
}

// listScheduledFilesHandler handles GET /scheduled
// @Summary List scheduled files
// @Description Get a list of files that are scheduled for processing
// @Tags files
// @Produce json
// @Param limit query int false "Limit number of results" default(50)
// @Success 200 {object} map[string]interface{} "files, count"
// @Failure 500 {string} string "Internal server error"
// @Router /scheduled [get]
func (s *Server) listScheduledFilesHandler(w http.ResponseWriter, r *http.Request) {
	requestID := r.Context().Value("request_id").(string)

	// Get query parameters
	limitStr := r.URL.Query().Get("limit")
	limit := 50 // default limit
	if limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 {
			limit = l
		}
	}

	log.Info().
		Str("request_id", requestID).
		Int("limit", limit).
		Msg("List scheduled files request")

	// Get scheduled files
	files, err := s.db.GetScheduledFiles(limit)
	if err != nil {
		log.Error().
			Str("request_id", requestID).
			Err(err).
			Msg("Failed to get scheduled files")
		http.Error(w, `{"error": "Failed to get scheduled files"}`, http.StatusInternalServerError)
		return
	}

	response := map[string]interface{}{
		"count": len(files),
		"files": files,
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Request-ID", requestID)
	json.NewEncoder(w).Encode(response)

	log.Info().
		Str("request_id", requestID).
		Int("count", len(files)).
		Msg("List scheduled files completed")
}

// downloadPDFHandler handles PDF download requests
// @Summary Download PDF file
// @Description Download the generated PDF file for a completed file
// @Tags files
// @Produce application/pdf
// @Param id path string true "File ID"
// @Success 200 {file} file "PDF file"
// @Failure 404 {string} string "File not found or PDF not available"
// @Failure 500 {string} string "Internal server error"
// @Router /files/{id}/download [get]
func (s *Server) downloadPDFHandler(w http.ResponseWriter, r *http.Request) {
	requestID, _ := r.Context().Value("request_id").(string)
	vars := mux.Vars(r)
	fileID := vars["id"]

	log.Info().
		Str("request_id", requestID).
		Str("file_id", fileID).
		Msg("Download PDF request")

	// Get file metadata from database
	fileMetadata, err := s.db.GetFileMetadata(fileID)
	if err != nil {
		log.Warn().
			Str("request_id", requestID).
			Err(err).
			Str("file_id", fileID).
			Msg("File not found")
		http.Error(w, `{"error": "File not found"}`, http.StatusNotFound)
		return
	}

	// Check if file is completed
	completedStatus, err := s.db.GetStatusByCode("completed")
	if err != nil {
		log.Error().
			Str("request_id", requestID).
			Err(err).
			Msg("Failed to get completed status")
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	if fileMetadata.Status != completedStatus.Code {
		log.Warn().
			Str("request_id", requestID).
			Str("file_id", fileID).
			Str("status", fileMetadata.Status).
			Msg("File is not completed yet")
		http.Error(w, `{"error": "File is not completed yet. PDF is only available for completed files."}`, http.StatusBadRequest)
		return
	}

	// Check if PDF path exists
	if fileMetadata.PDFPath == nil || *fileMetadata.PDFPath == "" {
		log.Warn().
			Str("request_id", requestID).
			Str("file_id", fileID).
			Msg("PDF not available for this file")
		http.Error(w, `{"error": "PDF not available for this file"}`, http.StatusNotFound)
		return
	}

	// Download PDF from MinIO
	ctx := context.Background()
	pdfReader, err := s.minio.DownloadFile(ctx, *fileMetadata.PDFPath)
	if err != nil {
		log.Error().
			Str("request_id", requestID).
			Err(err).
			Str("file_id", fileID).
			Str("pdf_path", *fileMetadata.PDFPath).
			Msg("Failed to download PDF from MinIO")
		http.Error(w, "Failed to download PDF", http.StatusInternalServerError)
		return
	}
	defer pdfReader.Close()

	// Set response headers
	w.Header().Set("Content-Type", "application/pdf")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s.pdf"`, fileMetadata.FileName))

	// Copy PDF data to response
	_, err = io.Copy(w, pdfReader)
	if err != nil {
		log.Error().
			Str("request_id", requestID).
			Err(err).
			Str("file_id", fileID).
			Msg("Failed to stream PDF to client")
		return
	}

	log.Info().
		Str("request_id", requestID).
		Str("file_id", fileID).
		Str("pdf_path", *fileMetadata.PDFPath).
		Msg("PDF downloaded successfully")
}

type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}
