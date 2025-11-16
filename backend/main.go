package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"time"

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
)

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
	// Setup logger
	logger.InitLogger()

	cfg := config.LoadConfig()

	log.Info().Msg("🚀 Starting Backend Service")

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

	// Initialize Kafka producer
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

	// Start metrics server
	go func() {
		metricsMux := http.NewServeMux()
		metricsMux.Handle("/metrics", promhttp.Handler())
		log.Info().Str("port", cfg.MetricsPort).Msg("Metrics server starting")
		if err := http.ListenAndServe(":"+cfg.MetricsPort, metricsMux); err != nil {
			log.Error().Err(err).Msg("Metrics server error")
		}
	}()

	log.Info().Str("port", cfg.ServerPort).Msg("Backend server starting")
	log.Fatal().Err(http.ListenAndServe(":"+cfg.ServerPort, server.router)).Msg("Server stopped")
}

func (s *Server) setupRoutes() {
	s.router.HandleFunc("/health", s.healthHandler).Methods("GET")
	s.router.HandleFunc("/upload", s.prometheusMiddleware(s.uploadHandler)).Methods("POST")
	s.router.HandleFunc("/files", s.prometheusMiddleware(s.listFilesHandler)).Methods("GET")
	s.router.HandleFunc("/files/{id}", s.prometheusMiddleware(s.getFileStatusHandler)).Methods("GET")
	s.router.Handle("/metrics", promhttp.Handler())
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

func (s *Server) uploadHandler(w http.ResponseWriter, r *http.Request) {
	log.Info().
		Str("remote_addr", r.RemoteAddr).
		Str("method", r.Method).
		Msg("New upload request")

	// Parse multipart form
	err := r.ParseMultipartForm(s.config.MaxFileSize)
	if err != nil {
		log.Error().
			Err(err).
			Msg("Failed to parse multipart form")
		http.Error(w, "Failed to parse multipart form", http.StatusBadRequest)
		return
	}

	file, header, err := r.FormFile("file")
	if err != nil {
		log.Error().
			Err(err).
			Msg("Failed to get file from form")
		http.Error(w, "Failed to get file from form", http.StatusBadRequest)
		return
	}
	defer file.Close()

	log.Info().
		Str("filename", header.Filename).
		Int64("size", header.Size).
		Msg("File received")

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
			Str("extension", ext).
			Str("filename", header.Filename).
			Msg("File type not allowed")
		http.Error(w, "File type not allowed", http.StatusBadRequest)
		return
	}

	// Validate file size
	if header.Size > s.config.MaxFileSize {
		log.Warn().
			Int64("file_size", header.Size).
			Int64("max_size", s.config.MaxFileSize).
			Msg("File size exceeds limit")
		http.Error(w, "File size exceeds limit", http.StatusBadRequest)
		return
	}

	// Generate unique object name
	objectName := fmt.Sprintf("%d_%s", time.Now().UnixNano(), header.Filename)
	log.Debug().
		Str("object_name", objectName).
		Msg("Generated object name")

	// Upload to MinIO
	ctx := r.Context()
	log.Info().
		Str("bucket", s.config.MinIOBucketName).
		Str("object", objectName).
		Msg("Uploading to MinIO")
	err = s.minio.UploadFile(ctx, objectName, file, header.Size, header.Header.Get("Content-Type"))
	if err != nil {
		log.Error().
			Err(err).
			Str("object", objectName).
			Msg("Failed to upload file to MinIO")
		http.Error(w, "Failed to upload file", http.StatusInternalServerError)
		return
	}
	log.Info().
		Str("object", objectName).
		Msg("Successfully uploaded to MinIO")

	// Save metadata to PostgreSQL
	fileMetadata := &models.FileMetadata{
		FileName:    header.Filename,
		FileSize:    header.Size,
		ContentType: header.Header.Get("Content-Type"),
		BucketName:  s.config.MinIOBucketName,
		ObjectName:  objectName,
		Status:      models.StatusPending,
	}

	log.Info().Msg("Saving metadata to PostgreSQL")
	err = s.db.CreateFileMetadata(fileMetadata)
	if err != nil {
		log.Error().
			Err(err).
			Str("filename", fileMetadata.FileName).
			Msg("Failed to save metadata")
		http.Error(w, "Failed to save metadata", http.StatusInternalServerError)
		return
	}
	log.Info().
		Int64("file_id", fileMetadata.ID).
		Str("filename", fileMetadata.FileName).
		Msg("Metadata saved successfully")

	// Publish event to Kafka
	event := &models.FileProcessingEvent{
		FileID:     fileMetadata.ID,
		FileName:   fileMetadata.FileName,
		BucketName: fileMetadata.BucketName,
		ObjectName: fileMetadata.ObjectName,
		EventType:  "file_uploaded",
	}

	log.Info().
		Int64("file_id", event.FileID).
		Str("filename", event.FileName).
		Msg("Publishing event to Kafka")
	err = s.kafka.PublishFileEvent(event)
	if err != nil {
		log.Error().
			Err(err).
			Int64("file_id", event.FileID).
			Msg("Failed to publish event to Kafka")
		// Don't fail the request, just log the error
	} else {
		log.Info().
			Int64("file_id", event.FileID).
			Msg("Event published successfully")
	}

	log.Info().
		Int64("file_id", fileMetadata.ID).
		Str("filename", fileMetadata.FileName).
		Str("status", fileMetadata.Status).
		Msg("File uploaded successfully")

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, `{"id": %d, "file_name": "%s", "status": "%s"}`,
		fileMetadata.ID, fileMetadata.FileName, fileMetadata.Status)
}

func (s *Server) getFileStatusHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	fileID := vars["id"]

	log.Info().
		Str("file_id", fileID).
		Msg("Get file status request")

	// Get file metadata from database
	fileMetadata, err := s.db.GetFileMetadata(fileID)
	if err != nil {
		log.Warn().
			Err(err).
			Str("file_id", fileID).
			Msg("File not found")
		http.Error(w, `{"error": "File not found"}`, http.StatusNotFound)
		return
	}

	// Get record count if completed
	var recordCount int64
	if fileMetadata.Status == models.StatusCompleted {
		recordCount, _ = s.db.GetRecordCount(fileMetadata.ID)
	}

	log.Info().
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

func (s *Server) listFilesHandler(w http.ResponseWriter, r *http.Request) {
	log.Info().Msg("List files request")

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
		Str("status", status).
		Int("limit", limit).
		Msg("Query parameters")

	// Get files from database
	files, err := s.db.ListFiles(status, limit)
	if err != nil {
		log.Error().
			Err(err).
			Msg("Failed to list files")
		http.Error(w, `{"error": "Failed to list files"}`, http.StatusInternalServerError)
		return
	}

	log.Info().
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

type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}
