package models

import "time"

// StatusType represents a status type from status_types table
type StatusType struct {
	ID           int64     `json:"id" db:"id"`
	Code         string    `json:"code" db:"code"`
	Name         string    `json:"name" db:"name"`
	Description  string    `json:"description" db:"description"`
	DisplayOrder int       `json:"display_order" db:"display_order"`
	IsActive     bool      `json:"is_active" db:"is_active"`
	CreatedAt    time.Time `json:"created_at" db:"created_at"`
	UpdatedAt    time.Time `json:"updated_at" db:"updated_at"`
}

// ScheduleType represents a schedule type from schedule_types table
type ScheduleType struct {
	ID           int64     `json:"id" db:"id"`
	Code         string    `json:"code" db:"code"`
	Name         string    `json:"name" db:"name"`
	Description  string    `json:"description" db:"description"`
	DisplayOrder int       `json:"display_order" db:"display_order"`
	IsActive     bool      `json:"is_active" db:"is_active"`
	CreatedAt    time.Time `json:"created_at" db:"created_at"`
	UpdatedAt    time.Time `json:"updated_at" db:"updated_at"`
}

// FileMetadata represents file metadata stored in PostgreSQL
type FileMetadata struct {
	ID           int64      `json:"id" db:"id"`
	FileName     string     `json:"file_name" db:"file_name"`
	FileSize     int64      `json:"file_size" db:"file_size"`
	ContentType  string     `json:"content_type" db:"content_type"`
	BucketName   string     `json:"bucket_name" db:"bucket_name"`
	ObjectName   string     `json:"object_name" db:"object_name"`
	Status       string     `json:"status" db:"status_code"` // Changed to status_code
	ScheduleType string     `json:"schedule_type" db:"schedule_type_code"`
	ScheduledAt  *time.Time `json:"scheduled_at,omitempty" db:"scheduled_at"`
	CreatedAt    time.Time  `json:"created_at" db:"created_at"`
	UpdatedAt    time.Time  `json:"updated_at" db:"updated_at"`
	ProcessedAt  *time.Time `json:"processed_at,omitempty" db:"processed_at"`
}

// FileProcessingEvent represents Kafka event for file processing
type FileProcessingEvent struct {
	FileID     int64  `json:"file_id"`
	FileName   string `json:"file_name"`
	BucketName string `json:"bucket_name"`
	ObjectName string `json:"object_name"`
	EventType  string `json:"event_type"` // "file_uploaded"
	RequestID  string `json:"request_id"` // Request ID from HTTP request
	TraceID    string `json:"trace_id"`   // Trace ID for distributed tracing (end-to-end)
}

// ProcessedRecord represents a processed record from CSV/XLSX
type ProcessedRecord struct {
	FileID    int64                  `json:"file_id" db:"file_id"`
	RowNumber int                    `json:"row_number" db:"row_number"`
	Data      map[string]interface{} `json:"data" db:"data"`
	CreatedAt time.Time              `json:"created_at" db:"created_at"`
}

// ProcessingResult contains both headers and records from file processing
type ProcessingResult struct {
	Headers []string
	Records []ProcessedRecord
}

// Batch represents a batch of items for processing
type Batch struct {
	ID               int64      `json:"id" db:"id"`
	FileID           int64      `json:"file_id" db:"file_id"`
	BatchNumber      int        `json:"batch_number" db:"batch_number"`
	TotalItems       int        `json:"total_items" db:"total_items"`
	ValidationStatus string     `json:"validation_status" db:"validation_status_code"`
	CreatedAt        time.Time  `json:"created_at" db:"created_at"`
	UpdatedAt        time.Time  `json:"updated_at" db:"updated_at"`
	ValidatedAt      *time.Time `json:"validated_at,omitempty" db:"validated_at"`
}

// Item represents an item in a batch
type Item struct {
	ID               int64                  `json:"id" db:"id"`
	BatchID          int64                  `json:"batch_id" db:"batch_id"`
	FileID           int64                  `json:"file_id" db:"file_id"`
	RowNumber        int                    `json:"row_number" db:"row_number"`
	Data             map[string]interface{} `json:"data" db:"data"`
	ValidationStatus string                 `json:"validation_status" db:"validation_status_code"`
	ValidationErrors map[string]interface{} `json:"validation_errors,omitempty" db:"validation_errors"`
	CreatedAt        time.Time              `json:"created_at" db:"created_at"`
	UpdatedAt        time.Time              `json:"updated_at" db:"updated_at"`
	ValidatedAt      *time.Time             `json:"validated_at,omitempty" db:"validated_at"`
}
