package models

import "time"

// FileMetadata represents file metadata stored in PostgreSQL
type FileMetadata struct {
	ID          int64     `json:"id" db:"id"`
	FileName    string    `json:"file_name" db:"file_name"`
	FileSize    int64     `json:"file_size" db:"file_size"`
	ContentType string    `json:"content_type" db:"content_type"`
	BucketName  string    `json:"bucket_name" db:"bucket_name"`
	ObjectName  string    `json:"object_name" db:"object_name"`
	Status      string    `json:"status" db:"status"`
	CreatedAt   time.Time `json:"created_at" db:"created_at"`
	UpdatedAt   time.Time `json:"updated_at" db:"updated_at"`
	ProcessedAt *time.Time `json:"processed_at,omitempty" db:"processed_at"`
}

// FileProcessingEvent represents Kafka event for file processing
type FileProcessingEvent struct {
	FileID     int64  `json:"file_id"`
	FileName   string `json:"file_name"`
	BucketName string `json:"bucket_name"`
	ObjectName string `json:"object_name"`
	EventType  string `json:"event_type"` // "file_uploaded"
}

// ProcessedRecord represents a processed record from CSV/XLSX
type ProcessedRecord struct {
	FileID    int64                  `json:"file_id" db:"file_id"`
	RowNumber int                    `json:"row_number" db:"row_number"`
	Data      map[string]interface{} `json:"data" db:"data"`
	CreatedAt time.Time              `json:"created_at" db:"created_at"`
}

const (
	StatusPending   = "pending"
	StatusProcessing = "processing"
	StatusCompleted  = "completed"
	StatusFailed     = "failed"
)

