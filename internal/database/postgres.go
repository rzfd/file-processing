package database

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	_ "github.com/lib/pq"
	"github.com/rzfd/file-processing-system/internal/config"
	"github.com/rzfd/file-processing-system/internal/models"
)

type DB struct {
	conn *sql.DB
}

// NewDB creates a new database connection
func NewDB(cfg *config.Config) (*DB, error) {
	fmt.Printf("[DATABASE] Connecting to PostgreSQL at %s:%s\n", cfg.DBHost, cfg.DBPort)
	conn, err := sql.Open("postgres", cfg.GetDSN())
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	fmt.Printf("[DATABASE] Pinging database...\n")
	if err := conn.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}
	fmt.Printf("[DATABASE] Connection established successfully\n")

	db := &DB{conn: conn}
	fmt.Printf("[DATABASE] Running migrations...\n")
	if err := db.migrate(); err != nil {
		return nil, fmt.Errorf("failed to migrate: %w", err)
	}
	fmt.Printf("[DATABASE] Migrations completed successfully\n")

	return db, nil
}

// migrate creates necessary tables
func (db *DB) migrate() error {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS file_metadata (
			id SERIAL PRIMARY KEY,
			file_name VARCHAR(255) NOT NULL,
			file_size BIGINT NOT NULL,
			content_type VARCHAR(100),
			bucket_name VARCHAR(100) NOT NULL,
			object_name VARCHAR(255) NOT NULL,
			status VARCHAR(50) NOT NULL DEFAULT 'pending',
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			processed_at TIMESTAMP
		)`,
		`CREATE TABLE IF NOT EXISTS processed_records (
			id SERIAL PRIMARY KEY,
			file_id INTEGER NOT NULL REFERENCES file_metadata(id),
			row_number INTEGER NOT NULL,
			data JSONB NOT NULL,
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE INDEX IF NOT EXISTS idx_file_metadata_status ON file_metadata(status)`,
		`CREATE INDEX IF NOT EXISTS idx_processed_records_file_id ON processed_records(file_id)`,
	}

	for _, query := range queries {
		if _, err := db.conn.Exec(query); err != nil {
			return fmt.Errorf("failed to execute migration: %w", err)
		}
	}

	return nil
}

// CreateFileMetadata inserts a new file metadata record
func (db *DB) CreateFileMetadata(file *models.FileMetadata) error {
	query := `INSERT INTO file_metadata 
		(file_name, file_size, content_type, bucket_name, object_name, status, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING id`

	err := db.conn.QueryRow(
		query,
		file.FileName,
		file.FileSize,
		file.ContentType,
		file.BucketName,
		file.ObjectName,
		file.Status,
		time.Now(),
		time.Now(),
	).Scan(&file.ID)

	if err == nil {
		fmt.Printf("[DATABASE] Created file metadata: ID=%d, Name=%s, Size=%d\n",
			file.ID, file.FileName, file.FileSize)
	}

	return err
}

// UpdateFileStatus updates file status
func (db *DB) UpdateFileStatus(fileID int64, status string) error {
	query := `UPDATE file_metadata SET status = $1, updated_at = $2 WHERE id = $3`
	_, err := db.conn.Exec(query, status, time.Now(), fileID)
	if err == nil {
		fmt.Printf("[DATABASE] Updated file status: ID=%d, Status=%s\n", fileID, status)
	}
	return err
}

// UpdateFileStatusWithProcessedAt updates file status and processed_at timestamp
func (db *DB) UpdateFileStatusWithProcessedAt(fileID int64, status string) error {
	query := `UPDATE file_metadata SET status = $1, updated_at = $2, processed_at = $2 WHERE id = $3`
	_, err := db.conn.Exec(query, status, time.Now(), fileID)
	return err
}

// BatchInsertProcessedRecords inserts multiple processed records
func (db *DB) BatchInsertProcessedRecords(fileID int64, records []models.ProcessedRecord) error {
	if len(records) == 0 {
		return nil
	}

	fmt.Printf("[DATABASE] Starting batch insert: FileID=%d, Records=%d\n", fileID, len(records))

	tx, err := db.conn.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`INSERT INTO processed_records (file_id, row_number, data, created_at) VALUES ($1, $2, $3, $4)`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, record := range records {
		// Convert map to JSON
		dataJSON, err := json.Marshal(record.Data)
		if err != nil {
			return fmt.Errorf("failed to marshal data: %w", err)
		}
		_, err = stmt.Exec(fileID, record.RowNumber, dataJSON, time.Now())
		if err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	fmt.Printf("[DATABASE] Batch insert completed: %d records inserted\n", len(records))
	return nil
}

// GetFileMetadata retrieves file metadata by ID
func (db *DB) GetFileMetadata(fileID string) (*models.FileMetadata, error) {
	query := `SELECT id, file_name, file_size, content_type, bucket_name, object_name, 
		status, created_at, updated_at, processed_at 
		FROM file_metadata WHERE id = $1`

	var file models.FileMetadata
	err := db.conn.QueryRow(query, fileID).Scan(
		&file.ID,
		&file.FileName,
		&file.FileSize,
		&file.ContentType,
		&file.BucketName,
		&file.ObjectName,
		&file.Status,
		&file.CreatedAt,
		&file.UpdatedAt,
		&file.ProcessedAt,
	)

	if err != nil {
		return nil, err
	}

	return &file, nil
}

// GetRecordCount returns the number of processed records for a file
func (db *DB) GetRecordCount(fileID int64) (int64, error) {
	query := `SELECT COUNT(*) FROM processed_records WHERE file_id = $1`

	var count int64
	err := db.conn.QueryRow(query, fileID).Scan(&count)

	return count, err
}

// ListFiles retrieves a list of files with optional status filter
func (db *DB) ListFiles(status string, limit int) ([]models.FileMetadata, error) {
	var query string
	var args []interface{}

	if status != "" {
		query = `SELECT id, file_name, file_size, content_type, bucket_name, object_name, 
			status, created_at, updated_at, processed_at 
			FROM file_metadata WHERE status = $1 
			ORDER BY created_at DESC LIMIT $2`
		args = []interface{}{status, limit}
	} else {
		query = `SELECT id, file_name, file_size, content_type, bucket_name, object_name, 
			status, created_at, updated_at, processed_at 
			FROM file_metadata 
			ORDER BY created_at DESC LIMIT $1`
		args = []interface{}{limit}
	}

	rows, err := db.conn.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var files []models.FileMetadata
	for rows.Next() {
		var file models.FileMetadata
		err := rows.Scan(
			&file.ID,
			&file.FileName,
			&file.FileSize,
			&file.ContentType,
			&file.BucketName,
			&file.ObjectName,
			&file.Status,
			&file.CreatedAt,
			&file.UpdatedAt,
			&file.ProcessedAt,
		)
		if err != nil {
			return nil, err
		}
		files = append(files, file)
	}

	return files, nil
}

// Close closes the database connection
func (db *DB) Close() error {
	return db.conn.Close()
}
