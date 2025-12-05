package database

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	_ "github.com/lib/pq"
	"github.com/rs/zerolog/log"
	"github.com/rzfd/file-processing-system/internal/config"
	"github.com/rzfd/file-processing-system/internal/models"
)

type DB struct {
	conn          *sql.DB
	statusCache   map[string]*models.StatusType
	scheduleCache map[string]*models.ScheduleType
	cacheExpiry   time.Time
	cacheDuration time.Duration
}

// NewDB creates a new database connection
func NewDB(cfg *config.Config) (*DB, error) {
	log.Info().
		Str("host", cfg.DBHost).
		Str("port", cfg.DBPort).
		Msg("Connecting to PostgreSQL")

	conn, err := sql.Open("postgres", cfg.GetDSN())
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	log.Info().Msg("Pinging database")
	if err := conn.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}
	log.Info().Msg("Database connection established successfully")

	// Set timezone for this connection (for existing databases)
	if _, err := conn.Exec("SET timezone = 'Asia/Jakarta'"); err != nil {
		log.Warn().Err(err).Msg("Failed to set timezone, using database default")
	}

	db := &DB{
		conn:          conn,
		statusCache:   make(map[string]*models.StatusType),
		scheduleCache: make(map[string]*models.ScheduleType),
		cacheDuration: 5 * time.Minute,
	}
	log.Info().Msg("Running database migrations")
	if err := db.migrate(); err != nil {
		return nil, fmt.Errorf("failed to migrate: %w", err)
	}
	log.Info().Msg("Database migrations completed successfully")

	// Load status types into cache
	if err := db.loadStatusCache(); err != nil {
		return nil, fmt.Errorf("failed to load status cache: %w", err)
	}
	log.Info().Msg("Status types loaded into cache")

	// Load schedule types into cache
	if err := db.loadScheduleCache(); err != nil {
		return nil, fmt.Errorf("failed to load schedule cache: %w", err)
	}
	log.Info().Msg("Schedule types loaded into cache")

	return db, nil
}

// migrate creates necessary tables
func (db *DB) migrate() error {
	queries := []string{
		// Create status_types table first
		`CREATE TABLE IF NOT EXISTS status_types (
			id SERIAL PRIMARY KEY,
			code VARCHAR(50) NOT NULL UNIQUE,
			name VARCHAR(100) NOT NULL,
			description TEXT,
			display_order INTEGER NOT NULL DEFAULT 0,
			is_active BOOLEAN NOT NULL DEFAULT true,
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
		)`,
		// Insert default status types (file processing status)
		`INSERT INTO status_types (code, name, description, display_order) 
		VALUES 
			('pending', 'Pending', 'File is waiting to be processed', 1),
			('processing', 'Processing', 'File is currently being processed', 2),
			('completed', 'Completed', 'File has been successfully processed', 3),
			('failed', 'Failed', 'File processing has failed', 4)
		ON CONFLICT (code) DO NOTHING`,
		// Insert validation status types
		`INSERT INTO status_types (code, name, description, display_order) 
		VALUES 
			('validation_not_started', 'Not Validated', 'Validation has not started', 10),
			('validation_in_progress', 'Validating', 'Validation is in progress', 11),
			('validation_success', 'Validated Success', 'Validation passed successfully', 12),
			('validation_failed', 'Validated Failed', 'Validation failed', 13)
		ON CONFLICT (code) DO NOTHING`,
		// Create schedule_types table
		`CREATE TABLE IF NOT EXISTS schedule_types (
			id SERIAL PRIMARY KEY,
			code VARCHAR(50) NOT NULL UNIQUE,
			name VARCHAR(100) NOT NULL,
			description TEXT,
			display_order INTEGER NOT NULL DEFAULT 0,
			is_active BOOLEAN NOT NULL DEFAULT true,
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
		)`,
		// Insert default schedule types
		`INSERT INTO schedule_types (code, name, description, display_order) 
		VALUES 
			('immediate', 'Immediate', 'File will be processed immediately', 1),
			('scheduled', 'Scheduled', 'File will be processed according to schedule', 2)
		ON CONFLICT (code) DO NOTHING`,
		// Create file_metadata table with foreign key to status_types and schedule_types
		`CREATE TABLE IF NOT EXISTS file_metadata (
			id SERIAL PRIMARY KEY,
			file_name VARCHAR(255) NOT NULL,
			file_size BIGINT NOT NULL,
			content_type VARCHAR(100),
			bucket_name VARCHAR(100) NOT NULL,
			object_name VARCHAR(255) NOT NULL,
			status_code VARCHAR(50) NOT NULL DEFAULT 'pending',
			schedule_type_code VARCHAR(50) NOT NULL DEFAULT 'immediate',
			scheduled_at TIMESTAMP,
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			processed_at TIMESTAMP,
			FOREIGN KEY (status_code) REFERENCES status_types(code),
			FOREIGN KEY (schedule_type_code) REFERENCES schedule_types(code)
		)`,
		`CREATE TABLE IF NOT EXISTS processed_records (
			id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
			file_id BIGINT NOT NULL REFERENCES file_metadata(id) ON DELETE CASCADE,
			row_number INTEGER NOT NULL CHECK (row_number > 0),
			data JSONB NOT NULL,
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			UNIQUE(file_id, row_number)
		)`,
		// Create batches table
		`CREATE TABLE IF NOT EXISTS batches (
			id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
			file_id BIGINT NOT NULL REFERENCES file_metadata(id) ON DELETE CASCADE,
			batch_number INTEGER NOT NULL,
			total_items INTEGER NOT NULL DEFAULT 0,
			items_processed INTEGER NOT NULL DEFAULT 0,
			items_success INTEGER NOT NULL DEFAULT 0,
			items_failed INTEGER NOT NULL DEFAULT 0,
			validation_status_code VARCHAR(50) NOT NULL DEFAULT 'validation_not_started',
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			validated_at TIMESTAMP,
			UNIQUE(file_id, batch_number),
			FOREIGN KEY (validation_status_code) REFERENCES status_types(code)
		)`,
		// Ensure counter columns exist for existing databases
		`ALTER TABLE batches ADD COLUMN IF NOT EXISTS items_processed INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE batches ADD COLUMN IF NOT EXISTS items_success INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE batches ADD COLUMN IF NOT EXISTS items_failed INTEGER NOT NULL DEFAULT 0`,
		// Create items table (with fixed columns: name, nominal)
		`CREATE TABLE IF NOT EXISTS items (
			id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
			batch_id BIGINT NOT NULL REFERENCES batches(id) ON DELETE CASCADE,
			file_id BIGINT NOT NULL REFERENCES file_metadata(id) ON DELETE CASCADE,
			row_number INTEGER NOT NULL CHECK (row_number > 0),
			name TEXT,
			nominal NUMERIC,
			validation_status_code VARCHAR(50) NOT NULL DEFAULT 'validation_not_started',
			validation_errors JSONB,
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			validated_at TIMESTAMP,
			UNIQUE(batch_id, row_number),
			FOREIGN KEY (validation_status_code) REFERENCES status_types(code)
		)`,
		// Drop legacy columns if exist
		`ALTER TABLE items DROP COLUMN IF EXISTS data`,
		`ALTER TABLE items DROP COLUMN IF EXISTS column_name`,
		`ALTER TABLE items DROP COLUMN IF EXISTS string_value`,
		`ALTER TABLE items DROP COLUMN IF EXISTS number_value`,
		`ALTER TABLE items DROP COLUMN IF EXISTS boolean_value`,
		`ALTER TABLE items DROP COLUMN IF EXISTS date_value`,
		`CREATE INDEX IF NOT EXISTS idx_file_metadata_status_code ON file_metadata(status_code)`,
		`CREATE INDEX IF NOT EXISTS idx_file_metadata_schedule_type ON file_metadata(schedule_type_code)`,
		`CREATE INDEX IF NOT EXISTS idx_file_metadata_scheduled_at ON file_metadata(scheduled_at) WHERE scheduled_at IS NOT NULL`,
		`CREATE INDEX IF NOT EXISTS idx_processed_records_file_id ON processed_records(file_id)`,
		`CREATE INDEX IF NOT EXISTS idx_file_metadata_file_name ON file_metadata(file_name)`,
		`CREATE INDEX IF NOT EXISTS idx_file_metadata_created_at ON file_metadata(created_at DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_processed_records_data_gin ON processed_records USING GIN (data)`,
		// Indexes for batches
		`CREATE INDEX IF NOT EXISTS idx_batches_file_id ON batches(file_id)`,
		`CREATE INDEX IF NOT EXISTS idx_batches_validation_status ON batches(validation_status_code)`,
		`CREATE INDEX IF NOT EXISTS idx_batches_items_processed ON batches(items_processed)`,
		// Indexes for items
		`CREATE INDEX IF NOT EXISTS idx_items_batch_id ON items(batch_id)`,
		`CREATE INDEX IF NOT EXISTS idx_items_file_id ON items(file_id)`,
		`CREATE INDEX IF NOT EXISTS idx_items_validation_status ON items(validation_status_code)`,
		`CREATE INDEX IF NOT EXISTS idx_items_name ON items(name)`,
		`CREATE INDEX IF NOT EXISTS idx_items_nominal ON items(nominal)`,
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
	// Set default schedule type if not provided
	if file.ScheduleType == "" {
		file.ScheduleType = "immediate"
	}

	query := `INSERT INTO file_metadata 
		(file_name, file_size, content_type, bucket_name, object_name, status_code, schedule_type_code, scheduled_at, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) RETURNING id`

	err := db.conn.QueryRow(
		query,
		file.FileName,
		file.FileSize,
		file.ContentType,
		file.BucketName,
		file.ObjectName,
		file.Status,
		file.ScheduleType,
		file.ScheduledAt,
		time.Now(),
		time.Now(),
	).Scan(&file.ID)

	if err == nil {
		log.Info().
			Int64("file_id", file.ID).
			Str("filename", file.FileName).
			Int64("size", file.FileSize).
			Str("schedule_type", file.ScheduleType).
			Msg("Created file metadata")
	}

	return err
}

// UpdateFileStatus updates file status
func (db *DB) UpdateFileStatus(fileID int64, status string) error {
	query := `UPDATE file_metadata SET status_code = $1, updated_at = $2 WHERE id = $3`
	_, err := db.conn.Exec(query, status, time.Now(), fileID)
	if err == nil {
		log.Info().
			Int64("file_id", fileID).
			Str("status", status).
			Msg("Updated file status")
	}
	return err
}

// UpdateFileStatusWithProcessedAt updates file status and processed_at timestamp
func (db *DB) UpdateFileStatusWithProcessedAt(fileID int64, status string) error {
	query := `UPDATE file_metadata SET status_code = $1, updated_at = $2, processed_at = $2 WHERE id = $3`
	_, err := db.conn.Exec(query, status, time.Now(), fileID)
	return err
}

// BatchInsertProcessedRecords inserts multiple processed records
func (db *DB) BatchInsertProcessedRecords(fileID int64, records []models.ProcessedRecord) error {
	if len(records) == 0 {
		return nil
	}

	log.Info().
		Int64("file_id", fileID).
		Int("record_count", len(records)).
		Msg("Starting batch insert")

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

	log.Info().
		Int64("file_id", fileID).
		Int("records_inserted", len(records)).
		Msg("Batch insert completed")
	return nil
}

// GetFileMetadata retrieves file metadata by ID
func (db *DB) GetFileMetadata(fileID string) (*models.FileMetadata, error) {
	// Convert scheduled_at from Asia/Jakarta to UTC for proper comparison
	// scheduled_at is stored as timestamp without timezone, interpreted as Asia/Jakarta
	query := `SELECT id, file_name, file_size, content_type, bucket_name, object_name, 
		status_code, schedule_type_code, 
		COALESCE((scheduled_at AT TIME ZONE 'Asia/Jakarta' AT TIME ZONE 'UTC')::timestamp, NULL), 
		created_at, updated_at, processed_at 
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
		&file.ScheduleType,
		&file.ScheduledAt,
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

// UpdateBatchCounters updates processed/success/failed counters for a batch
func (db *DB) UpdateBatchCounters(batchID int64, processed, success, failed int) error {
	query := `UPDATE batches 
		SET items_processed = $1, items_success = $2, items_failed = $3, updated_at = $4
		WHERE id = $5`
	_, err := db.conn.Exec(query, processed, success, failed, time.Now(), batchID)
	return err
}

// ListFiles retrieves a list of files with optional status filter
func (db *DB) ListFiles(status string, limit int) ([]models.FileMetadata, error) {
	var query string
	var args []interface{}

	if status != "" {
		query = `SELECT id, file_name, file_size, content_type, bucket_name, object_name, 
			status_code, created_at, updated_at, processed_at 
			FROM file_metadata WHERE status_code = $1 
			ORDER BY created_at DESC LIMIT $2`
		args = []interface{}{status, limit}
	} else {
		query = `SELECT id, file_name, file_size, content_type, bucket_name, object_name, 
			status_code, created_at, updated_at, processed_at 
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

// CheckDuplicateFilename checks if a filename already exists in the database
func (db *DB) CheckDuplicateFilename(filename string) (bool, error) {
	query := `SELECT COUNT(*) FROM file_metadata WHERE file_name = $1`

	var count int
	err := db.conn.QueryRow(query, filename).Scan(&count)
	if err != nil {
		return false, err
	}

	exists := count > 0
	if exists {
		log.Warn().
			Str("filename", filename).
			Int("count", count).
			Msg("Duplicate filename detected")
	}

	return exists, nil
}

// loadStatusCache loads all status types from database into memory cache
func (db *DB) loadStatusCache() error {
	query := `SELECT id, code, name, description, display_order, is_active, created_at, updated_at 
		FROM status_types WHERE is_active = true ORDER BY display_order`

	rows, err := db.conn.Query(query)
	if err != nil {
		return fmt.Errorf("failed to query status types: %w", err)
	}
	defer rows.Close()

	db.statusCache = make(map[string]*models.StatusType)
	for rows.Next() {
		var status models.StatusType
		err := rows.Scan(
			&status.ID,
			&status.Code,
			&status.Name,
			&status.Description,
			&status.DisplayOrder,
			&status.IsActive,
			&status.CreatedAt,
			&status.UpdatedAt,
		)
		if err != nil {
			return fmt.Errorf("failed to scan status type: %w", err)
		}
		db.statusCache[status.Code] = &status
	}

	db.cacheExpiry = time.Now().Add(db.cacheDuration)
	log.Info().
		Int("count", len(db.statusCache)).
		Msg("Status cache loaded")

	return nil
}

// GetStatusByCode retrieves a status type by code from cache
func (db *DB) GetStatusByCode(code string) (*models.StatusType, error) {
	// Refresh cache if expired
	if time.Now().After(db.cacheExpiry) {
		log.Info().Msg("Status cache expired, reloading")
		if err := db.loadStatusCache(); err != nil {
			return nil, err
		}
	}

	status, exists := db.statusCache[code]
	if !exists {
		return nil, fmt.Errorf("status code not found: %s", code)
	}

	return status, nil
}

// GetAllStatuses retrieves all active status types from cache
func (db *DB) GetAllStatuses() ([]*models.StatusType, error) {
	// Refresh cache if expired
	if time.Now().After(db.cacheExpiry) {
		log.Info().Msg("Status cache expired, reloading")
		if err := db.loadStatusCache(); err != nil {
			return nil, err
		}
	}

	statuses := make([]*models.StatusType, 0, len(db.statusCache))
	for _, status := range db.statusCache {
		statuses = append(statuses, status)
	}

	// Sort by display order
	for i := 0; i < len(statuses)-1; i++ {
		for j := i + 1; j < len(statuses); j++ {
			if statuses[i].DisplayOrder > statuses[j].DisplayOrder {
				statuses[i], statuses[j] = statuses[j], statuses[i]
			}
		}
	}

	return statuses, nil
}

// GetStatusCode returns status code by name (for backward compatibility)
func (db *DB) GetStatusCode(name string) (string, error) {
	statuses, err := db.GetAllStatuses()
	if err != nil {
		return "", err
	}

	for _, status := range statuses {
		if status.Name == name || status.Code == name {
			return status.Code, nil
		}
	}

	return "", fmt.Errorf("status not found: %s", name)
}

// loadScheduleCache loads all schedule types from database into memory cache
func (db *DB) loadScheduleCache() error {
	query := `SELECT id, code, name, description, display_order, is_active, created_at, updated_at 
		FROM schedule_types WHERE is_active = true ORDER BY display_order`

	rows, err := db.conn.Query(query)
	if err != nil {
		return fmt.Errorf("failed to query schedule types: %w", err)
	}
	defer rows.Close()

	db.scheduleCache = make(map[string]*models.ScheduleType)
	for rows.Next() {
		var schedule models.ScheduleType
		err := rows.Scan(
			&schedule.ID,
			&schedule.Code,
			&schedule.Name,
			&schedule.Description,
			&schedule.DisplayOrder,
			&schedule.IsActive,
			&schedule.CreatedAt,
			&schedule.UpdatedAt,
		)
		if err != nil {
			return fmt.Errorf("failed to scan schedule type: %w", err)
		}
		db.scheduleCache[schedule.Code] = &schedule
	}

	log.Info().
		Int("count", len(db.scheduleCache)).
		Msg("Schedule cache loaded")

	return nil
}

// GetScheduleByCode retrieves a schedule type by code from cache
func (db *DB) GetScheduleByCode(code string) (*models.ScheduleType, error) {
	// Refresh cache if expired
	if time.Now().After(db.cacheExpiry) {
		log.Info().Msg("Schedule cache expired, reloading")
		if err := db.loadScheduleCache(); err != nil {
			return nil, err
		}
	}

	schedule, exists := db.scheduleCache[code]
	if !exists {
		return nil, fmt.Errorf("schedule code not found: %s", code)
	}

	return schedule, nil
}

// GetAllSchedules retrieves all active schedule types from cache
func (db *DB) GetAllSchedules() ([]*models.ScheduleType, error) {
	// Refresh cache if expired
	if time.Now().After(db.cacheExpiry) {
		log.Info().Msg("Schedule cache expired, reloading")
		if err := db.loadScheduleCache(); err != nil {
			return nil, err
		}
	}

	schedules := make([]*models.ScheduleType, 0, len(db.scheduleCache))
	for _, schedule := range db.scheduleCache {
		schedules = append(schedules, schedule)
	}

	// Sort by display order
	for i := 0; i < len(schedules)-1; i++ {
		for j := i + 1; j < len(schedules); j++ {
			if schedules[i].DisplayOrder > schedules[j].DisplayOrder {
				schedules[i], schedules[j] = schedules[j], schedules[i]
			}
		}
	}

	return schedules, nil
}

// GetScheduledFiles retrieves files that are scheduled and ready to process
func (db *DB) GetScheduledFiles(limit int) ([]models.FileMetadata, error) {
	// Convert scheduled_at from Asia/Jakarta to UTC for proper comparison
	// scheduled_at is stored as timestamp without timezone, interpreted as Asia/Jakarta
	query := `SELECT id, file_name, file_size, content_type, bucket_name, object_name, 
		status_code, schedule_type_code, 
		(scheduled_at AT TIME ZONE 'Asia/Jakarta' AT TIME ZONE 'UTC')::timestamp, 
		created_at, updated_at, processed_at 
		FROM file_metadata 
		WHERE schedule_type_code = 'scheduled' 
		AND status_code = 'pending'
		AND scheduled_at IS NOT NULL
		AND scheduled_at <= CURRENT_TIMESTAMP
		ORDER BY scheduled_at ASC 
		LIMIT $1`

	rows, err := db.conn.Query(query, limit)
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
			&file.ScheduleType,
			&file.ScheduledAt,
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

// CreateBatch creates a new batch
func (db *DB) CreateBatch(batch *models.Batch) error {
	query := `INSERT INTO batches 
		(file_id, batch_number, total_items, items_processed, items_success, items_failed, validation_status_code, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) RETURNING id`

	err := db.conn.QueryRow(
		query,
		batch.FileID,
		batch.BatchNumber,
		batch.TotalItems,
		batch.ItemsProcessed,
		batch.ItemsSuccess,
		batch.ItemsFailed,
		batch.ValidationStatus,
		time.Now(),
		time.Now(),
	).Scan(&batch.ID)

	if err == nil {
		log.Info().
			Int64("batch_id", batch.ID).
			Int64("file_id", batch.FileID).
			Int("batch_number", batch.BatchNumber).
			Msg("Created batch")
	}

	return err
}

// UpdateBatchValidationStatus updates batch validation status
func (db *DB) UpdateBatchValidationStatus(batchID int64, statusCode string) error {
	now := time.Now()
	// Update status and updated_at
	_, err := db.conn.Exec(`UPDATE batches SET validation_status_code = $1, updated_at = $2 WHERE id = $3`, statusCode, now, batchID)
	if err != nil {
		return err
	}

	// If success or failed, set validated_at
	if statusCode == "validation_success" || statusCode == "validation_failed" {
		if _, err := db.conn.Exec(`UPDATE batches SET validated_at = $1 WHERE id = $2`, now, batchID); err != nil {
			return err
		}
	}

	log.Info().
		Int64("batch_id", batchID).
		Str("status", statusCode).
		Msg("Updated batch validation status")
	return nil
}

// GetBatchByID retrieves a batch by ID
func (db *DB) GetBatchByID(batchID int64) (*models.Batch, error) {
	query := `SELECT id, file_id, batch_number, total_items, items_processed, items_success, items_failed, validation_status_code, 
		created_at, updated_at, validated_at 
		FROM batches WHERE id = $1`

	var batch models.Batch
	err := db.conn.QueryRow(query, batchID).Scan(
		&batch.ID,
		&batch.FileID,
		&batch.BatchNumber,
		&batch.TotalItems,
		&batch.ItemsProcessed,
		&batch.ItemsSuccess,
		&batch.ItemsFailed,
		&batch.ValidationStatus,
		&batch.CreatedAt,
		&batch.UpdatedAt,
		&batch.ValidatedAt,
	)

	if err != nil {
		return nil, err
	}

	return &batch, nil
}

// GetBatchesByFileID retrieves all batches for a file
func (db *DB) GetBatchesByFileID(fileID int64) ([]models.Batch, error) {
	query := `SELECT id, file_id, batch_number, total_items, items_processed, items_success, items_failed, validation_status_code, 
		created_at, updated_at, validated_at 
		FROM batches WHERE file_id = $1 ORDER BY batch_number`

	rows, err := db.conn.Query(query, fileID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var batches []models.Batch
	for rows.Next() {
		var batch models.Batch
		err := rows.Scan(
			&batch.ID,
			&batch.FileID,
			&batch.BatchNumber,
			&batch.TotalItems,
			&batch.ItemsProcessed,
			&batch.ItemsSuccess,
			&batch.ItemsFailed,
			&batch.ValidationStatus,
			&batch.CreatedAt,
			&batch.UpdatedAt,
			&batch.ValidatedAt,
		)
		if err != nil {
			return nil, err
		}
		batches = append(batches, batch)
	}

	return batches, nil
}

// BatchInsertItems inserts multiple items into a batch
func (db *DB) BatchInsertItems(batchID int64, fileID int64, items []models.Item) error {
	if len(items) == 0 {
		return nil
	}

	log.Info().
		Int64("batch_id", batchID).
		Int64("file_id", fileID).
		Int("item_count", len(items)).
		Msg("Starting batch insert items")

	tx, err := db.conn.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`INSERT INTO items 
		(batch_id, file_id, row_number, name, nominal, validation_status_code, created_at, updated_at) 
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	now := time.Now()
	for _, item := range items {
		_, err = stmt.Exec(
			batchID,
			fileID,
			item.RowNumber,
			item.Name,
			item.Nominal,
			item.ValidationStatus,
			now,
			now,
		)
		if err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	log.Info().
		Int64("batch_id", batchID).
		Int("items_inserted", len(items)).
		Msg("Batch insert items completed")
	return nil
}

// UpdateItemValidationStatus updates item validation status
func (db *DB) UpdateItemValidationStatus(itemID int64, statusCode string, errors map[string]interface{}) error {
	now := time.Now()

	// Update status and updated_at
	var err error
	if errors != nil && len(errors) > 0 {
		errorsJSON, marshalErr := json.Marshal(errors)
		if marshalErr != nil {
			return fmt.Errorf("failed to marshal validation errors: %w", marshalErr)
		}
		// Update with errors
		_, err = db.conn.Exec(`UPDATE items 
			SET validation_status_code = $1, 
			    updated_at = $2,
			    validation_errors = $3::jsonb
			WHERE id = $4`, statusCode, now, errorsJSON, itemID)
	} else {
		// Update without errors (keep existing validation_errors)
		_, err = db.conn.Exec(`UPDATE items 
			SET validation_status_code = $1, 
			    updated_at = $2
			WHERE id = $3`, statusCode, now, itemID)
	}

	if err != nil {
		return err
	}

	// If success or failed, set validated_at
	if statusCode == "validation_success" || statusCode == "validation_failed" {
		if _, err := db.conn.Exec(`UPDATE items SET validated_at = $1 WHERE id = $2`, now, itemID); err != nil {
			return err
		}
	}

	log.Info().
		Int64("item_id", itemID).
		Str("status", statusCode).
		Msg("Updated item validation status")
	return nil
}

// GetItemsByBatchID retrieves all items for a batch
func (db *DB) GetItemsByBatchID(batchID int64) ([]models.Item, error) {
	query := `SELECT id, batch_id, file_id, row_number, name, nominal, validation_status_code, 
		validation_errors, created_at, updated_at, validated_at 
		FROM items WHERE batch_id = $1 ORDER BY row_number`

	rows, err := db.conn.Query(query, batchID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items []models.Item
	for rows.Next() {
		var item models.Item
		var errorsJSON []byte
		err := rows.Scan(
			&item.ID,
			&item.BatchID,
			&item.FileID,
			&item.RowNumber,
			&item.Name,
			&item.Nominal,
			&item.ValidationStatus,
			&errorsJSON,
			&item.CreatedAt,
			&item.UpdatedAt,
			&item.ValidatedAt,
		)
		if err != nil {
			return nil, err
		}

		if errorsJSON != nil {
			if err := json.Unmarshal(errorsJSON, &item.ValidationErrors); err != nil {
				return nil, fmt.Errorf("failed to unmarshal validation errors: %w", err)
			}
		}

		items = append(items, item)
	}

	return items, nil
}

// GetItemCountByBatchID returns the number of items in a batch
func (db *DB) GetItemCountByBatchID(batchID int64) (int64, error) {
	query := `SELECT COUNT(*) FROM items WHERE batch_id = $1`

	var count int64
	err := db.conn.QueryRow(query, batchID).Scan(&count)

	return count, err
}

// UpdateBatchTotalItems updates the total_items count in a batch
func (db *DB) UpdateBatchTotalItems(batchID int64, totalItems int) error {
	query := `UPDATE batches SET total_items = $1, updated_at = $2 WHERE id = $3`
	_, err := db.conn.Exec(query, totalItems, time.Now(), batchID)
	return err
}

// Close closes the database connection
func (db *DB) Close() error {
	return db.conn.Close()
}
