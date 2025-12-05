# Batch & Items Implementation

## ✅ Implementasi Selesai

### 1. **Status Types - Validation Status Codes** ✅

Ditambahkan 4 validation status codes ke `status_types`:
- `validation_not_started` (Display Order: 10)
- `validation_in_progress` (Display Order: 11)
- `validation_success` (Display Order: 12)
- `validation_failed` (Display Order: 13)

### 2. **Tabel `batches`** ✅

```sql
CREATE TABLE batches (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    file_id BIGINT NOT NULL REFERENCES file_metadata(id) ON DELETE CASCADE,
    batch_number INTEGER NOT NULL,
    total_items INTEGER NOT NULL DEFAULT 0,
    validation_status_code VARCHAR(50) NOT NULL DEFAULT 'validation_not_started',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    validated_at TIMESTAMP,
    UNIQUE(file_id, batch_number),
    FOREIGN KEY (validation_status_code) REFERENCES status_types(code)
);
```

**Indexes**:
- `idx_batches_file_id` - untuk query berdasarkan file_id
- `idx_batches_validation_status` - untuk query berdasarkan validation status

### 3. **Tabel `items`** ✅

```sql
CREATE TABLE items (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    batch_id BIGINT NOT NULL REFERENCES batches(id) ON DELETE CASCADE,
    file_id BIGINT NOT NULL REFERENCES file_metadata(id) ON DELETE CASCADE,
    row_number INTEGER NOT NULL CHECK (row_number > 0),
    data JSONB NOT NULL,
    validation_status_code VARCHAR(50) NOT NULL DEFAULT 'validation_not_started',
    validation_errors JSONB,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    validated_at TIMESTAMP,
    UNIQUE(batch_id, row_number),
    FOREIGN KEY (validation_status_code) REFERENCES status_types(code)
);
```

**Indexes**:
- `idx_items_batch_id` - untuk query berdasarkan batch_id
- `idx_items_file_id` - untuk query berdasarkan file_id
- `idx_items_validation_status` - untuk query berdasarkan validation status
- `idx_items_data_gin` - GIN index untuk query JSONB data

### 4. **Models** ✅

**Batch Model**:
```go
type Batch struct {
    ID                int64      `json:"id" db:"id"`
    FileID            int64      `json:"file_id" db:"file_id"`
    BatchNumber       int        `json:"batch_number" db:"batch_number"`
    TotalItems        int        `json:"total_items" db:"total_items"`
    ValidationStatus  string     `json:"validation_status" db:"validation_status_code"`
    CreatedAt         time.Time  `json:"created_at" db:"created_at"`
    UpdatedAt         time.Time  `json:"updated_at" db:"updated_at"`
    ValidatedAt       *time.Time `json:"validated_at,omitempty" db:"validated_at"`
}
```

**Item Model**:
```go
type Item struct {
    ID                int64                  `json:"id" db:"id"`
    BatchID           int64                  `json:"batch_id" db:"batch_id"`
    FileID            int64                  `json:"file_id" db:"file_id"`
    RowNumber         int                    `json:"row_number" db:"row_number"`
    Data              map[string]interface{} `json:"data" db:"data"`
    ValidationStatus  string                 `json:"validation_status" db:"validation_status_code"`
    ValidationErrors  map[string]interface{} `json:"validation_errors,omitempty" db:"validation_errors"`
    CreatedAt         time.Time              `json:"created_at" db:"created_at"`
    UpdatedAt         time.Time              `json:"updated_at" db:"updated_at"`
    ValidatedAt       *time.Time             `json:"validated_at,omitempty" db:"validated_at"`
}
```

### 5. **Database Functions** ✅

**Batch Functions**:
- `CreateBatch(batch *models.Batch)` - Create batch baru
- `UpdateBatchValidationStatus(batchID int64, statusCode string)` - Update validation status batch
- `GetBatchByID(batchID int64)` - Get batch by ID
- `GetBatchesByFileID(fileID int64)` - Get semua batch untuk file
- `UpdateBatchTotalItems(batchID int64, totalItems int)` - Update total items

**Item Functions**:
- `BatchInsertItems(batchID int64, fileID int64, items []models.Item)` - Insert multiple items
- `UpdateItemValidationStatus(itemID int64, statusCode string, errors map[string]interface{})` - Update validation status item
- `GetItemsByBatchID(batchID int64)` - Get semua items untuk batch
- `GetItemCountByBatchID(batchID int64)` - Get count items dalam batch

### 6. **Worker Updates** ✅

**Perubahan**:
1. Load validation status codes dari database
2. Create batches untuk setiap batch processing
3. Convert records ke items dan insert ke database
4. Validate items dan update validation status
5. Update batch validation status berdasarkan hasil validation items

**Flow**:
```
File Upload → Processing → Create Batches → Create Items → Validate Items → Update Status
```

## 📊 Relasi Database

```
status_types (Master)
├── File Status (1-4)
│   ├── pending
│   ├── processing
│   ├── completed
│   └── failed
└── Validation Status (10-13)
    ├── validation_not_started
    ├── validation_in_progress
    ├── validation_success
    └── validation_failed

file_metadata
└── status_code → status_types (file status)

batches
├── file_id → file_metadata(id)
└── validation_status_code → status_types (validation status)

items
├── batch_id → batches(id)
├── file_id → file_metadata(id)
└── validation_status_code → status_types (validation status)
```

## 🎯 Keuntungan

1. ✅ **Konsisten** - Semua status menggunakan `status_types` table
2. ✅ **Tidak Hardcode** - Status bisa diubah tanpa deploy
3. ✅ **Data Integrity** - Foreign key constraints
4. ✅ **Fleksibel** - Bisa tambah status baru dari database
5. ✅ **Trackable** - Bisa track validation status per batch dan item
6. ✅ **Scalable** - Mudah untuk extend fitur

## 📝 Usage Example

```go
// Create batch
batch := &models.Batch{
    FileID:           1,
    BatchNumber:      1,
    TotalItems:       100,
    ValidationStatus: "validation_not_started",
}
db.CreateBatch(batch)

// Create items
items := []models.Item{
    {
        BatchID:          batch.ID,
        FileID:           1,
        RowNumber:        1,
        Data:             map[string]interface{}{"name": "John"},
        ValidationStatus: "validation_not_started",
    },
}
db.BatchInsertItems(batch.ID, 1, items)

// Update validation status
db.UpdateItemValidationStatus(itemID, "validation_success", nil)
db.UpdateBatchValidationStatus(batchID, "validation_success")
```

## ✅ Checklist

- [x] Tambahkan validation status codes ke status_types
- [x] Buat tabel batches dengan relasi ke status_types
- [x] Buat tabel items dengan relasi ke batches dan status_types
- [x] Update models untuk Batch dan Item
- [x] Buat database functions untuk batches dan items
- [x] Update worker untuk menggunakan batch/items structure
- [x] Implementasi validation logic untuk batch dan items

## 🚀 Next Steps

1. Test dengan upload file
2. Verify batches dan items terbuat dengan benar
3. Verify validation status update dengan benar
4. Optional: Tambahkan API endpoints untuk query batches dan items

