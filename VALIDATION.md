# 📋 Validation Documentation

Dokumentasi lengkap untuk semua validasi yang diimplementasikan dalam File Processing System.

## ✅ Validasi yang Tersedia

### 1. **Max File Size Validation**

**Lokasi:** `backend/main.go` (line 207-216)

**Deskripsi:** Memvalidasi ukuran file yang diupload tidak melebihi batas maksimum.

**Konfigurasi:**
- Default: `10 MB` (10485760 bytes)
- Environment Variable: `MAX_FILE_SIZE`

**Contoh:**
```go
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
```

**Response:**
- **Success:** File diterima untuk processing
- **Error:** `400 Bad Request` - "File size exceeds limit"

---

### 2. **File Type Validation**

**Lokasi:** `backend/main.go` (line 187-205)

**Deskripsi:** Memvalidasi ekstensi file yang diupload sesuai dengan daftar yang diizinkan.

**Konfigurasi:**
- Allowed Extensions: `.csv`, `.xlsx`, `.xls`
- Dapat dikonfigurasi di `internal/config/config.go`

**Contoh:**
```go
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
```

**Response:**
- **Success:** File diterima untuk processing
- **Error:** `400 Bad Request` - "File type not allowed"

---

### 3. **Duplicate Filename Validation**

**Lokasi:** `backend/main.go` (line 217-231)

**Deskripsi:** Memvalidasi bahwa filename yang diupload belum pernah ada di database.

**Database Query:**
```sql
SELECT COUNT(*) FROM file_metadata WHERE file_name = $1
```

**Contoh:**
```go
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
```

**Response:**
- **Success:** File diterima untuk processing
- **Error:** `409 Conflict` - "File with this name already exists"

---

### 4. **Header Validation**

**Lokasi:** `internal/validator/validator.go` (line 334-373) & `worker/main.go` (line 393-407)

**Deskripsi:** Memvalidasi bahwa semua kolom yang required ada di header file CSV/XLSX.

**Validation Rules:**
```go
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
```

**Contoh:**
```go
// Validate headers
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
```

**Error Format:**
```go
ValidationError{
    Row:     0,
    Field:   "name",
    Value:   "",
    Message: "Required column 'name' is missing from file",
}
```

**Response:**
- **Success:** Processing dilanjutkan ke validasi record
- **Error:** File status diupdate ke `failed`, error log di Loki

---

### 5. **Field Validation (Data Type, Length, Range, Pattern)**

**Lokasi:** `internal/validator/validator.go` & `worker/main.go`

**Deskripsi:** Memvalidasi setiap field dalam record sesuai dengan aturan yang ditentukan.

**Validation Types:**

#### a. **Data Type Validation**
- `string`: Validasi tipe string
- `int`: Validasi integer
- `float`: Validasi float/decimal
- `email`: Validasi format email
- `date`: Validasi format tanggal (YYYY-MM-DD)

#### b. **Length Validation**
- `MinLength`: Panjang minimum string
- `MaxLength`: Panjang maksimum string

#### c. **Range Validation**
- `MinValue`: Nilai minimum untuk numeric
- `MaxValue`: Nilai maksimum untuk numeric

#### d. **Pattern Validation**
- `Pattern`: Regex pattern untuk validasi custom

**Contoh Rules:**
```go
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
    {
        Field:    "email",
        Required: false,
        DataType: "email",
    },
    {
        Field:   "phone",
        Pattern: `^\d{10,15}$`,
        CustomError: "Phone number must be 10-15 digits",
    },
}
```

**Response:**
- **Success:** Record valid
- **Error:** ValidationError dengan detail field, value, dan message

---

### 6. **Duplicate Data Validation**

**Lokasi:** `internal/validator/validator.go` (line 375-433) & `worker/main.go` (line 418-428)

**Deskripsi:** Mendeteksi record duplikat berdasarkan kombinasi field tertentu.

**Konfigurasi:**
```go
// Define key fields for duplicate detection
duplicateKeyFields := []string{"name", "nominal"}
```

**Algoritma:**
1. Membuat hash key dari kombinasi field yang ditentukan
2. Menyimpan hash key dan row number dalam map
3. Jika hash key sudah ada, record dianggap duplikat

**Contoh:**
```go
// Check for duplicate records
duplicateErrors := validator.ValidateDuplicates(recordData, duplicateKeyFields)
allErrors = append(allErrors, duplicateErrors...)
```

**Error Format:**
```go
ValidationError{
    Row:     5,
    Field:   "name+nominal",
    Value:   "John Doe|1000",
    Message: "Duplicate record found (same as row [2])",
}
```

**Response:**
- **Success:** Tidak ada duplikat
- **Error:** ValidationError dengan detail row duplikat

---

## 🔄 Validation Flow

```
┌─────────────────────────────────────────────────────────────┐
│                    UPLOAD REQUEST                           │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      ▼
         ┌────────────────────────┐
         │ 1. Max Size Validation │
         └────────┬───────────────┘
                  │ ✅ Pass
                  ▼
         ┌────────────────────────┐
         │ 2. File Type Validation│
         └────────┬───────────────┘
                  │ ✅ Pass
                  ▼
         ┌────────────────────────────┐
         │ 3. Duplicate Filename Check│
         └────────┬───────────────────┘
                  │ ✅ Pass
                  ▼
         ┌────────────────────────┐
         │   Upload to MinIO      │
         └────────┬───────────────┘
                  │
                  ▼
         ┌────────────────────────┐
         │  Save to PostgreSQL    │
         └────────┬───────────────┘
                  │
                  ▼
         ┌────────────────────────┐
         │  Publish to Kafka      │
         └────────┬───────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────────────────┐
│                    WORKER PROCESSING                         │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      ▼
         ┌────────────────────────┐
         │ Download from MinIO    │
         └────────┬───────────────┘
                  │
                  ▼
         ┌────────────────────────┐
         │ Parse CSV/XLSX         │
         │ (Extract Headers)      │
         └────────┬───────────────┘
                  │
                  ▼
         ┌────────────────────────┐
         │ 4. Header Validation   │
         └────────┬───────────────┘
                  │ ✅ Pass
                  ▼
         ┌────────────────────────┐
         │ 5. Field Validation    │
         │ (Type, Length, Range)  │
         └────────┬───────────────┘
                  │ ✅ Pass
                  ▼
         ┌────────────────────────┐
         │ 6. Duplicate Data Check│
         └────────┬───────────────┘
                  │ ✅ Pass
                  ▼
         ┌────────────────────────┐
         │ Batch Insert to DB     │
         └────────┬───────────────┘
                  │
                  ▼
         ┌────────────────────────┐
         │ Update Status: Completed│
         └────────────────────────┘
```

---

## 🧪 Testing Validations

### Test 1: Max Size Validation

```bash
# Create a file larger than 10MB
dd if=/dev/zero of=large_file.csv bs=1M count=11

# Upload
curl -X POST http://localhost:8080/upload \
  -F "file=@large_file.csv"

# Expected: 400 Bad Request - "File size exceeds limit"
```

### Test 2: File Type Validation

```bash
# Create a .txt file
echo "test" > test.txt

# Upload
curl -X POST http://localhost:8080/upload \
  -F "file=@test.txt"

# Expected: 400 Bad Request - "File type not allowed"
```

### Test 3: Duplicate Filename Validation

```bash
# Upload file pertama kali
curl -X POST http://localhost:8080/upload \
  -F "file=@test.csv"

# Upload file dengan nama yang sama
curl -X POST http://localhost:8080/upload \
  -F "file=@test.csv"

# Expected: 409 Conflict - "File with this name already exists"
```

### Test 4: Header Validation

**test_missing_header.csv:**
```csv
nama,jumlah
John,1000
Jane,2000
```

```bash
curl -X POST http://localhost:8080/upload \
  -F "file=@test_missing_header.csv"

# Check logs di Grafana Loki:
# Expected: "Required column 'name' is missing from file"
```

### Test 5: Field Validation

**test_invalid_data.csv:**
```csv
name,nominal
A,1000
John Doe,-500
VeryLongNameThatExceedsTheMaximumLengthOf100CharactersAndShouldFailValidation,2000
```

```bash
curl -X POST http://localhost:8080/upload \
  -F "file=@test_invalid_data.csv"

# Check logs di Grafana Loki:
# Expected errors:
# - Row 1: "Field 'name' must be at least 2 characters"
# - Row 2: "Field 'nominal' must be at least 0.00"
# - Row 3: "Field 'name' must be at most 100 characters"
```

### Test 6: Duplicate Data Validation

**test_duplicate_data.csv:**
```csv
name,nominal
John Doe,1000
Jane Smith,2000
John Doe,1000
Alice Brown,3000
Jane Smith,2000
```

```bash
curl -X POST http://localhost:8080/upload \
  -F "file=@test_duplicate_data.csv"

# Check logs di Grafana Loki:
# Expected:
# - Row 3: "Duplicate record found (same as row [1])"
# - Row 5: "Duplicate record found (same as row [2])"
```

---

## 📊 Monitoring Validation Errors

### Grafana Loki Queries

**1. All Validation Errors:**
```logql
{service_name="worker", detected_level="error"} |= "validation"
```

**2. Header Validation Errors:**
```logql
{service_name="worker"} |= "Header validation failed"
```

**3. Duplicate Data Errors:**
```logql
{service_name="worker"} |= "Duplicate record found"
```

**4. Field Validation Errors:**
```logql
{service_name="worker"} |= "Validation error" | json
```

**5. Duplicate Filename Errors:**
```logql
{service_name="backend"} |= "Duplicate filename detected"
```

---

## ⚙️ Customization

### Mengubah Validation Rules

Edit file `worker/main.go` pada fungsi `validateRecords`:

```go
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
    // Tambahkan rule baru di sini
    {
        Field:    "email",
        Required: false,
        DataType: "email",
    },
}
```

### Mengubah Duplicate Key Fields

Edit file `worker/main.go`:

```go
// Define key fields for duplicate detection
duplicateKeyFields := []string{"name", "nominal"}
// Ubah menjadi field lain, misalnya:
// duplicateKeyFields := []string{"email"}
```

### Mengubah Max File Size

Edit `docker-compose.yaml`:

```yaml
backend:
  environment:
    - MAX_FILE_SIZE=20971520  # 20MB
```

### Mengubah Allowed Extensions

Edit `internal/config/config.go`:

```go
AllowedExtensions: []string{".csv", ".xlsx", ".xls", ".json"},
```

---

## 📝 Error Logging

Semua validation error di-log dengan structured logging menggunakan `zerolog`:

```go
log.Warn().
    Str("trace_id", traceID).
    Int("row", verr.Row).
    Str("field", verr.Field).
    Str("value", verr.Value).
    Str("error", verr.Message).
    Msg("Validation error")
```

Log dapat dilihat di:
- **Grafana Loki:** http://localhost:3000
- **Docker logs:** `docker logs file-processing-worker`

---

## 🎯 Best Practices

1. **Selalu validasi di backend:** Jangan hanya mengandalkan validasi frontend
2. **Log semua validation errors:** Untuk debugging dan audit
3. **Return early:** Jika header validation gagal, tidak perlu lanjut ke field validation
4. **Batch validation:** Kumpulkan semua error sebelum return, jangan stop di error pertama
5. **Clear error messages:** Berikan pesan error yang jelas dan actionable
6. **Monitor validation metrics:** Track validation error rate di Prometheus/Grafana

---

## 🔗 Related Files

- `backend/main.go` - Upload validation
- `worker/main.go` - Processing validation
- `internal/validator/validator.go` - Validation logic
- `internal/database/postgres.go` - Duplicate filename check
- `internal/processor/csv.go` - CSV parsing with headers
- `internal/processor/xlsx.go` - XLSX parsing with headers
- `internal/models/file.go` - Data models

---

## 📚 References

- [Zerolog Documentation](https://github.com/rs/zerolog)
- [Go Validator Pattern](https://github.com/go-playground/validator)
- [CSV Processing Best Practices](https://en.wikipedia.org/wiki/Comma-separated_values)

