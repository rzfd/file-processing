# Analisis Normalisasi Database

## Ringkasan
Analisis ini mengevaluasi struktur tabel dari perspektif normalisasi database untuk mengidentifikasi masalah dan memberikan rekomendasi perbaikan.

---

## 🔴 Masalah Normalisasi yang Ditemukan

### 1. **Masalah Utama: JSONB Data Field (Tidak Ternormalisasi)**

**Masalah**: 
- Semua data dari CSV/XLSX disimpan sebagai JSONB di kolom `data`
- Tidak ada struktur kolom yang jelas di database
- Setiap file bisa memiliki struktur kolom yang berbeda

**Contoh Data Saat Ini**:
```json
{
  "file_id": 1,
  "row_number": 1,
  "data": {
    "name": "John Doe",
    "nominal": 1000
  }
}
```

**Dampak**:
- ❌ Tidak bisa query berdasarkan kolom tertentu dengan efisien
- ❌ Tidak bisa membuat index pada kolom tertentu (misal: `name`, `nominal`)
- ❌ Tidak bisa membuat foreign key ke tabel lain jika ada relasi
- ❌ Tidak bisa membuat constraint pada kolom tertentu
- ❌ Query menjadi lambat karena harus scan JSONB
- ❌ Tidak bisa melakukan JOIN dengan tabel lain berdasarkan data
- ❌ Tidak bisa melakukan aggregasi yang efisien (SUM, COUNT, GROUP BY)

**Contoh Query yang Tidak Efisien**:
```sql
-- Harus scan semua JSONB untuk filter
SELECT * FROM processed_records 
WHERE data->>'name' = 'John Doe';

-- Tidak bisa index pada kolom name
-- Harus parse JSONB setiap kali
```

---

### 2. **Headers Tidak Disimpan di Database**

**Masalah**:
- Headers dari CSV/XLSX tidak disimpan di database
- Hanya ada di memory saat processing
- Tidak ada metadata tentang struktur kolom per file

**Dampak**:
- ❌ Tidak bisa query berdasarkan struktur kolom
- ❌ Tidak bisa validasi struktur di database level
- ❌ Tidak bisa membuat relasi ke tabel lain
- ❌ Tidak bisa membuat view atau stored procedure berdasarkan struktur
- ❌ Sulit untuk reporting dan analytics

---

### 3. **Tidak Ada Normalisasi untuk Data Berulang**

**Masalah**:
- Jika CSV memiliki kolom seperti `category`, `department`, `status`, dll yang berulang
- Data tersebut disimpan berulang-ulang di setiap row
- Tidak ada normalisasi ke tabel terpisah

**Contoh**:
```json
// Row 1
{"name": "John", "category": "Electronics", "department": "Sales"}

// Row 2  
{"name": "Jane", "category": "Electronics", "department": "Sales"}

// Row 3
{"name": "Bob", "category": "Clothing", "department": "Sales"}
```

**Dampak**:
- ❌ Data redundancy (category "Electronics" disimpan berkali-kali)
- ❌ Update anomaly (jika category berubah, harus update banyak rows)
- ❌ Insert anomaly (tidak bisa insert category tanpa data)
- ❌ Delete anomaly (jika semua data di category dihapus, category hilang)

---

### 4. **Tidak Ada Tabel untuk Schema/Structure**

**Masalah**:
- Tidak ada tabel yang menyimpan informasi tentang struktur kolom per file
- Tidak ada metadata tentang tipe data, constraint, dll

**Dampak**:
- ❌ Tidak bisa validasi struktur di database level
- ❌ Tidak bisa membuat dynamic queries berdasarkan schema
- ❌ Tidak bisa membuat view atau stored procedure

---

## 📊 Evaluasi Normalisasi

### Form Normal (1NF) - ✅ Lolos
- Setiap kolom memiliki nilai atomic
- Tidak ada duplicate rows (ada unique constraint yang direkomendasikan)

### Second Normal Form (2NF) - ⚠️ Partial
- Masalah: `processed_records` memiliki partial dependency
- `row_number` bergantung pada `file_id`, bukan pada `id` (PK)
- Tapi ini acceptable karena `row_number` adalah bagian dari composite key

### Third Normal Form (3NF) - ❌ Tidak Lolos
- Masalah: Transitive dependency melalui JSONB
- Data dalam JSONB bisa memiliki transitive dependency
- Contoh: `data->>'category'` bisa memiliki dependency ke tabel lain

### Boyce-Codd Normal Form (BCNF) - ❌ Tidak Lolos
- Masalah: JSONB field tidak memenuhi BCNF
- Tidak ada struktur kolom yang jelas

---

## 💡 Solusi Normalisasi

### **Opsi 1: Dynamic Schema dengan Tabel Kolom (Recommended untuk Generic File Processing)**

**Struktur Baru**:
```sql
-- Tabel untuk menyimpan schema/kolom per file
CREATE TABLE file_schemas (
    id SERIAL PRIMARY KEY,
    file_id BIGINT NOT NULL REFERENCES file_metadata(id) ON DELETE CASCADE,
    column_name VARCHAR(255) NOT NULL,
    column_type VARCHAR(50) NOT NULL, -- 'string', 'number', 'boolean', 'date'
    column_order INTEGER NOT NULL,
    is_required BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(file_id, column_name),
    UNIQUE(file_id, column_order)
);

-- Tabel untuk menyimpan data yang dinormalisasi
CREATE TABLE processed_records (
    id SERIAL PRIMARY KEY,
    file_id BIGINT NOT NULL REFERENCES file_metadata(id) ON DELETE CASCADE,
    row_number INTEGER NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(file_id, row_number)
);

-- Tabel untuk menyimpan nilai per kolom (EAV Pattern)
CREATE TABLE processed_record_values (
    id SERIAL PRIMARY KEY,
    record_id BIGINT NOT NULL REFERENCES processed_records(id) ON DELETE CASCADE,
    schema_id BIGINT NOT NULL REFERENCES file_schemas(id) ON DELETE CASCADE,
    string_value VARCHAR(255),
    number_value NUMERIC,
    boolean_value BOOLEAN,
    date_value TIMESTAMP,
    json_value JSONB, -- untuk complex data
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(record_id, schema_id)
);

-- Index untuk performa
CREATE INDEX idx_record_values_record_id ON processed_record_values(record_id);
CREATE INDEX idx_record_values_schema_id ON processed_record_values(schema_id);
CREATE INDEX idx_record_values_string ON processed_record_values(string_value) WHERE string_value IS NOT NULL;
CREATE INDEX idx_record_values_number ON processed_record_values(number_value) WHERE number_value IS NOT NULL;
```

**Kelebihan**:
- ✅ Fully normalized
- ✅ Bisa query berdasarkan kolom tertentu dengan efisien
- ✅ Bisa membuat index pada kolom tertentu
- ✅ Bisa membuat foreign key ke tabel lain
- ✅ Bisa melakukan aggregasi yang efisien
- ✅ Fleksibel untuk berbagai struktur file

**Kekurangan**:
- ❌ Lebih kompleks
- ❌ Lebih banyak JOIN untuk query
- ❌ Lebih banyak storage (overhead)

---

### **Opsi 2: Hybrid Approach (JSONB + Normalized Columns)**

**Struktur Baru**:
```sql
-- Simpan schema per file
CREATE TABLE file_schemas (
    id SERIAL PRIMARY KEY,
    file_id BIGINT NOT NULL REFERENCES file_metadata(id) ON DELETE CASCADE,
    column_name VARCHAR(255) NOT NULL,
    column_type VARCHAR(50) NOT NULL,
    column_order INTEGER NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(file_id, column_name)
);

-- Simpan data dengan JSONB + kolom yang sering di-query
CREATE TABLE processed_records (
    id SERIAL PRIMARY KEY,
    file_id BIGINT NOT NULL REFERENCES file_metadata(id) ON DELETE CASCADE,
    row_number INTEGER NOT NULL,
    data JSONB NOT NULL, -- untuk fleksibilitas
    -- Kolom yang sering di-query bisa di-extract ke kolom terpisah
    -- Contoh jika banyak file punya kolom 'name' dan 'nominal':
    name VARCHAR(255), -- extracted dari JSONB jika ada
    nominal NUMERIC,  -- extracted dari JSONB jika ada
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(file_id, row_number)
);

-- Index pada kolom yang sering di-query
CREATE INDEX idx_processed_records_name ON processed_records(name) WHERE name IS NOT NULL;
CREATE INDEX idx_processed_records_nominal ON processed_records(nominal) WHERE nominal IS NOT NULL;
CREATE INDEX idx_processed_records_data_gin ON processed_records USING GIN (data);
```

**Kelebihan**:
- ✅ Balance antara normalisasi dan fleksibilitas
- ✅ Bisa query kolom umum dengan efisien
- ✅ Tetap fleksibel untuk kolom yang berbeda
- ✅ Lebih sederhana dari Opsi 1

**Kekurangan**:
- ❌ Tidak fully normalized
- ❌ Perlu extract kolom yang sering di-query
- ❌ Masih ada redundancy untuk kolom yang di-extract

---

### **Opsi 3: Schema Registry Pattern**

**Struktur Baru**:
```sql
-- Registry untuk schema yang umum digunakan
CREATE TABLE schema_registry (
    id SERIAL PRIMARY KEY,
    schema_name VARCHAR(255) NOT NULL UNIQUE,
    description TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Kolom untuk setiap schema
CREATE TABLE schema_columns (
    id SERIAL PRIMARY KEY,
    schema_id INTEGER NOT NULL REFERENCES schema_registry(id) ON DELETE CASCADE,
    column_name VARCHAR(255) NOT NULL,
    column_type VARCHAR(50) NOT NULL,
    column_order INTEGER NOT NULL,
    is_required BOOLEAN DEFAULT FALSE,
    UNIQUE(schema_id, column_name)
);

-- File metadata dengan schema reference
ALTER TABLE file_metadata 
ADD COLUMN schema_id INTEGER REFERENCES schema_registry(id);

-- Tabel untuk data yang dinormalisasi berdasarkan schema
CREATE TABLE processed_records (
    id SERIAL PRIMARY KEY,
    file_id BIGINT NOT NULL REFERENCES file_metadata(id) ON DELETE CASCADE,
    row_number INTEGER NOT NULL,
    -- Kolom dinamis berdasarkan schema
    -- Atau gunakan JSONB dengan validation berdasarkan schema
    data JSONB NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(file_id, row_number),
    -- Validasi JSONB berdasarkan schema (dengan trigger atau application level)
    CONSTRAINT valid_json_schema CHECK (-- validation logic)
);
```

**Kelebihan**:
- ✅ Bisa reuse schema
- ✅ Validasi struktur di database level
- ✅ Fleksibel untuk berbagai schema

**Kekurangan**:
- ❌ Lebih kompleks
- ❌ Perlu management schema registry

---

## 🎯 Rekomendasi

### **Untuk Generic File Processing System (Current Use Case)**

**Rekomendasi: Opsi 2 (Hybrid Approach)** dengan perbaikan:

1. **Simpan Headers/Schema**:
   ```sql
   CREATE TABLE file_schemas (
       id SERIAL PRIMARY KEY,
       file_id BIGINT NOT NULL REFERENCES file_metadata(id) ON DELETE CASCADE,
       column_name VARCHAR(255) NOT NULL,
       column_type VARCHAR(50) NOT NULL,
       column_order INTEGER NOT NULL,
       created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
       UNIQUE(file_id, column_name),
       UNIQUE(file_id, column_order)
   );
   ```

2. **Tetap Gunakan JSONB untuk Data** (untuk fleksibilitas):
   ```sql
   CREATE TABLE processed_records (
       id SERIAL PRIMARY KEY,
       file_id BIGINT NOT NULL REFERENCES file_metadata(id) ON DELETE CASCADE,
       row_number INTEGER NOT NULL,
       data JSONB NOT NULL,
       created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
       UNIQUE(file_id, row_number)
   );
   
   -- GIN index untuk query JSONB
   CREATE INDEX idx_processed_records_data_gin 
   ON processed_records USING GIN (data);
   ```

3. **Tambahkan Materialized View untuk Kolom yang Sering Di-query**:
   ```sql
   -- Jika banyak file punya kolom 'name' dan 'nominal'
   CREATE MATERIALIZED VIEW processed_records_extracted AS
   SELECT 
       pr.id,
       pr.file_id,
       pr.row_number,
       pr.data->>'name' as name,
       (pr.data->>'nominal')::NUMERIC as nominal,
       pr.created_at
   FROM processed_records pr;
   
   CREATE INDEX idx_mv_records_name ON processed_records_extracted(name);
   CREATE INDEX idx_mv_records_nominal ON processed_records_extracted(nominal);
   ```

**Alasan**:
- ✅ Tetap fleksibel untuk berbagai struktur file
- ✅ Bisa query berdasarkan kolom tertentu dengan efisien (melalui GIN index atau materialized view)
- ✅ Tidak terlalu kompleks
- ✅ Bisa di-scale sesuai kebutuhan

---

## 📝 Migration Plan

### Phase 1: Tambahkan Schema Table
1. Buat tabel `file_schemas`
2. Migrate data headers yang ada (jika ada)
3. Update code untuk menyimpan schema saat processing

### Phase 2: Optimasi JSONB
1. Tambahkan GIN index pada JSONB
2. Buat materialized view untuk kolom yang sering di-query
3. Update query untuk menggunakan materialized view jika perlu

### Phase 3: (Optional) Full Normalization
1. Jika perlu, migrate ke Opsi 1 (EAV Pattern)
2. Hanya jika benar-benar perlu query yang sangat kompleks

---

## 🔍 Query Performance Comparison

### Current (JSONB tanpa index):
```sql
-- Lambat: Full table scan + JSONB parsing
SELECT * FROM processed_records 
WHERE data->>'name' = 'John Doe';
-- Execution time: ~500ms untuk 100K rows
```

### Dengan GIN Index:
```sql
-- Lebih cepat: GIN index pada JSONB
CREATE INDEX idx_data_gin ON processed_records USING GIN (data);

SELECT * FROM processed_records 
WHERE data @> '{"name": "John Doe"}'::jsonb;
-- Execution time: ~50ms untuk 100K rows
```

### Dengan Materialized View:
```sql
-- Paling cepat: Index pada kolom extracted
SELECT * FROM processed_records_extracted 
WHERE name = 'John Doe';
-- Execution time: ~5ms untuk 100K rows
```

---

## ✅ Kesimpulan

**Status Normalisasi Saat Ini**: ❌ **Tidak Ternormalisasi dengan Baik**

**Masalah Utama**:
1. JSONB field tidak ternormalisasi
2. Headers tidak disimpan
3. Tidak ada struktur schema

**Rekomendasi**:
- **Short term**: Tambahkan `file_schemas` table dan GIN index pada JSONB
- **Long term**: Pertimbangkan hybrid approach atau full normalization jika perlu query yang lebih kompleks

**Trade-off**:
- Normalisasi penuh = lebih kompleks, lebih lambat untuk insert, lebih cepat untuk query kompleks
- JSONB = lebih sederhana, lebih cepat untuk insert, lebih lambat untuk query kompleks
- Hybrid = balance antara keduanya

