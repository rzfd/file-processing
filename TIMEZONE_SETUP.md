# Timezone Setup untuk Database yang Sudah Ada

## ✅ Solusi yang Sudah Diterapkan

### 1. **Set Timezone di Connection String** (Permanen untuk semua connection baru)
- File: `internal/config/config.go`
- Timezone: `Asia/Jakarta` (WIB, UTC+7)
- **Tidak perlu restart database** - langsung efektif untuk semua connection baru

### 2. **Set Timezone di Go Code** (Backup untuk existing connections)
- File: `internal/database/postgres.go`
- Set timezone saat connection dibuat
- **Tidak perlu restart database**

### 3. **Set Timezone di Database Level** (Untuk default database)
- Sudah dilakukan: `ALTER DATABASE fileprocessing SET timezone = 'Asia/Jakarta'`
- Akan digunakan jika connection string tidak specify timezone

### 4. **Init Script untuk Database Baru**
- File: `configs/postgres/init/01-set-timezone.sql`
- Akan otomatis set timezone saat database baru dibuat

## 📋 Status Saat Ini

**Timezone aktif:** `Asia/Jakarta` (UTC+7)

**Verifikasi:**
```sql
SELECT NOW();  -- Akan menampilkan waktu dengan timezone +07
```

## 🔧 Untuk Database yang Sudah Ada

### Opsi 1: Sudah Otomatis (Recommended)
Dengan perubahan di connection string dan Go code, semua connection baru akan otomatis menggunakan timezone `Asia/Jakarta`. **Tidak perlu action tambahan.**

### Opsi 2: Set Manual (Jika perlu)
Jika ingin set untuk semua database yang sudah ada:

```bash
# Set untuk database fileprocessing
docker exec file-processing-postgres psql -U postgres -d fileprocessing -c "ALTER DATABASE fileprocessing SET timezone = 'Asia/Jakarta';"

# Set untuk database postgres
docker exec file-processing-postgres psql -U postgres -c "ALTER DATABASE postgres SET timezone = 'Asia/Jakarta';"
```

Atau gunakan script:
```bash
docker exec -i file-processing-postgres psql -U postgres < configs/postgres/set-timezone-existing-db.sql
```

## 🔄 Restart Services

**Setelah perubahan TZ di docker-compose.yaml:**
- ✅ **Postgres container**: Cukup restart (tidak perlu rebuild)
  ```bash
  docker-compose restart postgres
  ```

**Setelah perubahan connection string di Go code:**
- ✅ **Backend & Worker**: Perlu rebuild dan restart
  ```bash
  docker-compose build backend worker
  docker-compose up -d backend worker
  ```

## ✅ Verifikasi

**1. Check timezone di database:**
```bash
docker exec file-processing-postgres psql -U postgres -d fileprocessing -c "SHOW timezone;"
```

**2. Check waktu saat ini:**
```bash
docker exec file-processing-postgres psql -U postgres -d fileprocessing -c "SELECT NOW();"
```

**3. Check timezone source:**
```bash
docker exec file-processing-postgres psql -U postgres -d fileprocessing -c "SELECT name, setting, source FROM pg_settings WHERE name = 'TimeZone';"
```

## 📝 Catatan

- **TZ di docker-compose.yaml**: Set timezone untuk OS container (tidak langsung set PostgreSQL timezone)
- **Connection string timezone**: Set timezone untuk setiap connection (paling efektif)
- **ALTER DATABASE**: Set default timezone untuk database (backup jika connection string tidak specify)
- **Init script**: Untuk database baru yang akan dibuat di masa depan

## 🎯 Kesimpulan

**Untuk database yang sudah ada:**
- ✅ **Tidak perlu action tambahan** - connection string sudah include timezone
- ✅ Semua connection baru akan otomatis menggunakan `Asia/Jakarta`
- ✅ Data yang sudah ada tetap aman (hanya display yang berubah)
- ✅ Timestamp baru akan menggunakan timezone `Asia/Jakarta`

