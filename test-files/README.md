# Test Files

File-file ini digunakan untuk testing validasi sistem.

## Files

### 1. `valid_data.csv` ✅
File valid yang akan berhasil diproses.

**Test:**
```bash
curl -X POST http://localhost:8080/upload -F "file=@test-files/valid_data.csv"
```

**Expected:** Status 200, file berhasil diproses

---

### 2. `missing_header.csv` ❌
File dengan header yang salah (menggunakan 'nama' dan 'jumlah' bukan 'name' dan 'nominal').

**Test:**
```bash
curl -X POST http://localhost:8080/upload -F "file=@test-files/missing_header.csv"
```

**Expected:** 
- Upload berhasil (200)
- Processing gagal dengan error: "Required column 'name' is missing from file"
- Check di Grafana Loki untuk melihat error

---

### 3. `invalid_data.csv` ❌
File dengan berbagai jenis invalid data:
- Row 1: Name terlalu pendek (< 2 karakter)
- Row 2: Nominal negatif (< 0)
- Row 3: Name terlalu panjang (> 100 karakter)
- Row 4: Nominal bukan angka

**Test:**
```bash
curl -X POST http://localhost:8080/upload -F "file=@test-files/invalid_data.csv"
```

**Expected:**
- Upload berhasil (200)
- Processing gagal dengan multiple validation errors
- Check di Grafana Loki untuk detail errors

---

### 4. `duplicate_data.csv` ❌
File dengan data duplikat:
- Row 3 duplikat dengan Row 1 (John Doe, 1000)
- Row 5 duplikat dengan Row 2 (Jane Smith, 2000)

**Test:**
```bash
curl -X POST http://localhost:8080/upload -F "file=@test-files/duplicate_data.csv"
```

**Expected:**
- Upload berhasil (200)
- Processing gagal dengan duplicate errors
- Check di Grafana Loki: "Duplicate record found (same as row [1])"

---

## Test Duplicate Filename

**Test:**
```bash
# Upload pertama kali
curl -X POST http://localhost:8080/upload -F "file=@test-files/valid_data.csv"

# Upload lagi dengan nama yang sama
curl -X POST http://localhost:8080/upload -F "file=@test-files/valid_data.csv"
```

**Expected:** 
- Upload pertama: 200 OK
- Upload kedua: 409 Conflict - "File with this name already exists"

---

## Test Max File Size

**Create large file:**
```bash
dd if=/dev/zero of=test-files/large_file.csv bs=1M count=11
```

**Test:**
```bash
curl -X POST http://localhost:8080/upload -F "file=@test-files/large_file.csv"
```

**Expected:** 400 Bad Request - "File size exceeds limit"

---

## Test Invalid File Type

**Create .txt file:**
```bash
echo "test" > test-files/test.txt
```

**Test:**
```bash
curl -X POST http://localhost:8080/upload -F "file=@test-files/test.txt"
```

**Expected:** 400 Bad Request - "File type not allowed"

---

## Monitoring

View logs in Grafana Loki:
```logql
{service_name="worker"} |= "validation"
```

View validation errors:
```logql
{service_name="worker", detected_level="error"} |= "Validation"
```

