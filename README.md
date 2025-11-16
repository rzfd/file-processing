# 🚀 File Processing Pipeline System

Sistem microservices untuk memproses file CSV/XLSX dengan arsitektur event-driven menggunakan Go, MinIO, PostgreSQL, Kafka, Prometheus, Grafana, Loki, dan Fluent Bit.

## 📋 Daftar Isi

- [Fitur](#-fitur)
- [Arsitektur](#-arsitektur)
- [Teknologi](#-teknologi)
- [Prasyarat](#-prasyarat)
- [Instalasi](#-instalasi)
- [Penggunaan](#-penggunaan)
- [API Endpoints](#-api-endpoints)
- [Monitoring](#-monitoring)
- [Logging](#-logging)
- [Troubleshooting](#-troubleshooting)

## ✨ Fitur

- ✅ Upload file CSV/XLSX via REST API
- ✅ Penyimpanan file di MinIO (S3-compatible)
- ✅ Event-driven processing dengan Kafka
- ✅ Batch processing untuk performa optimal
- ✅ Metadata tracking di PostgreSQL
- ✅ Monitoring dengan Prometheus & Grafana
- ✅ Centralized logging dengan Loki & Fluent Bit
- ✅ **Structured logging dengan Zerolog (INF, WRN, ERR, DBG)**
- ✅ Health checks untuk semua services
- ✅ Auto-retry mechanism
- ✅ Horizontal scalability

## 🏗️ Arsitektur

```
┌─────────────┐      ┌──────────┐      ┌─────────────┐
│   Client    │─────▶│ Backend  │─────▶│   MinIO     │
└─────────────┘      │  (API)   │      │  (Storage)  │
                     └──────────┘      └─────────────┘
                          │                    
                          │ Publish Event      
                          ▼                    
                     ┌──────────┐             
                     │  Kafka   │             
                     └──────────┘             
                          │                    
                          │ Consume Event      
                          ▼                    
                     ┌──────────┐      ┌─────────────┐
                     │  Worker  │─────▶│ PostgreSQL  │
                     └──────────┘      │  (Database) │
                          │            └─────────────┘
                          │                    
                          ▼                    
                     ┌──────────┐             
                     │  MinIO   │             
                     └──────────┘             
                                               
┌────────────────────────────────────────────────────┐
│              Monitoring & Logging                   │
├────────────────────────────────────────────────────┤
│  Prometheus  │  Grafana  │  Loki  │  Fluent Bit   │
└────────────────────────────────────────────────────┘
```

## 🛠️ Teknologi

- **Backend**: Go 1.24 + Zerolog
- **Storage**: MinIO (S3-compatible)
- **Database**: PostgreSQL 15
- **Message Broker**: Apache Kafka + Zookeeper
- **Monitoring**: Prometheus + Grafana
- **Logging**: Loki + Fluent Bit + Zerolog
- **Containerization**: Docker + Docker Compose

## 📦 Prasyarat

- Docker 20.10+
- Docker Compose 2.0+
- 4GB RAM minimum
- 10GB disk space

## 🚀 Instalasi

### 1. Clone Repository

```bash
git clone <repository-url>
cd file-processing-system
```

### 2. Start All Services

```bash
# Start semua services
docker-compose up -d

# Check status
docker-compose ps

# View logs
docker-compose logs -f
```

### 3. Verify Services

```bash
# Backend health
curl http://localhost:8080/health

# MinIO console
open http://localhost:9001
# User: minioadmin, Password: minioadmin

# Grafana dashboard
open http://localhost:3000
# User: admin, Password: admin

# Prometheus
open http://localhost:9090
```

## 💻 Penggunaan

### Upload File

```bash
# Upload CSV file
curl -X POST http://localhost:8080/upload \
  -F "file=@data.csv" \
  -F "filename=data.csv"

# Response
{
  "id": 1,
  "file_name": "data.csv",
  "status": "pending"
}
```

### Check File Status

```bash
# Get specific file status
curl http://localhost:8080/files/1 | jq .

# Response
{
  "id": 1,
  "file_name": "data.csv",
  "file_size": 1024,
  "content_type": "text/csv",
  "status": "completed",
  "record_count": 100,
  "created_at": "2025-11-16T10:00:00Z",
  "processed_at": "2025-11-16T10:00:05Z"
}
```

### List Files

```bash
# List all files (default limit: 10)
curl http://localhost:8080/files | jq .

# List with filters
curl "http://localhost:8080/files?status=completed&limit=20" | jq .

# Response
{
  "files": [...],
  "count": 10
}
```

## 📡 API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Health check |
| POST | `/upload` | Upload file |
| GET | `/files` | List files |
| GET | `/files/{id}` | Get file status |
| GET | `/metrics` | Prometheus metrics |

### Query Parameters

**GET /files**
- `status` - Filter by status (pending, processing, completed, failed)
- `limit` - Limit results (1-100, default: 10)

## 📊 Monitoring

### Grafana Dashboards

Akses: http://localhost:3000 (admin/admin)

**Metrics Dashboard** sudah include:
- HTTP Request Rate & Count
- Files Processed (Total & Rate)
- Records Processed (Total & Rate)
- Request Duration (p50, p90, p99)
- Processing Duration (p50, p90, p99)

**Logs Dashboard** (Loki):
- Upload activity logs
- Worker processing logs
- Error logs
- Database operations

### Prometheus Metrics

Akses: http://localhost:9090

Available metrics:
- `http_requests_total` - Total HTTP requests
- `http_request_duration_seconds` - Request duration
- `processed_files_total` - Total files processed
- `processed_records_total` - Total records processed
- `file_processing_duration_seconds` - Processing duration

### Loki Logs dengan Zerolog Levels

Query logs di Grafana Explore dengan LogQL:

```logql
# INFO logs
{job="fluentbit"} |= "INF"

# ERROR logs
{job="fluentbit"} |= "ERR"

# WARNING logs
{job="fluentbit"} |= "WRN"

# DEBUG logs
{job="fluentbit"} |= "DBG"

# Backend errors
{job="fluentbit", container_name="/file-processing-backend"} |= "ERR"

# Worker info logs
{job="fluentbit", container_name="/file-processing-worker"} |= "INF"
```

Lihat [GRAFANA_QUERIES.md](GRAFANA_QUERIES.md) untuk query lengkap.

## 📝 Logging

Sistem menggunakan **Zerolog** untuk structured logging dengan level:

- **INF** (INFO) - Informasi normal operasi
- **WRN** (WARN) - Warning yang perlu diperhatikan
- **ERR** (ERROR) - Error yang terjadi
- **DBG** (DEBUG) - Debug information
- **FTL** (FATAL) - Fatal error (app stops)

Format log:
```
[timestamp] [LEVEL] [message] [key=value]
```

Contoh:
```
2025-11-16T15:54:08Z INF Backend server starting port=8080
2025-11-16T15:54:08Z ERR Failed to connect error="connection refused"
```

### View Logs

```bash
# All logs
docker-compose logs -f

# Specific service
docker-compose logs -f backend
docker-compose logs -f worker

# Filter by level
docker logs file-processing-backend | grep "INF"
docker logs file-processing-backend | grep "ERR"
docker logs file-processing-worker | grep "WRN"
```

Lihat [ZEROLOG_GUIDE.md](ZEROLOG_GUIDE.md) untuk dokumentasi lengkap.

## 🧪 Testing

### Test Upload

```bash
# Run test script
./scripts/test-upload.sh
```

### Manual Test

```bash
# Create test CSV
cat > test.csv << EOF
id,name,email,age
1,John Doe,john@example.com,30
2,Jane Smith,jane@example.com,25
EOF

# Upload
curl -X POST http://localhost:8080/upload \
  -F "file=@test.csv" \
  -F "filename=test.csv"
```

## 🔧 Troubleshooting

### Services Not Starting

```bash
# Check logs
docker-compose logs

# Restart specific service
docker-compose restart backend

# Rebuild and restart
docker-compose up -d --build
```

### Database Connection Issues

```bash
# Check PostgreSQL
docker-compose logs postgres

# Connect to database
docker-compose exec postgres psql -U postgres -d fileprocessing

# Check tables
\dt
```

### Kafka Issues

```bash
# Check Kafka logs
docker-compose logs kafka

# Check Zookeeper
docker-compose logs zookeeper

# List topics
docker-compose exec kafka kafka-topics --list --bootstrap-server localhost:9092
```

### MinIO Issues

```bash
# Check MinIO logs
docker-compose logs minio

# Access MinIO console
open http://localhost:9001
```

### Loki/Fluent Bit Issues

```bash
# Check Loki
curl http://localhost:3100/ready

# Check Fluent Bit
curl http://localhost:2020/api/v1/health

# View logs
docker-compose logs loki
docker-compose logs fluent-bit
```

## 🛑 Stopping Services

```bash
# Stop all services
docker-compose down

# Stop and remove volumes
docker-compose down -v

# Stop specific service
docker-compose stop backend
```

## 📚 Additional Documentation

- [ZEROLOG_GUIDE.md](ZEROLOG_GUIDE.md) - Zerolog structured logging guide
- [GRAFANA_QUERIES.md](GRAFANA_QUERIES.md) - Grafana Loki query examples
- [Makefile](Makefile) - Convenient commands

## 🔗 Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| Backend API | http://localhost:8080 | - |
| Grafana | http://localhost:3000 | admin/admin |
| Prometheus | http://localhost:9090 | - |
| MinIO Console | http://localhost:9001 | minioadmin/minioadmin |
| MinIO API | http://localhost:9000 | - |
| Loki | http://localhost:3100 | - |
| Fluent Bit | http://localhost:2020 | - |
| PostgreSQL | localhost:5432 | postgres/postgres |
| Kafka | localhost:9092 | - |

## 📈 Performance

- **Throughput**: ~1000 records/second
- **Batch Size**: 1000 records per batch
- **Max File Size**: Unlimited (configurable)
- **Concurrent Workers**: Scalable (default: 1)

## 🔐 Security Notes

⚠️ **Production Considerations**:
- Change default passwords
- Enable TLS/SSL
- Implement authentication
- Set up network policies
- Configure firewall rules
- Enable audit logging
- Implement rate limiting

## 📄 License

MIT License

## 👥 Contributors

- Your Name

---

**Need Help?** Check the troubleshooting section or open an issue.

