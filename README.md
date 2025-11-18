# 🚀 File Processing Pipeline System

Sistem microservices untuk memproses file CSV/XLSX dengan arsitektur event-driven.

## 📋 Daftar Isi

- [Fitur](#-fitur)
- [Arsitektur](#-arsitektur)
- [Quick Start](#-quick-start)
- [API Endpoints](#-api-endpoints)
- [Monitoring & Logging](#-monitoring--logging)
- [Troubleshooting](#-troubleshooting)

## ✨ Fitur

- ✅ Upload & process file CSV/XLSX via REST API
- ✅ Event-driven architecture dengan Kafka
- ✅ Object storage dengan MinIO (S3-compatible)
- ✅ Batch processing untuk performa optimal
- ✅ Structured logging dengan Zerolog (JSON format)
- ✅ Monitoring dengan Prometheus & Grafana
- ✅ Centralized logging dengan Loki & Fluent Bit
- ✅ Distributed tracing dengan trace_id

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

## 🛠️ Tech Stack

| Component | Technology |
|-----------|------------|
| **Backend** | Go 1.24 + Zerolog |
| **Storage** | MinIO (S3-compatible) |
| **Database** | PostgreSQL 15 |
| **Message Broker** | Apache Kafka |
| **Monitoring** | Prometheus + Grafana |
| **Logging** | Loki + Fluent Bit |
| **Container** | Docker Compose |

## 🚀 Quick Start

### Prasyarat
- Docker 20.10+
- Docker Compose 2.0+
- 4GB RAM minimum

### Start Services

```bash
docker-compose up -d
```

### Access Services

| Service | URL | Credentials |
|---------|-----|-------------|
| **Backend API** | http://localhost:8080 | - |
| **Grafana** | http://localhost:3000 | admin/admin |
| **pgAdmin** | http://localhost:5050 | admin@admin.com/admin |
| **MinIO Console** | http://localhost:9001 | minioadmin/minioadmin |
| **Prometheus** | http://localhost:9090 | - |

## 💻 Usage

### Upload File

```bash
# Upload CSV/XLSX file
curl -X POST http://localhost:8080/upload -F "file=@data.csv"

# Check file status
curl http://localhost:8080/files/1

# List all files
curl http://localhost:8080/files?status=completed&limit=10
```

## 📡 API Endpoints

| Method | Endpoint | Description | Query Params |
|--------|----------|-------------|--------------|
| GET | `/health` | Health check | - |
| POST | `/upload` | Upload file | - |
| GET | `/files` | List files | `status`, `limit` |
| GET | `/files/{id}` | Get file status | - |
| GET | `/metrics` | Prometheus metrics | - |

## 📊 Monitoring & Logging

### Grafana (http://localhost:3000)

**📊 Pre-built Dashboards:**

1. **File Processing System - Overview**
   - URL: http://localhost:3000/d/file-processing-overview
   - Total files, completed, failed statistics
   - Success rate gauge
   - Files by status (pie chart)
   - HTTP request rate
   - Processing duration (p50/p90/p99)
   - Latest files table (from PostgreSQL)
   - Average processing time by content type

**Logs Dashboard (Loki):**

Query examples dengan LogQL:

```logql
# Filter by service
{job="fluentbit", service_name="backend"}
{job="fluentbit", service_name="worker"}

# Filter by log level
{job="fluentbit", detected_level="error"}
{job="fluentbit", detected_level="warn"}

# Search by content
{job="fluentbit"} | json | message =~ "(?i)upload"

# Track specific file
{job="fluentbit"} | json | file_id=21

# End-to-end tracing
{job="fluentbit"} | json | trace_id="..."
```

### Available Labels
- `job` - fluentbit
- `service_name` - backend / worker
- `detected_level` - info / warn / error

### Available Fields (access with `| json`)
- `level`, `message`, `time`
- `request_id`, `trace_id`
- `file_id`, `filename`
- All structured log fields

### Prometheus Metrics (http://localhost:9090)

| Metric | Description |
|--------|-------------|
| `http_requests_total` | Total HTTP requests |
| `http_request_duration_seconds` | Request latency |
| `processed_files_total` | Total files processed |
| `processed_records_total` | Total records processed |
| `file_processing_duration_seconds` | Processing duration |

## 🔧 Troubleshooting

### View Logs

```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f backend
docker-compose logs -f worker
```

### Restart Services

```bash
# Restart all
docker-compose restart

# Restart specific service
docker-compose restart backend

# Rebuild and restart
docker-compose up -d --build
```

### Common Issues

**Services not starting:**
```bash
docker-compose down && docker-compose up -d
```

**Database issues:**
```bash
docker-compose exec postgres psql -U postgres -d fileprocessing
```

**Check Kafka topics:**
```bash
docker-compose exec kafka kafka-topics --list --bootstrap-server localhost:9092
```

### Stop Services

```bash
# Stop all
docker-compose down

# Stop and remove volumes
docker-compose down -v
```

---

## 📝 Notes

**Performance:**
- Throughput: ~1000 records/second
- Batch size: 1000 records per batch
- Supports horizontal scaling

**Security (Production):**
- ⚠️ Change default passwords
- ⚠️ Enable TLS/SSL
- ⚠️ Implement authentication & authorization

---

**Need Help?** Check logs atau open an issue.

