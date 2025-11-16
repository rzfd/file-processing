package config

import (
	"fmt"
	"os"
	"strconv"
)

// Config holds all configuration for the application
type Config struct {
	// Server
	ServerPort string

	// PostgreSQL
	DBHost     string
	DBPort     string
	DBUser     string
	DBPassword string
	DBName     string
	DBSSLMode  string

	// MinIO
	MinIOEndpoint        string
	MinIOAccessKeyID     string
	MinIOSecretAccessKey string
	MinIOUseSSL          bool
	MinIOBucketName      string

	// Kafka
	KafkaBrokers      []string
	KafkaTopic        string
	KafkaConsumerGroup string

	// File Processing
	MaxFileSize      int64
	AllowedExtensions []string
	BatchSize        int

	// Prometheus
	MetricsPort string
}

// LoadConfig loads configuration from environment variables
func LoadConfig() *Config {
	minioUseSSL, _ := strconv.ParseBool(getEnv("MINIO_USE_SSL", "false"))
	maxFileSize, _ := strconv.ParseInt(getEnv("MAX_FILE_SIZE", "10485760"), 10, 64) // 10MB default
	batchSize, _ := strconv.Atoi(getEnv("BATCH_SIZE", "1000"))

	return &Config{
		ServerPort:           getEnv("PORT", "8080"),
		DBHost:               getEnv("DB_HOST", "postgres"),
		DBPort:               getEnv("DB_PORT", "5432"),
		DBUser:               getEnv("DB_USER", "postgres"),
		DBPassword:           getEnv("DB_PASSWORD", "postgres"),
		DBName:               getEnv("DB_NAME", "fileprocessing"),
		DBSSLMode:            getEnv("DB_SSLMODE", "disable"),
		MinIOEndpoint:        getEnv("MINIO_ENDPOINT", "minio:9000"),
		MinIOAccessKeyID:     getEnv("MINIO_ACCESS_KEY_ID", "minioadmin"),
		MinIOSecretAccessKey: getEnv("MINIO_SECRET_ACCESS_KEY", "minioadmin"),
		MinIOUseSSL:          minioUseSSL,
		MinIOBucketName:      getEnv("MINIO_BUCKET_NAME", "files"),
		KafkaBrokers:         []string{getEnv("KAFKA_BROKERS", "kafka:9092")},
		KafkaTopic:           getEnv("KAFKA_TOPIC", "file-processing"),
		KafkaConsumerGroup:   getEnv("KAFKA_CONSUMER_GROUP", "file-worker-group"),
		MaxFileSize:          maxFileSize,
		AllowedExtensions:    []string{".csv", ".xlsx", ".xls"},
		BatchSize:            batchSize,
		MetricsPort:          getEnv("METRICS_PORT", "2112"),
	}
}

// GetDSN returns PostgreSQL connection string
func (c *Config) GetDSN() string {
	return fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		c.DBHost, c.DBPort, c.DBUser, c.DBPassword, c.DBName, c.DBSSLMode)
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

