package minio

import (
	"context"
	"fmt"
	"io"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/rs/zerolog/log"
	"github.com/rzfd/file-processing-system/internal/config"
)

type Client struct {
	client     *minio.Client
	bucketName string
}

// NewClient creates a new MinIO client
func NewClient(cfg *config.Config) (*Client, error) {
	log.Info().
		Str("endpoint", cfg.MinIOEndpoint).
		Str("bucket", cfg.MinIOBucketName).
		Bool("ssl", cfg.MinIOUseSSL).
		Msg("Connecting to MinIO")

	minioClient, err := minio.New(cfg.MinIOEndpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.MinIOAccessKeyID, cfg.MinIOSecretAccessKey, ""),
		Secure: cfg.MinIOUseSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create minio client: %w", err)
	}

	client := &Client{
		client:     minioClient,
		bucketName: cfg.MinIOBucketName,
	}

	// Ensure bucket exists
	ctx := context.Background()
	log.Info().
		Str("bucket", cfg.MinIOBucketName).
		Msg("Checking if bucket exists")
	exists, err := minioClient.BucketExists(ctx, cfg.MinIOBucketName)
	if err != nil {
		return nil, fmt.Errorf("failed to check bucket existence: %w", err)
	}

	if !exists {
		log.Info().
			Str("bucket", cfg.MinIOBucketName).
			Msg("Bucket does not exist, creating")
		err = minioClient.MakeBucket(ctx, cfg.MinIOBucketName, minio.MakeBucketOptions{})
		if err != nil {
			return nil, fmt.Errorf("failed to create bucket: %w", err)
		}
		log.Info().
			Str("bucket", cfg.MinIOBucketName).
			Msg("Bucket created successfully")
	} else {
		log.Info().
			Str("bucket", cfg.MinIOBucketName).
			Msg("Bucket already exists")
	}

	log.Info().Msg("MinIO client initialized successfully")
	return client, nil
}

// UploadFile uploads a file to MinIO
func (c *Client) UploadFile(ctx context.Context, objectName string, reader io.Reader, size int64, contentType string) error {
	log.Info().
		Str("object", objectName).
		Int64("size", size).
		Str("content_type", contentType).
		Msg("Uploading object to MinIO")

	info, err := c.client.PutObject(ctx, c.bucketName, objectName, reader, size, minio.PutObjectOptions{
		ContentType: contentType,
	})

	if err == nil {
		log.Info().
			Str("object", objectName).
			Int64("size", info.Size).
			Msg("Upload successful")
	}

	return err
}

// DownloadFile downloads a file from MinIO
func (c *Client) DownloadFile(ctx context.Context, objectName string) (io.ReadCloser, error) {
	log.Info().
		Str("object", objectName).
		Str("bucket", c.bucketName).
		Msg("Downloading object from MinIO")

	obj, err := c.client.GetObject(ctx, c.bucketName, objectName, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get object: %w", err)
	}

	log.Info().
		Str("object", objectName).
		Msg("Download initiated successfully")
	return obj, nil
}

// DeleteFile deletes a file from MinIO
func (c *Client) DeleteFile(ctx context.Context, objectName string) error {
	return c.client.RemoveObject(ctx, c.bucketName, objectName, minio.RemoveObjectOptions{})
}
