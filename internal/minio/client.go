package minio

import (
	"context"
	"fmt"
	"io"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/rzfd/file-processing-system/internal/config"
)

type Client struct {
	client     *minio.Client
	bucketName string
}

// NewClient creates a new MinIO client
func NewClient(cfg *config.Config) (*Client, error) {
	fmt.Printf("[MINIO] Connecting to MinIO at %s\n", cfg.MinIOEndpoint)
	fmt.Printf("[MINIO] Bucket: %s, SSL: %v\n", cfg.MinIOBucketName, cfg.MinIOUseSSL)

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
	fmt.Printf("[MINIO] Checking if bucket '%s' exists...\n", cfg.MinIOBucketName)
	exists, err := minioClient.BucketExists(ctx, cfg.MinIOBucketName)
	if err != nil {
		return nil, fmt.Errorf("failed to check bucket existence: %w", err)
	}

	if !exists {
		fmt.Printf("[MINIO] Bucket does not exist, creating...\n")
		err = minioClient.MakeBucket(ctx, cfg.MinIOBucketName, minio.MakeBucketOptions{})
		if err != nil {
			return nil, fmt.Errorf("failed to create bucket: %w", err)
		}
		fmt.Printf("[MINIO] Bucket '%s' created successfully\n", cfg.MinIOBucketName)
	} else {
		fmt.Printf("[MINIO] Bucket '%s' already exists\n", cfg.MinIOBucketName)
	}

	fmt.Printf("[MINIO] Client initialized successfully\n")
	return client, nil
}

// UploadFile uploads a file to MinIO
func (c *Client) UploadFile(ctx context.Context, objectName string, reader io.Reader, size int64, contentType string) error {
	fmt.Printf("[MINIO] Uploading object: %s (size: %d bytes, type: %s)\n", objectName, size, contentType)

	info, err := c.client.PutObject(ctx, c.bucketName, objectName, reader, size, minio.PutObjectOptions{
		ContentType: contentType,
	})

	if err == nil {
		fmt.Printf("[MINIO] Upload successful: %s (%d bytes)\n", objectName, info.Size)
	}

	return err
}

// DownloadFile downloads a file from MinIO
func (c *Client) DownloadFile(ctx context.Context, objectName string) (io.ReadCloser, error) {
	fmt.Printf("[MINIO] Downloading object: %s from bucket: %s\n", objectName, c.bucketName)

	obj, err := c.client.GetObject(ctx, c.bucketName, objectName, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get object: %w", err)
	}

	fmt.Printf("[MINIO] Download initiated successfully\n")
	return obj, nil
}

// DeleteFile deletes a file from MinIO
func (c *Client) DeleteFile(ctx context.Context, objectName string) error {
	return c.client.RemoveObject(ctx, c.bucketName, objectName, minio.RemoveObjectOptions{})
}
