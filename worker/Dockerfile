# Build stage
FROM golang:1.24-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git

# Set working directory
WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the worker application
WORKDIR /app/worker
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o /app/worker-bin main.go

# Final stage
FROM alpine:latest

# Install ca-certificates for HTTPS requests
RUN apk --no-cache add ca-certificates curl

WORKDIR /root/

# Copy the binary from builder
COPY --from=builder /app/worker-bin ./worker

# Expose metrics port
EXPOSE 2112

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
  CMD curl -f http://localhost:2112/metrics || exit 1

# Run the application
CMD ["./worker"]

