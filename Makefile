.PHONY: help build up down logs clean test

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

build: ## Build all services
	docker-compose build

up: ## Start all services
	docker-compose up -d

down: ## Stop all services
	docker-compose down

logs: ## Show logs for all services
	docker-compose logs -f

logs-backend: ## Show backend logs
	docker-compose logs -f backend

logs-worker: ## Show worker logs
	docker-compose logs -f worker

logs-kafka: ## Show kafka logs
	docker-compose logs -f kafka

ps: ## Show running services
	docker-compose ps

clean: ## Remove all containers and volumes
	docker-compose down -v

clean-all: ## Remove all containers, volumes, and images
	docker-compose down -v --rmi all

restart: ## Restart all services
	docker-compose restart

restart-backend: ## Restart backend service
	docker-compose restart backend

restart-worker: ## Restart worker service
	docker-compose restart worker

test-upload: ## Test file upload with sample CSV
	@echo "Creating sample CSV file..."
	@echo "name,age,email" > /tmp/sample.csv
	@echo "John Doe,30,john@example.com" >> /tmp/sample.csv
	@echo "Jane Smith,25,jane@example.com" >> /tmp/sample.csv
	@echo "Bob Johnson,35,bob@example.com" >> /tmp/sample.csv
	@echo "Uploading file..."
	@curl -X POST http://localhost:8080/upload -F "file=@/tmp/sample.csv" -v
	@rm /tmp/sample.csv

health: ## Check backend health
	@curl http://localhost:8080/health

metrics-backend: ## Show backend metrics
	@curl http://localhost:2112/metrics

metrics-worker: ## Show worker metrics
	@curl http://localhost:2113/metrics

db-connect: ## Connect to PostgreSQL
	docker exec -it file-processing-postgres psql -U postgres -d fileprocessing

db-query: ## Query file metadata
	docker exec -it file-processing-postgres psql -U postgres -d fileprocessing -c "SELECT * FROM file_metadata;"

kafka-topics: ## List Kafka topics
	docker exec -it file-processing-kafka kafka-topics --list --bootstrap-server localhost:9092

kafka-consumer-groups: ## Show Kafka consumer groups
	docker exec -it file-processing-kafka kafka-consumer-groups --bootstrap-server localhost:9092 --list

scale-worker: ## Scale worker to 3 instances
	docker-compose up -d --scale worker=3

dev-backend: ## Run backend locally
	@echo "Make sure dependencies are running: make up-deps"
	@export DB_HOST=localhost && \
	export MINIO_ENDPOINT=localhost:9000 && \
	export KAFKA_BROKERS=localhost:9092 && \
	go run backend/main.go

dev-worker: ## Run worker locally
	@echo "Make sure dependencies are running: make up-deps"
	@export DB_HOST=localhost && \
	export MINIO_ENDPOINT=localhost:9000 && \
	export KAFKA_BROKERS=localhost:9092 && \
	go run worker/main.go

up-deps: ## Start only dependencies (postgres, minio, kafka, zookeeper)
	docker-compose up -d postgres minio kafka zookeeper prometheus grafana

generate-load: ## Generate load for testing (default 10 requests)
	@bash scripts/generate-load.sh $(or $(n),10)

grafana: ## Open Grafana dashboard
	@echo "Opening Grafana at http://localhost:3000"
	@echo "Username: admin"
	@echo "Password: admin"
	@command -v xdg-open > /dev/null && xdg-open http://localhost:3000 || open http://localhost:3000 || echo "Please open http://localhost:3000 manually"

postgres-exporter: ## Start postgres exporter
	docker-compose up -d postgres-exporter

metrics-postgres: ## Show postgres exporter metrics
	@curl http://localhost:9187/metrics

