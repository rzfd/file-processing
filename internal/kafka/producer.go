package kafka

import (
	"encoding/json"
	"fmt"

	"github.com/IBM/sarama"
	"github.com/rzfd/file-processing-system/internal/config"
	"github.com/rzfd/file-processing-system/internal/models"
)

type Producer struct {
	producer sarama.SyncProducer
	topic    string
}

// NewProducer creates a new Kafka producer
func NewProducer(cfg *config.Config) (*Producer, error) {
	fmt.Printf("[KAFKA] Creating producer for brokers: %v\n", cfg.KafkaBrokers)
	fmt.Printf("[KAFKA] Topic: %s\n", cfg.KafkaTopic)

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5

	producer, err := sarama.NewSyncProducer(cfg.KafkaBrokers, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka producer: %w", err)
	}

	fmt.Printf("[KAFKA] Producer created successfully\n")

	return &Producer{
		producer: producer,
		topic:    cfg.KafkaTopic,
	}, nil
}

// PublishFileEvent publishes a file processing event to Kafka
func (p *Producer) PublishFileEvent(event *models.FileProcessingEvent) error {
	fmt.Printf("[KAFKA] Publishing event: FileID=%d, FileName=%s, EventType=%s\n",
		event.FileID, event.FileName, event.EventType)

	eventJSON, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	msg := &sarama.ProducerMessage{
		Topic: p.topic,
		Value: sarama.StringEncoder(eventJSON),
	}

	partition, offset, err := p.producer.SendMessage(msg)
	if err != nil {
		return fmt.Errorf("failed to send message: %w", err)
	}

	fmt.Printf("[KAFKA] Message sent successfully to partition %d at offset %d\n", partition, offset)
	return nil
}

// Close closes the producer
func (p *Producer) Close() error {
	return p.producer.Close()
}
