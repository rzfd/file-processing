package kafka

import (
	"encoding/json"
	"fmt"

	"github.com/IBM/sarama"
	"github.com/rs/zerolog/log"
	"github.com/rzfd/file-processing-system/internal/config"
	"github.com/rzfd/file-processing-system/internal/models"
)

type Producer struct {
	producer sarama.SyncProducer
	topic    string
}

// NewProducer creates a new Kafka producer
func NewProducer(cfg *config.Config) (*Producer, error) {
	log.Info().
		Strs("brokers", cfg.KafkaBrokers).
		Str("topic", cfg.KafkaTopic).
		Msg("Creating Kafka producer")

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5

	producer, err := sarama.NewSyncProducer(cfg.KafkaBrokers, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka producer: %w", err)
	}

	log.Info().Msg("Kafka producer created successfully")

	return &Producer{
		producer: producer,
		topic:    cfg.KafkaTopic,
	}, nil
}

// PublishFileEvent publishes a file processing event to Kafka
func (p *Producer) PublishFileEvent(event *models.FileProcessingEvent) error {
	log.Info().
		Int64("file_id", event.FileID).
		Str("filename", event.FileName).
		Str("event_type", event.EventType).
		Msg("Publishing event to Kafka")

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

	log.Info().
		Int32("partition", partition).
		Int64("offset", offset).
		Int64("file_id", event.FileID).
		Msg("Message sent successfully to Kafka")
	return nil
}

// Close closes the producer
func (p *Producer) Close() error {
	return p.producer.Close()
}
