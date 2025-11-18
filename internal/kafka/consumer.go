package kafka

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/IBM/sarama"
	"github.com/rs/zerolog/log"
	"github.com/rzfd/file-processing-system/internal/config"
	"github.com/rzfd/file-processing-system/internal/models"
)

type Consumer struct {
	consumer sarama.ConsumerGroup
	topic    string
}

// NewConsumer creates a new Kafka consumer
func NewConsumer(cfg *config.Config) (*Consumer, error) {
	log.Info().
		Strs("brokers", cfg.KafkaBrokers).
		Str("consumer_group", cfg.KafkaConsumerGroup).
		Str("topic", cfg.KafkaTopic).
		Msg("Creating Kafka consumer")

	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategyRoundRobin()
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	consumer, err := sarama.NewConsumerGroup(cfg.KafkaBrokers, cfg.KafkaConsumerGroup, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka consumer: %w", err)
	}

	log.Info().Msg("Kafka consumer created successfully")

	return &Consumer{
		consumer: consumer,
		topic:    cfg.KafkaTopic,
	}, nil
}

// ConsumeMessages consumes messages from Kafka topic
func (c *Consumer) ConsumeMessages(handler func(*models.FileProcessingEvent) error) error {
	handlerWrapper := &consumerGroupHandler{
		handler: handler,
	}

	ctx := context.Background()
	for {
		err := c.consumer.Consume(ctx, []string{c.topic}, handlerWrapper)
		if err != nil {
			return fmt.Errorf("error from consumer: %w", err)
		}
	}
}

// Close closes the consumer
func (c *Consumer) Close() error {
	return c.consumer.Close()
}

type consumerGroupHandler struct {
	handler func(*models.FileProcessingEvent) error
}

func (h *consumerGroupHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *consumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *consumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		log.Info().
			Int32("partition", message.Partition).
			Int64("offset", message.Offset).
			Msg("Received message from Kafka")

		var event models.FileProcessingEvent
		if err := json.Unmarshal(message.Value, &event); err != nil {
			log.Error().
				Err(err).
				Msg("Failed to unmarshal Kafka message")
			session.MarkMessage(message, "")
			continue
		}

		log.Info().
			Int64("file_id", event.FileID).
			Str("filename", event.FileName).
			Msg("Processing event")

		if err := h.handler(&event); err != nil {
			log.Error().
				Err(err).
				Int64("file_id", event.FileID).
				Msg("Failed to process message")
			// Don't mark as processed if there's an error
			continue
		}

		log.Info().
			Int64("file_id", event.FileID).
			Msg("Message processed successfully, marking as consumed")
		session.MarkMessage(message, "")
	}
	return nil
}
