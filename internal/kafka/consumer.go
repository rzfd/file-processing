package kafka

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/IBM/sarama"
	"github.com/rzfd/file-processing-system/internal/config"
	"github.com/rzfd/file-processing-system/internal/models"
)

type Consumer struct {
	consumer sarama.ConsumerGroup
	topic    string
}

// NewConsumer creates a new Kafka consumer
func NewConsumer(cfg *config.Config) (*Consumer, error) {
	fmt.Printf("[KAFKA] Creating consumer for brokers: %v\n", cfg.KafkaBrokers)
	fmt.Printf("[KAFKA] Consumer Group: %s\n", cfg.KafkaConsumerGroup)
	fmt.Printf("[KAFKA] Topic: %s\n", cfg.KafkaTopic)

	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategyRoundRobin()
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	consumer, err := sarama.NewConsumerGroup(cfg.KafkaBrokers, cfg.KafkaConsumerGroup, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka consumer: %w", err)
	}

	fmt.Printf("[KAFKA] Consumer created successfully\n")

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
		fmt.Printf("[KAFKA] Received message from partition %d at offset %d\n",
			message.Partition, message.Offset)

		var event models.FileProcessingEvent
		if err := json.Unmarshal(message.Value, &event); err != nil {
			fmt.Printf("[KAFKA] ERROR: Failed to unmarshal message: %v\n", err)
			session.MarkMessage(message, "")
			continue
		}

		fmt.Printf("[KAFKA] Processing event: FileID=%d, FileName=%s\n", event.FileID, event.FileName)

		if err := h.handler(&event); err != nil {
			fmt.Printf("[KAFKA] ERROR: Failed to process message: %v\n", err)
			// Don't mark as processed if there's an error
			continue
		}

		fmt.Printf("[KAFKA] Message processed successfully, marking as consumed\n")
		session.MarkMessage(message, "")
	}
	return nil
}
