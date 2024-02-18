package producer

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func Init() (*kafka.Producer, error) {
	return kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost",
		"client.id":         "localhost",
		"acks":              "all"})
}

func Produce(p *kafka.Producer, topic string, value []byte) error {
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          value,
	}

	err := p.Produce(msg, nil)
	if err != nil {
		return fmt.Errorf("Failed to produce message: %w", err)
	}

	return nil
}
