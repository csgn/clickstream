package producer

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func Init() (*kafka.Producer, error) {
	return kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost",
		"acks":              "all"})
}

func Produce(p *kafka.Producer, topic string, value []byte) error {
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          value,
	}

	return p.Produce(msg, nil)
}
