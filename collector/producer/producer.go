package producer

import (
	"collector/event"
	"encoding/json"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func Init() (*kafka.Producer, error) {
	return kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost",
		"acks":              "all"})
}

func Produce(p *kafka.Producer, topic string, e *event.Event) error {
	value, err := json.Marshal(e)
	if err != nil {
		return err
	}

	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          value,
	}

	return p.Produce(msg, nil)
}
