package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

const (
	DirtyEventsTopic = "dirty-events"
)

func validateAndProduce(p *kafka.Producer, value string) {
	var event Event
	var msg *kafka.Message

	dirtyEventsTopic := DirtyEventsTopic

	byteOfValue := []byte(value)

	err := json.Unmarshal(byteOfValue, &event)
	if err != nil {
		msg = &kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &dirtyEventsTopic, Partition: kafka.PartitionAny},
			Value:          byteOfValue,
		}
	} else {
		if err := event.Validate(); err != nil {
			msg = &kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &dirtyEventsTopic, Partition: kafka.PartitionAny},
				Value:          byteOfValue,
			}
		} else {
			topic := string(event.Channel)
			msg = &kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
				Value:          byteOfValue,
			}
		}
	}

	p.Produce(msg, nil)
}

func main() {
	fmt.Printf(`
    ██    ██  █████  ██      ██ ██████   █████  ████████  ██████  ██████  
    ██    ██ ██   ██ ██      ██ ██   ██ ██   ██    ██    ██    ██ ██   ██ 
    ██    ██ ███████ ██      ██ ██   ██ ███████    ██    ██    ██ ██████  
     ██  ██  ██   ██ ██      ██ ██   ██ ██   ██    ██    ██    ██ ██   ██ 
      ████   ██   ██ ███████ ██ ██████  ██   ██    ██     ██████  ██   ██ 
  `)

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost",
		"group.id":          "localhost",
		"auto.offset.reset": "earliest",
	})

	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost",
		"acks":              "all"})

	defer p.Close()
	defer c.Close()

	if err != nil {
		panic(err)
	}

	fmt.Println("Listening topics")

	topics := []string{
		"events",
	}

	err = c.SubscribeTopics(topics, nil)

	run := true
	for run {
		ev := c.Poll(int(time.Millisecond))
		switch e := ev.(type) {
		case *kafka.Message:
			validateAndProduce(p, string(e.Value))
		case kafka.Error:
			fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
			run = false
		}
	}
}
