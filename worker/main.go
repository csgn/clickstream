package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func saveAsJson(key string, value string) {
	jsonData, err := json.Marshal(value)
	if err != nil {
		fmt.Println("error marshaling to JSON: ", err)
		return
	}

	outputFilePath := fmt.Sprintf("/tmp/clickstream/%s-%v.json", key, time.Now().UTC().UnixMilli())
	err = os.WriteFile(outputFilePath, jsonData, 0644)
	if err != nil {
		fmt.Println("Error writing to file:", err)
		return
	}

	fmt.Println("Event successfully written to ", outputFilePath)
}

func main() {
	fmt.Printf(`
    ██     ██  ██████  ██████  ██   ██ ███████ ██████  
    ██     ██ ██    ██ ██   ██ ██  ██  ██      ██   ██ 
    ██  █  ██ ██    ██ ██████  █████   █████   ██████  
    ██ ███ ██ ██    ██ ██   ██ ██  ██  ██      ██   ██ 
     ███ ███   ██████  ██   ██ ██   ██ ███████ ██   ██ 
  `)

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost",
		"group.id":          "localhost",
		"auto.offset.reset": "earliest",
	})

	defer c.Close()

	if err != nil {
		panic(err)
	}

	fmt.Println("Listening topics")

	topics := []string{
		"web",
		"mobile",
		"mobile-web",
	}

	err = c.SubscribeTopics(topics, nil)

	run := true
	for run {
		ev := c.Poll(int(time.Millisecond))
		switch e := ev.(type) {
		case *kafka.Message:
			key := string(*e.TopicPartition.Topic)
			value := string(e.Value)
			saveAsJson(key, value)
		case kafka.Error:
			fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
			run = false
		}
	}
}
