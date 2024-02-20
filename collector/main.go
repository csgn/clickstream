package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const (
	TOPIC    = "events"
	GIF_PATH = "./resources/__gc.gif"
)

func produce(p *kafka.Producer, b []byte) {
	topic := TOPIC
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          b,
	}

	p.Produce(msg, nil)
}

func HandlePixel(w http.ResponseWriter, r *http.Request, p *kafka.Producer) {
	if r.Method == http.MethodGet {
		values, err := url.ParseQuery(r.URL.Query().Encode())
		if err != nil {
			fmt.Println("Error parsing query string:", err)
			return
		}

		params := make(map[string]string)
		for key, value := range values {
			params[key] = value[0]
		}

		jsonString, err := json.Marshal(params)
		if err != nil {
			fmt.Println("Error marshaling to JSON:", err)
			return
		}

		produce(p, []byte(jsonString))
		http.ServeFile(w, r, GIF_PATH)
	}
}

func HandleEvent(w http.ResponseWriter, r *http.Request, p *kafka.Producer) {
	if r.Method == http.MethodPost {
		b, err := io.ReadAll(r.Body)
		if err != nil {
			return
		}

		produce(p, b)
	}
}

func main() {
	log.Println("Starting kafka producer.")
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost",
		"acks":              "all"})
	defer p.Close()

	if err != nil {
		log.Fatalln("Failed to create producer: %w\n", err)
		return
	}

	log.Println("Starting http server.")

	port := os.Getenv("PORT")
	host := fmt.Sprintf("127.0.0.1:%v", port)

	http.HandleFunc("/e", func(w http.ResponseWriter, r *http.Request) {
		HandleEvent(w, r, p)
	})

	http.HandleFunc("/pixel", func(w http.ResponseWriter, r *http.Request) {
		HandlePixel(w, r, p)
	})

	fmt.Printf(`
   ██████  ██████  ██      ██      ███████  ██████ ████████  ██████  ██████  
  ██      ██    ██ ██      ██      ██      ██         ██    ██    ██ ██   ██ 
  ██      ██    ██ ██      ██      █████   ██         ██    ██    ██ ██████  
  ██      ██    ██ ██      ██      ██      ██         ██    ██    ██ ██   ██ 
   ██████  ██████  ███████ ███████ ███████  ██████    ██     ██████  ██   ██ 

   Listening on http://%s
   To close connection CTRL+C
  `, host)

	http.ListenAndServe(host, nil)
}
