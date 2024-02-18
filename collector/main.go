package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"os"

	"collector/event"
	"collector/producer"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gin-gonic/gin"
)

func HandleEvent(c *gin.Context, p *kafka.Producer) {
	b, err := io.ReadAll(c.Request.Body)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"message": err.Error(),
		})
		return
	}

	event, err := event.Unmarshal(b)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"message": err.Error(),
		})
		return
	}

	fmt.Printf("Event{Variant=%s}\n", event.Variant)
}

func main() {
	log.Println("Starting kafka producer.")
	p, err := producer.Init()
	if err != nil {
		log.Fatalln("Failed to create producer: %w\n", err)
	}

	log.Println("Starting http server.")

	port := os.Getenv("PORT")
	listeningOn := fmt.Sprintf("127.0.0.1:%v", port)

	r := gin.Default()

	r.POST("/e", func(ctx *gin.Context) {
		HandleEvent(ctx, p)
	})

	log.Println("Started on port", port)
	fmt.Printf("Listening on http://%s\nTo close connection CTRL+C\n", listeningOn)

	r.Run(listeningOn)

}
