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

	e, err := event.Unmarshal(b)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"message": err.Error(),
		})
		return
	}

	marsheled_event, err := event.Marshal(e)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"message": err.Error(),
		})
		return
	}

	err = producer.Produce(p, string(e.Channel), marsheled_event)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"message": err.Error(),
		})
		return
	}

	log.Println("Event sent success")
}

func main() {
	log.Println("Starting kafka producer.")
	p, err := producer.Init()
	defer p.Close()

	if err != nil {
		log.Fatalln("Failed to create producer: %w\n", err)
		return
	}

	log.Println("Starting http server.")

	port := os.Getenv("PORT")
	host := fmt.Sprintf("127.0.0.1:%v", port)

	r := gin.Default()

	r.POST("/e", func(ctx *gin.Context) {
		HandleEvent(ctx, p)
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

	r.Run(host)
}
