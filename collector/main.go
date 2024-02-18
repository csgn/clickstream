package main

import (
	"fmt"
	"log"
	"net/http"
	"os"

	"collector/event"
	"collector/producer"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gin-gonic/gin"
)

const GIF_PATH = "./resources/__gc.gif"

func HandlePixel(c *gin.Context, p *kafka.Producer) {
	e := event.Event{}
	if err := c.ShouldBindQuery(&e); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"message": err.Error(),
		})
		return
	}

	if err := e.Validate(); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"message": err.Error(),
		})
		return
	}

	if err := producer.Produce(p, string(e.Channel), &e); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"message": err.Error(),
		})
		return
	}

	log.Println("Event sent success")

	http.ServeFile(c.Writer, c.Request, GIF_PATH)
}

func HandleEvent(c *gin.Context, p *kafka.Producer) {
	e := event.Event{}
	if err := c.ShouldBindJSON(&e); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"message": err.Error(),
		})
		return
	}

	if err := e.Validate(); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"message": err.Error(),
		})
		return
	}

	if err := producer.Produce(p, string(e.Channel), &e); err != nil {
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

	r.GET("/pixel", func(ctx *gin.Context) {
		HandlePixel(ctx, p)
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
