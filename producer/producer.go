package main

import (
	"encoding/json"
	"fmt"

	"github.com/IBM/sarama"
	"github.com/gofiber/fiber/v2"
)

type Comment struct {
	Text string `form:"text" json:"text"`
}

func main() {
	app := fiber.New()

	api := app.Group("/api/v1")
	api.Post("/comment", createComment)
	app.Listen(":3000")
}

func ConnectProducer(brokers []string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Retry.Backoff = 100
	config.Version = sarama.V0_11_0_0

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, err
	}
	return producer, nil
}

func PushCommentToQueue(topic string, message []byte) error {
	brokersUrls := []string{"localhost:29092"}
	producer, err := ConnectProducer(brokersUrls)

	if err != nil {
		return err
	}

	defer producer.Close()
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		return err
	}

	fmt.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n", topic, partition, offset)
	return nil
}

func createComment(c *fiber.Ctx) error {
	var comment Comment
	if err := c.BodyParser(&comment); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(&fiber.Map{
			"status": "error", 
			"message": "Review your input", 
			"data": err,
		})
	}
	
	commentInBytes, err := json.Marshal(comment)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(&fiber.Map{
			"status": "error", 
			"message": "Couldn't convert to JSON", 
			"data": err,
		})
	}
	PushCommentToQueue("comment", commentInBytes)

	return c.JSON(&fiber.Map{
		"status": "success", 
		"message": "Comment pushed succesfully", 
		"comment": comment,
	})
	
}