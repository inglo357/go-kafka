package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
)

func main() {
	topic := "comment"
	worker, err := connectConsumer([]string{"localhost:29092"})

	if err != nil {
		panic(err)
	}

	consumer, err := worker.ConsumePartition(topic, 0, sarama.OffsetOldest)

	if err != nil {
		panic(err)
	}

	fmt.Println("Consumer started")

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	msgCount := 0

	doneChan := make(chan struct{})

	go func() {
		for {
			select {
			case err := <-consumer.Errors():
				fmt.Println(err)
			case msg := <-consumer.Messages():
				msgCount++
				fmt.Printf("Received message Count %d: | topic: %s | message: %s\n", msgCount, string(msg.Topic), string(msg.Value))
			case <-sigChan:
				fmt.Println("Interrupt is detected")
				doneChan <- struct{}{}
			}
		}
	}()

	<-doneChan
	fmt.Println("Processed", msgCount, "messages")
	if err := worker.Close(); err != nil {
		panic(err)
	}
}

func connectConsumer(brokers []string) (sarama.Consumer, error) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	consumer, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		return nil, err
	}
	return consumer, nil
}