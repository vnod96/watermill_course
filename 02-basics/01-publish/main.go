package main

import (
	"fmt"
	"os"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-kafka/v3/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/message"
)

func main() {
	logger := watermill.NewSlogLogger(nil)

	pub, err := kafka.NewPublisher(
		kafka.PublisherConfig{
			Brokers: []string{os.Getenv("KAFKA_ADDR")},
		},
		logger,
	)
	if err != nil {
		panic(err)
	}

	// TODO Publish messages
	alpha_msg := message.NewMessage(watermill.NewUUID(), []byte("alpha"))
	bravo_msg := message.NewMessage(watermill.NewUUID(), []byte("bravo"))

	if err := pub.Publish("status", alpha_msg); err != nil {
		fmt.Printf("failed to publish msg %v", err)
	}

	if err := pub.Publish("status", bravo_msg); err != nil {
		fmt.Printf("failed to publish msg %v", err)
	}
}
