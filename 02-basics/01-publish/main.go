package main

import (
	"os"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-kafka/v3/pkg/kafka"
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
}
