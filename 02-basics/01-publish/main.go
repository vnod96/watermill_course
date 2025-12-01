package main

import (
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
	err = pub.Publish("status", message.NewMessage(watermill.NewUUID(), []byte("alpha")))
	if err != nil {
		logger.Error("error publishing message", err, nil)
	}

	err = pub.Publish("status", message.NewMessage(watermill.NewUUID(), []byte("bravo")))
	if err != nil {
		logger.Error("error publishing message", err, nil)
	}
}
