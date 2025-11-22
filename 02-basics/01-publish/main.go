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

	alpha_msg := message.NewMessage(watermill.NewUUID(), []byte("alpha"))
	bravo_msg := message.NewMessage(watermill.NewUUID(), []byte("bravo"))

	// TODO Publish messages
	pub.Publish("status", alpha_msg)
	pub.Publish("status", bravo_msg)
}
