package main

import (
	"context"
	"fmt"
	"os"

	"github.com/IBM/sarama"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-kafka/v3/pkg/kafka"
)

func main() {
	logger := watermill.NewSlogLogger(nil)

	sub, err := kafka.NewSubscriber(
		kafka.SubscriberConfig{
			Brokers:               []string{os.Getenv("KAFKA_ADDR")},
			OverwriteSaramaConfig: newSubscriberSaramaConfig(),
			// Used by SubscribeInitialize
			InitializeTopicDetails: &sarama.TopicDetail{
				NumPartitions:     1,
				ReplicationFactor: 1,
			},
		},
		logger,
	)
	if err != nil {
		panic(err)
	}

	// Ensure the topic exists before we start consuming messages.
	err = sub.SubscribeInitialize("progress")
	if err != nil {
		panic(err)
	}

	messages, err := sub.Subscribe(context.Background(), "progress")
	if err != nil {
		panic(err)
	}

	for msg := range messages {
		fmt.Printf("Message ID: %v - %v", msg.UUID, string(msg.Payload))
		msg.Ack()
	}
}

func newSubscriberSaramaConfig() *sarama.Config {
	cfg := kafka.DefaultSaramaSubscriberConfig()
	// We want to start consuming from the oldest message on the first run.
	// Without this setting, we may miss some messages published before the subscriber started.
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	return cfg
}
