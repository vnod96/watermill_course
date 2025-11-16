package main

import (
	"context"
	"os"

	"github.com/IBM/sarama"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-kafka/v3/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/message"
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

	pub, err := kafka.NewPublisher(
		kafka.PublisherConfig{
			Brokers: []string{os.Getenv("KAFKA_ADDR")},
		},
		logger,
	)
	if err != nil {
		panic(err)
	}

	// Ensure the topic exists before we start consuming messages.
	err = sub.SubscribeInitialize("words")
	if err != nil {
		panic(err)
	}

	router := message.NewDefaultRouter(logger)

	// TODO Add handler to the router
	router.AddHandler(
		"reverse_string",
		"words",
		sub,
		"reversed",
		pub,
func(msg *message.Message) ([]*message.Message, error) {
	word := string(msg.Payload)
	rev := reverseString(word)
	newMsg := message.NewMessage(watermill.NewUUID(), []byte(rev))
	return []*message.Message{newMsg}, nil
},
	)

	err = router.Run(context.Background())
	if err != nil {
		panic(err)
	}
}

func reverseString(s string) string {
	var out string
	for _, r := range s {
		out = string(r) + out
	}
	return out
}

func newSubscriberSaramaConfig() *sarama.Config {
	cfg := kafka.DefaultSaramaSubscriberConfig()
	// We want to start consuming from the oldest message on the first run.
	// Without this setting, we may miss some messages published before the subscriber started.
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	return cfg
}
