package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"

	"github.com/IBM/sarama"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-kafka/v3/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/message"
)

type DeviceReading struct {
	Type  ReadingType `json:"type"`
	Value string      `json:"value"`
}

type ReadingType string

const (
	ReadingTypeTemperature ReadingType = "temperature"
	ReadingTypeHumidity    ReadingType = "humidity"
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
	err = sub.SubscribeInitialize("readings")
	if err != nil {
		panic(err)
	}

	router := message.NewDefaultRouter(logger)

	router.AddConsumerHandler(
		"reading_consumer_handler",
		"readings",
		sub,
		func(msg *message.Message) error {
			var event DeviceReading
			err := json.Unmarshal(msg.Payload, &event)
			if err != nil {
				return err
			}
			readingType := event.Type

			m, err := json.Marshal(event)
			if err != nil {
				return err
			}

			switch readingType {
			case ReadingTypeTemperature:
				err := pub.Publish(string(ReadingTypeTemperature), message.NewMessage(watermill.NewUUID(), m))
				if err != nil {
					return err
				}
			case ReadingTypeHumidity:
				err := pub.Publish(string(ReadingTypeHumidity), message.NewMessage(watermill.NewUUID(), m))
				if err != nil {
					return err
				}
			default:
				return errors.New("unknown type")
			}
			return nil;
		},
	)

	err = router.Run(context.Background())
	if err != nil {
		panic(err)
	}
}

func newSubscriberSaramaConfig() *sarama.Config {
	cfg := kafka.DefaultSaramaSubscriberConfig()
	// We want to start consuming from the oldest message on the first run.
	// Without this setting, we may miss some messages published before the subscriber started.
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	return cfg
}
