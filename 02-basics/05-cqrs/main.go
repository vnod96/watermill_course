package main

import (
	"context"
	"errors"
	"os"

	"github.com/IBM/sarama"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-kafka/v3/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/components/cqrs"
	"github.com/ThreeDotsLabs/watermill/message"
)

type DeviceReadingReceived struct {
	Type  ReadingType `json:"type"`
	Value string      `json:"value"`
}

type ReadingType string

const (
	ReadingTypeTemperature ReadingType = "temperature"
	ReadingTypeHumidity    ReadingType = "humidity"
)

type TemperatureChanged struct {
	Value string `json:"value"`
}

type HumidityChanged struct {
	Value string `json:"value"`
}

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

	eventBus, err := cqrs.NewEventBusWithConfig(pub, cqrs.EventBusConfig{
		GeneratePublishTopic: func(params cqrs.GenerateEventPublishTopicParams) (string, error) {
			return params.EventName, nil
		},
		Marshaler: cqrs.JSONMarshaler{
			// By default, JSONMarshaler generates names like main.TemperatureChanged
			// This setting makes it generate just TemperatureChanged
			GenerateName: cqrs.StructName,
		},
		Logger: logger,
	})
	if err != nil {
		panic(err)
	}

	router := message.NewDefaultRouter(logger)

	eventProcessor, err := cqrs.NewEventProcessorWithConfig(router, cqrs.EventProcessorConfig{
		GenerateSubscribeTopic: func(params cqrs.EventProcessorGenerateSubscribeTopicParams) (string, error) {
			return params.EventName, nil
		},
		SubscriberConstructor: func(params cqrs.EventProcessorSubscriberConstructorParams) (message.Subscriber, error) {
			return kafka.NewSubscriber(
				kafka.SubscriberConfig{
					Brokers:               []string{os.Getenv("KAFKA_ADDR")},
					OverwriteSaramaConfig: newSubscriberSaramaConfig(),
				},
				logger,
			)
		},
		Marshaler: cqrs.JSONMarshaler{
			// By default, JSONMarshaler generates names like main.TemperatureChanged
			// This setting makes it generate just TemperatureChanged
			GenerateName: cqrs.StructName,
		},
		Logger: logger,
	})
	if err != nil {
		panic(err)
	}

	err = eventProcessor.AddHandlers(cqrs.NewEventHandler("device_reading_received_handler", func(ctx context.Context, event *DeviceReadingReceived) error {
		switch event.Type {
		case ReadingTypeHumidity:
			return eventBus.Publish(ctx, &HumidityChanged{Value: event.Value})
		case ReadingTypeTemperature:
			return eventBus.Publish(ctx, &TemperatureChanged{Value: event.Value})
		default:
			return errors.New("unknown event type")
		}
	}))

	if err != nil {
		panic(err)
	}

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
