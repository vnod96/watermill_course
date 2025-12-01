package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/IBM/sarama"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-kafka/v3/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/components/cqrs"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
)

const topic = "events"

func NewWatermillRouter() (*message.Router, error) {
	logger := newWatermillLogger()

	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create router: %w", err)
	}

	pub, err := kafka.NewPublisher(kafka.PublisherConfig{
		Brokers: []string{os.Getenv("KAFKA_ADDR")},
		Marshaler: KafkaMarshaler,
	}, logger)

	if err != nil {
		return nil, fmt.Errorf("error starting the publisher: %w", err)
	}

	sub, err := kafka.NewSubscriber(kafka.SubscriberConfig{
		OverwriteSaramaConfig: newSubscriberSaramaConfig(),
		Brokers: []string{os.Getenv("KAFKA_ADDR")},
		Unmarshaler: KafkaMarshaler,
		ConsumerGroup: "splitter",
	}, logger)
	
	if err != nil {
		return nil, fmt.Errorf("error starting the subscriber: %w", err)
	}

	router.AddConsumerHandler(
		"event-handler",
		topic,
		sub,
		func(msg *message.Message) error {
			eventName := CQRSMarshaler.NameFromMessage(msg)
			return pub.Publish(eventName, msg)
		},
	)

	// Simple middleware which will recover panics from event or command handlers.
	// More about router middlewares you can find in the documentation:
	// https://watermill.io/docs/messages-router/#middleware
	//
	// List of available middlewares you can find in message/router/middleware.
	router.AddMiddleware(middleware.Recoverer)

	return router, nil
}

func NewWatermillHandlers(
	router *message.Router,
	sender EmailSender,
	crm CRMClient,
) error {
	h := &WatermillHandlers{
		emailSender: sender,
		crmClient:   crm,
	}

	logger := newWatermillLogger()

	eventProcessor, err := cqrs.NewEventProcessorWithConfig(
		router,
		cqrs.EventProcessorConfig{
			GenerateSubscribeTopic: func(params cqrs.EventProcessorGenerateSubscribeTopicParams) (string, error) {
				return params.EventName, nil
			},
			SubscriberConstructor: func(params cqrs.EventProcessorSubscriberConstructorParams) (message.Subscriber, error) {
				return kafka.NewSubscriber(
					kafka.SubscriberConfig{
						OverwriteSaramaConfig: newSubscriberSaramaConfig(),
						Brokers:               []string{os.Getenv("KAFKA_ADDR")},
						Unmarshaler:           KafkaMarshaler,
						ConsumerGroup:         params.EventName,
					},
					logger,
				)
			},
			Marshaler: CQRSMarshaler,
			Logger:    logger,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to create event processor: %w", err)
	}

	return eventProcessor.AddHandlers(
		cqrs.NewEventHandler("SendWelcomeEmail", h.SendWelcomeEmail),
		cqrs.NewEventHandler("ConfirmEmailChange", h.ConfirmEmailChange),
	)
}

type WatermillHandlers struct {
	emailSender EmailSender
	crmClient   CRMClient
}

func (h *WatermillHandlers) SendWelcomeEmail(ctx context.Context, event *UserRegistered) error {
	return h.emailSender.SendEmail(
		ctx,
		event.Email,
		"Welcome to our website!",
		fmt.Sprintf("Hello %s,\n\nThank you for registering!", event.Name),
	)
}

func (h *WatermillHandlers) ConfirmEmailChange(ctx context.Context, event *UserEmailUpdated) error {
	return h.emailSender.SendEmail(
		ctx,
		event.NewEmail,
		"Confirm your new email address",
		"Hello,\n\nPlease confirm this is your new email address.",
	)
}

func NewEventBus() (*cqrs.EventBus, error) {
	logger := newWatermillLogger()

	pub, err := kafka.NewPublisher(
		kafka.PublisherConfig{
			Brokers:   []string{os.Getenv("KAFKA_ADDR")},
			Marshaler: KafkaMarshaler,
		},
		logger,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka publisher: %w", err)
	}

	eventBus, err := cqrs.NewEventBusWithConfig(
		pub,
		cqrs.EventBusConfig{
			GeneratePublishTopic: func(params cqrs.GenerateEventPublishTopicParams) (string, error) {
				return topic, nil
			},
			Marshaler: CQRSMarshaler,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create event bus: %w", err)
	}

	return eventBus, nil
}

// This marshaler converts Watermill messages to Kafka messages and vice versa.
var KafkaMarshaler = kafka.DefaultMarshaler{}

// This marshaler converts events to Watermill messages and vice versa.
var CQRSMarshaler = cqrs.JSONMarshaler{
	// It will generate topic names based on the event type.
	// So for example, for the struct "UserRegistered" the name will be "UserRegistered".
	GenerateName: cqrs.StructName,
}

func newSubscriberSaramaConfig() *sarama.Config {
	cfg := kafka.DefaultSaramaSubscriberConfig()
	// We want to start consuming from the oldest message on the first run.
	// Without this setting, we may miss some messages when we start the service for the first time.
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	return cfg
}

func newWatermillLogger() watermill.LoggerAdapter {
	return watermill.NewSlogLoggerWithLevelMapping(nil, map[slog.Level]slog.Level{
		slog.LevelInfo: slog.LevelDebug,
	})
}
