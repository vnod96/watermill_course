package main

import (
	"context"
	"fmt"
	"os"

	"github.com/ThreeDotsLabs/watermill-kafka/v3/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/components/cqrs"
	"github.com/ThreeDotsLabs/watermill/components/forwarder"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/jmoiron/sqlx"

	watermillSql "github.com/ThreeDotsLabs/watermill-sql/v4/pkg/sql"
)

const outboxTopic = "events_to_forward"

func PublishEventInTx(ctx context.Context, event Event, tx *sqlx.Tx) error {
	var pub message.Publisher
	var err error

	logger := newWatermillLogger()

	pub, err = watermillSql.NewPublisher(
		watermillSql.TxFromStdSQL(tx.Tx),
		watermillSql.PublisherConfig{
			SchemaAdapter: watermillSql.DefaultPostgreSQLSchema{},
		},
		logger,
	)

	if err != nil {
		return fmt.Errorf("failed to start the publisher: %w", err)
	}

	pub = forwarder.NewPublisher(pub, forwarder.PublisherConfig{
		ForwarderTopic: outboxTopic,
	})

	eb, err := cqrs.NewEventBusWithConfig(
		pub,
		cqrs.EventBusConfig{
			GeneratePublishTopic: func(geptp cqrs.GenerateEventPublishTopicParams) (string, error) {
				return "events", nil
			},
			Marshaler: CQRSMarshaler,
		},
	)

	if err != nil {
		return fmt.Errorf("failed to create event bus : %w", err)
	}

	return eb.Publish(ctx, event)
}

func RunForwarder(ctx context.Context, db *sqlx.DB) error {
	logger := newWatermillLogger()

	sub, err := watermillSql.NewSubscriber(
		watermillSql.BeginnerFromStdSQL(db),
		watermillSql.SubscriberConfig{
			InitializeSchema: true,
			SchemaAdapter: watermillSql.DefaultPostgreSQLSchema{},
			OffsetsAdapter: watermillSql.DefaultPostgreSQLOffsetsAdapter{},
		},
		logger,
	)
	if err  != nil {
		return err
	}

	err = sub.SubscribeInitialize(outboxTopic)

	if err != nil {
		return err
	}

	pub, err := kafka.NewPublisher(
		kafka.PublisherConfig{
			Brokers:  []string{os.Getenv("KAFKA_ADDR")},
			Marshaler: KafkaMarshaler,
		},
		logger,
	)

	if err != nil {
		return err
	}

	fwd, err := forwarder.NewForwarder(
		sub,
		pub,
		logger,
		forwarder.Config{
			ForwarderTopic: outboxTopic,
		},
	)

	if err != nil {
		return err
	}

	return fwd.Run(ctx)
}
