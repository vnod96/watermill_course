package main

import (
	"context"
	"os"

	"github.com/ThreeDotsLabs/watermill-kafka/v3/pkg/kafka"
	watermillSQL "github.com/ThreeDotsLabs/watermill-sql/v4/pkg/sql"
	"github.com/ThreeDotsLabs/watermill/components/cqrs"
	"github.com/ThreeDotsLabs/watermill/components/forwarder"
	"github.com/jmoiron/sqlx"
)

const outboxTopic = "events_to_forward"

func PublishEventInTx(ctx context.Context, event Event, tx *sqlx.Tx) error {

	logger := newWatermillLogger()

	pub, err := watermillSQL.NewPublisher(watermillSQL.TxFromStdSQL(tx.Tx),
		watermillSQL.PublisherConfig{
			SchemaAdapter: watermillSQL.DefaultPostgreSQLSchema{},
		}, logger)

	if err != nil {
		return err
	}

	frw := forwarder.NewPublisher(pub, forwarder.PublisherConfig{
		ForwarderTopic: outboxTopic,
	})

	eb, err := cqrs.NewEventBusWithConfig(frw, cqrs.EventBusConfig{
		GeneratePublishTopic: func(geptp cqrs.GenerateEventPublishTopicParams) (string, error) {
			return "events", nil
		},
		Marshaler: CQRSMarshaler,
	})

	if err != nil {
		return err
	}

	return eb.Publish(ctx, event)

}


func RunForwarder(ctx context.Context, db *sqlx.DB) error {
	logger := newWatermillLogger()

	sub, err := watermillSQL.NewSubscriber(
		watermillSQL.BeginnerFromStdSQL(db.DB),
		watermillSQL.SubscriberConfig{
			InitializeSchema: true,
			SchemaAdapter: watermillSQL.DefaultPostgreSQLSchema{},
			OffsetsAdapter: watermillSQL.DefaultPostgreSQLOffsetsAdapter{},
		}, logger,
	)

	if err != nil {
		return err
	}

	err = sub.SubscribeInitialize(outboxTopic)
	if err != nil {
		return err
	}

	pub, err := kafka.NewPublisher(kafka.PublisherConfig{
		Brokers: []string{os.Getenv("KAFKA_ADDR")},
		Marshaler: KafkaMarshaler,
	}, logger)

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