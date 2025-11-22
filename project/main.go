package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/lmittmann/tint"
	"golang.org/x/sync/errgroup"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	db, err := sqlx.Open("pgx", os.Getenv("POSTGRES_URL"))
	if err != nil {
		panic(err)
	}
	defer db.Close()

	err = MigrateDB(ctx, db)
	if err != nil {
		panic(err)
	}

	slog.SetDefault(slog.New(
		tint.NewHandler(os.Stderr, &tint.Options{
			Level:      slog.LevelDebug,
			TimeFormat: "15:04:05.000",
		}),
	))

	crmClient := CRMClient{
		ApiEndpoint: os.Getenv("GATEWAY_ADDR") + "/crm-api/crm/users",
	}

	emailSender := EmailSender{
		ApiEndpoint: os.Getenv("GATEWAY_ADDR") + "/emails-api/email/send",
	}

	watermillRouter, err := NewWatermillRouter()
	if err != nil {
		panic(err)
	}

	err = NewWatermillHandlers(watermillRouter, emailSender, crmClient)
	if err != nil {
		panic(err)
	}

	eventBus, err := NewEventBus()
	if err != nil {
		panic(err)
	}

	echoRouter := NewHTTPRouter(db, eventBus)

	slog.Info("Starting service")

	errgrp, ctx := errgroup.WithContext(ctx)

	errgrp.Go(func() error {
		return watermillRouter.Run(ctx)
	})

	errgrp.Go(func() error {
		// Wait for the Watermill router to be running before starting the HTTP server, so the service isn't marked as healthy before
		<-watermillRouter.Running()

		err := echoRouter.Start(":8080")

		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}

		return nil
	})

	errgrp.Go(func() error {
		<-ctx.Done()
		return echoRouter.Shutdown(context.Background())
	})

	errgrp.Go(func() error {
		return RunForwarder(ctx, db)
	})

	if err := errgrp.Wait(); err != nil {
		panic(err)
	}
}
