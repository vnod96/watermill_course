package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/jmoiron/sqlx"
)

func MigrateDB(ctx context.Context, db *sqlx.DB) error {
	dbSchema := `
	CREATE TABLE IF NOT EXISTS users (
		id UUID PRIMARY KEY,
		name TEXT NOT NULL,
		email TEXT NOT NULL,
		registered_at TIMESTAMPTZ NOT NULL
	);
	`
	if _, err := db.ExecContext(ctx, dbSchema); err != nil {
		return fmt.Errorf("could not create schema: %w", err)
	}

	return nil
}

func UpdateInTx(
	ctx context.Context,
	db *sqlx.DB,
	isolation sql.IsolationLevel,
	fn func(ctx context.Context, tx *sqlx.Tx) error,
) (err error) {
	tx, err := db.BeginTxx(ctx, &sql.TxOptions{Isolation: isolation})
	if err != nil {
		return fmt.Errorf("could not begin transaction: %w", err)
	}

	defer func() {
		if err != nil {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				err = errors.Join(err, rollbackErr)
			}
			return
		}

		err = tx.Commit()
	}()

	return fn(ctx, tx)
}
