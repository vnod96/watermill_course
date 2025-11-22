package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"
	"unicode/utf8"

	"github.com/ThreeDotsLabs/watermill/components/cqrs"
	"github.com/gofrs/uuid/v5"
	"github.com/jmoiron/sqlx"
	"github.com/labstack/echo/v4"
	echomiddleware "github.com/labstack/echo/v4/middleware"
)

func NewHTTPRouter(
	db *sqlx.DB,
	eventBus *cqrs.EventBus,
) *echo.Echo {
	e := echo.New()
	e.HideBanner = true
	e.HTTPErrorHandler = echoErrorHandler

	useEchoMiddleware(e)

	h := HTTPHandlers{
		db:       db,
		eventBus: eventBus,
	}

	e.GET("/health", func(c echo.Context) error {
		return c.String(http.StatusOK, "ok")
	})
	e.POST("/users", h.PostUsers)
	e.POST("/users/:id/email", h.PostUserEmail)
	e.GET("/users/:id", h.GetUser)

	return e
}

type HTTPHandlers struct {
	db       *sqlx.DB
	eventBus *cqrs.EventBus
}

func (h *HTTPHandlers) PostUsers(c echo.Context) error {
	var req struct {
		Name  string `json:"name"`
		Email string `json:"email"`
	}
	if err := c.Bind(&req); err != nil {
		return fmt.Errorf("invalid request: %w", err)
	}

	if req.Name == "" || req.Email == "" {
		return errors.New("empty name or email")
	}

	userID := uuid.Must(uuid.NewV7())
	now := time.Now().UTC()

	err := UpdateInTx(c.Request().Context(), h.db, sql.LevelRepeatableRead, func(ctx context.Context, tx *sqlx.Tx) error {
		_, err := tx.ExecContext(ctx, `
			INSERT INTO users (id, name, email, registered_at)
			VALUES ($1, $2, $3, $4)
		`, userID, req.Name, req.Email, now)
		if err != nil {
			return fmt.Errorf("failed to insert user: %w", err)
		}

		event := UserRegistered{
			UserID:       userID,
			Name:         req.Name,
			Email:        req.Email,
			RegisteredAt: now,
		}

		// Warning! This isn't done within the transaction. We'll fix it in the next modules.
		// if err := h.eventBus.Publish(ctx, event); err != nil {
		// 	return fmt.Errorf("failed to publish event: %w", err)
		// }
		if err := PublishEventInTx(ctx, event, tx); err != nil {
			return fmt.Errorf("failed to publish event: %w", err)
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to save user: %w", err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"user_id": userID,
	})
}

func (h *HTTPHandlers) PostUserEmail(c echo.Context) error {
	userIDStr := c.Param("id")
	userID, err := uuid.FromString(userIDStr)
	if err != nil {
		return fmt.Errorf("invalid user id: %w", err)
	}

	var req struct {
		NewEmail string `json:"new_email"`
	}
	if err := c.Bind(&req); err != nil {
		return fmt.Errorf("invalid request: %w", err)
	}

	if req.NewEmail == "" {
		return errors.New("empty new_email")
	}

	err = UpdateInTx(c.Request().Context(), h.db, sql.LevelRepeatableRead, func(ctx context.Context, tx *sqlx.Tx) error {
		var oldEmail string
		err := tx.GetContext(ctx, &oldEmail, `
			SELECT email
			FROM users
			WHERE id = $1
		`, userID)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return echo.ErrNotFound
			}
			return fmt.Errorf("failed to get user email: %w", err)
		}

		slog.Info("Changing user email",
			slog.String("user_id", userID.String()),
			slog.String("old_email", oldEmail),
			slog.String("new_email", req.NewEmail),
		)

		res, err := tx.ExecContext(ctx, `
			UPDATE users
			SET email = $1
			WHERE id = $2
		`, req.NewEmail, userID)
		if err != nil {
			return fmt.Errorf("failed to update user email: %w", err)
		}

		rowsAffected, err := res.RowsAffected()
		if err != nil {
			return fmt.Errorf("failed to get rows affected: %w", err)
		}
		if rowsAffected == 0 {
			return echo.ErrNotFound
		}

		// TODO: Publish UserEmailUpdated event
		event := UserEmailUpdated{
			UserID: userID,
			OldEmail: oldEmail,
			NewEmail: req.NewEmail,
			UpdatedAt: time.Now().UTC(),
		}

		// if err := h.eventBus.Publish(ctx, event); err != nil {
		if err := PublishEventInTx(ctx, event, tx); err != nil {
			return fmt.Errorf("failed to publish event: %w", err)
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to update user: %w", err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *HTTPHandlers) GetUser(c echo.Context) error {
	userIDStr := c.Param("id")
	userID, err := uuid.FromString(userIDStr)
	if err != nil {
		return fmt.Errorf("invalid user id: %w", err)
	}

	var user struct {
		ID           uuid.UUID `db:"id" json:"id"`
		Name         string    `db:"name" json:"name"`
		Email        string    `db:"email" json:"email"`
		RegisteredAt time.Time `db:"registered_at" json:"registered_at"`
	}

	err = h.db.GetContext(c.Request().Context(), &user, `
			SELECT id, name, email, registered_at
			FROM users
			WHERE id = $1
		`, userID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return echo.ErrNotFound
		}
		return fmt.Errorf("failed to get user: %w", err)
	}

	return c.JSON(http.StatusOK, user)
}

func echoErrorHandler(err error, c echo.Context) {
	slog.With("error", err).Error("HTTP error")

	httpCode := http.StatusInternalServerError
	msg := any("Internal server error")

	httpErr := &echo.HTTPError{}
	if errors.As(err, &httpErr) {
		httpCode = httpErr.Code
		msg = httpErr.Message
	}

	jsonErr := c.JSON(
		httpCode,
		map[string]any{
			"error": msg,
		},
	)
	if jsonErr != nil {
		panic(err)
	}
}

func useEchoMiddleware(e *echo.Echo) {
	e.Use(
		echomiddleware.BodyDump(func(c echo.Context, reqBody, resBody []byte) {
			reqID := c.Response().Header().Get(echo.HeaderXRequestID)

			logger := slog.With(
				"request_id", reqID,
				"request_body: ", string(reqBody),
			)

			if utf8.ValidString(string(resBody)) {
				logger = logger.With("response_body: ", string(resBody))
			} else {
				logger = logger.With("response_body: ", "<binary data>")
			}

			logger.Info("Request/response")
		}),
		echomiddleware.RequestLoggerWithConfig(echomiddleware.RequestLoggerConfig{
			LogURI:       true,
			LogRequestID: true,
			LogStatus:    true,
			LogMethod:    true,
			LogLatency:   true,
			LogValuesFunc: func(c echo.Context, values echomiddleware.RequestLoggerValues) error {
				slog.With(
					"URI", values.URI,
					"request_id", values.RequestID,
					"status", values.Status,
					"method", values.Method,
					"duration", values.Latency.String(),
					"error", values.Error,
				).Info("Request done")

				return nil
			},
		}),
	)
}
