package main

import (
	"time"

	"github.com/gofrs/uuid/v5"
)

type UserRegistered struct {
	UserID       uuid.UUID `json:"user_id"`
	Name         string    `json:"name"`
	Email        string    `json:"email"`
	RegisteredAt time.Time `json:"registered_at"`
}

type UserEmailUpdated struct {
	UserID uuid.UUID `json:"user_id"`
	OldEmail string `json:"old_email"`
	NewEmail string `json:"new_email"`
	UpdatedAt time.Time `json:"updated_at"`
}
