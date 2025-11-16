package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/gofrs/uuid/v5"
)

type EmailSender struct {
	ApiEndpoint string
}

type SendEmailRequest struct {
	Email   string
	Subject string
	Body    string
}

func (e EmailSender) SendEmail(ctx context.Context, email string, subject string, body string) error {
	jsonBody, err := json.Marshal(SendEmailRequest{
		Email:   email,
		Subject: subject,
		Body:    body,
	})
	if err != nil {
		return fmt.Errorf("failed to marshal SendEmailRequest: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.ApiEndpoint, bytes.NewBuffer(jsonBody))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send email: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to send email, status code: %d", resp.StatusCode)
	}

	return nil
}

type CRMClient struct {
	ApiEndpoint string
}

type SendUserToCRMRequest struct {
	UserID uuid.UUID `json:"user_id"`
	Name   string    `json:"name"`
	Email  string    `json:"email"`
}

func (c CRMClient) SendUserToCRM(ctx context.Context, userID uuid.UUID, name string, email string) error {
	jsonBody, err := json.Marshal(SendUserToCRMRequest{
		UserID: userID,
		Name:   name,
		Email:  email,
	})
	if err != nil {
		return fmt.Errorf("failed to marshal SendUserToCRMRequest: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.ApiEndpoint, bytes.NewBuffer(jsonBody))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send user to CRM: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to send user to CRM, status code: %d", resp.StatusCode)
	}

	return nil
}
