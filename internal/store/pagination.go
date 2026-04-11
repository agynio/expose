package store

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
)

const (
	defaultPageSize int32 = 50
	maxPageSize     int32 = 100
)

func normalizePageSize(size int32) int32 {
	if size <= 0 {
		return defaultPageSize
	}
	if size > maxPageSize {
		return maxPageSize
	}
	return size
}

type pageToken struct {
	ID string `json:"id"`
}

func EncodePageToken(id uuid.UUID) (string, error) {
	payload := pageToken{ID: id.String()}
	buf, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}

func DecodePageToken(token string) (uuid.UUID, error) {
	if token == "" {
		return uuid.UUID{}, errors.New("empty token")
	}
	data, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return uuid.UUID{}, fmt.Errorf("decode token: %w", err)
	}
	var payload pageToken
	if err := json.Unmarshal(data, &payload); err != nil {
		return uuid.UUID{}, fmt.Errorf("unmarshal token: %w", err)
	}
	if payload.ID == "" {
		return uuid.UUID{}, errors.New("token missing id")
	}
	parsed, err := uuid.Parse(payload.ID)
	if err != nil {
		return uuid.UUID{}, fmt.Errorf("parse token id: %w", err)
	}
	return parsed, nil
}
