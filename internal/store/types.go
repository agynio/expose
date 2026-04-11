package store

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

var ErrExposureNotFound = errors.New("exposure not found")
var ErrExposureAlreadyExists = errors.New("exposure already exists")

type ExposureStatus int16

const (
	ExposureStatusProvisioning ExposureStatus = 1
	ExposureStatusActive       ExposureStatus = 2
	ExposureStatusFailed       ExposureStatus = 3
	ExposureStatusRemoving     ExposureStatus = 4
)

type Exposure struct {
	ID                   uuid.UUID
	WorkloadID           uuid.UUID
	AgentID              uuid.UUID
	Port                 int32
	OpenZitiServiceID    string
	OpenZitiBindPolicyID string
	OpenZitiDialPolicyID string
	URL                  string
	Status               ExposureStatus
	CreatedAt            time.Time
	UpdatedAt            time.Time
}

type ExposureResourceIDs struct {
	OpenZitiServiceID    string
	OpenZitiBindPolicyID string
	OpenZitiDialPolicyID string
	URL                  string
}

type PageCursor struct {
	AfterID uuid.UUID
}

type ListResult struct {
	Exposures  []Exposure
	NextCursor *PageCursor
}
