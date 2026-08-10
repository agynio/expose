package store

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

var ErrExposureNotFound = errors.New("exposure not found")
var ErrExposureAlreadyExists = errors.New("exposure already exists")
var ErrExposureResourcesIncomplete = errors.New("exposure resources incomplete")

type ExposureStatus int16

const (
	ExposureStatusProvisioning ExposureStatus = 1
	ExposureStatusActive       ExposureStatus = 2
	ExposureStatusFailed       ExposureStatus = 3
	ExposureStatusRemoving     ExposureStatus = 4
)

// OwnerKind is the kind of entity the exposing workload runs for. Values match
// agynio.api.expose.v1.ExposureOwnerKind and the runners service's
// RuntimeOwnerKind, so the three never need translating by number.
type OwnerKind int16

const (
	OwnerKindUnspecified   OwnerKind = 0
	OwnerKindAgentInstance OwnerKind = 1
	OwnerKindSandbox       OwnerKind = 2
)

type Exposure struct {
	ID         uuid.UUID
	WorkloadID uuid.UUID
	OwnerKind  OwnerKind
	OwnerID    uuid.UUID
	// AgentID is the agent class behind an agent-instance owner. Unset for
	// sandbox-owned exposures, which have no agent.
	AgentID uuid.NullUUID
	// OrganizationID is unset on rows created before it was recorded;
	// reconciliation fills it from the workload record.
	OrganizationID       uuid.NullUUID
	Port                 int32
	OpenZitiServiceID    string
	OpenZitiBindPolicyID string
	OpenZitiDialPolicyID string
	// Hostname is the resolved intercept address, e.g. super-sandbox.acme.agyn.
	// URL is http://<Hostname>:<Port>; both are stored so the address a caller
	// was given and the address written into OpenZiti are the same string.
	Hostname  string
	URL       string
	Status    ExposureStatus
	CreatedAt time.Time
	UpdatedAt time.Time
}

type ExposureResourceIDs struct {
	OpenZitiServiceID    string
	OpenZitiBindPolicyID string
	OpenZitiDialPolicyID string
	Hostname             string
	URL                  string
}

func (resources ExposureResourceIDs) Complete() bool {
	return resources.OpenZitiServiceID != "" &&
		resources.OpenZitiBindPolicyID != "" &&
		resources.OpenZitiDialPolicyID != "" &&
		resources.Hostname != "" &&
		resources.URL != ""
}

// ExposureOwner is the owner and organization an exposure is named after.
// Reconciliation refreshes it from the workload record.
type ExposureOwner struct {
	OwnerKind      OwnerKind
	OwnerID        uuid.UUID
	AgentID        uuid.NullUUID
	OrganizationID uuid.NullUUID
}

type PageCursor struct {
	AfterID uuid.UUID
}

type ListResult struct {
	Exposures  []Exposure
	NextCursor *PageCursor
}
