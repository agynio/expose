package server

import (
	"fmt"
	"strings"

	exposev1 "github.com/agynio/expose/.gen/go/agynio/api/expose/v1"
	"github.com/agynio/expose/internal/store"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func parseUUID(value string, field string) (uuid.UUID, error) {
	if value == "" {
		return uuid.UUID{}, fmt.Errorf("%s is required", field)
	}
	id, err := uuid.Parse(value)
	if err != nil {
		return uuid.UUID{}, fmt.Errorf("%s must be a valid UUID: %w", field, err)
	}
	return id, nil
}

func toProtoExposure(exposure store.Exposure) *exposev1.Exposure {
	if exposure.Status == store.ExposureStatusActive && !exposureResourcesComplete(exposure) {
		panic(fmt.Sprintf("active exposure %s has incomplete OpenZiti resources", exposure.ID))
	}
	return &exposev1.Exposure{
		Meta: &exposev1.EntityMeta{
			Id:        exposure.ID.String(),
			CreatedAt: timestamppb.New(exposure.CreatedAt),
			UpdatedAt: timestamppb.New(exposure.UpdatedAt),
		},
		WorkloadId:           exposure.WorkloadID.String(),
		AgentId:              nullUUIDString(exposure.AgentID),
		OwnerKind:            toProtoOwnerKind(exposure.OwnerKind),
		OwnerId:              exposure.OwnerID.String(),
		OrganizationId:       nullUUIDString(exposure.OrganizationID),
		Port:                 exposure.Port,
		OpenzitiServiceId:    exposure.OpenZitiServiceID,
		OpenzitiBindPolicyId: exposure.OpenZitiBindPolicyID,
		OpenzitiDialPolicyId: exposure.OpenZitiDialPolicyID,
		Hostname:             exposure.Hostname,
		Url:                  exposure.URL,
		Status:               toProtoExposureStatus(exposure.Status),
	}
}

// nullUUIDString renders an unset id as the empty string. A sandbox-owned
// exposure has no agent class, and a row migrated from the agent-shaped schema
// has no organization until reconciliation fills it in.
func nullUUIDString(id uuid.NullUUID) string {
	if !id.Valid {
		return ""
	}
	return id.UUID.String()
}

func toProtoOwnerKind(kind store.OwnerKind) exposev1.ExposureOwnerKind {
	switch kind {
	case store.OwnerKindAgentInstance:
		return exposev1.ExposureOwnerKind_EXPOSURE_OWNER_KIND_AGENT_INSTANCE
	case store.OwnerKindSandbox:
		return exposev1.ExposureOwnerKind_EXPOSURE_OWNER_KIND_SANDBOX
	default:
		return exposev1.ExposureOwnerKind_EXPOSURE_OWNER_KIND_UNSPECIFIED
	}
}

func exposureResourcesComplete(exposure store.Exposure) bool {
	return strings.TrimSpace(exposure.OpenZitiServiceID) != "" &&
		strings.TrimSpace(exposure.OpenZitiBindPolicyID) != "" &&
		strings.TrimSpace(exposure.OpenZitiDialPolicyID) != "" &&
		strings.TrimSpace(exposure.Hostname) != "" &&
		strings.TrimSpace(exposure.URL) != ""
}

func toProtoExposureStatus(status store.ExposureStatus) exposev1.ExposureStatus {
	switch status {
	case store.ExposureStatusProvisioning:
		return exposev1.ExposureStatus_EXPOSURE_STATUS_PROVISIONING
	case store.ExposureStatusActive:
		return exposev1.ExposureStatus_EXPOSURE_STATUS_ACTIVE
	case store.ExposureStatusFailed:
		return exposev1.ExposureStatus_EXPOSURE_STATUS_FAILED
	case store.ExposureStatusRemoving:
		return exposev1.ExposureStatus_EXPOSURE_STATUS_REMOVING
	default:
		panic(fmt.Sprintf("unknown exposure status %d", status))
	}
}
