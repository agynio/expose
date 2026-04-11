package server

import (
	"fmt"

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
	return &exposev1.Exposure{
		Meta: &exposev1.EntityMeta{
			Id:        exposure.ID.String(),
			CreatedAt: timestamppb.New(exposure.CreatedAt),
			UpdatedAt: timestamppb.New(exposure.UpdatedAt),
		},
		WorkloadId:           exposure.WorkloadID.String(),
		AgentId:              exposure.AgentID.String(),
		Port:                 exposure.Port,
		OpenzitiServiceId:    exposure.OpenZitiServiceID,
		OpenzitiBindPolicyId: exposure.OpenZitiBindPolicyID,
		OpenzitiDialPolicyId: exposure.OpenZitiDialPolicyID,
		Url:                  exposure.URL,
		Status:               toProtoExposureStatus(exposure.Status),
	}
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
