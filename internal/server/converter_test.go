package server

import (
	"strings"
	"testing"

	exposev1 "github.com/agynio/expose/.gen/go/agynio/api/expose/v1"
	"github.com/agynio/expose/internal/store"
	"github.com/google/uuid"
)

func TestParseUUID(t *testing.T) {
	value := uuid.New()
	tests := []struct {
		name    string
		value   string
		field   string
		want    uuid.UUID
		wantErr string
	}{
		{
			name:  "valid",
			value: value.String(),
			field: "workload_id",
			want:  value,
		},
		{
			name:    "empty",
			value:   "",
			field:   "agent_id",
			wantErr: "agent_id is required",
		},
		{
			name:    "invalid",
			value:   "not-a-uuid",
			field:   "workload_id",
			wantErr: "workload_id must be a valid UUID",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseUUID(tc.value, tc.field)
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("expected error")
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("expected %v, got %v", tc.want, got)
			}
		})
	}
}

func TestToProtoExposureStatus(t *testing.T) {
	tests := []struct {
		name   string
		input  store.ExposureStatus
		expect exposev1.ExposureStatus
	}{
		{
			name:   "provisioning",
			input:  store.ExposureStatusProvisioning,
			expect: exposev1.ExposureStatus_EXPOSURE_STATUS_PROVISIONING,
		},
		{
			name:   "active",
			input:  store.ExposureStatusActive,
			expect: exposev1.ExposureStatus_EXPOSURE_STATUS_ACTIVE,
		},
		{
			name:   "failed",
			input:  store.ExposureStatusFailed,
			expect: exposev1.ExposureStatus_EXPOSURE_STATUS_FAILED,
		},
		{
			name:   "removing",
			input:  store.ExposureStatusRemoving,
			expect: exposev1.ExposureStatus_EXPOSURE_STATUS_REMOVING,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got := toProtoExposureStatus(tc.input)
			if got != tc.expect {
				t.Fatalf("expected %v, got %v", tc.expect, got)
			}
		})
	}
}
