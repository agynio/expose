package server

import (
	"testing"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestResolveExposureCallerAgent(t *testing.T) {
	workloadID := uuid.New().String()
	agentID := uuid.New().String()
	ctx := contextWithIdentity(agentID, string(identityTypeAgent), workloadID)

	caller, err := resolveExposureCaller(ctx, "cluster-admin")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if caller.isClusterAdmin {
		t.Fatalf("expected non-cluster admin caller")
	}
	if caller.identity.identityID != agentID {
		t.Fatalf("expected identity id %s, got %s", agentID, caller.identity.identityID)
	}
	if caller.identity.workloadID != workloadID {
		t.Fatalf("expected workload id %s, got %s", workloadID, caller.identity.workloadID)
	}
}

func TestResolveExposureCallerClusterAdmin(t *testing.T) {
	clusterAdminID := "cluster-admin"
	ctx := contextWithIdentity(clusterAdminID, string(identityTypeUser), "")

	caller, err := resolveExposureCaller(ctx, clusterAdminID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !caller.isClusterAdmin {
		t.Fatalf("expected cluster admin caller")
	}
}

func TestResolveExposureCallerRejectsNonAgent(t *testing.T) {
	ctx := contextWithIdentity("user-id", string(identityTypeUser), "")
	_, err := resolveExposureCaller(ctx, "cluster-admin")
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected permission denied, got %v", err)
	}
}

func TestResolveAddExposureIDsDefaults(t *testing.T) {
	caller := exposureCaller{identity: resolvedIdentity{
		identityID:   "agent-id",
		identityType: identityTypeAgent,
		workloadID:   "workload-id",
	}}
	workloadID, agentID, err := resolveAddExposureIDs(caller, "", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if workloadID != "workload-id" {
		t.Fatalf("expected workload id to default")
	}
	if agentID != "agent-id" {
		t.Fatalf("expected agent id to default")
	}
}

func TestResolveAddExposureIDsMismatch(t *testing.T) {
	caller := exposureCaller{identity: resolvedIdentity{
		identityID:   "agent-id",
		identityType: identityTypeAgent,
		workloadID:   "workload-id",
	}}
	_, _, err := resolveAddExposureIDs(caller, "other", "")
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected permission denied, got %v", err)
	}
}

func TestResolveWorkloadIDClusterAdminRequired(t *testing.T) {
	caller := exposureCaller{isClusterAdmin: true}
	_, err := resolveWorkloadID(caller, "")
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected permission denied, got %v", err)
	}
}
