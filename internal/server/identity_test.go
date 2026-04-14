package server

import (
	"context"
	"errors"
	"testing"

	authorizationv1 "github.com/agynio/expose/.gen/go/agynio/api/authorization/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestResolveExposureCallerAgent(t *testing.T) {
	workloadID := uuid.New().String()
	agentID := uuid.New().String()
	ctx := contextWithIdentity(agentID, string(identityTypeAgent), workloadID)

	caller, err := resolveExposureCaller(ctx, nil)
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
	authz := &mockAuthz{check: func(_ context.Context, req *authorizationv1.CheckRequest) (*authorizationv1.CheckResponse, error) {
		if req.GetTupleKey().GetUser() != identityUserPrefix+clusterAdminID {
			return nil, errors.New("unexpected user")
		}
		if req.GetTupleKey().GetRelation() != clusterAdminRelation {
			return nil, errors.New("unexpected relation")
		}
		if req.GetTupleKey().GetObject() != clusterAdminObject {
			return nil, errors.New("unexpected object")
		}
		return &authorizationv1.CheckResponse{Allowed: true}, nil
	}}

	caller, err := resolveExposureCaller(ctx, authz)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !caller.isClusterAdmin {
		t.Fatalf("expected cluster admin caller")
	}
}

func TestResolveExposureCallerRejectsNonAgent(t *testing.T) {
	ctx := contextWithIdentity("user-id", string(identityTypeUser), "")
	authz := &mockAuthz{check: func(_ context.Context, _ *authorizationv1.CheckRequest) (*authorizationv1.CheckResponse, error) {
		return &authorizationv1.CheckResponse{Allowed: false}, nil
	}}
	_, err := resolveExposureCaller(ctx, authz)
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected permission denied, got %v", err)
	}
}

func TestResolveExposureCallerAuthzError(t *testing.T) {
	ctx := contextWithIdentity("user-id", string(identityTypeUser), "")
	authz := &mockAuthz{check: func(_ context.Context, _ *authorizationv1.CheckRequest) (*authorizationv1.CheckResponse, error) {
		return nil, errors.New("boom")
	}}
	_, err := resolveExposureCaller(ctx, authz)
	if status.Code(err) != codes.Internal {
		t.Fatalf("expected internal error, got %v", err)
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
