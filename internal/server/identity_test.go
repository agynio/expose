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

	caller, err := resolveExposureCaller(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if caller.identity.identityID != agentID {
		t.Fatalf("expected identity id %s, got %s", agentID, caller.identity.identityID)
	}
	if caller.identity.workloadID != workloadID {
		t.Fatalf("expected workload id %s, got %s", workloadID, caller.identity.workloadID)
	}
}

func TestResolveExposureCallerUser(t *testing.T) {
	ctx := contextWithIdentity("user-id", string(identityTypeUser), "")

	caller, err := resolveExposureCaller(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if caller.identity.identityType != identityTypeUser {
		t.Fatalf("expected user identity, got %s", caller.identity.identityType)
	}
}

func TestRequireClusterAdmin(t *testing.T) {
	clusterAdminID := "cluster-admin"
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

	if err := requireClusterAdmin(context.Background(), authz, clusterAdminID); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRequireClusterAdminDenied(t *testing.T) {
	authz := &mockAuthz{check: func(_ context.Context, _ *authorizationv1.CheckRequest) (*authorizationv1.CheckResponse, error) {
		return &authorizationv1.CheckResponse{Allowed: false}, nil
	}}

	err := requireClusterAdmin(context.Background(), authz, "user-id")
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected permission denied, got %v", err)
	}
}

func TestResolveWorkloadIDFromRequestUsesCaller(t *testing.T) {
	caller := exposureCaller{identity: resolvedIdentity{
		identityType: identityTypeAgent,
		workloadID:   "workload-id",
	}}
	workloadID, err := resolveWorkloadIDFromRequest(caller, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if workloadID != "workload-id" {
		t.Fatalf("expected workload id, got %s", workloadID)
	}
}

func TestResolveWorkloadIDFromRequestExplicit(t *testing.T) {
	caller := exposureCaller{identity: resolvedIdentity{identityType: identityTypeUser}}
	workloadID, err := resolveWorkloadIDFromRequest(caller, "workload-123")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if workloadID != "workload-123" {
		t.Fatalf("expected explicit workload id, got %s", workloadID)
	}
}

func TestResolveWorkloadIDFromRequestRequiresWorkload(t *testing.T) {
	caller := exposureCaller{identity: resolvedIdentity{identityType: identityTypeUser}}
	_, err := resolveWorkloadIDFromRequest(caller, "")
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected invalid argument, got %v", err)
	}
}

func TestEnsureIDMatchMismatch(t *testing.T) {
	err := ensureIDMatch("expected", "other", "agent")
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected permission denied, got %v", err)
	}
}

func TestRequireOrgRelation(t *testing.T) {
	identityID := "identity-id"
	orgID := "org-id"
	authz := &mockAuthz{check: func(_ context.Context, req *authorizationv1.CheckRequest) (*authorizationv1.CheckResponse, error) {
		if req.GetTupleKey().GetUser() != identityUserPrefix+identityID {
			return nil, errors.New("unexpected user")
		}
		if req.GetTupleKey().GetRelation() != organizationOwnerRelation {
			return nil, errors.New("unexpected relation")
		}
		if req.GetTupleKey().GetObject() != organizationObjectPrefix+orgID {
			return nil, errors.New("unexpected object")
		}
		return &authorizationv1.CheckResponse{Allowed: true}, nil
	}}

	if err := requireOrgRelation(context.Background(), authz, identityID, orgID, organizationOwnerRelation); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
