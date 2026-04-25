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
	workloadID := uuid.New()
	agentID := uuid.New()
	ctx := contextWithIdentity(agentID.String(), string(identityTypeAgent), workloadID.String())

	caller, err := resolveExposureCaller(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if caller.identity.identityID != agentID {
		t.Fatalf("expected identity id %s, got %s", agentID.String(), caller.identity.identityID.String())
	}
	if caller.identity.workloadID != workloadID {
		t.Fatalf("expected workload id %s, got %s", workloadID.String(), caller.identity.workloadID.String())
	}
}

func TestResolveExposureCallerUser(t *testing.T) {
	userID := uuid.New()
	ctx := contextWithIdentity(userID.String(), string(identityTypeUser), "")

	caller, err := resolveExposureCaller(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if caller.identity.identityType != identityTypeUser {
		t.Fatalf("expected user identity, got %s", caller.identity.identityType)
	}
}

func TestRequireClusterAdmin(t *testing.T) {
	clusterAdminID := uuid.New()
	authz := &mockAuthz{check: func(_ context.Context, req *authorizationv1.CheckRequest) (*authorizationv1.CheckResponse, error) {
		if req.GetTupleKey().GetUser() != identityUserPrefix+clusterAdminID.String() {
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
	userID := uuid.New()
	err := requireClusterAdmin(context.Background(), authz, userID)
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected permission denied, got %v", err)
	}
}

func TestResolveWorkloadIDFromRequestUsesCaller(t *testing.T) {
	expectedWorkloadID := uuid.New()
	caller := exposureCaller{identity: resolvedIdentity{
		identityType: identityTypeAgent,
		workloadID:   expectedWorkloadID,
	}}
	resolvedID, err := resolveWorkloadIDFromRequest(caller, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resolvedID != caller.identity.workloadID {
		t.Fatalf("expected workload id %s, got %s", expectedWorkloadID.String(), resolvedID.String())
	}
}

func TestResolveWorkloadIDFromRequestExplicit(t *testing.T) {
	workloadID := uuid.New()
	caller := exposureCaller{identity: resolvedIdentity{identityType: identityTypeUser}}
	resolvedID, err := resolveWorkloadIDFromRequest(caller, workloadID.String())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resolvedID != workloadID {
		t.Fatalf("expected explicit workload id %s, got %s", workloadID.String(), resolvedID.String())
	}
}

func TestResolveWorkloadIDFromRequestAgentMismatch(t *testing.T) {
	workloadID := uuid.New()
	caller := exposureCaller{identity: resolvedIdentity{
		identityType: identityTypeAgent,
		workloadID:   workloadID,
	}}
	_, err := resolveWorkloadIDFromRequest(caller, uuid.NewString())
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected permission denied, got %v", err)
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
	expectedID := uuid.New()
	err := ensureIDMatch(expectedID, uuid.NewString(), "agent")
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected permission denied, got %v", err)
	}
}

func TestEnsureIDMatchInvalidArgument(t *testing.T) {
	expectedID := uuid.New()
	err := ensureIDMatch(expectedID, "not-a-uuid", "agent")
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected invalid argument, got %v", err)
	}
}

func TestRequireOrgRelation(t *testing.T) {
	identityID := uuid.New()
	orgID := uuid.New()
	authz := &mockAuthz{check: func(_ context.Context, req *authorizationv1.CheckRequest) (*authorizationv1.CheckResponse, error) {
		if req.GetTupleKey().GetUser() != identityUserPrefix+identityID.String() {
			return nil, errors.New("unexpected user")
		}
		if req.GetTupleKey().GetRelation() != organizationOwnerRelation {
			return nil, errors.New("unexpected relation")
		}
		if req.GetTupleKey().GetObject() != organizationObjectPrefix+orgID.String() {
			return nil, errors.New("unexpected object")
		}
		return &authorizationv1.CheckResponse{Allowed: true}, nil
	}}

	if err := requireOrgRelation(context.Background(), authz, identityID, orgID, organizationOwnerRelation); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
