package server

import (
	"context"
	"testing"

	authorizationv1 "github.com/agynio/expose/.gen/go/agynio/api/authorization/v1"
	exposev1 "github.com/agynio/expose/.gen/go/agynio/api/expose/v1"
	runnersv1 "github.com/agynio/expose/.gen/go/agynio/api/runners/v1"
	zitimanagementv1 "github.com/agynio/expose/.gen/go/agynio/api/ziti_management/v1"
	"github.com/agynio/expose/internal/store"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// relationAuthz allows exactly the (relation, object) pairs it is given and
// denies everything else, so a test can say which grant it is exercising.
func relationAuthz(allowed map[string]bool) *mockAuthz {
	return &mockAuthz{check: func(_ context.Context, req *authorizationv1.CheckRequest) (*authorizationv1.CheckResponse, error) {
		key := req.GetTupleKey().GetRelation() + " " + req.GetTupleKey().GetObject()
		return &authorizationv1.CheckResponse{Allowed: allowed[key]}, nil
	}}
}

func sandboxConnectAllowed(sandboxID uuid.UUID) *mockAuthz {
	return relationAuthz(map[string]bool{
		sandboxCanConnectRelation + " " + sandboxObjectPrefix + sandboxID.String(): true,
	})
}

// A person who can open a shell can run `agyn expose add` in it, so the same
// action through the API takes the same relation.
func TestAddExposureAllowsSandboxConnector(t *testing.T) {
	sandboxID, workloadID, orgID := uuid.New(), uuid.New(), uuid.New()
	storeMock := provisioningStore()
	svc := New(storeMock, provisioningZiti(),
		workloadOwnedBy(runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX, sandboxID, orgID),
		sandboxConnectAllowed(sandboxID), sandboxNames("acme", "super-sandbox"))

	// A browser is not the workload, so it names the workload explicitly.
	resp, err := svc.AddExposure(contextWithIdentity(uuid.New().String(), string(identityTypeUser), ""),
		&exposev1.AddExposureRequest{WorkloadId: workloadID.String(), Port: 3000})
	if err != nil {
		t.Fatalf("AddExposure: %v", err)
	}
	if want := "http://super-sandbox.acme.agyn:3000"; resp.GetExposure().GetUrl() != want {
		t.Fatalf("url: got %q, want %q", resp.GetExposure().GetUrl(), want)
	}
}

func TestRemoveExposureAllowsSandboxConnector(t *testing.T) {
	sandboxID, workloadID, orgID := uuid.New(), uuid.New(), uuid.New()
	exposure := store.Exposure{
		ID:                   uuid.New(),
		WorkloadID:           workloadID,
		OwnerKind:            store.OwnerKindSandbox,
		OwnerID:              sandboxID,
		OrganizationID:       uuid.NullUUID{UUID: orgID, Valid: true},
		Port:                 3000,
		OpenZitiServiceID:    "svc-id",
		OpenZitiBindPolicyID: "bind-id",
		OpenZitiDialPolicyID: "dial-id",
		Hostname:             "super-sandbox.acme.agyn",
		URL:                  "http://super-sandbox.acme.agyn:3000",
		Status:               store.ExposureStatusActive,
	}
	deleted := 0
	storeMock := &mockStore{
		getExposureByWorkloadAndPort: func(context.Context, uuid.UUID, int32) (store.Exposure, error) {
			return exposure, nil
		},
		updateExposureStatus: func(context.Context, uuid.UUID, store.ExposureStatus) error { return nil },
		deleteExposure:       func(context.Context, uuid.UUID) error { deleted++; return nil },
	}
	ziti := &mockZitiMgmt{
		deleteServicePolicy: func(context.Context, *zitimanagementv1.DeleteServicePolicyRequest) (*zitimanagementv1.DeleteServicePolicyResponse, error) {
			return &zitimanagementv1.DeleteServicePolicyResponse{}, nil
		},
		deleteService: func(context.Context, *zitimanagementv1.DeleteServiceRequest) (*zitimanagementv1.DeleteServiceResponse, error) {
			return &zitimanagementv1.DeleteServiceResponse{}, nil
		},
	}

	svc := New(storeMock, ziti,
		workloadOwnedBy(runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX, sandboxID, orgID),
		sandboxConnectAllowed(sandboxID), sandboxNames("acme", "super-sandbox"))

	if _, err := svc.RemoveExposure(contextWithIdentity(uuid.New().String(), string(identityTypeUser), ""),
		&exposev1.RemoveExposureRequest{WorkloadId: workloadID.String(), Port: 3000}); err != nil {
		t.Fatalf("RemoveExposure: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("expected the exposure to be deleted once, got %d", deleted)
	}
}

// A member of the organization who is neither owner nor collaborator holds no
// relation on the sandbox and is refused.
func TestAddExposureRefusesANonConnector(t *testing.T) {
	sandboxID, workloadID, orgID := uuid.New(), uuid.New(), uuid.New()
	storeMock := &mockStore{createExposure: func(context.Context, store.Exposure) error {
		t.Fatal("did not expect an exposure to be created")
		return nil
	}}
	svc := New(storeMock, &mockZitiMgmt{},
		workloadOwnedBy(runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX, sandboxID, orgID),
		relationAuthz(map[string]bool{}), sandboxNames("acme", "super-sandbox"))

	_, err := svc.AddExposure(contextWithIdentity(uuid.New().String(), string(identityTypeUser), ""),
		&exposev1.AddExposureRequest{WorkloadId: workloadID.String(), Port: 3000})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected permission denied, got %v", err)
	}
}

// can_connect on one sandbox says nothing about another.
func TestAddExposureRefusesAConnectorOfAnotherSandbox(t *testing.T) {
	sandboxID, workloadID, orgID := uuid.New(), uuid.New(), uuid.New()
	svc := New(&mockStore{}, &mockZitiMgmt{},
		workloadOwnedBy(runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX, sandboxID, orgID),
		sandboxConnectAllowed(uuid.New()), sandboxNames("acme", "super-sandbox"))

	_, err := svc.AddExposure(contextWithIdentity(uuid.New().String(), string(identityTypeUser), ""),
		&exposev1.AddExposureRequest{WorkloadId: workloadID.String(), Port: 3000})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected permission denied, got %v", err)
	}
}

// The sandbox clause does not reach agent-instance workloads: they have no
// comparable relation, so the explicit path stays cluster-admin-only.
func TestAddExposureIgnoresSandboxRelationForAgentWorkloads(t *testing.T) {
	instanceID, workloadID, orgID := uuid.New(), uuid.New(), uuid.New()
	svc := New(&mockStore{}, &mockZitiMgmt{},
		workloadOwnedBy(runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_AGENT_INSTANCE, instanceID, orgID),
		// can_connect on an object that happens to share the owner's id.
		sandboxConnectAllowed(instanceID), instanceNames("acme", "bob", "research"))

	_, err := svc.AddExposure(contextWithIdentity(uuid.New().String(), string(identityTypeUser), ""),
		&exposev1.AddExposureRequest{WorkloadId: workloadID.String(), Port: 3000})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected permission denied, got %v", err)
	}
}
