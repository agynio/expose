package reconciler

import (
	"context"
	"errors"
	"testing"
	"time"

	runnersv1 "github.com/agynio/expose/.gen/go/agynio/api/runners/v1"
	zitimanagementv1 "github.com/agynio/expose/.gen/go/agynio/api/ziti_management/v1"
	"github.com/agynio/expose/internal/hostname"
	"github.com/agynio/expose/internal/naming"
	"github.com/agynio/expose/internal/store"
	"github.com/google/uuid"
)

type stubNames struct {
	slug  string
	owner hostname.Owner
	err   error
}

func (s stubNames) Resolve(context.Context, naming.Target) (string, hostname.Owner, error) {
	return s.slug, s.owner, s.err
}

func sandboxNames(slug, name string) stubNames {
	return stubNames{slug: slug, owner: hostname.Owner{Kind: hostname.OwnerKindSandbox, SandboxName: name}}
}

func liveSandboxWorkload(ownerID, orgID uuid.UUID) *mockRunners {
	return &mockRunners{
		getWorkload: func(context.Context, *runnersv1.GetWorkloadRequest) (*runnersv1.GetWorkloadResponse, error) {
			return &runnersv1.GetWorkloadResponse{Workload: &runnersv1.Workload{
				Status:         runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING,
				OwnerKind:      runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX,
				OwnerId:        ownerID.String(),
				OrganizationId: orgID.String(),
			}}, nil
		},
	}
}

func activeSandboxExposure(ownerID, orgID uuid.UUID, host string) store.Exposure {
	return store.Exposure{
		ID:                   uuid.New(),
		WorkloadID:           uuid.New(),
		OwnerKind:            store.OwnerKindSandbox,
		OwnerID:              ownerID,
		OrganizationID:       uuid.NullUUID{UUID: orgID, Valid: true},
		Port:                 3000,
		OpenZitiServiceID:    "svc-id",
		OpenZitiBindPolicyID: "bind-id",
		OpenZitiDialPolicyID: "dial-id",
		Hostname:             host,
		URL:                  "http://" + host + ":3000",
		Status:               store.ExposureStatusActive,
	}
}

// A rename reaches a live exposure by rewriting its intercept.v1 config.
func TestReconcileRewritesTheInterceptAfterARename(t *testing.T) {
	ownerID, orgID := uuid.New(), uuid.New()
	exposure := activeSandboxExposure(ownerID, orgID, "super-sandbox.acme.agyn")

	var storedHost, storedURL string
	storeMock := &mockReconcilerStore{
		listByStatus: func(_ context.Context, s store.ExposureStatus) ([]store.Exposure, error) {
			if s != store.ExposureStatusActive {
				return nil, nil
			}
			return []store.Exposure{exposure}, nil
		},
		updateExposureAddress: func(_ context.Context, id uuid.UUID, host string, url string) error {
			if id != exposure.ID {
				t.Fatalf("unexpected exposure id %s", id)
			}
			storedHost, storedURL = host, url
			return nil
		},
	}

	var updateReq *zitimanagementv1.UpdateServiceRequest
	mgmt := &mockZitiMgmt{
		updateSvc: func(_ context.Context, req *zitimanagementv1.UpdateServiceRequest) (*zitimanagementv1.UpdateServiceResponse, error) {
			updateReq = req
			return &zitimanagementv1.UpdateServiceResponse{}, nil
		},
	}

	// The sandbox has been renamed since the exposure was created.
	New(storeMock, mgmt, liveSandboxWorkload(ownerID, orgID), nil,
		sandboxNames("acme", "renamed-sandbox"), time.Second).ReconcileOnce(context.Background())

	if updateReq == nil {
		t.Fatal("expected the intercept config to be rewritten")
	}
	if updateReq.GetZitiServiceId() != "svc-id" {
		t.Fatalf("rewrote the wrong service: %s", updateReq.GetZitiServiceId())
	}
	if got := updateReq.GetInterceptV1Config().GetAddresses(); len(got) != 1 || got[0] != "renamed-sandbox.acme.agyn" {
		t.Fatalf("unexpected intercept addresses %v", got)
	}
	if got := updateReq.GetInterceptV1Config().GetPortRanges(); len(got) != 1 || got[0].GetLow() != 3000 || got[0].GetHigh() != 3000 {
		t.Fatalf("unexpected port ranges %v", got)
	}
	if storedHost != "renamed-sandbox.acme.agyn" {
		t.Fatalf("stored hostname %q", storedHost)
	}
	if storedURL != "http://renamed-sandbox.acme.agyn:3000" {
		t.Fatalf("stored url %q", storedURL)
	}
}

// An unchanged name must not churn the Controller every pass.
func TestReconcileLeavesAnUnchangedAddressAlone(t *testing.T) {
	ownerID, orgID := uuid.New(), uuid.New()
	exposure := activeSandboxExposure(ownerID, orgID, "super-sandbox.acme.agyn")

	storeMock := &mockReconcilerStore{
		listByStatus: func(_ context.Context, s store.ExposureStatus) ([]store.Exposure, error) {
			if s != store.ExposureStatusActive {
				return nil, nil
			}
			return []store.Exposure{exposure}, nil
		},
		updateExposureAddress: func(context.Context, uuid.UUID, string, string) error {
			t.Fatal("did not expect the address to be stored again")
			return nil
		},
	}

	// A ziti mock with nothing overridden errors on any call, so a rewrite here
	// would surface as a log rather than a pass — assert on the store instead.
	New(storeMock, &mockZitiMgmt{}, liveSandboxWorkload(ownerID, orgID), nil,
		sandboxNames("acme", "super-sandbox"), time.Second).ReconcileOnce(context.Background())
}

// A name lookup that fails must not rewrite a working address to the fallback.
func TestReconcileKeepsTheAddressWhenTheLookupFails(t *testing.T) {
	ownerID, orgID := uuid.New(), uuid.New()
	exposure := activeSandboxExposure(ownerID, orgID, "super-sandbox.acme.agyn")

	storeMock := &mockReconcilerStore{
		listByStatus: func(_ context.Context, s store.ExposureStatus) ([]store.Exposure, error) {
			if s != store.ExposureStatusActive {
				return nil, nil
			}
			return []store.Exposure{exposure}, nil
		},
		updateExposureAddress: func(context.Context, uuid.UUID, string, string) error {
			t.Fatal("did not expect the address to change on a failed lookup")
			return nil
		},
	}

	New(storeMock, &mockZitiMgmt{}, liveSandboxWorkload(ownerID, orgID), nil,
		stubNames{err: errors.New("organizations unreachable")}, time.Second).ReconcileOnce(context.Background())
}

// Rows migrated from the agent-shaped schema carry no organization, so the
// workload record is what fills it in.
func TestReconcileBackfillsTheOwnerFromTheWorkload(t *testing.T) {
	ownerID, orgID := uuid.New(), uuid.New()
	exposure := activeSandboxExposure(ownerID, orgID, "exposed-legacy.agyn")
	exposure.OrganizationID = uuid.NullUUID{}
	exposure.OwnerKind = store.OwnerKindAgentInstance

	var backfilled store.ExposureOwner
	storeMock := &mockReconcilerStore{
		listByStatus: func(_ context.Context, s store.ExposureStatus) ([]store.Exposure, error) {
			if s != store.ExposureStatusActive {
				return nil, nil
			}
			return []store.Exposure{exposure}, nil
		},
		updateExposureOwner: func(_ context.Context, _ uuid.UUID, owner store.ExposureOwner) error {
			backfilled = owner
			return nil
		},
		updateExposureAddress: func(context.Context, uuid.UUID, string, string) error { return nil },
	}
	mgmt := &mockZitiMgmt{
		updateSvc: func(context.Context, *zitimanagementv1.UpdateServiceRequest) (*zitimanagementv1.UpdateServiceResponse, error) {
			return &zitimanagementv1.UpdateServiceResponse{}, nil
		},
	}

	New(storeMock, mgmt, liveSandboxWorkload(ownerID, orgID), nil,
		sandboxNames("acme", "super-sandbox"), time.Second).ReconcileOnce(context.Background())

	if backfilled.OwnerKind != store.OwnerKindSandbox {
		t.Fatalf("owner kind: got %v", backfilled.OwnerKind)
	}
	if backfilled.OwnerID != ownerID {
		t.Fatalf("owner id: got %s, want %s", backfilled.OwnerID, ownerID)
	}
	if !backfilled.OrganizationID.Valid || backfilled.OrganizationID.UUID != orgID {
		t.Fatalf("organization: got %v, want %s", backfilled.OrganizationID, orgID)
	}
}
