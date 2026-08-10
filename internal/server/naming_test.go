package server

import (
	"context"
	"errors"
	"fmt"
	"testing"

	exposev1 "github.com/agynio/expose/.gen/go/agynio/api/expose/v1"
	runnersv1 "github.com/agynio/expose/.gen/go/agynio/api/runners/v1"
	zitimanagementv1 "github.com/agynio/expose/.gen/go/agynio/api/ziti_management/v1"
	"github.com/agynio/expose/internal/hostname"
	"github.com/agynio/expose/internal/store"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// addExposureWith runs the standard self-service path and reports the address
// that reached OpenZiti alongside the record the caller got back.
func addExposureWith(t *testing.T, ctx context.Context, runners *mockRunners, names NameResolver) (*exposev1.Exposure, string) {
	t.Helper()
	storeMock := provisioningStore()
	zitiMock := provisioningZiti()
	var interceptAddress string
	createService := zitiMock.createService
	zitiMock.createService = func(ctx context.Context, req *zitimanagementv1.CreateServiceRequest) (*zitimanagementv1.CreateServiceResponse, error) {
		interceptAddress = req.GetInterceptV1Config().GetAddresses()[0]
		return createService(ctx, req)
	}

	svc := New(storeMock, zitiMock, runners, defaultAuthz(), names)
	resp, err := svc.AddExposure(ctx, &exposev1.AddExposureRequest{Port: 3000})
	if err != nil {
		t.Fatalf("add exposure: %v", err)
	}
	return resp.GetExposure(), interceptAddress
}

func TestAddExposureNamesTheSandbox(t *testing.T) {
	sandboxID, workloadID, orgID := uuid.New(), uuid.New(), uuid.New()
	exposure, intercept := addExposureWith(t,
		contextWithSandboxIdentity(sandboxID, workloadID),
		workloadOwnedBy(runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX, sandboxID, orgID),
		sandboxNames("acme", "super-sandbox"),
	)

	if want := "super-sandbox.acme.agyn"; intercept != want {
		t.Fatalf("intercept address: got %q, want %q", intercept, want)
	}
	if want := "http://super-sandbox.acme.agyn:3000"; exposure.GetUrl() != want {
		t.Fatalf("url: got %q, want %q", exposure.GetUrl(), want)
	}
	if exposure.GetHostname() != intercept {
		t.Fatalf("stored hostname %q differs from the intercept address %q", exposure.GetHostname(), intercept)
	}
	if exposure.GetOwnerKind() != exposev1.ExposureOwnerKind_EXPOSURE_OWNER_KIND_SANDBOX {
		t.Fatalf("owner kind: got %v", exposure.GetOwnerKind())
	}
	if exposure.GetOwnerId() != sandboxID.String() {
		t.Fatalf("owner id: got %q, want %q", exposure.GetOwnerId(), sandboxID)
	}
	// A sandbox has no agent behind it.
	if exposure.GetAgentId() != "" {
		t.Fatalf("expected no agent id, got %q", exposure.GetAgentId())
	}
}

// The instance form is the handle read back to front: @bob#research serves at
// research.bob.
func TestAddExposureNamesTheAgentInstance(t *testing.T) {
	instanceID, workloadID, orgID := uuid.New(), uuid.New(), uuid.New()
	exposure, intercept := addExposureWith(t,
		contextWithAgentIdentity(instanceID, workloadID),
		workloadOwnedBy(runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_AGENT_INSTANCE, instanceID, orgID),
		instanceNames("acme", "bob", "research"),
	)

	if want := "research.bob.acme.agyn"; intercept != want {
		t.Fatalf("intercept address: got %q, want %q", intercept, want)
	}
	if want := "http://research.bob.acme.agyn:3000"; exposure.GetUrl() != want {
		t.Fatalf("url: got %q, want %q", exposure.GetUrl(), want)
	}
	if exposure.GetOwnerKind() != exposev1.ExposureOwnerKind_EXPOSURE_OWNER_KIND_AGENT_INSTANCE {
		t.Fatalf("owner kind: got %v", exposure.GetOwnerKind())
	}
}

// A missing or underivable name is a cosmetic gap; refusing to expose the port
// over one would turn it into an outage.
func TestAddExposureFallsBackWhenTheNameIsUnusable(t *testing.T) {
	cases := []struct {
		name  string
		names NameResolver
	}{
		{"class carries no nickname", instanceNames("acme", "", "research")},
		{"nickname is not a DNS label", instanceNames("acme", "my_bot", "research")},
		{"organization slug is missing", instanceNames("", "bob", "research")},
		{"lookup failed", stubNames{err: errors.New("organizations unreachable")}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			instanceID, workloadID, orgID := uuid.New(), uuid.New(), uuid.New()
			exposure, intercept := addExposureWith(t,
				contextWithAgentIdentity(instanceID, workloadID),
				workloadOwnedBy(runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_AGENT_INSTANCE, instanceID, orgID),
				tc.names,
			)

			want := hostname.Fallback(uuid.MustParse(exposure.GetMeta().GetId()))
			if intercept != want {
				t.Fatalf("intercept address: got %q, want %q", intercept, want)
			}
			if got := exposure.GetUrl(); got != fmt.Sprintf("http://%s:3000", want) {
				t.Fatalf("url: got %q", got)
			}
		})
	}
}

// Exposures on one entity share a hostname and differ only in port, so a second
// record for the same port would be a duplicate OpenZiti intercept.
func TestAddExposureIsIdempotentPerPort(t *testing.T) {
	instanceID, workloadID, orgID := uuid.New(), uuid.New(), uuid.New()
	existing := store.Exposure{
		ID:                   uuid.New(),
		WorkloadID:           workloadID,
		OwnerKind:            store.OwnerKindAgentInstance,
		OwnerID:              instanceID,
		Port:                 3000,
		OpenZitiServiceID:    "svc-id",
		OpenZitiBindPolicyID: "bind-id",
		OpenZitiDialPolicyID: "dial-id",
		Hostname:             "research.bob.acme.agyn",
		URL:                  "http://research.bob.acme.agyn:3000",
		Status:               store.ExposureStatusActive,
	}
	creates := 0
	storeMock := &mockStore{
		getExposureByWorkloadAndPort: func(context.Context, uuid.UUID, int32) (store.Exposure, error) {
			return existing, nil
		},
		createExposure: func(context.Context, store.Exposure) error {
			creates++
			return nil
		},
	}

	// A ziti mock with nothing overridden fails any call, which is the point:
	// the second add must provision nothing.
	svc := New(storeMock, &mockZitiMgmt{}, workloadOwnedBy(runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_AGENT_INSTANCE, instanceID, orgID),
		defaultAuthz(), instanceNames("acme", "bob", "research"))
	resp, err := svc.AddExposure(contextWithAgentIdentity(instanceID, workloadID), &exposev1.AddExposureRequest{Port: 3000})
	if err != nil {
		t.Fatalf("add exposure: %v", err)
	}
	if creates != 0 {
		t.Fatalf("expected no second exposure to be created, got %d", creates)
	}
	if resp.GetExposure().GetMeta().GetId() != existing.ID.String() {
		t.Fatalf("expected the existing exposure back, got %s", resp.GetExposure().GetMeta().GetId())
	}
	if resp.GetExposure().GetUrl() != existing.URL {
		t.Fatalf("expected the same url back, got %q", resp.GetExposure().GetUrl())
	}
}

// The self-service check is identity equality against the workload's owner, and
// it reads the same for a sandbox as for an agent instance.
func TestAddExposureRejectsACallerThatIsNotTheWorkload(t *testing.T) {
	workloadID, orgID := uuid.New(), uuid.New()
	svc := New(&mockStore{}, &mockZitiMgmt{},
		workloadOwnedBy(runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX, uuid.New(), orgID),
		defaultAuthz(), sandboxNames("acme", "super-sandbox"))

	_, err := svc.AddExposure(contextWithSandboxIdentity(uuid.New(), workloadID), &exposev1.AddExposureRequest{Port: 3000})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected permission denied, got %v", err)
	}
}
