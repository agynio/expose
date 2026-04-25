package server

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	authorizationv1 "github.com/agynio/expose/.gen/go/agynio/api/authorization/v1"
	exposev1 "github.com/agynio/expose/.gen/go/agynio/api/expose/v1"
	runnerv1 "github.com/agynio/expose/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/expose/.gen/go/agynio/api/runners/v1"
	zitimanagementv1 "github.com/agynio/expose/.gen/go/agynio/api/ziti_management/v1"
	"github.com/agynio/expose/internal/store"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type mockStore struct {
	createExposure               func(ctx context.Context, exposure store.Exposure) error
	getExposure                  func(ctx context.Context, id uuid.UUID) (store.Exposure, error)
	getExposureByWorkloadAndPort func(ctx context.Context, workloadID uuid.UUID, port int32) (store.Exposure, error)
	listExposuresByWorkload      func(ctx context.Context, workloadID uuid.UUID, pageSize int32, cursor *store.PageCursor) (store.ListResult, error)
	updateExposureProvisioned    func(ctx context.Context, id uuid.UUID, resources store.ExposureResourceIDs) error
	updateExposureStatus         func(ctx context.Context, id uuid.UUID, status store.ExposureStatus) error
	updateExposureFailed         func(ctx context.Context, id uuid.UUID, resources store.ExposureResourceIDs) error
	deleteExposure               func(ctx context.Context, id uuid.UUID) error
}

func (m *mockStore) CreateExposure(ctx context.Context, exposure store.Exposure) error {
	if m.createExposure == nil {
		return errors.New("not implemented")
	}
	return m.createExposure(ctx, exposure)
}

func (m *mockStore) GetExposure(ctx context.Context, id uuid.UUID) (store.Exposure, error) {
	if m.getExposure == nil {
		return store.Exposure{}, errors.New("not implemented")
	}
	return m.getExposure(ctx, id)
}

func (m *mockStore) GetExposureByWorkloadAndPort(ctx context.Context, workloadID uuid.UUID, port int32) (store.Exposure, error) {
	if m.getExposureByWorkloadAndPort == nil {
		return store.Exposure{}, errors.New("not implemented")
	}
	return m.getExposureByWorkloadAndPort(ctx, workloadID, port)
}

func (m *mockStore) ListExposuresByWorkload(ctx context.Context, workloadID uuid.UUID, pageSize int32, cursor *store.PageCursor) (store.ListResult, error) {
	if m.listExposuresByWorkload == nil {
		return store.ListResult{}, errors.New("not implemented")
	}
	return m.listExposuresByWorkload(ctx, workloadID, pageSize, cursor)
}

func (m *mockStore) UpdateExposureProvisioned(ctx context.Context, id uuid.UUID, resources store.ExposureResourceIDs) error {
	if m.updateExposureProvisioned == nil {
		return errors.New("not implemented")
	}
	return m.updateExposureProvisioned(ctx, id, resources)
}

func (m *mockStore) UpdateExposureStatus(ctx context.Context, id uuid.UUID, status store.ExposureStatus) error {
	if m.updateExposureStatus == nil {
		return errors.New("not implemented")
	}
	return m.updateExposureStatus(ctx, id, status)
}

func (m *mockStore) UpdateExposureFailed(ctx context.Context, id uuid.UUID, resources store.ExposureResourceIDs) error {
	if m.updateExposureFailed == nil {
		return errors.New("not implemented")
	}
	return m.updateExposureFailed(ctx, id, resources)
}

func (m *mockStore) DeleteExposure(ctx context.Context, id uuid.UUID) error {
	if m.deleteExposure == nil {
		return errors.New("not implemented")
	}
	return m.deleteExposure(ctx, id)
}

type mockZitiMgmt struct {
	createService       func(ctx context.Context, req *zitimanagementv1.CreateServiceRequest) (*zitimanagementv1.CreateServiceResponse, error)
	createServicePolicy func(ctx context.Context, req *zitimanagementv1.CreateServicePolicyRequest) (*zitimanagementv1.CreateServicePolicyResponse, error)
	deleteServicePolicy func(ctx context.Context, req *zitimanagementv1.DeleteServicePolicyRequest) (*zitimanagementv1.DeleteServicePolicyResponse, error)
	deleteService       func(ctx context.Context, req *zitimanagementv1.DeleteServiceRequest) (*zitimanagementv1.DeleteServiceResponse, error)
}

func (m *mockZitiMgmt) CreateAgentIdentity(context.Context, *zitimanagementv1.CreateAgentIdentityRequest, ...grpc.CallOption) (*zitimanagementv1.CreateAgentIdentityResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (m *mockZitiMgmt) CreateAppIdentity(context.Context, *zitimanagementv1.CreateAppIdentityRequest, ...grpc.CallOption) (*zitimanagementv1.CreateAppIdentityResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (m *mockZitiMgmt) CreateService(ctx context.Context, req *zitimanagementv1.CreateServiceRequest, _ ...grpc.CallOption) (*zitimanagementv1.CreateServiceResponse, error) {
	if m.createService == nil {
		return nil, errors.New("not implemented")
	}
	return m.createService(ctx, req)
}

func (m *mockZitiMgmt) DeleteIdentity(context.Context, *zitimanagementv1.DeleteIdentityRequest, ...grpc.CallOption) (*zitimanagementv1.DeleteIdentityResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (m *mockZitiMgmt) DeleteAppIdentity(context.Context, *zitimanagementv1.DeleteAppIdentityRequest, ...grpc.CallOption) (*zitimanagementv1.DeleteAppIdentityResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (m *mockZitiMgmt) CreateRunnerIdentity(context.Context, *zitimanagementv1.CreateRunnerIdentityRequest, ...grpc.CallOption) (*zitimanagementv1.CreateRunnerIdentityResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (m *mockZitiMgmt) DeleteRunnerIdentity(context.Context, *zitimanagementv1.DeleteRunnerIdentityRequest, ...grpc.CallOption) (*zitimanagementv1.DeleteRunnerIdentityResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (m *mockZitiMgmt) ListManagedIdentities(context.Context, *zitimanagementv1.ListManagedIdentitiesRequest, ...grpc.CallOption) (*zitimanagementv1.ListManagedIdentitiesResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (m *mockZitiMgmt) ResolveIdentity(context.Context, *zitimanagementv1.ResolveIdentityRequest, ...grpc.CallOption) (*zitimanagementv1.ResolveIdentityResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (m *mockZitiMgmt) RequestServiceIdentity(context.Context, *zitimanagementv1.RequestServiceIdentityRequest, ...grpc.CallOption) (*zitimanagementv1.RequestServiceIdentityResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (m *mockZitiMgmt) ExtendIdentityLease(context.Context, *zitimanagementv1.ExtendIdentityLeaseRequest, ...grpc.CallOption) (*zitimanagementv1.ExtendIdentityLeaseResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (m *mockZitiMgmt) CreateServicePolicy(ctx context.Context, req *zitimanagementv1.CreateServicePolicyRequest, _ ...grpc.CallOption) (*zitimanagementv1.CreateServicePolicyResponse, error) {
	if m.createServicePolicy == nil {
		return nil, errors.New("not implemented")
	}
	return m.createServicePolicy(ctx, req)
}

func (m *mockZitiMgmt) DeleteServicePolicy(ctx context.Context, req *zitimanagementv1.DeleteServicePolicyRequest, _ ...grpc.CallOption) (*zitimanagementv1.DeleteServicePolicyResponse, error) {
	if m.deleteServicePolicy == nil {
		return nil, errors.New("not implemented")
	}
	return m.deleteServicePolicy(ctx, req)
}

func (m *mockZitiMgmt) DeleteService(ctx context.Context, req *zitimanagementv1.DeleteServiceRequest, _ ...grpc.CallOption) (*zitimanagementv1.DeleteServiceResponse, error) {
	if m.deleteService == nil {
		return nil, errors.New("not implemented")
	}
	return m.deleteService(ctx, req)
}

func (m *mockZitiMgmt) CreateDeviceIdentity(context.Context, *zitimanagementv1.CreateDeviceIdentityRequest, ...grpc.CallOption) (*zitimanagementv1.CreateDeviceIdentityResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (m *mockZitiMgmt) DeleteDeviceIdentity(context.Context, *zitimanagementv1.DeleteDeviceIdentityRequest, ...grpc.CallOption) (*zitimanagementv1.DeleteDeviceIdentityResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

type mockRunners struct {
	getWorkload func(ctx context.Context, req *runnersv1.GetWorkloadRequest) (*runnersv1.GetWorkloadResponse, error)
}

func (m *mockRunners) RegisterRunner(context.Context, *runnersv1.RegisterRunnerRequest, ...grpc.CallOption) (*runnersv1.RegisterRunnerResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (m *mockRunners) GetRunner(context.Context, *runnersv1.GetRunnerRequest, ...grpc.CallOption) (*runnersv1.GetRunnerResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (m *mockRunners) ListRunners(context.Context, *runnersv1.ListRunnersRequest, ...grpc.CallOption) (*runnersv1.ListRunnersResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (m *mockRunners) UpdateRunner(context.Context, *runnersv1.UpdateRunnerRequest, ...grpc.CallOption) (*runnersv1.UpdateRunnerResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (m *mockRunners) DeleteRunner(context.Context, *runnersv1.DeleteRunnerRequest, ...grpc.CallOption) (*runnersv1.DeleteRunnerResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (m *mockRunners) ValidateServiceToken(context.Context, *runnersv1.ValidateServiceTokenRequest, ...grpc.CallOption) (*runnersv1.ValidateServiceTokenResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (m *mockRunners) EnrollRunner(context.Context, *runnersv1.EnrollRunnerRequest, ...grpc.CallOption) (*runnersv1.EnrollRunnerResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (m *mockRunners) CreateWorkload(context.Context, *runnersv1.CreateWorkloadRequest, ...grpc.CallOption) (*runnersv1.CreateWorkloadResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (m *mockRunners) UpdateWorkload(context.Context, *runnersv1.UpdateWorkloadRequest, ...grpc.CallOption) (*runnersv1.UpdateWorkloadResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (m *mockRunners) UpdateWorkloadStatus(context.Context, *runnersv1.UpdateWorkloadStatusRequest, ...grpc.CallOption) (*runnersv1.UpdateWorkloadStatusResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (m *mockRunners) TouchWorkload(context.Context, *runnersv1.TouchWorkloadRequest, ...grpc.CallOption) (*runnersv1.TouchWorkloadResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (m *mockRunners) DeleteWorkload(context.Context, *runnersv1.DeleteWorkloadRequest, ...grpc.CallOption) (*runnersv1.DeleteWorkloadResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (m *mockRunners) GetWorkload(ctx context.Context, req *runnersv1.GetWorkloadRequest, _ ...grpc.CallOption) (*runnersv1.GetWorkloadResponse, error) {
	if m.getWorkload == nil {
		return nil, errors.New("not implemented")
	}
	return m.getWorkload(ctx, req)
}

func (m *mockRunners) ListWorkloadsByThread(context.Context, *runnersv1.ListWorkloadsByThreadRequest, ...grpc.CallOption) (*runnersv1.ListWorkloadsByThreadResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (m *mockRunners) ListWorkloads(context.Context, *runnersv1.ListWorkloadsRequest, ...grpc.CallOption) (*runnersv1.ListWorkloadsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (m *mockRunners) BatchUpdateWorkloadSampledAt(context.Context, *runnersv1.BatchUpdateWorkloadSampledAtRequest, ...grpc.CallOption) (*runnersv1.BatchUpdateWorkloadSampledAtResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (m *mockRunners) StreamWorkloadLogs(context.Context, *runnerv1.StreamWorkloadLogsRequest, ...grpc.CallOption) (grpc.ServerStreamingClient[runnerv1.StreamWorkloadLogsResponse], error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (m *mockRunners) CreateVolume(context.Context, *runnersv1.CreateVolumeRequest, ...grpc.CallOption) (*runnersv1.CreateVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (m *mockRunners) UpdateVolume(context.Context, *runnersv1.UpdateVolumeRequest, ...grpc.CallOption) (*runnersv1.UpdateVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (m *mockRunners) GetVolume(context.Context, *runnersv1.GetVolumeRequest, ...grpc.CallOption) (*runnersv1.GetVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (m *mockRunners) ListVolumes(context.Context, *runnersv1.ListVolumesRequest, ...grpc.CallOption) (*runnersv1.ListVolumesResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (m *mockRunners) ListVolumesByThread(context.Context, *runnersv1.ListVolumesByThreadRequest, ...grpc.CallOption) (*runnersv1.ListVolumesByThreadResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (m *mockRunners) BatchUpdateVolumeSampledAt(context.Context, *runnersv1.BatchUpdateVolumeSampledAtRequest, ...grpc.CallOption) (*runnersv1.BatchUpdateVolumeSampledAtResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func contextWithIdentity(identityID, identityType, workloadID string) context.Context {
	md := metadata.Pairs(
		identityIDMetadataKey, identityID,
		identityTypeMetadataKey, identityType,
	)
	if workloadID != "" {
		md.Append(workloadIDMetadataKey, workloadID)
	}
	return metadata.NewIncomingContext(context.Background(), md)
}

func contextWithAgentIdentity(agentID, workloadID uuid.UUID) context.Context {
	return contextWithIdentity(agentID.String(), string(identityTypeAgent), workloadID.String())
}

func assertOutgoingIdentity(t *testing.T, ctx context.Context, identityID, identityType, workloadID string) {
	t.Helper()
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		t.Fatal("expected outgoing metadata")
	}
	if value := metadataValue(md, identityIDMetadataKey); value != identityID {
		t.Fatalf("expected identity id %s, got %s", identityID, value)
	}
	if value := metadataValue(md, identityTypeMetadataKey); value != identityType {
		t.Fatalf("expected identity type %s, got %s", identityType, value)
	}
	if workloadID != "" {
		if value := metadataValue(md, workloadIDMetadataKey); value != workloadID {
			t.Fatalf("expected workload id %s, got %s", workloadID, value)
		}
	} else if value := metadataValue(md, workloadIDMetadataKey); value != "" {
		t.Fatalf("expected no workload id, got %s", value)
	}
}

func defaultAuthz() authorizationv1.AuthorizationServiceClient {
	return &mockAuthz{check: func(context.Context, *authorizationv1.CheckRequest) (*authorizationv1.CheckResponse, error) {
		return &authorizationv1.CheckResponse{Allowed: false}, nil
	}}
}

func TestAddExposureHappyPath(t *testing.T) {
	workloadID := uuid.New()
	agentID := uuid.New()
	orgID := uuid.New()
	ctx := contextWithAgentIdentity(agentID, workloadID)
	createdAt := time.Now().UTC()
	updatedAt := createdAt.Add(time.Minute)

	var created store.Exposure
	var provisioned store.ExposureResourceIDs
	var serviceReq *zitimanagementv1.CreateServiceRequest
	var bindReq *zitimanagementv1.CreateServicePolicyRequest
	var dialReq *zitimanagementv1.CreateServicePolicyRequest

	storeMock := &mockStore{}
	storeMock.createExposure = func(_ context.Context, exposure store.Exposure) error {
		created = exposure
		return nil
	}
	storeMock.updateExposureProvisioned = func(_ context.Context, id uuid.UUID, resources store.ExposureResourceIDs) error {
		if id != created.ID {
			return fmt.Errorf("unexpected exposure id %s", id)
		}
		provisioned = resources
		return nil
	}
	storeMock.getExposure = func(_ context.Context, id uuid.UUID) (store.Exposure, error) {
		if id != created.ID {
			return store.Exposure{}, fmt.Errorf("unexpected exposure id %s", id)
		}
		return store.Exposure{
			ID:                   created.ID,
			WorkloadID:           workloadID,
			AgentID:              agentID,
			Port:                 created.Port,
			OpenZitiServiceID:    provisioned.OpenZitiServiceID,
			OpenZitiBindPolicyID: provisioned.OpenZitiBindPolicyID,
			OpenZitiDialPolicyID: provisioned.OpenZitiDialPolicyID,
			URL:                  provisioned.URL,
			Status:               store.ExposureStatusActive,
			CreatedAt:            createdAt,
			UpdatedAt:            updatedAt,
		}, nil
	}

	zitiMock := &mockZitiMgmt{}
	zitiMock.createService = func(_ context.Context, req *zitimanagementv1.CreateServiceRequest) (*zitimanagementv1.CreateServiceResponse, error) {
		serviceReq = req
		return &zitimanagementv1.CreateServiceResponse{ZitiServiceId: "svc-id", ZitiServiceName: req.GetName()}, nil
	}
	zitiMock.createServicePolicy = func(_ context.Context, req *zitimanagementv1.CreateServicePolicyRequest) (*zitimanagementv1.CreateServicePolicyResponse, error) {
		switch req.GetType() {
		case zitimanagementv1.ServicePolicyType_SERVICE_POLICY_TYPE_BIND:
			bindReq = req
			return &zitimanagementv1.CreateServicePolicyResponse{ZitiServicePolicyId: "bind-id"}, nil
		case zitimanagementv1.ServicePolicyType_SERVICE_POLICY_TYPE_DIAL:
			dialReq = req
			return &zitimanagementv1.CreateServicePolicyResponse{ZitiServicePolicyId: "dial-id"}, nil
		default:
			return nil, fmt.Errorf("unexpected policy type")
		}
	}

	runnersMock := &mockRunners{}
	runnersMock.getWorkload = func(ctx context.Context, req *runnersv1.GetWorkloadRequest) (*runnersv1.GetWorkloadResponse, error) {
		if req.GetId() != workloadID.String() {
			return nil, fmt.Errorf("unexpected workload id %s", req.GetId())
		}
		assertOutgoingIdentity(t, ctx, agentID.String(), string(identityTypeAgent), workloadID.String())
		return &runnersv1.GetWorkloadResponse{Workload: &runnersv1.Workload{
			AgentId:        agentID.String(),
			OrganizationId: orgID.String(),
		}}, nil
	}

	svc := New(storeMock, zitiMock, runnersMock, defaultAuthz())
	resp, err := svc.AddExposure(ctx, &exposev1.AddExposureRequest{Port: 8080})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.GetExposure() == nil {
		t.Fatalf("expected exposure in response")
	}

	serviceName := fmt.Sprintf("exposed-%s", created.ID)
	if serviceReq.GetName() != serviceName {
		t.Fatalf("expected service name %s, got %s", serviceName, serviceReq.GetName())
	}
	if len(serviceReq.GetRoleAttributes()) != 1 || serviceReq.GetRoleAttributes()[0] != "exposed-services" {
		t.Fatalf("unexpected role attributes %v", serviceReq.GetRoleAttributes())
	}
	if serviceReq.GetHostV1Config().GetPort() != 8080 {
		t.Fatalf("unexpected host port %d", serviceReq.GetHostV1Config().GetPort())
	}
	if serviceReq.GetInterceptV1Config().GetPortRanges()[0].GetLow() != 8080 {
		t.Fatalf("unexpected intercept port %d", serviceReq.GetInterceptV1Config().GetPortRanges()[0].GetLow())
	}
	if bindReq == nil || dialReq == nil {
		t.Fatalf("expected bind and dial policy requests")
	}
	if bindReq.GetIdentityRoles()[0] != fmt.Sprintf("#workload-%s", workloadID) {
		t.Fatalf("unexpected bind identity roles %v", bindReq.GetIdentityRoles())
	}
	if dialReq.GetIdentityRoles()[0] != "#all" {
		t.Fatalf("unexpected dial identity roles %v", dialReq.GetIdentityRoles())
	}
	if len(bindReq.GetServiceRoles()) != 1 || bindReq.GetServiceRoles()[0] != "@svc-id" {
		t.Fatalf("unexpected bind service roles %v", bindReq.GetServiceRoles())
	}
	if len(dialReq.GetServiceRoles()) != 1 || dialReq.GetServiceRoles()[0] != "@svc-id" {
		t.Fatalf("unexpected dial service roles %v", dialReq.GetServiceRoles())
	}

	expectedURL := fmt.Sprintf("http://%s.ziti:8080", serviceName)
	if provisioned.URL != expectedURL {
		t.Fatalf("expected url %s, got %s", expectedURL, provisioned.URL)
	}
	if provisioned.OpenZitiServiceID != "svc-id" {
		t.Fatalf("expected service id, got %s", provisioned.OpenZitiServiceID)
	}
	if provisioned.OpenZitiBindPolicyID != "bind-id" || provisioned.OpenZitiDialPolicyID != "dial-id" {
		t.Fatalf("unexpected policy ids %+v", provisioned)
	}
	if resp.GetExposure().GetMeta().GetId() != created.ID.String() {
		t.Fatalf("unexpected response id %s", resp.GetExposure().GetMeta().GetId())
	}
}

func TestAddExposureInvalidPort(t *testing.T) {
	workloadID := uuid.New()
	agentID := uuid.New()
	ctx := contextWithAgentIdentity(agentID, workloadID)
	svc := New(&mockStore{}, &mockZitiMgmt{}, &mockRunners{}, defaultAuthz())
	_, err := svc.AddExposure(ctx, &exposev1.AddExposureRequest{Port: 70000})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected invalid argument, got %v", err)
	}
}

func TestAddExposureDuplicate(t *testing.T) {
	storeMock := &mockStore{
		createExposure: func(context.Context, store.Exposure) error {
			return store.ErrExposureAlreadyExists
		},
	}
	workloadID := uuid.New()
	agentID := uuid.New()
	orgID := uuid.New()
	ctx := contextWithAgentIdentity(agentID, workloadID)
	runnersMock := &mockRunners{getWorkload: func(context.Context, *runnersv1.GetWorkloadRequest) (*runnersv1.GetWorkloadResponse, error) {
		return &runnersv1.GetWorkloadResponse{Workload: &runnersv1.Workload{
			AgentId:        agentID.String(),
			OrganizationId: orgID.String(),
		}}, nil
	}}
	svc := New(storeMock, &mockZitiMgmt{}, runnersMock, defaultAuthz())
	_, err := svc.AddExposure(ctx, &exposev1.AddExposureRequest{Port: 8080})
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("expected already exists, got %v", err)
	}
}

func TestAddExposureExplicitRequiresClusterAdmin(t *testing.T) {
	workloadID := uuid.New()
	agentID := uuid.New()
	ctx := contextWithIdentity("user-id", string(identityTypeUser), "")

	storeMock := &mockStore{createExposure: func(context.Context, store.Exposure) error {
		return fmt.Errorf("unexpected create call")
	}}

	svc := New(storeMock, &mockZitiMgmt{}, &mockRunners{}, defaultAuthz())
	_, err := svc.AddExposure(ctx, &exposev1.AddExposureRequest{
		WorkloadId: workloadID.String(),
		AgentId:    agentID.String(),
		Port:       8080,
	})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected permission denied, got %v", err)
	}
}

func TestAddExposureExplicitClusterAdmin(t *testing.T) {
	workloadID := uuid.New()
	agentID := uuid.New()
	userID := "user-id"
	ctx := contextWithIdentity(userID, string(identityTypeUser), "")

	storeMock := &mockStore{}
	var created store.Exposure
	storeMock.createExposure = func(_ context.Context, exposure store.Exposure) error {
		created = exposure
		return nil
	}
	storeMock.updateExposureProvisioned = func(context.Context, uuid.UUID, store.ExposureResourceIDs) error {
		return nil
	}
	storeMock.getExposure = func(_ context.Context, id uuid.UUID) (store.Exposure, error) {
		return store.Exposure{
			ID:         id,
			WorkloadID: workloadID,
			AgentID:    agentID,
			Port:       8080,
			Status:     store.ExposureStatusActive,
			CreatedAt:  time.Now(),
			UpdatedAt:  time.Now(),
		}, nil
	}

	zitiMock := &mockZitiMgmt{}
	zitiMock.createService = func(_ context.Context, req *zitimanagementv1.CreateServiceRequest) (*zitimanagementv1.CreateServiceResponse, error) {
		return &zitimanagementv1.CreateServiceResponse{ZitiServiceId: "svc-id"}, nil
	}
	zitiMock.createServicePolicy = func(_ context.Context, req *zitimanagementv1.CreateServicePolicyRequest) (*zitimanagementv1.CreateServicePolicyResponse, error) {
		suffix := "bind"
		if req.GetType() == zitimanagementv1.ServicePolicyType_SERVICE_POLICY_TYPE_DIAL {
			suffix = "dial"
		}
		return &zitimanagementv1.CreateServicePolicyResponse{ZitiServicePolicyId: fmt.Sprintf("%s-id", suffix)}, nil
	}

	authz := &mockAuthz{check: func(_ context.Context, req *authorizationv1.CheckRequest) (*authorizationv1.CheckResponse, error) {
		if req.GetTupleKey().GetUser() != identityUserPrefix+userID {
			return nil, fmt.Errorf("unexpected user")
		}
		if req.GetTupleKey().GetRelation() != clusterAdminRelation {
			return nil, fmt.Errorf("unexpected relation")
		}
		if req.GetTupleKey().GetObject() != clusterAdminObject {
			return nil, fmt.Errorf("unexpected object")
		}
		return &authorizationv1.CheckResponse{Allowed: true}, nil
	}}

	svc := New(storeMock, zitiMock, &mockRunners{}, authz)
	_, err := svc.AddExposure(ctx, &exposev1.AddExposureRequest{
		WorkloadId: workloadID.String(),
		AgentId:    agentID.String(),
		Port:       8080,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if created.AgentID != agentID {
		t.Fatalf("expected agent id %s, got %s", agentID, created.AgentID)
	}
}

func TestAddExposureAgentMismatch(t *testing.T) {
	workloadID := uuid.New()
	ctx := contextWithIdentity(uuid.New().String(), string(identityTypeAgent), workloadID.String())

	storeMock := &mockStore{createExposure: func(context.Context, store.Exposure) error {
		return fmt.Errorf("unexpected create call")
	}}

	runnersMock := &mockRunners{getWorkload: func(context.Context, *runnersv1.GetWorkloadRequest) (*runnersv1.GetWorkloadResponse, error) {
		return &runnersv1.GetWorkloadResponse{Workload: &runnersv1.Workload{
			AgentId:        uuid.New().String(),
			OrganizationId: uuid.New().String(),
		}}, nil
	}}

	svc := New(storeMock, &mockZitiMgmt{}, runnersMock, defaultAuthz())
	_, err := svc.AddExposure(ctx, &exposev1.AddExposureRequest{Port: 8080})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected permission denied, got %v", err)
	}
}

func TestAddExposureWorkloadNotFound(t *testing.T) {
	storeMock := &mockStore{
		createExposure: func(_ context.Context, exposure store.Exposure) error {
			return fmt.Errorf("unexpected create call")
		},
	}

	runnersMock := &mockRunners{
		getWorkload: func(context.Context, *runnersv1.GetWorkloadRequest) (*runnersv1.GetWorkloadResponse, error) {
			return nil, status.Error(codes.NotFound, "missing")
		},
	}
	workloadID := uuid.New()
	agentID := uuid.New()
	ctx := contextWithAgentIdentity(agentID, workloadID)

	svc := New(storeMock, &mockZitiMgmt{}, runnersMock, defaultAuthz())
	_, err := svc.AddExposure(ctx, &exposev1.AddExposureRequest{Port: 8080})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("expected failed precondition, got %v", err)
	}
}

func TestAddExposureBindPolicyCleanupSuccess(t *testing.T) {
	var created store.Exposure
	deleted := 0
	updatedFailed := 0

	storeMock := &mockStore{
		createExposure: func(_ context.Context, exposure store.Exposure) error {
			created = exposure
			return nil
		},
		deleteExposure: func(_ context.Context, id uuid.UUID) error {
			deleted++
			return nil
		},
		updateExposureFailed: func(_ context.Context, id uuid.UUID, resources store.ExposureResourceIDs) error {
			updatedFailed++
			return nil
		},
	}

	zitiMock := &mockZitiMgmt{}
	zitiMock.createService = func(_ context.Context, req *zitimanagementv1.CreateServiceRequest) (*zitimanagementv1.CreateServiceResponse, error) {
		return &zitimanagementv1.CreateServiceResponse{ZitiServiceId: "svc-id"}, nil
	}
	zitiMock.createServicePolicy = func(_ context.Context, req *zitimanagementv1.CreateServicePolicyRequest) (*zitimanagementv1.CreateServicePolicyResponse, error) {
		if req.GetType() == zitimanagementv1.ServicePolicyType_SERVICE_POLICY_TYPE_BIND {
			return nil, status.Error(codes.Internal, "bind failed")
		}
		return &zitimanagementv1.CreateServicePolicyResponse{ZitiServicePolicyId: "dial-id"}, nil
	}
	zitiMock.deleteService = func(_ context.Context, req *zitimanagementv1.DeleteServiceRequest) (*zitimanagementv1.DeleteServiceResponse, error) {
		if req.GetZitiServiceId() != "svc-id" {
			return nil, fmt.Errorf("unexpected service id")
		}
		return &zitimanagementv1.DeleteServiceResponse{}, nil
	}

	workloadID := uuid.New()
	agentID := uuid.New()

	runnersMock := &mockRunners{
		getWorkload: func(context.Context, *runnersv1.GetWorkloadRequest) (*runnersv1.GetWorkloadResponse, error) {
			return &runnersv1.GetWorkloadResponse{Workload: &runnersv1.Workload{
				AgentId:        agentID.String(),
				OrganizationId: uuid.New().String(),
			}}, nil
		},
	}
	ctx := contextWithAgentIdentity(agentID, workloadID)

	svc := New(storeMock, zitiMock, runnersMock, defaultAuthz())
	_, err := svc.AddExposure(ctx, &exposev1.AddExposureRequest{Port: 8080})
	if status.Code(err) != codes.Internal {
		t.Fatalf("expected internal error, got %v", err)
	}
	if deleted != 1 {
		t.Fatalf("expected delete called once, got %d", deleted)
	}
	if updatedFailed != 0 {
		t.Fatalf("expected no failed update, got %d", updatedFailed)
	}
	if created.ID == uuid.Nil {
		t.Fatalf("expected created exposure id")
	}
}

func TestAddExposureBindPolicyCleanupFail(t *testing.T) {
	updatedFailed := 0
	deleted := 0

	storeMock := &mockStore{
		createExposure: func(_ context.Context, exposure store.Exposure) error {
			return nil
		},
		updateExposureFailed: func(_ context.Context, id uuid.UUID, resources store.ExposureResourceIDs) error {
			updatedFailed++
			if resources.OpenZitiServiceID != "svc-id" {
				return fmt.Errorf("unexpected service id")
			}
			return nil
		},
		deleteExposure: func(_ context.Context, id uuid.UUID) error {
			deleted++
			return nil
		},
	}

	zitiMock := &mockZitiMgmt{}
	zitiMock.createService = func(_ context.Context, req *zitimanagementv1.CreateServiceRequest) (*zitimanagementv1.CreateServiceResponse, error) {
		return &zitimanagementv1.CreateServiceResponse{ZitiServiceId: "svc-id"}, nil
	}
	zitiMock.createServicePolicy = func(_ context.Context, req *zitimanagementv1.CreateServicePolicyRequest) (*zitimanagementv1.CreateServicePolicyResponse, error) {
		return nil, status.Error(codes.Internal, "bind failed")
	}
	zitiMock.deleteService = func(_ context.Context, req *zitimanagementv1.DeleteServiceRequest) (*zitimanagementv1.DeleteServiceResponse, error) {
		return nil, status.Error(codes.Internal, "cleanup failed")
	}

	workloadID := uuid.New()
	agentID := uuid.New()

	runnersMock := &mockRunners{
		getWorkload: func(context.Context, *runnersv1.GetWorkloadRequest) (*runnersv1.GetWorkloadResponse, error) {
			return &runnersv1.GetWorkloadResponse{Workload: &runnersv1.Workload{
				AgentId:        agentID.String(),
				OrganizationId: uuid.New().String(),
			}}, nil
		},
	}
	ctx := contextWithAgentIdentity(agentID, workloadID)

	svc := New(storeMock, zitiMock, runnersMock, defaultAuthz())
	_, err := svc.AddExposure(ctx, &exposev1.AddExposureRequest{Port: 8080})
	if status.Code(err) != codes.Internal {
		t.Fatalf("expected internal error, got %v", err)
	}
	if updatedFailed != 1 {
		t.Fatalf("expected failed update called once, got %d", updatedFailed)
	}
	if deleted != 0 {
		t.Fatalf("expected no delete call, got %d", deleted)
	}
}

func TestRemoveExposureInvalidPort(t *testing.T) {
	workloadID := uuid.New()
	agentID := uuid.New()
	ctx := contextWithAgentIdentity(agentID, workloadID)
	svc := New(&mockStore{}, &mockZitiMgmt{}, &mockRunners{}, defaultAuthz())
	_, err := svc.RemoveExposure(ctx, &exposev1.RemoveExposureRequest{
		WorkloadId: workloadID.String(),
		Port:       0,
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected invalid argument, got %v", err)
	}
}

func TestRemoveExposureNotFound(t *testing.T) {
	storeMock := &mockStore{
		getExposureByWorkloadAndPort: func(context.Context, uuid.UUID, int32) (store.Exposure, error) {
			return store.Exposure{}, store.ErrExposureNotFound
		},
	}
	workloadID := uuid.New()
	agentID := uuid.New()
	orgID := uuid.New()
	ctx := contextWithAgentIdentity(agentID, workloadID)

	runnersMock := &mockRunners{getWorkload: func(context.Context, *runnersv1.GetWorkloadRequest) (*runnersv1.GetWorkloadResponse, error) {
		return &runnersv1.GetWorkloadResponse{Workload: &runnersv1.Workload{
			AgentId:        agentID.String(),
			OrganizationId: orgID.String(),
		}}, nil
	}}
	svc := New(storeMock, &mockZitiMgmt{}, runnersMock, defaultAuthz())
	_, err := svc.RemoveExposure(ctx, &exposev1.RemoveExposureRequest{
		WorkloadId: workloadID.String(),
		Port:       8080,
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("expected not found, got %v", err)
	}
}

func TestRemoveExposureSuccess(t *testing.T) {
	exposure := store.Exposure{
		ID:                   uuid.New(),
		WorkloadID:           uuid.New(),
		Port:                 8080,
		OpenZitiServiceID:    "svc-id",
		OpenZitiBindPolicyID: "bind-id",
		OpenZitiDialPolicyID: "dial-id",
	}

	updated := 0
	deleted := 0
	storeMock := &mockStore{
		getExposureByWorkloadAndPort: func(_ context.Context, id uuid.UUID, port int32) (store.Exposure, error) {
			return exposure, nil
		},
		updateExposureStatus: func(_ context.Context, id uuid.UUID, status store.ExposureStatus) error {
			updated++
			return nil
		},
		deleteExposure: func(_ context.Context, id uuid.UUID) error {
			deleted++
			return nil
		},
		updateExposureFailed: func(_ context.Context, id uuid.UUID, resources store.ExposureResourceIDs) error {
			return nil
		},
	}

	deletedPolicies := 0
	deletedServices := 0
	zitiMock := &mockZitiMgmt{
		deleteServicePolicy: func(_ context.Context, req *zitimanagementv1.DeleteServicePolicyRequest) (*zitimanagementv1.DeleteServicePolicyResponse, error) {
			deletedPolicies++
			return &zitimanagementv1.DeleteServicePolicyResponse{}, nil
		},
		deleteService: func(_ context.Context, req *zitimanagementv1.DeleteServiceRequest) (*zitimanagementv1.DeleteServiceResponse, error) {
			deletedServices++
			return &zitimanagementv1.DeleteServiceResponse{}, nil
		},
	}

	agentID := uuid.New()
	orgID := uuid.New()
	ctx := contextWithAgentIdentity(agentID, exposure.WorkloadID)
	runnersMock := &mockRunners{getWorkload: func(context.Context, *runnersv1.GetWorkloadRequest) (*runnersv1.GetWorkloadResponse, error) {
		return &runnersv1.GetWorkloadResponse{Workload: &runnersv1.Workload{
			AgentId:        agentID.String(),
			OrganizationId: orgID.String(),
		}}, nil
	}}
	svc := New(storeMock, zitiMock, runnersMock, defaultAuthz())
	_, err := svc.RemoveExposure(ctx, &exposev1.RemoveExposureRequest{
		WorkloadId: exposure.WorkloadID.String(),
		Port:       exposure.Port,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if updated != 1 {
		t.Fatalf("expected update called once, got %d", updated)
	}
	if deleted != 1 {
		t.Fatalf("expected delete called once, got %d", deleted)
	}
	if deletedPolicies != 2 {
		t.Fatalf("expected policy deletes 2, got %d", deletedPolicies)
	}
	if deletedServices != 1 {
		t.Fatalf("expected service deletes 1, got %d", deletedServices)
	}
}

func TestRemoveExposureOrgOwner(t *testing.T) {
	exposure := store.Exposure{
		ID:                   uuid.New(),
		WorkloadID:           uuid.New(),
		Port:                 8080,
		OpenZitiServiceID:    "svc-id",
		OpenZitiBindPolicyID: "bind-id",
		OpenZitiDialPolicyID: "dial-id",
	}

	storeMock := &mockStore{
		getExposureByWorkloadAndPort: func(_ context.Context, id uuid.UUID, port int32) (store.Exposure, error) {
			return exposure, nil
		},
		updateExposureStatus: func(_ context.Context, id uuid.UUID, status store.ExposureStatus) error {
			return nil
		},
		deleteExposure: func(_ context.Context, id uuid.UUID) error {
			return nil
		},
		updateExposureFailed: func(_ context.Context, id uuid.UUID, resources store.ExposureResourceIDs) error {
			return nil
		},
	}

	zitiMock := &mockZitiMgmt{
		deleteServicePolicy: func(_ context.Context, req *zitimanagementv1.DeleteServicePolicyRequest) (*zitimanagementv1.DeleteServicePolicyResponse, error) {
			return &zitimanagementv1.DeleteServicePolicyResponse{}, nil
		},
		deleteService: func(_ context.Context, req *zitimanagementv1.DeleteServiceRequest) (*zitimanagementv1.DeleteServiceResponse, error) {
			return &zitimanagementv1.DeleteServiceResponse{}, nil
		},
	}

	orgID := uuid.New()
	agentID := uuid.New()
	userID := "user-id"
	ctx := contextWithIdentity(userID, string(identityTypeUser), "")
	runnersMock := &mockRunners{getWorkload: func(context.Context, *runnersv1.GetWorkloadRequest) (*runnersv1.GetWorkloadResponse, error) {
		return &runnersv1.GetWorkloadResponse{Workload: &runnersv1.Workload{
			AgentId:        agentID.String(),
			OrganizationId: orgID.String(),
		}}, nil
	}}
	authz := &mockAuthz{check: func(_ context.Context, req *authorizationv1.CheckRequest) (*authorizationv1.CheckResponse, error) {
		if req.GetTupleKey().GetUser() != identityUserPrefix+userID {
			return nil, fmt.Errorf("unexpected user")
		}
		if req.GetTupleKey().GetRelation() != organizationOwnerRelation {
			return nil, fmt.Errorf("unexpected relation")
		}
		if req.GetTupleKey().GetObject() != organizationObjectPrefix+orgID.String() {
			return nil, fmt.Errorf("unexpected object")
		}
		return &authorizationv1.CheckResponse{Allowed: true}, nil
	}}

	svc := New(storeMock, zitiMock, runnersMock, authz)
	_, err := svc.RemoveExposure(ctx, &exposev1.RemoveExposureRequest{
		WorkloadId: exposure.WorkloadID.String(),
		Port:       exposure.Port,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRemoveExposureDeleteFailure(t *testing.T) {
	exposure := store.Exposure{
		ID:                   uuid.New(),
		WorkloadID:           uuid.New(),
		Port:                 8080,
		OpenZitiServiceID:    "svc-id",
		OpenZitiBindPolicyID: "bind-id",
		OpenZitiDialPolicyID: "dial-id",
	}

	updatedFailed := 0
	storeMock := &mockStore{
		getExposureByWorkloadAndPort: func(_ context.Context, id uuid.UUID, port int32) (store.Exposure, error) {
			return exposure, nil
		},
		updateExposureStatus: func(_ context.Context, id uuid.UUID, status store.ExposureStatus) error {
			return nil
		},
		updateExposureFailed: func(_ context.Context, id uuid.UUID, resources store.ExposureResourceIDs) error {
			updatedFailed++
			return nil
		},
	}

	zitiMock := &mockZitiMgmt{
		deleteServicePolicy: func(_ context.Context, req *zitimanagementv1.DeleteServicePolicyRequest) (*zitimanagementv1.DeleteServicePolicyResponse, error) {
			return nil, status.Error(codes.Internal, "boom")
		},
	}

	agentID := uuid.New()
	orgID := uuid.New()
	ctx := contextWithAgentIdentity(agentID, exposure.WorkloadID)
	runnersMock := &mockRunners{getWorkload: func(context.Context, *runnersv1.GetWorkloadRequest) (*runnersv1.GetWorkloadResponse, error) {
		return &runnersv1.GetWorkloadResponse{Workload: &runnersv1.Workload{
			AgentId:        agentID.String(),
			OrganizationId: orgID.String(),
		}}, nil
	}}
	svc := New(storeMock, zitiMock, runnersMock, defaultAuthz())
	_, err := svc.RemoveExposure(ctx, &exposev1.RemoveExposureRequest{
		WorkloadId: exposure.WorkloadID.String(),
		Port:       exposure.Port,
	})
	if status.Code(err) != codes.Internal {
		t.Fatalf("expected internal error, got %v", err)
	}
	if updatedFailed != 1 {
		t.Fatalf("expected failed update once, got %d", updatedFailed)
	}
}

func TestListExposuresSuccess(t *testing.T) {
	workloadID := uuid.New()
	nextID := uuid.New()
	orgID := uuid.New()
	storeMock := &mockStore{
		listExposuresByWorkload: func(_ context.Context, id uuid.UUID, size int32, cursor *store.PageCursor) (store.ListResult, error) {
			return store.ListResult{
				Exposures: []store.Exposure{{
					ID:         workloadID,
					WorkloadID: workloadID,
					AgentID:    uuid.New(),
					Port:       8080,
					Status:     store.ExposureStatusActive,
					CreatedAt:  time.Now(),
					UpdatedAt:  time.Now(),
				}},
				NextCursor: &store.PageCursor{AfterID: nextID},
			}, nil
		},
	}
	agentID := uuid.New()
	ctx := contextWithAgentIdentity(agentID, workloadID)
	runnersMock := &mockRunners{getWorkload: func(context.Context, *runnersv1.GetWorkloadRequest) (*runnersv1.GetWorkloadResponse, error) {
		return &runnersv1.GetWorkloadResponse{Workload: &runnersv1.Workload{
			AgentId:        agentID.String(),
			OrganizationId: orgID.String(),
		}}, nil
	}}

	svc := New(storeMock, &mockZitiMgmt{}, runnersMock, defaultAuthz())
	resp, err := svc.ListExposures(ctx, &exposev1.ListExposuresRequest{WorkloadId: workloadID.String()})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.GetExposures()) != 1 {
		t.Fatalf("expected one exposure")
	}
	encoded, err := store.EncodePageToken(nextID)
	if err != nil {
		t.Fatalf("encode token: %v", err)
	}
	if resp.GetNextPageToken() != encoded {
		t.Fatalf("expected next page token %s, got %s", encoded, resp.GetNextPageToken())
	}
}

func TestListExposuresOrgMember(t *testing.T) {
	workloadID := uuid.New()
	orgID := uuid.New()
	storeMock := &mockStore{
		listExposuresByWorkload: func(_ context.Context, id uuid.UUID, size int32, cursor *store.PageCursor) (store.ListResult, error) {
			return store.ListResult{Exposures: []store.Exposure{}}, nil
		},
	}
	userID := "user-id"
	ctx := contextWithIdentity(userID, string(identityTypeUser), "")
	runnersMock := &mockRunners{getWorkload: func(context.Context, *runnersv1.GetWorkloadRequest) (*runnersv1.GetWorkloadResponse, error) {
		return &runnersv1.GetWorkloadResponse{Workload: &runnersv1.Workload{
			AgentId:        uuid.New().String(),
			OrganizationId: orgID.String(),
		}}, nil
	}}
	authz := &mockAuthz{check: func(_ context.Context, req *authorizationv1.CheckRequest) (*authorizationv1.CheckResponse, error) {
		if req.GetTupleKey().GetUser() != identityUserPrefix+userID {
			return nil, fmt.Errorf("unexpected user")
		}
		if req.GetTupleKey().GetRelation() != organizationMemberRelation {
			return nil, fmt.Errorf("unexpected relation")
		}
		if req.GetTupleKey().GetObject() != organizationObjectPrefix+orgID.String() {
			return nil, fmt.Errorf("unexpected object")
		}
		return &authorizationv1.CheckResponse{Allowed: true}, nil
	}}

	svc := New(storeMock, &mockZitiMgmt{}, runnersMock, authz)
	_, err := svc.ListExposures(ctx, &exposev1.ListExposuresRequest{WorkloadId: workloadID.String()})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestListExposuresInvalidWorkload(t *testing.T) {
	ctx := contextWithIdentity("user-id", string(identityTypeUser), "")
	svc := New(&mockStore{}, &mockZitiMgmt{}, &mockRunners{}, defaultAuthz())
	_, err := svc.ListExposures(ctx, &exposev1.ListExposuresRequest{WorkloadId: "not-a-uuid"})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected invalid argument, got %v", err)
	}
}

func TestListExposuresInvalidPageToken(t *testing.T) {
	workloadID := uuid.New()
	agentID := uuid.New()
	orgID := uuid.New()
	ctx := contextWithAgentIdentity(agentID, workloadID)
	runnersMock := &mockRunners{getWorkload: func(context.Context, *runnersv1.GetWorkloadRequest) (*runnersv1.GetWorkloadResponse, error) {
		return &runnersv1.GetWorkloadResponse{Workload: &runnersv1.Workload{
			AgentId:        agentID.String(),
			OrganizationId: orgID.String(),
		}}, nil
	}}
	svc := New(&mockStore{}, &mockZitiMgmt{}, runnersMock, defaultAuthz())
	_, err := svc.ListExposures(ctx, &exposev1.ListExposuresRequest{
		WorkloadId: workloadID.String(),
		PageToken:  "invalid",
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected invalid argument, got %v", err)
	}
}

func TestListExposuresStoreError(t *testing.T) {
	workloadID := uuid.New()
	storeMock := &mockStore{
		listExposuresByWorkload: func(context.Context, uuid.UUID, int32, *store.PageCursor) (store.ListResult, error) {
			return store.ListResult{}, errors.New("boom")
		},
	}
	agentID := uuid.New()
	orgID := uuid.New()
	ctx := contextWithAgentIdentity(agentID, workloadID)
	runnersMock := &mockRunners{getWorkload: func(context.Context, *runnersv1.GetWorkloadRequest) (*runnersv1.GetWorkloadResponse, error) {
		return &runnersv1.GetWorkloadResponse{Workload: &runnersv1.Workload{
			AgentId:        agentID.String(),
			OrganizationId: orgID.String(),
		}}, nil
	}}
	svc := New(storeMock, &mockZitiMgmt{}, runnersMock, defaultAuthz())
	_, err := svc.ListExposures(ctx, &exposev1.ListExposuresRequest{WorkloadId: workloadID.String()})
	if status.Code(err) != codes.Internal {
		t.Fatalf("expected internal error, got %v", err)
	}
}
