package reconciler

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	notificationsv1 "github.com/agynio/expose/.gen/go/agynio/api/notifications/v1"
	runnerv1 "github.com/agynio/expose/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/expose/.gen/go/agynio/api/runners/v1"
	zitimanagementv1 "github.com/agynio/expose/.gen/go/agynio/api/ziti_management/v1"
	"github.com/agynio/expose/internal/store"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type mockReconcilerStore struct {
	listByStatus           func(ctx context.Context, status store.ExposureStatus) ([]store.Exposure, error)
	listByWorkloadAll      func(ctx context.Context, workloadID uuid.UUID) ([]store.Exposure, error)
	listAllActiveWorkloads func(ctx context.Context) ([]uuid.UUID, error)
	updateExposureStatus   func(ctx context.Context, id uuid.UUID, status store.ExposureStatus) error
	deleteExposure         func(ctx context.Context, id uuid.UUID) error
}

func (m *mockReconcilerStore) ListExposuresByStatus(ctx context.Context, status store.ExposureStatus) ([]store.Exposure, error) {
	if m.listByStatus == nil {
		return nil, nil
	}
	return m.listByStatus(ctx, status)
}

func (m *mockReconcilerStore) ListExposuresByWorkloadAll(ctx context.Context, workloadID uuid.UUID) ([]store.Exposure, error) {
	if m.listByWorkloadAll == nil {
		return nil, nil
	}
	return m.listByWorkloadAll(ctx, workloadID)
}

func (m *mockReconcilerStore) ListAllActiveWorkloadIDs(ctx context.Context) ([]uuid.UUID, error) {
	if m.listAllActiveWorkloads == nil {
		return nil, nil
	}
	return m.listAllActiveWorkloads(ctx)
}

func (m *mockReconcilerStore) UpdateExposureStatus(ctx context.Context, id uuid.UUID, status store.ExposureStatus) error {
	if m.updateExposureStatus == nil {
		return nil
	}
	return m.updateExposureStatus(ctx, id, status)
}

func (m *mockReconcilerStore) DeleteExposure(ctx context.Context, id uuid.UUID) error {
	if m.deleteExposure == nil {
		return nil
	}
	return m.deleteExposure(ctx, id)
}

type mockZitiMgmt struct {
	deletePolicy func(ctx context.Context, req *zitimanagementv1.DeleteServicePolicyRequest) (*zitimanagementv1.DeleteServicePolicyResponse, error)
	deleteSvc    func(ctx context.Context, req *zitimanagementv1.DeleteServiceRequest) (*zitimanagementv1.DeleteServiceResponse, error)
}

func (m *mockZitiMgmt) CreateAgentIdentity(context.Context, *zitimanagementv1.CreateAgentIdentityRequest, ...grpc.CallOption) (*zitimanagementv1.CreateAgentIdentityResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (m *mockZitiMgmt) CreateAppIdentity(context.Context, *zitimanagementv1.CreateAppIdentityRequest, ...grpc.CallOption) (*zitimanagementv1.CreateAppIdentityResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (m *mockZitiMgmt) CreateService(context.Context, *zitimanagementv1.CreateServiceRequest, ...grpc.CallOption) (*zitimanagementv1.CreateServiceResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
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

func (m *mockZitiMgmt) CreateServicePolicy(context.Context, *zitimanagementv1.CreateServicePolicyRequest, ...grpc.CallOption) (*zitimanagementv1.CreateServicePolicyResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (m *mockZitiMgmt) DeleteServicePolicy(ctx context.Context, req *zitimanagementv1.DeleteServicePolicyRequest, _ ...grpc.CallOption) (*zitimanagementv1.DeleteServicePolicyResponse, error) {
	if m.deletePolicy == nil {
		return nil, errors.New("not implemented")
	}
	return m.deletePolicy(ctx, req)
}

func (m *mockZitiMgmt) DeleteService(ctx context.Context, req *zitimanagementv1.DeleteServiceRequest, _ ...grpc.CallOption) (*zitimanagementv1.DeleteServiceResponse, error) {
	if m.deleteSvc == nil {
		return nil, errors.New("not implemented")
	}
	return m.deleteSvc(ctx, req)
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

type mockNotifications struct {
	subscribe func(ctx context.Context, req *notificationsv1.SubscribeRequest) (grpc.ServerStreamingClient[notificationsv1.SubscribeResponse], error)
}

func (m *mockNotifications) Publish(context.Context, *notificationsv1.PublishRequest, ...grpc.CallOption) (*notificationsv1.PublishResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (m *mockNotifications) Subscribe(ctx context.Context, req *notificationsv1.SubscribeRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[notificationsv1.SubscribeResponse], error) {
	if m.subscribe == nil {
		return nil, status.Error(codes.Unimplemented, "not implemented")
	}
	return m.subscribe(ctx, req)
}

type mockSubscribeStream struct {
	ctx context.Context
}

func (m *mockSubscribeStream) Recv() (*notificationsv1.SubscribeResponse, error) {
	<-m.ctx.Done()
	return nil, m.ctx.Err()
}

func (m *mockSubscribeStream) Header() (metadata.MD, error) {
	return nil, nil
}

func (m *mockSubscribeStream) Trailer() metadata.MD {
	return nil
}

func (m *mockSubscribeStream) CloseSend() error {
	return nil
}

func (m *mockSubscribeStream) Context() context.Context {
	return m.ctx
}

func (m *mockSubscribeStream) SendMsg(interface{}) error {
	return nil
}

func (m *mockSubscribeStream) RecvMsg(interface{}) error {
	<-m.ctx.Done()
	return m.ctx.Err()
}

func TestReconcileOrphanedRemovesExposure(t *testing.T) {
	ctx := context.Background()
	exposure := store.Exposure{
		ID:                   uuid.New(),
		WorkloadID:           uuid.New(),
		OpenZitiServiceID:    "svc",
		OpenZitiBindPolicyID: "bind",
		OpenZitiDialPolicyID: "dial",
	}

	updated := 0
	deleted := 0
	storeMock := &mockReconcilerStore{
		listByStatus: func(_ context.Context, status store.ExposureStatus) ([]store.Exposure, error) {
			if status == store.ExposureStatusActive {
				return []store.Exposure{exposure}, nil
			}
			return nil, nil
		},
		updateExposureStatus: func(_ context.Context, id uuid.UUID, status store.ExposureStatus) error {
			if id != exposure.ID {
				t.Fatalf("unexpected exposure id %s", id)
			}
			if status != store.ExposureStatusRemoving {
				t.Fatalf("unexpected status %v", status)
			}
			updated++
			return nil
		},
		deleteExposure: func(_ context.Context, id uuid.UUID) error {
			if id != exposure.ID {
				t.Fatalf("unexpected exposure id %s", id)
			}
			deleted++
			return nil
		},
	}

	deletedPolicies := 0
	deletedServices := 0
	mgmt := &mockZitiMgmt{
		deletePolicy: func(_ context.Context, req *zitimanagementv1.DeleteServicePolicyRequest) (*zitimanagementv1.DeleteServicePolicyResponse, error) {
			deletedPolicies++
			return &zitimanagementv1.DeleteServicePolicyResponse{}, nil
		},
		deleteSvc: func(_ context.Context, req *zitimanagementv1.DeleteServiceRequest) (*zitimanagementv1.DeleteServiceResponse, error) {
			deletedServices++
			return &zitimanagementv1.DeleteServiceResponse{}, nil
		},
	}

	runners := &mockRunners{
		getWorkload: func(_ context.Context, req *runnersv1.GetWorkloadRequest) (*runnersv1.GetWorkloadResponse, error) {
			return nil, status.Error(codes.NotFound, "missing")
		},
	}

	reconciler := New(storeMock, mgmt, runners, nil, time.Second)
	reconciler.ReconcileOnce(ctx)

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

func TestReconcileOrphanedRemovesExposureWhenRemovedAt(t *testing.T) {
	ctx := context.Background()
	exposure := store.Exposure{
		ID:                   uuid.New(),
		WorkloadID:           uuid.New(),
		OpenZitiServiceID:    "svc",
		OpenZitiBindPolicyID: "bind",
		OpenZitiDialPolicyID: "dial",
	}

	updated := 0
	deleted := 0
	storeMock := &mockReconcilerStore{
		listByStatus: func(_ context.Context, status store.ExposureStatus) ([]store.Exposure, error) {
			if status == store.ExposureStatusActive {
				return []store.Exposure{exposure}, nil
			}
			return nil, nil
		},
		updateExposureStatus: func(_ context.Context, id uuid.UUID, status store.ExposureStatus) error {
			if id != exposure.ID {
				t.Fatalf("unexpected exposure id %s", id)
			}
			if status != store.ExposureStatusRemoving {
				t.Fatalf("unexpected status %v", status)
			}
			updated++
			return nil
		},
		deleteExposure: func(_ context.Context, id uuid.UUID) error {
			if id != exposure.ID {
				t.Fatalf("unexpected exposure id %s", id)
			}
			deleted++
			return nil
		},
	}

	deletedPolicies := 0
	deletedServices := 0
	mgmt := &mockZitiMgmt{
		deletePolicy: func(_ context.Context, req *zitimanagementv1.DeleteServicePolicyRequest) (*zitimanagementv1.DeleteServicePolicyResponse, error) {
			deletedPolicies++
			return &zitimanagementv1.DeleteServicePolicyResponse{}, nil
		},
		deleteSvc: func(_ context.Context, req *zitimanagementv1.DeleteServiceRequest) (*zitimanagementv1.DeleteServiceResponse, error) {
			deletedServices++
			return &zitimanagementv1.DeleteServiceResponse{}, nil
		},
	}

	runners := &mockRunners{
		getWorkload: func(_ context.Context, req *runnersv1.GetWorkloadRequest) (*runnersv1.GetWorkloadResponse, error) {
			return &runnersv1.GetWorkloadResponse{
				Workload: &runnersv1.Workload{
					Status:    runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING,
					RemovedAt: timestamppb.New(time.Now()),
				},
			}, nil
		},
	}

	reconciler := New(storeMock, mgmt, runners, nil, time.Second)
	reconciler.ReconcileOnce(ctx)

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

func TestReconcileOrphanedRemovesExposureWhenFailed(t *testing.T) {
	ctx := context.Background()
	exposure := store.Exposure{
		ID:                   uuid.New(),
		WorkloadID:           uuid.New(),
		OpenZitiServiceID:    "svc",
		OpenZitiBindPolicyID: "bind",
		OpenZitiDialPolicyID: "dial",
	}

	updated := 0
	deleted := 0
	storeMock := &mockReconcilerStore{
		listByStatus: func(_ context.Context, status store.ExposureStatus) ([]store.Exposure, error) {
			if status == store.ExposureStatusActive {
				return []store.Exposure{exposure}, nil
			}
			return nil, nil
		},
		updateExposureStatus: func(_ context.Context, id uuid.UUID, status store.ExposureStatus) error {
			if id != exposure.ID {
				t.Fatalf("unexpected exposure id %s", id)
			}
			if status != store.ExposureStatusRemoving {
				t.Fatalf("unexpected status %v", status)
			}
			updated++
			return nil
		},
		deleteExposure: func(_ context.Context, id uuid.UUID) error {
			if id != exposure.ID {
				t.Fatalf("unexpected exposure id %s", id)
			}
			deleted++
			return nil
		},
	}

	deletedPolicies := 0
	deletedServices := 0
	mgmt := &mockZitiMgmt{
		deletePolicy: func(_ context.Context, req *zitimanagementv1.DeleteServicePolicyRequest) (*zitimanagementv1.DeleteServicePolicyResponse, error) {
			deletedPolicies++
			return &zitimanagementv1.DeleteServicePolicyResponse{}, nil
		},
		deleteSvc: func(_ context.Context, req *zitimanagementv1.DeleteServiceRequest) (*zitimanagementv1.DeleteServiceResponse, error) {
			deletedServices++
			return &zitimanagementv1.DeleteServiceResponse{}, nil
		},
	}

	runners := &mockRunners{
		getWorkload: func(_ context.Context, req *runnersv1.GetWorkloadRequest) (*runnersv1.GetWorkloadResponse, error) {
			return &runnersv1.GetWorkloadResponse{
				Workload: &runnersv1.Workload{Status: runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED},
			}, nil
		},
	}

	reconciler := New(storeMock, mgmt, runners, nil, time.Second)
	reconciler.ReconcileOnce(ctx)

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

func TestReconcileOrphanedRemovesExposureWhenNilWorkload(t *testing.T) {
	ctx := context.Background()
	exposure := store.Exposure{
		ID:                   uuid.New(),
		WorkloadID:           uuid.New(),
		OpenZitiServiceID:    "svc",
		OpenZitiBindPolicyID: "bind",
		OpenZitiDialPolicyID: "dial",
	}

	updated := 0
	deleted := 0
	storeMock := &mockReconcilerStore{
		listByStatus: func(_ context.Context, status store.ExposureStatus) ([]store.Exposure, error) {
			if status == store.ExposureStatusActive {
				return []store.Exposure{exposure}, nil
			}
			return nil, nil
		},
		updateExposureStatus: func(_ context.Context, id uuid.UUID, status store.ExposureStatus) error {
			if id != exposure.ID {
				t.Fatalf("unexpected exposure id %s", id)
			}
			if status != store.ExposureStatusRemoving {
				t.Fatalf("unexpected status %v", status)
			}
			updated++
			return nil
		},
		deleteExposure: func(_ context.Context, id uuid.UUID) error {
			if id != exposure.ID {
				t.Fatalf("unexpected exposure id %s", id)
			}
			deleted++
			return nil
		},
	}

	deletedPolicies := 0
	deletedServices := 0
	mgmt := &mockZitiMgmt{
		deletePolicy: func(_ context.Context, req *zitimanagementv1.DeleteServicePolicyRequest) (*zitimanagementv1.DeleteServicePolicyResponse, error) {
			deletedPolicies++
			return &zitimanagementv1.DeleteServicePolicyResponse{}, nil
		},
		deleteSvc: func(_ context.Context, req *zitimanagementv1.DeleteServiceRequest) (*zitimanagementv1.DeleteServiceResponse, error) {
			deletedServices++
			return &zitimanagementv1.DeleteServiceResponse{}, nil
		},
	}

	runners := &mockRunners{
		getWorkload: func(_ context.Context, req *runnersv1.GetWorkloadRequest) (*runnersv1.GetWorkloadResponse, error) {
			return &runnersv1.GetWorkloadResponse{}, nil
		},
	}

	reconciler := New(storeMock, mgmt, runners, nil, time.Second)
	reconciler.ReconcileOnce(ctx)

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

func TestReconcileOrphanedSkipsExistingWorkload(t *testing.T) {
	ctx := context.Background()
	exposure := store.Exposure{ID: uuid.New(), WorkloadID: uuid.New()}

	updated := 0
	deleted := 0
	storeMock := &mockReconcilerStore{
		listByStatus: func(_ context.Context, status store.ExposureStatus) ([]store.Exposure, error) {
			if status == store.ExposureStatusActive {
				return []store.Exposure{exposure}, nil
			}
			return nil, nil
		},
		updateExposureStatus: func(_ context.Context, id uuid.UUID, status store.ExposureStatus) error {
			updated++
			return nil
		},
		deleteExposure: func(_ context.Context, id uuid.UUID) error {
			deleted++
			return nil
		},
	}

	reconciler := New(storeMock, &mockZitiMgmt{}, &mockRunners{
		getWorkload: func(_ context.Context, req *runnersv1.GetWorkloadRequest) (*runnersv1.GetWorkloadResponse, error) {
			return &runnersv1.GetWorkloadResponse{
				Workload: &runnersv1.Workload{Status: runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING},
			}, nil
		},
	}, nil, time.Second)

	reconciler.ReconcileOnce(ctx)

	if updated != 0 {
		t.Fatalf("expected no updates, got %d", updated)
	}
	if deleted != 0 {
		t.Fatalf("expected no deletes, got %d", deleted)
	}
}

func TestReconcileWorkloadRemovesExposureWhenRemovedAt(t *testing.T) {
	ctx := context.Background()
	workloadID := uuid.New()
	exposure := store.Exposure{
		ID:                   uuid.New(),
		WorkloadID:           workloadID,
		OpenZitiServiceID:    "svc",
		OpenZitiBindPolicyID: "bind",
		OpenZitiDialPolicyID: "dial",
	}

	listCalls := 0
	deleted := 0
	storeMock := &mockReconcilerStore{
		listByWorkloadAll: func(_ context.Context, id uuid.UUID) ([]store.Exposure, error) {
			if id != workloadID {
				t.Fatalf("unexpected workload id %s", id)
			}
			listCalls++
			return []store.Exposure{exposure}, nil
		},
		deleteExposure: func(_ context.Context, id uuid.UUID) error {
			if id != exposure.ID {
				t.Fatalf("unexpected exposure id %s", id)
			}
			deleted++
			return nil
		},
	}

	deletedPolicies := 0
	deletedServices := 0
	mgmt := &mockZitiMgmt{
		deletePolicy: func(_ context.Context, req *zitimanagementv1.DeleteServicePolicyRequest) (*zitimanagementv1.DeleteServicePolicyResponse, error) {
			deletedPolicies++
			return &zitimanagementv1.DeleteServicePolicyResponse{}, nil
		},
		deleteSvc: func(_ context.Context, req *zitimanagementv1.DeleteServiceRequest) (*zitimanagementv1.DeleteServiceResponse, error) {
			deletedServices++
			return &zitimanagementv1.DeleteServiceResponse{}, nil
		},
	}

	runners := &mockRunners{
		getWorkload: func(_ context.Context, req *runnersv1.GetWorkloadRequest) (*runnersv1.GetWorkloadResponse, error) {
			if req.Id != workloadID.String() {
				t.Fatalf("unexpected workload id %s", req.Id)
			}
			return &runnersv1.GetWorkloadResponse{
				Workload: &runnersv1.Workload{
					Status:    runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING,
					RemovedAt: timestamppb.New(time.Now()),
				},
			}, nil
		},
	}

	reconciler := New(storeMock, mgmt, runners, nil, time.Second)
	reconciler.reconcileWorkload(ctx, workloadID)

	if listCalls != 1 {
		t.Fatalf("expected list called once, got %d", listCalls)
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

func TestReconcileWorkloadRemovesExposureWhenFailed(t *testing.T) {
	ctx := context.Background()
	workloadID := uuid.New()
	exposure := store.Exposure{
		ID:                   uuid.New(),
		WorkloadID:           workloadID,
		OpenZitiServiceID:    "svc",
		OpenZitiBindPolicyID: "bind",
		OpenZitiDialPolicyID: "dial",
	}

	listCalls := 0
	deleted := 0
	storeMock := &mockReconcilerStore{
		listByWorkloadAll: func(_ context.Context, id uuid.UUID) ([]store.Exposure, error) {
			if id != workloadID {
				t.Fatalf("unexpected workload id %s", id)
			}
			listCalls++
			return []store.Exposure{exposure}, nil
		},
		deleteExposure: func(_ context.Context, id uuid.UUID) error {
			if id != exposure.ID {
				t.Fatalf("unexpected exposure id %s", id)
			}
			deleted++
			return nil
		},
	}

	deletedPolicies := 0
	deletedServices := 0
	mgmt := &mockZitiMgmt{
		deletePolicy: func(_ context.Context, req *zitimanagementv1.DeleteServicePolicyRequest) (*zitimanagementv1.DeleteServicePolicyResponse, error) {
			deletedPolicies++
			return &zitimanagementv1.DeleteServicePolicyResponse{}, nil
		},
		deleteSvc: func(_ context.Context, req *zitimanagementv1.DeleteServiceRequest) (*zitimanagementv1.DeleteServiceResponse, error) {
			deletedServices++
			return &zitimanagementv1.DeleteServiceResponse{}, nil
		},
	}

	runners := &mockRunners{
		getWorkload: func(_ context.Context, req *runnersv1.GetWorkloadRequest) (*runnersv1.GetWorkloadResponse, error) {
			return &runnersv1.GetWorkloadResponse{
				Workload: &runnersv1.Workload{Status: runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED},
			}, nil
		},
	}

	reconciler := New(storeMock, mgmt, runners, nil, time.Second)
	reconciler.reconcileWorkload(ctx, workloadID)

	if listCalls != 1 {
		t.Fatalf("expected list called once, got %d", listCalls)
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

func TestReconcileWorkloadRemovesExposureWhenNilWorkload(t *testing.T) {
	ctx := context.Background()
	workloadID := uuid.New()
	exposure := store.Exposure{
		ID:                   uuid.New(),
		WorkloadID:           workloadID,
		OpenZitiServiceID:    "svc",
		OpenZitiBindPolicyID: "bind",
		OpenZitiDialPolicyID: "dial",
	}

	listCalls := 0
	deleted := 0
	storeMock := &mockReconcilerStore{
		listByWorkloadAll: func(_ context.Context, id uuid.UUID) ([]store.Exposure, error) {
			if id != workloadID {
				t.Fatalf("unexpected workload id %s", id)
			}
			listCalls++
			return []store.Exposure{exposure}, nil
		},
		deleteExposure: func(_ context.Context, id uuid.UUID) error {
			if id != exposure.ID {
				t.Fatalf("unexpected exposure id %s", id)
			}
			deleted++
			return nil
		},
	}

	deletedPolicies := 0
	deletedServices := 0
	mgmt := &mockZitiMgmt{
		deletePolicy: func(_ context.Context, req *zitimanagementv1.DeleteServicePolicyRequest) (*zitimanagementv1.DeleteServicePolicyResponse, error) {
			deletedPolicies++
			return &zitimanagementv1.DeleteServicePolicyResponse{}, nil
		},
		deleteSvc: func(_ context.Context, req *zitimanagementv1.DeleteServiceRequest) (*zitimanagementv1.DeleteServiceResponse, error) {
			deletedServices++
			return &zitimanagementv1.DeleteServiceResponse{}, nil
		},
	}

	runners := &mockRunners{
		getWorkload: func(_ context.Context, req *runnersv1.GetWorkloadRequest) (*runnersv1.GetWorkloadResponse, error) {
			if req.Id != workloadID.String() {
				t.Fatalf("unexpected workload id %s", req.Id)
			}
			return &runnersv1.GetWorkloadResponse{}, nil
		},
	}

	reconciler := New(storeMock, mgmt, runners, nil, time.Second)
	reconciler.reconcileWorkload(ctx, workloadID)

	if listCalls != 1 {
		t.Fatalf("expected list called once, got %d", listCalls)
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

func TestReconcileWorkloadSkipsRunningWorkload(t *testing.T) {
	ctx := context.Background()
	workloadID := uuid.New()
	exposure := store.Exposure{ID: uuid.New(), WorkloadID: workloadID}

	listCalls := 0
	deleted := 0
	storeMock := &mockReconcilerStore{
		listByWorkloadAll: func(_ context.Context, id uuid.UUID) ([]store.Exposure, error) {
			listCalls++
			return []store.Exposure{exposure}, nil
		},
		deleteExposure: func(_ context.Context, id uuid.UUID) error {
			deleted++
			return nil
		},
	}

	runners := &mockRunners{
		getWorkload: func(_ context.Context, req *runnersv1.GetWorkloadRequest) (*runnersv1.GetWorkloadResponse, error) {
			return &runnersv1.GetWorkloadResponse{
				Workload: &runnersv1.Workload{Status: runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING},
			}, nil
		},
	}

	reconciler := New(storeMock, &mockZitiMgmt{}, runners, nil, time.Second)
	reconciler.reconcileWorkload(ctx, workloadID)

	if listCalls != 0 {
		t.Fatalf("expected no list calls, got %d", listCalls)
	}
	if deleted != 0 {
		t.Fatalf("expected no deletes, got %d", deleted)
	}
}

func TestReconcileOrphanedSkipsOnError(t *testing.T) {
	ctx := context.Background()
	exposure := store.Exposure{ID: uuid.New(), WorkloadID: uuid.New()}

	updated := 0
	storeMock := &mockReconcilerStore{
		listByStatus: func(_ context.Context, status store.ExposureStatus) ([]store.Exposure, error) {
			if status == store.ExposureStatusActive {
				return []store.Exposure{exposure}, nil
			}
			return nil, nil
		},
		updateExposureStatus: func(_ context.Context, id uuid.UUID, status store.ExposureStatus) error {
			updated++
			return nil
		},
	}

	reconciler := New(storeMock, &mockZitiMgmt{}, &mockRunners{
		getWorkload: func(_ context.Context, req *runnersv1.GetWorkloadRequest) (*runnersv1.GetWorkloadResponse, error) {
			return nil, status.Error(codes.Internal, "boom")
		},
	}, nil, time.Second)

	reconciler.ReconcileOnce(ctx)

	if updated != 0 {
		t.Fatalf("expected no updates, got %d", updated)
	}
}

func TestReconcileFailedRemovesExposure(t *testing.T) {
	ctx := context.Background()
	exposure := store.Exposure{
		ID:                   uuid.New(),
		OpenZitiServiceID:    "svc",
		OpenZitiBindPolicyID: "bind",
		OpenZitiDialPolicyID: "dial",
	}

	deleted := 0
	storeMock := &mockReconcilerStore{
		listByStatus: func(_ context.Context, status store.ExposureStatus) ([]store.Exposure, error) {
			if status == store.ExposureStatusFailed {
				return []store.Exposure{exposure}, nil
			}
			return nil, nil
		},
		deleteExposure: func(_ context.Context, id uuid.UUID) error {
			deleted++
			return nil
		},
	}

	reconciler := New(storeMock, &mockZitiMgmt{
		deletePolicy: func(_ context.Context, req *zitimanagementv1.DeleteServicePolicyRequest) (*zitimanagementv1.DeleteServicePolicyResponse, error) {
			return &zitimanagementv1.DeleteServicePolicyResponse{}, nil
		},
		deleteSvc: func(_ context.Context, req *zitimanagementv1.DeleteServiceRequest) (*zitimanagementv1.DeleteServiceResponse, error) {
			return &zitimanagementv1.DeleteServiceResponse{}, nil
		},
	}, &mockRunners{}, nil, time.Second)

	reconciler.ReconcileOnce(ctx)

	if deleted != 1 {
		t.Fatalf("expected delete called once, got %d", deleted)
	}
}

func TestReconcileFailedStopsOnError(t *testing.T) {
	ctx := context.Background()
	exposure := store.Exposure{
		ID:                   uuid.New(),
		OpenZitiServiceID:    "svc",
		OpenZitiBindPolicyID: "bind",
		OpenZitiDialPolicyID: "dial",
	}

	deleted := 0
	storeMock := &mockReconcilerStore{
		listByStatus: func(_ context.Context, status store.ExposureStatus) ([]store.Exposure, error) {
			if status == store.ExposureStatusFailed {
				return []store.Exposure{exposure}, nil
			}
			return nil, nil
		},
		deleteExposure: func(_ context.Context, id uuid.UUID) error {
			deleted++
			return nil
		},
	}

	reconciler := New(storeMock, &mockZitiMgmt{
		deletePolicy: func(_ context.Context, req *zitimanagementv1.DeleteServicePolicyRequest) (*zitimanagementv1.DeleteServicePolicyResponse, error) {
			return nil, status.Error(codes.Internal, "boom")
		},
	}, &mockRunners{}, nil, time.Second)

	reconciler.ReconcileOnce(ctx)

	if deleted != 0 {
		t.Fatalf("expected no deletes, got %d", deleted)
	}
}

func TestReconcileRemovingHandlesNotFound(t *testing.T) {
	ctx := context.Background()
	exposure := store.Exposure{
		ID:                   uuid.New(),
		OpenZitiServiceID:    "svc",
		OpenZitiBindPolicyID: "bind",
		OpenZitiDialPolicyID: "dial",
	}

	deleted := 0
	storeMock := &mockReconcilerStore{
		listByStatus: func(_ context.Context, status store.ExposureStatus) ([]store.Exposure, error) {
			if status == store.ExposureStatusRemoving {
				return []store.Exposure{exposure}, nil
			}
			return nil, nil
		},
		deleteExposure: func(_ context.Context, id uuid.UUID) error {
			deleted++
			return nil
		},
	}

	reconciler := New(storeMock, &mockZitiMgmt{
		deletePolicy: func(_ context.Context, req *zitimanagementv1.DeleteServicePolicyRequest) (*zitimanagementv1.DeleteServicePolicyResponse, error) {
			return nil, status.Error(codes.NotFound, "missing")
		},
		deleteSvc: func(_ context.Context, req *zitimanagementv1.DeleteServiceRequest) (*zitimanagementv1.DeleteServiceResponse, error) {
			return nil, status.Error(codes.NotFound, "missing")
		},
	}, &mockRunners{}, nil, time.Second)

	reconciler.ReconcileOnce(ctx)

	if deleted != 1 {
		t.Fatalf("expected delete called once, got %d", deleted)
	}
}

func TestReconcileRemovingStopsOnError(t *testing.T) {
	ctx := context.Background()
	exposure := store.Exposure{
		ID:                   uuid.New(),
		OpenZitiServiceID:    "svc",
		OpenZitiBindPolicyID: "bind",
		OpenZitiDialPolicyID: "dial",
	}

	deleted := 0
	storeMock := &mockReconcilerStore{
		listByStatus: func(_ context.Context, status store.ExposureStatus) ([]store.Exposure, error) {
			if status == store.ExposureStatusRemoving {
				return []store.Exposure{exposure}, nil
			}
			return nil, nil
		},
		deleteExposure: func(_ context.Context, id uuid.UUID) error {
			deleted++
			return nil
		},
	}

	reconciler := New(storeMock, &mockZitiMgmt{
		deletePolicy: func(_ context.Context, req *zitimanagementv1.DeleteServicePolicyRequest) (*zitimanagementv1.DeleteServicePolicyResponse, error) {
			return &zitimanagementv1.DeleteServicePolicyResponse{}, nil
		},
		deleteSvc: func(_ context.Context, req *zitimanagementv1.DeleteServiceRequest) (*zitimanagementv1.DeleteServiceResponse, error) {
			return nil, status.Error(codes.Internal, "boom")
		},
	}, &mockRunners{}, nil, time.Second)

	reconciler.ReconcileOnce(ctx)

	if deleted != 0 {
		t.Fatalf("expected no deletes, got %d", deleted)
	}
}

func TestParseWorkloadRoom(t *testing.T) {
	validID := uuid.New()
	tests := []struct {
		name  string
		room  string
		ok    bool
		value uuid.UUID
	}{
		{
			name:  "valid",
			room:  "workload:" + validID.String(),
			ok:    true,
			value: validID,
		},
		{
			name: "wrong prefix",
			room: "agent:" + validID.String(),
		},
		{
			name: "invalid uuid",
			room: "workload:not-a-uuid",
		},
		{
			name: "empty",
			room: "",
		},
		{
			name: "prefix only",
			room: "workload:",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got, ok := parseWorkloadRoom(tc.room)
			if ok != tc.ok {
				t.Fatalf("expected ok=%v, got %v", tc.ok, ok)
			}
			if !tc.ok {
				return
			}
			if got != tc.value {
				t.Fatalf("expected %v, got %v", tc.value, got)
			}
		})
	}
}

func TestSubscribeAndProcessResubscribesOnRoomChange(t *testing.T) {
	previousInterval := notificationRoomPollInterval
	notificationRoomPollInterval = 10 * time.Millisecond
	t.Cleanup(func() {
		notificationRoomPollInterval = previousInterval
	})

	workloadA := uuid.New()
	workloadB := uuid.New()

	var (
		mu    sync.Mutex
		calls int
	)
	storeMock := &mockReconcilerStore{
		listAllActiveWorkloads: func(context.Context) ([]uuid.UUID, error) {
			mu.Lock()
			defer mu.Unlock()
			calls++
			if calls == 1 {
				return []uuid.UUID{workloadA}, nil
			}
			return []uuid.UUID{workloadA, workloadB}, nil
		},
	}

	roomsCh := make(chan []string, 2)
	notifications := &mockNotifications{
		subscribe: func(ctx context.Context, req *notificationsv1.SubscribeRequest) (grpc.ServerStreamingClient[notificationsv1.SubscribeResponse], error) {
			rooms := append([]string(nil), req.GetRooms()...)
			roomsCh <- rooms
			return &mockSubscribeStream{ctx: ctx}, nil
		},
	}

	reconciler := New(storeMock, nil, nil, notifications, time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- reconciler.subscribeAndProcess(ctx)
	}()

	firstRooms := waitForRooms(t, roomsCh)
	assertWorkloadRooms(t, firstRooms, []uuid.UUID{workloadA})

	secondRooms := waitForRooms(t, roomsCh)
	assertWorkloadRooms(t, secondRooms, []uuid.UUID{workloadA, workloadB})

	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("expected nil error, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for subscribe to exit")
	}
}

func waitForRooms(t *testing.T, roomsCh <-chan []string) []string {
	t.Helper()
	select {
	case rooms := <-roomsCh:
		return rooms
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for subscribe")
		return nil
	}
}

func assertWorkloadRooms(t *testing.T, got []string, want []uuid.UUID) {
	t.Helper()
	expected := make(map[string]struct{}, len(want))
	for _, id := range want {
		expected[workloadRoom(id)] = struct{}{}
	}
	if len(got) != len(expected) {
		t.Fatalf("expected %d rooms, got %d", len(expected), len(got))
	}
	for _, room := range got {
		if _, ok := expected[room]; !ok {
			t.Fatalf("unexpected room %s", room)
		}
	}
}
