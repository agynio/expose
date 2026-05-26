package server

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	exposev1 "github.com/agynio/expose/.gen/go/agynio/api/expose/v1"
	runnersv1 "github.com/agynio/expose/.gen/go/agynio/api/runners/v1"
	zitimanagementv1 "github.com/agynio/expose/.gen/go/agynio/api/ziti_management/v1"
	"github.com/agynio/expose/internal/identitymeta"
	"github.com/agynio/expose/internal/store"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestGRPCServerAddExposurePropagatesIncomingIdentityToRunners(t *testing.T) {
	workloadID := uuid.New()
	agentID := uuid.New()
	orgID := uuid.New()

	runnersServer := startTestRunnersServer(t, func(ctx context.Context, req *runnersv1.GetWorkloadRequest) (*runnersv1.GetWorkloadResponse, error) {
		if req.GetId() != workloadID.String() {
			return nil, fmt.Errorf("unexpected workload id %s", req.GetId())
		}
		assertIncomingIdentity(t, ctx, agentID.String(), string(identityTypeAgent), workloadID.String())
		return &runnersv1.GetWorkloadResponse{Workload: &runnersv1.Workload{
			AgentId:        agentID.String(),
			OrganizationId: orgID.String(),
		}}, nil
	})

	storeMock := &mockStore{}
	var created store.Exposure
	var provisioned store.ExposureResourceIDs
	storeMock.createExposure = func(_ context.Context, exposure store.Exposure) error {
		created = exposure
		return nil
	}
	storeMock.updateExposureProvisioned = func(_ context.Context, id uuid.UUID, resources store.ExposureResourceIDs) error {
		provisioned = resources
		return nil
	}
	storeMock.getExposure = func(_ context.Context, id uuid.UUID) (store.Exposure, error) {
		return store.Exposure{
			ID:                   id,
			WorkloadID:           workloadID,
			AgentID:              agentID,
			Port:                 created.Port,
			OpenZitiServiceID:    provisioned.OpenZitiServiceID,
			OpenZitiBindPolicyID: provisioned.OpenZitiBindPolicyID,
			OpenZitiDialPolicyID: provisioned.OpenZitiDialPolicyID,
			URL:                  provisioned.URL,
			Status:               store.ExposureStatusActive,
			CreatedAt:            time.Now().UTC(),
			UpdatedAt:            time.Now().UTC(),
		}, nil
	}

	zitiMock := &mockZitiMgmt{}
	zitiMock.createService = func(_ context.Context, req *zitimanagementv1.CreateServiceRequest) (*zitimanagementv1.CreateServiceResponse, error) {
		return &zitimanagementv1.CreateServiceResponse{ZitiServiceId: "svc-id", ZitiServiceName: req.GetName()}, nil
	}
	zitiMock.createServicePolicy = func(_ context.Context, req *zitimanagementv1.CreateServicePolicyRequest) (*zitimanagementv1.CreateServicePolicyResponse, error) {
		return &zitimanagementv1.CreateServicePolicyResponse{ZitiServicePolicyId: uuid.NewString()}, nil
	}

	serverConn, err := grpc.NewClient(runnersServer.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("create runners client: %v", err)
	}
	t.Cleanup(func() { _ = serverConn.Close() })

	svc := NewGRPCServer(storeMock, zitiMock, serverConn, defaultAuthz())
	ctx := contextWithAgentIdentity(agentID, workloadID)
	resp, err := svc.AddExposure(ctx, &exposev1.AddExposureRequest{Port: 8080})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.GetExposure() == nil {
		t.Fatal("expected exposure")
	}
}

func TestIdentityRunnersClientFactoryRejectsMissingIncomingIdentity(t *testing.T) {
	_, err := identityRunnersClientFactory{conn: noopClientConn{}}.client(context.Background())
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("expected unauthenticated, got %v", err)
	}
}

type noopClientConn struct{}

func (noopClientConn) Invoke(context.Context, string, any, any, ...grpc.CallOption) error {
	return nil
}

func (noopClientConn) NewStream(context.Context, *grpc.StreamDesc, string, ...grpc.CallOption) (grpc.ClientStream, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

type testRunnersServer struct {
	addr string
}

type testRunnersService struct {
	runnersv1.UnimplementedRunnersServiceServer
	getWorkload func(ctx context.Context, req *runnersv1.GetWorkloadRequest) (*runnersv1.GetWorkloadResponse, error)
}

func (s testRunnersService) GetWorkload(ctx context.Context, req *runnersv1.GetWorkloadRequest) (*runnersv1.GetWorkloadResponse, error) {
	return s.getWorkload(ctx, req)
}

func startTestRunnersServer(
	t *testing.T,
	getWorkload func(ctx context.Context, req *runnersv1.GetWorkloadRequest) (*runnersv1.GetWorkloadResponse, error),
) testRunnersServer {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server := grpc.NewServer(grpc.UnaryInterceptor(requireIncomingIdentityUnaryInterceptor))
	runnersv1.RegisterRunnersServiceServer(server, testRunnersService{getWorkload: getWorkload})
	go func() {
		if err := server.Serve(listener); err != nil {
			t.Logf("runners test server stopped: %v", err)
		}
	}()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})
	return testRunnersServer{addr: listener.Addr().String()}
}

func requireIncomingIdentityUnaryInterceptor(
	ctx context.Context,
	req any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "unauthenticated: metadata x-identity-id: expected single value, got 0")
	}
	values := md.Get(identitymeta.IdentityIDMetadataKey)
	if len(values) != 1 {
		return nil, status.Errorf(codes.Unauthenticated, "unauthenticated: metadata x-identity-id: expected single value, got %d", len(values))
	}
	return handler(ctx, req)
}

func assertIncomingIdentity(t *testing.T, ctx context.Context, identityID string, identityType string, workloadID string) {
	t.Helper()
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		t.Fatal("expected incoming metadata")
	}
	assertMetadataSingleValue(t, md, identitymeta.IdentityIDMetadataKey, identityID)
	assertMetadataSingleValue(t, md, identitymeta.IdentityTypeMetadataKey, identityType)
	assertMetadataSingleValue(t, md, identitymeta.WorkloadIDMetadataKey, workloadID)
}

func assertMetadataSingleValue(t *testing.T, md metadata.MD, key string, expected string) {
	t.Helper()
	values := md.Get(key)
	if len(values) != 1 {
		t.Fatalf("expected single %s value, got %v", key, values)
	}
	if values[0] != expected {
		t.Fatalf("expected %s %s, got %s", key, expected, values[0])
	}
}
