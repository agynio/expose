package server

import (
	"context"
	"errors"
	"fmt"
	"log"

	exposev1 "github.com/agynio/expose/.gen/go/agynio/api/expose/v1"
	runnersv1 "github.com/agynio/expose/.gen/go/agynio/api/runners/v1"
	zitimanagementv1 "github.com/agynio/expose/.gen/go/agynio/api/ziti_management/v1"
	"github.com/agynio/expose/internal/store"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ExposureStore interface {
	CreateExposure(ctx context.Context, exposure store.Exposure) error
	GetExposure(ctx context.Context, id uuid.UUID) (store.Exposure, error)
	GetExposureByWorkloadAndPort(ctx context.Context, workloadID uuid.UUID, port int32) (store.Exposure, error)
	ListExposuresByWorkload(ctx context.Context, workloadID uuid.UUID, pageSize int32, cursor *store.PageCursor) (store.ListResult, error)
	UpdateExposureProvisioned(ctx context.Context, id uuid.UUID, resources store.ExposureResourceIDs) error
	UpdateExposureStatus(ctx context.Context, id uuid.UUID, status store.ExposureStatus) error
	UpdateExposureFailed(ctx context.Context, id uuid.UUID, resources store.ExposureResourceIDs) error
	DeleteExposure(ctx context.Context, id uuid.UUID) error
}

type Server struct {
	exposev1.UnimplementedExposeServiceServer
	store    ExposureStore
	zitiMgmt zitimanagementv1.ZitiManagementServiceClient
	runners  runnersv1.RunnersServiceClient
}

func New(store ExposureStore, zitiMgmt zitimanagementv1.ZitiManagementServiceClient, runners runnersv1.RunnersServiceClient) *Server {
	return &Server{store: store, zitiMgmt: zitiMgmt, runners: runners}
}

func (s *Server) AddExposure(ctx context.Context, req *exposev1.AddExposureRequest) (*exposev1.AddExposureResponse, error) {
	workloadID, err := parseUUID(req.GetWorkloadId(), "workload_id")
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	agentID, err := parseUUID(req.GetAgentId(), "agent_id")
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	port := req.GetPort()
	if err := validatePort(port); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	exposureID := uuid.New()
	exposure := store.Exposure{
		ID:         exposureID,
		WorkloadID: workloadID,
		AgentID:    agentID,
		Port:       port,
		Status:     store.ExposureStatusProvisioning,
	}
	if err := s.store.CreateExposure(ctx, exposure); err != nil {
		return nil, toStatusError(err)
	}

	if _, err := s.runners.GetWorkload(ctx, &runnersv1.GetWorkloadRequest{Id: workloadID.String()}); err != nil {
		if cleanupErr := s.store.DeleteExposure(ctx, exposureID); cleanupErr != nil && !errors.Is(cleanupErr, store.ErrExposureNotFound) {
			log.Printf("failed to delete exposure %s after workload lookup error: %v", exposureID, cleanupErr)
		}
		return nil, status.Errorf(codes.FailedPrecondition, "workload not found: %v", err)
	}

	serviceName := fmt.Sprintf("exposed-%s", exposureID)
	interceptAddress := fmt.Sprintf("%s.ziti", serviceName)
	url := fmt.Sprintf("http://%s:%d", interceptAddress, port)

	resources := store.ExposureResourceIDs{URL: url}
	serviceResp, err := s.zitiMgmt.CreateService(ctx, &zitimanagementv1.CreateServiceRequest{
		Name:           serviceName,
		RoleAttributes: []string{"exposed-services"},
		HostV1Config: &zitimanagementv1.HostV1Config{
			Protocol: "tcp",
			Address:  "localhost",
			Port:     port,
		},
		InterceptV1Config: &zitimanagementv1.InterceptV1Config{
			Protocols: []string{"tcp"},
			Addresses: []string{interceptAddress},
			PortRanges: []*zitimanagementv1.PortRange{
				{Low: port, High: port},
			},
		},
	})
	if err != nil {
		s.handleProvisioningFailure(ctx, exposureID, resources)
		return nil, status.Errorf(codes.Internal, "create service: %v", err)
	}
	resources.OpenZitiServiceID = serviceResp.GetZitiServiceId()

	bindResp, err := s.zitiMgmt.CreateServicePolicy(ctx, &zitimanagementv1.CreateServicePolicyRequest{
		Type:          zitimanagementv1.ServicePolicyType_SERVICE_POLICY_TYPE_BIND,
		Name:          fmt.Sprintf("%s-bind", serviceName),
		IdentityRoles: []string{fmt.Sprintf("#workload-%s", workloadID)},
		ServiceRoles:  []string{fmt.Sprintf("@%s", serviceName)},
	})
	if err != nil {
		s.handleProvisioningFailure(ctx, exposureID, resources)
		return nil, status.Errorf(codes.Internal, "create bind policy: %v", err)
	}
	resources.OpenZitiBindPolicyID = bindResp.GetZitiServicePolicyId()

	dialResp, err := s.zitiMgmt.CreateServicePolicy(ctx, &zitimanagementv1.CreateServicePolicyRequest{
		Type:          zitimanagementv1.ServicePolicyType_SERVICE_POLICY_TYPE_DIAL,
		Name:          fmt.Sprintf("%s-dial", serviceName),
		IdentityRoles: []string{"#all"},
		ServiceRoles:  []string{fmt.Sprintf("@%s", serviceName)},
	})
	if err != nil {
		s.handleProvisioningFailure(ctx, exposureID, resources)
		return nil, status.Errorf(codes.Internal, "create dial policy: %v", err)
	}
	resources.OpenZitiDialPolicyID = dialResp.GetZitiServicePolicyId()

	if err := s.store.UpdateExposureProvisioned(ctx, exposureID, resources); err != nil {
		s.handleProvisioningFailure(ctx, exposureID, resources)
		return nil, status.Errorf(codes.Internal, "update exposure: %v", err)
	}

	stored, err := s.store.GetExposure(ctx, exposureID)
	if err != nil {
		return nil, toStatusError(err)
	}
	return &exposev1.AddExposureResponse{Exposure: toProtoExposure(stored)}, nil
}

func (s *Server) RemoveExposure(ctx context.Context, req *exposev1.RemoveExposureRequest) (*exposev1.RemoveExposureResponse, error) {
	workloadID, err := parseUUID(req.GetWorkloadId(), "workload_id")
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	port := req.GetPort()
	if err := validatePort(port); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	exposure, err := s.store.GetExposureByWorkloadAndPort(ctx, workloadID, port)
	if err != nil {
		return nil, toStatusError(err)
	}
	if err := s.store.UpdateExposureStatus(ctx, exposure.ID, store.ExposureStatusRemoving); err != nil {
		return nil, toStatusError(err)
	}

	if err := s.deleteExposureResources(ctx, exposure); err != nil {
		return nil, status.Errorf(codes.Internal, "delete exposure resources: %v", err)
	}
	if err := s.store.DeleteExposure(ctx, exposure.ID); err != nil {
		return nil, toStatusError(err)
	}
	return &exposev1.RemoveExposureResponse{}, nil
}

func (s *Server) ListExposures(ctx context.Context, req *exposev1.ListExposuresRequest) (*exposev1.ListExposuresResponse, error) {
	workloadID, err := parseUUID(req.GetWorkloadId(), "workload_id")
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	var cursor *store.PageCursor
	if token := req.GetPageToken(); token != "" {
		id, err := store.DecodePageToken(token)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "page_token: %v", err)
		}
		cursor = &store.PageCursor{AfterID: id}
	}
	result, err := s.store.ListExposuresByWorkload(ctx, workloadID, req.GetPageSize(), cursor)
	if err != nil {
		return nil, toStatusError(err)
	}
	items, nextToken, err := mapListResult(result)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "encode page token: %v", err)
	}
	return &exposev1.ListExposuresResponse{Exposures: items, NextPageToken: nextToken}, nil
}

func (s *Server) handleProvisioningFailure(ctx context.Context, exposureID uuid.UUID, resources store.ExposureResourceIDs) {
	remaining := resources
	cleanupFailed := false

	if remaining.OpenZitiDialPolicyID != "" {
		if err := s.deleteServicePolicy(ctx, remaining.OpenZitiDialPolicyID); err != nil {
			cleanupFailed = true
			log.Printf("failed to delete dial policy %s: %v", remaining.OpenZitiDialPolicyID, err)
		} else {
			remaining.OpenZitiDialPolicyID = ""
		}
	}
	if remaining.OpenZitiBindPolicyID != "" {
		if err := s.deleteServicePolicy(ctx, remaining.OpenZitiBindPolicyID); err != nil {
			cleanupFailed = true
			log.Printf("failed to delete bind policy %s: %v", remaining.OpenZitiBindPolicyID, err)
		} else {
			remaining.OpenZitiBindPolicyID = ""
		}
	}
	if remaining.OpenZitiServiceID != "" {
		if err := s.deleteService(ctx, remaining.OpenZitiServiceID); err != nil {
			cleanupFailed = true
			log.Printf("failed to delete service %s: %v", remaining.OpenZitiServiceID, err)
		} else {
			remaining.OpenZitiServiceID = ""
		}
	}

	if !cleanupFailed {
		if err := s.store.DeleteExposure(ctx, exposureID); err != nil && !errors.Is(err, store.ErrExposureNotFound) {
			log.Printf("failed to delete exposure %s after cleanup: %v", exposureID, err)
		}
		return
	}
	s.persistFailed(ctx, exposureID, remaining)
}

func (s *Server) deleteExposureResources(ctx context.Context, exposure store.Exposure) error {
	remaining := store.ExposureResourceIDs{
		OpenZitiServiceID:    exposure.OpenZitiServiceID,
		OpenZitiBindPolicyID: exposure.OpenZitiBindPolicyID,
		OpenZitiDialPolicyID: exposure.OpenZitiDialPolicyID,
		URL:                  exposure.URL,
	}
	if remaining.OpenZitiDialPolicyID != "" {
		if err := s.deleteServicePolicy(ctx, remaining.OpenZitiDialPolicyID); err != nil {
			s.persistFailed(ctx, exposure.ID, remaining)
			return err
		}
		remaining.OpenZitiDialPolicyID = ""
	}
	if remaining.OpenZitiBindPolicyID != "" {
		if err := s.deleteServicePolicy(ctx, remaining.OpenZitiBindPolicyID); err != nil {
			s.persistFailed(ctx, exposure.ID, remaining)
			return err
		}
		remaining.OpenZitiBindPolicyID = ""
	}
	if remaining.OpenZitiServiceID != "" {
		if err := s.deleteService(ctx, remaining.OpenZitiServiceID); err != nil {
			s.persistFailed(ctx, exposure.ID, remaining)
			return err
		}
		remaining.OpenZitiServiceID = ""
	}
	return nil
}

func (s *Server) persistFailed(ctx context.Context, exposureID uuid.UUID, resources store.ExposureResourceIDs) {
	if err := s.store.UpdateExposureFailed(ctx, exposureID, resources); err != nil {
		log.Printf("failed to persist exposure %s as failed: %v", exposureID, err)
	}
}

func (s *Server) deleteServicePolicy(ctx context.Context, id string) error {
	if id == "" {
		return nil
	}
	_, err := s.zitiMgmt.DeleteServicePolicy(ctx, &zitimanagementv1.DeleteServicePolicyRequest{ZitiServicePolicyId: id})
	return err
}

func (s *Server) deleteService(ctx context.Context, id string) error {
	if id == "" {
		return nil
	}
	_, err := s.zitiMgmt.DeleteService(ctx, &zitimanagementv1.DeleteServiceRequest{ZitiServiceId: id})
	return err
}

func validatePort(port int32) error {
	if port < 1 || port > 65535 {
		return fmt.Errorf("port must be between 1 and 65535")
	}
	return nil
}

func mapListResult(result store.ListResult) ([]*exposev1.Exposure, string, error) {
	items := make([]*exposev1.Exposure, len(result.Exposures))
	for i, exposure := range result.Exposures {
		items[i] = toProtoExposure(exposure)
	}
	if result.NextCursor == nil {
		return items, "", nil
	}
	token, err := store.EncodePageToken(result.NextCursor.AfterID)
	if err != nil {
		return nil, "", err
	}
	return items, token, nil
}

func toStatusError(err error) error {
	switch {
	case errors.Is(err, store.ErrExposureAlreadyExists):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, store.ErrExposureNotFound):
		return status.Error(codes.NotFound, err.Error())
	default:
		return status.Errorf(codes.Internal, "internal error: %v", err)
	}
}
