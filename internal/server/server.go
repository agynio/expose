package server

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"

	authorizationv1 "github.com/agynio/expose/.gen/go/agynio/api/authorization/v1"
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
	authz    authorizationv1.AuthorizationServiceClient
}

func New(store ExposureStore, zitiMgmt zitimanagementv1.ZitiManagementServiceClient, runners runnersv1.RunnersServiceClient, authz authorizationv1.AuthorizationServiceClient) *Server {
	if authz == nil {
		panic("authorization client is required")
	}
	return &Server{store: store, zitiMgmt: zitiMgmt, runners: runners, authz: authz}
}

func (s *Server) AddExposure(ctx context.Context, req *exposev1.AddExposureRequest) (*exposev1.AddExposureResponse, error) {
	caller, err := resolveExposureCaller(ctx)
	if err != nil {
		return nil, err
	}
	explicitWorkloadID := strings.TrimSpace(req.GetWorkloadId())
	var workloadID uuid.UUID
	var agentID uuid.UUID
	if explicitWorkloadID != "" {
		if err := requireClusterAdmin(ctx, s.authz, caller.identity.identityID); err != nil {
			return nil, err
		}
		parsedWorkloadID, err := parseUUID(explicitWorkloadID, "workload_id")
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		parsedAgentID, err := parseUUID(req.GetAgentId(), "agent_id")
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		workloadID = parsedWorkloadID
		agentID = parsedAgentID
	} else {
		if caller.identity.identityType != identityTypeAgent {
			return nil, status.Error(codes.PermissionDenied, "permission denied")
		}
		resolvedWorkloadID, err := resolveWorkloadIDFromRequest(caller, "")
		if err != nil {
			return nil, err
		}
		parsedWorkloadID, err := parseUUID(resolvedWorkloadID, "workload_id")
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		workloadID = parsedWorkloadID
	}
	port := req.GetPort()
	if err := validatePort(port); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if explicitWorkloadID == "" {
		workload, err := s.fetchWorkload(ctx, caller, workloadID)
		if err != nil {
			return nil, err
		}
		agentIDValue, err := workloadAgentID(workload)
		if err != nil {
			return nil, err
		}
		if err := ensureIDMatch(agentIDValue.String(), req.GetAgentId(), "agent"); err != nil {
			return nil, err
		}
		if err := requireAgentSelf(caller, agentIDValue); err != nil {
			return nil, err
		}
		agentID = agentIDValue
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
	serviceID := serviceResp.GetZitiServiceId()
	resources.OpenZitiServiceID = serviceID

	bindResp, err := s.zitiMgmt.CreateServicePolicy(ctx, &zitimanagementv1.CreateServicePolicyRequest{
		Type:          zitimanagementv1.ServicePolicyType_SERVICE_POLICY_TYPE_BIND,
		Name:          fmt.Sprintf("%s-bind", serviceName),
		IdentityRoles: []string{fmt.Sprintf("#workload-%s", workloadID)},
		ServiceRoles:  []string{fmt.Sprintf("@%s", serviceID)},
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
		ServiceRoles:  []string{fmt.Sprintf("@%s", serviceID)},
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
	caller, err := resolveExposureCaller(ctx)
	if err != nil {
		return nil, err
	}
	workloadIDValue, err := resolveWorkloadIDFromRequest(caller, req.GetWorkloadId())
	if err != nil {
		return nil, err
	}
	workloadID, err := parseUUID(workloadIDValue, "workload_id")
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	port := req.GetPort()
	if err := validatePort(port); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	workload, err := s.fetchWorkload(ctx, caller, workloadID)
	if err != nil {
		return nil, err
	}
	agentID, err := workloadAgentID(workload)
	if err != nil {
		return nil, err
	}
	orgID, err := workloadOrganizationID(workload)
	if err != nil {
		return nil, err
	}
	allowed, err := agentMatchesWorkload(caller, agentID)
	if err != nil {
		return nil, err
	}
	if !allowed {
		if err := requireOrgRelation(ctx, s.authz, caller.identity.identityID, orgID.String(), organizationOwnerRelation); err != nil {
			return nil, err
		}
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
	caller, err := resolveExposureCaller(ctx)
	if err != nil {
		return nil, err
	}
	workloadIDValue, err := resolveWorkloadIDFromRequest(caller, req.GetWorkloadId())
	if err != nil {
		return nil, err
	}
	workloadID, err := parseUUID(workloadIDValue, "workload_id")
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	workload, err := s.fetchWorkload(ctx, caller, workloadID)
	if err != nil {
		return nil, err
	}
	agentID, err := workloadAgentID(workload)
	if err != nil {
		return nil, err
	}
	orgID, err := workloadOrganizationID(workload)
	if err != nil {
		return nil, err
	}
	allowed, err := agentMatchesWorkload(caller, agentID)
	if err != nil {
		return nil, err
	}
	if !allowed {
		if err := requireOrgRelation(ctx, s.authz, caller.identity.identityID, orgID.String(), organizationMemberRelation); err != nil {
			return nil, err
		}
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

func (s *Server) fetchWorkload(ctx context.Context, caller exposureCaller, workloadID uuid.UUID) (*runnersv1.Workload, error) {
	resp, err := s.runners.GetWorkload(outgoingContextWithIdentity(ctx, caller.identity), &runnersv1.GetWorkloadRequest{Id: workloadID.String()})
	if err != nil {
		return nil, mapRunnersWorkloadError(err)
	}
	workload := resp.GetWorkload()
	if workload == nil {
		return nil, status.Error(codes.Internal, "workload missing from response")
	}
	return workload, nil
}

func mapRunnersWorkloadError(err error) error {
	st, ok := status.FromError(err)
	if !ok {
		return err
	}
	if st.Code() == codes.Unauthenticated {
		return status.Errorf(codes.Unauthenticated, "runners authentication failed: %s", st.Message())
	}
	if st.Code() == codes.PermissionDenied {
		return status.Errorf(codes.PermissionDenied, "runners authorization failed: %s", st.Message())
	}
	if st.Code() == codes.NotFound {
		return status.Errorf(codes.FailedPrecondition, "workload not found: %v", err)
	}
	return err
}

func workloadAgentID(workload *runnersv1.Workload) (uuid.UUID, error) {
	if workload == nil {
		return uuid.UUID{}, status.Error(codes.Internal, "workload missing")
	}
	agentID := strings.TrimSpace(workload.GetAgentId())
	if agentID == "" {
		return uuid.UUID{}, status.Error(codes.Internal, "workload agent_id missing")
	}
	parsed, err := parseUUID(agentID, "agent_id")
	if err != nil {
		return uuid.UUID{}, status.Errorf(codes.Internal, "workload agent_id invalid: %v", err)
	}
	return parsed, nil
}

func workloadOrganizationID(workload *runnersv1.Workload) (uuid.UUID, error) {
	if workload == nil {
		return uuid.UUID{}, status.Error(codes.Internal, "workload missing")
	}
	orgID := strings.TrimSpace(workload.GetOrganizationId())
	if orgID == "" {
		return uuid.UUID{}, status.Error(codes.Internal, "workload organization_id missing")
	}
	parsed, err := parseUUID(orgID, "organization_id")
	if err != nil {
		return uuid.UUID{}, status.Errorf(codes.Internal, "workload organization_id invalid: %v", err)
	}
	return parsed, nil
}

func agentMatchesWorkload(caller exposureCaller, agentID uuid.UUID) (bool, error) {
	if caller.identity.identityType != identityTypeAgent {
		return false, nil
	}
	callerID, err := parseIdentityUUID(caller.identity.identityID)
	if err != nil {
		return false, err
	}
	return callerID == agentID, nil
}

func requireAgentSelf(caller exposureCaller, agentID uuid.UUID) error {
	allowed, err := agentMatchesWorkload(caller, agentID)
	if err != nil {
		return err
	}
	if !allowed {
		return status.Error(codes.PermissionDenied, "agent id does not match workload")
	}
	return nil
}

func parseIdentityUUID(value string) (uuid.UUID, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return uuid.UUID{}, status.Error(codes.Unauthenticated, "identity id missing")
	}
	parsed, err := uuid.Parse(trimmed)
	if err != nil {
		return uuid.UUID{}, status.Errorf(codes.Unauthenticated, "identity id invalid: %v", err)
	}
	return parsed, nil
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
