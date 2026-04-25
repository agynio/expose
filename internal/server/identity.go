package server

import (
	"context"
	"fmt"
	"strings"

	authorizationv1 "github.com/agynio/expose/.gen/go/agynio/api/authorization/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	identityIDMetadataKey      = "x-identity-id"
	identityTypeMetadataKey    = "x-identity-type"
	workloadIDMetadataKey      = "x-workload-id"
	clusterAdminRelation       = "admin"
	clusterAdminObject         = "cluster:global"
	identityUserPrefix         = "identity:"
	organizationObjectPrefix   = "organization:"
	organizationOwnerRelation  = "owner"
	organizationMemberRelation = "member"
)

type identityType string

const (
	identityTypeUser   identityType = "user"
	identityTypeAgent  identityType = "agent"
	identityTypeApp    identityType = "app"
	identityTypeRunner identityType = "runner"
)

type resolvedIdentity struct {
	identityID   uuid.UUID
	identityType identityType
	workloadID   uuid.UUID
}

type exposureCaller struct {
	identity resolvedIdentity
}

func resolveExposureCaller(ctx context.Context) (exposureCaller, error) {
	resolved, err := identityFromContext(ctx)
	if err != nil {
		return exposureCaller{}, err
	}
	return exposureCaller{identity: resolved}, nil
}

func checkClusterAdmin(ctx context.Context, authz authorizationv1.AuthorizationServiceClient, identityID uuid.UUID) (bool, error) {
	if identityID == uuid.Nil {
		return false, status.Error(codes.Internal, "identity id missing for authorization check")
	}
	resp, err := authz.Check(ctx, &authorizationv1.CheckRequest{
		TupleKey: &authorizationv1.TupleKey{
			User:     identityUserPrefix + identityID.String(),
			Relation: clusterAdminRelation,
			Object:   clusterAdminObject,
		},
	})
	if err != nil {
		return false, status.Errorf(codes.Internal, "authorization check failed: %v", err)
	}
	return resp.GetAllowed(), nil
}

func requireClusterAdmin(ctx context.Context, authz authorizationv1.AuthorizationServiceClient, identityID uuid.UUID) error {
	allowed, err := checkClusterAdmin(ctx, authz, identityID)
	if err != nil {
		return err
	}
	if !allowed {
		return status.Error(codes.PermissionDenied, "identity is not authorized")
	}
	return nil
}

func resolveWorkloadIDFromRequest(caller exposureCaller, workloadID string) (uuid.UUID, error) {
	trimmed := strings.TrimSpace(workloadID)
	if trimmed != "" {
		parsedWorkloadID, err := parseUUID(trimmed, "workload_id")
		if err != nil {
			return uuid.UUID{}, status.Error(codes.InvalidArgument, err.Error())
		}
		if caller.identity.identityType == identityTypeAgent {
			if caller.identity.workloadID == uuid.Nil {
				return uuid.UUID{}, status.Error(codes.Unauthenticated, "workload id missing for agent identity")
			}
			if parsedWorkloadID != caller.identity.workloadID {
				return uuid.UUID{}, status.Error(codes.PermissionDenied, "workload id does not match agent identity")
			}
		}
		return parsedWorkloadID, nil
	}
	if caller.identity.identityType != identityTypeAgent {
		return uuid.UUID{}, status.Error(codes.InvalidArgument, "workload id is required")
	}
	if caller.identity.workloadID == uuid.Nil {
		return uuid.UUID{}, status.Error(codes.Unauthenticated, "workload id missing for agent identity")
	}
	return caller.identity.workloadID, nil
}

func ensureIDMatch(expectedID uuid.UUID, providedID, label string) error {
	trimmed := strings.TrimSpace(providedID)
	if trimmed == "" {
		return nil
	}
	parsedProvided, err := uuid.Parse(trimmed)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "%s id must be a valid UUID: %v", label, err)
	}
	if expectedID == uuid.Nil {
		return status.Errorf(codes.Internal, "expected %s id missing", label)
	}
	if parsedProvided != expectedID {
		return status.Error(codes.PermissionDenied, label+" id does not match workload")
	}
	return nil
}

func requireOrgRelation(ctx context.Context, authz authorizationv1.AuthorizationServiceClient, identityID, organizationID uuid.UUID, relation string) error {
	if identityID == uuid.Nil || organizationID == uuid.Nil {
		return status.Error(codes.Internal, "identity or organization id missing for authorization check")
	}
	resp, err := authz.Check(ctx, &authorizationv1.CheckRequest{
		TupleKey: &authorizationv1.TupleKey{
			User:     identityUserPrefix + identityID.String(),
			Relation: relation,
			Object:   organizationObjectPrefix + organizationID.String(),
		},
	})
	if err != nil {
		return status.Errorf(codes.Internal, "authorization check failed: %v", err)
	}
	if !resp.GetAllowed() {
		return status.Error(codes.PermissionDenied, "permission denied")
	}
	return nil
}

func identityFromContext(ctx context.Context) (resolvedIdentity, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return resolvedIdentity{}, status.Error(codes.Unauthenticated, "identity not available")
	}
	identityID := strings.TrimSpace(metadataValue(md, identityIDMetadataKey))
	identityTypeValue := strings.TrimSpace(metadataValue(md, identityTypeMetadataKey))
	if identityID == "" || identityTypeValue == "" {
		return resolvedIdentity{}, status.Error(codes.Unauthenticated, "identity not available")
	}
	identityType, err := parseIdentityType(identityTypeValue)
	if err != nil {
		return resolvedIdentity{}, status.Error(codes.Unauthenticated, err.Error())
	}
	parsedIdentityID, err := uuid.Parse(identityID)
	if err != nil {
		return resolvedIdentity{}, status.Errorf(codes.Unauthenticated, "identity id invalid: %v", err)
	}
	workloadIDValue := strings.TrimSpace(metadataValue(md, workloadIDMetadataKey))
	if identityType == identityTypeAgent && workloadIDValue == "" {
		return resolvedIdentity{}, status.Error(codes.Unauthenticated, "workload id missing for agent identity")
	}
	workloadID := uuid.Nil
	if workloadIDValue != "" {
		parsedWorkloadID, err := uuid.Parse(workloadIDValue)
		if err != nil {
			return resolvedIdentity{}, status.Errorf(codes.Unauthenticated, "workload id invalid: %v", err)
		}
		workloadID = parsedWorkloadID
	}
	return resolvedIdentity{identityID: parsedIdentityID, identityType: identityType, workloadID: workloadID}, nil
}

func parseIdentityType(value string) (identityType, error) {
	switch strings.TrimSpace(value) {
	case string(identityTypeUser):
		return identityTypeUser, nil
	case string(identityTypeAgent):
		return identityTypeAgent, nil
	case string(identityTypeApp):
		return identityTypeApp, nil
	case string(identityTypeRunner):
		return identityTypeRunner, nil
	default:
		return "", fmt.Errorf("unsupported identity type: %q", value)
	}
}

func metadataValue(md metadata.MD, key string) string {
	values := md.Get(key)
	if len(values) == 0 {
		return ""
	}
	return strings.TrimSpace(values[0])
}

func outgoingContextWithIdentity(ctx context.Context, identity resolvedIdentity) context.Context {
	md := metadata.Pairs(
		identityIDMetadataKey, identity.identityID.String(),
		identityTypeMetadataKey, string(identity.identityType),
	)
	if identity.workloadID != uuid.Nil {
		md.Append(workloadIDMetadataKey, identity.workloadID.String())
	}
	return metadata.NewOutgoingContext(ctx, md)
}
