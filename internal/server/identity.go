package server

import (
	"context"
	"fmt"
	"strings"

	authorizationv1 "github.com/agynio/expose/.gen/go/agynio/api/authorization/v1"
	"github.com/agynio/expose/internal/identitymeta"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	clusterAdminRelation       = "admin"
	clusterAdminObject         = "cluster:global"
	identityUserPrefix         = "identity:"
	organizationObjectPrefix   = "organization:"
	organizationOwnerRelation  = "owner"
	organizationMemberRelation = "member"
)

type identityType string

const (
	identityTypeUser   identityType = identitymeta.IdentityTypeUser
	identityTypeAgent  identityType = identitymeta.IdentityTypeAgent
	identityTypeApp    identityType = identitymeta.IdentityTypeApp
	identityTypeRunner identityType = identitymeta.IdentityTypeRunner
)

type resolvedIdentity struct {
	identityID   string
	identityType identityType
	workloadID   string
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

func checkClusterAdmin(ctx context.Context, authz authorizationv1.AuthorizationServiceClient, identityID string) (bool, error) {
	if identityID == "" {
		return false, status.Error(codes.Internal, "identity id missing for authorization check")
	}
	resp, err := authz.Check(ctx, &authorizationv1.CheckRequest{
		TupleKey: &authorizationv1.TupleKey{
			User:     identityUserPrefix + identityID,
			Relation: clusterAdminRelation,
			Object:   clusterAdminObject,
		},
	})
	if err != nil {
		return false, status.Errorf(codes.Internal, "authorization check failed: %v", err)
	}
	return resp.GetAllowed(), nil
}

func requireClusterAdmin(ctx context.Context, authz authorizationv1.AuthorizationServiceClient, identityID string) error {
	allowed, err := checkClusterAdmin(ctx, authz, identityID)
	if err != nil {
		return err
	}
	if !allowed {
		return status.Error(codes.PermissionDenied, "identity is not authorized")
	}
	return nil
}

func resolveWorkloadIDFromRequest(caller exposureCaller, workloadID string) (string, error) {
	trimmed := strings.TrimSpace(workloadID)
	if trimmed != "" {
		return trimmed, nil
	}
	if caller.identity.identityType != identityTypeAgent {
		return "", status.Error(codes.InvalidArgument, "workload id is required")
	}
	callerWorkloadID := strings.TrimSpace(caller.identity.workloadID)
	if callerWorkloadID == "" {
		return "", status.Error(codes.Unauthenticated, "workload id missing for agent identity")
	}
	return callerWorkloadID, nil
}

func ensureIDMatch(expectedID, providedID, label string) error {
	trimmed := strings.TrimSpace(providedID)
	if trimmed == "" {
		return nil
	}
	parsedProvided, err := uuid.Parse(trimmed)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "%s id must be a valid UUID: %v", label, err)
	}
	expectedParsed, err := uuid.Parse(strings.TrimSpace(expectedID))
	if err != nil {
		return status.Errorf(codes.Internal, "expected %s id invalid: %v", label, err)
	}
	if parsedProvided != expectedParsed {
		return status.Error(codes.PermissionDenied, label+" id does not match workload")
	}
	return nil
}

func requireOrgRelation(ctx context.Context, authz authorizationv1.AuthorizationServiceClient, identityID, organizationID, relation string) error {
	if identityID == "" || organizationID == "" {
		return status.Error(codes.Internal, "identity or organization id missing for authorization check")
	}
	resp, err := authz.Check(ctx, &authorizationv1.CheckRequest{
		TupleKey: &authorizationv1.TupleKey{
			User:     identityUserPrefix + identityID,
			Relation: relation,
			Object:   organizationObjectPrefix + organizationID,
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
	identityID := strings.TrimSpace(metadataValue(md, identitymeta.IdentityIDMetadataKey))
	identityTypeValue := strings.TrimSpace(metadataValue(md, identitymeta.IdentityTypeMetadataKey))
	if identityID == "" || identityTypeValue == "" {
		return resolvedIdentity{}, status.Error(codes.Unauthenticated, "identity not available")
	}
	identityType, err := parseIdentityType(identityTypeValue)
	if err != nil {
		return resolvedIdentity{}, status.Error(codes.Unauthenticated, err.Error())
	}
	workloadID := strings.TrimSpace(metadataValue(md, identitymeta.WorkloadIDMetadataKey))
	return resolvedIdentity{identityID: identityID, identityType: identityType, workloadID: workloadID}, nil
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
	merged := metadata.MD{}
	if existing, ok := metadata.FromOutgoingContext(ctx); ok {
		for key, values := range existing {
			if len(values) == 0 {
				continue
			}
			merged[key] = append([]string(nil), values...)
		}
	}
	merged.Set(identitymeta.IdentityIDMetadataKey, identity.identityID)
	merged.Set(identitymeta.IdentityTypeMetadataKey, string(identity.identityType))
	if strings.TrimSpace(identity.workloadID) != "" {
		merged.Set(identitymeta.WorkloadIDMetadataKey, identity.workloadID)
	} else {
		merged.Delete(identitymeta.WorkloadIDMetadataKey)
	}
	return metadata.NewOutgoingContext(ctx, merged)
}
