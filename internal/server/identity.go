package server

import (
	"context"
	"fmt"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	identityIDMetadataKey   = "x-identity-id"
	identityTypeMetadataKey = "x-identity-type"
	workloadIDMetadataKey   = "x-workload-id"
)

type identityType string

const (
	identityTypeUser   identityType = "user"
	identityTypeAgent  identityType = "agent"
	identityTypeApp    identityType = "app"
	identityTypeRunner identityType = "runner"
)

type resolvedIdentity struct {
	identityID   string
	identityType identityType
	workloadID   string
}

type exposureCaller struct {
	identity       resolvedIdentity
	isClusterAdmin bool
}

func resolveExposureCaller(ctx context.Context, clusterAdminIdentityID string) (exposureCaller, error) {
	resolved, err := identityFromContext(ctx)
	if err != nil {
		return exposureCaller{}, err
	}
	if clusterAdminIdentityID != "" && resolved.identityID == clusterAdminIdentityID {
		return exposureCaller{identity: resolved, isClusterAdmin: true}, nil
	}
	if resolved.identityType != identityTypeAgent {
		return exposureCaller{}, status.Error(codes.PermissionDenied, "identity is not an agent")
	}
	identityID := strings.TrimSpace(resolved.identityID)
	if identityID == "" {
		return exposureCaller{}, status.Error(codes.Internal, "agent id missing for agent identity")
	}
	workloadID := strings.TrimSpace(resolved.workloadID)
	if workloadID == "" {
		return exposureCaller{}, status.Error(codes.Internal, "workload id missing for agent identity")
	}
	resolved.identityID = identityID
	resolved.workloadID = workloadID
	return exposureCaller{identity: resolved}, nil
}

func resolveAddExposureIDs(caller exposureCaller, workloadID, agentID string) (string, string, error) {
	if caller.isClusterAdmin {
		resolvedWorkloadID, err := requireClusterAdminID(workloadID, "workload")
		if err != nil {
			return "", "", err
		}
		resolvedAgentID, err := requireClusterAdminID(agentID, "agent")
		if err != nil {
			return "", "", err
		}
		return resolvedWorkloadID, resolvedAgentID, nil
	}
	resolvedWorkloadID, err := resolveAgentIDMatch(caller.identity.workloadID, workloadID, "workload")
	if err != nil {
		return "", "", err
	}
	resolvedAgentID, err := resolveAgentIDMatch(caller.identity.identityID, agentID, "agent")
	if err != nil {
		return "", "", err
	}
	return resolvedWorkloadID, resolvedAgentID, nil
}

func resolveWorkloadID(caller exposureCaller, workloadID string) (string, error) {
	if caller.isClusterAdmin {
		return requireClusterAdminID(workloadID, "workload")
	}
	return resolveAgentIDMatch(caller.identity.workloadID, workloadID, "workload")
}

func requireClusterAdminID(value, label string) (string, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return "", status.Error(codes.PermissionDenied, label+" id required for cluster admin")
	}
	return trimmed, nil
}

func resolveAgentIDMatch(expectedID, providedID, label string) (string, error) {
	trimmed := strings.TrimSpace(providedID)
	if trimmed != "" && trimmed != expectedID {
		return "", status.Error(codes.PermissionDenied, label+" id does not match identity")
	}
	return expectedID, nil
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
	workloadID := strings.TrimSpace(metadataValue(md, workloadIDMetadataKey))
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
