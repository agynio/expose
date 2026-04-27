package reconciler

import (
	"context"

	"github.com/agynio/expose/internal/store"
	"google.golang.org/grpc/metadata"
)

const (
	identityIDMetadataKey   = "x-identity-id"
	identityTypeMetadataKey = "x-identity-type"
	workloadIDMetadataKey   = "x-workload-id"
	identityTypeAgent       = "agent"
)

func contextWithExposureIdentity(ctx context.Context, exposure store.Exposure) context.Context {
	md := metadata.Pairs(
		identityIDMetadataKey, exposure.AgentID.String(),
		identityTypeMetadataKey, identityTypeAgent,
		workloadIDMetadataKey, exposure.WorkloadID.String(),
	)
	return metadata.NewOutgoingContext(ctx, md)
}
