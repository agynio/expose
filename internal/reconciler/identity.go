package reconciler

import (
	"context"

	"github.com/agynio/expose/internal/identitymeta"
	"github.com/agynio/expose/internal/store"
	"google.golang.org/grpc/metadata"
)

func contextWithExposureIdentity(ctx context.Context, exposure store.Exposure) context.Context {
	merged := metadata.MD{}
	if existing, ok := metadata.FromOutgoingContext(ctx); ok {
		for key, values := range existing {
			if len(values) == 0 {
				continue
			}
			merged[key] = append([]string(nil), values...)
		}
	}
	merged.Set(identitymeta.IdentityIDMetadataKey, exposure.AgentID.String())
	merged.Set(identitymeta.IdentityTypeMetadataKey, identitymeta.IdentityTypeAgent)
	merged.Set(identitymeta.WorkloadIDMetadataKey, exposure.WorkloadID.String())
	return metadata.NewOutgoingContext(ctx, merged)
}
