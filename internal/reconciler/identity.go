package reconciler

import (
	"context"

	"github.com/agynio/expose/internal/identitymeta"
	"github.com/agynio/expose/internal/store"
	"google.golang.org/grpc/metadata"
)

// contextWithExposureIdentity presents the exposure's own workload to Runners,
// which is the identity that may read that workload's record. A workload
// authenticates as its owner, so the owner id and its kind are what to send.
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
	merged.Set(identitymeta.IdentityIDMetadataKey, exposure.OwnerID.String())
	merged.Set(identitymeta.IdentityTypeMetadataKey, identityTypeForOwner(exposure.OwnerKind))
	merged.Set(identitymeta.WorkloadIDMetadataKey, exposure.WorkloadID.String())
	return metadata.NewOutgoingContext(ctx, merged)
}

func identityTypeForOwner(kind store.OwnerKind) string {
	if kind == store.OwnerKindSandbox {
		return identitymeta.IdentityTypeSandbox
	}
	return identitymeta.IdentityTypeAgentInstance
}
