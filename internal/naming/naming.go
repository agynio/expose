// Package naming resolves the name components an exposure address is built
// from: the organization slug, and either a sandbox name or an agent instance
// handle.
//
// Every lookup here is off the request hot path — once the intercept.v1 config
// exists, the address is resolved inside the dialing tunneler and nothing in
// this package is consulted to serve traffic.
package naming

import (
	"context"
	"fmt"
	"strings"

	agentsv1 "github.com/agynio/expose/.gen/go/agynio/api/agents/v1"
	identityv1 "github.com/agynio/expose/.gen/go/agynio/api/identity/v1"
	organizationsv1 "github.com/agynio/expose/.gen/go/agynio/api/organizations/v1"
	"github.com/agynio/expose/internal/hostname"
	"github.com/google/uuid"
)

// Resolver reads names from the services that own them. Calls carry no caller
// identity: this is an internal lookup, and the services it reaches answer it
// as one — a sandbox workload holds no tuple over its own sandbox record, and
// the reconciler that re-derives addresses has no caller at all.
type Resolver struct {
	organizations organizationsv1.OrganizationsServiceClient
	agents        agentsv1.AgentsServiceClient
	identity      identityv1.IdentityServiceClient
}

func NewResolver(
	organizations organizationsv1.OrganizationsServiceClient,
	agents agentsv1.AgentsServiceClient,
	identity identityv1.IdentityServiceClient,
) *Resolver {
	return &Resolver{organizations: organizations, agents: agents, identity: identity}
}

// Target names the entity an exposure belongs to.
type Target struct {
	OrganizationID uuid.UUID
	OwnerKind      hostname.OwnerKind
	OwnerID        uuid.UUID
}

// Resolve returns the organization slug and owner labels for a target.
func (r *Resolver) Resolve(ctx context.Context, target Target) (string, hostname.Owner, error) {
	slug, err := r.organizationSlug(ctx, target.OrganizationID)
	if err != nil {
		return "", hostname.Owner{}, err
	}
	owner, err := r.owner(ctx, target)
	if err != nil {
		return "", hostname.Owner{}, err
	}
	return slug, owner, nil
}

func (r *Resolver) organizationSlug(ctx context.Context, organizationID uuid.UUID) (string, error) {
	resp, err := r.organizations.GetOrganization(ctx, &organizationsv1.GetOrganizationRequest{
		Id: organizationID.String(),
	})
	if err != nil {
		return "", fmt.Errorf("get organization %s: %w", organizationID, err)
	}
	return strings.TrimSpace(resp.GetOrganization().GetSlug()), nil
}

func (r *Resolver) owner(ctx context.Context, target Target) (hostname.Owner, error) {
	switch target.OwnerKind {
	case hostname.OwnerKindSandbox:
		resp, err := r.agents.GetSandbox(ctx, &agentsv1.GetSandboxRequest{
			Ref: &agentsv1.GetSandboxRequest_Id{Id: target.OwnerID.String()},
		})
		if err != nil {
			return hostname.Owner{}, fmt.Errorf("get sandbox %s: %w", target.OwnerID, err)
		}
		return hostname.Owner{
			Kind:        hostname.OwnerKindSandbox,
			SandboxName: strings.TrimSpace(resp.GetSandbox().GetName()),
		}, nil

	case hostname.OwnerKindAgentInstance:
		resp, err := r.identity.BatchGetNicknames(ctx, &identityv1.BatchGetNicknamesRequest{
			OrganizationId: target.OrganizationID.String(),
			IdentityIds:    []string{target.OwnerID.String()},
		})
		if err != nil {
			return hostname.Owner{}, fmt.Errorf("get nickname for instance %s: %w", target.OwnerID, err)
		}
		owner := hostname.Owner{Kind: hostname.OwnerKindAgentInstance}
		// An instance whose class carries no nickname is omitted from the
		// response rather than returned empty. That is not an error — it is the
		// case the opaque fallback exists for.
		for _, entry := range resp.GetEntries() {
			if !strings.EqualFold(strings.TrimSpace(entry.GetIdentityId()), target.OwnerID.String()) {
				continue
			}
			owner.Nickname = strings.TrimSpace(entry.GetNickname())
			owner.InstanceSuffix = strings.TrimSpace(entry.GetInstanceSuffix())
			break
		}
		return owner, nil

	default:
		return hostname.Owner{}, fmt.Errorf("unsupported owner kind %d", target.OwnerKind)
	}
}
