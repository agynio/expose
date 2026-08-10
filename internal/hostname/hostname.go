// Package hostname derives the intercept address an exposure is reachable at.
//
// An address names the entity whose workload serves it — a sandbox at
// <sandbox>.<org>.agyn, an agent instance at <suffix>.<nickname>.<org>.agyn.
// When no readable form can be built the exposure falls back to an opaque
// address derived from its own id, which always works.
package hostname

import (
	"strings"

	"github.com/google/uuid"
)

// Zone is the overlay suffix every exposure address ends in. It is resolved by
// the dialing tunneler from the intercept.v1 config, not by public DNS.
const Zone = "agyn"

// maxLabelLen is the DNS limit on a single label.
const maxLabelLen = 63

// OwnerKind is the kind of entity an exposing workload runs for.
type OwnerKind int

const (
	OwnerKindUnspecified OwnerKind = iota
	OwnerKindAgentInstance
	OwnerKindSandbox
)

// Owner carries the name components read from the services that own them:
// the sandbox name from Agents, or the handle parts from Identity.
type Owner struct {
	Kind OwnerKind

	// SandboxName is set when Kind is OwnerKindSandbox.
	SandboxName string

	// Nickname and InstanceSuffix are set when Kind is OwnerKindAgentInstance.
	// Nickname is the class handle stem; InstanceSuffix is the instance's label
	// or the stem the platform generated for it.
	Nickname       string
	InstanceSuffix string
}

// Fallback is the opaque address for an exposure with no readable form. It is
// derived from the exposure id alone, so it needs nothing from any other
// service and can never collide.
func Fallback(exposureID uuid.UUID) string {
	return "exposed-" + exposureID.String() + "." + Zone
}

// Derive returns the address for an exposure, or Fallback when any component is
// missing or is not a valid DNS label.
//
// Falling back rather than failing is deliberate: a missing nickname is a
// cosmetic gap, and refusing to expose a port over one would turn it into an
// outage.
func Derive(exposureID uuid.UUID, orgSlug string, owner Owner) string {
	labels, ok := readableLabels(orgSlug, owner)
	if !ok {
		return Fallback(exposureID)
	}
	return strings.Join(append(labels, Zone), ".")
}

// IsReadable reports whether Derive would produce a readable address rather
// than the fallback.
func IsReadable(orgSlug string, owner Owner) bool {
	_, ok := readableLabels(orgSlug, owner)
	return ok
}

func readableLabels(orgSlug string, owner Owner) ([]string, bool) {
	org := strings.TrimSpace(orgSlug)
	if !IsDNSLabel(org) {
		return nil, false
	}
	switch owner.Kind {
	case OwnerKindSandbox:
		name := strings.TrimSpace(owner.SandboxName)
		if !IsDNSLabel(name) {
			return nil, false
		}
		return []string{name, org}, true
	case OwnerKindAgentInstance:
		// The handle read back to front: @bob#research serves at research.bob.
		nickname := strings.TrimSpace(owner.Nickname)
		suffix := strings.TrimSpace(owner.InstanceSuffix)
		if !IsDNSLabel(nickname) || !IsDNSLabel(suffix) {
			return nil, false
		}
		return []string{suffix, nickname, org}, true
	default:
		return nil, false
	}
}

// IsDNSLabel reports whether s is usable as one label of a hostname:
// ^[a-z0-9]([a-z0-9-]*[a-z0-9])?$, at most 63 characters.
//
// Organization slugs and sandbox names are constrained to this where they are
// defined. Nicknames and instance suffixes are not — they permit '_', which is
// a legal @mention character and not a legal hostname one — so this is what
// sends those exposures to the fallback.
func IsDNSLabel(s string) bool {
	if s == "" || len(s) > maxLabelLen {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9':
		case c == '-':
			if i == 0 || i == len(s)-1 {
				return false
			}
		default:
			return false
		}
	}
	return true
}
