package identitymeta

const (
	IdentityIDMetadataKey   = "x-identity-id"
	IdentityTypeMetadataKey = "x-identity-type"
	WorkloadIDMetadataKey   = "x-workload-id"

	IdentityTypeUser = "user"
	// An agent workload authenticates as the instance it runs, not as the class.
	// IdentityTypeAgent remains for identities minted before the migration.
	IdentityTypeAgent         = "agent"
	IdentityTypeAgentInstance = "agent_instance"
	IdentityTypeApp           = "app"
	IdentityTypeRunner        = "runner"
)
