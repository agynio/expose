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
	// A sandbox workload authenticates as its sandbox. It exposes ports the same
	// way an agent does -- someone at the shell runs the same command.
	IdentityTypeSandbox = "sandbox"
	IdentityTypeApp     = "app"
	IdentityTypeRunner  = "runner"
)
