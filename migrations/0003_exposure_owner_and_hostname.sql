-- An exposure is addressed by the entity whose workload serves it, so the
-- record has to name that entity rather than assume an agent behind it.

ALTER TABLE exposures
    ADD COLUMN owner_kind      SMALLINT,
    ADD COLUMN owner_id        UUID,
    ADD COLUMN organization_id UUID,
    ADD COLUMN hostname        TEXT NOT NULL DEFAULT '';

-- Existing rows predate sandbox-owned exposures: every one is agent-owned, and
-- agent_id already holds the instance the workload runs for. Their address is
-- the opaque form derived from the exposure id, which is exactly what url
-- already carries -- so hostname is known here and needs no lookup.
--
-- organization_id is deliberately left NULL: it is not derivable from this
-- table, and reconciliation refreshes owner and organization from the workload
-- record on its next pass.
UPDATE exposures
SET owner_kind = 1,
    owner_id   = agent_id,
    hostname   = 'exposed-' || id::text || '.agyn'
WHERE owner_id IS NULL;

ALTER TABLE exposures
    ALTER COLUMN owner_kind SET NOT NULL,
    ALTER COLUMN owner_id   SET NOT NULL;

-- A sandbox-owned exposure has no agent behind it.
ALTER TABLE exposures ALTER COLUMN agent_id DROP NOT NULL;

CREATE INDEX exposures_owner_idx ON exposures (owner_kind, owner_id);

-- An active exposure must carry the address its intercept.v1 config was written
-- from, alongside the resources that config lives on.
ALTER TABLE exposures DROP CONSTRAINT exposures_active_resources_check;

ALTER TABLE exposures
ADD CONSTRAINT exposures_active_resources_check
CHECK (
    status <> 2
    OR (
        openziti_service_id <> ''
        AND openziti_bind_policy_id <> ''
        AND openziti_dial_policy_id <> ''
        AND url <> ''
        AND hostname <> ''
    )
);
