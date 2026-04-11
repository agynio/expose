CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE exposures (
    id                       UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
    workload_id              UUID        NOT NULL,
    agent_id                 UUID        NOT NULL,
    port                     INTEGER     NOT NULL,
    openziti_service_id      TEXT        NOT NULL DEFAULT '',
    openziti_bind_policy_id  TEXT        NOT NULL DEFAULT '',
    openziti_dial_policy_id  TEXT        NOT NULL DEFAULT '',
    url                      TEXT        NOT NULL DEFAULT '',
    status                   SMALLINT    NOT NULL DEFAULT 1,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX exposures_workload_id_idx ON exposures (workload_id);
CREATE UNIQUE INDEX exposures_workload_port_idx ON exposures (workload_id, port);
CREATE INDEX exposures_status_idx ON exposures (status);
