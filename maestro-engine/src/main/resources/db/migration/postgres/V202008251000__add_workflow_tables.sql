
-- --------------------------------------------------------------------------------------------------------------
-- SCHEMA FOR MAESTRO WORKFLOW RELATED TABLES
-- --------------------------------------------------------------------------------------------------------------
CREATE SEQUENCE IF NOT EXISTS workflow_id_seq START 10000 INCREMENT 1;          -- starting from 10K

CREATE TABLE IF NOT EXISTS maestro_workflow (   -- table to lock and store mutable workflow definition and instance info
  workflow_id           TEXT NOT NULL,
  internal_id           INT8 DEFAULT nextval('workflow_id_seq') NOT NULL,       -- internal unique sequence id
  active_version_id     INT8 DEFAULT 0 CHECK (active_version_id >= 0),          -- 0 means inactive
  activate_ts           TIMESTAMPTZ,
  activated_by          JSONB,
  properties_snapshot   JSONB CHECK (properties_snapshot IS NOT NULL),          -- current properties snapshot
  latest_version_id     INT8 DEFAULT 1 CHECK (latest_version_id > 0),           -- latest workflow version id
  latest_instance_id    INT8 DEFAULT 0 NOT NULL CHECK (latest_instance_id >= 0),-- latest workflow instance id
  modify_ts             TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,         -- last modified timestamp
  PRIMARY KEY (workflow_id)
);

CREATE TABLE IF NOT EXISTS maestro_workflow_timeline (  -- table to store the workflow changes
  workflow_id   TEXT NOT NULL,
  change_event  JSONB NOT NULL,
  hash_id       INT8 NOT NULL,                          -- hash of the change_event, used for deduplication
  create_ts     TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
  PRIMARY KEY (workflow_id, create_ts, hash_id)
);

CREATE TABLE IF NOT EXISTS maestro_workflow_version (   -- table of workflow version history, immutable
  workflow_id   TEXT GENERATED ALWAYS AS (definition->>'id') STORED NOT NULL,
  version_id    INT8 GENERATED ALWAYS AS ((metadata->>'workflow_version_id')::INT8) STORED NOT NULL CHECK (version_id > 0),
  metadata      JSONB NOT NULL,
  definition    JSONB NOT NULL,
  trigger_uuids JSONB,
  create_ts     TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL, -- processing delay is the time diff between it and metadata create_time
  PRIMARY KEY (workflow_id, version_id)
);

CREATE TABLE IF NOT EXISTS maestro_workflow_properties (    -- table of properties changes and previous snapshot, immutable
  workflow_id           TEXT NOT NULL,
  create_time           INT8 NOT NULL,      -- is treated as version (not strictly increasing), add version if needed
  author                JSONB NOT NULL,    -- author for the properties_changes
  properties_changes    JSONB NOT NULL,     -- properties changes (delta) for the workflow level settings
  previous_snapshot     JSONB,              -- properties snapshot just before the changes, used for reverting
  PRIMARY KEY (workflow_id, create_time)
);

CREATE TABLE IF NOT EXISTS maestro_workflow_deleted (   -- table to store the deleted workflow basic info for auditing
  workflow_id   TEXT GENERATED ALWAYS AS (workflow->>'workflow_id') STORED NOT NULL,             -- workflow id can be re-used
  internal_id   INT8 GENERATED ALWAYS AS ((workflow->>'internal_id')::INT8) STORED NOT NULL,     -- make the record unique
  workflow      JSONB NOT NULL,                                         -- a copy of data deleted from maestro_workflow
  create_ts     TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,         -- the moment the row is inserted
  stage         TEXT DEFAULT 'DELETING_VERSIONS' NOT NULL,              -- stage enum name
  timeline      TEXT[] NOT NULL,                                        -- delete timeline info, e.g. who deletes it, etc.
  modify_ts     TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,         -- last modified timestamp
  PRIMARY KEY (workflow_id, internal_id)
);
