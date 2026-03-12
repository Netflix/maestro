
-- --------------------------------------------------------------------------------------------------------------
-- SCHEMA FOR MAESTRO WORKFLOW AND STEP INSTANCE RELATED DAOs
-- --------------------------------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS maestro_workflow_instance (  -- table of workflow runs
  workflow_id       TEXT NOT NULL COLLATE "C",
  instance_id       INT8 NOT NULL CHECK (instance_id > 0),
  run_id            INT8 NOT NULL CHECK (run_id > 0),
  uuid              TEXT NOT NULL COLLATE "C",       -- unique id for idempotency
  correlation_id    TEXT NOT NULL,
  initiator         JSONB  NOT NULL,
  root_depth        INT8   NOT NULL CHECK (root_depth >= 0),
  initiator_type    TEXT NOT NULL,
  create_ts         TIMESTAMPTZ NOT NULL,
  instance          JSON NOT NULL,
  status            TEXT NOT NULL COLLATE "C",
  execution_id      TEXT,         -- null means it hasn't been run by the underlying engine
  start_ts          TIMESTAMPTZ,
  end_ts            TIMESTAMPTZ,
  modify_ts         TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
  runtime_overview  JSONB,          -- embedded step info and statistics
  artifacts         JSON,
  timeline          TEXT[],
  PRIMARY KEY (workflow_id, instance_id, run_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS workflow_unique_index ON maestro_workflow_instance (workflow_id, uuid);
CREATE INDEX workflow_status_index ON maestro_workflow_instance (workflow_id, status, instance_id ASC, run_id ASC) INCLUDE (uuid, execution_id) WHERE status IN ('CREATED','IN_PROGRESS','PAUSED','FAILED'); -- for run strategy
CREATE INDEX foreach_index ON maestro_workflow_instance (workflow_id, run_id DESC, instance_id DESC) INCLUDE (status, runtime_overview) WHERE initiator_type='FOREACH'; -- todo remove it after moving foreach to its own table

CREATE TABLE IF NOT EXISTS maestro_step_instance (
  workflow_id               TEXT NOT NULL COLLATE "C",
  workflow_instance_id      INT8 NOT NULL CHECK (workflow_instance_id > 0),
  workflow_run_id           INT8 NOT NULL CHECK (workflow_run_id > 0),
  step_id                   TEXT NOT NULL COLLATE "C",
  step_attempt_id           INT8 NOT NULL CHECK (step_attempt_id > 0),
  workflow_uuid             TEXT NOT NULL,
  step_uuid                 TEXT NOT NULL,
  correlation_id            TEXT NOT NULL,
  instance                  JSON NOT NULL,
  runtime_state             JSONB NOT NULL,
  dependencies              JSONB,
  outputs                   JSONB,
  artifacts                 JSONB,
  timeline                  TEXT[],
  PRIMARY KEY (workflow_id, workflow_instance_id, workflow_run_id, step_id, step_attempt_id)
);
