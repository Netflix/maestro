
-- --------------------------------------------------------------------------------------------------------------
-- SCHEMA FOR MAESTRO WORKFLOW AND STEP INSTANCE RELATED TABLES
-- --------------------------------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS maestro_workflow_instance (  -- table of workflow runs
  workflow_id       TEXT GENERATED ALWAYS AS (instance->>'workflow_id') STORED NOT NULL COLLATE "C",
  instance_id       INT8 GENERATED ALWAYS AS ((instance->>'workflow_instance_id')::INT8) STORED CHECK (instance_id > 0),
  run_id            INT8 GENERATED ALWAYS AS ((instance->>'workflow_run_id')::INT8) STORED CHECK (run_id > 0),
  uuid              TEXT GENERATED ALWAYS AS (instance->>'workflow_uuid') STORED NOT NULL COLLATE "C",  -- unique id for idempotency
  correlation_id    TEXT GENERATED ALWAYS AS (instance->>'correlation_id') STORED NOT NULL,
  initiator         JSONB GENERATED ALWAYS AS (instance->'initiator') STORED NOT NULL,
  root_depth        INT8 GENERATED ALWAYS AS ((instance->'initiator'->>'depth')::INT8) STORED CHECK (root_depth >= 0),
  initiator_type    TEXT GENERATED ALWAYS AS (instance->'initiator'->>'type') STORED NOT NULL,
  create_ts         TIMESTAMPTZ GENERATED ALWAYS AS (TO_TIMESTAMP(DIV((instance->>'create_time')::INT8,1000))) STORED NOT NULL,
  instance          JSONB NOT NULL, -- it does not contain info in cf2 family
  -- above columns are readonly after creation
  status            TEXT NOT NULL COLLATE "C",
  execution_id      TEXT,         -- null means it hasn't been run by the underlying engine
  start_ts          TIMESTAMPTZ,
  end_ts            TIMESTAMPTZ,
  modify_ts         TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
  runtime_overview  JSONB,          -- embedded step info and statistics
  artifacts         JSONB,
  timeline          TEXT[],
  -- above columns are mutable fields when step running
  PRIMARY KEY (workflow_id, instance_id, run_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS workflow_unique_index ON maestro_workflow_instance (workflow_id, uuid); -- for deduplication
CREATE INDEX IF NOT EXISTS workflow_status_index ON maestro_workflow_instance
    (workflow_id, status, instance_id ASC, run_id ASC) INCLUDE (uuid, execution_id)
    WHERE status IN ('CREATED','IN_PROGRESS','PAUSED','FAILED'); -- for run strategy
CREATE INDEX IF NOT EXISTS foreach_index ON maestro_workflow_instance
    (workflow_id, run_id DESC, instance_id DESC) INCLUDE (status, runtime_overview)
    WHERE initiator_type='FOREACH';

CREATE TABLE IF NOT EXISTS maestro_step_instance (
  workflow_id               TEXT GENERATED ALWAYS AS (instance->>'workflow_id') STORED NOT NULL COLLATE "C",
  workflow_instance_id      INT8 GENERATED ALWAYS AS ((instance->>'workflow_instance_id')::INT8) STORED CHECK (workflow_instance_id > 0),
  workflow_run_id           INT8 GENERATED ALWAYS AS ((instance->>'workflow_run_id')::INT8) STORED CHECK (workflow_run_id > 0),
  step_id                   TEXT GENERATED ALWAYS AS (instance->>'step_id') STORED NOT NULL COLLATE "C",
  step_attempt_id           INT8 GENERATED ALWAYS AS ((instance->>'step_attempt_id')::INT8) STORED CHECK (step_attempt_id > 0),
  workflow_uuid             TEXT GENERATED ALWAYS AS (instance->>'workflow_uuid') STORED NOT NULL,
  step_uuid                 TEXT GENERATED ALWAYS AS (instance->>'step_uuid') STORED NOT NULL,
  correlation_id            TEXT GENERATED ALWAYS AS (instance->>'correlation_id') STORED NOT NULL,
  instance                  JSONB NOT NULL, -- it does not contain info in cf2 family
  -- above columns are readonly after creation
  runtime_state             JSONB NOT NULL,
  dependencies              JSONB,  -- input signal dependencies for a step
  outputs                   JSONB,  -- output signal for a step
  artifacts                 JSONB,
  timeline                  TEXT[],
  -- above columns are mutable fields when step running
  PRIMARY KEY (workflow_id, workflow_instance_id, step_id, workflow_run_id, step_attempt_id)
);
