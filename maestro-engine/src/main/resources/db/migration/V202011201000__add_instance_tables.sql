
-- --------------------------------------------------------------------------------------------------------------
-- SCHEMA FOR MAESTRO WORKFLOW AND STEP INSTANCE RELATED TABLES
-- --------------------------------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS maestro_workflow_instance (  -- table of workflow runs
  workflow_id       STRING AS (instance->>'workflow_id') STORED NOT NULL,
  instance_id       INT8 AS ((instance->>'workflow_instance_id')::INT8) STORED CHECK (instance_id > 0),
  run_id            INT8 AS ((instance->>'workflow_run_id')::INT8) STORED CHECK (run_id > 0),
  uuid              STRING AS (instance->>'workflow_uuid') STORED NOT NULL,       -- unique id for idempotency
  correlation_id    STRING AS (instance->>'correlation_id') STORED NOT NULL,
  initiator         JSONB  AS (instance->'initiator') STORED NOT NULL,
  root_depth        INT8   AS ((instance->'initiator'->>'depth')::INT8) STORED CHECK (root_depth >= 0),
  initiator_type    STRING AS (instance->'initiator'->>'type') STORED NOT NULL,
  create_ts         TIMESTAMPTZ AS (DIV((instance->>'create_time')::INT8,1000)::TIMESTAMPTZ) STORED NOT NULL,
  instance          JSONB NOT NULL, -- it does not contain info in cf2 family
  FAMILY cf1 (workflow_id, instance_id, run_id, uuid, correlation_id, initiator, root_depth, initiator_type, create_ts, instance),   -- readonly after creation
  status            STRING NOT NULL,
  execution_id      STRING,         -- null means it hasn't been run by the underlying engine
  start_ts          TIMESTAMPTZ,
  end_ts            TIMESTAMPTZ,
  modify_ts         TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
  runtime_overview  JSONB,          -- embedded step info and statistics
  artifacts         JSONB,
  timeline          STRING[],
  FAMILY cf2 (status, execution_id, start_ts, end_ts, modify_ts, runtime_overview, artifacts, timeline), -- mutable fields when step running
  PRIMARY KEY (workflow_id, instance_id DESC, run_id DESC),
  UNIQUE INDEX workflow_unique_index (workflow_id, uuid),
  INDEX workflow_status_index (workflow_id, status, instance_id ASC, run_id ASC) STORING (uuid, execution_id)
    WHERE status IN ('CREATED','IN_PROGRESS','PAUSED','FAILED'), -- for run strategy
  INDEX foreach_index (workflow_id, run_id DESC, instance_id DESC) STORING (status, runtime_overview)
    WHERE initiator_type='FOREACH',
  INDEX correlation_index (correlation_id, root_depth) STORING (initiator, status)
);

CREATE TABLE IF NOT EXISTS maestro_step_instance (
  workflow_id               STRING AS (instance->>'workflow_id') STORED NOT NULL,
  workflow_instance_id      INT8 AS ((instance->>'workflow_instance_id')::INT8) STORED CHECK (workflow_instance_id > 0),
  workflow_run_id           INT8 AS ((instance->>'workflow_run_id')::INT8) STORED CHECK (workflow_run_id > 0),
  step_id                   STRING AS (instance->>'step_id') STORED NOT NULL,
  step_attempt_id           INT8 AS ((instance->>'step_attempt_id')::INT8) STORED CHECK (step_attempt_id > 0),
  workflow_uuid             STRING AS (instance->>'workflow_uuid') STORED NOT NULL,
  step_uuid                 STRING AS (instance->>'step_uuid') STORED NOT NULL,
  correlation_id            STRING AS (instance->>'correlation_id') STORED NOT NULL,
  instance                  JSONB NOT NULL, -- it does not contain info in cf2 family
  FAMILY cf1 (workflow_id, workflow_instance_id, workflow_run_id, step_id, step_attempt_id, workflow_uuid, step_uuid, correlation_id, instance),   -- readonly after creation
  runtime_state             JSONB NOT NULL,
  dependencies              JSONB,  -- input signal dependencies for a step
  outputs                   JSONB,  -- output signal for a step
  artifacts                 JSONB,
  timeline                  STRING[],
  FAMILY cf2 (runtime_state, dependencies, outputs, artifacts, timeline), -- mutable fields when step running
  PRIMARY KEY (workflow_id, workflow_instance_id DESC, step_id, workflow_run_id DESC, step_attempt_id DESC)
);
