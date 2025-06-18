
-- --------------------------------------------------------------------------------------------------------------
-- SCHEMA FOR WORKFLOW EXECUTION RELATED DATA TABLES
-- --------------------------------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS maestro_step_instance_action (   -- table to record step instance actions
  workflow_id               TEXT GENERATED ALWAYS AS (payload->>'workflow_id') STORED NOT NULL,
  workflow_instance_id      INT8 GENERATED ALWAYS AS ((payload->>'workflow_instance_id')::INT8) STORED CHECK (workflow_instance_id > 0),
  workflow_run_id           INT8 GENERATED ALWAYS AS ((payload->>'workflow_run_id')::INT8) STORED CHECK (workflow_run_id > 0),
  step_id                   TEXT GENERATED ALWAYS AS (payload->>'step_id') STORED NOT NULL,
  payload                   JSONB NOT NULL,
  create_ts                 TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
  PRIMARY KEY (workflow_id, workflow_instance_id, workflow_run_id, step_id)
);

CREATE TABLE IF NOT EXISTS maestro_step_breakpoint (
  system_generated          BOOL NOT NULL, -- True when inserting paused step attempt due to breakpoint. False to denote explicit user actions.
  workflow_id               TEXT NOT NULL,
  version                   INT8 NOT NULL,
  instance_id               INT8 NOT NULL,
  run_id                    INT8 NOT NULL,
  step_id                   TEXT NOT NULL,
  step_attempt_id           INT8 NOT NULL,
  create_ts                 TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
  created_by                JSONB,
  PRIMARY KEY (workflow_id, step_id, system_generated, version, instance_id, run_id, step_attempt_id)
);

CREATE TABLE IF NOT EXISTS output_data (
  external_job_id   TEXT GENERATED ALWAYS AS (payload->>'external_job_id') STORED NOT NULL,
  external_job_type TEXT GENERATED ALWAYS AS (payload->>'external_job_type') STORED NOT NULL,
  workflow_id       TEXT GENERATED ALWAYS AS (payload->>'workflow_id') STORED NOT NULL,
  payload           JSONB NOT NULL,
  create_ts         TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
  modify_ts         TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (external_job_type,external_job_id)
);
