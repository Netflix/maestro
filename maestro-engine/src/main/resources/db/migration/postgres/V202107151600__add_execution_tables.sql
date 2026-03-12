
-- --------------------------------------------------------------------------------------------------------------
-- SCHEMA FOR WORKFLOW OUTPUT DATA DAO
-- --------------------------------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS output_data (
  external_job_id   TEXT NOT NULL COLLATE "C",
  external_job_type TEXT NOT NULL COLLATE "C",
  workflow_id       TEXT NOT NULL,
  payload           JSON NOT NULL,
  create_ts         TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
  modify_ts         TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (external_job_type,external_job_id)
);

-- --------------------------------------------------------------------------------------------------------------
-- SCHEMA FOR MAESTRO INSTANCE ACTION RELATED DAOs
-- --------------------------------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS maestro_step_instance_action (   -- table to record step instance actions
  workflow_id               TEXT NOT NULL COLLATE "C",
  workflow_instance_id      INT8 NOT NULL CHECK (workflow_instance_id > 0),
  workflow_run_id           INT8 NOT NULL CHECK (workflow_run_id > 0),
  step_id                   TEXT NOT NULL COLLATE "C",
  payload                   JSON NOT NULL,
  create_ts                 TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
  PRIMARY KEY (workflow_id, workflow_instance_id, workflow_run_id, step_id)
);
CREATE TABLE IF NOT EXISTS maestro_step_breakpoint (
     system_generated          BOOL NOT NULL, -- True when inserting paused step attempt due to breakpoint. False to denote explicit user actions.
     workflow_id               TEXT NOT NULL COLLATE "C",
     version                   INT8 NOT NULL,
     instance_id               INT8 NOT NULL,
     run_id                    INT8 NOT NULL,
     step_id                   TEXT NOT NULL COLLATE "C",
     step_attempt_id           INT8 NOT NULL,
     create_ts                 TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
     created_by                JSONB,
     PRIMARY KEY (workflow_id, step_id, system_generated, version, instance_id, run_id, step_attempt_id)
    );
