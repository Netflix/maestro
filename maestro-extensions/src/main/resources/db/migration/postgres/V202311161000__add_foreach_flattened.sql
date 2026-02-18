CREATE TABLE IF NOT EXISTS maestro_step_foreach_flattened
(
    workflow_id                  TEXT NOT NULL,
    workflow_instance_id         INT8 NOT NULL CHECK (workflow_instance_id > 0),
    workflow_run_id              INT8 NOT NULL CHECK (workflow_run_id > 0),
    step_id                      TEXT NOT NULL,
    iteration_rank               TEXT NOT NULL,
    initial_step_created_ms      INT8 NOT NULL CHECK (initial_step_created_ms > 0),
    instance                     JSONB NOT NULL,
    run_id_validity_end          INT8 NOT NULL CHECK (run_id_validity_end > 0),
    step_attempt_seq             TEXT NOT NULL,
    loop_parameters              JSONB NOT NULL,
    step_status                  TEXT NOT NULL,
    step_status_encoded          INT8 NOT NULL CHECK (step_status_encoded >= 0),
    step_status_priority         INT8 NOT NULL CHECK (step_status_priority >= 0) DEFAULT 0,
    step_runtime_state           JSONB NOT NULL,
    is_inserted_in_new_run       BOOLEAN NOT NULL,
    PRIMARY KEY (workflow_id, workflow_instance_id, step_id, workflow_run_id, run_id_validity_end, iteration_rank)
);

-- Create indexes that were removed from inline table definitions
CREATE INDEX IF NOT EXISTS iterationIdx ON maestro_step_foreach_flattened (workflow_id, workflow_instance_id DESC, step_id, iteration_rank COLLATE "C" ASC, workflow_run_id ASC, run_id_validity_end DESC);
CREATE INDEX IF NOT EXISTS representativeIterationIdx ON maestro_step_foreach_flattened (workflow_id, workflow_instance_id DESC, step_id, workflow_run_id ASC, run_id_validity_end DESC, step_status_priority DESC, iteration_rank COLLATE "C" ASC);
ALTER TABLE maestro_step_foreach_flattened ADD CONSTRAINT uniqueConstraint UNIQUE (workflow_id, workflow_instance_id, step_id, workflow_run_id, iteration_rank);
