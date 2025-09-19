-- --------------------------------------------------------------------------------------------------------------
-- SCHEMA FOR MAESTRO TEMPLATE RELATED TABLES
-- --------------------------------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS maestro_job_template (   -- a job template maps to a step's subtype implementation
  job_type                  TEXT NOT NULL,          -- job type is always same as the sub_type value
  tag                       TEXT NOT NULL,          -- tag is used to versioning job templates for a given job type
  metadata                  JSON NOT NULL,          -- additional metadata about this job template
  definition                JSON NOT NULL,          -- keep original job template schema here
  PRIMARY KEY (job_type, tag)
);
