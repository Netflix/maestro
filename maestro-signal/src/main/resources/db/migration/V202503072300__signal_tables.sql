-- --------------------------------------------------------------------------------------------------------------
-- SCHEMA FOR MAESTRO SIGNAL RELATED TABLES
-- --------------------------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS maestro_signal_instance (    -- table to store received signal instances
  name              STRING      NOT NULL,               -- signal name is to group signal instances
  seq_id            INT8        NOT NULL,               -- sequence number to keep the order of signal instances per signal name
  instance_id       STRING      NOT NULL,               -- unique id, used for deduplication
  instance          STRING      NOT NULL,               -- signal instance data, can be compressed if needed
  create_ts         TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
  PRIMARY KEY (name, seq_id DESC)
);
CREATE UNIQUE INDEX IF NOT EXISTS signal_index ON maestro_signal_instance (name, instance_id DESC); -- for deduplication

CREATE TABLE IF NOT EXISTS maestro_signal_param (       -- table to store the indexed long & string type params from signal instances
  signal_name       STRING      NOT NULL,
  param_name        STRING      NOT NULL,
  encoded_val       STRING      NOT NULL,               -- encoded val saves long or string type value with the original order
  signal_seq_id     INT8        NOT NULL,
  PRIMARY KEY (signal_name, param_name, encoded_val, signal_seq_id ASC)
);

CREATE TABLE IF NOT EXISTS maestro_signal_trigger (     -- table to store the signal trigger definitions for workflows
  workflow_id       TEXT        NOT NULL,
  trigger_uuid      TEXT        NOT NULL,               -- workflow's signal trigger uuid
  definition        TEXT        NOT NULL,               -- signal trigger definition string
  signals           TEXT[]      NOT NULL,               -- signal name in the trigger
  checkpoints       INT8[]      NOT NULL,               -- inclusive signal seq ids to track consumed signal instances
  create_ts         TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
  PRIMARY KEY (workflow_id, trigger_uuid)
);
-- Get (workflow_id, trigger_uuid) by a signal name. won't cause locking by select for update
CREATE INDEX IF NOT EXISTS signal_trigger_index ON maestro_signal_trigger USING GIN (signals);
