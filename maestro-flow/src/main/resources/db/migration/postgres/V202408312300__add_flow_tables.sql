-- --------------------------------------------------------------------------------------------------------------
-- SCHEMA FOR MAESTRO FLOW RELATED TABLES
-- --------------------------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS maestro_flow (   -- table saving maestro flow for running maestro workflow instances
  group_id      INT8 NOT NULL,              -- group id
  flow_id       TEXT NOT NULL,              -- uuid, matching maestro workflow instance uuid
  generation    INT8 NOT NULL,              -- detect staleness or ownership change
  start_time    INT8 NOT NULL,              -- flow state to persist
  reference     TEXT NOT NULL,              -- maestro workflow reference info
  PRIMARY KEY (group_id, flow_id)
);

CREATE TABLE IF NOT EXISTS maestro_flow_group ( -- flow group management table to improve the performance
  group_id      INT8 NOT NULL,                  -- group id
  generation    INT8 NOT NULL,                  -- detect staleness or ownership change
  address       TEXT,                           -- unique reachable address to this node
  heartbeat_ts  TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,     -- heartbeat tracking
  PRIMARY KEY (group_id)
);
CREATE INDEX IF NOT EXISTS heartbeat_index ON maestro_flow_group (heartbeat_ts);
