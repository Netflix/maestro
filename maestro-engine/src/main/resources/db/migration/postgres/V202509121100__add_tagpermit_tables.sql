-- --------------------------------------------------------------------------------------------------------------
-- SCHEMA FOR MAESTRO TAG PERMIT RELATED TABLES
-- --------------------------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS maestro_tag_permit (
  tag                       TEXT NOT NULL COLLATE "C",
  max_allowed               INT4 CHECK (max_allowed >= 0),
  timeline                  TEXT[] NOT NULL,
  status                    int2 DEFAULT 0,     -- 0 means updated, 1 means deleted, 6 means synced
  PRIMARY KEY (tag)
);
CREATE INDEX IF NOT EXISTS changed_tag ON maestro_tag_permit (status) WHERE status<6;

CREATE TABLE IF NOT EXISTS maestro_step_tag_permit (    -- step and tag info
  uuid                      UUID NOT NULL,
  seq_num                   INT8 DEFAULT 0 NOT NULL,    -- used to order the queue, 0 means unknown
  status                    INT2 DEFAULT 0 NOT NULL,    -- 0 means unknown, 1 means deleted, 6 means QUEUED, 7 means ACQUIRED
  tags                      TEXT[] NOT NULL,            -- tag name is stored
  limits                    INT4[] NOT NULL,            -- NULL means no limit
  timeline                  TEXT[] NOT NULL,            -- additional info, e.g. which tags it waits for
  create_ts                 TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL, -- hint for the seq_num assignment
  acquire_ts                TIMESTAMPTZ,
  PRIMARY KEY (uuid)
);
CREATE INDEX IF NOT EXISTS changed_step ON maestro_step_tag_permit (status,create_ts) WHERE status<6;

INSERT INTO maestro_flow_group (group_id,generation,heartbeat_ts) VALUES (0,1, NOW() - INTERVAL '1 day') ON CONFLICT DO NOTHING;
INSERT INTO maestro_flow (group_id,flow_id,generation,start_time,reference) VALUES (0,'internal_flow_maestro',1,EXTRACT(EPOCH FROM NOW())*1000,'internal_flow_maestro') ON CONFLICT DO NOTHING;
