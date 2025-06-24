-- --------------------------------------------------------------------------------------------------------------
-- SCHEMA FOR MAESTRO QUEUE RELATED TABLES
-- --------------------------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS maestro_queue (  -- table for critical maestro job event queue
  queue_id      INT4 NOT NULL,              -- queue identifier
  owned_until   INT8 NOT NULL,              -- the message in the queue has been owned by a worker until this time
  msg_id        TEXT NOT NULL COLLATE "C",  -- message identifier
  payload       TEXT NOT NULL,              -- maestro job event string
  create_time   INT8 NOT NULL,              -- the time when the message was created
  PRIMARY KEY (queue_id, owned_until, msg_id)
);
