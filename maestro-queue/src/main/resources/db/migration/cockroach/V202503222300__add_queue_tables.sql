-- --------------------------------------------------------------------------------------------------------------
-- SCHEMA FOR MAESTRO QUEUE RELATED TABLES
-- --------------------------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS maestro_queue (  -- table for critical maestro job event queue
  queue_id      INT4 NOT NULL,          -- queue identifier
  owned_until   INT8 NOT NULL,          -- the message in the queue has been owned by a worker until this time
  msg_id        STRING NOT NULL,        -- message identifier
  payload       STRING NOT NULL,        -- maestro job event string
  create_time   INT8 NOT NULL,          -- the time when the message was created
  PRIMARY KEY (queue_id, owned_until ASC, msg_id)
);

CREATE TABLE IF NOT EXISTS maestro_queue_lock (  -- table for locking maestro job event queue
  queue_id      INT4 NOT NULL,                                   -- queue identifier to lock
  create_ts     TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,  -- the time when the lock was created
  PRIMARY KEY (queue_id)
);
