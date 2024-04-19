-- --------------------------------------------------------------------------------------------------------------
-- SCHEMA FOR EXECUTION RELATED DAOs
-- --------------------------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS workflow_instance (
  workflow_instance_id STRING NOT NULL,
  payload JSONB NOT NULL,
  PRIMARY KEY (workflow_instance_id),
  workflow_name STRING AS (payload->>'workflowName') STORED,
  status STRING AS (payload->>'status') STORED,
  start_time INT8 AS ((payload->>'startTime')::INT8) STORED,
  workflow_version INT4 AS ((payload->>'workflowVersion')::INT4) STORED,
  INDEX name_status_index (workflow_name,status,start_time)
);

CREATE TABLE IF NOT EXISTS task (
  workflow_instance_id STRING NOT NULL,
  task_id STRING NOT NULL,
  payload JSONB NOT NULL,
  PRIMARY KEY (workflow_instance_id, task_id),
  INDEX id_index (task_id),
  task_name STRING AS (payload->>'taskDefName') STORED,
  status STRING AS (payload->>'status') STORED,
  start_time INT8 AS ((payload->>'startTime')::INT8) STORED,
  INDEX name_status_index (task_name,status,start_time)
);

CREATE TABLE IF NOT EXISTS event_execution (
  event STRING NOT NULL,
  handler_name STRING NOT NULL,
  message_id STRING NOT NULL,
  execution_id STRING NOT NULL,
  payload JSONB NOT NULL,
  PRIMARY KEY (event,handler_name,message_id,execution_id)
);

CREATE TABLE IF NOT EXISTS task_execution_log (
  task_id STRING NOT NULL,
  created_time INT8,
  hash_code INT8 AS (fnv32a(log)) STORED,
  log STRING,
  PRIMARY KEY (task_id,created_time,hash_code)
);

CREATE TABLE IF NOT EXISTS poll_data (
  queue_name STRING NOT NULL,
  domain STRING NOT NULL,
  payload JSONB NOT NULL,
  PRIMARY KEY (queue_name,domain)
);


-- --------------------------------------------------------------------------------------------------------------
-- SCHEMA FOR METADATA RELATED DAOs
-- --------------------------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS event_handler (
  handler_name STRING,
  payload JSONB NOT NULL,
  PRIMARY KEY (handler_name),
  event STRING AS (payload->>'event') STORED,
  active BOOL AS ((payload->>'active')::BOOL) STORED,
  INDEX event_index (event,active)
);

CREATE TABLE IF NOT EXISTS workflow_definition (
  workflow_name STRING NOT NULL,
  version INT4 NOT NULL,
  payload STRING NOT NULL,
  PRIMARY KEY (workflow_name,version DESC)
);

CREATE TABLE IF NOT EXISTS task_definition (
  task_name STRING NOT NULL,
  payload STRING NOT NULL,
  PRIMARY KEY (task_name)
);
