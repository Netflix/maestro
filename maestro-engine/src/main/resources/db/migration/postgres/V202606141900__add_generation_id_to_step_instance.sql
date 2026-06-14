-- Add generation_id to maestro_step_instance to prevent reverting step data by delayed updates
ALTER TABLE maestro_step_instance ADD COLUMN IF NOT EXISTS generation_id INT8 NOT NULL DEFAULT 0;
