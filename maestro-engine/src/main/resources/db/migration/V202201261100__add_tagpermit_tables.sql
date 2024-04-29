------------------------
--- TAG Permit schema
-------------------------
CREATE TABLE IF NOT EXISTS maestro_tag_permit (
    tag                       STRING NOT NULL,
    max_allowed               INT8 CHECK (max_allowed >= 0),
    timeline                  STRING[],
    PRIMARY KEY (tag)
);
