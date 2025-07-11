------------------------
--- TAG Permit schema
-------------------------
CREATE TABLE IF NOT EXISTS maestro_tag_permit (
    tag                       TEXT NOT NULL COLLATE "C",
    max_allowed               INT8 CHECK (max_allowed >= 0),
    timeline                  TEXT[],
    PRIMARY KEY (tag)
);
