CREATE TABLE IF NOT EXISTS dim_users (
    user_id TEXT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS dim_actions (
    action_type TEXT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS fact_user_actions (
    user_id TEXT,
    action_type TEXT,
    timestamp TIMESTAMP WITHOUT TIME ZONE,
    device TEXT,
    location TEXT,
    PRIMARY KEY (user_id, timestamp)
);