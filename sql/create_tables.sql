DROP TABLE IF EXISTS events;
DROP TABLE IF EXISTS sessions;
DROP TABLE IF EXISTS users;
DROP TABLE IF EXISTS lessons;

CREATE TABLE users (
    user_id BIGINT PRIMARY KEY,
    signup_date DATE,
    signup_time TIMESTAMP,
    signup_channel TEXT,
    campaign_id TEXT,
    country TEXT,
    language_target TEXT,
    device_os TEXT,
    app_version TEXT,
    timezone TEXT,
    signup_platform TEXT,
    is_premium_at_signup BOOLEAN,
    variant TEXT
);

CREATE TABLE sessions (
    session_id TEXT PRIMARY KEY,
    user_id BIGINT,
    session_start TIMESTAMP,
    session_end TIMESTAMP,
    session_duration_sec INTEGER,
    device_os TEXT,
    app_version TEXT
);

CREATE TABLE lessons (
    lesson_id TEXT PRIMARY KEY,
    unit_id TEXT,
    skill TEXT,
    difficulty INTEGER,
    topic TEXT,
    expected_duration_sec INTEGER,
    expected_xp INTEGER
);

CREATE TABLE events (
    event_id TEXT PRIMARY KEY,
    user_id BIGINT,
    session_id TEXT,
    event_time TIMESTAMP,
    event_date DATE,
    event_name TEXT,
    screen TEXT,
    language_target TEXT,
    lesson_id TEXT,
    xp_delta INTEGER,
    hearts_delta INTEGER,
    streak_length INTEGER,
    is_premium BOOLEAN,
    experiment_id TEXT,
    variant TEXT
);