-- =====================================================
-- DATABASE & SCHEMA
-- =====================================================

CREATE DATABASE IF NOT EXISTS tlaquanet_analytics;
USE DATABASE tlaquanet_analytics;

CREATE SCHEMA IF NOT EXISTS public;
USE SCHEMA public;


-- =====================================================
-- RAW FACT TABLES
-- =====================================================

-- POSTS  (Postgres order: id, content, author_id, created_at)
CREATE OR REPLACE TABLE posts (
    id INTEGER,
    content STRING,
    author_id INTEGER,
    created_at TIMESTAMP_TZ
);


-- COMMENTS (Postgres order: id, content, author_id, post_id, created_at)
CREATE OR REPLACE TABLE comments (
    id INTEGER,
    content STRING,
    author_id INTEGER,
    post_id INTEGER,
    created_at TIMESTAMP_TZ
);


-- LIKES (Postgres order: id, user_id, post_id, created_at)
CREATE OR REPLACE TABLE likes (
    id INTEGER,
    user_id INTEGER,
    post_id INTEGER,
    created_at TIMESTAMP_TZ
);


-- EVENTS (Postgres order: id, event_type, user_id, target_id, metadata, created_at)
CREATE OR REPLACE TABLE events (
    id INTEGER,
    event_type STRING,
    user_id INTEGER,
    target_id INTEGER,
    metadata STRING,
    created_at TIMESTAMP_TZ
);


-- =====================================================
-- RAW USERS TABLE
-- =====================================================

-- Postgres order: id, username, display_name, created_at
CREATE OR REPLACE TABLE users_raw (
    id INTEGER,
    username STRING,
    display_name STRING,
    created_at TIMESTAMP_TZ
);


-- =====================================================
-- STAGING TABLE FOR SCD PROCESSING
-- =====================================================

CREATE OR REPLACE TABLE stg_users (
    user_id INTEGER,
    username STRING,
    display_name STRING,
    created_at TIMESTAMP_TZ
);


-- =====================================================
-- DIMENSION TABLE (SCD TYPE 2)
-- =====================================================

CREATE OR REPLACE TABLE dim_users (
    id NUMBER AUTOINCREMENT,
    user_id INTEGER,
    username STRING,
    display_name STRING,
    created_at TIMESTAMP_TZ,
    from_date TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
    to_date TIMESTAMP_TZ,
    is_current BOOLEAN DEFAULT TRUE
);
