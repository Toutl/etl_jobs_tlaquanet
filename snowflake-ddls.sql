-- Snowflake DDL Setup for TlaquaNet Analytics
CREATE DATABASE IF NOT EXISTS tlaquanet_analytics;
USE DATABASE tlaquanet_analytics;

CREATE SCHEMA IF NOT EXISTS public;
USE SCHEMA public;

-- Fact Tables
CREATE TABLE IF NOT EXISTS posts (
    id INTEGER,
    user_id INTEGER,
    content STRING,
    created_at TIMESTAMP_TZ
);

CREATE TABLE IF NOT EXISTS comments (
    id INTEGER,
    post_id INTEGER,
    user_id INTEGER,
    content STRING,
    created_at TIMESTAMP_TZ
);

CREATE TABLE IF NOT EXISTS likes (
    id INTEGER,
    post_id INTEGER,
    user_id INTEGER,
    created_at TIMESTAMP_TZ

);

CREATE TABLE IF NOT EXISTS events (
    id INTEGER,
    user_id INTEGER,
    event_type STRING,
    created_at TIMESTAMP_TZ,
    metadata VARIANT
);

-- Staging Table for Users
CREATE TABLE IF NOT EXISTS stg_users (
    user_id INTEGER,
    username STRING,
    display_name STRING,
    created_at TIMESTAMP_TZ
);

-- Dimension Table for users (SCD Type 2)
CREATE TABLE IF NOT EXISTS dim_users (
    id NUMBER AUTOINCREMENT,
    user_id INTEGER,
    username STRING,
    display_name STRING,
    created_at TIMESTAMP_TZ,
    from_date TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
    to_date TIMESTAMP_TZ,
    is_current BOOLEAN DEFAULT TRUE
);
