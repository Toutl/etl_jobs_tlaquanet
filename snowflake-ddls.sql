CREATE DATABASE IF NOT EXISTS tlaquanet_analytics;
USE DATABASE tlaquanet_analytics;

CREATE SCHEMA IF NOT EXISTS public;
USE SCHEMA public;

CREATE OR REPLACE TABLE users (
    id INTEGER,
    username STRING,
    display_name STRING,
    created_at TIMESTAMP_TZ
);
