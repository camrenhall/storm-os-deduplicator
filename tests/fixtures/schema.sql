-- Test schema for Deduplicator unit tests
-- This mirrors the production schema from Generator's schema.sql

CREATE EXTENSION IF NOT EXISTS postgis;

-- Clean up existing tables
DROP TABLE IF EXISTS flood_pixels_marketable CASCADE;
DROP TABLE IF EXISTS flood_pixels_unique CASCADE;
DROP TABLE IF EXISTS flood_pixels_raw CASCADE;

-- Append-only raw detections
CREATE TABLE flood_pixels_raw (
    id bigserial PRIMARY KEY,
    segment_id bigint NOT NULL,
    score smallint NOT NULL,
    homes integer NOT NULL,
    qpe_1h numeric NOT NULL,
    ffw boolean NOT NULL,
    geom geometry(Point, 4326) NOT NULL,
    first_seen timestamptz NOT NULL DEFAULT now()
);

-- Index for efficient pruning by time
CREATE INDEX idx_flood_pixels_raw_first_seen ON flood_pixels_raw(first_seen);

-- Deduplicated best-score per segment
CREATE TABLE flood_pixels_unique (
    segment_id bigint PRIMARY KEY,
    score smallint NOT NULL,
    homes integer NOT NULL,
    qpe_1h numeric NOT NULL,
    ffw boolean NOT NULL,
    geom geometry(Point, 4326) NOT NULL,
    first_seen timestamptz NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- Index for efficient marketable view filtering
CREATE INDEX idx_flood_pixels_unique_score_homes ON flood_pixels_unique(score DESC, homes) WHERE score >= 40 AND homes >= 200;

-- Materialized view for marketable leads (initially empty)
CREATE TABLE flood_pixels_marketable (
    segment_id bigint PRIMARY KEY,
    score smallint NOT NULL,
    homes integer NOT NULL,
    qpe_1h numeric NOT NULL,
    ffw boolean NOT NULL,
    geom geometry(Point, 4326) NOT NULL,
    first_seen timestamptz NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);