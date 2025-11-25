-- code/database/schema.sql

DROP TABLE IF EXISTS region_counts CASCADE;
DROP TABLE IF EXISTS brain_regions CASCADE;
DROP TABLE IF EXISTS subjects CASCADE;

-- 1. SUBJECTS
CREATE TABLE subjects (
    subject_id VARCHAR(50) PRIMARY KEY, -- e.g., 'sub-dbl01'
    original_id VARCHAR(100) UNIQUE,    -- e.g., 'DBL_A'
    sex CHAR(1),
    experiment_type VARCHAR(50),        -- 'double_injection' or 'rabies'
    details TEXT
);

-- 2. BRAIN REGIONS (Dictionary)
CREATE TABLE brain_regions (
    region_id INT PRIMARY KEY,          -- Matches CSV 'Region ID'
    name VARCHAR(255)                   -- Matches CSV 'Region name'
);

-- 3. EXPERIMENTAL DATA (The Metrics)
CREATE TABLE region_counts (
    id SERIAL PRIMARY KEY,
    subject_id VARCHAR(50) REFERENCES subjects(subject_id),
    region_id INT REFERENCES brain_regions(region_id),
    
    -- Metrics from your CSV
    region_pixels BIGINT,
    region_area_mm FLOAT,      -- Mapped from 'Region area'
    object_count INT,          -- Handles 'N/A' by storing NULL
    object_pixels BIGINT,
    object_area_mm FLOAT,
    load FLOAT,
    norm_load FLOAT,
    
    -- Metadata
    hemisphere VARCHAR(20)     -- 'left', 'right', or 'bilateral'
);