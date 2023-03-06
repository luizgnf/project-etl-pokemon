-- DATABASE: pokemon

DROP DATABASE IF EXISTS pokemon;
CREATE DATABASE pokemon
    WITH
    OWNER = admin_pgsql
    ENCODING = 'UTF8'
    LC_COLLATE = 'en_US.UTF-8'
    LC_CTYPE = 'en_US.UTF-8'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1
    IS_TEMPLATE = False;
COMMENT ON DATABASE pokemon
	IS 'datalake for general pokemon information';	

-- SCHEMA: landing

DROP SCHEMA IF EXISTS landing;
CREATE SCHEMA IF NOT EXISTS landing;
COMMENT ON SCHEMA landing
	IS 'temporary tables for extracting and loading data';

-- SCHEMA: currentraw

DROP SCHEMA IF EXISTS currentraw;
CREATE SCHEMA IF NOT EXISTS currentraw;
COMMENT ON SCHEMA currentraw
	IS 'first layer for raw data in any format';

-- SCHEMA: historyraw

DROP SCHEMA IF EXISTS historyraw;
CREATE SCHEMA IF NOT EXISTS historyraw;
COMMENT ON SCHEMA historyraw
	IS 'history of currentraw schema';

-- SCHEMA: structured

DROP SCHEMA IF EXISTS structured;
CREATE SCHEMA IF NOT EXISTS structured;
COMMENT ON SCHEMA structured
	IS 'second layer for data flattening';
	
-- SCHEMA: trusted

DROP SCHEMA IF EXISTS trusted;
CREATE SCHEMA IF NOT EXISTS trusted;
COMMENT ON SCHEMA trusted
	IS 'third layer for normalization and data processing';