CREATE DATABASE web_crawler_rds_psql;
\c web_crawler_rds_psql
CREATE SCHEMA IF NOT EXISTS web_crawler;
CREATE USER web_crawler_admin WITH SUPERUSER PASSWORD 'test';
CREATE EXTENSION IF NOT EXISTS pgcrypto;
ALTER DATABASE web_crawler_rds_psql SET search_path TO web_crawler;

CREATE TABLE IF NOT EXISTS web_crawler.crawl_metadata
(
    url_hash             VARCHAR(64) UNIQUE PRIMARY KEY,
    full_url             VARCHAR(1000) NOT NULL,
    time_to_crawl        INT           NOT NULL, -- in milliseconds
    timestamp            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status               VARCHAR(1000) NOT NULL, -- may contain an error message
    status_code          SMALLINT      NOT NULL,
    crawl_mechanism      VARCHAR(30)   NOT NULL,
    crawl_worker_version VARCHAR(30)   NOT NULL,
    e_tag                VARCHAR(255)  NULL
);

CREATE TABLE IF NOT EXISTS web_crawler.custom_rule
(
    id         SERIAL PRIMARY KEY,
    domain     VARCHAR(80) NOT NULL UNIQUE,
    robots_txt TEXT        NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT domain_index UNIQUE (domain)
);

CREATE TABLE IF NOT EXISTS web_crawler.api_key
(
    id         SERIAL PRIMARY KEY,
    api_key    VARCHAR(64)  NOT NULL UNIQUE,
    email      VARCHAR(100) NOT NULL,
    is_active  BOOLEAN   DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE OR REPLACE FUNCTION web_crawler.set_updated_at()
    RETURNS TRIGGER AS
$$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_custom_rule
    BEFORE UPDATE
    ON web_crawler.custom_rule
    FOR EACH ROW
EXECUTE FUNCTION web_crawler.set_updated_at();

CREATE OR REPLACE FUNCTION web_crawler.hash_api_key()
    RETURNS TRIGGER AS
$$
BEGIN
    NEW.api_key := encode(digest(NEW.api_key, 'sha256'), 'hex');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER before_insert_api_key
    BEFORE INSERT
    ON web_crawler.api_key
    FOR EACH ROW
EXECUTE FUNCTION web_crawler.hash_api_key();

CREATE USER web_crawler_rw_user WITH PASSWORD 'test';
GRANT USAGE ON SCHEMA web_crawler TO web_crawler_rw_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA web_crawler TO web_crawler_rw_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA web_crawler TO web_crawler_rw_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA web_crawler GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO web_crawler_rw_user;
GRANT EXECUTE ON FUNCTION web_crawler.set_updated_at TO web_crawler_rw_user;
GRANT EXECUTE ON FUNCTION web_crawler.hash_api_key TO web_crawler_rw_user;
