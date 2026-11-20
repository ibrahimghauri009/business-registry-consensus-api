-- =====================================================================
-- AI4012 Assignment 1 — storage engine
-- Product: gov.business.registration.v1
--
-- Three concerns, three tables:
--   raw_ingest         immutable per-source evidence  (audit trail)
--   consensus_record   the fused single source of truth (serving cache)
--   registration_rollup  historical summary that outlives the fine-grained rows
-- plus lifecycle_audit, which proves the purger actually ran.
-- =====================================================================

CREATE TABLE IF NOT EXISTS raw_ingest (
    id              BIGSERIAL   PRIMARY KEY,
    product_id      TEXT        NOT NULL,
    query_key       TEXT        NOT NULL,
    source_id       TEXT        NOT NULL,
    publisher       TEXT        NOT NULL,
    request_id      TEXT        NOT NULL,
    fetch_status    TEXT        NOT NULL CHECK (fetch_status IN ('ok', 'failed')),
    error_code      TEXT,
    payload         JSONB       NOT NULL DEFAULT '{}'::jsonb,
    normalised      JSONB,
    latency_ms      INTEGER     NOT NULL DEFAULT 0,
    retrieved_at    TIMESTAMPTZ NOT NULL,
    ttl_seconds     INTEGER     NOT NULL,
    expires_at      TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS consensus_record (
    id                      BIGSERIAL PRIMARY KEY,
    product_id              TEXT        NOT NULL,
    query_key               TEXT        NOT NULL,
    request_id              TEXT        NOT NULL,
    data                    JSONB       NOT NULL,
    meta                    JSONB       NOT NULL,
    confidence              NUMERIC(5,4) NOT NULL,
    quality_score           NUMERIC(5,4) NOT NULL,
    verified                BOOLEAN     NOT NULL,
    contributing_sources    TEXT[]      NOT NULL,
    source_last_updated_at  TIMESTAMPTZ,
    computed_at             TIMESTAMPTZ NOT NULL,
    ttl_seconds             INTEGER     NOT NULL,
    expires_at              TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS registration_rollup (
    product_id   TEXT  NOT NULL,
    query_key    TEXT  NOT NULL,
    bucket_day   DATE  NOT NULL,
    summary      JSONB NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (product_id, query_key, bucket_day)
);

CREATE TABLE IF NOT EXISTS lifecycle_audit (
    id            BIGSERIAL PRIMARY KEY,
    ran_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    action        TEXT        NOT NULL,
    target_table  TEXT        NOT NULL,
    rows_affected INTEGER     NOT NULL,
    detail        JSONB       NOT NULL DEFAULT '{}'::jsonb
);

-- ---------------------------------------------------------------------
-- Indexes
--   * JSONB payloads use jsonb_path_ops GIN: smaller and faster than the
--     default operator class for the containment (@>) queries we actually run.
--   * expires_at drives the purger, so it gets its own btree.
--   * (query_key, computed_at DESC) serves the hot "latest answer" read.
-- ---------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_raw_payload_gin
    ON raw_ingest USING GIN (payload jsonb_path_ops);
CREATE INDEX IF NOT EXISTS idx_raw_expires
    ON raw_ingest (expires_at);
CREATE INDEX IF NOT EXISTS idx_raw_lookup
    ON raw_ingest (product_id, query_key, retrieved_at DESC);

CREATE INDEX IF NOT EXISTS idx_consensus_data_gin
    ON consensus_record USING GIN (data jsonb_path_ops);
CREATE INDEX IF NOT EXISTS idx_consensus_meta_gin
    ON consensus_record USING GIN (meta jsonb_path_ops);
CREATE INDEX IF NOT EXISTS idx_consensus_expires
    ON consensus_record (expires_at);
CREATE INDEX IF NOT EXISTS idx_consensus_latest
    ON consensus_record (product_id, query_key, computed_at DESC);

-- ---------------------------------------------------------------------
-- Lifecycle: summarise, then purge.
-- Fine-grained consensus rows are rolled into one row per (query, day)
-- before they are deleted, so history survives TTL expiry without keeping
-- every observation forever.
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION summarise_expired_consensus()
RETURNS INTEGER AS $$
DECLARE
    affected INTEGER;
BEGIN
    WITH expired AS (
        SELECT product_id,
               query_key,
               date_trunc('day', computed_at)::date AS bucket_day,
               count(*)                              AS observations,
               avg(confidence)                       AS avg_confidence,
               min(confidence)                       AS min_confidence,
               avg(quality_score)                    AS avg_quality,
               bool_and(verified)                    AS always_verified,
               mode() WITHIN GROUP (ORDER BY data->>'status')     AS modal_status,
               mode() WITHIN GROUP (ORDER BY data->>'legal_name') AS modal_legal_name
        FROM consensus_record
        WHERE expires_at <= now()
        GROUP BY 1, 2, 3
    )
    INSERT INTO registration_rollup (product_id, query_key, bucket_day, summary, updated_at)
    SELECT product_id,
           query_key,
           bucket_day,
           jsonb_build_object(
               'observations',    observations,
               'avg_confidence',  round(avg_confidence, 4),
               'min_confidence',  round(min_confidence, 4),
               'avg_quality',     round(avg_quality, 4),
               'always_verified', always_verified,
               'modal_status',    modal_status,
               'modal_legal_name', modal_legal_name
           ),
           now()
    FROM expired
    ON CONFLICT (product_id, query_key, bucket_day) DO UPDATE
        SET summary    = registration_rollup.summary || EXCLUDED.summary,
            updated_at = now();

    GET DIAGNOSTICS affected = ROW_COUNT;

    INSERT INTO lifecycle_audit (action, target_table, rows_affected)
    VALUES ('summarise', 'registration_rollup', affected);

    RETURN affected;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION purge_expired()
RETURNS TABLE (summarised INTEGER, consensus_purged INTEGER, raw_purged INTEGER) AS $$
DECLARE
    s INTEGER;
    c INTEGER;
    r INTEGER;
BEGIN
    -- Always summarise before deleting: the rollup is what survives.
    s := summarise_expired_consensus();

    DELETE FROM consensus_record WHERE expires_at <= now();
    GET DIAGNOSTICS c = ROW_COUNT;
    INSERT INTO lifecycle_audit (action, target_table, rows_affected)
    VALUES ('purge', 'consensus_record', c);

    DELETE FROM raw_ingest WHERE expires_at <= now();
    GET DIAGNOSTICS r = ROW_COUNT;
    INSERT INTO lifecycle_audit (action, target_table, rows_affected)
    VALUES ('purge', 'raw_ingest', r);

    summarised := s; consensus_purged := c; raw_purged := r;
    RETURN NEXT;
END;
$$ LANGUAGE plpgsql;

-- Optional: run the purger inside the database itself if pg_cron is available.
-- The Python background worker in app/lifecycle.py does the same job and is
-- used by default, so this is a belt-and-braces alternative, not a dependency.
--
--   CREATE EXTENSION IF NOT EXISTS pg_cron;
--   SELECT cron.schedule('registration-ttl-purge', '*/5 * * * *',
--                        $$SELECT purge_expired()$$);
