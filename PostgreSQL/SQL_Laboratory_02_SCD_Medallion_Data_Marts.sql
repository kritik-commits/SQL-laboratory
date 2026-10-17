-- ============================================================================
-- SQL LABORATORY — SCD + MEDALLION ARCHITECTURE IMPLEMENTATION WORKBOOK
-- File 2 of 2: end-to-end, practical historical modelling and data pipeline lab.
--
-- PRIMARY DIALECT: PostgreSQL. Redshift-specific equivalents are noted inline.
-- Run each numbered section in order in a disposable PostgreSQL database.
-- A single source-of-truth sample customer feed is progressively transformed.
-- No GitHub files are modified by creating this local review artifact.
-- ============================================================================

-- CONTENT MAP
-- 01. Business scenario, grain, source contract, and reproducible setup
-- 02. Source landing and Bronze layer (raw preservation)
-- 03. Silver layer (typing, standardization, dedupe, quality, quarantine)
-- 04. SCD Type 0 (retain original value)
-- 05. SCD Type 1 (overwrite current value)
-- 06. SCD Type 2 (full effective-dated history)
-- 07. SCD Type 3 (limited previous-value history)
-- 08. SCD Type 4 (current dimension + separate history table)
-- 09. SCD Type 6 (hybrid Type 1/2/3)
-- 10. Gold layer and business data marts
-- 11. Incremental processing, watermarks, CDC, idempotency
-- 12. CDQM, reconciliation, audits, and operational controls
-- 13. Amazon Redshift adaptations and implementation checklist
--
-- BUSINESS CASE
-- An e-commerce company receives customer changes from an operational source.
-- The same customer may move city, change segment, or change consent status.
-- Analysts need current customer attributes and historically accurate reporting.
-- Grain: one source event per source_event_id; one dimension version per customer
-- and validity interval; one gold fact row per order line/order, as explicitly stated.

-- ============================================================================
-- 01. RESETTABLE PRACTICE SCHEMA AND SOURCE CONTRACT
-- ============================================================================
CREATE SCHEMA IF NOT EXISTS scd_lab;
SET search_path TO scd_lab, public;

-- For a fresh, isolated sandbox only. Drop dependencies in reverse order.
-- DROP SCHEMA scd_lab CASCADE;

CREATE TABLE IF NOT EXISTS source_customer_events (
    source_event_id   BIGINT PRIMARY KEY,
    customer_id       BIGINT NOT NULL,
    source_updated_at TIMESTAMPTZ NOT NULL,
    ingested_at       TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    operation         CHAR(1) NOT NULL CHECK (operation IN ('I','U','D')),
    email             TEXT,
    full_name         TEXT,
    city              TEXT,
    segment            TEXT,
    marketing_consent BOOLEAN,
    source_version    BIGINT NOT NULL
);

-- Insert an initial state and later updates. ON CONFLICT makes seed rerunnable.
INSERT INTO source_customer_events
(source_event_id, customer_id, source_updated_at, operation, email, full_name, city, segment, marketing_consent, source_version)
VALUES
(1001, 501, '2026-01-01 09:00:00+00', 'I', 'asha@example.com', 'Asha Rao', 'Bengaluru', 'Standard', TRUE, 1),
(1002, 502, '2026-01-01 09:02:00+00', 'I', 'dev@example.com', 'Dev Singh', 'Pune', 'Standard', FALSE, 1),
(1003, 503, '2026-01-02 10:00:00+00', 'I', 'meera@example.com', 'Meera Iyer', 'Chennai', 'Premium', TRUE, 1),
(1004, 501, '2026-03-15 11:00:00+00', 'U', 'asha@example.com', 'Asha Rao', 'Hyderabad', 'Premium', TRUE, 2),
(1005, 502, '2026-04-01 08:00:00+00', 'U', 'dev.singh@example.com', 'Dev Singh', 'Pune', 'Standard', FALSE, 2),
(1006, 501, '2026-06-01 07:00:00+00', 'U', 'asha@example.com', 'Asha Rao', 'Hyderabad', 'VIP', FALSE, 3)
ON CONFLICT (source_event_id) DO NOTHING;

-- Inspect source event history.
SELECT * FROM source_customer_events ORDER BY customer_id, source_version;

-- ============================================================================
-- 02. BRONZE LAYER — RAW PRESERVATION
-- ============================================================================
-- Design: preserve source shape and payload, ingestion metadata, and replayability.
-- Bronze should not silently discard malformed rows. Additive schema evolution is
-- generally safer than destructive reinterpretation of previously landed records.
CREATE TABLE IF NOT EXISTS bronze_customer_events (
    source_event_id   BIGINT PRIMARY KEY,
    customer_id       BIGINT,
    source_updated_at TIMESTAMPTZ,
    ingested_at       TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    operation         CHAR(1),
    email_raw         TEXT,
    full_name_raw     TEXT,
    city_raw          TEXT,
    segment_raw       TEXT,
    marketing_consent BOOLEAN,
    source_version    BIGINT,
    raw_payload       JSONB,
    batch_id          TEXT NOT NULL,
    landed_at         TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Idempotent landing: unique source_event_id prevents duplicate event ingestion.
INSERT INTO bronze_customer_events
(source_event_id, customer_id, source_updated_at, ingested_at, operation,
 email_raw, full_name_raw, city_raw, segment_raw, marketing_consent,
 source_version, raw_payload, batch_id)
SELECT source_event_id, customer_id, source_updated_at, ingested_at, operation,
       email, full_name, city, segment, marketing_consent, source_version,
       jsonb_build_object('email',email,'full_name',full_name,'city',city,
                          'segment',segment,'marketing_consent',marketing_consent),
       'batch_2026_10_17_001'
FROM source_customer_events
ON CONFLICT (source_event_id) DO NOTHING;

-- Bronze audit: compare source event count to landed event count.
SELECT 'source' AS layer, COUNT(*) AS rows FROM source_customer_events
UNION ALL
SELECT 'bronze', COUNT(*) FROM bronze_customer_events;

-- ============================================================================
-- 03. SILVER LAYER — STANDARDIZE, VALIDATE, DEDUPLICATE, QUARANTINE
-- ============================================================================
CREATE TABLE IF NOT EXISTS silver_customer_events (
    source_event_id   BIGINT PRIMARY KEY,
    customer_id       BIGINT NOT NULL,
    source_updated_at TIMESTAMPTZ NOT NULL,
    operation         CHAR(1) NOT NULL CHECK (operation IN ('I','U','D')),
    email             TEXT,
    full_name         TEXT,
    city              TEXT,
    segment            TEXT,
    marketing_consent BOOLEAN,
    source_version    BIGINT NOT NULL,
    email_normalized  TEXT,
    quality_status    TEXT NOT NULL CHECK (quality_status IN ('valid','quarantined')),
    quality_reason    TEXT,
    batch_id          TEXT NOT NULL,
    transformed_at    TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS silver_customer_quarantine (
    source_event_id BIGINT PRIMARY KEY,
    customer_id BIGINT,
    batch_id TEXT NOT NULL,
    reason TEXT NOT NULL,
    raw_payload JSONB,
    quarantined_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Classify all landed events; do not use DISTINCT as a substitute for a defined
-- dedupe rule. Event ID is unique; latest state per customer is selected later.
WITH normalized AS (
 SELECT b.*,
        NULLIF(LOWER(TRIM(email_raw)), '') AS email_clean,
        NULLIF(TRIM(full_name_raw), '') AS name_clean,
        NULLIF(INITCAP(TRIM(city_raw)), '') AS city_clean,
        NULLIF(INITCAP(TRIM(segment_raw)), '') AS segment_clean,
        CASE
          WHEN customer_id IS NULL THEN 'missing_customer_id'
          WHEN operation NOT IN ('I','U','D') OR operation IS NULL THEN 'invalid_operation'
          WHEN source_version IS NULL THEN 'missing_source_version'
          WHEN operation <> 'D' AND NULLIF(TRIM(email_raw),'') IS NULL THEN 'missing_email'
          ELSE NULL
        END AS reason
 FROM bronze_customer_events b
), classified AS (
 SELECT *, CASE WHEN reason IS NULL THEN 'valid' ELSE 'quarantined' END AS status
 FROM normalized
)
INSERT INTO silver_customer_events
(source_event_id, customer_id, source_updated_at, operation, email, full_name,
 city, segment, marketing_consent, source_version, email_normalized,
 quality_status, quality_reason, batch_id)
SELECT source_event_id, customer_id, source_updated_at, operation, email_clean,
       name_clean, city_clean, segment_clean, marketing_consent, source_version,
       email_clean, status, reason, batch_id
FROM classified
ON CONFLICT (source_event_id) DO UPDATE SET
  customer_id = EXCLUDED.customer_id,
  source_updated_at = EXCLUDED.source_updated_at,
  operation = EXCLUDED.operation,
  email = EXCLUDED.email,
  full_name = EXCLUDED.full_name,
  city = EXCLUDED.city,
  segment = EXCLUDED.segment,
  marketing_consent = EXCLUDED.marketing_consent,
  source_version = EXCLUDED.source_version,
  email_normalized = EXCLUDED.email_normalized,
  quality_status = EXCLUDED.quality_status,
  quality_reason = EXCLUDED.quality_reason,
  batch_id = EXCLUDED.batch_id,
  transformed_at = CURRENT_TIMESTAMP;

INSERT INTO silver_customer_quarantine(source_event_id, customer_id, batch_id, reason, raw_payload)
SELECT s.source_event_id, s.customer_id, s.batch_id, s.quality_reason, b.raw_payload
FROM silver_customer_events s JOIN bronze_customer_events b USING(source_event_id)
WHERE s.quality_status = 'quarantined'
ON CONFLICT (source_event_id) DO UPDATE SET reason = EXCLUDED.reason,
 raw_payload = EXCLUDED.raw_payload, quarantined_at = CURRENT_TIMESTAMP;

-- Silver quality checks.
SELECT quality_status, COUNT(*) FROM silver_customer_events GROUP BY quality_status;
SELECT email_normalized, COUNT(*) FROM silver_customer_events
WHERE quality_status = 'valid' AND operation <> 'D'
GROUP BY email_normalized HAVING COUNT(*) > 1;

-- Current source state: deterministic latest event per customer; deletes excluded.
CREATE OR REPLACE VIEW silver_customer_current AS
WITH ranked AS (
 SELECT s.*,
        ROW_NUMBER() OVER (PARTITION BY customer_id
                           ORDER BY source_version DESC, source_updated_at DESC, source_event_id DESC) AS rn
 FROM silver_customer_events s WHERE quality_status = 'valid'
)
SELECT * FROM ranked WHERE rn = 1 AND operation <> 'D';
SELECT customer_id, email, full_name, city, segment, marketing_consent
FROM silver_customer_current ORDER BY customer_id;

-- ============================================================================
-- 04. SCD TYPE 0 — RETAIN THE ORIGINAL VALUE
-- ============================================================================
-- Type 0 means the attribute is not updated after initial assignment.
-- Common examples: original acquisition channel, original signup cohort.
CREATE TABLE IF NOT EXISTS dim_customer_type0 (
 customer_id BIGINT PRIMARY KEY,
 original_acquisition_channel TEXT NOT NULL,
 first_seen_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
INSERT INTO dim_customer_type0(customer_id, original_acquisition_channel, first_seen_at)
VALUES (501,'organic','2026-01-01 09:00:00+00'), (502,'paid_search','2026-01-01 09:02:00+00')
ON CONFLICT(customer_id) DO NOTHING;
-- A later source channel should not overwrite the original Type 0 attribute.
INSERT INTO dim_customer_type0(customer_id, original_acquisition_channel)
VALUES (501,'affiliate') ON CONFLICT(customer_id) DO NOTHING;
SELECT * FROM dim_customer_type0 ORDER BY customer_id;

-- ============================================================================
-- 05. SCD TYPE 1 — OVERWRITE CURRENT VALUE; NO ATTRIBUTE HISTORY
-- ============================================================================
CREATE TABLE IF NOT EXISTS dim_customer_type1 (
 customer_id BIGINT PRIMARY KEY,
 email TEXT NOT NULL,
 full_name TEXT,
 city TEXT,
 segment TEXT,
 marketing_consent BOOLEAN,
 source_version BIGINT NOT NULL,
 updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
-- Initial load: current valid record per customer.
INSERT INTO dim_customer_type1(customer_id,email,full_name,city,segment,marketing_consent,source_version)
SELECT customer_id,email,full_name,city,segment,marketing_consent,source_version
FROM silver_customer_current
ON CONFLICT(customer_id) DO UPDATE SET
 email=EXCLUDED.email, full_name=EXCLUDED.full_name, city=EXCLUDED.city,
 segment=EXCLUDED.segment, marketing_consent=EXCLUDED.marketing_consent,
 source_version=EXCLUDED.source_version, updated_at=CURRENT_TIMESTAMP
WHERE EXCLUDED.source_version >= dim_customer_type1.source_version;

-- Inspect the result: only the latest attribute values are retained.
SELECT * FROM dim_customer_type1 ORDER BY customer_id;
-- Type 1 is suitable for corrections or attributes where prior values are irrelevant.
-- It cannot answer: "What city did the customer live in when the order occurred?"

-- ============================================================================
-- 06. SCD TYPE 2 — FULL HISTORICAL VERSIONS WITH EFFECTIVE INTERVALS
-- ============================================================================
-- One row per customer version. Convention: [valid_from, valid_to) intervals;
-- current rows have valid_to = infinity and is_current = true.
CREATE TABLE IF NOT EXISTS dim_customer_type2 (
 customer_sk BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
 customer_id BIGINT NOT NULL,
 email TEXT NOT NULL,
 full_name TEXT,
 city TEXT,
 segment TEXT,
 marketing_consent BOOLEAN,
 source_version BIGINT NOT NULL,
 valid_from TIMESTAMPTZ NOT NULL,
 valid_to TIMESTAMPTZ NOT NULL DEFAULT 'infinity'::timestamptz,
 is_current BOOLEAN NOT NULL DEFAULT TRUE,
 attribute_hash TEXT,
 CONSTRAINT ck_customer_type2_interval CHECK(valid_from < valid_to),
 CONSTRAINT uq_customer_type2_version UNIQUE(customer_id, source_version)
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_customer_type2_current
ON dim_customer_type2(customer_id) WHERE is_current;

-- First load: one current row per customer.
INSERT INTO dim_customer_type2
(customer_id,email,full_name,city,segment,marketing_consent,source_version,valid_from,valid_to,is_current,attribute_hash)
SELECT customer_id,email,full_name,city,segment,marketing_consent,source_version,
       source_updated_at,'infinity'::timestamptz,TRUE,
       MD5(CONCAT_WS('|',email,full_name,city,segment,marketing_consent::text))
FROM silver_customer_current
ON CONFLICT(customer_id,source_version) DO NOTHING;

-- View current versions.
SELECT customer_id, customer_sk, city, segment, marketing_consent, valid_from, valid_to, is_current
FROM dim_customer_type2 ORDER BY customer_id, valid_from;

-- Simulate a later source change by staging a new event (new source_version).
INSERT INTO source_customer_events
(source_event_id,customer_id,source_updated_at,operation,email,full_name,city,segment,marketing_consent,source_version)
VALUES (1007,503,'2026-08-10 12:00:00+00','U','meera@example.com','Meera Iyer','Mumbai','VIP',TRUE,2)
ON CONFLICT(source_event_id) DO NOTHING;
-- For the real pipeline, repeat Bronze -> Silver processing for the new event.
INSERT INTO bronze_customer_events
(source_event_id,customer_id,source_updated_at,ingested_at,operation,email_raw,full_name_raw,city_raw,segment_raw,marketing_consent,source_version,raw_payload,batch_id)
SELECT source_event_id,customer_id,source_updated_at,ingested_at,operation,email,full_name,city,segment,marketing_consent,source_version,
       jsonb_build_object('email',email,'city',city,'segment',segment),'batch_2026_08_10_001'
FROM source_customer_events WHERE source_event_id=1007
ON CONFLICT(source_event_id) DO NOTHING;
INSERT INTO silver_customer_events
(source_event_id,customer_id,source_updated_at,operation,email,full_name,city,segment,marketing_consent,source_version,email_normalized,quality_status,batch_id)
SELECT source_event_id,customer_id,source_updated_at,operation,NULLIF(LOWER(TRIM(email)) ,''),NULLIF(TRIM(full_name),''),
       NULLIF(INITCAP(TRIM(city)),''),NULLIF(INITCAP(TRIM(segment)),''),marketing_consent,source_version,
       NULLIF(LOWER(TRIM(email)),''),'valid','batch_2026_08_10_001'
FROM source_customer_events WHERE source_event_id=1007
ON CONFLICT(source_event_id) DO NOTHING;

-- SCD2 change application. This example compares a tracked-attribute hash and
-- applies only strictly newer source versions. Process per customer in source order.
WITH latest AS (
 SELECT DISTINCT ON(customer_id) customer_id,email,full_name,city,segment,marketing_consent,
        source_version,source_updated_at,
        MD5(CONCAT_WS('|',email,full_name,city,segment,marketing_consent::text)) AS new_hash
 FROM silver_customer_events
 WHERE quality_status='valid' AND operation <> 'D'
 ORDER BY customer_id,source_version DESC,source_updated_at DESC,source_event_id DESC
), changed AS (
 SELECT l.* FROM latest l JOIN dim_customer_type2 d
   ON d.customer_id=l.customer_id AND d.is_current
 WHERE l.source_version > d.source_version AND l.new_hash IS DISTINCT FROM d.attribute_hash
), closed AS (
 UPDATE dim_customer_type2 d SET valid_to=c.source_updated_at, is_current=FALSE
 FROM changed c
 WHERE d.customer_id=c.customer_id AND d.is_current
   AND c.source_updated_at > d.valid_from
 RETURNING d.customer_id
)
INSERT INTO dim_customer_type2
(customer_id,email,full_name,city,segment,marketing_consent,source_version,valid_from,valid_to,is_current,attribute_hash)
SELECT c.customer_id,c.email,c.full_name,c.city,c.segment,c.marketing_consent,c.source_version,
       c.source_updated_at,'infinity'::timestamptz,TRUE,c.new_hash
FROM changed c JOIN closed x USING(customer_id)
ON CONFLICT(customer_id,source_version) DO NOTHING;

-- Validate SCD2: exactly one current row per active customer; intervals do not overlap.
SELECT customer_id, COUNT(*) FILTER(WHERE is_current) AS current_versions
FROM dim_customer_type2 GROUP BY customer_id
HAVING COUNT(*) FILTER(WHERE is_current) <> 1;
WITH ordered AS (
 SELECT customer_id, valid_from, valid_to,
        LAG(valid_to) OVER(PARTITION BY customer_id ORDER BY valid_from) AS previous_valid_to
 FROM dim_customer_type2
)
SELECT * FROM ordered WHERE previous_valid_to > valid_from;
-- Point-in-time lookup: customer state at order timestamp.
SELECT f.order_id, d.customer_sk, d.city, d.segment
FROM fact_orders f JOIN dim_customer_type2 d
  ON f.customer_id=d.customer_id
 AND f.order_ts >= d.valid_from AND f.order_ts < d.valid_to;

-- Production notes for SCD2:
-- * Handle out-of-order events and retroactive corrections explicitly.
-- * If source_updated_at <= current valid_from, do not silently create invalid intervals.
-- * Decide whether all attributes or only selected attributes trigger a new version.
-- * Use a stable attribute hash or null-safe column-by-column comparison.
-- * Coordinate concurrent writers; enforce one current row with a partial unique index.

-- ============================================================================
-- 07. SCD TYPE 3 — LIMITED HISTORY IN COLUMNS
-- ============================================================================
-- Stores current and previous value for a selected attribute, not unlimited history.
CREATE TABLE IF NOT EXISTS dim_customer_type3 (
 customer_id BIGINT PRIMARY KEY,
 email TEXT NOT NULL,
 current_city TEXT,
 previous_city TEXT,
 city_changed_at TIMESTAMPTZ,
 segment TEXT,
 source_version BIGINT NOT NULL
);
INSERT INTO dim_customer_type3(customer_id,email,current_city,previous_city,city_changed_at,segment,source_version)
SELECT customer_id,email,city,NULL,NULL,segment,source_version FROM silver_customer_current
ON CONFLICT(customer_id) DO NOTHING;

-- Apply a city change. Previous city shifts to one previous-value column.
WITH src AS (
 SELECT customer_id,email,city,segment,source_version,source_updated_at
 FROM silver_customer_events WHERE source_event_id=1007 AND quality_status='valid'
)
UPDATE dim_customer_type3 d SET
 previous_city = CASE WHEN d.current_city IS DISTINCT FROM s.city THEN d.current_city ELSE d.previous_city END,
 city_changed_at = CASE WHEN d.current_city IS DISTINCT FROM s.city THEN s.source_updated_at ELSE d.city_changed_at END,
 current_city = s.city,
 email = s.email,
 segment = s.segment,
 source_version = s.source_version
FROM src s
WHERE d.customer_id=s.customer_id AND s.source_version > d.source_version;
SELECT * FROM dim_customer_type3 ORDER BY customer_id;

-- ============================================================================
-- 08. SCD TYPE 4 — CURRENT DIMENSION + SEPARATE HISTORY TABLE
-- ============================================================================
CREATE TABLE IF NOT EXISTS dim_customer_type4_current (
 customer_id BIGINT PRIMARY KEY,
 email TEXT NOT NULL,
 full_name TEXT,
 city TEXT,
 segment TEXT,
 marketing_consent BOOLEAN,
 source_version BIGINT NOT NULL,
 updated_at TIMESTAMPTZ NOT NULL
);
CREATE TABLE IF NOT EXISTS dim_customer_type4_history (
 history_sk BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
 customer_id BIGINT NOT NULL,
 email TEXT,
 full_name TEXT,
 city TEXT,
 segment TEXT,
 marketing_consent BOOLEAN,
 source_version BIGINT NOT NULL,
 valid_from TIMESTAMPTZ NOT NULL,
 valid_to TIMESTAMPTZ NOT NULL,
 change_reason TEXT,
 archived_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
 CHECK(valid_from < valid_to)
);
-- Initial current snapshot.
INSERT INTO dim_customer_type4_current(customer_id,email,full_name,city,segment,marketing_consent,source_version,updated_at)
SELECT customer_id,email,full_name,city,segment,marketing_consent,source_version,source_updated_at
FROM silver_customer_current
ON CONFLICT(customer_id) DO NOTHING;
-- When changing a current record: archive old row first, then update current row.
-- This is shown transactionally for customer 503. Re-run only if the version is new.
BEGIN;
WITH incoming AS (
 SELECT customer_id,email,full_name,city,segment,marketing_consent,source_version,source_updated_at
 FROM silver_customer_events WHERE source_event_id=1007 AND quality_status='valid'
), archived AS (
 INSERT INTO dim_customer_type4_history
 (customer_id,email,full_name,city,segment,marketing_consent,source_version,valid_from,valid_to,change_reason)
 SELECT d.customer_id,d.email,d.full_name,d.city,d.segment,d.marketing_consent,d.source_version,
        d.updated_at,i.source_updated_at,'source_attribute_change'
 FROM dim_customer_type4_current d JOIN incoming i USING(customer_id)
 WHERE i.source_version > d.source_version AND i.source_updated_at > d.updated_at
 RETURNING customer_id
)
UPDATE dim_customer_type4_current d SET email=i.email,full_name=i.full_name,city=i.city,
 segment=i.segment,marketing_consent=i.marketing_consent,source_version=i.source_version,updated_at=i.source_updated_at
FROM incoming i JOIN archived a USING(customer_id)
WHERE d.customer_id=i.customer_id;
COMMIT;
-- Type 4 offers a compact current lookup and a separate archive for history queries.

-- ============================================================================
-- 09. SCD TYPE 6 — HYBRID TYPE 1 + TYPE 2 + TYPE 3
-- ============================================================================
-- Common convention: Type 2 creates version rows; Type 1 overwrites a current
-- attribute across all versions; Type 3 stores a previous value on each version.
-- Implementations vary; document the exact hybrid contract in your warehouse.
CREATE TABLE IF NOT EXISTS dim_customer_type6 (
 customer_sk BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
 customer_id BIGINT NOT NULL,
 email_current TEXT NOT NULL,           -- Type 1: current value repeated on all versions
 city_versioned TEXT,                  -- Type 2: value at this historical version
 segment_versioned TEXT,               -- Type 2: value at this historical version
 previous_segment TEXT,                -- Type 3: previous segment on the version/current row
 source_version BIGINT NOT NULL,
 valid_from TIMESTAMPTZ NOT NULL,
 valid_to TIMESTAMPTZ NOT NULL DEFAULT 'infinity'::timestamptz,
 is_current BOOLEAN NOT NULL DEFAULT TRUE,
 attribute_hash TEXT,
 CHECK(valid_from < valid_to),
 UNIQUE(customer_id,source_version)
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_customer_type6_current ON dim_customer_type6(customer_id) WHERE is_current;
-- Seed one current version per customer.
INSERT INTO dim_customer_type6(customer_id,email_current,city_versioned,segment_versioned,previous_segment,
 source_version,valid_from,attribute_hash)
SELECT customer_id,email,city,segment,NULL,source_version,source_updated_at,
       MD5(CONCAT_WS('|',email,city,segment))
FROM silver_customer_current
ON CONFLICT(customer_id,source_version) DO NOTHING;

-- Type 6 operational rules:
-- 1. A tracked Type 2 change closes the current version and inserts a new row.
-- 2. On segment change, set previous_segment to the prior segment (Type 3 behavior).
-- 3. If email is Type 1, overwrite email_current in every historical version for that customer.
-- 4. Keep a clear definition of which attributes are Type 1, Type 2, and Type 3.
-- The following demonstrates the Type 1 overwrite across all versions.
UPDATE dim_customer_type6 SET email_current='new@example.com' WHERE customer_id=501;
-- A Type 2/3 change should be applied in a transaction similar to the SCD2 change routine.

-- ============================================================================
-- 10. GOLD LAYER — DIMENSIONS, FACTS, AND DATA MARTS
-- ============================================================================
-- Gold is a governed business interface. Each table must state its grain.
-- Mart 1: customer 360, one row per customer.
CREATE TABLE IF NOT EXISTS gold_customer_360 AS
SELECT c.customer_id, c.email, c.full_name, c.city, c.segment, c.marketing_consent,
       c.source_version,
       CURRENT_TIMESTAMP AS built_at
FROM dim_customer_type1 c;
-- Replace/rebuild pattern for a small lab only. For production, prefer atomic swap,
-- incremental MERGE/upsert, or materialized views depending on platform/workload.

-- Example order source and order-line fact model.
CREATE TABLE IF NOT EXISTS source_orders (
 order_id BIGINT PRIMARY KEY,
 customer_id BIGINT NOT NULL,
 order_ts TIMESTAMPTZ NOT NULL,
 order_status TEXT NOT NULL,
 currency_code CHAR(3) NOT NULL DEFAULT 'INR',
 order_total NUMERIC(14,2) NOT NULL CHECK(order_total >= 0)
);
CREATE TABLE IF NOT EXISTS source_order_items (
 order_item_id BIGINT PRIMARY KEY,
 order_id BIGINT NOT NULL,
 product_id BIGINT NOT NULL,
 quantity INTEGER NOT NULL CHECK(quantity > 0),
 unit_price NUMERIC(12,2) NOT NULL CHECK(unit_price >= 0),
 line_discount NUMERIC(12,2) NOT NULL DEFAULT 0 CHECK(line_discount >= 0)
);
INSERT INTO source_orders VALUES
 (9001,501,'2026-03-20 12:00:00+00','delivered','INR',1200.00),
 (9002,502,'2026-04-05 09:30:00+00','delivered','INR',799.00),
 (9003,503,'2026-08-12 14:00:00+00','paid','INR',2400.00)
ON CONFLICT(order_id) DO NOTHING;
INSERT INTO source_order_items VALUES
 (1,9001,101,2,600.00,0),(2,9002,102,1,799.00,0),(3,9003,103,3,800.00,0)
ON CONFLICT(order_item_id) DO NOTHING;

-- Gold fact: grain = one row per order. Use the order header total as revenue
-- if that is the finance-approved definition; don't sum order totals after joining
-- to multiple order items, or revenue will be duplicated (fanout).
CREATE TABLE IF NOT EXISTS gold_fact_orders (
 order_id BIGINT PRIMARY KEY,
 customer_id BIGINT NOT NULL,
 order_ts TIMESTAMPTZ NOT NULL,
 order_date DATE NOT NULL,
 order_status TEXT NOT NULL,
 currency_code CHAR(3) NOT NULL,
 order_total NUMERIC(14,2) NOT NULL,
 loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
INSERT INTO gold_fact_orders(order_id,customer_id,order_ts,order_date,order_status,currency_code,order_total)
SELECT order_id,customer_id,order_ts,order_ts::date,order_status,currency_code,order_total FROM source_orders
ON CONFLICT(order_id) DO UPDATE SET
 customer_id=EXCLUDED.customer_id,order_ts=EXCLUDED.order_ts,order_date=EXCLUDED.order_date,
 order_status=EXCLUDED.order_status,currency_code=EXCLUDED.currency_code,order_total=EXCLUDED.order_total,
 loaded_at=CURRENT_TIMESTAMP;

-- Mart 2: monthly sales performance, one row per month/status/currency.
CREATE OR REPLACE VIEW gold_mart_monthly_sales AS
SELECT DATE_TRUNC('month',order_ts)::date AS month_start,
       order_status,currency_code,COUNT(*) AS order_count,
       SUM(order_total) AS booked_order_value,
       AVG(order_total) AS average_order_value
FROM gold_fact_orders
GROUP BY 1,2,3;
SELECT * FROM gold_mart_monthly_sales ORDER BY month_start,order_status;

-- Mart 3: customer lifetime summary, one row per customer.
CREATE OR REPLACE VIEW gold_mart_customer_lifetime AS
SELECT customer_id,COUNT(*) AS order_count,
       SUM(order_total) FILTER(WHERE order_status IN ('paid','shipped','delivered')) AS eligible_revenue,
       MIN(order_ts) AS first_order_ts,MAX(order_ts) AS most_recent_order_ts,
       AVG(order_total) AS average_order_value
FROM gold_fact_orders GROUP BY customer_id;
SELECT * FROM gold_mart_customer_lifetime ORDER BY eligible_revenue DESC NULLS LAST;

-- Mart 4: product sales, grain = one row per product (requires item-level costs
-- and a product dimension for margin; revenue shown here is line net sales).
CREATE OR REPLACE VIEW gold_mart_product_sales AS
SELECT product_id,COUNT(DISTINCT order_id) AS orders_count,
       SUM(quantity) AS units_sold,
       SUM(quantity*unit_price-line_discount) AS net_line_sales
FROM source_order_items GROUP BY product_id;
SELECT * FROM gold_mart_product_sales ORDER BY net_line_sales DESC;

-- Mart 5: customer point-in-time attributes for historically correct analysis.
-- In a real warehouse, join order timestamp to SCD2 validity intervals.
SELECT f.order_id,f.order_total,d.city,d.segment
FROM gold_fact_orders f JOIN dim_customer_type2 d
  ON d.customer_id=f.customer_id
 AND f.order_ts >= d.valid_from AND f.order_ts < d.valid_to;

-- ============================================================================
-- 11. INCREMENTAL LOADS, WATERMARKS, CDC, IDEMPOTENCY
-- ============================================================================
CREATE TABLE IF NOT EXISTS pipeline_watermarks (
 pipeline_name TEXT NOT NULL,
 source_name TEXT NOT NULL,
 watermark_ts TIMESTAMPTZ NOT NULL,
 last_successful_batch TEXT,
 updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
 PRIMARY KEY(pipeline_name,source_name)
);
INSERT INTO pipeline_watermarks(pipeline_name,source_name,watermark_ts)
VALUES('customer_events','source_customer_events','2000-01-01 00:00:00+00')
ON CONFLICT DO NOTHING;

-- Extract an overlapping lookback window to capture late-arriving updates.
-- Stable source_event_id makes the landing idempotent; a later dedupe/upsert handles revisions.
SELECT * FROM source_customer_events
WHERE source_updated_at >= (
 SELECT watermark_ts - INTERVAL '24 hours' FROM pipeline_watermarks
 WHERE pipeline_name='customer_events' AND source_name='source_customer_events'
)
ORDER BY source_updated_at,source_version,source_event_id;

-- Only advance watermark after the batch has landed, transformed, and passed checks.
-- In production: perform target writes, audit record, and watermark update in one
-- transaction where supported; otherwise use a recoverable orchestration protocol.
UPDATE pipeline_watermarks SET watermark_ts=:max_successful_source_timestamp,
 last_successful_batch=:batch_id, updated_at=CURRENT_TIMESTAMP
WHERE pipeline_name='customer_events' AND source_name='source_customer_events';

-- CDC operation semantics:
-- I = insert; U = update; D = delete/tombstone. Apply events in source sequence order.
-- A delete should close the active SCD2 version at the delete event timestamp and
-- either set a deleted flag or create a separate tombstone, according to policy.
-- Never infer deletion merely because a row is absent from an incomplete extract.

-- ============================================================================
-- 12. CDQM, RECONCILIATION, AUDIT, FAILURE HANDLING
-- ============================================================================
CREATE TABLE IF NOT EXISTS pipeline_run_audit (
 run_id TEXT PRIMARY KEY,
 pipeline_name TEXT NOT NULL,
 batch_id TEXT NOT NULL,
 started_at TIMESTAMPTZ NOT NULL,
 completed_at TIMESTAMPTZ,
 status TEXT NOT NULL CHECK(status IN('running','succeeded','failed','quarantined')),
 source_row_count BIGINT,
 bronze_row_count BIGINT,
 silver_valid_count BIGINT,
 silver_quarantine_count BIGINT,
 gold_row_count BIGINT,
 error_message TEXT
);
CREATE TABLE IF NOT EXISTS dq_rule_results (
 run_id TEXT NOT NULL,
 rule_name TEXT NOT NULL,
 passed BOOLEAN NOT NULL,
 observed_count BIGINT,
 expected_condition TEXT NOT NULL,
 evaluated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
 PRIMARY KEY(run_id,rule_name)
);

-- Example CDQM checks: duplicate events, invalid statuses, SCD2 current-row uniqueness,
-- missing dimensions, future timestamps, impossible amounts, source-target reconciliation.
INSERT INTO dq_rule_results(run_id,rule_name,passed,observed_count,expected_condition)
SELECT 'run_demo_001','duplicate_source_event_id',COUNT(*)=0,COUNT(*),'count must equal zero'
FROM (SELECT source_event_id FROM bronze_customer_events GROUP BY source_event_id HAVING COUNT(*)>1) d
ON CONFLICT(run_id,rule_name) DO UPDATE SET passed=EXCLUDED.passed,
 observed_count=EXCLUDED.observed_count,evaluated_at=CURRENT_TIMESTAMP;

-- Reconciliation: source event IDs missing from Bronze.
SELECT s.source_event_id FROM source_customer_events s
LEFT JOIN bronze_customer_events b USING(source_event_id)
WHERE b.source_event_id IS NULL;
-- Reconciliation: Bronze events missing from Silver.
SELECT b.source_event_id FROM bronze_customer_events b
LEFT JOIN silver_customer_events s USING(source_event_id)
WHERE s.source_event_id IS NULL;
-- SCD2 integrity: one current version and no invalid ranges.
SELECT customer_id FROM dim_customer_type2 GROUP BY customer_id
HAVING COUNT(*) FILTER(WHERE is_current) <> 1;
SELECT customer_sk FROM dim_customer_type2 WHERE valid_from >= valid_to;

-- Production reliability checklist:
-- * Separate transient failures (retry) from deterministic data errors (quarantine).
-- * Log run_id, batch_id, source offsets, counts, timings, and failure reason.
-- * Alert on stale watermarks, sudden volume changes, DQ failure rates, and pipeline lag.
-- * Use bounded retries with backoff; make writes safe to replay.
-- * Never advance watermarks after partial failure.
-- * Keep secrets outside SQL files; use managed identity/secret stores.
-- * Version schema changes and communicate breaking contract changes.

-- ============================================================================
-- 13. AMAZON REDSHIFT ADAPTATION NOTES
-- ============================================================================
-- Redshift supports dimensional modelling and SCD patterns, but DDL, procedural
-- language, transaction behavior, and operational tuning differ from PostgreSQL.
-- Validate the exact feature set and syntax against the target Redshift version.
--
-- Redshift table design example (illustrative):
CREATE TABLE analytics.dim_customer_scd2 (
  customer_sk BIGINT IDENTITY(1,1),
  customer_id BIGINT NOT NULL,
  email VARCHAR(320), city VARCHAR(200), segment VARCHAR(100),
  source_version BIGINT NOT NULL,
  valid_from TIMESTAMP NOT NULL,
  valid_to TIMESTAMP NOT NULL,
  is_current BOOLEAN NOT NULL,
  attribute_hash VARCHAR(64)
) DISTSTYLE KEY DISTKEY(customer_id) SORTKEY(customer_id, valid_from);
--
-- Redshift SCD2 workflow (conceptual SQL sequence):
-- 1. COPY source extract from S3 into a staging table.
-- 2. Validate and deduplicate staging to one event per key/version.
-- 3. Create a changed-customer set by comparing source attributes/hash to current dimension.
-- 4. UPDATE current dimension rows to set valid_to = source effective timestamp and is_current = false.
-- 5. INSERT new version rows for changed/new customers.
-- 6. MERGE/UPDATE current-state Type 1 attributes where the business contract requires it.
-- 7. Reconcile counts, validate current rows and effective intervals, then record success.
-- 8. Commit watermark only after the load is successful.
--
-- Redshift MERGE example for Type 1 current dimension (source deduplicated first):
MERGE INTO analytics.dim_customer_type1 AS tgt
USING staging.customer_current_dedup AS src
  ON tgt.customer_id = src.customer_id
WHEN MATCHED AND src.source_version >= tgt.source_version THEN UPDATE SET
  email=src.email, city=src.city, segment=src.segment,
  source_version=src.source_version, updated_at=src.source_updated_at
WHEN NOT MATCHED THEN INSERT(customer_id,email,city,segment,source_version,updated_at)
VALUES(src.customer_id,src.email,src.city,src.segment,src.source_version,src.source_updated_at);
--
-- Performance review:
-- * Choose distribution to reduce expensive redistribution for common joins.
-- * Choose sort keys for frequent date filters and join/order patterns.
-- * Review EXPLAIN output for scans, redistribution/broadcast, joins, and estimates.
-- * Inspect table statistics and skew; keep statistics and physical layout current.
-- * Bulk-load using COPY from S3; avoid row-by-row ingestion for large batches.
-- * Do not create PostgreSQL-style indexes in Redshift; they are not its tuning model.

-- ============================================================================
-- 14. FINAL REVIEW CHECKLIST — INTERVIEW-READY EXPLANATIONS
-- ============================================================================
-- SCD0: preserve the first value; later changes are ignored for that attribute.
-- SCD1: overwrite; simple current truth, no attribute history.
-- SCD2: new row per tracked change; effective dates support as-of reporting.
-- SCD3: current plus one/few previous values in columns; limited history.
-- SCD4: current dimension plus separate history table.
-- SCD6: hybrid of Types 1, 2, and 3; document the exact implementation convention.
--
-- Bronze: raw, replayable, auditable source landing.
-- Silver: typed, standardized, deduplicated, validated, and quarantined where invalid.
-- Gold: business-defined facts, dimensions, and data marts with stable grain/contracts.
--
-- Watermark: progress boundary, often timestamp or source sequence.
-- Lookback: overlap to catch late-arriving changes; requires deterministic dedupe.
-- CDC: incremental change stream including updates/deletes, not merely new rows.
-- Idempotency: reruns do not create duplicate or divergent intended state.
-- CDQM: continuous checks, thresholds, history, alerting, and remediation ownership.
--
-- Before an interview, be able to explain for each design:
-- 1. Grain and business purpose.
-- 2. Source contract and primary/natural keys.
-- 3. Change detection and null-safe comparison.
-- 4. Late-arriving and out-of-order event behavior.
-- 5. Duplicate prevention and idempotent reruns.
-- 6. Historical query strategy and effective-date semantics.
-- 7. Data-quality controls and reconciliation.
-- 8. PostgreSQL vs Redshift implementation differences.
-- 9. Performance trade-offs and how you would measure them.
-- 10. Recovery behavior after a partial failure.
-- ============================================================================
