
Loading older messages…












































































ON CONFLICT (pipeline_name, source_name) DO NOTHING;
-- Typical extraction: use overlap/lookback for late-arriving updates; dedupe on stable key.
SELECT * FROM orders
WHERE updated_at >= (
  SELECT watermark_ts - INTERVAL '2 hours' FROM etl_watermarks
  WHERE pipeline_name = 'orders_incremental' AND source_name = 'orders_source'
)
ORDER BY updated_at, order_id;
-- Advance the watermark only after destination writes and quality checks succeed.
-- A production orchestration layer should commit destination and watermark atomically where possible.

-- [B04] Idempotent upsert example (PostgreSQL).
CREATE TABLE IF NOT EXISTS customer_status_snapshot (
  customer_id BIGINT PRIMARY KEY,
  email TEXT NOT NULL,
  is_active BOOLEAN NOT NULL,
  source_updated_at TIMESTAMPTZ NOT NULL,
  loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
INSERT INTO customer_status_snapshot(customer_id,email,is_active,source_updated_at)
SELECT customer_id,email,is_active,updated_at FROM customers
ON CONFLICT(customer_id) DO UPDATE SET
  email = EXCLUDED.email,
  is_active = EXCLUDED.is_active,
  source_updated_at = EXCLUDED.source_updated_at,
  loaded_at = CURRENT_TIMESTAMP
WHERE EXCLUDED.source_updated_at >= customer_status_snapshot.source_updated_at;

-- [B05] Change data capture (CDC) design checklist:
-- 1. Preserve source event/LSN/sequence and operation type (I/U/D).
-- 2. Apply events in deterministic order per entity key.
-- 3. Deduplicate by source event id or (key, sequence).
-- 4. Handle deletes explicitly (tombstone or soft-delete flag).
-- 5. Make retries idempotent and track source offsets.
-- 6. Monitor lag, missing sequence ranges, duplicates, and replay failures.

-- [B06] PostgreSQL function: immutable calculation, with explicit null/zero behavior.
CREATE OR REPLACE FUNCTION sql_lab.safe_divide(numerator NUMERIC, denominator NUMERIC)
RETURNS NUMERIC
LANGUAGE SQL
IMMUTABLE
PARALLEL SAFE
AS $$ SELECT numerator / NULLIF(denominator, 0) $$;
SELECT sql_lab.safe_divide(10,2) AS ratio, sql_lab.safe_divide(10,0) AS zero_denominator_result;

-- [B07] PostgreSQL stored procedure: transactional work belongs in a procedure
-- when the task is an operation, not merely a value-returning expression.
CREATE OR REPLACE PROCEDURE sql_lab.refresh_customer_status_snapshot()
LANGUAGE plpgsql
AS $$
BEGIN
  INSERT INTO sql_lab.customer_status_snapshot(customer_id,email,is_active,source_updated_at)
  SELECT customer_id,email,is_active,updated_at FROM sql_lab.customers
  ON CONFLICT(customer_id) DO UPDATE SET
    email = EXCLUDED.email,
    is_active = EXCLUDED.is_active,
    source_updated_at = EXCLUDED.source_updated_at,
    loaded_at = CURRENT_TIMESTAMP
  WHERE EXCLUDED.source_updated_at >= sql_lab.customer_status_snapshot.source_updated_at;
END;
$$;
CALL sql_lab.refresh_customer_status_snapshot();

-- [B08] Indexing: create indexes for observed access patterns, not every column.
CREATE INDEX IF NOT EXISTS ix_orders_customer_ts ON orders(customer_id, order_ts DESC);
CREATE INDEX IF NOT EXISTS ix_orders_status_ts ON orders(order_status, order_ts);
CREATE INDEX IF NOT EXISTS ix_web_events_user_ts ON web_events(user_id, event_ts DESC);
-- PostgreSQL only: partial index for a frequent, selective predicate.
CREATE INDEX IF NOT EXISTS ix_active_customers_email ON customers(email) WHERE is_active IS TRUE;
-- Inspect plan and actual execution in PostgreSQL:
EXPLAIN (ANALYZE, BUFFERS, VERBOSE) SELECT * FROM orders WHERE customer_id = 1 ORDER BY order_ts DESC LIMIT 20;

-- ============================================================================
-- EXECUTABLE LAB APPENDIX C — REDSHIFT REFERENCE PATTERNS
-- ============================================================================
-- Run only in Amazon Redshift. PostgreSQL syntax is not always interchangeable.

-- [C01] Redshift warehouse design considerations:
-- * Prefer columnar-friendly analytic scans and select only needed columns.
-- * Choose DISTSTYLE/DISTKEY based on joins, cardinality, skew, and data volume.
-- * Choose SORTKEY based on common filters, joins, and time-range pruning.
-- * Do not blindly DISTKEY every foreign key; evaluate query plans and skew.
-- * Redshift is not PostgreSQL: indexes are not used for ordinary table access.
-- * Use COPY for bulk loading from S3; prefer columnar/compressed formats when suitable.

-- Example Redshift table definition (illustrative; tune for workload):
CREATE TABLE analytics.fact_orders (
  order_id BIGINT NOT NULL,
  customer_id BIGINT NOT NULL,
  order_ts TIMESTAMP NOT NULL,
  order_status VARCHAR(30) NOT NULL,
  order_total DECIMAL(14,2) NOT NULL
) DISTSTYLE KEY DISTKEY(customer_id) SORTKEY(order_ts, customer_id);

-- [C02] Redshift MERGE pattern. Ensure source has at most one row per target key.
MERGE INTO analytics.dim_customer AS tgt
USING staging.customer_changes AS src
  ON tgt.customer_id = src.customer_id
WHEN MATCHED THEN UPDATE SET
  email = src.email,
  city = src.city,
  updated_at = src.updated_at
WHEN NOT MATCHED THEN INSERT (customer_id, email, city, updated_at)
VALUES (src.customer_id, src.email, src.city, src.updated_at);

-- [C03] Redshift EXPLAIN returns the planned execution steps; inspect distribution,
-- redistribution/broadcast, scans, joins, sort operations, and estimated row counts.
EXPLAIN SELECT customer_id, SUM(order_total)
FROM analytics.fact_orders
WHERE order_ts >= DATE '2026-10-01'
GROUP BY customer_id;

-- [C04] Practical tuning sequence:
-- 1. State expected grain and output row count.
-- 2. Validate joins do not multiply facts.
-- 3. Filter early where semantically valid; project only needed columns.
-- 4. Compare estimated and observed row counts where runtime telemetry is available.
-- 5. Check distribution skew, network movement, sort/spill behavior, and scan volume.
-- 6. Measure before/after with representative parameters and data volumes.
-- 7. Record the change and regression-test critical metrics.

-- ============================================================================
-- INTERVIEW QUICK RECALL — DEFINITIONS THAT MUST BE PRECISE
-- ============================================================================
-- CRUD: Create, Read, Update, Delete (operations on records).
-- DDL: defines database objects; DML: changes rows; DQL: queries rows;
-- DCL: permissions; TCL: transaction control. Classification varies by convention.
-- ACID: Atomicity, Consistency, Isolation, Durability.
-- ETL: transform before loading target; ELT: load raw data, transform in destination.
-- CDC: capture inserts/updates/deletes from source change stream/log.
-- Watermark: progress marker for source extraction; not a substitute for deduplication.
-- Idempotency: replaying the same operation yields the same intended final state.
-- SCD1: overwrite current attribute; SCD2: preserve versions with effective intervals;
-- SCD3: retain limited prior value; SCD4: current dimension plus history table;
-- SCD6: hybrid Type 1 + Type 2 + Type 3 behavior, implementation varies by convention.
-- Grain: what one row represents. Declare it before designing facts and joins.
-- Fact table: measurable business events/snapshots; dimension: descriptive context.
-- CTE: named query expression; window function: computes across related rows without collapsing them.
-- Indexes: PostgreSQL access structures; Redshift uses columnar storage, sort/distribution design,
-- compression and query execution planning rather than traditional row-store indexes.
-- Query plan: execution strategy chosen by optimizer; inspect data movement and cardinality.
-- Conversion rate: conversions / eligible randomized units; denominator definition is essential.
-- Relative lift: (treatment rate - control rate) / control rate; undefined if control is zero.
-- Period comparison: align complete comparable periods and account for missing dates.
-- ============================================================================

