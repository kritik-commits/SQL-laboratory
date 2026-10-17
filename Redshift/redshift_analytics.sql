-- Amazon Redshift Analytics Laboratory
-- Adapt schema names, data types, batch boundaries, and workload assumptions.

-- 01. Casting, NULL handling, aggregation
SELECT TRY_CAST(raw_amount AS DECIMAL(18,2)) AS amount,
       COALESCE(status,'unknown') AS normalized_status
FROM stg.orders;

SELECT customer_id,COUNT(*) AS order_count,SUM(order_amount) AS revenue,
       SUM(order_amount)/NULLIF(COUNT(*),0) AS avg_order_value
FROM mart.fact_orders
WHERE order_date>=DATE '2026-01-01'
GROUP BY customer_id HAVING SUM(order_amount)>1000;

-- 02. Latest row per business key
SELECT customer_id,customer_name,customer_segment,source_updated_at
FROM (
 SELECT s.*,ROW_NUMBER() OVER(PARTITION BY customer_id
   ORDER BY source_updated_at DESC,ingestion_id DESC) AS rn
 FROM stg.customer_changes s
) ranked WHERE rn=1;

-- 03. Target table and staging pattern for MERGE
CREATE TABLE IF NOT EXISTS mart.customer_current(
 customer_id BIGINT NOT NULL,customer_name VARCHAR(256),
 customer_segment VARCHAR(100),source_updated_at TIMESTAMP
) DISTSTYLE AUTO SORTKEY(customer_id);

CREATE TEMP TABLE stage_customer_changes(
 customer_id BIGINT,customer_name VARCHAR(256),
 customer_segment VARCHAR(100),source_updated_at TIMESTAMP,ingestion_id BIGINT
);
-- Load the batch into stage_customer_changes before continuing.
CREATE TEMP TABLE stage_customer_one_row AS
SELECT customer_id,customer_name,customer_segment,source_updated_at
FROM (
 SELECT s.*,ROW_NUMBER() OVER(PARTITION BY customer_id
   ORDER BY source_updated_at DESC,ingestion_id DESC) AS rn
 FROM stage_customer_changes s
) x WHERE rn=1;

-- Validate one source row per key before MERGE:
SELECT customer_id,COUNT(*) FROM stage_customer_one_row
GROUP BY customer_id HAVING COUNT(*)>1;

-- MERGE INTO mart.customer_current AS tgt
-- USING stage_customer_one_row AS src
-- ON tgt.customer_id=src.customer_id
-- WHEN MATCHED THEN UPDATE SET
--   customer_name=src.customer_name,
--   customer_segment=src.customer_segment,
--   source_updated_at=src.source_updated_at
-- WHEN NOT MATCHED THEN INSERT
--   (customer_id,customer_name,customer_segment,source_updated_at)
-- VALUES
--   (src.customer_id,src.customer_name,src.customer_segment,src.source_updated_at);

-- 04. Data quality: uniqueness, nulls, freshness
-- Redshift PK/UNIQUE/FK declarations are informational, not enforced.
SELECT customer_id,COUNT(*) AS row_count
FROM stage_customer_changes GROUP BY customer_id HAVING COUNT(*)>1;
SELECT COUNT(*) AS missing_keys FROM stage_customer_changes WHERE customer_id IS NULL;
SELECT MAX(source_updated_at) AS newest_source_record FROM stage_customer_changes;
-- Reconcile count and totals using identical batch filters:
-- SELECT COUNT(*),SUM(order_amount) FROM mart.fact_orders WHERE batch_id=:batch_id;

-- 05. Physical design: distribution and sort keys
-- DISTSTYLE AUTO is a useful baseline. Select DISTKEY/SORTKEY from measured workload.
CREATE TABLE IF NOT EXISTS mart.fact_orders_example(
 order_id BIGINT NOT NULL,customer_id BIGINT NOT NULL,
 order_date DATE NOT NULL,order_amount DECIMAL(18,2) NOT NULL,
 status VARCHAR(40)
) DISTSTYLE AUTO SORTKEY(order_date,customer_id);

SELECT customer_id,SUM(order_amount) AS revenue
FROM mart.fact_orders_example
WHERE order_date>=DATE '2026-01-01'
GROUP BY customer_id;

-- 06. Query plan and table diagnostics
EXPLAIN SELECT customer_id,SUM(order_amount)
FROM mart.fact_orders_example
WHERE order_date>=DATE '2026-01-01' GROUP BY customer_id;
-- Inspect metadata; available columns/permissions may vary:
-- SELECT "schema","table",diststyle,sortkey1,unsorted,stats_off,skew_rows
-- FROM svv_table_info WHERE "schema"='mart';

-- 07. Incremental load and watermark process
-- Use a stable half-open window [start,end):
-- SELECT * FROM src.events
-- WHERE updated_at>=:previous_watermark AND updated_at<:batch_end;
-- Production sequence:
-- 1. Capture batch boundaries/run ID.
-- 2. Load staging rows for that boundary.
-- 3. Deduplicate and validate keys, nulls, domains, and freshness.
-- 4. MERGE or insert idempotently into target.
-- 5. Reconcile row counts/totals and record outcome.
-- 6. Advance watermark only after successful reconciliation.

-- 08. SCD types
-- Type 0 preserve original; Type 1 overwrite; Type 2 versioned history;
-- Type 3 prior-value column; Type 4 separate current/history tables;
-- Type 6 combines Types 1, 2, and 3.
-- Validate current-row uniqueness explicitly because Redshift constraints are informational.

-- 09. Operational checklist
-- Filter early; project only needed columns; avoid SELECT * in production.
-- Review scan volume, redistribution, skew, sort, and spill indicators.
-- Use ANALYZE/VACUUM when appropriate to table state and workload.
-- Benchmark plans and runtime before/after physical design changes.
-- Keep batch logic repeatable; validate data before advancing watermarks.
