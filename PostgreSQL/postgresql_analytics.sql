-- PostgreSQL Analytics Laboratory
-- Run sections independently; sample data is illustrative.

-- 01. Casting, NULLs, and safe arithmetic
WITH raw_orders(order_id, raw_amount) AS (
  VALUES (1,'125.50'),(2,'invalid'),(3,NULL),(4,'0')
)
SELECT order_id, raw_amount,
       CASE WHEN raw_amount ~ '^-?[0-9]+(\.[0-9]+)?$'
            THEN raw_amount::numeric(12,2) END AS amount
FROM raw_orders;

SELECT 10::numeric / NULLIF(0,0) AS safe_division,
       COALESCE(NULL::numeric,0) AS null_as_zero;

-- 02. WHERE filters rows; HAVING filters groups
WITH orders(customer_id,amount,status) AS (
  VALUES (1,100::numeric,'complete'),(1,50,'complete'),
         (2,300,'pending'),(2,75,'complete'),(3,20,'complete')
)
SELECT customer_id, COUNT(*) AS orders, SUM(amount) AS revenue,
       AVG(amount) AS avg_order_value
FROM orders WHERE status='complete'
GROUP BY customer_id HAVING SUM(amount)>=100
ORDER BY revenue DESC;

-- 03. Aggregate before joining when the target grain is one row/customer
WITH customers AS (
  SELECT * FROM (VALUES (1,'Asha'),(2,'Ravi'),(3,'Mina')) v(customer_id,name)
), orders AS (
  SELECT * FROM (VALUES (101,1,100::numeric),(102,1,80),(103,2,50))
    v(order_id,customer_id,amount)
), totals AS (
  SELECT customer_id,COUNT(*) AS order_count,SUM(amount) AS revenue
  FROM orders GROUP BY customer_id
)
SELECT c.customer_id,c.name,COALESCE(t.order_count,0) AS order_count,
       COALESCE(t.revenue,0) AS revenue
FROM customers c LEFT JOIN totals t USING(customer_id)
ORDER BY c.customer_id;

-- 04. Deduplicate to the latest source row
WITH events(customer_id,status,updated_at,event_id) AS (
  VALUES (1,'new',TIMESTAMP '2026-01-01 09:00',1),
         (1,'active',TIMESTAMP '2026-01-02 09:00',2),
         (2,'new',TIMESTAMP '2026-01-01 12:00',3)
), ranked AS (
  SELECT *,ROW_NUMBER() OVER(PARTITION BY customer_id
    ORDER BY updated_at DESC,event_id DESC) AS rn FROM events
)
SELECT customer_id,status,updated_at FROM ranked WHERE rn=1;

-- 05. Window functions: rank, running totals, lag, and share
WITH sales(employee_id,team,sale_date,revenue) AS (
  VALUES (1,'A',DATE '2026-01-01',100::numeric),
         (2,'A',DATE '2026-01-01',150),(1,'A',DATE '2026-01-02',80),
         (3,'B',DATE '2026-01-01',120)
)
SELECT *,
 RANK() OVER(PARTITION BY team ORDER BY revenue DESC) AS team_rank,
 SUM(revenue) OVER(PARTITION BY employee_id ORDER BY sale_date
   ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_revenue,
 LAG(revenue) OVER(PARTITION BY employee_id ORDER BY sale_date) AS prior_revenue,
 revenue/NULLIF(SUM(revenue) OVER(PARTITION BY team),0) AS team_share
FROM sales ORDER BY team,sale_date,employee_id;

-- 06. Rolling window
WITH daily(sale_date,revenue) AS (
 VALUES (DATE '2026-01-01',100::numeric),(DATE '2026-01-02',130),
        (DATE '2026-01-03',90),(DATE '2026-01-04',160)
)
SELECT *,AVG(revenue) OVER(ORDER BY sale_date
 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS rolling_3_row_avg
FROM daily ORDER BY sale_date;

-- 07. Recursive CTE: sequence
WITH RECURSIVE numbers(n) AS (
 SELECT 1 UNION ALL SELECT n+1 FROM numbers WHERE n<10
)
SELECT n FROM numbers ORDER BY n;

-- 08. Recursive CTE: hierarchy traversal with path/cycle guard
WITH RECURSIVE employees(employee_id,manager_id,employee_name) AS (
 VALUES (1,NULL::integer,'CEO'),(2,1,'Director'),(3,1,'Finance'),
        (4,2,'Analyst'),(5,2,'Engineer')
), tree AS (
 SELECT employee_id,manager_id,employee_name,0 AS depth,ARRAY[employee_id] AS path
 FROM employees WHERE manager_id IS NULL
 UNION ALL
 SELECT e.employee_id,e.manager_id,e.employee_name,t.depth+1,t.path||e.employee_id
 FROM employees e JOIN tree t ON e.manager_id=t.employee_id
 WHERE NOT e.employee_id=ANY(t.path) AND t.depth<20
)
SELECT * FROM tree ORDER BY path;

-- 09. Data quality checks: replace stg.orders with your source
SELECT order_id,COUNT(*) AS duplicate_count
FROM stg.orders GROUP BY order_id HAVING COUNT(*)>1;
SELECT COUNT(*) AS missing_keys FROM stg.orders WHERE order_id IS NULL;
SELECT status,COUNT(*) FROM stg.orders
WHERE status NOT IN ('new','processing','complete','cancelled') OR status IS NULL
GROUP BY status;

-- 10. Schema and audit templates
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS audit;
CREATE TABLE IF NOT EXISTS core.orders_example(
 order_id bigint PRIMARY KEY, customer_id bigint NOT NULL,
 order_amount numeric(12,2) NOT NULL CHECK(order_amount>=0),
 status text NOT NULL, created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS audit.data_quality_results(
 check_name text NOT NULL, checked_at timestamptz NOT NULL DEFAULT now(),
 failed_rows bigint NOT NULL, details jsonb
);

-- 11. Surrogate key example
CREATE TABLE IF NOT EXISTS core.dim_product_example(
 product_sk bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 product_id text NOT NULL, product_name text NOT NULL,
 valid_from timestamptz NOT NULL DEFAULT now()
);

-- 12. SCD patterns: Type 0 preserve; Type 1 overwrite; Type 2 version history;
-- Type 3 prior-value column; Type 4 separate current/history tables;
-- Type 6 combines Type 1, Type 2, and Type 3.
CREATE TABLE IF NOT EXISTS core.dim_customer_type2(
 customer_sk bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 customer_id text NOT NULL, customer_name text NOT NULL, segment text,
 valid_from timestamptz NOT NULL, valid_to timestamptz,
 is_current boolean NOT NULL DEFAULT true,
 CHECK(valid_to IS NULL OR valid_to>valid_from)
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_customer_type2_current
 ON core.dim_customer_type2(customer_id) WHERE is_current;
-- Type 1 upsert outline; source must be deduplicated and target key unique:
-- INSERT INTO core.customer_current(customer_id,customer_name)
-- SELECT customer_id,customer_name FROM stg.customer_deduplicated
-- ON CONFLICT(customer_id) DO UPDATE SET customer_name=EXCLUDED.customer_name;
-- Type 2 batch: expire changed current row, then insert new/current version
-- within one transaction. Define late-arriving and same-timestamp policies.
SELECT customer_id,COUNT(*) AS current_rows FROM core.dim_customer_type2
WHERE is_current GROUP BY customer_id HAVING COUNT(*)<>1;

-- 13. Watermarks and pipeline run audit
CREATE TABLE IF NOT EXISTS audit.pipeline_watermarks(
 pipeline_name text PRIMARY KEY,last_successful_watermark timestamptz NOT NULL
);
CREATE TABLE IF NOT EXISTS audit.pipeline_runs(
 run_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 pipeline_name text NOT NULL,started_at timestamptz NOT NULL DEFAULT now(),
 completed_at timestamptz,
 status text NOT NULL CHECK(status IN ('running','succeeded','failed')),
 rows_loaded bigint
);
-- Use half-open windows [start,end), and advance only after reconciliation:
-- SELECT * FROM src.events WHERE updated_at>=:previous_watermark
-- AND updated_at<:batch_end;
-- Lookback windows can catch late data; target writes must be idempotent.

-- 14. Transaction safety: preview and validate before commit
BEGIN;
-- SELECT * FROM core.orders_example WHERE status='pending';
-- UPDATE core.orders_example SET status='processing' WHERE status='pending';
ROLLBACK;

-- 15. EXPLAIN and performance
EXPLAIN SELECT customer_id,SUM(order_amount)
FROM core.orders_example GROUP BY customer_id;
-- Executes the query and reports actual timing/buffers:
-- EXPLAIN (ANALYZE,BUFFERS) SELECT customer_id,SUM(order_amount)
-- FROM core.orders_example GROUP BY customer_id;
-- ANALYZE core.orders_example; -- refresh planner statistics after major changes
-- Reminders: use NUMERIC for money, NULLIF for division, deterministic tie-breakers,
-- and indexes based on measured plans/write costs.
