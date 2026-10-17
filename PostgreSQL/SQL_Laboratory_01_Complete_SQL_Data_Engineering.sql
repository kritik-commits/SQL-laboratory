-- ============================================================================
-- SQL LABORATORY — THE COMPLETE SQL + DATA ENGINEERING LEARNING BIBLE
-- File 1 of 2: SQL foundations, analytics, modelling, quality, pipelines,
-- PostgreSQL programming, Redshift engineering, and interview revision.
--
-- DIALECT POLICY
--   * Core examples use PostgreSQL unless explicitly labelled Redshift.
--   * This file is a reference workbook: read the explanatory comments first.
--   * Execute code blocks individually, in order, in a disposable practice DB.
--   * Some sections intentionally show alternatives; do not execute all variants.
--   * Never run destructive examples against production data.
--
-- CONTENTS ARE PRESERVED FROM THE COMPREHENSIVE SOURCE AND FOLLOWED BY
-- EXECUTABLE LAB EXAMPLES. Conceptual explanations remain SQL comments;
-- SQL examples are active statements for syntax highlighting and selective execution.
-- ============================================================================

-- # PostgreSQL + Amazon Redshift Analytics Engineering Cheat Sheet
--
-- **Beginner → Advanced SQL → ETL/ELT → Data Warehousing → Performance**
--
-- A practical reference for analytical SQL, data engineering, data
-- quality, dimensional modelling, and warehouse optimization.
--
-- ------------------------------------------------------------------------
--
-- ## 0. The mental model
--
-- -   **Database**: organized collection of data.
-- -   **Schema**: namespace grouping tables, views, functions, and other
--     objects.
-- -   **Table grain**: what one row represents. Define this before joining
--     or aggregating.
-- -   **Primary key (PK)**: uniquely identifies each row; cannot be
--     `NULL`.
-- -   **Foreign key (FK)**: enforces a relationship to a referenced key.
-- -   **Natural key**: business identifier, such as an external customer
--     number.
-- -   **Surrogate key (SK)**: artificial identifier, often generated for a
--     warehouse dimension.
-- -   **Fact table**: measurable events at a declared grain.
-- -   **Dimension table**: descriptive context used to slice facts.
-- -   **OLTP**: transaction processing; many small writes and point
--     lookups.
-- -   **OLAP**: analytics; scans, aggregations, joins, and large reads.
-- -   **PostgreSQL**: general-purpose relational database with strong
--     transactional features.
-- -   **Redshift**: distributed, columnar cloud data warehouse optimized
--     for analytics.
--
-- **Rule:** Before writing SQL, answer: What is one row? What does the
-- metric mean? Can joins duplicate it?
--
-- ------------------------------------------------------------------------
--
-- ## 1. SQL command families and CRUD
--
-- ### DDL --- Data Definition Language
--
-- Defines or changes database structures.
--

CREATE SCHEMA IF NOT EXISTS stg;
CREATE TABLE stg.orders (...);
ALTER TABLE stg.orders ADD COLUMN source_system text;
CREATE INDEX idx_orders_customer ON stg.orders(customer_id);
TRUNCATE TABLE stg.orders;
DROP TABLE stg.orders;

--
-- -   `CREATE`: create an object.
-- -   `ALTER`: change its structure.
-- -   `TRUNCATE`: remove all rows efficiently; table remains.
-- -   `DROP`: remove the object and its data.
-- -   `CREATE OR REPLACE VIEW`: replace a view definition.
-- -   `IF EXISTS` / `IF NOT EXISTS`: protect against some rerun errors.
--
-- ### DML --- Data Manipulation Language
--
-- Reads or changes table data.
--

SELECT * FROM src.orders;
INSERT INTO stg.orders (order_id, status) VALUES (10, 'new');
UPDATE stg.orders SET status = 'complete' WHERE order_id = 10;
DELETE FROM stg.orders WHERE order_id = 10;

--
-- -   **CRUD**: Create=`INSERT`, Read=`SELECT`, Update=`UPDATE`,
--     Delete=`DELETE`.
-- -   Always preview the target rows before `UPDATE` or `DELETE`.
-- -   An omitted `WHERE` updates or deletes every row.
--
-- ### DCL --- Data Control Language
--
-- Controls permissions.
--

GRANT SELECT ON mart.sales TO analyst_role;
REVOKE INSERT ON stg.orders FROM analyst_role;

--
-- ### TCL --- Transaction Control Language
--

BEGIN;
UPDATE core.orders SET status = 'complete' WHERE order_id = 10;
SAVEPOINT before_second_change;
UPDATE core.orders SET status = 'cancelled' WHERE order_id = 11;
ROLLBACK TO SAVEPOINT before_second_change;
COMMIT;
-- ROLLBACK; cancels uncommitted work instead of committing it.

--
-- -   `BEGIN`: start a transaction.
-- -   `COMMIT`: make transaction changes durable.
-- -   `ROLLBACK`: cancel uncommitted changes.
-- -   `SAVEPOINT`: create a partial rollback point.
-- -   PostgreSQL supports transactional DDL in many cases.
-- -   Do not assume every external operation participates in a database
--     transaction.
--
-- ### PostgreSQL vs Redshift
--
-- -   Both support common SQL DDL/DML concepts, but syntax and
--     capabilities differ.
-- -   Redshift has warehouse-specific restrictions; validate statements
--     against Redshift documentation.
-- -   Redshift `TRUNCATE` has transaction behavior different from
--     PostgreSQL; do not assume identical rollback semantics.
-- -   Redshift does not provide PostgreSQL-style enforced PK/FK/UNIQUE
--     constraints. Declare them only when true; Redshift may use them as
--     optimizer hints, so incorrect declarations can cause incorrect
--     results.
--
-- ------------------------------------------------------------------------
--
-- ## 2. Build a safe practice schema
--

CREATE SCHEMA IF NOT EXISTS practice;

CREATE TABLE practice.customers (
    customer_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_name text NOT NULL,
    email text,
    created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE practice.orders (
    order_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id bigint NOT NULL REFERENCES practice.customers(customer_id),
    order_ts timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    order_amount numeric(12,2) NOT NULL CHECK (order_amount >= 0),
    status text NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'completed', 'cancelled'))
);

--
-- ### Constraints
--
-- -   `PRIMARY KEY`: unique and non-null row identity.
-- -   `FOREIGN KEY`: referenced value must exist.
-- -   `UNIQUE`: prevents duplicate non-null key combinations; NULL
--     behavior depends on database and constraint options.
-- -   `NOT NULL`: value required.
-- -   `CHECK`: value must satisfy a condition.
-- -   `DEFAULT`: supplies a value when the insert omits it.
-- -   Constraints protect data at write time; validation queries detect
--     wider quality problems.
--
-- ### Identity, serial, auto-increment
--

id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY
id bigint GENERATED BY DEFAULT AS IDENTITY
id serial PRIMARY KEY

--
-- -   Identity columns are generally preferred for new PostgreSQL designs.
-- -   `GENERATED ALWAYS` restricts explicit values unless overriding
--     syntax is used.
-- -   `GENERATED BY DEFAULT` allows explicit values.
-- -   `SERIAL` is legacy shorthand involving an integer column and
--     sequence.
-- -   IDs can have gaps after rollbacks or failed inserts. Do not expect
--     gap-free IDs.
-- -   Redshift identity generation is not a substitute for a globally
--     unique distributed key in every loading pattern.
--
-- ------------------------------------------------------------------------
--
-- ## 3. Data types and casting
--
-- ### Common types
--
-- -   `smallint`, `integer`, `bigint`: whole numbers.
-- -   `numeric(p,s)` / `decimal(p,s)`: exact decimal values, preferred for
--     money.
-- -   `real`, `double precision`: approximate floating-point values.
-- -   `text`, `varchar(n)`, `char(n)`: strings.
-- -   `boolean`: `TRUE`, `FALSE`, or `NULL`.
-- -   `date`: calendar date.
-- -   `time`: time of day.
-- -   `timestamp`: date and time without time zone.
-- -   `timestamptz`: instant stored internally in normalized form;
--     displayed in session time zone.
-- -   `interval`: duration or calendar interval.
-- -   `uuid`: UUID identifier.
-- -   `json` / `jsonb`: JSON data; PostgreSQL `jsonb` supports indexing
--     and operators.
-- -   `text[]`: array of text values.
--
-- ### Casts
--

SELECT '42'::integer;
SELECT CAST('2026-10-17' AS date);
SELECT 10::numeric / 3;
SELECT NULLIF(denominator, 0);

--
-- -   `::type` is PostgreSQL cast syntax.
-- -   Casts can fail if values are malformed or out of range.
-- -   Cast before division when integer division would truncate.
-- -   Use `NULLIF(x, 0)` to avoid division by zero.
-- -   Use `numeric` for exact monetary calculations; floating-point values
--     can introduce rounding artifacts.
--
-- ------------------------------------------------------------------------
--
-- ## 4. NULL handling and conditional logic
--

COALESCE(email, 'missing')              -- first non-NULL expression
NULLIF(trimmed_value, '')               -- turns empty string into NULL
CASE WHEN revenue >= 1000 THEN 'high'
     ELSE 'low' END
value IS NULL
value IS NOT NULL
a IS DISTINCT FROM b                    -- NULL-safe inequality
a IS NOT DISTINCT FROM b                -- NULL-safe equality

--
-- -   `NULL` means unknown or absent; it is not zero or an empty string.
-- -   `x = NULL` is not the correct test; use `x IS NULL`.
-- -   `COUNT(*)` counts rows.
-- -   `COUNT(column)` counts non-NULL values.
-- -   `COUNT(DISTINCT column)` counts distinct non-NULL values.
-- -   `SUM()` and `AVG()` ignore NULL inputs.
-- -   `COALESCE(SUM(x), 0)` returns zero when the aggregate result is
--     NULL.
-- -   `NOT IN` can behave unexpectedly if the subquery returns NULL.
--     `NOT EXISTS` is often safer.
--
-- ------------------------------------------------------------------------
--
-- ## 5. Text and string cleaning
--
-- ### Core functions
--

trim(name)
btrim(name)
ltrim(name)
rtrim(name)
lower(email)
upper(country)
initcap(customer_name)
length(name)
char_length(name)
left(code, 3)
right(code, 4)
substring(value FROM 2 FOR 5)
split_part(email, '@', 2)
position('@' IN email)
replace(value, 'old', 'new')
concat(first_name, ' ', last_name)
value || suffix

--
-- ### Normalize messy text
--

NULLIF(
  lower(
    regexp_replace(btrim(email), '[[:space:]]+', '', 'g')
  ),
  ''
) AS normalized_email

--
-- Use whitespace removal carefully: removing every space may be
-- appropriate for email keys, but not names.
--
-- ### Regex essentials
--
-- -   `~`: case-sensitive regex match.
-- -   `~*`: case-insensitive regex match.
-- -   `!~`, `!~*`: does not match.
-- -   `^`: start of string.
-- -   `$`: end of string.
-- -   `.`: any character.
-- -   `+`: one or more.
-- -   `*`: zero or more.
-- -   `?`: optional.
-- -   `[...]`: character set.
-- -   `[^...]`: any character not in the set.
-- -   `(...)`: capture group.
-- -   `|`: alternation.
-- -   `\\.` in a regex pattern represents a literal dot in many regex
--     contexts; SQL string escaping behavior depends on string syntax.
--
-- Example:
--

SELECT 'kritikarya@gmail.com'
       ~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$';

--
-- This is a basic format check, not complete email validation.
--
-- Extract parts:
--

SELECT regexp_match(email, '^([^@]+)@([^@]+)$');

--
-- This returns capture groups for the local part and domain.
--
-- ### Duplicate text keys
--

SELECT lower(btrim(email)) AS normalized_email, count(*)
FROM practice.customers
WHERE email IS NOT NULL
GROUP BY lower(btrim(email))
HAVING count(*) > 1;

--
-- Normalize first, then decide the business rule for which record
-- survives.
--
-- ------------------------------------------------------------------------
--
-- ## 6. Dates, timestamps, and time zones
--
-- ### Current values
--

CURRENT_DATE
CURRENT_TIMESTAMP
LOCALTIME
LOCALTIMESTAMP
now()

--
-- `CURRENT_TIMESTAMP` / `now()` represent the transaction start time in
-- PostgreSQL.
--
-- ### Extract and format
--

EXTRACT(YEAR FROM order_ts)
EXTRACT(MONTH FROM order_ts)
EXTRACT(QUARTER FROM order_ts)
EXTRACT(DOW FROM order_ts)       -- Sunday=0
EXTRACT(ISODOW FROM order_ts)    -- Monday=1, Sunday=7
EXTRACT(EPOCH FROM interval_value)
to_char(order_ts, 'YYYY-MM-DD')
to_char(order_ts, 'Mon-YYYY')

--
-- -   `TO_CHAR` formats values as text.
-- -   `TO_DATE` and `TO_TIMESTAMP` parse text according to a format.
-- -   `EXTRACT` returns a numeric component.
-- -   ISO week-year can differ from calendar year near New Year; pair ISO
--     week with ISO year.
--
-- ### Truncation and boundaries
--

date_trunc('month', order_ts)
date_trunc('week', order_ts)     -- week begins Monday in PostgreSQL
(date_trunc('month', order_ts)
 + interval '1 month' - interval '1 day')::date AS month_end
(date_trunc('week', order_ts)
 + interval '1 week' - interval '1 day')::date AS sunday_week_end

--
-- ### Date arithmetic
--

end_date::date - start_date::date        -- integer days
end_ts - start_ts                        -- interval
age(end_ts, start_ts)                    -- calendar-style interval
extract(epoch FROM (end_ts - start_ts))/3600 AS elapsed_hours

--
-- -   For elapsed hours, subtract timestamps and convert the interval to
--     epoch seconds.
-- -   Avoid using `TO_CHAR` to calculate durations.
-- -   Month arithmetic has calendar edge cases; January 31 plus one month
--     may land in February.
--
-- ### Time zones
--

order_ts AT TIME ZONE 'Asia/Kolkata'

--
-- -   Use `timestamptz` for actual instants.
-- -   Convert to the reporting time zone before deriving local business
--     dates.
-- -   Casting `timestamptz` to `timestamp` removes zone-aware type
--     semantics.
-- -   Session time zone affects display and some date casts.
--
-- ### Half-open time filters
--

WHERE order_ts >= TIMESTAMPTZ '2026-10-01 00:00:00+05:30'
  AND order_ts <  TIMESTAMPTZ '2026-11-01 00:00:00+05:30'

--
-- Prefer `>= start AND < next_period_start` over end-of-day `BETWEEN`
-- filters.
--
-- ------------------------------------------------------------------------
--
-- ## 7. Arrays and JSONB
--
-- ### Arrays
--

tags @> ARRAY['promotion']::text[]  -- contains element
'promotion' = ANY(tags)             -- element membership
cardinality(tags)                   -- number of elements
unnest(tags)                        -- one row per element

--
-- Avoid casting arrays to text and searching with `LIKE`; it can create
-- false matches.
--
-- ### JSONB
--

source_payload -> 'device'          -- JSON value
source_payload ->> 'device'         -- text value
source_payload #> '{customer,address,city}'
source_payload #>> '{customer,address,city}'
source_payload ? 'device'           -- top-level key exists
source_payload @> '{"device":"mobile"}'::jsonb
jsonb_typeof(source_payload -> 'device')

--
-- -   `->` returns JSON/JSONB.
-- -   `->>` returns text.
-- -   `#>` and `#>>` traverse a path.
-- -   Missing keys typically yield NULL.
-- -   JSON `null` and SQL `NULL` are different concepts.
-- -   Validate and normalize JSON before downstream type casts.
--
-- Example:
--

SELECT COALESCE(
         NULLIF(lower(btrim(source_payload ->> 'device')), ''),
         'unknown'
       ) AS device,
       count(*)
FROM src.orders
GROUP BY 1;

--
-- ------------------------------------------------------------------------
--
-- ## 8. Joins and set operations
--
-- ### Joins
--

-- Matching rows only
SELECT ... FROM a JOIN b ON a.id = b.id;

-- Keep every row from left side
SELECT ... FROM a LEFT JOIN b ON a.id = b.id;

-- Keep unmatched left rows
SELECT a.*
FROM a LEFT JOIN b ON a.id = b.id
WHERE b.id IS NULL;

-- Prefer this anti-join pattern for complex exclusions
SELECT a.*
FROM a
WHERE NOT EXISTS (
  SELECT 1 FROM b WHERE b.id = a.id
);

--
-- -   `INNER JOIN`: matched rows.
-- -   `LEFT JOIN`: all left rows plus matches.
-- -   `RIGHT JOIN`: all right rows plus matches.
-- -   `FULL OUTER JOIN`: matched and unmatched rows from both.
-- -   `CROSS JOIN`: Cartesian product.
-- -   Self-join: join a table to itself.
-- -   A join can multiply rows. Check one-to-one, one-to-many, or
--     many-to-many cardinality.
-- -   Filter placement matters: putting right-table conditions in `WHERE`
--     can remove NULL-extended rows and effectively turn a left join into
--     an inner join.
--
-- ### Set operations
--

SELECT id FROM a
UNION
SELECT id FROM b;

SELECT id FROM a
UNION ALL
SELECT id FROM b;

SELECT id FROM a
INTERSECT
SELECT id FROM b;

SELECT id FROM a
EXCEPT
SELECT id FROM b;

--
-- -   `UNION`: combine and remove duplicates.
-- -   `UNION ALL`: combine and preserve duplicates; usually cheaper.
-- -   `INTERSECT`: rows common to both.
-- -   `EXCEPT`: rows from first query absent from second.
-- -   Each query must return compatible column counts and types.
-- -   `ORDER BY` normally belongs at the end of the combined query.
--
-- ------------------------------------------------------------------------
--
-- ## 9. Aggregation and conditional aggregation
--

SELECT
  country,
  COUNT(*) AS row_count,
  COUNT(DISTINCT customer_id) AS customers,
  SUM(revenue) AS revenue,
  AVG(revenue) AS average_revenue,
  MIN(order_ts) AS first_order,
  MAX(order_ts) AS last_order
FROM sales
GROUP BY country
HAVING SUM(revenue) > 10000;

--
-- -   `WHERE` filters rows before grouping.
-- -   `GROUP BY` defines output groups.
-- -   `HAVING` filters groups after aggregation.
-- -   Every selected non-aggregated expression generally must be grouped
--     or functionally dependent where supported.
-- -   `FILTER` offers concise conditional aggregation in PostgreSQL:
--

COUNT(*) FILTER (WHERE gender = 'Female') AS female_count

--
-- Equivalent pattern:
--

SUM(CASE WHEN gender = 'Female' THEN 1 ELSE 0 END)

--
-- -   Do not confuse counting users with counting joined friendship
--     records.
-- -   Define the metric and grain before counting.
-- -   `COUNT(DISTINCT ...)` can protect against duplicate entities, but it
--     does not repair every incorrect join or business definition.
--
-- ------------------------------------------------------------------------
--
-- ## 10. CTEs, subqueries, and recursive SQL
--
-- ### CTE
--

WITH monthly_sales AS (
  SELECT date_trunc('month', order_ts)::date AS month_start,
         sum(amount) AS revenue
  FROM orders
  GROUP BY 1
)
SELECT *
FROM monthly_sales
WHERE revenue > 10000;

--
-- -   `WITH` names a query result for the statement.
-- -   Use CTEs to make logical stages explicit.
-- -   A CTE is not automatically a persisted table.
-- -   PostgreSQL can inline or materialize CTEs depending on version,
--     references, and query structure.
-- -   Use `MATERIALIZED` or `NOT MATERIALIZED` only when justified and
--     supported.
-- -   Redshift optimizer behavior differs; inspect the query plan.
--
-- ### Subquery patterns
--

-- Scalar subquery
SELECT *
FROM products
WHERE price > (SELECT avg(price) FROM products);

-- EXISTS
SELECT c.*
FROM customers c
WHERE EXISTS (
  SELECT 1 FROM orders o WHERE o.customer_id = c.customer_id
);

-- NOT EXISTS
SELECT c.*
FROM customers c
WHERE NOT EXISTS (
  SELECT 1 FROM orders o WHERE o.customer_id = c.customer_id
);

-- Derived table
SELECT *
FROM (
  SELECT customer_id, sum(amount) AS revenue
  FROM orders
  GROUP BY customer_id
) x
WHERE revenue > 10000;

--
-- ### Correlated subquery
--
-- A subquery references the outer query. It can be clear, but repeated
-- execution can be costly; compare plans and alternatives.
--
-- ### Recursive CTE
--
-- Useful for hierarchies and graph traversal.
--

WITH RECURSIVE org AS (
  SELECT employee_id, manager_id, 0 AS depth
  FROM employees
  WHERE manager_id IS NULL
  UNION ALL
  SELECT e.employee_id, e.manager_id, o.depth + 1
  FROM employees e
  JOIN org o ON e.manager_id = o.employee_id
)
SELECT * FROM org;

--
-- Guard against cycles in graph data.
--
-- ------------------------------------------------------------------------
--
-- ## 11. Window functions
--
-- Window functions preserve rows while calculating across related rows.
--
-- ### Ranking
--

ROW_NUMBER() OVER (PARTITION BY country ORDER BY revenue DESC)
RANK()       OVER (PARTITION BY country ORDER BY revenue DESC)
DENSE_RANK() OVER (PARTITION BY country ORDER BY revenue DESC)
NTILE(4)     OVER (ORDER BY revenue)

--
-- -   `ROW_NUMBER`: unique sequential row number; add tie-breakers for
--     determinism.
-- -   `RANK`: ties share rank; later ranks have gaps.
-- -   `DENSE_RANK`: ties share rank; no gaps.
-- -   Filter a window result in an outer query or CTE.
--
-- ### LAG / LEAD
--

LAG(revenue) OVER (PARTITION BY product_id ORDER BY month_start)
LEAD(revenue) OVER (PARTITION BY product_id ORDER BY month_start)

--
-- -   `LAG`: previous row's value.
-- -   `LEAD`: next row's value.
-- -   They follow ordered rows, not automatically calendar periods.
--     Missing months require a date spine.
--
-- ### Running total and average
--

SUM(revenue) OVER (
  PARTITION BY product_id
  ORDER BY month_start
  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
) AS running_revenue,

AVG(revenue) OVER (
  PARTITION BY product_id
  ORDER BY month_start
  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
) AS running_average

--
-- ### Rolling six-row metric
--

AVG(revenue) OVER (
  PARTITION BY product_id
  ORDER BY month_start
  ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
)

--
-- This is six rows, not guaranteed six calendar months.
--
-- ### Other analytical patterns
--
-- -   Percent change:
--     `(current_value - previous_value) / NULLIF(previous_value, 0)`.
-- -   Deduplication:
--     `ROW_NUMBER() OVER (PARTITION BY business_key ORDER BY updated_at DESC, unique_id DESC)`.
-- -   Top N per group: rank within each partition, then filter rank.
-- -   Gaps/islands: combine row numbers with dates or sequence values.
-- -   Cohorts: group users by acquisition period, then measure later
--     behavior.
-- -   Funnels: define ordered events and conversion criteria explicitly.
--
-- ------------------------------------------------------------------------
--
-- ## 12. Deduplication patterns
--
-- ### Identify duplicates
--

SELECT lower(btrim(email)) AS email_key, count(*)
FROM customers
WHERE email IS NOT NULL
GROUP BY 1
HAVING count(*) > 1;

--
-- ### Keep the latest row
--

WITH ranked AS (
  SELECT t.*,
         row_number() OVER (
           PARTITION BY source_id
           ORDER BY updated_at DESC, row_id DESC
         ) AS rn
  FROM staging t
)
SELECT *
FROM ranked
WHERE rn = 1;

--
-- ### Delete duplicate physical rows
--

WITH ranked AS (
  SELECT row_id,
         row_number() OVER (
           PARTITION BY source_id
           ORDER BY updated_at DESC, row_id DESC
         ) AS rn
  FROM staging
)
DELETE FROM staging s
USING ranked r
WHERE s.row_id = r.row_id
  AND r.rn > 1;

--
-- Preview the deletion with `SELECT` first. Use a transaction for risky
-- cleanup.
--
-- ### Key principles
--
-- -   `DISTINCT` removes duplicate selected combinations; it does not
--     choose the best record.
-- -   Define the business key and survivor rule.
-- -   Normalize before comparing textual keys.
-- -   Use a deterministic tie-breaker.
-- -   Do not delete records merely because their values look similar.
--
-- ------------------------------------------------------------------------
--
-- ## 13. Data modelling and warehouse architecture
--
-- ### Common layers
--
-- -   **Source/raw/bronze**: preserve source records and lineage.
-- -   **Staging**: type casting, standardization, basic deduplication.
-- -   **Core/silver**: conformed entities, business keys, history,
--     validated relationships.
-- -   **Mart/gold**: business-ready facts, dimensions, aggregates, KPIs.
-- -   **Audit/control**: load history, row counts, reconciliation results,
--     errors.
--
-- Layer names are conventions; enforce clear ownership and contracts.
--
-- ### Fact and dimension design
--
-- -   Declare fact grain before selecting columns.
-- -   Examples: one row per order line, payment, daily account balance, or
--     staffing snapshot.
-- -   Dimensions describe customer, product, employee, location, or date.
-- -   Fact tables hold measurable events and dimension keys.
-- -   Avoid mixing different grains in one fact table.
-- -   Use conformed dimensions for consistent reporting across marts.
-- -   Choose additive, semi-additive, or non-additive measures
--     deliberately.
--
-- ### Keys
--
-- -   Natural key: source/business identifier.
-- -   Surrogate key: warehouse-managed identity.
-- -   Composite key: multiple columns jointly identify a record.
-- -   Degenerate dimension: business identifier stored directly in a fact,
--     such as order number.
-- -   Unknown/default dimension member: explicit handling for unresolved
--     dimension lookups, if the model requires it.
--
-- ### Star vs snowflake
--
-- -   Star: denormalized dimensions around facts; often easier for
--     analytics.
-- -   Snowflake: normalized dimension substructures; can reduce redundancy
--     but adds joins.
-- -   Choose based on query patterns, governance, storage, and
--     maintenance.
--
-- ------------------------------------------------------------------------
--
-- ## 14. SCD Type 0, 1, 2, 3, 4, and 6
--
-- Slowly Changing Dimensions (SCD) track changes in descriptive
-- attributes. Select a type by business history requirements.
--
-- ### Type 0 --- retain original
--
-- Never overwrite the original attribute. Useful for immutable-attribution
-- fields.
--
-- ### Type 1 --- overwrite
--
-- No history retained.
--

UPDATE dim_customer
SET city = 'Bengaluru'
WHERE customer_bk = 'C123';

--
-- Use for corrections or attributes where history is unnecessary.
--
-- ### Type 2 --- new version row
--
-- Preserves full effective history using a new surrogate key and effective
-- dates. Typical columns: - `customer_sk` - `customer_bk` - tracked
-- attributes - `effective_from` - `effective_to` - `is_current` - optional
-- load timestamp and change reason
--
-- Example pattern:
--

-- Conceptual Type 2 pattern; transaction and concurrency handling required.
UPDATE dim_customer
SET effective_to = :change_ts,
    is_current = false
WHERE customer_bk = :customer_bk
  AND is_current = true;

INSERT INTO dim_customer (
  customer_sk, customer_bk, city,
  effective_from, effective_to, is_current
)
VALUES (
  :new_sk, :customer_bk, :new_city,
  :change_ts, TIMESTAMP '9999-12-31 00:00:00', true
);

--
-- -   Facts should resolve the dimension version valid at event time.
-- -   Enforce one current row per business key in PostgreSQL where
--     appropriate.
-- -   Handle late-arriving changes and overlapping effective intervals.
--
-- ### Type 3 --- limited history
--
-- Stores current and previous value in separate columns, such as
-- `current_region`, `previous_region`. Limited history only.
--
-- ### Type 4 --- history table
--
-- Current attributes live in a current dimension; prior versions live in a
-- separate history table. Define keys and retention rules carefully.
--
-- ### Type 6 --- hybrid 1 + 2 + 3
--
-- Often combines Type 2 row versioning with Type 1 overwrite and Type 3
-- current/previous attributes. Implementations vary; document the exact
-- semantics rather than assuming one universal design.
--
-- ### SCD load decisions
--
-- -   Which attributes are tracked?
-- -   Which changes overwrite versus create history?
-- -   What is the business key?
-- -   How are effective dates and late-arriving changes handled?
-- -   Can two current versions exist?
-- -   How do facts map to the correct historical version?
-- -   Is a change a correction or a real business event?
--
-- ------------------------------------------------------------------------
--
-- ## 15. ETL / ELT pipeline design
--
-- ### ETL vs ELT
--
-- -   **ETL**: extract, transform, load.
-- -   **ELT**: extract, load raw data, transform inside the target
--     platform.
-- -   Modern warehouse workflows often use ELT; choose based on source,
--     governance, cost, latency, and platform capabilities.
--
-- ### Robust pipeline stages
--
-- 1.  Extract source data with a stable extraction boundary.
-- 2.  Land immutable/raw data with source metadata.
-- 3.  Validate file, schema, and batch metadata.
-- 4.  Load staging tables.
-- 5.  Normalize types, strings, timestamps, and codes.
-- 6.  Deduplicate by business key and survivor rule.
-- 7.  Validate required fields, domains, relationships, and totals.
-- 8.  Apply inserts/updates/deletes or CDC.
-- 9.  Merge into core entities and dimensions.
-- 10. Load facts and marts at declared grain.
-- 11. Reconcile source and target.
-- 12. Record batch status, errors, metrics, and lineage.
-- 13. Publish only successful/approved data.
-- 14. Make reruns safe and auditable.
--
-- ### Pipeline metadata
--
-- Track: - `batch_id`, source name, file/object name, extract window. -
-- `started_at`, `completed_at`, status, retry count. - source row count,
-- loaded row count, rejected row count. - inserted/updated/deleted
-- counts. - source and target control totals. - code/version identifier,
-- error details, watermark. - checksum or file hash where appropriate.
--
-- ### Incremental loading
--
-- Common strategies: - `updated_at` watermark. - Monotonic source
-- sequence. - Log-based CDC. - Source change tables. - Snapshot
-- comparison.
--
-- Use a safe overlap window when late updates are possible, then
-- deduplicate/upsert. Persist the watermark only after the batch succeeds.
-- A timestamp alone may miss deletes and tied/late updates.
--
-- ------------------------------------------------------------------------
--
-- ## 16. Schema enforcement and data quality
--
-- ### Schema enforcement
--
-- -   Required columns and types.
-- -   Required fields and nullability.
-- -   Allowed values and ranges.
-- -   Primary/foreign keys where actually enforced.
-- -   Source-to-target mapping and naming standards.
-- -   Versioned schema contracts.
-- -   Explicit policy for added, removed, or changed columns.
-- -   Quarantine incompatible records rather than silently coercing them.
--
-- ### Common checks
--

-- Required field failures
SELECT count(*) FROM stg.orders WHERE order_id IS NULL;

-- Duplicate business keys
SELECT source_order_id, count(*)
FROM stg.orders
GROUP BY source_order_id
HAVING count(*) > 1;

-- Invalid values
SELECT * FROM stg.orders WHERE amount < 0;

-- Orphan foreign keys
SELECT s.customer_id
FROM stg.orders s
LEFT JOIN core.customers c ON c.customer_id = s.customer_id
WHERE c.customer_id IS NULL;

-- Referential completeness
SELECT count(*)
FROM stg.orders s
WHERE NOT EXISTS (
  SELECT 1 FROM core.customers c
  WHERE c.customer_id = s.customer_id
);

--
-- ### Reconciliation
--
-- -   Source vs target row counts.
-- -   Distinct business-key counts.
-- -   Sum of amount, quantity, hours, headcount, or other control totals.
-- -   Min/max dates and batch boundaries.
-- -   Duplicate and orphan counts.
-- -   Insert/update/delete counts.
-- -   Per-partition counts and totals.
-- -   Hash/checksum comparisons when suitable.
--
-- **Important:** Matching row counts alone does not prove correctness.
-- Validate content, grain, and business totals.
--
-- ### Error handling
--
-- -   Reject, quarantine, or route bad records.
-- -   Preserve original source values for diagnosis.
-- -   Record row-level error reasons.
-- -   Set thresholds and alerting rules.
-- -   Avoid silently dropping data.
-- -   Separate technical failure from business-rule rejection.
--
-- ------------------------------------------------------------------------
--
-- ## 17. Idempotency, upserts, and CDC
--
-- ### Idempotency
--
-- An operation is idempotent when replaying the same input yields the same
-- intended final state.
--

INSERT INTO core.orders (source_order_id, amount, status)
VALUES (1001, 250.00, 'completed')
ON CONFLICT (source_order_id)
DO UPDATE SET
  amount = EXCLUDED.amount,
  status = EXCLUDED.status;

--
-- -   Requires a stable key and a clearly defined conflict policy.
-- -   In PostgreSQL, a matching unique constraint/index is required for
--     `ON CONFLICT`.
-- -   Avoid updating rows unnecessarily when values are unchanged.
-- -   For event streams, store processed event IDs when duplicate delivery
--     is possible.
-- -   Idempotency does not mean every side effect can safely be repeated.
--
-- ### CDC
--
-- Change Data Capture tracks inserts, updates, and deletes. - Timestamp
-- CDC: easy, but may miss deletes or late/tied changes. - Trigger CDC:
-- records changes transactionally but adds write overhead. - Log-based
-- CDC: reads transaction log/WAL changes; often better for complete change
-- streams. - Snapshot-diff CDC: compare current and prior snapshots;
-- potentially expensive. - Preserve operation type, source key, change
-- timestamp/sequence, and source metadata. - Handle ordering, retries,
-- duplicate events, schema changes, and delete tombstones.
--
-- ### PostgreSQL trigger example concept
--
-- A trigger function can store `OLD` and `NEW` values for
-- insert/update/delete. Design the audit schema and avoid recursive
-- triggers.
--
-- ### Redshift note
--
-- Redshift loading and change processing often use staged files, `COPY`,
-- `MERGE` where supported, staging tables, and orchestration. Do not
-- assume PostgreSQL triggers or row-level OLTP patterns apply directly.
--
-- ------------------------------------------------------------------------
--
-- ## 18. Stored procedures, functions, UDFs, triggers
--
-- ### PostgreSQL function
--
-- Returns a value or set and can be used in queries, subject to function
-- properties and side effects.
--

CREATE OR REPLACE FUNCTION practice.add_tax(amount numeric, rate numeric)
RETURNS numeric
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT amount * (1 + rate);
$$;

SELECT practice.add_tax(100, 0.18);

--
-- ### PostgreSQL procedure
--
-- Used for procedural operations and invoked with `CALL`.
--

CREATE OR REPLACE PROCEDURE practice.mark_completed(p_order_id bigint)
LANGUAGE plpgsql
AS $$
BEGIN
  UPDATE practice.orders
  SET status = 'completed'
  WHERE order_id = p_order_id;
END;
$$;

CALL practice.mark_completed(10);

--
-- -   Procedures can use procedural control flow.
-- -   Transaction control within procedures has restrictions and depends
--     on invocation context.
-- -   Functions are called as expressions; procedures are invoked with
--     `CALL`.
--
-- ### PL/pgSQL building blocks
--
-- -   `DECLARE`: local variables.
-- -   `BEGIN ... END`: block.
-- -   `IF / ELSIF / ELSE`: branching.
-- -   `LOOP`, `WHILE`, `FOR`: iteration.
-- -   `EXCEPTION`: handle errors.
-- -   `RAISE NOTICE` / `RAISE EXCEPTION`: messages and errors.
-- -   Dynamic SQL: `EXECUTE`; quote identifiers and parameterize values
--     safely.
-- -   Prefer set-based SQL over row-by-row loops where practical.
--
-- ### Trigger
--
-- Automatically runs in response to table events. - `BEFORE` / `AFTER` /
-- `INSTEAD OF`. - `INSERT`, `UPDATE`, `DELETE`, sometimes `TRUNCATE`. -
-- Row-level vs statement-level triggers. - Use for audit rules or
-- enforcing logic that truly belongs at the database layer. - Avoid hidden
-- heavy business logic that makes writes difficult to reason about.
--
-- ### UDF differences
--
-- -   PostgreSQL supports SQL and PL/pgSQL functions plus other languages
--     where configured.
-- -   Redshift supports SQL and Python UDFs subject to feature lifecycle
--     and platform restrictions. Check current Redshift documentation
--     before designing new Python UDF dependencies.
-- -   Prefer native SQL for performance, portability, and optimizer
--     visibility when feasible.
-- -   Mark function volatility accurately: `IMMUTABLE`, `STABLE`,
--     `VOLATILE`.
--
-- ------------------------------------------------------------------------
--
-- ## 19. Time-series data engineering
--
-- ### Date spine
--
-- A date spine fills missing periods so time windows represent actual
-- calendar periods.
--

SELECT generate_series(
  DATE '2026-01-01',
  DATE '2026-12-31',
  INTERVAL '1 day'
)::date AS calendar_date;

--
-- `generate_series` is PostgreSQL-specific; create/use a date dimension in
-- Redshift when appropriate.
--
-- ### Monthly series
--

SELECT date_trunc('month', order_ts)::date AS month_start,
       count(*) AS orders,
       sum(amount) AS revenue
FROM orders
GROUP BY 1
ORDER BY 1;

--
-- ### Rolling metrics
--
-- -   First aggregate raw events to the desired period and grain.
-- -   Join to a complete date spine if missing periods must count as zero.
-- -   Then apply `LAG`, running sums, or rolling frames.
-- -   Decide whether missing data means zero, unknown, or no observation.
-- -   Choose calendar windows versus row windows explicitly.
--
-- ### Forecast-ready data
--
-- -   Regular frequency and consistent time zone.
-- -   Explicit handling of missing timestamps and duplicate events.
-- -   Separate actuals from forecasts.
-- -   Avoid leakage: features must only use information available at
--     prediction time.
-- -   Split training/testing chronologically for time series.
-- -   Track forecast horizon, model version, actual value, prediction, and
--     error metric.
--
-- ------------------------------------------------------------------------
--
-- ## 20. Analytics patterns
--
-- ### Top N per country
--

WITH ranked AS (
  SELECT country, product_id, sum(revenue) AS revenue,
         DENSE_RANK() OVER (
           PARTITION BY country ORDER BY sum(revenue) DESC
         ) AS rnk
  FROM sales
  GROUP BY country, product_id
)
SELECT *
FROM ranked
WHERE rnk <= 3;

--
-- ### Compare period to prior period
--

WITH monthly AS (
  SELECT date_trunc('month', order_ts)::date AS month_start,
         sum(amount) AS revenue
  FROM orders
  GROUP BY 1
)
SELECT *,
       lag(revenue) OVER (ORDER BY month_start) AS previous_revenue,
       revenue - lag(revenue) OVER (ORDER BY month_start) AS change
FROM monthly;

--
-- ### Anti-join: rows without a match
--

SELECT o.*
FROM orders o
WHERE NOT EXISTS (
  SELECT 1 FROM payments p WHERE p.order_id = o.order_id
);

--
-- ### Conditional counts
--

SELECT country,
       count(*) FILTER (WHERE gender = 'Female') AS female_count,
       count(*) FILTER (WHERE gender = 'Male') AS male_count
FROM users
GROUP BY country;

--
-- ### Product budget utilization
--
-- Define the grain first: - Product total cost = sum of each project cost
-- once per product-project pair. - Manager product cost = sum of projects
-- assigned to that manager for that product. - Utilization = manager
-- product cost / product budget. - Product over budget = total product
-- cost \> product budget. - Decide how shared projects are allocated;
-- counting full cost for every manager can double-attribute manager
-- utilization.
--
-- ### A/B testing SQL support
--
-- -   Binary outcomes: conversions / eligible visitors; use a
--     two-proportion test.
-- -   Continuous outcomes: average order value; Welch's t-test is often
--     appropriate.
-- -   Randomization unit and analysis unit must align.
-- -   Validate sample sizes, missingness, assignment contamination, and
--     exposure windows.
-- -   Statistical significance does not prove business significance or
--     causality without a valid design.
--
-- ------------------------------------------------------------------------
--
-- ## 21. PostgreSQL performance and indexing
--
-- ### Inspect query plans
--

EXPLAIN SELECT ...;
EXPLAIN (ANALYZE, BUFFERS) SELECT ...;

--
-- -   `EXPLAIN` estimates the plan.
-- -   `EXPLAIN ANALYZE` executes the query and reports actuals.
-- -   `BUFFERS` helps inspect I/O activity.
-- -   `EXPLAIN ANALYZE` can execute modifications; use caution.
-- -   Compare estimated and actual rows, scan type, join strategy, sort,
--     spills, and time.
--
-- ### Index types
--
-- -   **B-tree**: equality/range, sorting, most common index.
-- -   **GIN**: inverted indexes, useful for JSONB/arrays/full-text
--     patterns.
-- -   **GiST**: extensible structures such as geometric/range use cases.
-- -   **BRIN**: compact summaries for very large, physically correlated
--     data.
-- -   **Hash**: equality comparisons; B-tree is often more generally
--     useful.
--
-- ### Index patterns
--

CREATE INDEX idx_orders_customer_ts
ON orders(customer_id, order_ts);

CREATE INDEX idx_orders_lower_email
ON customers(lower(email));

CREATE INDEX idx_active_orders
ON orders(order_ts)
WHERE status = 'active';

CREATE INDEX idx_payload_gin
ON events USING gin(payload);

--
-- -   Composite index column order matters.
-- -   A B-tree composite index commonly helps from its leading column(s).
-- -   Expression indexes help only when query expressions match
--     appropriately.
-- -   Partial indexes cover a subset of rows.
-- -   Indexes consume storage and slow writes; don't index every column.
-- -   PostgreSQL indexes can enforce uniqueness; Redshift
--     constraints/indexes work differently.
-- -   Use `ANALYZE` to refresh planner statistics; `VACUUM` addresses dead
--     tuples and table maintenance.
-- -   Check bloat, stale statistics, locks, and long-running transactions.
--
-- ### Common query improvements
--
-- -   Filter early when it reduces scanned data.
-- -   Select only needed columns.
-- -   Use sargable predicates; avoid wrapping indexed filter columns in
--     functions unless supported by a matching expression index.
-- -   Avoid unnecessary `DISTINCT`; it may hide join bugs and require
--     sorting/hashing.
-- -   Aggregate at the correct grain before joining when that reduces row
--     volume and preserves semantics.
-- -   Replace repeated correlated logic only when the alternative is
--     semantically equivalent and the plan improves.
-- -   Avoid functions on join keys that prevent efficient matching.
-- -   Use `EXISTS` for existence checks and `NOT EXISTS` for anti-joins
--     when suitable.
-- -   Do not optimize based on query appearance alone; inspect plans and
--     measure.
--
-- ------------------------------------------------------------------------
--
-- ## 22. Amazon Redshift optimization
--
-- Redshift is a distributed, columnar warehouse. Optimize data movement
-- and scan volume.
--
-- ### Columnar storage and compression
--
-- -   Read only required columns; avoid `SELECT *` in large analytical
--     scans.
-- -   Load using efficient bulk paths such as `COPY` from S3 for large
--     loads.
-- -   Choose data types carefully; avoid unnecessarily wide strings and
--     high-precision types.
-- -   Redshift manages encoding automatically in many cases; inspect table
--     design and actual workload.
-- -   Compression reduces storage and I/O, but validate workload effects.
--
-- ### Distribution styles
--
-- -   `AUTO`: let Redshift manage distribution where appropriate.
-- -   `KEY`: colocate rows with the same distribution key for common
--     joins.
-- -   `ALL`: replicate a small dimension to nodes; increases storage and
--     maintenance.
-- -   `EVEN`: distribute rows evenly when no suitable key exists.
-- -   Distribution choices trade data movement against skew and
--     maintenance.
--
-- ### Sort keys
--
-- -   Sort keys can improve block pruning and range-filter performance.
-- -   Choose based on frequent filter and join patterns.
-- -   Compound sort keys favor leading-column patterns.
-- -   Interleaved sort keys have specialized trade-offs and maintenance
--     costs; do not choose by default without workload evidence.
-- -   Automatic table optimization can manage sort/distribution choices in
--     supported setups.
--
-- ### Query execution
--
-- -   Inspect `EXPLAIN` for scans, joins, redistribution/broadcast, sorts,
--     aggregates, and estimated costs.
-- -   Reduce data before expensive joins where logically valid.
-- -   Watch data skew and redistribution.
-- -   Avoid cross joins and accidental many-to-many joins.
-- -   Use small dimensions strategically; `DISTSTYLE ALL` may help only
--     when the table is small enough.
-- -   Use materialized views for expensive repeated aggregations when
--     refresh semantics fit.
-- -   Use result caching when applicable, but do not rely on it as the
--     sole performance strategy.
--
-- ### Loading and maintenance
--
-- -   Prefer bulk `COPY` for large S3 loads over many individual inserts.
-- -   Use staging tables for validation and controlled merges.
-- -   Use `MERGE` where supported and suitable, or a tested delete/insert
--     transaction pattern.
-- -   Monitor load errors, rejected rows, queueing, spills, skew, and
--     query duration.
-- -   Redshift manages storage and many maintenance tasks, but inspect
--     table health and workload metrics.
-- -   Choose provisioned clusters vs Serverless based on concurrency,
--     utilization, operational preferences, and cost model.
-- -   Configure WLM/query priorities and concurrency controls based on
--     workload classes.
-- -   Use workload monitoring and cost controls; avoid scanning
--     unnecessary history.
--
-- ### PostgreSQL vs Redshift quick comparison
--
--   ------------------------------------------------------------------------------
--   Topic                   PostgreSQL                     Redshift
--   ----------------------- ------------------------------ -----------------------
--   Main use                OLTP + mixed analytics         OLAP warehouse
--
--   Storage                 Row-oriented by default        Columnar
--
--   Distribution            Single database                Data distributed across
--                           instance/cluster architecture  nodes
--
--   PK/FK enforcement       Enforced                       Generally informational
--                                                          constraints; ensure
--                                                          data correctness
--                                                          yourself
--
--   Indexes                 B-tree, GIN, GiST, BRIN, etc.  No PostgreSQL-style
--                                                          user-managed indexes
--
--   Bulk ingestion          `COPY` is available            `COPY` from S3 is a
--                                                          core bulk-load path
--
--   Query plans             `EXPLAIN (ANALYZE, BUFFERS)`   `EXPLAIN`, Redshift
--                                                          system monitoring views
--
--   Scale strategy          Instance, replicas,            Distribution, sort
--                           partitioning, extensions       keys, columnar scans,
--                                                          workload management
--
--   Procedures              PL/pgSQL functions/procedures  Redshift stored
--                                                          procedures and
--                                                          supported language
--                                                          features differ
--
--   Time-series helper      `generate_series`              Often use a date
--                                                          dimension or supported
--                                                          warehouse techniques
--   ------------------------------------------------------------------------------
--
-- ------------------------------------------------------------------------
--
-- ## 23. Reliability, transactions, and concurrency
--
-- -   **ACID**: atomicity, consistency, isolation, durability.
-- -   **MVCC**: multi-version concurrency control; readers and writers can
--     often proceed concurrently.
-- -   **Isolation levels**: control visibility and anomalies.
-- -   **Locks**: protect rows/tables or other resources; long transactions
--     can block work.
-- -   **Deadlock**: transactions wait in a cycle; database aborts one
--     participant.
-- -   **Optimistic conflict handling**: unique constraints and
--     `ON CONFLICT` can protect inserts.
-- -   **Retry policy**: retry transient failures carefully; make
--     operations idempotent.
-- -   **Atomic batch**: group related changes in one transaction where
--     appropriate.
-- -   **External side effects**: messages/files/API calls need separate
--     reliability patterns, such as outbox/inbox or idempotency keys.
-- -   **Least privilege**: grant only required permissions.
-- -   **Secrets**: do not hardcode passwords or access keys in SQL
--     scripts.
--
-- ------------------------------------------------------------------------
--
-- ## 24. Frequent mistakes checklist
--
-- Before submitting SQL, ask:
--
-- 1.  What does one row represent in each CTE?
-- 2.  Does any join multiply rows?
-- 3.  Am I counting people, events, paths, or joined records?
-- 4.  Is the business key truly unique?
-- 5.  Should this be `COUNT(*)`, `COUNT(column)`, or
--     `COUNT(DISTINCT ...)`?
-- 6.  Are NULLs, blanks, and zero handled intentionally?
-- 7.  Does `WHERE` belong before aggregation, or is this a `HAVING`
--     condition?
-- 8.  Does a window function need a partition and deterministic order?
-- 9.  Is `RANK`, `DENSE_RANK`, or `ROW_NUMBER` the right tie behavior?
-- 10. Does the time filter use the correct time zone and half-open
--     boundaries?
-- 11. Are missing periods represented as zero or unknown?
-- 12. Is deduplication done before aggregation at the correct grain?
-- 13. Can a rerun duplicate data?
-- 14. Are source/target row counts and control totals reconciled?
-- 15. Are PK/FK constraints truly enforced on the platform?
-- 16. Did I inspect the execution plan before claiming performance
--     improvement?
-- 17. Did I test edge cases: duplicates, NULLs, ties, empty inputs, zero
--     denominators, and late records?
--
-- ------------------------------------------------------------------------
--
-- ## 25. Recommended learning order
--
-- 1.  SELECT, filtering, sorting, NULLs, data types, casts.
-- 2.  String cleaning, dates, arrays, JSONB.
-- 3.  Joins, aggregation, conditional aggregation, set operations.
-- 4.  CTEs, subqueries, ranking, LAG/LEAD, running and rolling windows.
-- 5.  Deduplication, keys, constraints, DDL/DML/TCL, CRUD.
-- 6.  Data modelling, fact/dimension grain, star schemas.
-- 7.  Data quality, reconciliation, schema contracts, CDC, idempotency.
-- 8.  ETL/ELT, incremental loads, upserts, error handling, audit tables.
-- 9.  SCD types and historical fact-to-dimension joins.
-- 10. Stored procedures, functions, triggers, dynamic SQL.
-- 11. Time-series date spines, rolling metrics, cohort and trend analysis.
-- 12. PostgreSQL indexes, `EXPLAIN ANALYZE`, transactions, maintenance.
-- 13. Redshift distribution, sort keys, bulk loading, workload management.
-- 14. End-to-end projects: workforce/talent acquisition, pricing/revenue,
--     commercial/customer analytics.
--
-- **Final rule:** Correct grain → correct joins → correct metric →
-- validation → optimization.
-- ---
--
-- # Part II — Expanded, GitHub-Ready Worked Examples
--
-- > **Preservation note:** Part I above is the original cheat sheet, retained verbatim. This section adds explanations, runnable examples, validation queries, and platform-specific guidance.
--
-- ## 26. How to use this repository
--
-- ### Learning loop
--
-- 1. Read the concept and its business purpose.
-- 2. Run the smallest example first.
-- 3. Inspect the result before moving forward.
-- 4. Change the sample data and predict the result.
-- 5. Run the validation query.
-- 6. Only then optimize the query.
--
-- ### SQL platform labels
--
-- - **[PostgreSQL]**: Intended for PostgreSQL, including pgAdmin 4.
-- - **[Redshift]**: Intended for Amazon Redshift.
-- - **[Portable pattern]**: Common SQL concept, but test syntax on your platform.
--
-- Do not run every script in one transaction. Some examples create or replace objects.
--
-- ### Suggested repository layout
--

postgresql-redshift-analytics/
├── README.md
├── sql/
│   ├── 01_foundations_and_casting.sql
│   ├── 02_where_having_and_aggregation.sql
│   ├── 03_window_functions.sql
│   ├── 04_recursive_queries.sql
│   ├── 05_data_quality_and_schema_enforcement.sql
│   ├── 06_scd_templates_postgresql.sql
│   ├── 07_watermarks_and_incremental_loads.sql
│   ├── 08_postgresql_performance.sql
│   └── redshift/
│       ├── 01_merge_and_incremental_load.sql
│       ├── 02_distribution_sort_and_explain.sql
│       └── 03_redshift_data_quality_checks.sql
└── references/
    └── official_documentation.md

--
-- ---
--
-- ## 27. Typecasting: make data types explicit
--
-- **Definition:** Typecasting converts a value from one data type to another.
--
-- A cast can change interpretation, precision, comparison behaviour, and performance.
--
-- ### Example A — Clean a numeric value stored as text [PostgreSQL]
--

WITH raw_orders(order_id, raw_amount) AS (
    VALUES
        (1, '1200.50'),
        (2, ' 450.00 '),
        (3, ''),
        (4, NULL)
),
cleaned AS (
    SELECT
        order_id,
        NULLIF(BTRIM(raw_amount), '') AS amount_text
    FROM raw_orders
)
SELECT
    order_id,
    amount_text,
    amount_text::numeric(12, 2) AS amount_numeric
FROM cleaned;

--
-- **How it works**
--
-- - `BTRIM` removes leading and trailing spaces.
-- - `NULLIF(value, '')` converts an empty string to `NULL`.
-- - `::numeric(12, 2)` converts text to a fixed-precision number.
-- - The empty value becomes `NULL`, avoiding an invalid empty-string cast.
-- - Malformed text such as `'abc'` still fails the cast.
--
-- ### Example B — Guard against malformed numeric text [PostgreSQL]
--

WITH raw_values(raw_amount) AS (
    VALUES ('120.50'), ('bad-data'), (''), (NULL)
)
SELECT
    raw_amount,
    CASE
        WHEN BTRIM(raw_amount) ~ '^[+-]?[0-9]+(\.[0-9]+)?$'
        THEN BTRIM(raw_amount)::numeric(12, 2)
        ELSE NULL
    END AS safe_amount
FROM raw_values;

--
-- This regular expression accepts ordinary decimal notation. It does not accept commas, currency symbols, or scientific notation.
--
-- **Production rule:** Separate invalid records from valid records. Do not silently turn every cast failure into `NULL` without recording the error.
--
-- ### Example C — Dates need explicit formats [PostgreSQL]
--

SELECT
    TO_DATE('17-10-2026', 'DD-MM-YYYY') AS parsed_date,
    TIMESTAMP '2026-10-17 14:30:00' AS timestamp_value,
    DATE '2026-10-17' AS date_value;

--
-- For text timestamps with time zones, use a type and parser that preserve the intended time-zone semantics. Do not assume a timezone-free `timestamp` contains UTC.
--
-- ### Example D — Financial arithmetic
--

SELECT
    10::numeric / 4::numeric AS exact_decimal_ratio,
    ROUND(10::numeric / 4::numeric, 2) AS rounded_ratio;

--
-- Use `NUMERIC` for exact monetary arithmetic. Binary floating-point can produce representation artefacts.
--
-- ---
--
-- ## 28. `WHERE` versus `HAVING`: filter at the right stage
--
-- **`WHERE` filters rows before grouping. `HAVING` filters groups after aggregation.**
--
-- Imagine an order table with one row per order.
--

WITH orders(customer_id, order_amount, status) AS (
    VALUES
        (101, 100::numeric, 'paid'),
        (101, 200::numeric, 'paid'),
        (101, 900::numeric, 'cancelled'),
        (102, 400::numeric, 'paid'),
        (102, 300::numeric, 'paid'),
        (103, 100::numeric, 'paid')
)
SELECT
    customer_id,
    COUNT(*) AS paid_order_count,
    SUM(order_amount) AS paid_revenue
FROM orders
WHERE status = 'paid'
GROUP BY customer_id
HAVING SUM(order_amount) >= 500
ORDER BY paid_revenue DESC;

--
-- **Read it in execution logic**
--
-- 1. `WHERE status = 'paid'` removes cancelled orders.
-- 2. `GROUP BY customer_id` creates one group per customer.
-- 3. `COUNT` and `SUM` calculate each group's metrics.
-- 4. `HAVING` keeps customers with paid revenue of at least 500.
-- 5. `ORDER BY` sorts the remaining customer groups.
--
-- ### Common mistake
--

-- Incorrect: aggregate functions generally cannot be used in WHERE.
SELECT customer_id, SUM(order_amount)
FROM orders
WHERE SUM(order_amount) >= 500
GROUP BY customer_id;

--
-- Use `HAVING` for aggregate conditions. Use `WHERE` for row-level conditions.
--
-- ### Equivalent using a CTE
--

WITH customer_totals AS (
    SELECT
        customer_id,
        SUM(order_amount) AS revenue
    FROM orders
    WHERE status = 'paid'
    GROUP BY customer_id
)
SELECT *
FROM customer_totals
WHERE revenue >= 500;

--
-- The CTE version can be easier to debug and extend.
--
-- ---
--
-- ## 29. Window functions: the detailed mental model
--
-- **A window function calculates across related rows without collapsing them.**
--
-- Compare these two queries.
--

-- Aggregation: one row per customer.
SELECT customer_id, SUM(order_amount) AS customer_revenue
FROM orders
GROUP BY customer_id;

-- Window: each order remains visible.
SELECT
    order_id,
    customer_id,
    order_amount,
    SUM(order_amount) OVER (
        PARTITION BY customer_id
    ) AS customer_revenue
FROM orders;

--
-- ### Understand the three main clauses
--
-- - `PARTITION BY`: starts a separate calculation group.
-- - `ORDER BY`: defines row sequence inside each group.
-- - Frame clause: defines which ordered rows contribute to a frame calculation.
--
-- ### Example A — Ranking with deterministic tie-breaking
--

WITH sales(employee_id, team, revenue) AS (
    VALUES
        (1, 'A', 900::numeric),
        (2, 'A', 900::numeric),
        (3, 'A', 700::numeric),
        (4, 'B', 800::numeric),
        (5, 'B', 800::numeric)
)
SELECT
    employee_id,
    team,
    revenue,
    ROW_NUMBER() OVER (
        PARTITION BY team
        ORDER BY revenue DESC, employee_id
    ) AS row_num,
    RANK() OVER (
        PARTITION BY team
        ORDER BY revenue DESC
    ) AS rank_value,
    DENSE_RANK() OVER (
        PARTITION BY team
        ORDER BY revenue DESC
    ) AS dense_rank_value
FROM sales
ORDER BY team, row_num;

--
-- - `ROW_NUMBER` assigns a unique sequence to every row.
-- - `RANK` gives tied rows equal ranks and leaves gaps.
-- - `DENSE_RANK` gives tied rows equal ranks without gaps.
-- - The extra `employee_id` ordering makes `ROW_NUMBER` deterministic.
--
-- ### Example B — Running total
--

WITH daily_sales(sale_date, revenue) AS (
    VALUES
        (DATE '2026-01-01', 100::numeric),
        (DATE '2026-01-02', 150::numeric),
        (DATE '2026-01-03', 80::numeric)
)
SELECT
    sale_date,
    revenue,
    SUM(revenue) OVER (
        ORDER BY sale_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_revenue
FROM daily_sales
ORDER BY sale_date;

--
-- The frame starts at the first ordered row and ends at the current row.
--
-- ### Example C — Rolling three-row average
--

WITH daily_sales(sale_date, revenue) AS (
    VALUES
        (DATE '2026-01-01', 100::numeric),
        (DATE '2026-01-02', 150::numeric),
        (DATE '2026-01-03', 80::numeric),
        (DATE '2026-01-04', 170::numeric)
)
SELECT
    sale_date,
    revenue,
    AVG(revenue) OVER (
        ORDER BY sale_date
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS rolling_3_row_average
FROM daily_sales
ORDER BY sale_date;

--
-- **Important:** This is a three-row window, not necessarily three calendar days. Missing dates require a date spine first.
--
-- ### Example D — Compare each event with the previous event
--

WITH events(customer_id, event_ts, amount) AS (
    VALUES
        (10, TIMESTAMP '2026-01-01 10:00', 100::numeric),
        (10, TIMESTAMP '2026-01-03 10:00', 125::numeric),
        (10, TIMESTAMP '2026-01-06 10:00', 90::numeric)
)
SELECT
    customer_id,
    event_ts,
    amount,
    LAG(amount) OVER (
        PARTITION BY customer_id
        ORDER BY event_ts
    ) AS previous_amount,
    amount - LAG(amount) OVER (
        PARTITION BY customer_id
        ORDER BY event_ts
    ) AS change_from_previous
FROM events
ORDER BY customer_id, event_ts;

--
-- `LAG` returns the previous ordered row's value. It does not automatically mean the previous day.
--
-- ### Example E — Share of group total
--

SELECT
    department,
    employee_id,
    salary,
    SUM(salary) OVER (PARTITION BY department) AS department_payroll,
    salary / NULLIF(
        SUM(salary) OVER (PARTITION BY department), 0
    ) AS share_of_department_payroll
FROM employees;

--
-- `NULLIF(..., 0)` prevents division by zero. Cast or use `NUMERIC` when fractional precision matters.
--
-- ### Example F — Find each customer's latest order
--

WITH ranked_orders AS (
    SELECT
        o.*,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY order_ts DESC, order_id DESC
        ) AS rn
    FROM orders o
)
SELECT *
FROM ranked_orders
WHERE rn = 1;

--
-- A stable tie-breaker is essential when timestamps can match.
--
-- ---
--
-- ## 30. Recursive SQL: easy, medium, advanced
--
-- A recursive CTE has two parts:
--
-- - **Anchor member:** the starting rows.
-- - **Recursive member:** finds the next rows using prior results.
-- - `UNION ALL`: combines each iteration's rows.
-- - A termination condition must eventually stop recursion.
--
-- ### Easy — Generate integers [PostgreSQL]
--

WITH RECURSIVE numbers(n) AS (
    SELECT 1

    UNION ALL

    SELECT n + 1
    FROM numbers
    WHERE n < 10
)
SELECT n
FROM numbers
ORDER BY n;

--
-- **Result:** integers from 1 through 10.
--
-- The anchor returns `1`. Each iteration adds one. Recursion stops at `10`.
--
-- ### Medium — Build an employee reporting hierarchy [PostgreSQL]
--

CREATE TEMP TABLE employee_hierarchy (
    employee_id integer PRIMARY KEY,
    employee_name text NOT NULL,
    manager_id integer
);

INSERT INTO employee_hierarchy VALUES
    (1, 'Asha', NULL),
    (2, 'Bharat', 1),
    (3, 'Charu', 1),
    (4, 'Dev', 2),
    (5, 'Esha', 2),
    (6, 'Farhan', 4);

WITH RECURSIVE org_tree AS (
    -- Anchor: begin at the top-level manager.
    SELECT
        employee_id,
        employee_name,
        manager_id,
        0 AS depth,
        employee_name::text AS reporting_path
    FROM employee_hierarchy
    WHERE manager_id IS NULL

    UNION ALL

    -- Recursive member: find direct reports of prior rows.
    SELECT
        e.employee_id,
        e.employee_name,
        e.manager_id,
        t.depth + 1,
        t.reporting_path || ' -> ' || e.employee_name
    FROM employee_hierarchy e
    JOIN org_tree t
      ON e.manager_id = t.employee_id
)
SELECT *
FROM org_tree
ORDER BY depth, employee_id;

--
-- **How to read the result**
--
-- - `depth = 0` identifies the root.
-- - `depth = 1` identifies direct reports.
-- - `depth = 2` identifies second-level reports.
-- - `reporting_path` shows the route from the root to each employee.
--
-- **Business uses:** org charts, category trees, account hierarchies, product bills of materials, and dependency graphs.
--
-- ### Advanced — Traverse a graph and detect cycles [PostgreSQL]
--

CREATE TEMP TABLE graph_edges (
    parent_id integer NOT NULL,
    child_id integer NOT NULL
);

INSERT INTO graph_edges VALUES
    (1, 2), (2, 3), (3, 4), (4, 2), (1, 5);

WITH RECURSIVE walk AS (
    SELECT
        1 AS node_id,
        ARRAY[1] AS path,
        false AS is_cycle,
        0 AS depth

    UNION ALL

    SELECT
        e.child_id,
        w.path || e.child_id,
        e.child_id = ANY(w.path) AS is_cycle,
        w.depth + 1
    FROM walk w
    JOIN graph_edges e
      ON e.parent_id = w.node_id
    WHERE NOT w.is_cycle
      AND w.depth < 20
)
SELECT
    node_id,
    path,
    is_cycle,
    depth
FROM walk
ORDER BY depth, path;

--
-- The graph includes a cycle: `2 → 3 → 4 → 2`.
--
-- - `path` stores the visited nodes.
-- - `child_id = ANY(path)` detects a revisit.
-- - `WHERE NOT w.is_cycle` stops expanding a detected cycle.
-- - The depth limit is an additional defensive guard, not a replacement for cycle detection.
--
-- PostgreSQL also supports the SQL-standard `CYCLE` clause in supported versions. Verify your deployed PostgreSQL version before using it.
--
-- **Redshift note:** Do not assume PostgreSQL recursive-CTE features transfer unchanged. Confirm the current Redshift recursive CTE syntax and limits before adapting graph traversal.
--
-- ---
--
-- ## 31. Data annotation and database documentation
--
-- **Data annotation** means adding useful context to datasets, columns, values, or quality results. The term is broad; it is not one SQL command.
--
-- Useful annotations include:
--
-- - Business definition and calculation rules.
-- - Unit of measure and permitted values.
-- - Data owner and source system.
-- - PII/sensitivity classification.
-- - Refresh cadence and freshness expectation.
-- - Primary key, grain, and relationship semantics.
--
-- ### PostgreSQL: annotate tables and columns
--

COMMENT ON TABLE mart.monthly_revenue IS
'One row per calendar month and sales region.';

COMMENT ON COLUMN mart.monthly_revenue.net_revenue IS
'Net revenue in INR after discounts and refunds.';

COMMENT ON COLUMN mart.monthly_revenue.month_start IS
'First calendar day of the reporting month.';

--
-- ### Create a lightweight data dictionary
--

SELECT
    table_schema,
    table_name,
    column_name,
    data_type,
    is_nullable,
    ordinal_position
FROM information_schema.columns
WHERE table_schema IN ('src', 'stg', 'core', 'mart')
ORDER BY table_schema, table_name, ordinal_position;

--
-- `information_schema` exposes structural metadata. Table comments and column comments can be queried from PostgreSQL system catalogs.
--
-- **Practical distinction:** Data annotation explains what data means. Schema enforcement controls what data is allowed.
--
-- ---
--
-- ## 32. Schema enforcement: reject bad structures before publishing
--
-- Schema enforcement has multiple layers:
--
-- 1. **Structural:** expected columns, types, and nullability.
-- 2. **Relational:** keys and relationships.
-- 3. **Domain:** allowed values and valid ranges.
-- 4. **Semantic:** business rules and valid combinations.
-- 5. **Operational:** source version, freshness, row-count and reconciliation checks.
--
-- ### Example A — Enforce valid records in PostgreSQL
--

CREATE SCHEMA IF NOT EXISTS core;

CREATE TABLE core.orders (
    order_id bigint PRIMARY KEY,
    customer_id bigint NOT NULL,
    order_ts timestamptz NOT NULL,
    order_amount numeric(12, 2) NOT NULL
        CHECK (order_amount >= 0),
    order_status text NOT NULL
        CHECK (order_status IN ('pending', 'paid', 'cancelled'))
);

--
-- PostgreSQL checks the declared constraints during writes. A foreign key can enforce customer existence when a matching referenced table and constraint are defined.
--
-- ### Example B — Profile data before casting
--

WITH staged AS (
    SELECT
        raw_order_id,
        raw_amount,
        raw_status
    FROM stg.orders_raw
)
SELECT
    COUNT(*) AS source_rows,
    COUNT(*) FILTER (
        WHERE NULLIF(BTRIM(raw_order_id), '') IS NULL
    ) AS missing_order_ids,
    COUNT(*) FILTER (
        WHERE NULLIF(BTRIM(raw_amount), '') IS NULL
    ) AS missing_amounts,
    COUNT(*) FILTER (
        WHERE raw_status NOT IN ('pending', 'paid', 'cancelled')
           OR raw_status IS NULL
    ) AS invalid_statuses
FROM staged;

--
-- This is a **profiling check**, not a complete validation. It does not yet prove that numeric strings are parseable or IDs are unique.
--
-- ### Example C — Detect duplicates before enforcing uniqueness
--

SELECT
    order_id,
    COUNT(*) AS row_count
FROM stg.orders_clean
GROUP BY order_id
HAVING COUNT(*) > 1;

--
-- Resolve duplicate business keys before creating or loading a unique target. Do not use `DISTINCT` automatically; decide which source record is authoritative.
--
-- ### Example D — Quarantine invalid records
--

CREATE TABLE IF NOT EXISTS audit.order_rejects (
    batch_id text NOT NULL,
    rejected_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    source_order_id text,
    rejection_reason text NOT NULL,
    raw_payload jsonb
);

--
-- A production pipeline should preserve enough source information to diagnose rejects. Avoid storing sensitive payloads unnecessarily.
--
-- ---
--
-- ## 33. Continuous data quality monitoring (CDQM)
--
-- **Terminology note:** “CDQM” is not a universal PostgreSQL or Redshift SQL command. This guide uses it to mean *continuous data quality monitoring*. If your organization defines the acronym differently, retain its internal definition.
--
-- Monitor quality at three points:
--
-- - **Before load:** schema, source availability, file format, and required columns.
-- - **During transformation:** type validity, duplicate keys, valid relationships, business rules.
-- - **After publish:** row-count reconciliation, totals, freshness, drift, and consumer-facing checks.
--
-- ### A. Create an audit table
--

CREATE SCHEMA IF NOT EXISTS audit;

CREATE TABLE IF NOT EXISTS audit.data_quality_results (
    run_id text NOT NULL,
    checked_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    schema_name text NOT NULL,
    table_name text NOT NULL,
    rule_name text NOT NULL,
    failed_rows bigint NOT NULL,
    checked_rows bigint NOT NULL,
    status text NOT NULL CHECK (status IN ('PASS', 'FAIL', 'WARN')),
    details text
);

--
-- ### B. Record a duplicate-key rule [PostgreSQL]
--

WITH duplicate_keys AS (
    SELECT order_id
    FROM stg.orders_clean
    GROUP BY order_id
    HAVING COUNT(*) > 1
),
counts AS (
    SELECT
        (SELECT COUNT(*) FROM duplicate_keys) AS failed_rows,
        (SELECT COUNT(*) FROM stg.orders_clean) AS checked_rows
)
INSERT INTO audit.data_quality_results (
    run_id, schema_name, table_name, rule_name,
    failed_rows, checked_rows, status, details
)
SELECT
    'orders_batch_2026_10_17',
    'stg',
    'orders_clean',
    'unique_order_id',
    failed_rows,
    checked_rows,
    CASE WHEN failed_rows = 0 THEN 'PASS' ELSE 'FAIL' END,
    'Expected one row per order_id.'
FROM counts;

--
-- ### C. Monitor freshness
--

SELECT
    MAX(source_updated_at) AS newest_source_record,
    CURRENT_TIMESTAMP - MAX(source_updated_at) AS data_lag
FROM core.orders;

--
-- Set the acceptable lag based on the service-level objective. Freshness thresholds differ between hourly and daily pipelines.
--
-- ### D. Monitor source-to-target reconciliation
--

SELECT
    (SELECT COUNT(*) FROM stg.orders_clean) AS staged_rows,
    (SELECT COUNT(*) FROM core.orders) AS target_rows,
    (SELECT COALESCE(SUM(order_amount), 0)
       FROM stg.orders_clean) AS staged_amount,
    (SELECT COALESCE(SUM(order_amount), 0)
       FROM core.orders) AS target_amount;

--
-- Count equality alone does not prove equivalence. Reconcile by batch, business key, and important numeric control totals.
--
-- ### E. Production response pattern
--
-- 1. Persist the failed rule, batch ID, and affected count.
-- 2. Prevent invalid data from reaching trusted marts.
-- 3. Quarantine rows when safe and operationally appropriate.
-- 4. Alert the responsible owner.
-- 5. Retry only after fixing the cause.
-- 6. Record the rerun and its outcome.
--
-- AWS Glue Data Quality provides managed rules, monitoring, and alert integrations. It is a separate AWS service, not a built-in Redshift SQL statement.
--
-- ---
--
-- ## 34. Watermarks and incremental loading
--
-- **Definition:** A watermark records the progress boundary for incremental extraction or processing.
--
-- A simple watermark may be a source update timestamp. More robust designs can use CDC log positions, monotonically increasing sequence values, or compound ordering keys.
--
-- ### Why watermarks exist
--
-- Without a watermark, every run may reread the full source. With a watermark, the pipeline reads a bounded change set.
--
-- However, `updated_at > last_watermark` can miss tied timestamps, late commits, and deletes. Use an overlap window or a source change log when required.
--
-- ### A. Create watermark control and batch audit tables [PostgreSQL]
--

CREATE TABLE IF NOT EXISTS audit.pipeline_watermarks (
    pipeline_name text PRIMARY KEY,
    last_successful_watermark timestamptz,
    updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS audit.pipeline_runs (
    run_id text PRIMARY KEY,
    pipeline_name text NOT NULL,
    started_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at timestamptz,
    lower_watermark timestamptz,
    upper_watermark timestamptz,
    status text NOT NULL CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED')),
    rows_read bigint,
    rows_written bigint,
    error_message text
);

--
-- ### B. Use a fixed extraction window
--
-- The following is a template. Replace the placeholder values through a parameterized application or orchestration tool.
--

-- Example extraction boundary:
-- lower_watermark = '2026-10-16 00:00:00+00'
-- upper_watermark = '2026-10-17 00:00:00+00'

SELECT *
FROM src.orders
WHERE source_updated_at >= TIMESTAMPTZ '2026-10-16 00:00:00+00'
  AND source_updated_at <  TIMESTAMPTZ '2026-10-17 00:00:00+00';

--
-- Use half-open intervals: inclusive lower bound, exclusive upper bound. Adjacent batches then avoid overlapping boundary records.
--
-- ### C. Add a lookback window for late-arriving changes
--

-- Re-read the last 2 hours to catch some delayed updates.
SELECT *
FROM src.orders
WHERE source_updated_at >= :last_successful_watermark - INTERVAL '2 hours'
  AND source_updated_at <  :current_upper_watermark;

--
-- This is conceptual parameter syntax. Bind values through your client; do not paste `:name` into every SQL console and assume it is supported.
--
-- **Important:** An overlap window deliberately rereads rows. The target load must be idempotent, typically using a stable business key and an upsert/merge strategy.
--
-- ### D. Safe watermark advancement
--
-- Advance the watermark only after:
--
-- 1. Extraction completes.
-- 2. Schema and data-quality checks pass.
-- 3. The target merge completes.
-- 4. Reconciliation succeeds.
-- 5. The run is marked successful.
--
-- Do not advance it merely because extraction started. Failed batches must be safely replayable.
--
-- ### E. Timestamp ties and compound watermarks
--
-- If many records share the same timestamp, use a stable tie-breaker such as `(source_updated_at, source_id)` when the source supports it. Persist both values and compare them lexicographically.
--
-- For deletes, a timestamp on the current source table is often insufficient. Use CDC tombstones, delete logs, snapshots with comparison, or another explicit deletion-capture strategy.
--
-- ---
--
-- ## 35. Surrogate keys and dimension design
--
-- **Natural key:** business identity, such as `customer_number`.
--
-- **Surrogate key:** warehouse-generated identity for a dimension row, such as `customer_sk`.
--
-- Surrogate keys are especially important for Type 2 dimensions. One natural customer can have multiple historical dimension rows.
--
-- ### Example dimension table [PostgreSQL]
--

CREATE TABLE IF NOT EXISTS core.dim_customer (
    customer_sk bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id text NOT NULL,
    customer_name text,
    customer_segment text,
    effective_from timestamptz NOT NULL,
    effective_to timestamptz,
    is_current boolean NOT NULL DEFAULT true,
    source_updated_at timestamptz,
    CHECK (effective_to IS NULL OR effective_to > effective_from)
);

--
-- A basic primary key does not prevent two current rows for the same natural key. Add an appropriate partial unique index in PostgreSQL:
--

CREATE UNIQUE INDEX IF NOT EXISTS uq_dim_customer_current
ON core.dim_customer(customer_id)
WHERE is_current = true;

--
-- This is PostgreSQL-specific. Redshift does not enforce ordinary unique indexes in the same way; validate uniqueness in ETL and quality checks.
--
-- ---
--
-- ## 36. SCD implementation templates
--
-- SCD stands for Slowly Changing Dimension. First decide which attributes need history and which business key identifies the entity.
--
-- ### Type 0 — Retain the original value
--
-- Use when the attribute should not change after first capture.
--

-- PostgreSQL example: preserve original acquisition source.
UPDATE core.dim_customer d
SET acquisition_source = s.acquisition_source
FROM stg.customer_changes s
WHERE d.customer_id = s.customer_id
  AND d.acquisition_source IS NULL;

--
-- This updates only missing original values. It is a simplified Type 0 pattern; use a dedicated insert process for new entities.
--
-- ### Type 1 — Overwrite the current value
--
-- History is not retained.
--

-- PostgreSQL UPSERT template.
INSERT INTO core.dim_customer_current (
    customer_id, customer_name, customer_segment
)
SELECT
    customer_id, customer_name, customer_segment
FROM stg.customer_deduplicated
ON CONFLICT (customer_id)
DO UPDATE SET
    customer_name = EXCLUDED.customer_name,
    customer_segment = EXCLUDED.customer_segment;

--
-- The target must have a unique constraint on `customer_id`. `ON CONFLICT` is PostgreSQL syntax, not a universal Redshift replacement.
--
-- ### Type 2 — Preserve full history
--
-- Assume the target contains `customer_sk`, `customer_id`, descriptive attributes, `effective_from`, `effective_to`, and `is_current`.
--
-- **Step 1: identify changed current records**
--

SELECT
    d.customer_id,
    d.customer_name AS old_name,
    s.customer_name AS new_name,
    d.customer_segment AS old_segment,
    s.customer_segment AS new_segment
FROM core.dim_customer d
JOIN stg.customer_deduplicated s
  ON s.customer_id = d.customer_id
WHERE d.is_current
  AND (
       d.customer_name IS DISTINCT FROM s.customer_name
    OR d.customer_segment IS DISTINCT FROM s.customer_segment
  );

--
-- `IS DISTINCT FROM` treats NULL comparisons as a definite true/false result. Verify support before porting it to another engine.
--
-- **Step 2: materialize the exact new/changed rows before expiring anything**
--

CREATE TEMP TABLE customer_type2_changes AS
SELECT
    s.customer_id,
    s.customer_name,
    s.customer_segment,
    s.change_ts,
    s.source_updated_at
FROM stg.customer_deduplicated s
LEFT JOIN core.dim_customer d
  ON d.customer_id = s.customer_id
 AND d.is_current
WHERE d.customer_sk IS NULL
   OR d.customer_name IS DISTINCT FROM s.customer_name
   OR d.customer_segment IS DISTINCT FROM s.customer_segment;

--
-- This work table contains only new entities and changed current records. In production, make sure `stg.customer_deduplicated` has one row per `customer_id`.
--
-- **Step 3: expire the prior current versions**
--

UPDATE core.dim_customer d
SET
    effective_to = c.change_ts,
    is_current = false
FROM customer_type2_changes c
WHERE d.customer_id = c.customer_id
  AND d.is_current
  AND d.effective_from < c.change_ts;

--
-- The strict timestamp comparison protects against zero-length history intervals. Same-timestamp source changes need an explicit business policy.
--
-- **Step 4: insert one new version for every staged change**
--

INSERT INTO core.dim_customer (
    customer_id,
    customer_name,
    customer_segment,
    effective_from,
    effective_to,
    is_current,
    source_updated_at
)
SELECT
    c.customer_id,
    c.customer_name,
    c.customer_segment,
    c.change_ts,
    NULL,
    true,
    c.source_updated_at
FROM customer_type2_changes c;

--
-- Run the materialization, expiration, insertion, and validation as one controlled batch. Where supported, execute the target changes inside a transaction. Add a batch identifier or persistent work table when recovery and auditability require it.
--
-- ### Type 2: safer orchestration outline
--
-- 1. Deduplicate source changes to one authoritative row per natural key.
-- 2. Validate source timestamps and business keys.
-- 3. Identify new keys and changed current rows.
-- 4. Materialize those changes into a batch-scoped work table.
-- 5. Expire old current versions using the work table.
-- 6. Insert one new version for each new or changed key.
-- 7. Validate exactly one current row per natural key.
-- 8. Reconcile inserted and expired counts.
-- 9. Commit and record the batch.
--
-- ### Type 3 — Keep selected previous values in columns
--
-- Use when only a small amount of prior state is needed.
--

UPDATE core.dim_customer_current d
SET
    previous_segment = d.customer_segment,
    customer_segment = s.customer_segment
FROM stg.customer_deduplicated s
WHERE d.customer_id = s.customer_id
  AND d.customer_segment IS DISTINCT FROM s.customer_segment;

--
-- This preserves one previous value, not a full history. Repeated updates overwrite the prior value.
--
-- ### Type 4 — Keep history in a separate table
--
-- Current-state dimension:
--

CREATE TABLE IF NOT EXISTS core.dim_customer_current (
    customer_id text PRIMARY KEY,
    customer_name text,
    customer_segment text,
    source_updated_at timestamptz
);

--
-- History table:
--

CREATE TABLE IF NOT EXISTS core.dim_customer_history (
    history_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id text NOT NULL,
    customer_name text,
    customer_segment text,
    captured_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    change_reason text
);

--
-- Before overwriting the current row, insert the old values into the history table. Ensure that the archive and update happen in the same transaction where supported.
--
-- ### Type 6 — Hybrid Type 1 + Type 2 + Type 3
--
-- Type 6 is implemented differently across organizations. Define the hybrid rules explicitly before coding.
--
-- A common design stores:
-- - Type 2 rows with effective dates and a current-row flag.
-- - Current attribute values repeated on historical rows.
-- - Optional previous-value columns for selected attributes.
--
-- **Example structure**
--

CREATE TABLE IF NOT EXISTS core.dim_customer_type6 (
    customer_sk bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id text NOT NULL,
    customer_name text,
    customer_segment_historical text,
    customer_segment_current text,
    previous_segment text,
    effective_from timestamptz NOT NULL,
    effective_to timestamptz,
    is_current boolean NOT NULL
);

--
-- When a Type 2 change occurs, expire the old row and insert a new version. When the current Type 1 value changes, update that current-value column across all historical rows for the natural key. Update `previous_segment` only according to the agreed Type 3 rule.
--
-- **Validation**
--

SELECT customer_id, COUNT(*) AS current_rows
FROM core.dim_customer_type6
WHERE is_current
GROUP BY customer_id
HAVING COUNT(*) <> 1;

--
-- For zero-current entities, decide whether the entity should be inactive, deleted, or retained with a terminal status. Do not assume every natural key must always have one current row.
--
-- ---
--
-- ## 37. Redshift `MERGE INTO`: safe upsert pattern
--
-- `MERGE` combines matched-row updates and unmatched-row inserts. Redshift supports `MERGE INTO`; PostgreSQL uses different syntax and version-dependent alternatives.
--
-- ### A. Create the target and stage [Redshift]
--

CREATE TABLE IF NOT EXISTS mart.customer_current (
    customer_id BIGINT,
    customer_name VARCHAR(200),
    customer_segment VARCHAR(100),
    source_updated_at TIMESTAMP
)
DISTSTYLE AUTO
SORTKEY AUTO;

CREATE TEMP TABLE stage_customer_changes (
    customer_id BIGINT,
    customer_name VARCHAR(200),
    customer_segment VARCHAR(100),
    source_updated_at TIMESTAMP
);

--
-- ### B. Deduplicate the source before merging
--

CREATE TEMP TABLE stage_customer_one_row AS
SELECT
    customer_id,
    customer_name,
    customer_segment,
    source_updated_at
FROM (
    SELECT
        s.*,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY source_updated_at DESC
        ) AS rn
    FROM stage_customer_changes s
) ranked
WHERE rn = 1;

--
-- If timestamps can tie, add a stable source sequence or deterministic tie-breaker. Do not assume duplicate source rows can be safely merged.
--
-- ### C. Merge into the target
--

MERGE INTO mart.customer_current AS target
USING stage_customer_one_row AS source
ON target.customer_id = source.customer_id

WHEN MATCHED THEN UPDATE SET
    customer_name = source.customer_name,
    customer_segment = source.customer_segment,
    source_updated_at = source.source_updated_at

WHEN NOT MATCHED THEN INSERT (
    customer_id,
    customer_name,
    customer_segment,
    source_updated_at
)
VALUES (
    source.customer_id,
    source.customer_name,
    source.customer_segment,
    source.source_updated_at
);

--
-- ### D. Validate the target after the merge
--

SELECT customer_id, COUNT(*) AS row_count
FROM mart.customer_current
GROUP BY customer_id
HAVING COUNT(*) > 1;

--
-- Redshift primary-key and unique declarations are informational rather than enforced. This validation is therefore important when the pipeline depends on uniqueness.
--
-- **Important:** Redshift's `MERGE` does not allow multiple source rows to match the same target row. Deduplicate the source and verify the target grain before merging. Check current Redshift documentation for the exact syntax supported by your environment.
--
-- ---
--
-- ## 38. Redshift physical design: distribution and sort keys
--
-- Redshift is a distributed, columnar analytical warehouse. Physical design affects data movement, block pruning, and query execution.
--
-- ### Distribution styles
--
-- - `AUTO`: Redshift chooses an appropriate distribution strategy.
-- - `KEY`: rows are distributed using a selected column.
-- - `ALL`: a full table copy is distributed to each node.
-- - `EVEN`: rows are distributed evenly without a chosen key.
--
-- ### Example fact table [Redshift]
--

CREATE TABLE mart.fact_orders (
    order_id BIGINT,
    customer_id BIGINT,
    order_ts TIMESTAMP,
    order_amount DECIMAL(12, 2)
)
DISTSTYLE KEY
DISTKEY (customer_id)
COMPOUND SORTKEY (order_ts, customer_id);

--
-- This is an illustrative design, not a universal recommendation. A customer distribution key may help customer joins but cause skew if customer activity is highly uneven. A timestamp-leading compound sort key can help time-range filtering.
--
-- ### Practical decision process
--
-- 1. Start with `AUTO` unless workload evidence justifies manual choices.
-- 2. Identify the largest tables and most frequent expensive joins.
-- 3. Measure filtered data volume, not just full-table size.
-- 4. Prefer high-cardinality distribution keys with balanced values.
-- 5. Align distribution keys for large tables when joins benefit.
-- 6. Choose sort keys based on common filter and join patterns.
-- 7. Test representative queries before and after the change.
-- 8. Check data skew, query runtime, and data movement in plans.
--
-- ### Inspect table layout
--

SELECT
    "schema",
    "table",
    diststyle,
    sortkey1,
    unsorted,
    stats_off,
    skew_rows
FROM svv_table_info
WHERE "schema" = 'mart'
ORDER BY "table";

--
-- System-view columns can vary with Redshift features and permissions. Confirm the current view schema if a column is unavailable.
--
-- ---
--
-- ## 39. `EXPLAIN`, `ANALYZE`, and query optimization
--
-- These commands have related names but different purposes.
--
-- ### PostgreSQL
--
-- - `EXPLAIN`: displays the estimated execution plan without executing the query.
-- - `EXPLAIN ANALYZE`: executes the query and reports actual runtime and row counts.
-- - `ANALYZE`: refreshes planner statistics.
-- - `VACUUM`: reclaims/reuses dead-tuple space and performs maintenance.
-- - `EXPLAIN (ANALYZE, BUFFERS)`: adds buffer activity for deeper diagnosis.
--

ANALYZE core.orders;

EXPLAIN
SELECT customer_id, SUM(order_amount)
FROM core.orders
WHERE order_ts >= TIMESTAMPTZ '2026-10-01 00:00:00+00'
GROUP BY customer_id;

EXPLAIN (ANALYZE, BUFFERS)
SELECT customer_id, SUM(order_amount)
FROM core.orders
WHERE order_ts >= TIMESTAMPTZ '2026-10-01 00:00:00+00'
GROUP BY customer_id;

--
-- **Caution:** `EXPLAIN ANALYZE` executes the statement. For `UPDATE`, `DELETE`, or other side-effecting statements, use a safe test environment or a carefully managed transaction.
--
-- ### Amazon Redshift
--
-- - `EXPLAIN`: displays a query plan; it does not execute the query.
-- - `ANALYZE`: updates table statistics for the optimizer.
-- - Inspect redistribution operators and actual workload metrics.
-- - Do not expect PostgreSQL's `EXPLAIN (ANALYZE, BUFFERS)` syntax to work in Redshift.
--

ANALYZE mart.fact_orders;

EXPLAIN
SELECT
    f.customer_id,
    SUM(f.order_amount) AS revenue
FROM mart.fact_orders f
WHERE f.order_ts >= TIMESTAMP '2026-10-01 00:00:00'
GROUP BY f.customer_id;

--
-- ### Redshift plan-reading checklist
--
-- - `DS_DIST_NONE`: joined data is collocated; generally desirable.
-- - `DS_DIST_ALL_NONE`: the replicated inner table requires no redistribution.
-- - `DS_BCAST_INNER`: inner table is broadcast to compute nodes.
-- - `DS_DIST_INNER` / `DS_DIST_OUTER`: one side is redistributed.
-- - `DS_DIST_BOTH`: both sides are redistributed; investigate.
-- - Nested-loop joins can signal a missing join predicate or an inequality join.
-- - High-cost sorts and aggregates may be important, but cost is an estimate.
--
-- Do not optimize a single plan label in isolation. Compare representative query timings and verify result equivalence.
--
-- ---
--
-- ## 40. End-to-end incremental pipeline blueprint
--
-- A dependable ETL pipeline is a controlled sequence, not just an `INSERT`.
--
-- ### Pipeline stages
--
-- 1. **Extract:** select a source window using a saved watermark.
-- 2. **Land:** preserve the raw batch and source metadata.
-- 3. **Profile:** count rows, inspect nulls, and identify duplicate keys.
-- 4. **Validate:** check schema, types, domains, and relationships.
-- 5. **Quarantine:** persist rejected rows with reasons.
-- 6. **Transform:** clean strings, cast values, normalize dates, apply rules.
-- 7. **Deduplicate:** retain one authoritative row per business key.
-- 8. **Load:** merge/upsert into the target at the correct grain.
-- 9. **Reconcile:** compare counts, keys, totals, and batch coverage.
-- 10. **Commit:** mark the run successful and advance the watermark.
-- 11. **Observe:** record runtime, errors, freshness, and quality metrics.
--
-- ### Pseudocode
--

read previous successful watermark
choose a stable upper boundary
extract [lower boundary, upper boundary)
write raw batch and batch ID
validate schema and required fields
split valid records from rejected records
deduplicate valid records by business key
transform valid records
merge into the target idempotently
reconcile source and target controls
if all critical checks pass:
    mark batch successful
    advance watermark
else:
    mark batch failed
    retain diagnostics
    do not advance watermark

--
-- ### Minimum operational metadata
--
-- - Pipeline name and run ID.
-- - Source and target names.
-- - Lower and upper watermark.
-- - Start/end timestamps and status.
-- - Rows read, accepted, rejected, inserted, and updated.
-- - Data-quality rule outcomes.
-- - Reconciliation differences.
-- - Error details and retry count.
-- - Code or schema version.
--
-- ---
--
-- ## 41. Common traps in advanced SQL and warehouse engineering
--
-- - **Window function:** always confirm the partition, ordering, and frame.
-- - **Recursive CTE:** prove termination and handle cycles.
-- - **SCD Type 2:** expire the old version and insert the new version exactly once.
-- - **Surrogate key:** do not confuse it with the natural/business key.
-- - **Watermark:** do not advance it before a successful target commit and validation.
-- - **Late-arriving data:** use an overlap window or CDC design where needed.
-- - **MERGE:** ensure one authoritative source row per match key.
-- - **Schema enforcement:** distinguish hard constraints from monitoring checks.
-- - **CDQM:** define rules, thresholds, owners, alert paths, and failure actions.
-- - **Redshift distribution:** optimize data movement without creating skew.
-- - **Sort key:** optimize common access patterns, not every possible query.
-- - **ANALYZE:** refresh statistics when data changes enough to affect planning.
-- - **EXPLAIN:** use plans to form hypotheses, then measure.
-- - **Data annotation:** document grain, units, definitions, owners, and sensitivity.
-- - **Typecasting:** validate malformed values before casting.
-- - **Aggregation:** establish row grain before joins and metric calculations.
--
-- ---
--
-- ## 42. Official references
--
-- Use official documentation as the final authority when syntax differs by engine or release.
--
-- ### PostgreSQL
--
-- - Recursive queries and cycle detection: https://www.postgresql.org/docs/current/queries-with.html
-- - Window functions: https://www.postgresql.org/docs/current/functions-window.html
-- - Window function tutorial: https://www.postgresql.org/docs/current/tutorial-window.html
-- - Query plans and `EXPLAIN`: https://www.postgresql.org/docs/current/using-explain.html
-- - `ANALYZE`: https://www.postgresql.org/docs/current/sql-analyze.html
--
-- ### Amazon Redshift
--
-- - `MERGE`: https://docs.aws.amazon.com/redshift/latest/dg/r_MERGE.html
-- - `EXPLAIN`: https://docs.aws.amazon.com/redshift/latest/dg/r_EXPLAIN.html
-- - Analyze table statistics: https://docs.aws.amazon.com/redshift/latest/dg/r_ANALYZE.html
-- - Distribution styles: https://docs.aws.amazon.com/redshift/latest/dg/c_choosing_dist_sort.html
-- - Sort keys: https://docs.aws.amazon.com/redshift/latest/dg/t_Sorting_data.html
-- - Query-plan analysis: https://docs.aws.amazon.com/redshift/latest/dg/c-analyzing-the-query-plan.html
-- - Constraint behaviour: https://docs.aws.amazon.com/redshift/latest/dg/t_Defining_constraints.html
-- - AWS Glue Data Quality: https://docs.aws.amazon.com/glue/latest/dg/glue-data-quality.html
--
-- ---
--
-- ## 43. Recommended implementation order
--
-- 1. Run casting, `WHERE`/`HAVING`, and window-function examples.
-- 2. Run all three recursive CTE examples in PostgreSQL.
-- 3. Build schema constraints and data-quality audit tables.
-- 4. Add annotations and a basic data dictionary.
-- 5. Implement watermark control and replayable batch extraction.
-- 6. Implement Type 1, then Type 2, then Type 3/4/6.
-- 7. Run the Redshift `MERGE` example after source deduplication.
-- 8. Compare `EXPLAIN` plans before and after physical-design changes.
-- 9. Add run logging, alerting, and reconciliation.
-- 10. Commit each module separately with tests and a clear README.
--
-- **Core engineering principle:** Correct grain → deterministic transformations → enforced or tested quality → idempotent loads → reconciliation → measured optimization.

-- ============================================================================
-- EXECUTABLE LAB APPENDIX A — SAFE SANDBOX AND FOUNDATIONS
-- ============================================================================

-- [A01] Create a dedicated schema. Run inside a practice database.
CREATE SCHEMA IF NOT EXISTS sql_lab;
SET search_path TO sql_lab, public;

-- [A02] A compact e-commerce model; grain is documented in comments.
-- customers: one row per customer
CREATE TABLE IF NOT EXISTS customers (
    customer_id       BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    email             TEXT NOT NULL,
    full_name         TEXT,
    city              TEXT,
    country_code      CHAR(2) NOT NULL DEFAULT 'IN',
    created_at        TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    is_active         BOOLEAN NOT NULL DEFAULT TRUE,
    CONSTRAINT uq_customers_email UNIQUE (email)
);
-- products: one row per sellable product/SKU
CREATE TABLE IF NOT EXISTS products (
    product_id        BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    sku               TEXT NOT NULL UNIQUE,
    product_name      TEXT NOT NULL,
    category          TEXT NOT NULL,
    list_price        NUMERIC(12,2) NOT NULL CHECK (list_price >= 0),
    is_active         BOOLEAN NOT NULL DEFAULT TRUE,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
-- orders: one row per order; amounts are currency values, not floating point.
CREATE TABLE IF NOT EXISTS orders (
    order_id          BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    customer_id       BIGINT NOT NULL REFERENCES customers(customer_id),
    order_ts          TIMESTAMPTZ NOT NULL,
    order_status      TEXT NOT NULL CHECK (order_status IN
                         ('created','paid','shipped','delivered','cancelled','refunded')),
    currency_code     CHAR(3) NOT NULL DEFAULT 'INR',
    subtotal          NUMERIC(14,2) NOT NULL CHECK (subtotal >= 0),
    discount_amount   NUMERIC(14,2) NOT NULL DEFAULT 0 CHECK (discount_amount >= 0),
    tax_amount        NUMERIC(14,2) NOT NULL DEFAULT 0 CHECK (tax_amount >= 0),
    shipping_amount   NUMERIC(14,2) NOT NULL DEFAULT 0 CHECK (shipping_amount >= 0),
    order_total       NUMERIC(14,2) NOT NULL CHECK (order_total >= 0),
    created_at        TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
-- order_items: one row per product line within an order.
CREATE TABLE IF NOT EXISTS order_items (
    order_item_id     BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    order_id          BIGINT NOT NULL REFERENCES orders(order_id),
    product_id        BIGINT NOT NULL REFERENCES products(product_id),
    quantity          INTEGER NOT NULL CHECK (quantity > 0),
    unit_price        NUMERIC(12,2) NOT NULL CHECK (unit_price >= 0),
    line_discount     NUMERIC(12,2) NOT NULL DEFAULT 0 CHECK (line_discount >= 0),
    CONSTRAINT uq_order_product UNIQUE (order_id, product_id)
);
-- payments: one row per payment attempt/transaction, not one row per order.
CREATE TABLE IF NOT EXISTS payments (
    payment_id        BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    order_id          BIGINT NOT NULL REFERENCES orders(order_id),
    payment_ts        TIMESTAMPTZ,
    payment_method    TEXT,
    payment_status    TEXT NOT NULL CHECK (payment_status IN
                         ('initiated','authorized','captured','failed','refunded')),
    amount            NUMERIC(14,2) NOT NULL CHECK (amount >= 0),
    provider_txn_id   TEXT UNIQUE
);

-- CRUD: CREATE / INSERT, READ / SELECT, UPDATE, DELETE.
INSERT INTO customers(email, full_name, city) VALUES
 ('asha@example.com','Asha Rao','Bengaluru'),
 ('dev@example.com','Dev Singh','Pune')
ON CONFLICT (email) DO NOTHING;
INSERT INTO products(sku, product_name, category, list_price) VALUES
 ('SKU-COF-001','Ground Coffee','Grocery',350.00),
 ('SKU-BTL-002','Steel Bottle','Home',799.00)
ON CONFLICT (sku) DO NOTHING;

-- Read: select only required columns; qualify predicates explicitly.
SELECT customer_id, email, city, created_at
FROM customers
WHERE is_active IS TRUE
ORDER BY created_at DESC;

-- Update: target a deterministic key and inspect affected rows.
UPDATE customers SET city = 'Bengaluru', updated_at = CURRENT_TIMESTAMP
WHERE email = 'asha@example.com';

-- Delete: demonstrate only in a sandbox. Prefer soft deletion when history matters.
DELETE FROM customers WHERE email = 'dev@example.com';

-- Transactions and ACID: demonstrate atomic multi-table changes.
BEGIN;
INSERT INTO orders(customer_id, order_ts, order_status, subtotal, order_total)
SELECT customer_id, CURRENT_TIMESTAMP, 'created', 350.00, 350.00
FROM customers WHERE email = 'asha@example.com';
-- Inspect before COMMIT in real workflows. ROLLBACK cancels all changes in transaction.
COMMIT;

-- [A03] Data type and casting patterns.
SELECT
  CAST('2026-10-17' AS DATE) AS parsed_date,
  '125.50'::NUMERIC(12,2) AS parsed_amount,
  NULLIF('  ', '') AS nullif_only_exact_empty,
  NULLIF(TRIM('  '), '') AS normalized_blank_to_null,
  COALESCE(NULLIF(TRIM(''), ''), 'unknown') AS safe_label,
  CASE WHEN 10 = 0 THEN NULL ELSE 100.0 / 10 END AS safe_ratio;

-- [A04] Text cleaning: normalize case/space, and guard against bad inputs.
SELECT
  LOWER(TRIM('  Asha Rao  ')) AS normalized_name,
  REGEXP_REPLACE('Order # IN-00123', '[^A-Za-z0-9]+', '', 'g') AS compact_key,
  SPLIT_PART('IN-BLR-0007', '-', 2) AS city_code,
  LENGTH(TRIM('analytics')) AS character_count;

-- [A05] Date/time: use half-open intervals for timestamp filtering.
-- This includes all events on 2026-10-01 without relying on 23:59:59 precision.
SELECT order_id, order_ts, order_total
FROM orders
WHERE order_ts >= TIMESTAMPTZ '2026-10-01 00:00:00+05:30'
  AND order_ts <  TIMESTAMPTZ '2026-10-02 00:00:00+05:30';

-- [A06] Joins and aggregation. Avoid fanout by aggregating each many-side first.
WITH item_totals AS (
  SELECT order_id, SUM(quantity * unit_price - line_discount) AS item_value
  FROM order_items GROUP BY order_id
), payment_totals AS (
  SELECT order_id, SUM(amount) FILTER (WHERE payment_status = 'captured') AS captured_value
  FROM payments GROUP BY order_id
)
SELECT o.order_id, o.order_total, i.item_value, p.captured_value
FROM orders o
LEFT JOIN item_totals i USING (order_id)
LEFT JOIN payment_totals p USING (order_id);

-- [A07] Conditional aggregation and FILTER.
SELECT
  COUNT(*) AS all_orders,
  COUNT(*) FILTER (WHERE order_status = 'delivered') AS delivered_orders,
  COUNT(*) FILTER (WHERE order_status = 'cancelled') AS cancelled_orders,
  SUM(order_total) FILTER (WHERE order_status IN ('paid','shipped','delivered')) AS gross_booked_value
FROM orders;

-- [A08] CTEs: name steps to make logic auditable.
WITH eligible_orders AS (
  SELECT * FROM orders WHERE order_status IN ('paid','shipped','delivered')
), customer_revenue AS (
  SELECT customer_id, COUNT(*) AS orders_count, SUM(order_total) AS revenue
  FROM eligible_orders GROUP BY customer_id
)
SELECT c.customer_id, c.full_name, r.orders_count, r.revenue
FROM customer_revenue r JOIN customers c USING(customer_id)
ORDER BY r.revenue DESC NULLS LAST;

-- [A09] Window function: ranking, running totals, previous-period value.
WITH daily_sales AS (
  SELECT order_ts::date AS order_date, SUM(order_total) AS revenue
  FROM orders WHERE order_status IN ('paid','shipped','delivered')
  GROUP BY order_ts::date
)
SELECT order_date, revenue,
       SUM(revenue) OVER (ORDER BY order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_revenue,
       LAG(revenue) OVER (ORDER BY order_date) AS previous_observed_day_revenue,
       revenue - LAG(revenue) OVER (ORDER BY order_date) AS absolute_change
FROM daily_sales ORDER BY order_date;

-- [A10] Period comparison: calendar alignment matters.
WITH monthly AS (
  SELECT DATE_TRUNC('month', order_ts)::date AS month_start,
         SUM(order_total) AS revenue
  FROM orders WHERE order_status IN ('paid','shipped','delivered')
  GROUP BY 1
), compared AS (
  SELECT month_start, revenue,
         LAG(revenue) OVER (ORDER BY month_start) AS previous_observed_month
  FROM monthly
)
SELECT month_start, revenue, previous_observed_month,
       revenue - previous_observed_month AS revenue_change,
       (revenue - previous_observed_month) / NULLIF(previous_observed_month, 0) AS fractional_change
FROM compared ORDER BY month_start;

-- [A11] Funnel and conversion rates. Denominator must match the business definition.
-- Example event table grain: one row per user event.
CREATE TABLE IF NOT EXISTS web_events (
  event_id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
  user_id BIGINT NOT NULL,
  event_ts TIMESTAMPTZ NOT NULL,
  event_name TEXT NOT NULL,
  session_id TEXT
);
WITH user_flags AS (
 SELECT user_id,
        BOOL_OR(event_name = 'product_view') AS viewed_product,
        BOOL_OR(event_name = 'add_to_cart') AS added_to_cart,
        BOOL_OR(event_name = 'purchase') AS purchased
 FROM web_events GROUP BY user_id
)
SELECT COUNT(*) FILTER (WHERE viewed_product) AS product_viewers,
       COUNT(*) FILTER (WHERE added_to_cart) AS cart_users,
       COUNT(*) FILTER (WHERE purchased) AS purchasers,
       COUNT(*) FILTER (WHERE purchased)::NUMERIC / NULLIF(COUNT(*),0) AS user_purchase_rate
FROM user_flags;

-- [A12] A/B testing SQL preparation: one randomized unit per row is essential.
-- This query computes experiment-level counts; statistical testing is performed separately.
CREATE TABLE IF NOT EXISTS experiment_assignments (
  experiment_id TEXT NOT NULL,
  user_id BIGINT NOT NULL,
  variant TEXT NOT NULL CHECK (variant IN ('control','treatment')),
  assigned_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY(experiment_id, user_id)
);
CREATE TABLE IF NOT EXISTS experiment_outcomes (
  experiment_id TEXT NOT NULL,
  user_id BIGINT NOT NULL,
  converted BOOLEAN NOT NULL,
  order_value NUMERIC(12,2),
  PRIMARY KEY(experiment_id, user_id)
);
SELECT a.experiment_id, a.variant,
       COUNT(*) AS randomized_users,
       COUNT(*) FILTER (WHERE o.converted) AS converters,
       COUNT(*) FILTER (WHERE o.converted)::NUMERIC / NULLIF(COUNT(*),0) AS conversion_rate,
       AVG(o.order_value) FILTER (WHERE o.order_value IS NOT NULL) AS mean_order_value
FROM experiment_assignments a
LEFT JOIN experiment_outcomes o USING(experiment_id,user_id)
WHERE a.experiment_id = 'landing_page_v1'
GROUP BY a.experiment_id, a.variant ORDER BY a.variant;

-- ============================================================================
-- EXECUTABLE LAB APPENDIX B — DATA QUALITY, INCREMENTAL LOADS, AND ENGINEERING
-- ============================================================================

-- [B01] Basic quality checks. Each query should return zero rows unless noted.
-- Duplicate natural keys:
SELECT LOWER(TRIM(email)) AS normalized_email, COUNT(*)
FROM customers GROUP BY 1 HAVING COUNT(*) > 1;
-- Invalid monetary values:
SELECT order_id FROM orders WHERE subtotal < 0 OR discount_amount < 0 OR order_total < 0;
-- Orphaned keys (normally prevented by foreign keys; useful in raw/staging data):
SELECT s.order_id FROM staging_order_items s LEFT JOIN orders o USING(order_id) WHERE o.order_id IS NULL;
-- Order total reconciliation (tolerance depends on tax/shipping/business rules):
SELECT order_id, subtotal - discount_amount + tax_amount + shipping_amount AS recomputed_total,
       order_total
FROM orders
WHERE ABS((subtotal - discount_amount + tax_amount + shipping_amount) - order_total) > 0.01;

-- [B02] Audit and reject/quarantine pattern.
CREATE TABLE IF NOT EXISTS data_quality_issues (
  issue_id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
  source_name TEXT NOT NULL,
  source_record_key TEXT,
  rule_name TEXT NOT NULL,
  severity TEXT NOT NULL CHECK (severity IN ('info','warning','error','critical')),
  observed_value TEXT,
  detected_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
  run_id TEXT NOT NULL
);
INSERT INTO data_quality_issues(source_name, source_record_key, rule_name, severity, observed_value, run_id)
SELECT 'orders', order_id::text, 'negative_order_total', 'error', order_total::text, 'manual_run_001'
FROM orders WHERE order_total < 0;

-- [B03] Watermark control table. Store high-watermark per pipeline/source.
CREATE TABLE IF NOT EXISTS etl_watermarks (
  pipeline_name TEXT NOT NULL,
  source_name TEXT NOT NULL,
  watermark_ts TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY(pipeline_name, source_name)
);
INSERT INTO etl_watermarks(pipeline_name, source_name, watermark_ts)
VALUES ('orders_incremental','orders_source', TIMESTAMPTZ '2000-01-01 00:00:00+00')
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
