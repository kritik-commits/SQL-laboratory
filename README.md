# SQL Laboratory

A practical SQL learning repository for PostgreSQL and Amazon Redshift. The goal is to progress from query fundamentals to analytics engineering, dimensional modelling, data quality, incremental pipelines, and query optimization.

## Repository layout

```text
SQL-laboratory/
├── README.md
├── PostgreSQL/
│   └── postgresql_analytics.sql
├── Redshift/
│   └── redshift_analytics.sql
├── references/
│   └── official_documentation.md
└── .gitignore
```

## Learning path

1. **Foundations:** types, NULLs, casting, predicates, `WHERE`, `HAVING`, and aggregation.
2. **Analytical SQL:** joins, CTEs, window functions, ranking, running totals, and time-series calculations.
3. **Advanced querying:** recursive CTEs, hierarchy traversal, graph traversal, and cycle guards.
4. **Analytics engineering:** data quality checks, schema design, surrogate keys, SCD Types 0/1/2/3/4/6, and watermarks.
5. **Warehouse operations:** incremental loading, `MERGE`, distribution/sort design, query plans, and performance diagnostics.

## How to use

- Start with `PostgreSQL/postgresql_analytics.sql` for general SQL and PostgreSQL-specific patterns.
- Use `Redshift/redshift_analytics.sql` for Redshift warehouse patterns.
- Each file is divided into numbered sections; run examples individually and adapt sample table names.
- Examples use illustrative data or assumed staging tables unless stated otherwise.
- Review each statement before running it against non-disposable data. Some sections create schemas, tables, or temporary tables.

## Dialect and safety notes

PostgreSQL and Redshift share much SQL syntax but are not interchangeable. Redshift is a distributed analytical warehouse; PostgreSQL is a general-purpose relational database. Validate syntax and behavior against your engine version.

Primary-key and uniqueness constraints in Redshift are informational rather than enforced. Validate critical business keys and referential assumptions with explicit checks. Choose distribution styles, sort keys, and optimization strategies from measured workload evidence, not rules of thumb alone.

SCD and incremental-load examples are templates, not drop-in production pipelines. Deduplicate staging inputs, define a stable batch boundary, make writes idempotent, handle late-arriving data, and advance watermarks only after successful load and reconciliation.

## Suggested practice routine

For each section: predict the output, execute the query, inspect edge cases, explain the business meaning, then rewrite it using a CTE or window function where appropriate. Record assumptions about grain, null behavior, duplicate keys, and time boundaries.

## Documentation

See [`references/official_documentation.md`](references/official_documentation.md) for official PostgreSQL and Amazon Redshift references.

## Scope

This repository is a learning lab, not a substitute for production review, workload testing, access controls, backups, or change-management procedures.