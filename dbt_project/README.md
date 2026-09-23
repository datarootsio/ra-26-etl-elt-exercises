# Olist ETL/ELT exercise — dbt project

This is the ELT half of the exercise. dbt only ever reads from **your** Bronze
tables (built in `01_bronze_ingestion.py`) and writes back into **your own**
schema — nothing here touches raw files directly, and nothing here touches
anyone else's schema.

## Setup (5 minutes)

`profiles.yml` in this folder is the real, committed profile — every
credential in it comes from an environment variable (`env_var(...)`), so
it's safe to commit and the same for everyone. You supply the values via a
local `.env` file, which is gitignored.

Everything in this repo runs through `uv` — `dbt-databricks` is already a
dev dependency in the root `pyproject.toml`, there's nothing to `pip
install` separately.

1. From the repo root: `uv sync` (reads `[dependency-groups].dev`,
   installs `dbt-databricks` and everything else into `.venv`)
2. `cd dbt_project`, copy `.env.example` to `.env`, fill in
   `DATABRICKS_HOST` / `DATABRICKS_HTTP_PATH` (your teacher gives you
   these — a SQL warehouse's HTTP path, not a cluster's), `DATABRICKS_TOKEN`
   (your own personal access token), and `DBT_SCHEMA` = your own schema,
   `ra26_elt_ex_<your first name>` (already provisioned for you by the
   bundle — see the top-level README.md if it's missing)
3. `source .env` (or use `direnv`/your shell's usual way of loading a
   `.env` file)
4. `DBT_PROFILES_DIR=. uv run dbt debug` — should print all green

## What's already done for you

- `models/staging/` — one clean staging model per Bronze table (rename/cast only)
- `models/staging/_sources.yml` — points at your Bronze tables automatically

## What you build

Build in this order — the intermediate model comes first because Q1 (and
Q3, if you pick dbt for it) both depend on it:

- `models/intermediate/int_delivered_orders_with_delay.sql` — the one
  place `delay_days` gets computed (delivered orders, days late vs. the
  estimate). This is dbt's own **intermediate** layer — a shared building
  block between staging and marts, not a business-ready table on its own.
  Build this first; you'll only need to get `datediff()` right once.
- `models/marts/gold_delivery_performance_by_state.sql` — Q1, ELT version
  (you're building the same question in PySpark too — that's the point).
  Reads `int_delivered_orders_with_delay`, joins to customers, aggregates.
- `models/marts/gold_monthly_revenue_by_category.sql` — Q2, **only if** you
  picked dbt for this one (doesn't touch the intermediate model)
- `models/marts/gold_review_score_vs_delivery_delay.sql` — Q3, **only if**
  you picked dbt for this one. Reads the same
  `int_delivered_orders_with_delay` you already built for Q1 — if that's
  done, this one is mostly just the bucketing logic.
- `models/marts/schema.yml` / `models/intermediate/schema.yml` — tests for
  whatever you built

Run everything with (from `dbt_project/`, with `.env` sourced):

```
DBT_PROFILES_DIR=. uv run dbt build
```

Solutions for all three marts are in `solutions/` — don't peek until you've
had a real go, or until your teacher releases them.
