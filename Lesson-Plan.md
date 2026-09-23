# ETL & ELT Exercise — Olist Dataset — Lesson Plan

**Session:** ETL Pipeline exercises (RootsAcademy 2026, DE & DP track)
**Duration:** 4h · **Format:** solo, hands-on · **Teacher:** Han
**Prerequisites already covered:** Spark, dbt, Python, SQL. Also already
covered and reusable without re-teaching: **Modern Storage Architecture**
(medallion / bronze-silver-gold, Delta) and **Orchestration** (Airflow) —
just recap and point back to them, don't rebuild the concepts from scratch.

## Why this design

Frame it as one dataset, one set of business questions, every layer of the
platform: ingest it, model/store it, transform it two ways (ETL vs. ELT),
run it on a schedule. The ETL vs. ELT comparison is the spine; ingestion,
storage/modeling, and orchestration are there to show it touches the whole
platform, not just the transform step. New this round: there's also a
tested, production-style Python implementation of the same three questions
(`src/olist_pipeline/`) — worth pointing students at it during the share &
reflect block, as a "what would this look like if you had to run it
unattended every night" contrast to both the notebook and the dbt version.

## Timeline

| Time | Block | Detail |
|---|---|---|
| 0:00–0:15 | Framing | Preview the whole arc. Show the Olist schema on screen — 9 raw tables, 8 of which this exercise actually uses (`geolocation` isn't needed for any of the 3 questions). State the 3 fixed business questions up front. |
| 0:15–0:40 | Live demo | One small worked example end to end, fast: land one table to Bronze, transform it ETL-style vs. ELT-style, wire into a Job. Orientation only — this is not their exercise, don't let it run long. |
| 0:40–1:10 | Phase A — Ingest & model | Solo: `01_bronze_ingestion.py` (or the combined `notebooks/olist_pipeline.ipynb`). Worked example given for `customers`; they repeat the pattern for the other 7 tables. Prompt: partitioning/schema decisions per table. |
| 1:10–2:25 | Phase B — ETL vs ELT core | Q1 (fixed): everyone builds it twice — `02_etl_style_transform.py` (PySpark) and, in dbt, `int_delivered_orders_with_delay.sql` (the shared `delay_days` logic, build this first) then `gold_delivery_performance_by_state.sql`. Q2 & Q3: `03_question_choice_and_build.py` — pick PySpark or dbt per question, justify the choice; anyone doing Q3 in dbt reuses the intermediate model they already built for Q1. |
| 2:25–2:50 | Phase C — Light orchestration | Trigger `olist_reference_pipeline` (the bundle's job resource) with their own `schema` parameter — "Run now with different parameters" — and look at what a scheduled, production version of Phase B looks like end to end. UI click-through, not code-first. |
| 2:50–3:10 | Share & reflect | 3–4 people share their Q2/Q3 choices and reasoning. Tie back to real client tradeoffs: latency, team skill (SQL vs. engineering), cost, tooling maturity. |
| 3:10–4:00 | Buffer | Protect this explicitly. Solo work means more individual unblocking (environment issues, Spark errors, dbt connection issues). Don't let Phase B creep into it — better to cut Q2/Q3 scope than eat the buffer. |

## Pre-class setup checklist

- [x] Deploy the bundle once, yourself: `databricks bundle validate -t dev`
      then `databricks bundle deploy -t dev` (from this repo). This creates
      every participant's `ra26_elt_ex_<first_name>` schema + `raw_data`
      volume under the shared `dtr_sandbox` catalog, grants-locked to them.
      See the top-level README.md.
- [x] One-time manual grant (not in the bundle — see README.md "One-time
      manual setup"): `GRANT USE CATALOG ON CATALOG dtr_sandbox TO
      \`ra-26-etl-elt-exercise\`;` so the group can even see the catalog.
- [x] Run `./scripts/upload_datasets.sh` to copy the Olist CSVs into every
      participant's own volume (standard Kaggle "Brazilian E-Commerce Public
      Dataset by Olist" filenames — double check column names against
      whatever mirror you pull from; the exact Kaggle export has changed
      casing/columns before).
- [ ] Confirm everyone in the `ra-26-etl-elt-exercise` group can attach to a
      cluster (or serverless) and has a SQL warehouse for the dbt task, plus
      `DATABRICKS_HOST` / `DATABRICKS_HTTP_PATH` / `DATABRICKS_TOKEN` values
      (or a way to generate their own token).
- [ ] If someone new joins the exercise, add them to `participants.yml`, add
      them to the `ra-26-etl-elt-exercise` group in the workspace, re-run
      `uv run python scripts/generate_resources.py` (plain `python3` will
      hit `ModuleNotFoundError: No module named 'yaml'`), redeploy, and
      re-run the upload script (it's safe to re-run for everyone).
- [ ] Import the notebooks + dbt project into a shared workspace folder
      students can clone/copy from (or just have them clone this repo — it's
      already a Databricks Repo-friendly layout).
- [ ] Decide whether you pre-run the solutions once yourself the morning of,
      to catch any schema drift in the raw CSVs before 7 people hit it at once.

## Common pitfalls to warn about up front

- `inferSchema=True` on `orders` will read the timestamp columns as strings
  unless the raw file's date format is clean — worth checking once yourself
  and telling students what to expect, rather than having 7 people
  independently discover the same cast issue.
- Everyone shares ONE schema now (`ra26_elt_ex_<first_name>`), not separate
  bronze/silver/gold schemas per handle. If a student's `.env` `DBT_SCHEMA`
  doesn't exactly match their provisioned schema name, `_sources.yml`
  won't find their Bronze tables. This is still the #1 likely
  support ticket — the error message (table not found) doesn't obviously
  point at "your schema name is wrong."
- Databricks Jobs dbt task needs a SQL warehouse, not a cluster — different
  compute selection dropdown than the notebook tasks. Easy to miss.
- Grants are per-schema, not per-catalog: a student who tries to browse a
  classmate's schema will get a permission error by design — reassure them
  that's expected, not broken.
- Running `dbt run`/`dbt build` before Phase A's Bronze ingestion has
  happened for that schema fails every `stg_*` model with
  `TABLE_OR_VIEW_NOT_FOUND` on `bronze_*` — confirmed live while testing
  this exercise. Not a dbt bug: staging views read Bronze tables that only
  exist once `01_bronze_ingestion.py` has actually been run for that
  schema. Worth saying out loud up front so nobody burns time debugging
  dbt when the real fix is "go run Phase A first."

## What "done" looks like (not graded)

- All 8 Bronze tables exist, in their own schema
- Q1 built both ways, with matching results (the union query in
  `02_etl_style_transform.py`'s last cell is the check)
- Q2 and Q3 each built at least one way, with a one-line justification for
  the choice
- They've run `olist_reference_pipeline` at least once with their own
  `schema` parameter and can point at the resulting `_prod` gold tables
- Can articulate, out loud, one thing they'd do differently at 100x the data
  volume

## Links back to curriculum-de.md

This lesson plan supersedes the placeholder outline currently in
`2026/curriculum-de.md` under "ETL Pipeline exercises" (the generic
dbt+Scheduling+Spark integration sketch drafted earlier in the year). Worth
syncing that file to this real version once this has actually been taught
once — happy to do that update when you're ready.
