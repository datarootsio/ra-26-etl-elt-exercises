# ETL & ELT Exercise — Olist Dataset

*RootsAcademy 2026 · DE & DP Track · 4 hours · Solo*

## The idea

One dataset. One set of business questions. Every layer of the platform.
Today you'll ingest raw data, make storage/modeling decisions, answer the
same question twice — once ETL-style, once ELT-style — and finish by
wiring your pipeline into a schedule. The ETL vs. ELT comparison is the
main event; the rest is there to show you it touches everything.

You've already covered Spark, dbt, Python, and SQL — this is where they
come together. You've also already seen Modern Storage Architecture
(bronze/silver/gold, Delta) and Orchestration (Airflow) — we'll refer back
to both rather than re-teach them.

## The dataset: Olist (Brazilian e-commerce)

Nine raw tables are pre-staged for you in your own Volume, but this
exercise only uses eight of them: customers, orders, order_items,
order_payments, order_reviews, products, sellers, and a product-category
translation table. (There's also a geolocation table in your Volume, from
the same Kaggle export — it isn't needed for any of the 3 questions
below, but it's there if you want to go further.) Real, slightly messy,
genuinely joinable data — enough to make both ETL and ELT feel like real
work.

## Your 3 business questions

- **Q1 (fixed — build it BOTH ways):** Delivery performance by state —
  average delivery delay and % of late orders, grouped by customer state.
- **Q2 (your choice of PySpark or dbt):** Monthly revenue by product
  category.
- **Q3 (your choice of PySpark or dbt):** Does shipping speed drive
  satisfaction? — review score by delivery-delay bucket.

Exact required output columns for each are in the notebook/dbt file
headers — keep to them so your results are comparable with everyone
else's at the wrap-up.

## Setup (do this first, ~5 min)

- Your schema is already provisioned: `ra26_elt_ex_<your first name>`
  (e.g. `ra26_elt_ex_nils`), under the shared `dtr_sandbox` catalog. Use
  it everywhere — it's what keeps your tables separate from everyone
  else's.
- Open `01_bronze_ingestion.py` (or `notebooks/olist_pipeline.ipynb`) in
  your Databricks workspace, set the `schema` widget.
- Set up the dbt project: run `uv sync` once from the repo root, then
  `cd dbt_project`, copy `.env.example` to `.env`, fill in your
  `DBT_SCHEMA` plus the connection details you were given, source it, and
  run `DBT_PROFILES_DIR=. uv run dbt debug` (`profiles.yml` is already
  there, committed — it just reads `.env`; `dbt-databricks` is already a
  dependency, nothing to `pip install`).

`dbt debug` can pass before you've built anything in Databricks — it only
checks the connection. But don't run `dbt run`/`dbt build` until you've
actually landed your Bronze tables (Phase A below): every staging model
reads a `bronze_*` table, so running dbt first fails all of them with
"table or view not found." That's expected, not a dbt bug — just do
Phase A first.

## How the 4 hours break down

| Time | Phase | What you're doing |
|---|---|---|
| ~55 min | Framing + demo | Orientation — watch, don't build yet. |
| 30 min | A — Ingest & model | `01_bronze_ingestion.py` — land 8 of the 9 raw tables into your schema (`bronze_` prefixed; geolocation is skipped, see above). |
| 75 min | B — ETL vs. ELT | Build Q1 twice (PySpark + dbt). Build Q2 and Q3, your choice of tool each. |
| 25 min | C — Schedule it | Wire your pipeline into a Databricks Job with a (paused) schedule. |
| 20 min | Share & reflect | A few people share their Q2/Q3 choices and why. |
| rest | Buffer | Yours to finish, debug, or go deeper. Ask for help early rather than late. |

## What "done" looks like

- All 8 Bronze tables you actually use exist in your own schema (not
  counting the unused `geolocation` CSV also sitting in your Volume).
- Q1 built both ways — and the two results match.
- Q2 and Q3 each built at least one way, with a one-line reason for the
  tool you picked.
- A Databricks Job exists with your tasks wired up and a schedule
  attached (paused is fine).
- You can say out loud, in one sentence, what you'd do differently if
  this dataset were 100x bigger.

This isn't graded. The point is to leave with a real opinion about when
you'd reach for ETL vs. ELT — and to have actually built both, not just
heard about the difference.

---
*RootsAcademy 2026 — ETL & ELT Exercise — not graded, built to show you
where the bar is*
