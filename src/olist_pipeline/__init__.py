"""Production-style reference implementation of the Olist exercise pipeline.

This is deliberately NOT the notebook exercise (see notebooks/). It's the
"how you'd actually write this for a real team" version: plain, tested,
importable functions instead of notebook cells full of TODOs and globals.

    staging.py    — bronze -> typed/cleaned staging DataFrames (mirrors the
                    dbt staging models 1:1, so numbers match either way)
    transforms.py — staging -> the three gold business tables (mirrors the
                    dbt marts / SOLUTION.sql logic 1:1)
    pipeline.py   — orchestration entrypoint (reads bronze from Unity
                    Catalog, calls the above, writes gold) — this is what
                    resources/reference_pipeline_job.yml runs as a job task

Everything in staging.py and transforms.py is a pure function: DataFrame(s)
in, DataFrame out, no I/O. That's what makes tests/ possible without a
Databricks cluster — see tests/conftest.py for the local SparkSession.
"""
