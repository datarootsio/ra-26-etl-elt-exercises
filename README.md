# ra-26-etl-elt-exercise

RootsAcademy 2026 ETL/ELT exercise (Olist dataset) — dbt + PySpark, deployed
via a Declarative Automation Bundles (DAB). One personal Unity Catalog schema +
volume per participant, grants-locked so nobody can see into anyone else's.

Group: [`ra-26-etl-elt-exercise`](https://adb-3020218199835395.15.azuredatabricks.net/settings/workspace/identity-and-access/groups/152573525405235?o=3020218199835395)
· Catalog: `dtr_sandbox` · Workspace: `adb-3020218199835395.15.azuredatabricks.net`

## What's here

```
databricks.yml                    bundle root — variables, target, includes resources/*.yml
participants.yml                  source of truth: who gets a schema (name, email)
resources/
  participant_schemas.yml         GENERATED — one schema + volume per participant, with grants
  reference_pipeline_job.yml      job resource that runs src/olist_pipeline as a job task
scripts/
  generate_resources.py           participants.yml -> resources/participant_schemas.yml
  upload_datasets.sh              one-time: copy Olist CSVs into every participant's volume
  build_ipynb.py                  notebooks/01-03 (.py) -> notebooks/olist_pipeline.ipynb
notebooks/                        THE EXERCISE — students edit these (TODOs + widgets)
  01_bronze_ingestion.py            CSV -> Bronze Delta tables
  02_etl_style_transform.py         Q1 in PySpark (ETL: transform before load)
  03_question_choice_and_build.py   Q2 & Q3, student's choice of PySpark or dbt
  olist_pipeline.ipynb              same three, as one importable .ipynb (generated)
  solutions/                        reference solutions, released after the exercise
dbt_project/                      THE EXERCISE, ELT half — Q1 (fixed) + Q2/Q3 (if chosen)
  profiles.yml                      committed — reads host/http_path/token/schema from .env, safe to commit
  .env.example                      copy to .env (gitignored, same folder) — HOST/HTTP_PATH/TOKEN, DBT_SCHEMA
  macros/generate_schema_name.sql   pins every model to the student's single schema
  models/staging/                   bronze -> typed staging views (given)
  models/intermediate/              shared delay_days logic (student builds, Q1 + Q3 both read it)
  models/marts/                     gold tables (student builds these)
  solutions/                        reference SQL solutions
src/olist_pipeline/               "HOW IT'S DONE NORMALLY" — production-style reference
  staging.py, transforms.py         pure, tested functions (no TODOs, no notebook globals)
  pipeline.py                       orchestration entrypoint; what the reference job runs
  session.py                        local/Databricks Connect Spark session helper
src/tests/                        pytest — runs locally, no Databricks connection needed
Exercise-Brief.md               participant-facing brief
Lesson-Plan.md                    teacher-facing lesson plan
pyproject.toml                    dev deps (pytest, ruff, databricks-connect, dbt-databricks, pyyaml) + pytest config
```

## Deploy

Needs the [Databricks CLI](https://docs.databricks.com/aws/en/dev-tools/cli/install)
configured with access to this workspace (`databricks auth login` or a
profile). Not available in the sandbox this was built in — run these
yourself:

```bash
databricks bundle validate -t dev
databricks bundle deploy   -t dev
```

This provisions, for every person in `participants.yml`: a schema
`dtr_sandbox.ra26_elt_ex_<first_name>` and a managed volume `raw_data`
inside it, with `ALL_PRIVILEGES` granted to that person's email — and
nobody else's. It also deploys `olist_reference_pipeline`, a job that runs
`src/olist_pipeline` (defaults to Han's schema).

The deploy also syncs the whole bundle into **your own** personal
Workspace folder (this target's `root_path`, always deployed as Han) —
including `notebooks/solutions/` and `dbt_project/solutions/`, which are
force-included via `sync.include` in `databricks.yml` even though
they're gitignored. That's deliberate: participants get this repo via
git (where `solutions/` never appears at all), not via Han's personal
Workspace folder, so having the solutions land there too is harmless —
it just means Han has them on hand in Databricks without needing to
open them locally.

## One-time manual setup (not in the bundle, on purpose)

Two things live outside the bundle deliberately, because they touch
resources this bundle doesn't own:

1. **Group membership.** Managed in the Databricks workspace UI (the link
   at the top of this file), not here. `participants.yml` is the list of
   who gets a schema; it doesn't add anyone to the group.
2. **`USE_CATALOG` on `dtr_sandbox`.** The catalog is shared with other
   bundles (e.g. the sibling `sdp-tech-talk` project), so it isn't created
   or grant-managed by this bundle — declaring `resources.catalogs` here
   would fight that other bundle's Terraform state over the same catalog.
   Grant it once, out of band:

   ```sql
   GRANT USE CATALOG ON CATALOG dtr_sandbox TO `ra-26-etl-elt-exercise`;
   ```

   (Run as a workspace/catalog admin — SQL editor, a `databricks sql`
   command, or whatever you'd normally use for a one-off UC grant.)

## Load the dataset

```bash
./scripts/upload_datasets.sh                     # defaults to ../olist
./scripts/upload_datasets.sh /path/to/olist/csvs  # or point it elsewhere
```

Copies all 9 Olist CSVs (the raw Kaggle export, including `geolocation` —
not just the 8 this exercise's Bronze ingestion actually uses) into
**every** participant's own `raw_data` volume (not a shared one — each
person's copy is theirs, matching their grants). Safe to re-run.

## Run the exercise

Participants: open `notebooks/01_bronze_ingestion.py` (or the generated
`notebooks/olist_pipeline.ipynb`) in Databricks, set the `schema` widget to
their own `ra26_elt_ex_<first_name>`, and follow `Exercise-Brief.md`. For
the dbt half: `cd dbt_project`, copy `.env.example` to `.env`, fill in
their own `DBT_SCHEMA` plus the connection details you give them, then
`DBT_PROFILES_DIR=. uv run dbt debug` — `profiles.yml`
itself is already committed and reads everything from `.env`; `dbt-databricks`
is already a dev dependency, nothing to `pip install`.

Teacher: `Lesson-Plan.md` has the timing and setup checklist. To see "the
same three questions, done as a tested production pipeline instead of a
notebook", trigger `olist_reference_pipeline` from the Jobs UI — "Run now
with different parameters" lets anyone point it at their own schema. This
works for any participant's schema no matter who clicks run: the job always
executes as its deploy-time `run_as` identity (Han), and Han owns every
schema this bundle created — ownership, not the per-participant grants, is
what makes that work. The job's `permissions:` block is what lets the group
see and trigger it at all.

## Adding or removing a participant

1. Edit `participants.yml`
2. `uv run python scripts/generate_resources.py` (regenerates
   `resources/participant_schemas.yml` — plain `python3` will hit
   `ModuleNotFoundError: No module named 'yaml'` unless it happens to be
   installed system-wide; `uv run` uses this project's own env, which has
   `pyyaml`)
3. `databricks bundle deploy -t dev`
4. `./scripts/upload_datasets.sh` (idempotent — fine to re-run for everyone)
5. Add/remove them from the `ra-26-etl-elt-exercise` group in the workspace
   UI (step 1 above doesn't do this)

## Local dev / running the tests

The production package (`src/olist_pipeline`) is plain, tested PySpark —
no Databricks connection needed to run its test suite:

```bash
uv sync            # reads [dependency-groups].dev in pyproject.toml
uv run pytest
# no uv? : pip install pyspark pytest databricks-connect && pytest
```

`pytest` uses a local `SparkSession` (see `src/tests/conftest.py`), so this
runs anywhere Java + PySpark work, including CI.
