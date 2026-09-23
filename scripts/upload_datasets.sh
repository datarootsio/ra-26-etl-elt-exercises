#!/usr/bin/env bash
# One-time: copy the local Olist CSVs into EVERY participant's own volume.
#
# Run this AFTER `databricks bundle deploy -t dev` (the volumes have to
# exist first). Needs the Databricks CLI configured (`databricks auth
# login` or a profile). Re-running is safe — `fs cp` overwrites.
#
# Usage:
#   ./scripts/upload_datasets.sh [path-to-olist-csv-folder]
#
# Default: ../olist (sibling folder, as laid out under rootsacademy26/).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

DATASET_DIR="${1:-$REPO_ROOT/../olist}"

if [ ! -d "$DATASET_DIR" ]; then
  echo "Dataset folder not found: $DATASET_DIR" >&2
  echo "Usage: $0 [path-to-olist-csv-folder]" >&2
  exit 1
fi

# Read the catalog default straight out of databricks.yml, so this never
# drifts from what was actually deployed. Override with DATABRICKS_CATALOG=
# if you deployed with a non-default `catalog` bundle variable.
CATALOG="${DATABRICKS_CATALOG:-$(python3 -c "
import yaml
print(yaml.safe_load(open('$REPO_ROOT/databricks.yml'))['variables']['catalog']['default'])
")}"

echo "Dataset:     $DATASET_DIR"
echo "Catalog:     $CATALOG"
echo

python3 - "$REPO_ROOT/participants.yml" <<'PY' > /tmp/ra26_first_names.txt
import sys, yaml
data = yaml.safe_load(open(sys.argv[1]))
for p in data["participants"]:
    print(p["first_name"])
PY

while read -r first_name; do
  schema="ra26_elt_ex_${first_name}"
  dest="dbfs:/Volumes/${CATALOG}/${schema}/raw_data/"
  echo "==> ${schema}"
  # -r copies the whole folder's *.csv files; the CSVs only (no .duckdb etc).
  for csv in "$DATASET_DIR"/*.csv; do
    databricks fs cp "$csv" "$dest" --overwrite
  done
done < /tmp/ra26_first_names.txt

rm -f /tmp/ra26_first_names.txt
echo
echo "Done. Every participant's raw_data volume now has the Olist CSVs."
