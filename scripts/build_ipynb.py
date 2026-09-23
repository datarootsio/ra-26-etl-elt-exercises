#!/usr/bin/env python3
"""Build notebooks/olist_pipeline.ipynb from the Databricks-source-format
.py notebooks (01/02/03), so the exercise ships as an actual .ipynb too —
not just something you have to `databricks workspace import` to see as one.

The .py files (Databricks' native "source format", `# COMMAND ----------`
cell markers) stay the source of truth: they're what you edit, what git
diffs cleanly, and what a Databricks Repo runs directly. This script derives
the .ipynb from them, it never the other way around — re-run it after
editing any of the three .py notebooks:

    python3 scripts/build_ipynb.py

Databricks itself can do this conversion via `databricks workspace export
--format JUPYTER`, but that needs a live workspace. This works offline, so
the .ipynb can be committed and opened in plain Jupyter/VS Code too.
"""
from __future__ import annotations

import hashlib
from pathlib import Path

import nbformat as nbf

ROOT = Path(__file__).resolve().parent.parent
NOTEBOOKS_DIR = ROOT / "notebooks"
SOURCES = [
    ("01 — Bronze ingestion", NOTEBOOKS_DIR / "01_bronze_ingestion.py"),
    ("02 — Question 1, the ETL way", NOTEBOOKS_DIR / "02_etl_style_transform.py"),
    ("03 — Questions 2 & 3, your choice", NOTEBOOKS_DIR / "03_question_choice_and_build.py"),
]
OUT_FILE = NOTEBOOKS_DIR / "olist_pipeline.ipynb"

CELL_MARKER = "# COMMAND ----------"
HEADER = "# Databricks notebook source"
MAGIC_MD = "# MAGIC %md"
MAGIC_PREFIX = "# MAGIC "


def _strip_magic(line: str) -> str:
    """"# MAGIC foo" -> "foo"; "# MAGIC" (no trailing space, blank line) -> ""."""
    if line.startswith(MAGIC_PREFIX):
        return line[len(MAGIC_PREFIX):]
    if line.strip() == "# MAGIC":
        return ""
    return line


def parse_databricks_py(path: Path) -> list[tuple[str, str]]:
    """Return [(cell_type, source), ...] for a Databricks source-format .py file."""
    lines = path.read_text().splitlines()
    if lines and lines[0].strip() == HEADER:
        lines = lines[1:]

    raw_cells: list[list[str]] = [[]]
    for line in lines:
        if line.strip() == CELL_MARKER:
            raw_cells.append([])
        else:
            raw_cells[-1].append(line)

    cells: list[tuple[str, str]] = []
    for raw in raw_cells:
        # drop leading/trailing blank lines
        while raw and not raw[0].strip():
            raw.pop(0)
        while raw and not raw[-1].strip():
            raw.pop()
        if not raw:
            continue
        if raw[0].startswith(MAGIC_MD):
            first_rest = raw[0][len(MAGIC_MD):]
            body = raw[1:] if raw[0].strip() == MAGIC_MD.strip() else [first_rest] + raw[1:]
            md_lines = [_strip_magic(line) for line in body]
            cells.append(("markdown", "\n".join(md_lines)))
        else:
            cells.append(("code", "\n".join(raw)))
    return cells


def _stable_id(index: int, source: str) -> str:
    """Deterministic cell id (index + content hash), so re-running this
    script with no real changes produces a byte-identical file — nbformat's
    default is a random UUID per cell, which would otherwise make every
    regeneration look like a full-file diff in git for no reason."""
    return hashlib.sha1(f"{index}:{source}".encode()).hexdigest()[:8]


def main() -> None:
    nb = nbf.v4.new_notebook()
    nb["metadata"] = {
        "kernelspec": {"name": "python3", "display_name": "Python 3"},
        "language_info": {"name": "python"},
    }
    nb["cells"] = [
        nbf.v4.new_markdown_cell(
            "# Olist ETL/ELT exercise — full pipeline\n\n"
            "Generated from `notebooks/01_bronze_ingestion.py`, "
            "`02_etl_style_transform.py`, `03_question_choice_and_build.py` "
            "by `scripts/build_ipynb.py`. Edit those three files, not this "
            "one directly — re-run the script to regenerate.\n\n"
            "Import this into Databricks (Workspace > Import) or run it "
            "against a cluster with Databricks Connect configured."
        )
    ]
    for title, path in SOURCES:
        nb["cells"].append(nbf.v4.new_markdown_cell(f"---\n## {title}\n\n*(`{path.name}`)*"))
        for cell_type, source in parse_databricks_py(path):
            if cell_type == "markdown":
                nb["cells"].append(nbf.v4.new_markdown_cell(source))
            else:
                nb["cells"].append(nbf.v4.new_code_cell(source))

    for i, cell in enumerate(nb["cells"]):
        cell["id"] = _stable_id(i, cell["source"])

    nbf.write(nb, OUT_FILE)
    print(f"wrote {OUT_FILE} ({len(nb['cells'])} cells)")


if __name__ == "__main__":
    main()
