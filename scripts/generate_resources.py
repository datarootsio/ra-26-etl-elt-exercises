#!/usr/bin/env python3
"""Generate resources/participant_schemas.yml from participants.yml.

One UC schema + one UC volume per participant, each with a `grants:` block
that gives ONLY that participant privileges on ONLY their own schema. Every
participant gets the exact same privilege set, so access is identical in
shape, isolated in scope.

Re-run this whenever participants.yml changes:
    uv run python scripts/generate_resources.py
"""
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
PARTICIPANTS_FILE = ROOT / "participants.yml"
OUT_FILE = ROOT / "resources" / "participant_schemas.yml"

VOLUME_NAME = "raw_data"

# Same privilege set for everyone, scoped to their own schema only. Volume
# read/write is inherited from the schema-level grant in Unity Catalog.
SCHEMA_PRIVILEGES = ["ALL_PRIVILEGES"]

HEADER = """\
# =============================================================================
# GENERATED FILE — do not hand-edit. Run scripts/generate_resources.py after
# changing participants.yml.
#
# One schema + one volume per participant under ${var.catalog}, named
# ra26_elt_ex_<first_name>. Each schema's `grants:` block gives ONLY that
# participant privileges on ONLY that schema (Unity Catalog privilege
# inheritance carries these down to the volume and to every table/view the
# participant creates in it). Nobody else in the group can see into it.
#
# Everyone also needs USE_CATALOG on ${var.catalog} itself to even see their
# schema. That catalog is shared with other bundles/trainings, so it is NOT
# created here — grant it once, out of band, documented in README.md.
# =============================================================================
"""


def main() -> None:
    data = yaml.safe_load(PARTICIPANTS_FILE.read_text())
    participants = data["participants"]

    lines = [HEADER, "resources:", "  schemas:"]
    for p in participants:
        name = p["first_name"]
        schema_name = f"ra26_elt_ex_{name}"
        lines += [
            f"    schema_{name}:",
            "      catalog_name: ${var.catalog}",
            f"      name: {schema_name}",
            f"      comment: \"RootsAcademy 2026 ETL/ELT exercise — "
            f"{p['full_name']}'s personal sandbox.\"",
            "      grants:",
            f"        - principal: {p['email']}",
            "          privileges:",
        ]
        lines += [f"            - {priv}" for priv in SCHEMA_PRIVILEGES]
        lines.append("")

    lines.append("  volumes:")
    for p in participants:
        name = p["first_name"]
        lines += [
            f"    volume_{name}:",
            "      catalog_name: ${var.catalog}",
            # Reference (not a literal string) so Terraform knows the schema
            # must exist before the volume — same trick as the sdp-tech-talk
            # bundle's volumes.yml.
            f"      schema_name: ${{resources.schemas.schema_{name}.name}}",
            f"      name: {VOLUME_NAME}",
            "      volume_type: MANAGED",
            f"      comment: \"Raw Olist CSVs for {p['full_name']} — "
            f"uploaded once via scripts/upload_datasets.sh.\"",
        ]
        lines.append("")

    OUT_FILE.write_text("\n".join(lines).rstrip() + "\n")
    print(f"wrote {OUT_FILE} ({len(participants)} participants)")


if __name__ == "__main__":
    main()
