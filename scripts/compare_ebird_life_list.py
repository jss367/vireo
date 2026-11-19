"""Compare the running Vireo Life List with Cornell's v2026 English names workbook.

Download the common names workbook at https://support.ebird.org/en/support/solutions/articles/48000804865
and run: python scripts/compare_ebird_life_list.py workbook.xlsx > comparison.csv
Requires openpyxl for this offline checklist read.
"""
import argparse
import csv
import json
import sys
import urllib.request
from collections import defaultdict
from pathlib import Path

from openpyxl import load_workbook

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "vireo"))
from ebird_names import reviewed_relationship
from runtime import read_runtime_json


def read_checklist(path):
    workbook = load_workbook(path, read_only=True)
    try:
        versions = workbook["Fleshed"].values
        version = dict(zip(next(versions), next(versions), strict=True))
        if (version["authority_code"], str(version["authority_ver"])) != ("EBIRD", "2026"):
            raise ValueError("Expected Cornell's eBird v2026 workbook")
        rows = workbook["Sparse"].iter_rows(max_col=6, values_only=True)
        header = next(rows)
        species = []
        for values in rows:
            row = dict(zip(header, values, strict=True))
            if row["category"] == "species":
                species.append(row)
        return species
    finally:
        workbook.close()


def comparison(entries, checklist):
    by_name = defaultdict(list)
    by_code = {}
    for row in checklist:
        by_name[row["sci_name"]].append(row)
        by_code[row["species_code"]] = row
    for entry in entries:
        if entry.get("taxon_rank") != "species" or (entry.get("taxonomic_class") or {}).get("name") != "Aves":
            continue
        relation = reviewed_relationship(entry)
        targets = by_name.get(entry.get("scientific_name"), [])
        status = "candidate" if targets else "unmatched"
        evidence = "Scientific-name lookup in eBird v2026"
        if relation:
            targets = [by_code[target["species_code"]] for target in relation["targets"]]
            for row, target in zip(targets, relation["targets"], strict=True):
                if (row["bio_concept_code"], row["primary_com_name"]) != (target["concept_id"], target["common_name"]):
                    raise ValueError(f"Checklist differs from reviewed record for {row['species_code']}")
            status = relation["relationship"]
            evidence = " | ".join(relation["evidence"])
        for row in targets or [{}]:
            yield [entry["species"], entry.get("taxon_id"), entry.get("inat_id"), entry.get("scientific_name"),
                   "2026", row.get("primary_com_name"), row.get("sci_name"), row.get("species_code"),
                   row.get("bio_concept_code"), status, evidence,
                   f"https://www.inaturalist.org/taxa/{entry['inat_id']}/taxonomy_details" if entry.get("inat_id") else "",
                   f"https://ebird.org/species/{row['species_code']}" if row else ""]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("workbook", type=Path)
    args = parser.parse_args()
    runtime = read_runtime_json()
    if not runtime:
        parser.error("Start Vireo to read its Life List")
    request = urllib.request.Request(
        f"http://127.0.0.1:{int(runtime['port'])}/api/life-list?photos_per_species=0",
        headers={"X-Vireo-Token": runtime["token"]},
    )
    with urllib.request.urlopen(request, timeout=120) as response:
        entries = json.load(response)["species"]
    rows = list(comparison(entries, read_checklist(args.workbook)))
    writer = csv.writer(sys.stdout)
    writer.writerow(["catalog_name", "local_taxon_id", "inat_id", "catalog_scientific_name",
                     "ebird_release", "ebird_name", "ebird_scientific_name", "ebird_code",
                     "avibase_concept_id", "relationship", "evidence", "inat_details", "ebird_species_page"])
    for row in rows:
        writer.writerow(["'" + value if isinstance(value, str) and value.lstrip().startswith(("=", "+", "-", "@"))
                         else value for value in row])


if __name__ == "__main__":
    main()
