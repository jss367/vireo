"""Reviewed iNaturalist to eBird/Clements v2026 relationships for Life List."""
import json
from functools import cache
from pathlib import Path


@cache
def _relationships():
    return json.loads((Path(__file__).parent / "data" / "ebird_names_v2026.json").read_text())


def reviewed_relationship(entry):
    if entry.get("taxon_rank") != "species" or (entry.get("taxonomic_class") or {}).get("name") != "Aves":
        return None
    relation = _relationships().get(str(entry.get("inat_id")))
    if relation and relation["scientific_name"] == entry.get("scientific_name"):
        return relation
    return None


def add_ebird_names(entries):
    for entry in entries:
        relation = reviewed_relationship(entry)
        if relation and relation["relationship"] == "match":
            entry["ebird_common_name"] = relation["targets"][0]["common_name"]
