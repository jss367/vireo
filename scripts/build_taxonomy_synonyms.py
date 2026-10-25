#!/usr/bin/env python3
"""Regenerate vireo/data/scientific_name_synonyms.json.

Classifier class lists are frozen at training time (the iNat21 list is 2021
vintage), while taxonomy.json indexes only currently-accepted scientific
names. Renamed taxa ("Bubulcus ibis" -> "Ardea ibis") therefore miss every
taxonomy lookup and leak raw binomials into predictions. This script derives
an old-name -> current-name map by routing each unresolvable class name
through its common name:

    old binomial --(label_descriptions.json)--> common name
                 --(taxonomy.json taxa_by_common)--> current species entry

A mapping is only accepted when the specific epithet survives the rename
modulo Latin gender endings (brasilianus == brasilianum). That filters out
lumps, splits, and common-name collisions, which would map a name onto a
genuinely different taxon.

Usage:
    python scripts/build_taxonomy_synonyms.py \
        [--label-descriptions PATH]  # default: fetch from the upstream timm repo
        [--taxonomy ~/.vireo/taxonomy.json] \
        [--output vireo/data/scientific_name_synonyms.json]
"""

import argparse
import json
import logging
import os
import re
import sys

log = logging.getLogger(__name__)

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_OUTPUT = os.path.join(
    REPO_ROOT, "vireo", "data", "scientific_name_synonyms.json"
)
UPSTREAM_TIMM_REPO = "timm/eva02_large_patch14_clip_336.merged2b_ft_inat21"

# Latin gender/declension endings that commonly flip when a species moves
# to a genus of different grammatical gender.
_GENDER_ENDING_RE = re.compile(r"(us|um|a|is|e|os|on)$")


def epithet_stem(binomial):
    """Stem of the specific epithet, ignoring Latin gender endings."""
    return _GENDER_ENDING_RE.sub("", binomial.split()[-1].lower())


def load_label_descriptions(path):
    if path:
        with open(path) as f:
            return json.load(f)
    from huggingface_hub import hf_hub_download

    config_path = hf_hub_download(
        repo_id=UPSTREAM_TIMM_REPO, filename="config.json"
    )
    with open(config_path) as f:
        descs = json.load(f).get("label_descriptions")
    if not isinstance(descs, dict) or not descs:
        raise SystemExit(
            f"Upstream config for {UPSTREAM_TIMM_REPO} has no "
            "label_descriptions"
        )
    return descs


def derive_synonyms(label_descriptions, taxonomy_data):
    by_scientific = taxonomy_data.get("taxa_by_scientific", {})
    by_common = taxonomy_data.get("taxa_by_common", {})

    synonyms = {}
    rejected = []
    for sci_name, desc in sorted(label_descriptions.items()):
        key = sci_name.lower().strip()
        if key in by_scientific:
            continue  # still current — nothing to map
        common = desc.rsplit(", ", 1)[0]
        if common.lower() == key:
            continue  # no common name to pivot through
        entry = by_common.get(common.lower())
        if not entry:
            continue
        current = entry.get("scientific_name", "")
        if entry.get("rank") not in ("species", "subspecies"):
            rejected.append((sci_name, current, f"rank={entry.get('rank')}"))
            continue
        if epithet_stem(sci_name) != epithet_stem(current):
            rejected.append((sci_name, current, "epithet mismatch"))
            continue
        synonyms[key] = current
    return synonyms, rejected


def main():
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from vireo.app_paths import default_path

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--label-descriptions", default=None)
    parser.add_argument(
        "--taxonomy", default=default_path("taxonomy_json")
    )
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    args = parser.parse_args()

    if not os.path.isfile(args.taxonomy):
        raise SystemExit(
            f"taxonomy.json not found at {args.taxonomy} — download it from "
            "Settings first, or pass --taxonomy"
        )

    label_descriptions = load_label_descriptions(args.label_descriptions)
    log.info("Loaded %d label descriptions", len(label_descriptions))
    with open(args.taxonomy) as f:
        taxonomy_data = json.load(f)

    synonyms, rejected = derive_synonyms(label_descriptions, taxonomy_data)
    log.info(
        "Derived %d synonyms (%d candidates rejected by rank/epithet guard)",
        len(synonyms), len(rejected),
    )
    for old, current, reason in rejected:
        log.info("  rejected: %s -> %s (%s)", old, current, reason)

    if len(synonyms) < 100:
        raise SystemExit(
            f"Only {len(synonyms)} synonyms derived — refusing to overwrite "
            "the shipped map with a suspiciously small one."
        )

    out_dir = os.path.dirname(args.output)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    with open(args.output, "w") as f:
        json.dump(synonyms, f, indent=1, sort_keys=True, ensure_ascii=False)
        f.write("\n")
    log.info("Wrote %s", args.output)
    return 0


if __name__ == "__main__":
    sys.exit(main())
