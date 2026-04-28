"""Bucket unresolved duplicate proposals by parent-dir set.

When the same dedup question repeats hundreds of times — same two folders,
different filenames — the UI lets the user decide once for the whole bucket
instead of clicking through every group. Pure function: input is the
``proposals`` list from :func:`duplicate_scan.run_duplicate_scan`, output is
a list of bucket dicts ready for the UI.
"""
import os
from collections import defaultdict


def bucket_unresolved_proposals(proposals):
    """Group unresolved proposals by the set of parent directories of their
    candidates. Returns a list of bucket dicts sorted by ``group_count`` desc.

    A bucket dict has:

    - ``folders``: sorted list of parent directory paths
    - ``group_count``: number of duplicate groups in this bucket
    - ``file_hashes``: list of file_hashes belonging to this bucket
    - ``total_size``: bytes recoverable if the user picks any one folder
      (``sum_per_group(n_candidates - 1) * file_size``)
    - ``example_filenames``: up to 3 sample filenames for UI preview

    Resolved proposals are filtered out — they're already done.
    """
    by_key = defaultdict(list)
    for p in proposals:
        if p.get("status") != "unresolved":
            continue
        paths = [p["winner"]["path"]] + [l["path"] for l in p["losers"]]
        key = frozenset(os.path.dirname(path) for path in paths)
        by_key[key].append(p)

    buckets = []
    for key, group_proposals in by_key.items():
        total_size = sum(
            len(p["losers"]) * p["winner"]["file_size"]
            for p in group_proposals
        )
        examples = [p["winner"]["filename"] for p in group_proposals[:3]]
        buckets.append({
            "folders": sorted(key),
            "group_count": len(group_proposals),
            "file_hashes": [p["file_hash"] for p in group_proposals],
            "total_size": total_size,
            "example_filenames": examples,
        })

    buckets.sort(key=lambda b: -b["group_count"])
    return buckets
