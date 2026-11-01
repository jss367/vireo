"""Names of the pipeline and culling result files kept next to the catalog.

``services/result_files.py`` owns their lifecycle (pruning, deleting a
workspace's files). The listing lives here, in the data layer, because the
``Database`` façade reads it too: ``create_workspace`` never issues an id
that an existing ``*_results_ws<N>.json`` already claims, so a new workspace
cannot adopt a deleted one's results. Only exact Vireo filenames count,
never arbitrary JSON siblings.
"""

from __future__ import annotations

import os
import re

RESULT_NAME = re.compile(r"^(pipeline|culling)_results_ws([1-9][0-9]*)\.json$")


def result_files(catalog_dir: str) -> list[tuple[str, str, int]]:
    try:
        with os.scandir(catalog_dir or os.curdir) as entries:
            return [
                (entry.path, match[1], int(match[2]))
                for entry in entries
                if (match := RESULT_NAME.fullmatch(entry.name)) and entry.is_file()
                and not entry.is_symlink()
                and int(match[2]) < 2**63 - 1
            ]
    except FileNotFoundError:
        return []


def highest_workspace_id(catalog_dir: str) -> int:
    return max((ws for _path, _kind, ws in result_files(catalog_dir)), default=0)
