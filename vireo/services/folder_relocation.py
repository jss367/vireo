"""Relocating a folder: its two outcomes are two explicit units of work.

``POST /api/folders/<id>/relocate`` points a folder row at a new path. When
another row already tracks that path, a *missing* folder is merged into it
(``services.folder_merges.merge_into_existing``); a folder that is not missing is
refused. A refusal is reported to the user as a 409, and what the refusal
keeps differs by cause:

- **A refused merge keeps nothing.** The shared-identity check can refuse
  part-way through the merge (``IdentityMergeConflict``), after rows have
  moved, so the merge runs in a savepoint and a refusal rolls back to it.
- **A folder whose own path came back keeps its status refresh.** The folder
  was marked missing, but its original path is on disk again, so it is not
  merged; its status becomes ``ok`` in the same unit that reports the
  refusal, so the folder tree stops showing it as missing.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from repositories.folders import OriginalPathReturned
from services import folder_merges

if TYPE_CHECKING:
    from db import Database


def relocate_folder(
    db: Database, folder_id: int, new_path: str,
) -> tuple[list[dict[str, Any]], ValueError | None]:
    """Relocate ``folder_id`` to ``new_path``; return ``(cascaded, refusal)``.

    ``cascaded`` lists the missing child folders that moved with it. A refusal
    is returned, not raised, after the unit has committed what it keeps (see
    the module docstring).
    """
    with db.transaction():
        try:
            # A savepoint: a refusal raised part-way through a merge leaves
            # none of the merge behind.
            with db.transaction():
                return folder_merges.relocate(db, folder_id, new_path), None
        except OriginalPathReturned as exc:
            db.folders.mark_ok(folder_id)
            return [], exc
        except ValueError as exc:
            return [], exc
