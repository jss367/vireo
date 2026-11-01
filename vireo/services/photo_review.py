"""Photo rating and flag workflow coordination."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence

    from db import Database
    from services.service_types import JsonDict

#: Bounds on the provenance a flag write may carry (``source``: who decided,
#: ``reason``: why). Long enough for a sentence, short enough for a panel.
FLAG_SOURCE_MAX_CHARS = 60
FLAG_REASON_MAX_CHARS = 300


def flag_description(
    flag: str, *, count: int | None = None, source: str | None = None, reason: str | None = None,
) -> str:
    """The edit-history line for a flag write.

    ``Set flag to rejected`` for the user's own click, ``... on 3 photos``
    for a batch, and with provenance ``Set flag to rejected — <source>:
    <reason>`` (or just the source, or just the reason) so the Undo panel
    and toast say who decided and why.
    """
    text = f"Set flag to {flag}"
    if count is not None:
        text += f" on {count} photos"
    if source and reason:
        text += f" — {source}: {reason}"
    elif source:
        text += f" — by {source}"
    elif reason:
        text += f" — {reason}"
    return text


def flag_note(db: Database, photo_id: int, current_flag: str | None) -> JsonDict | None:
    """Only durable current attribution, never a guess from retained undo history."""
    note = db.photo_review.current_flag_note(photo_id)
    if note is None or (note["flag"] or "none") != (current_flag or "none"):
        return None
    return note


class FlagConflict(ValueError):
    """The flag displayed by a reviewer is no longer current."""


class WorkspaceConflict(ValueError):
    """The workspace displayed by a reviewer is no longer active."""


class PhotoReviewService:
    def __init__(self, db: Database) -> None:
        self.db = db

    def set_rating(self, photo_id: int, rating: int) -> None:
        self._write_ratings([photo_id], rating, is_batch=False)

    def set_flag(
        self, photo_id: int, flag: str, *, source: str | None = None, reason: str | None = None,
        observed_flag: str | None = None, observed_workspace_id: int | None = None,
    ) -> None:
        self._write_flags(
            [photo_id], flag, source=source, reason=reason, is_batch=False,
            observed_flags={photo_id: observed_flag} if observed_flag is not None else None,
            observed_workspace_id=observed_workspace_id,
        )

    def set_ratings(self, photo_ids: Sequence[int], rating: int) -> int:
        return self._write_ratings(photo_ids, rating, is_batch=True)

    def _write_ratings(self, photo_ids: Sequence[int], rating: int, *, is_batch: bool) -> int:
        # The rating, its sidecar work and its undo entry are one unit. Inside
        # a caller's unit or transaction it is a savepoint that leaves the
        # commit (and the history prune) to that caller.
        owns_transaction = not self.db.in_transaction
        with self.db.transaction(immediate=True, join=True):
            photos = self.db.photos.get_by_ids(photo_ids)
            old_values = {pid: photos[pid]["rating"] for pid in photo_ids if pid in photos}
            if not old_values:
                if not is_batch:
                    raise ValueError("Photo not in active workspace")
                return 0
            valid_ids = list(old_values)
            self.db.photo_review.set_ratings(valid_ids, rating)
            for pid in valid_ids:
                self.db.queue_change(pid, "rating", str(rating))
            description = f"Set rating to {rating}"
            if is_batch:
                description += f" on {len(photo_ids)} photos"
            self.db.record_edit(
                "rating", description, str(rating),
                [{"photo_id": pid, "old_value": str(old), "new_value": str(rating)}
                 for pid, old in old_values.items()],
                is_batch=is_batch, _commit=False,
            )
        if owns_transaction:
            self.db.prune_edit_history()
        return len(old_values)

    def set_flags(
        self, photo_ids: Sequence[int], flag: str, *, source: str | None = None, reason: str | None = None,
        description: str | None = None,
    ) -> int:
        return self._write_flags(
            photo_ids, flag, source=source, reason=reason, is_batch=True, description=description,
        )

    def apply_culling_flags(
        self, keepers: Sequence[int], rejects: Sequence[int], unflag: Sequence[int],
    ) -> dict[str, int]:
        """Save explicit Cull choices and their attribution as one reversible edit.

        Every explicit Review choice publishes photographer attribution and an
        edit-history row even when the raw flag is already clear, so a later
        ranking job cannot replace it and an unflag-only Apply still produces
        an edit to Undo. ``cleared`` names the subset whose raw flag actually
        changed, used only for the user-visible count.
        """
        ids = list(keepers) + list(rejects) + list(unflag)
        owns_transaction = not self.db.in_transaction
        with self.db.transaction(immediate=True, join=True):
            photos = self.db.photos.get_by_ids(ids)
            if any(pid not in photos for pid in ids):
                raise ValueError("Photo not in active workspace")
            for pid in ids:
                self.db._verify_photo_in_workspace(pid)
            old = {pid: photos[pid]["flag"] or "none" for pid in ids}
            cleared = [pid for pid in unflag if old[pid] != "none"]
            effective_ids = list(keepers) + list(rejects) + list(unflag)
            before = self.db.photo_review.flag_attributions(effective_ids)
            items = []
            summary = f"Culling: flagged {len(keepers)}, rejected {len(rejects)}"
            if cleared:
                summary += f", cleared {len(cleared)}"
            for photo_ids, flag in (
                (list(keepers), "flagged"), (list(rejects), "rejected"), (list(unflag), "none"),
            ):
                for pid in photo_ids:
                    self.db.photo_review.set_flag(pid, flag)
                    self.db.queue_flag_change_if_enabled(pid, flag)
                    items.append({"photo_id": pid, "old_value": old[pid], "new_value": flag})
                if photo_ids:
                    self.db.photo_review.set_current_flag_note(photo_ids, flag, summary)
            if items:
                edit_id = self.db.record_edit(
                    "flag", summary, "culling_apply", items, is_batch=True, _commit=False,
                )
                self.db.photo_review.record_flag_attribution_history(
                    edit_id, effective_ids, before,
                    self.db.photo_review.flag_attributions(effective_ids),
                )
        if owns_transaction and items:
            self.db.prune_edit_history()
        return {"keepers": len(keepers), "rejects": len(rejects), "cleared": len(cleared)}

    def _write_flags(
        self, photo_ids: Sequence[int], flag: str, *, source: str | None, reason: str | None, is_batch: bool,
        observed_flags: dict[int, str] | None = None, observed_workspace_id: int | None = None,
        description: str | None = None,
    ) -> int:
        owns_transaction = not self.db.in_transaction
        with self.db.transaction(immediate=True, join=True):
            # Database caches its workspace when opened. Activation in another
            # request must be checked from persisted state under this writer lock.
            if observed_workspace_id is not None and (
                self.db.active_workspace_id != observed_workspace_id
                or self.db.workspaces.most_recently_opened_id() != observed_workspace_id
            ):
                raise WorkspaceConflict("The active workspace changed. Reload Review before changing flags.")
            photos = self.db.photos.get_by_ids(photo_ids)
            old_values = {pid: photos[pid]["flag"] for pid in photo_ids if pid in photos}
            if not old_values:
                if not is_batch:
                    raise ValueError("Photo not in active workspace")
                return 0
            if observed_flags is not None and any(
                (old_values.get(pid) or "none") != observed for pid, observed in observed_flags.items()
            ):
                raise FlagConflict("This photo's flag changed elsewhere. Reload it before changing the flag.")
            valid_ids = list(old_values)
            before = self.db.photo_review.flag_attributions(valid_ids)
            description = description or flag_description(
                flag, count=len(photo_ids) if is_batch else None, source=source, reason=reason,
            )
            self.db.photo_review.set_flags(valid_ids, flag)
            for pid in valid_ids:
                self.db.queue_flag_change_if_enabled(pid, flag)
            edit_id = self.db.record_edit(
                "flag", description, flag,
                [{"photo_id": pid, "old_value": old, "new_value": flag} for pid, old in old_values.items()],
                is_batch=is_batch, source=source, reason=reason, _commit=False,
            )
            self.db.photo_review.set_current_flag_note(
                valid_ids, flag, description, source=source, reason=reason,
            )
            self.db.photo_review.record_flag_attribution_history(
                edit_id, valid_ids, before, self.db.photo_review.flag_attributions(valid_ids),
            )
        if owns_transaction:
            self.db.prune_edit_history()
        return len(old_values)
