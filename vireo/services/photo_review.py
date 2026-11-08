"""Photo rating and flag workflow coordination."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

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


def keeper_status(note: Mapping[str, object]) -> str | None:
    """Whether a relational reject's keeper still justifies it.

    ``kept`` while the keeper is in the catalog and not rejected,
    ``rejected`` once it is, ``deleted`` when it has left the catalog; None
    for a note that names no keeper.
    """
    if note.get("keeper_photo_id") is None:
        return None
    if note.get("keeper_filename") is None:
        return "deleted"
    if note.get("keeper_flag") == "rejected":
        return "rejected"
    return "kept"


def _keeper_id(rejection: Mapping[str, object]) -> int | None:
    """The keeper of one rejection record (None: decided with nothing kept to compare against)."""
    keeper = rejection.get("keeper_photo_id")
    if keeper is None:
        return None
    if isinstance(keeper, bool) or not isinstance(keeper, int):
        raise ValueError("keeper_photo_id must be an integer photo id")
    return keeper


def flag_note(db: Database, photo_id: int, current_flag: str | None) -> JsonDict | None:
    """Only durable current attribution, never a guess from retained undo history.

    A reject recorded in favor of another photo carries ``keeper_photo_id``,
    ``keeper_filename``, ``comparison`` and ``keeper_status`` (see
    ``keeper_status``) so the detail panel can show what it lost to and
    whether that reason still holds.
    """
    note = db.photo_review.current_flag_note(photo_id)
    if note is None or (note["flag"] or "none") != (current_flag or "none"):
        return None
    note["keeper_status"] = keeper_status(note)
    note.pop("keeper_flag", None)
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
        # commit to that caller; the history prune follows the commit.
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
                self.db.pending_changes.queue(pid, "rating", str(rating))
            description = f"Set rating to {rating}"
            if is_batch:
                description += f" on {len(photo_ids)} photos"
            self.db.record_edit(
                "rating", description, str(rating),
                [{"photo_id": pid, "old_value": str(old), "new_value": str(rating)}
                 for pid, old in old_values.items()],
                is_batch=is_batch,
            )
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
        rejections: Mapping[int, Mapping[str, object]] | None = None,
    ) -> dict[str, int]:
        """Save explicit Cull choices and their attribution as one reversible edit.

        Every explicit Review choice publishes photographer attribution and an
        edit-history row even when the raw flag is already clear, so a later
        ranking job cannot replace it and an unflag-only Apply still produces
        an edit to Undo. ``cleared`` names the subset whose raw flag actually
        changed, used only for the user-visible count.

        ``rejections`` maps a rejected photo to ``{"keeper_photo_id",
        "comparison"}``: the photo kept instead of it and the measurement that
        decided (``comparison`` may be None when none was shared). The keeper
        must be a photo of the active workspace that is kept: flagged by this
        Apply or not currently rejected, so a reject is never recorded against
        a photo that cannot justify it. The attribution then names it so the
        reject stays explainable and the orphan check can find it if the
        keeper is later deleted or rejected. An entry whose keeper is None
        is a reject decided now with nothing kept to compare against, and
        gets a plain note; a reject sent with no entry at all keeps whatever
        record it has (the page sends none for a reject it did not decide).
        """
        rejections = dict(rejections or {})
        ids = list(keepers) + list(rejects) + list(unflag)
        keeper_ids = [k for k in (_keeper_id(r) for r in rejections.values()) if k is not None]
        with self.db.transaction(immediate=True, join=True):
            photos = self.db.photos.get_by_ids(ids + keeper_ids)
            if any(pid not in photos for pid in ids):
                raise ValueError("Photo not in active workspace")
            if any(pid not in photos for pid in keeper_ids):
                raise ValueError("Keeper photo not in active workspace")
            for pid in keeper_ids:
                if pid not in keepers and photos[pid]["flag"] == "rejected":
                    raise ValueError(f"Keeper photo {pid} is itself rejected")
            for pid in ids + keeper_ids:
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
                # A reject already saved in favor of a keeper keeps that record
                # when Apply rewrites it unchanged: the flag UPDATE drops the
                # attribution, and a fresh summary note would erase the relation.
                preserved = {
                    pid for pid in photo_ids
                    if flag == "rejected" and pid not in rejections and old[pid] == "rejected"
                    and before.get(pid, {}).get("keeper_photo_id") is not None
                }
                plain_ids = [pid for pid in photo_ids if pid not in rejections and pid not in preserved]
                if plain_ids:
                    self.db.photo_review.set_current_flag_note(plain_ids, flag, summary)
                for pid in photo_ids:
                    if pid in rejections:
                        self._note_relational_reject(pid, rejections[pid], photos, summary)
                    elif pid in preserved:
                        self.db.photo_review.republish_flag_note(pid, before[pid])
            if items:
                edit_id = self.db.record_edit(
                    "flag", summary, "culling_apply", items, is_batch=True,
                )
                self.db.photo_review.record_flag_attribution_history(
                    edit_id, effective_ids, before,
                    self.db.photo_review.flag_attributions(effective_ids),
                )
        return {"keepers": len(keepers), "rejects": len(rejects), "cleared": len(cleared)}

    def _note_relational_reject(
        self, photo_id: int, rejection: Mapping[str, object],
        photos: Mapping[int, Mapping[str, object]], summary: str,
    ) -> None:
        """Attribute one reject to the photo kept instead of it."""
        keeper_id = _keeper_id(rejection)
        if keeper_id is None:
            self.db.photo_review.set_current_flag_note([photo_id], "rejected", summary)
            return
        comparison = rejection.get("comparison")
        description = f"{summary} — kept {photos[keeper_id]['filename']} instead"
        self.db.photo_review.set_current_flag_note(
            [photo_id], "rejected", description,
            keeper_photo_id=keeper_id,
            comparison=comparison if isinstance(comparison, dict) else None,
        )

    def _write_flags(
        self, photo_ids: Sequence[int], flag: str, *, source: str | None, reason: str | None, is_batch: bool,
        observed_flags: dict[int, str] | None = None, observed_workspace_id: int | None = None,
        description: str | None = None,
    ) -> int:
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
                is_batch=is_batch, source=source, reason=reason,
            )
            self.db.photo_review.set_current_flag_note(
                valid_ids, flag, description, source=source, reason=reason,
            )
            self.db.photo_review.record_flag_attribution_history(
                edit_id, valid_ids, before, self.db.photo_review.flag_attributions(valid_ids),
            )
        return len(old_values)
