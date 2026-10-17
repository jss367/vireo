"""Photo rating and flag workflow coordination."""

#: Bounds on the provenance a flag write may carry (``source``: who decided,
#: ``reason``: why). Long enough for a sentence, short enough for a panel.
FLAG_SOURCE_MAX_CHARS = 60
FLAG_REASON_MAX_CHARS = 300


def flag_description(flag, *, count=None, source=None, reason=None):
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


def flag_note(db, photo_id, current_flag):
    """Who set this photo's current flag and why, when that was recorded.

    The newest live ``flag`` edit covering the photo, as ``{"flag",
    "source", "reason", "description", "at"}``, provided it set the flag the
    photo carries now (a flag changed since without history, by a pipeline
    apply for example, has no trustworthy note) and carries a source or a
    reason (a hand click has neither, and showing "Rejected" twice says
    nothing). None otherwise.
    """
    note = db.edit_history.latest_flag_note(photo_id)
    if not note:
        return None
    if (note["new_value"] or "none") != (current_flag or "none"):
        return None
    if not note["source"] and not note["reason"]:
        return None
    return {
        "flag": note["new_value"] or "none",
        "source": note["source"],
        "reason": note["reason"],
        "description": note["description"],
        "at": note["created_at"],
    }


class PhotoReviewService:
    def __init__(self, db):
        self.db = db

    def set_rating(self, photo_id, rating):
        old = self.db.get_photo(photo_id)
        old_rating = old["rating"] if old else 0
        self.db.photo_review.set_rating(photo_id, rating)
        self.db.queue_change(photo_id, "rating", str(rating))
        self.db.record_edit(
            "rating",
            f"Set rating to {rating}",
            str(rating),
            [{
                "photo_id": photo_id,
                "old_value": str(old_rating),
                "new_value": str(rating),
            }],
        )

    def set_flag(self, photo_id, flag, *, source=None, reason=None):
        old = self.db.get_photo(photo_id)
        old_flag = old["flag"] if old else "none"
        self.db.photo_review.set_flag(photo_id, flag)
        self.db.queue_flag_change_if_enabled(photo_id, flag)
        self.db.record_edit(
            "flag",
            flag_description(flag, source=source, reason=reason),
            flag,
            [{
                "photo_id": photo_id,
                "old_value": old_flag,
                "new_value": flag,
            }],
            source=source,
            reason=reason,
        )

    def set_ratings(self, photo_ids, rating):
        photos_map = self.db.get_photos_by_ids(photo_ids)
        old_values = {
            photo_id: photos_map[photo_id]["rating"]
            for photo_id in photo_ids
            if photo_id in photos_map
        }
        valid_ids = list(old_values)
        self.db.photo_review.set_ratings(valid_ids, rating)
        for photo_id in valid_ids:
            self.db.queue_change(photo_id, "rating", str(rating))
        items = [
            {
                "photo_id": photo_id,
                "old_value": str(old_values[photo_id]),
                "new_value": str(rating),
            }
            for photo_id in old_values
        ]
        self.db.record_edit(
            "rating",
            f"Set rating to {rating} on {len(photo_ids)} photos",
            str(rating),
            items,
            is_batch=True,
        )
        return len(old_values)

    def set_flags(self, photo_ids, flag, *, source=None, reason=None):
        photos_map = self.db.get_photos_by_ids(photo_ids)
        old_values = {
            photo_id: photos_map[photo_id]["flag"]
            for photo_id in photo_ids
            if photo_id in photos_map
        }
        valid_ids = list(old_values)
        self.db.photo_review.set_flags(valid_ids, flag)
        for index, photo_id in enumerate(valid_ids):
            self.db.queue_flag_change_if_enabled(
                photo_id,
                flag,
                _commit=index == len(valid_ids) - 1,
            )
        items = [
            {
                "photo_id": photo_id,
                "old_value": old_values[photo_id],
                "new_value": flag,
            }
            for photo_id in old_values
        ]
        self.db.record_edit(
            "flag",
            flag_description(flag, count=len(photo_ids), source=source, reason=reason),
            flag,
            items,
            is_batch=True,
            source=source,
            reason=reason,
        )
        return len(old_values)
