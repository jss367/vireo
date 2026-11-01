"""Photo color-label workflow coordination."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Collection, Iterable

    from db import Database


class PhotoLabelService:
    def __init__(self, db: Database) -> None:
        self.db = db

    def labels_for_photos(self, photo_ids: Collection[int]) -> dict[int, str]:
        return self.db.photo_labels.get_for_photos(photo_ids)

    def descriptions(self) -> dict[str, str]:
        return self.db.photo_labels.get_descriptions()

    def set_description(self, color: str, description: str) -> str:
        # Read-modify-write of the workspace's config overrides.
        with self.db.transaction(immediate=True):
            return self.db.photo_labels.set_description(color, description)

    def set_label(self, photo_id: int, color: str | None) -> None:
        photo = self.db.photos.get(photo_id)
        if photo is None:
            raise LookupError("not found")
        self.db._verify_photo_in_workspace(photo_id)

        # The label and the history entry that undoes it land together, and
        # the recorded old value is the one this write replaces.
        with self.db.transaction(immediate=True):
            old_color = self.db.photo_labels.get(photo_id) or ""
            new_color = color or ""
            if color:
                self.db.photo_labels.set(photo_id, color)
            else:
                self.db.photo_labels.remove(photo_id)
            self.db.record_edit(
                "color_label",
                f'Set color to {color or "none"}',
                new_color,
                [{
                    "photo_id": photo_id,
                    "old_value": old_color,
                    "new_value": new_color,
                }],
            )

    def set_labels(self, photo_ids: Iterable[int], color: str | None) -> int:
        valid_ids = self.db.photo_visibility.visible_photo_ids(photo_ids)
        with self.db.transaction(immediate=True):
            old_labels = self.db.photo_labels.get_for_photos(valid_ids)
            new_color = color or ""
            self.db.photo_labels.set_many(valid_ids, color)
            items = [
                {
                    "photo_id": photo_id,
                    "old_value": old_labels.get(photo_id, ""),
                    "new_value": new_color,
                }
                for photo_id in valid_ids
            ]
            if items:
                self.db.record_edit(
                    "color_label",
                    f'Set color to {color or "none"} on {len(valid_ids)} photos',
                    new_color,
                    items,
                    is_batch=True,
                )
        return len(valid_ids)
