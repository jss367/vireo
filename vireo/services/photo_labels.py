"""Photo color-label workflow coordination."""


class PhotoLabelService:
    def __init__(self, db):
        self.db = db

    def labels_for_photos(self, photo_ids):
        return self.db.get_color_labels_for_photos(photo_ids)

    def set_label(self, photo_id, color):
        photo = self.db.get_photo(photo_id)
        if photo is None:
            raise LookupError("not found")
        self.db._verify_photo_in_workspace(photo_id)

        old_color = self.db.get_color_label(photo_id) or ""
        new_color = color or ""
        if color:
            self.db.set_color_label(photo_id, color)
        else:
            self.db.remove_color_label(photo_id)
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

    def set_labels(self, photo_ids, color):
        valid_ids = self.db.filter_photo_ids_in_workspace(photo_ids)
        old_labels = self.db.get_color_labels_for_photos(valid_ids)
        new_color = color or ""
        self.db.batch_set_color_label(valid_ids, color)
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
