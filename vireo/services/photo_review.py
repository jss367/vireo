"""Photo rating and flag workflow coordination."""


class PhotoReviewService:
    def __init__(self, db):
        self.db = db

    def set_rating(self, photo_id, rating):
        old = self.db.get_photo(photo_id)
        old_rating = old["rating"] if old else 0
        self.db.update_photo_rating(photo_id, rating)
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

    def set_flag(self, photo_id, flag):
        old = self.db.get_photo(photo_id)
        old_flag = old["flag"] if old else "none"
        self.db.update_photo_flag(photo_id, flag)
        self.db.queue_flag_change_if_enabled(photo_id, flag)
        self.db.record_edit(
            "flag",
            f"Set flag to {flag}",
            flag,
            [{
                "photo_id": photo_id,
                "old_value": old_flag,
                "new_value": flag,
            }],
        )

    def set_ratings(self, photo_ids, rating):
        photos_map = self.db.get_photos_by_ids(photo_ids)
        old_values = {
            photo_id: photos_map[photo_id]["rating"]
            for photo_id in photo_ids
            if photo_id in photos_map
        }
        valid_ids = list(old_values)
        self.db.batch_update_photo_rating(valid_ids, rating)
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

    def set_flags(self, photo_ids, flag):
        photos_map = self.db.get_photos_by_ids(photo_ids)
        old_values = {
            photo_id: photos_map[photo_id]["flag"]
            for photo_id in photo_ids
            if photo_id in photos_map
        }
        valid_ids = list(old_values)
        self.db.batch_update_photo_flag(valid_ids, flag)
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
            f"Set flag to {flag} on {len(photo_ids)} photos",
            flag,
            items,
            is_batch=True,
        )
        return len(old_values)
