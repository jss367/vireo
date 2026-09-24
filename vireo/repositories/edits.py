"""Persistence for non-destructive edits: per-photo recipes and global presets.

Neither table is workspace-scoped (a recipe belongs to its photo, a preset is
a look that is the same in every workspace), so the repository takes no
workspace id. The optional active-workspace check on recipe writes stays on
``Database``, which runs it before delegating here.
"""

import logging

log = logging.getLogger(__name__)


class EditsRepository:
    def __init__(self, conn, *, chunk_size=800, preset_name_max=80):
        self.conn = conn
        self.chunk_size = chunk_size
        self.preset_name_max = preset_name_max

    def _chunks(self, values):
        values = list(values)
        return (
            values[index:index + self.chunk_size]
            for index in range(0, len(values), self.chunk_size)
        )

    # -- per-photo edit recipes ------------------------------------------------

    def get_photo_recipe(self, photo_id):
        """Return the normalized edit recipe dict for a photo, or None."""
        row = self.conn.execute(
            "SELECT recipe_json FROM photo_edit_recipes WHERE photo_id = ?",
            (photo_id,),
        ).fetchone()
        if not row:
            return None
        try:
            from image_edits import copy_recipe
            return copy_recipe(row["recipe_json"])
        except Exception:
            log.warning("Invalid stored edit recipe for photo %s", photo_id, exc_info=True)
            return None

    def get_photo_recipes(self, photo_ids):
        """Return {photo_id: normalized recipe dict} for the given photos."""
        if not photo_ids:
            return {}
        out = {}
        from image_edits import copy_recipe
        for chunk in self._chunks(photo_ids):
            placeholders = ",".join("?" for _ in chunk)
            rows = self.conn.execute(
                f"SELECT photo_id, recipe_json FROM photo_edit_recipes "
                f"WHERE photo_id IN ({placeholders})",
                list(chunk),
            ).fetchall()
            for row in rows:
                try:
                    recipe = copy_recipe(row["recipe_json"])
                except Exception:
                    log.warning(
                        "Invalid stored edit recipe for photo %s",
                        row["photo_id"], exc_info=True,
                    )
                    continue
                if recipe:
                    out[row["photo_id"]] = recipe
        return out

    def set_photo_recipe(self, photo_id, recipe, _commit=True):
        """Set or clear a photo's edit recipe; see ``Database.set_photo_edit_recipe``."""
        from image_edits import copy_recipe, recipe_to_json
        recipe_json = recipe_to_json(recipe)
        if recipe_json is None:
            self.conn.execute(
                "DELETE FROM photo_edit_recipes WHERE photo_id = ?",
                (photo_id,),
            )
            if _commit:
                self.conn.commit()
            return None
        self.conn.execute(
            """INSERT INTO photo_edit_recipes (photo_id, recipe_json, updated_at)
               VALUES (?, ?, datetime('now'))
               ON CONFLICT(photo_id) DO UPDATE SET
                   recipe_json = excluded.recipe_json,
                   updated_at = excluded.updated_at""",
            (photo_id, recipe_json),
        )
        if _commit:
            self.conn.commit()
        return copy_recipe(recipe_json)

    def clear_photo_recipe(self, photo_id):
        """Remove a photo's edit recipe. Returns True if a row was removed."""
        cur = self.conn.execute(
            "DELETE FROM photo_edit_recipes WHERE photo_id = ?",
            (photo_id,),
        )
        self.conn.commit()
        return cur.rowcount > 0

    # -- edit presets (global reusable development settings) -------------------

    def list_presets(self):
        """Return all edit presets, sorted case-insensitively by name."""
        from edit_batch import decode_preset

        rows = self.conn.execute(
            "SELECT id, name, recipe_json, updated_at FROM edit_presets"
        ).fetchall()
        out = []
        for row in rows:
            try:
                recipe, fields = decode_preset(row["recipe_json"])
            except Exception:
                log.warning(
                    "Invalid stored edit preset %s (%r)",
                    row["id"], row["name"], exc_info=True,
                )
                continue
            out.append({
                "id": row["id"],
                "name": row["name"],
                "recipe": recipe,
                **({"fields": fields} if fields is not None else {}),
                "updated_at": row["updated_at"],
            })
        out.sort(key=lambda p: p["name"].casefold())
        return out

    def save_preset(self, name, recipe, fields=None):
        """Create or overwrite (by trimmed name) a global edit preset.

        See ``Database.save_edit_preset`` for the validation rules.
        Returns the stored preset dict.
        """
        from image_edits import (
            RecipeError,
            normalize_recipe,
            recipe_to_json,
        )

        if not isinstance(name, str) or not name.strip():
            raise ValueError("preset name must not be blank")
        name = name.strip()
        if len(name) > self.preset_name_max:
            raise ValueError(
                f"preset name must be {self.preset_name_max} "
                "characters or fewer"
            )

        if isinstance(recipe, str):
            recipe = normalize_recipe(recipe) or {}
        if not isinstance(recipe, dict):
            raise RecipeError("recipe must be an object")
        from edit_batch import decode_preset, encode_preset

        if fields is not None:
            recipe_json = encode_preset(recipe, fields)
        else:
            normalized = normalize_recipe(
                {"adjustments": recipe.get("adjustments") or {}}
            )
            if not (normalized or {}).get("adjustments"):
                raise ValueError("preset must include at least one adjustment")
            recipe_json = recipe_to_json(normalized)

        self.conn.execute(
            """INSERT INTO edit_presets (name, recipe_json, updated_at)
               VALUES (?, ?, datetime('now'))
               ON CONFLICT(name) DO UPDATE SET
                   recipe_json = excluded.recipe_json,
                   updated_at = excluded.updated_at""",
            (name, recipe_json),
        )
        self.conn.commit()
        row = self.conn.execute(
            "SELECT id, name, recipe_json, updated_at FROM edit_presets "
            "WHERE name = ?",
            (name,),
        ).fetchone()
        saved_recipe, saved_fields = decode_preset(row["recipe_json"])
        return {
            "id": row["id"],
            "name": row["name"],
            "recipe": saved_recipe,
            **({"fields": saved_fields} if saved_fields is not None else {}),
            "updated_at": row["updated_at"],
        }

    def delete_preset(self, preset_id):
        """Delete an edit preset. Returns True if a row was removed."""
        cur = self.conn.execute(
            "DELETE FROM edit_presets WHERE id = ?", (preset_id,)
        )
        self.conn.commit()
        return cur.rowcount > 0
