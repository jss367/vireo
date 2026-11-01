"""Static-only Database contracts; these functions never open a real catalog.

The ignores deliberately name invalid calls. Unnecessary-ignore errors make
the fixture fail if an API becomes too permissive to catch those mistakes.
"""

# pyright: reportUnnecessaryTypeIgnoreComment=true

from pathlib import Path
from typing import Any, assert_type

from db import BrowseStackConfig, Database, normalize_browse_stack_config
from repositories.collections import CollectionRepository
from repositories.photos import PhotoRepository
from repositories.species_provenance import SpeciesProvenanceRepository
from repositories.storage import StorageRepository
from services.edit_history import redo_last_undo, undo_last_edit
from services.workspace_config import browse_stack_settings


def check_database_api(db: Database) -> None:
    assert_type(db.closed, bool)
    assert_type(db.in_transaction, bool)
    assert_type(db.active_workspace_id, int | None)
    assert_type(db.require_workspace_id(), int)
    db.set_active_workspace(None)
    db.set_active_workspace(1)
    assert_type(db.create_workspace("Birds"), int)
    assert_type(db.photos, PhotoRepository)
    assert_type(db.collections, CollectionRepository)
    assert_type(db.species_provenance, SpeciesProvenanceRepository)
    assert_type(db.storage, StorageRepository)
    assert_type(db._photos_repository(), PhotoRepository)
    assert_type(db._collection_repository(), CollectionRepository)
    assert_type(db.add_keyword("Robin", is_species=True), int)
    assert_type(db.get_photo_edit_recipe(1), dict[str, Any] | None)
    assert_type(db.accept_prediction(1), dict[str, Any] | None)
    assert_type(undo_last_edit(db), dict[str, Any] | None)
    assert_type(redo_last_undo(db), dict[str, Any] | None)
    assert_type(db.queue_change(1, "rating", "5"), str | None)
    assert_type(db.remove_pending_changes(1), int)
    db.set_progress_handler(lambda: 1, 100)
    db.set_progress_handler(None, 0)
    with db.transaction() as result:
        assert_type(result, None)
    with db.transaction() as result:
        assert_type(result, None)
    with db as opened:
        assert_type(opened, Database)
    assert_type(normalize_browse_stack_config({"time_gap": 2.0}), BrowseStackConfig)
    assert_type(browse_stack_settings(db), BrowseStackConfig)


def check_path_input(path: Path) -> None:
    assert_type(Database(path)._db_path, str)


def check_invalid_calls(db: Database) -> None:
    db.set_active_workspace("1")  # pyright: ignore[reportArgumentType]
    db.add_keyword("Robin", parent_id="1")  # pyright: ignore[reportArgumentType]
    db.tag_photo(1, None)  # pyright: ignore[reportArgumentType]
    db.commit()
    db.record_edit("rating", "Rated photo", "5", [None])  # pyright: ignore[reportArgumentType]
