"""Import provenance and current collection coverage, without filesystem work."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

from db import AUTO_MATCH_REVIEW_MARKER

if TYPE_CHECKING:
    from db import Database
    from services.service_types import JsonDict


def collection_coverage(db: Database, collection_id: int, missing_snapshot: JsonDict) -> JsonDict:
    """Counts share current collection membership, including offline folders.

    A persisted prediction decision proves neither a completed inspection nor
    a resolved identification. Species keywords are counted independently.
    The original import total is never reconstructed from mutable membership.
    """
    owns_transaction = not db.in_transaction
    if owns_transaction:
        db.begin()
    try:
        return _coverage_snapshot(db, collection_id, missing_snapshot)
    finally:
        if owns_transaction:
            db.rollback()


def _coverage_snapshot(db: Database, collection_id: int, missing_snapshot: JsonDict) -> JsonDict:
    """All catalog counters read one SQLite snapshot, even during background work."""
    row = db.collections.import_provenance(collection_id)
    if row is None:
        raise LookupError("Collection not found in this workspace")
    counts = db.collections.count_photo_availability(collection_id)
    identified = db.collections.count_photos_for_rules(
        [{"field": "has_species", "op": "is", "value": True}], collection_id=collection_id,
        include_offline_folders=True,
    )
    return {
        "collection_id": collection_id,
        "name": row["name"],
        "snapshot_at": datetime.now(UTC).isoformat(),
        "original_import": {
            "count": row["original_import_count"],
            "recorded_at": row["import_recorded_at"],
        },
        "current_membership": {
            "catalog_count": counts["total"],
            "online_folder_count": counts["available"],
            "offline_folder_count": counts["offline"],
        },
        "original_files": _original_file_counts(db, collection_id, missing_snapshot),
        "review": {
            "denominator": counts["total"],
            "denominator_definition": "Current catalog records matching this collection in this workspace, including offline folders",
            "inspected_count": None,
            "inspection_definition": "Inspection is not recorded by the catalog; loading a saved result does not prove inspection",
            "prediction_decision_count": db.collections.count_prediction_decision_photos(
                collection_id, AUTO_MATCH_REVIEW_MARKER, db.DECIDED_PREDICTION_STATUSES,
            ),
            "decision_definition": "Photos with at least one accepted, rejected or marked-reviewed prediction recorded in this workspace, excluding automatic taxonomy matches; other predictions may remain pending",
            "with_species_count": identified,
            "without_species_count": counts["total"] - identified,
            "identification_definition": "Species-rank keywords currently attached to the photo; absence does not prove the photo was inspected or has wildlife",
            "unresolved_identity_count": _explicitly_unresolved_count(db, collection_id),
            "unresolved_definition": "Photos explicitly retained with unresolved identity alternatives in this workspace; may also carry an unrelated confirmed species. Unknown when that review workflow is unavailable",
        },
    }


def _explicitly_unresolved_count(db: Database, collection_id: int) -> int | None:
    """Compose with the separately shipped unresolved-identification workflow.

    Its accessor owns the workspace and visibility rules. An older build with
    no such domain must report unknown, rather than turn absent species
    keywords into evidence of an explicit unresolved decision.
    """
    alternatives = getattr(db, "identity_alternatives", None)
    if alternatives is None:
        return None
    ids = list(alternatives.photo_ids())
    if not ids:
        return 0
    return db.collections.count_photos_for_rules(
        [{"field": "photo_ids", "value": ids}], collection_id=collection_id,
        include_offline_folders=True,
    )


def _original_file_counts(db: Database, collection_id: int, snapshot: JsonDict) -> JsonDict:
    """Only report known missing originals from a fresh workspace scan.

    Do not infer that every other file is accessible: Missing Originals skips
    offline folders, and a collection may have gained members since the scan.
    """
    fresh = snapshot.get("status") == "ready" and not snapshot.get("stale")
    missing = None
    if fresh:
        ids = [p["id"] for p in snapshot.get("photos", [])]
        missing = db.collections.count_photos_for_rules(
            [{"field": "photo_ids", "value": ids}], collection_id=collection_id,
            include_offline_folders=True,
        ) if ids else 0
    return {
        "accessible_count": None,
        "known_missing_count": missing,
        "checked_at": snapshot.get("checked_at"),
        "scan_status": snapshot.get("status", "not_ready"),
        "stale": bool(snapshot.get("stale")),
        "definition": "Known missing originals from the last workspace Missing Originals scan, intersected with current membership. Online folder status does not verify individual files; offline originals are not checked",
    }
