"""Sync engine: reconcile database and XMP sidecars."""

import logging
import os
from collections import defaultdict

from keyword_normalization import keyword_match_key, normalize_keyword_display
from xmp import (
    read_keywords,
    remove_keywords,
    remove_vireo_gps_location,
    write_edit_recipe,
    write_gps_location,
    write_pick_flag,
    write_rating,
    write_sidecar,
)

log = logging.getLogger(__name__)


def _get_xmp_path_for_photo(db, photo_id):
    """Determine the XMP sidecar path for a photo."""
    photo = db.get_photo(photo_id)
    if not photo:
        return None
    folders = {f["id"]: f["path"] for f in db.get_folder_tree()}
    folder_path = folders.get(photo["folder_id"], "")
    base = os.path.splitext(photo["filename"])[0]
    return os.path.join(folder_path, base + ".xmp")


def _sync_flags_to_xmp_enabled(db):
    """Return whether the active workspace should write flags to XMP."""
    try:
        import config as cfg

        return bool(db.get_effective_config(cfg.load()).get("sync_flags_to_xmp", False))
    except Exception:
        log.warning("Failed to read sync_flags_to_xmp config", exc_info=True)
        return False


def _write_assigned_location_to_xmp_enabled(db):
    """Return whether the active workspace should write assigned GPS to XMP."""
    try:
        import config as cfg

        return bool(
            db.get_effective_config(cfg.load()).get(
                "write_assigned_location_to_xmp", False
            )
        )
    except Exception:
        log.warning("Failed to read write_assigned_location_to_xmp config", exc_info=True)
        return False


def sync_to_xmp(db, progress_callback=None, change_ids=None):
    """Write pending changes to XMP sidecars.

    Args:
        db: Database instance
        progress_callback: optional callable(current, total)
        change_ids: optional pending_changes ids to sync. When provided, any
            other queued changes are left pending.

    Returns:
        dict with synced, failed, failures counts
    """
    changes = db.get_pending_changes()
    if change_ids is not None:
        selected_ids = {int(cid) for cid in change_ids}
        # Auto-include any unselected pending keyword_add / keyword_remove
        # changes that share a (photo_id, normalized key) with a selected
        # one. Both remove_keywords() (for keyword_remove) and the
        # add-canonicalization pass below match by normalized key, so a
        # rename's paired add(clean) + remove(legacy variant) split across
        # two syncs lets each half clobber the sidecar entry the other
        # half writes -- the add-only sync strips the legacy `<rdf:li>`
        # before writing the clean spelling, and a later remove-only sync
        # strips the clean spelling under the same normalized match. Sync
        # both sides together whenever the user picks either.
        kw_index = defaultdict(list)
        for c in changes:
            if c["change_type"] in ("keyword_add", "keyword_remove") and c["value"]:
                key = (c["photo_id"], keyword_match_key(c["value"]))
                kw_index[key].append(c["id"])
        for c in changes:
            if c["id"] not in selected_ids:
                continue
            if c["change_type"] not in ("keyword_add", "keyword_remove"):
                continue
            if not c["value"]:
                continue
            key = (c["photo_id"], keyword_match_key(c["value"]))
            selected_ids.update(kw_index[key])
        changes = [c for c in changes if c["id"] in selected_ids]
    if not changes:
        return {"synced": 0, "failed": 0, "failures": []}

    # Group changes by photo_id
    by_photo = defaultdict(list)
    for c in changes:
        by_photo[c["photo_id"]].append(c)

    sync_flags = _sync_flags_to_xmp_enabled(db)
    sync_locations = _write_assigned_location_to_xmp_enabled(db)
    synced = 0
    failed = 0
    failures = []
    synced_ids = []

    total = len(by_photo)
    for i, (photo_id, photo_changes) in enumerate(by_photo.items()):
        xmp_path = _get_xmp_path_for_photo(db, photo_id)
        if not xmp_path:
            failed += 1
            failures.append({"photo_id": photo_id, "error": "photo not found in DB"})
            continue

        # Check if the folder exists (NAS might be offline)
        folder = os.path.dirname(xmp_path)
        if not os.path.isdir(folder):
            failed += 1
            failures.append(
                {"photo_id": photo_id, "error": f"folder not accessible: {folder}"}
            )
            continue

        try:
            # Collect keyword adds/removes and rating/flag changes
            keywords_to_add = set()
            keywords_to_remove = set()
            new_rating = None
            new_flag = None
            edit_recipe_json = None
            sync_location = False
            cleanup_location = False
            supported_ids = []
            unsupported_changes = []

            for c in photo_changes:
                if c["change_type"] == "keyword_add":
                    keywords_to_add.add(c["value"])
                    supported_ids.append(c["id"])
                elif c["change_type"] == "keyword_remove":
                    keywords_to_remove.add(c["value"])
                    supported_ids.append(c["id"])
                elif c["change_type"] == "rating":
                    new_rating = int(c["value"])
                    supported_ids.append(c["id"])
                elif c["change_type"] == "flag":
                    if sync_flags:
                        new_flag = c["value"] or "none"
                        supported_ids.append(c["id"])
                    else:
                        unsupported_changes.append(c)
                elif c["change_type"] == "location":
                    supported_ids.append(c["id"])
                    if sync_locations:
                        sync_location = True
                    else:
                        cleanup_location = True
                elif c["change_type"] == "edit_recipe":
                    edit_recipe_json = c["value"] or ""
                    supported_ids.append(c["id"])

            # Apply keyword removals BEFORE additions. remove_keywords()
            # compares by normalized match key, so a remove of `‘apapane`
            # matches any `<rdf:li>` whose text normalizes to `apapane` --
            # including a clean `apapane` we would otherwise have just added.
            # A rename that queues remove `‘apapane` and add `apapane` for
            # the same photo would then have its newly-written clean entry
            # stripped along with the old quoted one, clearing pending
            # changes and leaving the sidecar without the keyword. Applying
            # the remove first strips only the pre-existing quoted variant;
            # the subsequent write_sidecar then adds the clean spelling.
            #
            # Split removals by whether they're paired with an add for the
            # same normalized key. A paired remove+add is a normalization-only
            # rename (e.g. remove `‘Birds` + add `Birds`); hierarchical mode
            # would then strip unrelated hierarchies like `Animals|Birds|Hawk`
            # because remove_keywords() matches by any pipe-segment key. Use
            # flat-only removal for those paired removes so the rename only
            # touches the flat `dc:subject` legacy entry. Solo removes keep
            # hierarchical semantics so real keyword deletions still drop
            # pipe-segment matches.
            if keywords_to_remove:
                paired_keys = {
                    keyword_match_key(kw) for kw in keywords_to_add
                }
                paired_keys.discard("")
                paired_removes = {
                    kw for kw in keywords_to_remove
                    if keyword_match_key(kw) in paired_keys
                }
                solo_removes = keywords_to_remove - paired_removes
                if solo_removes:
                    remove_keywords(xmp_path, solo_removes)
                if paired_removes:
                    remove_keywords(
                        xmp_path, paired_removes, hierarchical=False,
                    )

            # Strip any sidecar dc:subject entry that normalizes to a
            # keyword we're about to add. write_sidecar() dedupes with an
            # exact-string set difference, so a pure keyword_add for
            # `apapane` against a legacy sidecar `‘apapane` would append a
            # second <rdf:li>. Canonicalizing first collapses variants into
            # the clean spelling that write_sidecar writes below. Use the
            # flat-only mode: a hierarchical remove (which drops any entry
            # whose segment matches) would delete unrelated hierarchies
            # such as `Animals|Birds|Hawk` when we add flat `Birds`.
            if keywords_to_add:
                remove_keywords(xmp_path, keywords_to_add, hierarchical=False)

            # Write keyword additions after removals so a same-photo
            # remove+add pair does not race (see above).
            if keywords_to_add:
                write_sidecar(
                    xmp_path, flat_keywords=keywords_to_add, hierarchical_keywords=set()
                )

            # Write flag before rating: write_pick_flag creates a sidecar if
            # needed, while write_rating intentionally only updates existing
            # sidecars.
            if new_flag is not None:
                write_pick_flag(xmp_path, new_flag)

            # Write rating
            if new_rating is not None:
                write_rating(xmp_path, new_rating)

            if sync_location:
                loc = db.get_assigned_photo_location(photo_id)
                if loc and loc.get("latitude") is not None and loc.get("longitude") is not None:
                    write_gps_location(
                        xmp_path,
                        loc["latitude"],
                        loc["longitude"],
                        source=loc.get("source") or "assigned",
                    )
                else:
                    remove_vireo_gps_location(xmp_path)
            elif cleanup_location:
                remove_vireo_gps_location(xmp_path)

            if edit_recipe_json is not None:
                write_edit_recipe(xmp_path, edit_recipe_json)

            if supported_ids:
                synced += 1
                synced_ids.extend(supported_ids)

            for c in unsupported_changes:
                failed += 1
                failures.append(
                    {
                        "photo_id": photo_id,
                        "change_id": c["id"],
                        "error": f"unsupported change type: {c['change_type']}",
                    }
                )

        except Exception as e:
            failed += 1
            failures.append({"photo_id": photo_id, "error": str(e)})
            log.warning("Failed to sync photo %d: %s", photo_id, e)

        if progress_callback:
            progress_callback(i + 1, total)

    # Clear successfully synced changes
    if synced_ids:
        db.clear_pending(synced_ids)

    log.info("Sync complete: %d synced, %d failed", synced, failed)
    return {"synced": synced, "failed": failed, "failures": failures}


def sync_from_xmp(db, photo_ids):
    """Re-read XMP sidecars and update database keywords.

    Args:
        db: Database instance
        photo_ids: list of photo ids to re-sync
    """
    folders = {f["id"]: f["path"] for f in db.get_folder_tree()}

    for photo_id in photo_ids:
        photo = db.get_photo(photo_id)
        if not photo:
            continue

        folder_path = folders.get(photo["folder_id"], "")
        base = os.path.splitext(photo["filename"])[0]
        xmp_path = os.path.join(folder_path, base + ".xmp")

        if not os.path.exists(xmp_path):
            continue

        # Read current XMP keywords. Compare with a normalized match key on
        # both sides so an XMP variant like `‘apapane` matches a DB row
        # stored as `apapane` (add_keyword normalizes on insert). A plain
        # `.lower()` comparison would treat them as different names, making
        # the add-side an INSERT-OR-IGNORE no-op and then prune the DB tag
        # because the raw DB name is not in the raw XMP set -- leaving the
        # photo untagged.
        #
        # Skip XMP entries whose normalized match key is empty (e.g. a
        # lone ASCII or smart quote). add_keyword() now raises ValueError
        # for names that normalize to empty, so keeping such entries would
        # abort the whole sidecar reconcile on a malformed edge-quote
        # keyword instead of ignoring it and processing the rest.
        xmp_keywords = read_keywords(xmp_path)
        xmp_keywords_by_key = {}
        for kw in xmp_keywords:
            key = keyword_match_key(kw)
            if not key:
                continue
            xmp_keywords_by_key.setdefault(key, kw)

        # Get current DB keywords, grouped by (normalized match key,
        # parent_id, type) so we can detect and prune duplicate
        # normalized-equivalent rows that a single XMP entry should map
        # to (e.g. an upgraded photo tagged with both legacy `‘apapane`
        # and clean `apapane` when the sidecar only carries `apapane`).
        #
        # Include parent_id and type in the slot key so different-slot
        # homonyms — e.g. a taxonomy `Robin` and an individual `Robin`,
        # or the same leaf name under different hierarchical parents —
        # do NOT collapse into a single keep-one/untag-the-rest group.
        # The dedup boundary elsewhere in this codebase (add_keyword,
        # update_keyword peer lookup, keyword cleanup) is
        # (name, parent_id, type); a single flat `Robin` in the sidecar
        # cannot disambiguate between the two homonym DB rows, so both
        # legitimate tags must survive here rather than have one
        # arbitrarily untagged.
        db_keywords = db.get_photo_keywords(photo_id)
        db_slot_groups = defaultdict(list)
        for k in db_keywords:
            slot = (keyword_match_key(k["name"]), k["parent_id"], k["type"])
            db_slot_groups[slot].append(k)
        db_key_set = {slot[0] for slot in db_slot_groups}

        # Reconcile DB keyword associations to match the current XMP file.
        for kw_key, kw_name in xmp_keywords_by_key.items():
            if kw_key in db_key_set:
                continue
            kid = db.add_keyword(kw_name)
            db.tag_photo(photo_id, kid)

        for slot, rows in db_slot_groups.items():
            kw_key = slot[0]
            if kw_key not in xmp_keywords_by_key:
                for kw in rows:
                    db.untag_photo(photo_id, kw["id"])
                continue
            if len(rows) <= 1:
                continue
            # Multiple DB rows in the SAME slot (same normalized text,
            # parent_id, and type) collapse to the same XMP entry: keep
            # one and untag the rest. Prefer the row whose stored
            # spelling matches what add_keyword() would produce for the
            # sidecar value (the canonical form), then an exact
            # stored=XMP text match, then the lowest id for a
            # deterministic tie-break.
            xmp_name = xmp_keywords_by_key[kw_key]
            canonical_name = normalize_keyword_display(xmp_name)
            keeper = min(
                rows,
                key=lambda row: (
                    row["name"] != canonical_name,
                    row["name"] != xmp_name,
                    row["id"],
                ),
            )
            for kw in rows:
                if kw["id"] == keeper["id"]:
                    continue
                db.untag_photo(photo_id, kw["id"])

        # Update xmp_mtime
        xmp_mtime = os.path.getmtime(xmp_path)
        db.conn.execute(
            "UPDATE photos SET xmp_mtime = ? WHERE id = ?", (xmp_mtime, photo_id)
        )
        db.conn.commit()

        log.info(
            "Synced XMP -> DB for photo %d: %d keywords", photo_id, len(xmp_keywords)
        )
