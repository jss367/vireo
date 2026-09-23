"""Tests for the one-shot keyword normalization migration.

The migration (`Database._normalize_keyword_data_once`, gated by the
`keyword_names_normalized` db_meta marker) brings a database that predates
write-side normalization onto the invariant the runtime relies on: every
stored keyword/species name is in `normalize_keyword_display()` form. Tests
seed legacy state with raw SQL (the write choke points would otherwise
normalize it away) and then invoke the migration directly; the marker
gating itself is covered by the reopen test at the bottom.
"""

import json
import os
import sqlite3
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from db import AUTO_MATCH_REVIEW_MARKER, Database  # noqa: E402


def _make_db(tmp_path):
    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    fid = db.add_folder("/photos", name="photos")
    p1 = db.add_photo(
        folder_id=fid, filename="a.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    p2 = db.add_photo(
        folder_id=fid, filename="b.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    return db, ws_id, p1, p2


def _insert_keyword(db, name, kw_type="general", parent_id=None, is_species=0):
    cur = db.conn.execute(
        "INSERT INTO keywords (name, parent_id, is_species, type) "
        "VALUES (?, ?, ?, ?)",
        (name, parent_id, is_species, kw_type),
    )
    return cur.lastrowid


def _insert_pending(db, photo_id, change_type, value, ws_id):
    db.conn.execute(
        "INSERT INTO pending_changes "
        "(photo_id, change_type, value, change_token, workspace_id) "
        "VALUES (?, ?, ?, ?, ?)",
        (photo_id, change_type, value, f"tok-{photo_id}-{value}", ws_id),
    )


def test_migration_renames_edge_quote_keyword(tmp_path):
    """A lone `‘apapane` row is renamed in place, and its pending sidecar
    change plus curation rows follow the same old→clean mapping."""
    db, ws_id, p1, _p2 = _make_db(tmp_path)
    try:
        kid = _insert_keyword(db, "‘apapane", "taxonomy", is_species=1)
        db.conn.execute(
            "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
            (p1, kid),
        )
        _insert_pending(db, p1, "keyword_add", "‘apapane", ws_id)
        db.conn.execute(
            "INSERT INTO species_highlights "
            "(workspace_id, species, photo_id, rank) VALUES (?, ?, ?, 1)",
            (ws_id, "‘apapane", p1),
        )
        db.conn.commit()

        db._normalize_keyword_data_once()
        db.conn.commit()

        row = db.conn.execute(
            "SELECT name FROM keywords WHERE id = ?", (kid,)
        ).fetchone()
        assert row["name"] == "apapane"
        pending = db.conn.execute(
            "SELECT value FROM pending_changes WHERE photo_id = ?", (p1,)
        ).fetchall()
        assert [r["value"] for r in pending] == ["apapane"]
        highlights = db.conn.execute(
            "SELECT species FROM species_highlights WHERE photo_id = ?", (p1,)
        ).fetchall()
        assert [r["species"] for r in highlights] == ["apapane"]
    finally:
        db.close()


def test_migration_merges_variant_rows_and_tags(tmp_path):
    """`‘apapane` and `apapane` rows collapse to one row carrying both
    photo tags; the clean spelling survives."""
    db, _ws_id, p1, p2 = _make_db(tmp_path)
    try:
        variant_id = _insert_keyword(db, "‘apapane", "taxonomy", is_species=1)
        clean_id = _insert_keyword(db, "apapane", "taxonomy", is_species=1)
        db.conn.execute(
            "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
            (p1, variant_id),
        )
        db.conn.execute(
            "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
            (p2, clean_id),
        )
        db.conn.commit()

        db._normalize_keyword_data_once()
        db.conn.commit()

        rows = db.conn.execute(
            "SELECT id, name, type, is_species FROM keywords "
            "WHERE name LIKE '%apapane%'"
        ).fetchall()
        assert len(rows) == 1
        assert rows[0]["name"] == "apapane"
        assert rows[0]["type"] == "taxonomy"
        assert rows[0]["is_species"] == 1
        tagged = {
            r["photo_id"] for r in db.conn.execute(
                "SELECT photo_id FROM photo_keywords WHERE keyword_id = ?",
                (rows[0]["id"],),
            )
        }
        assert tagged == {p1, p2}
    finally:
        db.close()


def test_migration_folds_general_variant_into_taxonomy(tmp_path):
    """A 'general' variant folds into a same-key specific-typed peer,
    mirroring add_keyword's general→specific promotion."""
    db, _ws_id, p1, _p2 = _make_db(tmp_path)
    try:
        general_id = _insert_keyword(db, "‘apapane", "general")
        taxonomy_id = _insert_keyword(db, "apapane", "taxonomy", is_species=1)
        db.conn.execute(
            "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
            (p1, general_id),
        )
        db.conn.commit()

        db._normalize_keyword_data_once()
        db.conn.commit()

        rows = db.conn.execute(
            "SELECT id, name, type FROM keywords WHERE name LIKE '%apapane%'"
        ).fetchall()
        assert len(rows) == 1
        assert rows[0]["id"] == taxonomy_id
        assert rows[0]["name"] == "apapane"
        assert rows[0]["type"] == "taxonomy"
        tagged = db.conn.execute(
            "SELECT photo_id FROM photo_keywords WHERE keyword_id = ?",
            (taxonomy_id,),
        ).fetchall()
        assert [r["photo_id"] for r in tagged] == [p1]
    finally:
        db.close()


def test_migration_keeps_distinct_specific_types_separate(tmp_path):
    """Two deliberate non-'general' types sharing a normalized key stay
    separate rows (the dedupe boundary includes type); at the top level
    both end up with the clean spelling since NULL parents don't collide
    on UNIQUE(name, parent_id)."""
    db, _ws_id, _p1, _p2 = _make_db(tmp_path)
    try:
        _insert_keyword(db, "‘Springfield", "location")
        _insert_keyword(db, "Springfield", "individual")
        db.conn.commit()

        db._normalize_keyword_data_once()
        db.conn.commit()

        rows = db.conn.execute(
            "SELECT name, type FROM keywords WHERE name LIKE '%Springfield%' "
            "ORDER BY type"
        ).fetchall()
        assert [(r["name"], r["type"]) for r in rows] == [
            ("Springfield", "individual"),
            ("Springfield", "location"),
        ]
    finally:
        db.close()


def test_migration_keeps_clean_general_homonym_of_specific_type(tmp_path):
    """A clean 'general' row sharing a match_key with a specific-type peer
    is an intentional homonym (e.g. general 'Robin' as a bird-tag hint plus
    individual 'Robin' as a person). The migration must not fold the
    general onto the individual — _merge_keyword_into's cross-type merge
    clears species metadata, so folding a legacy `type='general',
    is_species=1` species row into an individual peer would silently drop
    those photos out of species/life-list filters. Only variant spellings
    should fold across types."""
    db, _ws_id, p1, p2 = _make_db(tmp_path)
    try:
        # Legacy species row stored as type='general', is_species=1 — the
        # exact shape the finding calls out. Clean spelling.
        general_species_id = _insert_keyword(
            db, "Robin", "general", is_species=1
        )
        individual_id = _insert_keyword(db, "Robin", "individual")
        db.conn.execute(
            "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
            (p1, general_species_id),
        )
        db.conn.execute(
            "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
            (p2, individual_id),
        )
        db.conn.commit()

        db._normalize_keyword_data_once()
        db.conn.commit()

        rows = db.conn.execute(
            "SELECT id, name, type, is_species FROM keywords "
            "WHERE name = 'Robin' ORDER BY type"
        ).fetchall()
        # Both rows survive, each keeps its own photo tag and metadata.
        assert [(r["id"], r["type"], r["is_species"]) for r in rows] == [
            (general_species_id, "general", 1),
            (individual_id, "individual", 0),
        ]
        general_tags = {
            r["photo_id"] for r in db.conn.execute(
                "SELECT photo_id FROM photo_keywords WHERE keyword_id = ?",
                (general_species_id,),
            )
        }
        individual_tags = {
            r["photo_id"] for r in db.conn.execute(
                "SELECT photo_id FROM photo_keywords WHERE keyword_id = ?",
                (individual_id,),
            )
        }
        assert general_tags == {p1}
        assert individual_tags == {p2}
    finally:
        db.close()


def test_migration_folds_general_variant_alongside_clean_general_homonym(tmp_path):
    """A variant 'general' merges into its clean-general homonym at the
    same slot (same tag intent, just a spelling variant) — but does NOT
    cross into a non-taxonomy specific-typed peer. Folding the variant
    onto the individual would silently retype the variant's photos across
    the general/individual slot boundary."""
    db, _ws_id, p1, p2 = _make_db(tmp_path)
    try:
        clean_general_id = _insert_keyword(db, "Robin", "general")
        variant_general_id = _insert_keyword(db, "‘Robin", "general")
        individual_id = _insert_keyword(db, "Robin", "individual")
        db.conn.execute(
            "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
            (p1, variant_general_id),
        )
        db.conn.execute(
            "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
            (p2, clean_general_id),
        )
        db.conn.commit()

        db._normalize_keyword_data_once()
        db.conn.commit()

        surviving = db.conn.execute(
            "SELECT id, name, type FROM keywords WHERE name = 'Robin' "
            "ORDER BY type"
        ).fetchall()
        # Variant is gone; clean general and individual coexist.
        assert [(r["id"], r["type"]) for r in surviving] == [
            (clean_general_id, "general"),
            (individual_id, "individual"),
        ]
        # Variant's tag moved onto the clean general (same slot / same tag
        # intent), NOT onto the individual peer.
        individual_tags = {
            r["photo_id"] for r in db.conn.execute(
                "SELECT photo_id FROM photo_keywords WHERE keyword_id = ?",
                (individual_id,),
            )
        }
        clean_general_tags = {
            r["photo_id"] for r in db.conn.execute(
                "SELECT photo_id FROM photo_keywords WHERE keyword_id = ?",
                (clean_general_id,),
            )
        }
        assert individual_tags == set()
        assert clean_general_tags == {p1, p2}
    finally:
        db.close()


def test_migration_preserves_variant_species_general_alongside_individual(tmp_path):
    """A variant 'general' row that carries the legacy species flag
    (``type='general', is_species=1``) must NOT be folded into a
    non-taxonomy specific-type peer: _merge_keyword_into's
    ``leaks_species_into_nontaxonomy`` branch would clear the species
    flag on the destination, so every photo previously tagged with the
    legacy species row would silently drop out of species/life-list
    filters. At the top level, SQLite treats NULL parents as distinct
    for UNIQUE(name, parent_id), so the disambiguating rename can bring
    the variant onto its clean spelling without colliding with the
    individual peer."""
    db, _ws_id, p1, p2 = _make_db(tmp_path)
    try:
        variant_species_id = _insert_keyword(
            db, "‘Robin", "general", is_species=1
        )
        individual_id = _insert_keyword(db, "Robin", "individual")
        db.conn.execute(
            "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
            (p1, variant_species_id),
        )
        db.conn.execute(
            "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
            (p2, individual_id),
        )
        db.conn.commit()

        db._normalize_keyword_data_once()
        db.conn.commit()

        rows = db.conn.execute(
            "SELECT id, name, type, is_species FROM keywords "
            "WHERE name = 'Robin' ORDER BY type"
        ).fetchall()
        # Both rows survive with the clean spelling; each keeps its
        # metadata and its own photo tag. Critically, the species flag
        # on the legacy general row is preserved so species/life-list
        # queries still surface p1's photo.
        assert [(r["id"], r["type"], r["is_species"]) for r in rows] == [
            (variant_species_id, "general", 1),
            (individual_id, "individual", 0),
        ]
        variant_tags = {
            r["photo_id"] for r in db.conn.execute(
                "SELECT photo_id FROM photo_keywords WHERE keyword_id = ?",
                (variant_species_id,),
            )
        }
        individual_tags = {
            r["photo_id"] for r in db.conn.execute(
                "SELECT photo_id FROM photo_keywords WHERE keyword_id = ?",
                (individual_id,),
            )
        }
        assert variant_tags == {p1}
        assert individual_tags == {p2}
    finally:
        db.close()


def test_migration_keeps_species_general_separate_from_plain_general_homonym(tmp_path):
    """A legacy species-bearing general (``type='general', is_species=1``)
    and a plain general homonym (``type='general', is_species=0``) with
    no specific-type peer must stay separate: species queries
    ``is_species = 1 OR type = 'taxonomy'`` distinguish them, so folding
    them into one general survivor would either strip the species flag
    from the legacy row's photos or (via _merge_keyword_into's same-type
    is_species CASE) stamp is_species=1 onto the plain general and every
    photo already tagged with it, sending them into species/life-list
    filters. Top-level NULL parents let both survive under the same
    match key (SQLite treats NULL parents as distinct for
    UNIQUE(name, parent_id))."""
    db, _ws_id, p1, p2 = _make_db(tmp_path)
    try:
        # Plain general inserted first so it wins the earliest-id tiebreak
        # in the merge loop — this is the direction that leaks is_species=1
        # onto the non-species survivor without the split.
        plain_general_id = _insert_keyword(db, "Robin", "general")
        species_general_id = _insert_keyword(
            db, "robin", "general", is_species=1
        )
        db.conn.execute(
            "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
            (p1, plain_general_id),
        )
        db.conn.execute(
            "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
            (p2, species_general_id),
        )
        db.conn.commit()

        db._normalize_keyword_data_once()
        db.conn.commit()

        rows = db.conn.execute(
            "SELECT id, name, type, is_species FROM keywords "
            "WHERE LOWER(name) = 'robin' ORDER BY id"
        ).fetchall()
        # Both rows survive; each keeps its own is_species value and its
        # own photo tag. The plain general must NOT have gained is_species=1.
        assert [(r["id"], r["type"], r["is_species"]) for r in rows] == [
            (plain_general_id, "general", 0),
            (species_general_id, "general", 1),
        ]
        plain_tags = {
            r["photo_id"] for r in db.conn.execute(
                "SELECT photo_id FROM photo_keywords WHERE keyword_id = ?",
                (plain_general_id,),
            )
        }
        species_tags = {
            r["photo_id"] for r in db.conn.execute(
                "SELECT photo_id FROM photo_keywords WHERE keyword_id = ?",
                (species_general_id,),
            )
        }
        assert plain_tags == {p1}
        assert species_tags == {p2}
    finally:
        db.close()


def test_migration_folds_variant_species_general_into_taxonomy_peer(tmp_path):
    """A variant species-bearing general still folds into a same-slot
    taxonomy peer — that's a species-to-species merge, the species flag
    survives, and the fold resolves the imminent name collision."""
    db, _ws_id, p1, p2 = _make_db(tmp_path)
    try:
        variant_species_id = _insert_keyword(
            db, "‘Robin", "general", is_species=1
        )
        taxonomy_id = _insert_keyword(db, "Robin", "taxonomy", is_species=1)
        db.conn.execute(
            "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
            (p1, variant_species_id),
        )
        db.conn.execute(
            "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
            (p2, taxonomy_id),
        )
        db.conn.commit()

        db._normalize_keyword_data_once()
        db.conn.commit()

        rows = db.conn.execute(
            "SELECT id, name, type, is_species FROM keywords "
            "WHERE name = 'Robin'"
        ).fetchall()
        assert len(rows) == 1
        assert rows[0]["id"] == taxonomy_id
        assert rows[0]["type"] == "taxonomy"
        assert rows[0]["is_species"] == 1
        tagged = {
            r["photo_id"] for r in db.conn.execute(
                "SELECT photo_id FROM photo_keywords WHERE keyword_id = ?",
                (taxonomy_id,),
            )
        }
        assert tagged == {p1, p2}
    finally:
        db.close()


def test_migration_cross_type_child_collision_disambiguates_variant(tmp_path):
    """Under a non-NULL parent, UNIQUE(name, parent_id) blocks renaming a
    variant onto a clean name a different-type sibling already holds. The
    migration disambiguates the variant with an id suffix so no stored
    variant survives (the marker below can honestly advertise the
    invariant) while the different-type peer keeps its clean slot."""
    db, _ws_id, _p1, _p2 = _make_db(tmp_path)
    try:
        parent_id = _insert_keyword(db, "Birds", "general")
        peer_id = _insert_keyword(db, "Hawk", "location", parent_id=parent_id)
        variant_id = _insert_keyword(
            db, "‘Hawk", "taxonomy", parent_id=parent_id, is_species=1
        )
        db.conn.commit()

        db._normalize_keyword_data_once()
        db.conn.commit()

        row = db.conn.execute(
            "SELECT name FROM keywords WHERE id = ?", (variant_id,)
        ).fetchone()
        assert row["name"] == f"Hawk (id-{variant_id})"
        # The different-type peer is untouched.
        peer = db.conn.execute(
            "SELECT name FROM keywords WHERE id = ?", (peer_id,)
        ).fetchone()
        assert peer["name"] == "Hawk"
    finally:
        db.close()


def test_migration_drops_empty_named_keywords(tmp_path):
    """A keyword whose name normalizes to empty (pure stray punctuation)
    is deleted; its children reparent upward and its tags are dropped."""
    db, _ws_id, p1, _p2 = _make_db(tmp_path)
    try:
        junk_id = _insert_keyword(db, "’", "general")
        child_id = _insert_keyword(db, "Hawk", "general", parent_id=junk_id)
        db.conn.execute(
            "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
            (p1, junk_id),
        )
        db.conn.commit()

        db._normalize_keyword_data_once()
        db.conn.commit()

        assert db.conn.execute(
            "SELECT id FROM keywords WHERE id = ?", (junk_id,)
        ).fetchone() is None
        child = db.conn.execute(
            "SELECT parent_id FROM keywords WHERE id = ?", (child_id,)
        ).fetchone()
        assert child["parent_id"] is None
        assert db.conn.execute(
            "SELECT 1 FROM photo_keywords WHERE keyword_id = ?", (junk_id,)
        ).fetchone() is None
    finally:
        db.close()


def test_migration_converges_variant_parent_chains(tmp_path):
    """`‘Birds > ‘Hawk` and `Birds > Hawk` collapse to one clean chain —
    the children only become same-slot duplicates after their parents
    merge, which is what the convergence loop is for."""
    db, _ws_id, p1, p2 = _make_db(tmp_path)
    try:
        vb = _insert_keyword(db, "‘Birds", "general")
        vh = _insert_keyword(db, "‘Hawk", "general", parent_id=vb)
        cb = _insert_keyword(db, "Birds", "general")
        ch = _insert_keyword(db, "Hawk", "general", parent_id=cb)
        db.conn.execute(
            "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
            (p1, vh),
        )
        db.conn.execute(
            "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
            (p2, ch),
        )
        db.conn.commit()

        db._normalize_keyword_data_once()
        db.conn.commit()

        rows = db.conn.execute(
            "SELECT id, name, parent_id FROM keywords "
            "WHERE name LIKE '%Birds%' OR name LIKE '%Hawk%' ORDER BY name"
        ).fetchall()
        assert [r["name"] for r in rows] == ["Birds", "Hawk"]
        birds = rows[0]
        hawk = rows[1]
        assert hawk["parent_id"] == birds["id"]
        tagged = {
            r["photo_id"] for r in db.conn.execute(
                "SELECT photo_id FROM photo_keywords WHERE keyword_id = ?",
                (hawk["id"],),
            )
        }
        assert tagged == {p1, p2}
    finally:
        db.close()


def test_migration_normalizes_orphan_pending_changes(tmp_path):
    """Pending sidecar values are normalized even when no keyword row
    references them; quote-only values are deleted; rows that collide
    after normalization are deduped."""
    db, ws_id, p1, _p2 = _make_db(tmp_path)
    try:
        _insert_pending(db, p1, "keyword_add", "‘orphan", ws_id)
        _insert_pending(db, p1, "keyword_add", "orphan", ws_id)
        _insert_pending(db, p1, "keyword_remove", "’", ws_id)
        db.conn.commit()

        db._normalize_keyword_data_once()
        db.conn.commit()

        rows = db.conn.execute(
            "SELECT change_type, value FROM pending_changes WHERE photo_id = ?",
            (p1,),
        ).fetchall()
        assert [(r["change_type"], r["value"]) for r in rows] == [
            ("keyword_add", "orphan"),
        ]
    finally:
        db.close()


def test_migration_cancels_opposite_pending_pair_after_normalization(tmp_path):
    """Opposite-type pending changes that share the same normalized value
    must cancel each other during the migration — same as the
    queue_keyword_add / queue_keyword_remove cancel semantics at
    runtime. Without this, a stray-quote keyword_add(`‘Apapane`) plus a
    clean keyword_remove(`Apapane`) queued before the upgrade would both
    survive as add+remove(Apapane) after normalization, and sync_to_xmp
    reads a same-value add/remove pair as a paired rename and rewrites
    the removed spelling back into the sidecar.
    """
    db, ws_id, p1, _p2 = _make_db(tmp_path)
    try:
        _insert_pending(db, p1, "keyword_add", "‘Apapane", ws_id)
        _insert_pending(db, p1, "keyword_remove", "Apapane", ws_id)
        db.conn.commit()

        db._normalize_keyword_data_once()
        db.conn.commit()

        rows = db.conn.execute(
            "SELECT change_type, value FROM pending_changes WHERE photo_id = ?",
            (p1,),
        ).fetchall()
        assert rows == []
    finally:
        db.close()


def test_migration_normalizes_curation_tables(tmp_path):
    """Curation rows keyed by variant species strings move to the clean
    spelling; a variant row colliding with an existing clean row is
    dropped rather than duplicated."""
    db, ws_id, p1, p2 = _make_db(tmp_path)
    try:
        db.conn.execute(
            "INSERT INTO photo_preferences "
            "(workspace_id, purpose, species, photo_id) VALUES (?, ?, ?, ?)",
            (ws_id, "life_list", "‘apapane", p1),
        )
        db.conn.execute(
            "INSERT INTO species_representatives "
            "(species, photo_id, selected_order) VALUES (?, ?, 1)",
            ("‘apapane", p1),
        )
        # Highlight rows under both spellings for the same photo: the
        # variant one must be dropped, not duplicated.
        db.conn.execute(
            "INSERT INTO species_highlights "
            "(workspace_id, species, photo_id, rank) VALUES (?, ?, ?, 1)",
            (ws_id, "apapane", p1),
        )
        db.conn.execute(
            "INSERT INTO species_highlights "
            "(workspace_id, species, photo_id, rank) VALUES (?, ?, ?, 1)",
            (ws_id, "‘apapane", p1),
        )
        db.conn.execute(
            "INSERT INTO species_highlights "
            "(workspace_id, species, photo_id, rank) VALUES (?, ?, ?, 2)",
            (ws_id, "‘apapane", p2),
        )
        db.conn.commit()

        db._normalize_keyword_data_once()
        db.conn.commit()

        prefs = db.conn.execute(
            "SELECT species FROM photo_preferences"
        ).fetchall()
        assert [r["species"] for r in prefs] == ["apapane"]
        reps = db.conn.execute(
            "SELECT species FROM species_representatives"
        ).fetchall()
        assert [r["species"] for r in reps] == ["apapane"]
        highlights = db.conn.execute(
            "SELECT species, photo_id FROM species_highlights ORDER BY rank"
        ).fetchall()
        assert {(r["species"], r["photo_id"]) for r in highlights} == {
            ("apapane", p1), ("apapane", p2),
        }
    finally:
        db.close()


def test_migration_normalizes_relabel_curation_history_payloads(tmp_path):
    """The live curation sweep and undo snapshots must use the same species
    spelling. Otherwise undoing a pre-migration relabel recreates curation
    rows under the legacy quoted name after the keyword row was normalized.
    """
    db, ws_id, p1, _p2 = _make_db(tmp_path)
    try:
        kid = _insert_keyword(db, "‘Apapane", "taxonomy", is_species=1)
        db.conn.execute(
            "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
            (p1, kid),
        )
        payload = {
            "keyword_id": kid,
            "keyword_ids": [kid],
            "curation": {
                "hl_prev": [
                    "‘Apapane",
                    {"species": "‘Apapane", "rank": 2},
                ],
                "pref_prev": [
                    {"species": "‘Apapane", "purpose": "life_list"},
                ],
                "rep_prev": [
                    {"species": "‘Apapane", "selected_order": 3},
                ],
            },
        }
        db.conn.execute(
            "INSERT INTO edit_history "
            "(action_type, description, new_value, workspace_id) "
            "VALUES ('species_replace', 'legacy relabel', ?, ?)",
            (str(kid), ws_id),
        )
        edit_id = db.conn.execute(
            "SELECT id FROM edit_history WHERE description = 'legacy relabel'"
        ).fetchone()["id"]
        db.conn.execute(
            "INSERT INTO edit_history_items "
            "(edit_id, photo_id, old_value, new_value) VALUES (?, ?, ?, ?)",
            (edit_id, p1, json.dumps(payload, sort_keys=True), str(kid)),
        )
        db.conn.commit()

        db._normalize_keyword_data_once()
        db.conn.commit()

        stored = db.conn.execute(
            "SELECT old_value FROM edit_history_items WHERE edit_id = ?",
            (edit_id,),
        ).fetchone()
        migrated = json.loads(stored["old_value"])["curation"]
        assert migrated["hl_prev"][0] == "Apapane"
        assert migrated["hl_prev"][1]["species"] == "Apapane"
        assert migrated["pref_prev"][0]["species"] == "Apapane"
        assert migrated["rep_prev"][0]["species"] == "Apapane"
    finally:
        db.close()


def test_migration_aligns_curation_case_with_stored_keyword(tmp_path):
    """Curation rows differing from the species keyword only by case are
    re-keyed to the stored spelling. normalize_keyword_display() preserves
    case, so the punctuation sweep alone leaves `Saffron Finch` curation
    orphaned from a `Saffron finch` keyword row — and the eligible
    highlight/life-list queries compare those strings exact."""
    db, ws_id, p1, _p2 = _make_db(tmp_path)
    try:
        kid = _insert_keyword(db, "Saffron finch", "taxonomy", is_species=1)
        db.conn.execute(
            "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
            (p1, kid),
        )
        db.conn.execute(
            "INSERT INTO species_highlights "
            "(workspace_id, species, photo_id, rank) VALUES (?, ?, ?, 1)",
            (ws_id, "Saffron Finch", p1),
        )
        db.conn.execute(
            "INSERT INTO photo_preferences "
            "(workspace_id, purpose, species, photo_id) VALUES (?, ?, ?, ?)",
            (ws_id, "life_list", "Saffron Finch", p1),
        )
        db.conn.commit()

        db._normalize_keyword_data_once()
        db.conn.commit()

        hl = db.conn.execute(
            "SELECT species FROM species_highlights WHERE photo_id = ?", (p1,)
        ).fetchall()
        assert [r["species"] for r in hl] == ["Saffron finch"]
        prefs = db.conn.execute(
            "SELECT species FROM photo_preferences WHERE photo_id = ?", (p1,)
        ).fetchall()
        assert [r["species"] for r in prefs] == ["Saffron finch"]
    finally:
        db.close()


def test_migration_leaves_curation_alone_for_ambiguous_species_homonyms(
    tmp_path,
):
    """When the DB keeps multiple species-bearing keywords under the same
    match_key (for example a taxonomy `robin` and a legacy
    `type='general', is_species=1` `Robin`), the second curation pass must
    NOT remap curation rows for one spelling onto the other. Doing so
    would silently drop the highlight/preference for the untouched
    keyword because the eligible queries join sh.species = k.name exact
    and the photo is only tagged with the other keyword.
    """
    db, ws_id, p1, p2 = _make_db(tmp_path)
    try:
        tax_kid = _insert_keyword(db, "robin", "taxonomy", is_species=1)
        gen_kid = _insert_keyword(db, "Robin", "general", is_species=1)
        db.conn.execute(
            "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
            (p1, tax_kid),
        )
        db.conn.execute(
            "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
            (p2, gen_kid),
        )
        db.conn.execute(
            "INSERT INTO species_highlights "
            "(workspace_id, species, photo_id, rank) VALUES (?, ?, ?, 1)",
            (ws_id, "robin", p1),
        )
        db.conn.execute(
            "INSERT INTO species_highlights "
            "(workspace_id, species, photo_id, rank) VALUES (?, ?, ?, 2)",
            (ws_id, "Robin", p2),
        )
        db.conn.commit()

        db._normalize_keyword_data_once()
        db.conn.commit()

        rows = db.conn.execute(
            "SELECT species, photo_id FROM species_highlights ORDER BY rank"
        ).fetchall()
        assert [(r["species"], r["photo_id"]) for r in rows] == [
            ("robin", p1),
            ("Robin", p2),
        ]
    finally:
        db.close()


def test_migration_merges_case_variant_keyword_rows(tmp_path):
    """`Snowy Egret` and `Snowy egret` rows merge into one, and curation
    keyed under the merged-away spelling follows to the survivor."""
    db, ws_id, p1, p2 = _make_db(tmp_path)
    try:
        first = _insert_keyword(db, "Snowy egret", "taxonomy", is_species=1)
        second = _insert_keyword(db, "Snowy Egret", "taxonomy", is_species=1)
        db.conn.execute(
            "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
            (p1, first),
        )
        db.conn.execute(
            "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
            (p2, second),
        )
        db.conn.execute(
            "INSERT INTO species_highlights "
            "(workspace_id, species, photo_id, rank) VALUES (?, ?, ?, 1)",
            (ws_id, "Snowy Egret", p2),
        )
        db.conn.commit()

        db._normalize_keyword_data_once()
        db.conn.commit()

        rows = db.conn.execute(
            "SELECT id, name FROM keywords WHERE name LIKE '%egret%' "
            "OR name LIKE '%Egret%'"
        ).fetchall()
        assert len(rows) == 1
        survivor = rows[0]["name"]
        tagged = {
            r["photo_id"] for r in db.conn.execute(
                "SELECT photo_id FROM photo_keywords WHERE keyword_id = ?",
                (rows[0]["id"],),
            )
        }
        assert tagged == {p1, p2}
        hl = db.conn.execute(
            "SELECT species FROM species_highlights WHERE photo_id = ?", (p2,)
        ).fetchall()
        assert [r["species"] for r in hl] == [survivor]
    finally:
        db.close()


def test_migration_preserves_okina_names(tmp_path):
    """A legitimate leading okina (U+02BB) is not a stray quote; the
    migration must leave such names untouched."""
    db, _ws_id, _p1, _p2 = _make_db(tmp_path)
    try:
        kid = _insert_keyword(db, "ʻApapane", "taxonomy", is_species=1)
        db.conn.commit()

        db._normalize_keyword_data_once()
        db.conn.commit()

        row = db.conn.execute(
            "SELECT name FROM keywords WHERE id = ?", (kid,)
        ).fetchone()
        assert row["name"] == "ʻApapane"
    finally:
        db.close()


def test_migration_merges_typographic_apostrophe_keyword_rows(tmp_path):
    """`Say’s phoebe` (U+2019) and `Say's phoebe` (U+0027) are one bird.

    Both spellings could be stored as separate rows because SQLite's
    COLLATE NOCASE cannot fold U+2019, which produced two Life List cards
    with two lifer numbers for the same species. Tags from the variant must
    migrate onto the ASCII survivor.
    """
    db, ws_id, p1, p2 = _make_db(tmp_path)
    try:
        ascii_id = _insert_keyword(
            db, "Say's phoebe", "taxonomy", is_species=1
        )
        curly_id = _insert_keyword(
            db, "Say’s phoebe", "taxonomy", is_species=1
        )
        db.conn.execute(
            "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
            (p1, ascii_id),
        )
        db.conn.execute(
            "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
            (p2, curly_id),
        )
        db.conn.commit()

        db._normalize_keyword_data_once()
        db.conn.commit()

        names = [
            r["name"] for r in db.conn.execute(
                "SELECT name FROM keywords WHERE name LIKE 'Say%phoebe'"
            ).fetchall()
        ]
        assert names == ["Say's phoebe"], names
        survivor = db.conn.execute(
            "SELECT id FROM keywords WHERE name = ?", ("Say's phoebe",)
        ).fetchone()["id"]
        tagged = {
            r["photo_id"] for r in db.conn.execute(
                "SELECT photo_id FROM photo_keywords WHERE keyword_id = ?",
                (survivor,),
            ).fetchall()
        }
        assert tagged == {p1, p2}
    finally:
        db.close()


def test_migration_preserves_internal_okina_against_apostrophe_fold(tmp_path):
    """U+02BB inside a name (``Hawaiʻi ʻamakihi``) is a letter, not a
    quote, and the apostrophe fold must not reach it. The edge-quote strip
    never touched internal characters, so this is only at risk now that the
    fold applies mid-string."""
    db, _ws_id, _p1, _p2 = _make_db(tmp_path)
    try:
        kid = _insert_keyword(db, "Hawaiʻi ʻamakihi", "taxonomy", is_species=1)
        db.conn.commit()

        db._normalize_keyword_data_once()
        db.conn.commit()

        row = db.conn.execute(
            "SELECT name FROM keywords WHERE id = ?", (kid,)
        ).fetchone()
        assert row["name"] == "Hawaiʻi ʻamakihi"
    finally:
        db.close()


def _insert_prediction(db, photo_id, species, confidence=0.9, model="m1"):
    det = db.conn.execute(
        "INSERT INTO detections (photo_id, category, detector_confidence) "
        "VALUES (?, 'animal', 0.99)",
        (photo_id,),
    ).lastrowid
    db.conn.execute(
        "INSERT INTO predictions (detection_id, classifier_model, "
        "labels_fingerprint, species, confidence) VALUES (?, ?, 'fp1', ?, ?)",
        (det, model, species, confidence),
    )
    return det


def test_migration_folds_prediction_species_apostrophes(tmp_path):
    """predictions.species is joined to keywords.name with COLLATE NOCASE,
    which cannot fold U+2019 — so a `Swinhoe’s White-eye` prediction never
    matched the accepted `Swinhoe's white-eye` keyword and the photo's own
    prediction rendered as unaccepted."""
    db, _ws_id, p1, _p2 = _make_db(tmp_path)
    try:
        _insert_prediction(db, p1, "Swinhoe’s White-eye")
        db.conn.commit()

        assert db._fold_prediction_species_apostrophes() == 1
        db.conn.commit()

        species = [
            r["species"] for r in db.conn.execute(
                "SELECT species FROM predictions"
            ).fetchall()
        ]
        assert species == ["Swinhoe's White-eye"]
    finally:
        db.close()


def test_prediction_fold_resolves_unique_collision_by_confidence(tmp_path):
    """predictions has UNIQUE(detection_id, classifier_model,
    labels_fingerprint, species). When both spellings exist on one
    detection, folding would violate it — keep the higher-confidence row
    rather than aborting the whole migration."""
    db, _ws_id, p1, _p2 = _make_db(tmp_path)
    try:
        det = _insert_prediction(db, p1, "Say's phoebe", confidence=0.4)
        db.conn.execute(
            "INSERT INTO predictions (detection_id, classifier_model, "
            "labels_fingerprint, species, confidence) "
            "VALUES (?, 'm1', 'fp1', ?, ?)",
            (det, "Say’s phoebe", 0.8),
        )
        db.conn.commit()

        db._fold_prediction_species_apostrophes()
        db.conn.commit()

        rows = db.conn.execute(
            "SELECT species, confidence FROM predictions"
        ).fetchall()
        assert len(rows) == 1
        assert rows[0]["species"] == "Say's phoebe"
        assert rows[0]["confidence"] == 0.8
    finally:
        db.close()


def test_prediction_fold_collision_matches_case_insensitively(tmp_path):
    """Downstream keyword joins use ``COLLATE NOCASE``, so a DB carrying
    both `Say's Phoebe` (ASCII, title-case) and `Say’s phoebe` (curly,
    lowercase) already renders as one bird. The fold must merge them
    the same way; a case-sensitive collision lookup would rename only
    the curly row and leave two ASCII case-variants that survive as
    duplicate predictions on the same (detection, model, fingerprint).
    """
    db, _ws_id, p1, _p2 = _make_db(tmp_path)
    try:
        det = _insert_prediction(db, p1, "Say's Phoebe", confidence=0.9)
        db.conn.execute(
            "INSERT INTO predictions (detection_id, classifier_model, "
            "labels_fingerprint, species, confidence) "
            "VALUES (?, 'm1', 'fp1', ?, ?)",
            (det, "Say’s phoebe", 0.3),
        )
        db.conn.commit()

        db._fold_prediction_species_apostrophes()
        db.conn.commit()

        rows = db.conn.execute(
            "SELECT species, confidence FROM predictions"
        ).fetchall()
        assert len(rows) == 1
        # Higher-confidence winner keeps its casing; loser's curly variant
        # is deleted rather than left as a folded duplicate.
        assert rows[0]["species"] == "Say's Phoebe"
        assert rows[0]["confidence"] == 0.9
    finally:
        db.close()


def test_prediction_fold_merges_three_way_case_and_apostrophe_collision(tmp_path):
    """A single (detection, model, fingerprint) scope can legally hold three
    NOCASE-equivalent variants at once (``predictions.species`` is BINARY
    UNIQUE): ``Say's Phoebe`` (ASCII title-case), ``Say's phoebe`` (ASCII
    lowercase), ``Say's phoebe`` (curly, lowercase). A per-row ``fetchone``
    peer lookup would only merge one of the two ASCII neighbours, and if
    the curly row wins by confidence the subsequent
    ``UPDATE ... SET species = 'Say's phoebe'`` collides with the unmerged
    ASCII-lowercase row and aborts the migration under UNIQUE — reopening
    the DB just fails again. All three must merge in one pass.
    """
    db, _ws_id, p1, _p2 = _make_db(tmp_path)
    try:
        det = _insert_prediction(db, p1, "Say's Phoebe", confidence=0.5)
        db.conn.execute(
            "INSERT INTO predictions (detection_id, classifier_model, "
            "labels_fingerprint, species, confidence) "
            "VALUES (?, 'm1', 'fp1', ?, ?)",
            (det, "Say's phoebe", 0.3),
        )
        db.conn.execute(
            "INSERT INTO predictions (detection_id, classifier_model, "
            "labels_fingerprint, species, confidence) "
            "VALUES (?, 'm1', 'fp1', ?, ?)",
            (det, "Say’s phoebe", 0.9),
        )
        db.conn.commit()

        # No exception: the whole collision set is merged in one pass.
        db._fold_prediction_species_apostrophes()
        db.conn.commit()

        rows = db.conn.execute(
            "SELECT species, confidence FROM predictions"
        ).fetchall()
        assert len(rows) == 1
        # Curly row (0.9) is the highest-confidence winner; it gets folded
        # to the clean spelling, and neither ASCII peer remains to collide
        # with the UPDATE.
        assert rows[0]["species"] == "Say's phoebe"
        assert rows[0]["confidence"] == 0.9
    finally:
        db.close()


def test_prediction_fold_preserves_review_when_loser_carries_decision(tmp_path):
    """When the collision-merge deletes the losing prediction, its per-
    workspace prediction_review row must migrate onto the surviving row.
    `prediction_review.prediction_id` uses `ON DELETE CASCADE`, so a bare
    delete would silently drop the user's accepted/rejected decision (and
    its group metadata) during the one-shot migration.

    Reproduces the P1 finding on the direction where the curly variant is
    the *losing* row: its review sits on the id that gets deleted.
    """
    db, ws_id, p1, _p2 = _make_db(tmp_path)
    try:
        det = _insert_prediction(db, p1, "Say's phoebe", confidence=0.8)
        # Curly variant has lower confidence and is therefore the loser.
        db.conn.execute(
            "INSERT INTO predictions (detection_id, classifier_model, "
            "labels_fingerprint, species, confidence) "
            "VALUES (?, 'm1', 'fp1', ?, ?)",
            (det, "Say’s phoebe", 0.4),
        )
        loser_pid = db.conn.execute(
            "SELECT id FROM predictions WHERE species = ?", ("Say’s phoebe",)
        ).fetchone()["id"]
        db.conn.execute(
            "INSERT INTO prediction_review "
            "(prediction_id, workspace_id, status, reviewed_at, "
            " individual, group_id, vote_count, total_votes) "
            "VALUES (?, ?, 'accepted', '2025-01-02 03:04:05', "
            "        'reviewer-note', 'g-1', 3, 5)",
            (loser_pid, ws_id),
        )
        db.conn.commit()

        db._fold_prediction_species_apostrophes()
        db.conn.commit()

        rows = db.conn.execute(
            "SELECT species FROM predictions"
        ).fetchall()
        assert [r["species"] for r in rows] == ["Say's phoebe"]
        survivor_pid = db.conn.execute(
            "SELECT id FROM predictions WHERE species = ?", ("Say's phoebe",)
        ).fetchone()["id"]
        review = db.conn.execute(
            "SELECT status, individual, group_id, vote_count, total_votes "
            "FROM prediction_review "
            "WHERE prediction_id = ? AND workspace_id = ?",
            (survivor_pid, ws_id),
        ).fetchone()
        assert review is not None, "loser's accepted review was dropped"
        assert review["status"] == "accepted"
        assert review["individual"] == "reviewer-note"
        assert review["group_id"] == "g-1"
        assert review["vote_count"] == 3
        assert review["total_votes"] == 5
    finally:
        db.close()


def test_prediction_fold_preserves_review_when_variant_wins_collision(tmp_path):
    """Symmetric to the loser-side case: when the variant has *higher*
    confidence, the migration deletes the clean row and renames the variant
    onto the clean spelling. A review row attached to the deleted clean row
    would be lost without an explicit merge step."""
    db, ws_id, p1, _p2 = _make_db(tmp_path)
    try:
        det = _insert_prediction(db, p1, "Say's phoebe", confidence=0.4)
        db.conn.execute(
            "INSERT INTO predictions (detection_id, classifier_model, "
            "labels_fingerprint, species, confidence) "
            "VALUES (?, 'm1', 'fp1', ?, ?)",
            (det, "Say’s phoebe", 0.8),
        )
        clean_pid = db.conn.execute(
            "SELECT id FROM predictions WHERE species = ?", ("Say's phoebe",)
        ).fetchone()["id"]
        db.conn.execute(
            "INSERT INTO prediction_review "
            "(prediction_id, workspace_id, status, reviewed_at) "
            "VALUES (?, ?, 'rejected', '2025-01-02 03:04:05')",
            (clean_pid, ws_id),
        )
        db.conn.commit()

        db._fold_prediction_species_apostrophes()
        db.conn.commit()

        rows = db.conn.execute(
            "SELECT species FROM predictions"
        ).fetchall()
        assert [r["species"] for r in rows] == ["Say's phoebe"]
        survivor_pid = db.conn.execute(
            "SELECT id FROM predictions WHERE species = ?", ("Say's phoebe",)
        ).fetchone()["id"]
        review = db.conn.execute(
            "SELECT status FROM prediction_review "
            "WHERE prediction_id = ? AND workspace_id = ?",
            (survivor_pid, ws_id),
        ).fetchone()
        assert review is not None
        assert review["status"] == "rejected"
    finally:
        db.close()


def test_prediction_fold_prefers_stronger_decision_on_review_collision(
    tmp_path,
):
    """Both spellings can independently have review rows in the same
    workspace (e.g. one was reviewed under the curly spelling, then the
    clean one appeared and was left pending). The merge must keep the
    non-pending user decision instead of the pending stub."""
    db, ws_id, p1, _p2 = _make_db(tmp_path)
    try:
        det = _insert_prediction(db, p1, "Say's phoebe", confidence=0.8)
        db.conn.execute(
            "INSERT INTO predictions (detection_id, classifier_model, "
            "labels_fingerprint, species, confidence) "
            "VALUES (?, 'm1', 'fp1', ?, ?)",
            (det, "Say’s phoebe", 0.4),
        )
        winner_pid = db.conn.execute(
            "SELECT id FROM predictions WHERE species = ?", ("Say's phoebe",)
        ).fetchone()["id"]
        loser_pid = db.conn.execute(
            "SELECT id FROM predictions WHERE species = ?", ("Say’s phoebe",)
        ).fetchone()["id"]
        # Winner has a pending stub row; loser carries the real decision.
        db.conn.execute(
            "INSERT INTO prediction_review "
            "(prediction_id, workspace_id, status) VALUES (?, ?, 'pending')",
            (winner_pid, ws_id),
        )
        db.conn.execute(
            "INSERT INTO prediction_review "
            "(prediction_id, workspace_id, status, reviewed_at, individual) "
            "VALUES (?, ?, 'accepted', '2025-01-02 03:04:05', 'kept-me')",
            (loser_pid, ws_id),
        )
        db.conn.commit()

        db._fold_prediction_species_apostrophes()
        db.conn.commit()

        survivor_pid = db.conn.execute(
            "SELECT id FROM predictions WHERE species = ?", ("Say's phoebe",)
        ).fetchone()["id"]
        review = db.conn.execute(
            "SELECT status, individual FROM prediction_review "
            "WHERE prediction_id = ? AND workspace_id = ?",
            (survivor_pid, ws_id),
        ).fetchone()
        assert review is not None
        assert review["status"] == "accepted"
        assert review["individual"] == "kept-me"


        reviews = db.conn.execute(
            "SELECT COUNT(*) AS c FROM prediction_review"
        ).fetchone()["c"]
        assert reviews == 1
    finally:
        db.close()


def test_prediction_fold_preserves_review_per_workspace_independently(tmp_path):
    """A collision can touch multiple workspaces at once: each workspace's
    review row on the loser must migrate to the survivor independently and
    unaffected by other workspaces' state.
    """
    db, ws_a, p1, _p2 = _make_db(tmp_path)
    try:
        ws_b = db.create_workspace("Beta")
        det = _insert_prediction(db, p1, "Say's phoebe", confidence=0.8)
        db.conn.execute(
            "INSERT INTO predictions (detection_id, classifier_model, "
            "labels_fingerprint, species, confidence) "
            "VALUES (?, 'm1', 'fp1', ?, ?)",
            (det, "Say’s phoebe", 0.4),
        )
        loser_pid = db.conn.execute(
            "SELECT id FROM predictions WHERE species = ?", ("Say’s phoebe",)
        ).fetchone()["id"]
        db.conn.execute(
            "INSERT INTO prediction_review "
            "(prediction_id, workspace_id, status, reviewed_at) "
            "VALUES (?, ?, 'accepted', '2025-01-02 03:04:05')",
            (loser_pid, ws_a),
        )
        db.conn.execute(
            "INSERT INTO prediction_review "
            "(prediction_id, workspace_id, status, reviewed_at) "
            "VALUES (?, ?, 'rejected', '2025-01-03 03:04:05')",
            (loser_pid, ws_b),
        )
        db.conn.commit()

        db._fold_prediction_species_apostrophes()
        db.conn.commit()

        survivor_pid = db.conn.execute(
            "SELECT id FROM predictions WHERE species = ?", ("Say's phoebe",)
        ).fetchone()["id"]
        rows = {
            r["workspace_id"]: r["status"]
            for r in db.conn.execute(
                "SELECT workspace_id, status FROM prediction_review "
                "WHERE prediction_id = ?",
                (survivor_pid,),
            ).fetchall()
        }
        assert rows == {ws_a: "accepted", ws_b: "rejected"}
    finally:
        db.close()


def _insert_prediction_accept_history(db, ws_id, photo_id, old_value,
                                      new_value="42"):
    """Insert a `prediction_accept` edit_history + edit_history_items pair
    directly, mirroring what `record_edit` would write for an accept edit
    without going through the full API. Returns (edit_id, item_id)."""
    cur = db.conn.execute(
        "INSERT INTO edit_history (workspace_id, action_type, description, "
        "new_value) VALUES (?, 'prediction_accept', 'accept', ?)",
        (ws_id, new_value),
    )
    edit_id = cur.lastrowid
    cur = db.conn.execute(
        "INSERT INTO edit_history_items (edit_id, photo_id, old_value, "
        "new_value) VALUES (?, ?, ?, ?)",
        (edit_id, photo_id, old_value, new_value),
    )
    return edit_id, cur.lastrowid


def test_prediction_fold_retargets_bare_edit_history_on_loser_delete(tmp_path):
    """The compact single-model accept encodes the prediction id as a
    bare-int string in `edit_history_items.old_value`. When the collision-
    merge deletes the loser, that id must be rewritten to the surviving
    prediction id so `_apply_undo` can still restore the status flip."""
    db, ws_id, p1, _p2 = _make_db(tmp_path)
    try:
        det = _insert_prediction(db, p1, "Say's phoebe", confidence=0.8)
        db.conn.execute(
            "INSERT INTO predictions (detection_id, classifier_model, "
            "labels_fingerprint, species, confidence) "
            "VALUES (?, 'm1', 'fp1', ?, ?)",
            (det, "Say’s phoebe", 0.4),
        )
        loser_pid = db.conn.execute(
            "SELECT id FROM predictions WHERE species = ?", ("Say’s phoebe",)
        ).fetchone()["id"]
        _, item_id = _insert_prediction_accept_history(
            db, ws_id, p1, old_value=str(loser_pid),
        )
        db.conn.commit()

        db._fold_prediction_species_apostrophes()
        db.conn.commit()

        survivor_pid = db.conn.execute(
            "SELECT id FROM predictions WHERE species = ?", ("Say's phoebe",)
        ).fetchone()["id"]
        row = db.conn.execute(
            "SELECT old_value FROM edit_history_items WHERE id = ?",
            (item_id,),
        ).fetchone()
        assert row["old_value"] == str(survivor_pid), (
            "loser prediction id in bare-int edit history was not retargeted"
        )
    finally:
        db.close()


def test_prediction_fold_retargets_json_prediction_id_on_loser_delete(tmp_path):
    """`no_tag` accepts serialize `{"prediction_id": N, "no_tag": true}` as
    the item's old_value. That id must be rewritten too — otherwise the
    reverse-a-no-op-accept undo path silently loses the reference and the
    prediction stays flipped."""
    db, ws_id, p1, _p2 = _make_db(tmp_path)
    try:
        det = _insert_prediction(db, p1, "Say's phoebe", confidence=0.8)
        db.conn.execute(
            "INSERT INTO predictions (detection_id, classifier_model, "
            "labels_fingerprint, species, confidence) "
            "VALUES (?, 'm1', 'fp1', ?, ?)",
            (det, "Say’s phoebe", 0.4),
        )
        loser_pid = db.conn.execute(
            "SELECT id FROM predictions WHERE species = ?", ("Say’s phoebe",)
        ).fetchone()["id"]
        payload = json.dumps({"prediction_id": loser_pid, "no_tag": True})
        _, item_id = _insert_prediction_accept_history(
            db, ws_id, p1, old_value=payload,
        )
        db.conn.commit()

        db._fold_prediction_species_apostrophes()
        db.conn.commit()

        survivor_pid = db.conn.execute(
            "SELECT id FROM predictions WHERE species = ?", ("Say's phoebe",)
        ).fetchone()["id"]
        row = db.conn.execute(
            "SELECT old_value FROM edit_history_items WHERE id = ?",
            (item_id,),
        ).fetchone()
        data = json.loads(row["old_value"])
        assert data["prediction_id"] == survivor_pid, (
            "JSON prediction_id was not retargeted to the surviving row"
        )
        assert data.get("no_tag") is True, (
            "unrelated JSON keys must survive the retarget"
        )
    finally:
        db.close()


def test_prediction_fold_retargets_prediction_ids_list_entry(tmp_path):
    """Accept-subject records `{"prediction_ids": [pid_a, pid_b, ...]}`
    covering agreeing sibling classifier models on one detection. If one
    sibling is the collision loser, only that entry must be rewritten:
    the untouched siblings must remain byte-identical so undo/redo still
    reaches every model's scope."""
    db, ws_id, p1, _p2 = _make_db(tmp_path)
    try:
        det = _insert_prediction(db, p1, "Say's phoebe", confidence=0.8)
        db.conn.execute(
            "INSERT INTO predictions (detection_id, classifier_model, "
            "labels_fingerprint, species, confidence) "
            "VALUES (?, 'm1', 'fp1', ?, ?)",
            (det, "Say’s phoebe", 0.4),
        )
        loser_pid = db.conn.execute(
            "SELECT id FROM predictions WHERE species = ?", ("Say’s phoebe",)
        ).fetchone()["id"]
        sibling_a = 9001
        sibling_b = 9002
        payload = json.dumps({
            "prediction_ids": [sibling_a, loser_pid, sibling_b],
        })
        _, item_id = _insert_prediction_accept_history(
            db, ws_id, p1, old_value=payload,
        )
        db.conn.commit()

        db._fold_prediction_species_apostrophes()
        db.conn.commit()

        survivor_pid = db.conn.execute(
            "SELECT id FROM predictions WHERE species = ?", ("Say's phoebe",)
        ).fetchone()["id"]
        row = db.conn.execute(
            "SELECT old_value FROM edit_history_items WHERE id = ?",
            (item_id,),
        ).fetchone()
        data = json.loads(row["old_value"])
        assert data["prediction_ids"] == [sibling_a, survivor_pid, sibling_b]
    finally:
        db.close()


def test_prediction_fold_retargets_edit_history_when_variant_wins_collision(
    tmp_path,
):
    """Symmetric case: when the curly variant has higher confidence, the
    migration deletes the *clean* row. A prediction_accept history item
    that references the clean row must be retargeted to the variant id
    (which then gets renamed onto the clean spelling in place)."""
    db, ws_id, p1, _p2 = _make_db(tmp_path)
    try:
        det = _insert_prediction(db, p1, "Say's phoebe", confidence=0.4)
        db.conn.execute(
            "INSERT INTO predictions (detection_id, classifier_model, "
            "labels_fingerprint, species, confidence) "
            "VALUES (?, 'm1', 'fp1', ?, ?)",
            (det, "Say’s phoebe", 0.8),
        )
        clean_pid = db.conn.execute(
            "SELECT id FROM predictions WHERE species = ?", ("Say's phoebe",)
        ).fetchone()["id"]
        _, item_id = _insert_prediction_accept_history(
            db, ws_id, p1, old_value=str(clean_pid),
        )
        db.conn.commit()

        db._fold_prediction_species_apostrophes()
        db.conn.commit()

        survivor_pid = db.conn.execute(
            "SELECT id FROM predictions WHERE species = ?", ("Say's phoebe",)
        ).fetchone()["id"]
        row = db.conn.execute(
            "SELECT old_value FROM edit_history_items WHERE id = ?",
            (item_id,),
        ).fetchone()
        assert row["old_value"] == str(survivor_pid)
    finally:
        db.close()


def test_prediction_fold_retargets_relabel_edit_history(tmp_path):
    """``/api/highlights/relabel`` records edits as ``keyword_add`` or
    ``species_replace`` (not ``prediction_accept``), stashing the top
    prediction's id in ``edit_history_items.old_value`` as JSON
    ``{"prediction_id": N, "prediction_status": ...}`` so undo can restore
    the pending status via ``_restore_edit_prediction_status``.

    Before the fold retargets these action types too, an undo/redo would
    call ``update_prediction_status(loser_id, ...)`` on a vanished
    prediction id, failing the ``prediction_review`` FK to
    ``predictions`` and aborting the undo/redo."""
    db, ws_id, p1, _p2 = _make_db(tmp_path)
    try:
        det = _insert_prediction(db, p1, "Say's phoebe", confidence=0.8)
        db.conn.execute(
            "INSERT INTO predictions (detection_id, classifier_model, "
            "labels_fingerprint, species, confidence) "
            "VALUES (?, 'm1', 'fp1', ?, ?)",
            (det, "Say’s phoebe", 0.4),
        )
        loser_pid = db.conn.execute(
            "SELECT id FROM predictions WHERE species = ?", ("Say’s phoebe",)
        ).fetchone()["id"]

        payload = json.dumps({
            "keyword_id": "17",
            "keyword_ids": [17],
            "prediction_id": loser_pid,
            "prediction_status": "pending",
        }, sort_keys=True)

        # keyword_add relabel
        ka_edit = db.conn.execute(
            "INSERT INTO edit_history (workspace_id, action_type, description, "
            "new_value) VALUES (?, 'keyword_add', 'relabel', '17')",
            (ws_id,),
        ).lastrowid
        ka_item = db.conn.execute(
            "INSERT INTO edit_history_items (edit_id, photo_id, old_value, "
            "new_value) VALUES (?, ?, ?, '17')",
            (ka_edit, p1, payload),
        ).lastrowid

        # species_replace relabel (photo already had a species tag)
        sr_edit = db.conn.execute(
            "INSERT INTO edit_history (workspace_id, action_type, description, "
            "new_value) VALUES (?, 'species_replace', 'relabel', '17')",
            (ws_id,),
        ).lastrowid
        sr_item = db.conn.execute(
            "INSERT INTO edit_history_items (edit_id, photo_id, old_value, "
            "new_value) VALUES (?, ?, ?, '17')",
            (sr_edit, p1, payload),
        ).lastrowid

        db.conn.commit()

        db._fold_prediction_species_apostrophes()
        db.conn.commit()

        survivor_pid = db.conn.execute(
            "SELECT id FROM predictions WHERE species = ?", ("Say's phoebe",)
        ).fetchone()["id"]
        for item_id, label in ((ka_item, "keyword_add"),
                               (sr_item, "species_replace")):
            row = db.conn.execute(
                "SELECT old_value FROM edit_history_items WHERE id = ?",
                (item_id,),
            ).fetchone()
            data = json.loads(row["old_value"])
            assert data["prediction_id"] == survivor_pid, (
                f"{label}.old_value prediction_id was not retargeted to the "
                f"surviving prediction"
            )
            assert data["keyword_id"] == "17", (
                f"{label}.old_value keyword_id must survive the retarget "
                "unchanged"
            )
    finally:
        db.close()


def test_prediction_fold_only_retargets_prediction_accept_history(tmp_path):
    """Guard against a blanket UPDATE: `keyword_add.new_value` and
    `species_replace.new_value`/`.old_value` store keyword ids in the
    same columns. A keyword id that numerically happens to equal a
    deleted-prediction id must not be silently rewritten to the
    surviving prediction id."""
    db, ws_id, p1, _p2 = _make_db(tmp_path)
    try:
        det = _insert_prediction(db, p1, "Say's phoebe", confidence=0.8)
        db.conn.execute(
            "INSERT INTO predictions (detection_id, classifier_model, "
            "labels_fingerprint, species, confidence) "
            "VALUES (?, 'm1', 'fp1', ?, ?)",
            (det, "Say’s phoebe", 0.4),
        )
        loser_pid = db.conn.execute(
            "SELECT id FROM predictions WHERE species = ?", ("Say’s phoebe",)
        ).fetchone()["id"]
        # A `keyword_add` row whose new_value is the numeric string that
        # matches the loser prediction id. Must survive the fold unchanged.
        kw_edit = db.conn.execute(
            "INSERT INTO edit_history (workspace_id, action_type, description, "
            "new_value) VALUES (?, 'keyword_add', 'add', ?)",
            (ws_id, str(loser_pid)),
        ).lastrowid
        kw_item = db.conn.execute(
            "INSERT INTO edit_history_items (edit_id, photo_id, old_value, "
            "new_value) VALUES (?, ?, ?, ?)",
            (kw_edit, p1, "", str(loser_pid)),
        ).lastrowid
        db.conn.commit()

        db._fold_prediction_species_apostrophes()
        db.conn.commit()

        row = db.conn.execute(
            "SELECT old_value, new_value FROM edit_history_items WHERE id = ?",
            (kw_item,),
        ).fetchone()
        assert row["old_value"] == "", (
            "keyword_add.old_value was unexpectedly rewritten by the "
            "prediction-id retarget"
        )
        assert row["new_value"] == str(loser_pid), (
            "keyword_add.new_value carries a keyword id and must not be "
            "retargeted by the prediction-id path"
        )
    finally:
        db.close()


def test_add_prediction_folds_curly_apostrophe_species(tmp_path):
    """`Database.add_prediction` is the shared choke point for the classify
    job's storage helpers (`_store_pending_detection_prediction`,
    `_store_match_prediction`). Neither production path runs
    `normalize_keyword_display` before calling here, so a bundled label
    that spells `Swinhoe’s White-eye` with U+2019 would still land in
    predictions.species as-is and fail to match its accepted
    `Swinhoe's white-eye` keyword row (both exact and COLLATE NOCASE joins
    are quote-preserving). Folding inside `add_prediction` closes that
    hole once for every caller."""
    db, _ws_id, p1, _p2 = _make_db(tmp_path)
    try:
        det = db.conn.execute(
            "INSERT INTO detections (photo_id, category, detector_confidence) "
            "VALUES (?, 'animal', 0.99)",
            (p1,),
        ).lastrowid
        db.conn.commit()

        db.add_prediction(
            detection_id=det,
            species="Swinhoe’s White-eye",
            confidence=0.8,
            model="m1",
            labels_fingerprint="fp1",
        )

        stored = [
            r["species"] for r in db.conn.execute(
                "SELECT species FROM predictions"
            ).fetchall()
        ]
        assert stored == ["Swinhoe's White-eye"]
    finally:
        db.close()


def test_add_prediction_folds_review_row_to_normalized_species(tmp_path):
    """When add_prediction folds species, the workspace-scoped
    prediction_review row must land on the *folded* prediction row's id,
    not on a stray original-spelling row (which would never exist because
    the fold applies before insert). Verifies the id lookup after fold."""
    db, ws_id, p1, _p2 = _make_db(tmp_path)
    try:
        det = db.conn.execute(
            "INSERT INTO detections (photo_id, category, detector_confidence) "
            "VALUES (?, 'animal', 0.99)",
            (p1,),
        ).lastrowid
        db.conn.commit()

        db.add_prediction(
            detection_id=det,
            species="Swinhoe’s White-eye",
            confidence=0.8,
            model="m1",
            status="accepted",
            labels_fingerprint="fp1",
        )

        row = db.conn.execute(
            "SELECT p.id AS pid, p.species, pr.status "
            "FROM predictions p "
            "LEFT JOIN prediction_review pr "
            "  ON pr.prediction_id = p.id AND pr.workspace_id = ? "
            "WHERE p.detection_id = ?",
            (ws_id, det),
        ).fetchone()
        assert row is not None
        assert row["species"] == "Swinhoe's White-eye"
        assert row["status"] == "accepted"
    finally:
        db.close()


def test_apostrophe_fold_migration_gated_by_its_own_marker(tmp_path):
    """The v1 sweep already ran on live DBs (marker
    `keyword_names_normalized` is set), so the apostrophe fold needs its
    own marker or it would silently never run on the databases that
    actually have the duplicate rows."""
    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    # Simulate a DB that completed the earlier sweeps, then acquired an
    # apostrophe-variant duplicate before the fold existed.
    db.set_meta("keyword_names_normalized", "1")
    db.set_meta("curation_species_case_aligned_v2", "1")
    db.conn.execute(
        "DELETE FROM db_meta WHERE key = 'keyword_apostrophes_folded_v1'"
    )
    _insert_keyword(db, "Say's phoebe", "taxonomy", is_species=1)
    _insert_keyword(db, "Say’s phoebe", "taxonomy", is_species=1)
    db.conn.commit()
    db.close()

    reopened = Database(db_path)
    try:
        names = [
            r["name"] for r in reopened.conn.execute(
                "SELECT name FROM keywords WHERE name LIKE 'Say%phoebe'"
            ).fetchall()
        ]
        assert names == ["Say's phoebe"], names
        assert reopened.get_meta("keyword_apostrophes_folded_v1") == "1"
    finally:
        reopened.close()


def test_migration_gated_by_db_meta_marker(tmp_path):
    """The backfill runs once per database, gated by the db_meta marker
    (NOT PRAGMA user_version — live DBs have been advanced past the next
    free version number by unmerged branch builds, which would silently
    skip a version-gated migration). Clearing the marker re-runs the sweep
    on the next open; with the marker present, a raw-seeded variant
    survives reopen untouched."""
    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    db.close()

    conn = sqlite3.connect(db_path)
    conn.execute(
        "INSERT INTO keywords (name, parent_id, is_species, type) "
        "VALUES ('‘apapane', NULL, 1, 'taxonomy')"
    )
    conn.execute("DELETE FROM db_meta WHERE key = 'keyword_names_normalized'")
    conn.commit()
    conn.close()

    db = Database(db_path)
    try:
        names = [
            r["name"] for r in db.conn.execute(
                "SELECT name FROM keywords WHERE name LIKE '%apapane%'"
            )
        ]
        assert names == ["apapane"]
        assert db.get_meta("keyword_names_normalized") == "1"
    finally:
        db.close()

    # Marker present: a raw variant seeded now must survive reopen
    # untouched (one-shot semantics — later opens don't re-run the sweep).
    conn = sqlite3.connect(db_path)
    conn.execute(
        "INSERT INTO keywords (name, parent_id, is_species, type) "
        "VALUES ('‘elepaio', NULL, 1, 'taxonomy')"
    )
    conn.commit()
    conn.close()

    db = Database(db_path)
    try:
        names = {
            r["name"] for r in db.conn.execute(
                "SELECT name FROM keywords WHERE name LIKE '%elepaio%'"
            )
        }
        assert names == {"‘elepaio"}
    finally:
        db.close()


def test_migration_keeps_plain_general_out_of_taxonomy_fold(tmp_path):
    """When a taxonomy peer exists alongside a clean general with the
    same match key, the migration folds species-bearing generals into
    the taxonomy row (species survivor), but a plain
    ``type='general', is_species=0`` homonym must stay separate: the
    taxonomy destination is species-bearing, and merging the plain
    general would (via _merge_keyword_into's same-boundary is_species
    CASE, since the taxonomy row already has is_species=1) leave every
    plain-general photo tagged with a species keyword it never had.
    Partitioning clean_generals by is_species inside the taxonomy
    branch of the subgroup construction keeps the plain general in its
    own subgroup."""
    db, _ws_id, p1, p2 = _make_db(tmp_path)
    try:
        taxonomy_id = _insert_keyword(db, "Robin", "taxonomy", is_species=1)
        plain_general_id = _insert_keyword(db, "robin", "general")
        db.conn.execute(
            "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
            (p1, taxonomy_id),
        )
        db.conn.execute(
            "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
            (p2, plain_general_id),
        )
        db.conn.commit()

        db._normalize_keyword_data_once()
        db.conn.commit()

        rows = db.conn.execute(
            "SELECT id, type, is_species FROM keywords "
            "WHERE LOWER(name) = 'robin' ORDER BY id"
        ).fetchall()
        # Both rows survive with their own type/is_species and their own
        # photo tag. p2's photo must NOT have been swept onto the
        # taxonomy row, and the plain general must still read is_species=0.
        assert [(r["id"], r["type"], r["is_species"]) for r in rows] == [
            (taxonomy_id, "taxonomy", 1),
            (plain_general_id, "general", 0),
        ]
        tax_tags = {
            r["photo_id"] for r in db.conn.execute(
                "SELECT photo_id FROM photo_keywords WHERE keyword_id = ?",
                (taxonomy_id,),
            )
        }
        plain_tags = {
            r["photo_id"] for r in db.conn.execute(
                "SELECT photo_id FROM photo_keywords WHERE keyword_id = ?",
                (plain_general_id,),
            )
        }
        assert tax_tags == {p1}
        assert plain_tags == {p2}
    finally:
        db.close()


def test_migration_species_replace_preexisting_survivor_undo_safe(tmp_path):
    """When a legacy species keyword is merged into a survivor on a
    photo that already carried the survivor, the retargeted
    ``species_replace`` edit-history item must not, on undo, remove the
    pre-existing survivor tag. The migration deletes such items before
    retargeting so undo iterates 0 items for that photo instead of
    untagging the survivor the edit never created."""
    db, ws_id, p1, _p2 = _make_db(tmp_path)
    try:
        # Two species keywords at the same match key: SQLite treats NULL
        # parents as distinct for UNIQUE(name, parent_id), so both can
        # coexist at the top level. The variant is the src; the clean
        # spelling wins as survivor.
        src_id = _insert_keyword(db, "‘Robin", "taxonomy", is_species=1)
        dst_id = _insert_keyword(db, "Robin", "taxonomy", is_species=1)
        # p1 already carried the survivor before the merge — the edit
        # history entry below points at the src, but the retargeted
        # item.new_value = dst_id would silently untag the survivor on
        # undo if the migration didn't strip the item first.
        db.conn.execute(
            "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
            (p1, dst_id),
        )
        db.conn.execute(
            "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
            (p1, src_id),
        )
        db.conn.execute(
            "INSERT INTO edit_history "
            "(action_type, description, new_value, workspace_id) "
            "VALUES ('species_replace', 'x', ?, ?)",
            (str(src_id), ws_id),
        )
        edit_id = db.conn.execute(
            "SELECT id FROM edit_history WHERE new_value = ? ORDER BY id DESC LIMIT 1",
            (str(src_id),),
        ).fetchone()["id"]
        db.conn.execute(
            "INSERT INTO edit_history_items "
            "(edit_id, photo_id, old_value, new_value) "
            "VALUES (?, ?, '', ?)",
            (edit_id, p1, str(src_id)),
        )
        db.conn.commit()

        db._normalize_keyword_data_once()
        db.conn.commit()

        # The migration retargets the edit-history entry-level new_value
        # to the survivor id, so undo would look up dst_id. The
        # per-photo item pointing at the survivor for a pre-existing tag
        # must have been dropped so undo doesn't untag it.
        remaining = db.conn.execute(
            "SELECT new_value FROM edit_history_items WHERE edit_id = ?",
            (edit_id,),
        ).fetchall()
        assert remaining == []
        # The survivor tag must still be on the photo.
        surv_still_tagged = db.conn.execute(
            "SELECT 1 FROM photo_keywords "
            "WHERE photo_id = ? AND keyword_id = ?",
            (p1, dst_id),
        ).fetchone()
        assert surv_still_tagged is not None
    finally:
        db.close()


def test_migration_species_replace_old_side_preexisting_survivor_undo_safe(tmp_path):
    """Symmetric to the new-side case above. A prior ``species_replace``
    swapped src → some other species; ``item.old_value`` stores str(src_id)
    (bare-string form). If the merged photo already carried the survivor
    at merge time, a src→dst retarget of item.old_value would leave
    ``_apply_redo`` iterating old_kids=[dst_id] and untag_photo(pid,
    dst_id), stripping the pre-existing survivor tag. Same applies to the
    JSON ``keyword_ids`` payload form used by newer swaps. The migration
    must drop those items instead of retargeting them."""
    db, ws_id, p1, _p2 = _make_db(tmp_path)
    try:
        src_id = _insert_keyword(db, "‘Robin", "taxonomy", is_species=1)
        dst_id = _insert_keyword(db, "Robin", "taxonomy", is_species=1)
        other_id = _insert_keyword(db, "Sparrow", "taxonomy", is_species=1)
        # p1 pre-existed with the survivor tag before the merge.
        db.conn.execute(
            "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
            (p1, dst_id),
        )
        db.conn.execute(
            "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
            (p1, src_id),
        )
        # Bare-string old_value species_replace: the edit swapped
        # src → Sparrow. edit_history.new_value = str(other_id) (NOT
        # src_id), so the existing new-side cleanup can't reach this row.
        db.conn.execute(
            "INSERT INTO edit_history "
            "(action_type, description, new_value, workspace_id) "
            "VALUES ('species_replace', 'x', ?, ?)",
            (str(other_id), ws_id),
        )
        bare_edit_id = db.conn.execute(
            "SELECT id FROM edit_history WHERE description = 'x' "
            "ORDER BY id DESC LIMIT 1"
        ).fetchone()["id"]
        db.conn.execute(
            "INSERT INTO edit_history_items "
            "(edit_id, photo_id, old_value, new_value) "
            "VALUES (?, ?, ?, ?)",
            (bare_edit_id, p1, str(src_id), str(other_id)),
        )
        # JSON-payload old_value species_replace: another swap that
        # replaced [src, Sparrow] → some third species. keyword_ids
        # references src; a naive rewrite would replace it with dst
        # in the list and leave redo untagging dst_id (the survivor).
        db.conn.execute(
            "INSERT INTO edit_history "
            "(action_type, description, new_value, workspace_id) "
            "VALUES ('species_replace', 'y', ?, ?)",
            (str(other_id), ws_id),
        )
        json_edit_id = db.conn.execute(
            "SELECT id FROM edit_history WHERE description = 'y' "
            "ORDER BY id DESC LIMIT 1"
        ).fetchone()["id"]
        import json as _json
        json_payload = _json.dumps(
            {"keyword_id": src_id, "keyword_ids": [src_id, other_id]},
            sort_keys=True,
        )
        db.conn.execute(
            "INSERT INTO edit_history_items "
            "(edit_id, photo_id, old_value, new_value) "
            "VALUES (?, ?, ?, ?)",
            (json_edit_id, p1, json_payload, str(other_id)),
        )
        db.conn.commit()

        db._normalize_keyword_data_once()
        db.conn.commit()

        # Both items on p1 must have been dropped so redo can't strip
        # the survivor tag by untagging dst_id.
        bare_remaining = db.conn.execute(
            "SELECT id FROM edit_history_items WHERE edit_id = ?",
            (bare_edit_id,),
        ).fetchall()
        assert bare_remaining == []
        json_remaining = db.conn.execute(
            "SELECT id FROM edit_history_items WHERE edit_id = ?",
            (json_edit_id,),
        ).fetchall()
        assert json_remaining == []
        surv_still_tagged = db.conn.execute(
            "SELECT 1 FROM photo_keywords "
            "WHERE photo_id = ? AND keyword_id = ?",
            (p1, dst_id),
        ).fetchone()
        assert surv_still_tagged is not None
    finally:
        db.close()


def test_migration_keeps_keyword_remove_item_when_survivor_added_later(tmp_path):
    """A ``keyword_remove`` edit for the src, followed by a later
    ``keyword_add`` of the survivor to the same photo, must survive
    the migration retargeted rather than being dropped. Latest-first
    undo runs the later add's untag first — dst_id leaves the photo
    — and the earlier remove's undo is then the only item that can
    restore the merged keyword by re-tagging dst_id. Dropping the
    item would leave the photo un-tagged after both edits are undone,
    silently losing the survivor tag from history."""
    db, ws_id, p1, _p2 = _make_db(tmp_path)
    try:
        src_id = _insert_keyword(db, "‘Robin", "taxonomy", is_species=1)
        dst_id = _insert_keyword(db, "Robin", "taxonomy", is_species=1)
        # Photo currently carries dst_id (added later, see edit A below).
        db.conn.execute(
            "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
            (p1, dst_id),
        )
        # Edit R: keyword_remove src from p1 (earlier).
        db.conn.execute(
            "INSERT INTO edit_history "
            "(action_type, description, new_value, workspace_id) "
            "VALUES ('keyword_remove', 'r', ?, ?)",
            (str(src_id), ws_id),
        )
        remove_edit_id = db.conn.execute(
            "SELECT id FROM edit_history WHERE description = 'r' "
            "ORDER BY id DESC LIMIT 1"
        ).fetchone()["id"]
        db.conn.execute(
            "INSERT INTO edit_history_items "
            "(edit_id, photo_id, old_value, new_value) "
            "VALUES (?, ?, ?, '')",
            (remove_edit_id, p1, str(src_id)),
        )
        # Edit A: keyword_add dst to p1 (later — higher id).
        db.conn.execute(
            "INSERT INTO edit_history "
            "(action_type, description, new_value, workspace_id) "
            "VALUES ('keyword_add', 'a', ?, ?)",
            (str(dst_id), ws_id),
        )
        add_edit_id = db.conn.execute(
            "SELECT id FROM edit_history WHERE description = 'a' "
            "ORDER BY id DESC LIMIT 1"
        ).fetchone()["id"]
        db.conn.execute(
            "INSERT INTO edit_history_items "
            "(edit_id, photo_id, old_value, new_value) "
            "VALUES (?, ?, '', ?)",
            (add_edit_id, p1, str(dst_id)),
        )
        db.conn.commit()

        db._normalize_keyword_data_once()
        db.conn.commit()

        # The remove item must have survived and been retargeted to dst.
        remove_items = db.conn.execute(
            "SELECT old_value FROM edit_history_items WHERE edit_id = ?",
            (remove_edit_id,),
        ).fetchall()
        assert [r["old_value"] for r in remove_items] == [str(dst_id)]
        assert db.conn.execute(
            "SELECT 1 FROM photo_keywords "
            "WHERE photo_id = ? AND keyword_id = ?",
            (p1, dst_id),
        ).fetchone() is not None

        # Simulate latest-first undo: undo A (untag dst), then undo R
        # (tag dst via INSERT OR IGNORE). After both, dst must be back
        # on the photo — the historical remove of the merged keyword
        # got restored by the item the migration preserved.
        db.conn.execute(
            "DELETE FROM photo_keywords "
            "WHERE photo_id = ? AND keyword_id = ?",
            (p1, dst_id),
        )
        db.conn.execute(
            "INSERT OR IGNORE INTO photo_keywords (photo_id, keyword_id) "
            "VALUES (?, ?)",
            (p1, int(remove_items[0]["old_value"])),
        )
        db.conn.commit()
        assert db.conn.execute(
            "SELECT 1 FROM photo_keywords "
            "WHERE photo_id = ? AND keyword_id = ?",
            (p1, dst_id),
        ).fetchone() is not None
    finally:
        db.close()


def test_migration_drops_keyword_remove_item_when_survivor_pre_existed(tmp_path):
    """Negative counterpart of the case above. When the survivor
    genuinely pre-existed a ``keyword_remove`` edit — no later add of
    dst brings it back — the retargeted item must still be dropped so
    that redoing the remove does not strip the pre-existing survivor
    tag the edit never created."""
    db, ws_id, p1, _p2 = _make_db(tmp_path)
    try:
        src_id = _insert_keyword(db, "‘Robin", "taxonomy", is_species=1)
        dst_id = _insert_keyword(db, "Robin", "taxonomy", is_species=1)
        # Photo carries dst_id and previously carried src_id. dst_id
        # was tagged before the remove and never touched afterwards.
        db.conn.execute(
            "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
            (p1, dst_id),
        )
        # Edit R: keyword_remove src from p1. No subsequent add of
        # dst — the current dst_id tag pre-existed R.
        db.conn.execute(
            "INSERT INTO edit_history "
            "(action_type, description, new_value, workspace_id) "
            "VALUES ('keyword_remove', 'r', ?, ?)",
            (str(src_id), ws_id),
        )
        remove_edit_id = db.conn.execute(
            "SELECT id FROM edit_history WHERE description = 'r' "
            "ORDER BY id DESC LIMIT 1"
        ).fetchone()["id"]
        db.conn.execute(
            "INSERT INTO edit_history_items "
            "(edit_id, photo_id, old_value, new_value) "
            "VALUES (?, ?, ?, '')",
            (remove_edit_id, p1, str(src_id)),
        )
        db.conn.commit()

        db._normalize_keyword_data_once()
        db.conn.commit()

        remove_items = db.conn.execute(
            "SELECT id FROM edit_history_items WHERE edit_id = ?",
            (remove_edit_id,),
        ).fetchall()
        assert remove_items == []
        assert db.conn.execute(
            "SELECT 1 FROM photo_keywords "
            "WHERE photo_id = ? AND keyword_id = ?",
            (p1, dst_id),
        ).fetchone() is not None
    finally:
        db.close()


def test_migration_keeps_source_add_when_survivor_was_added_later(tmp_path):
    """If src was added first and dst later, merging them makes the later
    add redundant. Preserve and retarget the original source add while
    dropping the later survivor item, so latest-first undo leaves the merged
    tag present until the original add is undone.
    """
    db, ws_id, p1, _p2 = _make_db(tmp_path)
    try:
        src_id = _insert_keyword(db, "‘Robin", "taxonomy", is_species=1)
        dst_id = _insert_keyword(db, "Robin", "taxonomy", is_species=1)
        db.conn.executemany(
            "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
            ((p1, src_id), (p1, dst_id)),
        )

        db.conn.execute(
            "INSERT INTO edit_history "
            "(action_type, description, new_value, workspace_id) "
            "VALUES ('keyword_add', 'source add', ?, ?)",
            (str(src_id), ws_id),
        )
        source_edit_id = db.conn.execute(
            "SELECT id FROM edit_history WHERE description = 'source add'"
        ).fetchone()["id"]
        db.conn.execute(
            "INSERT INTO edit_history_items "
            "(edit_id, photo_id, old_value, new_value) VALUES (?, ?, '', ?)",
            (source_edit_id, p1, str(src_id)),
        )

        db.conn.execute(
            "INSERT INTO edit_history "
            "(action_type, description, new_value, workspace_id) "
            "VALUES ('keyword_add', 'survivor add', ?, ?)",
            (str(dst_id), ws_id),
        )
        survivor_edit_id = db.conn.execute(
            "SELECT id FROM edit_history WHERE description = 'survivor add'"
        ).fetchone()["id"]
        db.conn.execute(
            "INSERT INTO edit_history_items "
            "(edit_id, photo_id, old_value, new_value) VALUES (?, ?, '', ?)",
            (survivor_edit_id, p1, str(dst_id)),
        )
        db.conn.commit()

        db._normalize_keyword_data_once()
        db.conn.commit()

        source_items = db.conn.execute(
            "SELECT new_value FROM edit_history_items WHERE edit_id = ?",
            (source_edit_id,),
        ).fetchall()
        survivor_items = db.conn.execute(
            "SELECT new_value FROM edit_history_items WHERE edit_id = ?",
            (survivor_edit_id,),
        ).fetchall()
        assert [r["new_value"] for r in source_items] == [str(dst_id)]
        assert survivor_items == []

        # Undoing the now-redundant later add has no per-photo work; the
        # merged tag remains until undo reaches the original source add.
        assert db.conn.execute(
            "SELECT 1 FROM photo_keywords WHERE photo_id = ? AND keyword_id = ?",
            (p1, dst_id),
        ).fetchone() is not None
        db.conn.execute(
            "DELETE FROM photo_keywords WHERE photo_id = ? AND keyword_id = ?",
            (p1, int(source_items[0]["new_value"])),
        )
        assert db.conn.execute(
            "SELECT 1 FROM photo_keywords WHERE photo_id = ? AND keyword_id = ?",
            (p1, dst_id),
        ).fetchone() is None
    finally:
        db.close()


def test_curation_case_alignment_v2_runs_once_by_marker(tmp_path):
    """The v2 case-alignment sweep runs once per database under its own
    marker, catching curation rows starred with prediction casing between
    the v1 repair and the setter canonicalization fix."""
    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    db.close()

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    ws_id = conn.execute("SELECT id FROM workspaces LIMIT 1").fetchone()["id"]
    conn.execute(
        "INSERT INTO folders (path, name, status) VALUES ('/p', 'p', 'ok')"
    )
    fid = conn.execute("SELECT id FROM folders WHERE path = '/p'").fetchone()["id"]
    conn.execute(
        "INSERT INTO photos (folder_id, filename) VALUES (?, 'a.jpg')", (fid,)
    )
    pid = conn.execute("SELECT id FROM photos LIMIT 1").fetchone()["id"]
    conn.execute(
        "INSERT INTO keywords (name, type, is_species) "
        "VALUES ('Saffron finch', 'taxonomy', 1)"
    )
    # Simulate the pre-fix state: a highlight starred from a
    # prediction-cased bucket label after the v1 repair already ran.
    conn.execute(
        "INSERT INTO species_highlights "
        "(workspace_id, species, photo_id, rank) VALUES (?, ?, ?, 1)",
        (ws_id, "Saffron Finch", pid),
    )
    conn.execute(
        "DELETE FROM db_meta WHERE key = 'curation_species_case_aligned_v2'"
    )
    conn.commit()
    conn.close()

    db = Database(db_path)
    try:
        rows = db.conn.execute(
            "SELECT species FROM species_highlights"
        ).fetchall()
        assert [r["species"] for r in rows] == ["Saffron finch"]
        assert db.get_meta("curation_species_case_aligned_v2") == "1"
    finally:
        db.close()

    # Marker present: a raw mismatch seeded now survives reopen untouched
    # (one-shot semantics — the setters prevent new mismatches instead).
    conn = sqlite3.connect(db_path)
    conn.execute(
        "INSERT INTO species_highlights "
        "(workspace_id, species, photo_id, rank) "
        "SELECT workspace_id, 'SAFFRON FINCH', photo_id, 2 "
        "FROM species_highlights LIMIT 1"
    )
    conn.commit()
    conn.close()

    db = Database(db_path)
    try:
        rows = db.conn.execute(
            "SELECT species FROM species_highlights ORDER BY rank"
        ).fetchall()
        assert [r["species"] for r in rows] == ["Saffron finch", "SAFFRON FINCH"]
    finally:
        db.close()


def test_curation_case_alignment_v2_rewrites_history_snapshots(tmp_path):
    """Undo/redo snapshots keyed on prediction casing (created between the
    v1 sweep and the setter canonicalization fix) get rewritten by the v2
    gate too, so a later undo can't recreate the orphaned curation rows
    the v2 sweep is meant to repair."""
    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    db.close()

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute(
        "INSERT INTO folders (path, name, status) VALUES ('/p', 'p', 'ok')"
    )
    fid = conn.execute("SELECT id FROM folders WHERE path = '/p'").fetchone()["id"]
    conn.execute(
        "INSERT INTO photos (folder_id, filename) VALUES (?, 'a.jpg')", (fid,)
    )
    pid = conn.execute("SELECT id FROM photos LIMIT 1").fetchone()["id"]
    conn.execute(
        "INSERT INTO keywords (name, type, is_species) "
        "VALUES ('Saffron finch', 'taxonomy', 1)"
    )
    # Seed a relabel edit_history_item captured with prediction casing.
    # Both hl_prev entry shapes (bare string and dict) plus pref_prev
    # and rep_prev cover the branches in _align_curation_history_species.
    conn.execute(
        "INSERT INTO edit_history (action_type, description) "
        "VALUES ('relabel', 't')"
    )
    edit_id = conn.execute(
        "SELECT id FROM edit_history LIMIT 1"
    ).fetchone()["id"]
    payload = {
        "curation": {
            "hl_prev": [
                "Saffron Finch",
                {"species": "Saffron Finch", "rank": 1, "photo_id": pid},
            ],
            "pref_prev": [{"species": "Saffron Finch", "photo_id": pid}],
            "rep_prev": [{"species": "Saffron Finch", "photo_id": pid}],
        }
    }
    conn.execute(
        "INSERT INTO edit_history_items "
        "(edit_id, photo_id, old_value, new_value) VALUES (?, ?, ?, '')",
        (edit_id, pid, json.dumps(payload)),
    )
    conn.execute(
        "DELETE FROM db_meta WHERE key = 'curation_species_case_aligned_v2'"
    )
    conn.commit()
    conn.close()

    db = Database(db_path)
    try:
        row = db.conn.execute(
            "SELECT old_value FROM edit_history_items LIMIT 1"
        ).fetchone()
        rewritten = json.loads(row["old_value"])
        curation = rewritten["curation"]
        assert curation["hl_prev"][0] == "Saffron finch"
        assert curation["hl_prev"][1]["species"] == "Saffron finch"
        assert curation["pref_prev"][0]["species"] == "Saffron finch"
        assert curation["rep_prev"][0]["species"] == "Saffron finch"
        assert db.get_meta("curation_species_case_aligned_v2") == "1"
    finally:
        db.close()


def test_curation_case_alignment_v2_rekeys_prediction_only_rows(tmp_path, monkeypatch):
    """A ``species_highlights`` row starred from an unconfirmed prediction
    before the corresponding keyword row exists must still land on the
    canonical spelling the bucket-collection path emits — otherwise the
    star silently disappears from the UI (bucket keyed
    ``Common waxbill`` vs stored row ``Common Waxbill``) and a DELETE
    keyed on the bucket label deletes zero rows.

    The v2 sweep therefore falls back to the detected case convention
    when no keyword row exists for the row's match_key, mirroring
    ``resolve_species_display_name`` (which is what the bucket path uses
    to canonicalize prediction labels).
    """
    # Isolate config so a real ~/.vireo/config.json `keyword_case`
    # override can't preempt the auto-detected "lower" convention below.
    import config as cfg
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    db.close()

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    ws_id = conn.execute("SELECT id FROM workspaces LIMIT 1").fetchone()["id"]
    conn.execute(
        "INSERT INTO folders (path, name, status) VALUES ('/p', 'p', 'ok')"
    )
    fid = conn.execute("SELECT id FROM folders WHERE path = '/p'").fetchone()["id"]
    conn.execute(
        "INSERT INTO photos (folder_id, filename) VALUES (?, 'a.jpg')", (fid,)
    )
    pid = conn.execute("SELECT id FROM photos LIMIT 1").fetchone()["id"]
    # Three lower-convention species keywords establish the convention
    # detector so the fallback picks "lower" for unseen predictions.
    # None of them match "Common Waxbill" — the prediction has no keyword
    # row at all, exercising the no-keyword branch.
    for name in ("Black phoebe", "Saffron finch", "Anna's hummingbird"):
        conn.execute(
            "INSERT INTO keywords (name, type, is_species) "
            "VALUES (?, 'taxonomy', 1)",
            (name,),
        )
    # Pre-fix state: highlight starred from an unconfirmed prediction
    # bucket labeled `Common Waxbill`; no `Common waxbill` keyword row
    # yet because the photo was never accepted.
    conn.execute(
        "INSERT INTO species_highlights "
        "(workspace_id, species, photo_id, rank) VALUES (?, ?, ?, 1)",
        (ws_id, "Common Waxbill", pid),
    )
    # Same state on the two neighbour curation tables.
    conn.execute(
        "INSERT INTO photo_preferences "
        "(workspace_id, purpose, species, photo_id) VALUES (?, 'picked', ?, ?)",
        (ws_id, "Common Waxbill", pid),
    )
    conn.execute(
        "INSERT INTO species_representatives "
        "(species, photo_id, selected_order) VALUES (?, ?, 1)",
        ("Common Waxbill", pid),
    )
    # Seed a matching relabel undo snapshot too — the same no-keyword
    # branch has to run in `_align_curation_history_species` or a later
    # undo would recreate the orphaned rows.
    conn.execute(
        "INSERT INTO edit_history (action_type, description) "
        "VALUES ('relabel', 't')"
    )
    edit_id = conn.execute(
        "SELECT id FROM edit_history LIMIT 1"
    ).fetchone()["id"]
    history_payload = {
        "curation": {
            "hl_prev": [{"species": "Common Waxbill", "rank": 1, "photo_id": pid}],
            "pref_prev": [{"species": "Common Waxbill", "photo_id": pid}],
            "rep_prev": [{"species": "Common Waxbill", "photo_id": pid}],
        }
    }
    conn.execute(
        "INSERT INTO edit_history_items "
        "(edit_id, photo_id, old_value, new_value) VALUES (?, ?, ?, '')",
        (edit_id, pid, json.dumps(history_payload)),
    )
    conn.execute(
        "DELETE FROM db_meta WHERE key = 'curation_species_case_aligned_v2'"
    )
    conn.commit()
    conn.close()

    db = Database(db_path)
    try:
        highlight_species = [
            r["species"] for r in db.conn.execute(
                "SELECT species FROM species_highlights"
            ).fetchall()
        ]
        preference_species = [
            r["species"] for r in db.conn.execute(
                "SELECT species FROM photo_preferences"
            ).fetchall()
        ]
        representative_species = [
            r["species"] for r in db.conn.execute(
                "SELECT species FROM species_representatives"
            ).fetchall()
        ]
        assert highlight_species == ["Common waxbill"]
        assert preference_species == ["Common waxbill"]
        assert representative_species == ["Common waxbill"]
        # The bucket path uses `resolve_species_display_name` to
        # canonicalize the raw prediction; storage and bucket agree now.
        assert db.resolve_species_display_name("Common Waxbill") == "Common waxbill"
        row = db.conn.execute(
            "SELECT old_value FROM edit_history_items LIMIT 1"
        ).fetchone()
        rewritten = json.loads(row["old_value"])["curation"]
        assert rewritten["hl_prev"][0]["species"] == "Common waxbill"
        assert rewritten["pref_prev"][0]["species"] == "Common waxbill"
        assert rewritten["rep_prev"][0]["species"] == "Common waxbill"
        assert db.get_meta("curation_species_case_aligned_v2") == "1"
    finally:
        db.close()


def test_curation_case_alignment_leaves_ambiguous_homonyms_alone(tmp_path):
    """When the match_key has multiple distinct keyword spellings —
    intentionally distinct homonyms — the sweep must not silently pick
    one and rewrite curation across the other. The "no keyword row →
    case-convert" fallback added for prediction-only rows must not
    regress this safety.
    """
    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    db.close()

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    ws_id = conn.execute("SELECT id FROM workspaces LIMIT 1").fetchone()["id"]
    conn.execute(
        "INSERT INTO folders (path, name, status) VALUES ('/p', 'p', 'ok')"
    )
    fid = conn.execute("SELECT id FROM folders WHERE path = '/p'").fetchone()["id"]
    conn.execute(
        "INSERT INTO photos (folder_id, filename) VALUES (?, 'a.jpg')", (fid,)
    )
    pid = conn.execute("SELECT id FROM photos LIMIT 1").fetchone()["id"]
    # Two distinct root species keyword spellings for the same match_key.
    conn.execute(
        "INSERT INTO keywords (name, type, is_species) VALUES ('Robin', 'general', 1)"
    )
    conn.execute(
        "INSERT INTO keywords (name, type, is_species) VALUES ('robin', 'taxonomy', 1)"
    )
    conn.execute(
        "INSERT INTO species_highlights "
        "(workspace_id, species, photo_id, rank) VALUES (?, 'ROBIN', ?, 1)",
        (ws_id, pid),
    )
    conn.execute(
        "DELETE FROM db_meta WHERE key = 'curation_species_case_aligned_v2'"
    )
    conn.commit()
    conn.close()

    db = Database(db_path)
    try:
        species = [
            r["species"] for r in db.conn.execute(
                "SELECT species FROM species_highlights"
            ).fetchall()
        ]
        assert species == ["ROBIN"]
        assert db.get_meta("curation_species_case_aligned_v2") == "1"
    finally:
        db.close()


def test_prime_symbol_not_folded_in_keywords(tmp_path):
    """U+2032 PRIME is the semantic prime symbol used for feet and
    arcminutes. Keywords like `10′ waterfall` must not be silently
    rewritten to `10' waterfall` by the apostrophe fold — the fold table
    intentionally excludes U+2032 so measurement notation survives the
    display/storage normalization applied on every keyword write."""
    from keyword_normalization import normalize_keyword_display

    assert normalize_keyword_display("10′ waterfall") == "10′ waterfall"
    # Middle-of-string preservation matters most, but a lone prime as an
    # edge char is stripped by _EDGE_QUOTES (measurement notation almost
    # never appears at the boundary of a keyword name — the edge behavior
    # is unchanged from before the fold table existed).

    db, _ws_id, p1, _p2 = _make_db(tmp_path)
    try:
        det = db.conn.execute(
            "INSERT INTO detections (photo_id, category, detector_confidence) "
            "VALUES (?, 'animal', 0.99)",
            (p1,),
        ).lastrowid
        db.conn.commit()

        # A prediction whose species carries a legitimate prime stays intact
        # after add_prediction normalizes on write.
        db.add_prediction(
            detection_id=det,
            species="10′ waterfall bird",
            confidence=0.5,
            model="m1",
            labels_fingerprint="fp1",
        )
        stored = db.conn.execute(
            "SELECT species FROM predictions WHERE detection_id = ?", (det,)
        ).fetchone()["species"]
        assert stored == "10′ waterfall bird"
    finally:
        db.close()


def test_prediction_fold_merge_backfills_pending_group_metadata(tmp_path):
    """Two spellings can both have PENDING review rows in the same
    workspace — the loser carrying the current burst's ``group_id`` and
    vote counts, the winner just a bare pending stub. The prior merge
    logic only preserved group metadata when the loser's status was
    accepted/rejected, so a pending loser's grouping was cascaded away
    and the surviving prediction silently fell out of its burst group.
    The merge helper now backfills group metadata from whichever side
    supplies it, independently of the accepted/rejected decision."""
    db, ws_id, p1, _p2 = _make_db(tmp_path)
    try:
        det = _insert_prediction(db, p1, "Say's phoebe", confidence=0.8)
        db.conn.execute(
            "INSERT INTO predictions (detection_id, classifier_model, "
            "labels_fingerprint, species, confidence) "
            "VALUES (?, 'm1', 'fp1', ?, ?)",
            (det, "Say’s phoebe", 0.4),
        )
        winner_pid = db.conn.execute(
            "SELECT id FROM predictions WHERE species = ?", ("Say's phoebe",)
        ).fetchone()["id"]
        loser_pid = db.conn.execute(
            "SELECT id FROM predictions WHERE species = ?", ("Say’s phoebe",)
        ).fetchone()["id"]
        db.conn.execute(
            "INSERT INTO prediction_review "
            "(prediction_id, workspace_id, status) VALUES (?, ?, 'pending')",
            (winner_pid, ws_id),
        )
        db.conn.execute(
            "INSERT INTO prediction_review "
            "(prediction_id, workspace_id, status, group_id, vote_count, "
            " total_votes, individual) "
            "VALUES (?, ?, 'pending', 'burst-1', 2, 3, 'ind-1')",
            (loser_pid, ws_id),
        )
        db.conn.commit()

        db._fold_prediction_species_apostrophes()
        db.conn.commit()

        survivor_pid = db.conn.execute(
            "SELECT id FROM predictions WHERE species = ?", ("Say's phoebe",)
        ).fetchone()["id"]
        review = db.conn.execute(
            "SELECT status, group_id, vote_count, total_votes, individual "
            "FROM prediction_review "
            "WHERE prediction_id = ? AND workspace_id = ?",
            (survivor_pid, ws_id),
        ).fetchone()
        assert review is not None
        assert review["status"] == "pending"
        assert review["group_id"] == "burst-1"
        assert review["vote_count"] == 2
        assert review["total_votes"] == 3
        assert review["individual"] == "ind-1"
    finally:
        db.close()


def test_prediction_fold_merges_taxonomy_metadata_from_loser(tmp_path):
    """When the higher-confidence row happens to be the one written by a
    raw-classifier path that never filled in `scientific_name` or the
    taxonomy hierarchy, the migration must backfill those fields from the
    loser before the CASCADEd DELETE strips them. Otherwise taxonomy
    filters and review displays that rely on `taxonomy_family` etc. show
    the surviving prediction as unclassified. `category` also promotes
    from the schema default `'new'` to the loser's more specific
    `'match'`, since the two rows describe the same (detection, species)
    pair and any classifier enrichment belongs to whichever survives."""
    db, _ws_id, p1, _p2 = _make_db(tmp_path)
    try:
        det = db.conn.execute(
            "INSERT INTO detections (photo_id, category, detector_confidence) "
            "VALUES (?, 'animal', 0.99)",
            (p1,),
        ).lastrowid
        # Winner (higher confidence, clean spelling): no taxonomy fields.
        db.conn.execute(
            "INSERT INTO predictions (detection_id, classifier_model, "
            "labels_fingerprint, species, confidence, category) "
            "VALUES (?, 'm1', 'fp1', ?, ?, 'new')",
            (det, "Say's phoebe", 0.8),
        )
        # Loser (curly, lower confidence): full taxonomy.
        db.conn.execute(
            "INSERT INTO predictions (detection_id, classifier_model, "
            "labels_fingerprint, species, confidence, category, "
            "scientific_name, taxonomy_kingdom, taxonomy_phylum, "
            "taxonomy_class, taxonomy_order, taxonomy_family, "
            "taxonomy_genus) "
            "VALUES (?, 'm1', 'fp1', ?, ?, 'match', ?, 'Animalia', "
            "'Chordata', 'Aves', 'Passeriformes', 'Tyrannidae', 'Sayornis')",
            (det, "Say’s phoebe", 0.4, "Sayornis saya"),
        )
        db.conn.commit()

        db._fold_prediction_species_apostrophes()
        db.conn.commit()

        row = db.conn.execute(
            "SELECT species, category, scientific_name, taxonomy_kingdom, "
            "taxonomy_phylum, taxonomy_class, taxonomy_order, "
            "taxonomy_family, taxonomy_genus, confidence "
            "FROM predictions"
        ).fetchone()
        assert row["species"] == "Say's phoebe"
        assert row["confidence"] == 0.8
        assert row["scientific_name"] == "Sayornis saya"
        assert row["taxonomy_kingdom"] == "Animalia"
        assert row["taxonomy_phylum"] == "Chordata"
        assert row["taxonomy_class"] == "Aves"
        assert row["taxonomy_order"] == "Passeriformes"
        assert row["taxonomy_family"] == "Tyrannidae"
        assert row["taxonomy_genus"] == "Sayornis"
        assert row["category"] == "match"
    finally:
        db.close()


def test_prediction_fold_metadata_merge_does_not_clobber_winner(tmp_path):
    """When the winning row already carries taxonomy fields, the merge
    must not overwrite them with the loser's values. Backfill-only
    keeps a deliberate override in place on the survivor."""
    db, _ws_id, p1, _p2 = _make_db(tmp_path)
    try:
        det = db.conn.execute(
            "INSERT INTO detections (photo_id, category, detector_confidence) "
            "VALUES (?, 'animal', 0.99)",
            (p1,),
        ).lastrowid
        db.conn.execute(
            "INSERT INTO predictions (detection_id, classifier_model, "
            "labels_fingerprint, species, confidence, category, "
            "scientific_name, taxonomy_family) "
            "VALUES (?, 'm1', 'fp1', ?, ?, 'match', 'Sayornis saya', "
            "'Tyrannidae')",
            (det, "Say's phoebe", 0.8),
        )
        db.conn.execute(
            "INSERT INTO predictions (detection_id, classifier_model, "
            "labels_fingerprint, species, confidence, category, "
            "scientific_name, taxonomy_family) "
            "VALUES (?, 'm1', 'fp1', ?, ?, 'new', 'wrong sci name', "
            "'WrongFamily')",
            (det, "Say’s phoebe", 0.4),
        )
        db.conn.commit()

        db._fold_prediction_species_apostrophes()
        db.conn.commit()

        row = db.conn.execute(
            "SELECT scientific_name, taxonomy_family, category FROM predictions"
        ).fetchone()
        assert row["scientific_name"] == "Sayornis saya"
        assert row["taxonomy_family"] == "Tyrannidae"
        assert row["category"] == "match"
    finally:
        db.close()


def test_prediction_fold_keeps_pending_winner_when_loser_is_alternative(tmp_path):
    """A pre-migration setup can leave the clean spelling as the pending
    top-1 (no ``prediction_review`` row at all, since absence == pending)
    and the curly spelling as a per-frame runner-up stored with an explicit
    ``status='alternative'`` row. After the fold collides them onto one
    ``predictions`` row, blindly re-pointing the loser's row at the winner
    would flip the surviving top-1 to ``alternative`` and hide it from the
    pending queue. The merge must treat an absent winner review as implicit
    pending and drop a bare ``alternative`` loser instead of transferring
    it."""
    db, ws_id, p1, _p2 = _make_db(tmp_path)
    try:
        det = _insert_prediction(db, p1, "Say's phoebe", confidence=0.8)
        db.conn.execute(
            "INSERT INTO predictions (detection_id, classifier_model, "
            "labels_fingerprint, species, confidence) "
            "VALUES (?, 'm1', 'fp1', ?, ?)",
            (det, "Say’s phoebe", 0.4),
        )
        loser_pid = db.conn.execute(
            "SELECT id FROM predictions WHERE species = ?", ("Say’s phoebe",)
        ).fetchone()["id"]
        db.conn.execute(
            "INSERT INTO prediction_review "
            "(prediction_id, workspace_id, status) VALUES (?, ?, 'alternative')",
            (loser_pid, ws_id),
        )
        db.conn.commit()

        db._fold_prediction_species_apostrophes()
        db.conn.commit()

        survivor_pid = db.conn.execute(
            "SELECT id FROM predictions WHERE species = ?", ("Say's phoebe",)
        ).fetchone()["id"]
        review = db.conn.execute(
            "SELECT status FROM prediction_review "
            "WHERE prediction_id = ? AND workspace_id = ?",
            (survivor_pid, ws_id),
        ).fetchone()
        assert review is None, (
            "loser's status='alternative' row was moved onto the surviving "
            "pending primary, hiding it from the review queue"
        )
    finally:
        db.close()


def test_prediction_fold_moves_loser_alternative_metadata_when_present(tmp_path):
    """Complement to the previous test: when the loser's alternative row
    still carries burst grouping metadata (an atypical but legal state),
    the merge should preserve that metadata on the winner rather than
    discarding it — the ``continue`` shortcut only applies when the loser
    row has nothing worth keeping beyond ``status='alternative'``.

    Status is downgraded to ``pending`` on the survivor, though: the
    winner had no review row of its own (implicit pending), and
    propagating ``alternative`` would hide the higher-confidence primary
    from the pending queue even though we're keeping its burst
    metadata."""
    db, ws_id, p1, _p2 = _make_db(tmp_path)
    try:
        det = _insert_prediction(db, p1, "Say's phoebe", confidence=0.8)
        db.conn.execute(
            "INSERT INTO predictions (detection_id, classifier_model, "
            "labels_fingerprint, species, confidence) "
            "VALUES (?, 'm1', 'fp1', ?, ?)",
            (det, "Say’s phoebe", 0.4),
        )
        loser_pid = db.conn.execute(
            "SELECT id FROM predictions WHERE species = ?", ("Say’s phoebe",)
        ).fetchone()["id"]
        db.conn.execute(
            "INSERT INTO prediction_review "
            "(prediction_id, workspace_id, status, group_id, vote_count, "
            " total_votes) "
            "VALUES (?, ?, 'alternative', 'burst-9', 4, 5)",
            (loser_pid, ws_id),
        )
        db.conn.commit()

        db._fold_prediction_species_apostrophes()
        db.conn.commit()

        survivor_pid = db.conn.execute(
            "SELECT id FROM predictions WHERE species = ?", ("Say's phoebe",)
        ).fetchone()["id"]
        review = db.conn.execute(
            "SELECT status, group_id, vote_count, total_votes "
            "FROM prediction_review "
            "WHERE prediction_id = ? AND workspace_id = ?",
            (survivor_pid, ws_id),
        ).fetchone()
        assert review is not None
        assert review["status"] == "pending", (
            "surviving pending primary must not inherit the loser's "
            "'alternative' status even when transferring its burst metadata"
        )
        assert review["group_id"] == "burst-9"
        assert review["vote_count"] == 4
        assert review["total_votes"] == 5
    finally:
        db.close()


def test_prediction_fold_drops_sentinel_only_alternative_loser(tmp_path):
    """A losing ``status='alternative'`` row whose only non-null metadata
    is ``individual=AUTO_MATCH_REVIEW_MARKER`` carries no user decision
    and no burst grouping — the sentinel is provenance for auto-accepted
    taxonomy matches, not metadata worth preserving. Transferring it
    would flip the higher-confidence pending winner to ``alternative``
    (and NULL-out the marker on the way) even though nothing survives
    the scrub except the misleading status."""
    from db import AUTO_MATCH_REVIEW_MARKER

    db, ws_id, p1, _p2 = _make_db(tmp_path)
    try:
        det = _insert_prediction(db, p1, "Say's phoebe", confidence=0.8)
        db.conn.execute(
            "INSERT INTO predictions (detection_id, classifier_model, "
            "labels_fingerprint, species, confidence) "
            "VALUES (?, 'm1', 'fp1', ?, ?)",
            (det, "Say’s phoebe", 0.4),
        )
        loser_pid = db.conn.execute(
            "SELECT id FROM predictions WHERE species = ?", ("Say’s phoebe",)
        ).fetchone()["id"]
        db.conn.execute(
            "INSERT INTO prediction_review "
            "(prediction_id, workspace_id, status, individual) "
            "VALUES (?, ?, 'alternative', ?)",
            (loser_pid, ws_id, AUTO_MATCH_REVIEW_MARKER),
        )
        db.conn.commit()

        db._fold_prediction_species_apostrophes()
        db.conn.commit()

        survivor_pid = db.conn.execute(
            "SELECT id FROM predictions WHERE species = ?", ("Say's phoebe",)
        ).fetchone()["id"]
        review = db.conn.execute(
            "SELECT status FROM prediction_review "
            "WHERE prediction_id = ? AND workspace_id = ?",
            (survivor_pid, ws_id),
        ).fetchone()
        assert review is None, (
            "sentinel-only 'alternative' loser was moved onto the "
            "surviving pending primary, hiding it from the review queue"
        )
    finally:
        db.close()


def test_prediction_fold_does_not_stamp_auto_marker_on_manual_review(tmp_path):
    """When two spellings each carry a review row and the chosen (later)
    row is a manual accept with ``individual=NULL``, the merge previously
    copied the losing spelling's ``AUTO_MATCH_REVIEW_MARKER`` into the
    ``individual`` slot as a backfill. That misrepresents provenance:
    ``preserve_manual_review`` and ``reconcile_match_review_state`` both
    treat the marker as "auto-generated", so a subsequent classify run
    could overwrite or delete the user's manual decision. The merge must
    never propagate the auto-match sentinel onto a chosen manual review.
    """
    db, ws_id, p1, _p2 = _make_db(tmp_path)
    try:
        det = _insert_prediction(db, p1, "Say's phoebe", confidence=0.8)
        db.conn.execute(
            "INSERT INTO predictions (detection_id, classifier_model, "
            "labels_fingerprint, species, confidence) "
            "VALUES (?, 'm1', 'fp1', ?, ?)",
            (det, "Say’s phoebe", 0.4),
        )
        winner_pid = db.conn.execute(
            "SELECT id FROM predictions WHERE species = ?", ("Say's phoebe",)
        ).fetchone()["id"]
        loser_pid = db.conn.execute(
            "SELECT id FROM predictions WHERE species = ?", ("Say’s phoebe",)
        ).fetchone()["id"]
        # Winner carries the manual accept (later reviewed_at, individual NULL).
        db.conn.execute(
            "INSERT INTO prediction_review "
            "(prediction_id, workspace_id, status, reviewed_at, individual) "
            "VALUES (?, ?, 'accepted', '2026-01-02 03:04:05', NULL)",
            (winner_pid, ws_id),
        )
        # Loser is an older auto-accept — the sentinel must not migrate onto
        # the manual decision when it wins the tie-break.
        db.conn.execute(
            "INSERT INTO prediction_review "
            "(prediction_id, workspace_id, status, reviewed_at, individual) "
            "VALUES (?, ?, 'accepted', '2025-06-01 00:00:00', ?)",
            (loser_pid, ws_id, AUTO_MATCH_REVIEW_MARKER),
        )
        db.conn.commit()

        db._fold_prediction_species_apostrophes()
        db.conn.commit()

        survivor_pid = db.conn.execute(
            "SELECT id FROM predictions WHERE species = ?", ("Say's phoebe",)
        ).fetchone()["id"]
        review = db.conn.execute(
            "SELECT status, individual FROM prediction_review "
            "WHERE prediction_id = ? AND workspace_id = ?",
            (survivor_pid, ws_id),
        ).fetchone()
        assert review is not None
        assert review["status"] == "accepted"
        assert review["individual"] is None, (
            "auto-match sentinel leaked from loser onto the manual accept — "
            "later reconcile / preserve_manual_review would treat this as "
            "auto-generated"
        )
    finally:
        db.close()


def test_prediction_fold_scrubs_auto_marker_when_transferring_pending_loser(tmp_path):
    """Same anti-provenance concern applies on the "winner has no row"
    path: if the loser is a pending row (some odd historical state) that
    still carries the auto-match sentinel in ``individual``, moving it
    onto the winner would stamp the sentinel onto a row the user has never
    reviewed. Scrub the sentinel to NULL when transferring a non-decided
    row so future automation cannot misinterpret provenance."""
    db, ws_id, p1, _p2 = _make_db(tmp_path)
    try:
        det = _insert_prediction(db, p1, "Say's phoebe", confidence=0.8)
        db.conn.execute(
            "INSERT INTO predictions (detection_id, classifier_model, "
            "labels_fingerprint, species, confidence) "
            "VALUES (?, 'm1', 'fp1', ?, ?)",
            (det, "Say’s phoebe", 0.4),
        )
        loser_pid = db.conn.execute(
            "SELECT id FROM predictions WHERE species = ?", ("Say’s phoebe",)
        ).fetchone()["id"]
        db.conn.execute(
            "INSERT INTO prediction_review "
            "(prediction_id, workspace_id, status, group_id, individual) "
            "VALUES (?, ?, 'pending', 'burst-11', ?)",
            (loser_pid, ws_id, AUTO_MATCH_REVIEW_MARKER),
        )
        db.conn.commit()

        db._fold_prediction_species_apostrophes()
        db.conn.commit()

        survivor_pid = db.conn.execute(
            "SELECT id FROM predictions WHERE species = ?", ("Say's phoebe",)
        ).fetchone()["id"]
        review = db.conn.execute(
            "SELECT status, group_id, individual FROM prediction_review "
            "WHERE prediction_id = ? AND workspace_id = ?",
            (survivor_pid, ws_id),
        ).fetchone()
        assert review is not None
        assert review["group_id"] == "burst-11"
        assert review["individual"] is None, (
            "auto-match sentinel from a non-decided loser leaked onto the "
            "surviving prediction"
        )
    finally:
        db.close()


def test_prediction_fold_preserves_auto_marker_on_decided_transfer(tmp_path):
    """When the winner has no ``prediction_review`` row (implicit pending)
    and the loser IS a genuine auto-accepted taxonomy match — ``status=
    'accepted'`` with ``individual=AUTO_MATCH_REVIEW_MARKER`` — moving the
    row must preserve the marker. ``reconcile_match_review_state`` deletes
    stale auto-accepts by matching the marker; scrubbing it here would
    convert a real auto-accept into an apparent manual decision, stranding
    the acceptance even after the XMP match evidence goes away."""
    db, ws_id, p1, _p2 = _make_db(tmp_path)
    try:
        det = _insert_prediction(db, p1, "Say's phoebe", confidence=0.8)
        db.conn.execute(
            "INSERT INTO predictions (detection_id, classifier_model, "
            "labels_fingerprint, species, confidence, category) "
            "VALUES (?, 'm1', 'fp1', ?, ?, 'match')",
            (det, "Say’s phoebe", 0.4),
        )
        loser_pid = db.conn.execute(
            "SELECT id FROM predictions WHERE species = ?", ("Say’s phoebe",)
        ).fetchone()["id"]
        db.conn.execute(
            "INSERT INTO prediction_review "
            "(prediction_id, workspace_id, status, individual) "
            "VALUES (?, ?, 'accepted', ?)",
            (loser_pid, ws_id, AUTO_MATCH_REVIEW_MARKER),
        )
        db.conn.commit()

        db._fold_prediction_species_apostrophes()
        db.conn.commit()

        survivor_pid = db.conn.execute(
            "SELECT id FROM predictions WHERE species = ?", ("Say's phoebe",)
        ).fetchone()["id"]
        review = db.conn.execute(
            "SELECT status, individual FROM prediction_review "
            "WHERE prediction_id = ? AND workspace_id = ?",
            (survivor_pid, ws_id),
        ).fetchone()
        assert review is not None
        assert review["status"] == "accepted"
        assert review["individual"] == AUTO_MATCH_REVIEW_MARKER, (
            "auto-match sentinel was scrubbed while transferring a genuine "
            "auto-accept; reconcile_match_review_state can no longer "
            "identify (and clean up) this row when the match evidence "
            "goes away"
        )
    finally:
        db.close()


def test_prediction_fold_preserves_individual_json_on_pending_transfer(tmp_path):
    """Complement to the sentinel-scrub test: ``individual`` on a pending
    review row can be the real JSON vote-breakdown that
    ``_store_grouped_predictions`` stores alongside ``group_id`` /
    ``vote_count`` / ``total_votes``, not only the auto-match sentinel.
    Blanket-scrubbing ``individual`` to NULL when transferring a pending
    loser would silently drop the burst's per-frame vote payload so a
    later group-accept could no longer derive the consensus species from
    the individual votes. Only the sentinel gets scrubbed; ordinary
    JSON payloads are preserved verbatim."""
    import json

    db, ws_id, p1, _p2 = _make_db(tmp_path)
    try:
        det = _insert_prediction(db, p1, "Say's phoebe", confidence=0.8)
        db.conn.execute(
            "INSERT INTO predictions (detection_id, classifier_model, "
            "labels_fingerprint, species, confidence) "
            "VALUES (?, 'm1', 'fp1', ?, ?)",
            (det, "Say’s phoebe", 0.4),
        )
        loser_pid = db.conn.execute(
            "SELECT id FROM predictions WHERE species = ?", ("Say’s phoebe",)
        ).fetchone()["id"]
        individual_json = json.dumps(
            [{"species": "Say's phoebe", "confidence": 0.91},
             {"species": "Say's phoebe", "confidence": 0.83}]
        )
        db.conn.execute(
            "INSERT INTO prediction_review "
            "(prediction_id, workspace_id, status, group_id, vote_count, "
            " total_votes, individual) "
            "VALUES (?, ?, 'pending', 'burst-13', 2, 3, ?)",
            (loser_pid, ws_id, individual_json),
        )
        db.conn.commit()

        db._fold_prediction_species_apostrophes()
        db.conn.commit()

        survivor_pid = db.conn.execute(
            "SELECT id FROM predictions WHERE species = ?", ("Say's phoebe",)
        ).fetchone()["id"]
        review = db.conn.execute(
            "SELECT status, group_id, vote_count, total_votes, individual "
            "FROM prediction_review "
            "WHERE prediction_id = ? AND workspace_id = ?",
            (survivor_pid, ws_id),
        ).fetchone()
        assert review is not None
        assert review["group_id"] == "burst-13"
        assert review["vote_count"] == 2
        assert review["total_votes"] == 3
        assert review["individual"] == individual_json, (
            "burst per-frame vote JSON was scrubbed to NULL on transfer; "
            "later group acceptance can no longer derive the consensus "
            "species from the individual votes"
        )
    finally:
        db.close()


def test_prediction_fold_preserves_individual_json_on_alternative_transfer(
    tmp_path,
):
    """Same shape as the pending-transfer test but with the loser row
    carrying ``status='alternative'`` plus burst metadata *and* the JSON
    vote payload. The existing ``moves_loser_alternative_metadata_when_
    present`` test only pins the ``group_id`` / ``vote_count`` /
    ``total_votes`` copy-through and leaves the ``individual`` slot
    untested; without this the JSON silently disappears on transfer,
    breaking the same burst-consensus flow. Only the sentinel gets
    scrubbed."""
    import json

    db, ws_id, p1, _p2 = _make_db(tmp_path)
    try:
        det = _insert_prediction(db, p1, "Say's phoebe", confidence=0.8)
        db.conn.execute(
            "INSERT INTO predictions (detection_id, classifier_model, "
            "labels_fingerprint, species, confidence) "
            "VALUES (?, 'm1', 'fp1', ?, ?)",
            (det, "Say’s phoebe", 0.4),
        )
        loser_pid = db.conn.execute(
            "SELECT id FROM predictions WHERE species = ?", ("Say’s phoebe",)
        ).fetchone()["id"]
        individual_json = json.dumps(
            [{"species": "Say's phoebe", "confidence": 0.72}]
        )
        db.conn.execute(
            "INSERT INTO prediction_review "
            "(prediction_id, workspace_id, status, group_id, vote_count, "
            " total_votes, individual) "
            "VALUES (?, ?, 'alternative', 'burst-14', 3, 4, ?)",
            (loser_pid, ws_id, individual_json),
        )
        db.conn.commit()

        db._fold_prediction_species_apostrophes()
        db.conn.commit()

        survivor_pid = db.conn.execute(
            "SELECT id FROM predictions WHERE species = ?", ("Say's phoebe",)
        ).fetchone()["id"]
        review = db.conn.execute(
            "SELECT status, group_id, vote_count, total_votes, individual "
            "FROM prediction_review "
            "WHERE prediction_id = ? AND workspace_id = ?",
            (survivor_pid, ws_id),
        ).fetchone()
        assert review is not None
        # Alternative status is downgraded to pending on the survivor so
        # the higher-confidence primary stays visible in the queue.
        assert review["status"] == "pending"
        assert review["group_id"] == "burst-14"
        assert review["vote_count"] == 3
        assert review["total_votes"] == 4
        assert review["individual"] == individual_json
    finally:
        db.close()
