"""Tests for the one-shot keyword normalization migration.

The migration (`Database._normalize_keyword_data_once`, gated by the
`keyword_names_normalized` db_meta marker) brings a database that predates
write-side normalization onto the invariant the runtime relies on: every
stored keyword/species name is in `normalize_keyword_display()` form. Tests
seed legacy state with raw SQL (the write choke points would otherwise
normalize it away) and then invoke the migration directly; the marker
gating itself is covered by the reopen test at the bottom.
"""

import os
import sqlite3
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from db import Database  # noqa: E402


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
