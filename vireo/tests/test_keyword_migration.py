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
    _queue_keyword_add / _queue_keyword_remove cancel semantics at
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
