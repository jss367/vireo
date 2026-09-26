"""Behavior pins for the keyword-provenance writers of ``Database``.

These are the ``photo_keywords`` writers that create or converge an
association, plus the flows that call them mid-transaction: ``tag_photo``,
``_merge_keyword_into``, ``link_keyword_to_place``,
``retire_builtin_wildlife_genre``, ``_upsert_one_keyword``,
``_normalize_keyword_data_once`` and ``accept_prediction``.

The tests go through the public ``Database`` façade only, so they hold
whether the SQL lives in ``db.py`` or in a repository. They pin the
``_commit`` seams (who commits, and that a caller-owned transaction stays
open), rollback on failure, the provenance lattice outcome at every
convergence point, that the calls these methods make to each other and to
the rest of the façade (``_merge_keyword_into`` recursion, ``tag_photo``,
``queue_change``, the curation renames, ...) still route through
``Database`` so monkeypatches take effect, and that every helper that is
handed "the database" receives the ``Database`` itself. The structural
tests at the end pin the delegation to ``KeywordProvenanceRepository``.
"""

import ast
import contextlib
import inspect
import json
import sqlite3
import textwrap

import pytest
from db import (
    KEYWORD_SOURCE_ACCEPT,
    KEYWORD_SOURCE_MANUAL,
    KEYWORD_SOURCE_UNKNOWN,
    Database,
)


def _visible(db, sql, params=()):
    """Rows as a second connection sees them (i.e. committed)."""
    with contextlib.closing(sqlite3.connect(db._db_path)) as conn:
        return [tuple(r) for r in conn.execute(sql, params).fetchall()]


def _source(db, photo_id, keyword_id):
    row = db.conn.execute(
        "SELECT source FROM photo_keywords WHERE photo_id = ? AND keyword_id = ?",
        (photo_id, keyword_id),
    ).fetchone()
    return "<absent>" if row is None else row["source"]


def _raw_kw(db, name, parent_id=None, kw_type="general", is_species=0,
            taxon_id=None, place_id=None):
    """Insert a keyword row verbatim (``add_keyword`` would normalize it)."""
    return db.conn.execute(
        "INSERT INTO keywords (name, parent_id, type, is_species, taxon_id, place_id) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (name, parent_id, kw_type, is_species, taxon_id, place_id),
    ).lastrowid


def _raw_tag(db, photo_id, keyword_id, source):
    db.conn.execute(
        "INSERT INTO photo_keywords (photo_id, keyword_id, source) VALUES (?, ?, ?)",
        (photo_id, keyword_id, source),
    )


def _spy(monkeypatch, db, name):
    """Record every call to ``db.<name>`` and forward it to the real method."""
    calls = []
    real = getattr(db, name)

    def spy(*args, **kwargs):
        calls.append((args, kwargs))
        return real(*args, **kwargs)

    monkeypatch.setattr(db, name, spy)
    return calls


@pytest.fixture
def lib(db, tmp_path, monkeypatch):
    import config as cfg

    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    ws = db._active_workspace_id
    root = tmp_path / "photos"
    root.mkdir()
    fid = db.add_folder(str(root), name="photos")
    photos = [db.add_photo(fid, f"p{i}.jpg", ".jpg", 10 + i, float(i)) for i in range(4)]
    db.conn.commit()
    return {"ws": ws, "fid": fid, "root": root, "p": photos}


# -- tag_photo ------------------------------------------------------------------------------


def test_tag_photo_commits_by_default(db, lib):
    p0 = lib["p"][0]
    kid = db.add_keyword("Driftwood")
    db.tag_photo(p0, kid)
    assert not db.conn.in_transaction
    assert _visible(
        db, "SELECT photo_id, keyword_id, source FROM photo_keywords",
    ) == [(p0, kid, "manual")]


def test_tag_photo_without_commit_leaves_transaction_open(db, lib):
    p0 = lib["p"][0]
    kid = db.add_keyword("Driftwood")
    db.tag_photo(p0, kid, source=KEYWORD_SOURCE_UNKNOWN, _commit=False)
    assert db.conn.in_transaction
    assert _visible(db, "SELECT COUNT(*) FROM photo_keywords") == [(0,)]
    db.conn.rollback()
    assert _source(db, p0, kid) == "<absent>"


@pytest.mark.parametrize(
    ("first", "second", "expected"),
    [
        (KEYWORD_SOURCE_UNKNOWN, KEYWORD_SOURCE_UNKNOWN, None),
        (KEYWORD_SOURCE_UNKNOWN, KEYWORD_SOURCE_ACCEPT, "accept"),
        (KEYWORD_SOURCE_ACCEPT, KEYWORD_SOURCE_UNKNOWN, "accept"),
        (KEYWORD_SOURCE_ACCEPT, KEYWORD_SOURCE_MANUAL, "manual"),
        (KEYWORD_SOURCE_MANUAL, KEYWORD_SOURCE_ACCEPT, "manual"),
        (KEYWORD_SOURCE_MANUAL, KEYWORD_SOURCE_UNKNOWN, "manual"),
        # An unrecognized stamp ranks with 'accept'; the incoming side wins
        # a tie, and neither ever outranks 'manual'.
        ("from-the-future", KEYWORD_SOURCE_UNKNOWN, "from-the-future"),
        (KEYWORD_SOURCE_ACCEPT, "from-the-future", "from-the-future"),
        ("from-the-future", KEYWORD_SOURCE_ACCEPT, "accept"),
        ("from-the-future", KEYWORD_SOURCE_MANUAL, "manual"),
        (KEYWORD_SOURCE_MANUAL, "from-the-future", "manual"),
    ],
)
def test_tag_photo_retag_stores_the_lattice_max(db, lib, first, second, expected):
    p0 = lib["p"][0]
    kid = db.add_keyword("Wildlife", kw_type="genre")
    db.tag_photo(p0, kid, source=first)
    db.tag_photo(p0, kid, source=second)
    assert _visible(
        db, "SELECT source FROM photo_keywords WHERE photo_id = ? AND keyword_id = ?",
        (p0, kid),
    ) == [(expected,)]


# -- _merge_keyword_into --------------------------------------------------------------------


def test_merge_keyword_into_leaves_commit_to_caller(db, lib):
    p0 = lib["p"][0]
    src = db.add_keyword("Egret")
    dst = db.add_keyword("Heron")
    db.tag_photo(p0, src)
    assert db._merge_keyword_into(src, dst) == 1
    assert db.conn.in_transaction
    # Nothing is visible to another connection until the caller commits.
    assert _visible(db, "SELECT id FROM keywords WHERE id = ?", (src,)) == [(src,)]
    db.conn.rollback()
    assert _source(db, p0, src) == "manual"
    db._merge_keyword_into(src, dst)
    db.conn.commit()
    assert _visible(db, "SELECT id FROM keywords WHERE id = ?", (src,)) == []
    assert _visible(
        db, "SELECT keyword_id, source FROM photo_keywords WHERE photo_id = ?", (p0,),
    ) == [(dst, "manual")]


@pytest.mark.parametrize(
    ("src_source", "dst_source", "expected"),
    [
        (KEYWORD_SOURCE_MANUAL, "<absent>", "manual"),
        (KEYWORD_SOURCE_UNKNOWN, "<absent>", None),
        (KEYWORD_SOURCE_ACCEPT, "<absent>", "accept"),
        (KEYWORD_SOURCE_MANUAL, KEYWORD_SOURCE_UNKNOWN, "manual"),
        (KEYWORD_SOURCE_UNKNOWN, KEYWORD_SOURCE_MANUAL, "manual"),
        (KEYWORD_SOURCE_ACCEPT, KEYWORD_SOURCE_UNKNOWN, "accept"),
        (KEYWORD_SOURCE_UNKNOWN, KEYWORD_SOURCE_ACCEPT, "accept"),
        (KEYWORD_SOURCE_ACCEPT, KEYWORD_SOURCE_MANUAL, "manual"),
        (KEYWORD_SOURCE_MANUAL, KEYWORD_SOURCE_ACCEPT, "manual"),
        (KEYWORD_SOURCE_UNKNOWN, KEYWORD_SOURCE_UNKNOWN, None),
    ],
)
def test_merge_keyword_into_folds_provenance(db, lib, src_source, dst_source, expected):
    p0 = lib["p"][0]
    src = _raw_kw(db, "Egret")
    dst = _raw_kw(db, "Heron")
    _raw_tag(db, p0, src, src_source)
    if dst_source != "<absent>":
        _raw_tag(db, p0, dst, dst_source)
    db.conn.commit()
    db._merge_keyword_into(src, dst)
    db.conn.commit()
    assert _visible(
        db, "SELECT keyword_id, source FROM photo_keywords WHERE photo_id = ?", (p0,),
    ) == [(dst, expected)]


def test_merge_keyword_into_moves_import_aliases_and_metadata(db, lib):
    src = _raw_kw(db, "Egret", is_species=1)
    dst = _raw_kw(db, "Heron")
    db.conn.execute(
        "UPDATE keywords SET latitude = 1.5, longitude = 2.5, source_taxon_id = 77 "
        "WHERE id = ?", (src,),
    )
    db.conn.execute(
        "INSERT INTO keyword_import_aliases (path_key, path_json, keyword_id) "
        "VALUES ('egret', '[\"Egret\"]', ?)", (src,),
    )
    db._merge_keyword_into(src, dst)
    row = db.conn.execute(
        "SELECT is_species, latitude, longitude, source_taxon_id FROM keywords "
        "WHERE id = ?", (dst,),
    ).fetchone()
    assert tuple(row) == (1, 1.5, 2.5, 77)
    assert db.conn.execute(
        "SELECT keyword_id FROM keyword_import_aliases WHERE path_key = 'egret'"
    ).fetchone()[0] == dst


def test_merge_keyword_into_recurses_through_the_facade(db, lib, monkeypatch):
    p0, p1 = lib["p"][:2]
    src_parent = db.add_keyword("Waders")
    dst_parent = db.add_keyword("Shorebirds")
    src_child = db.add_keyword("Heron", parent_id=src_parent)
    dst_child = db.add_keyword("heron", parent_id=dst_parent)
    db.tag_photo(p0, src_child)
    db.tag_photo(p1, dst_child, source=KEYWORD_SOURCE_UNKNOWN)
    db.tag_photo(p1, src_child)

    calls = _spy(monkeypatch, db, "_merge_keyword_into")
    # The top-level call enters through the class, past the spy; the
    # recursion into the colliding child must come back through the façade.
    merged = Database._merge_keyword_into(
        db, src_parent, dst_parent, pending_source_only=True,
    )
    assert merged == 2
    assert calls == [((src_child, dst_child), {"pending_source_only": True})]
    db.conn.commit()
    assert _visible(db, "SELECT id FROM keywords WHERE id IN (?, ?)",
                    (src_parent, src_child)) == []
    assert sorted(_visible(
        db, "SELECT photo_id, keyword_id, source FROM photo_keywords",
    )) == [(p0, dst_child, "manual"), (p1, dst_child, "manual")]


def test_merge_keyword_into_disambiguates_cross_type_children_through_the_facade(
    db, lib, monkeypatch,
):
    src_parent = db.add_keyword("Waders")
    dst_parent = db.add_keyword("Shorebirds")
    child = db.add_keyword("Macro", parent_id=src_parent)
    peer = db.add_keyword("Macro", parent_id=dst_parent, kw_type="individual")
    calls = _spy(monkeypatch, db, "_reparent_disambiguated")
    assert db._merge_keyword_into(src_parent, dst_parent) == 1
    assert len(calls) == 1
    args, kwargs = calls[0]
    assert (args[0]["id"], args[1], args[2]) == (child, dst_parent, f"Macro (id-{child})")
    assert kwargs == {}
    rows = db.conn.execute(
        "SELECT id, name, parent_id FROM keywords WHERE id IN (?, ?) ORDER BY id",
        (child, peer),
    ).fetchall()
    assert [tuple(r) for r in rows] == [
        (child, f"Macro (id-{child})", dst_parent),
        (peer, "Macro", dst_parent),
    ]


def test_merge_keyword_into_keeps_distinct_taxa_apart_and_hands_over_the_database(
    db, lib, monkeypatch,
):
    import keyword_identity

    db.conn.execute(
        "INSERT INTO taxa (id, inat_id, name, rank) VALUES "
        "(1, 101, 'Turdus migratorius', 'species'), "
        "(2, 202, 'Erithacus rubecula', 'species')"
    )
    src_parent = db.add_keyword("Waders")
    dst_parent = db.add_keyword("Shorebirds")
    child = _raw_kw(db, "Robin", src_parent, "taxonomy", 1, taxon_id=1)
    peer = _raw_kw(db, "Robin", dst_parent, "taxonomy", 1, taxon_id=2)
    seen = []
    real = keyword_identity.taxon_identity

    def spy(database, row):
        seen.append(database)
        return real(database, row)

    monkeypatch.setattr(keyword_identity, "taxon_identity", spy)
    assert db._merge_keyword_into(src_parent, dst_parent) == 1
    assert seen and all(d is db for d in seen)
    names = dict(db.conn.execute(
        "SELECT id, name FROM keywords WHERE id IN (?, ?)", (child, peer),
    ).fetchall())
    assert names == {child: f"Robin (id-{child})", peer: "Robin"}


def test_merge_keyword_into_retargets_pending_and_curation_through_the_facade(
    db, lib, monkeypatch,
):
    p0, p1 = lib["p"][:2]
    ws = lib["ws"]
    src = db.add_keyword("Egret", is_species=True)
    dst = db.add_keyword("Heron", is_species=True)
    db.tag_photo(p0, src)
    db.tag_photo(p1, dst)
    db.queue_change(p0, "keyword_add", "Egret", workspace_id=ws)
    db.queue_change(p1, "keyword_remove", "Egret", workspace_id=ws)
    highlights = _spy(monkeypatch, db, "rename_species_highlights_species")
    prefs = _spy(monkeypatch, db, "rename_photo_preferences_species")

    db._merge_keyword_into(src, dst, pending_source_only=True)

    pairs = sorted([(p0, ws), (p1, ws)])
    for calls in (highlights, prefs):
        assert len(calls) == 1
        args, kwargs = calls[0]
        assert args == ("Egret", "Heron")
        assert sorted(kwargs.pop("photo_workspace_pairs")) == pairs
        assert kwargs == {"_commit": False}
    rows = sorted(tuple(r) for r in db.conn.execute(
        "SELECT photo_id, change_type, value FROM pending_changes"
    ).fetchall())
    # p1 never carried the source, so its removal of the old spelling stays.
    assert rows == sorted([(p0, "keyword_add", "Heron"), (p1, "keyword_remove", "Egret")])


def test_merge_keyword_into_retargets_edit_history(db, lib):
    p0 = lib["p"][0]
    src = db.add_keyword("Egret")
    dst = db.add_keyword("Heron")
    db.tag_photo(p0, src)
    db.record_edit(
        "keyword_add", "add Egret", str(src),
        [{"photo_id": p0, "old_value": "", "new_value": str(src)}],
    )
    db._merge_keyword_into(src, dst)
    assert db.conn.execute(
        "SELECT new_value FROM edit_history WHERE action_type = 'keyword_add'"
    ).fetchone()[0] == str(dst)
    assert db.conn.execute(
        "SELECT new_value FROM edit_history_items WHERE photo_id = ?", (p0,),
    ).fetchone()[0] == str(dst)


# -- _upsert_one_keyword --------------------------------------------------------------------


def test_upsert_one_keyword_leaves_commit_to_caller(db, lib):
    kid = db._upsert_one_keyword("Paris", None)
    assert db.conn.in_transaction
    assert _visible(db, "SELECT COUNT(*) FROM keywords WHERE id = ?", (kid,)) == [(0,)]
    assert db._upsert_one_keyword("paris", None) == kid
    db.conn.commit()
    assert _visible(db, "SELECT name, type FROM keywords WHERE id = ?", (kid,)) == [
        ("Paris", "location"),
    ]
    with pytest.raises(ValueError, match="empty after normalization"):
        db._upsert_one_keyword("  ", None)


def test_upsert_one_keyword_place_id_upsert_and_disambiguation(db, lib):
    state = db._upsert_one_keyword("California", None)
    park = db._upsert_one_keyword("Park", state, place_id="place-00000001",
                                  latitude=1.0, longitude=2.0)
    # Re-picking the same Google place updates the row in place.
    assert db._upsert_one_keyword("Park", state, place_id="place-00000001",
                                  latitude=3.0, longitude=4.0) == park
    # A different place with the same (name, parent) gets a suffixed name.
    other = db._upsert_one_keyword("Park", state, place_id="place-00000002")
    names = dict(db.conn.execute(
        "SELECT id, name FROM keywords WHERE id IN (?, ?)", (park, other),
    ).fetchall())
    assert names == {park: "Park", other: "Park (00000002)"}
    assert tuple(db.conn.execute(
        "SELECT latitude, longitude FROM keywords WHERE id = ?", (park,),
    ).fetchone()) == (3.0, 4.0)


def test_upsert_one_keyword_merges_suffixed_place_row_through_the_facade(
    db, lib, monkeypatch,
):
    p0, p1 = lib["p"][:2]
    component = _raw_kw(db, "California", kw_type="location")
    suffixed = _raw_kw(db, "California (00000042)", kw_type="location",
                       place_id="place-00000042")
    _raw_tag(db, p0, suffixed, KEYWORD_SOURCE_MANUAL)
    _raw_tag(db, p1, suffixed, KEYWORD_SOURCE_UNKNOWN)
    _raw_tag(db, p1, component, KEYWORD_SOURCE_ACCEPT)
    db.conn.commit()
    calls = _spy(monkeypatch, db, "_merge_keyword_into")

    got = db._upsert_one_keyword(
        "California", None, place_id="place-00000042", latitude=5.0,
        longitude=6.0, reuse_location_component=True,
    )
    assert got == component
    assert calls == [((suffixed, component), {})]
    assert db.conn.in_transaction
    db.conn.commit()
    assert _visible(
        db, "SELECT place_id, latitude, longitude FROM keywords WHERE id = ?",
        (component,),
    ) == [("place-00000042", 5.0, 6.0)]
    assert sorted(_visible(
        db, "SELECT photo_id, keyword_id, source FROM photo_keywords",
    )) == [(p0, component, "manual"), (p1, component, "accept")]


def test_upsert_one_keyword_rejects_cross_type_collision(db, lib):
    france = db._upsert_one_keyword("France", None)
    general = _raw_kw(db, "Paris", france)
    with pytest.raises(RuntimeError, match="can't reuse for location chain"):
        db._upsert_one_keyword("Paris", france)
    assert db.conn.execute(
        "SELECT type FROM keywords WHERE id = ?", (general,),
    ).fetchone()[0] == "general"


# -- link_keyword_to_place ------------------------------------------------------------------


_COMPONENTS = [{"name": "California", "types": ["administrative_area_level_1"]}]


def test_link_keyword_to_place_commits_and_builds_chain_through_the_facade(
    db, lib, monkeypatch,
):
    kid = db.get_or_create_text_location("Home")
    chain_calls = _spy(monkeypatch, db, "_upsert_location_parent_chain")
    result = db.link_keyword_to_place(kid, {
        "place_id": "place-home", "name": "Home Park", "lat": 1.0, "lng": 2.0,
        "address_components": _COMPONENTS, "types": ["park"],
    })
    assert result == {"keyword_id": kid, "merged": False}
    assert not db.conn.in_transaction
    assert len(chain_calls) == 1
    args, kwargs = chain_calls[0]
    assert args == (_COMPONENTS,)
    assert kwargs == {"leaf_name": "Home Park", "leaf_types": ["park"]}
    state = _visible(db, "SELECT id FROM keywords WHERE name = 'California'")
    assert _visible(
        db, "SELECT name, place_id, latitude, longitude, parent_id FROM keywords "
        "WHERE id = ?", (kid,),
    ) == [("Home Park", "place-home", 1.0, 2.0, state[0][0])]


def test_link_keyword_to_place_merges_into_canonical_and_folds_provenance(db, lib):
    p0, p1, p2 = lib["p"][:3]
    canonical = db.upsert_place_chain({
        "place_id": "place-home", "name": "Home Park", "lat": 1.0, "lng": 2.0,
        "address_components": [],
    })
    kid = db.get_or_create_text_location("Home")
    child = db._upsert_one_keyword("Garden", kid)
    db.conn.commit()
    db.tag_photo(p0, kid)  # manual
    db.tag_photo(p0, canonical, source=KEYWORD_SOURCE_UNKNOWN)
    db.tag_photo(p1, kid, source=KEYWORD_SOURCE_UNKNOWN)
    db.tag_photo(p2, kid, source=KEYWORD_SOURCE_ACCEPT)
    db.tag_photo(p2, canonical)  # manual

    result = db.link_keyword_to_place(kid, {
        "place_id": "place-home", "name": "Home Park", "address_components": [],
    })
    assert result == {"keyword_id": canonical, "merged": True}
    assert not db.conn.in_transaction
    assert _visible(db, "SELECT id FROM keywords WHERE id = ?", (kid,)) == []
    assert _visible(db, "SELECT parent_id FROM keywords WHERE id = ?", (child,)) == [
        (canonical,),
    ]
    assert sorted(_visible(
        db, "SELECT photo_id, keyword_id, source FROM photo_keywords",
    )) == [(p0, canonical, "manual"), (p1, canonical, None), (p2, canonical, "manual")]


def test_link_keyword_to_place_rolls_back_on_failure(db, lib, monkeypatch):
    kid = db.get_or_create_text_location("Home")
    db.conn.commit()

    def boom(*args, **kwargs):
        db.conn.execute(
            "INSERT INTO keywords (name, type) VALUES ('Stray', 'location')"
        )
        raise RuntimeError("chain failed")

    monkeypatch.setattr(db, "_upsert_location_parent_chain", boom)
    with pytest.raises(RuntimeError, match="chain failed"):
        db.link_keyword_to_place(kid, {"place_id": "place-home", "name": "Home"})
    assert not db.conn.in_transaction
    assert db.conn.execute(
        "SELECT COUNT(*) FROM keywords WHERE name = 'Stray'"
    ).fetchone()[0] == 0
    assert db.conn.execute(
        "SELECT place_id FROM keywords WHERE id = ?", (kid,),
    ).fetchone()[0] is None


def test_link_keyword_to_place_validates_target(db, lib):
    with pytest.raises(ValueError, match="requires details"):
        db.link_keyword_to_place(1, {})
    with pytest.raises(ValueError, match="does not exist"):
        db.link_keyword_to_place(999_999, {"place_id": "x"})
    general = db.add_keyword("Driftwood")
    with pytest.raises(ValueError, match="not 'location'"):
        db.link_keyword_to_place(general, {"place_id": "x"})


def test_link_keyword_to_place_returns_keyword_reused_in_its_own_chain(db, lib):
    kid = db.get_or_create_text_location("California")
    db.conn.commit()
    result = db.link_keyword_to_place(kid, {
        "place_id": "place-park", "name": "Park",
        "address_components": _COMPONENTS,
    })
    assert result == {"keyword_id": kid, "merged": False}
    assert db.conn.execute(
        "SELECT place_id FROM keywords WHERE id = ?", (kid,),
    ).fetchone()[0] is None


# -- _normalize_keyword_data_once -----------------------------------------------------------


def test_normalize_keyword_data_once_merges_through_the_facade_and_leaves_commit(
    db, lib, monkeypatch,
):
    p0, p1, p2 = lib["p"][:3]
    ws = lib["ws"]
    clean = _raw_kw(db, "Apapane")
    variant = _raw_kw(db, "‘Apapane")
    stray = _raw_kw(db, "'")
    orphan = _raw_kw(db, "Orphan", stray)
    _raw_tag(db, p0, clean, KEYWORD_SOURCE_UNKNOWN)
    _raw_tag(db, p0, variant, KEYWORD_SOURCE_MANUAL)
    _raw_tag(db, p1, variant, KEYWORD_SOURCE_ACCEPT)
    _raw_tag(db, p2, stray, KEYWORD_SOURCE_MANUAL)
    db.conn.execute(
        "INSERT INTO pending_changes (photo_id, change_type, value, workspace_id) "
        "VALUES (?, 'keyword_add', ?, ?)", (p2, "‘Kiwi", ws),
    )
    db.conn.commit()
    merges = _spy(monkeypatch, db, "_merge_keyword_into")

    db._normalize_keyword_data_once()

    assert merges == [((variant, clean), {"pending_source_only": True})]
    assert db.conn.in_transaction
    assert _visible(db, "SELECT COUNT(*) FROM keywords WHERE id = ?", (stray,)) == [(1,)]
    db.conn.commit()
    assert _visible(db, "SELECT id FROM keywords WHERE id IN (?, ?)", (variant, stray)) == []
    assert _visible(db, "SELECT parent_id FROM keywords WHERE id = ?", (orphan,)) == [
        (None,),
    ]
    assert sorted(_visible(
        db, "SELECT photo_id, keyword_id, source FROM photo_keywords",
    )) == [(p0, clean, "manual"), (p1, clean, "accept")]
    assert _visible(db, "SELECT value FROM pending_changes WHERE photo_id = ?", (p2,)) == [
        ("Kiwi",),
    ]


def test_normalize_keyword_data_once_routes_curation_through_the_facade(
    db, lib, monkeypatch,
):
    p0 = lib["p"][0]
    ws = lib["ws"]
    db.conn.execute(
        "INSERT INTO photo_preferences (workspace_id, purpose, species, photo_id) "
        "VALUES (?, 'representative', ?, ?)", (ws, "‘Apapane", p0),
    )
    db.conn.execute(
        "INSERT INTO species_highlights (workspace_id, species, photo_id, rank) "
        "VALUES (?, ?, ?, 1)", (ws, "‘Apapane", p0),
    )
    db.conn.commit()
    prefs = _spy(monkeypatch, db, "rename_photo_preferences_species")
    highlights = _spy(monkeypatch, db, "rename_species_highlights_species")
    align = _spy(monkeypatch, db, "_align_curation_species_case")
    history = _spy(monkeypatch, db, "_align_curation_history_species")

    db._normalize_keyword_data_once()

    expected = [(("‘Apapane", "Apapane"), {"_commit": False})]
    assert prefs == expected
    assert highlights == expected
    assert align == [((), {})]
    assert history == [((), {})]
    assert db.conn.execute(
        "SELECT species FROM photo_preferences"
    ).fetchall()[0][0] == "Apapane"


def test_normalize_keyword_data_once_renames_variants_through_the_facade(
    db, lib, monkeypatch,
):
    birds = _raw_kw(db, "Birds")
    variant = _raw_kw(db, "“Heron”", birds)
    db.conn.commit()
    calls = _spy(monkeypatch, db, "_normalize_keyword_row_name")
    db._normalize_keyword_data_once()
    assert calls == [((variant,), {"disambiguate_on_conflict": True})]
    assert db.conn.execute(
        "SELECT name FROM keywords WHERE id = ?", (variant,),
    ).fetchone()[0] == "Heron"


# -- retire_builtin_wildlife_genre ----------------------------------------------------------


def _wildlife(db, lib, *, photo=None, source="generated"):
    """A generated Wildlife genre on a photo that also carries a species."""
    pid = photo if photo is not None else lib["p"][0]
    wildlife = db.conn.execute(
        "SELECT id FROM keywords WHERE name = 'Wildlife' AND type = 'genre' "
        "AND parent_id IS NULL"
    ).fetchone()
    wildlife = wildlife[0] if wildlife else _raw_kw(db, "Wildlife", kw_type="genre")
    species = db.add_keyword("House Sparrow", is_species=True)
    db.tag_photo(pid, species)
    _raw_tag(db, pid, wildlife, source)
    db.conn.commit()
    db.set_meta(Database._RETIRED_WILDLIFE_GENRE_KEY, "0")
    return wildlife


def test_retire_wildlife_marker_short_circuits_through_the_facade(db, lib, monkeypatch):
    wildlife = _wildlife(db, lib)
    reads = []
    real = db.get_meta
    monkeypatch.setattr(db, "get_meta", lambda key: reads.append(key) or "1")
    assert db.retire_builtin_wildlife_genre() == 0
    assert reads == [Database._RETIRED_WILDLIFE_GENRE_KEY]
    assert _source(db, lib["p"][0], wildlife) == "generated"
    monkeypatch.setattr(db, "get_meta", real)
    assert db.retire_builtin_wildlife_genre(force=True) == 1
    assert _source(db, lib["p"][0], wildlife) == "<absent>"


def test_retire_wildlife_without_rows_sets_marker_through_the_facade(db, lib, monkeypatch):
    calls = _spy(monkeypatch, db, "set_meta")
    assert db.retire_builtin_wildlife_genre(force=True) == 0
    assert calls == [((Database._RETIRED_WILDLIFE_GENRE_KEY, "1"), {})]
    assert not db.conn.in_transaction
    assert _visible(
        db, "SELECT value FROM db_meta WHERE key = ?",
        (Database._RETIRED_WILDLIFE_GENRE_KEY,),
    ) == [("1",)]


def test_retire_wildlife_commits_and_queues_through_the_facade(db, lib, monkeypatch):
    p0 = lib["p"][0]
    wildlife = _wildlife(db, lib)
    queued = _spy(monkeypatch, db, "queue_change")
    marks = _spy(monkeypatch, db, "set_meta")
    assert db.retire_builtin_wildlife_genre() == 1
    assert not db.conn.in_transaction
    assert queued == [(
        (p0, "keyword_remove_flat", "Wildlife"),
        {"workspace_id": lib["ws"], "_commit": False},
    )]
    assert marks == [((Database._RETIRED_WILDLIFE_GENRE_KEY, "1"), {"_commit": False})]
    assert _visible(
        db, "SELECT COUNT(*) FROM photo_keywords WHERE keyword_id = ?", (wildlife,),
    ) == [(0,)]
    assert _visible(
        db, "SELECT change_type, value FROM pending_changes WHERE photo_id = ?", (p0,),
    ) == [("keyword_remove_flat", "Wildlife")]


@pytest.mark.parametrize("active", [True, False])
def test_retire_wildlife_unlinked_photo_queues_to_fallback_workspace(
    db, lib, tmp_path, monkeypatch, active,
):
    other = tmp_path / "unlinked"
    other.mkdir()
    fid = db.add_folder(str(other), name="unlinked", link_to_workspace=False)
    pid = db.add_photo(fid, "u.jpg", ".jpg", 1, 1.0)
    _wildlife(db, lib, photo=pid)
    second = db.create_workspace("Second")
    db.set_active_workspace(second)
    first_ws = db.conn.execute("SELECT MIN(id) FROM workspaces").fetchone()[0]
    expected = second if active else first_ws
    if not active:
        db._active_workspace_id = None
    queued = _spy(monkeypatch, db, "queue_change")
    assert db.retire_builtin_wildlife_genre() == 1
    assert [c[1]["workspace_id"] for c in queued] == [expected]


def test_retire_wildlife_rolls_back_chunk_but_keeps_latch(db, lib, monkeypatch):
    p0, p1 = lib["p"][:2]
    wildlife = _wildlife(db, lib, photo=p0)
    _wildlife(db, lib, photo=p1)
    # p1's association has authorship evidence (an unsynced add), so the
    # latch stamps it manual and commits before any retirement is tried.
    db.queue_change(p1, "keyword_add", "Wildlife", workspace_id=lib["ws"])

    def boom(*args, **kwargs):
        db.conn.execute("INSERT INTO db_meta (key, value) VALUES ('probe', '1')")
        raise RuntimeError("queue failed")

    monkeypatch.setattr(db, "queue_change", boom)
    with pytest.raises(RuntimeError, match="queue failed"):
        db.retire_builtin_wildlife_genre()
    assert not db.conn.in_transaction
    assert _visible(db, "SELECT value FROM db_meta WHERE key = 'probe'") == []
    assert _visible(
        db, "SELECT photo_id, source FROM photo_keywords WHERE keyword_id = ? "
        "ORDER BY photo_id", (wildlife,),
    ) == [(p0, "generated"), (p1, "manual")]
    assert db.get_meta(Database._RETIRED_WILDLIFE_GENRE_KEY) == "0"


def test_retire_wildlife_latches_legacy_association_without_sidecar(db, lib):
    p0 = lib["p"][0]
    wildlife = _wildlife(db, lib, source=None)
    assert db.retire_builtin_wildlife_genre() == 0
    assert not db.conn.in_transaction
    assert _visible(
        db, "SELECT source FROM photo_keywords WHERE photo_id = ? AND keyword_id = ?",
        (p0, wildlife),
    ) == [("manual",)]


def test_retire_wildlife_logs_corrupt_sidecar_through_db_logger(db, lib, caplog):
    p0 = lib["p"][0]
    wildlife = _wildlife(db, lib)
    (lib["root"] / "p0.xmp").write_text("<not-xml")
    with caplog.at_level("WARNING", logger="db"):
        assert db.retire_builtin_wildlife_genre() == 0
    assert any(
        r.name == "db" and "Corrupt sidecar" in r.getMessage() for r in caplog.records
    )
    assert _source(db, p0, wildlife) == "generated"
    assert db.get_meta(Database._RETIRED_WILDLIFE_GENRE_KEY) == "0"


# -- accept_prediction ----------------------------------------------------------------------


def _det(db, pid, x=0.1):
    return db.save_detections(
        pid,
        [{"box": {"x": x, "y": 0.1, "w": 0.5, "h": 0.5}, "confidence": 0.9,
          "category": "animal"}],
        "md",
    )[0]


def _pred(db, det, species, model="m1"):
    db.add_prediction(det, species, 0.9, model)
    return db.conn.execute(
        "SELECT id FROM predictions WHERE detection_id = ? AND species = ? "
        "AND classifier_model = ?",
        (det, species, model),
    ).fetchone()[0]


def test_accept_prediction_tags_manual_and_queues_add_through_the_facade(
    db, lib, monkeypatch,
):
    p0 = lib["p"][0]
    pid = _pred(db, _det(db, p0), "Robin")
    db.conn.commit()
    tags = _spy(monkeypatch, db, "tag_photo")
    queued = _spy(monkeypatch, db, "queue_change")
    statuses = _spy(monkeypatch, db, "update_prediction_status")
    added = _spy(monkeypatch, db, "add_keyword")

    result = db.accept_prediction(pid)

    kid = result["keyword_id"]
    assert added == [(("Robin",), {"is_species": True, "_commit": False})]
    assert tags == [((p0, kid), {"source": "manual", "_commit": False})]
    assert queued == [((p0, "keyword_add", "Robin"), {"_commit": False})]
    assert statuses == [((pid, "accepted"), {"_commit": False})]
    assert result["accepted_prediction_ids"] == [pid]
    assert result["affected"] == [{
        "photo_id": p0, "prediction_id": pid, "old_species": [], "changed_tag": True,
    }]
    assert not db.conn.in_transaction
    assert _visible(db, "SELECT photo_id, keyword_id, source FROM photo_keywords") == [
        (p0, kid, "manual"),
    ]
    assert _visible(db, "SELECT change_type, value FROM pending_changes") == [
        ("keyword_add", "Robin"),
    ]
    assert _visible(
        db, "SELECT status FROM prediction_review WHERE prediction_id = ?", (pid,),
    ) == [("accepted",)]


def test_accept_prediction_without_commit_leaves_transaction_open(db, lib):
    p0 = lib["p"][0]
    pid = _pred(db, _det(db, p0), "Robin")
    db.conn.commit()
    db.accept_prediction(pid, _commit=False)
    assert db.conn.in_transaction
    assert _visible(db, "SELECT COUNT(*) FROM pending_changes") == [(0,)]
    db.conn.rollback()
    assert db.conn.execute("SELECT COUNT(*) FROM photo_keywords").fetchone()[0] == 0


def test_accept_prediction_rolls_back_when_queueing_fails(db, lib, monkeypatch):
    p0 = lib["p"][0]
    pid = _pred(db, _det(db, p0), "Robin")
    db.conn.commit()

    def boom(*args, **kwargs):
        raise RuntimeError("queue failed")

    monkeypatch.setattr(db, "queue_change", boom)
    with pytest.raises(RuntimeError, match="queue failed"):
        db.accept_prediction(pid)
    assert not db.conn.in_transaction
    assert _visible(db, "SELECT COUNT(*) FROM photo_keywords") == [(0,)]
    assert _visible(db, "SELECT COUNT(*) FROM prediction_review WHERE status = 'accepted'") == [
        (0,),
    ]
    # With _commit=False the caller owns the rollback.
    with pytest.raises(RuntimeError, match="queue failed"):
        db.accept_prediction(pid, _commit=False)
    assert db.conn.in_transaction
    db.conn.rollback()


def test_accept_prediction_already_tagged_is_status_only(db, lib, monkeypatch):
    p0 = lib["p"][0]
    kid = db.add_keyword("Robin", is_species=True)
    db.tag_photo(p0, kid, source=KEYWORD_SOURCE_UNKNOWN)
    pid = _pred(db, _det(db, p0), "Robin")
    db.conn.commit()
    tags = _spy(monkeypatch, db, "tag_photo")
    queued = _spy(monkeypatch, db, "queue_change")
    result = db.accept_prediction(pid)
    assert tags == [] and queued == []
    assert result["affected"] == [{
        "photo_id": p0, "prediction_id": pid, "old_species": [], "changed_tag": False,
    }]
    assert _source(db, p0, kid) is None


def test_accept_prediction_replace_species_queues_removals_through_the_facade(
    db, lib, monkeypatch,
):
    import taxonomy

    monkeypatch.setattr(taxonomy, "load_local_taxonomy", lambda *a, **k: None)
    p0, p1 = lib["p"][:2]
    ws = lib["ws"]
    jay = db.add_keyword("Jay", is_species=True)
    wren = db.add_keyword("Wren", is_species=True)
    db.tag_photo(p0, jay)
    db.tag_photo(p1, wren)
    db.queue_change(p1, "keyword_add", "Wren", workspace_id=ws)
    db.conn.execute(
        "INSERT INTO species_highlights (workspace_id, species, photo_id, rank) "
        "VALUES (?, 'Jay', ?, 1)", (ws, p0),
    )
    pred0 = _pred(db, _det(db, p0), "Robin")
    pred1 = _pred(db, _det(db, p1), "Robin")
    db.conn.commit()
    cancels = _spy(monkeypatch, db, "remove_pending_changes")
    queued = _spy(monkeypatch, db, "queue_change")
    highlights = _spy(monkeypatch, db, "rename_species_highlights_species")
    prefs = _spy(monkeypatch, db, "rename_photo_preferences_species")
    configs = _spy(monkeypatch, db, "get_effective_config")

    r0 = db.accept_prediction(pred0, replace_species=True)
    r1 = db.accept_prediction(pred1, replace_species=True)

    assert r0["affected"][0]["old_species"] == ["Jay"]
    assert r1["affected"][0]["old_species"] == ["Wren"]
    assert len(configs) == 2
    assert cancels == [
        ((p0, "keyword_add", "Jay"), {"_commit": False}),
        ((p1, "keyword_add", "Wren"), {"_commit": False}),
    ]
    # p0 had no pending add to cancel, so its removal is queued; p1's
    # still-pending add cancels out instead.
    assert queued == [
        ((p0, "keyword_remove", "Jay"), {"_commit": False}),
        ((p0, "keyword_add", "Robin"), {"_commit": False}),
        ((p1, "keyword_add", "Robin"), {"_commit": False}),
    ]
    assert highlights[0] == (("Jay", "Robin", [(p0, ws)]), {"_commit": False})
    assert prefs[0] == (("Jay", "Robin", [(p0, ws)]), {"_commit": False})
    assert _visible(
        db, "SELECT species FROM species_highlights WHERE photo_id = ?", (p0,),
    ) == [("Robin",)]
    assert sorted(_visible(
        db, "SELECT photo_id, change_type, value FROM pending_changes",
    )) == sorted([
        (p0, "keyword_remove", "Jay"),
        (p0, "keyword_add", "Robin"),
        (p1, "keyword_add", "Robin"),
    ])


def test_accept_prediction_hands_the_database_to_species_resolver(db, lib, monkeypatch):
    import species_identity

    seen = []
    real = species_identity.SpeciesResolver

    class Spy(real):
        def __init__(self, taxonomy=None, db=None):
            seen.append(db)
            super().__init__(taxonomy=taxonomy, db=db)

    monkeypatch.setattr(species_identity, "SpeciesResolver", Spy)
    pid = _pred(db, _det(db, lib["p"][0]), "Robin")
    db.conn.commit()
    db.accept_prediction(pid)
    assert seen == [db]


def test_accept_prediction_requires_an_active_workspace(db, lib):
    pid = _pred(db, _det(db, lib["p"][0]), "Robin")
    db.conn.commit()
    db._active_workspace_id = None
    with pytest.raises(RuntimeError, match="No active workspace"):
        db.accept_prediction(pid)


def test_accept_prediction_out_of_scope_is_a_no_op(db, lib):
    p0, p1 = lib["p"][:2]
    pid = _pred(db, _det(db, p0), "Robin")
    db.conn.commit()
    result = db.accept_prediction(pid, photo_ids=[p1])
    assert result["accepted_prediction_ids"] == []
    assert result["keyword_id"] is None
    assert result["species"] == "Robin"
    assert db.conn.execute("SELECT COUNT(*) FROM keywords WHERE name = 'Robin'").fetchone()[0] == 0
    assert db.accept_prediction(999_999) is None


def test_accept_prediction_grouped_skips_decided_siblings(db, lib):
    p0, p1, p2 = lib["p"][:3]
    ws = lib["ws"]
    dets = [_det(db, p) for p in (p0, p1, p2)]
    preds = [_pred(db, det, "Robin") for det in dets]
    for det in dets:
        db.update_prediction_group_info(
            det, "m1", "g1", 3, 3, json.dumps({"Robin": 3}),
            labels_fingerprint="legacy",
        )
    db.update_prediction_status(preds[2], "rejected")
    db.conn.commit()
    result = db.accept_prediction(preds[0])
    assert sorted(result["accepted_prediction_ids"]) == sorted(preds[:2])
    assert sorted(result["photo_ids"]) == sorted([p0, p1])
    assert sorted(_visible(
        db, "SELECT prediction_id, status FROM prediction_review WHERE workspace_id = ?",
        (ws,),
    )) == sorted([(preds[0], "accepted"), (preds[1], "accepted"), (preds[2], "rejected")])


# -- structure ----------------------------------------------------------------------------


_DELEGATING_PROVENANCE_METHODS = (
    "tag_photo",
    "_merge_keyword_into",
    "link_keyword_to_place",
    "retire_builtin_wildlife_genre",
    "_upsert_one_keyword",
    "_normalize_keyword_data_once",
    "accept_prediction",
)


def _self_attrs(fn):
    source = textwrap.dedent(inspect.getsource(fn))
    node = ast.parse(source).body[0]
    return {
        n.attr
        for n in ast.walk(node)
        if isinstance(n, ast.Attribute)
        and isinstance(n.value, ast.Name)
        and n.value.id == "self"
    }


def _module_tree():
    import repositories.keyword_provenance as module

    return module, ast.parse(inspect.getsource(module))


def _repository_methods(tree):
    cls = next(
        n for n in tree.body
        if isinstance(n, ast.ClassDef) and n.name == "KeywordProvenanceRepository"
    )
    return {n.name: n for n in cls.body if isinstance(n, ast.FunctionDef)}


@pytest.mark.parametrize("name", _DELEGATING_PROVENANCE_METHODS)
def test_provenance_method_delegates_to_repository(name):
    attrs = _self_attrs(getattr(Database, name))
    assert "conn" not in attrs, (
        f"Database.{name} touches self.conn; move the SQL to "
        "KeywordProvenanceRepository"
    )
    assert "_keyword_provenance_repository" in attrs, (
        f"Database.{name} no longer delegates to KeywordProvenanceRepository"
    )


def test_keyword_provenance_repository_routes_facade_calls_through_database(db):
    from repositories.keyword_provenance import FACADE_METHODS

    repo = db._keyword_provenance_repository()
    for name in FACADE_METHODS:
        assert getattr(repo, name) == getattr(db, name), name
    assert repo.db is db


def test_keyword_provenance_facade_names_never_shadow_repository_methods():
    module, tree = _module_tree()
    assert not set(_repository_methods(tree)) & set(module.FACADE_METHODS)


def test_moved_writers_reach_each_other_only_through_the_facade():
    """Calls between the writers go through their ``Database`` names, so a
    monkeypatch of ``Database._merge_keyword_into`` / ``tag_photo`` still
    intercepts the recursion, the mid-flight merges and the accept's tag."""
    _module, tree = _module_tree()
    methods = _repository_methods(tree)

    def self_attrs(node):
        return {
            n.attr for n in ast.walk(node)
            if isinstance(n, ast.Attribute)
            and isinstance(n.value, ast.Name) and n.value.id == "self"
        }

    own = set(methods) - {"__init__", "workspace_id", "_active_workspace_id"}
    for name, node in methods.items():
        assert not self_attrs(node) & own, (name, self_attrs(node) & own)
    expected = {
        "merge_keyword_into": "_merge_keyword_into",
        "upsert_one_keyword": "_merge_keyword_into",
        "normalize_keyword_data_once": "_merge_keyword_into",
        "accept_prediction": "tag_photo",
    }
    for name, facade_name in expected.items():
        assert facade_name in self_attrs(methods[name]), (name, facade_name)


def test_keyword_provenance_repository_does_not_import_db():
    """The fold stays defined once in ``db`` and is injected, not re-imported."""
    _module, tree = _module_tree()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            assert all(a.name != "db" for a in node.names)
        elif isinstance(node, ast.ImportFrom):
            assert node.module != "db"


def test_keyword_provenance_repository_injects_the_db_fold(db):
    import db as db_module

    repo = db._keyword_provenance_repository()
    assert repo.keyword_source_max_sql is db_module.keyword_source_max_sql
    assert repo.KEYWORD_SOURCE_CONFLICT_SQL is db_module.KEYWORD_SOURCE_CONFLICT_SQL
    assert repo.KEYWORD_SOURCE_MANUAL == db_module.KEYWORD_SOURCE_MANUAL
    assert repo._chunks is db_module._chunks
    assert repo.log is db_module.log


def test_keyword_provenance_repository_never_hands_itself_out_as_the_database():
    """Moved bodies that passed ``self`` (the Database) to a helper must pass
    ``self.db`` now; a bare ``self`` argument would hand over the repository."""
    _module, tree = _module_tree()
    parents = {child: node for node in ast.walk(tree) for child in ast.iter_child_nodes(node)}
    bare = [
        node.lineno for node in ast.walk(tree)
        if isinstance(node, ast.Name) and node.id == "self"
        and not (isinstance(parents.get(node), ast.Attribute)
                 and parents[node].value is node)
        and not isinstance(parents.get(node), ast.arguments)
    ]
    # The only one is ``__init__`` storing the façade (``setattr(self, ...)``).
    assert len(bare) == 1


def test_keyword_provenance_facade_signatures_unchanged():
    sig = {n: str(inspect.signature(getattr(Database, n)))
           for n in _DELEGATING_PROVENANCE_METHODS}
    assert sig == {
        "tag_photo": "(self, photo_id, keyword_id, source='manual', _commit=True)",
        "_merge_keyword_into": "(self, src_id, dst_id, *, pending_source_only=False)",
        "link_keyword_to_place": "(self, keyword_id, details)",
        "retire_builtin_wildlife_genre": "(self, force=False)",
        "_upsert_one_keyword": (
            "(self, name, parent_id, place_id=None, latitude=None, longitude=None, "
            "reuse_location_component=False)"
        ),
        "_normalize_keyword_data_once": "(self)",
        "accept_prediction": (
            "(self, prediction_id, replace_species=False, photo_ids=None, "
            "prediction_ids=None, _commit=True)"
        ),
    }
    # The repository's ``tag`` has no default to fall back on: the façade's
    # fail-safe ``'manual'`` is the only one.
    from repositories.keyword_provenance import KeywordProvenanceRepository

    params = inspect.signature(KeywordProvenanceRepository.tag).parameters
    assert params["source"].default is inspect.Parameter.empty
    assert params["source"].kind is inspect.Parameter.KEYWORD_ONLY
