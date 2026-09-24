"""Behavior pins for the species-curation domain of ``Database``.

Species curation covers the Highlights ordering (``species_highlights``),
the global representative photos (``species_representatives``), the
per-workspace photo preferences (``photo_preferences``), the Life List and
taxonomy-explorer reads, and the two one-shot legacy backfills.

The tests go through the public ``Database`` façade only, so they hold
whether the SQL lives in ``db.py`` or in ``repositories/species_curation.py``.
They pin return shapes and ordering, the ``_commit=False`` nested-transaction
seams (the caller's transaction stays open and nothing is committed), commit
visibility from a second connection, the backfill marker gates, lazy
active-workspace resolution, chunking, and that composition
(``resolve_species_display_name``, ``get_folder_subtree_ids``, ``set_meta``,
``rename_species_representatives_species``, ...) still routes through the
façade so monkeypatches take effect.
"""

import ast
import contextlib
import inspect
import sqlite3
import textwrap

import pytest
from db import Database


def _visible(db, sql, params=()):
    """Rows as a second connection sees them (i.e. committed)."""
    with contextlib.closing(sqlite3.connect(db._db_path)) as conn:
        return [tuple(r) for r in conn.execute(sql, params).fetchall()]


def _trace(db):
    statements = []
    db.conn.set_trace_callback(statements.append)
    return statements


def _highlights(db, ws=None):
    ws = db._active_workspace_id if ws is None else ws
    return [
        tuple(r)
        for r in db.conn.execute(
            "SELECT species, photo_id, rank FROM species_highlights "
            "WHERE workspace_id = ? ORDER BY species, rank, photo_id",
            (ws,),
        ).fetchall()
    ]


def _reps(db):
    return [
        tuple(r)
        for r in db.conn.execute(
            "SELECT species, photo_id, selected_order FROM species_representatives "
            "ORDER BY species, selected_order, photo_id"
        ).fetchall()
    ]


def _prefs(db):
    return [
        tuple(r)
        for r in db.conn.execute(
            "SELECT workspace_id, purpose, species, photo_id FROM photo_preferences "
            "ORDER BY workspace_id, purpose, species, photo_id"
        ).fetchall()
    ]


def _taxon(db, tid, name, rank, parent_id=None, common_name=None):
    db.conn.execute(
        "INSERT INTO taxa (id, name, common_name, rank, parent_id) "
        "VALUES (?, ?, ?, ?, ?)",
        (tid, name, common_name, rank, parent_id),
    )


def _leaf(db, name, parent_id, kw_type="taxonomy", is_species=0):
    """Insert a keyword row verbatim (``add_keyword`` would re-case it)."""
    return db.conn.execute(
        "INSERT INTO keywords (name, parent_id, type, is_species) "
        "VALUES (?, ?, ?, ?)",
        (name, parent_id, kw_type, is_species),
    ).lastrowid


def _link(db, keyword_id, taxon_id):
    db.conn.execute(
        "UPDATE keywords SET taxon_id = ? WHERE id = ?", (taxon_id, keyword_id)
    )


@pytest.fixture
def cur(db):
    """A small curated catalog in the active workspace.

    Taxonomy: Aves (class 1) > Turdidae (family 2) > Turdus (genus 3) >
    American Robin (species 4); Cardinalidae (family 5) > Northern Cardinal
    (species 6); Mammalia (class 7) > Puma concolor (species 8).
    """
    ws = db._active_workspace_id
    fid = db.add_folder("/cur", name="cur")
    sub = db.add_folder("/cur/sub", name="sub", parent_id=fid)
    other_folder = db.add_folder("/elsewhere", name="elsewhere",
                                 link_to_workspace=False)
    p = {}
    for i, (name, folder) in enumerate(
        [("robin1", fid), ("robin2", sub), ("card1", fid), ("card2", fid),
         ("puma", sub), ("plain", fid), ("rejected", fid), ("outside", other_folder)]
    ):
        p[name] = db.add_photo(
            folder, f"{name}.jpg", ".jpg", 100 + i, float(i),
            timestamp=f"2024-01-0{i + 1}T00:00:00",
        )
    db.conn.execute("UPDATE photos SET flag = 'rejected' WHERE id = ?",
                    (p["rejected"],))
    for name, score in (("robin1", 0.9), ("robin2", 0.4), ("card1", 0.7),
                        ("card2", 0.2), ("rejected", 0.99), ("outside", 0.99)):
        db.conn.execute("UPDATE photos SET quality_score = ? WHERE id = ?",
                        (score, p[name]))
    _taxon(db, 1, "Aves", "class", common_name="Birds")
    _taxon(db, 2, "Turdidae", "family", 1)
    _taxon(db, 3, "Turdus", "genus", 2)
    _taxon(db, 4, "Turdus migratorius", "species", 3, "American Robin")
    _taxon(db, 5, "Cardinalidae", "family", 1)
    _taxon(db, 6, "Cardinalis cardinalis", "species", 5, "Northern Cardinal")
    _taxon(db, 7, "Mammalia", "class")
    _taxon(db, 8, "Puma concolor", "species", 7, "Cougar")
    k = {
        "robin": db.add_keyword("American Robin", kw_type="taxonomy"),
        "cardinal": db.add_keyword("Northern Cardinal", is_species=True),
        "turdus": db.add_keyword("Turdus", kw_type="taxonomy"),
        "puma": db.add_keyword("Cougar", kw_type="taxonomy"),
        "mystery": db.add_keyword("Mystery Bird", is_species=True),
        "loc": db.add_keyword("Backyard", kw_type="location"),
        "park": db.add_keyword("Park", kw_type="location"),
        "sunset": db.add_keyword("Sunset"),
    }
    _link(db, k["robin"], 4)
    _link(db, k["cardinal"], 6)
    _link(db, k["turdus"], 3)
    _link(db, k["puma"], 8)
    db.conn.commit()
    db.tag_photo(p["robin1"], k["robin"])
    db.tag_photo(p["robin1"], k["turdus"])  # suppressed ancestor
    db.tag_photo(p["robin1"], k["loc"])
    db.tag_photo(p["robin2"], k["robin"])
    db.tag_photo(p["robin2"], k["park"])
    db.tag_photo(p["card1"], k["cardinal"])
    db.tag_photo(p["card1"], k["sunset"])
    db.tag_photo(p["card2"], k["turdus"])  # genus only: uncounted
    db.tag_photo(p["puma"], k["puma"])
    db.tag_photo(p["plain"], k["mystery"])  # unmatched
    db.tag_photo(p["rejected"], k["cardinal"])
    db.tag_photo(p["outside"], k["cardinal"])
    return {"ws": ws, "fid": fid, "sub": sub, "p": p, "k": k}


# -- legacy backfills ---------------------------------------------------------------------


_HL_KEY = "species_highlights_from_preferences_backfill"
_REP_KEY = "species_representatives_from_preferences_backfill"


def test_backfill_markers_are_set_on_init(db):
    assert db.get_meta(_HL_KEY) == "1"
    assert db.get_meta(_REP_KEY) == "1"
    assert Database._SPECIES_HIGHLIGHTS_BACKFILL_KEY == _HL_KEY
    assert Database._SPECIES_REPRESENTATIVES_BACKFILL_KEY == _REP_KEY


def test_highlights_backfill_is_gated_by_its_marker(db, cur):
    db.conn.execute(
        "INSERT INTO photo_preferences (workspace_id, purpose, species, photo_id) "
        "VALUES (?, 'highlights', 'American Robin', ?)",
        (cur["ws"], cur["p"]["robin1"]),
    )
    db.conn.commit()
    statements = _trace(db)
    db.backfill_species_highlights_from_legacy_preferences()
    db.conn.set_trace_callback(None)
    assert [s for s in statements if s.strip()] == [
        "SELECT value FROM db_meta WHERE key = "
        "'species_highlights_from_preferences_backfill'"
    ]
    assert _highlights(db) == []


def test_highlights_backfill_appends_legacy_picks_and_commits(db, cur):
    ws, p = cur["ws"], cur["p"]
    other = db.create_workspace("Other")
    db.conn.execute("DELETE FROM db_meta WHERE key = ?", (_HL_KEY,))
    db.conn.executemany(
        "INSERT INTO species_highlights (workspace_id, species, photo_id, rank) "
        "VALUES (?, ?, ?, ?)",
        [(ws, "American Robin", p["robin1"], 5)],
    )
    db.conn.executemany(
        "INSERT INTO photo_preferences (workspace_id, purpose, species, photo_id) "
        "VALUES (?, ?, ?, ?)",
        [
            (ws, "highlights", "American Robin", p["robin1"]),  # already there
            (ws, "highlights", "Northern Cardinal", p["card1"]),
            (other, "highlights", "American Robin", p["robin2"]),
            (ws, "life_list", "Cougar", p["puma"]),  # not a highlight
        ],
    )
    db.conn.execute(
        "UPDATE photo_preferences SET photo_id = ? WHERE workspace_id = ? "
        "AND species = 'American Robin'",
        (p["robin1"], ws),
    )
    db.conn.commit()
    db.conn.execute(
        "INSERT INTO photo_preferences (workspace_id, purpose, species, photo_id) "
        "VALUES (?, 'highlights', 'Tanager', ?)",
        (ws, p["robin2"]),
    )
    db.conn.commit()

    db.backfill_species_highlights_from_legacy_preferences()

    assert not db.conn.in_transaction
    assert _visible(
        db,
        "SELECT workspace_id, species, photo_id, rank FROM species_highlights "
        "ORDER BY workspace_id, species, rank",
    ) == [
        (ws, "American Robin", p["robin1"], 5),
        (ws, "Northern Cardinal", p["card1"], 1),
        (ws, "Tanager", p["robin2"], 1),
        (other, "American Robin", p["robin2"], 1),
    ]
    assert _visible(db, "SELECT value FROM db_meta WHERE key = ?", (_HL_KEY,)) == [("1",)]


def test_highlights_backfill_ranks_after_the_existing_bucket(db, cur):
    ws, p = cur["ws"], cur["p"]
    db.conn.execute("DELETE FROM db_meta WHERE key = ?", (_HL_KEY,))
    db.conn.execute(
        "INSERT INTO species_highlights (workspace_id, species, photo_id, rank) "
        "VALUES (?, 'American Robin', ?, 3)",
        (ws, p["robin2"]),
    )
    db.conn.execute(
        "INSERT INTO photo_preferences (workspace_id, purpose, species, photo_id) "
        "VALUES (?, 'highlights', 'American Robin', ?)",
        (ws, p["robin1"]),
    )
    db.conn.commit()
    db.backfill_species_highlights_from_legacy_preferences()
    assert _highlights(db) == [
        ("American Robin", p["robin2"], 3),
        ("American Robin", p["robin1"], 4),
    ]


def test_highlights_backfill_tolerates_a_missing_preferences_table(db):
    db.conn.execute("DELETE FROM db_meta WHERE key = ?", (_HL_KEY,))
    db.conn.execute("ALTER TABLE photo_preferences RENAME TO photo_preferences_old")
    db.conn.commit()
    db.backfill_species_highlights_from_legacy_preferences()
    assert db.get_meta(_HL_KEY) == "1"
    assert not db.conn.in_transaction


def test_representatives_backfill_is_gated_by_its_marker(db, cur):
    db.conn.execute(
        "INSERT INTO photo_preferences (workspace_id, purpose, species, photo_id) "
        "VALUES (?, 'life_list', 'American Robin', ?)",
        (cur["ws"], cur["p"]["robin1"]),
    )
    db.conn.commit()
    db.backfill_species_representatives_from_legacy_preferences()
    assert _reps(db) == []


def test_representatives_backfill_orders_by_purpose_precedence(db, cur, monkeypatch):
    ws, p = cur["ws"], cur["p"]
    other = db.create_workspace("Other")
    db.conn.execute("DELETE FROM db_meta WHERE key = ?", (_REP_KEY,))
    rows = [
        (ws, "species_representative", "American Robin", p["robin1"], "2024-01-03"),
        (ws, "life_list", "American Robin", p["robin2"], "2024-01-01"),
        (other, "highlights", "American Robin", p["robin1"], "2024-01-02"),
        (ws, "highlights", "Northern Cardinal", p["card1"], None),
        (ws, "other_purpose", "Cougar", p["puma"], "2024-01-01"),
    ]
    for ws_id, purpose, species, pid, ts in rows:
        db.conn.execute(
            "INSERT INTO photo_preferences "
            "(workspace_id, purpose, species, photo_id, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (ws_id, purpose, species, pid, ts, ts),
        )
    db.conn.commit()
    meta_calls = []
    real_set_meta = db.set_meta

    def recording_set_meta(key, value, _commit=True):
        meta_calls.append((key, value, _commit))
        return real_set_meta(key, value, _commit=_commit)

    monkeypatch.setattr(db, "set_meta", recording_set_meta)

    db.backfill_species_representatives_from_legacy_preferences()

    assert meta_calls == [(_REP_KEY, "1", False)]
    assert not db.conn.in_transaction
    # highlights rows first (the NULL-timestamp one sorts first), then
    # life_list, then species_representative promotes robin1 to newest.
    assert _visible(
        db,
        "SELECT species, photo_id, selected_order FROM species_representatives "
        "ORDER BY selected_order",
    ) == [
        ("Northern Cardinal", p["card1"], 1),
        ("American Robin", p["robin2"], 3),
        ("American Robin", p["robin1"], 4),
    ]
    assert db.get_meta(_REP_KEY) == "1"


def test_representatives_backfill_tolerates_a_missing_preferences_table(db):
    db.conn.execute("DELETE FROM db_meta WHERE key = ?", (_REP_KEY,))
    db.conn.execute("ALTER TABLE photo_preferences RENAME TO photo_preferences_old")
    db.conn.commit()
    db.backfill_species_representatives_from_legacy_preferences()
    assert db.get_meta(_REP_KEY) == "1"
    assert not db.conn.in_transaction


def test_next_species_representative_order(db, cur):
    assert db._next_species_representative_order() == 1
    db.conn.execute(
        "INSERT INTO species_representatives (species, photo_id, selected_order) "
        "VALUES ('American Robin', ?, 7)",
        (cur["p"]["robin1"],),
    )
    assert db._next_species_representative_order() == 8


# -- highlights candidates ----------------------------------------------------------------


def _detect(db, pid, species_conf, model="m1", fingerprint="fp1"):
    det_ids = db.save_detections(
        pid,
        [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
          "confidence": 0.9, "category": "animal"}],
        "md",
    )
    for species, conf in species_conf:
        db.add_prediction(det_ids[0], species, conf, model,
                          labels_fingerprint=fingerprint)
    return det_ids[0]


def test_highlights_candidates_workspace_wide(db, cur):
    p = cur["p"]
    _detect(db, p["card2"], [("Blue Jay", 0.6), ("Steller's Jay", 0.8)])
    rows = db.get_highlights_candidates(None)
    ids = [r["id"] for r in rows]
    # Rejected and out-of-workspace photos never appear; unscored photos
    # (puma, plain) are included at the default floor.
    assert set(ids) == {p["robin1"], p["robin2"], p["card1"], p["card2"],
                        p["puma"], p["plain"]}
    assert ids[:4] == [p["robin1"], p["card1"], p["robin2"], p["card2"]]
    by_id = {r["id"]: r for r in rows}
    assert by_id[p["robin1"]]["species"] == "American Robin"
    assert by_id[p["card1"]]["species"] == "Northern Cardinal"
    assert by_id[p["card2"]]["species"] is None  # genus-rank only
    assert by_id[p["card2"]]["predicted_species"] == "Steller's Jay"
    assert by_id[p["card2"]]["predicted_confidence"] == pytest.approx(0.8)
    assert by_id[p["card1"]]["keyword_names"].split(",") == [
        "Northern Cardinal", "Sunset"
    ] or set(by_id[p["card1"]]["keyword_names"].split(",")) == {
        "Northern Cardinal", "Sunset"
    }
    assert by_id[p["robin1"]]["folder_name"] == "cur"


def test_highlights_candidates_quality_floor_drops_unscored(db, cur):
    p = cur["p"]
    rows = db.get_highlights_candidates(None, min_quality=0.5)
    assert [r["id"] for r in rows] == [p["robin1"], p["card1"]]


def test_highlights_candidates_folder_subtree_and_single_photo(db, cur, monkeypatch):
    p = cur["p"]
    calls = []
    real = db.get_folder_subtree_ids

    def recording(folder_id):
        calls.append(folder_id)
        return real(folder_id)

    monkeypatch.setattr(db, "get_folder_subtree_ids", recording)
    rows = db.get_highlights_candidates(cur["sub"])
    assert calls == [cur["sub"]]
    assert {r["id"] for r in rows} == {p["robin2"], p["puma"]}
    rows = db.get_highlights_candidates(cur["fid"], photo_id=p["robin2"])
    assert [r["id"] for r in rows] == [p["robin2"]]
    assert rows[0]["species"] == "American Robin"
    assert db.get_highlights_candidates(None, photo_id=p["rejected"]) == []


def test_highlights_candidates_skip_rejected_and_stale_predictions(db, cur):
    p = cur["p"]
    det = _detect(db, p["plain"], [("Old Label", 0.99)], fingerprint="old")
    db.add_prediction(det, "New Label", 0.3, "m1", labels_fingerprint="new")
    db.conn.execute(
        "UPDATE predictions SET created_at = '2030-01-01' WHERE species = 'New Label'"
    )
    db.conn.commit()
    row = db.get_highlights_candidates(None, photo_id=p["plain"])[0]
    assert row["predicted_species"] == "New Label"
    pred_id = row["prediction_id"]
    db.conn.execute(
        "INSERT INTO prediction_review (prediction_id, workspace_id, status) "
        "VALUES (?, ?, 'rejected')",
        (pred_id, cur["ws"]),
    )
    db.conn.commit()
    row = db.get_highlights_candidates(None, photo_id=p["plain"])[0]
    assert row["prediction_id"] is None


def test_highlights_candidates_resolves_workspace_before_subtree(db, cur, monkeypatch):
    calls = []
    monkeypatch.setattr(db, "get_folder_subtree_ids",
                        lambda fid: calls.append(fid) or [fid])
    db.set_active_workspace(None)
    with pytest.raises(RuntimeError):
        db.get_highlights_candidates(cur["fid"])
    assert calls == []


# -- life list reads -----------------------------------------------------------------------


def test_life_list_candidates(db, cur):
    p = cur["p"]
    rows = db.get_life_list_candidates()
    got = [(r["species"], r["id"], r["taxon_rank"]) for r in rows]
    # Turdus on robin1 is suppressed by the descendant American Robin tag;
    # Turdus on card2 (the only identification there) survives.
    assert got == [
        ("American Robin", p["robin1"], "species"),
        ("American Robin", p["robin2"], "species"),
        ("Cougar", p["puma"], "species"),
        ("Mystery Bird", p["plain"], None),
        ("Northern Cardinal", p["card1"], "species"),
        ("Turdus", p["card2"], "genus"),
    ]
    assert rows[0]["scientific_name"] == "Turdus migratorius"
    assert rows[0]["common_name"] == "American Robin"
    assert rows[0]["taxon_id"] == 4


def test_life_list_candidates_species_filter_follows_taxon_root(db, cur):
    p = cur["p"]
    leaf = _leaf(db, "american robin leaf", cur["k"]["turdus"])
    _link(db, leaf, 4)
    db.conn.commit()
    db.tag_photo(p["card2"], leaf)
    rows = db.get_life_list_candidates(species="American Robin")
    assert [(r["species"], r["id"]) for r in rows] == [
        ("American Robin", p["robin1"]),
        ("American Robin", p["robin2"]),
        ("american robin leaf", p["card2"]),
    ]
    assert db.get_life_list_candidates(species="Nope") == []


def test_explorer_root_and_taxon_lookup(db, cur):
    assert db.get_explorer_root() == {
        "id": 1, "name": "Aves", "common_name": "Birds", "rank": "class",
    }
    assert db.get_explorer_root("Mammalia")["id"] == 7
    assert db.get_explorer_root("Mammalia", rank="order") is None
    assert db.get_taxon_by_id(3) == {
        "id": 3, "name": "Turdus", "common_name": None, "rank": "genus",
        "parent_id": 2,
    }
    assert db.get_taxon_by_id(999) is None


def test_life_list_taxon_ids(db, cur):
    assert db.get_life_list_taxon_ids() == {4, 6, 8}


def test_life_list_uncounted_identifications(db, cur):
    got = db.get_life_list_uncounted_identifications()
    assert got == [
        {
            "name": "Mystery Bird", "taxon_id": None, "taxon_rank": None,
            "class": None, "reason": "unmatched", "photo_count": 1,
            "filter_token": '{"name":"Mystery Bird","taxon_id":null}',
        },
        {
            "name": "Turdus", "taxon_id": 3, "taxon_rank": "genus",
            "class": {"id": 1, "name": "Aves", "common_name": "Birds"},
            "reason": "higher_rank", "photo_count": 1,
            "filter_token": '{"name":"Turdus","taxon_id":3}',
        },
    ]
    assert db.get_life_list_unmatched_species() == ["Mystery Bird", "Turdus"]


def test_uncounted_identifications_route_class_lookup_through_facade(db, cur, monkeypatch):
    seen = []
    monkeypatch.setattr(db, "get_class_ancestors_for_taxa",
                        lambda ids: seen.append(list(ids)) or {})
    got = db.get_life_list_uncounted_identifications()
    assert seen == [[None, 3]]
    assert [g["class"] for g in got] == [None, None]


def test_uncounted_filter_token_keeps_non_ascii(db, cur):
    kid = db.add_keyword("Mésange", is_species=True)
    db.tag_photo(cur["p"]["card2"], kid)
    names = {g["name"]: g for g in db.get_life_list_uncounted_identifications()}
    assert names["Mésange"]["filter_token"] == '{"name":"Mésange","taxon_id":null}'


def test_taxon_subtree_and_depth_cap(db, cur):
    got = db.get_taxon_subtree(1)
    assert {r["id"] for r in got} == {1, 2, 3, 4, 5, 6}
    assert got[0] == {"id": 1, "name": "Aves", "common_name": "Birds",
                      "rank": "class", "parent_id": None}
    assert {r["id"] for r in db.get_taxon_subtree(1, max_depth=1)} == {1, 2, 5}
    assert db.get_taxon_subtree(999) == []


def test_classes_for_taxa(db, cur):
    assert db.get_classes_for_taxa([]) == []
    assert db.get_classes_for_taxa([None]) == []
    # Sorted by common_name, falling back to name.
    assert db.get_classes_for_taxa([8, 4, 6, None]) == [
        {"id": 1, "name": "Aves", "common_name": "Birds"},
        {"id": 7, "name": "Mammalia", "common_name": None},
    ]


def test_classes_for_taxa_chunks_large_inputs(db, cur):
    statements = _trace(db)
    got = db.get_classes_for_taxa([4] + list(range(10_000, 10_800)) + [8])
    db.conn.set_trace_callback(None)
    assert [c["id"] for c in got] == [1, 7]
    assert len([s for s in dict.fromkeys(statements) if "WITH RECURSIVE" in s]) == 2


def test_class_ancestors_for_taxa(db, cur):
    assert db.get_class_ancestors_for_taxa([]) == {}
    assert db.get_class_ancestors_for_taxa([None]) == {}
    assert db.get_class_ancestors_for_taxa([4, 3, 1, 8, 999]) == {
        4: {"id": 1, "name": "Aves", "common_name": "Birds"},
        3: {"id": 1, "name": "Aves", "common_name": "Birds"},
        1: {"id": 1, "name": "Aves", "common_name": "Birds"},
        8: {"id": 7, "name": "Mammalia", "common_name": None},
    }


def test_class_ancestors_chunk_and_depth_cap(db, cur):
    # A cycle must terminate at the depth cap.
    _taxon(db, 50, "Loop A", "order")
    _taxon(db, 51, "Loop B", "order", 50)
    db.conn.execute("UPDATE taxa SET parent_id = 51 WHERE id = 50")
    statements = _trace(db)
    got = db.get_class_ancestors_for_taxa([50] + list(range(10_000, 10_800)) + [4])
    db.conn.set_trace_callback(None)
    assert got == {4: {"id": 1, "name": "Aves", "common_name": "Birds"}}
    assert len([s for s in dict.fromkeys(statements) if "WITH RECURSIVE" in s]) == 2


def test_best_photo_by_taxon(db, cur):
    p = cur["p"]
    assert db.get_life_list_best_photo_by_taxon([4, 6, 8, 3, 999]) == {
        4: {"id": p["robin1"], "filename": "robin1.jpg"},
        6: {"id": p["card1"], "filename": "card1.jpg"},
        8: {"id": p["puma"], "filename": "puma.jpg"},
        3: {"id": p["robin1"], "filename": "robin1.jpg"},
    }


def test_best_photo_by_taxon_empty_input_needs_no_workspace(db):
    db.set_active_workspace(None)
    assert db.get_life_list_best_photo_by_taxon([]) == {}
    assert db.get_life_list_best_photo_by_taxon([None]) == {}
    with pytest.raises(RuntimeError):
        db.get_life_list_best_photo_by_taxon([4])


def test_best_photo_by_taxon_chunks_at_900(db, cur):
    p = cur["p"]
    statements = _trace(db)
    got = db.get_life_list_best_photo_by_taxon(
        [4] + list(range(10_000, 10_899)) + [8]
    )
    db.conn.set_trace_callback(None)
    assert got == {4: {"id": p["robin1"], "filename": "robin1.jpg"},
                   8: {"id": p["puma"], "filename": "puma.jpg"}}
    assert len([s for s in dict.fromkeys(statements) if "k.taxon_id IN" in s]) == 2


def test_photo_life_list_species(db, cur):
    p = cur["p"]
    assert db.get_photo_life_list_species(p["robin1"]) == ["American Robin"]
    assert db.get_photo_life_list_species(p["card2"]) == ["Turdus"]
    assert db.get_photo_life_list_species(p["rejected"]) == []
    assert db.get_photo_life_list_species(p["outside"]) == []
    assert db.get_photo_life_list_species(999_999) == []


def test_photo_life_list_species_canonicalizes_leaves_to_root(db, cur):
    p = cur["p"]
    leaf = _leaf(db, "robin (leaf)", cur["k"]["turdus"])
    _link(db, leaf, 4)
    orphan_leaf = _leaf(db, "orphan leaf", cur["k"]["turdus"])
    _taxon(db, 50, "Orphanus", "species", 3)
    _link(db, orphan_leaf, 50)
    upper = _leaf(db, "zeta", None, kw_type="general", is_species=1)
    lower = _leaf(db, "Alpha", None, kw_type="general", is_species=1)
    db.conn.commit()
    db.tag_photo(p["card2"], leaf)
    db.tag_photo(p["card2"], orphan_leaf)
    db.tag_photo(p["card2"], upper)
    db.tag_photo(p["card2"], lower)
    assert db.get_photo_life_list_species(p["card2"]) == [
        "Alpha", "American Robin", "orphan leaf", "zeta",
    ]


def test_life_list_locations(db, cur):
    assert db.get_life_list_locations() == {
        "American Robin": ["Backyard", "Park"],
    }
    assert db.get_life_list_locations(species="American Robin") == {
        "American Robin": ["Backyard", "Park"],
    }
    assert db.get_life_list_locations(species="Northern Cardinal") == {}
    db.tag_photo(cur["p"]["card1"], cur["k"]["loc"])
    assert db.get_life_list_locations(species="Northern Cardinal") == {
        "Northern Cardinal": ["Backyard"],
    }
    assert db.get_life_list_locations(species="") == {
        "American Robin": ["Backyard", "Park"],
        "Northern Cardinal": ["Backyard"],
    }


# -- photo preferences and representatives ------------------------------------------------


def test_photo_preferences_scoped_to_workspace(db, cur):
    p = cur["p"]
    other = db.create_workspace("Other")
    db.set_photo_preference("life_list", "American Robin", p["robin1"])
    db.set_active_workspace(other)
    db.set_photo_preference("life_list", "American Robin", p["robin2"])
    assert db.get_photo_preferences("life_list") == {"American Robin": p["robin2"]}
    db.set_active_workspace(cur["ws"])
    assert db.get_photo_preferences("life_list") == {"American Robin": p["robin1"]}
    assert db.get_photo_preferences("highlights") == {}


def test_set_photo_preference_upserts_and_commits(db, cur):
    p = cur["p"]
    db.set_photo_preference("custom", "American Robin", p["robin1"])
    db.set_photo_preference("custom", "American Robin", p["robin2"])
    assert not db.conn.in_transaction
    assert _visible(
        db, "SELECT purpose, species, photo_id FROM photo_preferences"
    ) == [("custom", "American Robin", p["robin2"])]
    # Only the representative purposes write a global representative row.
    assert _reps(db) == []


@pytest.mark.parametrize("purpose", ["species_representative", "life_list", "highlights"])
def test_set_photo_preference_representative_purposes(db, cur, purpose):
    p = cur["p"]
    db.set_photo_preference(purpose, "American Robin", p["robin1"])
    db.set_photo_preference(purpose, "American Robin", p["robin2"])
    assert _reps(db) == [
        ("American Robin", p["robin1"], 1),
        ("American Robin", p["robin2"], 2),
    ]


def test_set_photo_preference_without_commit_leaves_transaction_open(db, cur):
    p = cur["p"]
    db.set_photo_preference("life_list", "American Robin", p["robin1"], _commit=False)
    assert db.conn.in_transaction
    assert _visible(db, "SELECT COUNT(*) FROM photo_preferences") == [(0,)]
    assert _visible(db, "SELECT COUNT(*) FROM species_representatives") == [(0,)]
    db.conn.commit()
    assert _visible(db, "SELECT COUNT(*) FROM species_representatives") == [(1,)]


def test_set_photo_preference_canonicalizes_species_through_facade(db, cur, monkeypatch):
    p = cur["p"]
    names = []
    monkeypatch.setattr(db, "resolve_species_display_name",
                        lambda name: names.append(name) or "Canonical")
    db.set_photo_preference("life_list", "raw label", p["robin1"])
    assert names == ["raw label"]
    assert _prefs(db) == [(cur["ws"], "life_list", "Canonical", p["robin1"])]
    assert _reps(db) == [("Canonical", p["robin1"], 1)]


def test_set_photo_preference_resolves_name_before_workspace(db, cur, monkeypatch):
    names = []
    monkeypatch.setattr(db, "resolve_species_display_name",
                        lambda name: names.append(name) or name)
    db.set_active_workspace(None)
    with pytest.raises(RuntimeError):
        db.set_photo_preference("life_list", "American Robin", cur["p"]["robin1"])
    assert names == ["American Robin"]


def test_set_species_representative(db, cur):
    p = cur["p"]
    db.set_species_representative("American Robin", p["robin1"])
    db.set_species_representative("American Robin", p["robin2"])
    db.set_species_representative("American Robin", p["robin1"])
    assert _reps(db) == [
        ("American Robin", p["robin2"], 2),
        ("American Robin", p["robin1"], 3),
    ]
    assert _prefs(db) == [
        (cur["ws"], "species_representative", "American Robin", p["robin1"]),
    ]
    db.set_species_representative("Northern Cardinal", p["card1"], _commit=False)
    assert db.conn.in_transaction
    db.conn.rollback()


def test_representative_lists_newest_first_and_workspace_scoped(db, cur):
    p = cur["p"]
    db.set_species_representative("American Robin", p["robin2"])
    db.set_species_representative("American Robin", p["robin1"])
    db.set_species_representative("Northern Cardinal", p["outside"])
    db.set_species_representative("Northern Cardinal", p["card1"])
    assert db.get_species_representative_lists() == {
        "American Robin": [p["robin1"], p["robin2"]],
        "Northern Cardinal": [p["card1"]],
    }
    assert db.get_species_representatives() == {
        "American Robin": p["robin1"],
        "Northern Cardinal": p["card1"],
    }
    assert db.get_species_representative_lists(species="Northern Cardinal") == {
        "Northern Cardinal": [p["card1"]],
    }
    assert db.get_species_representative_lists(species="") == {
        "American Robin": [p["robin1"], p["robin2"]],
        "Northern Cardinal": [p["card1"]],
    }


def test_representative_lists_eligible_only(db, cur):
    p = cur["p"]
    db.set_species_representative("American Robin", p["robin1"])
    db.set_species_representative("American Robin", p["card1"])  # wrong species
    db.set_species_representative("Northern Cardinal", p["rejected"])
    db.set_species_representative("Turdus", p["robin1"])  # suppressed ancestor
    db.set_species_representative("Turdus", p["card2"])
    leaf = _leaf(db, "robin leaf", cur["k"]["turdus"])
    _link(db, leaf, 4)
    db.conn.commit()
    db.tag_photo(p["puma"], leaf)
    db.set_species_representative("American Robin", p["puma"])
    assert db.get_species_representative_lists(eligible_only=True) == {
        "American Robin": [p["puma"], p["robin1"]],
        "Turdus": [p["card2"]],
    }
    assert db.get_species_representatives(eligible_only=True) == {
        "American Robin": p["puma"],
        "Turdus": p["card2"],
    }
    db.conn.execute("UPDATE folders SET status = 'missing' WHERE id = ?",
                    (cur["sub"],))
    assert db.get_species_representative_lists(eligible_only=True) == {
        "American Robin": [p["robin1"]],
        "Turdus": [p["card2"]],
    }


def test_representative_lists_canonicalize_species_filter(db, cur, monkeypatch):
    p = cur["p"]
    db.set_species_representative("American Robin", p["robin1"])
    names = []
    monkeypatch.setattr(db, "resolve_species_display_name",
                        lambda name: names.append(name) or "American Robin")
    assert db.get_species_representative_lists(species="american robin") == {
        "American Robin": [p["robin1"]],
    }
    assert names == ["american robin"]
    names.clear()
    db.get_species_representative_lists()
    assert names == []


def test_restore_species_representative(db, cur):
    p = cur["p"]
    db.set_species_representative("American Robin", p["robin1"])  # order 1
    db._restore_species_representative("American Robin", p["robin2"])
    db._restore_species_representative("American Robin", p["card1"], "bogus")
    db._restore_species_representative("American Robin", p["card2"], [1])
    db._restore_species_representative("American Robin", p["puma"], "0")
    db._restore_species_representative("American Robin", p["robin1"], 9)
    assert _reps(db) == [
        ("American Robin", p["puma"], 0),
        ("American Robin", p["robin2"], 2),
        ("American Robin", p["card1"], 3),
        ("American Robin", p["card2"], 4),
        ("American Robin", p["robin1"], 9),
    ]
    # Restores never commit: they run inside undo's transaction.
    assert db.conn.in_transaction
    assert _visible(db, "SELECT COUNT(*) FROM species_representatives") == [(1,)]


def test_restore_species_representative_routes_fallback_through_facade(db, cur, monkeypatch):
    calls = []
    monkeypatch.setattr(db, "_set_global_species_representative",
                        lambda species, pid: calls.append((species, pid)))
    db._restore_species_representative("A", 1)
    db._restore_species_representative("B", 2, "x")
    db._restore_species_representative("C", cur["p"]["robin1"], 4)
    assert calls == [("A", 1), ("B", 2)]
    assert _reps(db) == [("C", cur["p"]["robin1"], 4)]


def test_set_global_species_representative_never_commits(db, cur):
    db._set_global_species_representative("American Robin", cur["p"]["robin1"])
    db._set_global_species_representative("American Robin", cur["p"]["robin1"])
    assert _reps(db) == [("American Robin", cur["p"]["robin1"], 2)]
    assert db.conn.in_transaction
    assert _visible(db, "SELECT COUNT(*) FROM species_representatives") == [(0,)]


def test_clear_photo_preference(db, cur):
    p = cur["p"]
    other = db.create_workspace("Other")
    db.set_photo_preference("life_list", "American Robin", p["robin1"])
    db.set_photo_preference("custom", "American Robin", p["robin1"])
    db.set_active_workspace(other)
    db.set_photo_preference("life_list", "American Robin", p["robin2"])
    db.set_active_workspace(cur["ws"])
    db.clear_photo_preference("life_list", "American Robin")
    assert not db.conn.in_transaction
    assert _visible(
        db,
        "SELECT workspace_id, purpose, photo_id FROM photo_preferences "
        "ORDER BY workspace_id, purpose",
    ) == [(cur["ws"], "custom", p["robin1"]), (other, "life_list", p["robin2"])]
    # The global representative row is untouched.
    assert len(_reps(db)) == 2


def test_clear_photo_preference_without_commit(db, cur, monkeypatch):
    db.set_photo_preference("custom", "American Robin", cur["p"]["robin1"])
    names = []
    monkeypatch.setattr(db, "resolve_species_display_name",
                        lambda name: names.append(name) or "American Robin")
    db.clear_photo_preference("custom", "american robin", _commit=False)
    assert names == ["american robin"]
    assert db.conn.in_transaction
    assert _prefs(db) == []
    assert _visible(db, "SELECT COUNT(*) FROM photo_preferences") == [(1,)]


def test_clear_photo_preference_requires_workspace(db):
    db.set_active_workspace(None)
    with pytest.raises(RuntimeError):
        db.clear_photo_preference("custom", "American Robin")


def test_clear_species_representative(db, cur):
    p = cur["p"]
    other = db.create_workspace("Other")
    db.set_species_representative("American Robin", p["robin1"])
    db.set_photo_preference("life_list", "American Robin", p["robin2"])
    db.set_photo_preference("custom", "American Robin", p["robin2"])
    db.set_species_representative("Northern Cardinal", p["card1"])
    db.set_active_workspace(other)
    db.set_photo_preference("highlights", "American Robin", p["robin2"])
    db.set_active_workspace(cur["ws"])
    db.clear_species_representative("American Robin")
    assert not db.conn.in_transaction
    assert _visible(
        db, "SELECT species, photo_id FROM species_representatives"
    ) == [("Northern Cardinal", p["card1"])]
    assert _prefs(db) == [
        (cur["ws"], "custom", "American Robin", p["robin2"]),
        (cur["ws"], "species_representative", "Northern Cardinal", p["card1"]),
        (other, "highlights", "American Robin", p["robin2"]),
    ]


def test_clear_species_representative_without_commit(db, cur):
    db.set_species_representative("American Robin", cur["p"]["robin1"])
    db.clear_species_representative("American Robin", _commit=False)
    assert db.conn.in_transaction
    assert _visible(db, "SELECT COUNT(*) FROM species_representatives") == [(1,)]
    assert _reps(db) == []


# -- species highlights --------------------------------------------------------------------


def test_add_species_highlight_appends_and_commits(db, cur):
    p = cur["p"]
    assert db.add_species_highlight("American Robin", p["robin1"]) == 1
    assert db.add_species_highlight("American Robin", p["robin2"]) == 2
    assert db.add_species_highlight("Northern Cardinal", p["card1"]) == 1
    assert not db.conn.in_transaction
    assert _visible(
        db,
        "SELECT species, photo_id, rank FROM species_highlights "
        "ORDER BY species, rank",
    ) == [
        ("American Robin", p["robin1"], 1),
        ("American Robin", p["robin2"], 2),
        ("Northern Cardinal", p["card1"], 1),
    ]


def test_add_species_highlight_existing_row_keeps_rank(db, cur):
    p = cur["p"]
    db.add_species_highlight("American Robin", p["robin1"])
    db.add_species_highlight("American Robin", p["robin2"])
    db.conn.execute("UPDATE species_highlights SET updated_at = '2000-01-01'")
    db.conn.commit()
    assert db.add_species_highlight("American Robin", p["robin2"]) == 2
    assert not db.conn.in_transaction
    assert _visible(
        db,
        "SELECT photo_id FROM species_highlights WHERE updated_at != '2000-01-01'",
    ) == [(p["robin2"],)]
    assert db.add_species_highlight("American Robin", p["robin1"], _commit=False) == 1
    assert db.conn.in_transaction
    db.conn.rollback()


def test_add_species_highlight_without_commit(db, cur):
    assert db.add_species_highlight("American Robin", cur["p"]["robin1"],
                                    _commit=False) == 1
    assert db.conn.in_transaction
    assert _visible(db, "SELECT COUNT(*) FROM species_highlights") == [(0,)]


def test_add_species_highlight_canonicalizes_through_facade(db, cur, monkeypatch):
    names = []
    monkeypatch.setattr(db, "resolve_species_display_name",
                        lambda name: names.append(name) or "Canonical")
    db.add_species_highlight("raw", cur["p"]["robin1"])
    assert names == ["raw"]
    assert _highlights(db) == [("Canonical", cur["p"]["robin1"], 1)]


def test_species_highlights_read_shapes(db, cur):
    p = cur["p"]
    other = db.create_workspace("Other")
    db.add_species_highlight("American Robin", p["robin2"])
    db.add_species_highlight("American Robin", p["robin1"])
    db.add_species_highlight("Northern Cardinal", p["card1"])
    db.set_active_workspace(other)
    db.add_species_highlight("American Robin", p["card2"])
    db.set_active_workspace(cur["ws"])
    assert db.get_species_highlights() == {
        "American Robin": {p["robin2"]: 1, p["robin1"]: 2},
        "Northern Cardinal": {p["card1"]: 1},
    }
    assert list(db.get_species_highlights()["American Robin"]) == [
        p["robin2"], p["robin1"],
    ]
    assert db.get_species_highlights(species="Northern Cardinal") == {
        "Northern Cardinal": {p["card1"]: 1},
    }
    assert db.get_species_highlights(species="Nope") == {}


def test_species_highlights_eligible_only(db, cur):
    p = cur["p"]
    ws = cur["ws"]
    # Accepted keyword matches; rejected photo and wrong-species photo drop.
    db.add_species_highlight("American Robin", p["robin1"])
    db.add_species_highlight("American Robin", p["card1"])
    db.add_species_highlight("Northern Cardinal", p["rejected"])
    db.add_species_highlight("Northern Cardinal", p["card1"])
    # Prediction fallback for a photo without any species-rank keyword,
    # compared NOCASE against the canonical spelling.
    _detect(db, p["card2"], [("Blue Jay", 0.9), ("Gray Jay", 0.2)])
    db.conn.execute(
        "INSERT INTO species_highlights (workspace_id, species, photo_id, rank) "
        "VALUES (?, 'blue jay', ?, 1)",
        (ws, p["card2"]),
    )
    db.conn.execute(
        "INSERT INTO species_highlights (workspace_id, species, photo_id, rank) "
        "VALUES (?, 'Gray Jay', ?, 1)",
        (ws, p["card2"]),
    )
    # Unscored photo stays eligible via its keyword.
    db.add_species_highlight("Cougar", p["puma"])
    db.conn.commit()
    assert db.get_species_highlights(eligible_only=True) == {
        "American Robin": {p["robin1"]: 1},
        "Cougar": {p["puma"]: 1},
        "Northern Cardinal": {p["card1"]: 2},
        "blue jay": {p["card2"]: 1},
    }
    assert db.get_species_highlights(species="American Robin",
                                     eligible_only=True) == {
        "American Robin": {p["robin1"]: 1},
    }


def test_species_highlights_eligible_prediction_root_canonicalization(db, cur):
    p = cur["p"]
    ws = cur["ws"]
    # A hierarchy leaf with a unique linked taxon canonicalizes to the root.
    leaf = _leaf(db, "Robin Leaf", cur["k"]["turdus"])
    _link(db, leaf, 4)
    db.conn.commit()
    _detect(db, p["plain"], [("robin leaf", 0.9)])
    db.conn.execute("DELETE FROM photo_keywords WHERE photo_id = ?", (p["plain"],))
    db.conn.execute(
        "INSERT INTO species_highlights (workspace_id, species, photo_id, rank) "
        "VALUES (?, 'American Robin', ?, 1)",
        (ws, p["plain"]),
    )
    db.conn.commit()
    assert db.get_species_highlights(eligible_only=True) == {
        "American Robin": {p["plain"]: 1},
    }


def test_species_highlights_canonicalize_species_filter(db, cur, monkeypatch):
    db.add_species_highlight("American Robin", cur["p"]["robin1"])
    names = []
    monkeypatch.setattr(db, "resolve_species_display_name",
                        lambda name: names.append(name) or "American Robin")
    assert db.get_species_highlights(species="american robin") == {
        "American Robin": {cur["p"]["robin1"]: 1},
    }
    assert names == ["american robin"]
    names.clear()
    db.get_species_highlights()
    assert names == []


def test_promote_species_highlight(db, cur):
    p = cur["p"]
    for pid in (p["robin1"], p["robin2"], p["card1"]):
        db.add_species_highlight("American Robin", pid)
    assert db.promote_species_highlight("American Robin", p["card1"]) == 1
    assert not db.conn.in_transaction
    assert _visible(
        db,
        "SELECT photo_id, rank FROM species_highlights ORDER BY rank",
    ) == [(p["card1"], 1), (p["robin1"], 2), (p["robin2"], 3)]
    # A photo not yet in the bucket is inserted at the top.
    assert db.promote_species_highlight("American Robin", p["puma"]) == 1
    assert _highlights(db) == [
        ("American Robin", p["puma"], 1),
        ("American Robin", p["card1"], 2),
        ("American Robin", p["robin1"], 3),
        ("American Robin", p["robin2"], 4),
    ]


def test_promote_species_highlight_without_commit(db, cur):
    db.add_species_highlight("American Robin", cur["p"]["robin1"])
    assert db.promote_species_highlight("American Robin", cur["p"]["robin2"],
                                        _commit=False) == 1
    assert db.conn.in_transaction
    assert _visible(db, "SELECT COUNT(*) FROM species_highlights") == [(1,)]


def test_remove_species_highlight(db, cur):
    p = cur["p"]
    db.add_species_highlight("American Robin", p["robin1"])
    db.add_species_highlight("American Robin", p["robin2"])
    assert db.remove_species_highlight("American Robin", p["robin1"]) == 1
    assert not db.conn.in_transaction
    assert _visible(db, "SELECT photo_id, rank FROM species_highlights") == [
        (p["robin2"], 2)
    ]
    assert db.remove_species_highlight("American Robin", p["robin1"]) == 0
    assert db.remove_species_highlight("American Robin", p["robin2"],
                                       _commit=False) == 1
    assert db.conn.in_transaction
    assert _visible(db, "SELECT COUNT(*) FROM species_highlights") == [(1,)]


def test_move_species_highlight(db, cur):
    p = cur["p"]
    for pid in (p["robin1"], p["robin2"], p["card1"]):
        db.add_species_highlight("American Robin", pid)
    order = lambda: [pid for _s, pid, _r in _highlights(db)]  # noqa: E731
    assert db.move_species_highlight("American Robin", p["puma"], "up") is False
    assert db.move_species_highlight("American Robin", p["robin2"], "sideways") is False
    statements = _trace(db)
    assert db.move_species_highlight("American Robin", p["robin1"], "up") is True
    assert db.move_species_highlight("American Robin", p["card1"], "down") is True
    db.conn.set_trace_callback(None)
    assert not any(s.lstrip().startswith("UPDATE") for s in statements)
    assert db.move_species_highlight("American Robin", p["robin1"], "down") is True
    assert not db.conn.in_transaction
    assert order() == [p["robin2"], p["robin1"], p["card1"]]
    assert _visible(
        db, "SELECT photo_id, rank FROM species_highlights ORDER BY rank"
    ) == [(p["robin2"], 1), (p["robin1"], 2), (p["card1"], 3)]
    assert db.move_species_highlight("American Robin", p["card1"], "up") is True
    assert order() == [p["robin2"], p["card1"], p["robin1"]]


def test_move_species_highlight_without_commit(db, cur):
    p = cur["p"]
    db.add_species_highlight("American Robin", p["robin1"])
    db.add_species_highlight("American Robin", p["robin2"])
    assert db.move_species_highlight("American Robin", p["robin2"], "up",
                                     _commit=False) is True
    assert db.conn.in_transaction
    assert _visible(
        db, "SELECT photo_id FROM species_highlights ORDER BY rank"
    ) == [(p["robin1"],), (p["robin2"],)]


@pytest.mark.parametrize("method,args", [
    ("promote_species_highlight", ("American Robin", 1)),
    ("remove_species_highlight", ("American Robin", 1)),
    ("move_species_highlight", ("American Robin", 1, "up")),
    ("add_species_highlight", ("American Robin", 1)),
    ("clear_species_representative", ("American Robin",)),
    ("get_species_highlights", ()),
    ("get_species_representative_lists", ()),
    ("get_photo_preferences", ("life_list",)),
    ("get_life_list_candidates", ()),
    ("get_life_list_taxon_ids", ()),
    ("get_life_list_uncounted_identifications", ()),
    ("get_photo_life_list_species", (1,)),
    ("get_life_list_locations", ()),
])
def test_workspace_scoped_methods_require_active_workspace(db, method, args):
    db.set_active_workspace(None)
    with pytest.raises(RuntimeError):
        getattr(db, method)(*args)


# -- species renames -----------------------------------------------------------------------


@pytest.mark.parametrize("old,new", [("", "B"), ("A", ""), (None, "B"), ("A", "A")])
def test_renames_ignore_empty_and_identity(db, cur, old, new):
    statements = _trace(db)
    assert db.rename_photo_preferences_species(old, new) == 0
    assert db.rename_species_representatives_species(old, new) == 0
    assert db.rename_species_highlights_species(old, new) == 0
    assert db.rename_photo_preferences_species(old, new, [(1, 1)]) == 0
    db.conn.set_trace_callback(None)
    assert statements == []


def _seed_renames(db, cur):
    p, ws = cur["p"], cur["ws"]
    other = db.create_workspace("Other")
    db.conn.executemany(
        "INSERT INTO species_representatives (species, photo_id, selected_order) "
        "VALUES (?, ?, ?)",
        [("Old Name", p["robin1"], 1), ("Old Name", p["robin2"], 2),
         ("New Name", p["robin2"], 3)],
    )
    db.conn.executemany(
        "INSERT INTO photo_preferences (workspace_id, purpose, species, photo_id) "
        "VALUES (?, ?, ?, ?)",
        [(ws, "life_list", "Old Name", p["robin1"]),
         (ws, "custom", "Old Name", p["robin2"]),
         (other, "life_list", "Old Name", p["robin2"]),
         (other, "custom", "New Name", p["card1"]),
         (other, "custom", "Old Name", p["card2"])],
    )
    db.conn.commit()
    return other


def test_rename_photo_preferences_all_workspaces(db, cur):
    p, ws = cur["p"], cur["ws"]
    other = _seed_renames(db, cur)
    # INSERT OR IGNORE: other/custom already has a New Name row, so the old
    # one is dropped rather than moved.
    assert db.rename_photo_preferences_species("Old Name", "New Name") == 3
    assert not db.conn.in_transaction
    assert _visible(
        db,
        "SELECT workspace_id, purpose, species, photo_id FROM photo_preferences "
        "ORDER BY workspace_id, purpose, species",
    ) == [
        (ws, "custom", "New Name", p["robin2"]),
        (ws, "life_list", "New Name", p["robin1"]),
        (other, "custom", "New Name", p["card1"]),
        (other, "life_list", "New Name", p["robin2"]),
    ]
    assert _visible(
        db,
        "SELECT species, photo_id FROM species_representatives "
        "ORDER BY species, photo_id",
    ) == [("New Name", p["robin1"]), ("New Name", p["robin2"])]


def test_rename_photo_preferences_selected_pairs(db, cur):
    p, ws = cur["p"], cur["ws"]
    other = _seed_renames(db, cur)
    moved = db.rename_photo_preferences_species(
        "Old Name", "New Name",
        [(p["robin2"], other), (p["robin2"], other), (p["robin1"], ws),
         (p["puma"], ws)],
    )
    assert moved == 2
    assert not db.conn.in_transaction
    assert _visible(
        db,
        "SELECT workspace_id, purpose, species, photo_id FROM photo_preferences "
        "ORDER BY workspace_id, purpose, species",
    ) == [
        (ws, "custom", "Old Name", p["robin2"]),
        (ws, "life_list", "New Name", p["robin1"]),
        (other, "custom", "New Name", p["card1"]),
        (other, "custom", "Old Name", p["card2"]),
        (other, "life_list", "New Name", p["robin2"]),
    ]


def test_rename_photo_preferences_routes_representatives_through_facade(db, cur, monkeypatch):
    p, ws = cur["p"], cur["ws"]
    calls = []
    monkeypatch.setattr(
        db, "rename_species_representatives_species",
        lambda old, new, photo_ids=None, _commit=True: calls.append(
            (old, new, photo_ids, _commit)
        ),
    )
    db.rename_photo_preferences_species("A", "B")
    db.rename_photo_preferences_species(
        "A", "B", [(p["robin2"], ws), (p["robin1"], ws), (p["robin2"], 99)]
    )
    assert calls == [
        ("A", "B", None, False),
        ("A", "B", sorted([p["robin1"], p["robin2"]]), False),
    ]


@pytest.mark.parametrize("pairs", [None, "selected"])
def test_rename_photo_preferences_without_commit(db, cur, pairs):
    p, ws = cur["p"], cur["ws"]
    _seed_renames(db, cur)
    if pairs == "selected":
        pairs = [(p["robin1"], ws)]
    db.rename_photo_preferences_species("Old Name", "New Name", pairs, _commit=False)
    assert db.conn.in_transaction
    assert _visible(
        db, "SELECT COUNT(*) FROM photo_preferences WHERE species = 'New Name'"
    ) == [(1,)]


def test_rename_species_representatives_all(db, cur):
    p = cur["p"]
    _seed_renames(db, cur)
    assert db.rename_species_representatives_species("Old Name", "New Name") == 1
    assert not db.conn.in_transaction
    assert _visible(
        db,
        "SELECT species, photo_id, selected_order FROM species_representatives "
        "ORDER BY photo_id",
    ) == [("New Name", p["robin1"], 1), ("New Name", p["robin2"], 3)]


def test_rename_species_representatives_selected_photos(db, cur):
    p = cur["p"]
    _seed_renames(db, cur)
    assert db.rename_species_representatives_species(
        "Old Name", "New Name", photo_ids=[str(p["robin1"])]
    ) == 1
    assert _visible(
        db,
        "SELECT species, photo_id FROM species_representatives "
        "ORDER BY species, photo_id",
    ) == [("New Name", p["robin1"]), ("New Name", p["robin2"]),
          ("Old Name", p["robin2"])]
    statements = _trace(db)
    assert db.rename_species_representatives_species("Old Name", "New Name",
                                                     photo_ids=[]) == 0
    db.conn.set_trace_callback(None)
    assert statements == []


def test_rename_species_representatives_chunks(db, cur):
    p = cur["p"]
    _seed_renames(db, cur)
    statements = _trace(db)
    moved = db.rename_species_representatives_species(
        "Old Name", "New Name",
        photo_ids=list(range(10_000, 10_800)) + [p["robin1"], p["robin2"]],
    )
    db.conn.set_trace_callback(None)
    assert moved == 1
    distinct = list(dict.fromkeys(s.strip() for s in statements))
    assert len([s for s in distinct if s.startswith("INSERT OR IGNORE")]) == 2
    assert len([s for s in distinct if s.startswith("DELETE")]) == 2


@pytest.mark.parametrize("photo_ids", [None, "selected"])
def test_rename_species_representatives_without_commit(db, cur, photo_ids):
    _seed_renames(db, cur)
    if photo_ids == "selected":
        photo_ids = [cur["p"]["robin1"]]
    db.rename_species_representatives_species("Old Name", "New Name",
                                              photo_ids=photo_ids, _commit=False)
    assert db.conn.in_transaction
    assert _visible(
        db, "SELECT COUNT(*) FROM species_representatives WHERE species = 'Old Name'"
    ) == [(2,)]


def _seed_highlight_renames(db, cur):
    p, ws = cur["p"], cur["ws"]
    other = db.create_workspace("Other")
    for pid in (p["robin1"], p["robin2"], p["card1"]):
        db.add_species_highlight("Old Name", pid)
    db.add_species_highlight("New Name", p["card2"])
    db.add_species_highlight("New Name", p["robin2"])
    db.set_active_workspace(other)
    db.add_species_highlight("Old Name", p["puma"])
    db.add_species_highlight("Old Name", p["plain"])
    db.set_active_workspace(ws)
    return other


def test_rename_species_highlights_all_workspaces(db, cur):
    p, ws = cur["p"], cur["ws"]
    other = _seed_highlight_renames(db, cur)
    assert db.rename_species_highlights_species("Old Name", "New Name") == 4
    assert not db.conn.in_transaction
    assert _visible(
        db,
        "SELECT workspace_id, species, photo_id, rank FROM species_highlights "
        "ORDER BY workspace_id, species, rank",
    ) == [
        (ws, "New Name", p["card2"], 1),
        (ws, "New Name", p["robin2"], 2),
        (ws, "New Name", p["robin1"], 3),
        (ws, "New Name", p["card1"], 4),
        (other, "New Name", p["puma"], 1),
        (other, "New Name", p["plain"], 2),
    ]


def test_rename_species_highlights_selected_pairs(db, cur):
    p, ws = cur["p"], cur["ws"]
    other = _seed_highlight_renames(db, cur)
    moved = db.rename_species_highlights_species(
        "Old Name", "New Name",
        [(p["card1"], ws), (p["robin1"], ws), (p["card1"], ws),
         (p["plain"], other), (p["robin1"], 999)],
    )
    assert moved == 3
    assert _highlights(db) == [
        ("New Name", p["card2"], 1),
        ("New Name", p["robin2"], 2),
        ("New Name", p["robin1"], 3),
        ("New Name", p["card1"], 4),
        ("Old Name", p["robin2"], 2),
    ]
    assert _highlights(db, other) == [
        ("New Name", p["plain"], 1),
        ("Old Name", p["puma"], 1),
    ]


def test_rename_species_highlights_chunks_selected_photos(db, cur):
    p, ws = cur["p"], cur["ws"]
    _seed_highlight_renames(db, cur)
    pairs = [(pid, ws) for pid in range(10_000, 10_800)] + [(p["card1"], ws)]
    statements = _trace(db)
    assert db.rename_species_highlights_species("Old Name", "New Name", pairs) == 1
    db.conn.set_trace_callback(None)
    distinct = list(dict.fromkeys(s.strip() for s in statements))
    assert len([s for s in distinct if "AND photo_id IN (" in s]) == 2


@pytest.mark.parametrize("pairs", [None, "selected"])
def test_rename_species_highlights_without_commit(db, cur, pairs):
    p, ws = cur["p"], cur["ws"]
    _seed_highlight_renames(db, cur)
    if pairs == "selected":
        pairs = [(p["robin1"], ws)]
    db.rename_species_highlights_species("Old Name", "New Name", pairs, _commit=False)
    assert db.conn.in_transaction
    assert _visible(
        db, "SELECT COUNT(*) FROM species_highlights WHERE species = 'Old Name'"
    ) == [(5,)]


def test_rename_species_highlights_no_source_rows(db, cur):
    p, ws = cur["p"], cur["ws"]
    _seed_highlight_renames(db, cur)
    statements = _trace(db)
    assert db.rename_species_highlights_species(
        "Old Name", "New Name", [(p["card2"], ws)]
    ) == 0
    db.conn.set_trace_callback(None)
    assert not any("MAX(rank)" in s for s in statements)
    assert db.rename_species_highlights_species("Missing", "New Name") == 0


# -- structure ----------------------------------------------------------------------------


_DELEGATING_SPECIES_CURATION_METHODS = (
    "backfill_species_highlights_from_legacy_preferences",
    "_next_species_representative_order",
    "backfill_species_representatives_from_legacy_preferences",
    "get_highlights_candidates",
    "get_life_list_candidates",
    "get_explorer_root",
    "get_life_list_taxon_ids",
    "get_life_list_uncounted_identifications",
    "get_taxon_subtree",
    "get_classes_for_taxa",
    "get_class_ancestors_for_taxa",
    "get_life_list_best_photo_by_taxon",
    "get_taxon_by_id",
    "get_photo_life_list_species",
    "get_life_list_locations",
    "get_photo_preferences",
    "get_species_representative_lists",
    "_set_global_species_representative",
    "_restore_species_representative",
    "set_photo_preference",
    "clear_photo_preference",
    "clear_species_representative",
    "get_species_highlights",
    "add_species_highlight",
    "promote_species_highlight",
    "remove_species_highlight",
    "move_species_highlight",
    "rename_photo_preferences_species",
    "rename_species_representatives_species",
    "rename_species_highlights_species",
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


@pytest.mark.parametrize("name", _DELEGATING_SPECIES_CURATION_METHODS)
def test_species_curation_method_delegates_to_repository(name):
    attrs = _self_attrs(getattr(Database, name))
    assert "conn" not in attrs, (
        f"Database.{name} touches self.conn; move the SQL to "
        "SpeciesCurationRepository"
    )
    assert "_species_curation_repository" in attrs, (
        f"Database.{name} no longer delegates to SpeciesCurationRepository"
    )


def test_species_curation_repository_never_touches_keyword_writers():
    """Keyword provenance writers stay on the façade (see
    ``test_keyword_provenance_contract``); curation must not reach them."""
    import repositories.species_curation as module

    source = inspect.getsource(module)
    for writer in ("tag_photo", "untag_photo", "_merge_keyword_into",
                   "link_keyword_to_place", "retire_builtin_wildlife_genre",
                   "photo_keywords ("):
        assert writer not in source


def test_species_curation_facade_signatures_unchanged():
    sig = {n: str(inspect.signature(getattr(Database, n)))
           for n in _DELEGATING_SPECIES_CURATION_METHODS}
    assert sig["get_highlights_candidates"] == (
        "(self, folder_id, min_quality=0.0, photo_id=None)"
    )
    assert sig["set_photo_preference"] == (
        "(self, purpose, species, photo_id, _commit=True)"
    )
    assert sig["move_species_highlight"] == (
        "(self, species, photo_id, direction, _commit=True)"
    )
    assert sig["rename_photo_preferences_species"] == (
        "(self, old_species, new_species, photo_workspace_pairs=None, "
        "_commit=True)"
    )
    assert sig["rename_species_representatives_species"] == (
        "(self, old_species, new_species, photo_ids=None, _commit=True)"
    )
    assert sig["_restore_species_representative"] == (
        "(self, species, photo_id, selected_order=None)"
    )
    assert sig["get_taxon_subtree"] == "(self, root_id, max_depth=12)"
    assert sig["get_explorer_root"] == "(self, name='Aves', rank='class')"
    assert sig["get_species_representative_lists"] == (
        "(self, eligible_only=False, species=None)"
    )
    assert sig["get_species_highlights"] == (
        "(self, species=None, eligible_only=False)"
    )
