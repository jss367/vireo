"""Behavior pins for the species-curation domain of ``Database``.

Species curation covers the Highlights ordering (``species_highlights``),
the global representative photos (``species_representatives``), the
per-workspace photo preferences (``photo_preferences``), the Life List and
taxonomy-explorer reads, and the two one-shot legacy backfills.

The tests go through ``db.species_curation`` (the domain accessor) and the
two renames that stay on the ``Database`` façade. They pin return shapes and
ordering, that the writers leave the commit to the caller (the caller's
transaction stays open and nothing is committed), commit visibility from a
second connection, lazy active-workspace resolution, chunking, and that
composition still routes where patches reach it: the other domains'
``resolve_species_display_name`` and ``get_folder_subtree_ids`` through the
façade, and this domain's own entry points (``rename_representatives``,
``set_global_representative``, ...) through the repository class.
"""

import ast
import contextlib
import inspect
import sqlite3
import textwrap

import pytest
from db import Database
from repositories.species_curation import SpeciesCurationRepository
from services import photo_ingest, workspace_folders
from signature_contract import calling_signature


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
    fid = workspace_folders.add_folder(db, "/cur", name="cur")
    sub = workspace_folders.add_folder(db, "/cur/sub", name="sub", parent_id=fid)
    other_folder = workspace_folders.add_folder(db, "/elsewhere", name="elsewhere",
                                 link_to_workspace=False)
    p = {}
    for i, (name, folder) in enumerate(
        [("robin1", fid), ("robin2", sub), ("card1", fid), ("card2", fid),
         ("puma", sub), ("plain", fid), ("rejected", fid), ("outside", other_folder)]
    ):
        p[name] = photo_ingest.add_photo(
            db, folder, f"{name}.jpg", ".jpg", 100 + i, float(i),
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
    db.commit()
    return {"ws": ws, "fid": fid, "sub": sub, "p": p, "k": k}


# -- legacy backfills ---------------------------------------------------------------------


def test_next_species_representative_order(db, cur):
    assert db.species_curation.next_representative_order() == 1
    db.conn.execute(
        "INSERT INTO species_representatives (species, photo_id, selected_order) "
        "VALUES ('American Robin', ?, 7)",
        (cur["p"]["robin1"],),
    )
    assert db.species_curation.next_representative_order() == 8


# -- highlights candidates ----------------------------------------------------------------


def _detect(db, pid, species_conf, model="m1", fingerprint="fp1"):
    det_ids = db.detections.save(
        pid,
        [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
          "confidence": 0.9, "category": "animal"}],
        "md",
    )
    db.commit()
    for species, conf in species_conf:
        db.predictions.add(det_ids[0], species, conf, model,
                          labels_fingerprint=fingerprint)
        db.commit()
    return det_ids[0]


def test_highlights_candidates_workspace_wide(db, cur):
    p = cur["p"]
    _detect(db, p["card2"], [("Blue Jay", 0.6), ("Steller's Jay", 0.8)])
    rows = db.species_curation.get_highlights_candidates(None)
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
    rows = db.species_curation.get_highlights_candidates(None, min_quality=0.5)
    assert [r["id"] for r in rows] == [p["robin1"], p["card1"]]


def test_highlights_candidates_folder_subtree_and_single_photo(db, cur, monkeypatch):
    p = cur["p"]
    calls = []
    real = db.get_folder_subtree_ids

    def recording(folder_id):
        calls.append(folder_id)
        return real(folder_id)

    monkeypatch.setattr(db, "get_folder_subtree_ids", recording)
    rows = db.species_curation.get_highlights_candidates(cur["sub"])
    assert calls == [cur["sub"]]
    assert {r["id"] for r in rows} == {p["robin2"], p["puma"]}
    rows = db.species_curation.get_highlights_candidates(cur["fid"], photo_id=p["robin2"])
    assert [r["id"] for r in rows] == [p["robin2"]]
    assert rows[0]["species"] == "American Robin"
    assert db.species_curation.get_highlights_candidates(None, photo_id=p["rejected"]) == []


def test_highlights_candidates_skip_rejected_and_stale_predictions(db, cur):
    p = cur["p"]
    det = _detect(db, p["plain"], [("Old Label", 0.99)], fingerprint="old")
    db.predictions.add(det, "New Label", 0.3, "m1", labels_fingerprint="new")
    db.commit()
    db.conn.execute(
        "UPDATE predictions SET created_at = '2030-01-01' WHERE species = 'New Label'"
    )
    db.conn.commit()
    row = db.species_curation.get_highlights_candidates(None, photo_id=p["plain"])[0]
    assert row["predicted_species"] == "New Label"
    pred_id = row["prediction_id"]
    db.conn.execute(
        "INSERT INTO prediction_review (prediction_id, workspace_id, status) "
        "VALUES (?, ?, 'rejected')",
        (pred_id, cur["ws"]),
    )
    db.conn.commit()
    row = db.species_curation.get_highlights_candidates(None, photo_id=p["plain"])[0]
    assert row["prediction_id"] is None


def test_highlights_candidates_resolves_workspace_before_subtree(db, cur, monkeypatch):
    calls = []
    monkeypatch.setattr(db, "get_folder_subtree_ids",
                        lambda fid: calls.append(fid) or [fid])
    db.set_active_workspace(None)
    with pytest.raises(RuntimeError):
        db.species_curation.get_highlights_candidates(cur["fid"])
    assert calls == []


# -- life list reads -----------------------------------------------------------------------


def test_life_list_candidates(db, cur):
    p = cur["p"]
    rows = db.species_curation.get_life_list_candidates()
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
    db.commit()
    rows = db.species_curation.get_life_list_candidates(species="American Robin")
    assert [(r["species"], r["id"]) for r in rows] == [
        ("American Robin", p["robin1"]),
        ("American Robin", p["robin2"]),
        ("american robin leaf", p["card2"]),
    ]
    assert db.species_curation.get_life_list_candidates(species="Nope") == []


def test_explorer_root_and_taxon_lookup(db, cur):
    assert db.species_curation.get_explorer_root() == {
        "id": 1, "name": "Aves", "common_name": "Birds", "rank": "class",
    }
    assert db.species_curation.get_explorer_root("Mammalia")["id"] == 7
    assert db.species_curation.get_explorer_root("Mammalia", rank="order") is None
    assert db.species_curation.get_taxon_by_id(3) == {
        "id": 3, "name": "Turdus", "common_name": None, "rank": "genus",
        "parent_id": 2,
    }
    assert db.species_curation.get_taxon_by_id(999) is None


def test_life_list_taxon_ids(db, cur):
    assert db.species_curation.get_life_list_taxon_ids() == {4, 6, 8}


def test_life_list_uncounted_identifications(db, cur):
    got = db.species_curation.get_life_list_uncounted_identifications()
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
    monkeypatch.setattr(SpeciesCurationRepository, "get_class_ancestors",
                        lambda self, ids: seen.append(list(ids)) or {})
    got = db.species_curation.get_life_list_uncounted_identifications()
    assert seen == [[None, 3]]
    assert [g["class"] for g in got] == [None, None]


def test_uncounted_filter_token_keeps_non_ascii(db, cur):
    kid = db.add_keyword("Mésange", is_species=True)
    db.tag_photo(cur["p"]["card2"], kid)
    db.commit()
    names = {g["name"]: g for g in db.species_curation.get_life_list_uncounted_identifications()}
    assert names["Mésange"]["filter_token"] == '{"name":"Mésange","taxon_id":null}'


def test_taxon_subtree_and_depth_cap(db, cur):
    got = db.species_curation.get_taxon_subtree(1)
    assert {r["id"] for r in got} == {1, 2, 3, 4, 5, 6}
    assert got[0] == {"id": 1, "name": "Aves", "common_name": "Birds",
                      "rank": "class", "parent_id": None}
    assert {r["id"] for r in db.species_curation.get_taxon_subtree(1, max_depth=1)} == {1, 2, 5}
    assert db.species_curation.get_taxon_subtree(999) == []


def test_classes_for_taxa(db, cur):
    assert db.species_curation.get_classes_for_taxa([]) == []
    assert db.species_curation.get_classes_for_taxa([None]) == []
    # Sorted by common_name, falling back to name.
    assert db.species_curation.get_classes_for_taxa([8, 4, 6, None]) == [
        {"id": 1, "name": "Aves", "common_name": "Birds"},
        {"id": 7, "name": "Mammalia", "common_name": None},
    ]


def test_classes_for_taxa_chunks_large_inputs(db, cur):
    statements = _trace(db)
    got = db.species_curation.get_classes_for_taxa([4] + list(range(10_000, 10_800)) + [8])
    db.conn.set_trace_callback(None)
    assert [c["id"] for c in got] == [1, 7]
    assert len([s for s in dict.fromkeys(statements) if "WITH RECURSIVE" in s]) == 2


def test_class_ancestors_for_taxa(db, cur):
    assert db.species_curation.get_class_ancestors([]) == {}
    assert db.species_curation.get_class_ancestors([None]) == {}
    assert db.species_curation.get_class_ancestors([4, 3, 1, 8, 999]) == {
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
    got = db.species_curation.get_class_ancestors([50] + list(range(10_000, 10_800)) + [4])
    db.conn.set_trace_callback(None)
    assert got == {4: {"id": 1, "name": "Aves", "common_name": "Birds"}}
    assert len([s for s in dict.fromkeys(statements) if "WITH RECURSIVE" in s]) == 2


def test_best_photo_by_taxon(db, cur):
    p = cur["p"]
    assert db.species_curation.get_life_list_best_photo_by_taxon([4, 6, 8, 3, 999]) == {
        4: {"id": p["robin1"], "filename": "robin1.jpg"},
        6: {"id": p["card1"], "filename": "card1.jpg"},
        8: {"id": p["puma"], "filename": "puma.jpg"},
        3: {"id": p["robin1"], "filename": "robin1.jpg"},
    }


def test_best_photo_by_taxon_empty_input_needs_no_workspace(db):
    db.set_active_workspace(None)
    assert db.species_curation.get_life_list_best_photo_by_taxon([]) == {}
    assert db.species_curation.get_life_list_best_photo_by_taxon([None]) == {}
    with pytest.raises(RuntimeError):
        db.species_curation.get_life_list_best_photo_by_taxon([4])


def test_best_photo_by_taxon_chunks_at_900(db, cur):
    p = cur["p"]
    statements = _trace(db)
    got = db.species_curation.get_life_list_best_photo_by_taxon(
        [4] + list(range(10_000, 10_899)) + [8]
    )
    db.conn.set_trace_callback(None)
    assert got == {4: {"id": p["robin1"], "filename": "robin1.jpg"},
                   8: {"id": p["puma"], "filename": "puma.jpg"}}
    assert len([s for s in dict.fromkeys(statements) if "k.taxon_id IN" in s]) == 2


def test_photo_life_list_species(db, cur):
    p = cur["p"]
    assert db.species_curation.get_photo_life_list_species(p["robin1"]) == ["American Robin"]
    assert db.species_curation.get_photo_life_list_species(p["card2"]) == ["Turdus"]
    assert db.species_curation.get_photo_life_list_species(p["rejected"]) == []
    assert db.species_curation.get_photo_life_list_species(p["outside"]) == []
    assert db.species_curation.get_photo_life_list_species(999_999) == []


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
    db.commit()
    assert db.species_curation.get_photo_life_list_species(p["card2"]) == [
        "Alpha", "American Robin", "orphan leaf", "zeta",
    ]


def test_life_list_locations(db, cur):
    assert db.species_curation.get_life_list_locations() == {
        "American Robin": ["Backyard", "Park"],
    }
    assert db.species_curation.get_life_list_locations(species="American Robin") == {
        "American Robin": ["Backyard", "Park"],
    }
    assert db.species_curation.get_life_list_locations(species="Northern Cardinal") == {}
    db.tag_photo(cur["p"]["card1"], cur["k"]["loc"])
    db.commit()
    assert db.species_curation.get_life_list_locations(species="Northern Cardinal") == {
        "Northern Cardinal": ["Backyard"],
    }
    assert db.species_curation.get_life_list_locations(species="") == {
        "American Robin": ["Backyard", "Park"],
        "Northern Cardinal": ["Backyard"],
    }


# -- photo preferences and representatives ------------------------------------------------


def test_photo_preferences_scoped_to_workspace(db, cur):
    p = cur["p"]
    other = db.create_workspace("Other")
    db.commit()
    db.species_curation.set_photo_preference("life_list", "American Robin", p["robin1"])
    db.set_active_workspace(other)
    db.species_curation.set_photo_preference("life_list", "American Robin", p["robin2"])
    assert db.species_curation.get_photo_preferences("life_list") == {"American Robin": p["robin2"]}
    db.set_active_workspace(cur["ws"])
    assert db.species_curation.get_photo_preferences("life_list") == {"American Robin": p["robin1"]}
    assert db.species_curation.get_photo_preferences("highlights") == {}


def test_set_photo_preference_upserts_and_commits_with_the_callers_unit(db, cur):
    p = cur["p"]
    with db.transaction():
        db.species_curation.set_photo_preference("custom", "American Robin", p["robin1"])
        db.species_curation.set_photo_preference("custom", "American Robin", p["robin2"])
    assert not db.conn.in_transaction
    assert _visible(
        db, "SELECT purpose, species, photo_id FROM photo_preferences"
    ) == [("custom", "American Robin", p["robin2"])]
    # Only the representative purposes write a global representative row.
    assert _reps(db) == []


@pytest.mark.parametrize("purpose", ["species_representative", "life_list", "highlights"])
def test_set_photo_preference_representative_purposes(db, cur, purpose):
    p = cur["p"]
    db.species_curation.set_photo_preference(purpose, "American Robin", p["robin1"])
    db.species_curation.set_photo_preference(purpose, "American Robin", p["robin2"])
    assert _reps(db) == [
        ("American Robin", p["robin1"], 1),
        ("American Robin", p["robin2"], 2),
    ]


def test_set_photo_preference_leaves_commit_to_caller(db, cur):
    p = cur["p"]
    db.species_curation.set_photo_preference("life_list", "American Robin", p["robin1"])
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
    db.species_curation.set_photo_preference("life_list", "raw label", p["robin1"])
    assert names == ["raw label"]
    assert _prefs(db) == [(cur["ws"], "life_list", "Canonical", p["robin1"])]
    assert _reps(db) == [("Canonical", p["robin1"], 1)]


def test_set_photo_preference_resolves_name_before_workspace(db, cur, monkeypatch):
    names = []
    monkeypatch.setattr(db, "resolve_species_display_name",
                        lambda name: names.append(name) or name)
    db.set_active_workspace(None)
    with pytest.raises(RuntimeError):
        db.species_curation.set_photo_preference("life_list", "American Robin", cur["p"]["robin1"])
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
    db.set_species_representative("Northern Cardinal", p["card1"])
    assert db.conn.in_transaction
    db.conn.rollback()


def test_representative_lists_newest_first_and_workspace_scoped(db, cur):
    p = cur["p"]
    db.set_species_representative("American Robin", p["robin2"])
    db.set_species_representative("American Robin", p["robin1"])
    db.set_species_representative("Northern Cardinal", p["outside"])
    db.set_species_representative("Northern Cardinal", p["card1"])
    assert db.species_curation.get_representative_lists() == {
        "American Robin": [p["robin1"], p["robin2"]],
        "Northern Cardinal": [p["card1"]],
    }
    assert db.get_species_representatives() == {
        "American Robin": p["robin1"],
        "Northern Cardinal": p["card1"],
    }
    assert db.species_curation.get_representative_lists(species="Northern Cardinal") == {
        "Northern Cardinal": [p["card1"]],
    }
    assert db.species_curation.get_representative_lists(species="") == {
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
    db.commit()
    db.set_species_representative("American Robin", p["puma"])
    assert db.species_curation.get_representative_lists(eligible_only=True) == {
        "American Robin": [p["puma"], p["robin1"]],
        "Turdus": [p["card2"]],
    }
    assert db.get_species_representatives(eligible_only=True) == {
        "American Robin": p["puma"],
        "Turdus": p["card2"],
    }
    db.conn.execute("UPDATE folders SET status = 'missing' WHERE id = ?",
                    (cur["sub"],))
    assert db.species_curation.get_representative_lists(eligible_only=True) == {
        "American Robin": [p["robin1"]],
        "Turdus": [p["card2"]],
    }


def test_representative_lists_canonicalize_species_filter(db, cur, monkeypatch):
    p = cur["p"]
    db.set_species_representative("American Robin", p["robin1"])
    names = []
    monkeypatch.setattr(db, "resolve_species_display_name",
                        lambda name: names.append(name) or "American Robin")
    assert db.species_curation.get_representative_lists(species="american robin") == {
        "American Robin": [p["robin1"]],
    }
    assert names == ["american robin"]
    names.clear()
    db.species_curation.get_representative_lists()
    assert names == []


def test_restore_species_representative(db, cur):
    p = cur["p"]
    with db.transaction():
        db.set_species_representative("American Robin", p["robin1"])  # order 1
    db.species_curation.restore_representative("American Robin", p["robin2"])
    db.species_curation.restore_representative("American Robin", p["card1"], "bogus")
    db.species_curation.restore_representative("American Robin", p["card2"], [1])
    db.species_curation.restore_representative("American Robin", p["puma"], "0")
    db.species_curation.restore_representative("American Robin", p["robin1"], 9)
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
    monkeypatch.setattr(SpeciesCurationRepository, "set_global_representative",
                        lambda self, species, pid: calls.append((species, pid)))
    db.species_curation.restore_representative("A", 1)
    db.species_curation.restore_representative("B", 2, "x")
    db.species_curation.restore_representative("C", cur["p"]["robin1"], 4)
    assert calls == [("A", 1), ("B", 2)]
    assert _reps(db) == [("C", cur["p"]["robin1"], 4)]


def test_set_global_species_representative_never_commits(db, cur):
    db.species_curation.set_global_representative("American Robin", cur["p"]["robin1"])
    db.species_curation.set_global_representative("American Robin", cur["p"]["robin1"])
    assert _reps(db) == [("American Robin", cur["p"]["robin1"], 2)]
    assert db.conn.in_transaction
    assert _visible(db, "SELECT COUNT(*) FROM species_representatives") == [(0,)]


def test_clear_photo_preference(db, cur):
    p = cur["p"]
    other = db.create_workspace("Other")
    db.commit()
    db.species_curation.set_photo_preference("life_list", "American Robin", p["robin1"])
    db.species_curation.set_photo_preference("custom", "American Robin", p["robin1"])
    db.set_active_workspace(other)
    db.species_curation.set_photo_preference("life_list", "American Robin", p["robin2"])
    db.set_active_workspace(cur["ws"])
    with db.transaction():
        db.species_curation.clear_photo_preference("life_list", "American Robin")
    assert not db.conn.in_transaction
    assert _visible(
        db,
        "SELECT workspace_id, purpose, photo_id FROM photo_preferences "
        "ORDER BY workspace_id, purpose",
    ) == [(cur["ws"], "custom", p["robin1"]), (other, "life_list", p["robin2"])]
    # The global representative row is untouched.
    assert len(_reps(db)) == 2


def test_clear_photo_preference_leaves_commit_to_caller(db, cur, monkeypatch):
    with db.transaction():
        db.species_curation.set_photo_preference("custom", "American Robin", cur["p"]["robin1"])
    names = []
    monkeypatch.setattr(db, "resolve_species_display_name",
                        lambda name: names.append(name) or "American Robin")
    db.species_curation.clear_photo_preference("custom", "american robin")
    assert names == ["american robin"]
    assert db.conn.in_transaction
    assert _prefs(db) == []
    assert _visible(db, "SELECT COUNT(*) FROM photo_preferences") == [(1,)]


def test_clear_photo_preference_requires_workspace(db):
    db.set_active_workspace(None)
    with pytest.raises(RuntimeError):
        db.species_curation.clear_photo_preference("custom", "American Robin")


def test_clear_species_representative(db, cur):
    p = cur["p"]
    other = db.create_workspace("Other")
    db.commit()
    db.set_species_representative("American Robin", p["robin1"])
    db.species_curation.set_photo_preference("life_list", "American Robin", p["robin2"])
    db.species_curation.set_photo_preference("custom", "American Robin", p["robin2"])
    db.set_species_representative("Northern Cardinal", p["card1"])
    db.set_active_workspace(other)
    db.species_curation.set_photo_preference("highlights", "American Robin", p["robin2"])
    db.set_active_workspace(cur["ws"])
    with db.transaction():
        db.species_curation.clear_representative("American Robin")
    assert not db.conn.in_transaction
    assert _visible(
        db, "SELECT species, photo_id FROM species_representatives"
    ) == [("Northern Cardinal", p["card1"])]
    assert _prefs(db) == [
        (cur["ws"], "custom", "American Robin", p["robin2"]),
        (cur["ws"], "species_representative", "Northern Cardinal", p["card1"]),
        (other, "highlights", "American Robin", p["robin2"]),
    ]


def test_clear_species_representative_leaves_commit_to_caller(db, cur):
    with db.transaction():
        db.set_species_representative("American Robin", cur["p"]["robin1"])
    db.species_curation.clear_representative("American Robin")
    assert db.conn.in_transaction
    assert _visible(db, "SELECT COUNT(*) FROM species_representatives") == [(1,)]
    assert _reps(db) == []


# -- species highlights --------------------------------------------------------------------


def test_add_species_highlight_appends_and_commits_with_the_callers_unit(db, cur):
    p = cur["p"]
    with db.transaction():
        assert db.species_curation.add_highlight("American Robin", p["robin1"]) == 1
        assert db.species_curation.add_highlight("American Robin", p["robin2"]) == 2
        assert db.species_curation.add_highlight("Northern Cardinal", p["card1"]) == 1
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
    db.species_curation.add_highlight("American Robin", p["robin1"])
    db.species_curation.add_highlight("American Robin", p["robin2"])
    db.conn.execute("UPDATE species_highlights SET updated_at = '2000-01-01'")
    db.conn.commit()
    with db.transaction():
        assert db.species_curation.add_highlight("American Robin", p["robin2"]) == 2
    assert not db.conn.in_transaction
    assert _visible(
        db,
        "SELECT photo_id FROM species_highlights WHERE updated_at != '2000-01-01'",
    ) == [(p["robin2"],)]
    assert db.species_curation.add_highlight("American Robin", p["robin1"]) == 1
    assert db.conn.in_transaction
    db.conn.rollback()


def test_add_species_highlight_leaves_commit_to_caller(db, cur):
    assert db.species_curation.add_highlight("American Robin", cur["p"]["robin1"]) == 1
    assert db.conn.in_transaction
    assert _visible(db, "SELECT COUNT(*) FROM species_highlights") == [(0,)]


def test_add_species_highlight_canonicalizes_through_facade(db, cur, monkeypatch):
    names = []
    monkeypatch.setattr(db, "resolve_species_display_name",
                        lambda name: names.append(name) or "Canonical")
    db.species_curation.add_highlight("raw", cur["p"]["robin1"])
    assert names == ["raw"]
    assert _highlights(db) == [("Canonical", cur["p"]["robin1"], 1)]


def test_species_highlights_read_shapes(db, cur):
    p = cur["p"]
    other = db.create_workspace("Other")
    db.commit()
    db.species_curation.add_highlight("American Robin", p["robin2"])
    db.species_curation.add_highlight("American Robin", p["robin1"])
    db.species_curation.add_highlight("Northern Cardinal", p["card1"])
    db.set_active_workspace(other)
    db.species_curation.add_highlight("American Robin", p["card2"])
    db.set_active_workspace(cur["ws"])
    assert db.species_curation.get_highlights() == {
        "American Robin": {p["robin2"]: 1, p["robin1"]: 2},
        "Northern Cardinal": {p["card1"]: 1},
    }
    assert list(db.species_curation.get_highlights()["American Robin"]) == [
        p["robin2"], p["robin1"],
    ]
    assert db.species_curation.get_highlights(species="Northern Cardinal") == {
        "Northern Cardinal": {p["card1"]: 1},
    }
    assert db.species_curation.get_highlights(species="Nope") == {}


def test_species_highlights_eligible_only(db, cur):
    p = cur["p"]
    ws = cur["ws"]
    # Accepted keyword matches; rejected photo and wrong-species photo drop.
    db.species_curation.add_highlight("American Robin", p["robin1"])
    db.species_curation.add_highlight("American Robin", p["card1"])
    db.species_curation.add_highlight("Northern Cardinal", p["rejected"])
    db.species_curation.add_highlight("Northern Cardinal", p["card1"])
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
    db.species_curation.add_highlight("Cougar", p["puma"])
    db.conn.commit()
    assert db.species_curation.get_highlights(eligible_only=True) == {
        "American Robin": {p["robin1"]: 1},
        "Cougar": {p["puma"]: 1},
        "Northern Cardinal": {p["card1"]: 2},
        "blue jay": {p["card2"]: 1},
    }
    assert db.species_curation.get_highlights(species="American Robin",
                                              eligible_only=True) == {
        "American Robin": {p["robin1"]: 1},
    }


def _detect_subjects(db, pid, species_conf, model="m1"):
    """One detection per (species, confidence), each its own subject."""
    det_ids = db.detections.save(
        pid,
        [{"box": {"x": 0.05 + 0.3 * i, "y": 0.1, "w": 0.2, "h": 0.2},
          "confidence": 0.9, "category": "animal"}
         for i in range(len(species_conf))],
        "md",
    )
    db.commit()
    for det, (species, conf) in zip(det_ids, species_conf, strict=True):
        db.predictions.add(det, species, conf, model, labels_fingerprint="fp1")
        db.commit()
    return det_ids


def test_species_highlights_eligible_second_bucket_of_tagged_photo(db, cur):
    """A highlight saved from the second-species bucket of a tagged photo
    stays eligible exactly as long as the photo stays in that bucket.

    card1 is tagged Northern Cardinal. With a Cardinal subject and a Blue
    Jay subject, ``photo_species_memberships`` puts it in the Blue Jay
    bucket, so a Blue Jay highlight survives reload. Once the Cardinal
    prediction is rejected no subject accounts for the keyword, the photo
    leaves the Blue Jay bucket, and the highlight must stop counting, or
    the Blue Jay bucket (kept visible by another photo) would report a
    selected highlight it does not show.
    """
    from highlights_payload import HighlightsQuery, build_highlights_payload

    p = cur["p"]
    cardinal_det, _jay_det = _detect_subjects(
        db, p["card1"], [("Northern Cardinal", 0.9), ("Blue Jay", 0.8)],
    )
    # card2 (genus-only keyword) keeps a Blue Jay bucket on the page.
    _detect(db, p["card2"], [("Blue Jay", 0.9)])
    db.species_curation.add_highlight("Blue Jay", p["card1"])
    db.conn.commit()
    assert db.species_curation.get_highlights(
        species="Blue Jay", eligible_only=True
    ) == {"Blue Jay": {p["card1"]: 1}}

    cardinal_pred = db.conn.execute(
        "SELECT id FROM predictions WHERE detection_id = ?", (cardinal_det,),
    ).fetchone()["id"]
    db.update_prediction_status(cardinal_pred, "rejected")

    assert db.species_curation.get_highlights(
        species="Blue Jay", eligible_only=True
    ) == {}
    payload = build_highlights_payload(db, HighlightsQuery(scope="workspace"))
    jay = next(b for b in payload["buckets"] if b["species"] == "Blue Jay")
    assert [ph["id"] for ph in jay["photos"]] == [p["card2"]]
    assert jay["has_highlight_selection"] is False
    # The stored row is kept, so the selection returns with the subject.
    assert db.species_curation.get_highlights(species="Blue Jay") == {
        "Blue Jay": {p["card1"]: 1},
    }


def test_species_highlights_eligible_lower_confidence_subject_bucket(db, cur):
    """A highlight saved from a lower-confidence subject's prediction
    bucket of an untagged photo survives on reload.

    ``collect_highlight_buckets`` places the photo in each subject's
    bucket. Before the fix, the eligibility filter only accepted
    ``sh.species`` equal to the photo-wide top prediction (``ORDER BY
    pr.confidence DESC LIMIT 1``), so a highlight starred from the
    second subject's bucket was dropped. The prediction branch now
    iterates each detection's top non-rejected prediction.
    """
    p = cur["p"]
    ws = cur["ws"]
    # Two separate detections on card2 (its genus-rank Turdus keyword
    # doesn't satisfy the species accepted branch); Blue Jay is the
    # photo-wide top, Gray Jay is a second subject's top.
    _detect(db, p["card2"], [("Blue Jay", 0.9)])
    _detect(db, p["card2"], [("Gray Jay", 0.5)], fingerprint="fp2")
    db.conn.execute(
        "INSERT INTO species_highlights (workspace_id, species, photo_id, rank) "
        "VALUES (?, 'Gray Jay', ?, 1)",
        (ws, p["card2"]),
    )
    db.conn.commit()
    assert db.species_curation.get_highlights(
        species="Gray Jay", eligible_only=True
    ) == {"Gray Jay": {p["card2"]: 1}}


def test_species_highlights_eligible_rejected_sibling_prediction_dropped(
    db, cur,
):
    """A highlight saved against a prediction species that was then
    rejected per-detection is dropped on reload — the detection's top
    non-rejected prediction is now a different species.
    """
    p = cur["p"]
    ws = cur["ws"]
    did = _detect(db, p["card2"], [("Blue Jay", 0.9), ("Gray Jay", 0.5)])
    # Blue Jay highlight is eligible: it's the detection's top non-rejected
    # prediction.
    db.conn.execute(
        "INSERT INTO species_highlights (workspace_id, species, photo_id, rank) "
        "VALUES (?, 'Blue Jay', ?, 1)",
        (ws, p["card2"]),
    )
    db.conn.commit()
    assert p["card2"] in db.species_curation.get_highlights(
        species="Blue Jay", eligible_only=True
    ).get("Blue Jay", {})
    # Reject the Blue Jay prediction; Gray Jay (0.5) becomes the top
    # non-rejected prediction for this detection. The Blue Jay highlight
    # loses eligibility because no detection's top prediction now
    # canonicalizes to Blue Jay.
    blue_pred_id = db.conn.execute(
        "SELECT id FROM predictions WHERE detection_id = ? AND species = ?",
        (did, "Blue Jay"),
    ).fetchone()["id"]
    db.conn.execute(
        "INSERT INTO prediction_review (prediction_id, workspace_id, status) "
        "VALUES (?, ?, 'rejected')",
        (blue_pred_id, ws),
    )
    db.conn.commit()
    assert db.species_curation.get_highlights(
        species="Blue Jay", eligible_only=True
    ) == {}


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
    assert db.species_curation.get_highlights(eligible_only=True) == {
        "American Robin": {p["plain"]: 1},
    }


def test_species_highlights_canonicalize_species_filter(db, cur, monkeypatch):
    db.species_curation.add_highlight("American Robin", cur["p"]["robin1"])
    names = []
    monkeypatch.setattr(db, "resolve_species_display_name",
                        lambda name: names.append(name) or "American Robin")
    assert db.species_curation.get_highlights(species="american robin") == {
        "American Robin": {cur["p"]["robin1"]: 1},
    }
    assert names == ["american robin"]
    names.clear()
    db.species_curation.get_highlights()
    assert names == []


def test_promote_species_highlight(db, cur):
    p = cur["p"]
    for pid in (p["robin1"], p["robin2"], p["card1"]):
        db.species_curation.add_highlight("American Robin", pid)
    with db.transaction():
        assert db.species_curation.promote_highlight("American Robin", p["card1"]) == 1
    assert not db.conn.in_transaction
    assert _visible(
        db,
        "SELECT photo_id, rank FROM species_highlights ORDER BY rank",
    ) == [(p["card1"], 1), (p["robin1"], 2), (p["robin2"], 3)]
    # A photo not yet in the bucket is inserted at the top.
    assert db.species_curation.promote_highlight("American Robin", p["puma"]) == 1
    assert _highlights(db) == [
        ("American Robin", p["puma"], 1),
        ("American Robin", p["card1"], 2),
        ("American Robin", p["robin1"], 3),
        ("American Robin", p["robin2"], 4),
    ]


def test_promote_species_highlight_leaves_commit_to_caller(db, cur):
    with db.transaction():
        db.species_curation.add_highlight("American Robin", cur["p"]["robin1"])
    assert db.species_curation.promote_highlight("American Robin", cur["p"]["robin2"]) == 1
    assert db.conn.in_transaction
    assert _visible(db, "SELECT COUNT(*) FROM species_highlights") == [(1,)]


def test_remove_species_highlight(db, cur):
    p = cur["p"]
    db.species_curation.add_highlight("American Robin", p["robin1"])
    db.species_curation.add_highlight("American Robin", p["robin2"])
    with db.transaction():
        assert db.species_curation.remove_highlight("American Robin", p["robin1"]) == 1
    assert not db.conn.in_transaction
    assert _visible(db, "SELECT photo_id, rank FROM species_highlights") == [
        (p["robin2"], 2)
    ]
    assert db.species_curation.remove_highlight("American Robin", p["robin1"]) == 0
    assert db.species_curation.remove_highlight("American Robin", p["robin2"]) == 1
    assert db.conn.in_transaction
    assert _visible(db, "SELECT COUNT(*) FROM species_highlights") == [(1,)]


def test_move_species_highlight(db, cur):
    p = cur["p"]
    for pid in (p["robin1"], p["robin2"], p["card1"]):
        db.species_curation.add_highlight("American Robin", pid)
    order = lambda: [pid for _s, pid, _r in _highlights(db)]  # noqa: E731
    assert db.species_curation.move_highlight("American Robin", p["puma"], "up") is False
    assert db.species_curation.move_highlight("American Robin", p["robin2"], "sideways") is False
    statements = _trace(db)
    assert db.species_curation.move_highlight("American Robin", p["robin1"], "up") is True
    assert db.species_curation.move_highlight("American Robin", p["card1"], "down") is True
    db.conn.set_trace_callback(None)
    assert not any(s.lstrip().startswith("UPDATE") for s in statements)
    with db.transaction():
        assert db.species_curation.move_highlight("American Robin", p["robin1"], "down") is True
    assert not db.conn.in_transaction
    assert order() == [p["robin2"], p["robin1"], p["card1"]]
    assert _visible(
        db, "SELECT photo_id, rank FROM species_highlights ORDER BY rank"
    ) == [(p["robin2"], 1), (p["robin1"], 2), (p["card1"], 3)]
    assert db.species_curation.move_highlight("American Robin", p["card1"], "up") is True
    assert order() == [p["robin2"], p["card1"], p["robin1"]]


def test_move_species_highlight_leaves_commit_to_caller(db, cur):
    p = cur["p"]
    with db.transaction():
        db.species_curation.add_highlight("American Robin", p["robin1"])
        db.species_curation.add_highlight("American Robin", p["robin2"])
    assert db.species_curation.move_highlight("American Robin", p["robin2"], "up") is True
    assert db.conn.in_transaction
    assert _visible(
        db, "SELECT photo_id FROM species_highlights ORDER BY rank"
    ) == [(p["robin1"],), (p["robin2"],)]


@pytest.mark.parametrize("method,args", [
    ("promote_highlight", ("American Robin", 1)),
    ("remove_highlight", ("American Robin", 1)),
    ("move_highlight", ("American Robin", 1, "up")),
    ("add_highlight", ("American Robin", 1)),
    ("clear_representative", ("American Robin",)),
    ("get_highlights", ()),
    ("get_representative_lists", ()),
    ("get_photo_preferences", ("life_list",)),
    ("get_life_list_candidates", ()),
    ("get_life_list_taxon_ids", ()),
    ("get_life_list_uncounted_identifications", ()),
    ("get_photo_life_list_species", (1,)),
    ("get_life_list_locations", ()),
    ("highlight_rows_for_photos", ([1],)),
    ("preference_purposes_for_species", ("American Robin",)),
    ("preference_rows_for_photos", ([1],)),
    ("photo_is_life_list_preference_eligible", ("American Robin", 1)),
])
def test_workspace_scoped_methods_require_active_workspace(db, method, args):
    db.set_active_workspace(None)
    with pytest.raises(RuntimeError):
        getattr(db.species_curation, method)(*args)


# -- species renames -----------------------------------------------------------------------


@pytest.mark.parametrize("old,new", [("", "B"), ("A", ""), (None, "B"), ("A", "A")])
def test_renames_ignore_empty_and_identity(db, cur, old, new):
    statements = _trace(db)
    assert db.rename_photo_preferences_species(old, new) == 0
    assert db.species_curation.rename_representatives(old, new) == 0
    assert db.rename_species_highlights_species(old, new) == 0
    assert db.rename_photo_preferences_species(old, new, [(1, 1)]) == 0
    db.conn.set_trace_callback(None)
    assert statements == []


def _seed_renames(db, cur):
    p, ws = cur["p"], cur["ws"]
    other = db.create_workspace("Other")
    db.commit()
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
    with db.transaction():
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
    with db.transaction():
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
        SpeciesCurationRepository, "rename_representatives",
        lambda self, old, new, photo_ids=None: calls.append(
            (old, new, photo_ids)
        ),
    )
    db.rename_photo_preferences_species("A", "B")
    db.rename_photo_preferences_species(
        "A", "B", [(p["robin2"], ws), (p["robin1"], ws), (p["robin2"], 99)]
    )
    assert calls == [
        ("A", "B", None),
        ("A", "B", sorted([p["robin1"], p["robin2"]])),
    ]


@pytest.mark.parametrize("pairs", [None, "selected"])
def test_rename_photo_preferences_leaves_commit_to_caller(db, cur, pairs):
    p, ws = cur["p"], cur["ws"]
    _seed_renames(db, cur)
    if pairs == "selected":
        pairs = [(p["robin1"], ws)]
    db.rename_photo_preferences_species("Old Name", "New Name", pairs)
    assert db.conn.in_transaction
    assert _visible(
        db, "SELECT COUNT(*) FROM photo_preferences WHERE species = 'New Name'"
    ) == [(1,)]


def test_rename_species_representatives_all(db, cur):
    p = cur["p"]
    _seed_renames(db, cur)
    with db.transaction():
        assert db.species_curation.rename_representatives("Old Name", "New Name") == 1
    assert not db.conn.in_transaction
    assert _visible(
        db,
        "SELECT species, photo_id, selected_order FROM species_representatives "
        "ORDER BY photo_id",
    ) == [("New Name", p["robin1"], 1), ("New Name", p["robin2"], 3)]


def test_rename_species_representatives_selected_photos(db, cur):
    p = cur["p"]
    _seed_renames(db, cur)
    with db.transaction():
        assert db.species_curation.rename_representatives(
            "Old Name", "New Name", photo_ids=[str(p["robin1"])]
        ) == 1
    assert _visible(
        db,
        "SELECT species, photo_id FROM species_representatives "
        "ORDER BY species, photo_id",
    ) == [("New Name", p["robin1"]), ("New Name", p["robin2"]),
          ("Old Name", p["robin2"])]
    statements = _trace(db)
    assert db.species_curation.rename_representatives("Old Name", "New Name",
                                                      photo_ids=[]) == 0
    db.conn.set_trace_callback(None)
    assert statements == []


def test_rename_species_representatives_chunks(db, cur):
    p = cur["p"]
    _seed_renames(db, cur)
    statements = _trace(db)
    moved = db.species_curation.rename_representatives(
        "Old Name", "New Name",
        photo_ids=list(range(10_000, 10_800)) + [p["robin1"], p["robin2"]],
    )
    db.conn.set_trace_callback(None)
    assert moved == 1
    distinct = list(dict.fromkeys(s.strip() for s in statements))
    assert len([s for s in distinct if s.startswith("INSERT OR IGNORE")]) == 2
    assert len([s for s in distinct if s.startswith("DELETE")]) == 2


@pytest.mark.parametrize("photo_ids", [None, "selected"])
def test_rename_species_representatives_leave_commit_to_caller(db, cur, photo_ids):
    _seed_renames(db, cur)
    if photo_ids == "selected":
        photo_ids = [cur["p"]["robin1"]]
    db.species_curation.rename_representatives("Old Name", "New Name",
                                               photo_ids=photo_ids)
    assert db.conn.in_transaction
    assert _visible(
        db, "SELECT COUNT(*) FROM species_representatives WHERE species = 'Old Name'"
    ) == [(2,)]


def _seed_highlight_renames(db, cur):
    p, ws = cur["p"], cur["ws"]
    other = db.create_workspace("Other")
    db.commit()
    for pid in (p["robin1"], p["robin2"], p["card1"]):
        db.species_curation.add_highlight("Old Name", pid)
    db.species_curation.add_highlight("New Name", p["card2"])
    db.species_curation.add_highlight("New Name", p["robin2"])
    db.set_active_workspace(other)
    db.species_curation.add_highlight("Old Name", p["puma"])
    db.species_curation.add_highlight("Old Name", p["plain"])
    db.set_active_workspace(ws)
    db.commit()
    return other


def test_rename_species_highlights_all_workspaces(db, cur):
    p, ws = cur["p"], cur["ws"]
    other = _seed_highlight_renames(db, cur)
    with db.transaction():
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
def test_rename_species_highlights_leave_commit_to_caller(db, cur, pairs):
    p, ws = cur["p"], cur["ws"]
    _seed_highlight_renames(db, cur)
    if pairs == "selected":
        pairs = [(p["robin1"], ws)]
    db.rename_species_highlights_species("Old Name", "New Name", pairs)
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


# -- relabel and eligibility reads --------------------------------------------------------


def test_relabel_snapshot_reads(db, cur):
    p, ws = cur["p"], cur["ws"]
    other = _seed_renames(db, cur)
    db.conn.executemany(
        "INSERT INTO species_highlights (workspace_id, species, photo_id, rank) "
        "VALUES (?, ?, ?, ?)",
        [(ws, "Old Name", p["robin1"], 1), (ws, "Old Name", p["robin2"], 2),
         (ws, "Other Bird", p["card1"], 1), (other, "Old Name", p["card2"], 1)],
    )
    db.conn.commit()
    ids = [p["robin1"], p["robin2"], p["card2"], 987_654]

    assert sorted(tuple(r) for r in db.species_curation.highlight_rows_for_photos(ids)) == sorted(
        [("Old Name", p["robin1"], 1), ("Old Name", p["robin2"], 2)]
    )
    # Only the representative-style purposes, only this workspace.
    assert sorted(tuple(r) for r in db.species_curation.preference_rows_for_photos(ids)) == [
        ("Old Name", p["robin1"], "life_list"),
    ]
    assert db.species_curation.preference_purposes_for_species("Old Name") == {"life_list"}
    assert db.species_curation.preference_purposes_for_species("New Name") == set()
    db.set_active_workspace(other)
    assert db.species_curation.preference_purposes_for_species("Old Name") == {"life_list"}
    assert db.species_curation.preference_purposes_for_species("New Name") == set()  # custom
    db.set_active_workspace(ws)

    # Representatives are global.
    assert db.species_curation.representative_photo_ids_for_species("Old Name", ids) == {
        p["robin1"], p["robin2"],
    }
    assert db.species_curation.representative_photo_ids_for_species("New Name", ids) == {p["robin2"]}
    assert sorted(
        tuple(r) for r in db.species_curation.representative_rows_for_photos(ids)
    ) == sorted([
        ("Old Name", p["robin1"], 1), ("Old Name", p["robin2"], 2),
        ("New Name", p["robin2"], 3),
    ])
    assert not db.conn.in_transaction


def test_relabel_snapshot_reads_chunk_photo_ids(db, cur):
    p = cur["p"]
    _seed_renames(db, cur)
    padded = list(range(1_000_000, 1_000_900)) + [p["robin1"]]
    statements = _trace(db)
    db.species_curation.highlight_rows_for_photos(padded)
    db.species_curation.preference_rows_for_photos(padded)
    assert db.species_curation.representative_photo_ids_for_species("Old Name", padded) == {p["robin1"]}
    assert len(db.species_curation.representative_rows_for_photos(padded)) == 1
    db.conn.set_trace_callback(None)
    distinct = list(dict.fromkeys(statements))
    for needle in ("FROM species_highlights", "FROM photo_preferences",
                   "SELECT photo_id FROM species_representatives",
                   "SELECT species, photo_id, selected_order"):
        assert len([s for s in distinct if needle in s]) == 2, needle


def test_life_list_preference_eligibility(db, cur):
    p, k = cur["p"], cur["k"]
    eligible = db.species_curation.photo_is_life_list_preference_eligible
    assert eligible("American Robin", p["robin1"])
    # Turdus is suppressed on robin1, which also carries its descendant.
    assert not eligible("Turdus", p["robin1"])
    # A linked higher-rank identification on its own is eligible.
    assert eligible("Turdus", p["card2"])
    assert eligible("Mystery Bird", p["plain"])
    assert not eligible("Northern Cardinal", p["robin1"])
    assert not eligible("Backyard", p["robin1"])  # location keyword
    assert not eligible("Northern Cardinal", p["rejected"])
    assert not eligible("Northern Cardinal", p["outside"])
    # A hierarchy leaf matches through its taxon's root identification.
    leaf = _leaf(db, "robin (leaf)", k["sunset"])
    _link(db, leaf, 4)
    db.tag_photo(p["card1"], leaf)
    db.commit()
    assert eligible("American Robin", p["card1"])
    assert eligible("robin (leaf)", p["card1"])
    assert not db.conn.in_transaction


# -- structure ----------------------------------------------------------------------------


# Still forwarders on ``Database``: ``KeywordTreeRepository`` and
# ``KeywordProvenanceRepository`` bind these from the façade by name
# (``FACADE_METHODS``), so patches of the ``Database`` methods reach the
# keyword renames and merges that call them.
_DELEGATING_SPECIES_CURATION_METHODS = (
    "rename_photo_preferences_species",
    "rename_species_highlights_species",
)

# The forwarding wrappers this domain used to have on ``Database``; callers
# reach the same methods through ``db.species_curation`` now.
_REMOVED_SPECIES_CURATION_WRAPPERS = (
    "_next_species_representative_order",
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
    "rename_species_representatives_species",
    "get_species_highlight_rows_for_photos",
    "get_photo_preference_purposes_for_species",
    "get_photo_preference_rows_for_photos",
    "get_representative_photo_ids_for_species",
    "get_species_representative_rows_for_photos",
    "is_photo_life_list_preference_eligible",
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


def test_species_curation_has_no_other_forwarding_wrappers_on_database():
    """The domain is reached through ``db.species_curation``; Database keeps no aliases."""
    for name in _REMOVED_SPECIES_CURATION_WRAPPERS:
        assert not hasattr(Database, name), (
            f"Database.{name} came back; call db.species_curation"
        )
    accessor = Database.__dict__["species_curation"]
    assert isinstance(accessor, property)
    attrs = _self_attrs(accessor.fget)
    assert "_species_curation_repository" in attrs
    assert "conn" not in attrs


def test_species_curation_accessor_builds_a_fresh_repository(db):
    first = db.species_curation
    assert db.species_curation is not first
    assert first.conn is db.conn
    # The workspace is still resolved lazily, at each read.
    db.set_active_workspace(None)
    repo = db.species_curation
    with pytest.raises(RuntimeError):
        repo.workspace_id  # noqa: B018


def test_species_curation_repository_never_touches_keyword_writers():
    """Keyword provenance writers stay on the façade (see
    ``test_keyword_provenance_contract``); curation must not reach them."""
    import repositories.species_curation as module

    source = inspect.getsource(module)
    for writer in ("tag_photo", "untag_photo", "_merge_keyword_into",
                   "link_keyword_to_place", "photo_keywords ("):
        assert writer not in source


def test_species_curation_signatures_unchanged():
    from repositories.species_curation import SpeciesCurationRepository

    sig = {n: calling_signature(getattr(Database, n))
           for n in _DELEGATING_SPECIES_CURATION_METHODS}
    sig.update({
        f"repo.{n}": str(inspect.signature(getattr(SpeciesCurationRepository, n)))
        for n in ("get_highlights_candidates", "set_photo_preference",
                  "move_highlight", "rename_representatives",
                  "restore_representative", "get_taxon_subtree",
                  "get_explorer_root", "get_representative_lists",
                  "get_highlights")
    })
    assert sig["repo.get_highlights_candidates"] == (
        "(self, folder_id, min_quality=0.0, photo_id=None)"
    )
    assert sig["repo.set_photo_preference"] == (
        "(self, purpose, species, photo_id)"
    )
    assert sig["repo.move_highlight"] == (
        "(self, species, photo_id, direction)"
    )
    assert sig["rename_photo_preferences_species"] == (
        "(self, old_species, new_species, photo_workspace_pairs=None)"
    )
    assert sig["rename_species_highlights_species"] == (
        "(self, old_species, new_species, photo_workspace_pairs=None)"
    )
    assert sig["repo.rename_representatives"] == (
        "(self, old_species, new_species, photo_ids=None)"
    )
    assert sig["repo.restore_representative"] == (
        "(self, species, photo_id, selected_order=None)"
    )
    assert sig["repo.get_taxon_subtree"] == "(self, root_id, max_depth=12)"
    assert sig["repo.get_explorer_root"] == "(self, name='Aves', rank='class')"
    assert sig["repo.get_representative_lists"] == (
        "(self, eligible_only=False, species=None)"
    )
    assert sig["repo.get_highlights"] == (
        "(self, species=None, eligible_only=False)"
    )
