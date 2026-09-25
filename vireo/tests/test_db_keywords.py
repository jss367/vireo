"""Behavior pins for the keywords domain of ``Database``.

The tests go through the public ``Database`` façade only, so they hold
whether the SQL lives in ``db.py`` or in ``repositories/keywords.py``. They
pin the ``_commit=False`` nested-transaction seams (the caller's
transaction stays open and nothing is committed), commit visibility, the
all-or-nothing rollbacks of the one-shot sweeps, species-name resolution and
case conventions, taxon lookup, the source-taxon species path, the rename
and duplicate-merge flows (which still run through the provenance-pinned
``_merge_keyword_into`` on the façade), and that composition still routes
through ``Database`` so monkeypatches take effect.
"""

import ast
import contextlib
import inspect
import json
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


def _kw(db, kid):
    row = db.conn.execute(
        "SELECT name, parent_id, type, is_species, taxon_id, source_taxon_id "
        "FROM keywords WHERE id = ?",
        (kid,),
    ).fetchone()
    return tuple(row) if row else None


def _raw_kw(db, name, parent_id=None, kw_type="general", is_species=0, taxon_id=None):
    """Insert a keyword row verbatim (``add_keyword`` would normalize it)."""
    return db.conn.execute(
        "INSERT INTO keywords (name, parent_id, type, is_species, taxon_id) "
        "VALUES (?, ?, ?, ?, ?)",
        (name, parent_id, kw_type, is_species, taxon_id),
    ).lastrowid


def _taxon(db, tid, name, rank, parent_id=None, common_name=None, inat_id=None):
    db.conn.execute(
        "INSERT INTO taxa (id, inat_id, name, common_name, rank, parent_id) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (tid, inat_id, name, common_name, rank, parent_id),
    )


@pytest.fixture
def case(tmp_path, monkeypatch):
    """Isolated config; ``case("lower")`` sets the keyword_case override."""
    import config as cfg

    path = tmp_path / "config.json"
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(path))

    def set_case(value):
        path.write_text(json.dumps({"keyword_case": value}))

    set_case("auto")
    return set_case


@pytest.fixture
def lib(db, case):
    ws = db._active_workspace_id
    fid = db.add_folder("/kw", name="kw")
    photos = [db.add_photo(fid, f"p{i}.jpg", ".jpg", 10 + i, float(i)) for i in range(4)]
    _taxon(db, 1, "Aves", "class")
    _taxon(db, 2, "Turdus", "genus", 1)
    _taxon(db, 3, "Turdus migratorius", "species", 2, "American Robin", inat_id=12727)
    _taxon(db, 4, "Puma", "genus")
    _taxon(db, 5, "Puma concolor", "species", 4, "Cougar", inat_id=42007)
    db.conn.commit()
    return {"ws": ws, "fid": fid, "p": photos}


# -- add_keyword and the _commit seams ----------------------------------------------------


def test_add_keyword_inserts_and_commits(db, lib):
    kid = db.add_keyword("Driftwood")
    assert not db.conn.in_transaction
    assert _visible(db, "SELECT name, type FROM keywords WHERE id = ?", (kid,)) == [
        ("Driftwood", "general")
    ]
    # Case-insensitive reuse.
    assert db.add_keyword("driftwood") == kid


def test_add_keyword_without_commit_leaves_transaction_open(db, lib):
    kid = db.add_keyword("Driftwood", _commit=False)
    assert db.conn.in_transaction
    assert _visible(db, "SELECT COUNT(*) FROM keywords WHERE id = ?", (kid,)) == [(0,)]
    db.conn.commit()
    assert _visible(db, "SELECT COUNT(*) FROM keywords WHERE id = ?", (kid,)) == [(1,)]


def _race_add(db, monkeypatch, insert):
    """Run ``insert(other)`` on a second connection right after ``db``'s lookup."""
    from repositories.keywords import KeywordRepository

    other = Database(db._db_path)
    other.set_active_workspace(db._active_workspace_id)
    real = KeywordRepository._find_add_candidate
    state = {"fired": False, "winner": None}

    def racing(self, *args, **kwargs):
        found = real(self, *args, **kwargs)
        if not state["fired"]:
            state["fired"] = True
            state["winner"] = insert(other)
        return found

    monkeypatch.setattr(KeywordRepository, "_find_add_candidate", racing)
    return other, state


def test_add_keyword_race_reuses_concurrent_top_level_row(db, lib, monkeypatch):
    # UNIQUE(name, parent_id) never fires for NULL parents, so without the
    # write-locked re-check both callers would insert a root "Sunrise".
    other, state = _race_add(db, monkeypatch, lambda o: o.add_keyword("Sunrise"))
    try:
        kid = db.add_keyword("sunrise")
    finally:
        other.close()
    assert kid == state["winner"]
    assert not db.conn.in_transaction
    assert _visible(
        db, "SELECT COUNT(*) FROM keywords WHERE name = 'sunrise' COLLATE NOCASE",
    ) == [(1,)]


def test_add_keyword_race_reuses_concurrent_child_row(db, lib, monkeypatch):
    # With a parent the UNIQUE index does fire; the loser must reuse the
    # winner's row instead of raising IntegrityError.
    birds = db.add_keyword("Birds")
    other, state = _race_add(
        db, monkeypatch, lambda o: o.add_keyword("Heron", parent_id=birds),
    )
    try:
        kid = db.add_keyword("Heron", parent_id=birds)
    finally:
        other.close()
    assert kid == state["winner"]
    assert _visible(
        db, "SELECT COUNT(*) FROM keywords WHERE name = 'Heron' AND parent_id = ?",
        (birds,),
    ) == [(1,)]


@pytest.mark.parametrize("kwargs", [
    {"is_species": True},
    {"kw_type": "genre"},
    {"kw_type": "taxonomy"},
])
def test_add_keyword_promotions_respect_commit_flag(db, lib, kwargs):
    kid = db.add_keyword("Heron")
    db.add_keyword("Heron", _commit=False, **kwargs)
    assert db.conn.in_transaction
    assert _visible(db, "SELECT type, is_species FROM keywords WHERE id = ?", (kid,)) == [
        ("general", 0)
    ]
    db.conn.rollback()
    got = db.add_keyword("Heron", **kwargs)
    assert got == kid
    assert not db.conn.in_transaction
    want_type = kwargs.get("kw_type", "taxonomy")
    assert _visible(db, "SELECT type FROM keywords WHERE id = ?", (kid,)) == [(want_type,)]


def test_add_keyword_auto_promotes_general_taxon_match(db, lib):
    kid = _raw_kw(db, "American Robin")
    db.conn.commit()
    assert db.add_keyword("American Robin", _commit=False) == kid
    assert db.conn.in_transaction
    assert _visible(db, "SELECT type FROM keywords WHERE id = ?", (kid,)) == [("general",)]
    db.conn.rollback()
    assert db.add_keyword("American Robin") == kid
    assert _visible(db, "SELECT type, is_species, taxon_id FROM keywords WHERE id = ?",
                    (kid,)) == [("taxonomy", 1, 3)]


def test_add_keyword_rejects_bad_input(db, lib):
    with pytest.raises(ValueError, match="invalid keyword type"):
        db.add_keyword("x", kw_type="bogus")
    with pytest.raises(ValueError, match="empty after normalization"):
        db.add_keyword("'")
    with pytest.raises(ValueError, match="requires kw_type='taxonomy'"):
        db.add_keyword("x", is_species=True, kw_type="genre")
    with pytest.raises(ValueError, match="source_taxon_id requires a species keyword"):
        db.add_keyword("x", source_taxon_id=5)


def test_add_keyword_species_links_species_rank_taxon(db, lib):
    kid = db.add_keyword("american robin", is_species=True)
    assert _kw(db, kid)[2:5] == ("taxonomy", 1, 3)
    # A genus-only match leaves taxon_id NULL on an explicit species add.
    puma = db.add_keyword("Puma", is_species=True)
    assert _kw(db, puma)[2:5] == ("taxonomy", 1, None)
    # A general add links a higher-rank match and types it taxonomy.
    genus = db.add_keyword("Turdus")
    assert _kw(db, genus)[2:5] == ("taxonomy", 1, 2)


def test_add_keyword_backfills_existing_species_taxon(db, lib):
    kid = _raw_kw(db, "American Robin", kw_type="taxonomy", is_species=1, taxon_id=2)
    db.conn.commit()
    assert db.add_keyword("American Robin", is_species=True) == kid
    assert _kw(db, kid)[4] == 3


@pytest.mark.parametrize("override,expected", [
    ("lower", "Black phoebe"),
    ("title", "Black Phoebe"),
])
def test_add_keyword_case_override_for_new_species(db, lib, case, override, expected):
    case(override)
    kid = db.add_keyword("black PHOEBE", is_species=True)
    assert _kw(db, kid)[0] == expected


def test_add_keyword_detected_case_convention(db, lib):
    for name in ("Black phoebe", "Say's phoebe", "Snowy egret"):
        _raw_kw(db, name, kw_type="taxonomy", is_species=1)
    db.conn.commit()
    kid = db.add_keyword("Great Blue Heron", is_species=True)
    assert _kw(db, kid)[0] == "Great blue heron"


def test_add_keyword_resolve_alias_short_circuits(db, lib, monkeypatch):
    import db as dbmod

    calls = []
    monkeypatch.setattr(dbmod, "resolve_import_alias",
                        lambda d, name, parent, kw_type=None: calls.append(
                            (d, name, parent, kw_type)) or 77)
    assert db.add_keyword("Leaf", parent_id=None, _resolve_alias=True) == 77
    # The helper receives the Database itself, not some other handle.
    assert calls == [(db, "Leaf", None, None)]


def test_add_keyword_routes_lookup_and_source_path_through_facade(db, lib, monkeypatch):
    seen = []
    monkeypatch.setattr(db, "_lookup_taxon_id_for_keyword",
                        lambda name, **kw: seen.append((name, kw)) or None)
    db.add_keyword("Mystery")
    assert seen == [("Mystery", {"prefer_species": True})]
    src = []
    monkeypatch.setattr(db, "_add_source_species_keyword",
                        lambda *a: src.append(a) or 99)
    assert db.add_keyword("Robin", is_species=True, source_taxon_id=12727,
                          _commit=False) == 99
    assert src == [("Robin", 12727, None, False)]


# -- source-taxon species keywords ---------------------------------------------------------


def test_add_source_species_keyword_validates_id(db, lib):
    for bad in (0, -1, 1 << 63, "5", 5.0, True):
        with pytest.raises(ValueError, match="positive SQLite integer"):
            db._add_source_species_keyword("x", bad)


def test_add_source_species_keyword_rebinds_existing(db, lib):
    kid = _raw_kw(db, "Robin", kw_type="general", taxon_id=3)
    db.conn.commit()
    got = db._add_source_species_keyword("Robin", 12727, _commit=False)
    assert got == kid
    assert db.conn.in_transaction
    assert _kw(db, kid)[2:] == ("taxonomy", 1, 3, 12727)
    assert _visible(db, "SELECT source_taxon_id FROM keywords WHERE id = ?", (kid,)) == [(None,)]
    db.conn.commit()


def test_add_source_species_keyword_suffixes_same_name_rows(db, lib, monkeypatch):
    import species_identity

    class _Identity:
        display_name = "Cougar"

    resolver_dbs = []

    class _Resolver:
        def __init__(self, db):
            resolver_dbs.append(db)

        def resolve(self, name, source):
            return _Identity()

    monkeypatch.setattr(species_identity, "SpeciesResolver", _Resolver)
    _raw_kw(db, "Cougar", kw_type="location")
    _raw_kw(db, "Cougar (taxon 42007)", kw_type="location")
    db.conn.commit()
    kid = db._add_source_species_keyword("Cougar (taxon 42007)", 42007)
    assert not db.conn.in_transaction
    assert _kw(db, kid) == ("Cougar (taxon 42007) (2)", None, "taxonomy", 1, 5, 42007)
    assert resolver_dbs == [db]


def test_relink_source_species_keywords(db, lib):
    kid = _raw_kw(db, "Robin", kw_type="taxonomy", is_species=1)
    db.conn.execute("UPDATE keywords SET source_taxon_id = 12727 WHERE id = ?", (kid,))
    other = _raw_kw(db, "Ghost", kw_type="taxonomy", is_species=1)
    db.conn.execute("UPDATE keywords SET source_taxon_id = 999 WHERE id = ?", (other,))
    db.conn.commit()
    db.relink_source_species_keywords()
    assert db.conn.in_transaction  # caller commits
    assert _kw(db, kid)[4] == 3
    assert _kw(db, other)[4] is None


# -- species-name resolution ---------------------------------------------------------------


def test_resolve_species_display_name_cases(db, lib):
    assert db.resolve_species_display_name("'") == ""
    _raw_kw(db, "Common waxbill", kw_type="taxonomy", is_species=1)
    assert db.resolve_species_display_name("COMMON WAXBILL") == "Common waxbill"
    # Two species-bearing homonyms: caller's spelling wins when it matches
    # one, else it is kept verbatim.
    _raw_kw(db, "Robin", kw_type="general", is_species=1)
    _raw_kw(db, "robin", kw_type="taxonomy", is_species=1)
    assert db.resolve_species_display_name("Robin") == "Robin"
    assert db.resolve_species_display_name("ROBIN") == "ROBIN"
    # A general-only row keeps its own spelling.
    _raw_kw(db, "Sunset glow")
    assert db.resolve_species_display_name("sunset GLOW") == "Sunset glow"


def test_resolve_species_display_name_hierarchy_leaf(db, lib):
    parent = _raw_kw(db, "Birds")
    _raw_kw(db, "Desert Verdin", parent, "taxonomy", 1, taxon_id=3)
    db.conn.commit()
    # No root row for the taxon: the leaf's own spelling.
    assert db.resolve_species_display_name("desert verdin") == "Desert Verdin"
    _raw_kw(db, "Verdin", None, "taxonomy", 1, taxon_id=3)
    assert db.resolve_species_display_name("desert verdin") == "Verdin"


def test_resolve_species_display_name_case_convention_paths(db, lib, case, monkeypatch):
    assert db.resolve_species_display_name("bubulcus ibis",
                                           apply_case_convention=False) == "bubulcus ibis"
    assert db.resolve_species_display_name("bubulcus ibis",
                                           case_convention="title") == "Bubulcus Ibis"
    assert db.resolve_species_display_name("bubulcus ibis",
                                           case_convention=None) == "bubulcus ibis"
    case("lower")
    assert db.resolve_species_display_name("Bubulcus Ibis") == "Bubulcus ibis"
    calls = []
    monkeypatch.setattr(db, "species_case_convention", lambda: calls.append(1) or "title")
    assert db.resolve_species_display_name("snowy egret") == "Snowy Egret"
    assert calls == [1]


def test_species_case_convention(db, lib, case):
    assert db.species_case_convention() is None
    case("title")
    assert db.species_case_convention() == "title"


def test_detect_keyword_case_convention(db, lib):
    assert db.detect_keyword_case_convention() is None
    for name in ("Black Phoebe", "Snowy Egret", "Heron"):
        _raw_kw(db, name, kw_type="taxonomy", is_species=1)
    assert db.detect_keyword_case_convention() == "title"
    for name in ("Great egret", "Say's phoebe", "Tree swallow"):
        _raw_kw(db, name, kw_type="taxonomy", is_species=1)
    assert db.detect_keyword_case_convention() == "lower"
    _raw_kw(db, "Blue Jay", kw_type="taxonomy", is_species=1)
    assert db.detect_keyword_case_convention() is None
    _raw_kw(db, "Emu", kw_type="taxonomy", is_species=1)
    assert db.detect_keyword_case_convention() is None


def test_apply_case_convention_helpers(db):
    assert db._apply_case_convention("mcKay's BUNTING", "lower") == "McKay's bunting"
    assert db._apply_case_convention("HERON", "lower") == "Heron"
    assert db._apply_case_convention("snowy egret", "title") == "Snowy Egret"
    assert db._apply_case_convention("snowy egret", "upper") == "snowy egret"
    assert Database._sentence_case_first_word("") == ""
    assert Database._sentence_case_first_word("123") == "123"


def test_species_root_name_for_taxon(db, lib):
    assert db._species_root_name_for_taxon(None) is None
    assert db._species_root_name_for_taxon(3) is None
    _raw_kw(db, "American Robin", kw_type="taxonomy", is_species=1, taxon_id=3)
    assert db._species_root_name_for_taxon(3) == "American Robin"


def test_lookup_taxon_id_for_keyword(db, lib):
    assert db._lookup_taxon_id_for_keyword("american robin") == 3
    assert db._lookup_taxon_id_for_keyword("Nope") is None
    # prefer_species falls back to the higher-rank direct hit.
    assert db._lookup_taxon_id_for_keyword("Puma", prefer_species=True) == 4
    assert db._lookup_taxon_id_for_keyword("Puma", species_only=True) is None
    db.conn.execute(
        "INSERT INTO taxa_common_names (taxon_id, name) VALUES (5, 'Mountain lion')"
    )
    db.conn.execute(
        "INSERT INTO taxa_common_names (taxon_id, name) VALUES (4, 'Big cats')"
    )
    assert db._lookup_taxon_id_for_keyword("mountain lion") == 5
    assert db._lookup_taxon_id_for_keyword("mountain lion", species_only=True) == 5
    assert db._lookup_taxon_id_for_keyword("big cats", prefer_species=True) == 4
    assert db._lookup_taxon_id_for_keyword("big cats", species_only=True) is None


# -- tagging -------------------------------------------------------------------------------


def test_tag_and_untag_commit_flags(db, lib):
    p = lib["p"][0]
    kid = db.add_keyword("Sunset")
    db.tag_photo(p, kid, _commit=False)
    assert db.conn.in_transaction
    assert _visible(db, "SELECT COUNT(*) FROM photo_keywords") == [(0,)]
    db.conn.commit()
    db.untag_photo(p, kid, _commit=False)
    assert db.conn.in_transaction
    assert _visible(db, "SELECT COUNT(*) FROM photo_keywords") == [(1,)]
    db.conn.rollback()
    db.untag_photo(p, kid)
    assert not db.conn.in_transaction
    assert _visible(db, "SELECT COUNT(*) FROM photo_keywords") == [(0,)]


def test_photo_keyword_reads(db, lib):
    p0, p1, p2, _ = lib["p"]
    a = db.add_keyword("Alpha")
    b = db.add_keyword("American Robin", is_species=True)
    db.tag_photo(p0, a)
    db.tag_photo(p0, b)
    db.tag_photo(p1, b)
    assert [tuple(r) for r in db.get_photo_keywords(p0)] == [
        (a, "Alpha", None, "general"), (b, "American Robin", None, "taxonomy"),
    ]
    assert db.get_keywords_for_photos([]) == {}
    got = db.get_keywords_for_photos([p0, p1, p0, p2])
    assert sorted(got) == [p0, p1]
    assert [k["name"] for k in got[p0]] == ["Alpha", "American Robin"]
    assert got[p1][0]["taxon_rank"] == "species"


def test_keywords_for_photos_chunks(db, lib):
    p0 = lib["p"][0]
    db.tag_photo(p0, db.add_keyword("Alpha"))
    statements = _trace(db)
    got = db.get_keywords_for_photos(list(range(10_000, 10_800)) + [p0])
    db.conn.set_trace_callback(None)
    assert list(got) == [p0]
    assert len([s for s in dict.fromkeys(statements) if "pk.photo_id IN" in s]) == 2


def test_species_keywords_and_equivalents(db, lib):
    p0, p1, p2, p3 = lib["p"]
    root = db.add_keyword("American Robin", is_species=True)
    birds = db.add_keyword("Birds")
    leaf = _raw_kw(db, "american robin", birds, "taxonomy", 1, taxon_id=3)
    genus = db.add_keyword("Turdus")
    db.tag_photo(p0, root)
    db.tag_photo(p1, leaf)
    db.tag_photo(p2, genus)
    got = db.get_species_keywords_for_photos([p0, p1, p2, p3])
    assert got[p0] == ["American Robin"]
    assert got[p1] == ["American Robin"]
    assert p2 not in got
    assert db.get_photos_with_equivalent_species([p0, p1, p2], root) == {p0, p1}
    assert db.get_photos_with_equivalent_species([p0, p1], root,
                                                 exclude_keyword_ids=[leaf]) == {p0}
    assert db.get_photos_with_equivalent_species([p0], 999_999) == set()
    assert db.get_photos_with_equivalent_species([], root) == set()


def test_is_keyword_species(db, lib):
    sp = db.add_keyword("American Robin", is_species=True)
    genus = db.add_keyword("Turdus")
    gen = db.add_keyword("Sunset")
    assert db.is_keyword_species(sp)
    assert not db.is_keyword_species(genus)
    assert not db.is_keyword_species(gen)
    assert not db.is_keyword_species(999_999)


def test_filter_out_subject_tagged(db, lib, monkeypatch):
    p0, p1, p2, p3 = lib["p"]
    db.tag_photo(p0, db.add_keyword("American Robin", is_species=True))
    legacy = _raw_kw(db, "Legacy Bird", kw_type="general", is_species=1)
    db.conn.commit()
    db.tag_photo(p1, legacy)
    db.tag_photo(p2, db.add_keyword("Party", kw_type="genre"))
    ids = [p3, p2, p1, p0]
    assert db.filter_out_subject_tagged(ids, []) == ids
    assert db.filter_out_subject_tagged([], ["taxonomy"]) == []
    assert db.filter_out_subject_tagged(ids, ["bogus"]) == ids
    assert db.filter_out_subject_tagged(ids, ["taxonomy"]) == [p3, p2]
    assert db.filter_out_subject_tagged(ids, ["genre"]) == [p3, p1, p0]
    monkeypatch.setattr(Database, "_FILTER_SUBJECT_CHUNK", 2)
    statements = _trace(db)
    assert db.filter_out_subject_tagged(tuple(ids), ["taxonomy", "genre"]) == [p3]
    db.conn.set_trace_callback(None)
    assert len([s for s in statements if "pk.photo_id IN" in s]) == 2


def test_keyword_tree_and_counts(db, lib):
    p0, p1 = lib["p"][:2]
    birds = db.add_keyword("Birds")
    heron = db.add_keyword("Heron", parent_id=birds)
    db.add_keyword("Unused")
    db.tag_photo(p0, heron)
    tree = [tuple(r) for r in db.get_keyword_tree()]
    assert tree == [(birds, "Birds", None, "general"), (heron, "Heron", birds, "general")]
    assert db.count_keywords() == 1
    assert db.count_keywords_in_workspace() == 1
    db.tag_photo(p1, birds)
    db.conn.execute("UPDATE folders SET status = 'missing' WHERE id = ?", (lib["fid"],))
    assert db.count_keywords() == 0
    assert db.count_keywords_in_workspace() == 2
    rows = {r["name"]: r for r in db.get_all_keywords()}
    assert set(rows) == {"Birds", "Heron"}
    db.set_active_workspace(None)
    for method in (db.get_keyword_tree, db.count_keywords, db.count_keywords_in_workspace,
                   db.get_all_keywords, db.get_accepted_species):
        with pytest.raises(RuntimeError):
            method()


def test_get_accepted_species(db, lib):
    p0, p1, p2 = lib["p"][:3]
    db.tag_photo(p0, db.add_keyword("American Robin", is_species=True))
    db.tag_photo(p1, db.add_keyword("Cougar", is_species=True))
    db.tag_photo(p2, db.add_keyword("Egret", is_species=True))
    db.conn.execute("UPDATE photos SET latitude = 1, longitude = 2 WHERE id = ?", (p0,))
    place = db.add_keyword("Park", kw_type="location")
    db.conn.execute("UPDATE keywords SET latitude = 1, longitude = 2 WHERE id = ?", (place,))
    db.tag_photo(p1, place)
    assert db.get_accepted_species() == ["American Robin", "Cougar"]


# -- seeds and legacy migrations ----------------------------------------------------------


def test_ensure_default_genre_keywords_is_idempotent(db):
    before = db.conn.execute("SELECT COUNT(*) FROM keywords WHERE type = 'genre'").fetchone()[0]
    db.ensure_default_genre_keywords()
    assert not db.conn.in_transaction
    after = db.conn.execute("SELECT COUNT(*) FROM keywords WHERE type = 'genre'").fetchone()[0]
    assert after == before


def test_migrate_legacy_keyword_types(db):
    ids = {
        legacy: _raw_kw(db, f"Legacy {legacy}", kw_type=legacy)
        for legacy in ("people", "descriptive", "event", "location")
    }
    db.conn.commit()
    db.migrate_legacy_keyword_types()
    assert not db.conn.in_transaction
    got = {
        legacy: _visible(db, "SELECT type FROM keywords WHERE id = ?", (kid,))[0][0]
        for legacy, kid in ids.items()
    }
    assert got == {
        "people": "individual",
        "descriptive": "general",
        "event": "general",
        "location": "location",
    }
    # Warm path: nothing legacy left, so only the probe runs.
    statements = _trace(db)
    db.migrate_legacy_keyword_types()
    db.conn.set_trace_callback(None)
    assert [s for s in statements if s.strip()] == [
        "SELECT 1 FROM keywords WHERE type IN ('people', 'descriptive', 'event') LIMIT 1"
    ]


# -- update_keyword ------------------------------------------------------------------------


def test_update_keyword_noop_and_invalid(db, lib):
    kid = db.add_keyword("Sunset")
    statements = _trace(db)
    assert db.update_keyword(kid, bogus=1) == kid
    db.conn.set_trace_callback(None)
    assert statements == []
    with pytest.raises(ValueError, match="Invalid keyword type"):
        db.update_keyword(kid, type="bogus")
    with pytest.raises(ValueError, match="Invalid keyword type"):
        db.update_keyword(kid, type=["taxonomy"])
    with pytest.raises(ValueError, match="empty after normalization"):
        db.update_keyword(kid, name="'")


def test_update_keyword_plain_fields_commit(db, lib):
    kid = db.add_keyword("Sunset")
    assert db.update_keyword(kid, latitude=1.5, longitude=2.5) == kid
    assert not db.conn.in_transaction
    assert _visible(db, "SELECT latitude, longitude FROM keywords WHERE id = ?",
                    (kid,)) == [(1.5, 2.5)]


def test_update_keyword_rename_promotes_and_links(db, lib):
    kid = db.add_keyword("Mystery")
    assert db.update_keyword(kid, name="american robin") == kid
    assert _kw(db, kid)[:5] == ("american robin", None, "taxonomy", 1, 3)
    tax = db.add_keyword("Cougar", is_species=True)
    db.update_keyword(tax, name="Turdus migratorius")
    assert _kw(db, tax)[4] == 3
    db.update_keyword(tax, name="Something", type="location")
    assert _kw(db, tax)[2:4] == ("location", 0)


def test_update_keyword_retype_resets_is_species(db, lib):
    kid = db.add_keyword("Robin", is_species=True)
    db.update_keyword(kid, type="taxonomy")  # no transition: untouched
    assert _kw(db, kid)[3] == 1
    db.update_keyword(kid, type="individual")
    assert _kw(db, kid)[2:4] == ("individual", 0)


def test_update_keyword_merges_into_same_slot_peer(db, lib, monkeypatch):
    p0 = lib["p"][0]
    keep = db.add_keyword("Heron")
    dup = db.add_keyword("Egret")
    db.tag_photo(p0, dup)
    merges = []
    real = db._merge_keyword_into

    def recording(src, dst, **kwargs):
        merges.append((src, dst))
        return real(src, dst, **kwargs)

    monkeypatch.setattr(db, "_merge_keyword_into", recording)
    assert db.update_keyword(dup, name="heron") == keep
    assert merges == [(dup, keep)]
    assert not db.conn.in_transaction
    assert _visible(db, "SELECT keyword_id FROM photo_keywords") == [(keep,)]
    birds = db.add_keyword("Birds")
    a = db.add_keyword("A", parent_id=birds)
    b = db.add_keyword("B", parent_id=birds)
    assert db.update_keyword(b, name="a") == a
    assert merges[-1] == (b, a)


def test_update_keyword_merge_keeps_unrelated_pending_removal(db, lib):
    # P carries Egret and Heron, then drops Egret (queued for the sidecar).
    # Renaming Egret onto Heron must not turn that queued removal into
    # "remove Heron": P is still tagged Heron.
    p0, p1 = lib["p"][:2]
    ws = lib["ws"]
    heron = db.add_keyword("Heron")
    egret = db.add_keyword("Egret")
    db.tag_photo(p0, heron)
    db.tag_photo(p0, egret)
    db.tag_photo(p1, egret)
    db.untag_photo(p0, egret)
    db.queue_change(p0, "keyword_remove", "Egret", workspace_id=ws)
    db.queue_change(p1, "keyword_add", "Egret", workspace_id=ws)

    assert db.update_keyword(egret, name="Heron") == heron

    rows = sorted(
        tuple(r) for r in db.conn.execute(
            "SELECT photo_id, change_type, value FROM pending_changes"
        ).fetchall()
    )
    # p1 carried the merged-away spelling, so its add follows the survivor.
    assert rows == sorted([
        (p0, "keyword_remove", "Egret"),
        (p1, "keyword_add", "Heron"),
    ])
    assert _visible(
        db, "SELECT keyword_id FROM photo_keywords WHERE photo_id = ?", (p0,),
    ) == [(heron,)]


def test_merge_duplicate_keywords_keeps_unrelated_pending_removal(db, lib):
    p0, p1 = lib["p"][:2]
    ws = lib["ws"]
    keep = _raw_kw(db, "Heron")
    dup = _raw_kw(db, "heron")
    db.conn.commit()
    db.tag_photo(p0, keep)
    db.tag_photo(p1, dup)
    db.queue_change(p0, "keyword_remove", "heron", workspace_id=ws)

    assert db.merge_duplicate_keywords() >= 1

    assert _kw(db, dup) is None
    assert _visible(
        db, "SELECT change_type, value FROM pending_changes WHERE photo_id = ?", (p0,),
    ) == [("keyword_remove", "heron")]


def test_update_keyword_cross_type_child_conflict(db, lib):
    birds = db.add_keyword("Birds")
    db.add_keyword("Place", parent_id=birds, kw_type="location")
    child = db.add_keyword("Other", parent_id=birds)
    with pytest.raises(ValueError, match="'location' keyword with that name"):
        db.update_keyword(child, name="place")
    # Top-level cross-type homonyms may coexist.
    db.add_keyword("Spot", kw_type="location")
    top = db.add_keyword("Else")
    assert db.update_keyword(top, name="Spot") == top


def test_update_keyword_missing_row(db, lib):
    assert db.update_keyword(999_999, name="Ghost") == 999_999


# -- duplicate merge and normalization sweeps ---------------------------------------------


def test_merge_duplicate_keywords_merges_and_commits(db, lib, monkeypatch):
    p0, p1 = lib["p"][:2]
    a = _raw_kw(db, "Heron")
    b = _raw_kw(db, "heron")
    c = _raw_kw(db, "Heron!")  # different match key? normalize strips edge punct
    db.conn.commit()
    db.tag_photo(p0, a)
    db.tag_photo(p1, b)
    db.tag_photo(p1, c)
    merges = []
    real = db._merge_keyword_into
    monkeypatch.setattr(db, "_merge_keyword_into",
                        lambda s, d, **kw: merges.append((s, d)) or real(s, d, **kw))
    total = db.merge_duplicate_keywords()
    assert total >= 1
    assert all(dst == a for _src, dst in merges)
    assert not db.conn.in_transaction
    assert db.merge_duplicate_keywords() == 0


def test_merge_duplicate_keywords_rolls_back_on_failure(db, lib, monkeypatch):
    p0, p1 = lib["p"][:2]
    a = _raw_kw(db, "Heron")
    b = _raw_kw(db, "heron")
    db.conn.commit()
    db.tag_photo(p0, a)
    db.tag_photo(p1, b)

    def boom(src, dst, **kwargs):
        db.conn.execute("UPDATE keywords SET name = 'changed' WHERE id = ?", (dst,))
        raise RuntimeError("merge failed")

    monkeypatch.setattr(db, "_merge_keyword_into", boom)
    with pytest.raises(RuntimeError, match="merge failed"):
        db.merge_duplicate_keywords()
    assert not db.conn.in_transaction
    assert _kw(db, a)[0] == "Heron"


def test_merge_duplicate_pass_skips_blank_keys_and_stale_groups(db, lib, monkeypatch):
    p0, p1, p2 = lib["p"][:3]
    blank = _raw_kw(db, "'")
    a = _raw_kw(db, "Heron")
    b = _raw_kw(db, "heron")
    db.conn.commit()
    db.tag_photo(p0, blank)
    db.tag_photo(p1, a)
    db.tag_photo(p2, b)
    calls = []
    monkeypatch.setattr(db, "_normalize_keyword_row_name", lambda kid: calls.append(kid))

    def fake_merge(src, dst, **kwargs):
        db.conn.execute("UPDATE photo_keywords SET keyword_id = ? WHERE keyword_id = ?",
                        (dst, src))
        db.conn.execute("DELETE FROM keywords WHERE id = ?", (src,))
        return 1

    monkeypatch.setattr(db, "_merge_keyword_into", fake_merge)
    assert db._merge_duplicate_keywords_pass(lib["ws"]) == 1
    assert calls == [a]
    assert _kw(db, blank) is not None


def test_merge_duplicate_pass_skips_group_whose_survivor_vanished(db, lib, monkeypatch):
    p = lib["p"]
    parent_a = _raw_kw(db, "Birds")
    parent_b = _raw_kw(db, "birds")
    child_a = _raw_kw(db, "Heron", parent_a)
    child_b = _raw_kw(db, "heron", parent_a)
    db.conn.commit()
    for pid, kid in zip(p, (parent_a, parent_b, child_a, child_b), strict=False):
        db.tag_photo(pid, kid)
    order = []

    def fake_merge(src, dst, **kwargs):
        order.append((src, dst))
        # Deleting the parent-group loser also removes every child row,
        # so the child group's survivor is gone on this pass.
        db.conn.execute("DELETE FROM photo_keywords WHERE keyword_id IN (?, ?, ?)",
                        (src, child_a, child_b))
        db.conn.execute("DELETE FROM keywords WHERE id IN (?, ?)", (child_a, child_b))
        db.conn.execute("DELETE FROM keywords WHERE id = ?", (src,))
        return 1

    monkeypatch.setattr(db, "_merge_keyword_into", fake_merge)
    assert db._merge_duplicate_keywords_pass(lib["ws"]) == 1
    assert order == [(parent_b, parent_a)]


def test_normalize_keyword_row_name(db, lib, monkeypatch):
    db._normalize_keyword_row_name(999_999)
    birds = _raw_kw(db, "Birds")
    clean = _raw_kw(db, "Heron", birds)
    kid = _raw_kw(db, '"Heron"', birds, kw_type="taxonomy")
    db.conn.commit()
    renames = []
    monkeypatch.setattr(db, "_rename_keyword_dependents",
                        lambda k, old, new: renames.append((k, old, new)))
    db._normalize_keyword_row_name(clean)
    db._normalize_keyword_row_name(kid)  # collides with the general 'Heron'
    assert renames == []
    assert _kw(db, kid)[0] == '"Heron"'
    db._normalize_keyword_row_name(kid, disambiguate_on_conflict=True)
    assert _kw(db, kid)[0] == f"Heron (id-{kid})"
    assert renames == [(kid, '"Heron"', f"Heron (id-{kid})")]
    free = _raw_kw(db, "'Egret'")
    db._normalize_keyword_row_name(free)
    assert _kw(db, free)[0] == "Egret"
    assert renames[-1] == (free, "'Egret'", "Egret")


def test_rename_keyword_dependents(db, lib, monkeypatch):
    p0, p1 = lib["p"][:2]
    ws = lib["ws"]
    kid = _raw_kw(db, "'Verdin'", kw_type="taxonomy", is_species=1)
    db.conn.commit()
    db.tag_photo(p0, kid)
    db.conn.executemany(
        "INSERT INTO pending_changes (photo_id, change_type, value, workspace_id) "
        "VALUES (?, ?, ?, ?)",
        [
            (p0, "keyword_add", "'Verdin'", ws),
            (p1, "keyword_add", "'Verdin'", ws),  # photo not tagged: untouched
            (p0, "keyword_remove", "'Verdin'", ws),  # collides: dropped
            (p0, "keyword_remove", "Verdin", ws),
            (p0, "rating", "'Verdin'", ws),  # other change types untouched
        ],
    )
    db.conn.commit()
    calls = []
    monkeypatch.setattr(db, "rename_species_highlights_species",
                        lambda o, n, photo_workspace_pairs=None, _commit=True:
                        calls.append(("hl", o, n, photo_workspace_pairs, _commit)))
    monkeypatch.setattr(db, "rename_photo_preferences_species",
                        lambda o, n, photo_workspace_pairs=None, _commit=True:
                        calls.append(("pref", o, n, photo_workspace_pairs, _commit)))
    statements = _trace(db)
    db._rename_keyword_dependents(kid, "'Verdin'", "'Verdin'")
    db._rename_keyword_dependents(kid, "", "Verdin")
    db.conn.set_trace_callback(None)
    assert statements == []
    db._rename_keyword_dependents(kid, "'Verdin'", "Verdin")
    assert calls == [
        ("hl", "'Verdin'", "Verdin", [(p0, ws)], False),
        ("pref", "'Verdin'", "Verdin", [(p0, ws)], False),
    ]
    rows = sorted(
        tuple(r) for r in db.conn.execute(
            "SELECT photo_id, change_type, value FROM pending_changes"
        ).fetchall()
    )
    assert rows == sorted([
        (p0, "keyword_add", "Verdin"),
        (p1, "keyword_add", "'Verdin'"),
        (p0, "keyword_remove", "Verdin"),
        (p0, "rating", "'Verdin'"),
    ])
    assert db.conn.in_transaction  # caller commits
    db.conn.rollback()
    # An untagged keyword touches no pending rows and no curation.
    lone = _raw_kw(db, "Lone")
    calls.clear()
    db._rename_keyword_dependents(lone, "Lone", "Alone")
    assert calls == []


def test_reparent_disambiguated_routes_through_facade(db, lib, monkeypatch):
    birds = _raw_kw(db, "Birds")
    kid = _raw_kw(db, "Heron (dup)", birds)
    db.conn.commit()
    calls = []
    monkeypatch.setattr(db, "_rename_keyword_dependents",
                        lambda k, old, new: calls.append((k, old, new)))
    other = _raw_kw(db, "Waders")
    child = db.conn.execute("SELECT id, name FROM keywords WHERE id = ?", (kid,)).fetchone()
    db._reparent_disambiguated(child, other, "Heron")
    assert calls == [(kid, "Heron (dup)", "Heron")]
    assert _kw(db, kid)[:2] == ("Heron", other)
    assert db.conn.in_transaction  # caller commits


def _fail_after(db, monkeypatch, name):
    def boom(*a, **k):
        db.conn.execute("INSERT INTO db_meta (key, value) VALUES ('sweep-probe', 'x')")
        raise RuntimeError("sweep failed")

    monkeypatch.setattr(db, name, boom)


@pytest.mark.parametrize("marker,failing", [
    ("keyword_names_normalized", "_normalize_keyword_data_once"),
    ("curation_species_case_aligned_v2", "_align_curation_species_case"),
    ("curation_species_case_aligned_v2", "_align_curation_history_species"),
    ("keyword_apostrophes_folded_v1", "_fold_prediction_species_apostrophes"),
])
def test_normalize_keyword_data_rolls_back_each_sweep(db, monkeypatch, marker, failing):
    db.conn.execute("DELETE FROM db_meta WHERE key = ?", (marker,))
    db.conn.commit()
    _fail_after(db, monkeypatch, failing)
    if failing == "_fold_prediction_species_apostrophes":
        monkeypatch.setattr(db, "_normalize_keyword_data_once", lambda: None)
    with pytest.raises(RuntimeError, match="sweep failed"):
        db.normalize_keyword_data()
    assert not db.conn.in_transaction
    assert db.get_meta(marker) is None
    assert db.get_meta("sweep-probe") is None


def test_normalize_keyword_data_runs_each_sweep_once(db, monkeypatch, caplog):
    for marker in ("keyword_names_normalized", "curation_species_case_aligned_v2",
                   "keyword_apostrophes_folded_v1"):
        db.conn.execute("DELETE FROM db_meta WHERE key = ?", (marker,))
    db.conn.commit()
    calls = []
    monkeypatch.setattr(db, "_normalize_keyword_data_once", lambda: calls.append("once"))
    monkeypatch.setattr(db, "_align_curation_species_case", lambda: calls.append("case") or 2)
    monkeypatch.setattr(db, "_align_curation_history_species",
                        lambda: calls.append("hist") or 1)
    monkeypatch.setattr(db, "_fold_prediction_species_apostrophes",
                        lambda: calls.append("fold") or 3)
    meta = []
    real_set_meta = db.set_meta
    monkeypatch.setattr(db, "set_meta", lambda k, v, _commit=True: meta.append(
        (k, v, _commit)) or real_set_meta(k, v, _commit=_commit))
    with caplog.at_level("INFO", logger="db"):
        db.normalize_keyword_data()
    assert calls == ["once", "case", "hist", "once", "fold"]
    assert meta == [
        ("keyword_names_normalized", "1", False),
        ("curation_species_case_aligned_v2", "1", False),
        ("keyword_apostrophes_folded_v1", "1", False),
    ]
    assert not db.conn.in_transaction
    assert "moved 2 row(s), rewrote 1 history item(s)" in caplog.text
    assert "apostrophe fold: rewrote 3 prediction species" in caplog.text
    calls.clear()
    db.normalize_keyword_data()
    assert calls == []


def _prediction(db, pid, species, conf=0.5, model="m", fp="fp", **cols):
    det = db.save_detections(
        pid, [{"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.9,
               "category": "animal"}], "md",
    )[0]
    db.conn.execute(
        "INSERT INTO predictions (detection_id, classifier_model, labels_fingerprint, "
        "species, confidence) VALUES (?, ?, ?, ?, ?)",
        (det, model, fp, species, conf),
    )
    pred_id = db.conn.execute("SELECT MAX(id) FROM predictions").fetchone()[0]
    for col, val in cols.items():
        db.conn.execute(f"UPDATE predictions SET {col} = ? WHERE id = ?", (val, pred_id))
    return det, pred_id


def test_fold_prediction_species_apostrophes(db, lib):
    p0, p1 = lib["p"][:2]
    det, curly = _prediction(db, p0, "Say’s phoebe", 0.9)
    db.conn.execute(
        "INSERT INTO predictions (detection_id, classifier_model, labels_fingerprint, "
        "species, confidence, category, scientific_name) "
        "VALUES (?, 'm', 'fp', ?, 0.4, 'match', 'Sayornis saya')",
        (det, "Say's Phoebe"),
    )
    ascii_id = db.conn.execute("SELECT MAX(id) FROM predictions").fetchone()[0]
    _d2, lone = _prediction(db, p1, "Black’s bird", 0.3)
    _d3, junk = _prediction(db, lib["p"][2], "’", 0.3)
    db.conn.commit()
    folded = db._fold_prediction_species_apostrophes()
    assert folded == 2
    rows = {r["id"]: tuple(r)[1:] for r in db.conn.execute(
        "SELECT id, species, category, scientific_name FROM predictions")}
    assert ascii_id not in rows
    assert rows[curly] == ("Say's phoebe", "match", "Sayornis saya")
    assert rows[lone][0] == "Black's bird"
    assert rows[junk][0] == "’"
    assert db.conn.in_transaction  # caller commits


def test_merge_prediction_metadata_guards(db, lib):
    _d, a = _prediction(db, lib["p"][0], "A", category="new")
    _d, b = _prediction(db, lib["p"][1], "B", category="change",
                        taxonomy_genus="G", scientific_name="S")
    statements = _trace(db)
    db._merge_prediction_metadata_before_delete(a, a)
    db.conn.set_trace_callback(None)
    assert statements == []
    db._merge_prediction_metadata_before_delete(999_999, a)
    db._merge_prediction_metadata_before_delete(b, a)
    row = db.conn.execute(
        "SELECT category, taxonomy_genus, scientific_name FROM predictions WHERE id = ?",
        (a,),
    ).fetchone()
    assert tuple(row) == ("change", "G", "S")
    db.conn.execute("UPDATE predictions SET category = 'match' WHERE id = ?", (a,))
    db._merge_prediction_metadata_before_delete(b, a)
    assert db.conn.execute("SELECT category FROM predictions WHERE id = ?",
                           (a,)).fetchone()[0] == "match"


def test_merge_prediction_review_before_delete(db, lib):
    _d, a = _prediction(db, lib["p"][0], "A")
    _d, b = _prediction(db, lib["p"][1], "B")
    ws = lib["ws"]
    other = db.create_workspace("Other")
    db.conn.execute(
        "INSERT INTO prediction_review (prediction_id, workspace_id, status) "
        "VALUES (?, ?, 'accepted')", (b, ws))
    db.conn.execute(
        "INSERT INTO prediction_review (prediction_id, workspace_id, status) "
        "VALUES (?, ?, 'rejected')", (b, other))
    db.conn.execute(
        "INSERT INTO prediction_review (prediction_id, workspace_id, status) "
        "VALUES (?, ?, 'pending')", (a, other))
    db._merge_prediction_review_before_delete(loser_id=b, winner_id=a)
    got = sorted(tuple(r) for r in db.conn.execute(
        "SELECT prediction_id, workspace_id, status FROM prediction_review"))
    assert (a, ws, "accepted") in got


def test_retarget_prediction_edit_history(db, lib):
    p0 = lib["p"][0]
    edit = db.conn.execute(
        "INSERT INTO edit_history (action_type, description, new_value, workspace_id) "
        "VALUES ('prediction_accept', 'x', 'v', ?)", (lib["ws"],)).lastrowid
    items = [
        "5",
        json.dumps({"prediction_id": 5, "no_tag": True}),
        json.dumps({"prediction_ids": [5, "x", 6]}),
        json.dumps({"prediction_id": "bad"}),
        json.dumps([5]),
        "{not json",
        json.dumps({"prediction_id": 6}),
    ]
    ids = []
    for old in items:
        ids.append(db.conn.execute(
            "INSERT INTO edit_history_items (edit_id, photo_id, old_value, new_value) "
            "VALUES (?, ?, ?, 'n')", (edit, p0, old)).lastrowid)
    statements = _trace(db)
    db._retarget_prediction_edit_history(loser_id=5, winner_id=5)
    db.conn.set_trace_callback(None)
    assert statements == []
    db._retarget_prediction_edit_history(loser_id=5, winner_id=9)
    got = [db.conn.execute("SELECT old_value FROM edit_history_items WHERE id = ?",
                           (i,)).fetchone()[0] for i in ids]
    assert got[0] == "9"
    assert json.loads(got[1]) == {"prediction_id": 9, "no_tag": True}
    assert json.loads(got[2]) == {"prediction_ids": [9, "x", 6]}
    assert got[3:] == items[3:]


def test_align_curation_species_case(db, lib, monkeypatch):
    p0 = lib["p"][0]
    ws = lib["ws"]
    _raw_kw(db, "Saffron finch", kw_type="taxonomy", is_species=1)
    _raw_kw(db, "Robin", kw_type="general", is_species=1)
    _raw_kw(db, "robin", kw_type="taxonomy", is_species=1)
    for species in ("Saffron Finch", "ROBIN", "Saffron finch"):
        db.conn.execute(
            "INSERT INTO species_highlights (workspace_id, species, photo_id, rank) "
            "VALUES (?, ?, ?, 1)", (ws, species, p0))
    db.conn.execute(
        "INSERT INTO photo_preferences (workspace_id, purpose, species, photo_id) "
        "VALUES (?, 'life_list', 'saffron FINCH', ?)", (ws, p0))
    db.conn.commit()
    moved = db._align_curation_species_case()
    assert moved >= 1
    assert sorted(r[0] for r in db.conn.execute(
        "SELECT species FROM species_highlights")) == ["ROBIN", "Saffron finch"]
    assert [r[0] for r in db.conn.execute(
        "SELECT species FROM photo_preferences")] == ["Saffron finch"]
    assert db.conn.in_transaction  # caller commits


def test_align_curation_history_species(db, lib):
    p0 = lib["p"][0]
    _raw_kw(db, "Saffron finch", kw_type="taxonomy", is_species=1)
    edit = db.conn.execute(
        "INSERT INTO edit_history (action_type, description, new_value, workspace_id) "
        "VALUES ('species_replace', 'x', 'v', ?)", (lib["ws"],)).lastrowid
    payloads = [
        {"curation": {"hl_prev": ["Saffron Finch", {"species": "saffron finch"}, 3],
                      "pref_prev": [{"species": "SAFFRON FINCH"}, "x", {"species": 4}],
                      "rep_prev": "nope"}},
        {"curation": "nope"},
        {"curation": {"hl_prev": ["Saffron finch"]}},
    ]
    raw = [json.dumps(p) for p in payloads] + ['{"curation": [', '["curation"]']
    ids = [db.conn.execute(
        "INSERT INTO edit_history_items (edit_id, photo_id, old_value, new_value) "
        "VALUES (?, ?, ?, 'n')", (edit, p0, v)).lastrowid for v in raw]
    assert db._align_curation_history_species() == 1
    first = json.loads(db.conn.execute(
        "SELECT old_value FROM edit_history_items WHERE id = ?", (ids[0],)).fetchone()[0])
    assert first["curation"]["hl_prev"] == ["Saffron finch", {"species": "Saffron finch"}, 3]
    assert first["curation"]["pref_prev"][0] == {"species": "Saffron finch"}
    for i, v in zip(ids[1:], raw[1:], strict=True):
        assert db.conn.execute("SELECT old_value FROM edit_history_items WHERE id = ?",
                               (i,)).fetchone()[0] == v


def test_species_keyword_maps_and_canonical(db, lib, monkeypatch):
    _raw_kw(db, "Saffron finch", kw_type="taxonomy", is_species=1)
    _raw_kw(db, "Robin", kw_type="general", is_species=1)
    _raw_kw(db, "robin", kw_type="taxonomy", is_species=1)
    birds = _raw_kw(db, "Birds")
    _raw_kw(db, "Leafy", birds, "taxonomy", 1)
    unique, all_keys = db._species_keyword_maps()
    assert unique == {"saffron finch": "Saffron finch"}
    assert all_keys == {"saffron finch", "robin"}
    assert db._canonical_curation_species("'", unique, all_keys) == ""
    assert db._canonical_curation_species(None, unique, all_keys) == ""
    assert db._canonical_curation_species("SAFFRON FINCH", unique, all_keys) == "Saffron finch"
    assert db._canonical_curation_species("ROBIN", unique, all_keys) == "ROBIN"
    seen = []
    monkeypatch.setattr(db, "resolve_species_display_name",
                        lambda n: seen.append(n) or "Resolved")
    assert db._canonical_curation_species("New bird", unique, all_keys) == "Resolved"
    assert seen == ["New bird"]


# -- duplicate photo species repair --------------------------------------------------------


def _repair_setup(db, lib):
    p0, p1 = lib["p"][:2]
    root = _raw_kw(db, "Verdin", kw_type="taxonomy", is_species=1, taxon_id=3)
    birds = _raw_kw(db, "Birds")
    leaf = _raw_kw(db, "Desert Verdin", birds, "taxonomy", 1, taxon_id=3)
    db.conn.execute("DELETE FROM db_meta WHERE key = ?",
                    (Database._DUPLICATE_PHOTO_SPECIES_REPAIR_KEY,))
    db.conn.commit()
    for pid in (p0, p1):
        db.tag_photo(pid, root)
        db.tag_photo(pid, leaf)
    return root, leaf


def test_repair_duplicate_photo_species_detaches_roots(db, lib, monkeypatch, caplog):
    p0, p1 = lib["p"][:2]
    root, leaf = _repair_setup(db, lib)
    db.queue_change(p0, "keyword_add", "Verdin")
    queued = []
    real_queue = db.queue_change
    monkeypatch.setattr(db, "queue_change",
                        lambda *a, **k: queued.append((a, k)) or real_queue(*a, **k))
    with caplog.at_level("INFO", logger="db"):
        assert db.repair_duplicate_photo_species() == 2
    assert not db.conn.in_transaction
    assert _visible(db, "SELECT photo_id, keyword_id FROM photo_keywords ORDER BY photo_id") == [
        (p0, leaf), (p1, leaf),
    ]
    # p0's pending add was cancelled; p1 gets a sidecar remove queued.
    assert [(a[0], a[1], a[2], k) for a, k in queued] == [
        (p1, "keyword_remove", "Verdin", {"workspace_id": lib["ws"], "_commit": False}),
    ]
    assert db.get_meta(Database._DUPLICATE_PHOTO_SPECIES_REPAIR_KEY) == "1"
    assert "repaired 2 redundant" in caplog.text
    assert db.repair_duplicate_photo_species() == 0


def test_repair_duplicate_photo_species_waits_for_taxa(db, lib):
    _repair_setup(db, lib)
    db.conn.execute("UPDATE taxa SET rank = 'genus' WHERE rank = 'species'")
    db.conn.commit()
    assert db.repair_duplicate_photo_species() == 0
    assert db.get_meta(Database._DUPLICATE_PHOTO_SPECIES_REPAIR_KEY) is None


def test_repair_duplicate_photo_species_rolls_back(db, lib, monkeypatch):
    _repair_setup(db, lib)

    def boom(*a, **k):
        raise RuntimeError("queue failed")

    monkeypatch.setattr(db, "queue_change", boom)
    with pytest.raises(RuntimeError, match="queue failed"):
        db.repair_duplicate_photo_species()
    assert not db.conn.in_transaction
    assert _visible(db, "SELECT COUNT(*) FROM photo_keywords") == [(4,)]
    assert db.get_meta(Database._DUPLICATE_PHOTO_SPECIES_REPAIR_KEY) is None


def test_repair_duplicate_photo_species_prunes_edit_history(db, lib):
    p0 = lib["p"][0]
    root, leaf = _repair_setup(db, lib)
    edit = db.conn.execute(
        "INSERT INTO edit_history (action_type, description, new_value, workspace_id) "
        "VALUES ('species_replace', 'x', 'v', ?)", (lib["ws"],)).lastrowid
    values = [
        json.dumps({"keyword_id": root}),
        json.dumps({"keyword_ids": ["x", root]}),
        json.dumps({"keyword_id": "bad", "keyword_ids": [leaf]}),
        "{broken",
        json.dumps([root]),
    ]
    ids = [db.conn.execute(
        "INSERT INTO edit_history_items (edit_id, photo_id, old_value, new_value) "
        "VALUES (?, ?, ?, 'n')", (edit, p0, v)).lastrowid for v in values]
    db.conn.commit()
    db.repair_duplicate_photo_species()
    left = [r[0] for r in db.conn.execute("SELECT id FROM edit_history_items ORDER BY id")]
    assert left == ids[2:]


# -- species marking -----------------------------------------------------------------------


class _Tax:
    def __init__(self, table):
        self.table = table

    def lookup(self, name):
        return self.table.get(name)


def test_mark_species_keywords(db, lib):
    general = _raw_kw(db, "American Robin")
    rebind = _raw_kw(db, "Cougar", kw_type="taxonomy", is_species=1, taxon_id=4)
    keep = _raw_kw(db, "Turdus", kw_type="taxonomy", is_species=1, taxon_id=2)
    place = _raw_kw(db, "Puma", kw_type="location")
    unknown = _raw_kw(db, "Nothing")
    db.conn.commit()
    tax = _Tax({
        "American Robin": {"taxon_id": 12727},
        "Cougar": {"taxon_id": 42007},
        "Turdus": {"taxon_id": None},
        "Puma": {"taxon_id": 42007},
    })
    assert db.mark_species_keywords(tax) == 2
    assert not db.conn.in_transaction
    assert _kw(db, general)[2:5] == ("taxonomy", 1, 3)
    assert _kw(db, rebind)[4] == 5
    assert _kw(db, keep)[4] == 2
    assert _kw(db, place)[2] == "location"
    assert _kw(db, unknown)[2] == "general"
    statements = _trace(db)
    assert db.mark_species_keywords(tax) == 0
    db.conn.set_trace_callback(None)
    assert not any(s.strip().startswith("UPDATE") for s in statements)


def test_mark_species_keywords_links_subspecies_through_lineage(db, lib, monkeypatch):
    kid = _raw_kw(db, "Eastern Robin")
    db.conn.commit()
    tax = _Tax({"Eastern Robin": {
        "taxon_id": 999,
        "lineage_names": ["Aves", "Turdus", "Turdus migratorius", "T. m. migratorius"],
        "lineage_ranks": ["class", "genus", "species", "subspecies"],
    }})
    calls = []
    real = db._resolve_species_by_lineage
    monkeypatch.setattr(db, "_resolve_species_by_lineage",
                        lambda n, a: calls.append((n, a)) or real(n, a))
    assert db.mark_species_keywords(tax) == 1
    assert calls == [("Turdus migratorius", ["Aves", "Turdus"])]
    assert _kw(db, kid)[4] == 3


def test_resolve_species_by_lineage(db, lib):
    assert db._resolve_species_by_lineage("Nope", ["Aves"]) is None
    assert db._resolve_species_by_lineage("Turdus migratorius", []) is None
    assert db._resolve_species_by_lineage("Turdus migratorius", ["Aves"])["id"] == 3
    assert db._resolve_species_by_lineage("Turdus migratorius", ["Mammalia"]) is None
    # A dangling parent pointer ends the walk.
    db.conn.execute("PRAGMA foreign_keys = OFF")
    _taxon(db, 9, "Orphan species", "species", 777)
    db.conn.execute("PRAGMA foreign_keys = ON")
    assert db._resolve_species_by_lineage("Orphan species", ["Aves"]) is None
    # Two matching homonyms are ambiguous.
    _taxon(db, 10, "Turdus migratorius", "species", 2)
    assert db._resolve_species_by_lineage("Turdus migratorius", ["Aves"]) is None


# -- structure ----------------------------------------------------------------------------


_DELEGATING_KEYWORD_METHODS = (
    "filter_out_subject_tagged",
    "ensure_default_genre_keywords",
    "migrate_legacy_keyword_types",
    "count_keywords",
    "count_keywords_in_workspace",
    "get_accepted_species",
    "detect_keyword_case_convention",
    "resolve_species_display_name",
    "_species_root_name_for_taxon",
    "_lookup_taxon_id_for_keyword",
    "_add_source_species_keyword",
    "relink_source_species_keywords",
    "add_keyword",
    "merge_duplicate_keywords",
    "_merge_duplicate_keywords_pass",
    "_normalize_keyword_row_name",
    "_rename_keyword_dependents",
    "normalize_keyword_data",
    "_fold_prediction_species_apostrophes",
    "_merge_prediction_review_before_delete",
    "_merge_prediction_metadata_before_delete",
    "_retarget_prediction_edit_history",
    "_align_curation_species_case",
    "_align_curation_history_species",
    "_species_keyword_maps",
    "has_possible_duplicate_photo_species",
    "repair_duplicate_photo_species",
    "_reparent_disambiguated",
    "get_keyword_tree",
    "untag_photo",
    "get_photo_keywords",
    "get_keywords_for_photos",
    "get_species_keywords_for_photos",
    "get_photos_with_equivalent_species",
    "update_keyword",
    "get_all_keywords",
    "is_keyword_species",
    "_resolve_species_by_lineage",
    "mark_species_keywords",
)

# ``test_keyword_provenance_contract`` keys these writers to db.py, and the
# methods that call ``_merge_keyword_into`` mid-flight keep that call there.
_KEYWORD_METHODS_KEPT_ON_DATABASE = (
    "tag_photo",
    "_merge_keyword_into",
    "retire_builtin_wildlife_genre",
    "_upsert_one_keyword",
    "_normalize_keyword_data_once",
)

_PROVENANCE_WRITERS = (
    "tag_photo",
    "_merge_keyword_into",
    "retire_builtin_wildlife_genre",
    "link_keyword_to_place",
    "_apply_winner_loser_merge",
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


@pytest.mark.parametrize("name", _DELEGATING_KEYWORD_METHODS)
def test_keyword_method_delegates_to_repository(name):
    attrs = _self_attrs(getattr(Database, name))
    assert "conn" not in attrs, (
        f"Database.{name} touches self.conn; move the SQL to KeywordRepository"
    )
    assert "_keyword_repository" in attrs, (
        f"Database.{name} no longer delegates to KeywordRepository"
    )


@pytest.mark.parametrize("name", _KEYWORD_METHODS_KEPT_ON_DATABASE)
def test_provenance_writers_and_their_merge_callers_stay_on_database(name):
    attrs = _self_attrs(getattr(Database, name))
    assert "_keyword_repository" not in attrs, (
        f"Database.{name} must keep its photo_keywords write (or its "
        "_merge_keyword_into call) in db.py; see test_keyword_provenance_contract"
    )


@pytest.mark.parametrize("name", ["_merge_duplicate_keywords_pass", "update_keyword"])
def test_split_methods_still_call_merge_on_the_facade(name):
    assert "_merge_keyword_into" in _self_attrs(getattr(Database, name))


def test_keyword_repository_never_references_provenance_writers():
    import repositories.keywords as module

    tree = ast.parse(inspect.getsource(module))
    names = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Attribute):
            names.add(node.attr)
        elif isinstance(node, ast.Name):
            names.add(node.id)
        elif isinstance(node, ast.Constant) and isinstance(node.value, str):
            names.add(node.value)
        elif isinstance(node, ast.FunctionDef):
            names.add(node.name)
    assert not names & set(_PROVENANCE_WRITERS)
    assert not set(module.FACADE_METHODS) & set(_PROVENANCE_WRITERS)


def test_keyword_repository_routes_facade_calls_through_database(db):
    repo = db._keyword_repository()
    from repositories.keywords import FACADE_METHODS

    for name in FACADE_METHODS:
        assert getattr(repo, name) == getattr(db, name), name


def test_keyword_facade_signatures_unchanged():
    sig = {n: str(inspect.signature(getattr(Database, n)))
           for n in _DELEGATING_KEYWORD_METHODS}
    assert sig["add_keyword"] == (
        "(self, name, parent_id=None, is_species=False, kw_type=None, "
        "_commit=True, source_taxon_id=None, _resolve_alias=False)"
    )
    assert sig["resolve_species_display_name"].startswith(
        "(self, name, apply_case_convention=True, case_convention=<object object"
    )
    assert sig["_add_source_species_keyword"] == (
        "(self, name, source_taxon_id, parent_id=None, _commit=True)"
    )
    assert sig["untag_photo"] == "(self, photo_id, keyword_id, _commit=True)"
    assert sig["update_keyword"] == "(self, keyword_id, **kwargs)"
    assert sig["_lookup_taxon_id_for_keyword"] == (
        "(self, name, prefer_species=False, species_only=False)"
    )
    assert sig["get_photos_with_equivalent_species"] == (
        "(self, photo_ids, keyword_id, exclude_keyword_ids=None)"
    )
    assert sig["get_species_keywords_for_photos"] == (
        "(self, photo_ids, include_identities=False)"
    )
    assert sig["_normalize_keyword_row_name"] == (
        "(self, keyword_id, disambiguate_on_conflict=False)"
    )


def test_keyword_repository_never_hands_itself_out_as_the_database():
    """Moved bodies that passed ``self`` (the Database) to a helper must pass
    ``self.db`` now; a bare ``self`` argument would hand over the repository."""
    import repositories.keywords as module

    tree = ast.parse(inspect.getsource(module))
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
