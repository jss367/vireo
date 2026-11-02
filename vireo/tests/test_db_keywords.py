"""Behavior pins for the keywords domain of ``Database``.

The tests go through the public ``Database`` façade only, so they hold
whether the SQL lives in ``db.py`` or in ``repositories/keywords.py``. They
pin that the writers leave the commit to the caller (the caller's
transaction stays open and nothing is committed until its unit of work
ends), the all-or-nothing rollbacks of the one-shot sweeps, species-name resolution and
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
    fid = workspace_folders.add_folder(db, "/kw", name="kw")
    photos = [photo_ingest.add_photo(db, fid, f"p{i}.jpg", ".jpg", 10 + i, float(i)) for i in range(4)]
    _taxon(db, 1, "Aves", "class")
    _taxon(db, 2, "Turdus", "genus", 1)
    _taxon(db, 3, "Turdus migratorius", "species", 2, "American Robin", inat_id=12727)
    _taxon(db, 4, "Puma", "genus")
    _taxon(db, 5, "Puma concolor", "species", 4, "Cougar", inat_id=42007)
    db.conn.commit()
    return {"ws": ws, "fid": fid, "p": photos}


# -- add_keyword and the caller-owned commit ----------------------------------------------


def test_add_keyword_inserts_and_commits_with_the_callers_unit(db, lib):
    with db.transaction():
        kid = db.add_keyword("Driftwood")
    assert not db.conn.in_transaction
    assert _visible(db, "SELECT name, type FROM keywords WHERE id = ?", (kid,)) == [
        ("Driftwood", "general")
    ]
    # Case-insensitive reuse.
    assert db.add_keyword("driftwood") == kid


def test_add_keyword_without_commit_leaves_transaction_open(db, lib):
    kid = db.add_keyword("Driftwood")
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


def _committed_add(other, *args, **kwargs):
    """The concurrent writer's own unit of work: insert and commit."""
    with other.transaction():
        return other.add_keyword(*args, **kwargs)


def test_add_keyword_race_reuses_concurrent_top_level_row(db, lib, monkeypatch):
    # UNIQUE(name, parent_id) never fires for NULL parents, so without the
    # write-locked re-check both callers would insert a root "Sunrise".
    other, state = _race_add(db, monkeypatch, lambda o: _committed_add(o, "Sunrise"))
    try:
        with db.transaction():
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
    with db.transaction():
        birds = db.add_keyword("Birds")
    other, state = _race_add(
        db, monkeypatch, lambda o: _committed_add(o, "Heron", parent_id=birds),
    )
    try:
        with db.transaction():
            kid = db.add_keyword("Heron", parent_id=birds)
    finally:
        other.close()
    assert kid == state["winner"]
    assert _visible(
        db, "SELECT COUNT(*) FROM keywords WHERE name = 'Heron' AND parent_id = ?",
        (birds,),
    ) == [(1,)]


def test_add_keyword_race_reuse_leaves_the_callers_transaction_open(db, lib, monkeypatch):
    # ``add_keyword`` is often the first write of a caller's unit (sync.py,
    # web/encounters.py, web/highlights.py, the import job at
    # web/imports.py). Without the write-locked re-check both callers would
    # commit a root "Sunrise" — SQLite treats NULL parents as distinct in
    # UNIQUE(name, parent_id), so IntegrityError never fires.
    other, state = _race_add(db, monkeypatch, lambda o: _committed_add(o, "Sunrise"))
    try:
        kid = db.add_keyword("sunrise")
        # The recursion at the ``existing`` fast path must leave the caller's
        # transaction open — the caller (encounters, highlights, import, sync)
        # commits later with its own follow-up writes.
        assert db.conn.in_transaction
        db.conn.commit()
    finally:
        other.close()
    assert kid == state["winner"]
    assert _visible(
        db, "SELECT COUNT(*) FROM keywords WHERE name = 'sunrise' COLLATE NOCASE",
    ) == [(1,)]


@pytest.mark.parametrize("kwargs", [
    {"is_species": True},
    {"kw_type": "genre"},
    {"kw_type": "taxonomy"},
])
def test_add_keyword_promotions_leave_commit_to_caller(db, lib, kwargs):
    with db.transaction():
        kid = db.add_keyword("Heron")
    db.add_keyword("Heron", **kwargs)
    assert db.conn.in_transaction
    assert _visible(db, "SELECT type, is_species FROM keywords WHERE id = ?", (kid,)) == [
        ("general", 0)
    ]
    db.conn.rollback()
    with db.transaction():
        got = db.add_keyword("Heron", **kwargs)
    assert got == kid
    assert not db.conn.in_transaction
    want_type = kwargs.get("kw_type", "taxonomy")
    assert _visible(db, "SELECT type FROM keywords WHERE id = ?", (kid,)) == [(want_type,)]


def test_add_keyword_auto_promotes_general_taxon_match(db, lib):
    kid = _raw_kw(db, "American Robin")
    db.conn.commit()
    assert db.add_keyword("American Robin") == kid
    assert db.conn.in_transaction
    assert _visible(db, "SELECT type FROM keywords WHERE id = ?", (kid,)) == [("general",)]
    db.conn.rollback()
    with db.transaction():
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
    import repositories.keywords as keywords_module
    from repositories.keyword_merges import KeywordMergeRepository

    calls = []
    monkeypatch.setattr(keywords_module, "resolve_import_alias",
                        lambda m, name, parent, kw_type=None: calls.append(
                            (m, name, parent, kw_type)) or 77)
    assert db.add_keyword("Leaf", parent_id=None, _resolve_alias=True) == 77
    # The helper receives the alias repository on the same connection.
    [(merges, *rest)] = calls
    assert rest == ["Leaf", None, None]
    assert isinstance(merges, KeywordMergeRepository)
    assert merges.conn is db.conn


def test_add_keyword_routes_lookup_and_source_path_through_repositories(db, lib, monkeypatch):
    from repositories.keyword_taxa import KeywordTaxonRepository
    from repositories.keywords import KeywordRepository

    seen = []
    monkeypatch.setattr(KeywordTaxonRepository, "lookup_taxon_id",
                        lambda self, name, **kw: seen.append((name, kw)) or None)
    db.add_keyword("Mystery")
    assert seen == [("Mystery", {"prefer_species": True})]
    src = []
    monkeypatch.setattr(KeywordRepository, "add_source_species",
                        lambda self, *a: src.append(a) or 99)
    assert db.add_keyword("Robin", is_species=True, source_taxon_id=12727) == 99
    assert src == [("Robin", 12727, None)]


# -- source-taxon species keywords ---------------------------------------------------------


def test_add_source_species_validates_id(db, lib):
    for bad in (0, -1, 1 << 63, "5", 5.0, True):
        with pytest.raises(ValueError, match="positive SQLite integer"):
            db.keywords.add_source_species("x", bad)


def test_add_source_species_rebinds_existing(db, lib):
    kid = _raw_kw(db, "Robin", kw_type="general", taxon_id=3)
    db.conn.commit()
    got = db.keywords.add_source_species("Robin", 12727)
    assert got == kid
    assert db.conn.in_transaction
    assert _kw(db, kid)[2:] == ("taxonomy", 1, 3, 12727)
    assert _visible(db, "SELECT source_taxon_id FROM keywords WHERE id = ?", (kid,)) == [(None,)]
    db.conn.commit()


def test_add_source_species_suffixes_same_name_rows(db, lib, monkeypatch):
    import species_identity
    from repositories.keywords import TaxonCatalog

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
    with db.transaction():
        kid = db.keywords.add_source_species("Cougar (taxon 42007)", 42007)
    assert not db.conn.in_transaction
    assert _kw(db, kid) == ("Cougar (taxon 42007) (2)", None, "taxonomy", 1, 5, 42007)
    # The resolver reads db_meta and taxa through repositories on db.conn,
    # never through the Database.
    [catalog] = resolver_dbs
    assert isinstance(catalog, TaxonCatalog)
    assert catalog.meta.conn is db.conn and catalog.taxa.conn is db.conn


def test_relink_source_species_keywords(db, lib):
    kid = _raw_kw(db, "Robin", kw_type="taxonomy", is_species=1)
    db.conn.execute("UPDATE keywords SET source_taxon_id = 12727 WHERE id = ?", (kid,))
    other = _raw_kw(db, "Ghost", kw_type="taxonomy", is_species=1)
    db.conn.execute("UPDATE keywords SET source_taxon_id = 999 WHERE id = ?", (other,))
    db.conn.commit()
    db.keyword_taxa.relink_source_species()
    assert db.conn.in_transaction  # caller commits
    assert _kw(db, kid)[4] == 3
    assert _kw(db, other)[4] is None


# -- species-name resolution ---------------------------------------------------------------


def test_resolve_species_display_cases(db, lib):
    assert db.keyword_taxa.resolve_species_display("'") == ""
    _raw_kw(db, "Common waxbill", kw_type="taxonomy", is_species=1)
    assert db.keyword_taxa.resolve_species_display("COMMON WAXBILL") == "Common waxbill"
    # Two species-bearing homonyms: caller's spelling wins when it matches
    # one, else it is kept verbatim.
    _raw_kw(db, "Robin", kw_type="general", is_species=1)
    _raw_kw(db, "robin", kw_type="taxonomy", is_species=1)
    assert db.keyword_taxa.resolve_species_display("Robin") == "Robin"
    assert db.keyword_taxa.resolve_species_display("ROBIN") == "ROBIN"
    # A general-only row keeps its own spelling.
    _raw_kw(db, "Sunset glow")
    assert db.keyword_taxa.resolve_species_display("sunset GLOW") == "Sunset glow"


def test_resolve_species_display_hierarchy_leaf(db, lib):
    parent = _raw_kw(db, "Birds")
    _raw_kw(db, "Desert Verdin", parent, "taxonomy", 1, taxon_id=3)
    db.conn.commit()
    # No root row for the taxon: the leaf's own spelling.
    assert db.keyword_taxa.resolve_species_display("desert verdin") == "Desert Verdin"
    _raw_kw(db, "Verdin", None, "taxonomy", 1, taxon_id=3)
    assert db.keyword_taxa.resolve_species_display("desert verdin") == "Verdin"


def test_resolve_species_display_case_convention_paths(db, lib, case, monkeypatch):
    assert db.keyword_taxa.resolve_species_display("bubulcus ibis",
                                           apply_case_convention=False) == "bubulcus ibis"
    assert db.keyword_taxa.resolve_species_display("bubulcus ibis",
                                           case_convention="title") == "Bubulcus Ibis"
    assert db.keyword_taxa.resolve_species_display("bubulcus ibis",
                                           case_convention=None) == "bubulcus ibis"
    case("lower")
    assert db.keyword_taxa.resolve_species_display("Bubulcus Ibis") == "Bubulcus ibis"
    from repositories.keyword_taxa import KeywordTaxonRepository

    calls = []
    monkeypatch.setattr(KeywordTaxonRepository, "species_case_convention",
                        lambda self: calls.append(1) or "title")
    assert db.keyword_taxa.resolve_species_display("snowy egret") == "Snowy Egret"
    assert calls == [1]


def test_species_case_convention(db, lib, case):
    assert db.keyword_taxa.species_case_convention() is None
    case("title")
    assert db.keyword_taxa.species_case_convention() == "title"


def test_detect_keyword_case_convention(db, lib):
    assert db.keyword_taxa.detect_case_convention() is None
    for name in ("Black Phoebe", "Snowy Egret", "Heron"):
        _raw_kw(db, name, kw_type="taxonomy", is_species=1)
    assert db.keyword_taxa.detect_case_convention() == "title"
    for name in ("Great egret", "Say's phoebe", "Tree swallow"):
        _raw_kw(db, name, kw_type="taxonomy", is_species=1)
    assert db.keyword_taxa.detect_case_convention() == "lower"
    _raw_kw(db, "Blue Jay", kw_type="taxonomy", is_species=1)
    assert db.keyword_taxa.detect_case_convention() is None
    _raw_kw(db, "Emu", kw_type="taxonomy", is_species=1)
    assert db.keyword_taxa.detect_case_convention() is None


def test_apply_case_convention_helpers():
    from repositories.keyword_taxa import apply_species_case_convention, sentence_case_first_word

    assert apply_species_case_convention("mcKay's BUNTING", "lower") == "McKay's bunting"
    assert apply_species_case_convention("HERON", "lower") == "Heron"
    assert apply_species_case_convention("snowy egret", "title") == "Snowy Egret"
    assert apply_species_case_convention("snowy egret", "upper") == "snowy egret"
    assert sentence_case_first_word("") == ""
    assert sentence_case_first_word("123") == "123"


def test_species_root_name_for_taxon(db, lib):
    assert db.keyword_taxa.species_root_name_for_taxon(None) is None
    assert db.keyword_taxa.species_root_name_for_taxon(3) is None
    _raw_kw(db, "American Robin", kw_type="taxonomy", is_species=1, taxon_id=3)
    assert db.keyword_taxa.species_root_name_for_taxon(3) == "American Robin"


def test_lookup_taxon_id_for_keyword(db, lib):
    assert db.keyword_taxa.lookup_taxon_id("american robin") == 3
    assert db.keyword_taxa.lookup_taxon_id("Nope") is None
    # prefer_species falls back to the higher-rank direct hit.
    assert db.keyword_taxa.lookup_taxon_id("Puma", prefer_species=True) == 4
    assert db.keyword_taxa.lookup_taxon_id("Puma", species_only=True) is None
    db.conn.execute(
        "INSERT INTO taxa_common_names (taxon_id, name) VALUES (5, 'Mountain lion')"
    )
    db.conn.execute(
        "INSERT INTO taxa_common_names (taxon_id, name) VALUES (4, 'Big cats')"
    )
    assert db.keyword_taxa.lookup_taxon_id("mountain lion") == 5
    assert db.keyword_taxa.lookup_taxon_id("mountain lion", species_only=True) == 5
    assert db.keyword_taxa.lookup_taxon_id("big cats", prefer_species=True) == 4
    assert db.keyword_taxa.lookup_taxon_id("big cats", species_only=True) is None


# -- tagging -------------------------------------------------------------------------------


def test_tag_and_untag_leave_commit_to_caller(db, lib):
    p = lib["p"][0]
    with db.transaction():
        kid = db.add_keyword("Sunset")
    db.tag_photo(p, kid)
    assert db.conn.in_transaction
    assert _visible(db, "SELECT COUNT(*) FROM photo_keywords") == [(0,)]
    db.conn.commit()
    db.untag_photo(p, kid)
    assert db.conn.in_transaction
    assert _visible(db, "SELECT COUNT(*) FROM photo_keywords") == [(1,)]
    db.conn.rollback()
    with db.transaction():
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
    assert [tuple(r) for r in db.photo_keywords.get_for_photo(p0)] == [
        (a, "Alpha", None, "general"), (b, "American Robin", None, "taxonomy"),
    ]
    assert db.photo_keywords.get_for_photos([]) == {}
    got = db.photo_keywords.get_for_photos([p0, p1, p0, p2])
    assert sorted(got) == [p0, p1]
    assert [k["name"] for k in got[p0]] == ["Alpha", "American Robin"]
    assert got[p1][0]["taxon_rank"] == "species"


def test_keywords_for_photos_chunks(db, lib):
    p0 = lib["p"][0]
    db.tag_photo(p0, db.add_keyword("Alpha"))
    statements = _trace(db)
    got = db.photo_keywords.get_for_photos(list(range(10_000, 10_800)) + [p0])
    db.conn.set_trace_callback(None)
    assert list(got) == [p0]
    assert len([s for s in dict.fromkeys(statements) if "pk.photo_id IN" in s]) == 2


def test_get_keyword_name_reads_the_stored_spelling(db):
    kid = db.add_keyword("Robin")
    db.conn.execute("UPDATE keywords SET name = 'ROBIN' WHERE id = ?", (kid,))
    assert db.keywords.name_of(kid) == "ROBIN"
    assert db.keywords.name_of(987_654) is None


def test_get_top_level_species_keyword_prefers_taxonomy_then_lowest_id(db):
    assert db.keywords.top_level_species_keyword("Robin") is None
    general = _raw_kw(db, "robin", None, "general")
    _raw_kw(db, "Robin", None, "individual")
    _raw_kw(db, "Robin", _raw_kw(db, "Birds"), "general")
    # Only the top-level general row qualifies: other types and nested
    # non-taxonomy rows never match.
    row = db.keywords.top_level_species_keyword("ROBIN")
    assert (row["id"], row["name"]) == (general, "robin")
    # The one taxonomy row filed in the tree, linked to the species the name
    # resolves to, stands in for the general row as it does for add_keyword;
    # a second one leaves the name ambiguous.
    _taxon(db, 3, "Turdus migratorius", "species", common_name="Robin")
    placed = _raw_kw(db, "Robin", _raw_kw(db, "Thrushes"), "taxonomy", 1, taxon_id=3)
    row = db.keywords.top_level_species_keyword("robin")
    assert (row["id"], row["name"]) == (placed, "Robin")
    assert db.add_keyword("robin", is_species=True) == placed
    _raw_kw(db, "Robin", _raw_kw(db, "Garden birds"), "taxonomy", 1, taxon_id=3)
    assert db.keywords.top_level_species_keyword("robin")["id"] == general
    _raw_kw(db, "Robin", None, "general")
    taxonomy = _raw_kw(db, "ROBIN", None, "taxonomy", 1)
    row = db.keywords.top_level_species_keyword("robin")
    assert (row["id"], row["name"]) == (taxonomy, "ROBIN")
    assert db.keywords.top_level_species_keyword("Wren") is None


def test_get_keyword_row_reads_id_name_and_type(db):
    kid = db.add_keyword("Back garden", kw_type="location")
    assert tuple(db.keywords.get_row(kid)) == (kid, "Back garden", "location")
    assert db.keywords.get_row(987_654) is None


def test_get_photo_ids_with_keyword_and_chunks(db, lib):
    p0, p1, p2, _ = lib["p"]
    kid = db.add_keyword("Alpha")
    other = db.add_keyword("Beta")
    db.tag_photo(p0, kid)
    db.tag_photo(p2, kid)
    db.tag_photo(p1, other)
    assert db.photo_keywords.photo_ids_tagged_with(kid, []) == set()
    assert db.photo_keywords.photo_ids_tagged_with(kid, (p0, p1)) == {p0}
    statements = _trace(db)
    got = db.photo_keywords.photo_ids_tagged_with(kid, list(range(10_000, 10_800)) + [p2, p0])
    db.conn.set_trace_callback(None)
    assert got == {p0, p2}
    assert len([s for s in dict.fromkeys(statements) if "photo_id IN" in s]) == 2


def test_species_keyword_identity_rows(db, lib):
    linked = _raw_kw(db, "American Robin", kw_type="taxonomy", is_species=1, taxon_id=3)
    sourced = _raw_kw(db, "Cougar", kw_type="taxonomy", is_species=1, taxon_id=5)
    db.conn.execute(
        "UPDATE keywords SET source_taxon_id = ? WHERE id = ?", (99_999, sourced),
    )
    unlinked_taxonomy = _raw_kw(db, "Mystery", kw_type="taxonomy")
    flagged = _raw_kw(db, "Old Species", is_species=1)
    _raw_kw(db, "Sunset")
    _raw_kw(db, "Back garden", kw_type="location")
    db.conn.commit()
    rows = db.keyword_taxa.species_identity_rows()
    assert {tuple(r.keys()) for r in rows} == {
        ("id", "name", "source_id", "scientific_name")
    }
    assert {r["id"]: tuple(r) for r in rows} == {
        # source_id falls back to the linked taxon's inat_id ...
        linked: (linked, "American Robin", 12727, "Turdus migratorius"),
        # ... but a keyword's own source_taxon_id wins.
        sourced: (sourced, "Cougar", 99_999, "Puma concolor"),
        unlinked_taxonomy: (unlinked_taxonomy, "Mystery", None, None),
        flagged: (flagged, "Old Species", None, None),
    }


def test_get_keyword_rename_state_reads_name_species_and_type(db, lib):
    kid = db.add_keyword("Back garden", kw_type="location")
    species = _raw_kw(db, "Robin", is_species=1)
    assert tuple(db.keyword_tree.rename_state(kid)) == ("Back garden", 0, "location")
    assert tuple(db.keyword_tree.rename_state(species)) == ("Robin", 1, "general")
    assert db.keyword_tree.rename_state(987_654) is None


def test_get_photo_workspaces_with_keyword_lists_each_visible_workspace(db, lib):
    p0, p1, p2, _ = lib["p"]
    other_ws = db.create_workspace("Other")
    db.commit()
    workspace_folders.add_workspace_folder(db, other_ws, lib["fid"])
    kid = db.add_keyword("Alpha")
    db.tag_photo(p0, kid)
    db.tag_photo(p1, kid)
    db.tag_photo(p2, db.add_keyword("Beta"))
    got = sorted(tuple(r) for r in db.photo_keywords.photo_workspaces_tagged_with(kid))
    assert got == sorted(
        [(p0, lib["ws"]), (p1, lib["ws"]), (p0, other_ws), (p1, other_ws)]
    )
    # A photo no workspace can see has no row.
    workspace_folders.remove_workspace_folder(db, other_ws, lib["fid"])
    workspace_folders.remove_workspace_folder(db, lib["ws"], lib["fid"])
    assert db.photo_keywords.photo_workspaces_tagged_with(kid) == []


def test_get_keyword_subtree_photo_workspaces_walks_descendants(db, lib):
    p0, p1, p2, p3 = lib["p"]
    country = _raw_kw(db, "France", kw_type="location")
    city = _raw_kw(db, "Paris", parent_id=country, kw_type="location")
    park = _raw_kw(db, "Parc Monceau", parent_id=city, kw_type="location")
    elsewhere = _raw_kw(db, "Spain", kw_type="location")
    db.conn.commit()
    db.tag_photo(p0, city)
    db.tag_photo(p0, park)  # two subtree links, one row
    db.tag_photo(p1, park)
    db.tag_photo(p2, country)
    db.tag_photo(p3, elsewhere)
    ws = lib["ws"]
    assert sorted(tuple(r) for r in db.photo_keywords.subtree_photo_workspaces(country)) == [
        (p0, ws), (p1, ws), (p2, ws),
    ]
    assert sorted(tuple(r) for r in db.photo_keywords.subtree_photo_workspaces(park)) == [
        (p0, ws), (p1, ws),
    ]
    assert db.photo_keywords.subtree_photo_workspaces(987_654) == []


def test_get_keyword_duplicate_groups_and_workspace_photo_counts(db, lib):
    p0, p1, p2, p3 = lib["p"]
    ws = lib["ws"]
    upper = _raw_kw(db, "Heron")
    lower = _raw_kw(db, "heron")
    species = _raw_kw(db, "HERON", is_species=1)  # a different merge slot
    lone = _raw_kw(db, "Egret")
    db.conn.commit()
    db.tag_photo(p0, upper)
    db.tag_photo(p1, upper)
    db.tag_photo(p2, lower)
    db.tag_photo(p3, species)
    db.tag_photo(p3, lone)
    groups = db.keyword_tree.workspace_duplicate_groups(ws)
    assert len(groups) == 1
    assert groups[0]["lname"] == "heron"
    assert groups[0]["cnt"] == 2
    assert {int(x) for x in groups[0]["ids"].split(",")} == {upper, lower}
    assert db.keyword_tree.workspace_duplicate_groups(None) == []
    assert tuple(db.photo_keywords.workspace_photo_count(upper, ws)) == ("Heron", 2)
    assert tuple(db.photo_keywords.workspace_photo_count(lower, ws)) == ("heron", 1)
    # An aggregate with no GROUP BY: always one row, NULL name when nothing matches.
    assert tuple(db.photo_keywords.workspace_photo_count(upper, ws + 999)) == (None, 0)


def test_search_species_keyword_names_matches_species_rows_only(db, lib):
    _raw_kw(db, "American Robin", is_species=1)
    _raw_kw(db, "European Robin", is_species=1)
    _raw_kw(db, "Robin Hood")  # not a species
    db.conn.commit()
    assert db.keyword_taxa.species_names_matching("robin", False, False) == [
        "American Robin", "European Robin",
    ]
    assert db.keyword_taxa.species_names_matching("robin", True, False) == []
    assert db.keyword_taxa.species_names_matching("Rob", False, True) == []
    assert db.keyword_taxa.species_names_matching("Robin", False, True) == [
        "American Robin", "European Robin",
    ]


def test_delete_keyword_detaches_children_unlinks_photos_with_the_callers_unit(db, lib):
    p0, p1, _, _ = lib["p"]
    parent = _raw_kw(db, "France", kw_type="location")
    child = _raw_kw(db, "Paris", parent_id=parent, kw_type="location")
    db.conn.commit()
    db.tag_photo(p0, parent)
    db.tag_photo(p1, child)
    # Writes the caller left open join the unit and share the delete's commit.
    db.queue_change(p1, "location", "effective")
    with db.transaction():
        db.keyword_tree.delete(parent)
    assert not db.conn.in_transaction
    assert _visible(db, "SELECT id FROM keywords WHERE id = ?", (parent,)) == []
    assert _visible(db, "SELECT parent_id FROM keywords WHERE id = ?", (child,)) == [(None,)]
    assert _visible(db, "SELECT photo_id, keyword_id FROM photo_keywords") == [(p1, child)]
    assert _visible(
        db, "SELECT photo_id FROM pending_changes WHERE change_type = 'location'"
    ) == [(p1,)]


# -- species-confirmation reads -----------------------------------------------------------


def test_get_species_root_keyword(db, lib):
    legacy = _raw_kw(db, "Robin", is_species=1)
    linked = _raw_kw(db, "robin", kw_type="taxonomy", taxon_id=3)
    _raw_kw(db, "ROBIN", parent_id=legacy, kw_type="taxonomy", taxon_id=3)  # not a root
    _raw_kw(db, "Robin ", kw_type="general")  # a different name
    _raw_kw(db, "robin", kw_type="location")  # not a species row
    db.conn.commit()
    row = db.keyword_taxa.species_root_by_name("ROBIN", prefer_taxonomy=True)
    assert tuple(row) == (linked, "robin", 3)
    assert db.keyword_taxa.species_root_by_name("ROBIN")["id"] in {legacy, linked}
    assert db.keyword_taxa.species_root_by_name("Wren") is None
    assert db.keyword_taxa.species_root_by_name("Wren", prefer_taxonomy=True) is None


def test_get_species_root_keyword_prefers_lowest_id_within_a_type(db, lib):
    first = _raw_kw(db, "Thrush", is_species=1)
    _raw_kw(db, "thrush", is_species=1)
    db.conn.commit()
    assert db.keyword_taxa.species_root_by_name("THRUSH", prefer_taxonomy=True)["id"] == first


def test_get_other_linked_species_names(db, lib):
    robin = _raw_kw(db, "American Robin", kw_type="taxonomy", taxon_id=3)
    alias = _raw_kw(db, "Robin", is_species=1, taxon_id=3)
    _raw_kw(db, "Cougar", kw_type="taxonomy", taxon_id=5)
    legacy = _raw_kw(db, "Legacy Bird", is_species=1)  # unlinked
    _raw_kw(db, "Thrushes", kw_type="general", taxon_id=2)  # not a species row
    db.conn.commit()
    # Linked target: rows on any other taxon.
    assert db.keyword_taxa.other_linked_species_names(3, robin) == ["Cougar"]
    # Unlinked target: every linked row but the target itself.
    assert sorted(db.keyword_taxa.other_linked_species_names(None, legacy)) == [
        "American Robin", "Cougar", "Robin",
    ]
    assert sorted(db.keyword_taxa.other_linked_species_names(None, alias)) == [
        "American Robin", "Cougar",
    ]


@pytest.fixture
def confirmed(db, lib):
    """Species rows on two photos: a linked root, an alias leaf, a homonym
    leaf on another taxon, a genus-rank row and a non-species row."""
    p0, p1, p2, _ = lib["p"]
    parent = _raw_kw(db, "Birds")
    root = _raw_kw(db, "Robin", kw_type="taxonomy", taxon_id=3)
    alias = _raw_kw(db, "Desert Robin", parent_id=parent, kw_type="taxonomy", taxon_id=3)
    homonym = _raw_kw(db, "robin", parent_id=parent, kw_type="taxonomy", taxon_id=5)
    genus = _raw_kw(db, "Robin", parent_id=root, kw_type="taxonomy", taxon_id=2)
    general = _raw_kw(db, "Garden", kw_type="general")
    db.conn.commit()
    for pid, kid in ((p0, alias), (p0, root), (p0, general), (p1, homonym),
                     (p1, genus), (p2, alias)):
        db.tag_photo(pid, kid)
    return {"p": lib["p"], "root": root, "alias": alias, "homonym": homonym}


def _rows(rows, *cols):
    return [tuple(r[c] for c in cols) for r in rows]


def test_get_previous_species_candidates(db, confirmed):
    p0, p1, p2, _ = confirmed["p"]
    root, alias, homonym = confirmed["root"], confirmed["alias"], confirmed["homonym"]
    # Linked root taxon: the taxon arm adds the alias. Root rows first, then
    # by keyword id (not by photo).
    rows = db.photo_species.previous_species_candidates([p0, p1, p2], "ROBIN", 3)
    assert [r["id"] for r in rows] == [root, alias, alias, homonym]
    assert set(_rows(rows, "photo_id", "id")) == {
        (p0, root), (p0, alias), (p2, alias), (p1, homonym),
    }
    assert tuple(rows[0]) == (root, "Robin", 3, p0)
    # Name only: the alias has another name.
    rows = db.photo_species.previous_species_candidates([p0, p1, p2], "robin", None)
    assert _rows(rows, "photo_id", "id") == [(p0, root), (p1, homonym)]
    assert db.photo_species.previous_species_candidates([], "Robin", 3) == []


def test_get_previous_species_candidates_chunks(db, confirmed):
    p0, p1, _, _ = confirmed["p"]
    statements = _trace(db)
    rows = db.photo_species.previous_species_candidates(
        list(range(10_000, 10_800)) + [p1, p0], "Robin", None,
    )
    db.conn.set_trace_callback(None)
    assert _rows(rows, "photo_id", "id") == [
        (p0, confirmed["root"]), (p1, confirmed["homonym"]),
    ]
    assert len([s for s in dict.fromkeys(statements) if "pk.photo_id IN" in s]) == 2


def test_get_attached_species_rows(db, confirmed):
    p0, p1, p2, p3 = confirmed["p"]
    root, alias, homonym = confirmed["root"], confirmed["alias"], confirmed["homonym"]
    rows = db.photo_species.attached_species_rows([p2, p1, p0, p3])
    # By photo, root rows first, then id; genus-rank and non-species rows
    # are left out.
    assert _rows(rows, "photo_id", "id") == [
        (p0, root), (p0, alias), (p1, homonym), (p2, alias),
    ]
    assert tuple(rows[1]) == (p0, alias, "Desert Robin", 3)
    statements = _trace(db)
    rows = db.photo_species.attached_species_rows(list(range(10_000, 10_800)) + [p1])
    db.conn.set_trace_callback(None)
    assert _rows(rows, "photo_id", "id") == [(p1, homonym)]
    assert len([s for s in dict.fromkeys(statements) if "pk.photo_id IN" in s]) == 2


def test_species_rank_keywords_for_photo(db, lib):
    p0, p1 = lib["p"][0], lib["p"][1]
    unlinked = _raw_kw(db, "Mystery", kw_type="taxonomy")
    species = _raw_kw(db, "American Robin", kw_type="taxonomy", is_species=1, taxon_id=3)
    genus = _raw_kw(db, "Turdus", kw_type="taxonomy", taxon_id=2)  # genus rank
    flagged = _raw_kw(db, "Old Species", is_species=1)  # general, is_species
    plain = _raw_kw(db, "Sunset")
    for kid in (unlinked, species, genus, flagged, plain):
        db.tag_photo(p0, kid)
    rows = db.photo_species.species_rank_keywords_for_photo(p0)
    # is_species rows first, then most recently tagged first.
    assert [tuple(r) for r in rows] == [
        (flagged, "Old Species", 1, "general"),
        (species, "American Robin", 1, "taxonomy"),
        (unlinked, "Mystery", 0, "taxonomy"),
    ]
    assert db.photo_species.species_rank_keywords_for_photo(p1) == []


def test_photo_ids_with_species_rank_keyword(db, lib):
    p0, p1, p2, p3 = lib["p"]
    db.tag_photo(p0, _raw_kw(db, "American Robin", kw_type="taxonomy", taxon_id=3))
    db.tag_photo(p0, _raw_kw(db, "Mystery", is_species=1))
    db.tag_photo(p1, _raw_kw(db, "Turdus", kw_type="taxonomy", taxon_id=2))
    db.tag_photo(p2, _raw_kw(db, "Sunset"))
    db.tag_photo(p3, _raw_kw(db, "Unlinked", kw_type="taxonomy"))
    assert db.photo_species.photo_ids_with_species_rank_keyword([p0, p1, p2, p3]) == {p0, p3}
    assert db.photo_species.photo_ids_with_species_rank_keyword([]) == set()

    statements = _trace(db)
    padded = list(range(10_000, 10_800)) + [p3]
    assert db.photo_species.photo_ids_with_species_rank_keyword(padded) == {p3}
    db.conn.set_trace_callback(None)
    assert len([s for s in dict.fromkeys(statements) if "pk.photo_id IN" in s]) == 2


def test_get_keyword_parent_rows(db, lib):
    root = db.add_keyword("Root")
    child = db.add_keyword("Child", parent_id=root)
    rows = {r["id"]: (r["name"], r["parent_id"]) for r in db.keyword_tree.parent_rows()}
    assert rows[root] == ("Root", None)
    assert rows[child] == ("Child", root)
    assert len(rows) == db.conn.execute("SELECT COUNT(*) FROM keywords").fetchone()[0]


def test_species_keywords_and_equivalents(db, lib):
    p0, p1, p2, p3 = lib["p"]
    root = db.add_keyword("American Robin", is_species=True)
    birds = db.add_keyword("Birds")
    leaf = _raw_kw(db, "american robin", birds, "taxonomy", 1, taxon_id=3)
    genus = db.add_keyword("Turdus")
    db.tag_photo(p0, root)
    db.tag_photo(p1, leaf)
    db.tag_photo(p2, genus)
    got = db.photo_species.get_species_for_photos([p0, p1, p2, p3])
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
    assert db.keyword_taxa.is_species(sp)
    assert not db.keyword_taxa.is_species(genus)
    assert not db.keyword_taxa.is_species(gen)
    assert not db.keyword_taxa.is_species(999_999)


def test_filter_out_subject_tagged(db, lib, monkeypatch):
    p0, p1, p2, p3 = lib["p"]
    db.tag_photo(p0, db.add_keyword("American Robin", is_species=True))
    legacy = _raw_kw(db, "Legacy Bird", kw_type="general", is_species=1)
    db.conn.commit()
    db.tag_photo(p1, legacy)
    db.tag_photo(p2, db.add_keyword("Party", kw_type="genre"))
    ids = [p3, p2, p1, p0]
    assert db.photo_keywords.filter_out_subject_tagged(ids, []) == ids
    assert db.photo_keywords.filter_out_subject_tagged([], ["taxonomy"]) == []
    assert db.photo_keywords.filter_out_subject_tagged(ids, ["bogus"]) == ids
    assert db.photo_keywords.filter_out_subject_tagged(ids, ["taxonomy"]) == [p3, p2]
    assert db.photo_keywords.filter_out_subject_tagged(ids, ["genre"]) == [p3, p1, p0]
    monkeypatch.setattr(Database, "_FILTER_SUBJECT_CHUNK", 2)
    statements = _trace(db)
    assert db.photo_keywords.filter_out_subject_tagged(tuple(ids), ["taxonomy", "genre"]) == [p3]
    db.conn.set_trace_callback(None)
    assert len([s for s in statements if "pk.photo_id IN" in s]) == 2


def test_keyword_tree_and_counts(db, lib):
    p0, p1 = lib["p"][:2]
    birds = db.add_keyword("Birds")
    heron = db.add_keyword("Heron", parent_id=birds)
    db.add_keyword("Unused")
    db.tag_photo(p0, heron)
    tree = [tuple(r) for r in db.keyword_tree.get_tree()]
    assert tree == [(birds, "Birds", None, "general"), (heron, "Heron", birds, "general")]
    assert db.keywords.count() == 1
    assert db.keywords.count_in_workspace() == 1
    db.tag_photo(p1, birds)
    db.conn.execute("UPDATE folders SET status = 'missing' WHERE id = ?", (lib["fid"],))
    assert db.keywords.count() == 0
    assert db.keywords.count_in_workspace() == 2
    rows = {r["name"]: r for r in db.keyword_tree.list_all()}
    assert set(rows) == {"Birds", "Heron"}
    db.set_active_workspace(None)
    for method in (db.keyword_tree.get_tree, db.keywords.count, db.keywords.count_in_workspace,
                   db.keyword_tree.list_all, db.photo_species.get_accepted_species):
        with pytest.raises(RuntimeError):
            method()


def test_get_all_keywords_counts(db, lib):
    """``photo_count`` is distinct workspace photos in the subtree; ``direct_photo_count`` is the keyword's own."""
    p0, p1, p2 = lib["p"][:3]
    other_fid = workspace_folders.add_folder(db, "/elsewhere", name="elsewhere")
    hidden = photo_ingest.add_photo(db, other_fid, "hidden.jpg", ".jpg", 99, 9.0)
    db.conn.execute("DELETE FROM workspace_folders WHERE folder_id = ?", (other_fid,))
    birds = db.add_keyword("Birds")
    herons = db.add_keyword("Herons", parent_id=birds)
    heron = db.add_keyword("Great Blue Heron", parent_id=herons)
    egret = db.add_keyword("Great Egret", parent_id=herons)
    db.tag_photo(p0, heron)
    db.tag_photo(p0, egret)  # one photo under two siblings counts once for each ancestor
    db.tag_photo(p1, egret)
    db.tag_photo(p2, birds)
    db.tag_photo(hidden, heron)  # tagged, but outside the active workspace
    rows = {r["name"]: (r["photo_count"], r["direct_photo_count"]) for r in db.keyword_tree.list_all()}
    assert rows == {
        "Birds": (3, 1),
        "Herons": (2, 0),
        "Great Blue Heron": (1, 1),
        "Great Egret": (2, 2),
    }


def test_get_all_keywords_counts_without_per_keyword_subqueries(db, lib):
    """A correlated count per keyword re-scanned the workspace's links for every row (~150 s on 65k photos)."""
    db.tag_photo(lib["p"][0], db.add_keyword("Heron", parent_id=db.add_keyword("Birds")))
    statements = _trace(db)
    db.keyword_tree.list_all()
    db.conn.set_trace_callback(None)
    (sql,) = [s for s in statements if "ws_links" in s]
    plan = [row[3] for row in db.conn.execute("EXPLAIN QUERY PLAN " + sql)]
    assert not [step for step in plan if "CORRELATED" in step], plan


def test_get_accepted_species(db, lib):
    p0, p1, p2 = lib["p"][:3]
    db.tag_photo(p0, db.add_keyword("American Robin", is_species=True))
    db.tag_photo(p1, db.add_keyword("Cougar", is_species=True))
    db.tag_photo(p2, db.add_keyword("Egret", is_species=True))
    db.conn.execute("UPDATE photos SET latitude = 1, longitude = 2 WHERE id = ?", (p0,))
    place = db.add_keyword("Park", kw_type="location")
    db.conn.execute("UPDATE keywords SET latitude = 1, longitude = 2 WHERE id = ?", (place,))
    db.tag_photo(p1, place)
    assert db.photo_species.get_accepted_species() == ["American Robin", "Cougar"]


# -- seeds and legacy migrations ----------------------------------------------------------


def test_ensure_default_genre_keywords_is_idempotent(db):
    before = db.conn.execute("SELECT COUNT(*) FROM keywords WHERE type = 'genre'").fetchone()[0]
    db.keywords.ensure_default_genres()
    assert not db.conn.in_transaction
    after = db.conn.execute("SELECT COUNT(*) FROM keywords WHERE type = 'genre'").fetchone()[0]
    assert after == before


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


def test_update_keyword_plain_fields_commit_with_the_callers_unit(db, lib):
    with db.transaction():
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
    with db.transaction():
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
    db.commit()
    db.queue_change(p1, "keyword_add", "Egret", workspace_id=ws)
    db.commit()

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
    db.commit()

    assert db.keyword_tree.merge_duplicates() >= 1

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


def test_merge_duplicate_keywords_merges_with_the_callers_unit(db, lib, monkeypatch):
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
    with db.transaction():
        total = db.keyword_tree.merge_duplicates()
    assert total >= 1
    assert all(dst == a for _src, dst in merges)
    assert not db.conn.in_transaction
    assert db.keyword_tree.merge_duplicates() == 0


def test_merge_duplicate_keywords_rolls_back_on_failure(db, lib, monkeypatch):
    p0, p1 = lib["p"][:2]
    a = _raw_kw(db, "Heron")
    b = _raw_kw(db, "heron")
    db.conn.commit()
    db.tag_photo(p0, a)
    db.tag_photo(p1, b)
    db.commit()

    def boom(src, dst, **kwargs):
        db.conn.execute("UPDATE keywords SET name = 'changed' WHERE id = ?", (dst,))
        raise RuntimeError("merge failed")

    monkeypatch.setattr(db, "_merge_keyword_into", boom)
    with pytest.raises(RuntimeError, match="merge failed"), db.transaction(immediate=True):
        db.keyword_tree.merge_duplicates()
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
    from repositories.keyword_tree import KeywordTreeRepository

    monkeypatch.setattr(
        KeywordTreeRepository, "normalize_row_name", lambda _repo, kid: calls.append(kid),
    )

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
    db.keyword_tree.normalize_row_name(999_999)
    birds = _raw_kw(db, "Birds")
    clean = _raw_kw(db, "Heron", birds)
    kid = _raw_kw(db, '"Heron"', birds, kw_type="taxonomy")
    db.conn.commit()
    renames = []
    monkeypatch.setattr(db, "_rename_keyword_dependents",
                        lambda k, old, new: renames.append((k, old, new)))
    db.keyword_tree.normalize_row_name(clean)
    db.keyword_tree.normalize_row_name(kid)  # collides with the general 'Heron'
    assert renames == []
    assert _kw(db, kid)[0] == '"Heron"'
    free = _raw_kw(db, "'Egret'")
    db.keyword_tree.normalize_row_name(free)
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
                        lambda o, n, photo_workspace_pairs=None:
                        calls.append(("hl", o, n, photo_workspace_pairs)))
    monkeypatch.setattr(db, "rename_photo_preferences_species",
                        lambda o, n, photo_workspace_pairs=None:
                        calls.append(("pref", o, n, photo_workspace_pairs)))
    statements = _trace(db)
    db._rename_keyword_dependents(kid, "'Verdin'", "'Verdin'")
    db._rename_keyword_dependents(kid, "", "Verdin")
    db.conn.set_trace_callback(None)
    assert statements == []
    db._rename_keyword_dependents(kid, "'Verdin'", "Verdin")
    assert calls == [
        ("hl", "'Verdin'", "Verdin", [(p0, ws)]),
        ("pref", "'Verdin'", "Verdin", [(p0, ws)]),
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


# -- duplicate photo species repair --------------------------------------------------------


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
    # The caller's unit commits; the repository writes and returns.
    with db.transaction():
        assert db.keyword_taxa.mark_species(tax) == 2
        assert db.conn.in_transaction
    assert not db.conn.in_transaction
    assert _kw(db, general)[2:5] == ("taxonomy", 1, 3)
    assert _kw(db, rebind)[4] == 5
    assert _kw(db, keep)[4] == 2
    assert _kw(db, place)[2] == "location"
    assert _kw(db, unknown)[2] == "general"
    statements = _trace(db)
    assert db.keyword_taxa.mark_species(tax) == 0
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
    from repositories.keyword_taxa import KeywordTaxonRepository

    calls = []
    real = KeywordTaxonRepository.resolve_species_by_lineage
    monkeypatch.setattr(KeywordTaxonRepository, "resolve_species_by_lineage",
                        lambda self, n, a: calls.append((n, a)) or real(self, n, a))
    assert db.keyword_taxa.mark_species(tax) == 1
    assert calls == [("Turdus migratorius", ["Aves", "Turdus"])]
    assert _kw(db, kid)[4] == 3


def test_resolve_species_by_lineage(db, lib):
    assert db.keyword_taxa.resolve_species_by_lineage("Nope", ["Aves"]) is None
    assert db.keyword_taxa.resolve_species_by_lineage("Turdus migratorius", []) is None
    assert db.keyword_taxa.resolve_species_by_lineage("Turdus migratorius", ["Aves"])["id"] == 3
    assert db.keyword_taxa.resolve_species_by_lineage("Turdus migratorius", ["Mammalia"]) is None
    # A dangling parent pointer ends the walk.
    db.conn.execute("PRAGMA foreign_keys = OFF")
    _taxon(db, 9, "Orphan species", "species", 777)
    db.conn.execute("PRAGMA foreign_keys = ON")
    assert db.keyword_taxa.resolve_species_by_lineage("Orphan species", ["Aves"]) is None
    # Two matching homonyms are ambiguous.
    _taxon(db, 10, "Turdus migratorius", "species", 2)
    assert db.keyword_taxa.resolve_species_by_lineage("Turdus migratorius", ["Aves"]) is None


# -- structure ----------------------------------------------------------------------------


# Still on ``Database``: the forwarders other repositories bind by name
# (``FACADE_METHODS``), ``untag_photo`` (kept beside ``tag_photo`` for the
# structural guards that find keyword writers by name), and the coordinated
# methods that delegate their statements; each mapped to the factory of the
# repository that holds its SQL.
_DELEGATING_KEYWORD_METHODS = {
    "add_keyword": "_keyword_repository",
    "_merge_duplicate_keywords_pass": "_keyword_tree_repository",
    "_rename_keyword_dependents": "_keyword_tree_repository",
    "_reparent_disambiguated": "_keyword_tree_repository",
    "untag_photo": "_photo_keyword_repository",
    "get_photos_with_equivalent_species": "_photo_species_repository",
    "update_keyword": "_keyword_tree_repository",
}

# The keyword-domain repositories that still bind ``Database`` methods by name
# (``FACADE_METHODS``). The others take no façade at all.
_KEYWORD_REPOSITORIES_WITH_FACADE_METHODS = {"repositories.keyword_tree"}

# Every keyword-domain repository: its module, class, ``Database`` accessor
# and the factory the accessor builds it through.
_KEYWORD_REPOSITORIES = {
    "repositories.keywords": ("KeywordRepository", "keywords", "_keyword_repository"),
    "repositories.keyword_taxa": (
        "KeywordTaxonRepository", "keyword_taxa", "_keyword_taxon_repository",
    ),
    "repositories.keyword_tree": (
        "KeywordTreeRepository", "keyword_tree", "_keyword_tree_repository",
    ),
    "repositories.photo_keywords": (
        "PhotoKeywordRepository", "photo_keywords", "_photo_keyword_repository",
    ),
    "repositories.photo_species": (
        "PhotoSpeciesRepository", "photo_species", "_photo_species_repository",
    ),
}

# The forwarding wrappers this domain used to have on ``Database``; callers
# reach the same methods through ``db.keywords`` now.
_REMOVED_KEYWORD_WRAPPERS = (
    "filter_out_subject_tagged",
    "ensure_default_genre_keywords",
    "count_keywords",
    "count_keywords_in_workspace",
    "get_accepted_species",
    "relink_source_species_keywords",
    "merge_duplicate_keywords",
    "_normalize_keyword_row_name",
    "get_keyword_tree",
    "get_keyword_name",
    "get_top_level_species_keyword",
    "get_keyword_row",
    "get_photo_ids_with_keyword",
    "get_keyword_parent_rows",
    "get_keyword_rename_state",
    "get_photo_workspaces_with_keyword",
    "get_keyword_subtree_photo_workspaces",
    "get_keyword_duplicate_groups",
    "get_keyword_workspace_photo_count",
    "search_species_keyword_names",
    "delete_keyword",
    "get_species_root_keyword",
    "get_other_linked_species_names",
    "get_previous_species_candidates",
    "get_attached_species_rows",
    "get_species_rank_keywords_for_photo",
    "get_photo_ids_with_species_rank_keyword",
    "get_photo_keywords",
    "get_keywords_for_photos",
    "get_species_keyword_identity_rows",
    "get_species_keywords_for_photos",
    "get_all_keywords",
    "is_keyword_species",
    "get_embedded_keyword_offered_keys",
    "record_embedded_keyword_offered",
    "transfer_embedded_keyword_offered_for_merge",
    "mark_species_keywords",
    "detect_keyword_case_convention",
    "resolve_species_display_name",
    "species_case_convention",
    "_apply_case_convention",
    "_sentence_case_first_word",
    "_species_root_name_for_taxon",
    "_lookup_taxon_id_for_keyword",
    "_add_source_species_keyword",
    "_resolve_species_by_lineage",
)

# The provenance writers and the methods that call ``_merge_keyword_into``
# mid-flight live in ``KeywordProvenanceRepository``, not here (see
# ``test_db_keyword_provenance``); ``test_keyword_provenance_contract`` keys
# the writers to that module.
_KEYWORD_METHODS_IN_PROVENANCE_REPOSITORY = (
    "tag_photo",
    "_merge_keyword_into",
)

_PROVENANCE_WRITERS = (
    "tag_photo",
    "_merge_keyword_into",
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


@pytest.mark.parametrize("name, factory", sorted(_DELEGATING_KEYWORD_METHODS.items()))
def test_keyword_method_delegates_to_repository(name, factory):
    attrs = _self_attrs(getattr(Database, name))
    assert "conn" not in attrs, (
        f"Database.{name} touches self.conn; move the SQL to a keyword repository"
    )
    assert factory in attrs, (
        f"Database.{name} no longer delegates through {factory}"
    )


def test_keywords_has_no_forwarding_wrappers_on_database():
    """The domain is reached through ``db.keywords``; Database keeps no aliases."""
    for name in _REMOVED_KEYWORD_WRAPPERS:
        assert not hasattr(Database, name), f"Database.{name} came back; call db.keywords"
    for _cls, name, factory in _KEYWORD_REPOSITORIES.values():
        accessor = Database.__dict__[name]
        assert isinstance(accessor, property)
        attrs = _self_attrs(accessor.fget)
        assert factory in attrs, name
        assert "conn" not in attrs, name


@pytest.mark.parametrize("module_name", sorted(_KEYWORD_REPOSITORIES))
def test_keyword_accessors_build_a_fresh_repository_per_access(db, module_name):
    import importlib

    class_name, name, _factory = _KEYWORD_REPOSITORIES[module_name]
    cls = getattr(importlib.import_module(module_name), class_name)
    first = getattr(db, name)
    assert isinstance(first, cls)
    assert getattr(db, name) is not first
    assert first.conn is db.conn


@pytest.mark.parametrize("name", _KEYWORD_METHODS_IN_PROVENANCE_REPOSITORY)
def test_provenance_writers_and_their_merge_callers_delegate_to_provenance_repository(
    name,
):
    attrs = _self_attrs(getattr(Database, name))
    assert "conn" not in attrs, (
        f"Database.{name} touches self.conn; its SQL belongs in "
        "KeywordProvenanceRepository"
    )
    assert "_keyword_provenance_repository" in attrs, (
        f"Database.{name} no longer delegates to KeywordProvenanceRepository"
    )
    assert "_keyword_repository" not in attrs, (
        f"Database.{name} is a photo_keywords writer (or merges mid-flight); "
        "it must not route through KeywordRepository"
    )


@pytest.mark.parametrize("name", ["_merge_duplicate_keywords_pass", "update_keyword"])
def test_split_methods_still_call_merge_on_the_facade(name):
    assert "_merge_keyword_into" in _self_attrs(getattr(Database, name))


@pytest.mark.parametrize("module_name", sorted(_KEYWORD_REPOSITORIES))
def test_keyword_repository_never_references_provenance_writers(module_name):
    import importlib

    module = importlib.import_module(module_name)
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
    assert not set(getattr(module, "FACADE_METHODS", ())) & set(_PROVENANCE_WRITERS)


@pytest.mark.parametrize("module_name", sorted(
    name for name in _KEYWORD_REPOSITORIES
    if name in _KEYWORD_REPOSITORIES_WITH_FACADE_METHODS
))
def test_keyword_repository_routes_facade_calls_through_database(db, module_name):
    import importlib

    module = importlib.import_module(module_name)
    _class_name, _accessor, factory = _KEYWORD_REPOSITORIES[module_name]
    repo = getattr(db, factory)()
    assert module.FACADE_METHODS
    for name in module.FACADE_METHODS:
        assert getattr(repo, name) == getattr(db, name), name


def test_keyword_repositories_without_facade_methods_take_no_facade():
    import importlib

    for module_name in set(_KEYWORD_REPOSITORIES) - _KEYWORD_REPOSITORIES_WITH_FACADE_METHODS:
        module = importlib.import_module(module_name)
        assert not hasattr(module, "FACADE_METHODS"), module_name
        class_name = _KEYWORD_REPOSITORIES[module_name][0]
        params = inspect.signature(getattr(module, class_name)).parameters
        assert "facade" not in params, module_name


def test_keyword_facade_signatures_unchanged():
    sig = {n: calling_signature(getattr(Database, n))
           for n in _DELEGATING_KEYWORD_METHODS}
    assert sig["add_keyword"] == (
        "(self, name, parent_id=None, is_species=False, kw_type=None, "
        "source_taxon_id=None, _resolve_alias=False)"
    )
    assert sig["untag_photo"] == (
        "(self, photo_id, keyword_id, *, _record_history=False)"
    )
    assert sig["update_keyword"] == "(self, keyword_id, **kwargs)"
    assert sig["get_photos_with_equivalent_species"] == (
        "(self, photo_ids, keyword_id, exclude_keyword_ids=None)"
    )
    from repositories.keyword_taxa import KeywordTaxonRepository
    from repositories.keyword_tree import KeywordTreeRepository
    from repositories.keywords import KeywordRepository
    from repositories.photo_species import PhotoSpeciesRepository

    assert calling_signature(KeywordTaxonRepository.resolve_species_display).startswith(
        "(self, name, apply_case_convention=True, *, case_convention=<object object"
    )
    assert calling_signature(KeywordRepository.add_source_species) == (
        "(self, name, source_taxon_id, parent_id=None)"
    )
    assert calling_signature(KeywordTaxonRepository.lookup_taxon_id) == (
        "(self, name, prefer_species=False, species_only=False)"
    )

    params = inspect.signature(PhotoSpeciesRepository.get_species_for_photos).parameters
    assert [(p.name, p.default) for p in params.values()] == [
        ("self", inspect.Parameter.empty),
        ("photo_ids", inspect.Parameter.empty),
        ("include_identities", False),
    ]
    params = inspect.signature(KeywordTreeRepository.normalize_row_name).parameters
    assert list(params) == ["self", "keyword_id"]


@pytest.mark.parametrize("module_name", sorted(_KEYWORD_REPOSITORIES))
def test_keyword_repository_never_hands_itself_out_as_the_database(module_name):
    """Moved bodies that passed ``self`` (the Database) to a helper must pass
    ``self.db`` now; a bare ``self`` argument would hand over the repository."""
    import importlib

    module = importlib.import_module(module_name)
    tree = ast.parse(inspect.getsource(module))
    parents = {child: node for node in ast.walk(tree) for child in ast.iter_child_nodes(node)}
    bare = [
        node.lineno for node in ast.walk(tree)
        if isinstance(node, ast.Name) and node.id == "self"
        and not (isinstance(parents.get(node), ast.Attribute)
                 and parents[node].value is node)
        and not isinstance(parents.get(node), ast.arguments)
    ]
    # The only one is ``__init__`` storing the façade (``setattr(self,
    # ...)``) in a repository that still binds façade methods; the others
    # have none.
    assert len(bare) == (1 if hasattr(module, "FACADE_METHODS") else 0)


# -- species filed in the user's tree vs. top-level twins ----------------------------------


def _placed_robin(db, parent_name="Thrushes"):
    """``<parent_name> > American Robin``: the user's own taxonomy row."""
    parent = _raw_kw(db, parent_name)
    return parent, _raw_kw(
        db, "American Robin", parent, kw_type="taxonomy", is_species=1, taxon_id=3,
    )


def _top_level_rows(db, name):
    return db.conn.execute(
        "SELECT id FROM keywords WHERE name = ? COLLATE NOCASE AND parent_id IS NULL",
        (name,),
    ).fetchall()


@pytest.mark.parametrize("kwargs", [
    {"kw_type": "taxonomy"},
    {"is_species": True},
    {},
    {"_resolve_alias": True},
])
def test_add_keyword_reuses_species_filed_in_tree(db, lib, kwargs):
    _parent, placed = _placed_robin(db)
    db.conn.commit()
    assert db.add_keyword("american robin", **kwargs) == placed
    assert _top_level_rows(db, "American Robin") == []


def test_add_keyword_taxonomy_passes_over_top_level_general_for_placed_species(db, lib):
    _parent, placed = _placed_robin(db)
    general = _raw_kw(db, "American Robin")
    db.conn.commit()
    assert db.add_keyword("American Robin", kw_type="taxonomy") == placed
    # Promoting the general row would have minted the twin.
    assert _kw(db, general)[2:4] == ("general", 0)
    # A type-agnostic add keeps the top-level row it names.
    assert db.add_keyword("American Robin") == general


def test_add_keyword_ambiguous_placed_species_mints_top_level(db, lib):
    _placed_robin(db, "Thrushes")
    _placed_robin(db, "Backyard birds")
    db.conn.commit()
    kid = db.add_keyword("American Robin", kw_type="taxonomy")
    assert _kw(db, kid)[1:3] == (None, "taxonomy")


def test_add_keyword_explicit_general_ignores_placed_species(db, lib):
    _parent, placed = _placed_robin(db)
    db.conn.commit()
    kid = db.add_keyword("American Robin", kw_type="general")
    assert kid != placed
    assert _kw(db, kid)[1:3] == (None, "general")


def test_add_source_species_binds_species_filed_in_tree(db, lib):
    _parent, placed = _placed_robin(db)
    db.conn.commit()
    assert db.keywords.add_source_species("American Robin", 12727) == placed
    assert _kw(db, placed)[4:] == (3, 12727)
    assert _top_level_rows(db, "American Robin") == []


def test_add_source_species_ambiguous_placed_rows_mint_top_level(db, lib):
    _placed_robin(db, "Thrushes")
    _placed_robin(db, "Backyard birds")
    db.conn.commit()
    kid = db.keywords.add_source_species("American Robin", 12727)
    assert _kw(db, kid)[1] is None


def test_merge_taxonomy_root_twins_folds_top_level_rows_into_tree(db, lib):
    p0, p1, p2 = lib["p"][:3]
    _parent, placed = _placed_robin(db)
    twin = _raw_kw(db, "American Robin", kw_type="taxonomy", is_species=1, taxon_id=3)
    db.conn.execute("UPDATE keywords SET source_taxon_id = 12727 WHERE id = ?", (twin,))
    # Not twins: a top-level row with children, one whose placed row claims
    # another iNat id, one whose only same-name row is another taxon, and
    # one spelled in another case.
    case_variant = _raw_kw(db, "Cougar cub", kw_type="taxonomy", taxon_id=5)
    _raw_kw(db, "Cougar Cub", _raw_kw(db, "Young"), kw_type="taxonomy", taxon_id=5)
    parent_root = _raw_kw(db, "Cougar", kw_type="taxonomy", is_species=1, taxon_id=5)
    _raw_kw(db, "Cougar", _raw_kw(db, "Cats"), kw_type="taxonomy", is_species=1, taxon_id=5)
    _raw_kw(db, "Cougar kitten", parent_root)
    conflicting = _raw_kw(db, "Puma", kw_type="taxonomy", taxon_id=4)
    db.conn.execute("UPDATE keywords SET source_taxon_id = 1 WHERE id = ?", (conflicting,))
    placed_puma = _raw_kw(db, "Puma", _raw_kw(db, "Genera"), kw_type="taxonomy", taxon_id=4)
    db.conn.execute("UPDATE keywords SET source_taxon_id = 2 WHERE id = ?", (placed_puma,))
    other_taxon = _raw_kw(db, "Turdus", kw_type="taxonomy", taxon_id=2)
    _raw_kw(db, "Turdus", _raw_kw(db, "Aves"), kw_type="taxonomy", taxon_id=1)
    db.conn.commit()
    db.tag_photo(p0, twin, source=None)
    db.tag_photo(p1, placed, source=None)
    db.tag_photo(p1, twin)
    db.tag_photo(p2, placed, source=None)
    db.queue_change(p1, "keyword_add", "American Robin")
    db.commit()
    pending = _visible(db, "SELECT photo_id, change_type, value FROM pending_changes")

    with db.transaction(immediate=True):
        assert db.merge_taxonomy_root_twins() == 1

    assert not db.conn.in_transaction
    assert _visible(db, "SELECT id FROM keywords WHERE id = ?", (twin,)) == []
    assert _visible(
        db, "SELECT photo_id, source FROM photo_keywords WHERE keyword_id = ? ORDER BY photo_id",
        (placed,),
    ) == [(p0, None), (p1, "manual"), (p2, None)]
    assert _kw(db, placed)[4:] == (3, 12727)
    # Same flat keyword in every sidecar: the merge queues no XMP change.
    assert _visible(db, "SELECT photo_id, change_type, value FROM pending_changes") == pending
    for kept in (parent_root, conflicting, other_taxon, case_variant):
        assert _kw(db, kept) is not None
    assert db.merge_taxonomy_root_twins() == 0


def test_merge_taxonomy_root_twins_rolls_back_on_failure(db, lib, monkeypatch):
    _parent, placed = _placed_robin(db)
    twin = _raw_kw(db, "American Robin", kw_type="taxonomy", is_species=1, taxon_id=3)
    db.conn.commit()
    db.tag_photo(lib["p"][0], twin)
    db.commit()

    def boom(src, dst, **kwargs):
        db.conn.execute("DELETE FROM photo_keywords WHERE keyword_id = ?", (src,))
        raise RuntimeError("merge failed")

    monkeypatch.setattr(db, "_merge_keyword_into", boom)
    with pytest.raises(RuntimeError, match="merge failed"), db.transaction(immediate=True):
        db.merge_taxonomy_root_twins()
    assert not db.conn.in_transaction
    assert _visible(db, "SELECT photo_id FROM photo_keywords WHERE keyword_id = ?",
                    (twin,)) == [(lib["p"][0],)]


def test_startup_repairs_merge_taxonomy_root_twins(db, lib, monkeypatch, caplog):
    import logging

    from services.startup_tasks import StartupTasks

    _parent, placed = _placed_robin(db)
    twin = _raw_kw(db, "American Robin", kw_type="taxonomy", is_species=1, taxon_id=3)
    db.conn.commit()
    tasks = StartupTasks(app=None, db_path=db._db_path, init_db=db)
    tasks.run_catalog_repairs()
    assert _kw(db, twin) is None

    def boom():
        raise RuntimeError("disk I/O error")

    monkeypatch.setattr(db, "merge_taxonomy_root_twins", boom)
    with caplog.at_level(logging.ERROR, logger="services.startup_tasks"):
        tasks.run_catalog_repairs()
    assert "Could not merge top-level species keyword twins" in caplog.text
