"""Selected AST and pure lookup helpers; SQLite :memory: only; no real data."""

import __future__

import ast
import builtins
import contextlib
import json
import logging
import sqlite3
import types
import unittest
import uuid
from pathlib import Path

from unit_of_work import UnitOfWork

ROOT = Path(__file__).resolve().parents[1]


def forbidden(*args, **kwargs):
    raise AssertionError("Unexpected product/system operation")


def load(file, ns, methods=None, functions=None):
    tree = ast.parse((ROOT / file).read_text(encoding="utf-8"))
    nodes = []
    for n in tree.body:
        if isinstance(n, ast.FunctionDef) and (functions is None or n.name in functions):
            nodes.append(n)
        if isinstance(n, ast.ClassDef) and methods and n.name in methods:
            n.body = [f for f in n.body if isinstance(f, ast.FunctionDef) and f.name in methods[n.name]]
            nodes.append(n)
    exec(
        compile(
            ast.Module(body=nodes, type_ignores=[]),
            "<selected-" + file + ">",
            "exec",
            flags=__future__.annotations.compiler_flag,
        ),
        ns,
    )


def setup():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript("""CREATE TABLE taxa(id INTEGER PRIMARY KEY, inat_id INTEGER, name TEXT, common_name TEXT, rank TEXT);
 CREATE TABLE taxa_common_names(taxon_id INTEGER,name TEXT);
 CREATE TABLE keywords(id INTEGER PRIMARY KEY,name TEXT,parent_id INTEGER REFERENCES keywords(id),type TEXT DEFAULT 'general',is_species INTEGER DEFAULT 0,taxon_id INTEGER REFERENCES taxa(id),source_taxon_id INTEGER,place_id TEXT,latitude REAL,longitude REAL,UNIQUE(name,parent_id));
 CREATE TABLE photo_identity_alternatives(photo_id INTEGER,group_id TEXT,candidates TEXT);
 CREATE TABLE photo_keywords(photo_id INTEGER,keyword_id INTEGER REFERENCES keywords(id),source TEXT,species_provenance TEXT,PRIMARY KEY(photo_id,keyword_id));
 CREATE TABLE keyword_import_aliases(path_key TEXT PRIMARY KEY,path_json TEXT,keyword_id INTEGER REFERENCES keywords(id) ON DELETE CASCADE);
 CREATE TABLE pending_changes(id INTEGER PRIMARY KEY,photo_id INTEGER,workspace_id INTEGER,change_type TEXT,value TEXT);
 CREATE TABLE edit_history(id INTEGER PRIMARY KEY,action_type TEXT,new_value TEXT,old_value TEXT,workspace_id INTEGER);
 CREATE TABLE edit_history_items(id INTEGER PRIMARY KEY,edit_id INTEGER REFERENCES edit_history(id),photo_id INTEGER,new_value TEXT,old_value TEXT,species_provenance TEXT);
 INSERT INTO taxa VALUES(1,101,'Inventus avis','Invented bird','species'),(2,202,'Inventus','Other species','genus');
 """)
    from repositories.species_taxon_lookup import lookup_taxon_id, species_preferring_taxon_match
    ns = {
        "__builtins__": {**vars(builtins), "__import__": forbidden},
        "json": json,
        "_lookup_species_taxon_id": lookup_taxon_id,
        "_species_taxon_match": species_preferring_taxon_match,
        "uuid": uuid,
        "sqlite3": sqlite3,
        "log": logging.getLogger("invented"),
        "contextmanager": contextlib.contextmanager,
        "contextlib": contextlib,
        "keyword_match_key": lambda x: x.lower(),
        "normalize_keyword_display": lambda x: x.strip(),
        "KEYWORD_SOURCE_UNKNOWN": None,
        "KEYWORD_SOURCE_PRECEDENCE": (None, "accept", "manual"),
        "_KEYWORD_SOURCE_RANK": {None: 0, "accept": 1, "manual": 2},
        "_KEYWORD_SOURCE_UNRECOGNIZED_RANK": 1,
        "_MERGE_KEYWORD_ID_ACTIONS": ("keyword_add", "keyword_remove", "prediction_accept", "species_replace"),
    }
    load(
        "db.py",
        ns,
        methods={"Database": {"merge_taxonomy_root_twins", "transaction", "in_transaction", "begin_immediate"}},
        functions={"keyword_source_rank", "keyword_source_max", "keyword_source_rank_sql", "keyword_source_max_sql"},
    )
    load(
        "repositories/keywords.py",
        ns,
        methods={
            "KeywordRepository": {
                "_placed_taxonomy_keyword",
                "_find_add_candidate",
                "top_level_species_keyword",
                "add_source_species",
                "add",
                "_reconcile_species_type",
                "_backfill_candidate_taxon",
                "_promote_candidate_type",
                "_new_keyword_name_and_type",
                "_insert_new_keyword",
            }
        },
        functions=set(),
    )
    load(
        "repositories/keyword_taxa.py",
        ns,
        methods={"KeywordTaxonRepository": {"lookup_taxon_id", "_species_preferring_taxon_match"}},
        functions=set(),
    )
    load(
        "repositories/keyword_tree.py",
        ns,
        methods={"KeywordTreeRepository": {"taxonomy_root_twins"}},
        functions=set(),
    )
    load("repositories/identity_alternatives.py", ns)
    load("repositories/keyword_provenance.py", ns, methods={"KeywordProvenanceRepository": {"merge_keyword_into"}})
    species_prov_ns = {
        "__builtins__": {**vars(builtins), "__import__": forbidden},
        "json": json,
        "sqlite3": sqlite3,
    }
    load("repositories/species_provenance.py", species_prov_ns,
         functions={"retarget_keyword_history", "_fold_history_state"})
    # One stand-in object answers for the three keyword repositories the
    # selected methods are split across.
    for name in ("lookup_taxon_id", "_species_preferring_taxon_match"):
        setattr(ns["KeywordRepository"], name, getattr(ns["KeywordTaxonRepository"], name))
    ns["KeywordRepository"].taxonomy_root_twins = ns["KeywordTreeRepository"].taxonomy_root_twins
    db = ns["Database"]()
    db.conn = conn
    db._unit = UnitOfWork()
    db._species_provenance_before = {}
    repo = ns["KeywordRepository"]()
    repo.conn = conn
    db.keywords = repo
    db.keyword_tree = repo
    repo.db = db

    class Resolver:
        def __init__(self, **kwargs):
            pass

        def resolve(self, name, **kwargs):
            return types.SimpleNamespace(display_name=name)

    modules = {
        "species_identity": types.SimpleNamespace(COMMON_NAME_CORRECTIONS={}, SpeciesResolver=Resolver),
        "repositories.species_provenance": types.SimpleNamespace(
            retarget_keyword_history=species_prov_ns["retarget_keyword_history"],
        ),
    }
    ns["__builtins__"]["__import__"] = lambda name, *a, **kw: modules[name] if name in modules else forbidden()
    repo._lookup_taxon_id_for_keyword = repo.lookup_taxon_id
    repo._taxon_lookup_variants = lambda name: [name]
    repo.resolve_species_display_name = lambda name: name
    repo.species_case_convention = lambda: None
    repo._apply_case_convention = lambda name, case: name
    repo.KEYWORD_TYPES = {"general", "taxonomy", "location", "individual", "genre"}

    prov = ns["KeywordProvenanceRepository"]()
    prov.conn = conn
    prov.db = db
    prov._chunks = lambda xs: [list(xs)[i : i + 100] for i in range(0, len(xs), 100)]
    prov._edit_prediction_ids = lambda *_a: []
    prov.rename_species_highlights_species = forbidden
    prov.rename_photo_preferences_species = forbidden
    prov.keyword_source_max_sql = ns["keyword_source_max_sql"]
    prov.keyword_source_max = ns["keyword_source_max"]
    prov._merge_keyword_into = prov.merge_keyword_into
    prov._reparent_disambiguated = forbidden
    db._merge_keyword_into = prov.merge_keyword_into

    def kw(name="Invented bird", parent=None, taxon=1, source=None, kind="taxonomy"):
        return conn.execute(
            "INSERT INTO keywords(name,parent_id,type,is_species,taxon_id,source_taxon_id) VALUES(?,?,?,1,?,?)",
            (name, parent, kind, taxon, source),
        ).lastrowid

    parent = kw("Invented parent", taxon=None, kind="general")
    conn.commit()
    return conn, repo, db, kw, parent, ns


class Checks(unittest.TestCase):
    def test_success_references_provenance_pending_and_idempotence(self):
        c, r, d, k, p, ns = setup()
        self.addCleanup(c.close)
        dst = k(parent=p)
        src = k(source=101)
        c.executemany(
            "INSERT INTO photo_keywords(photo_id,keyword_id,source) VALUES(?,?,?)",
            [(1, src, None), (2, src, "manual"), (2, dst, "accept"), (3, dst, "manual")],
        )
        c.execute("INSERT INTO keyword_import_aliases VALUES(?,?,?)", ("invented", '["Invented bird"]', src))
        c.execute("INSERT INTO pending_changes VALUES(1,2,7,'keyword_add','Invented bird')")
        c.execute("INSERT INTO edit_history VALUES(1,'keyword_add',?,NULL,7)", (str(src),))
        c.execute(
            "INSERT INTO edit_history_items(id,edit_id,photo_id,new_value,old_value) VALUES(1,1,1,?,NULL)",
            (str(src),),
        )
        c.commit()
        pending = [tuple(x) for x in c.execute("SELECT * FROM pending_changes")]
        self.assertEqual(d.merge_taxonomy_root_twins(), 1)
        self.assertEqual(
            [
                tuple(x)
                for x in c.execute(
                    "SELECT photo_id,source FROM photo_keywords WHERE keyword_id=? ORDER BY photo_id", (dst,)
                )
            ],
            [(1, None), (2, "manual"), (3, "manual")],
        )
        self.assertEqual(c.execute("SELECT source_taxon_id FROM keywords WHERE id=?", (dst,)).fetchone()[0], 101)
        self.assertEqual(c.execute("SELECT keyword_id FROM keyword_import_aliases").fetchone()[0], dst)
        self.assertEqual(c.execute("SELECT new_value FROM edit_history_items").fetchone()[0], str(dst))
        self.assertEqual([tuple(x) for x in c.execute("SELECT * FROM pending_changes")], pending)
        self.assertEqual(d.merge_taxonomy_root_twins(), 0)
        self.assertEqual(c.execute("PRAGMA foreign_key_check").fetchall(), [])

    def test_conflicting_root_identities_must_not_collapse_into_one_placed_row(self):
        c, r, d, k, p, ns = setup()
        self.addCleanup(c.close)
        dst = k(parent=p)
        a = k(source=101)
        b = k(source=202)
        c.executemany(
            "INSERT INTO photo_keywords(photo_id,keyword_id,source) VALUES(?,?,?)",
            [(1, a, "manual"), (2, b, "manual")],
        )
        c.commit()
        self.assertEqual(r.taxonomy_root_twins(), [])
        merged = d.merge_taxonomy_root_twins()
        c.commit()
        self.assertEqual(merged, 0, "conflicting explicit source identities must remain for review")

    def test_exact_spelling_case_ambiguity_and_conflict_boundaries(self):
        for case in ["same", "case", "different_taxon", "conflicting_source", "two_placed", "child"]:
            with self.subTest(case=case):
                c, r, d, k, p, ns = setup()
                self.addCleanup(c.close)
                dst = k(parent=p, source=202 if case == "conflicting_source" else None)
                src = k(
                    name="invented bird" if case == "case" else "Invented bird",
                    taxon=2 if case == "different_taxon" else 1,
                    source=101,
                )
                if case == "two_placed":
                    k(parent=k("Another parent", taxon=None, kind="general"))
                if case == "child":
                    k("Child", parent=src, taxon=None, kind="general")
                c.commit()
                self.assertEqual(r.taxonomy_root_twins(), [(src, dst)] if case == "same" else [])

    def test_lookup_case_type_and_multiple_placed_rows(self):
        c, r, d, k, p, ns = setup()
        self.addCleanup(c.close)
        dst = k(parent=p)
        self.assertEqual(r._find_add_candidate("invented BIRD", None, "taxonomy")["id"], dst)
        self.assertEqual(r.top_level_species_keyword("invented BIRD")["name"], "Invented bird")
        self.assertIsNone(r._find_add_candidate("Invented bird", None, "general"))
        k(parent=k("Second parent", taxon=None, kind="general"))
        self.assertIsNone(r._placed_taxonomy_keyword("Invented bird"))

    def test_explicit_source_accept_preserves_identity_and_ambiguous_placed_rows(self):
        for ambiguous in [False, True]:
            with self.subTest(ambiguous=ambiguous):
                c, r, d, k, p, ns = setup()
                self.addCleanup(c.close)
                dst = k(parent=p, source=101)
                if ambiguous:
                    k(parent=k("Second parent", taxon=None, kind="general"), source=101)

                class Resolver:
                    def __init__(self, **kwargs):
                        pass

                    def resolve(self, name, **kwargs):
                        return types.SimpleNamespace(display_name=name)

                ns["__builtins__"]["__import__"] = lambda name, *a, **kw: (
                    types.SimpleNamespace(SpeciesResolver=Resolver) if name == "species_identity" else forbidden()
                )
                r.resolve_species_display_name = lambda name: name
                result = r.add_source_species("Invented bird", 101)
                self.assertEqual(result == dst, not ambiguous)
                self.assertEqual(
                    c.execute("SELECT source_taxon_id FROM keywords WHERE id=?", (result,)).fetchone()[0], 101
                )

    def test_source_carryover_and_json_history_references(self):
        for source, target in [(None, 101), (101, None), (101, 101)]:
            with self.subTest(source=source, target=target):
                c, r, d, k, p, ns = setup()
                self.addCleanup(c.close)
                dst = k(parent=p, source=target)
                src = k(source=source)
                c.execute("INSERT INTO edit_history VALUES(1,'keyword_add',?,NULL,9)", (str(src),))
                c.execute(
                    "INSERT INTO edit_history_items(id,edit_id,photo_id,new_value,old_value) VALUES(1,1,4,?,?)",
                    (str(src), json.dumps({"keyword_id": src, "keyword_ids": [src]})),
                )
                c.commit()
                self.assertEqual(d.merge_taxonomy_root_twins(), 1)
                row = c.execute("SELECT new_value,old_value FROM edit_history_items").fetchone()
                self.assertEqual(row["new_value"], str(dst))
                self.assertEqual(json.loads(row["old_value"]), {"keyword_id": dst, "keyword_ids": [dst]})
                self.assertEqual(
                    c.execute("SELECT source_taxon_id FROM keywords WHERE id=?", (dst,)).fetchone()[0], 101
                )

    def test_rollback_restores_prior_merges_and_references(self):
        c, r, d, k, p, ns = setup()
        self.addCleanup(c.close)
        dst = k(parent=p)
        a = k()
        b = k("Second bird")
        k("Second bird", parent=p)
        c.execute("INSERT INTO photo_keywords(photo_id,keyword_id,source) VALUES(1,?,'manual')", (a,))
        c.commit()
        before = list(c.iterdump())
        real = d._merge_keyword_into
        calls = []

        def merge(src, dst, **kwargs):
            calls.append(src)
            if len(calls) == 2:
                raise RuntimeError("invented merge failure")
            return real(src, dst, **kwargs)

        d._merge_keyword_into = merge
        # The startup repair owns the unit; a failure part-way rolls it back.
        with self.assertRaisesRegex(RuntimeError, "invented merge failure"), d.transaction(immediate=True):
            d.merge_taxonomy_root_twins()
        self.assertEqual(list(c.iterdump()), before)
        self.assertFalse(c.in_transaction)

    def test_startup_orders_location_repair_and_logs_failed_merge_then_continues(self):
        c, r, d, k, p, ns = setup()
        self.addCleanup(c.close)
        events = []

        def fail():
            events.append("merge")
            raise RuntimeError("invented merge failure")

        fake = types.SimpleNamespace(
            repair_stale_folder_parents=lambda: events.append("folders"),
            repair_misclassified_location_ancestors=lambda: events.append("locations") or 0,
            merge_taxonomy_root_twins=fail,
            transaction=lambda **_options: contextlib.nullcontext(),
        )
        ns["workspace_config"] = types.SimpleNamespace(
            create_default_collections_for_all_workspaces=lambda db: events.append("defaults"),
        )
        load("services/startup_tasks.py", ns, methods={"StartupTasks": {"run_catalog_repairs"}}, functions=set())
        task = ns["StartupTasks"]()
        task._init_db = fake
        task.prune_collection_ids_of_missing_photos = lambda: events.append("prune")
        with self.assertLogs("invented", level="ERROR") as logged:
            task.run_catalog_repairs()
        self.assertIn("Could not merge top-level species keyword twins", logged.output[0])
        self.assertEqual(events, ["folders", "locations", "merge", "defaults", "prune"])

    def test_species_name_add_must_not_rewrite_placed_genus_identity(self):
        c, r, d, k, p, ns = setup()
        self.addCleanup(c.close)
        c.execute("UPDATE taxa SET name='Puma',common_name='Genus Puma' WHERE id=2")
        c.execute("UPDATE taxa SET common_name='Puma' WHERE id=1")
        placed = k("Puma", parent=p, taxon=2)
        c.execute("INSERT INTO photo_keywords(photo_id,keyword_id,source) VALUES(1,?,'manual')", (placed,))
        c.commit()
        result = r.add("Puma", is_species=True)
        self.assertNotEqual(result, placed, "A placed genus must not be reused for the resolved species")
        self.assertEqual(c.execute("SELECT taxon_id FROM keywords WHERE id=?", (placed,)).fetchone()[0], 2)

    def test_exact_placed_alias_precedes_same_source_alias_ambiguity(self):
        c, r, d, k, p, ns = setup()
        self.addCleanup(c.close)
        placed = k(parent=p, source=101)
        k("Other alias", parent=k("Other parent", taxon=None, kind="general"), source=101)
        c.commit()
        self.assertEqual(r.add_source_species("Invented bird", 101), placed)

    def test_source_only_twins_have_definitive_identity_without_local_taxa(self):
        c, r, d, k, p, ns = setup()
        self.addCleanup(c.close)
        placed = k(parent=p, taxon=None, source=999)
        root = k(taxon=None, source=999)
        c.commit()
        self.assertEqual(r.taxonomy_root_twins(), [(root, placed)])
        self.assertEqual(d.merge_taxonomy_root_twins(), 1)

    def test_placed_homonyms_unknown_identity_and_matching_source_boundaries(self):
        for case in ["other_species", "wrong_source", "matching_source", "unknown_request"]:
            with self.subTest(case=case):
                c, r, d, k, p, ns = setup()
                self.addCleanup(c.close)
                c.execute("UPDATE taxa SET rank='species' WHERE id=2")
                placed = k(
                    parent=p,
                    taxon=2 if case == "other_species" else None,
                    source=202 if case == "wrong_source" else 101,
                )
                if case == "unknown_request":
                    c.execute("UPDATE taxa SET common_name='Other bird' WHERE id=1")
                c.execute("INSERT INTO photo_keywords(photo_id,keyword_id,source) VALUES(1,?,'manual')", (placed,))
                c.commit()
                result = r.add("Invented bird", is_species=True)
                self.assertEqual(result == placed, case == "matching_source")
                self.assertEqual(c.execute("SELECT keyword_id FROM photo_keywords").fetchone()[0], placed)
                if case == "matching_source":
                    self.assertEqual(c.execute("SELECT taxon_id FROM keywords WHERE id=?", (placed,)).fetchone()[0], 1)

    def test_exact_alias_ambiguity_remains_when_two_rows_have_the_same_name(self):
        c, r, d, k, p, ns = setup()
        self.addCleanup(c.close)
        a = k(parent=p, source=101)
        b = k(parent=k("Other parent", taxon=None, kind="general"), source=101)
        c.commit()
        result = r.add_source_species("Invented bird", 101)
        self.assertNotIn(result, [a, b])

    def test_repair_rejects_group_conflicts_before_selecting_any_pair(self):
        for case in ["two_placed", "different_local", "different_source", "local_source_conflict", "case_variant"]:
            with self.subTest(case=case):
                c, r, d, k, p, ns = setup()
                self.addCleanup(c.close)
                a = k(parent=p, taxon=None if case == "different_source" else 1, source=101)
                root = k(
                    name="invented bird" if case == "case_variant" else "Invented bird",
                    taxon=None if case == "different_source" else 1,
                    source=202 if case in ["different_source", "local_source_conflict"] else 101,
                )
                if case == "two_placed":
                    k(parent=k("Other parent", taxon=None, kind="general"), source=101)
                if case == "different_local":
                    c.execute("UPDATE keywords SET taxon_id=2 WHERE id=?", (a,))
                c.commit()
                before = list(c.iterdump())
                self.assertEqual(d.merge_taxonomy_root_twins(), 0)
                self.assertEqual(list(c.iterdump()), before)

    def test_consistent_multiple_roots_merge_atomically_and_idempotently(self):
        c, r, d, k, p, ns = setup()
        self.addCleanup(c.close)
        dst = k(parent=p, source=101)
        a = k(source=101)
        b = k(source=101)
        c.executemany(
            "INSERT INTO photo_keywords(photo_id,keyword_id,source) VALUES(?,?,?)",
            [(1, a, "manual"), (2, b, "accept")],
        )
        c.commit()
        self.assertEqual(d.merge_taxonomy_root_twins(), 2)
        self.assertEqual(d.merge_taxonomy_root_twins(), 0)
        self.assertEqual(
            [tuple(x) for x in c.execute("SELECT keyword_id,source FROM photo_keywords ORDER BY photo_id")],
            [(dst, "manual"), (dst, "accept")],
        )


if __name__ == "__main__":
    unittest.main()
