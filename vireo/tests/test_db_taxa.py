"""Behavior pins for the reference-taxonomy domain of ``Database``.

``taxonomy.py`` parses the iNat payloads and owns every commit; the SQL it
runs lives in ``repositories/taxa.py`` behind the ``db.taxa`` accessor. These
tests pin each repository method on a real catalog, check that none of them
commits on its own, and keep ``taxonomy.py`` free of ``.conn`` uses.
"""

import ast
import inspect
import sqlite3
import textwrap
from pathlib import Path

import pytest
from db import Database
from repositories.taxa import TaxaRepository


@pytest.fixture
def tree(db):
    """Aves (class) > Accipitriformes (order) > Accipitridae (family), committed."""
    taxa = db.taxa
    aves = taxa.upsert_inat_export_taxon(3, "Aves", "class", "Animalia")
    order = taxa.upsert_inat_export_taxon(71261, "Accipitriformes", "order", "Animalia")
    family = taxa.upsert_inat_export_taxon(5067, "Accipitridae", "family", "Animalia")
    taxa.set_parent(order, aves)
    taxa.set_parent(family, order)
    db.commit()
    return {"aves": aves, "order": order, "family": family}


# -- taxa rows ------------------------------------------------------------------


def test_upsert_inat_export_taxon_returns_local_id_and_keeps_common_name(db):
    local_id = db.taxa.upsert_inat_export_taxon(3, "Aves", "class", "Animalia")
    assert local_id == db.taxa.local_id_for_inat_id(3)
    db.taxa.set_common_name(local_id, "Birds")

    again = db.taxa.upsert_inat_export_taxon(3, "Aves2", "class", "Animalia")

    assert again == local_id
    row = db.taxa.node_by_inat_id(3)
    assert (row["name"], row["rank"]) == ("Aves2", "class")
    assert db.conn.execute(
        "SELECT common_name FROM taxa WHERE id = ?", (local_id,)
    ).fetchone()[0] == "Birds"


def test_upsert_taxon_overwrites_common_name_including_to_null(db):
    db.taxa.upsert_taxon(9, "Ardea herodias", "species", "Animalia", "Great Blue Heron")
    db.taxa.upsert_taxon(9, "Ardea herodias", "species", "Animalia", None)
    row = db.conn.execute(
        "SELECT name, kingdom, common_name FROM taxa WHERE inat_id = 9"
    ).fetchone()
    assert tuple(row) == ("Ardea herodias", "Animalia", None)


def test_lookups_and_count(db, tree):
    assert db.taxa.count() == 3
    assert db.taxa.local_id_for_inat_id(5067) == tree["family"]
    assert db.taxa.local_id_for_inat_id(999) is None
    assert db.taxa.local_id_for_name("Accipitriformes") == tree["order"]
    assert db.taxa.local_id_for_name("Nope") is None
    assert sorted(tuple(r) for r in db.taxa.ids_with_inat_id()) == sorted(
        [(tree["aves"], 3), (tree["order"], 71261), (tree["family"], 5067)]
    )


def test_node_lookups_walk_the_parent_chain(db, tree):
    node = db.taxa.node_by_inat_id(5067)
    assert (node["id"], node["name"], node["rank"], node["parent_id"]) == (
        tree["family"], "Accipitridae", "family", tree["order"],
    )
    parent = db.taxa.node_by_id(node["parent_id"])
    assert parent["parent_id"] == tree["aves"]
    assert db.taxa.node_by_id(-1) is None
    assert db.taxa.node_by_inat_id(-1) is None


def test_prune_absent_inat_ids_clears_references_and_drops_temp_table(db, tree):
    db.conn.execute(
        "INSERT INTO keywords (name, taxon_id) VALUES ('Hawks', ?)", (tree["order"],)
    )
    db.commit()

    pruned = db.taxa.prune_absent_inat_ids([3, 5067])

    assert pruned == 1
    assert db.taxa.local_id_for_inat_id(71261) is None
    assert db.conn.execute(
        "SELECT taxon_id FROM keywords WHERE name = 'Hawks'"
    ).fetchone()[0] is None
    assert db.taxa.node_by_inat_id(5067)["parent_id"] is None
    assert db.conn.execute(
        "SELECT COUNT(*) FROM sqlite_temp_master WHERE name = 'fresh_inat_ids'"
    ).fetchone()[0] == 0


# -- common names and informal groups -------------------------------------------


def test_common_name_index_add_delete_and_clear(db, tree):
    taxa = db.taxa
    taxa.add_common_name(tree["family"], "hawks", "en")
    taxa.add_common_name(tree["family"], "hawks", "en")
    taxa.add_common_name(tree["family"], "rapaces", "fr")
    taxa.add_common_name(tree["order"], "raptors", "en")

    def names():
        return sorted(
            tuple(r) for r in db.conn.execute(
                "SELECT taxon_id, name, locale FROM taxa_common_names"
            )
        )

    assert names() == sorted([
        (tree["family"], "hawks", "en"),
        (tree["family"], "rapaces", "fr"),
        (tree["order"], "raptors", "en"),
    ])
    taxa.delete_common_names(tree["family"], "en")
    assert names() == sorted([
        (tree["family"], "rapaces", "fr"),
        (tree["order"], "raptors", "en"),
    ])
    taxa.clear_english_common_names()
    assert names() == [(tree["family"], "rapaces", "fr")]


def test_informal_groups_are_idempotent(db, tree):
    group_id = db.taxa.ensure_informal_group("Raptors")
    assert db.taxa.ensure_informal_group("Raptors") == group_id
    db.taxa.link_informal_group(group_id, tree["order"])
    db.taxa.link_informal_group(group_id, tree["order"])
    assert [
        tuple(r) for r in db.conn.execute(
            "SELECT group_id, taxon_id FROM informal_group_taxa"
        )
    ] == [(group_id, tree["order"])]


def test_repository_methods_never_commit(db):
    taxa = db.taxa
    local_id = taxa.upsert_inat_export_taxon(3, "Aves", "class", "Animalia")
    taxa.upsert_taxon(4, "Mammalia", "class", "Animalia", "Mammals")
    taxa.set_parent(local_id, local_id)
    taxa.set_common_name(local_id, "Birds")
    taxa.add_common_name(local_id, "birds", "en")
    taxa.link_informal_group(taxa.ensure_informal_group("Birds"), local_id)
    taxa.prune_absent_inat_ids([3])
    assert db.in_transaction
    other = sqlite3.connect(db.conn.execute("PRAGMA database_list").fetchone()[2])
    try:
        assert other.execute("SELECT COUNT(*) FROM taxa").fetchone()[0] == 0
        assert other.execute("SELECT COUNT(*) FROM informal_groups").fetchone()[0] == 0
    finally:
        other.close()


# -- structure ------------------------------------------------------------------


def test_taxa_is_a_fresh_repository_on_the_connection_per_access(db):
    """``db.taxa`` builds a new repository each time, never a cached one."""
    first, second = db.taxa, db.taxa
    assert isinstance(first, TaxaRepository)
    assert first is not second
    assert first.conn is db.conn


def test_taxa_accessor_builds_through_the_factory_not_the_connection():
    accessor = Database.__dict__["taxa"]
    assert isinstance(accessor, property)
    source = textwrap.dedent(inspect.getsource(accessor.fget))
    attrs = {
        node.attr
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name)
        and node.value.id == "self"
    }
    assert "_taxa_repository" in attrs
    assert "conn" not in attrs


def test_taxonomy_module_runs_no_sql_on_the_connection():
    """The loaders reach the catalog through ``db.taxa``, never ``<expr>.conn``."""
    path = Path(__file__).resolve().parent.parent / "taxonomy.py"
    tree = ast.parse(path.read_text(encoding="utf-8"))
    uses = [
        node.lineno
        for node in ast.walk(tree)
        if isinstance(node, ast.Attribute) and node.attr == "conn"
    ]
    assert uses == []
