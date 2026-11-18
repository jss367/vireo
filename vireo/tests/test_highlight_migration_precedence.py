"""Legacy highlight ranks cannot overwrite curated photo comparisons."""

import sqlite3

import pytest
from repositories.species_choices import SpeciesChoiceRepository
from schema import _merge_species_highlights
from species_preference import Preferences
from test_species_highlights import _legacy_catalog


@pytest.fixture
def catalog():
    conn = _legacy_catalog()
    conn.row_factory = sqlite3.Row
    conn.executemany("INSERT INTO photos VALUES (?)", [(pid,) for pid in (10, 11, 12, 13)])
    conn.commit()
    conn.execute("PRAGMA foreign_keys = ON")
    yield conn, SpeciesChoiceRepository(conn)
    conn.close()


def _representatives(conn, ids):
    conn.executemany(
        "INSERT INTO species_representatives (species, photo_id, selected_order) VALUES ('Verdin', ?, ?)",
        [(pid, rank) for rank, pid in enumerate(ids, 1)],
    )


def _legacy(conn, ids, workspace=1):
    conn.executemany(
        "INSERT INTO species_highlights (workspace_id, species, photo_id, rank) VALUES (?, 'Verdin', ?, ?)",
        [(workspace, pid, rank) for rank, pid in enumerate(ids, 1)],
    )


def _curated(repo, better, worse, edge_kind):
    # A relabel may leave the comparison owned by another species.
    if edge_kind == "losers":
        return repo.record("Prior species", better, [worse], source="reviewer", reason="Synthetic curated choice")
    return repo.record("Prior species", worse, winner_ids=[better], source="reviewer", reason="Synthetic curated choice")


def _snapshot(conn):
    return {table: {tuple(row) for row in conn.execute(f"SELECT * FROM {table}")}
            for table in ("species_choices", "species_choice_losers", "species_choice_winners")}


def _migrate_preserving_choices(conn, repo):
    before = _snapshot(conn)
    _merge_species_highlights(conn)
    after = _snapshot(conn)
    for table in before:
        assert before[table] <= after[table]
    assert not list(conn.execute("PRAGMA foreign_key_check"))
    migrated = list(conn.iterdump())
    _merge_species_highlights(conn)
    assert list(conn.iterdump()) == migrated
    return Preferences(repo.comparisons())


@pytest.mark.parametrize("edge_kind", ["losers", "winners"])
@pytest.mark.parametrize("legacy", [[11, 10], [10, 11]])
def test_existing_curated_pair_wins_over_conflicting_or_matching_legacy_order(catalog, edge_kind, legacy):
    conn, repo = catalog
    _representatives(conn, [10, 11])
    _curated(repo, 10, 11, edge_kind)
    _legacy(conn, legacy)

    preferences = _migrate_preserving_choices(conn, repo)

    assert preferences.beats(10, 11)
    assert not preferences.beats(11, 10)


@pytest.mark.parametrize("edge_kind", ["losers", "winners"])
def test_nonoverlapping_legacy_edges_are_migrated(catalog, edge_kind):
    conn, repo = catalog
    _representatives(conn, [10, 11, 12, 13])
    _curated(repo, 10, 11, edge_kind)
    _legacy(conn, [12, 13])

    preferences = _migrate_preserving_choices(conn, repo)

    assert preferences.beats(10, 11)
    assert preferences.beats(12, 13)
    assert not preferences.beats(10, 12) and not preferences.beats(12, 10)


@pytest.mark.parametrize("edge_kind", ["losers", "winners"])
def test_legacy_multi_edge_cycle_cannot_reverse_curated_chain(catalog, edge_kind):
    conn, repo = catalog
    _representatives(conn, [10, 11, 12, 13])
    _curated(repo, 10, 11, edge_kind)
    _curated(repo, 11, 12, edge_kind)
    _legacy(conn, [12, 13, 11, 10])

    preferences = _migrate_preserving_choices(conn, repo)

    assert preferences.beats(10, 11) and preferences.beats(11, 12)
    assert preferences.beats(10, 12) and not preferences.beats(12, 10)
    assert preferences.beats(13, 10)


@pytest.mark.parametrize("edge_kind", ["losers", "winners"])
def test_representatives_still_lead_new_highlights_without_losing_curated_order(catalog, edge_kind):
    conn, repo = catalog
    _representatives(conn, [10, 11])
    _curated(repo, 10, 11, edge_kind)
    _legacy(conn, [12, 11, 10])

    preferences = _migrate_preserving_choices(conn, repo)

    assert preferences.rank([12, 11, 10]).order == (10, 11, 12)


def test_latest_curated_decision_still_wins_without_deleting_older_history(catalog):
    conn, repo = catalog
    _representatives(conn, [10, 11])
    _curated(repo, 10, 11, "losers")
    _curated(repo, 11, 10, "losers")
    _legacy(conn, [10, 11])
    _legacy(conn, [11, 10], workspace=2)

    preferences = _migrate_preserving_choices(conn, repo)

    assert preferences.beats(11, 10)
    assert not preferences.beats(10, 11)


@pytest.mark.parametrize("edge_kind", ["losers", "winners"])
def test_only_conflicting_edges_of_a_legacy_choice_are_discarded(catalog, edge_kind):
    conn, repo = catalog
    _representatives(conn, [10, 11, 12])
    _curated(repo, 10, 11, edge_kind)
    _legacy(conn, [11, 10, 12])

    preferences = _migrate_preserving_choices(conn, repo)

    assert preferences.rank([12, 11, 10]).order == (10, 11, 12)
    assert any(c.better == 11 and c.worse == 12 for c in repo.comparisons())
    assert not any(c.better == 11 and c.worse == 10 for c in repo.comparisons())
