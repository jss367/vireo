"""Remapping comparisons excludes photos that will not survive deletion."""

import sqlite3
import unittest

from repositories.species_choices import SpeciesChoiceRepository
from species_preference import Comparison


class SpeciesChoiceRemapTests(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.addCleanup(self.conn.close)
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript("""
            PRAGMA foreign_keys = ON;
            CREATE TABLE photos (id INTEGER PRIMARY KEY);
            CREATE TABLE species_representatives (
                species TEXT, photo_id INTEGER REFERENCES photos ON DELETE CASCADE,
                selected_order INTEGER, created_at TEXT, updated_at TEXT,
                UNIQUE(species, photo_id)
            );
            CREATE TABLE photo_preferences (photo_id INTEGER, purpose TEXT);
            CREATE TABLE species_choices (
                id INTEGER PRIMARY KEY, species TEXT,
                photo_id INTEGER REFERENCES photos ON DELETE CASCADE,
                source TEXT, reason TEXT
            );
            CREATE TABLE species_choice_losers (
                choice_id INTEGER REFERENCES species_choices ON DELETE CASCADE,
                photo_id INTEGER REFERENCES photos ON DELETE CASCADE,
                PRIMARY KEY(choice_id, photo_id)
            );
            INSERT INTO photos VALUES (1), (2), (3), (4);
        """)
        self.repo = SpeciesChoiceRepository(self.conn)

    def test_dropped_photo_cannot_discard_a_surviving_comparison(self):
        # Oldest first: Y>S, X>Y, T>X. S folds into T; X disappears.
        older = self.repo.record("Synthetic species", 1, [2])
        self.repo.record("Synthetic species", 3, [1])
        self.repo.record("Synthetic species", 4, [3])

        self.repo.remap_photos({2: 4, 3: None})

        self.assertIn(Comparison(older, 1, 4), self.repo.comparisons())
        self.conn.execute("DELETE FROM photos WHERE id IN (2, 3)")
        self.assertEqual(self.repo.comparisons(), [Comparison(older, 1, 4)])

    def test_surviving_cycle_still_keeps_the_newest_choice(self):
        self.repo.record("Synthetic species", 1, [2])
        middle = self.repo.record("Synthetic species", 3, [1])
        newest = self.repo.record("Synthetic species", 4, [3])

        self.repo.remap_photos({2: 4})
        self.conn.execute("DELETE FROM photos WHERE id = 2")

        self.assertEqual(self.repo.comparisons(), [
            Comparison(middle, 3, 1), Comparison(newest, 4, 3),
        ])

    def test_dropped_only_mapping_leaves_surviving_comparisons_alone(self):
        older = self.repo.record("Synthetic species", 1, [2])
        self.repo.record("Synthetic species", 3, [1])

        self.repo.remap_photos({3: None})
        self.conn.execute("DELETE FROM photos WHERE id = 3")

        self.assertEqual(self.repo.comparisons(), [Comparison(older, 1, 2)])
