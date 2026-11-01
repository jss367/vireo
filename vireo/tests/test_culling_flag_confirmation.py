"""Recorded decisions, ranking writes and replay against actual SQLite repositories.

This module also runs directly with the standard-library unittest runner. It
does not start a server, load pytest plugins, or open a real photo catalog.
"""

import ast
import json
import sqlite3
import sys
import tempfile
import unittest
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace

from services.edit_history import redo_last_undo, undo_last_edit

ROOT = Path(__file__).resolve().parents[1]
if __name__ == "__main__":
    sys.path.insert(0, str(ROOT))

from canonical_schema import SCHEMA_VERSION, create_current_flag_attribution
from keep_safety import review_culling_suggestions
from repositories.edit_history import EditHistoryRepository
from repositories.photo_review import PhotoReviewRepository
from services.photo_review import PhotoReviewService, flag_note
from unit_of_work import UnitOfWork


def replay_methods():
    """Use the production replay bodies without importing unrelated app services."""
    names = {"_apply_edit_items", "_edit_set_flag"}
    nodes = [node for node in ast.walk(ast.parse((ROOT / "db.py").read_text()))
             if isinstance(node, ast.FunctionDef) and node.name in names]
    namespace = {}
    exec(compile(ast.Module(body=nodes, type_ignores=[]), "<production flag replay>", "exec"), namespace)
    return namespace


class Catalog:
    """Synthetic catalog boundary; flag/history SQL is the production code."""

    def __init__(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.executescript("""
            CREATE TABLE workspaces(id INTEGER PRIMARY KEY);
            CREATE TABLE photos(id INTEGER PRIMARY KEY, flag TEXT);
            CREATE TABLE photo_workspace_visibility(photo_id INTEGER, workspace_id INTEGER);
            CREATE TABLE edit_history(id INTEGER PRIMARY KEY, workspace_id INTEGER,
                action_type TEXT, description TEXT, new_value TEXT, source TEXT, reason TEXT,
                is_batch INTEGER DEFAULT 0, undone INTEGER DEFAULT 0,
                created_at TEXT DEFAULT 'synthetic-time');
            CREATE TABLE edit_history_items(id INTEGER PRIMARY KEY, edit_id INTEGER,
                photo_id INTEGER, old_value TEXT, new_value TEXT,
                species_provenance TEXT);
            CREATE TABLE queued_flags(photo_id INTEGER, flag TEXT);
            INSERT INTO workspaces VALUES(1),(2);
            INSERT INTO photos VALUES(7,'none'),(8,'none');
            INSERT INTO photo_workspace_visibility VALUES(7,1),(7,2),(8,1);
        """)
        create_current_flag_attribution(self.conn)
        self.active_workspace_id = 1
        self.fail_history = False
        self._unit = UnitOfWork()
        self.history = EditHistoryRepository(
            self.conn, lambda: self.active_workspace_id, non_undoable=[],
            restore_species_representative=lambda *args: None,
        )
        self.conn.commit()

    @property
    def photo_review(self):
        return PhotoReviewRepository(self.conn, self.active_workspace_id)

    @property
    def in_transaction(self):
        return self.conn.in_transaction

    def transaction(self, **options):
        return self._unit.enter(self.conn, immediate=options.get("immediate", False),
                                retry_busy=False, join=options.get("join", False))

    def commit(self):
        self.conn.commit()

    @property
    def photos(self):
        def get_by_ids(ids):
            return {row["id"]: dict(row) for row in self.conn.execute(
                "SELECT p.id,p.flag FROM photos p JOIN photo_workspace_visibility v "
                "ON v.photo_id=p.id WHERE v.workspace_id=?", (self.active_workspace_id,),
            ) if row["id"] in ids}
        return SimpleNamespace(get_by_ids=get_by_ids)

    def queue_flag_change_if_enabled(self, photo_id, flag):
        self.conn.execute("INSERT INTO queued_flags VALUES(?,?)", (photo_id, flag))

    def record_edit(self, *args, **kwargs):
        if self.fail_history:
            raise ValueError("synthetic history failure")
        return self.history.record(*args, **kwargs)

    def prune_edit_history(self):
        pass

    def replay(self, edit_id, *, undo):
        entry = dict(self.conn.execute("SELECT * FROM edit_history WHERE id=?", (edit_id,)).fetchone())
        items = [dict(row) for row in self.conn.execute(
            "SELECT * FROM edit_history_items WHERE edit_id=?", (edit_id,),
        )]
        self._apply_edit_items(entry, items, undo=undo)


_REPLAY = replay_methods()
Catalog._apply_edit_items = _REPLAY["_apply_edit_items"]
Catalog._edit_set_flag = _REPLAY["_edit_set_flag"]
Catalog._EDIT_FIELD_SETTERS = {"flag": Catalog._edit_set_flag}
Catalog._UNDO_HANDLERS = {}
Catalog._REDO_HANDLERS = {}


class CullingFlagConfirmationTests(unittest.TestCase):
    def setUp(self):
        self.db = Catalog()
        self.addCleanup(self.db.conn.close)
        self.service = PhotoReviewService(self.db)

    def cull(self, states=None):
        original = {"species_groups": [{"scene_groups": [{"photos": [
            {"photo_id": 7, "action": "keep", "flag": "flagged", "label": "KEEP",
             "decision_source": "confirmed", "automatic_keep_eligible": True},
        ]}], "photos_data": [{"photo_id": 7, "action": "keep", "flag": "flagged"}]}]}
        result = review_culling_suggestions(
            deepcopy(original), self.db.photo_review.culling_flag_states([7]) if states is None else states,
        )
        photo = result["species_groups"][0]["scene_groups"][0]["photos"][0]
        raw = result["species_groups"][0]["photos_data"][0]
        self.assertEqual((photo["label"], photo["decision_source"]), (raw["label"], raw["decision_source"]))
        return result, photo

    def latest_edit(self):
        return self.db.conn.execute("SELECT MAX(id) FROM edit_history").fetchone()[0]

    def test_legacy_and_cached_claims_do_not_establish_confirmation(self):
        self.db.photo_review.set_flag(7, "flagged")
        self.db.commit()
        for states in (None, {7: "flagged"}):
            with self.subTest(states=states):
                result, photo = self.cull(states)
                self.assertEqual(photo["label"], "REVIEW")
                self.assertEqual(result["suggested_keepers"], 0)
                self.assertEqual(result["suggested_reviews"], 1)
        self.assertEqual(self.db.photos.get_by_ids([7])[7]["flag"], "flagged")

    def test_ranking_is_unconfirmed_until_an_explicit_manual_write(self):
        self.assertTrue(self.db.photo_review.flag_ranked_candidate(7))
        self.assertEqual(self.cull()[1]["label"], "REVIEW")
        self.service.set_flag(7, "flagged")
        self.assertEqual(self.cull()[1]["label"], "KEEP")
        self.assertFalse(self.cull()[1]["automatic_keep_eligible"])
        self.assertIsNone(flag_note(self.db, 7, "flagged"))

    def test_manual_keep_and_reject_survive_ranking_runs(self):
        for flag, label in (("flagged", "KEEP"), ("rejected", "REJECT")):
            with self.subTest(flag=flag):
                self.service.set_flag(7, flag)
                self.assertFalse(self.db.photo_review.flag_ranked_candidate(7))
                self.assertEqual(self.cull()[1]["label"], label)

    def test_manual_clear_is_not_overwritten_by_a_ranking_job(self):
        self.service.set_flag(7, "none")
        self.assertFalse(self.db.photo_review.flag_ranked_candidate(7))
        self.assertEqual(self.db.photos.get_by_ids([7])[7]["flag"], "none")

    def test_attributed_machine_decisions_are_not_photographer_confirmation(self):
        self.service.set_flag(7, "flagged", source="synthetic-model", reason="relative winner")
        self.assertEqual(self.cull()[1]["label"], "REVIEW")
        self.assertEqual(flag_note(self.db, 7, "flagged")["source"], "synthetic-model")

    def test_reason_only_attribution_is_not_photographer_confirmation(self):
        # Headless /api/v1 callers may send a reason without a source; that
        # row carries attribution, so Cull must not promote it to a confirmed
        # photographer decision or protect it from ranking.
        self.service.set_flag(7, "flagged", reason="attributed note without source")
        self.assertEqual(self.cull()[1]["label"], "REVIEW")
        self.assertIsNone(self.db.photo_review.culling_flag_states([7])[7]["confirmed_flag"])
        self.service.set_flag(7, "none", reason="attributed clear without source")
        self.assertTrue(self.db.photo_review.flag_ranked_candidate(7))

    def test_unattributed_same_value_write_invalidates_confirmation(self):
        self.service.set_flag(7, "flagged")
        self.db.conn.execute("UPDATE photos SET flag='flagged' WHERE id=7")
        self.assertEqual(self.cull()[1]["label"], "REVIEW")

    def test_undo_redo_preserve_captured_manual_and_unknown_states(self):
        self.db.photo_review.flag_ranked_candidate(7)
        self.db.commit()
        self.service.set_flag(7, "flagged")
        first = self.latest_edit()
        self.db.replay(first, undo=True)
        self.assertEqual(self.cull()[1]["label"], "REVIEW")
        self.db.replay(first, undo=False)
        self.assertEqual(self.cull()[1]["label"], "KEEP")
        self.service.set_flag(7, "rejected")
        second = self.latest_edit()
        self.db.replay(second, undo=True)
        self.assertEqual(self.cull()[1]["label"], "KEEP")
        self.db.replay(second, undo=False)
        self.assertEqual(self.cull()[1]["label"], "REJECT")

    def test_legacy_history_without_snapshots_cannot_invent_manual_origin(self):
        edit = self.db.history.record("flag", "legacy", "flagged", [
            {"photo_id": 7, "old_value": "none", "new_value": "flagged"},
        ])
        self.db.replay(edit, undo=False)
        self.assertEqual(self.cull()[1]["label"], "REVIEW")

    def test_failure_and_outer_rollback_preserve_flag_history_and_confirmation(self):
        self.service.set_flag(7, "flagged")
        self.db.fail_history = True
        with self.assertRaises(ValueError):
            self.service.set_flag(7, "rejected")
        self.assertEqual(self.cull()[1]["label"], "KEEP")
        self.db.fail_history = False
        self.db.conn.execute("BEGIN")
        self.service.set_flag(7, "rejected")
        self.db.conn.rollback()
        self.assertEqual(self.cull()[1]["label"], "KEEP")

    def test_visibility_scope_and_global_manual_decisions(self):
        self.service.set_flag(7, "flagged")
        self.service.set_flag(8, "flagged")
        self.db.active_workspace_id = 2
        states = self.db.photo_review.culling_flag_states([7, 8])
        self.assertEqual(set(states), {7})
        self.assertEqual(self.cull()[1]["label"], "KEEP")

    def test_pruning_cascades_replay_metadata_without_erasing_current_decision(self):
        self.service.set_flag(7, "flagged")
        create_current_flag_attribution(self.db.conn)
        self.db.conn.execute("DELETE FROM edit_history")
        self.db.conn.commit()
        self.assertEqual(self.db.conn.execute("SELECT COUNT(*) FROM flag_attribution_history").fetchone()[0], 0)
        self.assertEqual(self.cull()[1]["label"], "KEEP")

    def test_wrong_photo_snapshot_rolls_back_replay(self):
        self.service.set_flag(7, "flagged")
        edit = self.latest_edit()
        state = self.db.photo_review.flag_attributions([7])[7]
        state["photo_id"] = 8
        self.db.conn.execute("UPDATE flag_attribution_history SET old_state=? WHERE edit_id=?",
                             (json.dumps(state), edit))
        self.db.conn.commit()
        with self.assertRaisesRegex(ValueError, "different photo"):
            self.db.replay(edit, undo=True)
        self.assertEqual(self.cull()[1]["label"], "KEEP")


class CullingCatalogIntegrationTests(unittest.TestCase):
    """Real catalog and socket-free Flask client; no model or judge is called."""

    def setUp(self):
        from db import Database
        from PIL import Image
        from services import photo_ingest, workspace_folders

        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.db_path = self.root / "catalog.db"
        self.db = Database(str(self.db_path))
        self.addCleanup(self.db.close)
        self.db.set_active_workspace(self.db.ensure_default_workspace())
        folder = self.root / "photos"
        folder.mkdir()
        folder_id = workspace_folders.add_folder(self.db, str(folder), name="synthetic")
        self.ids = []
        for index in range(3):
            path = folder / f"synthetic-{index}.jpg"
            Image.new("RGB", (16, 16), (index * 60, 0, 0)).save(path)
            stat = path.stat()
            pid = photo_ingest.add_photo(
                self.db, folder_id, path.name, ".jpg", stat.st_size, stat.st_mtime,
            )
            self.ids.append(pid)
            detections = self.db.detections.save(pid, [
                {"box": {"x": .1, "y": .1, "w": .3, "h": .4},
                 "confidence": .9, "category": "animal"},
            ], detector_model="synthetic-fixture")
            self.db.predictions.add(detections[0], "Robin", .9, "synthetic-fixture")
            self.db.commit()
        self.service = PhotoReviewService(self.db)

    def test_real_catalog_undo_redo_preserves_confirmation(self):
        pid = self.ids[0]
        self.db.photo_review.flag_ranked_candidate(pid)
        self.db.commit()
        self.service.set_flag(pid, "flagged")
        self.assertEqual(self.db.photo_review.culling_flag_states([pid])[pid]["confirmed_flag"], "flagged")
        undo_last_edit(self.db)
        self.assertIsNone(self.db.photo_review.culling_flag_states([pid])[pid]["confirmed_flag"])
        redo_last_undo(self.db)
        self.assertEqual(self.db.photo_review.culling_flag_states([pid])[pid]["confirmed_flag"], "flagged")
        self.service.set_flag(pid, "rejected")
        undo_last_edit(self.db)
        self.assertEqual(self.db.photo_review.culling_flag_states([pid])[pid]["confirmed_flag"], "flagged")

    def test_cached_endpoint_ignores_claimed_confirmation_without_modifying_cache(self):
        from flask import Flask, jsonify
        from web.caches import create_caches_blueprint

        self.service.set_flag(self.ids[0], "flagged")
        self.service.set_flag(self.ids[1], "rejected")
        self.db.photo_review.flag_ranked_candidate(self.ids[2])
        self.db.commit()
        app = Flask(__name__)
        app.register_blueprint(create_caches_blueprint(
            lambda: self.db, lambda message, status: (jsonify(error=message), status), str(self.db_path), {},
            start_thumbnail_recheck=lambda: {},
        ))
        cache = self.root / f"culling_results_ws{self.db.active_workspace_id}.json"
        for group_key in ("scene_groups", "pose_groups"):
            with self.subTest(group_key=group_key):
                cached = {"species_groups": [{group_key: [{"photos": [
                    {"photo_id": pid, "action": "keep", "decision_source": "confirmed",
                     "flag_confirmation_status": "confirmed"} for pid in self.ids
                ]}], "photos_data": [{"photo_id": pid, "action": "keep"} for pid in self.ids]}]}
                cache.write_text(json.dumps(cached))
                before = cache.read_bytes()
                for _ in range(2):
                    response = app.test_client().get("/api/culling/results")
                    self.assertEqual(response.status_code, 200)
                    result = response.get_json()
                    photos = result["species_groups"][0][group_key][0]["photos"]
                    self.assertEqual([p["action"] for p in photos], ["keep", "reject", "review"])
                    self.assertEqual((result["suggested_keepers"], result["suggested_rejects"],
                                      result["suggested_reviews"]), (1, 1, 1))
                self.assertEqual(cache.read_bytes(), before)
        self.assertEqual([self.db.photos.get(pid)["flag"] for pid in self.ids],
                         ["flagged", "rejected", "flagged"])

    def test_best_batch_requires_confirmed_flags_without_promoting_raw_flags(self):
        from flask import Flask, jsonify
        from web.batch import create_batch_blueprint

        app = Flask(__name__)
        app.register_blueprint(create_batch_blueprint(
            lambda: self.db, lambda message, status=400: (jsonify(error=message), status),
            location_errors=(), bulk_gps_location_payload=None, run_batch_delete=None,
            invalidate_missing_originals=None,
        ))
        client = app.test_client()
        best, reject = self.ids[:2]
        for source, reason in [(None, None), ("ranker", None), (None, "automated")]:
            with self.subTest(source=source, reason=reason):
                self.db.photo_review.set_flag(best, "none")
                self.db.commit()
                self.db.photo_review.flag_ranked_candidate(best)
                self.db.commit()
                if source or reason:
                    with self.db.transaction(immediate=True):
                        self.db.photo_review.set_current_flag_note(
                            [best], "flagged", "ranking", source=source, reason=reason,
                        )
                self.service.set_flag(reject, "rejected")
                before = self.db.photo_review.flag_attributions([best, reject])
                history = self.db.edit_history.list_recent()
                response = client.post("/api/batch/best-batch-flags", json={
                    "best_photo_id": best, "reject_photo_ids": [reject],
                })
                self.assertEqual(response.status_code, 409)
                self.assertEqual(self.db.photo_review.flag_attributions([best, reject]), before)
                self.assertEqual(self.db.edit_history.list_recent(), history)
                self.assertIsNone(self.db.photo_review.culling_flag_states([best])[best]["confirmed_flag"])
        # A confirmed keeper cannot turn an unconfirmed raw rejection into a
        # photographer decision either. Once both are confirmed, replay is valid.
        self.service.set_flag(best, "flagged")
        self.db.photo_review.set_flag(reject, "rejected")
        self.db.commit()
        payload = {"best_photo_id": best, "reject_photo_ids": [reject]}
        self.assertEqual(client.post("/api/batch/best-batch-flags", json=payload).status_code, 409)
        self.assertIsNone(self.db.photo_review.culling_flag_states([reject])[reject]["confirmed_flag"])
        self.service.set_flag(reject, "rejected")
        self.assertEqual(client.post("/api/batch/best-batch-flags", json=payload).status_code, 200)
        states = self.db.photo_review.culling_flag_states([best, reject])
        self.assertEqual([states[pid]["confirmed_flag"] for pid in [best, reject]], ["flagged", "rejected"])

    def test_misses_bulk_reject_records_confirmation_and_restores_prior_attribution(self):
        from flask import Flask, jsonify
        from web.misses import create_misses_blueprint

        app = Flask(__name__)
        app.register_blueprint(create_misses_blueprint(
            lambda: self.db, lambda message, status=400: (jsonify(error=message), status),
            resolve_visual=None,
        ))
        first, second, excluded = self.ids
        self.service.set_flag(first, "flagged")
        before = self.db.photo_review.flag_attributions(self.ids)
        self.db.conn.execute("UPDATE photos SET flag=NULL WHERE id=?", (second,))
        self.db.conn.execute("UPDATE photos SET miss_clipped=1, miss_computed_at='2026-10-25'")
        self.db.conn.execute("UPDATE photos SET miss_computed_at='2026-10-01' WHERE id=?", (excluded,))
        self.db.conn.commit()
        history_count = len(self.db.edit_history.list_recent())
        response = app.test_client().post("/api/misses/reject", json={
            "category": "clipped", "since": "2026-10-24",
        })
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json(), {"rejected": 2, "category": "clipped"})
        states = self.db.photo_review.culling_flag_states(self.ids)
        self.assertEqual([states[pid]["confirmed_flag"] for pid in self.ids], ["rejected", "rejected", None])
        self.assertEqual(states[excluded]["flag"], "none")
        self.assertEqual(len(self.db.edit_history.list_recent()), history_count + 1)
        self.assertIn("category=clipped", self.db.edit_history.list_recent()[0]["description"])
        after = self.db.photo_review.flag_attributions(self.ids)
        undo_last_edit(self.db)
        self.assertEqual(self.db.photo_review.flag_attributions(self.ids), before)
        states = self.db.photo_review.culling_flag_states(self.ids)
        self.assertIsNone(self.db.photos.get(second)["flag"])
        self.assertEqual([states[pid]["flag"] for pid in self.ids], ["flagged", None, "none"])
        self.assertEqual(states[first]["confirmed_flag"], "flagged")
        self.assertIsNone(states[second]["confirmed_flag"])
        redo_last_undo(self.db)
        self.assertEqual(self.db.photo_review.flag_attributions(self.ids), after)

    def test_misses_bulk_reject_rolls_back_flags_and_attribution_on_history_failure(self):
        from unittest.mock import patch

        from services import miss_flags

        self.service.set_flag(self.ids[0], "flagged")
        before = self.db.photo_review.flag_attributions(self.ids)
        self.db.conn.execute("UPDATE photos SET miss_clipped=1")
        self.db.conn.commit()
        history = self.db.edit_history.list_recent()
        with patch.object(self.db, "record_edit", side_effect=ValueError("synthetic history failure")):
            with self.assertRaisesRegex(ValueError, "synthetic history failure"):
                miss_flags.bulk_reject_miss_category(self.db, "clipped", photo_ids=self.ids[:2])
        self.assertEqual(self.db.photo_review.flag_attributions(self.ids), before)
        self.assertEqual([self.db.photos.get(pid)["flag"] for pid in self.ids], ["flagged", "none", "none"])
        self.assertEqual(self.db.edit_history.list_recent(), history)
        self.assertFalse(self.db.conn.in_transaction)

    def test_workspace_deletion_keeps_global_confirmation_and_replay(self):
        from services import workspace_folders, workspace_lifecycle

        origin = self.db.active_workspace_id
        pid = self.ids[0]
        self.service.set_flag(pid, "flagged")
        other = self.db.create_workspace("Surviving workspace")
        self.db.commit()
        workspace_folders.add_workspace_folder(self.db, other, self.db.photos.get(pid)["folder_id"])
        self.db.set_active_workspace(other)
        workspace_lifecycle.delete_workspace(self.db, origin)
        state = self.db.photo_review.culling_flag_states([pid])[pid]
        self.assertEqual(state["confirmed_flag"], "flagged")
        self.assertIsNone(self.db.photo_review.flag_attributions([pid])[pid]["workspace_id"])
        self.service.set_flag(pid, "rejected")
        undo_last_edit(self.db)
        self.assertEqual(self.db.photo_review.culling_flag_states([pid])[pid]["confirmed_flag"], "flagged")
        redo_last_undo(self.db)
        self.assertEqual(self.db.photo_review.culling_flag_states([pid])[pid]["confirmed_flag"], "rejected")

    def test_replay_of_snapshot_from_deleted_workspace_retains_confirmation(self):
        from services import workspace_folders, workspace_lifecycle

        origin = self.db.active_workspace_id
        pid = self.ids[0]
        self.service.set_flag(pid, "flagged")
        other = self.db.create_workspace("Surviving workspace")
        self.db.commit()
        workspace_folders.add_workspace_folder(self.db, other, self.db.photos.get(pid)["folder_id"])
        self.db.set_active_workspace(other)
        self.service.set_flag(pid, "rejected")
        workspace_lifecycle.delete_workspace(self.db, origin)
        undo_last_edit(self.db)
        self.assertEqual(self.db.photo_review.culling_flag_states([pid])[pid]["confirmed_flag"], "flagged")
        self.assertIsNone(self.db.photo_review.flag_attributions([pid])[pid]["workspace_id"])

    def test_merge_carries_only_the_queue_winners_matching_attribution(self):
        from repositories.photo_row_deletion import photo_row_deletion
        from services import photo_ingest

        folder = self.db.photos.get(self.ids[0])["folder_id"]
        for winner_is_loser, evidence in [(True, "manual"), (False, "manual"),
                                         (True, "unknown"), (True, "automated"), (True, "stale")]:
            with self.subTest(winner_is_loser=winner_is_loser, evidence=evidence):
                loser, survivor = [photo_ingest.add_photo(
                    self.db, folder, f"merge-{winner_is_loser}-{evidence}-{n}.jpg", ".jpg", 10, 1.0,
                ) for n in range(2)]
                winner = loser if winner_is_loser else survivor
                self.service.set_flag(loser, "rejected", source="ranker" if evidence == "automated" else None)
                self.service.set_flag(survivor, "flagged")
                if evidence == "unknown":
                    self.db.photo_review.set_flag(loser, "rejected")
                    self.db.commit()
                winning_flag = "rejected" if winner_is_loser else "flagged"
                if evidence == "stale":
                    winning_flag = "none"
                before = self.db.photo_review.flag_attributions([loser, survivor])
                self.db.conn.execute("DELETE FROM pending_changes WHERE photo_id IN (?,?)", (loser, survivor))
                for pid in (loser, survivor):
                    self.db.conn.execute(
                        "INSERT INTO pending_changes(photo_id,workspace_id,change_type,value,created_at) "
                        "VALUES (?,?,'flag',?,?)",
                        (pid, self.db.active_workspace_id, winning_flag if pid == winner else "none",
                         "2026-10-25" if pid == winner else "2026-10-24"),
                    )
                self.db.conn.commit()
                self.db.moves_merge.transfer_review_state(loser, survivor)
                self.assertTrue(self.db.conn.in_transaction)
                self.db.conn.rollback()
                self.assertEqual(self.db.photo_review.flag_attributions([loser, survivor]), before)
                self.db.moves_merge.transfer_review_state(loser, survivor)
                self.db.conn.execute("UPDATE pending_changes SET photo_id=? WHERE photo_id=?", (survivor, loser))
                with photo_row_deletion(self.db.conn) as rows:
                    rows.delete({loser: survivor})
                self.db.conn.commit()
                state = self.db.photo_review.culling_flag_states([survivor])[survivor]
                self.assertEqual(state["flag"], winning_flag)
                self.assertEqual(state["confirmed_flag"], winning_flag if evidence == "manual" else None)
                note = self.db.photo_review.flag_attributions([survivor]).get(survivor)
                if evidence in ("manual", "automated"):
                    self.assertEqual(note, dict(before[winner], photo_id=survivor))
                else:
                    self.assertIsNone(note)

    def test_version_28_attribution_migration_preserves_data_and_global_lifetime(self):
        from canonical_schema import CURRENT_FLAG_ATTRIBUTION_OBJECTS
        from schema import ensure_schema

        pid = self.ids[0]
        self.service.set_flag(pid, "flagged")
        before = self.db.photo_review.flag_attributions([pid])
        self.db.conn.execute("DROP TRIGGER invalidate_current_flag_attribution")
        self.db.conn.execute("ALTER TABLE current_flag_attribution RENAME TO new_attribution")
        self.db.conn.execute(CURRENT_FLAG_ATTRIBUTION_OBJECTS[0].replace(
            "workspace_id INTEGER REFERENCES workspaces(id) ON DELETE SET NULL",
            "workspace_id INTEGER NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE",
        ))
        self.db.conn.execute("INSERT INTO current_flag_attribution SELECT * FROM new_attribution")
        self.db.conn.execute("DROP TABLE new_attribution")
        self.db.conn.execute(CURRENT_FLAG_ATTRIBUTION_OBJECTS[1])
        self.db.conn.execute("PRAGMA user_version=28")
        self.db.conn.commit()
        ensure_schema(str(self.db_path))
        self.assertEqual(self.db.conn.execute("PRAGMA user_version").fetchone()[0], SCHEMA_VERSION)
        self.assertEqual(self.db.photo_review.flag_attributions([pid]), before)
        self.db.conn.execute("DELETE FROM workspaces WHERE id=?", (self.db.active_workspace_id,))
        self.db.conn.commit()
        self.assertIsNone(self.db.conn.execute(
            "SELECT workspace_id FROM current_flag_attribution WHERE photo_id=?", (pid,),
        ).fetchone()[0])
        self.assertEqual(list(self.db.conn.execute("PRAGMA foreign_key_check")), [])

    def test_explicit_duplicate_choices_confirm_flags_but_automatic_resolution_does_not(self):
        from db import Database
        from services import duplicate_resolution

        for mode in ("keep", "bulk", "automatic"):
            for original_flag in ("none", "flagged", None):
                with self.subTest(mode=mode, original_flag=original_flag):
                    self.db.conn.execute("UPDATE photos SET file_hash='same-content', flag=?", (original_flag,))
                    self.db.conn.execute("DELETE FROM pending_changes")
                    self.db.conn.commit()
                    if mode == "keep":
                        result = duplicate_resolution.keep_duplicate_copy(self.db, self.ids[1])
                    elif mode == "bulk":
                        result = duplicate_resolution.bulk_resolve_by_folder(
                            self.db, ["same-content"], str(self.root / "photos"),
                        )["resolved"][0]
                    else:
                        result = duplicate_resolution.apply_duplicate_resolution(self.db, self.ids)
                    winner = result["winner_id"]
                    self.assertIn(winner, self.ids)
                    self.assertEqual(len(result["loser_ids"]), 2)
                    # Reload through a separate real catalog: no client pins or
                    # transient resolver state may stand in for attribution.
                    reopened = Database(str(self.db_path))
                    try:
                        reopened.set_active_workspace(self.db.active_workspace_id)
                        states = reopened.photo_review.culling_flag_states(self.ids)
                        for pid, state in states.items():
                            expected = (original_flag or "none") if pid == winner else "rejected"
                            if mode == "automatic":
                                self.assertIsNone(state["confirmed_flag"])
                            else:
                                self.assertEqual(state["flag"], expected)
                                self.assertEqual(state["confirmed_flag"], expected)
                    finally:
                        reopened.close()

    def test_manual_duplicate_attribution_failure_rolls_back_merge(self):
        from unittest.mock import patch

        from services import duplicate_resolution

        winner = self.ids[0]
        self.db.conn.execute("UPDATE photos SET file_hash='same-content'")
        self.db.conn.commit()
        self.service.set_flag(winner, "flagged")
        before = self.db.photo_review.flag_attributions(self.ids)
        with patch.object(PhotoReviewRepository, "set_current_flag_note", side_effect=ValueError("note failure")):
            with self.assertRaisesRegex(ValueError, "note failure"):
                duplicate_resolution.keep_duplicate_copy(self.db, self.ids[1])
        self.assertEqual(self.db.photo_review.flag_attributions(self.ids), before)
        self.assertEqual([self.db.photos.get(pid)["flag"] for pid in self.ids], ["flagged", "none", "none"])
        self.assertFalse(self.db.conn.in_transaction)

    def test_manual_apply_round_trip_is_one_attributed_reversible_edit(self):
        from flask import Flask, jsonify
        from web.caches import create_caches_blueprint

        self.db.photo_review.flag_ranked_candidate(self.ids[0])
        self.db.commit()
        app = Flask(__name__)
        app.register_blueprint(create_caches_blueprint(
            lambda: self.db, lambda message, status=400: (jsonify(error=message), status),
            str(self.db_path), {}, start_thumbnail_recheck=lambda: {},
        ))
        response = app.test_client().post("/api/culling/apply", json={
            "keepers": self.ids[:1], "rejects": self.ids[1:2], "unflag": self.ids[2:],
        })
        self.assertEqual(response.status_code, 200)
        states = self.db.photo_review.culling_flag_states(self.ids)
        self.assertEqual([states[pid]["confirmed_flag"] for pid in self.ids],
                         ["flagged", "rejected", "none"])
        undo_last_edit(self.db)
        states = self.db.photo_review.culling_flag_states(self.ids)
        self.assertEqual([states[pid]["flag"] for pid in self.ids], ["flagged", "none", "none"])
        self.assertTrue(all(states[pid]["confirmed_flag"] is None for pid in self.ids))
        redo_last_undo(self.db)
        states = self.db.photo_review.culling_flag_states(self.ids)
        self.assertEqual([states[pid]["confirmed_flag"] for pid in self.ids],
                         ["flagged", "rejected", "none"])
        self.assertFalse(self.db.photo_review.flag_ranked_candidate(self.ids[2]))

    def test_apply_outer_transaction_rollback_restores_all_evidence(self):
        self.db.conn.execute("BEGIN IMMEDIATE")
        self.service.apply_culling_flags(self.ids[:1], self.ids[1:2], self.ids[2:])
        self.assertTrue(self.db.conn.in_transaction)
        self.db.rollback()
        states = self.db.photo_review.culling_flag_states(self.ids)
        self.assertTrue(all(states[pid]["flag"] == "none" for pid in self.ids))
        self.assertTrue(all(states[pid]["confirmed_flag"] is None for pid in self.ids))

    def test_apply_invalid_id_cannot_leave_a_partial_decision(self):
        with self.assertRaises(ValueError):
            self.service.apply_culling_flags(self.ids[:1], [999999], [])
        state = self.db.photo_review.culling_flag_states(self.ids)[self.ids[0]]
        self.assertEqual(state["flag"], "none")
        self.assertIsNone(state["confirmed_flag"])

    def test_version_25_catalog_upgrade_preserves_unknown_raw_flags(self):
        from db import Database

        pid = self.ids[0]
        self.db.photo_review.flag_ranked_candidate(pid)
        self.db.commit()
        self.db.conn.execute("DROP TABLE flag_attribution_history")
        self.db.conn.execute("PRAGMA user_version=25")
        self.db.commit()
        from schema import ensure_schema
        ensure_schema(str(self.db_path))
        upgraded = Database(str(self.db_path))
        try:
            upgraded.set_active_workspace(self.db.active_workspace_id)
            self.assertEqual(upgraded.conn.execute("PRAGMA user_version").fetchone()[0], SCHEMA_VERSION)
            self.assertEqual(upgraded.photo_review.culling_flag_states([pid])[pid]["flag"], "flagged")
            self.assertIsNone(upgraded.photo_review.culling_flag_states([pid])[pid]["confirmed_flag"])
            PhotoReviewService(upgraded).set_flag(pid, "flagged")
            undo_last_edit(upgraded)
            self.assertIsNone(upgraded.photo_review.culling_flag_states([pid])[pid]["confirmed_flag"])
        finally:
            upgraded.close()

    def test_pipeline_overlay_replaces_cached_claims_with_live_confirmation(self):
        from web.pipeline import _attach_live_flag_confirmations

        self.service.set_flag(self.ids[0], "flagged")
        self.service.set_flag(self.ids[1], "rejected")
        self.db.photo_review.flag_ranked_candidate(self.ids[2])
        self.db.commit()
        photos = [{"id": pid, "flag": "flagged", "flag_confirmation_status": "confirmed", "label": "KEEP",
                   "automatic_keep_eligible": True, "keep_status": "validated"}
                  for pid in self.ids + [999999]]
        results = {"photos": photos, "summary": {"keep_count": 4}}
        _attach_live_flag_confirmations(self.db, results)
        self.assertEqual([p["flag_confirmation_status"] for p in photos],
                         ["confirmed", "confirmed", "unverified", "unverified"])
        self.assertEqual([p["flag"] for p in photos], ["flagged", "rejected", "flagged", "none"])
        self.assertEqual([p["label"] for p in photos], ["KEEP", "REJECT", "REVIEW", "REVIEW"])
        self.assertTrue(all(p["automatic_keep_eligible"] is False for p in photos))
        self.assertEqual(results["summary"]["keep_count"], 1)
        self.assertEqual(results["summary"]["review_count"], 2)
        self.assertEqual(results["summary"]["reject_count"], 1)

        self.service.set_flag(self.ids[0], "none")
        _attach_live_flag_confirmations(self.db, results)
        self.assertEqual(photos[0]["label"], "REVIEW")
        self.assertEqual(results["summary"]["keep_count"], 0)

    def test_fresh_culling_uses_confirmations_at_completion(self):
        from culling import analyze_for_culling

        changed = False

        def confirm_during_run():
            nonlocal changed
            if not changed:
                self.service.set_flag(self.ids[0], "flagged")
                self.service.set_flag(self.ids[1], "rejected")
                self.db.photo_review.flag_ranked_candidate(self.ids[2])
                self.db.commit()
                changed = True

        result = analyze_for_culling(self.db, pause_callback=confirm_during_run)
        self.assertTrue(changed)
        photos = {p["photo_id"]: p for g in result["species_groups"]
                  for s in g["scene_groups"] for p in s["photos"]}
        self.assertEqual([photos[pid]["action"] for pid in self.ids], ["keep", "reject", "review"])
        self.assertEqual((result["suggested_keepers"], result["suggested_rejects"],
                          result["suggested_reviews"]), (1, 1, 1))


if __name__ == "__main__":
    unittest.main()
