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

ROOT = Path(__file__).resolve().parents[1]
if __name__ == "__main__":
    sys.path.insert(0, str(ROOT))

from canonical_schema import SCHEMA_VERSION, create_current_flag_attribution
from keep_safety import review_culling_suggestions
from repositories.edit_history import EditHistoryRepository
from repositories.photo_review import PhotoReviewRepository
from services.photo_review import PhotoReviewService, flag_note


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
        self.history = EditHistoryRepository(
            self.conn, lambda: self.active_workspace_id, non_undoable=[],
            restore_species_representative=lambda *args: None,
        )
        self.conn.commit()

    @property
    def photo_review(self):
        return PhotoReviewRepository(self.conn, self.active_workspace_id)

    @property
    def photos(self):
        def get_by_ids(ids):
            return {row["id"]: dict(row) for row in self.conn.execute(
                "SELECT p.id,p.flag FROM photos p JOIN photo_workspace_visibility v "
                "ON v.photo_id=p.id WHERE v.workspace_id=?", (self.active_workspace_id,),
            ) if row["id"] in ids}
        return SimpleNamespace(get_by_ids=get_by_ids)

    def queue_flag_change_if_enabled(self, photo_id, flag, *, _commit=True):
        self.conn.execute("INSERT INTO queued_flags VALUES(?,?)", (photo_id, flag))
        if _commit:
            self.conn.commit()

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
        self.service = PhotoReviewService(self.db)

    def test_real_catalog_undo_redo_preserves_confirmation(self):
        pid = self.ids[0]
        self.db.photo_review.flag_ranked_candidate(pid)
        self.service.set_flag(pid, "flagged")
        self.assertEqual(self.db.photo_review.culling_flag_states([pid])[pid]["confirmed_flag"], "flagged")
        self.db.undo_last_edit()
        self.assertIsNone(self.db.photo_review.culling_flag_states([pid])[pid]["confirmed_flag"])
        self.db.redo_last_undo()
        self.assertEqual(self.db.photo_review.culling_flag_states([pid])[pid]["confirmed_flag"], "flagged")
        self.service.set_flag(pid, "rejected")
        self.db.undo_last_edit()
        self.assertEqual(self.db.photo_review.culling_flag_states([pid])[pid]["confirmed_flag"], "flagged")

    def test_cached_endpoint_ignores_claimed_confirmation_without_modifying_cache(self):
        from flask import Flask, jsonify
        from web.caches import create_caches_blueprint

        self.service.set_flag(self.ids[0], "flagged")
        self.service.set_flag(self.ids[1], "rejected")
        self.db.photo_review.flag_ranked_candidate(self.ids[2])
        app = Flask(__name__)
        app.register_blueprint(create_caches_blueprint(
            lambda: self.db, lambda message, status: (jsonify(error=message), status), str(self.db_path), {},
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
                self.db.photo_review.flag_ranked_candidate(best)
                if source or reason:
                    with self.db.photo_review.flag_write():
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
        self.db.undo_last_edit()
        self.assertEqual(self.db.photo_review.flag_attributions(self.ids), before)
        states = self.db.photo_review.culling_flag_states(self.ids)
        self.assertIsNone(self.db.photos.get(second)["flag"])
        self.assertEqual([states[pid]["flag"] for pid in self.ids], ["flagged", None, "none"])
        self.assertEqual(states[first]["confirmed_flag"], "flagged")
        self.assertIsNone(states[second]["confirmed_flag"])
        self.db.redo_last_undo()
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

    def test_manual_apply_round_trip_is_one_attributed_reversible_edit(self):
        from flask import Flask, jsonify
        from web.caches import create_caches_blueprint

        self.db.photo_review.flag_ranked_candidate(self.ids[0])
        app = Flask(__name__)
        app.register_blueprint(create_caches_blueprint(
            lambda: self.db, lambda message, status=400: (jsonify(error=message), status),
            str(self.db_path), {},
        ))
        response = app.test_client().post("/api/culling/apply", json={
            "keepers": self.ids[:1], "rejects": self.ids[1:2], "unflag": self.ids[2:],
        })
        self.assertEqual(response.status_code, 200)
        states = self.db.photo_review.culling_flag_states(self.ids)
        self.assertEqual([states[pid]["confirmed_flag"] for pid in self.ids],
                         ["flagged", "rejected", "none"])
        self.db.undo_last_edit()
        states = self.db.photo_review.culling_flag_states(self.ids)
        self.assertEqual([states[pid]["flag"] for pid in self.ids], ["flagged", "none", "none"])
        self.assertTrue(all(states[pid]["confirmed_flag"] is None for pid in self.ids))
        self.db.redo_last_undo()
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
            upgraded.undo_last_edit()
            self.assertIsNone(upgraded.photo_review.culling_flag_states([pid])[pid]["confirmed_flag"])
        finally:
            upgraded.close()

    def test_pipeline_overlay_replaces_cached_claims_with_live_confirmation(self):
        from web.pipeline import _attach_live_flag_confirmations

        self.service.set_flag(self.ids[0], "flagged")
        self.service.set_flag(self.ids[1], "rejected")
        self.db.photo_review.flag_ranked_candidate(self.ids[2])
        photos = [{"id": pid, "flag": "flagged", "flag_confirmation_status": "confirmed"}
                  for pid in self.ids + [999999]]
        _attach_live_flag_confirmations(self.db, photos)
        self.assertEqual([p["flag_confirmation_status"] for p in photos],
                         ["confirmed", "confirmed", "unverified", "unverified"])
        self.assertEqual([p["flag"] for p in photos], ["flagged", "rejected", "flagged", "none"])

    def test_fresh_culling_uses_confirmations_at_completion(self):
        from culling import analyze_for_culling

        changed = False

        def confirm_during_run():
            nonlocal changed
            if not changed:
                self.service.set_flag(self.ids[0], "flagged")
                self.service.set_flag(self.ids[1], "rejected")
                self.db.photo_review.flag_ranked_candidate(self.ids[2])
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
