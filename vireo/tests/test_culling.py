# vireo/tests/test_culling.py
"""Tests for the culling engine (duplicate detection and scene grouping).

Tests cover the internal helper functions that form the culling pipeline:
- _cluster_photos: single-linkage embedding clustering
- _phash_merge: perceptual hash merging within a group
- _merge_buckets_by_phash: cross-bucket pHash merging (union-find)
- _group_into_scenes: time bucketing + pHash scene grouping
- _build_scene_groups: keep/reject decisions from scene + redundancy clusters
- _scene_label: human-readable scene labels
"""
import os
import sys
from datetime import datetime

import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from culling import (
    _build_scene_groups,
    _cluster_photos,
    _group_into_scenes,
    _merge_buckets_by_phash,
    _phash_merge,
    _scene_label,
    analyze_for_culling,
)

# ---------------------------------------------------------------------------
# _cluster_photos  (single-linkage embedding clustering)
# ---------------------------------------------------------------------------


def test_cluster_photos_identical_embeddings():
    """Identical embeddings should land in one cluster."""
    emb = np.ones(128, dtype=np.float32)
    emb /= np.linalg.norm(emb)
    embeddings = {1: emb, 2: emb.copy(), 3: emb.copy()}
    clusters = _cluster_photos(embeddings, threshold=0.88)
    assert len(clusters) == 1
    assert set(clusters[0]) == {1, 2, 3}


def test_cluster_photos_orthogonal_embeddings():
    """Orthogonal embeddings should each get their own cluster."""
    e1 = np.zeros(128, dtype=np.float32); e1[0] = 1.0
    e2 = np.zeros(128, dtype=np.float32); e2[1] = 1.0
    e3 = np.zeros(128, dtype=np.float32); e3[2] = 1.0
    clusters = _cluster_photos({1: e1, 2: e2, 3: e3}, threshold=0.88)
    assert len(clusters) == 3


def test_cluster_photos_single_photo():
    """A single photo returns one cluster with one element."""
    emb = np.random.randn(128).astype(np.float32)
    clusters = _cluster_photos({42: emb}, threshold=0.88)
    assert clusters == [[42]]


def test_cluster_photos_empty():
    """Empty input returns empty list with one empty cluster."""
    # _cluster_photos expects at least 1 element; with 0 it hits the early return
    clusters = _cluster_photos({}, threshold=0.88)
    assert clusters == [[]]


def test_cluster_photos_two_clusters():
    """Two groups of similar embeddings form two clusters."""
    base_a = np.random.randn(128).astype(np.float32)
    base_a /= np.linalg.norm(base_a)
    base_b = -base_a  # opposite direction, cosine sim = -1

    # Small perturbations keep within-group similarity high
    noise = lambda: np.random.randn(128).astype(np.float32) * 0.01
    embeddings = {
        1: base_a + noise(),
        2: base_a + noise(),
        3: base_b + noise(),
        4: base_b + noise(),
    }
    # Re-normalize
    for k in embeddings:
        embeddings[k] /= np.linalg.norm(embeddings[k])

    clusters = _cluster_photos(embeddings, threshold=0.88)
    assert len(clusters) == 2
    cluster_sets = [set(c) for c in clusters]
    assert {1, 2} in cluster_sets
    assert {3, 4} in cluster_sets


def test_cluster_photos_threshold_boundary():
    """Photos above the threshold should be clustered together."""
    # Create two embeddings with known cosine similarity ~0.90 (above 0.88)
    e1 = np.zeros(128, dtype=np.float32)
    e1[0] = 1.0
    import math
    theta = math.acos(0.90)
    e2 = np.zeros(128, dtype=np.float32)
    e2[0] = math.cos(theta)
    e2[1] = math.sin(theta)

    clusters = _cluster_photos({1: e1, 2: e2}, threshold=0.88)
    assert len(clusters) == 1

    # Below threshold — should NOT cluster
    theta2 = math.acos(0.85)
    e3 = np.zeros(128, dtype=np.float32)
    e3[0] = math.cos(theta2)
    e3[1] = math.sin(theta2)
    clusters = _cluster_photos({1: e1, 2: e3}, threshold=0.88)
    assert len(clusters) == 2


def test_cluster_photos_single_linkage():
    """Single-linkage: A~B and B~C should merge A, B, C even if A !~ C."""
    dim = 128
    e1 = np.zeros(dim, dtype=np.float32); e1[0] = 1.0
    # e2 is similar to e1 (sim ~ 0.90)
    import math
    theta1 = math.acos(0.90)
    e2 = np.zeros(dim, dtype=np.float32)
    e2[0] = math.cos(theta1)
    e2[1] = math.sin(theta1)
    # e3 is similar to e2 (sim ~ 0.90) but dissimilar to e1
    theta2 = math.acos(0.90)
    e3 = np.zeros(dim, dtype=np.float32)
    e3[0] = math.cos(theta1 + theta2)
    e3[1] = math.sin(theta1 + theta2)

    clusters = _cluster_photos({1: e1, 2: e2, 3: e3}, threshold=0.89)
    # e1~e2 and e2~e3 should chain them all into one cluster
    assert len(clusters) == 1
    assert set(clusters[0]) == {1, 2, 3}


# ---------------------------------------------------------------------------
# _phash_merge  (perceptual hash single-linkage within a group)
# ---------------------------------------------------------------------------


class FakeHash:
    """Minimal mock for imagehash objects — supports subtraction as Hamming distance."""
    def __init__(self, val):
        self.val = val

    def __sub__(self, other):
        return abs(self.val - other.val)


def test_phash_merge_single_photo():
    """Single photo returns one cluster."""
    result = _phash_merge([1], {1: FakeHash(0)}, threshold=10)
    assert result == [[1]]


def test_phash_merge_empty():
    """Empty list returns one empty cluster."""
    result = _phash_merge([], {}, threshold=10)
    assert result == [[]]


def test_phash_merge_similar():
    """Photos with similar pHash merge into one cluster."""
    phashes = {1: FakeHash(0), 2: FakeHash(5), 3: FakeHash(8)}
    result = _phash_merge([1, 2, 3], phashes, threshold=10)
    assert len(result) == 1
    assert set(result[0]) == {1, 2, 3}


def test_phash_merge_dissimilar():
    """Photos with very different pHash stay in separate clusters."""
    phashes = {1: FakeHash(0), 2: FakeHash(100), 3: FakeHash(200)}
    result = _phash_merge([1, 2, 3], phashes, threshold=10)
    assert len(result) == 3


def test_phash_merge_missing_phash():
    """Photos without pHash get their own cluster."""
    phashes = {1: FakeHash(0), 2: FakeHash(5)}
    result = _phash_merge([1, 2, 3], phashes, threshold=10)
    # 1 and 2 should merge; 3 has no phash and gets its own cluster
    assert len(result) == 2
    merged = [set(c) for c in result]
    assert {1, 2} in merged
    assert {3} in merged


def test_phash_merge_threshold_boundary():
    """Photos at exactly the threshold distance should merge."""
    phashes = {1: FakeHash(0), 2: FakeHash(10)}
    result = _phash_merge([1, 2], phashes, threshold=10)
    assert len(result) == 1


def test_phash_merge_just_above_threshold():
    """Photos just above threshold should NOT merge."""
    phashes = {1: FakeHash(0), 2: FakeHash(11)}
    result = _phash_merge([1, 2], phashes, threshold=10)
    assert len(result) == 2


# ---------------------------------------------------------------------------
# _merge_buckets_by_phash  (cross-bucket union-find merging)
# ---------------------------------------------------------------------------


def test_merge_buckets_similar():
    """Buckets with similar pHashes merge."""
    phashes = {1: FakeHash(0), 2: FakeHash(5), 3: FakeHash(100)}
    buckets = [[1], [2], [3]]
    result = _merge_buckets_by_phash(buckets, phashes, threshold=10)
    # bucket [1] and [2] should merge; [3] stays separate
    assert len(result) == 2
    merged_sets = [set(b) for b in result]
    assert {1, 2} in merged_sets
    assert {3} in merged_sets


def test_merge_buckets_none_similar():
    """Buckets with no similar pHashes stay separate."""
    phashes = {1: FakeHash(0), 2: FakeHash(100), 3: FakeHash(200)}
    buckets = [[1], [2], [3]]
    result = _merge_buckets_by_phash(buckets, phashes, threshold=10)
    assert len(result) == 3


def test_merge_buckets_all_similar():
    """All buckets merge when all pHashes are similar."""
    phashes = {1: FakeHash(0), 2: FakeHash(3), 3: FakeHash(6)}
    buckets = [[1], [2], [3]]
    result = _merge_buckets_by_phash(buckets, phashes, threshold=10)
    assert len(result) == 1
    assert set(result[0]) == {1, 2, 3}


def test_merge_buckets_transitive():
    """Union-find merges transitively: A~B and B~C merges A+B+C."""
    phashes = {1: FakeHash(0), 2: FakeHash(8), 3: FakeHash(16)}
    buckets = [[1], [2], [3]]
    # 1~2 (dist=8 <= 10), 2~3 (dist=8 <= 10), but 1~3 (dist=16 > 10)
    result = _merge_buckets_by_phash(buckets, phashes, threshold=10)
    assert len(result) == 1
    assert set(result[0]) == {1, 2, 3}


def test_merge_buckets_missing_phash():
    """Buckets with no pHash data stay isolated."""
    phashes = {1: FakeHash(0)}
    buckets = [[1], [2], [3]]
    result = _merge_buckets_by_phash(buckets, phashes, threshold=10)
    assert len(result) == 3


# ---------------------------------------------------------------------------
# _group_into_scenes  (time bucketing + pHash)
# ---------------------------------------------------------------------------


def test_group_into_scenes_by_time():
    """Photos within time_window seconds group together."""
    ts = {
        1: datetime(2024, 1, 1, 10, 0, 0),
        2: datetime(2024, 1, 1, 10, 0, 30),  # 30s gap
        3: datetime(2024, 1, 1, 10, 5, 0),   # 270s gap — new scene
    }
    # Must provide similar pHashes within time buckets, otherwise _phash_merge
    # splits photos without pHash into separate scenes
    phashes = {1: FakeHash(0), 2: FakeHash(1), 3: FakeHash(0)}
    scenes = _group_into_scenes([1, 2, 3], ts, phashes, time_window=60, phash_threshold=10, cross_bucket_merge=False)
    assert len(scenes) == 2
    scene_sets = [set(s) for s in scenes]
    assert {1, 2} in scene_sets
    assert {3} in scene_sets


def test_group_into_scenes_no_timestamps():
    """Photos without timestamps each get their own scene."""
    scenes = _group_into_scenes([1, 2, 3], {}, {}, time_window=60, phash_threshold=10, cross_bucket_merge=False)
    assert len(scenes) == 3


def test_group_into_scenes_mixed_timestamps():
    """Photos with and without timestamps are handled correctly."""
    ts = {1: datetime(2024, 1, 1, 10, 0, 0), 2: datetime(2024, 1, 1, 10, 0, 10)}
    phashes = {1: FakeHash(0), 2: FakeHash(1)}
    scenes = _group_into_scenes([1, 2, 3], ts, phashes, time_window=60, phash_threshold=10, cross_bucket_merge=False)
    # 1 and 2 group by time + similar pHash; 3 has no timestamp so gets its own scene
    assert len(scenes) == 2
    scene_sets = [set(s) for s in scenes]
    assert {1, 2} in scene_sets
    assert {3} in scene_sets


def test_group_into_scenes_cross_bucket_merge():
    """cross_bucket_merge=True merges time-separated scenes with similar pHash."""
    ts = {
        1: datetime(2024, 1, 1, 10, 0, 0),
        2: datetime(2024, 1, 1, 12, 0, 0),  # 2 hours later — different time bucket
    }
    phashes = {1: FakeHash(0), 2: FakeHash(5)}
    scenes = _group_into_scenes(
        [1, 2], ts, phashes, time_window=60, phash_threshold=10, cross_bucket_merge=True
    )
    assert len(scenes) == 1
    assert set(scenes[0]) == {1, 2}


def test_group_into_scenes_cross_bucket_no_merge():
    """cross_bucket_merge=False keeps time-separated scenes apart."""
    ts = {
        1: datetime(2024, 1, 1, 10, 0, 0),
        2: datetime(2024, 1, 1, 12, 0, 0),
    }
    phashes = {1: FakeHash(0), 2: FakeHash(5)}
    scenes = _group_into_scenes(
        [1, 2], ts, phashes, time_window=60, phash_threshold=10, cross_bucket_merge=False
    )
    assert len(scenes) == 2


def test_group_into_scenes_single_photo():
    """Single photo returns one scene."""
    ts = {1: datetime(2024, 1, 1, 10, 0, 0)}
    scenes = _group_into_scenes([1], ts, {}, time_window=60, phash_threshold=10, cross_bucket_merge=False)
    assert scenes == [[1]]


# ---------------------------------------------------------------------------
# _build_scene_groups  (keep/reject decisions)
# ---------------------------------------------------------------------------


def test_build_scene_groups_single_redundancy_cluster():
    """In a redundancy cluster, highest quality photo is kept, rest rejected."""
    scene_clusters = [[1, 2, 3]]
    redundancy_clusters = [[1, 2, 3]]
    photo_data = [
        {"photo_id": 1, "quality": 0.5, "filename": "a.jpg", "timestamp": None, "phash": None},
        {"photo_id": 2, "quality": 0.9, "filename": "b.jpg", "timestamp": None, "phash": None},
        {"photo_id": 3, "quality": 0.3, "filename": "c.jpg", "timestamp": None, "phash": None},
    ]

    groups, keepers, rejects = _build_scene_groups(
        scene_clusters, redundancy_clusters, photo_data, {}, {}
    )

    assert len(groups) == 1
    assert keepers == 1
    assert rejects == 2

    # Photo 2 has highest quality so it should be the keeper
    photos = groups[0]["photos"]
    keeper = [p for p in photos if p["action"] == "keep"]
    assert len(keeper) == 1
    assert keeper[0]["photo_id"] == 2


def test_build_scene_groups_separate_redundancy_clusters():
    """Photos in different redundancy clusters each get a keeper."""
    scene_clusters = [[1, 2, 3, 4]]
    redundancy_clusters = [[1, 2], [3, 4]]  # Two separate clusters
    photo_data = [
        {"photo_id": 1, "quality": 0.9, "filename": "a.jpg", "timestamp": None, "phash": None},
        {"photo_id": 2, "quality": 0.3, "filename": "b.jpg", "timestamp": None, "phash": None},
        {"photo_id": 3, "quality": 0.8, "filename": "c.jpg", "timestamp": None, "phash": None},
        {"photo_id": 4, "quality": 0.2, "filename": "d.jpg", "timestamp": None, "phash": None},
    ]

    groups, keepers, rejects = _build_scene_groups(
        scene_clusters, redundancy_clusters, photo_data, {}, {}
    )

    assert keepers == 2  # One keeper per redundancy cluster
    assert rejects == 2


def test_build_scene_groups_multiple_scenes():
    """Multiple scenes produce multiple scene groups."""
    scene_clusters = [[1, 2], [3]]
    redundancy_clusters = [[1, 2], [3]]
    photo_data = [
        {"photo_id": 1, "quality": 0.5, "filename": "a.jpg", "timestamp": None, "phash": None},
        {"photo_id": 2, "quality": 0.9, "filename": "b.jpg", "timestamp": None, "phash": None},
        {"photo_id": 3, "quality": 0.7, "filename": "c.jpg", "timestamp": None, "phash": None},
    ]

    groups, keepers, rejects = _build_scene_groups(
        scene_clusters, redundancy_clusters, photo_data, {}, {}
    )

    assert len(groups) == 2
    assert keepers == 2  # One per scene
    assert rejects == 1


def test_build_scene_groups_no_redundancy():
    """Photos not in any redundancy cluster each become keepers."""
    scene_clusters = [[1, 2, 3]]
    redundancy_clusters = [[1], [2], [3]]  # Each in its own cluster
    photo_data = [
        {"photo_id": 1, "quality": 0.5, "filename": "a.jpg", "timestamp": None, "phash": None},
        {"photo_id": 2, "quality": 0.9, "filename": "b.jpg", "timestamp": None, "phash": None},
        {"photo_id": 3, "quality": 0.3, "filename": "c.jpg", "timestamp": None, "phash": None},
    ]

    groups, keepers, rejects = _build_scene_groups(
        scene_clusters, redundancy_clusters, photo_data, {}, {}
    )

    assert keepers == 3
    assert rejects == 0


# ---------------------------------------------------------------------------
# _scene_label
# ---------------------------------------------------------------------------


def test_scene_label_with_timestamps():
    """Scene label includes time range when timestamps available."""
    ts = {
        1: datetime(2024, 1, 1, 10, 0, 0),
        2: datetime(2024, 1, 1, 10, 5, 30),
    }
    label = _scene_label(0, [1, 2], ts, {})
    assert "Scene 1" in label
    assert "10:00:00" in label
    assert "10:05:30" in label
    assert "2 photos" in label


def test_scene_label_same_timestamp():
    """Single timestamp shows just one time, no range."""
    ts = {1: datetime(2024, 1, 1, 10, 0, 0)}
    label = _scene_label(0, [1], ts, {})
    assert "10:00:00" in label
    assert " to " not in label  # no time range for single timestamp


def test_scene_label_no_timestamps():
    """Scene label without timestamps shows just scene number and count."""
    label = _scene_label(2, [1, 2, 3], {}, {})
    assert "Scene 3" in label
    assert "3 photos" in label


def test_scene_label_numbering():
    """Scene ID 0 becomes Scene 1 (1-indexed for display)."""
    label = _scene_label(0, [1], {}, {})
    assert "Scene 1" in label
    label = _scene_label(4, [1], {}, {})
    assert "Scene 5" in label


# ---------------------------------------------------------------------------
# analyze_for_culling  (integration test with real DB)
# ---------------------------------------------------------------------------


def test_analyze_for_culling_empty(tmp_path):
    """Analyzing an empty database returns zero counts."""
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)

    result = analyze_for_culling(db)
    assert result["total_photos"] == 0
    assert result["suggested_keepers"] == 0
    assert result["suggested_rejects"] == 0
    assert result["species_groups"] == []


def _setup_culling_db(tmp_path, with_embeddings=True):
    """Helper: create a DB with photos, predictions, and optional embeddings."""
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)

    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    fid = db.add_folder(folder_path, name="photos")

    # Create test images so pHash backfill doesn't fail
    from PIL import Image
    photo_ids = []
    for i in range(4):
        fname = f"bird{i}.jpg"
        img = Image.new("RGB", (100, 100), color=(i * 60, i * 60, i * 60))
        img.save(os.path.join(folder_path, fname))
        ts = f"2024-01-01T10:00:{i * 10:02d}"
        pid = db.add_photo(fid, fname, ".jpg", 1000, 1.0, timestamp=ts)
        photo_ids.append(pid)

    # Add detections and predictions (all same species)
    for pid in photo_ids:
        det_ids = db.save_detections(pid, [
            {"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.4}, "confidence": 0.9, "category": "animal"}
        ], detector_model="MDV6")
        db.add_prediction(det_ids[0], "Robin", 0.95, "test-model")

    if with_embeddings:
        # Add similar embeddings for first 3 (they should cluster), different for 4th
        base_emb = np.random.randn(128).astype(np.float32)
        base_emb /= np.linalg.norm(base_emb)
        for i, pid in enumerate(photo_ids[:3]):
            noise = np.random.randn(128).astype(np.float32) * 0.01
            emb = base_emb + noise
            emb /= np.linalg.norm(emb)
            db.conn.execute(
                "UPDATE photos SET embedding = ?, quality_score = ? WHERE id = ?",
                (emb.tobytes(), 0.5 + i * 0.1, pid),
            )
        # 4th photo: orthogonal embedding
        diff_emb = np.random.randn(128).astype(np.float32)
        diff_emb -= diff_emb.dot(base_emb) * base_emb  # Gram-Schmidt
        diff_emb /= np.linalg.norm(diff_emb)
        db.conn.execute(
            "UPDATE photos SET embedding = ?, quality_score = ? WHERE id = ?",
            (diff_emb.tobytes(), 0.9, photo_ids[3]),
        )

    db.conn.commit()
    return db, photo_ids


def test_analyze_for_culling_with_predictions(tmp_path):
    """Full pipeline produces keepers and rejects."""
    from culling import analyze_for_culling

    db, photo_ids = _setup_culling_db(tmp_path)
    result = analyze_for_culling(db)

    assert result["total_photos"] == 4
    assert result["suggested_keepers"] >= 1
    assert result["suggested_keepers"] + result["suggested_rejects"] == 4
    assert len(result["species_groups"]) >= 1

    # Check structure of species group
    sg = result["species_groups"][0]
    assert "Robin" in sg["species"]
    assert sg["photo_count"] == 4
    assert len(sg["scene_groups"]) >= 1
    assert len(sg["redundancy_clusters"]) >= 1
    assert isinstance(sg["embedding_sims"], dict)


def test_analyze_for_culling_no_embeddings(tmp_path):
    """Pipeline works even without embeddings (no clustering, all kept)."""
    from culling import analyze_for_culling

    db, photo_ids = _setup_culling_db(tmp_path, with_embeddings=False)
    result = analyze_for_culling(db)

    assert result["total_photos"] == 4
    # Without embeddings, each photo is its own cluster — all keepers
    assert result["suggested_keepers"] == 4
    assert result["suggested_rejects"] == 0


def test_analyze_for_culling_collection_scope(tmp_path):
    """Scoping to a collection limits which photos are analyzed."""
    import json as _json

    db, photo_ids = _setup_culling_db(tmp_path)

    # Create a static collection with only 2 photos via rules
    rules = _json.dumps([{"field": "photo_ids", "value": [photo_ids[0], photo_ids[1]]}])
    cid = db.add_collection("test collection", rules)

    result = analyze_for_culling(db, collection_id=cid)
    assert result["total_photos"] == 2


def test_analyze_for_culling_progress_callback(tmp_path):
    """Progress callback is called during analysis."""
    from culling import analyze_for_culling

    db, _ = _setup_culling_db(tmp_path, with_embeddings=False)

    messages = []
    analyze_for_culling(db, progress_callback=lambda msg: messages.append(msg))
    # Should have called with pHash backfill message
    assert any("hash" in m.lower() for m in messages)


def test_analyze_for_culling_backfills_phash_from_working_copy(tmp_path):
    """When the source file can't be opened (e.g. RAW), the phash backfill
    should use the working-copy JPEG if available."""
    from culling import analyze_for_culling
    from db import Database
    from PIL import Image

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)

    vireo_dir = tmp_path
    folder_path = str(tmp_path / "raws")
    os.makedirs(folder_path, exist_ok=True)
    fid = db.add_folder(folder_path, name="raws")

    # Simulate a RAW: register the photo with a .NEF extension, but don't
    # write a real NEF — PIL can't open it. The working copy IS available.
    fname = "bird.NEF"
    with open(os.path.join(folder_path, fname), "wb") as f:
        f.write(b"not a real raw file")

    wc_rel = "working/bird.jpg"
    wc_abs = os.path.join(vireo_dir, wc_rel)
    os.makedirs(os.path.dirname(wc_abs), exist_ok=True)
    Image.new("RGB", (100, 100), color=(50, 120, 80)).save(wc_abs)

    pid = db.add_photo(fid, fname, ".NEF", 1000, 1.0, timestamp="2024-01-01T10:00:00")
    db.conn.execute(
        "UPDATE photos SET working_copy_path = ? WHERE id = ?", (wc_rel, pid)
    )
    det_ids = db.save_detections(pid, [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.4}, "confidence": 0.9, "category": "animal"}
    ], detector_model="MDV6")
    db.add_prediction(det_ids[0], "Robin", 0.95, "test-model")
    db.conn.commit()

    analyze_for_culling(db, vireo_dir=str(vireo_dir))

    row = db.conn.execute("SELECT phash FROM photos WHERE id = ?", (pid,)).fetchone()
    assert row["phash"], "phash should be backfilled from working-copy JPEG"


def test_analyze_for_culling_reports_missing_phash_count(tmp_path):
    """Photos that can't be hashed (no working copy, unreadable original)
    should be counted in result['photos_missing_phash']."""
    from culling import analyze_for_culling
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)

    folder_path = str(tmp_path / "raws")
    os.makedirs(folder_path, exist_ok=True)
    fid = db.add_folder(folder_path, name="raws")

    # Unreadable "RAW": junk bytes, no working copy
    fname = "broken.NEF"
    with open(os.path.join(folder_path, fname), "wb") as f:
        f.write(b"not a real raw file")

    pid = db.add_photo(fid, fname, ".NEF", 1000, 1.0, timestamp="2024-01-01T10:00:00")
    det_ids = db.save_detections(pid, [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.4}, "confidence": 0.9, "category": "animal"}
    ], detector_model="MDV6")
    db.add_prediction(det_ids[0], "Robin", 0.95, "test-model")
    db.conn.commit()

    result = analyze_for_culling(db, vireo_dir=str(tmp_path))
    assert result["photos_missing_phash"] == 1


def test_analyze_for_culling_missing_phash_zero_when_all_hashed(tmp_path):
    """When every photo gets a phash, the missing count is zero."""
    from culling import analyze_for_culling

    db, _ = _setup_culling_db(tmp_path, with_embeddings=False)
    result = analyze_for_culling(db)
    assert result["photos_missing_phash"] == 0


def test_analyze_for_culling_falls_back_to_source_when_working_copy_corrupt(tmp_path):
    """If the working-copy JPEG can't be decoded, the backfill should still
    try the original source file instead of giving up."""
    from culling import analyze_for_culling
    from db import Database
    from PIL import Image

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)

    vireo_dir = tmp_path
    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    fid = db.add_folder(folder_path, name="photos")

    # Valid JPEG at the source path — can be opened.
    fname = "bird.jpg"
    Image.new("RGB", (100, 100), color=(20, 140, 90)).save(
        os.path.join(folder_path, fname)
    )

    # Working-copy path points to a file that exists but is junk bytes —
    # _load_standard will return None.
    wc_rel = "working/broken.jpg"
    wc_abs = os.path.join(vireo_dir, wc_rel)
    os.makedirs(os.path.dirname(wc_abs), exist_ok=True)
    with open(wc_abs, "wb") as f:
        f.write(b"not a real jpeg")

    pid = db.add_photo(fid, fname, ".jpg", 1000, 1.0, timestamp="2024-01-01T10:00:00")
    db.conn.execute(
        "UPDATE photos SET working_copy_path = ? WHERE id = ?", (wc_rel, pid)
    )
    det_ids = db.save_detections(pid, [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.4}, "confidence": 0.9, "category": "animal"}
    ], detector_model="MDV6")
    db.add_prediction(det_ids[0], "Robin", 0.95, "test-model")
    db.conn.commit()

    result = analyze_for_culling(db, vireo_dir=str(vireo_dir))
    assert result["photos_missing_phash"] == 0
    row = db.conn.execute("SELECT phash FROM photos WHERE id = ?", (pid,)).fetchone()
    assert row["phash"], "phash should fall back to the valid source JPEG"
