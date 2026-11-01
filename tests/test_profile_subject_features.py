"""The Extract features profiler: bounded sampling and temporary-mask cleanup.

Nothing here loads a model or opens a real catalog: the sampler runs on a
scratch SQLite file or a fake connection, and the profiling loop on stubs.
"""
import os
import sqlite3
import types

import numpy as np
import pytest

from scripts import profile_subject_features as profiler


def _catalog(path, photo_ids, variant="sam2-large"):
    conn = sqlite3.connect(path)
    conn.executescript(
        "CREATE TABLE folders (id INTEGER PRIMARY KEY, path TEXT);"
        "CREATE TABLE photos (id INTEGER PRIMARY KEY, filename TEXT, folder_id INTEGER);"
        "CREATE TABLE photo_masks (photo_id INTEGER, variant TEXT, prompt_x REAL,"
        " prompt_y REAL, prompt_w REAL, prompt_h REAL);"
        "INSERT INTO folders VALUES (1, '/photos');"
    )
    conn.executemany("INSERT INTO photos VALUES (?, ?, 1)", [(i, f"{i}.NEF") for i in photo_ids])
    conn.executemany(
        "INSERT INTO photo_masks VALUES (?, ?, 0.1, 0.1, 0.5, 0.5)",
        [(i, variant) for i in photo_ids],
    )
    conn.commit()
    conn.close()
    return str(path)


def _ids(rows):
    return sorted(row["id"] for row in rows)


def test_sample_photos_returns_a_seeded_sample_of_the_range(tmp_path):
    db = _catalog(tmp_path / "catalog.db", range(1, 1001))

    first = profiler.sample_photos(db, 100, 900, "sam2-large", 20, seed=1)

    assert len(first) == 20 and len(set(_ids(first))) == 20
    assert all(100 <= photo_id <= 900 for photo_id in _ids(first))
    assert _ids(profiler.sample_photos(db, 100, 900, "sam2-large", 20, seed=1)) == _ids(first)
    assert _ids(profiler.sample_photos(db, 100, 900, "sam2-large", 20, seed=2)) != _ids(first)


def test_sample_photos_returns_every_row_when_fewer_than_the_sample(tmp_path):
    db = _catalog(tmp_path / "catalog.db", range(1, 6))

    assert _ids(profiler.sample_photos(db, 1, 5, "sam2-large", 20, seed=1)) == [1, 2, 3, 4, 5]
    assert profiler.sample_photos(db, 50, 60, "sam2-large", 20, seed=1) == []
    assert profiler.sample_photos(db, 1, 5, "sam2-small", 20, seed=1) == []


class _FakeConnection:
    """Yields rows one at a time and records closing; fails after ``fail_after``."""

    def __init__(self, rows, fail_after=None):
        self.rows, self.fail_after = rows, fail_after
        self.closed = False
        self.yielded = 0

    def execute(self, sql, params):
        def stream():
            for row in self.rows:
                if self.fail_after is not None and self.yielded == self.fail_after:
                    raise sqlite3.OperationalError("disk I/O error")
                self.yielded += 1
                yield row
        return stream()

    def close(self):
        self.closed = True


def _patch_connect(monkeypatch, connection):
    # Only the profiler module's ``sqlite3`` name is replaced, never the
    # global ``sqlite3.connect`` that coverage uses.
    fake = types.SimpleNamespace(connect=lambda *a, **k: connection, Row=sqlite3.Row)
    monkeypatch.setattr(profiler, "sqlite3", fake)


def test_sample_photos_streams_the_cursor_and_closes_the_connection(monkeypatch):
    connection = _FakeConnection([{"id": i} for i in range(500)])
    _patch_connect(monkeypatch, connection)

    rows = profiler.sample_photos("catalog.db", 0, 500, "sam2-large", 7, seed=3)

    assert len(rows) == 7 and connection.yielded == 500
    assert connection.closed


def test_sample_photos_closes_the_connection_when_iteration_fails(monkeypatch):
    connection = _FakeConnection([{"id": i} for i in range(50)], fail_after=10)
    _patch_connect(monkeypatch, connection)

    with pytest.raises(sqlite3.OperationalError):
        profiler.sample_photos("catalog.db", 0, 50, "sam2-large", 5, seed=3)

    assert connection.closed


class _FakeSession:
    def get_inputs(self):
        return [types.SimpleNamespace(name="input")]

    def run(self, outputs, feeds):
        return [np.zeros((1, 1)), np.zeros((1, 1))]


class _Interrupted(BaseException):
    """Stands in for Ctrl-C without stopping the test run."""


def _stub_models(monkeypatch, *, fail_decode_on=None, fail_save=None):
    import dino_embed
    import masking
    import onnx_runtime
    import quality

    seen = {"dirs": set()}
    proxy = types.SimpleNamespace(size=(4, 3))

    def render_proxy(path, longest_edge):
        if fail_decode_on is not None and path.endswith(fail_decode_on):
            raise OSError("source went away")
        return proxy

    def save_mask(mask, masks_dir, photo_id, variant):
        seen["dirs"].add(masks_dir)
        if fail_save is not None:
            raise fail_save
        path = os.path.join(masks_dir, f"{photo_id}.png")
        with open(path, "wb") as handle:
            handle.write(b"png")
        return path

    monkeypatch.setattr(masking, "_get_sam2_sessions", lambda variant: (_FakeSession(), _FakeSession()))
    monkeypatch.setattr(dino_embed, "_get_dinov2_session", lambda variant: None)
    monkeypatch.setattr(masking, "render_proxy", render_proxy)
    monkeypatch.setattr(onnx_runtime, "preprocess_image", lambda *a, **k: np.zeros((1, 3, 8, 8)))
    monkeypatch.setattr(masking, "_sam2_decoder_inputs", lambda *a: {})
    monkeypatch.setattr(masking, "_best_sam2_mask", lambda *a: np.ones((3, 4), dtype=bool))
    monkeypatch.setattr(masking, "crop_completeness", lambda mask: 1.0)
    monkeypatch.setattr(quality, "compute_all_quality_features", lambda proxy, mask: {})
    monkeypatch.setattr(masking, "crop_subject", lambda proxy, mask, margin: None)
    monkeypatch.setattr(dino_embed, "embed_batch", lambda images, variant: np.zeros((len(images), 4)))
    monkeypatch.setattr(masking, "save_mask", save_mask)
    return seen


ROWS = [
    {"id": photo_id, "filename": f"{photo_id}.NEF", "folder": "/photos",
     "x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5}
    for photo_id in (1, 2, 3)
]


def test_profile_times_each_phase_and_removes_its_mask_directory(monkeypatch):
    seen = _stub_models(monkeypatch)

    timings = profiler.profile(ROWS, "sam2-large", "vit-b14", 2048)

    assert set(timings) == {
        "decode", "sam2_encoder", "sam2_decoder", "analyze", "embed", "store_png", "total",
    }
    assert len(timings["total"]) == 3
    (masks_dir,) = seen["dirs"]
    assert not os.path.exists(masks_dir)


@pytest.mark.parametrize(
    ("stub", "error"),
    (
        ({"fail_decode_on": "2.NEF"}, OSError),
        ({"fail_save": OSError("disk full")}, OSError),
        ({"fail_save": _Interrupted()}, _Interrupted),
    ),
    ids=("later-row-failure", "save-failure", "interruption"),
)
def test_profile_removes_its_mask_directory_when_it_stops_early(monkeypatch, stub, error):
    seen = _stub_models(monkeypatch, **stub)

    with pytest.raises(error):
        profiler.profile(ROWS, "sam2-large", "vit-b14", 2048)

    (masks_dir,) = seen["dirs"]
    assert not os.path.exists(masks_dir)
