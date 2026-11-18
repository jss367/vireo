"""Unsafe independent reset options stay visible as evidence, without writes."""

from dataclasses import replace
from datetime import timedelta

import pytest
from camera_clock import analyze_camera
from test_camera_clock import _reset_frames, frames_at


def _mixed_epoch(*, later_delay=0, westward=False):
    frames = _reset_frames(gps_error_before=0)
    return [replace(
        frame,
        captured=frame.captured + timedelta(minutes=later_delay if frame.ref > 9 else 0),
        latitude=1. if frame.ref > 6 else None,
        longitude=0. if frame.ref <= 9 else (-1. if westward else 1.),
    ) for frame in frames]


def _zone(_lat, longitude, _utc):
    return ("Synthetic zone", -420 + int(longitude) * 120)


def test_unsafe_independent_reset_options_have_no_actionable_fixes():
    frames = _mixed_epoch()

    problem = analyze_camera(frames, zone_lookup=_zone)

    assert problem is not None and problem.jump_count == 1
    assert problem.fixes == []
    payload = problem.to_json()
    assert payload["fixes"] == []
    assert "set back" in payload["summary"]
    assert any("clock went back" in line for line in payload["evidence"])
    assert any("GPS time agrees" in line for line in payload["evidence"])
    assert any("Automatic corrections are unavailable" in line for line in payload["evidence"])
    assert any("DSC_0001.NEF" in line and "DSC_0012.NEF" in line for line in payload["evidence"])


@pytest.mark.parametrize("independent_reset", [False, True])
def test_suppression_preserves_supported_independent_shot_sequences(independent_reset):
    frames = _mixed_epoch()
    if independent_reset:
        other = [replace(frame, ref=frame.ref + 100, filename=frame.filename.replace("DSC_", "IMG_"))
                 for frame in _reset_frames(gps_error_before=0)]
    else:
        other = frames_at(frames[0].captured, 4, offset=-420, first_count=100,
                          gps_error=30, prefix="IMG_", position=False)
    unsafe_refs = {frame.ref for frame in frames}
    other_refs = {frame.ref for frame in other}

    problem = analyze_camera([*frames, *other], zone_lookup=_zone)

    assert problem is not None and problem.fixes
    changed_refs = {frame.ref for root in problem.fixes
                    for step in [root, *root.dependent_fixes] for frame in step.frames}
    assert not changed_refs & unsafe_refs
    assert changed_refs <= other_refs
    assert any("Automatic corrections are unavailable" in line for line in problem.evidence)
    assert any("clock went back" in line for line in problem.evidence)


@pytest.mark.parametrize("later_delay,westward", [(180, False), (120, True)])
def test_supported_mixed_zone_options_remain_available(later_delay, westward):
    frames = _mixed_epoch(later_delay=later_delay, westward=westward)

    problem = analyze_camera(frames, zone_lookup=_zone)

    assert problem is not None
    assert len(problem.fixes) == 3
    assert any(frames[6] in fix.frames and fix.shift_minutes == 60 for fix in problem.fixes)
    assert not any("Automatic corrections are unavailable" in line for line in problem.evidence)


def test_unsafe_options_withhold_the_connected_repeated_reset_chain():
    frames = _mixed_epoch()
    frames += [replace(frame, ref=frame.ref + 6, filename=f"DSC_{frame.ref + 6:04d}.NEF",
                       captured=frame.captured - timedelta(minutes=54), latitude=None, longitude=None)
               for frame in frames[6:]]

    problem = analyze_camera(frames, zone_lookup=_zone)

    assert problem is not None and problem.jump_count == 2
    assert problem.fixes == []
    assert any("DSC_0001.NEF-DSC_0018.NEF" in line for line in problem.evidence)


def test_audit_still_reports_the_finding_without_actionable_corrections(app_and_db, tmp_path, monkeypatch):
    import json

    import time_zones
    from services import photo_ingest, workspace_folders

    app, db = app_and_db
    folder = workspace_folders.add_folder(db, str(tmp_path / "synthetic-shoot"))
    for frame in _mixed_epoch():
        pid = photo_ingest.add_photo(db, folder, frame.filename, ".nef", 1, 0,
                                     timestamp=frame.captured.isoformat())
        metadata = {"EXIF": {"DateTimeOriginal": frame.captured.strftime("%Y:%m:%d %H:%M:%S"),
                             "OffsetTimeOriginal": "-07:00", "Model": "Synthetic camera"},
                    "Composite": {"GPSLatitude": frame.latitude, "GPSLongitude": frame.longitude}}
        if frame.gps_utc is not None:
            metadata["Composite"]["GPSDateTime"] = frame.gps_utc.strftime("%Y:%m:%d %H:%M:%S")
        db.conn.execute("UPDATE photos SET exif_data = ? WHERE id = ?", (json.dumps(metadata), pid))
    db.commit()
    before = [tuple(row) for row in db.conn.execute("SELECT * FROM photos ORDER BY id")]
    monkeypatch.setattr(time_zones, "available", lambda: True)
    monkeypatch.setattr(time_zones, "local_zone", _zone)

    response = app.test_client().get("/api/audit/camera-clocks")

    assert response.status_code == 200
    [finding] = response.get_json()["findings"]
    assert finding["folder_id"] == folder and finding["jump_count"] == 1
    assert finding["fixes"] == []
    assert any("Automatic corrections are unavailable" in line for line in finding["evidence"])
    assert [tuple(row) for row in db.conn.execute("SELECT * FROM photos ORDER BY id")] == before


@pytest.mark.parametrize("later_shift,should_withhold", [(60, True), (240, False)])
def test_independent_groups_on_opposite_sides_of_reset(later_shift, should_withhold):
    frames = []
    for frame in _reset_frames(gps_error_after=-later_shift):
        if frame.ref <= 6:
            frames.append(replace(frame, captured=frame.captured + timedelta(minutes=48),
                                  latitude=1., longitude=0.))
        else:
            delay = timedelta(minutes=29)
            frames.append(replace(frame, captured=frame.captured - delay, gps_utc=frame.gps_utc - delay))

    problem = analyze_camera(frames, zone_lookup=lambda *_: ("Synthetic zone", -360))

    assert problem is not None and problem.jump_count == 1
    assert any("Automatic corrections are unavailable" in line for line in problem.evidence) == should_withhold
    if should_withhold:
        assert problem.fixes == []
    else:
        assert {fix.shift_minutes for fix in problem.fixes} == {0, 60, later_shift}
