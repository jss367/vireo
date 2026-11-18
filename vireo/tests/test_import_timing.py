"""An import reports where its time went.

The job has one step, so a slow run used to read as one long "Copy &
catalog". The result now carries seconds per phase, the bytes copied, and a
sentence the Jobs details and the Import page show.
"""
from datetime import datetime

import pytest
from import_job.state import _ImportRunState
from import_job.timing import (
    CATALOG,
    COPY,
    FIND,
    WORKING_COPIES,
    timed,
    timing_notice,
    timing_result,
)
from test_import_job import _make_card, _run_import


def _state(**seconds):
    state = _ImportRunState(log_label="test")
    state.phase_seconds.update(seconds)
    return state


def test_a_phase_entered_several_times_adds_up(monkeypatch):
    import import_job.timing as timing

    clock = iter([10.0, 12.5, 100.0, 104.0])
    monkeypatch.setattr(timing.time, "monotonic", lambda: next(clock))
    state = _state()

    with timed(state, COPY):
        pass
    with timed(state, COPY):
        pass

    assert state.phase_seconds == {COPY: pytest.approx(6.5)}


def test_a_phase_that_raises_still_counts_its_time(monkeypatch):
    import import_job.timing as timing

    clock = iter([10.0, 13.0])
    monkeypatch.setattr(timing.time, "monotonic", lambda: next(clock))
    state = _state()

    with pytest.raises(OSError), timed(state, CATALOG):
        raise OSError("mount went away")

    assert state.phase_seconds == {CATALOG: pytest.approx(3.0)}


def test_the_notice_names_each_phase_with_the_copy_rate():
    state = _state(find=12.3, copy=4404.0, catalog=1381.0, working_copies=1792.0)
    state.transferred_bytes = 4404 * 8 * 1024 * 1024

    assert timing_notice(state, verified=True) == (
        "Time spent: finding photos 12 s; copying and verifying 1 h 13 min "
        "(34.4 GB at 8.0 MB/s); cataloging 23 min; working copies 29 min."
    )


def test_a_transport_that_does_not_verify_is_not_said_to():
    state = _state(copy=120.0)
    state.transferred_bytes = 120 * 1024 * 1024

    assert timing_notice(state, verified=False) == (
        "Time spent: copying 2 min (120.0 MB at 1.0 MB/s)."
    )


def test_a_copy_phase_that_copied_nothing_has_no_rate():
    """A duplicates-only run spends its copy phase in the duplicate gate."""
    state = _state(find=1.0, copy=5.0)

    assert timing_notice(state, verified=True) == (
        "Time spent: finding photos 1 s; copying and verifying 5 s."
    )


def test_only_the_phases_a_run_entered_are_reported():
    state = _state(find=2.0)

    assert timing_notice(state, verified=True) == "Time spent: finding photos 2 s."
    assert timing_result(state) == {"seconds": {FIND: 2.0}, "transferred_bytes": 0}
    assert timing_notice(_state(), verified=True) is None


def _card(tmp_path):
    return _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0002.jpg", datetime(2026, 7, 3, 11, 0, 0), "green"),
        ("DSC_0003.jpg", datetime(2026, 7, 4, 9, 0, 0), "blue"),
    ])


def test_an_import_reports_its_phases_and_the_bytes_it_copied(tmp_path):
    from import_job import ImportParams

    card = _card(tmp_path)
    card_bytes = sum(p.stat().st_size for p in card.iterdir())

    _db, _ws, result = _run_import(tmp_path, ImportParams(
        sources=[str(card)], destination=str(tmp_path / "archive"),
        vireo_dir=str(tmp_path / "vireo"),
    ))

    assert result["copied"] == 3
    timing = result["timing"]
    assert list(timing["seconds"]) == [FIND, COPY, CATALOG, WORKING_COPIES]
    assert all(seconds >= 0 for seconds in timing["seconds"].values())
    assert timing["transferred_bytes"] == card_bytes
    assert result["timing_notice"].startswith("Time spent: finding photos ")
    assert "copying and verifying" in result["timing_notice"]
    assert "working copies" in result["timing_notice"]
    # Information about the run, not a problem with it.
    assert result["ok"] is True
    assert result["errors"] == []


def test_an_import_without_working_copies_reports_no_working_copy_phase(tmp_path):
    from import_job import ImportParams

    card = _card(tmp_path)

    _db, _ws, result = _run_import(tmp_path, ImportParams(
        sources=[str(card)], destination=str(tmp_path / "archive"),
    ))

    assert list(result["timing"]["seconds"]) == [FIND, COPY, CATALOG]
    assert "working copies" not in result["timing_notice"]


def test_a_duplicates_only_import_copies_no_bytes(tmp_path):
    from import_job import ImportParams

    card = _card(tmp_path)
    params = ImportParams(sources=[str(card)], destination=str(tmp_path / "archive"))
    _run_import(tmp_path, params)

    _db, _ws, again = _run_import(tmp_path, params)

    assert again["copied"] == 0
    assert again["skipped_duplicate"] == 3
    assert again["timing"]["transferred_bytes"] == 0
    assert " at " not in again["timing_notice"]


def test_the_jobs_details_show_the_timing_sentence_and_not_its_raw_numbers(tmp_path):
    from import_job import ImportParams
    from job_summaries import describe_result

    card = _card(tmp_path)
    _db, _ws, result = _run_import(tmp_path, ImportParams(
        sources=[str(card)], destination=str(tmp_path / "archive"),
    ))

    described = describe_result("import", result)

    assert result["timing_notice"] in described["details"]
    assert "Timing" not in described["summary"]
    assert "transferred" not in described["summary"].lower()
