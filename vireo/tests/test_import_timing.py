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


def test_a_phase_shorter_than_a_second_is_not_reported_as_zero():
    state = _state(find=0.1, catalog=0.5)

    assert timing_notice(state, verified=True) == (
        "Time spent: finding photos under 1 s; cataloging under 1 s."
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


def test_a_copy_that_fails_verification_still_counts_its_bytes(tmp_path, monkeypatch):
    """The file was written and read back; that time is in the copy phase,
    so leaving its bytes out would understate the rate of a failing run."""
    import import_job.transports as transports
    from import_job import ImportParams

    card = _card(tmp_path)
    card_bytes = sum(p.stat().st_size for p in card.iterdir())
    monkeypatch.setattr(
        transports, "copy_and_hash_verify", lambda *args, **kwargs: (False, None),
    )

    _db, _ws, result = _run_import(tmp_path, ImportParams(
        sources=[str(card)], destination=str(tmp_path / "archive"),
    ))

    assert (result["copied"], result["failed"]) == (0, 3)
    assert result["timing"]["transferred_bytes"] == card_bytes


def test_an_rsync_landing_that_fails_remote_verification_still_counts_its_bytes(
    tmp_path, monkeypatch,
):
    """rsync sent the file; the remote check then refused it. Its bytes
    crossed the network during the copy phase either way."""
    result, sizes = _remote_import(
        tmp_path, monkeypatch, ["DSC_0001.jpg"], rsync_reports={"DSC_0001.jpg"},
        verify=("DSC_0001.jpg", "checksum differs"),
    )

    assert (result["copied"], result["failed"]) == (0, 1)
    assert result["timing"]["transferred_bytes"] == sizes["DSC_0001.jpg"]


def test_time_spent_paused_inside_a_phase_is_left_out(monkeypatch):
    """A Pause parks the worker at a cancellation probe inside the block.
    Counting it would report an hour's pause as an hour of slow copying."""
    import import_job.timing as timing

    clock = iter([100.0, 4000.0])
    monkeypatch.setattr(timing.time, "monotonic", lambda: next(clock))
    paused = iter([50.0, 3650.0])
    state = _state()
    state.paused_seconds = lambda: next(paused)

    with timed(state, COPY):
        pass

    assert state.phase_seconds == {COPY: pytest.approx(300.0)}


def test_the_import_reads_paused_time_from_its_runner(tmp_path):
    from import_job import ImportParams
    from test_import_job import FakeRunner

    class PausedRunner(FakeRunner):
        """Reports a day of paused time accruing across every phase."""

        def __init__(self):
            super().__init__()
            self.paused = 0.0

        def pause_elapsed_seconds(self, job_id):
            self.paused += 86_400.0
            return self.paused

    _db, _ws, result = _run_import(
        tmp_path,
        ImportParams(sources=[str(_card(tmp_path))], destination=str(tmp_path / "archive")),
        runner=PausedRunner(),
    )

    assert result["copied"] == 3
    assert result["timing"]["seconds"] == {FIND: 0.0, COPY: 0.0, CATALOG: 0.0}


def _remote_import(tmp_path, monkeypatch, names, *, rsync_reports, verify=None):
    """A remote import of ``names`` whose fake rsync lands every file at the
    mount but reports only ``rsync_reports`` as transferred."""
    import move as _move
    from db import Database
    from import_job import ImportParams, run_import_job
    from test_import_job import FakeRunner, _make_job, _remote_archive_for

    remote_archive = _remote_archive_for(tmp_path)

    def fake_rsync(src_path, dest_spec, rsync_flags, total_files, progress_cb,
                   rsync_bin="rsync", extra_args=None, src_specs=None,
                   src_specs_dest_is_dir=True, **kwargs):
        import os
        import shutil

        rel = os.path.relpath(dest_spec.split(":", 1)[1], remote_archive["ssh_base"])
        mount_dir = os.path.join(remote_archive["mount_base"], rel)
        os.makedirs(mount_dir, exist_ok=True)
        for done, source in enumerate(src_specs, start=1):
            shutil.copy2(source, os.path.join(mount_dir, os.path.basename(source)))
            if os.path.basename(source) in rsync_reports and progress_cb:
                progress_cb(done, len(src_specs), os.path.basename(source), "")
        return (0, "", False)

    monkeypatch.setattr(_move, "_run_rsync_streamed", fake_rsync)
    monkeypatch.setattr(_move, "_remote_mkdir_p", lambda remote, path: (True, ""))
    monkeypatch.setattr(_move, "remote_verify_files", lambda *args, **kwargs: verify)
    card = _make_card(tmp_path, [
        (name, datetime(2026, 7, 3, 10, minute, 0), color)
        for minute, (name, color) in enumerate(zip(names, ("red", "green", "blue"), strict=False))
    ])
    db_path = str(tmp_path / "test.db")
    workspace_id = Database(db_path)._active_workspace_id
    result = run_import_job(
        _make_job(), FakeRunner(), db_path, workspace_id,
        ImportParams(
            sources=[str(card)], destination=remote_archive["mount_base"],
            remote_target=remote_archive, verify_by_hash=True,
        ),
    )
    return result, {name: (card / name).stat().st_size for name in names}


def test_an_rsync_import_counts_the_files_rsync_reports_sending(tmp_path, monkeypatch):
    names = ["DSC_0001.jpg", "DSC_0002.jpg"]

    result, sizes = _remote_import(tmp_path, monkeypatch, names, rsync_reports=set(names))

    assert result["copied"] == 2
    assert result["timing"]["transferred_bytes"] == sum(sizes.values())


def test_a_file_rsync_skipped_as_already_there_is_not_counted(tmp_path, monkeypatch):
    """``--ignore-existing`` lets rsync succeed without sending a file
    another writer put at the destination first; it names only what it sent."""
    names = ["DSC_0001.jpg", "DSC_0002.jpg"]

    result, sizes = _remote_import(
        tmp_path, monkeypatch, names, rsync_reports={"DSC_0002.jpg"},
    )

    assert result["copied"] == 2
    assert result["timing"]["transferred_bytes"] == sizes["DSC_0002.jpg"]


@pytest.mark.parametrize(("reported", "on_disk"), [
    ("DSC_0001.jpg", "DSC_0001.jpg"),
    ("foo\\#012bar.jpg", "foo\nbar.jpg"),
    ("caf\\#303\\#251.jpg", "café.jpg"),
    ("back\\#134#012slash.jpg", "back\\#012slash.jpg"),
    ("plain\\backslash.jpg", "plain\\backslash.jpg"),
])
def test_rsyncs_escaped_names_decode_to_the_names_on_disk(reported, on_disk):
    from import_job.transports import _unescape_rsync_name

    assert _unescape_rsync_name(reported) == on_disk


def test_a_file_whose_name_rsync_escapes_is_still_counted():
    """rsync prints a newline in a name as ``\\#012``; the batch is keyed by
    the name on disk."""
    from pathlib import Path

    from import_job.transports import _SentBytes

    state = _state()
    sent = _SentBytes(state, [(Path("/card/foo\nbar.jpg"), "foo\nbar.jpg", None, 1234, 0)])

    sent.named("foo\\#012bar.jpg")
    sent.named("foo\\#012bar.jpg")

    assert state.transferred_bytes == 1234
