from job_contract import failure_event, progress_event


def test_progress_event_has_shared_workflow_shape():
    assert progress_event("Copying", 2, 5, "bird.jpg", folders={"a": 2}) == {
        "phase": "Copying",
        "current": 2,
        "total": 5,
        "current_file": "bird.jpg",
        "folders": {"a": 2},
    }


def test_failure_event_records_phase_and_retryability():
    assert failure_event(
        OSError("volume unavailable"), phase="Copying", retryable=True,
    ) == {
        "message": "volume unavailable",
        "phase": "Copying",
        "retryable": True,
    }
