import http.client
import json
import socket
import threading
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
from testing.waits import synchronization_timeout

from scripts.monitor_vireo_workload import (
    ProcessTreeSampler,
    VireoApiClient,
    _prepare_output_path,
    collect_workload,
    compact_jobs_payload,
    discover_server,
    parse_args,
)


class _FakeClock:
    def __init__(self):
        self.now = 0.0

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


class _SequenceApi:
    def __init__(self, samples):
        self.samples = iter(samples)
        self.authenticated = False

    def authenticate(self):
        self.authenticated = True

    def sample(self):
        return next(self.samples)


class _SequenceSampler:
    def __init__(self, samples, metadata):
        self.samples = iter(samples)
        self._metadata = metadata
        self.primed = False

    def prime(self):
        self.primed = True

    def sample(self):
        return next(self.samples)

    def verify_identity(self):
        pass

    def metadata(self):
        return self._metadata


def _api_sample(
    *, latency, wait_count, wait_seconds, producer_starts, waiter_joins,
    status="running", duration=None, progress=0,
):
    source = "history" if status == "completed" else "active"
    return {
        "status": 200,
        "latency_seconds": latency,
        "resource_budget": {
            "cpu": {"capacity": 12, "allocated": 8, "available": 4},
            "wait_count": wait_count,
            "wait_seconds": wait_seconds,
            "waiters": 1 if status == "running" else 0,
        },
        "workload_metrics": {
            "embedding_cache": {
                "cache_hits": 4,
                "cache_misses": 2,
                "producer_starts": producer_starts,
                "producer_publications": producer_starts,
                "producer_failures": 0,
                "waiter_joins": waiter_joins,
                "single_flight_violations": 0,
            }
        },
        "jobs": [{
            "id": "pipeline-1",
            "type": "pipeline",
            "status": status,
            "source": source,
            "workspace_id": 7,
            "started_at": "2026-08-17T12:00:00+00:00",
            "finished_at": (
                "2026-08-17T12:00:42+00:00" if status == "completed" else None
            ),
            "duration": duration,
            "resource_wait_seconds": wait_seconds,
            "resource_wait_count": wait_count,
            "progress": {"current": progress, "total": 10},
        }],
    }


def test_collect_workload_builds_deltas_targets_and_job_summary():
    clock = _FakeClock()
    api = _SequenceApi([
        _api_sample(
            latency=0.001, wait_count=10, wait_seconds=20.0,
            producer_starts=5, waiter_joins=2, progress=0,
        ),
        _api_sample(
            latency=0.010, wait_count=11, wait_seconds=21.0,
            producer_starts=6, waiter_joins=3, progress=5,
        ),
        _api_sample(
            latency=0.020, wait_count=12, wait_seconds=22.5,
            producer_starts=6, waiter_joins=3, status="completed",
            duration=42.0, progress=10,
        ),
    ])
    process = _SequenceSampler(
        [
            {"cpu_percent": 800.0, "rss_bytes": 100, "executable_exists": True},
            {"cpu_percent": 900.0, "rss_bytes": 120, "executable_exists": True},
        ],
        {
            "pid": 123,
            "initial_rss_bytes": 80,
            "cpu_accounting_complete": False,
            "executable_exists": True,
        },
    )
    system = _SequenceSampler(
        [
            {"cpu_idle_percent": 30.0},
            {"cpu_idle_percent": 20.0},
        ],
        {"logical_cpu_count": 16},
    )

    ready_states = []
    report = collect_workload(
        duration=2.0,
        interval=1.0,
        api_client=api,
        process_sampler=process,
        system_sampler=system,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
        on_ready=lambda: ready_states.append(
            (api.authenticated, process.primed, system.primed)
        ),
    )

    assert ready_states == [(True, True, True)]
    assert api.authenticated
    assert process.primed and system.primed
    assert [sample["elapsed_seconds"] for sample in report["samples"]] == [1.0, 2.0]
    summary = report["summary"]
    assert summary["jobs_api_latency_seconds"]["p95"] == 0.02
    assert summary["system_cpu_idle_percent"]["p05"] == 20.0
    assert summary["vireo_process_tree_rss_bytes"]["growth"] == 40
    assert summary["vireo_process_tree_cpu_percent"]["accounting_complete"] is False
    assert summary["resource_wait_delta"] == {
        "wait_count": 2,
        "wait_seconds": 2.5,
    }
    assert summary["embedding_cache_delta"]["producer_starts"] == 1
    assert summary["embedding_cache_delta"]["waiter_joins"] == 1
    assert summary["targets"] == {
        "jobs_api_p95_below_500ms": True,
        "system_idle_cpu_p05_at_least_10_percent": True,
        "no_embedding_single_flight_violations": True,
        "vireo_executable_present_throughout": True,
    }
    assert summary["scenario"] == {
        "job_observation_complete": True,
        "observed_job_count": 1,
        "terminal_job_count": 1,
        "all_observed_jobs_terminal": True,
        "workload_makespan_seconds": 42.0,
    }
    assert summary["jobs"][0]["last_status"] == "completed"
    assert summary["jobs"][0]["duration"] == 42.0
    assert summary["jobs"][0]["observed_resource_wait_seconds"] == 2.5
    assert summary["jobs"][0]["resource_wait_count"] == 2


@pytest.mark.parametrize("argument", ["--duration", "--interval", "--timeout"])
def test_sampling_cli_rejects_non_finite_values(argument):
    with pytest.raises(SystemExit):
        parse_args([argument, "nan"])


def test_output_path_validation_rejects_directory(tmp_path):
    with pytest.raises(RuntimeError, match="not a file"):
        _prepare_output_path(tmp_path)


def test_slow_poll_skips_missed_slots_instead_of_firing_back_to_back():
    """A single poll that takes longer than `--interval` (a transient
    slow request, for example) previously left `next_sample` in the past
    and caused back-to-back catch-up polls that contaminate CPU/latency
    distributions.  Missed slots must be skipped instead so the sampling
    cadence stays honest.
    """
    clock = _FakeClock()

    class _SlowApi:
        def __init__(self):
            self.calls = 0

        def authenticate(self):
            pass

        def sample(self):
            self.calls += 1
            # Loop iteration #1 (call #2 overall) takes 5s — more than
            # the 2s interval.
            if self.calls == 2:
                clock.sleep(5.0)
            return _api_sample(
                latency=0.001, wait_count=0, wait_seconds=0.0,
                producer_starts=0, waiter_joins=0,
            )

    api = _SlowApi()
    n = 8
    process = _SequenceSampler(
        [{"cpu_percent": 100.0, "rss_bytes": 100, "executable_exists": True}
         for _ in range(n)],
        {"pid": 123, "executable_exists": True},
    )
    system = _SequenceSampler(
        [{"cpu_idle_percent": 50.0} for _ in range(n)],
        {"logical_cpu_count": 16},
    )

    report = collect_workload(
        duration=10.0,
        interval=2.0,
        api_client=api,
        process_sampler=process,
        system_sampler=system,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
    )

    elapsed = [sample["elapsed_seconds"] for sample in report["samples"]]
    # No two adjacent samples may collapse into a back-to-back burst.
    for prev, cur in zip(elapsed, elapsed[1:], strict=False):
        assert cur - prev >= 2.0, f"back-to-back polls at {prev}s → {cur}s"
    # We expected samples at approximately t=2 (fast), then t=8 and t=10
    # after skipping the missed slots at 4 and 6 — never at t=7 (immediate
    # after the slow poll ended).
    assert elapsed == [2.0, 8.0, 10.0]


def test_slow_poll_finishing_on_cadence_boundary_skips_that_slot():
    """A poll ending exactly on a later cadence boundary must not cause an
    immediate second sample at that same timestamp.
    """
    clock = _FakeClock()

    class _BoundaryApi:
        def __init__(self):
            self.calls = 0

        def authenticate(self):
            pass

        def sample(self):
            self.calls += 1
            if self.calls == 2:
                clock.sleep(4.0)
            return _api_sample(
                latency=0.001, wait_count=0, wait_seconds=0.0,
                producer_starts=0, waiter_joins=0,
            )

    n = 8
    report = collect_workload(
        duration=10.0,
        interval=2.0,
        api_client=_BoundaryApi(),
        process_sampler=_SequenceSampler(
            [{"cpu_percent": 100.0, "rss_bytes": 100,
              "executable_exists": True} for _ in range(n)],
            {"pid": 123, "executable_exists": True},
        ),
        system_sampler=_SequenceSampler(
            [{"cpu_idle_percent": 50.0} for _ in range(n)],
            {"logical_cpu_count": 16},
        ),
        monotonic=clock.monotonic,
        sleep=clock.sleep,
    )

    assert [sample["elapsed_seconds"] for sample in report["samples"]] == [
        2.0, 8.0, 10.0,
    ]


def test_executable_target_is_none_when_presence_is_unknown():
    """When psutil can't read `Process.exe()` (e.g. `AccessDenied`),
    every sample reports `executable_exists=None`.  The old predicate
    treated None as success and made the trustworthiness gate falsely
    pass; the gate should now surface as None ("unknown") in that case.
    """
    clock = _FakeClock()
    api = _SequenceApi([
        _api_sample(
            latency=0.001, wait_count=0, wait_seconds=0.0,
            producer_starts=0, waiter_joins=0,
        ),
        _api_sample(
            latency=0.001, wait_count=0, wait_seconds=0.0,
            producer_starts=0, waiter_joins=0,
        ),
    ])
    process = _SequenceSampler(
        [{"cpu_percent": 100.0, "rss_bytes": 100, "executable_exists": None}],
        {"pid": 123, "executable_exists": None},
    )
    system = _SequenceSampler(
        [{"cpu_idle_percent": 50.0}],
        {"logical_cpu_count": 16},
    )

    report = collect_workload(
        duration=1.0,
        interval=1.0,
        api_client=api,
        process_sampler=process,
        system_sampler=system,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
    )

    assert report["summary"]["targets"]["vireo_executable_present_throughout"] is None


def test_executable_target_is_false_when_presence_verified_missing():
    clock = _FakeClock()
    api = _SequenceApi([
        _api_sample(
            latency=0.001, wait_count=0, wait_seconds=0.0,
            producer_starts=0, waiter_joins=0,
        ),
        _api_sample(
            latency=0.001, wait_count=0, wait_seconds=0.0,
            producer_starts=0, waiter_joins=0,
        ),
    ])
    process = _SequenceSampler(
        [{"cpu_percent": 100.0, "rss_bytes": 100, "executable_exists": False}],
        {"pid": 123, "executable_exists": False},
    )
    system = _SequenceSampler(
        [{"cpu_idle_percent": 50.0}],
        {"logical_cpu_count": 16},
    )

    report = collect_workload(
        duration=1.0,
        interval=1.0,
        api_client=api,
        process_sampler=process,
        system_sampler=system,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
    )

    assert report["summary"]["targets"]["vireo_executable_present_throughout"] is False


def test_executable_target_includes_final_metadata_check():
    clock = _FakeClock()
    api = _SequenceApi([
        _api_sample(
            latency=0.001, wait_count=0, wait_seconds=0.0,
            producer_starts=0, waiter_joins=0,
        ),
        _api_sample(
            latency=0.001, wait_count=0, wait_seconds=0.0,
            producer_starts=0, waiter_joins=0,
        ),
    ])
    process = _SequenceSampler(
        [{"cpu_percent": 100.0, "rss_bytes": 100, "executable_exists": True}],
        {
            "pid": 123,
            "initial_executable_exists": True,
            "executable_exists": False,
        },
    )
    system = _SequenceSampler(
        [{"cpu_idle_percent": 50.0}],
        {"logical_cpu_count": 16},
    )

    report = collect_workload(
        duration=1.0,
        interval=1.0,
        api_client=api,
        process_sampler=process,
        system_sampler=system,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
    )

    assert report["process"]["executable_exists"] is False
    assert report["summary"]["targets"]["vireo_executable_present_throughout"] is False


def test_job_summary_tracks_history_only_jobs_and_ignores_baseline_history():
    """A short job (e.g. a cached embedding request) may start and finish
    between two polling intervals, appearing only in `history`.  Such jobs
    must be tracked so the scenario reflects the full workload, but jobs
    already terminal at baseline predate the monitoring window and must
    not appear in the per-job outcomes.
    """
    clock = _FakeClock()
    pre_existing = {
        "id": "pre-existing",
        "type": "pipeline",
        "status": "completed",
        "source": "history",
        "workspace_id": 1,
        "started_at": "2026-08-17T11:00:00+00:00",
        "finished_at": "2026-08-17T11:00:10+00:00",
        "duration": 10.0,
    }
    short_lived = {
        "id": "short-lived",
        "type": "embedding_prep",
        "status": "completed",
        "source": "history",
        "workspace_id": 1,
        "started_at": "2026-08-17T12:00:00.500+00:00",
        "finished_at": "2026-08-17T12:00:00.800+00:00",
        "duration": 0.3,
        "resource_wait_seconds": 1.25,
    }
    empty_metrics = {"embedding_cache": {}}
    baseline_payload = {
        "status": 200,
        "latency_seconds": 0.001,
        "resource_budget": {"waiters": 0},
        "workload_metrics": empty_metrics,
        "jobs": [pre_existing],
    }
    poll_payload = {
        "status": 200,
        "latency_seconds": 0.002,
        "resource_budget": {"waiters": 0},
        "workload_metrics": empty_metrics,
        "jobs": [pre_existing, short_lived],
    }
    api = _SequenceApi([baseline_payload, poll_payload])
    process = _SequenceSampler(
        [{"cpu_percent": 100.0, "rss_bytes": 100, "executable_exists": True}],
        {"pid": 123, "executable_exists": True},
    )
    system = _SequenceSampler(
        [{"cpu_idle_percent": 50.0}],
        {"logical_cpu_count": 16},
    )

    report = collect_workload(
        duration=1.0,
        interval=1.0,
        api_client=api,
        process_sampler=process,
        system_sampler=system,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
    )

    job_ids = [job["id"] for job in report["summary"]["jobs"]]
    assert job_ids == ["short-lived"]
    scenario = report["summary"]["scenario"]
    assert scenario["observed_job_count"] == 1
    assert scenario["terminal_job_count"] == 1
    assert scenario["all_observed_jobs_terminal"] is True
    # Makespan is derived from the short-lived job's own timestamps.
    assert scenario["workload_makespan_seconds"] == 0.3
    # The job was absent at baseline, so wait already accrued when it first
    # appears still belongs to the monitored workload.
    assert report["summary"]["jobs"][0]["observed_resource_wait_seconds"] == 1.25


def test_queued_job_refreshes_started_at_for_scenario_makespan():
    clock = _FakeClock()
    empty = {
        "status": 200,
        "latency_seconds": 0.001,
        "resource_budget": {"waiters": 0},
        "workload_metrics": {"embedding_cache": {}},
        "jobs": [],
    }
    queued = {
        **empty,
        "jobs": [{
            "id": "queued-job",
            "type": "pipeline",
            "status": "queued",
            "source": "active",
            "started_at": None,
        }],
    }
    completed = {
        **empty,
        "jobs": [{
            "id": "queued-job",
            "type": "pipeline",
            "status": "completed",
            "source": "history",
            "started_at": "2026-08-17T12:00:00+00:00",
            "finished_at": "2026-08-17T12:00:10+00:00",
            "duration": 10.0,
        }],
    }
    api = _SequenceApi([empty, queued, completed])
    process = _SequenceSampler(
        [
            {"cpu_percent": 100.0, "rss_bytes": 100, "executable_exists": True},
            {"cpu_percent": 100.0, "rss_bytes": 100, "executable_exists": True},
        ],
        {"pid": 123, "executable_exists": True},
    )
    system = _SequenceSampler(
        [{"cpu_idle_percent": 50.0}, {"cpu_idle_percent": 50.0}],
        {"logical_cpu_count": 16},
    )

    report = collect_workload(
        duration=2.0,
        interval=1.0,
        api_client=api,
        process_sampler=process,
        system_sampler=system,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
    )

    assert report["summary"]["jobs"][0]["started_at"] == (
        "2026-08-17T12:00:00+00:00"
    )
    assert report["summary"]["scenario"]["workload_makespan_seconds"] == 10.0


def test_single_flight_target_is_unknown_when_any_api_sample_fails():
    """`embedding_delta` is computed from the last successful API poll,
    so any single-flight violation happening after the last success is
    invisible.  When there were API failures the invariant must be
    surfaced as unknown (None), not as verified True.
    """
    clock = _FakeClock()
    api = _SequenceApi([
        _api_sample(  # baseline
            latency=0.001, wait_count=0, wait_seconds=0.0,
            producer_starts=0, waiter_joins=0,
        ),
        _api_sample(
            latency=0.010, wait_count=0, wait_seconds=0.0,
            producer_starts=0, waiter_joins=0,
        ),
        {"status": None, "latency_seconds": 5.0, "error": "timeout"},
    ])
    process = _SequenceSampler(
        [
            {"cpu_percent": 100.0, "rss_bytes": 100, "executable_exists": True},
            {"cpu_percent": 100.0, "rss_bytes": 100, "executable_exists": True},
        ],
        {"pid": 123, "executable_exists": True},
    )
    system = _SequenceSampler(
        [{"cpu_idle_percent": 50.0}, {"cpu_idle_percent": 50.0}],
        {"logical_cpu_count": 16},
    )

    report = collect_workload(
        duration=2.0,
        interval=1.0,
        api_client=api,
        process_sampler=process,
        system_sampler=system,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
    )

    targets = report["summary"]["targets"]
    assert report["summary"]["api_failure_count"] == 1
    assert report["summary"]["scenario"]["job_observation_complete"] is False
    assert targets["no_embedding_single_flight_violations"] is None


def test_malformed_200_jobs_response_counts_as_api_failure():
    clock = _FakeClock()
    api = _SequenceApi([
        _api_sample(
            latency=0.001, wait_count=0, wait_seconds=0.0,
            producer_starts=0, waiter_joins=0,
        ),
        {
            "status": 200,
            "latency_seconds": 0.01,
            "error": "invalid JSON: malformed response",
        },
    ])
    process = _SequenceSampler(
        [{"cpu_percent": 1500.0, "rss_bytes": 100, "executable_exists": True}],
        {"pid": 123, "executable_exists": True},
    )
    system = _SequenceSampler(
        [{"cpu_idle_percent": 50.0}],
        {"logical_cpu_count": 16},
    )

    report = collect_workload(
        duration=1.0,
        interval=1.0,
        api_client=api,
        process_sampler=process,
        system_sampler=system,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
    )

    assert report["summary"]["api_failure_count"] == 1
    assert report["summary"]["cpu_capacity_burst_samples"] == 1
    assert report["summary"]["jobs_api_latency_seconds"] is None
    assert report["summary"]["targets"]["jobs_api_p95_below_500ms"] is False


def test_interrupt_before_poll_marks_api_targets_unknown():
    clock = _FakeClock()
    api = _SequenceApi([
        _api_sample(
            latency=0.001, wait_count=0, wait_seconds=0.0,
            producer_starts=0, waiter_joins=0,
        ),
    ])
    process = _SequenceSampler([], {"pid": 123, "executable_exists": True})
    system = _SequenceSampler([], {"logical_cpu_count": 16})

    def interrupted_sleep(_seconds):
        raise KeyboardInterrupt

    report = collect_workload(
        duration=1.0,
        interval=1.0,
        api_client=api,
        process_sampler=process,
        system_sampler=system,
        monotonic=clock.monotonic,
        sleep=interrupted_sleep,
    )

    assert report["interrupted"] is True
    assert report["samples"] == []
    assert report["summary"]["scenario"]["job_observation_complete"] is False
    assert report["summary"]["targets"]["jobs_api_p95_below_500ms"] is None
    assert report["summary"]["targets"][
        "no_embedding_single_flight_violations"
    ] is None


def test_malformed_200_jobs_baseline_is_rejected():
    api = _SequenceApi([{
        "status": 200,
        "latency_seconds": 0.001,
        "error": "invalid JSON: malformed response",
    }])

    with pytest.raises(RuntimeError, match="invalid JSON"):
        collect_workload(
            duration=1.0,
            interval=1.0,
            api_client=api,
            process_sampler=None,
            system_sampler=None,
        )


def test_process_identity_is_rechecked_after_terminal_api_poll():
    clock = _FakeClock()
    exited = False

    class _ExitDuringPollApi:
        def __init__(self):
            self.calls = 0

        def authenticate(self):
            pass

        def sample(self):
            nonlocal exited
            self.calls += 1
            if self.calls == 3:
                exited = True
            return _api_sample(
                latency=0.001, wait_count=0, wait_seconds=0.0,
                producer_starts=0, waiter_joins=0,
            )

    class _IdentitySampler(_SequenceSampler):
        def verify_identity(self):
            if exited:
                raise RuntimeError("monitored Vireo PID 123 exited")

    process = _IdentitySampler(
        [
            {"cpu_percent": 100.0, "rss_bytes": 100, "executable_exists": True},
            {"cpu_percent": 200.0, "rss_bytes": 200, "executable_exists": True},
        ],
        {"pid": 123, "executable_exists": True},
    )
    system = _SequenceSampler(
        [{"cpu_idle_percent": 50.0}, {"cpu_idle_percent": 25.0}],
        {"logical_cpu_count": 16},
    )

    report = collect_workload(
        duration=2.0,
        interval=1.0,
        api_client=_ExitDuringPollApi(),
        process_sampler=process,
        system_sampler=system,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
    )

    assert report["process_exit_error"] == "monitored Vireo PID 123 exited"
    assert len(report["samples"]) == 1
    assert report["samples"][0]["elapsed_seconds"] == 1.0
    assert report["summary"]["targets"][
        "vireo_executable_present_throughout"
    ] is False


def test_process_exit_before_resource_sample_preserves_prior_samples():
    clock = _FakeClock()

    class _ExitBeforeSecondSample(_SequenceSampler):
        def sample(self):
            if clock.monotonic() >= 2.0:
                raise RuntimeError("monitored Vireo PID 123 exited")
            return super().sample()

        def metadata_unverified(self):
            return {
                "pid": 123,
                "identity_verified": False,
                "cpu_accounting_complete": True,
            }

    report = collect_workload(
        duration=2.0,
        interval=1.0,
        api_client=_SequenceApi([
            _api_sample(
                latency=0.001, wait_count=0, wait_seconds=0.0,
                producer_starts=0, waiter_joins=0,
            ),
            _api_sample(
                latency=0.001, wait_count=0, wait_seconds=0.0,
                producer_starts=0, waiter_joins=0,
            ),
        ]),
        process_sampler=_ExitBeforeSecondSample(
            [{
                "cpu_percent": 100.0,
                "rss_bytes": 100,
                "executable_exists": True,
                "cpu_accounting_complete": True,
            }],
            {"pid": 123, "executable_exists": True},
        ),
        system_sampler=_SequenceSampler(
            [{"cpu_idle_percent": 50.0}],
            {"logical_cpu_count": 16},
        ),
        monotonic=clock.monotonic,
        sleep=clock.sleep,
    )

    assert report["process_exit_error"] == "monitored Vireo PID 123 exited"
    assert len(report["samples"]) == 1
    assert report["summary"]["scenario"]["job_observation_complete"] is False
    assert report["summary"]["targets"]["jobs_api_p95_below_500ms"] is None
    assert report["summary"]["targets"][
        "no_embedding_single_flight_violations"
    ] is None
    assert report["summary"]["vireo_process_tree_cpu_percent"][
        "accounting_complete"
    ] is False
    assert report["summary"]["targets"][
        "system_idle_cpu_p05_at_least_10_percent"
    ] is None


def test_terminal_api_overrun_marks_resource_interval_incomplete():
    clock = _FakeClock()

    class _OverrunApi:
        def __init__(self):
            self.calls = 0

        def authenticate(self):
            pass

        def sample(self):
            self.calls += 1
            if self.calls == 2:
                clock.sleep(2.0)
            return _api_sample(
                latency=2.0, wait_count=0, wait_seconds=0.0,
                producer_starts=0, waiter_joins=0,
            )

    report = collect_workload(
        duration=2.0,
        interval=1.0,
        api_client=_OverrunApi(),
        process_sampler=_SequenceSampler(
            [{
                "cpu_percent": 100.0,
                "rss_bytes": 100,
                "executable_exists": True,
                "cpu_accounting_complete": True,
            }],
            {
                "pid": 123,
                "executable_exists": True,
                "cpu_accounting_complete": True,
            },
        ),
        system_sampler=_SequenceSampler(
            [{"cpu_idle_percent": 50.0}],
            {"logical_cpu_count": 16},
        ),
        monotonic=clock.monotonic,
        sleep=clock.sleep,
    )

    assert report["samples"][0]["resource_interval_complete"] is False
    assert report["summary"]["incomplete_resource_interval_count"] == 1
    assert report["summary"]["vireo_process_tree_cpu_percent"][
        "accounting_complete"
    ] is False
    assert report["summary"]["targets"][
        "system_idle_cpu_p05_at_least_10_percent"
    ] is None


def test_interrupted_api_poll_retains_resource_sample_and_marks_failure():
    clock = _FakeClock()

    class _InterruptedApi:
        def __init__(self):
            self.calls = 0

        def authenticate(self):
            pass

        def sample(self):
            self.calls += 1
            if self.calls == 2:
                raise KeyboardInterrupt
            return _api_sample(
                latency=0.001, wait_count=0, wait_seconds=0.0,
                producer_starts=0, waiter_joins=0,
            )

    process = _SequenceSampler(
        [{
            "cpu_percent": 100.0,
            "rss_bytes": 100,
            "executable_exists": True,
            "cpu_accounting_complete": True,
        }],
        {
            "pid": 123,
            "executable_exists": True,
            "cpu_accounting_complete": True,
        },
    )
    system = _SequenceSampler(
        [{"cpu_idle_percent": 50.0}],
        {"logical_cpu_count": 16},
    )

    report = collect_workload(
        duration=1.0,
        interval=1.0,
        api_client=_InterruptedApi(),
        process_sampler=process,
        system_sampler=system,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
    )

    assert report["interrupted"] is True
    assert len(report["samples"]) == 1
    assert report["samples"][0]["process"]["cpu_percent"] == 100.0
    assert report["samples"][0]["api"]["error"].startswith("interrupted")
    assert report["summary"]["api_failure_count"] == 1
    assert report["summary"]["scenario"]["job_observation_complete"] is False
    assert report["summary"]["targets"]["jobs_api_p95_below_500ms"] is None
    assert report["summary"]["vireo_process_tree_cpu_percent"][
        "accounting_complete"
    ] is False
    assert report["summary"]["targets"][
        "system_idle_cpu_p05_at_least_10_percent"
    ] is None


def test_late_process_exit_preserves_report_and_fails_trust_gate():
    clock = _FakeClock()
    api = _SequenceApi([
        _api_sample(
            latency=0.001, wait_count=0, wait_seconds=0.0,
            producer_starts=0, waiter_joins=0,
        ),
        _api_sample(
            latency=0.001, wait_count=0, wait_seconds=0.0,
            producer_starts=0, waiter_joins=0,
        ),
    ])

    class _LateExitSampler(_SequenceSampler):
        def metadata(self):
            raise RuntimeError("monitored Vireo PID 123 exited")

        def metadata_unverified(self):
            return {
                "pid": 123,
                "identity_verified": False,
                "executable_exists": True,
            }

    process = _LateExitSampler(
        [{"cpu_percent": 100.0, "rss_bytes": 100, "executable_exists": True}],
        {},
    )
    system = _SequenceSampler(
        [{"cpu_idle_percent": 50.0}],
        {"logical_cpu_count": 16},
    )

    report = collect_workload(
        duration=1.0,
        interval=1.0,
        api_client=api,
        process_sampler=process,
        system_sampler=system,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
    )

    assert report["process_exit_error"] == "monitored Vireo PID 123 exited"
    assert len(report["samples"]) == 1
    assert report["summary"]["scenario"]["job_observation_complete"] is False
    assert report["summary"]["targets"]["jobs_api_p95_below_500ms"] is None
    assert report["summary"]["targets"][
        "no_embedding_single_flight_violations"
    ] is None
    assert report["summary"]["targets"]["vireo_executable_present_throughout"] is False


def test_latency_target_fails_when_api_samples_fail():
    """A run with one fast success and one long timeout used to report
    ``jobs_api_p95_below_500ms: true`` because failed requests were excluded
    from the percentile — the responsiveness gate must not pass when any
    API call failed.
    """
    clock = _FakeClock()
    api = _SequenceApi([
        _api_sample(  # baseline
            latency=0.001, wait_count=0, wait_seconds=0.0,
            producer_starts=0, waiter_joins=0,
        ),
        _api_sample(
            latency=0.010, wait_count=0, wait_seconds=0.0,
            producer_starts=0, waiter_joins=0,
        ),
        {"status": None, "latency_seconds": 5.0, "error": "timeout"},
    ])
    process = _SequenceSampler(
        [
            {"cpu_percent": 100.0, "rss_bytes": 100, "executable_exists": True},
            {"cpu_percent": 100.0, "rss_bytes": 100, "executable_exists": True},
        ],
        {"pid": 123, "executable_exists": True},
    )
    system = _SequenceSampler(
        [{"cpu_idle_percent": 50.0}, {"cpu_idle_percent": 50.0}],
        {"logical_cpu_count": 16},
    )

    report = collect_workload(
        duration=2.0,
        interval=1.0,
        api_client=api,
        process_sampler=process,
        system_sampler=system,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
    )

    summary = report["summary"]
    assert summary["api_failure_count"] == 1
    # Only the successful 10ms response is in the percentile — p95 is well
    # under 500ms — but the target still has to fail because of the timeout.
    assert summary["jobs_api_latency_seconds"]["p95"] == 0.01
    assert summary["targets"]["jobs_api_p95_below_500ms"] is False


def test_compact_jobs_payload_removes_paths_configs_results_and_filenames():
    compact = compact_jobs_payload({
        "active": [{
            "id": "pipeline-1",
            "type": "pipeline",
            "status": "running",
            "config": {"source": "/private/photos"},
            "result": {"path": "/private/result"},
            "progress": {
                "current": 2,
                "total": 10,
                "phase": "Detecting",
                "current_file": "private-name.nef",
            },
            "steps": [{
                "id": "detect",
                "label": "Detect",
                "status": "running",
                "current_file": "private-name.nef",
                "progress": {"current": 2, "total": 10},
            }],
        }],
        "history": [],
        "workspace_names": {"7": "Private workspace"},
        "resource_budget": {"waiters": 0},
    })

    encoded = json.dumps(compact)
    assert "/private" not in encoded
    assert "private-name" not in encoded
    assert "Private workspace" not in encoded
    # `phase` is deliberately dropped even when its content looks safe.
    assert compact["jobs"][0]["progress"] == {
        "current": 2,
        "total": 10,
    }


def test_compact_jobs_payload_retains_terminal_ephemeral_jobs_once():
    ephemeral = {
        "id": "walk-1",
        "type": "new_images_walk",
        "status": "completed",
        "duration": 0.2,
    }
    persisted = {
        "id": "pipeline-1",
        "type": "pipeline",
        "status": "completed",
        "duration": 10.0,
    }

    compact = compact_jobs_payload({
        # A persisted job may briefly be present in both lists. An ephemeral
        # job has no history row, so its terminal active record must survive.
        "active": [ephemeral, persisted],
        "history": [persisted],
    })

    assert [(job["id"], job["source"]) for job in compact["jobs"]] == [
        ("walk-1", "active"),
        ("pipeline-1", "history"),
    ]


def test_full_history_window_without_prior_overlap_marks_observation_incomplete():
    history = [
        {
            "id": f"completed-{index}",
            "type": "pipeline",
            "status": "completed",
            "started_at": f"2026-08-17T12:00:{index:02d}+00:00",
            "finished_at": f"2026-08-17T12:00:{index + 1:02d}+00:00",
        }
        for index in range(10)
    ]
    compact = compact_jobs_payload({"active": [], "history": history})

    assert compact["history_window_full"] is True

    baseline = {
        "status": 200,
        "latency_seconds": 0.001,
        "jobs": [],
        "history_job_ids": [],
        "history_window_full": False,
    }
    poll = {"status": 200, "latency_seconds": 0.001, **compact}
    clock = _FakeClock()
    report = collect_workload(
        duration=1.0,
        interval=1.0,
        api_client=_SequenceApi([baseline, poll]),
        process_sampler=_SequenceSampler(
            [{"cpu_percent": 100.0, "rss_bytes": 100, "executable_exists": True}],
            {"pid": 123, "executable_exists": True},
        ),
        system_sampler=_SequenceSampler(
            [{"cpu_idle_percent": 50.0}],
            {"logical_cpu_count": 16},
        ),
        monotonic=clock.monotonic,
        sleep=clock.sleep,
    )

    scenario = report["summary"]["scenario"]
    assert scenario["job_observation_complete"] is False
    assert scenario["observed_job_count"] == 10
    assert scenario["workload_makespan_seconds"] is None


def test_full_history_window_with_prior_overlap_remains_complete():
    history = [
        {
            "id": f"completed-{index}",
            "type": "pipeline",
            "status": "completed",
            "started_at": f"2026-08-17T12:00:{index:02d}+00:00",
            "finished_at": f"2026-08-17T12:00:{index + 1:02d}+00:00",
        }
        for index in range(10)
    ]
    baseline_compact = compact_jobs_payload({"active": [], "history": history})
    replacement = {
        "id": "completed-new",
        "type": "pipeline",
        "status": "completed",
        "started_at": "2026-08-17T12:01:00+00:00",
        "finished_at": "2026-08-17T12:01:01+00:00",
    }
    poll_compact = compact_jobs_payload({
        "active": [],
        "history": [replacement, *history[:9]],
    })
    clock = _FakeClock()
    report = collect_workload(
        duration=1.0,
        interval=1.0,
        api_client=_SequenceApi([
            {"status": 200, "latency_seconds": 0.001, **baseline_compact},
            {"status": 200, "latency_seconds": 0.001, **poll_compact},
        ]),
        process_sampler=_SequenceSampler(
            [{"cpu_percent": 100.0, "rss_bytes": 100, "executable_exists": True}],
            {"pid": 123, "executable_exists": True},
        ),
        system_sampler=_SequenceSampler(
            [{"cpu_idle_percent": 50.0}],
            {"logical_cpu_count": 16},
        ),
        monotonic=clock.monotonic,
        sleep=clock.sleep,
    )

    scenario = report["summary"]["scenario"]
    assert scenario["job_observation_complete"] is True
    assert scenario["observed_job_count"] == 1
    assert scenario["workload_makespan_seconds"] == 1.0


def test_compact_jobs_payload_omits_user_derived_step_labels():
    """For local-folder/sync/discard jobs, `step.label` is built from
    `root_names[root_id]` (e.g. "Copy <folder> locally").  Copying it
    verbatim would leak user folder names into every sanitized sample.
    """
    compact = compact_jobs_payload({
        "active": [{
            "id": "local-folder-1",
            "type": "local_folder_copy",
            "status": "running",
            "steps": [{
                "id": "copy",
                "label": "Copy My Vacation 2024 locally",
                "status": "running",
                "progress": {"current": 3, "total": 10},
            }],
        }],
        "history": [],
        "resource_budget": {"waiters": 0},
    })

    step = compact["jobs"][0]["running_steps"][0]
    assert step == {
        "id": "copy",
        "status": "running",
        "progress": {"current": 3, "total": 10},
    }
    assert "My Vacation 2024" not in json.dumps(compact)


def test_compact_progress_drops_free_form_phase_entirely():
    """Phase text is a mix of generic labels ("Importing photos"), paths
    ("Scanning root 1 of 2: /private/photos"), filenames ("Downloading 1/3:
    capture.nef..."), and user folder names ("Copying My Vacation locally").
    No regex can reliably split the safe from the unsafe, so `_compact_progress`
    omits the phase field entirely and reports only numeric progress plus
    `stage_id`.
    """
    unsafe_phases = [
        "Scanning root 1 of 2: /private/photos",
        "Scanning root 1 of 2: /家族/写真",
        "Scanning C:\\Program Files\\Vireo\\lib",
        "Downloading 1/3: capture.nef...",
        "Retrying secret.raw in 3s ...",
        "Copying My Vacation 2024 locally",
        "Syncing Птицы to remote",
        "Importing photos",  # generic — still dropped
    ]
    compact = compact_jobs_payload({
        "active": [
            {
                "id": f"job-{i}",
                "type": "scan",
                "status": "running",
                "progress": {
                    "current": i,
                    "total": 10,
                    "phase": phase,
                    "stage_id": "walk",
                },
            }
            for i, phase in enumerate(unsafe_phases)
        ],
        "history": [],
        "resource_budget": {"waiters": 0},
    })

    encoded = json.dumps(compact, ensure_ascii=False)
    for fragment in (
        "/private", "家族", "写真", "Program Files", "capture.nef",
        "secret.raw", "My Vacation", "Птицы", "Importing photos",
    ):
        assert fragment not in encoded, f"phase text {fragment!r} leaked into report"
    for job in compact["jobs"]:
        assert "phase" not in job["progress"]
        assert job["progress"]["stage_id"] == "walk"
        assert "current" in job["progress"] and "total" in job["progress"]


def test_api_client_establishes_browser_cookie_before_reading_jobs():
    host_headers = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            host_headers.append(self.headers.get("Host"))
            if self.path == "/welcome?force=1":
                self.send_response(200)
                self.send_header("Set-Cookie", "vireo_session=test; Path=/; HttpOnly")
                self.end_headers()
                self.wfile.write(b"<html>Vireo</html>")
                return
            if self.path == "/api/jobs" and "vireo_session=test" in self.headers.get("Cookie", ""):
                body = json.dumps({
                    "active": [],
                    "history": [],
                    "resource_budget": {"waiters": 0},
                }).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            self.send_response(401)
            self.end_headers()

        def log_message(self, _format, *_args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    try:
        client = VireoApiClient(
            f"http://127.0.0.1:{server.server_port}",
            host_header="localhost",
        )
        client.authenticate()
        sample = client.sample()
    finally:
        server.shutdown()
        thread.join(timeout=synchronization_timeout(2))
        server.server_close()

    assert sample["status"] == 200
    assert sample["resource_budget"] == {"waiters": 0}
    assert host_headers == ["localhost", "localhost"]


def test_api_client_disables_environment_proxies(monkeypatch):
    monkeypatch.setattr(
        urllib.request,
        "getproxies",
        lambda: {"http": "http://proxy.example:8080"},
    )

    client = VireoApiClient("http://127.0.0.1:50222")

    assert not any(
        isinstance(handler, urllib.request.ProxyHandler)
        for handler in client.opener.handlers
    )


def test_api_client_returns_failure_for_truncated_http_body():
    class _TruncatedResponse:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            pass

        def read(self):
            raise http.client.IncompleteRead(b'{"active":', 100)

    class _Opener:
        def open(self, _url, timeout):
            assert timeout == 5.0
            return _TruncatedResponse()

    result = VireoApiClient(
        "http://127.0.0.1:50222",
        opener=_Opener(),
    )._request("/api/jobs")

    assert result["status"] is None
    assert "IncompleteRead" in result["error"]


def test_process_sampler_counts_cpu_for_newly_discovered_child():
    clock = _FakeClock()

    class _CpuProcess:
        def __init__(
            self, pid, created_at, cpu_seconds, rss, reaped_cpu_seconds=0.0,
        ):
            self.pid = pid
            self.created_at = created_at
            self.cpu_seconds = cpu_seconds
            self.rss = rss
            self.reaped_cpu_seconds = reaped_cpu_seconds
            self.alive = True
            self.status_value = "running"
            self.child_processes = []
            self.children_error = False
            self.cpu_error = False
            self.create_time_error = False

        def exe(self):
            return "/tmp/vireo-server"

        def cmdline(self):
            return ["vireo-server"]

        def create_time(self):
            if self.create_time_error:
                raise RuntimeError("transient identity read failure")
            return self.created_at

        def is_running(self):
            return self.alive

        def status(self):
            return self.status_value

        def children(self, recursive=False):
            if self.children_error:
                raise RuntimeError("transient enumeration failure")
            return list(self.child_processes)

        def cpu_times(self):
            if self.cpu_error:
                raise RuntimeError("transient CPU read failure")
            return type("CpuTimes", (), {
                "user": self.cpu_seconds,
                "system": 0.0,
                "children_user": self.reaped_cpu_seconds,
                "children_system": 0.0,
            })()

        def memory_info(self):
            return type("MemoryInfo", (), {"rss": self.rss})()

        def num_threads(self):
            return 1

    class _CpuPsutil:
        Error = RuntimeError

        def __init__(self, root):
            self.root = root

        def Process(self, pid):
            assert pid == self.root.pid
            return self.root

    root = _CpuProcess(100, 1.0, 1.0, 100)
    sampler = ProcessTreeSampler(
        root.pid,
        psutil_module=_CpuPsutil(root),
        monotonic=clock.monotonic,
        platform_system=lambda: "Linux",
    )
    sampler.prime()

    # During the one-second window the root consumes 0.2 CPU seconds and a
    # newly spawned worker consumes 0.5. Its first sample must contribute its
    # lifetime CPU instead of psutil's first-call cpu_percent() zero.
    root.cpu_seconds = 1.2
    child = _CpuProcess(101, 2.0, 0.5, 200)
    root.child_processes = [child]
    clock.sleep(1.0)
    sample = sampler.sample()

    assert sample["cpu_percent"] == 70.0
    assert sample["rss_bytes"] == 300
    assert sample["process_count"] == 2
    assert sample["cpu_accounting_complete"] is True
    assert sampler.metadata()["initial_rss_bytes"] == 100

    # A helper spawned and reaped by the still-live child contributes through
    # that child's cumulative children CPU counter on the next interval.
    child.reaped_cpu_seconds = 0.3
    clock.sleep(1.0)
    assert sampler.sample()["cpu_percent"] == 30.0

    # Reusing the child's PID produces a new identity and uses the newly
    # discovered process object rather than stale cached counters.
    replacement = _CpuProcess(101, 3.0, 0.2, 150)
    root.child_processes = [replacement]
    clock.sleep(1.0)
    assert sampler.sample()["cpu_percent"] == 20.0

    # A transient children() error reuses the cached child and retains its
    # baseline while marking the interval unsuitable for distributions.
    root.children_error = True
    replacement.cpu_seconds = 0.3
    clock.sleep(1.0)
    incomplete_sample = sampler.sample()
    assert incomplete_sample["cpu_percent"] == 10.0
    assert incomplete_sample["process_tree_complete"] is False
    root.children_error = False
    replacement.cpu_seconds = 0.4
    clock.sleep(1.0)
    recovery_sample = sampler.sample()
    assert recovery_sample["cpu_percent"] == 10.0
    assert recovery_sample["process_tree_complete"] is False
    replacement.cpu_seconds = 0.5
    clock.sleep(1.0)
    assert sampler.sample()["process_tree_complete"] is True

    # If a brand-new child cannot be read during the incomplete interval, its
    # first successful read establishes a baseline in the excluded recovery
    # interval instead of charging its lifetime CPU over one second.
    unreadable_child = _CpuProcess(102, 4.0, 0.3, 100)
    unreadable_child.create_time_error = True
    root.child_processes = [unreadable_child]
    clock.sleep(1.0)
    assert sampler.sample()["process_tree_complete"] is False
    unreadable_child.create_time_error = False
    clock.sleep(1.0)
    recovered_child = sampler.sample()
    assert recovered_child["cpu_percent"] == 0.0
    assert recovered_child["process_tree_complete"] is False
    unreadable_child.cpu_seconds = 0.4
    clock.sleep(1.0)
    assert sampler.sample()["cpu_percent"] == 10.0

    # If that child exits and is reaped while enumeration fails, preserve the
    # parent's prior reaped baseline. The recovery poll can then reconcile the
    # confirmed departure instead of leaving stale unreconciled CPU that would
    # hide work from a later unrelated helper.
    reap_root = _CpuProcess(300, 1.0, 1.0, 100)
    reap_child = _CpuProcess(301, 2.0, 0.4, 100)
    reap_root.child_processes = [reap_child]
    reap_sampler = ProcessTreeSampler(
        reap_root.pid,
        psutil_module=_CpuPsutil(reap_root),
        monotonic=clock.monotonic,
        platform_system=lambda: "Linux",
    )
    reap_sampler.prime()
    reap_root.child_processes = []
    reap_root.children_error = True
    reap_root.reaped_cpu_seconds = 0.4
    reap_child.cpu_error = True
    clock.sleep(1.0)
    assert reap_sampler.sample()["process_tree_complete"] is False
    reap_root.children_error = False
    clock.sleep(1.0)
    assert reap_sampler.sample()["process_tree_complete"] is False
    assert reap_sampler._unreconciled_departed_child_cpu == 0.0
    reap_root.reaped_cpu_seconds = 0.6
    clock.sleep(1.0)
    assert reap_sampler.sample()["cpu_percent"] == 20.0

    bad_root = _CpuProcess(200, 1.0, 1.0, 100)
    bad_child = _CpuProcess(201, 1.5, 0.2, 100)
    bad_child.cpu_error = True
    bad_root.child_processes = [bad_child]
    incomplete_prime = ProcessTreeSampler(
        bad_root.pid,
        psutil_module=_CpuPsutil(bad_root),
        monotonic=clock.monotonic,
        platform_system=lambda: "Linux",
    )
    with pytest.raises(RuntimeError, match="complete Vireo process-tree baseline"):
        incomplete_prime.prime()

    root.alive = False
    with pytest.raises(RuntimeError, match="exited or was replaced"):
        sampler.sample()
    root.alive = True
    root.status_value = "zombie"
    with pytest.raises(RuntimeError, match="exited or was replaced"):
        sampler.sample()


def test_process_sampler_counts_children_reaped_between_polls():
    clock = _FakeClock()

    class _RootProcess:
        pid = 100
        own_cpu = 1.0
        reaped_cpu = 0.0

        def exe(self):
            return "/tmp/vireo-server"

        def cmdline(self):
            return ["vireo-server"]

        def create_time(self):
            return 1.0

        def is_running(self):
            return True

        def status(self):
            return "running"

        def children(self, recursive=False):
            return []

        def cpu_times(self):
            return type("CpuTimes", (), {
                "user": self.own_cpu,
                "system": 0.0,
                "children_user": self.reaped_cpu,
                "children_system": 0.0,
            })()

        def memory_info(self):
            return type("MemoryInfo", (), {"rss": 100})()

        def num_threads(self):
            return 1

    class _CpuPsutil:
        Error = RuntimeError

        def __init__(self, root):
            self.root = root

        def Process(self, pid):
            assert pid == self.root.pid
            return self.root

    root = _RootProcess()
    sampler = ProcessTreeSampler(
        root.pid,
        psutil_module=_CpuPsutil(root),
        monotonic=clock.monotonic,
        platform_system=lambda: "Linux",
    )
    sampler.prime()

    # A helper consumes 0.4 CPU seconds and is reaped before the next poll,
    # so it never appears in children(). The parent's cumulative child time
    # is the only process-tree accounting record that survives.
    root.reaped_cpu = 0.4
    clock.sleep(1.0)

    assert sampler.sample()["cpu_percent"] == 40.0
    assert sampler.metadata()["cpu_accounting_complete"] is True

    incomplete = ProcessTreeSampler(
        root.pid,
        psutil_module=_CpuPsutil(root),
        monotonic=clock.monotonic,
        platform_system=lambda: "Darwin",
    )
    incomplete.prime()
    assert incomplete.metadata()["cpu_accounting_complete"] is False


class _FakeConn:
    def __init__(self, port, *, ip="127.0.0.1", status="LISTEN"):
        self.status = status
        self.laddr = type("Addr", (), {"port": port, "ip": ip})()


class _FakeProc:
    def __init__(
        self,
        pid,
        *,
        name="vireo-server",
        cmdline=None,
        port=None,
        listener_ip="127.0.0.1",
        children=None,
    ):
        self.pid = pid
        self._name = name
        self._cmdline = cmdline if cmdline is not None else [name]
        self._port = port
        self._listener_ip = listener_ip
        self._children = list(children or [])

    def as_dict(self, attrs=None):
        return {"name": self._name, "cmdline": list(self._cmdline)}

    def net_connections(self, kind="tcp"):
        if self._port is None:
            return []
        return [_FakeConn(self._port, ip=self._listener_ip)]

    def children(self, recursive=False):
        return list(self._children)


class _FakePsutil:
    Error = RuntimeError
    CONN_LISTEN = "LISTEN"

    def __init__(self, processes):
        self._processes = {p.pid: p for p in processes}

    def Process(self, pid):
        if pid not in self._processes:
            raise self.Error(f"no process with PID {pid}")
        return self._processes[pid]


def _fake_resolver(host_to_ips):
    def resolver(host, _port, type=None):
        ips = host_to_ips.get(host, [])
        if not ips:
            raise OSError(f"unknown host {host}")
        return [(0, 0, 0, "", (ip, 0)) for ip in ips]
    return resolver


_LOOPBACK_RESOLVER = _fake_resolver({"127.0.0.1": ["127.0.0.1"]})


def test_explicit_server_accepted_when_pid_owns_url_port():
    proc = _FakeProc(4242, port=50222)
    fake = _FakePsutil([proc])

    server = discover_server(
        requested_pid=4242,
        requested_url="http://127.0.0.1:50222",
        psutil_module=fake,
        resolver=_LOOPBACK_RESOLVER,
    )

    assert server == {"pid": 4242, "url": "http://127.0.0.1:50222"}


def test_explicit_server_accepted_when_child_process_owns_port():
    child = _FakeProc(4243, port=50222)
    parent = _FakeProc(4242, port=None, children=[child])
    fake = _FakePsutil([parent, child])

    server = discover_server(
        requested_pid=4242,
        requested_url="http://127.0.0.1:50222",
        psutil_module=fake,
        resolver=_LOOPBACK_RESOLVER,
    )

    assert server == {"pid": 4243, "url": "http://127.0.0.1:50222"}


def test_explicit_server_accepted_when_listener_binds_all_interfaces():
    proc = _FakeProc(4242, port=50222, listener_ip="0.0.0.0")
    fake = _FakePsutil([proc])

    server = discover_server(
        requested_pid=4242,
        requested_url="http://127.0.0.1:50222",
        psutil_module=fake,
        resolver=_LOOPBACK_RESOLVER,
    )

    assert server == {"pid": 4242, "url": "http://127.0.0.1:50222"}


def test_listener_rejects_loopback_url_from_different_address_family():
    proc = _FakeProc(4242, port=50222, listener_ip="127.0.0.1")
    fake = _FakePsutil([proc])
    resolver = _fake_resolver({"::1": ["::1"]})

    with pytest.raises(RuntimeError, match="does not own"):
        discover_server(
            requested_pid=4242,
            requested_url="http://[::1]:50222",
            psutil_module=fake,
            resolver=resolver,
        )


def test_specific_loopback_listener_requires_exact_address():
    proc = _FakeProc(4242, port=50222, listener_ip="127.0.0.1")
    fake = _FakePsutil([proc])
    resolver = _fake_resolver({"other-loopback": ["127.0.0.2"]})

    with pytest.raises(RuntimeError, match="does not own"):
        discover_server(
            requested_pid=4242,
            requested_url="http://other-loopback:50222",
            psutil_module=fake,
            resolver=resolver,
        )


def test_multi_address_hostname_is_pinned_to_verified_listener():
    proc = _FakeProc(4242, port=50222, listener_ip="127.0.0.1")
    fake = _FakePsutil([proc])
    resolver = _fake_resolver({"localhost": ["::1", "127.0.0.1"]})

    server = discover_server(
        requested_pid=4242,
        requested_url="http://localhost:50222",
        psutil_module=fake,
        resolver=resolver,
    )

    assert server == {"pid": 4242, "url": "http://127.0.0.1:50222"}


def test_url_only_multi_address_hostname_is_pinned_to_verified_listener():
    proc = _FakeProc(4242, port=50222, listener_ip="127.0.0.1")
    proc.info = {
        "pid": 4242, "name": "vireo-server",
        "cmdline": ["vireo-server"], "create_time": 0,
    }
    proc.parents = lambda: []
    fake = _FakePsutil([proc])
    fake.process_iter = lambda attrs=None: iter([proc])
    resolver = _fake_resolver({
        "vireo.local": ["192.168.1.50", "127.0.0.1"],
    })

    server = discover_server(
        requested_url="http://vireo.local:50222",
        psutil_module=fake,
        resolver=resolver,
    )

    assert server == {"pid": 4242, "url": "http://127.0.0.1:50222"}


def test_ipv6_wildcard_listener_accepts_ipv6_loopback_url():
    proc = _FakeProc(4242, port=50222, listener_ip="::")
    fake = _FakePsutil([proc])
    resolver = _fake_resolver({"::1": ["::1"]})

    server = discover_server(
        requested_pid=4242,
        requested_url="http://[::1]:50222",
        psutil_module=fake,
        resolver=resolver,
    )

    assert server == {"pid": 4242, "url": "http://[::1]:50222"}


def test_wildcard_listener_accepts_verified_local_lan_address():
    proc = _FakeProc(4242, port=50222, listener_ip="0.0.0.0")
    fake = _FakePsutil([proc])
    resolver = _fake_resolver({"vireo.local": ["192.168.1.20"]})

    server = discover_server(
        requested_pid=4242,
        requested_url="http://vireo.local:50222",
        psutil_module=fake,
        resolver=resolver,
        local_addresses={"192.168.1.20"},
    )

    assert server == {
        "pid": 4242,
        "url": "http://vireo.local:50222",
        "host_header": "localhost",
    }


def test_wildcard_listener_uses_interface_address_not_hostname_only():
    proc = _FakeProc(4242, port=50222, listener_ip="0.0.0.0")
    fake = _FakePsutil([proc])
    fake.net_if_addrs = lambda: {
        "en0": [type("InterfaceAddress", (), {
            "family": socket.AF_INET,
            "address": "192.168.1.20",
        })()],
    }
    resolver = _fake_resolver({
        "vireo.local": ["192.168.1.20"],
        # Deliberately omit the machine hostname to model /etc/hosts setups
        # where hostname resolution does not expose the LAN interface.
    })

    server = discover_server(
        requested_pid=4242,
        requested_url="http://vireo.local:50222",
        psutil_module=fake,
        resolver=resolver,
    )

    assert server == {
        "pid": 4242,
        "url": "http://vireo.local:50222",
        "host_header": "localhost",
    }


def test_wildcard_listener_rejects_remote_address():
    proc = _FakeProc(4242, port=50222, listener_ip="0.0.0.0")
    fake = _FakePsutil([proc])
    resolver = _fake_resolver({"remote-host": ["192.168.1.50"]})

    with pytest.raises(RuntimeError, match="does not own"):
        discover_server(
            requested_pid=4242,
            requested_url="http://remote-host:50222",
            psutil_module=fake,
            resolver=resolver,
            local_addresses={"192.168.1.20"},
        )


def test_wildcard_listener_rejects_local_address_from_other_family():
    proc = _FakeProc(4242, port=50222, listener_ip="0.0.0.0")
    fake = _FakePsutil([proc])
    resolver = _fake_resolver({"vireo-v6.local": ["2001:db8::20"]})

    with pytest.raises(RuntimeError, match="does not own"):
        discover_server(
            requested_pid=4242,
            requested_url="http://vireo-v6.local:50222",
            psutil_module=fake,
            resolver=resolver,
            local_addresses={"2001:db8::20"},
        )


def test_explicit_server_rejected_when_pid_does_not_own_url_port():
    """A stale/typo'd PID that happens to name a live process must not be
    accepted: process CPU/memory would come from one server while API samples
    would come from another, silently corrupting every workload comparison.
    """
    proc = _FakeProc(4242, port=60000)
    fake = _FakePsutil([proc])

    with pytest.raises(RuntimeError, match="does not own"):
        discover_server(
            requested_pid=4242,
            requested_url="http://127.0.0.1:50222",
            psutil_module=fake,
            resolver=_LOOPBACK_RESOLVER,
        )


@pytest.mark.parametrize("url", [
    "http://127.0.0.1:not-a-port",
    "http://127.0.0.1:99999",
    "http://[::1",
])
def test_discovery_rejects_invalid_url_ports_without_value_error(url):
    with pytest.raises(RuntimeError, match="valid port"):
        discover_server(requested_url=url, psutil_module=_FakePsutil([]))


def test_url_only_discovery_rejects_host_that_local_listener_does_not_serve():
    """`--url http://remote-host:50222` with no `--pid`: even though a
    local vireo-server also listens on port 50222, the URL points to a
    remote host, so selecting the local process would pair local CPU/RSS
    with remote Jobs API data.  The URL-only branch must apply the same
    host reachability check as `--pid`+`--url`.
    """
    proc = _FakeProc(4242, port=50222, listener_ip="127.0.0.1")
    fake = _FakePsutil([proc])
    # Emulate psutil.process_iter over our fake.
    fake.process_iter = lambda attrs=None: iter([])
    proc.info = {
        "pid": 4242, "name": "vireo-server",
        "cmdline": ["vireo-server"], "create_time": 0,
    }
    fake.process_iter = lambda attrs=None: iter([proc])
    proc.parents = lambda: []
    resolver = _fake_resolver({"remote-host": ["192.168.1.5"]})

    with pytest.raises(RuntimeError, match="no listening local vireo-server"):
        discover_server(
            requested_url="http://remote-host:50222",
            psutil_module=fake,
            resolver=resolver,
        )


def test_discovery_brackets_specific_ipv6_listener_address():
    proc = _FakeProc(4242, port=50222, listener_ip="2001:db8::1")
    proc.info = {
        "pid": 4242, "name": "vireo-server",
        "cmdline": ["vireo-server"], "create_time": 0,
    }
    proc.parents = lambda: []
    fake = _FakePsutil([proc])
    fake.process_iter = lambda attrs=None: iter([proc])

    server = discover_server(psutil_module=fake)

    assert server == {
        "pid": 4242,
        "url": "http://[2001:db8::1]:50222",
        "host_header": "localhost",
    }


@pytest.mark.parametrize("listener_ip", ["::1", "::"])
def test_discovery_preserves_ipv6_family_for_loopback_listeners(listener_ip):
    proc = _FakeProc(4242, port=50222, listener_ip=listener_ip)
    proc.info = {
        "pid": 4242, "name": "vireo-server",
        "cmdline": ["vireo-server"], "create_time": 0,
    }
    proc.parents = lambda: []
    fake = _FakePsutil([proc])
    fake.process_iter = lambda attrs=None: iter([proc])

    server = discover_server(psutil_module=fake)

    assert server == {"pid": 4242, "url": "http://[::1]:50222"}


def test_discovery_deduplicates_multiple_listeners_from_same_process():
    proc = _FakeProc(4242, port=None)
    proc.info = {
        "pid": 4242, "name": "vireo-server",
        "cmdline": ["vireo-server"], "create_time": 0,
    }
    proc.parents = lambda: []
    proc.net_connections = lambda kind="tcp": [
        _FakeConn(50222, ip="127.0.0.1"),
        _FakeConn(50222, ip="::1"),
    ]
    fake = _FakePsutil([proc])
    fake.process_iter = lambda attrs=None: iter([proc])

    server = discover_server(psutil_module=fake)

    assert server == {"pid": 4242, "url": "http://127.0.0.1:50222"}


def test_url_only_discovery_accepts_loopback_hostname_for_local_listener():
    proc = _FakeProc(4242, port=50222, listener_ip="127.0.0.1")
    proc.info = {
        "pid": 4242, "name": "vireo-server",
        "cmdline": ["vireo-server"], "create_time": 0,
    }
    proc.parents = lambda: []
    fake = _FakePsutil([proc])
    fake.process_iter = lambda attrs=None: iter([proc])

    server = discover_server(
        requested_url="http://127.0.0.1:50222",
        psutil_module=fake,
        resolver=_LOOPBACK_RESOLVER,
    )

    assert server == {"pid": 4242, "url": "http://127.0.0.1:50222"}


def test_explicit_server_rejected_when_url_host_does_not_match_listener_ip():
    """The URL host may resolve to a different machine even when the port
    matches, so the local PID's CPU/RSS would be paired with API samples
    from an unrelated server on the LAN.  The listener bound to loopback
    must not be accepted for a URL that resolves off-loopback.
    """
    proc = _FakeProc(4242, port=50222, listener_ip="127.0.0.1")
    fake = _FakePsutil([proc])
    resolver = _fake_resolver({"otherhost.local": ["192.168.1.5"]})

    with pytest.raises(RuntimeError, match="does not own"):
        discover_server(
            requested_pid=4242,
            requested_url="http://otherhost.local:50222",
            psutil_module=fake,
            resolver=resolver,
        )


def test_explicit_server_rejected_when_owning_process_is_not_vireo():
    proc = _FakeProc(4242, name="postgres", cmdline=["postgres", "-D", "/db"], port=50222)
    fake = _FakePsutil([proc])

    with pytest.raises(RuntimeError, match="not a vireo-server"):
        discover_server(
            requested_pid=4242,
            requested_url="http://127.0.0.1:50222",
            psutil_module=fake,
            resolver=_LOOPBACK_RESOLVER,
        )
