"""Latest-wins cancellation of superseded Browse reads (services.search_lanes)."""

import json
import logging
import sqlite3
import threading
import time

import pytest
from services import search_lanes
from services.search_lanes import SearchLanes
from testing.waits import synchronization_timeout


def test_newer_claim_supersedes_older_on_the_same_lane_only():
    lanes = SearchLanes()
    first = lanes.claim("page-a:grid", 1)
    other_loader = lanes.claim("page-a:summary", 1)
    other_window = lanes.claim("page-b:grid", 1)
    assert not first()
    second = lanes.claim("page-a:grid", 2)
    assert first() and not second()
    assert not other_loader() and not other_window()
    # Same sequence (a later page of one search) never cancels its sibling.
    sibling = lanes.claim("page-a:grid", 2)
    assert not second() and not sibling()


def test_straggler_older_than_the_lane_is_superseded_from_the_start():
    lanes = SearchLanes()
    lanes.claim("page:grid", 5)
    assert lanes.claim("page:grid", 4)()
    assert not lanes.claim("page:grid", 5)()


def test_forgotten_lanes_run_to_completion():
    lanes = SearchLanes(max_lanes=2)
    lanes.claim("a", 5)
    lanes.claim("b", 1)
    lanes.claim("c", 1)  # evicts "a", the least recently claimed
    assert not lanes.claim("a", 1)()
    assert len(lanes._latest) == 2


@pytest.mark.parametrize("lane, seq", [
    (None, "1"), ("page:grid", None), ("", "1"), ("page:grid", ""),
    ("page grid", "1"), ("x" * 97, "1"), ("page:grid", "-1"),
    ("page:grid", "1.5"), ("page:grid", "9" * 16),
])
def test_malformed_headers_opt_out(lane, seq):
    assert search_lanes.parse_lane(lane, seq) is None


def test_parse_lane_accepts_page_lane_and_counter():
    assert search_lanes.parse_lane("3f2a9c:summary", "12") == ("3f2a9c:summary", 12)


def test_newer_claim_interrupts_a_running_statement():
    lanes = SearchLanes()
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    superseded = lanes.claim("page:grid", 1)
    search_lanes.cancel_when_superseded(conn, superseded)
    outcome = {}

    def slow_query():
        started = time.monotonic()
        try:
            conn.execute(
                "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n "
                "WHERE i < 1000000000) SELECT COUNT(*) FROM n"
            ).fetchone()
        except sqlite3.OperationalError as exc:
            outcome["error"] = exc
        outcome["seconds"] = time.monotonic() - started

    worker = threading.Thread(target=slow_query)
    worker.start()
    time.sleep(0.2)
    lanes.claim("page:grid", 2)
    worker.join(timeout=synchronization_timeout(10))
    assert not worker.is_alive()
    assert search_lanes.is_superseded_interrupt(outcome["error"], superseded)
    assert outcome["seconds"] < 5
    conn.close()


def test_interrupt_without_supersession_is_not_mistaken_for_one():
    exc = sqlite3.OperationalError("interrupted")
    assert not search_lanes.is_superseded_interrupt(exc, None)
    assert not search_lanes.is_superseded_interrupt(exc, lambda: False)
    assert not search_lanes.is_superseded_interrupt(
        sqlite3.OperationalError("database is locked"), lambda: True)


READS = [
    ("post", "/api/photos/query", {"json": {"rules": [], "page": 1, "per_page": 10}}),
    ("get", "/api/photos/calendar?year=2024", {}),
    ("get", "/api/browse/summary", {}),
]


@pytest.mark.parametrize("method, url, kwargs", READS, ids=["query", "calendar", "summary"])
def test_superseded_browse_read_answers_quietly(app_and_db, monkeypatch, caplog,
                                                method, url, kwargs):
    """A read whose lane moved on is interrupted and answered with a 409 the
    page can ignore, not logged as an unhandled error; the current request
    and requests without a lane are served normally."""
    app, _ = app_and_db
    lanes = SearchLanes()
    monkeypatch.setattr(search_lanes, "SEARCH_LANES", lanes)
    # Check before every VM step so the tiny fixture catalog is interrupted.
    monkeypatch.setattr(search_lanes, "PROGRESS_INSTRUCTIONS", 1)
    client = app.test_client()
    lane = "page1:" + url.split("?")[0].rsplit("/", 1)[-1]
    lanes.claim(lane, 2)

    def send(headers):
        return getattr(client, method)(url, headers=headers, **kwargs)

    with caplog.at_level(logging.INFO):
        stale = send({"X-Vireo-Search-Lane": lane, "X-Vireo-Search-Seq": "1"})
    assert stale.status_code == 409, stale.get_json()
    assert stale.get_json()["code"] == "search_superseded"
    assert not [r for r in caplog.records if r.levelno >= logging.ERROR]

    current = send({"X-Vireo-Search-Lane": lane, "X-Vireo-Search-Seq": "2"})
    assert current.status_code == 200, current.get_json()
    assert send({}).status_code == 200


def test_superseded_response_skips_slow_request_warning(app_and_db, monkeypatch, caplog):
    """A pause of more than 0.5s before the next keystroke leaves the older
    calendar or summary GET running past the slow-request threshold. When it
    is interrupted, the after-hook must not add a misleading WARNING on top of
    the quiet INFO the error handler already logged."""
    import web.app_hooks as app_hooks
    app, _ = app_and_db
    lanes = SearchLanes()
    monkeypatch.setattr(search_lanes, "SEARCH_LANES", lanes)
    monkeypatch.setattr(search_lanes, "PROGRESS_INSTRUCTIONS", 1)
    # Advance every clock read by 10s so elapsed is well past 0.5s without
    # actually sleeping. Only ``_start_timer`` and ``_log_requests`` call
    # ``time.time`` through this module reference.
    tick = [0.0]

    def fake_time():
        tick[0] += 10.0
        return tick[0]

    monkeypatch.setattr(app_hooks.time, "time", fake_time)
    client = app.test_client()
    lane = "page:calendar"
    lanes.claim(lane, 2)
    with caplog.at_level(logging.WARNING):
        stale = client.get(
            "/api/photos/calendar?year=2024",
            headers={"X-Vireo-Search-Lane": lane, "X-Vireo-Search-Seq": "1"},
        )
    assert stale.status_code == 409, stale.get_json()
    assert stale.get_json()["code"] == "search_superseded"
    slow_warnings = [
        r for r in caplog.records
        if r.levelno >= logging.WARNING and "Slow request" in r.getMessage()
    ]
    assert not slow_warnings, [r.getMessage() for r in slow_warnings]


def test_slow_non_superseded_request_still_warns(app_and_db, monkeypatch, caplog):
    """The warning suppression is scoped to superseded responses only; an
    ordinary slow GET must still produce the WARNING that operators rely on."""
    import web.app_hooks as app_hooks
    app, _ = app_and_db
    tick = [0.0]

    def fake_time():
        tick[0] += 10.0
        return tick[0]

    monkeypatch.setattr(app_hooks.time, "time", fake_time)
    client = app.test_client()
    with caplog.at_level(logging.WARNING):
        resp = client.get("/api/browse/summary")
    assert resp.status_code == 200
    slow_warnings = [
        r for r in caplog.records
        if r.levelno >= logging.WARNING and "Slow request" in r.getMessage()
    ]
    assert slow_warnings, [r.getMessage() for r in caplog.records]


def test_summary_route_claims_its_lane(app_and_db, monkeypatch):
    """Each opted-in route installs the handler on its own request DB."""
    app, _ = app_and_db
    lanes = SearchLanes()
    monkeypatch.setattr(search_lanes, "SEARCH_LANES", lanes)
    client = app.test_client()
    resp = client.get("/api/browse/summary", headers={
        "X-Vireo-Search-Lane": "page:summary", "X-Vireo-Search-Seq": "7"})
    assert resp.status_code == 200
    assert lanes.claim("page:summary", 6)()  # the route recorded seq 7
    assert json.loads(resp.data)["filtered_total"] >= 0
