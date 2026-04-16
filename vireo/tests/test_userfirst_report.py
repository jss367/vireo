"""Tests for the user-first testing report/findings module."""
import json

from testing.userfirst.report import Finding, Report


def test_finding_types():
    f = Finding.bug("page not found", url="/missing")
    assert f.kind == "BUG"
    assert f.message == "page not found"
    assert f.context == {"url": "/missing"}


def test_report_counts_by_kind():
    r = Report(name="sweep")
    r.add(Finding.bug("x"))
    r.add(Finding.bug("y"))
    r.add(Finding.warn("z"))
    r.add(Finding.suspect("q"))
    counts = r.counts()
    assert counts["BUG"] == 2
    assert counts["WARN"] == 1
    assert counts["SUSPECT"] == 1
    assert counts["PERF"] == 0


def test_report_step_tracking():
    r = Report(name="scenario")
    r.record_step("goto /browse", status=200, elapsed_ms=123)
    r.record_step("click save", status=None, elapsed_ms=5)
    assert len(r.steps) == 2
    assert r.steps[0]["action"] == "goto /browse"
    assert r.steps[0]["status"] == 200
    assert r.steps[0]["elapsed_ms"] == 123


def test_report_markdown_summary():
    r = Report(name="sweep")
    r.add(Finding.bug("/static/help.js 404", url="/browse"))
    r.add(Finding.warn("console deprecation", source="vireo-utils.js"))
    r.duration_s = 2.5
    md = r.to_markdown()
    assert "sweep" in md
    assert "BUG" in md
    assert "/static/help.js 404" in md
    assert "console deprecation" in md
    assert "2.5" in md


def test_report_round_trip_json(tmp_path):
    r = Report(name="scenario_x")
    r.add(Finding.bug("boom", url="/x"))
    r.record_step("goto /x", status=200, elapsed_ms=10)
    out = tmp_path / "findings.json"
    r.write_json(out)
    data = json.loads(out.read_text())
    assert data["name"] == "scenario_x"
    assert len(data["findings"]) == 1
    assert data["findings"][0]["kind"] == "BUG"
    assert len(data["steps"]) == 1


def test_report_screenshots_listed(tmp_path):
    r = Report(name="scenario")
    shot1 = tmp_path / "01-initial.png"
    shot2 = tmp_path / "02-after.png"
    shot1.write_bytes(b"fake png")
    shot2.write_bytes(b"fake png")
    r.add_screenshot(shot1)
    r.add_screenshot(shot2)
    assert len(r.screenshots) == 2
    md = r.to_markdown()
    assert "01-initial.png" in md
    assert "02-after.png" in md


def test_report_has_bugs_predicate():
    r = Report(name="x")
    assert not r.has_bugs()
    r.add(Finding.warn("x"))
    assert not r.has_bugs()
    r.add(Finding.bug("y"))
    assert r.has_bugs()
