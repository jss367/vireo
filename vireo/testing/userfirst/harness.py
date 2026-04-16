"""User-first testing harness — runs Vireo in a subprocess, drives it via Playwright."""
import contextlib
import os
import shutil
import socket
import subprocess
import sys
import time
import uuid
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import urlopen

from .profile import (
    profile_paths,
    resolve_photos_root,
    resolve_profile,
    validate_db_folders,
)
from .report import Finding, Report

_APP_PY = (Path(__file__).parent.parent.parent / "app.py").resolve()


def _free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _new_run_id():
    return datetime.now().strftime("%Y%m%d-%H%M%S-%f-") + uuid.uuid4().hex[:4]


def _prune_runs(runs_dir, keep):
    runs_dir = Path(runs_dir)
    if not runs_dir.exists():
        return
    entries = sorted(p for p in runs_dir.iterdir() if p.is_dir())
    for old in entries[:-keep]:
        shutil.rmtree(old, ignore_errors=True)


def _relative_url_same_origin(url, origin):
    """Return the path+query of `url` if it shares origin with `origin`, else None."""
    u, o = urlparse(url), urlparse(origin)
    if (u.scheme, u.hostname, u.port) != (o.scheme, o.hostname, o.port):
        return None
    path = u.path
    if u.query:
        path += "?" + u.query
    return path


def _wait_for_health(base_url, timeout=30.0):
    deadline = time.time() + timeout
    last_err = None
    while time.time() < deadline:
        try:
            with urlopen(f"{base_url}/api/health", timeout=1) as r:
                if r.status == 200:
                    return
        except Exception as e:
            last_err = e
        time.sleep(0.1)
    raise RuntimeError(f"server at {base_url} not healthy in {timeout}s (last: {last_err})")


def _start_app(paths, port, log_file, fake_home):
    env = os.environ.copy()
    # Isolate the subprocess from the user's real ~/.vireo/ — this redirects
    # config, logs, models, masks, and any other ~/.vireo/* reads/writes.
    env["HOME"] = str(fake_home)
    cmd = [
        sys.executable,
        str(_APP_PY),
        "--db", str(paths["db"]),
        "--thumb-dir", str(paths["thumbnails"]),
        "--port", str(port),
        "--no-browser",
    ]
    # The log handle must outlive this function — it's closed on session exit.
    log = open(log_file, "wb")  # noqa: SIM115 — closed in vireo_session()
    return subprocess.Popen(cmd, env=env, stdout=log, stderr=subprocess.STDOUT), log


class VireoSession:
    """User-first testing session — wraps a Playwright page with instrumentation."""

    def __init__(self, base_url, page, report, run_dir):
        self.base_url = base_url
        self.page = page
        self.report = report
        self.run_dir = Path(run_dir)
        (self.run_dir / "screens").mkdir(parents=True, exist_ok=True)
        self._screen_counter = 0
        self._wire_listeners()

    def _wire_listeners(self):
        def on_console(msg):
            if msg.type in ("error", "warning"):
                kind = Finding.bug if msg.type == "error" else Finding.warn
                text = msg.text
                # Filter out resource-load 404s — those are captured via requestfailed/response
                if "Failed to load resource" in text:
                    return
                self.report.add(kind(f"console.{msg.type}: {text}"))

        def on_response(resp):
            rel = _relative_url_same_origin(resp.url, self.base_url)
            if rel is None:
                return
            if resp.status >= 400:
                self.report.add(
                    Finding.bug(
                        f"HTTP {resp.status} on same-origin request",
                        url=rel,
                        method=resp.request.method,
                    )
                )

        def on_requestfailed(req):
            rel = _relative_url_same_origin(req.url, self.base_url)
            if rel is None:
                return
            self.report.add(
                Finding.bug(
                    f"request failed: {req.failure}",
                    url=rel,
                    method=req.method,
                )
            )

        self.page.on("console", on_console)
        self.page.on("response", on_response)
        self.page.on("requestfailed", on_requestfailed)

    def goto(self, path, wait_until="networkidle", timeout=20000):
        url = self.base_url + path
        t0 = time.time()
        try:
            resp = self.page.goto(url, wait_until=wait_until, timeout=timeout)
        except Exception as e:
            self.report.record_step(f"goto {path}", status=None, elapsed_ms=None, error=str(e))
            self.report.add(Finding.bug(f"goto failed: {e}", url=path))
            return None
        elapsed_ms = int((time.time() - t0) * 1000)
        status = resp.status if resp else None
        self.report.record_step(f"goto {path}", status=status, elapsed_ms=elapsed_ms)
        if elapsed_ms > 5000:
            self.report.add(Finding.perf(f"slow page load: {elapsed_ms}ms", url=path))
        return resp

    def click(self, selector, timeout=5000):
        try:
            self.page.click(selector, timeout=timeout)
            self.report.record_step(f"click {selector}")
        except Exception as e:
            self.report.record_step(f"click {selector}", error=str(e))
            self.report.add(Finding.bug(f"click failed: {e}", selector=selector))

    def fill(self, selector, text, timeout=5000):
        try:
            self.page.fill(selector, text, timeout=timeout)
            self.report.record_step(f"fill {selector!r}")
        except Exception as e:
            self.report.record_step(f"fill {selector}", error=str(e))
            self.report.add(Finding.bug(f"fill failed: {e}", selector=selector))

    def eval(self, js):
        return self.page.evaluate(js)

    def screenshot(self, label):
        self._screen_counter += 1
        name = f"{self._screen_counter:02d}-{label}.png"
        path = self.run_dir / "screens" / name
        try:
            self.page.screenshot(path=str(path), full_page=False)
            self.report.add_screenshot(path)
        except Exception as e:
            self.report.record_step(f"screenshot {label}", error=str(e))

    def assert_that(self, cond, msg, **ctx):
        """Soft assert — records a BUG finding, does not abort."""
        if not cond:
            self.report.add(Finding.bug(f"assertion failed: {msg}", **ctx))

    def flag_suspect(self, message, **ctx):
        self.report.add(Finding.suspect(message, **ctx))


@contextmanager
def vireo_session(name="session", startup_timeout=30.0, keep_runs=20):
    """Start Vireo against the test profile + launch Playwright; yield a session.

    Requires:
      - VIREO_PROFILE env var set to a safe test directory
      - optionally, VIREO_TEST_PHOTOS set for photo-folder validation
      - `playwright` installed with Chromium browsers
    """
    profile = resolve_profile()
    photos_root = resolve_photos_root()
    paths = profile_paths(profile)

    profile.mkdir(parents=True, exist_ok=True)
    paths["thumbnails"].mkdir(parents=True, exist_ok=True)
    paths["runs"].mkdir(parents=True, exist_ok=True)

    validate_db_folders(paths["db"], photos_root)

    run_id = _new_run_id()
    run_dir = paths["runs"] / run_id
    run_dir.mkdir(parents=True)

    port = _free_port()
    # Flask binds to 127.0.0.1 (see app.py); using the literal IPv4 address
    # instead of `localhost` avoids environments where `localhost` resolves to
    # IPv6 (`::1`) first and the health probe/navigation hits the wrong stack.
    base_url = f"http://127.0.0.1:{port}"
    log_file = run_dir / "app.log"

    # Subprocess gets a fake HOME inside the profile so all ~/.vireo/* paths
    # resolve to <profile>/fake_home/.vireo/* — isolated from real data.
    fake_home = profile / "fake_home"
    (fake_home / ".vireo").mkdir(parents=True, exist_ok=True)

    proc, log_fh = _start_app(paths, port, log_file, fake_home)
    print(f"USER-FIRST TEST MODE — profile={profile}, photos={photos_root}, port={port}")
    started = time.time()

    browser = pw = ctx = page = None
    report = None
    try:
        _wait_for_health(base_url, timeout=startup_timeout)

        from playwright.sync_api import sync_playwright
        pw = sync_playwright().start()
        browser = pw.chromium.launch()
        ctx = browser.new_context(viewport={"width": 1440, "height": 900})
        page = ctx.new_page()

        report = Report(name=name)
        session = VireoSession(base_url, page, report, run_dir)
        yield session
    finally:
        # Persist the report even when the session body raises — otherwise
        # findings and screenshots from a crashed scenario are silently lost.
        if report is not None:
            report.duration_s = time.time() - started
            with contextlib.suppress(Exception):
                report.write_json(run_dir / "findings.json")
            with contextlib.suppress(Exception):
                report.write_markdown(run_dir / "report.md")
        for close in (
            lambda: ctx and ctx.close(),
            lambda: browser and browser.close(),
            lambda: pw and pw.stop(),
        ):
            with contextlib.suppress(Exception):
                close()
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
        log_fh.close()
        _prune_runs(paths["runs"], keep=keep_runs)
