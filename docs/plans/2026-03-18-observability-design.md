# Vireo Observability Design

**Goal:** Make long-running operations transparent with live progress, streaming logs, and job history. No more black-box waits.

**Key decisions:**
- SSE (Server-Sent Events) for streaming — no new dependencies, native browser support
- Background threads for long operations via a JobRunner module
- Global log capture via a custom Python logging handler
- In-memory job state while running, SQLite job_history for the archive
- Log panel on every page (via shared partial), dedicated /logs page for deep debugging

---

## Job System

A `vireo/jobs.py` module with a `JobRunner` class. Holds a dict of active jobs in memory and runs work in background threads.

Each job is a dict:
```python
{
    'id': 'scan-1710729600',
    'type': 'scan',           # scan, thumbnails, import, sync
    'status': 'running',      # pending, running, completed, failed
    'started_at': '2024-03-18T02:00:00',
    'finished_at': None,
    'progress': {'current': 1234, 'total': 50000, 'current_file': 'DSC_0042.NEF'},
    'result': None,           # set on completion: {photos_indexed: 50000}
    'errors': [],             # list of error strings as they occur
}
```

API:
- `POST /api/jobs/scan` — start a scan job, returns `{job_id}`
- `POST /api/jobs/thumbnails` — start thumbnail generation
- `POST /api/jobs/import` — start LR import
- `POST /api/jobs/sync` — start XMP sync
- `GET /api/jobs` — list all active + recent jobs
- `GET /api/jobs/<id>` — get job status (polling fallback)
- `GET /api/jobs/<id>/stream` — SSE stream of progress events

The old synchronous routes (`/api/scan`, `/api/scan/thumbnails`) get replaced by these.

Job history persists to a `job_history` table in SQLite so you can see past runs after a server restart.

---

## SSE Progress Streaming

The `GET /api/jobs/<id>/stream` endpoint returns a `text/event-stream` response. Flask does this with a generator function — no new dependencies.

Event types sent over the stream:

```
event: progress
data: {"current": 1234, "total": 50000, "current_file": "DSC_0042.NEF", "rate": 142.3}

event: log
data: {"level": "WARNING", "message": "Could not read EXIF from DSC_0099.NEF"}

event: error
data: {"message": "Permission denied: /Volumes/Photography/2019/"}

event: complete
data: {"status": "completed", "result": {"photos_indexed": 50000}, "duration": 272.5}
```

The browser connects with `new EventSource('/api/jobs/<id>/stream')` and updates the UI as events arrive. If the connection drops, `EventSource` auto-reconnects (built-in browser behavior).

For the log events — we hook into Python's `logging` module with a custom handler that pushes log records into a per-job queue. The SSE generator reads from that queue. This means any `log.warning()` call inside scanner, thumbnails, importer, or sync automatically shows up in the browser without those modules knowing about SSE.

Rate is calculated from the progress callback — `current / elapsed_seconds` — so the UI can show "142 photos/sec."

---

## Log Viewer

Two parts: the slide-out panel and the dedicated page.

**Slide-out panel** — Added to `_navbar.html` so it appears on every page. A thin bar at the bottom with a toggle button. Clicking it expands a panel (like browser dev tools) showing the last ~100 log lines. It connects to `GET /api/logs/stream` — a global SSE endpoint that streams all server log output regardless of which job produced it. Color-coded by level: grey for DEBUG, white for INFO, yellow for WARNING, red for ERROR. A level filter dropdown to hide noise.

**Dedicated `/logs` page** — Full-page version of the same stream. Adds: scrollback buffer (keeps all logs from the session), text search/filter, auto-scroll toggle, clear button. Small icon link in the nav bar (developer tool, not a primary workflow).

**Global log capture** — A `LogBroadcaster` class that installs a Python `logging.Handler` on the root logger. It maintains a ring buffer of the last 500 log records and a list of SSE subscriber queues. When a log record arrives, it pushes to all subscribers. The `/api/logs/stream` endpoint creates a subscriber queue and yields from it.

This is separate from the per-job log stream — the global stream shows everything (startup messages, request logs, scan progress, XMP parse warnings), while the per-job stream only shows logs from that job's thread.

---

## UI Integration

**Settings page** — The scan and thumbnail buttons POST to `/api/jobs/scan`, get back a `job_id`, then connect to the SSE stream. The progress section shows: a progress bar, current file being processed, photos/sec rate, elapsed time, and a scrolling mini-log of warnings/errors. The "Scan" button becomes "Stop" while running.

**Import page** — Same pattern. Phase 3 (execute) connects to the job stream instead of waiting for a single response.

**Nav bar** — A small activity indicator appears next to "Vireo" when any job is running. Clicking it opens the log panel. This way you can navigate away from settings, browse photos, and still see that the scan is running.

**Status dashboard** — Expand the settings page stats section to show: DB file size, thumbnail cache size, total keywords, pending sync changes, and a "Recent Jobs" table showing the last 10 jobs with type, status, duration, and result summary. Reads from the `job_history` table.

**`/logs` page** — Small icon in the nav bar corner. Full-page log viewer with scrollback, search, level filter, auto-scroll toggle.

---

## Job History Table

```sql
job_history (
    id          TEXT PRIMARY KEY,
    type        TEXT,
    status      TEXT,
    started_at  TEXT,
    finished_at TEXT,
    duration    REAL,
    result      TEXT,
    error_count INTEGER DEFAULT 0,
    config      TEXT
)
```

Written once when a job finishes. The in-memory job dict is the live record while running; this is the archive. Settings page "Recent Jobs" table reads from here. No cleanup needed — even years of daily scans would be a few hundred rows.
