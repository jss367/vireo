"""Background job runner with SSE progress streaming and log broadcasting."""

import json
import logging
import queue
import threading
import time
from collections import deque
from datetime import datetime

log = logging.getLogger(__name__)


class JobRunner:
    """Runs long operations in background threads with progress tracking.

    Args:
        db: optional Database instance for persisting job history
    """

    def __init__(self, db=None):
        self._jobs = {}
        self._events = {}  # job_id -> deque of events
        self._subscribers = {}  # job_id -> list of queues
        self._lock = threading.Lock()
        self._db_path = None
        if db:
            self._db_path = db.conn.execute("PRAGMA database_list").fetchone()[2]
            self._ensure_history_table(db)

    def _ensure_history_table(self, db):
        db.conn.executescript("""
            CREATE TABLE IF NOT EXISTS job_history (
                id          TEXT PRIMARY KEY,
                type        TEXT,
                status      TEXT,
                started_at  TEXT,
                finished_at TEXT,
                duration    REAL,
                result      TEXT,
                error_count INTEGER DEFAULT 0,
                config      TEXT
            );
        """)

    def start(self, job_type, work_fn, config=None):
        """Start a background job.

        Args:
            job_type: string like 'scan', 'thumbnails', 'import', 'sync'
            work_fn: callable(job_dict) that does the work. Can update
                     job['progress'] and return a result dict.
            config: optional dict of job configuration (persisted to history)

        Returns:
            job_id string
        """
        job_id = f"{job_type}-{int(time.time() * 1000)}"
        now = datetime.now().isoformat()

        job = {
            'id': job_id,
            'type': job_type,
            'status': 'running',
            'started_at': now,
            'finished_at': None,
            'progress': {'current': 0, 'total': 0, 'current_file': ''},
            'result': None,
            'errors': [],
            'config': config or {},
        }

        with self._lock:
            self._jobs[job_id] = job
            self._events[job_id] = deque(maxlen=1000)
            self._subscribers[job_id] = []

        thread = threading.Thread(
            target=self._run_job, args=(job, work_fn), daemon=True
        )
        thread.start()
        return job_id

    def _run_job(self, job, work_fn):
        start_time = time.time()
        try:
            result = work_fn(job)
            job['status'] = 'completed'
            job['result'] = result
        except Exception as e:
            job['status'] = 'failed'
            job['errors'].append(str(e))
            log.exception("Job %s failed", job['id'])
        finally:
            elapsed = time.time() - start_time
            job['finished_at'] = datetime.now().isoformat()
            self.push_event(job['id'], 'complete', {
                'status': job['status'],
                'result': job['result'],
                'duration': round(elapsed, 1),
                'errors': job['errors'],
            })
            if self._db_path:
                self._persist_job(job, elapsed)

    def _persist_job(self, job, duration):
        """Persist job to history table using a thread-local connection."""
        if not self._db_path:
            return
        try:
            import sqlite3
            conn = sqlite3.connect(self._db_path)
            conn.execute(
                """INSERT OR REPLACE INTO job_history
                   (id, type, status, started_at, finished_at, duration,
                    result, error_count, config)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (job['id'], job['type'], job['status'],
                 job['started_at'], job['finished_at'], round(duration, 1),
                 json.dumps(job['result']),
                 len(job['errors']),
                 json.dumps(job['config'])),
            )
            conn.commit()
            conn.close()
        except Exception:
            log.warning("Failed to persist job history for %s", job['id'], exc_info=True)

    def get(self, job_id):
        """Get a job by id."""
        return self._jobs.get(job_id)

    def list_jobs(self):
        """List all tracked jobs (active and recently completed)."""
        with self._lock:
            return list(self._jobs.values())

    def get_history(self, db, limit=10):
        """Get recent job history from the database.

        Args:
            db: Database instance (must be from the calling thread)
            limit: max number of rows
        """
        try:
            rows = db.conn.execute(
                "SELECT * FROM job_history ORDER BY started_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
            return [dict(r) for r in rows]
        except Exception:
            return []

    def push_event(self, job_id, event_type, data):
        """Push an event to the job's event stream."""
        event = {'type': event_type, 'data': data, 'time': time.time()}
        with self._lock:
            if job_id in self._events:
                self._events[job_id].append(event)
            for q in self._subscribers.get(job_id, []):
                try:
                    q.put_nowait(event)
                except queue.Full:
                    pass

    def get_events(self, job_id):
        """Get all buffered events for a job."""
        with self._lock:
            return list(self._events.get(job_id, []))

    def subscribe(self, job_id):
        """Subscribe to a job's event stream. Returns a queue."""
        q = queue.Queue(maxsize=200)
        with self._lock:
            if job_id not in self._subscribers:
                self._subscribers[job_id] = []
            self._subscribers[job_id].append(q)
        return q

    def unsubscribe(self, job_id, q):
        """Unsubscribe from a job's event stream."""
        with self._lock:
            subs = self._subscribers.get(job_id, [])
            if q in subs:
                subs.remove(q)


class LogBroadcaster(logging.Handler):
    """Captures log records and broadcasts to SSE subscribers.

    Maintains a ring buffer of recent records and a list of subscriber queues.
    """

    def __init__(self, buffer_size=500):
        super().__init__()
        self._buffer = deque(maxlen=buffer_size)
        self._subscribers = []
        self._lock = threading.Lock()
        self._installed = False

    def install(self):
        """Install this handler on the root logger."""
        if not self._installed:
            root = logging.getLogger()
            root.addHandler(self)
            self._installed = True

    def uninstall(self):
        """Remove this handler from the root logger."""
        if self._installed:
            root = logging.getLogger()
            root.removeHandler(self)
            self._installed = False

    def emit(self, record):
        """Called by the logging framework for each log record."""
        entry = {
            'time': record.created,
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
        }
        with self._lock:
            self._buffer.append(entry)
            for q in self._subscribers:
                try:
                    q.put_nowait(entry)
                except queue.Full:
                    pass

    def get_recent(self, count=100):
        """Get the most recent log entries from the ring buffer."""
        with self._lock:
            items = list(self._buffer)
        return items[-count:]

    def subscribe(self):
        """Subscribe to the log stream. Returns a queue."""
        q = queue.Queue(maxsize=500)
        with self._lock:
            self._subscribers.append(q)
        return q

    def unsubscribe(self, q):
        """Unsubscribe from the log stream."""
        with self._lock:
            if q in self._subscribers:
                self._subscribers.remove(q)
