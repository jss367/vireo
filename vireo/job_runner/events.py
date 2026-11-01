"""Per-job event buffers and the SSE subscriber queues they fan out to."""

import logging
import queue
import time
from collections import deque

# These lines were logged by ``jobs.JobRunner``; keep the logger name the log
# and its readers know them by.
log = logging.getLogger("jobs")


class EventStreams:
    """Owns each job's buffered events and its subscriber queues.

    ``events`` (job_id -> deque of events) and ``subscribers`` (job_id ->
    list of queues) are guarded by the runner lock it is given. Reads the
    runner's job registry to mirror progress onto the job.
    """

    def __init__(self, lock, jobs):
        self._lock = lock
        self._jobs = jobs
        self.events = {}  # job_id -> deque of events
        self.subscribers = {}  # job_id -> list of queues

    def open_locked(self, job_id):
        """Give a newly registered job a fresh buffer and subscriber list.

        Must be called with the runner lock held.
        """
        self.events[job_id] = deque(maxlen=1000)
        self.subscribers[job_id] = []

    def open_promoted_locked(self, job_id):
        """Give a promoted pipeline a fresh buffer, keeping its subscribers.

        Must be called with the runner lock held.
        """
        self.events[job_id] = deque(maxlen=1000)
        # setdefault, NOT assignment: clients can subscribe to the
        # SSE stream while the pipeline is still queued. Replacing
        # the list at promotion time would silently drop those
        # waiters' queues.
        self.subscribers.setdefault(job_id, [])

    def ensure_locked(self, job_id):
        """Make sure ``job_id`` has a buffer and subscriber list.

        Must be called with the runner lock held.
        """
        self.events.setdefault(job_id, deque(maxlen=1000))
        self.subscribers.setdefault(job_id, [])

    def drop_locked(self, job_id):
        """Forget a job's buffer and subscribers.

        Must be called with the runner lock held.
        """
        self.events.pop(job_id, None)
        self.subscribers.pop(job_id, None)

    def push(self, job_id, event_type, data):
        """Push an event to the job's event stream."""
        is_critical = event_type in ("complete", "error")
        with self._lock:
            if event_type == "progress":
                job = self._jobs.get(job_id)
                if job is not None:
                    # Mirror latest progress fields onto job["progress"] so
                    # clients polling /api/jobs or /api/jobs/history see the
                    # current phase/current_file without needing SSE.
                    prog = job.setdefault(
                        "progress",
                        {"current": 0, "total": 0, "current_file": ""},
                    )
                    for key, value in data.items():
                        if key == "steps":
                            continue
                        prog[key] = value
                if job and job.get("steps"):
                    data = dict(data)
                    data["steps"] = [dict(s) for s in job["steps"]]
            event = {"type": event_type, "data": data, "time": time.time()}
            if job_id in self.events:
                self.events[job_id].append(event)
            # Snapshot subscriber list so we can deliver outside the lock
            subscribers = list(self.subscribers.get(job_id, []))

        # Deliver to subscribers outside the lock to avoid blocking
        for q in subscribers:
            if is_critical:
                # Critical events must not be dropped
                try:
                    q.put(event, timeout=5)
                except queue.Full:
                    log.warning(
                        "Failed to deliver critical '%s' event for job %s "
                        "after 5s — subscriber queue full",
                        event_type, job_id,
                    )
            else:
                try:
                    q.put_nowait(event)
                except queue.Full:
                    log.debug(
                        "Dropped '%s' event for job %s — subscriber queue full",
                        event_type, job_id,
                    )

    def publish_status_locked(self, job, status):
        """Change *job* status and publish its event as one locked action.

        The caller must hold the runner lock (directly or through
        ``_pause_condition``). Keeping the state change, buffered event,
        and non-blocking subscriber delivery under that same lock prevents
        competing pause, resume, and completion paths from publishing stale
        status events out of order.
        """
        job_id = job["id"]
        job["status"] = status
        event = {
            "type": "status",
            "data": {"job_id": job_id, "status": status,
                     "pausable": job.get("pausable", False),
                     "cancellable": job.get("cancellable", True),
                     "control_unavailable_reason": job.get("control_unavailable_reason")},
            "time": time.time(),
        }
        if job_id in self.events:
            self.events[job_id].append(event)
        for subscriber in self.subscribers.get(job_id, []):
            try:
                subscriber.put_nowait(event)
            except queue.Full:
                log.debug(
                    "Dropped 'status' event for job %s — subscriber queue full",
                    job_id,
                )

    def get_events(self, job_id):
        """Get all buffered events for a job."""
        with self._lock:
            return list(self.events.get(job_id, []))

    def subscribe(self, job_id):
        """Subscribe to a job's event stream. Returns a queue."""
        q = queue.Queue(maxsize=200)
        with self._lock:
            if job_id not in self.subscribers:
                self.subscribers[job_id] = []
            self.subscribers[job_id].append(q)
        return q

    def unsubscribe(self, job_id, q):
        """Unsubscribe from a job's event stream."""
        with self._lock:
            subs = self.subscribers.get(job_id, [])
            if q in subs:
                subs.remove(q)
