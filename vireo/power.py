"""Keep the system awake while long-running jobs are in flight.

A multi-hour import on battery is suspended by macOS idle
sleep roughly ten minutes in, and an SMB-over-Tailscale mount does not
survive the repeated sleep/DarkWake cycling — the job wakes to a share that
is no longer there.

Best-effort by design: if the platform tool is missing or fails, the job
still runs. Keeping the machine awake is a convenience; completing the
user's import is not.
"""

import logging
import os
import subprocess
import sys
import threading

log = logging.getLogger(__name__)


class _ProcessInhibitor:
    """An inhibitor backed by a child process (caffeinate, systemd-inhibit).

    A brief health check after Popen catches the case where the supervisor
    launches successfully but exits at once — systemd-inhibit does this
    when the bus is unavailable or the lock is denied, and caffeinate does
    it when ``-w`` points at a pid that has already gone. Without the
    check, ``self.proc`` stays non-null, ``SleepBlocker.active`` reports
    the machine as inhibited, ``/api/jobs`` says so, and the job runs
    unprotected anyway. A real supervisor sleeps forever (systemd-inhibit
    watching ``sleep infinity``) or waits on our pid (caffeinate -w), so
    the wait is a no-op for healthy processes.
    """

    _STARTUP_HEALTH_WAIT_S = 0.2

    def __init__(self, argv):
        self.proc = subprocess.Popen(
            argv,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        try:
            rc = self.proc.wait(timeout=self._STARTUP_HEALTH_WAIT_S)
        except subprocess.TimeoutExpired:
            return
        raise RuntimeError(
            f"inhibitor {argv[0]!r} exited immediately with rc={rc}; "
            "running unprotected"
        )

    def stop(self):
        if self.proc.poll() is not None:
            return
        self.proc.terminate()
        try:
            self.proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait(timeout=5)


class _WindowsInhibitor:
    """ES_CONTINUOUS | ES_SYSTEM_REQUIRED held on a dedicated thread.

    Deliberately omits ES_DISPLAY_REQUIRED: we keep the machine computing,
    we do not keep the screen lit.

    Why a dedicated thread: SetThreadExecutionState applies to the
    *calling* thread, and its ES_CONTINUOUS assertion is dropped by
    Windows when that thread terminates. This inhibitor is shared across
    JobRunner worker threads that come and go — if the first-acquiring
    worker finishes and its thread exits while another job is still
    holding the refcount, the OS assertion silently disappears and the
    machine sleeps mid-job. Pinning both the ``set`` and the ``clear``
    calls to a thread whose lifetime spans acquire → release is what
    keeps the assertion in force.
    """

    ES_CONTINUOUS = 0x80000000
    ES_SYSTEM_REQUIRED = 0x00000001

    _START_TIMEOUT_S = 5.0
    _STOP_TIMEOUT_S = 5.0

    def __init__(self):
        import ctypes

        self._ctypes = ctypes
        self._stop_event = threading.Event()
        self._ready_event = threading.Event()
        self._start_error = None
        self._thread = threading.Thread(
            target=self._run,
            name="vireo-sleep-inhibitor",
            daemon=True,
        )
        self._thread.start()
        if not self._ready_event.wait(timeout=self._START_TIMEOUT_S):
            self._stop_event.set()
            raise RuntimeError(
                "Windows sleep-inhibitor thread did not start in "
                f"{self._START_TIMEOUT_S}s"
            )
        if self._start_error is not None:
            raise self._start_error

    def _run(self):
        try:
            rv = self._ctypes.windll.kernel32.SetThreadExecutionState(
                self.ES_CONTINUOUS | self.ES_SYSTEM_REQUIRED
            )
            if rv == 0:
                # Docs: returns 0 on failure. Report so SleepBlocker can
                # fall back to unprotected rather than lying about the
                # machine being kept awake.
                self._start_error = OSError(
                    "SetThreadExecutionState returned 0"
                )
                return
        except Exception as exc:  # noqa: BLE001  # broad: ctypes raises a wide set; re-raised to the caller by start()
            self._start_error = exc
            return
        finally:
            self._ready_event.set()
        self._stop_event.wait()
        try:
            self._ctypes.windll.kernel32.SetThreadExecutionState(
                self.ES_CONTINUOUS
            )
        except Exception:  # broad: clearing the sleep assertion is best-effort on the way out
            log.warning(
                "Clearing Windows sleep assertion failed", exc_info=True,
            )

    def stop(self):
        self._stop_event.set()
        self._thread.join(timeout=self._STOP_TIMEOUT_S)


def start_platform_inhibitor(reason):
    """Start an OS-level idle-sleep inhibitor and return a handle.

    Raises on failure; ``SleepBlocker`` treats that as "run unprotected".

    Every platform blocks *idle* sleep only. Closing the lid, choosing
    Sleep from the menu, or a low-battery sleep all still work — the user
    asking the machine to sleep outranks a background job.
    """
    if sys.platform == "darwin":
        # -i: prevent idle system sleep. Not -s (that also covers the
        # on-AC case we do not need) and not -d (display).
        # -w <pid>: exit when Vireo exits, so a crash cannot strand a
        # caffeinate process holding the machine awake indefinitely.
        return _ProcessInhibitor(
            ["caffeinate", "-i", "-w", str(os.getpid())]
        )
    if sys.platform == "win32":
        return _WindowsInhibitor()
    # Linux and other POSIX. --what=idle leaves lid/power-button handling
    # alone. systemd-inhibit needs a command to supervise; ``tail --pid``
    # follows /dev/null until Vireo's pid exits and then returns, so the
    # supervised command dies whenever Vireo dies (SIGTERM from
    # ``/api/shutdown``, an OOM kill, a crash), systemd-inhibit releases
    # the lock, and the inhibitor process reaps. ``sleep infinity`` would
    # instead ignore Vireo's death entirely — the JobRunner worker
    # threads are daemons so their ``finally`` blocks are not guaranteed
    # to run on interpreter shutdown, and PR_SET_PDEATHSIG cannot help
    # because it fires when the *thread* that forked exits (each job
    # worker), not when the Vireo process exits.
    return _ProcessInhibitor([
        "systemd-inhibit",
        "--what=idle",
        "--who=Vireo",
        f"--why={reason}",
        "--mode=block",
        "tail", "--pid", str(os.getpid()), "-f", "/dev/null",
    ])


class SleepBlocker:
    """Refcounted idle-sleep inhibitor.

    Holders come and go; the underlying OS inhibitor starts on the first
    acquire and stops on the last release.
    """

    def __init__(self, start_inhibitor):
        self._start_inhibitor = start_inhibitor
        self._handle = None
        self._count = 0
        # ``self._count += 1`` is load/add/store, not atomic. Jobs acquire
        # and release from their own worker threads, so a lost update
        # either strands the inhibitor on forever (machine never idles) or
        # drops it while a job is still running (the bug we are fixing).
        self._lock = threading.Lock()

    @property
    def active(self):
        return self._handle is not None

    def acquire(self):
        with self._lock:
            self._count += 1
            if self._count != 1 or self._handle is not None:
                return
            try:
                self._handle = self._start_inhibitor(
                    "Vireo is running a job"
                )
            except Exception:
                # broad: missing caffeinate/systemd-inhibit, denied permission,
                # unsupported platform. Log once and carry on unprotected
                # rather than failing the job.
                log.warning(
                    "Could not inhibit idle sleep; a long job may be "
                    "suspended if this machine sleeps",
                    exc_info=True,
                )
                self._handle = None

    def release(self):
        with self._lock:
            if self._count == 0:
                # Unbalanced release. Clamping at zero matters: a negative
                # count would make the next real acquire a no-op and leave
                # a long job unprotected.
                return
            self._count -= 1
            if self._count != 0:
                return
            handle, self._handle = self._handle, None
            if handle is None:
                return
            try:
                handle.stop()
            except Exception:
                # broad: already reaped, or the platform call failed. The handle
                # is dropped either way — holding onto it would leave the
                # blocker permanently "active" and stop it ever inhibiting
                # again.
                log.warning(
                    "Failed to release idle-sleep inhibitor", exc_info=True,
                )
