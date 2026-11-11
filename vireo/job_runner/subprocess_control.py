"""Stop and reap external work before publishing a cooperative pause."""

import os
import signal
import subprocess
import sys
import time

from proc import no_window_kwargs


def process_group_kwargs():
    """Keep POSIX subprocesses and their helpers in an owned process group."""
    return {"start_new_session": True} if sys.platform != "win32" else {}


def _signal_process_group(proc, sig):
    """Reap through macOS's transient EPERM for zombie-only process groups."""
    deadline = time.monotonic() + 3
    while True:
        try:
            os.killpg(proc.pid, sig)
            return
        except PermissionError:
            # XNU excludes zombies when checking group signal permissions.
            # Reaping our child lets an empty group disappear; never infer
            # that surviving descendants are stopped from the leader alone.
            if sys.platform != "darwin" or time.monotonic() >= deadline:
                raise
            proc.poll()
            time.sleep(0.05)


def stop_process_tree(proc):
    """Stop local descendants too, including rsync's SSH transport."""
    if proc.poll() is not None:
        return
    if sys.platform == "win32":
        try:
            subprocess.run(
                ["taskkill", "/PID", str(proc.pid), "/T", "/F"],
                capture_output=True, timeout=15, **no_window_kwargs(),
            )
        except (OSError, subprocess.TimeoutExpired):
            proc.kill()
    else:
        try:
            # Give rsync a chance to save its partial file and shut down the
            # receiver/SSH transport. Escalate for tools that ignore TERM.
            _signal_process_group(proc, signal.SIGTERM)
            deadline = time.monotonic() + 3
            while time.monotonic() < deadline:
                proc.poll()
                _signal_process_group(proc, 0)
                time.sleep(0.05)
            _signal_process_group(proc, signal.SIGKILL)
        except ProcessLookupError:
            pass
    # Also handles a Windows taskkill that returned without killing its root.
    if proc.poll() is None:
        proc.kill()


def run_controlled_process(cmd, control, *, timeout):
    """Run a read-only subprocess, restarting its work after any pause.

    communicate drains both streams. No checkpoint can publish ``paused``
    until the process has exited and both output streams are closed.
    """
    while True:
        control.checkpoint()
        with subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
            **no_window_kwargs(), **process_group_kwargs(),
        ) as proc:
            deadline = time.monotonic() + timeout
            interrupted = False
            try:
                while True:
                    if control.stop_requested():
                        interrupted = True
                        stop_process_tree(proc)
                    remaining = deadline - time.monotonic()
                    if remaining <= 0 and not interrupted:
                        raise subprocess.TimeoutExpired(cmd, timeout)
                    try:
                        stdout, stderr = proc.communicate(timeout=0.2)
                        break
                    except subprocess.TimeoutExpired:
                        continue
            finally:
                if proc.poll() is None:
                    stop_process_tree(proc)
                    proc.communicate()
        if not interrupted:
            return subprocess.CompletedProcess(cmd, proc.returncode, stdout, stderr)
        control.checkpoint()
