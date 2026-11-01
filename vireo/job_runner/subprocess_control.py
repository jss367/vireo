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
            os.killpg(proc.pid, signal.SIGTERM)
            deadline = time.monotonic() + 3
            while time.monotonic() < deadline:
                proc.poll()
                os.killpg(proc.pid, 0)
                time.sleep(0.05)
            os.killpg(proc.pid, signal.SIGKILL)
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
