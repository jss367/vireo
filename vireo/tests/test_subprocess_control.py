"""Process-group cleanup tolerates exit races without hiding live writers."""

import signal
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from job_runner import subprocess_control

SIGKILL = getattr(signal, "SIGKILL", 9)


@pytest.fixture
def group_signals(monkeypatch):
    clock = [0.0]
    signals = Mock()
    monkeypatch.setattr(subprocess_control, "sys", SimpleNamespace(platform="darwin"))
    monkeypatch.setattr(subprocess_control, "os", SimpleNamespace(killpg=signals))
    monkeypatch.setattr(subprocess_control, "signal", SimpleNamespace(
        SIGTERM=signal.SIGTERM, SIGKILL=SIGKILL,
    ))
    monkeypatch.setattr(subprocess_control, "time", SimpleNamespace(
        monotonic=lambda: clock[0],
        sleep=lambda seconds: clock.__setitem__(0, clock[0] + seconds),
    ))
    return signals, clock


@pytest.mark.parametrize("sig", [signal.SIGTERM, 0, SIGKILL])
def test_macos_zombie_group_is_reaped_and_rechecked(group_signals, sig):
    signals, clock = group_signals
    proc = Mock(pid=1234)
    proc.poll.return_value = -signal.SIGTERM
    signals.side_effect = [PermissionError(), PermissionError(), ProcessLookupError()]

    with pytest.raises(ProcessLookupError):
        subprocess_control._signal_process_group(proc, sig)

    assert signals.call_args_list == [((proc.pid, sig),)] * 3
    assert proc.poll.call_count == 2
    assert clock[0] == pytest.approx(0.1)


def test_macos_group_probe_exit_race_does_not_fail_cleanup(group_signals):
    signals, _ = group_signals
    proc = Mock(pid=1234)
    proc.poll.side_effect = [None, None, -signal.SIGTERM, -signal.SIGTERM]
    signals.side_effect = [None, PermissionError(), ProcessLookupError()]

    subprocess_control.stop_process_tree(proc)

    assert signals.call_args_list == [
        ((proc.pid, signal.SIGTERM),), ((proc.pid, 0),), ((proc.pid, 0),),
    ]
    proc.kill.assert_not_called()


def test_macos_surviving_group_is_still_killed_after_leader_exits(group_signals):
    signals, _ = group_signals
    proc = Mock(pid=1234)
    proc.poll.side_effect = [None, None] + [-signal.SIGTERM] * 100
    signals.side_effect = [None, PermissionError()] + [None] * 100

    subprocess_control.stop_process_tree(proc)

    signals.assert_called_with(proc.pid, SIGKILL)
    proc.kill.assert_not_called()


@pytest.mark.parametrize("sig", [signal.SIGTERM, 0, SIGKILL])
def test_macos_persistent_permission_denial_is_not_hidden(group_signals, sig):
    signals, clock = group_signals
    proc = Mock(pid=1234)
    # A dead leader does not prove that its descendants are gone.
    proc.poll.return_value = -signal.SIGTERM
    denial = PermissionError("live descendant cannot be signalled")
    signals.side_effect = denial

    with pytest.raises(PermissionError) as raised:
        subprocess_control._signal_process_group(proc, sig)

    assert raised.value is denial
    assert 3 <= clock[0] < 3.1


def test_other_posix_permission_denial_is_not_retried(group_signals, monkeypatch):
    signals, clock = group_signals
    proc = Mock(pid=1234)
    monkeypatch.setattr(subprocess_control, "sys", SimpleNamespace(platform="linux"))
    signals.side_effect = PermissionError()

    with pytest.raises(PermissionError):
        subprocess_control._signal_process_group(proc, signal.SIGTERM)

    signals.assert_called_once_with(proc.pid, signal.SIGTERM)
    proc.poll.assert_not_called()
    assert clock[0] == 0
