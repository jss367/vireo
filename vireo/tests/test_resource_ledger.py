import os
import sys
import threading

import pytest
from testing.waits import synchronization_timeout

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import resource_ledger
from resource_ledger import (
    CpuRequest,
    ResourceLedger,
    ResourceRequest,
    ResourceWaitCancelled,
    bind_resource_cancel_check,
    bind_resource_owner,
)


def test_automatic_capacity_reserves_interactive_cores():
    # ``usable_cores=None`` / ``usable_physical_cores=None`` bypass the
    # process-affinity clamps so this exercises the reserve math on the
    # raw host topology — otherwise a 2-vCPU CI runner would clamp the
    # input to 2 and derive capacity 1, measuring the runner's
    # constraint instead of the reserve formula.
    assert resource_ledger.automatic_cpu_capacity(
        physical_cores=16, usable_cores=None, usable_physical_cores=None,
    ) == 12
    assert resource_ledger.automatic_cpu_capacity(
        physical_cores=8, usable_cores=None, usable_physical_cores=None,
    ) == 6


def test_automatic_capacity_survives_unavailable_core_counts(monkeypatch):
    monkeypatch.setattr(resource_ledger, "detect_physical_core_count", lambda: None)
    monkeypatch.setattr(resource_ledger.os, "cpu_count", lambda: None)
    assert resource_ledger.automatic_cpu_capacity(
        usable_cores=None, usable_physical_cores=None,
    ) == 1


def test_automatic_capacity_clamps_to_process_affinity():
    """A process constrained via ``taskset`` / systemd / container cpuset
    / cgroup CPU quota must derive its capacity from the CPUs it can
    actually schedule on, not from the host's full topology. Otherwise
    a 32-core host running Vireo under ``taskset -c 0,1`` would create
    dozens of scanner workers and ONNX threads for a 2-CPU sandbox and
    defeat both the process-wide budget and the interactive reserve.
    """
    # Simulate a 16-core host with the process pinned to 4 usable CPUs.
    # Without the clamp this would return 12 (16-core reserve math);
    # with the clamp it must derive from 4 → reserve 2 → capacity 2.
    assert resource_ledger.automatic_cpu_capacity(
        physical_cores=16, usable_cores=4, usable_physical_cores=None,
    ) == 2

    # Larger usable_cores than physical_cores is a no-op: the smaller
    # bound wins, so a container with a generous quota on a small host
    # still respects the host topology.
    assert resource_ledger.automatic_cpu_capacity(
        physical_cores=8, usable_cores=32, logical_cores=8,
        usable_physical_cores=None,
    ) == 6


def test_automatic_capacity_prefers_physical_over_logical_usable():
    """Regression: when the affinity set covers hyperthread siblings of
    the same physical cores (SMT-heavy pinning), ``usable_cores``
    reports logical CPUs while capacity sizing must use physical
    cores — the reserve formula is calibrated against them. Without a
    separate physical-usable clamp, eight allowed logical CPUs covering
    four physical cores on a 16-core host would give ``cores=8`` and a
    6-permit budget, oversubscribing the same four physical cores the
    ONNX pool actually runs on. With the physical clamp: ``cores=4``,
    reserve=2, budget=2.
    """
    assert resource_ledger.automatic_cpu_capacity(
        physical_cores=16, usable_cores=8, usable_physical_cores=4,
    ) == 2

    # And the physical clamp cannot make the answer larger than the
    # logical clamp when logical is tighter (e.g. a cgroup quota
    # below the affinity set): min of both still wins.
    assert resource_ledger.automatic_cpu_capacity(
        physical_cores=16, usable_cores=2, usable_physical_cores=4,
    ) == 1  # min(16, 4, 2)=2, reserve max(2, 0.4)=2, budget max(1, 2-2)=1

    # Physical clamp absent (Darwin/Windows/no affinity API) → fall
    # back to logical-only clamp behaviour.
    assert resource_ledger.automatic_cpu_capacity(
        physical_cores=16, usable_cores=4, usable_physical_cores=None,
    ) == 2


def test_process_usable_physical_cpu_count_returns_positive_int_or_none():
    """The helper returns a positive int on Linux with affinity support
    and ``None`` elsewhere. A zero or negative value would silently
    corrupt the derived capacity (callers gate on truthiness), and any
    non-integer type would trip the downstream ``min``.
    """
    result = resource_ledger.process_usable_physical_cpu_count()
    assert result is None or (isinstance(result, int) and result >= 1)


def test_linux_physical_core_count_filters_by_affinity_collapses_smt(
    monkeypatch,
):
    """Regression: SMT siblings pinned to the same physical core count
    once when the affinity filter is applied. Simulate a 4-package
    layout where physical ids 0-3 each expose two hyperthreads
    (processors 0-7 map to (physical_id, core_id) tuples
    (0,0), (1,0), (2,0), (3,0), (0,0), (1,0), (2,0), (3,0) — HT siblings
    reuse the same (physical_id, core_id)). An affinity of {0,4} covers
    both HTs of physical core (0,0) — the count must be 1, not 2.
    """
    cpuinfo = "".join(
        # Each processor entry: processor: N \n physical id: P \n core id: C
        # HT sibling pairs share (physical_id, core_id). We define 8
        # processors mapping to 4 unique (physical_id, core_id) pairs.
        f"processor\t: {p}\nphysical id\t: {p % 4}\ncore id\t: 0\n\n"
        for p in range(8)
    )
    monkeypatch.setattr(
        "builtins.open",
        _make_open_stub({"/proc/cpuinfo": cpuinfo}),
    )
    # No filter: all 4 unique (physical_id, core_id) pairs count.
    assert resource_ledger._linux_physical_core_count() == 4
    # Affinity {0, 4}: both are on (physical_id=0, core_id=0) — 1 core.
    assert resource_ledger._linux_physical_core_count(
        processor_filter={0, 4},
    ) == 1
    # Affinity {0, 1}: (physical_id=0, core_id=0) and (physical_id=1,
    # core_id=0) — 2 distinct physical cores.
    assert resource_ledger._linux_physical_core_count(
        processor_filter={0, 1},
    ) == 2


def test_process_usable_cpu_count_returns_positive_int():
    """The helper must return a positive integer or ``None`` — never a
    zero, negative value, or arbitrary object. Callers gate on
    truthiness before clamping, so a zero would be equivalent to
    'unknown' anyway, but a nonsense value would silently corrupt the
    derived capacity.
    """
    result = resource_ledger.process_usable_cpu_count()
    assert result is None or (isinstance(result, int) and result >= 1)


def _make_open_stub(files):
    """Return a fake ``open`` that serves in-memory contents for known paths.

    Any path not present raises ``FileNotFoundError`` so the caller's
    ``except OSError`` branch fires — mirrors a real system where the
    cgroup interface just isn't there.
    """
    import io

    def _fake_open(path, *args, **kwargs):
        if path in files:
            return io.StringIO(files[path])
        raise FileNotFoundError(path)

    return _fake_open


def test_cgroup_cpu_quota_reads_v2_unified_interface(monkeypatch):
    """cgroup v2 exposes ``<quota> <period>`` in ``/sys/fs/cgroup/cpu.max``.

    Simulate a root-level cgroup (no /proc/self/cgroup subpath), so the
    quota lookup falls through to the root ``cpu.max``.
    """
    monkeypatch.setattr(
        "builtins.open",
        _make_open_stub({
            "/proc/self/cgroup": "0::/\n",
            "/sys/fs/cgroup/cpu.max": "200000 100000\n",
        }),
    )
    # 200000 / 100000 = 2 CPUs (Docker --cpus=2 equivalent).
    assert resource_ledger._cgroup_cpu_quota_cpus() == 2


def test_cgroup_cpu_quota_v2_unlimited_returns_none(monkeypatch):
    """``max <period>`` means unlimited — no CPU ceiling, return ``None``."""
    monkeypatch.setattr(
        "builtins.open",
        _make_open_stub({
            "/proc/self/cgroup": "0::/\n",
            "/sys/fs/cgroup/cpu.max": "max 100000\n",
        }),
    )
    assert resource_ledger._cgroup_cpu_quota_cpus() is None


def test_cgroup_cpu_quota_v2_fractional_rounds_up(monkeypatch):
    """``--cpus=1.5`` -> 150000/100000. Round up so a whole-CPU decision
    doesn't accidentally treat 1.5 as 1 (which would waste headroom).
    """
    monkeypatch.setattr(
        "builtins.open",
        _make_open_stub({
            "/proc/self/cgroup": "0::/\n",
            "/sys/fs/cgroup/cpu.max": "150000 100000\n",
        }),
    )
    assert resource_ledger._cgroup_cpu_quota_cpus() == 2


def test_cgroup_cpu_quota_v2_uses_process_cgroup_path_not_root(monkeypatch):
    """Regression: nested cgroup v2 (systemd service, containerd) stores
    the effective quota under the process's own cgroup path, not the
    hierarchy root. A ``vireo.service`` with ``CPUQuota=200%`` has
    ``/sys/fs/cgroup/system.slice/vireo.service/cpu.max`` = ``200000
    100000`` while ``/sys/fs/cgroup/cpu.max`` reads ``max <period>``.
    Reading only the root would silently fall back to the affinity
    count and derive a much larger CPU budget than the service is
    allowed. Resolve the current cgroup path from ``/proc/self/cgroup``.
    """
    monkeypatch.setattr(
        "builtins.open",
        _make_open_stub({
            "/proc/self/cgroup": "0::/system.slice/vireo.service\n",
            "/sys/fs/cgroup/cpu.max": "max 100000\n",
            "/sys/fs/cgroup/system.slice/cpu.max": "max 100000\n",
            "/sys/fs/cgroup/system.slice/vireo.service/cpu.max": (
                "200000 100000\n"
            ),
        }),
    )
    assert resource_ledger._cgroup_cpu_quota_cpus() == 2


def test_cgroup_cpu_quota_v2_takes_min_of_ancestor_chain(monkeypatch):
    """cgroup enforces the tightest quota along the ancestor chain. A
    slice with ``CPUQuota=400%`` containing a service with
    ``CPUQuota=200%`` gives an effective 2 CPUs, but a service with
    ``CPUQuota=800%`` under the same 400%-slice gives an effective
    4 CPUs. Walk every ancestor and take the ``min`` so ancestor caps
    apply even when the leaf's own quota is looser.
    """
    monkeypatch.setattr(
        "builtins.open",
        _make_open_stub({
            "/proc/self/cgroup": "0::/system.slice/vireo.service\n",
            "/sys/fs/cgroup/cpu.max": "max 100000\n",
            "/sys/fs/cgroup/system.slice/cpu.max": "400000 100000\n",
            "/sys/fs/cgroup/system.slice/vireo.service/cpu.max": (
                "800000 100000\n"
            ),
        }),
    )
    # Leaf allows 8, ancestor slice caps at 4 — min wins.
    assert resource_ledger._cgroup_cpu_quota_cpus() == 4


def test_cgroup_cpu_quota_falls_back_to_v1_split(monkeypatch):
    """cgroup v1 has separate quota and period files, and ``-1`` for
    quota means unlimited — verify the v1 pair parses when v2 is absent
    and unlimited surfaces as ``None`` on this legacy interface too.
    """
    monkeypatch.setattr(
        "builtins.open",
        _make_open_stub({
            # v1 process cgroup identification.
            "/proc/self/cgroup": (
                "9:cpu,cpuacct:/\n"
            ),
            "/sys/fs/cgroup/cpu/cpu.cfs_quota_us": "400000\n",
            "/sys/fs/cgroup/cpu/cpu.cfs_period_us": "100000\n",
        }),
    )
    assert resource_ledger._cgroup_cpu_quota_cpus() == 4

    monkeypatch.setattr(
        "builtins.open",
        _make_open_stub({
            "/proc/self/cgroup": "9:cpu,cpuacct:/\n",
            "/sys/fs/cgroup/cpu/cpu.cfs_quota_us": "-1\n",
            "/sys/fs/cgroup/cpu/cpu.cfs_period_us": "100000\n",
        }),
    )
    assert resource_ledger._cgroup_cpu_quota_cpus() is None


def test_ancestor_dirs_ignores_non_absolute_paths():
    """Regression: a stubbed or malformed ``/proc/self/cgroup`` line
    could hand ``_ancestor_dirs`` a non-absolute path like ``"foo"``.
    ``"foo".rsplit("/", 1)[0]`` returns ``"foo"``, so the ancestor walk
    would loop forever and the caller (unbounded ``for``) would hang
    process startup. Guarded by returning without yielding on any
    input that does not begin with ``/``.
    """
    assert list(resource_ledger._ancestor_dirs("")) == []
    assert list(resource_ledger._ancestor_dirs("foo")) == []
    assert list(resource_ledger._ancestor_dirs("foo/bar")) == []
    # Absolute paths still walk as before.
    assert list(resource_ledger._ancestor_dirs("/")) == ["/"]
    assert list(resource_ledger._ancestor_dirs("/a/b")) == ["/a/b", "/a", "/"]


def test_cgroup_cpu_quota_v1_reads_comounted_cpu_cpuacct(monkeypatch):
    """Regression: on Ubuntu/Debian/Alpine/older-Fedora hosts, the
    ``cpu`` and ``cpuacct`` controllers are co-mounted at
    ``/sys/fs/cgroup/cpu,cpuacct`` — ``/sys/fs/cgroup/cpu`` does not
    exist on those systems even though ``/proc/self/cgroup`` reports
    ``cpu,cpuacct`` as the controller list. Reading only the
    ``/sys/fs/cgroup/cpu`` path would return ``None`` for a container
    with a CFS limit on such a host, and the ledger would fall back to
    the affinity count. Probe the co-mounted directory too so the
    quota is still detected.
    """
    monkeypatch.setattr(
        "builtins.open",
        _make_open_stub({
            "/proc/self/cgroup": "9:cpu,cpuacct:/\n",
            # /sys/fs/cgroup/cpu deliberately absent — only the
            # co-mounted controller directory has the quota files.
            "/sys/fs/cgroup/cpu,cpuacct/cpu.cfs_quota_us": "200000\n",
            "/sys/fs/cgroup/cpu,cpuacct/cpu.cfs_period_us": "100000\n",
        }),
    )
    assert resource_ledger._cgroup_cpu_quota_cpus() == 2


def test_cgroup_cpu_quota_v1_comounted_nested_walks_ancestors(monkeypatch):
    """Same ancestor-walk semantics on the co-mounted layout: a nested
    service under ``system.slice`` on ``cpu,cpuacct`` picks up the
    tightest cap along the chain, and the mount path for every
    ancestor uses the co-mounted name.
    """
    monkeypatch.setattr(
        "builtins.open",
        _make_open_stub({
            "/proc/self/cgroup": (
                "9:cpu,cpuacct:/system.slice/vireo.service\n"
            ),
            "/sys/fs/cgroup/cpu,cpuacct/cpu.cfs_quota_us": "-1\n",
            "/sys/fs/cgroup/cpu,cpuacct/cpu.cfs_period_us": "100000\n",
            "/sys/fs/cgroup/cpu,cpuacct/system.slice"
            "/cpu.cfs_quota_us": "400000\n",
            "/sys/fs/cgroup/cpu,cpuacct/system.slice"
            "/cpu.cfs_period_us": "100000\n",
            "/sys/fs/cgroup/cpu,cpuacct/system.slice/vireo.service"
            "/cpu.cfs_quota_us": "800000\n",
            "/sys/fs/cgroup/cpu,cpuacct/system.slice/vireo.service"
            "/cpu.cfs_period_us": "100000\n",
        }),
    )
    # Leaf allows 8, ancestor slice caps at 4 — min wins across the
    # ancestor chain on the co-mounted layout.
    assert resource_ledger._cgroup_cpu_quota_cpus() == 4


def test_cgroup_cpu_quota_v1_nested_walks_ancestors(monkeypatch):
    """v1 nested hierarchies work the same as v2 — ``vireo.service``
    under ``system.slice`` looks up the tightest quota along the chain.
    """
    monkeypatch.setattr(
        "builtins.open",
        _make_open_stub({
            "/proc/self/cgroup": (
                "9:cpu,cpuacct:/system.slice/vireo.service\n"
            ),
            "/sys/fs/cgroup/cpu/cpu.cfs_quota_us": "-1\n",
            "/sys/fs/cgroup/cpu/cpu.cfs_period_us": "100000\n",
            "/sys/fs/cgroup/cpu/system.slice/cpu.cfs_quota_us": "-1\n",
            "/sys/fs/cgroup/cpu/system.slice/cpu.cfs_period_us": (
                "100000\n"
            ),
            "/sys/fs/cgroup/cpu/system.slice/vireo.service"
            "/cpu.cfs_quota_us": "200000\n",
            "/sys/fs/cgroup/cpu/system.slice/vireo.service"
            "/cpu.cfs_period_us": "100000\n",
        }),
    )
    assert resource_ledger._cgroup_cpu_quota_cpus() == 2


def test_cgroup_cpu_quota_v2_delegated_subtree_uses_mount_root(monkeypatch):
    """Regression: a container with a delegated cgroup subtree has
    ``/proc/self/cgroup`` reporting the FULL cgroup path (e.g.
    ``/docker/<id>``) while ``/proc/self/mountinfo`` shows that
    subtree mounted AS the root of ``/sys/fs/cgroup``. Concatenating
    ``/sys/fs/cgroup`` + ``/docker/<id>`` + ``/cpu.max`` probes a file
    that does not exist in the container's mount namespace; the
    effective file is at ``/sys/fs/cgroup/cpu.max``.

    Prior to the mount-root translation the quota lookup returned
    ``None`` for these containers, and ``automatic_cpu_capacity`` fell
    back to the affinity count — which for a Docker container using
    ``--cpuset-cpus`` alongside ``--cpus`` still reports the wider
    cpuset. Result: derived CPU budget exceeds the enforced quota.
    """
    monkeypatch.setattr(
        "builtins.open",
        _make_open_stub({
            "/proc/self/cgroup": "0::/docker/abc123\n",
            "/proc/self/mountinfo": (
                # Mount root is /docker/abc123, mount point is
                # /sys/fs/cgroup — the effective cpu.max is at
                # /sys/fs/cgroup/cpu.max, NOT
                # /sys/fs/cgroup/docker/abc123/cpu.max.
                "27 25 0:22 /docker/abc123 /sys/fs/cgroup "
                "rw,nosuid,nodev,noexec - cgroup2 cgroup2 "
                "rw,nsdelegate\n"
            ),
            "/sys/fs/cgroup/cpu.max": "200000 100000\n",
        }),
    )
    # 200000 / 100000 = 2 CPUs — the container's enforced ceiling.
    assert resource_ledger._cgroup_cpu_quota_cpus() == 2


def test_cgroup_cpu_quota_v2_delegated_subtree_walks_ancestors_within_mount(
    monkeypatch,
):
    """A container with a nested cgroup inside its delegated subtree
    (e.g. ``/docker/abc/svc``) must walk ancestors DOWN TO the mount
    point, not past it. Walking above the mount would leak into the
    host's cgroup files (or read nothing) — neither matches the
    "tightest along the chain" semantic for a process confined to the
    delegated subtree.
    """
    monkeypatch.setattr(
        "builtins.open",
        _make_open_stub({
            "/proc/self/cgroup": "0::/docker/abc/svc\n",
            "/proc/self/mountinfo": (
                "27 25 0:22 /docker/abc /sys/fs/cgroup "
                "rw,nosuid,nodev,noexec - cgroup2 cgroup2 rw\n"
            ),
            # svc is 4 CPUs
            "/sys/fs/cgroup/svc/cpu.max": "400000 100000\n",
            # The mount point (the ancestor above svc INSIDE the mount)
            # is 2 CPUs — the tightest.
            "/sys/fs/cgroup/cpu.max": "200000 100000\n",
        }),
    )
    assert resource_ledger._cgroup_cpu_quota_cpus() == 2


def test_cgroup_cpu_quota_v1_delegated_subtree_uses_mount_root(monkeypatch):
    """Same delegated-subtree translation for cgroup v1. A container
    with a v1 cpu controller exposed at ``/sys/fs/cgroup/cpu`` whose
    mount root is ``/docker/<id>`` reads its quota from that mount
    point, not from ``/sys/fs/cgroup/cpu/docker/<id>``.
    """
    monkeypatch.setattr(
        "builtins.open",
        _make_open_stub({
            "/proc/self/cgroup": (
                "3:cpu,cpuacct:/docker/abc123\n"
            ),
            "/proc/self/mountinfo": (
                "27 25 0:22 /docker/abc123 /sys/fs/cgroup/cpu,cpuacct "
                "rw - cgroup cgroup rw,cpu,cpuacct\n"
            ),
            "/sys/fs/cgroup/cpu,cpuacct/cpu.cfs_quota_us": "200000\n",
            "/sys/fs/cgroup/cpu,cpuacct/cpu.cfs_period_us": "100000\n",
        }),
    )
    assert resource_ledger._cgroup_cpu_quota_cpus() == 2


def test_cgroup_cpu_quota_mountinfo_absent_preserves_legacy_behavior(
    monkeypatch,
):
    """Without ``/proc/self/mountinfo`` (missing or unreadable) the
    helper falls back to the historical ``/sys/fs/cgroup{path}`` layout.
    Guarantees the pre-parser test corpus still describes real behavior
    on any Linux where mountinfo is available but the delegated-subtree
    edge case doesn't apply.
    """
    monkeypatch.setattr(
        "builtins.open",
        _make_open_stub({
            "/proc/self/cgroup": "0::/system.slice/vireo.service\n",
            # No /proc/self/mountinfo — parser returns [], []; the
            # helper falls back to ("/", "/sys/fs/cgroup").
            "/sys/fs/cgroup/system.slice/vireo.service/cpu.max": (
                "200000 100000\n"
            ),
        }),
    )
    assert resource_ledger._cgroup_cpu_quota_cpus() == 2


def test_cgroup_cpu_quota_missing_files_returns_none(monkeypatch):
    """Darwin/Windows/host-linux without cgroups: both paths raise
    ``OSError``, and the helper returns ``None`` so the caller falls
    back to affinity-based counts.
    """
    monkeypatch.setattr("builtins.open", _make_open_stub({}))
    assert resource_ledger._cgroup_cpu_quota_cpus() is None


def test_process_usable_cpu_count_takes_minimum_of_affinity_and_cgroup(
    monkeypatch,
):
    """When a Docker container imposes a CFS quota WITHOUT narrowing the
    process's affinity set (``docker run --cpus=2`` on a 16-core host),
    the affinity signal reports 16 while the cgroup quota reports 2.
    The helper must return the ``min`` so
    ``automatic_cpu_capacity`` clamps by the true ceiling. Without this
    combined view, a 16-core-affinity + 2-cgroup-quota container derives
    a 12-permit budget and oversubscribes its two-CPU allocation.
    """
    # Simulate a host with wide affinity (16 CPUs) but a 2-CPU cgroup
    # quota. ``os.process_cpu_count`` only exists on Python 3.13+; on
    # 3.11/3.12 the helper falls through to ``os.sched_getaffinity`` on
    # Linux. Patch BOTH surfaces (with ``raising=False`` so a missing
    # attribute on the current interpreter is added rather than an
    # error) so the test asserts the same clamped result on every
    # supported Python version regardless of which branch the helper
    # takes.
    monkeypatch.setattr(
        resource_ledger.os, "process_cpu_count", lambda: 16, raising=False,
    )
    monkeypatch.setattr(
        resource_ledger.os,
        "sched_getaffinity", lambda _pid: set(range(16)), raising=False,
    )
    monkeypatch.setattr(
        "builtins.open",
        _make_open_stub({
            "/proc/self/cgroup": "0::/\n",
            "/sys/fs/cgroup/cpu.max": "200000 100000\n",
        }),
    )
    assert resource_ledger.process_usable_cpu_count() == 2

    # And with a 4-CPU affinity + 8-CPU quota, affinity wins.
    monkeypatch.setattr(
        resource_ledger.os, "process_cpu_count", lambda: 4, raising=False,
    )
    monkeypatch.setattr(
        resource_ledger.os,
        "sched_getaffinity", lambda _pid: set(range(4)), raising=False,
    )
    monkeypatch.setattr(
        "builtins.open",
        _make_open_stub({
            "/proc/self/cgroup": "0::/\n",
            "/sys/fs/cgroup/cpu.max": "800000 100000\n",
        }),
    )
    assert resource_ledger.process_usable_cpu_count() == 4


@pytest.mark.parametrize("value", [True, False, 1.5, "2", None])
def test_cpu_request_rejects_non_integer_permits(value):
    with pytest.raises(TypeError):
        CpuRequest(value, value, value)


def test_cpu_grants_preferred_then_available_above_minimum():
    ledger = ResourceLedger(cpu_capacity=6)
    outer_request = ResourceRequest(cpu=CpuRequest(2, 4, 6))
    inner_request = ResourceRequest(cpu=CpuRequest(2, 4, 4))

    with ledger.acquire(outer_request) as outer:
        assert outer.cpu_permits == 4
        with ledger.acquire(inner_request) as inner:
            assert inner.cpu_permits == 2
            assert ledger.snapshot()["cpu"] == {
                "capacity": 6, "allocated": 6, "available": 0,
            }
        assert ledger.snapshot()["cpu"]["allocated"] == 4
    assert ledger.snapshot()["cpu"]["allocated"] == 0


def test_cpu_and_lane_claim_waits_without_partial_allocation():
    ledger = ResourceLedger(cpu_capacity=2)
    lane_holder = ledger.acquire(ResourceRequest(lanes=("cpu_ml",)))
    waiting = threading.Event()
    acquired = threading.Event()

    def claim_both():
        request = ResourceRequest(
            cpu=CpuRequest(2, 2, 2), lanes=("cpu_ml",),
        )
        with ledger.acquire(request, on_wait=lambda _request: waiting.set()):
            acquired.set()

    thread = threading.Thread(target=claim_both)
    thread.start()
    assert waiting.wait(timeout=synchronization_timeout(1.0))
    assert ledger.snapshot()["cpu"]["allocated"] == 0
    assert not acquired.is_set()

    with ledger.acquire(ResourceRequest(cpu=CpuRequest(2, 2, 2))):
        assert ledger.snapshot()["cpu"]["allocated"] == 2
    lane_holder.release()
    assert acquired.wait(timeout=synchronization_timeout(1.0))
    thread.join(timeout=synchronization_timeout(1.0))
    assert not thread.is_alive()


def test_cpu_reserve_applies_across_concurrent_flexible_claims():
    """Separate scanner-like requests cannot spend the same reserve."""
    ledger = ResourceLedger(cpu_capacity=12)
    scan_request = ResourceRequest(
        cpu=CpuRequest(1, 8, 8),
        cpu_reserve=8,
        label="scanner hashing",
    )
    first_scan = ledger.acquire(scan_request)
    assert first_scan.cpu_permits == 4

    second_waiting = threading.Event()
    second_acquired = threading.Event()

    def acquire_second_scan():
        with ledger.acquire(
            scan_request,
            on_wait=lambda _request: second_waiting.set(),
        ):
            second_acquired.set()

    thread = threading.Thread(target=acquire_second_scan)
    thread.start()
    try:
        assert second_waiting.wait(timeout=synchronization_timeout(1.0))
        with ledger.acquire(ResourceRequest(
            cpu=CpuRequest(8, 8, 8),
            lanes=("cpu_ml",),
        )) as inference:
            assert inference.cpu_permits == 8
            assert ledger.snapshot()["cpu"]["allocated"] == 12
        assert not second_acquired.wait(timeout=0.1)
    finally:
        first_scan.release()

    assert second_acquired.wait(timeout=synchronization_timeout(1.0))
    thread.join(timeout=synchronization_timeout(1.0))
    assert not thread.is_alive()


def test_cpu_reserve_does_not_double_count_existing_inference_claim():
    """Scanner throughput must not depend on which claimant arrives first."""
    ledger = ResourceLedger(cpu_capacity=12)
    inference_request = ResourceRequest(
        cpu=CpuRequest(8, 8, 8),
        lanes=("cpu_ml",),
    )
    scan_request = ResourceRequest(
        cpu=CpuRequest(1, 8, 8),
        cpu_reserve=8,
        label="scanner hashing",
    )

    with ledger.acquire(inference_request) as inference:
        assert inference.cpu_permits == 8
        with ledger.acquire(scan_request) as scan:
            assert scan.cpu_permits == 4
            assert ledger.snapshot()["cpu"]["allocated"] == 12


def test_owner_wait_timing_uses_injected_clock():
    now = [10.0]
    ledger = ResourceLedger(cpu_capacity=1, clock=lambda: now[0])
    holder = ledger.acquire(ResourceRequest(cpu=CpuRequest(1, 1, 1)))
    waiting = threading.Event()
    acquired = threading.Event()

    def waiter():
        with bind_resource_owner("job-1"):
            with ledger.acquire(
                ResourceRequest(cpu=CpuRequest(1, 1, 1)),
                on_wait=lambda _request: waiting.set(),
            ):
                acquired.set()

    thread = threading.Thread(target=waiter)
    thread.start()
    assert waiting.wait(timeout=synchronization_timeout(1.0))
    now[0] = 12.5
    holder.release()
    assert acquired.wait(timeout=synchronization_timeout(1.0))
    thread.join(timeout=synchronization_timeout(1.0))

    assert ledger.owner_timing("job-1") == {
        "wait_seconds": 2.5, "wait_count": 1,
    }
    assert ledger.snapshot()["wait_seconds"] == 2.5


def test_cancel_check_wrapped_in_suspend_excludes_park_from_wait_timer():
    """Regression for the standalone scan/import wiring: a cancel_check
    closure that internally parks (matching ``runner.is_cancelled``'s
    behaviour on pause via ``wait_if_paused``) must not inflate the
    ledger's active-wait timer while it is parked. The closures in
    ``app.py`` and ``import_job.py`` wrap the parking call in
    ``suspend_resource_wait_timing()`` for exactly this reason —
    without the wrap, an hour-long user pause during a scan is
    persisted as an hour of resource contention on the job's
    diagnostics.
    """
    now = [10.0]
    ledger = ResourceLedger(cpu_capacity=1, clock=lambda: now[0])

    from resource_ledger import suspend_resource_wait_timing

    # This closure mirrors the shape of scan_cancel_check in the
    # standalone paths: enter a suspend context around a call that may
    # park (here simulated by advancing the clock while inside the
    # suspend).
    def scan_cancel_check_shape():
        with suspend_resource_wait_timing():
            now[0] += 100.0  # simulate the runner.is_cancelled park
            return False

    with bind_resource_owner("paused-scan"):
        with ledger.track_external_wait():
            now[0] = 12.0
            # 2 seconds of real contention before the parking probe.
            assert ledger.owner_timing("paused-scan") == {
                "wait_seconds": 2.0,
                "wait_count": 1,
            }
            # Invoke the wrapped probe — the +100s inside the suspend
            # must NOT contribute to wait_seconds.
            assert scan_cancel_check_shape() is False
            # Post-park real contention: another 1 second.
            now[0] += 1.0

    # Total real contention: 2s + 1s = 3s. The +100s park is excluded.
    # Without the closure's suspend wrap, this would report 103s.
    assert ledger.owner_timing("paused-scan") == {
        "wait_seconds": 3.0,
        "wait_count": 1,
    }


def test_suspended_resource_wait_excludes_parked_time():
    """A user-requested pause must not inflate contention diagnostics."""
    now = [10.0]
    ledger = ResourceLedger(cpu_capacity=1, clock=lambda: now[0])

    from resource_ledger import suspend_resource_wait_timing

    with bind_resource_owner("paused-job"):
        with ledger.track_external_wait():
            now[0] = 12.0
            with suspend_resource_wait_timing():
                assert ledger.owner_timing("paused-job") == {
                    "wait_seconds": 2.0,
                    "wait_count": 1,
                }
                now[0] = 112.0
                assert ledger.owner_timing("paused-job") == {
                    "wait_seconds": 2.0,
                    "wait_count": 1,
                }
            now[0] = 114.0

    assert ledger.owner_timing("paused-job") == {
        "wait_seconds": 4.0,
        "wait_count": 1,
    }
    assert ledger.snapshot()["wait_seconds"] == 4.0


def test_owner_timing_includes_active_wait_before_grant():
    """A blocked owner shows its live wait in owner_timing snapshots.

    ``/api/jobs`` reads ``resource_wait_*`` from ``owner_timing`` while a
    job is running. Waits are otherwise only recorded once the acquire
    call returns, so a job currently blocked on ``cpu_ml`` or the CPU
    budget would report zero wait — precisely for the job that most
    needs the diagnostic.
    """
    now = [10.0]
    ledger = ResourceLedger(cpu_capacity=1, clock=lambda: now[0])
    holder = ledger.acquire(ResourceRequest(cpu=CpuRequest(1, 1, 1)))
    waiting = threading.Event()
    acquired = threading.Event()

    def waiter():
        with bind_resource_owner("job-2"):
            with ledger.acquire(
                ResourceRequest(cpu=CpuRequest(1, 1, 1)),
                on_wait=lambda _request: waiting.set(),
            ):
                acquired.set()

    thread = threading.Thread(target=waiter)
    thread.start()
    try:
        assert waiting.wait(timeout=synchronization_timeout(1.0))
        now[0] = 13.5
        # Wait has not returned yet, but owner_timing must already reflect
        # the 3.5s the job has been blocked.
        active = ledger.owner_timing("job-2")
        assert active == {"wait_seconds": 3.5, "wait_count": 1}
        # remove=True on an active wait clears the recorded totals but
        # leaves the in-flight wait entry alone; the running acquire()
        # will record its own totals when it eventually returns.
        active_remove = ledger.owner_timing("job-2", remove=True)
        assert active_remove == {"wait_seconds": 3.5, "wait_count": 1}
    finally:
        now[0] = 15.0
        holder.release()
        assert acquired.wait(timeout=synchronization_timeout(1.0))
        thread.join(timeout=synchronization_timeout(1.0))

    final = ledger.owner_timing("job-2")
    assert final == {"wait_seconds": 5.0, "wait_count": 1}


def test_cancelled_wait_releases_waiter_accounting():
    ledger = ResourceLedger(cpu_capacity=1)
    holder = ledger.acquire(ResourceRequest(cpu=CpuRequest(1, 1, 1)))
    waiting = threading.Event()
    cancelled = threading.Event()
    outcome = []

    def waiter():
        try:
            ledger.acquire(
                ResourceRequest(cpu=CpuRequest(1, 1, 1)),
                cancel_check=cancelled.is_set,
                on_wait=lambda _request: waiting.set(),
            )
        except ResourceWaitCancelled:
            outcome.append("cancelled")

    thread = threading.Thread(target=waiter)
    thread.start()
    assert waiting.wait(timeout=synchronization_timeout(1.0))
    cancelled.set()
    # Wake the condition immediately; production cancellation otherwise gets
    # noticed by the bounded 200 ms poll.
    holder.release()
    try:
        thread.join(timeout=1.0)
        assert not thread.is_alive()
    finally:
        thread.join(timeout=synchronization_timeout(1.0))
    assert outcome == ["cancelled"]
    assert ledger.snapshot()["waiters"] == 0


def test_raising_cancel_check_releases_waiter_accounting():
    class CallerCancelled(RuntimeError):
        pass

    ledger = ResourceLedger(cpu_capacity=1)
    holder = ledger.acquire(ResourceRequest(cpu=CpuRequest(1, 1, 1)))
    waiting = threading.Event()
    cancel_now = threading.Event()
    outcome = []

    def cancel_check():
        if cancel_now.is_set():
            raise CallerCancelled("stop")
        return False

    def waiter():
        try:
            ledger.acquire(
                ResourceRequest(cpu=CpuRequest(1, 1, 1)),
                cancel_check=cancel_check,
                on_wait=lambda _request: waiting.set(),
            )
        except CallerCancelled:
            outcome.append("cancelled")

    thread = threading.Thread(target=waiter)
    thread.start()
    assert waiting.wait(timeout=synchronization_timeout(1.0))
    cancel_now.set()
    holder.release()
    try:
        thread.join(timeout=1.0)
        assert not thread.is_alive()
    finally:
        thread.join(timeout=synchronization_timeout(1.0))
    assert outcome == ["cancelled"]
    assert ledger.snapshot()["waiters"] == 0


@pytest.mark.parametrize(
    "values",
    [(0, 1, 1), (2, 1, 2), (1, 3, 2)],
)
def test_invalid_cpu_request_rejected(values):
    with pytest.raises(ValueError):
        CpuRequest(*values)


def test_bound_cancel_check_wakes_waiter_without_explicit_argument():
    """A job that binds its cancel probe cancels downstream ledger waits.

    Downstream inference sites (CPU classify/detect/mask/embed) claim the
    ``cpu_ml`` lane through ``acquire_inference_resources`` without seeing
    the job's cancellation callable. Binding the probe via
    ``bind_resource_cancel_check`` at the top of a job means those waits
    still wake when the job is cancelled.
    """
    ledger = ResourceLedger(cpu_capacity=1)
    holder = ledger.acquire(ResourceRequest(cpu=CpuRequest(1, 1, 1)))
    waiting = threading.Event()
    cancelled = threading.Event()
    outcome = []

    def waiter():
        with bind_resource_cancel_check(cancelled.is_set):
            try:
                ledger.acquire(
                    ResourceRequest(cpu=CpuRequest(1, 1, 1)),
                    on_wait=lambda _request: waiting.set(),
                )
            except ResourceWaitCancelled:
                outcome.append("cancelled")

    thread = threading.Thread(target=waiter)
    thread.start()
    assert waiting.wait(timeout=synchronization_timeout(1.0))
    cancelled.set()
    holder.release()
    try:
        thread.join(timeout=1.0)
        assert not thread.is_alive()
    finally:
        thread.join(timeout=synchronization_timeout(1.0))
    assert outcome == ["cancelled"]
    assert ledger.snapshot()["waiters"] == 0


def test_explicit_cancel_check_overrides_bound_probe():
    """A caller that passes cancel_check explicitly is trusted verbatim.

    Scanner hashing already threads its own probe and would otherwise be
    surprised by a bound probe silently taking over — the explicit
    argument must win, and ``None`` means "no cancellation" for callers
    that opted out on purpose.
    """
    ledger = ResourceLedger(cpu_capacity=1)
    holder = ledger.acquire(ResourceRequest(cpu=CpuRequest(1, 1, 1)))
    waiting = threading.Event()
    bound_cancelled = threading.Event()
    explicit_cancelled = threading.Event()
    explicit_polled = threading.Event()
    outcome = []

    def explicit_probe():
        explicit_polled.set()
        return explicit_cancelled.is_set()

    def waiter():
        with bind_resource_cancel_check(bound_cancelled.is_set):
            try:
                ledger.acquire(
                    ResourceRequest(cpu=CpuRequest(1, 1, 1)),
                    cancel_check=explicit_probe,
                    on_wait=lambda _request: waiting.set(),
                )
            except ResourceWaitCancelled:
                outcome.append("cancelled")

    thread = threading.Thread(target=waiter)
    thread.start()
    assert waiting.wait(timeout=synchronization_timeout(1.0))
    # Flip the bound probe first. If bind took precedence, this would
    # cancel the waiter; the assertions below prove it did not.
    explicit_polled.clear()
    bound_cancelled.set()
    assert explicit_polled.wait(timeout=synchronization_timeout(1.0))
    assert thread.is_alive()
    assert outcome == []
    explicit_cancelled.set()
    # Cancellation must finish before the held resource is released.
    thread.join(timeout=1.0)
    assert not thread.is_alive()
    assert outcome == ["cancelled"]
    holder.release()
