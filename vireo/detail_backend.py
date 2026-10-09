"""Optional native detail filters with bounded CPU use.

Preview subprocesses already have a thread allocation; other callers acquire
one from the process resource ledger. Pools belong to one detail pass and are
released with it, rather than accumulating idle pools for different grants.
"""

import contextlib
import contextvars
import functools
import importlib
import logging

try:
    from .resource_ledger import ResourceRequest, cpu_phase_request, get_resource_ledger
except ImportError:
    from resource_ledger import ResourceRequest, cpu_phase_request, get_resource_ledger

logger = logging.getLogger(__name__)
MIN_NATIVE_PIXELS = 1_000_000
_THREAD_BUDGET = contextvars.ContextVar("vireo_detail_threads", default=None)


@functools.cache
def _load_native():
    # app.py also runs directly from the vireo/ directory. Prefer its colocated
    # extension in that layout; frozen apps expose the qualified package name.
    names = ("vireo._native_detail", "_native_detail") if __package__ else ("_native_detail", "vireo._native_detail")
    for name in names:
        try:
            return importlib.import_module(name)
        except ModuleNotFoundError as error:
            if error.name not in {"vireo", *names}:
                raise
        except (ImportError, OSError):
            logger.warning("Native detail filters could not load; using NumPy", exc_info=True)
            return None
    return None


@contextlib.contextmanager
def detail_thread_budget(threads):
    """Use a preview worker's allocation; zero explicitly selects NumPy."""
    token = _THREAD_BUDGET.set(max(0, min(4, int(threads))))
    try:
        yield
    finally:
        _THREAD_BUDGET.reset(token)


@contextlib.contextmanager
def filters_for_image(pixels):
    budget = _THREAD_BUDGET.get()
    if pixels < MIN_NATIVE_PIXELS or budget == 0:
        yield None
        return
    native = _load_native()
    if native is None:
        yield None
        return
    with contextlib.ExitStack() as stack:
        if budget is None:
            ledger = get_resource_ledger()
            lease = stack.enter_context(ledger.acquire(ResourceRequest(
                cpu=cpu_phase_request(ledger.cpu_capacity, preferred=4, maximum=4),
                label="photo detail filters",
            )))
            budget = lease.cpu_permits
        try:
            filters = native.Filters(budget)
        except (ValueError, RuntimeError):
            logger.warning("Native detail pool could not start; using NumPy", exc_info=True)
            filters = None
        yield filters
