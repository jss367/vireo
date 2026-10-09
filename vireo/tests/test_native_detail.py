"""Native filter fidelity, fallback, and shared CPU accounting contracts."""

import threading
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

import detail
import detail_backend as backend
import numpy as np
import pytest
from float_image import FloatImage
from PIL import Image
from resource_ledger import (
    CpuRequest,
    ResourceLedger,
    ResourceRequest,
    ResourceWaitCancelled,
    bind_resource_cancel_check,
)
from testing.waits import synchronization_timeout


@pytest.fixture
def native():
    return pytest.importorskip("vireo._native_detail")


@pytest.mark.parametrize("shape", [(1, 1), (1, 9), (7, 1), (2, 3), (19, 23)])
@pytest.mark.parametrize("radius", [0, 1, 3])
def test_bilateral_boundaries_and_input_ownership(native, shape, radius):
    image = np.random.default_rng(42).random(shape, dtype=np.float32)
    original = image.copy()
    expected = detail._bilateral(image, 1.6, .0705, radius)
    np.testing.assert_allclose(native.Filters(2).bilateral(image, 1.6, .0705, radius), expected, rtol=0, atol=4e-7)
    np.testing.assert_array_equal(image, original)


@pytest.mark.parametrize("shape", [(1, 1), (1, 9, 3), (7, 1, 4), (2, 3), (19, 23, 3)])
@pytest.mark.parametrize("sigma", [.3, .5, 1.8, 3.0])
def test_gaussian_boundaries(native, shape, sigma):
    image = np.random.default_rng(21).random(shape, dtype=np.float32)
    actual = native.Filters(2).gaussian(image, detail._gaussian_kernel(sigma))
    np.testing.assert_allclose(actual, detail._gaussian_blur(image, sigma), rtol=0, atol=4e-7)


def test_strided_input_and_parallelism_are_deterministic(native):
    image = np.random.default_rng(2).random((21, 17), dtype=np.float32)[::-1, ::2]
    serial = native.Filters(1).bilateral(image, 1.6, .0705, 3)
    parallel = native.Filters(4).bilateral(image, 1.6, .0705, 3)
    np.testing.assert_array_equal(serial, parallel)
    np.testing.assert_allclose(parallel, detail._bilateral(image, 1.6, .0705, 3), rtol=0, atol=4e-7)


@pytest.mark.parametrize("threads", [0, 5])
def test_native_rejects_unbounded_pool_sizes(native, threads):
    with pytest.raises(ValueError):
        native.Filters(threads)


@pytest.mark.parametrize("shape", [(0, 3), (3,), (1, 1, 5), (1, 8_000_001)])
def test_native_rejects_unsupported_or_oversized_arrays(native, shape):
    with pytest.raises(ValueError):
        native.Filters(1).gaussian(np.empty(shape, dtype=np.float32), np.ones(1, dtype=np.float32))


@pytest.mark.parametrize("kernel", [[], [1, 1], [float("nan")], [1] * 131])
def test_native_rejects_invalid_gaussian_weights(native, kernel):
    with pytest.raises(ValueError):
        native.Filters(1).gaussian(np.ones((2, 3), dtype=np.float32), np.array(kernel, dtype=np.float32))


@pytest.mark.parametrize("spatial,range_sigma,radius", [(0, .1, 3), (1, float("nan"), 3), (1, 1e-100, 3), (1, .1, 9)])
def test_native_rejects_invalid_bilateral_parameters(native, spatial, range_sigma, radius):
    with pytest.raises(ValueError):
        native.Filters(1).bilateral(np.ones((2, 3), dtype=np.float32), spatial, range_sigma, radius)


@pytest.mark.parametrize("floating", [False, True])
@pytest.mark.parametrize("scale", [.17, 1.0])
def test_production_dispatch_preserves_alpha_and_tile_seams(native, monkeypatch, floating, scale):
    monkeypatch.setattr(backend, "MIN_NATIVE_PIXELS", 0)
    monkeypatch.setattr(backend, "_load_native", lambda: native)
    data = np.random.default_rng(7).integers(0, 256, (90, 17, 4), dtype=np.uint8)
    image = FloatImage(data[..., :3].astype(np.float32) / 255, encoding="srgb") if floating else Image.fromarray(data)
    original = np.asarray(image).copy()
    options = dict(sharpen=55, sharpen_radius=1.8, noise_reduction=45, scale=scale)
    with backend.detail_thread_budget(0):
        expected = np.asarray(detail.apply_detail(image, **options))
    with backend.detail_thread_budget(2):
        actual = np.asarray(detail.apply_detail(image, **options))
        monkeypatch.setattr(detail, "_DETAIL_TILE_PIXELS", 17 * 4)
        tiled = np.asarray(detail.apply_detail(image, **options))
    np.testing.assert_array_equal(actual, tiled)
    if floating:
        np.testing.assert_allclose(actual, expected, rtol=0, atol=2e-6)
        assert np.max(np.abs(np.rint(actual * 65535) - np.rint(expected * 65535))) <= 1
    else:
        assert np.max(np.abs(actual.astype(np.int16) - expected.astype(np.int16))) <= 1
    if not floating:
        np.testing.assert_array_equal(actual[..., 3], original[..., 3])
    np.testing.assert_array_equal(np.asarray(image), original)


def test_small_and_disabled_renders_do_not_load_native(monkeypatch):
    def unexpected():
        pytest.fail("Small previews must stay on NumPy")
    monkeypatch.setattr(backend, "_load_native", unexpected)
    with backend.filters_for_image(backend.MIN_NATIVE_PIXELS - 1) as filters:
        assert filters is None
    with backend.detail_thread_budget(0), backend.filters_for_image(10_000_000) as filters:
        assert filters is None


def test_missing_extension_keeps_python_output(monkeypatch):
    monkeypatch.setattr(backend, "MIN_NATIVE_PIXELS", 0)
    monkeypatch.setattr(backend, "_load_native", lambda: None)
    image = Image.fromarray(np.full((12, 13, 3), 120, dtype=np.uint8))
    with backend.detail_thread_budget(0):
        expected = np.asarray(detail.apply_detail(image, sharpen=50, noise_reduction=50))
    np.testing.assert_array_equal(detail.apply_detail(image, sharpen=50, noise_reduction=50), expected)


@pytest.mark.parametrize("error", [ModuleNotFoundError(name="vireo._native_detail"), ModuleNotFoundError(name="vireo"), ImportError("ABI mismatch")])
def test_unavailable_extension_load_falls_back(monkeypatch, error):
    def unavailable(_name):
        raise error
    backend._load_native.cache_clear()
    try:
        monkeypatch.setattr(backend.importlib, "import_module", unavailable)
        assert backend._load_native() is None
    finally:
        backend._load_native.cache_clear()


def test_direct_source_layout_loads_the_colocated_extension(native):
    import subprocess
    import sys
    from pathlib import Path

    directory = Path(detail.__file__).resolve().parent
    code = f"import sys; sys.path.insert(0, {str(directory)!r}); import detail_backend; native=detail_backend._load_native(); assert native.Filters(2).thread_count == 2; assert native.__name__ == '_native_detail'"
    subprocess.run([sys.executable, "-I", "-c", code], check=True)


def test_preview_uses_its_allocation_and_restores_context(native, monkeypatch):
    monkeypatch.setattr(backend, "_load_native", lambda: native)
    def unexpected():
        pytest.fail("Preview subprocesses already have an allocation")
    monkeypatch.setattr(backend, "get_resource_ledger", unexpected)
    with backend.detail_thread_budget(2), backend.filters_for_image(1_000_000) as filters:
        assert filters.thread_count == 2
    with backend.detail_thread_budget(99), backend.filters_for_image(1_000_000) as filters:
        assert filters.thread_count == 4
    assert backend._THREAD_BUDGET.get() is None


def test_background_pool_matches_available_grant_and_releases_on_error(native, monkeypatch):
    ledger = ResourceLedger(3)
    monkeypatch.setattr(backend, "_load_native", lambda: native)
    monkeypatch.setattr(backend, "get_resource_ledger", lambda: ledger)
    with ledger.acquire(ResourceRequest(cpu=CpuRequest(2, 2, 2))):
        with pytest.raises(RuntimeError, match="render failed"), backend.filters_for_image(1_000_000) as filters:
            assert filters.thread_count == 1
            assert ledger.snapshot()["cpu"]["allocated"] == 3
            raise RuntimeError("render failed")
        assert ledger.snapshot()["cpu"]["allocated"] == 2
    assert ledger.snapshot()["cpu"]["allocated"] == 0


def test_concurrent_passes_wait_for_cpu_grant(native, monkeypatch):
    ledger = ResourceLedger(2)
    monkeypatch.setattr(backend, "_load_native", lambda: native)
    monkeypatch.setattr(backend, "get_resource_ledger", lambda: ledger)
    entered = threading.Event()
    def second():
        entered.set()
        with backend.filters_for_image(1_000_000) as filters:
            assert filters.thread_count == 2
    with ThreadPoolExecutor(1) as executor:
        with backend.filters_for_image(1_000_000):
            pending = executor.submit(second)
            assert entered.wait(synchronization_timeout(5))
            assert not pending.done()
        pending.result(timeout=5)
    assert ledger.snapshot()["cpu"]["allocated"] == 0


def test_cpu_wait_honors_bound_cancellation(native, monkeypatch):
    ledger = ResourceLedger(1)
    monkeypatch.setattr(backend, "_load_native", lambda: native)
    monkeypatch.setattr(backend, "get_resource_ledger", lambda: ledger)
    with bind_resource_cancel_check(lambda: True), pytest.raises(ResourceWaitCancelled), backend.filters_for_image(1_000_000):
        pytest.fail("Cancelled job entered native work")
    assert ledger.snapshot()["cpu"]["allocated"] == 0


def test_pool_creation_failure_falls_back_and_releases_lease(monkeypatch):
    ledger = ResourceLedger(2)
    def unavailable(_threads):
        raise RuntimeError("cannot start a thread")
    monkeypatch.setattr(backend, "_load_native", lambda: SimpleNamespace(Filters=unavailable))
    monkeypatch.setattr(backend, "get_resource_ledger", lambda: ledger)
    with backend.filters_for_image(1_000_000) as filters:
        assert filters is None
    assert ledger.snapshot()["cpu"]["allocated"] == 0
