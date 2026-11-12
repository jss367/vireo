"""Do not freeze an older decoder or silently lose existing RAW codecs."""

import hashlib
import io
import json
import sys
from types import SimpleNamespace

import pytest

from scripts import build_rawpy_snapshot as snapshot
from scripts import build_sidecar
from scripts.check_nikon_raw import download_sample


@pytest.fixture
def decoder(monkeypatch, tmp_path):
    provenance = tmp_path / "vireo-build.json"
    provenance.write_text(json.dumps({"rawpy": snapshot.RAWPY_COMMIT, "libraw": snapshot.LIBRAW_COMMIT,
                                      "libraw_cmake": snapshot.CMAKE_COMMIT, "vcpkg": snapshot.VCPKG_COMMIT,
                                      "version": snapshot.VERSION}))
    module = SimpleNamespace(__file__=str(tmp_path / "__init__.py"), __version__=snapshot.VERSION,
                             libraw_version=snapshot.LIBRAW_VERSION, libraw_version_compiled=snapshot.LIBRAW_VERSION,
                             flags=dict.fromkeys(snapshot.REQUIRED_FLAGS, True))
    monkeypatch.setitem(sys.modules, "rawpy", module)
    return module


def test_accepts_matching_snapshot(decoder):
    snapshot.check_installation()


def test_desktop_build_rejects_published_wheel_before_freezing(decoder, monkeypatch):
    decoder.__version__ = "0.28.0"
    monkeypatch.setattr(sys, "argv", ["build_sidecar.py"])
    with pytest.raises(RuntimeError, match="Run python scripts/build_rawpy_snapshot.py"):
        build_sidecar.main()


@pytest.mark.parametrize(("attribute", "value"), [
    ("__version__", "0.28.0"), ("libraw_version", (0, 22, 2)), ("libraw_version_compiled", (0, 22, 2)),
])
def test_rejects_old_library_or_headers(decoder, attribute, value):
    setattr(decoder, attribute, value)
    with pytest.raises(RuntimeError):
        snapshot.check_installation()


def test_rejects_wrong_sources(decoder, tmp_path):
    (tmp_path / "vireo-build.json").write_text('{"libraw": "unknown"}')
    with pytest.raises(RuntimeError, match="provenance"):
        snapshot.check_installation()


@pytest.mark.parametrize("codec", snapshot.REQUIRED_FLAGS)
def test_existing_formats_must_remain_supported(decoder, codec):
    decoder.flags[codec] = False
    with pytest.raises(RuntimeError, match="lost existing codec"):
        snapshot.check_installation()


def test_rejects_changed_public_sample_and_cleans_partial_download(monkeypatch, tmp_path):
    monkeypatch.setattr("urllib.request.urlopen", lambda *a, **kw: io.BytesIO(b"unexpected content"))
    with pytest.raises(ValueError, match="checksum mismatch"):
        download_sample(tmp_path, "test", 1, "0" * 64)
    assert not list(tmp_path.iterdir())


def test_verified_samples_can_run_offline(monkeypatch, tmp_path):
    sample = tmp_path / "test.NEF"
    sample.write_bytes(b"verified bytes")
    digest = hashlib.sha256(sample.read_bytes()).hexdigest()

    def no_network(*args, **kwargs):
        pytest.fail("A verified cached sample should not be downloaded")

    monkeypatch.setattr("urllib.request.urlopen", no_network)
    assert download_sample(tmp_path, "test", 1, digest) == sample


def test_windows_codec_libraries_use_pythons_dynamic_runtime(monkeypatch):
    monkeypatch.setattr(snapshot.sys, "platform", "win32")
    monkeypatch.setattr(snapshot.platform, "machine", lambda: "AMD64")
    assert snapshot.triplet() == "x64-windows-static-md"


def test_rejects_whitespace_in_build_staging_path_before_fetching(monkeypatch, tmp_path):
    monkeypatch.setattr(snapshot.tempfile, "gettempdir", lambda: "/tmp/space in name")
    output = tmp_path / "wheels"
    with pytest.raises(RuntimeError, match="Set TMPDIR"):
        snapshot.build(output)
    assert not output.exists()
