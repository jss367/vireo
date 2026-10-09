# Native photo detail filters

This private PyO3 extension accelerates the existing Gaussian and square-window
bilateral filters. Python still controls recipes, tone, scale, tile halos, alpha,
and output encoding. The NumPy kernels remain the reference and fallback.

Install with Vireo's Python 3.14 interpreter and a stable Rust toolchain:

```sh
python -m pip install -e ".[dev]"
python -c "from vireo._native_detail import Filters; print(Filters(2).thread_count)"
python -m pytest -q vireo/tests/test_native_detail.py vireo/tests/test_detail.py vireo/tests/test_preview_workers.py
python scripts/smoke_native_detail.py
```

The extension always builds in release mode, including editable installs. Source
installs can omit it if Rust is unavailable. Desktop builds require it and check
the PyInstaller archive before distributing the sidecar. CI builds and exercises
it on macOS, Windows, and Linux.

`detail_backend.py` selects native filtering for images of at least one million
pixels. Smaller previews stay on NumPy. Preview children use their existing
thread allocation (normally two); other callers obtain one to four CPU permits
from the resource ledger. Each detail pass owns its pool and releases it on exit.
Preview cancellation terminates the supervised process, including native work.

Input snapshots protect borrowed NumPy buffers while the interpreter lock is
released. Each call accepts at most eight million pixels and four channels;
Gaussian kernels have at most 129 taps. Unsupported tile sizes, nonfinite tiles,
and wider Gaussian kernels use the Python implementation. Accumulation order and
reflection match the reference; platform math libraries can introduce small
float differences. Tests bound float and rounded 16-bit error and check tile seams.

When changing Cargo.lock, refresh the corresponding notices in
`vireo/data/native-detail/THIRD_PARTY_LICENSES.txt` from the pinned crate sources.
See [performance measurements](../../docs/photo-editing-rust-experiment.md).
