# Photo editing filter performance experiment

This experiment compares the current Vireo editing renderer with alternative
Gaussian and bilateral filters. The experimental Rust extension stays separate
from the application package. The production port lives in
[`native/detail`](../../native/detail/README.md).
The experimental crate preserves the measured prototype as a comparison baseline;
changes to shipped kernels belong in `native/detail`.

See the [measured results and integration assessment](../../docs/photo-editing-rust-experiment.md)
for the camera comparison and validation results.

The candidates are:

- **Original:** the current NumPy implementation.
- **SciPy:** `correlate1d` for the two Gaussian passes; the existing bilateral filter.
- **OpenCV:** two `sepFilter2D` calls for Gaussian filtering; the existing bilateral filter.
- **Rust:** both kernels in a PyO3 extension, using a pool of at most four threads.
- **Rust and OpenCV:** Rust bilateral filtering and OpenCV Gaussian filtering.
- **Production:** the installed Vireo extension with its normal size threshold.

Explicit comparison backends disable production dispatch while measuring, so
the original baseline continues to exercise NumPy. The production backend needs
`python -m pip install -e .`; it does not need the experimental extension.

All candidates use the same Gaussian coefficients, square bilateral window,
reflection at image boundaries, blending, clipping, and render-scale rules.
OpenCV's built-in bilateral filter is deliberately excluded: its circular window
would change Vireo's filter behavior. The Rust functions snapshot NumPy input
before releasing Python's interpreter lock; the benchmark includes those input
copies. Owned output buffers transfer back to NumPy without another pixel copy.

## Build and verify

Use the same Python 3.14 environment as Vireo, with its development dependencies
installed. Install the experiment into a private directory:

```sh
python -m pip install --no-deps --target .context/detail-extension ./tools/detail-performance
PYTHONPATH=.context/detail-extension python -m pytest tools/detail-performance/test_filters.py -q
cargo fmt --manifest-path tools/detail-performance/Cargo.toml --check
cargo clippy --manifest-path tools/detail-performance/Cargo.toml --all-targets -- -D warnings
```

The independent Cargo lockfile pins the Rust dependencies. Builds use the release
profile through maturin. This does not add a dependency to Vireo or change the
Tauri build.

## Reproduce the comparison

For deterministic synthetic noise, gradients, and edges:

```sh
python tools/detail-performance/benchmark.py \
  --extension-dir .context/detail-extension \
  --sizes 1024 2048 6000 --samples 5 \
  --output .context/detail-synthetic.json
```

For real RAW files, use the private manifest format described in
[RAW preview benchmarks](../../docs/raw-preview-benchmarks.md). Paths are relative
to the manifest; files are read without being modified. Reports contain descriptive
source names and content hashes, without source paths or photo contents.

```sh
python tools/detail-performance/benchmark.py \
  --manifest .context/raw-benchmark/corpus.json \
  --extension-dir .context/detail-extension \
  --sizes 1024 2048 8288 --samples 5 \
  --output .context/detail-camera.json
```

The requested size is capped by the source dimensions. Thus 8,288 pixels requests
native resolution for both the earlier 24-megapixel and 46-megapixel Nikon corpus.
Synthetic images use a fixed 6,000 × 4,000 native size for preview-scale calculations.
The **basic** recipe applies exposure and Shadows. The **detail** recipe adds
sharpening and standard noise reduction; it does not use camera-guided denoising.

Each backend/source/size/recipe runs sequentially in a fresh process. One warm-up
render precedes the timed samples. Timings cover the production in-process editing
pipeline, display quantization and quality-90 JPEG encoding. They exclude RAW
decoding, HTTP dispatch, preview-worker scheduling, browser decoding and display.
The existing HTTP benchmark remains the measure of actual endpoint performance.

The experiment also supplies a wrapper around that existing benchmark. It
substitutes the worker's render callable within temporary test applications, so
the same candidate kernels run through the real supervised preview endpoint.
The detail recipe is added inside that experimental callable. Production routes,
worker supervision, catalogs, and configuration are unchanged.

```sh
python tools/detail-performance/endpoint.py \
  --manifest .context/raw-benchmark/corpus.json \
  --extension-dir .context/detail-extension \
  --views quick fit native --caches warm \
  --backends original rust --recipes detail --samples 5 \
  --output .context/detail-camera-endpoint.json
```

This reports actual handler latency, excluding browser display, and the combined
resident memory of the temporary server and its child processes. The accounting
and warm/cold source-cache rules match the existing RAW preview benchmark. The
experimental wrapper recreates its filter pool per request; that setup cost is
included. Use `--caches cold warm` to include decode-heavy cold-source scenarios.

Resident memory is sampled every 5 ms during timed renders after warm-up. It includes
the input image, interpreter and allocations retained by the allocator. The report
also records resident memory immediately before timing. It excludes the benchmark
driver and the later reference render and fidelity comparisons. Small differences
in resident memory can reflect allocator behavior rather than filter allocations.

The original backend additionally records a separate, untimed function profile.
The report includes every sample, median, nearest-rank 95th percentile, environment,
source revision, dirty-tree status, and hashes of the measured source code.

## Output fidelity

Every candidate's full float render is compared with the original renderer on the
same input. Reports record maximum error, root mean square error, and both the count
and magnitude of changed channels after 8-bit and 16-bit rounding. The experiment
fails if a channel differs by more than `2e-6` in float or one 16-bit level. Passing
this bound does **not** mean byte-identical output or identical JPEG files.

Tests cover small images, image boundaries, tiled output, unchanged alpha and input
buffers, noncontiguous NumPy views, worker-count reproducibility, and rejected
invalid parameters. Larger camera comparisons complement these boundary checks.
An actual-LibRaw synthetic DNG test checks that the optional extension also loads
and renders successfully in the supervised child-process path.
