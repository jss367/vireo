# Rust photo editing filter experiment

Vireo now includes an optional Rust accelerator for its existing Gaussian and
standard bilateral detail filters. At the normal two-thread preview budget,
integrated warm full-resolution requests improved by 22% on the 24-megapixel
RAW and 18% on the 46-megapixel RAW. Full float output and rounded 8-bit and
16-bit channels matched the NumPy reference exactly on both inputs.

| Normal two-thread preview | NumPy reference | Production Rust filters |
| --- | ---: | ---: |
| 24 megapixels | 12.71 s | 9.92 s |
| 46 megapixels | 22.89 s | 18.84 s |

The [production integration report](performance/photo-editing-production-comparison.json)
contains all samples, source hashes, source-file checksums, small-preview timings,
and fidelity results. These are three-sample observations on a shared machine,
not release performance budgets. Quick previews were similar; fit-preview results
were mixed, with a modest improvement on the 24-megapixel input.

Python retains recipe handling, tone, scale, tiling, alpha, and encoding. Images
below one million pixels use NumPy. Preview children use their existing thread
allocation; other renders acquire one to four CPU permits from the resource
ledger. Source installs fall back to NumPy when the extension is unavailable.
Desktop builds require the extension and verify its presence in the frozen archive.
See [build and runtime details](../native/detail/README.md).

The measurements below describe the earlier isolated four-thread experiment under
[`tools/detail-performance`](../tools/detail-performance/README.md). Its reports
preserve source hashes from before production dispatch was added. At four threads,
the pure Rust prototype improved warm full-resolution requests by 31% and 35%.
Gaussian replacements using SciPy or OpenCV alone did not improve native latency.

## What was measured

Measurements used an Apple M3 Max with 16 CPU cores, macOS 27.0.1, Python 3.14.3,
NumPy 2.4.2, SciPy 1.17.1, OpenCV 4.11.0, and Pillow 12.1.1. Native-library thread
limits and the Rust pool were capped at four. Rust was compiled in release mode
with PyO3 0.29.3, rust-numpy 0.29.0, and Rayon 1.12.0. Each row contains three
timed samples after a warm-up render in a fresh process.

This was a shared-machine experiment with other workloads running. Sample counts
are small, and measurements are not release performance budgets. Rust's explicit
parallelism contributes to the improvement; this is a comparison of implementations,
not an attempt to isolate the language from scheduling and memory-access choices.

The camera inputs were 6,016 × 4,016 and 8,288 × 5,520 Nikon RAW files. The recipe
applied exposure +0.3, Shadows +15, sharpening 55 with radius 1.8, and standard
noise reduction 45. It does not exercise camera-guided OpenCV denoising.

## Filter and rendering comparison

The timed path includes the production tone and detail passes, display
quantization, and quality-90 JPEG encoding. It excludes RAW decoding, HTTP
dispatch, preview-worker supervision, and browser display. The complete samples,
function profiles, source hashes, environment, and fidelity results are in the
[rendering comparison report](performance/photo-editing-filter-comparison.json).

| Implementation | 24-megapixel median | 46-megapixel median |
| --- | ---: | ---: |
| Current NumPy filters | 10.99 s | 21.32 s |
| SciPy Gaussian; current bilateral | 11.17 s | 21.59 s |
| OpenCV Gaussian; current bilateral | 10.98 s | 22.03 s |
| Rust Gaussian and bilateral | 8.06 s | 16.78 s |
| OpenCV Gaussian; Rust bilateral | 7.26 s | 17.18 s |

The original 24-megapixel profile spent 5.39 seconds in detail filtering and
5.47 seconds in tone rendering. The bilateral filter accounted for 3.91 seconds;
Gaussian filtering accounted for 0.90 seconds. That identifies the bilateral
filter as the largest opportunity within the detail pass. The basic recipe used
by the earlier RAW preview benchmark contains no detail adjustments, so rewriting
these filters does not accelerate that recipe.

Peak resident memory for the pure Rust candidate was 3,684 MiB versus 3,781 MiB
for the original at 24 megapixels, and 4,850 MiB versus 5,045 MiB at 46 megapixels.
These modest differences include allocator retention and do not establish a fixed
memory saving. The complete report includes resident memory before measurement.

## Preview endpoint comparison

The [preview comparison report](performance/photo-editing-preview-comparison.json)
uses the existing RAW benchmark's temporary catalog, real HTTP handler, supervised
render child, decoded-source cache, output-dimension checks, and verification that
the source stayed linear rather than falling back to a camera JPEG. The experiment
substitutes only the child render callable. Each backend runs in a fresh temporary
application; the filter pool is recreated per request, with its setup cost included.

The existing benchmark changes exposure between requests; the three timed samples
use +0.3, +0.4, and +0.5. Shadows and detail controls match the rendering trial.
All rows below use warm decoded-source caches and four numeric/filter threads.
Times include handler and worker costs through JPEG completion, and exclude
browser display. The recorded 95th percentile is the slowest of three samples.

| Source | Preview | Original median | Rust median | Change |
| --- | --- | ---: | ---: | ---: |
| 24-megapixel Nikon | Quick, 1,024 pixels | 226 ms | 237 ms | 5% slower |
| 24-megapixel Nikon | Fit, 2,048 pixels | 1,329 ms | 840 ms | 37% faster |
| 24-megapixel Nikon | Native resolution | 12.32 s | 8.54 s | 31% faster |
| 46-megapixel Nikon | Quick, 1,024 pixels | 221 ms | 229 ms | 4% slower |
| 46-megapixel Nikon | Fit, 2,048 pixels | 859 ms | 803 ms | 7% faster |
| 46-megapixel Nikon | Native resolution | 23.90 s | 15.55 s | 35% faster |

The small quick-preview differences do not establish a consistent benefit at that
size. Filter radii scale with output pixels per native pixel, so the two cameras'
fit previews have different bilateral window sizes. The strongest benefit is in
larger renders with standard noise reduction enabled. Cold-source decoding was not
measured in this comparison; none of the candidate filters changes RAW decoding.

Aggregated server-and-child resident memory was 3,720 MiB versus 3,685 MiB for the
24-megapixel native case, and 5,225 MiB versus 5,124 MiB for the 46-megapixel native
case. Memory reductions are small compared with the latency improvement.

## Image fidelity

The pure Rust candidate produced identical float arrays and identical rounded
8-bit and 16-bit channels on both full-resolution camera renders. This is an
observation for these inputs and this build, rather than a cross-platform promise.

The SciPy, OpenCV, and hybrid candidates passed the numerical tolerance but changed
some rounded channels. The largest float difference across camera trials was
`2.98e-7`; the largest rounded difference was one level at either 8 or 16 bits.
The hybrid was faster on the 24-megapixel case, but did not beat pure Rust on the
46-megapixel case. Pure Rust is the stronger initial candidate because it offers
consistent gains here with exact observed output.

The implementation retains NumPy's reflection convention and the square bilateral
window. OpenCV's built-in circular-window bilateral filter was excluded because it
would change the filtering behavior. Existing Python code continues to control
tile overlap, alpha preservation, scale, blending, and recipe ordering.

## Validation and integration decision

The extension was also exercised inside Vireo's supervised preview child process
using a synthetic DNG decoded by actual LibRaw. A focused run of the experiment,
existing detail-filter tests, and existing RAW preview benchmark tests passed
100 tests. Rust formatting, Clippy with warnings rejected, and Python lint checks
passed. Tests check borders, small images, tiled seams, unchanged alpha and input,
16-bit precision, noncontiguous views, worker-count reproducibility, and invalid
inputs.

The production port retains the measured Gaussian and bilateral kernels and the
existing Python orchestration. Integration checks cover CPU grants, fallback,
actual native-work cancellation and worker recovery, concurrent preview children,
wheel/source packaging, and extension loading in a frozen executable. The CI
matrix repeats native validation on macOS, Windows, and Linux. Tone rendering
remains a substantial cost after detail filtering is accelerated.
