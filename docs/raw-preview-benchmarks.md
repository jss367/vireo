# RAW preview benchmarks and recovery

`scripts/benchmark_raw_previews.py` measures the production edit-preview HTTP
handler with real RAW files. It exercises full-precision decoding, exposure and
Shadows adjustments, resizing, and JPEG encoding. It fails if decoding falls
back to an embedded JPEG or delivers the wrong output size.

Keep the corpus outside Git. Create a local JSON manifest, using descriptive
names that are safe to include in reports:

```json
[
  {"name": "camera-24mp", "path": "camera-24mp.nef"},
  {"name": "camera-46mp", "path": "camera-46mp.nef"}
]
```

Paths are relative to the manifest (absolute paths also work). Use the same
files and storage location on repeat runs. The report stores content hashes,
dimensions and these names, but omits file paths and image contents. Include
representative 24–60 MP cameras when available; a synthetic DNG smoke test in
the normal test suite verifies the harness but does not represent camera speed.

Install the development dependencies, close other heavy applications, then run:

```sh
python scripts/benchmark_raw_previews.py \
  --manifest .context/raw-benchmark/corpus.json \
  --machine-label dedicated-benchmark-machine \
  --output .context/raw-benchmark/baseline.json
```

Each file runs six scenarios in separate processes: quick (1,024 pixels), fit
(a fixed 2,048 pixels), and native resolution (100% zoom), with cold and warm
decoded-source caches. Each scenario uses five changing recipes. Numeric
libraries use at most four threads; scenarios run sequentially.

- **Cold** clears Vireo's decoded-source cache before each sample. It does not
  flush the OS disk cache. This is not a cold-disk or network-storage benchmark.
- **Warm** primes the same-size source once before timing changing recipes.
- **Latency** is handler time through encoded JPEG completion, including worker
  dispatch and initial process startup. It excludes browser image decoding,
  network transfer, browser scheduling and display. The browser
  suite separately measures input-to-display latency on a synthetic JPEG.
- **Memory** is the sum of resident memory in the scenario's server process and
  its descendants, sampled every 10 ms during measured requests. It includes
  retained caches, interpreter memory and the render child. Shared pages may be
  counted more than once. It excludes the benchmark driver/browser and may miss
  peaks shorter than the interval. Scenarios send requests sequentially, so only
  one of the app's two available render workers is needed.
- **Percentiles** use the median and nearest-rank p95. With five samples, p95
  is the slowest sample; use more samples for a steadier tail estimate.

Compare after changes to decoding, rendering or caching:

```sh
python scripts/benchmark_raw_previews.py \
  --manifest .context/raw-benchmark/corpus.json \
  --machine-label dedicated-benchmark-machine \
  --baseline .context/raw-benchmark/baseline.json \
  --output .context/raw-benchmark/current.json
```

The command exits unsuccessfully for p50/p95 increases exceeding both 25% and
50 ms, or peak-memory increases exceeding both 15% and 64 MiB. It refuses
comparisons with different inputs, sizes, sample counts, machine labels or
recorded software environments. These thresholds allow measurement noise;
confirm a regression on an otherwise idle machine before changing a baseline.
The local camera corpus is not uploaded to CI. CI runs the small real-LibRaw
smoke test and tests the comparison logic. Changes to this measurement protocol
must increment the report schema version.

Schema 2 reports include the executed command (with manifest/output/baseline paths
redacted), Git revision, dirty-tree flag, CPU model, CPU accelerator mode and
numeric-library versions. Commit code before recording a reference report so the
revision identifies the measured implementation. Source revisions are provenance,
not comparison constraints, because before/after runs must use different code.
The worker-process and aggregate-memory protocol differs from schema 1; the
historical reports below remain useful records but cannot be passed directly as
baselines to the current runner.

## Initial camera measurements

[Reference report](performance/raw-preview-reference.json), recorded on a shared
16-core arm64 Mac with four numeric threads, uses real 24 MP and 46 MP Nikon RAW
files copied to local storage. It contains five samples per scenario. The
24 MP measurements overlapped a browser-test run, so this is an initial
reference, not a dedicated-runner release budget. A 60 MP camera file was not
available for this run; the manifest can include one without code changes.

| Source | Preview | Cache | Median | p95 | Peak worker memory |
| --- | --- | --- | ---: | ---: | ---: |
| 24 MP | Quick | Cold | 1.46 s | 1.61 s | 1,211 MiB |
| 24 MP | Quick | Warm | 169 ms | 345 ms | 1,228 MiB |
| 24 MP | Native | Cold | 9.68 s | 9.82 s | 2,703 MiB |
| 24 MP | Native | Warm | 8.43 s | 8.46 s | 2,775 MiB |
| 46 MP | Quick | Cold | 2.44 s | 2.64 s | 2,080 MiB |
| 46 MP | Quick | Warm | 164 ms | 167 ms | 1,413 MiB |
| 46 MP | Native | Cold | 20.72 s | 21.01 s | 4,029 MiB |
| 46 MP | Native | Warm | 19.26 s | 20.89 s | 4,093 MiB |

These measurements establish a starting point; they are not a before/after
speed comparison. Memory includes allocations retained by the process allocator
after decoding, in addition to live buffers. The report also includes the fit
scenarios and every individual timing sample.

## Native-resolution filtering optimization

Profiling the 46 MP endpoint identified the tone pass, particularly spatial
Shadows/Highlights, as the main rendering cost. Its one-million-pixel row strips
were only 120 rows tall at 8,288 pixels wide, with up to 96 extra rows of filter
support. Replacing those strips with rectangular tiles retains the same
48-pixel neighborhood on every edge while reducing processed pixels from
81.55 million to 54.34 million for the 45.75-million-pixel frame. The core tile
budget and tone equations are unchanged. Images that fit the budget still use
one pass; pointwise edits retain contiguous row strips.

A fresh sequential comparison on the same shared arm64 Mac, with the same
24 MP and 46 MP files and four numeric threads, produced these results. The
[before report](performance/raw-preview-tiling-before.json) used `d6aa7b8ae`;
the [after report](performance/raw-preview-tiling-after.json) used the rectangular
tiling change. Each scenario contains five samples. No other tests or benchmark
runs from this task ran concurrently with either measurement run.

| Source | Cache | Median before → after | p95 before → after | Peak worker MiB before → after |
| --- | --- | ---: | ---: | ---: |
| 24 MP | Cold | 9.57 → 7.65 s | 9.75 → 7.86 s | 2,581 → 2,480 |
| 24 MP | Warm | 8.35 → 6.68 s | 8.48 → 6.80 s | 2,642 → 2,453 |
| 46 MP | Cold | 20.94 → 13.88 s | 20.97 → 14.40 s | 3,774 → 3,650 |
| 46 MP | Warm | 17.85 → 11.91 s | 18.48 → 11.96 s | 3,791 → 3,707 |

Warm native medians improved by 20% and 33%, respectively. Memory improvements
are smaller because peak worker memory also includes decoded-source caches,
full-frame buffers, and allocator retention. All twelve quick/fit/native,
cold/warm scenarios passed the existing latency and memory comparison thresholds.
Quick and fit measurements remain subject to shared-machine noise. Full-resolution
decoding is unchanged, and native previews still take several seconds. The command above reproduces the comparison by recording the before
report on the base revision and passing it as `--baseline` on the changed code.

[Output comparisons](performance/raw-preview-tiling-output-parity.json) rendered
both cameras at native resolution with the benchmark recipe and an extreme
Shadows/Highlights recipe. All four comparisons had identical rounded 8-bit and
16-bit channels and identical quality-90 JPEGs when supplied the same ICC profile
bytes (excluding profile creation timestamps). Up to five float channels per
render differed by at most 8.95e-8 because box-filter accumulation order changes
with tile origins. Automated tests bound that rounding error and verify tile
seams, corners, image boundaries, alpha preservation, local weights, and source
immutability. A work-count regression guards overlap cost without relying on
wall-clock timing in CI.

## Preview worker measurements

The [schema 2 worker report](performance/raw-preview-workers.json) records the
24 MP and 46 MP corpus on an Apple M3 Max, using four numeric threads and five
samples per scenario. It was recorded from clean revision `51023c555` after the
test runs finished. Every request used the supervised child-process path and
passed the linear-source and output-dimension checks.

| Source | Preview | Cache | Median | p95 | Peak server and child memory |
| --- | --- | --- | ---: | ---: | ---: |
| 24 MP | Quick | Cold | 1.32 s | 1.62 s | 1,209 MiB |
| 24 MP | Quick | Warm | 143 ms | 153 ms | 1,138 MiB |
| 24 MP | Native | Cold | 6.63 s | 6.88 s | 2,467 MiB |
| 24 MP | Native | Warm | 5.62 s | 5.70 s | 2,509 MiB |
| 46 MP | Quick | Cold | 2.26 s | 2.64 s | 1,946 MiB |
| 46 MP | Quick | Warm | 142 ms | 157 ms | 1,407 MiB |
| 46 MP | Native | Cold | 12.55 s | 12.84 s | 3,639 MiB |
| 46 MP | Native | Warm | 10.60 s | 10.86 s | 3,694 MiB |

This is a new reference under the process-worker protocol, not a speedup claim
against the historical reports. It measures sequential requests with one active
render child; simultaneous renders can consume more memory. The report includes
the command, complete source revision, CPU and library versions for repeat runs.

## Stalled preview recovery

The editor gives an image request 60 seconds from dispatch. Reusing a request
for newer input does not extend that deadline. On timeout it detaches the old
image, releases the queue slot and starts the latest pending edit. The last
displayed image stays visible. If the current request fails with no replacement,
the footer offers **Retry preview**; editing also schedules a new request.
There is no automatic retry loop. Navigation clears the old timer, and late
load/error events cannot replace the new preview or release its queue slot.

The server runs editor previews in at most two spawned child processes per app
instance, with up to eight queued requests and a 45-second deadline including
queue time. Overload returns 503, timeout returns 504, and superseded work returns
409. A random session per editor page and increasing request sequence make the
latest edit replace earlier work while keeping tabs independent. Photo navigation,
page exit and browser timeout send an explicit cancellation; a subsequent render
also rejects older arrivals. Cancellation sequences are retained for up to 1,024
sessions, evicting the oldest idle sessions when needed. Requests from clients
without session parameters still use the bounded workers.

Superseded or stalled child processes are terminated and reaped before replacement,
so a stuck native decoder cannot block the same tab's retry indefinitely. Healthy
workers retain decoded-source caches and prefer the same tab; idle workers exit
after 60 seconds to release memory. Killing a worker also discards its cache, so
the next edit may need a cold decode. These are concurrency and lifetime limits,
not a fixed memory cap; two large native renders can still use substantial RAM.

Only control messages cross the process pipe. Request metadata and encoded output
use private temporary files that are cleaned after use. The parent holds the
working-copy eviction guard while an offline source is rendered, and waiting for
that guard is cancellable. Source selection, RAW precision, fallback, crop and
local-mask rendering use the existing renderer. The parent still performs source
metadata checks; this does not promise to interrupt an operating-system filesystem
call that blocks there. A dropped connection alone cannot cancel older clients'
work, but the server deadline still applies.
