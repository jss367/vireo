# Native Nikon High Efficiency RAW decoding

Vireo desktop builds use a temporary rawpy 0.28.0 build with the official
LibRaw November 2026 snapshot to decode Nikon HE and HE* sensor data.
This enables RAW white balance and highlight processing instead of using
the embedded JPEG fallback. Browsing still prefers embedded JPEGs for speed.
The edit-render cache version advances so existing edited previews regenerate
with sensor data when their originals are available.

## Build and source installs

The release workflow builds and installs the custom wheel before freezing
the sidecar. `scripts/build_sidecar.py` refuses an older decoder, so
`scripts/release.sh` (local non-publish builds) and `scripts/build_signed.sh`
also require the snapshot; `release.sh` fails fast with the install command
before running the E2E gate, and `build_signed.sh` fails at the sidecar step.
Ordinary `pip install -e .` and `uv sync` still select published rawpy wheels;
source installations opt in by running the builder **after** installing
Vireo's dependencies, with the same Python interpreter:

```sh
python scripts/build_rawpy_snapshot.py
python scripts/build_rawpy_snapshot.py --check
```

Prerequisites: Git, a C++ compiler, Python with pip, and platform build tools.
On macOS, install `pkgconf autoconf automake libtool ninja` with Homebrew.
On Linux, install `pkg-config autoconf automake libtool ninja-build` with the
system package manager. On Windows, run from an x64 MSVC developer shell.
The shared CI action `.github/actions/build-raw-decoder/action.yml` sets these up.
Build parallelism is capped at four processes.

The builder pins full Git revisions for rawpy, LibRaw, LibRaw's CMake support,
and vcpkg. Codec dependencies (Little CMS, libjpeg-turbo with JPEG 8 support,
Jasper, and zlib) are built statically through the pinned vcpkg ports.
The resulting rawpy version is `0.28.0+vireo.libraw202611`; it carries source
provenance, licenses, and its private LibRaw shared library. The build checks
both compiled and loaded LibRaw versions and retains the existing codec flags.
It never changes system LibRaw or a running Vireo installation.

Wheel and corresponding source archives are written to `build/rawpy-snapshot/`.
The release workflow publishes those source archives beside the installers.
Temporary upstream checkouts are deleted automatically. vcpkg's binary cache
can be reused; CI caches it separately from the source trees.

Running dependency synchronization again may restore the published wheel.
Rerun the builder afterward; `--check` reports exactly which decoder is loaded.

## Verification

```sh
python scripts/check_nikon_raw.py --report build/nikon-raw-results.json
python scripts/smoke_raw_decoder.py
python -m pytest tests/test_rawpy_snapshot.py vireo/tests/test_image_loader.py vireo/tests/test_raw_precision.py -q
```

The first command downloads checksum-pinned CC0 photographs from raw.pixls.us:
Z8 HE, Z8 HE*, Z9 HE, Z9 HE*, and a Z8 lossless control. It requires native
8280 × 5520 Bayer data with more than 8-bit precision, performs 16-bit
demosaicing, and checks that embedded JPEG extraction still works. Opening a
file or displaying a JPEG alone does not pass. Sensor hashes in the report
are checked against recorded baselines across platforms and future decoder versions; they are
regression evidence, not an independent proof of pixel correctness.

The second command freezes a minimal executable with the same rawpy hook as
the desktop app, then decodes the samples with library search overrides removed.
The dedicated Nikon RAW decoder CI workflow runs both on macOS, Windows, and Linux.
Sample originals stay outside Git. Your own photographs can be checked through
Vireo's normal RAW editing path after installing the wheel.

LibRaw documents about 400 MB of intermediate buffers per 45 MP HE/HE* decode,
in addition to Vireo's rendering buffers. This does not change browsing's
JPEG-first behavior or increase Vireo's worker counts.

## Removing the temporary build

Once a published rawpy release bundles the official HE/HE* decoder:

1. Raise Vireo's rawpy minimum and refresh `uv.lock` to that release.
2. Delete `scripts/build_rawpy_snapshot.py`, its build action, the snapshot unit
   tests, and the pre-freeze snapshot check in `scripts/build_sidecar.py`.
3. Remove the custom-build and source-archive steps from the release workflow.
4. Keep `scripts/check_nikon_raw.py`, the rawpy packaging hook, and the frozen
   decoder smoke check. Replace the smoke check's snapshot assertion with the
   released version check, and have its CI install the official wheel.
5. Rerun native and frozen decoding on all platforms, compare the sensor hashes,
   and retain Vireo's RAW precision and fallback tests.
6. Update these instructions and the third-party notices to describe the
   official distribution and its included licensing/source information.

Upstream: [LibRaw's official snapshot changelog](https://github.com/LibRaw/LibRaw/blob/a9a14b1970d188c25dade81f8326ff00a6e33649/Changelog.txt).
