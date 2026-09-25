"""iNaturalist taxonomy: download, parse, and lookup.

Includes two loading approaches:
1. DWCA-based: Downloads the DarwinCore Archive zip and builds a JSON lookup file.
2. AWS open-data based: Downloads taxa.csv.gz and loads taxa into the SQLite
   database for structured querying. This is the newer approach used by
   load_taxa_from_file() and load_taxonomy().

Data source (AWS): https://inaturalist-open-data.s3.amazonaws.com/taxa.csv.gz
Format: Tab-separated, 6 columns: taxon_id, ancestry, rank_level, rank, name, active

Usage:
    python vireo/taxonomy.py --download [--output taxonomy.json]
"""

import argparse
import contextlib
import csv
import gzip
import hashlib
import io
import json
import logging
import os
import re
import socket
import ssl
import stat as stat_module
import tempfile
import threading
import time
import unicodedata
import urllib.error
import urllib.request
import zipfile
from datetime import date

import certifi
import requests

log = logging.getLogger(__name__)

# Use certifi's CA bundle so HTTPS works on macOS without Install Certificates.command
_ssl_ctx = ssl.create_default_context(cafile=certifi.where())


class DownloadCancelled(Exception):
    """Raised when should_cancel() asked us to stop."""


# Old scientific name (lowercase) -> current accepted scientific name.
# taxonomy.json indexes only currently-accepted names, but classifier class
# lists are frozen at training time (iNat21 is 2021 vintage), so renamed taxa
# ("Bubulcus ibis" -> "Ardea ibis") would otherwise miss every lookup and
# leak raw binomials into predictions and needless review conflicts. The map
# ships with the app (scripts/build_taxonomy_synonyms.py regenerates it) and
# is consulted only after direct lookups miss.
SCIENTIFIC_SYNONYMS_PATH = os.path.join(
    os.path.dirname(__file__), "data", "scientific_name_synonyms.json"
)
_SCIENTIFIC_SYNONYMS = None
_scientific_synonyms_lock = threading.Lock()
# (loaded map object, digest) — see scientific_synonyms_identity().
_SCIENTIFIC_SYNONYMS_IDENTITY = None


def load_scientific_synonyms():
    """Load the packaged scientific-name synonym map (cached per process).

    Returns {} when the data file is missing or unreadable — synonym
    resolution is an enhancement, never a load-time failure.
    """
    global _SCIENTIFIC_SYNONYMS
    if _SCIENTIFIC_SYNONYMS is not None:
        return _SCIENTIFIC_SYNONYMS
    with _scientific_synonyms_lock:
        if _SCIENTIFIC_SYNONYMS is not None:
            return _SCIENTIFIC_SYNONYMS
        try:
            with open(SCIENTIFIC_SYNONYMS_PATH) as f:
                loaded = json.load(f)
            if not isinstance(loaded, dict):
                raise ValueError("synonym file is not a JSON object")
        except (OSError, ValueError) as e:
            # Loud enough to diagnose, not fatal: name the path and the
            # likeliest cause. An empty map disables the whole synonym fix
            # (raw binomials and false model disagreements come back) with
            # no other symptom, so a bare "unavailable" is a black box.
            log.warning(
                "Scientific-name synonyms unavailable at %s (%s); outdated "
                "binomials such as 'Bubulcus ibis' will not resolve to their "
                "current names. In an installed build this means the data "
                "file was not packaged — check [tool.setuptools.package-data] "
                "in pyproject.toml.",
                SCIENTIFIC_SYNONYMS_PATH, e,
            )
            loaded = {}
        _SCIENTIFIC_SYNONYMS = loaded
    return _SCIENTIFIC_SYNONYMS


def scientific_synonyms_identity():
    """Content identity of the synonym map actually backing lookups.

    ``Taxonomy.lookup`` falls back to this map, so the map is part of
    what a classifier run *means*: the same model and taxonomy.json can
    emit "Bubulcus ibis" with no map installed and "Cattle Egret" (plus
    the full hierarchy) once it is. Anything that identifies a run's
    output enrichment therefore has to include this alongside the
    taxonomy digest, or a pre-synonym run compares equal to a
    post-synonym one and the cache skips the work that would fix the
    raw binomials (Codex #1560 P2).

    Derived from the loaded mapping rather than the file bytes so the
    identity describes what lookups will actually use: a missing or
    malformed file loads as ``{}`` and yields the ``"no-synonyms"``
    sentinel, which is exactly the no-enrichment case. Memoized against
    the loaded map object, so it recomputes if the map is reloaded or
    replaced (tests monkeypatch it) but costs nothing on the hot path.
    """
    global _SCIENTIFIC_SYNONYMS_IDENTITY
    synonyms = load_scientific_synonyms()
    cached = _SCIENTIFIC_SYNONYMS_IDENTITY
    if cached is not None and cached[0] is synonyms:
        return cached[1]
    if not synonyms:
        digest = "no-synonyms"
    else:
        payload = json.dumps(
            {str(k).lower(): str(v) for k, v in synonyms.items()},
            sort_keys=True, separators=(",", ":"),
        )
        digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    _SCIENTIFIC_SYNONYMS_IDENTITY = (synonyms, digest)
    return digest


# `Content-Range: bytes */<total>` — the header a 416 response uses to tell
# the client what the actual size of the resource is. RFC 7233 §4.2 pins the
# format; a compliant server puts the total after the slash.
_CONTENT_RANGE_TOTAL_RE = re.compile(r"bytes\s*\*/(\d+)", re.IGNORECASE)

# `Content-Range: bytes <start>-<end>/<total>` — the header a 206 response
# uses to describe which bytes the body actually spans. If the server (or an
# intermediary) resumes from a different byte than we asked for, appending
# would splice mismatched content into the partial file.
_CONTENT_RANGE_START_RE = re.compile(r"bytes\s+(\d+)\s*-", re.IGNORECASE)


def _content_range_total(headers):
    """Total resource size advertised by a Content-Range: bytes */<total>.

    Returns None when the header is absent or malformed — the caller falls
    back to restarting the download from scratch, which is the safe outcome.
    """
    if headers is None:
        return None
    header = headers.get("Content-Range")
    if not header:
        return None
    match = _CONTENT_RANGE_TOTAL_RE.search(header)
    if not match:
        return None
    try:
        return int(match.group(1))
    except (TypeError, ValueError):
        return None


def _content_range_start(headers):
    """First byte covered by a Content-Range: bytes <start>-<end>/<total>.

    Returns None when the header is absent or malformed. Used to detect a
    206 that begins at a different byte than the client's Range request so
    the caller can restart from scratch rather than splice mismatched bytes
    onto its existing ``.partial``.
    """
    if headers is None:
        return None
    header = headers.get("Content-Range")
    if not header:
        return None
    match = _CONTENT_RANGE_START_RE.search(header)
    if not match:
        return None
    try:
        return int(match.group(1))
    except (TypeError, ValueError):
        return None


def _download_with_resume(url, dest_path, progress_callback=None,
                          max_stalled=3, chunk_size=256 * 1024,
                          *, byte_callback=None, should_cancel=None,
                          _emit_interval=0.25):
    """Download a file with retry and resume support.

    Streams to ``dest_path + ".partial"``, resuming from the last byte on
    failure using HTTP Range headers.  Retries indefinitely as long as each
    attempt downloads new data; gives up after *max_stalled* consecutive
    failures with no progress.

    Args:
        url: URL to download.
        dest_path: Final file path.  A ``.partial`` sibling is used during
            download and renamed on success.
        progress_callback: optional ``callback(message)`` for status updates.
        max_stalled: Give up after this many consecutive zero-progress retries.
        chunk_size: Bytes per read chunk (default 256 KB).
        byte_callback: optional ``callback(downloaded, total_or_None)`` for
            byte-level progress.  Throttled to ~4 Hz.
        should_cancel: optional ``callback() -> bool``.  When it returns True
            the download aborts, leaving the ``.partial`` file for resume.
        _emit_interval: minimum seconds between byte_callback emits (private;
            for tests).

    Neither ``byte_callback`` nor ``should_cancel`` may raise: they are called
    inside the transfer's ``try`` block, so an exception from either is caught
    by the generic handler and misreported to the user as a network failure,
    triggering a full retry (and discarding the partial when the server
    answers 200).  Callers must swallow their own errors.
    """
    partial_path = dest_path + ".partial"
    attempt = 0
    stalled_count = 0

    while True:
        attempt += 1
        downloaded_before = os.path.getsize(partial_path) if os.path.exists(partial_path) else 0

        try:
            req = urllib.request.Request(url)
            req.add_header("User-Agent", "vireo-taxonomy/1.0")
            if downloaded_before > 0:
                req.add_header("Range", f"bytes={downloaded_before}-")
                if progress_callback:
                    mb = downloaded_before // (1024 * 1024)
                    progress_callback(f"Resuming download at {mb} MB (attempt {attempt})...")
                log.info("Resuming download at byte %d (attempt %d)", downloaded_before, attempt)
            else:
                if attempt == 1:
                    if progress_callback:
                        progress_callback(f"Downloading {url.rsplit('/', 1)[-1]}...")
                    log.info("Downloading %s ...", url)
                else:
                    if progress_callback:
                        progress_callback(f"Retrying download (attempt {attempt})...")
                    log.info("Retrying download (attempt %d)", attempt)

            with urllib.request.urlopen(req, timeout=120, context=_ssl_ctx) as resp:
                # Interrupt a stalled resp.read from a watcher thread when the
                # caller asks to cancel.  Without this, a Stop press during a
                # stalled read is not felt until the socket read returns or
                # the 120 s timeout expires — up to two minutes on the
                # unreliable-network scenario where cancellation is most
                # needed.  resp.close() would only set a flag; it does not
                # unblock a blocked recv on the underlying socket.  A
                # socket.shutdown(SHUT_RDWR) does — it forces the kernel to
                # return the pending recv with an error, which the outer
                # handler then reclassifies as DownloadCancelled (see the
                # should_cancel() check below).
                cancel_watcher_stop = threading.Event()
                watcher = None
                if should_cancel is not None:
                    # Default-argument bind so the closure captures THIS
                    # iteration's stop event / resp / cancel callback rather
                    # than the outer-scope names — otherwise a late thread
                    # could observe the next iteration's variables (and ruff
                    # B023 objects to the same issue).
                    def _close_on_cancel(
                        _stop=cancel_watcher_stop,
                        _resp=resp,
                        _cancel=should_cancel,
                    ):
                        while not _stop.wait(0.25):
                            if _cancel():
                                # SocketIO -> raw socket.  Not part of the
                                # public API, but the only reliable way to
                                # break a recv from another thread — see
                                # the module comment above.
                                sock = getattr(getattr(_resp, "fp", None),
                                               "raw", None)
                                sock = getattr(sock, "_sock", None)
                                if sock is not None:
                                    # Already closed or half-closed — the
                                    # blocked read will surface the error
                                    # regardless.
                                    with contextlib.suppress(OSError):
                                        sock.shutdown(socket.SHUT_RDWR)
                                return
                    watcher = threading.Thread(
                        target=_close_on_cancel, daemon=True,
                        name="download-cancel-watcher",
                    )
                    watcher.start()

                try:
                    # If server returned 200 (not 206), it doesn't support
                    # Range — start from scratch.  Don't reset
                    # downloaded_before: it's the stall-detection baseline
                    # (did we get further than last time?).
                    if resp.status == 200 and downloaded_before > 0:
                        log.info("Server does not support Range; restarting download")

                    # Determine expected size so we can detect truncated responses
                    content_length = resp.headers.get("Content-Length")
                    expected_bytes = int(content_length) if content_length else None

                    mode = "ab" if resp.status == 206 else "wb"
                    # A 206 must begin exactly at ``downloaded_before`` — the
                    # byte we asked to resume from. A proxy or rebuilt artifact
                    # can return a valid range that starts elsewhere; appending
                    # that body onto our ``.partial`` would silently splice
                    # mismatched bytes into the file. On mismatch, restart
                    # from scratch. (For darktable installs the SHA256 check
                    # catches it; download_taxa() has no digest and would fail
                    # later as an opaque gzip/CSV parse error.)
                    if mode == "ab":
                        range_start = _content_range_start(resp.headers)
                        if (
                            range_start is not None
                            and range_start != downloaded_before
                        ):
                            log.warning(
                                "Server resumed at byte %d, expected %d;"
                                " restarting download",
                                range_start, downloaded_before,
                            )
                            mode = "wb"
                    # Bytes already on disk that we are keeping. Distinct from
                    # downloaded_before, which stays put as the stall baseline even
                    # when mode == "wb" truncates the partial.
                    progress_base = downloaded_before if mode == "ab" else 0
                    expected_total = (
                        expected_bytes + progress_base
                        if expected_bytes is not None
                        else None
                    )
                    received = 0
                    with open(partial_path, mode) as f:
                        last_emit = 0.0
                        while True:
                            if should_cancel is not None and should_cancel():
                                raise DownloadCancelled("Download cancelled")
                            chunk = resp.read(chunk_size)
                            if not chunk:
                                break
                            f.write(chunk)
                            received += len(chunk)
                            if byte_callback is not None:
                                now = time.monotonic()
                                if now - last_emit >= _emit_interval:
                                    last_emit = now
                                    byte_callback(progress_base + received, expected_total)
                        if byte_callback is not None:
                            byte_callback(progress_base + received, expected_total)

                    # Check for truncated response
                    if expected_bytes is not None and received < expected_bytes:
                        raise OSError(
                            f"Incomplete download: got {received} of "
                            f"{expected_bytes} bytes"
                        )
                finally:
                    cancel_watcher_stop.set()
                    if watcher is not None:
                        watcher.join(timeout=2)

        except DownloadCancelled:
            raise
        except Exception as e:
            # 416 Requested Range Not Satisfiable: our Range starts at or
            # past the end of the resource.  Reachable when cancellation
            # lands after the final chunk is written but before the
            # empty-read break — the loop's cancel check raises, leaving a
            # ``.partial`` already at the full size, so the next attempt
            # asks for ``bytes=<full>-``.  Without this branch the retry
            # loop would burn ``max_stalled`` attempts against a permanent
            # 416 and give up despite promising "try again and the download
            # will resume".  Verify the total via Content-Range and, when
            # sizes agree, promote the partial to the final path.  On
            # mismatch (the release was rebuilt) delete the stale partial
            # and let the next iteration re-download from byte 0 — that is
            # safer than shipping bytes of the wrong artifact to the
            # caller's digest check.
            if (
                isinstance(e, urllib.error.HTTPError)
                and e.code == 416
                and downloaded_before > 0
            ):
                total = _content_range_total(getattr(e, "headers", None))
                current_size = (
                    os.path.getsize(partial_path)
                    if os.path.exists(partial_path)
                    else 0
                )
                if total is not None and current_size == total:
                    if os.path.exists(dest_path):
                        os.remove(dest_path)
                    os.rename(partial_path, dest_path)
                    if byte_callback is not None:
                        byte_callback(current_size, total)
                    mb = current_size // (1024 * 1024)
                    log.info(
                        "Server reported 416 with matching total (%d bytes);"
                        " promoting complete .partial to %s",
                        total, dest_path,
                    )
                    if progress_callback:
                        progress_callback(f"Downloaded {mb} MB")
                    return dest_path
                # Stale or unverifiable partial: delete so the next iteration
                # restarts from byte 0 rather than sending the same doomed
                # Range header on every attempt.  If the removal fails
                # (permission denied, Windows lock, read-only dir),
                # ``downloaded_before`` stays > 0 next iteration, the same
                # Range is sent, the server answers 416 again, and the loop
                # would spin forever — count it as a stall so ``max_stalled``
                # eventually breaks us out with a real error.
                try:
                    os.remove(partial_path)
                except OSError as remove_error:
                    stalled_count += 1
                    log.warning(
                        "Could not remove stale partial %s after 416: %s"
                        " (stalled %d/%d)",
                        partial_path, remove_error, stalled_count, max_stalled,
                    )
                    if stalled_count >= max_stalled:
                        raise RuntimeError(
                            f"Server rejected resume range and the partial "
                            f"file {partial_path} could not be removed: "
                            f"{remove_error}"
                        ) from e
                log.warning(
                    "Server returned 416 but partial size (%d) does not match"
                    " advertised total (%r); restarting download",
                    current_size, total,
                )
                if progress_callback:
                    progress_callback(
                        f"Resume rejected by server (attempt {attempt}), "
                        f"restarting download..."
                    )
                # Poll cancellation during the backoff so Stop feels
                # responsive on a rejected-resume loop too.
                for _ in range(6):
                    if should_cancel is not None and should_cancel():
                        raise DownloadCancelled("Download cancelled") from None
                    time.sleep(0.5)
                continue

            # The watcher thread closes ``resp`` on cancel, which surfaces
            # here as a socket error.  Reclassify it before the stall
            # detection can turn "user pressed Stop" into "download stalled
            # after N attempts" — the user asked to cancel, so ``from None``
            # keeps that unrelated network error out of the traceback.
            if should_cancel is not None and should_cancel():
                raise DownloadCancelled("Download cancelled") from None
            current_size = os.path.getsize(partial_path) if os.path.exists(partial_path) else 0
            gained = current_size - downloaded_before

            if gained > 0:
                stalled_count = 0
                mb = current_size // (1024 * 1024)
                log.info("Download interrupted at %d MB, will resume: %s", mb, e)
                if progress_callback:
                    progress_callback(f"Connection lost at {mb} MB, retrying in 3s...")
            else:
                stalled_count += 1
                log.warning(
                    "Download attempt %d: no progress (%d/%d stalled): %s",
                    attempt, stalled_count, max_stalled, e,
                )
                if progress_callback:
                    progress_callback(f"Download failed (attempt {attempt}), retrying in 3s...")

            if stalled_count >= max_stalled:
                mb = current_size // (1024 * 1024)
                raise RuntimeError(
                    f"Download stalled after {attempt} attempts with no new data. "
                    f"Downloaded {mb} MB so far. "
                    f"The partial file is kept at {partial_path} — "
                    f"try again and the download will resume."
                ) from e

            # Poll the backoff in short slices so Cancel is felt within ~0.5 s
            # instead of after the full 3 s wait.  `from None`: the user asked
            # to stop, so the network error we were backing off from is not the
            # cause and must not be chained onto the cancel.
            for _ in range(6):
                if should_cancel is not None and should_cancel():
                    raise DownloadCancelled("Download cancelled") from None
                time.sleep(0.5)
            continue

        # Success — rename partial to final
        if os.path.exists(dest_path):
            os.remove(dest_path)
        os.rename(partial_path, dest_path)
        size_mb = os.path.getsize(dest_path) // (1024 * 1024)
        log.info("Downloaded %s (%d MB)", dest_path, size_mb)
        if progress_callback:
            progress_callback(f"Downloaded {size_mb} MB")
        return dest_path

DWCA_URL = "https://www.inaturalist.org/taxa/inaturalist-taxonomy.dwca.zip"

# Persistent path for the DWCA-based taxonomy.json. Lives under ~/.vireo so
# it survives app restarts — in PyInstaller-bundled builds the package
# directory is an ephemeral _MEI* extraction dir that's rebuilt per run.
TAXONOMY_JSON_PATH = os.path.expanduser("~/.vireo/taxonomy.json")

# Fallback for dev checkouts that downloaded a taxonomy.json next to this
# module before the persistent path existed. Named so tests (and the browser
# suite, which must not pick up a developer's ~500MB local copy) can
# monkeypatch it instead of patching os.path.dirname globally.
LEGACY_TAXONOMY_JSON_PATH = os.path.join(os.path.dirname(__file__), "taxonomy.json")


def find_taxonomy_json():
    """Return the first existing taxonomy.json path, or the persistent path.

    Prefers ~/.vireo/taxonomy.json, then falls back to a taxonomy.json next
    to this module (for dev checkouts where a taxonomy.json was committed
    or previously downloaded). Always returns a path — callers should check
    os.path.exists() if they need to know whether data is actually present.

    For loading (not just path-checking) use load_local_taxonomy(), which
    also tries the legacy path when the persistent file is unreadable.
    """
    if os.path.exists(TAXONOMY_JSON_PATH):
        return TAXONOMY_JSON_PATH
    if os.path.exists(LEGACY_TAXONOMY_JSON_PATH):
        return LEGACY_TAXONOMY_JSON_PATH
    return TAXONOMY_JSON_PATH


# Process-wide cache for the parsed taxonomy. A full iNaturalist
# taxonomy.json is ~500MB of JSON that expands to a couple of GB of dicts
# and takes seconds to parse, and request handlers (notably
# /api/predictions/compare) plus accept/replace call load_local_taxonomy()
# on every invocation. Re-parsing per call made those requests take
# seconds-to-minutes and let concurrent requests each allocate their own
# copy. Keyed by (path, mtime_ns, size) so a re-downloaded taxonomy is
# still picked up without restarting the app.
_taxonomy_cache_lock = threading.Lock()
_taxonomy_cache = None  # (path, stat_key, Taxonomy)

# Per-path record of parses that failed (e.g., corrupt JSON). Keyed by path
# and validated against the current stat key on lookup, so a repaired or
# re-downloaded file automatically drops its old failure record. Without
# this, a corrupt preferred candidate would be re-parsed on every request
# — expensive on its own, and the trigger for a worse problem: it prevents
# us from evicting the fallback cache before parsing, because the corrupt
# case would otherwise re-parse the multi-GB fallback each time.
_taxonomy_failed_stats = {}  # {path: stat_key}


class _KnownCorruptTaxonomy(Exception):
    """Raised when a path is known to have failed to parse at its current stat."""


# Parse failures that are environmental rather than the file's fault, so a
# later attempt at the same bytes can legitimately succeed: a read permission
# bit, momentary fd exhaustion, an allocation failure on a ~2.8GB parse. These
# must stay retryable — memoizing them against (mtime_ns, size) would key the
# record to a stat the repair does not change. Everything else (malformed JSON
# raising ValueError, or valid JSON in the wrong shape raising AttributeError
# or TypeError as the parser walks it) is the content itself and cannot fix
# itself without a rewrite, which does change the stat.
_TRANSIENT_TAXONOMY_ERRORS = (OSError, MemoryError)


# Cap on how many times _load_taxonomy_cached will re-parse a file that
# keeps changing mid-read. A rewrite during parse means the parsed object
# does not correspond to the post-parse stat, so caching that pair would
# serve stale data forever. If the file is still moving after this many
# tries, we return the last parse without caching it and let the next
# call try again.
_TAXONOMY_PARSE_RETRY_LIMIT = 3


def clear_taxonomy_cache():
    """Drop the cached Taxonomy so the next load re-reads from disk."""
    global _taxonomy_cache
    with _taxonomy_cache_lock:
        _taxonomy_cache = None
        _taxonomy_failed_stats.clear()


def _write_taxonomy_json_atomically(path, data):
    """Serialize ``data`` to ``path`` via a temp sibling and an atomic rename.

    Both taxonomy writers use this. `open(path, "w")` truncates the target
    for as long as it takes to serialize ~500MB, and anything reading it in
    that window — including _load_taxonomy_cached from a concurrent request
    — gets a partial document that will not parse; its retry loop is bounded
    and cannot wait out an in-place write. Rename is atomic within a
    filesystem, so a reader sees the whole old file or the whole new one,
    and an interrupted write leaves the previous taxonomy intact.

    Renames onto the *resolved* target: os.replace() would otherwise swap a
    symlink for a regular file, detaching a taxonomy linked in from
    elsewhere and leaving a duplicate ~500MB copy. Keeping the temp file
    beside the resolved target also keeps the rename within one filesystem,
    which is what makes it atomic.
    """
    target = os.path.realpath(path)
    # A unique temp name per write, not a fixed "<target>.tmp". Two writes
    # can overlap — POSTing the download endpoint twice starts two workers,
    # since it uses runner.start() rather than start_singleton() — and a
    # shared name lets one writer rename its inode out from under the other,
    # exposing a partial target and then failing the second writer when its
    # pathname has vanished.
    fd, tmp_path = tempfile.mkstemp(
        dir=os.path.dirname(target) or ".",
        prefix=f"{os.path.basename(target)}.",
        suffix=".tmp",
    )
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(data, f)
            # Closing only hands the bytes to the page cache. Without fsync,
            # a crash just after the rename can expose the target with
            # unflushed content — the half-written taxonomy this whole dance
            # exists to prevent.
            f.flush()
            os.fsync(f.fileno())
        # mkstemp creates 0600; open(path, "w") kept the target's mode.
        # Carry it over when there is a target to copy from, so writing
        # cannot silently loosen or tighten access. With no existing target
        # (a first download) the file stays 0600 — ~/.vireo is single-user
        # data, so private is the right default there.
        with contextlib.suppress(OSError):
            os.chmod(tmp_path, stat_module.S_IMODE(os.stat(target).st_mode))
        os.replace(tmp_path, target)
    except BaseException:
        with contextlib.suppress(OSError):
            os.remove(tmp_path)
        raise


def _taxonomy_stat_key(path):
    st = os.stat(path)
    # Identity and ctime, not just (mtime, size). A metadata-preserving
    # install — shutil.copy2, rsync -t, a restore from backup — can land
    # different content at the same size and mtime, and we would serve the
    # stale parse forever (and keep rejecting a repaired file as corrupt,
    # since _taxonomy_failed_stats uses the same key). The inode and device
    # change under an atomic rename; ctime changes on an in-place rewrite or
    # a permission repair.
    return (
        st.st_mtime_ns, st.st_size, st.st_dev, st.st_ino, st.st_ctime_ns,
    )


def _load_taxonomy_cached(path):
    """Return the parsed Taxonomy for ``path``, reusing the cached instance.

    The parse happens under the lock on purpose: without it, two requests
    arriving together would each build a multi-GB structure and thrash swap
    instead of one waiting for the other's result.
    """
    global _taxonomy_cache
    with _taxonomy_cache_lock:
        # Stat inside the lock. Read before it, a caller that then waits on
        # the lock compares a pre-wait stat against an entry another caller
        # refreshed while it waited, decides that entry is stale, evicts it
        # and re-parses ~2.8GB for nothing.
        stat_key = _taxonomy_stat_key(path)

        # Short-circuit a path we've already established is corrupt at this
        # stat. Without this, load_local_taxonomy() would attempt the parse
        # (and log a warning) on every request for as long as the file stays
        # broken. This also lets the eviction below evict cross-path fallback
        # entries safely: even if the current parse fails, a subsequent call
        # skips it here instead of re-parsing the multi-GB fallback to
        # rediscover it's still broken.
        failed_stat = _taxonomy_failed_stats.get(path)
        if failed_stat == stat_key:
            raise _KnownCorruptTaxonomy(path)
        if failed_stat is not None:
            # File has changed since it last failed; give it another chance.
            _taxonomy_failed_stats.pop(path, None)

        cached = _taxonomy_cache
        if cached is not None and cached[0] == path and cached[1] == stat_key:
            return cached[2]
        # We're about to parse. A parsed iNaturalist taxonomy is ~2.8GB, so
        # letting any prior instance stay reachable through _taxonomy_cache
        # (or the local ``cached`` tuple) while Taxonomy(path) allocates its
        # replacement doubles peak RSS — and a rewrite mid-parse stacks
        # another live copy on top of that per retry.
        #
        # Only evict an entry for *this* path. A cross-path entry is a live
        # fallback, not a stale copy, and dropping it here is not safe: the
        # failure memo covers content failures, but deliberately does not
        # cover transient ones (see _TRANSIENT_TAXONOMY_ERRORS). So a
        # preferred file that keeps raising OSError — wrong permissions, say
        # — is re-attempted on every request, and an unconditional evict
        # would drop the cached legacy taxonomy each time and re-parse
        # ~500MB of fallback per compare or accept.
        #
        # The migration case that motivated evicting cross-path (a cached
        # legacy still live while the newly-appeared preferred file parses)
        # is already covered: download_taxonomy() clears the cache before it
        # builds a replacement, so the normal path never holds both. A manual
        # drop-in can still hold two instances for one parse; that is a
        # one-off, where the fallback re-parse would be per-request forever.
        if cached is not None and cached[0] == path:
            _taxonomy_cache = None
        cached = None
        # Bracket the parse with a pre- and post-stat: if a background
        # download rewrites the file while Taxonomy(path) is reading it,
        # the parsed data is a stale snapshot even though a fresh
        # post-parse stat would look current. Caching that pair would
        # keep serving the old taxonomy indefinitely. Retry a few times
        # to get a stable read; if the file keeps changing, return the
        # latest parse but skip the cache so the next call re-checks.
        taxonomy = None
        # Keep only the *text* of a failed parse, never the exception. Its
        # traceback holds the Taxonomy.__init__ frame, which still
        # references the multi-GB dict json.load() just decoded — so
        # retaining it across the loop would keep a failed attempt alive
        # through the next allocation, defeating the between-retry release
        # right below.
        last_error_text = None
        for _ in range(_TAXONOMY_PARSE_RETRY_LIMIT):
            # Same peak-RSS reason: release the previous retry's
            # instance before Taxonomy(path) builds the next one, so a
            # file that keeps changing does not pile up N live copies.
            taxonomy = None
            pre_stat = _taxonomy_stat_key(path)
            try:
                taxonomy = Taxonomy(path)
            except Exception as parse_error:
                last_error_text = f"{type(parse_error).__name__}: {parse_error}"
                # A rewrite caught mid-stream — from an atomic-save gap
                # on another platform, an interrupted download, or an
                # external tool — leaves a partial JSON document that
                # Taxonomy() cannot parse. That is the same kind of
                # transient instability the stat-drift check retries
                # against; propagating here would surface as a spurious
                # failure that the very next call could succeed at, and
                # load_local_taxonomy() would silently fall through to
                # a stale or nonexistent alternate candidate instead.
                #
                # Only retry when the file actually moved, though. A file
                # that is simply corrupt fails identically every time, and
                # retrying re-reads most of ~500MB two more times on every
                # request — reinstating the latency and allocation pressure
                # this cache exists to remove, for a read that cannot
                # succeed. Truncated JSON only raises at the end of the
                # parse, so this is the expensive case, not the cheap one.
                try:
                    file_moved = _taxonomy_stat_key(path) != pre_stat
                except OSError:
                    # Cannot even stat it now; a re-read will not fare
                    # better, so report the parse failure we already have.
                    file_moved = False
                if not file_moved:
                    # Durable failure at this stat. Record it so subsequent
                    # requests short-circuit before hitting the parse and,
                    # crucially, before the eviction above would drop a
                    # still-valid fallback in a corrupt-preferred scenario.
                    #
                    # Only memoize a *content* failure, though — see
                    # _TRANSIENT_TAXONOMY_ERRORS. Memoizing an environmental
                    # failure would key the record to a stat its repair does
                    # not change, leaving taxonomy features off until the
                    # contents happen to change or the process restarts.
                    if not isinstance(parse_error, _TRANSIENT_TAXONOMY_ERRORS):
                        _taxonomy_failed_stats[path] = pre_stat
                    raise
                continue
            post_stat = _taxonomy_stat_key(path)
            if pre_stat == post_stat:
                _taxonomy_cache = (path, post_stat, taxonomy)
                return taxonomy
        # Retries exhausted. Nothing was cached for this path above, so
        # there is no stale entry left to clear — and clearing here would
        # evict a different path's still-valid entry. If every attempt
        # raised, surface a clean error rather than returning None and
        # letting the caller misread it as "file was fine but empty".
        if taxonomy is None:
            # Carry the last parse failure's message: load_local_taxonomy()
            # logs this, and "file kept changing" alone doesn't tell you
            # which byte of which file was malformed. The message rather
            # than the exception, so no traceback pins the decoded document.
            detail = f" (last error: {last_error_text})" if last_error_text else ""
            raise ValueError(
                f"Unable to parse {path}: file kept changing during read{detail}"
            )
        return taxonomy


def _drop_cached_taxonomy_path(path):
    """Release the cached parse for ``path`` (and its failure memo, if any).

    Used when the file has gone away: the entry can never be served again,
    and a full taxonomy is ~2.8GB of otherwise unreachable memory.
    """
    global _taxonomy_cache
    with _taxonomy_cache_lock:
        _taxonomy_failed_stats.pop(path, None)
        cached = _taxonomy_cache
        if cached is not None and cached[0] == path:
            _taxonomy_cache = None


def _restamp_taxonomy_cache(taxonomy):
    """Refresh the cache key after ``taxonomy`` rewrote its own file."""
    global _taxonomy_cache
    with _taxonomy_cache_lock:
        cached = _taxonomy_cache
        if cached is None or cached[2] is not taxonomy:
            return
        try:
            _taxonomy_cache = (
                cached[0], _taxonomy_stat_key(cached[0]), taxonomy,
            )
        except OSError:
            _taxonomy_cache = None


def load_local_taxonomy(path=None):
    """Load a Taxonomy from disk, falling back across known paths.

    Tries ~/.vireo/taxonomy.json first, then the package-dir legacy path.
    A truncated or corrupt persistent file (e.g., from an interrupted
    write) no longer disables taxonomy features if a valid legacy file
    is present. Returns a Taxonomy instance on success, or None if no
    readable taxonomy file exists.

    The returned instance is shared across callers and cached until the
    file on disk changes — treat it as read-mostly.

    Args:
        path: load only this file, with no fallback. Use it when a
            specific artifact has to be the one loaded: the post-download
            retype must fail loudly if the file it just wrote won't parse,
            rather than quietly retyping keywords from a stale legacy copy
            while the taxa tables hold the new download's data.
    """
    candidates = [path] if path else [TAXONOMY_JSON_PATH, LEGACY_TAXONOMY_JSON_PATH]
    for path in candidates:
        if not os.path.exists(path):
            # The file backing a cached parse is gone, so that ~2.8GB object
            # can never be served again. Release it here: _load_taxonomy_cached
            # deliberately keeps cross-path entries (a live fallback is not
            # stale), so a rollback to the legacy file would otherwise hold the
            # deleted file's parse alive while allocating the legacy one — and
            # with no fallback at all it would stay resident for the life of
            # the process.
            _drop_cached_taxonomy_path(path)
            continue
        try:
            return _load_taxonomy_cached(path)
        except _KnownCorruptTaxonomy:
            # We've already logged the real error for this stat; skip
            # quietly on subsequent requests so a persistently-broken
            # preferred file doesn't flood the log.
            continue
        except Exception as e:
            log.warning(
                "Failed to load taxonomy from %s: %s — trying next candidate",
                path, e,
            )
    return None


# --- AWS open-data taxa.csv.gz loader constants ---
TAXA_URL = "https://inaturalist-open-data.s3.amazonaws.com/taxa.csv.gz"

TARGET_KINGDOMS = {"Animalia", "Plantae", "Fungi"}
TARGET_KINGDOM_INAT_IDS = {1, 47126, 47170}  # Animalia, Plantae, Fungi

MAJOR_RANK_LEVELS = {70, 60, 50, 40, 30, 20, 10}  # kingdom through species
RANK_LEVEL_TO_NAME = {
    70: "kingdom", 60: "phylum", 50: "class", 40: "order",
    30: "family", 20: "genus", 10: "species",
}

INAT_API_BASE = "https://api.inaturalist.org/v1"
INAT_BATCH_SIZE = 30  # iNat API allows up to 30 IDs per request

# Ranks we care about, in order from broad to specific
RANK_ORDER = [
    "kingdom",
    "phylum",
    "class",
    "order",
    "family",
    "genus",
    "species",
    "subspecies",
]


COMMON_NAME_IDENTITY_VERSION = 1


def _scientific_entries(data):
    """Every taxon in a taxonomy.json, one entry per taxon.

    ``taxa_by_scientific`` holds one entry per name, so a cross-kingdom
    homonym (the bird and plant genera *Prunella*) keeps only one there;
    ``scientific_homonyms`` lists every taxon under such a name.
    """
    seen = set()
    for entries in ([*data.get("taxa_by_scientific", {}).values()],
                    *data.get("scientific_homonyms", {}).values()):
        for entry in entries:
            identity = entry.get("taxon_id")
            if identity is not None:
                if identity in seen:
                    continue
                seen.add(identity)
            yield entry


def _common_name_ambiguity(data):
    """Recover retained collisions and distrust lossy, unversioned DWCA indexes.

    Old dumps retained only one target per alternate name. No migration can
    reconstruct the discarded targets; a fresh download is required before
    those names can identify a species. Scientific names and IDs remain usable.
    """
    ambiguous = set(data.get("ambiguous_common_names", []))
    if (data.get("source") == "iNaturalist DWCA"
            and data.get("common_name_identity_version") != COMMON_NAME_IDENTITY_VERSION):
        ambiguous.update(data.get("taxa_by_common", {}))
    targets = {}
    for entry in _scientific_entries(data):
        name = (entry.get("common_name") or "").lower().strip()
        if not name:
            continue
        identity = entry.get("taxon_id") or entry.get("scientific_name")
        if name in targets and targets[name] != identity:
            ambiguous.add(name)
        targets[name] = identity
    return ambiguous


class Taxonomy:
    """Taxonomy lookup backed by a local JSON file.

    Args:
        taxonomy_path: path to taxonomy.json
    """

    def __init__(self, taxonomy_path):
        self._path = taxonomy_path
        with open(taxonomy_path) as f:
            data = json.load(f)
        self._by_common = data.get("taxa_by_common", {})
        self._by_scientific = data.get("taxa_by_scientific", {})
        # A name shared by several taxa can't identify one: lookups by that
        # name return None rather than an arbitrary kingdom's lineage.
        self._scientific_homonyms = data.get("scientific_homonyms", {})
        from species_identity import COMMON_NAME_CORRECTIONS, correct_common_name_index
        self._ambiguous_common = _common_name_ambiguity(data) - set(COMMON_NAME_CORRECTIONS)
        for name in self._ambiguous_common:
            self._by_common.pop(name, None)
        correct_common_name_index(self._by_common, self._by_scientific)
        self._api_misses = set(data.get("api_misses", []))
        self.last_updated = data.get("last_updated")
        self.taxa_count = len(self._by_common) + len(self._by_scientific)
        # Build normalized index for fuzzy lookups (handles hyphens, etc.)
        self._by_common_normalized = {}
        self._ambiguous_normalized = {self._normalize(n) for n in self._ambiguous_common}
        for key, val in self._by_common.items():
            nk = self._normalize(key)
            previous = self._by_common_normalized.get(nk)
            if previous and previous.get("scientific_name") != val.get("scientific_name"):
                self._ambiguous_normalized.add(nk)
            if nk not in self._by_common_normalized:
                self._by_common_normalized[nk] = val
        # Track whether new data was added (for save)
        self._dirty = False
        log.info(
            "Loaded taxonomy: %d entries (updated %s)",
            self.taxa_count,
            self.last_updated,
        )

    @staticmethod
    def _normalize(name):
        """Normalize a name for lookup: lowercase, fold punctuation, collapse spaces."""
        text = unicodedata.normalize("NFKC", name).casefold().strip()
        text = text.translate(str.maketrans({
            "’": "'",
            "‘": "'",
            "`": "'",
            "´": "'",
            "ʼ": "'",
            "ʹ": "'",
            "‛": "'",
            "“": '"',
            "”": '"',
            "„": '"',
            "‟": '"',
            "‐": "-",
            "‑": "-",
            "‒": "-",
            "–": "-",
            "—": "-",
            "―": "-",
        }))
        return " ".join(text.replace("-", " ").split())

    def lookup(self, name):
        """Look up a taxon by common name or scientific name.

        Handles punctuation differences like "Scrub-Jay" vs "scrub jay".

        Args:
            name: common name (e.g., "Song Sparrow") or scientific name

        Returns:
            dict with taxon_id, scientific_name, common_name, rank,
            lineage_names, lineage_ranks — or None if not found
        """
        key = name.lower().strip()
        if key in getattr(self, "_ambiguous_common", set()):
            return self._scientific(key)
        # A scientific homonym is unresolvable by bare name — check before
        # _by_common so an api_lookup that cached a query as a common name
        # (or any other stale entry) can't shadow the ambiguity.
        if key in getattr(self, "_scientific_homonyms", {}):
            return None
        result = self._by_common.get(key)
        if result:
            return result
        result = self._by_scientific.get(key)
        if result:
            return result

        # Outdated binomial? Resolve through the shipped synonym map
        # (e.g. "Bubulcus ibis" -> current entry for "Ardea ibis").
        current_name = load_scientific_synonyms().get(key)
        if current_name:
            result = self._scientific(current_name.lower())
            if result:
                return result

        # Fuzzy: try normalized lookup (handles hyphens, e.g. "scrub jay" vs "scrub-jay")
        if self._normalize(name) in getattr(self, "_ambiguous_normalized", set()):
            return None
        return self._by_common_normalized.get(self._normalize(name))

    def _scientific(self, key):
        """The one taxon named ``key``, or None when the name is a homonym."""
        if key in getattr(self, "_scientific_homonyms", {}):
            return None
        return self._by_scientific.get(key)

    def is_taxon(self, name):
        """Check if a name is a recognized taxon."""
        return self.lookup(name) is not None

    def lookup_id(self, taxon_id):
        """Resolve a source ID even after its scientific/common names change."""
        if not hasattr(self, "_by_taxon_id"):
            self._by_taxon_id = {
                entry["taxon_id"]: entry
                for entry in _scientific_entries({
                    "taxa_by_scientific": self._by_scientific,
                    "scientific_homonyms": getattr(self, "_scientific_homonyms", {}),
                })
                if entry.get("taxon_id") is not None
            }
        return self._by_taxon_id.get(taxon_id)

    def api_lookup(self, name):
        """Look up a name via the iNaturalist API (handles alternate/regional names).

        Queries the autocomplete endpoint which matches against all known
        common names, not just the preferred one. If a match is found, the
        alternate name is cached locally so future lookups are instant.
        Names that don't match are also cached to avoid repeated API calls.

        Returns:
            taxon dict (same shape as lookup()), or None
        """
        # Skip names we've already tried and failed to resolve
        norm_name = self._normalize(name)
        if norm_name in self._ambiguous_normalized:
            return None
        if norm_name in self._api_misses:
            return None
        # A query that is itself a scientific homonym must stay ambiguous:
        # the API returns whichever kingdom's row scored highest, so caching
        # its first ID-resolved hit as a common name would shadow the homonym
        # gate on every future lookup. Reject before touching the network.
        alt_key = name.lower().strip()
        if alt_key in getattr(self, "_scientific_homonyms", {}):
            return None

        import urllib.request

        try:
            q = urllib.parse.quote(name)
            url = f"https://api.inaturalist.org/v1/taxa/autocomplete?q={q}&per_page=5&rank=species,subspecies,genus,family,order,class,phylum,kingdom"
            req = urllib.request.Request(url)
            req.add_header("User-Agent", "vireo-taxonomy/1.0")
            with urllib.request.urlopen(req, timeout=10, context=_ssl_ctx) as resp:
                data = json.loads(resp.read())
        except Exception:
            log.debug("iNat API lookup failed for '%s'", name, exc_info=True)
            return None

        # Find a result where the matched_term matches our query
        for result in data.get("results", []):
            matched = result.get("matched_term", "")
            if self._normalize(matched) != norm_name:
                continue
            # Found a match — look up by the taxon's scientific name first
            sci = result.get("name", "").lower()
            existing = self.lookup_id(result.get("id")) or self._scientific(sci)
            if existing:
                # Cache this alternate name for future lookups
                self._by_common[alt_key] = existing
                self._by_common_normalized[norm_name] = existing
                self._dirty = True
                log.info(
                    "Resolved alternate name '%s' -> '%s' (%s) via iNat API",
                    name,
                    existing.get("common_name"),
                    existing.get("scientific_name"),
                )
                return existing

        # No match — remember this so we don't ask again
        self._api_misses.add(norm_name)
        self._dirty = True
        return None

    def save(self):
        """Persist any newly discovered alternate names and misses back to taxonomy.json."""
        if not self._dirty:
            return
        with open(self._path) as f:
            data = json.load(f)
        data["taxa_by_common"] = self._by_common
        data["ambiguous_common_names"] = sorted(self._ambiguous_common)
        data["api_misses"] = sorted(self._api_misses)
        _write_taxonomy_json_atomically(self._path, data)
        self._dirty = False
        # This instance is the one the cache hands out, and it already holds
        # everything we just wrote. Re-stamp the cache key so the rewrite
        # doesn't look like an external change and force a needless re-parse
        # of the file we authored.
        _restamp_taxonomy_cache(self)
        log.info("Saved updated taxonomy with new alternate names")

    def get_hierarchy(self, name):
        """Look up a species and return its full hierarchy as a flat dict.

        Returns:
            dict with keys: kingdom, phylum, class, order, family, genus,
            scientific_name — or empty dict if not found
        """
        taxon = self.lookup(name)
        if not taxon:
            return {}

        hierarchy = {"scientific_name": taxon.get("scientific_name", "")}
        lineage_names = taxon.get("lineage_names", [])
        lineage_ranks = taxon.get("lineage_ranks", [])

        for rank_name, sci_name in zip(lineage_ranks, lineage_names):
            if rank_name in ("kingdom", "phylum", "class", "order", "family", "genus"):
                hierarchy[rank_name] = sci_name

        return hierarchy

    def relationship(self, name_a, name_b):
        """Determine the taxonomic relationship between two names.

        Returns:
            'same' — identical taxon
            'ancestor' — a is an ancestor of b (a's name appears in b's lineage)
            'descendant' — b is an ancestor of a
            'sibling' — same immediate parent (same genus for species)
            'unrelated' — different lineages (no close relationship)
            None — one or both names not found in taxonomy
        """
        taxon_a = self.lookup(name_a)
        taxon_b = self.lookup(name_b)
        if taxon_a is None or taxon_b is None:
            return None

        lineage_a = [n.lower() for n in taxon_a["lineage_names"]]
        lineage_b = [n.lower() for n in taxon_b["lineage_names"]]

        sci_a = taxon_a["scientific_name"].lower()
        sci_b = taxon_b["scientific_name"].lower()

        # Same taxon
        if sci_a == sci_b:
            return "same"

        # a is an ancestor of b (a's scientific name appears in b's lineage)
        if sci_a in lineage_b:
            return "ancestor"

        # b is an ancestor of a
        if sci_b in lineage_a:
            return "descendant"

        # Sibling: same immediate parent (last shared ancestor is the direct parent of both)
        # For species, this means same genus
        if len(lineage_a) >= 2 and len(lineage_b) >= 2:
            parent_a = lineage_a[-2]
            parent_b = lineage_b[-2]
            if parent_a == parent_b:
                return "sibling"

        return "unrelated"


# --- AWS open-data taxa.csv.gz loader functions ---


def download_taxa(dest_path, progress_callback=None):
    """Download the iNat taxa.csv.gz file from AWS open data.

    Uses resumable download — safe on flaky connections.
    """
    return _download_with_resume(TAXA_URL, dest_path,
                                 progress_callback=progress_callback)


def load_taxa_from_file(db, gz_path):
    """Parse taxa.csv.gz and insert filtered taxa into the database.

    Filters to: active taxa, under Animalia/Plantae/Fungi, at major ranks.
    Resolves parent_id to the nearest ancestor also in the filtered set.

    Returns dict with 'loaded' and 'skipped' counts.
    """
    # Pass 1: read all taxa into memory, filter, and determine kingdoms
    all_taxa = {}   # inat_id -> {name, rank, rank_level, ancestry_ids, kingdom}
    kept_ids = set()

    with gzip.open(gz_path, 'rt') as f:
        reader = csv.reader(f, delimiter='\t')
        for row in reader:
            if len(row) < 6:
                continue
            try:
                inat_id = int(row[0])
                rank_level = float(row[2])
            except ValueError:
                # The real iNaturalist open-data dump ships with a header
                # row (taxon_id\tancestry\t...) — skip it (and any other
                # malformed line) instead of aborting the whole import.
                continue
            ancestry_str = row[1]
            rank = row[3]
            name = row[4]
            active = row[5].lower() == 'true'

            if not active:
                continue

            ancestry_ids = []
            if ancestry_str:
                ancestry_ids = [int(x) for x in ancestry_str.split('/')]

            all_taxa[inat_id] = {
                'name': name,
                'rank': rank,
                'rank_level': rank_level,
                'ancestry_ids': ancestry_ids,
            }

    # Determine kingdom for each taxon and filter
    filtered = {}
    for inat_id, t in all_taxa.items():
        # Skip non-major ranks
        rl = int(t['rank_level']) if t['rank_level'] == int(t['rank_level']) else None
        if rl not in MAJOR_RANK_LEVELS:
            continue

        # Determine kingdom: check if taxon IS a target kingdom, or has one as ancestor
        kingdom = None
        if inat_id in TARGET_KINGDOM_INAT_IDS:
            kingdom = t['name']
        else:
            for aid in t['ancestry_ids']:
                if aid in TARGET_KINGDOM_INAT_IDS:
                    kingdom = all_taxa[aid]['name'] if aid in all_taxa else None
                    break

        if kingdom is None:
            continue

        filtered[inat_id] = {
            'name': t['name'],
            'rank': RANK_LEVEL_TO_NAME[rl],
            'ancestry_ids': t['ancestry_ids'],
            'kingdom': kingdom,
        }
        kept_ids.add(inat_id)

    # Pass 2: resolve parent_id to nearest kept ancestor
    for _inat_id, t in filtered.items():
        parent_inat_id = None
        for aid in reversed(t['ancestry_ids']):
            if aid in kept_ids:
                parent_inat_id = aid
                break
        t['parent_inat_id'] = parent_inat_id

    # Insert into database
    # First pass: insert all taxa without parent_id (to get local IDs)
    inat_to_local = {}
    for inat_id, t in filtered.items():
        db.conn.execute(
            "INSERT INTO taxa (inat_id, name, rank, kingdom) "
            "VALUES (?, ?, ?, ?) "
            "ON CONFLICT(inat_id) DO UPDATE SET "
            "name = excluded.name, rank = excluded.rank, kingdom = excluded.kingdom",
            (inat_id, t['name'], t['rank'], t['kingdom']),
        )
        row = db.conn.execute(
            "SELECT id FROM taxa WHERE inat_id = ?", (inat_id,)
        ).fetchone()
        if row:
            inat_to_local[inat_id] = row['id']

    # Second pass: set parent_id using local IDs
    for inat_id, t in filtered.items():
        if t['parent_inat_id'] and t['parent_inat_id'] in inat_to_local:
            local_id = inat_to_local[inat_id]
            parent_local_id = inat_to_local[t['parent_inat_id']]
            db.conn.execute(
                "UPDATE taxa SET parent_id = ? WHERE id = ?",
                (parent_local_id, local_id),
            )

    db.relink_source_species_keywords()
    db.conn.commit()

    loaded = len(filtered)
    skipped = len(all_taxa) - loaded
    log.info("Taxonomy loaded: %d taxa imported, %d skipped", loaded, skipped)
    return {"loaded": loaded, "skipped": skipped}


def load_taxonomy(db, data_dir=None):
    """Download and load the full iNat taxonomy.

    Args:
        db: Database instance
        data_dir: directory to store downloaded files (default: ~/.vireo/taxonomy/)
    """
    if data_dir is None:
        data_dir = os.path.expanduser("~/.vireo/taxonomy")
    os.makedirs(data_dir, exist_ok=True)

    gz_path = os.path.join(data_dir, "taxa.csv.gz")
    if not os.path.exists(gz_path):
        download_taxa(gz_path)

    return load_taxa_from_file(db, gz_path)


_DB_MAJOR_RANKS = {"kingdom", "phylum", "class", "order",
                   "family", "genus", "species"}


def populate_taxa_db_from_json(db, taxonomy_json_path, progress_callback=None):
    """Populate the taxa + taxa_common_names tables from a DWCA taxonomy.json.

    Lets a DWCA download (which already has scientific + common names +
    lineage) double as the data source for the local taxa DB that
    add_keyword's auto-detect reads. Avoids the slow iNat API round-trip
    that fetch_common_names would otherwise require.

    Filters to major ranks (kingdom–species); skips subspecies.

    Returns dict with taxa_loaded and common_names_loaded counts.
    """

    def _status(msg):
        log.info(msg)
        if progress_callback:
            progress_callback(msg)

    _status("Reading taxonomy.json...")
    with open(taxonomy_json_path) as f:
        data = json.load(f)

    taxa_by_sci = data.get("taxa_by_scientific", {})
    taxa_by_common = data.get("taxa_by_common", {})

    from species_identity import COMMON_NAME_CORRECTIONS, correct_common_name_index
    ambiguous_common = _common_name_ambiguity(data) - set(COMMON_NAME_CORRECTIONS)
    for name in ambiguous_common:
        taxa_by_common.pop(name, None)
    correct_common_name_index(taxa_by_common, taxa_by_sci)

    # Dedupe by inat_id (same entry appears in both indices and multiple
    # common-name keys can point to the same entry).
    entries_by_inat_id = {}
    for source in (list(_scientific_entries(data)), taxa_by_common.values()):
        for entry in source:
            if entry.get("rank") not in _DB_MAJOR_RANKS:
                continue
            inat_id = entry.get("taxon_id")
            if inat_id is None:
                continue
            entries_by_inat_id.setdefault(int(inat_id), entry)

    # Refuse to proceed on suspicious payloads — the prune step below
    # drops every taxa row whose inat_id isn't in entries_by_inat_id,
    # so an empty or drastically-reduced payload would destroy a good
    # existing DB. Fail loudly before any destructive writes.
    if not entries_by_inat_id:
        raise ValueError(
            "Taxonomy payload has no usable entries — refusing to "
            "populate (would delete the entire local taxa table)"
        )
    existing_count = db.conn.execute(
        "SELECT COUNT(*) FROM taxa"
    ).fetchone()[0]
    if existing_count > 100 and len(entries_by_inat_id) < existing_count * 0.1:
        raise ValueError(
            f"Taxonomy payload has only {len(entries_by_inat_id):,} entries "
            f"but local taxa table already has {existing_count:,}; refusing "
            f"as likely corrupt/partial"
        )

    # Prune stale taxa whose inat_id isn't in the new payload, so taxa
    # that disappeared from iNat (or dropped out of our major-ranks
    # filter) stop being matched by add_keyword's auto-detect. Build a
    # temp table of fresh ids first — the set is too large for a
    # parameterized IN clause. Several FKs point at taxa(id); none have
    # ON DELETE SET NULL, so we preempt them:
    #   - keywords.taxon_id: null out to preserve the keyword row.
    #   - taxa.parent_id (self-ref): null out on children whose parent
    #     is being pruned, so a parent-gone-but-child-kept reshuffle
    #     doesn't trip FK enforcement. The parent-resolve pass below
    #     reinstates parent_id from the new lineage.
    #   - taxa_common_names and informal_group_taxa have ON DELETE
    #     CASCADE and go automatically; seed_informal_groups reseeds
    #     its side from the fresh taxa afterward.
    _status("Pruning stale taxa...")
    db.conn.execute(
        "CREATE TEMP TABLE IF NOT EXISTS fresh_inat_ids "
        "(inat_id INTEGER PRIMARY KEY)"
    )
    db.conn.execute("DELETE FROM fresh_inat_ids")
    db.conn.executemany(
        "INSERT OR IGNORE INTO fresh_inat_ids (inat_id) VALUES (?)",
        [(iid,) for iid in entries_by_inat_id],
    )
    stale_local_ids_sql = (
        "SELECT id FROM taxa "
        "WHERE inat_id IS NOT NULL "
        "  AND inat_id NOT IN (SELECT inat_id FROM fresh_inat_ids)"
    )
    db.conn.execute(
        f"UPDATE keywords SET taxon_id = NULL "
        f"WHERE taxon_id IN ({stale_local_ids_sql})"
    )
    db.conn.execute(
        f"UPDATE taxa SET parent_id = NULL "
        f"WHERE parent_id IN ({stale_local_ids_sql})"
    )
    pruned = db.conn.execute(
        "DELETE FROM taxa WHERE inat_id IS NOT NULL "
        "  AND inat_id NOT IN (SELECT inat_id FROM fresh_inat_ids)"
    ).rowcount
    db.conn.execute("DROP TABLE fresh_inat_ids")
    if pruned:
        _status(f"Pruned {pruned:,} taxa no longer in the taxonomy")

    _status(f"Inserting {len(entries_by_inat_id):,} taxa...")
    for inat_id, entry in entries_by_inat_id.items():
        lineage_names = entry.get("lineage_names") or []
        lineage_ranks = entry.get("lineage_ranks") or []
        kingdom = None
        if lineage_ranks and lineage_ranks[0] == "kingdom":
            kingdom = lineage_names[0] if lineage_names else None
        common_name = entry.get("common_name") or None
        # On conflict, overwrite every column including common_name —
        # don't COALESCE. If upstream removed or emptied a preferred
        # common name, we need to let it drop to NULL here, otherwise
        # add_keyword's auto-detect (which reads taxa.common_name before
        # taxa_common_names) keeps matching the obsolete name.
        db.conn.execute(
            "INSERT INTO taxa (inat_id, name, rank, kingdom, common_name) "
            "VALUES (?, ?, ?, ?, ?) "
            "ON CONFLICT(inat_id) DO UPDATE SET "
            "name=excluded.name, rank=excluded.rank, "
            "kingdom=excluded.kingdom, common_name=excluded.common_name",
            (inat_id, entry["scientific_name"], entry["rank"],
             kingdom, common_name),
        )

    # Resolve parent_id using the full lineage path as the key, not just
    # the parent's scientific name. Scientific names aren't globally unique
    # — homonyms exist at different ranks (e.g. plant/animal genera sharing
    # a name) — so a name-keyed map silently overwrites one inat_id with
    # another and wires parent_id to the wrong node. Indexing by the full
    # tuple of lineage_names disambiguates: two taxa with the same
    # scientific name always have different ancestry.
    _status("Resolving parent relationships...")
    inat_id_by_lineage = {}
    local_id_by_inat_id = {}
    for inat_id, entry in entries_by_inat_id.items():
        lineage = tuple(entry.get("lineage_names") or [])
        if lineage:
            # First winner by iteration order; conflicts would mean two
            # taxa share the exact same lineage path, which shouldn't
            # happen in well-formed data.
            inat_id_by_lineage.setdefault(lineage, inat_id)
        row = db.conn.execute(
            "SELECT id FROM taxa WHERE inat_id = ?", (inat_id,)
        ).fetchone()
        if row:
            local_id_by_inat_id[inat_id] = row["id"]

    for inat_id, entry in entries_by_inat_id.items():
        lineage = tuple(entry.get("lineage_names") or [])
        if len(lineage) < 2:
            continue
        parent_inat_id = inat_id_by_lineage.get(lineage[:-1])
        if parent_inat_id is None:
            continue
        parent_local = local_id_by_inat_id.get(parent_inat_id)
        own_local = local_id_by_inat_id.get(inat_id)
        if parent_local is None or own_local is None or own_local == parent_local:
            continue
        db.conn.execute(
            "UPDATE taxa SET parent_id = ? WHERE id = ?",
            (parent_local, own_local),
        )

    # Populate taxa_common_names — index every English common name (including
    # alternates) under its taxon so add_keyword's auto-detect can match
    # regional/alt names like "Green heron" or "Common gallinule".
    #
    # Clear the English index first so names that disappeared or were
    # reassigned in the new taxonomy drop out. Without this the INSERT
    # OR IGNORE below would leave stale rows behind and add_keyword would
    # keep matching obsolete common names across re-downloads. Still
    # inside the populate transaction, so a failure rolls it back.
    _status(f"Indexing {len(taxa_by_common):,} common names...")
    db.conn.execute("DELETE FROM taxa_common_names WHERE locale = 'en'")
    cn_loaded = 0
    for name_lower, entry in taxa_by_common.items():
        inat_id = entry.get("taxon_id")
        if inat_id is None:
            continue
        row = db.conn.execute(
            "SELECT id FROM taxa WHERE inat_id = ?", (int(inat_id),)
        ).fetchone()
        if not row:
            continue
        db.conn.execute(
            "INSERT OR IGNORE INTO taxa_common_names "
            "(taxon_id, name, locale) VALUES (?, ?, 'en')",
            (row["id"], name_lower),
        )
        cn_loaded += 1

    db.relink_source_species_keywords()
    db.set_meta("common_name_identity_version", str(COMMON_NAME_IDENTITY_VERSION), _commit=False)
    db.set_meta("ambiguous_common_names", json.dumps(sorted(ambiguous_common)), _commit=False)
    db.conn.commit()
    result = {
        "taxa_loaded": len(entries_by_inat_id),
        "common_names_loaded": cn_loaded,
    }
    _status(
        f"Loaded {result['taxa_loaded']:,} taxa and "
        f"{result['common_names_loaded']:,} common names into DB"
    )
    return result


def fetch_common_names(db, locale='en'):
    """Fetch common names from the iNat API for all taxa in the database.

    Batches requests to the iNat API, updates taxa.common_name with the
    preferred common name, and inserts all English names into taxa_common_names.

    Returns dict with 'updated' count.
    """
    rows = db.conn.execute(
        "SELECT id, inat_id FROM taxa WHERE inat_id IS NOT NULL"
    ).fetchall()

    inat_ids = [(r['id'], r['inat_id']) for r in rows]
    updated = 0
    remaining = {iid for _, iid in inat_ids}
    # A partial refresh cannot certify an old, lossy name index.
    if locale == 'en':
        db.set_meta("common_name_identity_version", "", _commit=False)

    for i in range(0, len(inat_ids), INAT_BATCH_SIZE):
        batch = inat_ids[i:i + INAT_BATCH_SIZE]
        id_str = ','.join(str(iid) for _, iid in batch)
        local_by_inat = {iid: lid for lid, iid in batch}

        try:
            resp = requests.get(
                f"{INAT_API_BASE}/taxa",
                params={'id': id_str, 'per_page': INAT_BATCH_SIZE},
                timeout=30,
            )
            if resp.status_code != 200:
                log.warning("iNat API returned %d for batch %d", resp.status_code, i)
                continue

            for taxon in resp.json().get('results', []):
                inat_id = taxon['id']
                local_id = local_by_inat.get(inat_id)
                if not local_id:
                    continue

                remaining.discard(inat_id)
                # Replace each returned taxon's names so a removed alias does
                # not survive a complete refresh as false identity evidence.
                db.conn.execute("DELETE FROM taxa_common_names WHERE taxon_id = ? AND locale = ?",
                                (local_id, locale))
                preferred = taxon.get('preferred_common_name')
                db.conn.execute("UPDATE taxa SET common_name = ? WHERE id = ?", (preferred, local_id))
                if preferred:
                    updated += 1

                for name_entry in taxon.get('names', []):
                    if name_entry.get('locale') == locale:
                        db.conn.execute(
                            "INSERT OR IGNORE INTO taxa_common_names "
                            "(taxon_id, name, locale) VALUES (?, ?, ?)",
                            (local_id, name_entry['name'], locale),
                        )
        except requests.RequestException as e:
            log.warning("iNat API request failed: %s", e)
            continue

    if locale == 'en' and inat_ids and not remaining:
        db.set_meta("common_name_identity_version", str(COMMON_NAME_IDENTITY_VERSION), _commit=False)
        db.set_meta("ambiguous_common_names", "[]", _commit=False)
    db.conn.commit()
    log.info("Common names: %d taxa updated", updated)
    return {"updated": updated}


# Default informal groups for wildlife photography.
# Each maps a common name to a list of scientific names (order or family level).
DEFAULT_INFORMAL_GROUPS = {
    "Raptors": ["Accipitriformes", "Falconiformes", "Strigiformes"],
    "Shorebirds": ["Charadriiformes"],
    "Waterfowl": ["Anseriformes"],
    "Songbirds": ["Passeriformes"],
    "Hummingbirds": ["Trochilidae"],
    "Wading birds": ["Ardeidae", "Ciconiidae", "Threskiornithidae"],
    "Woodpeckers": ["Picidae"],
    "Gamebirds": ["Galliformes"],
}


def seed_informal_groups(db):
    """Create default informal groups and link them to taxa nodes.

    Only links groups to taxa that exist in the database. Skips groups
    that already exist (idempotent).

    Returns dict with 'groups_created' count.
    """
    created = 0
    for group_name, taxon_names in DEFAULT_INFORMAL_GROUPS.items():
        # Insert group (ignore if exists)
        db.conn.execute(
            "INSERT OR IGNORE INTO informal_groups (name) VALUES (?)",
            (group_name,),
        )
        group_row = db.conn.execute(
            "SELECT id FROM informal_groups WHERE name = ?", (group_name,)
        ).fetchone()
        group_id = group_row["id"]

        linked_any = False
        for taxon_name in taxon_names:
            taxon_row = db.conn.execute(
                "SELECT id FROM taxa WHERE name = ?", (taxon_name,)
            ).fetchone()
            if taxon_row:
                db.conn.execute(
                    "INSERT OR IGNORE INTO informal_group_taxa "
                    "(group_id, taxon_id) VALUES (?, ?)",
                    (group_id, taxon_row["id"]),
                )
                linked_any = True

        if linked_any:
            created += 1

    db.conn.commit()
    log.info("Informal groups: %d created/verified", created)
    return {"groups_created": created}


# --- DWCA-based taxonomy loader (legacy) ---


def download_taxonomy(output_path, progress_callback=None):
    """Download iNaturalist DWCA taxonomy and build taxonomy.json.

    Downloads the zip, parses taxa.csv and VernacularNames.csv,
    and writes a JSON file keyed by common name and scientific name.

    Uses resumable download — safe on flaky connections.

    Args:
        progress_callback: optional callable(message) for status updates
    """

    def _status(msg):
        log.info(msg)
        if progress_callback:
            progress_callback(msg)

    # Drop the cache's own reference to the old parse before we start
    # building the replacement. A cached iNat dump is ~2.8GB, so letting
    # it stay strongly reachable through _taxonomy_cache while this
    # function also holds the ~2.8GB dict it is about to write can double
    # peak RSS. The post-download retype already routes through
    # load_local_taxonomy() (which re-evicts on parse), but that runs
    # *after* the build. Evicting here covers the overlap. Concurrent
    # requests that borrowed the taxonomy still keep it alive until they
    # return; this only releases the cache's own reference.
    clear_taxonomy_cache()

    # Download zip to a file (resumable) instead of holding in memory
    zip_dir = os.path.dirname(output_path) or "."
    os.makedirs(zip_dir, exist_ok=True)
    zip_path = os.path.join(zip_dir, "taxonomy-dwca.zip")
    try:
        _download_with_resume(DWCA_URL, zip_path, progress_callback=_status)
        _status("Download complete — parsing...")

        # Parse the DWCA zip
        taxa_by_id = {}
        common_names = {}  # taxon_id -> preferred common_name
        alt_names = {}  # taxon_id -> [all English vernacular names]

        with zipfile.ZipFile(zip_path) as zf:
            file_list = zf.namelist()
            log.info("Archive contents: %s", file_list)

            # Parse taxa.csv — columns: id, parentNameUsageID, scientificName, taxonRank
            taxa_file = None
            for name in file_list:
                if name.lower().endswith("taxa.csv") or name.lower() == "taxa.csv":
                    taxa_file = name
                    break
            if not taxa_file:
                raise FileNotFoundError("taxa.csv not found in DWCA archive")

            log.info("Parsing %s ...", taxa_file)
            with zf.open(taxa_file) as f:
                reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8"))
                for row in reader:
                    taxon_id = row.get("id") or row.get("taxonID")
                    if not taxon_id:
                        continue
                    # parentNameUsageID may be a URL like https://www.inaturalist.org/taxa/48460
                    parent_raw = row.get("parentNameUsageID", "")
                    if parent_raw and "/" in parent_raw:
                        parent_id = parent_raw.rsplit("/", 1)[-1]
                    else:
                        parent_id = parent_raw

                    taxa_by_id[taxon_id] = {
                        "taxon_id": int(taxon_id),
                        "scientific_name": row.get("scientificName", ""),
                        "rank": (row.get("taxonRank") or "").lower(),
                        "parent_id": parent_id,
                    }
            _status(f"Parsed {len(taxa_by_id):,} taxa")

            # Parse VernacularNames (English common names)
            # Prefer VernacularNames-english.csv, fall back to VernacularNames.csv
            vn_file = None
            for name in file_list:
                if name.lower() == "vernacularnames-english.csv":
                    vn_file = name
                    break
            if not vn_file:
                for name in file_list:
                    if name.lower() == "vernacularnames.csv":
                        vn_file = name
                        break

            if vn_file:
                log.info("Parsing %s ...", vn_file)
                with zf.open(vn_file) as f:
                    reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8"))
                    for row in reader:
                        # Language-specific files may not have a language column
                        lang = row.get("language", "en")
                        if lang and lang.lower() != "en":
                            continue
                        taxon_id = row.get("id") or row.get("taxonID")
                        vn = row.get("vernacularName", "")
                        if taxon_id and taxon_id in taxa_by_id and vn:
                            if taxon_id not in common_names:
                                common_names[taxon_id] = vn
                            # Collect all English names per taxon for alternate-name indexing
                            if taxon_id not in alt_names:
                                alt_names[taxon_id] = []
                            alt_names[taxon_id].append(vn)
                _status(f"Found {len(common_names):,} English common names ({sum(len(v) for v in alt_names.values()):,} total including alternates)")
            else:
                log.warning("No VernacularNames file found in archive")

        # Build lineages by walking parent chains
        def _build_lineage(taxon_id):
            lineage_names = []
            lineage_ranks = []
            current = taxon_id
            seen = set()
            while current and current in taxa_by_id and current not in seen:
                seen.add(current)
                t = taxa_by_id[current]
                if t["rank"] in RANK_ORDER:
                    lineage_names.append(t["scientific_name"])
                    lineage_ranks.append(t["rank"])
                current = t["parent_id"]
            lineage_names.reverse()
            lineage_ranks.reverse()
            return lineage_names, lineage_ranks

        # Build the lookup dictionaries
        _status(f"Building lineages for {len(taxa_by_id):,} taxa...")
        taxa_by_common = {}
        entries_by_scientific = {}
        entries_by_taxon = {}

        for taxon_id, taxon in taxa_by_id.items():
            rank = taxon["rank"]
            if rank not in RANK_ORDER:
                continue

            lineage_names, lineage_ranks = _build_lineage(taxon_id)

            entry = {
                "taxon_id": taxon["taxon_id"],
                "scientific_name": taxon["scientific_name"],
                "common_name": common_names.get(taxon_id, ""),
                "rank": rank,
                "lineage_names": lineage_names,
                "lineage_ranks": lineage_ranks,
            }
            entries_by_taxon[taxon_id] = entry

            # Index by scientific name
            sci_key = taxon["scientific_name"].lower()
            entries_by_scientific.setdefault(sci_key, []).append(entry)

        # Scientific names are unique only within a kingdom: keep every
        # homonym, and index the lowest taxon id so the choice is stable.
        taxa_by_scientific = {}
        scientific_homonyms = {}
        for sci_key, entries in entries_by_scientific.items():
            entries.sort(key=lambda e: e["taxon_id"])
            taxa_by_scientific[sci_key] = entries[0]
            if len(entries) > 1:
                scientific_homonyms[sci_key] = entries

        # Index preferred common names first so they always win.
        # Keep the first mapping when two taxa share a preferred name.
        for taxon_id, cn in common_names.items():
            entry = entries_by_taxon.get(taxon_id)
            cn_key = cn.lower()
            if entry and cn_key not in taxa_by_common:
                taxa_by_common[cn_key] = entry

        # Then index alternate names only for still-unmapped keys
        common_candidates = {}
        for taxon_id, names in alt_names.items():
            entry = entries_by_taxon.get(taxon_id)
            if not entry:
                continue
            for cn in names:
                cn_key = cn.lower()
                common_candidates.setdefault(cn_key, set()).add(taxon_id)
                if cn_key not in taxa_by_common:
                    taxa_by_common[cn_key] = entry

        ambiguous_common_names = sorted(
            name for name, ids in common_candidates.items() if len(ids) > 1
        )
        for name in ambiguous_common_names:
            taxa_by_common.pop(name, None)

        result = {
            "common_name_identity_version": COMMON_NAME_IDENTITY_VERSION,
            "ambiguous_common_names": ambiguous_common_names,
            "last_updated": str(date.today()),
            "source": "iNaturalist DWCA",
            "taxa_by_common": taxa_by_common,
            "taxa_by_scientific": taxa_by_scientific,
            "scientific_homonyms": scientific_homonyms,
        }

        _status(
            f"Writing taxonomy ({len(taxa_by_common):,} common + {len(taxa_by_scientific):,} scientific names)..."
        )
        _write_taxonomy_json_atomically(output_path, result)
        _status(
            f"Taxonomy complete: {len(taxa_by_common):,} common names, {len(taxa_by_scientific):,} scientific names"
        )
        return result
    finally:
        # Clean up the downloaded zip — the JSON is all we need
        if os.path.exists(zip_path):
            os.remove(zip_path)


def classify_to_keypoint_group(db, inat_id):
    """Walk a taxon's lineage; return 'Aves' or 'Mammalia' if in ancestry, else None.

    Used to route keypoint-model selection for eye-focus detection. Returns
    None for fish, reptiles, insects, invertebrates, or any taxon absent
    from the local taxa table.

    Note: ``taxa.parent_id`` references ``taxa.id`` (local PK), not
    ``taxa.inat_id``. The walk chains local IDs after the initial inat_id
    lookup.
    """
    if inat_id is None:
        return None
    row = db.conn.execute(
        "SELECT id, name, rank, parent_id FROM taxa WHERE inat_id=?",
        (inat_id,),
    ).fetchone()
    if row is None:
        return None
    seen = set()
    while row is not None:
        local_id = row["id"] if hasattr(row, "keys") else row[0]
        name = row["name"] if hasattr(row, "keys") else row[1]
        rank = row["rank"] if hasattr(row, "keys") else row[2]
        parent_id = row["parent_id"] if hasattr(row, "keys") else row[3]
        if local_id in seen:
            return None
        seen.add(local_id)
        if rank == "class" and name in ("Aves", "Mammalia"):
            return name
        if parent_id is None:
            return None
        row = db.conn.execute(
            "SELECT id, name, rank, parent_id FROM taxa WHERE id=?",
            (parent_id,),
        ).fetchone()
    return None


def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    parser = argparse.ArgumentParser(
        description="Download and manage iNaturalist taxonomy."
    )
    parser.add_argument(
        "--download", action="store_true", help="Download taxonomy from iNaturalist"
    )
    parser.add_argument(
        "--output",
        default=TAXONOMY_JSON_PATH,
        help="Output path for taxonomy.json",
    )
    args = parser.parse_args()

    if args.download:
        download_taxonomy(args.output)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
