"""Scan folders, discover photos, read metadata, populate database.

The package's modules, by responsibility:

- ``options``: ``ScanOptions``, ``ScanHooks`` and ``ScanControl``, the
  arguments ``scan()`` takes besides the root and the database.
- ``scan_run``: ``scan()`` and ``_ScanRun``, the orchestrator that runs one
  scan's phases in order, plus metadata extraction and the end-of-scan
  pairing and working-copy pass.
- ``run_state``: the cancel/status signals and the progress/counts tally
  every phase shares.
- ``discovery``: the files a scan considers.
- ``folders``: the folder rows a scan creates, links and stamps.
- ``triage``: the incremental pre-pass.
- ``features``: hashing the work queue under a CPU lease.
- ``indexing``: the per-file catalog writes.
- ``companion_keep`` and ``companion_attach``: companion JPEGs already on
  their RAW's row, and pairing a JPEG before it gets a row.
- ``hashing``: content hashes and the CPU lease the hashing pool runs under.
- ``status``: calling a status callback with or without phase keywords.
- ``exif_fields``: dimensions and capture time from ExifTool's groups.
- ``keywords``: sidecar and embedded keyword import, and the embedded
  backfill.
- ``companions``: RAW+JPEG companion pairing.
- ``cache_invalidation``: dropping derived files when source bytes change.
- ``working_copies`` and ``working_copy_batch``: choosing which photos get
  a JPEG working copy, and the extraction pass that writes them.

The SQL lives in ``vireo/repositories/`` (``db.scanning``,
``db.companion_pairing``, ``db.working_copies``).

This module re-exports what code outside the package imports. A module
inside the package looks a name up in its own globals, so a test that
patches one of those names patches the submodule that calls it (for
example ``scanner.scan_run.extract_metadata``), not this package.
"""

# ``ScanCancelled`` is defined in ``image_loader`` (where the low-level
# walkers raise it) and re-exported here for callers that use
# ``from scanner import ScanCancelled``.
from image_loader import ScanCancelled, is_excluded_scan_path
from scanner.cache_invalidation import (
    _invalidate_derived_caches,
    _sweep_untracked_previews_for_photos,
)
from scanner.companions import COMPANION_TIMESTAMP_TOLERANCE_SECS
from scanner.hashing import EMPTY_FILE_SHA256, compute_fd_hash, compute_file_hash
from scanner.keywords import EMBEDDED_KEYWORDS_BACKFILL_KEY, backfill_embedded_keywords
from scanner.options import ScanControl, ScanHooks, ScanOptions
from scanner.scan_run import scan
from scanner.working_copies import (
    _extract_working_copies,
    backfill_working_copies,
    import_working_copy_notice,
    working_copy_backfill_candidate_count,
)

__all__ = [
    "COMPANION_TIMESTAMP_TOLERANCE_SECS",
    "EMBEDDED_KEYWORDS_BACKFILL_KEY",
    "EMPTY_FILE_SHA256",
    "ScanCancelled",
    "ScanControl",
    "ScanHooks",
    "ScanOptions",
    "_extract_working_copies",
    "_invalidate_derived_caches",
    "_sweep_untracked_previews_for_photos",
    "backfill_embedded_keywords",
    "backfill_working_copies",
    "compute_fd_hash",
    "compute_file_hash",
    "import_working_copy_notice",
    "is_excluded_scan_path",
    "scan",
    "working_copy_backfill_candidate_count",
]
