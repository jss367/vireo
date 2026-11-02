"""Import job: copy card -> archive directly, hash-verify, catalog incrementally.

The import half of the import/process split (design doc
2026-07-04-import-process-split-design.md). The core invariant:

    A photo row is created only when its file verifiably exists at its
    final archive path.

Files are copied per destination-folder batch, each copy is verified by
content hash before promotion, and each batch is cataloged via the
scanner's restricted-scan path immediately after it lands. A run that
dies at any point leaves a valid partial catalog; a retry's duplicate
gate skips exactly the files that landed and copies the rest. The caller
may supply managed temporary storage for a process-before-archive run,
then chain a verified move to the final destination. This job always
catalogs the supplied destination; failed runs keep those local originals.

How the job relies on the rest of Vireo:

1. ``scanner.scan()`` computes and writes ``photos.file_hash`` itself
   (``_compute_file_features`` hashes every new/changed file). It does
   NOT write ``hash_status``/``hash_checked_at`` — those belong to the
   integrity-audit vocabulary, so the import job stamps them after each
   batch's scan, gated on the scan-computed hash matching the hash this
   job verified at copy time (a free cross-check: a mismatch means the
   destination changed between copy and scan and the file is bucketed
   as failed instead of silently trusted).
2. ``CatalogIndex`` retains only identity sets (hashes/keys/sizes), not
   paths. Key-match twin resolution for the safe-to-format ledger uses
   a direct DB query on (filename, file_size) + stored_metadata_key
   equality, joining folders for the archive path.
3. Cancellation mirrors the scan job: the work function polls
   ``runner.is_cancelled(job_id)`` at batch boundaries and passes
   ``cancel_check`` into ``scan()``; the runner flips the job status to
   "cancelled" when the work function returns after a Stop. The remote
   path additionally passes ``cancel_check`` into the per-batch rsync so
   Stop kills the subprocess mid-transfer instead of waiting out the
   batch — the interrupted batch is cancelled work (nothing failed,
   nothing cataloged), recovered like a mid-batch crash. Destination-side
   hash reads (duplicate gate, crash-recovery adopt, post-scan re-checks)
   go through ``_hash_dest_file``, which watches the same Stop signal and
   a stall watchdog — a read blocked on a dead SMB mount can otherwise
   pin the worker for the mount's own multi-minute timeout per file while
   cancellation goes unobserved.
4. Batch unit: files grouped by destination (template) folder,
   processed in template order, chunked to at most
   ``IMPORT_BATCH_SIZE`` files per scan call. Restricted scans only
   enumerate the files the batch actually landed, so per-batch scan cost
   tracks the batch, not the archive tree. Duplicate-matched folders are
   linked to the active workspace directly from their existing catalog
   rows. They are deliberately NOT scanned as part of the import: even an
   incremental scan must enumerate/stat every entry, which can block a
   zero-copy import for hours on SMB. Uncataloged strays and ``partial``
   folder health remain visible for the explicit folder-rescan workflow;
   archive repair is not part of the import's critical path.

Package layout (one module per responsibility; this ``__init__`` only
re-exports the names callers outside the package import):

- ``run`` — ``run_import_job``, the entry point.
- ``planning`` — discovery, selection and batching (``IMPORT_BATCH_SIZE``).
- ``batch_loop`` — ``_ImportBatchLoop``, the per-batch loop.
- ``preflight`` — the per-batch guard chain and the mount-loss rollback.
- ``duplicate_gate`` / ``twins`` — the duplicate gate, catalog twins, and
  linking duplicate-matched folders.
- ``transports`` / ``adoption`` / ``siblings`` — the local and rsync
  transports, destination collision resolution and crash-recovery
  adoption, and RAW+JPEG companion slots.
- ``copy_verify`` — ``copy_and_hash_verify`` and destination hashing.
- ``cataloging`` — the per-batch scan, hash stamping, cache invalidation
  and deferred working copies.
- ``resume`` / ``snapshots`` — resume bookkeeping, source snapshots and
  landed-photo fingerprints.
- ``destination`` / ``state`` / ``progress`` / ``file_selection`` /
  ``result`` — destination context, run state and bookkeeping, ETA and
  progress, file selection, and the final result.

A test that patches a helper must patch it in the module that looks it up
(``import_job.planning.IMPORT_BATCH_SIZE``, not ``import_job.IMPORT_BATCH_SIZE``).
``run_import_job`` itself is looked up here: the import services run
``from import_job import run_import_job`` when the job starts, so patching
``import_job.run_import_job`` still takes effect.
"""

from import_job.copy_verify import DestReadCancelled, copy_and_hash_verify
from import_job.run import run_import_job
from import_job.snapshots import _capture_source_snapshots, _fingerprint_for_row
from import_job.state import ImportParams

__all__ = [
    "DestReadCancelled",
    "ImportParams",
    "_capture_source_snapshots",
    "_fingerprint_for_row",
    "copy_and_hash_verify",
    "run_import_job",
]
