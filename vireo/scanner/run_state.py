"""State every phase of one ``scan()`` shares: its signals and its tally.

``_ScanSignals`` is the cancel probe and the status callback every phase
reports through. ``_ScanTally`` owns the progress counter, the per-bucket
``counts`` and what the scan has reported to its receiver (``photo`` and
``photo_merged`` callbacks), so each disposition of a file is credited in
one place.
"""

from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING, cast

from image_loader import ScanCancelled
from resource_ledger import suspend_resource_wait_timing
from scanner.status import _call_status_callback

if TYPE_CHECKING:
    from db import Database
    from scanner.scan_types import (
        CheckCallback,
        PhotoCallback,
        PhotoIdentity,
        PhotoMergedCallback,
        ProgressCallback,
        StatusCallback,
    )

log = logging.getLogger(__name__)


class _ScanSignals:
    """The caller's cancel probe and status callback."""

    def __init__(
        self, cancel_check: CheckCallback | None, status_callback: StatusCallback | None,
        status_supports_phase: bool,
    ) -> None:
        self.cancel_check = cancel_check
        self.status_callback = status_callback
        self.status_supports_phase = status_supports_phase

    def check_cancelled(self) -> None:
        if self.cancel_check is None:
            return
        # ``check_cancelled`` is threaded into ``ResourceLedger.acquire``
        # through ``_claim_worker_count`` as its cancellation probe. On a
        # standalone ``/api/jobs/scan`` or ``/api/jobs/import-photos`` job
        # the caller-supplied ``cancel_check`` is the runner's pause-aware
        # probe, so it parks for the whole pause; without the suspend
        # bracket, the ledger's active wait timer accumulates that idle
        # time as resource contention and inflates the persisted
        # ``resource_wait_seconds`` diagnostic by the full pause length.
        # The pipeline path already brackets its checkpoints with
        # ``suspend_resource_wait_timing()`` inside ``_pause_checkpoint``;
        # mirror that here so both call sites report contention honestly.
        # When there is no active wait (every non-``ledger.acquire`` call
        # site), the suspend context is a cheap no-op.
        with suspend_resource_wait_timing():
            cancelled = self.cancel_check()
        if cancelled:
            raise ScanCancelled("scan cancelled")

    def emit_status(
        self, message: str, phase_current: int | None = None, phase_total: int | None = None,
        phase_label: str | None = None,
    ) -> None:
        if not self.status_callback:
            return
        _call_status_callback(
            self.status_callback,
            message,
            phase_current=phase_current,
            phase_total=phase_total,
            phase_label=phase_label,
            supports_phase=self.status_supports_phase,
        )


class _ScanTally:
    """Progress, per-bucket counts, and what the receiver has been told.

    ``processed_count`` advances for every file the scan *disposes of*,
    including ones it deliberately skips — that is what a progress bar
    needs to reach 100%. It is NOT the number of photos indexed, and the
    two diverge exactly when something is wrong (a share that unmounts
    mid-scan makes every remaining file vanish). Every ``processed_count``
    bump is therefore paired with a bump of exactly one bucket in
    ``counts``, so the summary reports what actually landed in the
    catalog. Classifying at each site (rather than deriving one bucket by
    subtraction) means a disposition added later has to state which
    bucket it belongs to instead of silently defaulting to "indexed" —
    the invariant check in ``log_summary`` enforces it.
    """

    def __init__(
        self, counts: dict[str, int], *, progress_callback: ProgressCallback | None,
        photo_callback: PhotoCallback | None, photo_merged_callback: PhotoMergedCallback | None,
        reported_identities: dict[int, PhotoIdentity] | None = None,
    ) -> None:
        self.counts = counts
        self.progress_callback = progress_callback
        self.photo_callback = photo_callback
        self.photo_merged_callback = photo_merged_callback
        # The identity history spans the receiver's scanner calls. Do not
        # reset it at a per-root phase boundary: end-of-scan pairing may
        # merge ids reported by an earlier root.
        self.reported_identities = (
            reported_identities if reported_identities is not None else {}
        )
        self.total = 0
        self.processed_count = 0
        # The ids behind ``counts["indexed"]``. Needed because the companion
        # pairing pass at the end of the scan runs against the entire photos
        # table, so its merges must be intersected with what *this*
        # invocation counted (see ``_pair_raw_jpeg_companions``).
        self.indexed_photo_ids = set()

    def start(self, total: int, excluded_frozen: int) -> None:
        """Open the tally on ``total`` files, ``excluded_frozen`` already vanished."""
        self.total = total
        self.processed_count = excluded_frozen
        self.counts["discovered"] = total
        self.counts["vanished"] = excluded_frozen

    def discount_merged_companions(self, merged_ids: dict[int, int]) -> None:
        """Move JPEGs the end-of-scan pairing merged from ``indexed`` to merged."""
        # Discount only companions THIS scan counted. The pairing pass
        # queries the whole photos table, so it also cleans up pairs left
        # pending elsewhere in the catalog (an interrupted earlier scan,
        # an older build). Debiting a scoped scan for those would make it
        # undercount, and scanning a root with no new files while one
        # stale pair was pending would report -1 photos indexed.
        _mine = merged_ids.keys() & self.indexed_photo_ids
        self.counts["merged_companions"] += len(_mine)
        self.counts["indexed"] -= len(_mine)

    def report_progress(self) -> None:
        if self.progress_callback:
            self.progress_callback(self.processed_count, self.total)

    def skip_file(self, bucket: str) -> None:
        """Dispose of a file that will not be indexed, under ``bucket``."""
        self.processed_count += 1
        self.counts[bucket] += 1
        self.report_progress()

    def credit_reused_row(self, photo_id: int, full_path_str: str, folder_id: int, filename: str) -> None:
        self.processed_count += 1
        self.counts["indexed"] += 1
        self.indexed_photo_ids.add(photo_id)
        self.reported_identities[photo_id] = (folder_id, filename)
        if self.photo_callback:
            self.photo_callback(photo_id, full_path_str)
        self.report_progress()

    def credit_known_companion(
        self, owner_id: int, full_path_str: str, owner_folder_id: int, owner_filename: str,
    ) -> None:
        """Dispose of a companion JPEG its RAW's row already holds.

        ``photo_callback`` gets the RAW's id: that row is where the file is
        cataloged (callers such as import-in-place count the path as
        indexed). It is not ``indexed`` again, the RAW was.
        """
        self.processed_count += 1
        self.counts["known_companions"] += 1
        self.reported_identities[owner_id] = (owner_folder_id, owner_filename)
        if self.photo_callback:
            self.photo_callback(owner_id, full_path_str)
        self.report_progress()

    def credit_indexed(self, photo_id: int) -> None:
        """Count a row this scan inserted or refreshed."""
        self.counts["indexed"] += 1
        self.indexed_photo_ids.add(photo_id)

    def credit_merged_companion(self) -> None:
        """Dispose of a JPEG recorded onto its RAW's row without a row of its own."""
        self.processed_count += 1
        self.counts["merged_companions"] += 1

    def report_cataloged(self, photo_id: int, full_path_str: str, folder_id: int, filename: str) -> None:
        """Report a row ``credit_indexed`` counted, then advance progress."""
        self.reported_identities[photo_id] = (folder_id, filename)
        if self.photo_callback:
            self.photo_callback(photo_id, full_path_str)
        self.processed_count += 1
        self.report_progress()

    def report_merged_photos(self, db: Database, merged: dict[int, int]) -> None:
        """Tell ``photo_merged_callback`` which RAW absorbed each merged id."""
        if not merged or self.photo_merged_callback is None:
            return
        # Pairing has committed, so readers on other connections can see
        # the merge. Reacquire the writer lock before validating the RAW
        # and publishing its id; deletion and rowid reuse must not interleave
        # with a callback that updates an import's in-memory membership.
        companion_identities = getattr(merged, "companions", {})
        reported_identities = self.reported_identities
        with db._commits_held():
            db.begin_immediate()
            for old_id, new_id in merged.items():
                row = db.companion_pairing.owner_row(new_id)
                expected = getattr(merged, "owners", {}).get(new_id)
                # The merged-away JPEG must still be the same file the
                # scanner reported under ``old_id``. If a concurrent delete
                # freed the id and SQLite reused it for an unrelated row
                # that pairing then merged, substituting ``new_id`` for
                # ``old_id`` in the receiver's collection would adopt an
                # unrelated RAW. A missing reported identity means none of
                # this receiver's scanner calls reported the id; pairing
                # also covers catalog rows outside those calls.
                reported_identity = reported_identities.get(old_id)
                pairing_identity = companion_identities.get(old_id)
                identity_matches = (
                    reported_identity is None
                    or pairing_identity is None
                    or reported_identity == pairing_identity
                )
                if (
                    row is None
                    or (row[0], row[1]) != expected
                    or not identity_matches
                ):
                    self.photo_merged_callback(old_id, None, None)
                else:
                    self.photo_merged_callback(
                        old_id, new_id, os.path.join(row[2], row[1]),
                    )
                    if reported_identity is not None:
                        # This branch matched the RAW row against ``expected``.
                        reported_identities[new_id] = cast("PhotoIdentity", expected)
                reported_identities.pop(old_id, None)

    def log_summary(self) -> None:
        counts = self.counts
        processed_count = self.processed_count
        # Every file the loop disposed of landed in exactly one bucket. If this
        # ever trips, a new disposition was added without classifying it, and
        # the summary below would silently over-claim by that many photos —
        # log it rather than raising, since the scan's real work is committed
        # and a miscounted summary is no reason to fail the run.
        vanished_count = counts["vanished"]
        skipped_uncataloged_count = counts["skipped_uncataloged"]
        merged_count = counts["merged_companions"]
        known_companion_count = counts["known_companions"]
        indexed_count = counts["indexed"]
        accounted = (
            indexed_count + vanished_count + skipped_uncataloged_count
            + merged_count + known_companion_count
        )
        if accounted != processed_count:
            log.error(
                "Scan count invariant broken: indexed=%d + vanished=%d + "
                "skipped=%d + merged=%d + known companions=%d != processed=%d "
                "(a file disposition is unclassified)",
                indexed_count, vanished_count, skipped_uncataloged_count,
                merged_count, known_companion_count, processed_count,
            )

        # Report what reached the catalog, not what the walk turned up. These
        # used to be the same number ("Scan complete: %d photos indexed" logged
        # ``total``), which reads as a success line and stays reassuring
        # precisely when the scan achieved nothing — an archive share that
        # dropped mid-scan logged "984 photos indexed" having indexed zero.
        summary = f"Scan complete: {indexed_count} photos indexed"
        if merged_count:
            summary += f", {merged_count} JPEG(s) merged into their RAW"
        if known_companion_count:
            summary += (
                f", {known_companion_count} JPEG(s) already paired with "
                "their RAW"
            )
        if vanished_count:
            summary += f", {vanished_count} vanished"
        if skipped_uncataloged_count:
            summary += f", {skipped_uncataloged_count} uncataloged (skipped)"
        if (merged_count or known_companion_count or vanished_count
                or skipped_uncataloged_count):
            summary += f" of {self.total} discovered"
        log.info(summary)
