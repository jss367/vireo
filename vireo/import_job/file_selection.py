"""The user's file selection and its effect on safe-to-format."""

from __future__ import annotations

from typing import TYPE_CHECKING, NamedTuple

# Shared with the pipeline readiness copy so "1 file" / "2 files" reads the
# same way everywhere the UI states a count. pipeline_plan imports only
# stdlib at module level, so this does not cycle back through import_job.
from pipeline_plan import _plural

if TYPE_CHECKING:
    from pathlib import Path

    from import_job.import_types import UnsafeFile
    from import_job.state import ImportParams


def _selection_blocks_format(*, deselected: int, vanished_paths: set[str]) -> bool:
    """True when the user's selection means the card is NOT fully archived.

    Deliberately separate from the ``(copied + skipped_duplicate) ==
    discovered`` ledger check. That equality catches these cases *usually*,
    and three data-loss bugs in review came from trusting "usually":
      - the selection filter applied above ``discovered`` instead of below
        it, so a partial import still balanced the equality;
      - a vanished selected file blocked ``safe_to_format`` but not
        ``unverified_duplicates_only``, whose amber pill's stated remedy
        (re-run with ``verify_by_hash``) then turns it green;
      - deselect X, then X also vanishes -> discovered shrinks too, equality
        balances, nothing is wrong arithmetically, and a file the user
        excluded is reported as archived.
    Do not delete either condition because the other "already covers it".

    ``deselected != 0`` rather than ``> 0``: a negative count means the
    caller previewed fewer files than it selected, which is self-inconsistent
    input, and this module fails closed on inconsistency.

    Intended as the single home for selection-based card-safety conditions:
    every card-safety verdict on every copy path should call this so a new
    condition added here reaches all of them. All four verdicts do —
    ``safe_to_format`` and ``unverified_duplicates_only``, for both
    transports behind ``run_import_job``.
    Do not inline the condition at a call site.
    """
    return deselected != 0 or bool(vanished_paths)


class _Selection(NamedTuple):
    """What the per-file selection did to one import run.

    ``files`` is the copy set after filtering; ``queued`` is its length as
    the progress denominator; the rest is drift, feeding the card-safety
    verdict, the ``unsafe_files`` lines, and the caller's readout.

    Both copy paths destructure this BY NAME (``sel.queued``), never by
    position. Four of the six fields are plain ints and two are sets, so a
    positional unpack that transposed a same-typed pair would still run and
    still type-check, and the local/remote parity test would not see it: that
    test compares the two paths to *each other*, so a transposition applied
    to both is invisible to it. (Measured, not assumed — swapping
    ``queued``/``deselected`` or ``queued``/``appeared`` in a positional
    unpack survives parity; only a cross-type swap like
    ``files``/``include_paths`` fails it.) Reordering these fields is
    therefore safe today; keep it that way by not reintroducing a positional
    unpack.
    """

    files: list[Path]
    include_paths: set[str] | None
    queued: int
    deselected: int
    vanished_paths: set[str]
    appeared: int


def _apply_selection(files: list[Path], params: ImportParams) -> _Selection:
    """Filter the discovered files by the user's selection and measure drift.

    THE single home for the pre-copy-loop selection block, called by the
    ``run_import_job`` orchestrator for both transports.

    ``files`` is the raw discovery result; ``discovered`` (its length) is
    the caller's, and stays the card-safety denominator.
    """
    # Coerce ONCE, above the filter, and use this set everywhere below.
    # ``ImportParams.include_paths`` is typed ``set | None`` but a JSON
    # payload deserializes to a list, and the drift math below does set
    # arithmetic on it (``- discovered_paths``), which raises TypeError on a
    # list before a single file is copied or rsynced. Deduping here also
    # keeps ``len(include_paths)`` agreeing with the set the filter actually
    # used, so a payload with repeats can't skew ``deselected``.
    include_paths = (
        set(params.include_paths) if params.include_paths is not None else None
    )
    # Snapshot BEFORE filtering — drift is measured against what the card
    # actually holds, and computing it post-filter makes files-appeared zero.
    # NOTE: this is a set while the caller's ``discovered`` is a raw list
    # length, so overlapping sources (``/card`` plus ``/card/DCIM``)
    # enumerate a file twice and make ``len(discovered_paths) <
    # discovered``. Do not assume the two agree.
    discovered_paths = {str(f) for f in files}
    if include_paths is not None:
        # Matching is exact string equality against ``str(f)`` as produced by
        # ``discover_source_files``. The caller's paths come from that same
        # enumeration over the same raw source strings (the ``path`` field of
        # ``/api/import/folder-preview``), and NEITHER side resolves symlinks
        # or otherwise normalizes. Realpath-ing ``params.sources`` here would
        # silently empty this filter and copy (or rsync) nothing. Covered by
        # ``test_selection_filter_matches_unresolved_paths_*``, which import
        # through a symlinked directory — an ordinary ``tmp_path`` card is
        # already fully resolved and cannot tell the two apart.
        files = [f for f in files if str(f) in include_paths]
    # Progress denominator. Deliberately NOT ``discovered``: that counts the
    # whole card, so a half-deselected import would run to completion with
    # the bar stalled near 50% — a finished job that looks hung. ``queued``
    # is the work actually enqueued; ``discovered`` keeps backing the
    # card-safety verdict. Two denominators, on purpose.
    queued = len(files)

    # Selection drift. Computed against the pre-filter snapshot.
    deselected = 0
    vanished_paths = set()
    appeared = 0
    if include_paths is not None:
        # Deliberately NOT gated on ``previewed_count``. A selected file the
        # card no longer holds is drift whether or not the caller told us how
        # big the preview was, the ledger equality cannot see it (discovered
        # shrinks in step with copied), and ``include_paths`` without
        # ``previewed_count`` is constructible today — both fields are
        # independently optional on ``ImportParams``. Gating this would fail
        # OPEN on exactly the case this code exists to close.
        vanished_paths = include_paths - discovered_paths
    if include_paths is not None and params.previewed_count is not None:
        # ``deselected`` is compared with ``!= 0``, not ``> 0`` (see
        # ``_selection_blocks_format``): a negative value means the caller
        # previewed fewer files than it selected, which is self-inconsistent,
        # and this module fails closed on inconsistency rather than reading
        # it as "nothing deselected".
        deselected = params.previewed_count - len(include_paths)
        # The clamp conflates "no new files" with "the card holds fewer
        # files than were previewed". That second case is not lost: some
        # previewed file must be missing, and it is caught either by
        # ``vanished_paths`` (if it was selected) or by a positive
        # ``deselected`` (if it was not, since ``include_paths`` is then a
        # proper subset of the previewed set). NOT by a negative
        # ``deselected`` — previewed=3 with all three selected and one
        # vanished gives ``deselected == 0``, and only ``vanished_paths``
        # blocks it. ``appeared`` is a report count, so a negative would be
        # meaningless.
        appeared = max(0, len(discovered_paths) - params.previewed_count)

    return _Selection(
        files=files, include_paths=include_paths, queued=queued,
        deselected=deselected, vanished_paths=vanished_paths,
        appeared=appeared,
    )


def _selection_summary(params: ImportParams, include_paths: set[str] | None, *, discovered: int, copied: int,
                       skipped_duplicate: int, failed: int) -> str:
    """The import step's summary line. Shared by both copy paths.

    Two forms; the discovered total appears exactly once in each.

      no selection: "5 copied, 0 already present, 0 failed of
                     5 discovered"   (byte-identical to pre-selection)
      selection:    "1 selected of 3 discovered, 1 copied,
                     0 already present, 0 failed"

    Dropping the tail from the no-selection form to make room for the
    prefix would leave every user who never touches a checkbox with counts
    and no total to read them against.

    The gate is conjunctive, and must stay that way: ``include_paths`` and
    ``checked_count`` are independently optional on ``ImportParams``, and it
    is ``include_paths`` alone that decides whether the copy set was
    filtered. Keying this on ``checked_count`` by itself would let
    ``ImportParams(sources=..., checked_count=3)`` — no ``include_paths``,
    so the whole card is copied — report "3 selected of 5 discovered",
    claiming a selection for a run where none was applied.

    The selected figure is ``checked_count``, NOT ``len(include_paths)``:
    that set also carries files the user left unchecked because they were
    flagged duplicates, so it would overstate what was chosen.

    Counts are keyword-only: four ints in a row is a swap waiting to happen,
    and a swapped ``copied``/``failed`` reads as a clean run.
    """
    if include_paths is not None and params.checked_count is not None:
        return (f"{params.checked_count} selected of {discovered} "
                f"discovered, {copied} copied, "
                f"{skipped_duplicate} already present, {failed} failed")
    return (f"{copied} copied, {skipped_duplicate} already present, "
            f"{failed} failed of {discovered} discovered")


def _append_selection_unsafe(unsafe_files: list[UnsafeFile], *, deselected: int, vanished_paths: set[str],
                             appeared: int) -> None:
    """Append the selection-drift lines to ``unsafe_files``, in place.

    Shared by both copy paths, and the reporting counterpart to
    ``_selection_blocks_format``: every selection signal that can flip the
    pill red appends a line here, because ``renderResult`` HIDES the unsafe
    list when it is empty — otherwise the user gets "Do NOT format the card
    yet" with no stated reason. That property holds for the selection
    signals only, not for every caller: ``partial_scope``
    (``recursive=False``, or a narrowed ``file_types``) flips
    ``safe_to_format`` False and appends nothing, so that path renders a
    bare red pill.

    These lines *attribute* the gap between what was previewed and what was
    copied; they do not claim to enumerate it. Deselect one file, have
    another vanish, and have a third arrive after the preview, and the
    counts no longer map one-to-one onto the files still card-only — do not
    reword these into a claim that the list is exhaustive.

    Both branches of ``deselected`` are covered on purpose:
    ``_selection_blocks_format`` blocks on ``deselected != 0``, so a
    negative count (the payload previewed fewer files than it selected)
    turns the pill red too. A lone ``if deselected > 0`` would leave exactly
    that red pill bare. The negative branch does not quote the number: the
    payload is self-inconsistent, so the count is precisely the thing that
    cannot be trusted. A negative ``deselected`` also cannot occur alone —
    ``len(include_paths) > previewed_count`` forces either a selected path
    the card no longer holds (``vanished_paths``) or a card holding more
    than was previewed (``appeared``) — so its line always ships alongside
    another.

    ``appeared`` likewise only ever renders under a red pill: a balanced
    ledger with ``appeared > 0`` forces ``deselected <= 0`` (negative
    blocks; zero would make ``appeared`` zero too).

    Counts are pluralized (``_plural`` + verb agreement) because 1 is the
    single most likely real case — one frame deselected, one file gone.
    """
    if deselected > 0:
        unsafe_files.append({
            "path": "Deselected files",
            "reason": (
                f"{deselected} file{_plural(deselected)} you deselected "
                f"{'was' if deselected == 1 else 'were'} not copied"
            ),
        })
    if deselected < 0:
        unsafe_files.append({
            "path": "Selection count mismatch",
            "reason": ("your preview reported fewer files than were "
                       "selected, so how many files went uncopied could not "
                       "be determined — re-preview before formatting"),
        })
    if vanished_paths:
        vanished_count = len(vanished_paths)
        unsafe_files.append({
            "path": "Files missing at import time",
            "reason": (
                f"{vanished_count} file{_plural(vanished_count)} "
                f"{'was' if vanished_count == 1 else 'were'} in scope but "
                "had disappeared from the source when the import ran"
            ),
        })
    if appeared > 0:
        unsafe_files.append({
            "path": "Files added after preview",
            "reason": (
                f"at least {appeared} file{_plural(appeared)} arrived after "
                f"your preview and {'was' if appeared == 1 else 'were'} not "
                f"imported — re-preview to include "
                f"{'it' if appeared == 1 else 'them'}"
            ),
        })
