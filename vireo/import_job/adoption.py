"""Destination collision resolution and crash-recovery adoption of
bytes already at the destination.
"""

from __future__ import annotations

import os
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from import_job.collision import Adoption, candidate_adoption, collision_candidates
from import_job.copy_verify import _hash_dest_file
from import_job.siblings import _companion_key, _sibling_blocks_slot
from import_job.state import _counts, _LandedFile, _record_checker

if TYPE_CHECKING:
    from import_dedup import DuplicateChecker
    from import_job.destination import _DestContext
    from import_job.import_types import StopCheck
    from import_job.state import _ImportBatchState, _ImportRunState


@dataclass(frozen=True)
class _CardFile:
    """The card file one collision walk places, with its source identity.

    ``hash_fn`` is the zero-arg RAW source hash (see
    ``_resolve_dest_collision``); ``size`` and ``mtime_ns`` are the source
    stat the transport captured before the walk.
    """

    file: Path
    rel: str
    """The file's card folder, keying its per-folder counts."""
    hash_fn: Callable[[], str | None]
    size: int
    mtime_ns: int


def _book_adoption(
    state: _ImportRunState, batch_st: _ImportBatchState, checker: DuplicateChecker | None,
    src: _CardFile, *, dest_path: str, verified_hash: str, record_hash: str | None,
) -> None:
    """Book one crash-recovery adoption: count the duplicate skip, fold
    the file into ``landed`` with origin "skipped_duplicate" (see the
    rsync call site's rationale comment for why adoptions ride
    ``landed`` and deliberately NOT ``dup_skips``), and register the
    identity with the intra-run checker.

    ``verified_hash`` is the ledger-normalized hash (never None — the
    zero-byte convention is ``EMPTY_FILE_SHA256``); ``record_hash`` is
    the RAW source hash the checker's intra-batch maps rely on (still
    None for a zero-byte source on the rsync transport).
    """
    state.skipped_duplicate += 1
    _counts(state, src.rel)["skipped_duplicate"] += 1
    batch_st.landed.append(_LandedFile(
        dest_path=dest_path,
        verified_hash=verified_hash,
        source_path=str(src.file),
        origin="skipped_duplicate",
        src_size=src.size,
        src_mtime_ns=src.mtime_ns,
    ))
    _record_checker(
        state, checker, src.file, batch_st.dest_folder, record_hash,
    )


# Verdicts from ``_resolve_dest_collision``. _WALK_HANDLED means the file
# reached a terminal bucket inside the walk (a crash-recovery adoption or
# an intra-batch duplicate skip) and is fully booked — the caller returns
# _ENQ_HANDLED. _WALK_PLACED carries the destination BASENAME the caller
# must copy/queue the bytes to. Cancellation is NOT a verdict: the walk
# lets ``DestReadCancelled`` propagate to each transport's existing
# handler, which is the only place that knows how to book it.
_WALK_HANDLED = "handled"


_WALK_PLACED = "placed"


# A plain Stop observed BETWEEN candidate probes. Deliberately distinct
# from ``DestReadCancelled``, which the walk still lets propagate: that
# exception means a destination READ was interrupted mid-flight, i.e. the
# mount may be wedged, and its handlers set ``dest_read_cancelled``, which
# suppresses both the post-loop mount probe and the batch's catalog pass.
# A healthy Stop observed between probes is no evidence of a sick mount,
# and suppressing the catalog pass for it would leave already-copied files
# uncataloged until some later run adopted them. See
# ``_ImportBatchLoop._catalog_batch``.
_WALK_CANCELLED = "cancelled"


def _resolve_dest_collision(
    state: _ImportRunState, batch_st: _ImportBatchState, ctx: _DestContext, src: _CardFile, *,
    checker: DuplicateChecker | None, claims: dict[str, str | None] | None,
    stop_requested: StopCheck,
) -> tuple[str, str | None]:
    """Walk the primary basename and its numeric suffixes to an adoption,
    an intra-batch duplicate skip, or a free destination basename.

    ONE walk for both transports (PR 7b; PR 7 adaptation 2 left a
    hand-mirrored copy inside each of them). The two remaining
    transport-shaped inputs are parameters rather than branches:

    ``src.hash_fn``: zero-arg callable returning the RAW source hash —
    ``None`` means a checker'd zero-byte source, the convention the
    intra-batch maps rely on. Memoized-lazy on the local transport (which
    may never need a source hash at all), precomputed on rsync (which
    already hashed eagerly for its ``queued_src_hashes`` dedup). Called
    only where a hash is genuinely required, and always AFTER the
    destination read, so a Stop mid-read is observed before the card is
    touched.

    ``claims``: the rsync transport's batch-scoped ``claimed_basenames``
    reservation map, or ``None`` on the local transport — where the
    filesystem itself is the reservation, because bytes land inside
    ``enqueue`` rather than at the batch's flush. That asymmetry is
    inherent to deferred transfer, not drift: for a same-basename
    byte-identical sibling the local path adopts the copy it just made
    while rsync dup-skips against the claim.

    ``DestReadCancelled`` (and any source-read ``OSError`` from
    ``src.hash_fn``) propagate to the caller's existing handlers. An
    unreadable *candidate*, by contrast, is advanced past — it is a
    destination artifact, not a source problem.

    Returns ``(_WALK_HANDLED, None)`` or ``(_WALK_PLACED, basename)``.
    """
    source_file = src.file
    stem = os.path.splitext(source_file.name)[0]
    group_key = _companion_key(source_file)
    anchor = batch_st.companion_slots.get(group_key)
    for counter, probe in enumerate(collision_candidates(source_file, batch_st.dest_folder, anchor)):
        if _stopped_while_advancing(counter, stop_requested):
            return _WALK_CANCELLED, None
        slot, candidate, cand_path = probe.slot, probe.name, probe.path
        if claims is not None:
            candidate_key = ctx.fold_basename(candidate)
            if candidate_key in claims:
                if _skip_claimed_identical_sibling(
                    state, batch_st, claims[candidate_key], src, checker=checker,
                ):
                    return _WALK_HANDLED, None
                continue
        if os.path.lexists(cand_path):
            # ``lexists``, not ``exists``: a DANGLING symlink is a
            # directory entry that this walk must not treat as a free
            # slot. ``exists`` follows the link and reports False, so the
            # name got picked — and then ``copy_and_hash_verify``'s
            # no-overwrite ``os.link`` promote hit the entry that does
            # exist, raised FileExistsError, and failed a healthy card
            # file with the misleading reason "copy verification failed".
            # (It does not write THROUGH the link: the copy goes to a
            # hidden sibling temp and is promoted by link/rename, neither
            # of which follows a symlink.) Nothing to adopt either — there
            # are no bytes behind it — so advance.
            adopt = _existing_candidate_adoption(
                ctx, src, cand_path=cand_path,
                stop_requested=stop_requested,
            )
            if adopt is not None and _settle_adoption(
                state, batch_st, ctx, adopt, src, checker=checker,
                claims=claims, stop_requested=stop_requested, slot=slot,
                anchor=anchor, candidate=candidate,
            ):
                return _WALK_HANDLED, None
            continue
        if anchor is None and _sibling_blocks_slot(
            batch_st, source_file, stem, slot,
            checker=checker, stop_requested=stop_requested,
            claims=claims, ctx=ctx,
        ):
            # This name is free, but a same-stem sibling from the card
            # would collide with a different file at the same suffix and
            # be renamed away from us (or the same collision is already
            # queued for the rsync batch under that basename). Move the
            # whole group on together.
            continue
        batch_st.companion_slots.setdefault(group_key, slot)
        return _WALK_PLACED, candidate
    raise AssertionError("collision candidate traversal must not end")


def _stopped_while_advancing(counter: int, stop_requested: StopCheck) -> bool:
    """The collision walk's cancellation poll; True means return
    ``_WALK_CANCELLED``.
    """
    # Only an iteration reached by ADVANCING past a candidate polls,
    # and every advance path — claimed sibling, dangling entry,
    # source-backed entry, unreadable candidate, size mismatch,
    # hash mismatch — can run without calling _hash_dest_file,
    # which is the walk's only other cancellation poll. Without
    # this a Stop goes unobserved for the whole collision chain
    # while the walk keeps probing a (possibly slow, possibly
    # dying) network destination.
    #
    # Reported as a VERDICT, not as ``DestReadCancelled``: no
    # destination read was interrupted here, so this must not
    # set ``dest_read_cancelled`` and suppress the batch's mount
    # probe and catalog pass. See ``_WALK_CANCELLED``.
    #
    # Gated on ``counter`` deliberately: the no-collision fast
    # path stays unpolled, so a Stop racing the orchestrator's
    # loop-top check cannot flip an otherwise-clean copy into a
    # cancellation.
    if not counter:
        return False
    return stop_requested()


def _skip_claimed_identical_sibling(
    state: _ImportRunState, batch_st: _ImportBatchState, claimed_hash: str | None, src: _CardFile,
    *, checker: DuplicateChecker | None,
) -> bool:
    """Book an intra-batch duplicate skip against a claimed basename.

    Returns True when the file was booked (the walk is done), False when
    the walk must advance past the claimed name.
    """
    # Claimed earlier in this batch (a same-basename sibling
    # already queued). If that sibling has our exact bytes,
    # skip as an intra-batch duplicate; otherwise advance.
    # Gated on ``checker``: with ``skip_duplicates=False``
    # both files must be queued under distinct suffixes
    # instead of the second being counted as
    # ``skipped_duplicate`` — which would otherwise let a
    # verified run report ``safe_to_format=True`` because
    # ``copied + skipped_duplicate == discovered`` without an
    # off-card row for it.
    if checker is not None and claimed_hash == src.hash_fn():
        state.skipped_duplicate += 1
        _counts(state, src.rel)["skipped_duplicate"] += 1
        batch_st.dup_skips.append((src.file, False))
        _record_checker(state, checker, src.file,
                        batch_st.dest_folder, src.hash_fn())
        return True
    return False


def _existing_candidate_adoption(
    ctx: _DestContext, src: _CardFile, *, cand_path: str,
    stop_requested: StopCheck,
) -> Adoption | None:
    """Probe one occupied entry; a failed stat advances rather than frees it."""
    try:
        cand_stat = os.stat(cand_path)
    except OSError:
        return None
    return candidate_adoption(
        src.file, cand_path, cand_stat, source_size=src.size,
        source_hash=src.hash_fn, path_under_any_source=ctx.path_under_any_source,
        hash_candidate=lambda path: _hash_dest_file(path, stop_requested),
    )


def _settle_adoption(
    state: _ImportRunState, batch_st: _ImportBatchState, ctx: _DestContext, adopt: Adoption,
    src: _CardFile, *, checker: DuplicateChecker | None, claims: dict[str, str | None] | None,
    stop_requested: StopCheck, slot: int, anchor: int | None, candidate: str,
) -> bool:
    """Book an adoptable candidate unless its companion group cannot
    settle at ``slot``; returns True when booked, False to advance.
    """
    source_file = src.file
    stem = os.path.splitext(source_file.name)[0]
    # A byte-identical file already at this slot is only a valid
    # adopt if the whole companion group can settle here — same
    # gate as the walk's ``_WALK_PLACED`` path. Without this, adopting the
    # RAW at slot 0 sets the anchor, and a same-stem sibling
    # (JPEG) that collides with an unrelated file at slot 0
    # advances to ``_1``, splitting the pair. The scanner can
    # then merge the RAW with the unrelated JPEG when their
    # capture metadata is missing or compatible.
    if anchor is None and _sibling_blocks_slot(
        batch_st, source_file, stem, slot,
        checker=checker, stop_requested=stop_requested,
        claims=claims, ctx=ctx,
    ):
        return False
    dest_path, verified_hash, record_hash = adopt
    if claims is not None:
        # Claim with the RAW hash (``None`` for a checker'd
        # zero-byte source) so a later same-basename sibling
        # compares against the same convention the queued path
        # uses.
        claims[ctx.fold_basename(candidate)] = src.hash_fn()
    # ``_book_adoption`` folds the file into ``landed`` with
    # origin "skipped_duplicate" — deliberately NOT ``dup_skips``
    # — so the restricted scan catalogs it and the rollback
    # decrements the right counter. See its docstring.
    _book_adoption(
        state, batch_st, checker, src, dest_path=dest_path,
        verified_hash=verified_hash, record_hash=record_hash,
    )
    batch_st.companion_slots.setdefault(_companion_key(source_file), slot)
    return True
