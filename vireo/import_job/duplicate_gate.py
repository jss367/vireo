"""The per-file duplicate gate and its byte-verified twins."""

import logging
import os

from import_job.copy_verify import DestReadCancelled, _hash_dest_file
from import_job.state import _counts, _fail
from import_job.twins import _hash_twin_rows, _key_twin_rows, _likely_twin_rows

log = logging.getLogger(__name__)


# Verdicts returned by _duplicate_gate. The CALLER maps them onto the
# per-file loop's control flow, and the mapping is load-bearing:
# _GATE_CANCELLED must map to ``break`` (Stop interrupted a destination
# read — every further step this batch would touch the same, possibly
# dead, mount; mapping it to ``continue`` would reintroduce the
# wedged-mount cancellation pin fixed in PR #1423). _GATE_SKIPPED maps
# to ``continue`` (the file was counted as a duplicate skip or failed
# its duplicate check); _GATE_PROCEED falls through to the
# collision/adopt/transfer path.
_GATE_SKIPPED = "skipped"


_GATE_PROCEED = "proceed"


_GATE_CANCELLED = "cancelled"


def _duplicate_gate(state, batch_st, *, source_file, rel, checker, db,
                    params, ctx, stop_requested):
    """Decide whether ``source_file`` is a duplicate of cataloged or
    intra-run bytes, doing the byte-verification reads that decision
    requires. Shared verbatim by both import paths (the 2026-08-08
    phase map verified the two nested copies identical); returns one of
    the ``_GATE_*`` verdicts — see the constants above for the caller's
    mapping contract.
    """
    if checker is None:
        return _GATE_PROCEED
    try:
        token = checker.match(source_file)
    except OSError as e:
        _fail(state, rel, source_file, f"duplicate check failed: {e}")
        return _GATE_SKIPPED
    if token is None:
        return _GATE_PROCEED
    if (
        params.trust_likely_duplicates
        and not params.verify_by_hash
    ):
        likely_rows = _likely_twin_rows(
            db, token, source_file, ctx.path_under_any_source,
        )
        if likely_rows:
            if not _grant_twin_visibility(
                state, batch_st, db, rel, source_file, likely_rows,
            ):
                return _GATE_SKIPPED
            state.skipped_duplicate += 1
            state.unverified_duplicate += 1
            _counts(state, rel)["skipped_duplicate"] += 1
            batch_st.dup_skips.append((source_file, True))
            return _GATE_SKIPPED
    twins = _twin_rows_and_source_hash(
        state, db, checker, token, rel=rel, source_file=source_file,
    )
    if twins is None:
        return _GATE_SKIPPED
    twin_rows, src_hash = twins
    accept = _intra_run_token_proven(state, token, src_hash)
    verified_twin_rows = []
    if not accept:
        accept, verified_twin_rows = _byte_verified_twins(
            state, batch_st, ctx, twin_rows, src_hash, stop_requested,
        )
    if state.cancelled:
        # Stop interrupted a twin hash above. Don't let this file fall
        # through to the adopt/copy path — every further step touches
        # the same (possibly dead) mount.
        return _GATE_CANCELLED
    if accept:
        if not _grant_twin_visibility(
            state, batch_st, db, rel, source_file, verified_twin_rows,
        ):
            return _GATE_SKIPPED
        state.skipped_duplicate += 1
        _counts(state, rel)["skipped_duplicate"] += 1
        batch_st.dup_skips.append((source_file, False))
        # Existing verified twins receive photo-only membership, including
        # moved twins outside the current destination. Never share siblings.
        run_dest = state.run_dest_folders.get(token)
        if run_dest is not None:
            batch_st.dup_dirs.add(run_dest)
        return _GATE_SKIPPED
    # No byte-identical twin remains on disk — the card file is a
    # distinct photo; import it normally.
    return _GATE_PROCEED


def _grant_twin_visibility(state, batch_st, db, rel, source_file, rows):
    """Grant the active workspace the accepted twin ``rows`` and record
    the new grants and promotions on ``batch_st`` for a mount-loss
    rollback. Returns False when the grant failed (the file is booked
    failed and the gate answers ``_GATE_SKIPPED``).
    """
    try:
        new_grants, promoted = db.photo_visibility.grant_verified_twins_tracked(
            db.active_workspace_id, rows,
        )
        db.commit()
    except Exception as exc:
        log.exception("Duplicate photo workspace visibility failed")
        db.rollback()
        _fail(state, rel, source_file, f"duplicate photo workspace visibility failed: {exc}")
        return False
    batch_st.dup_granted_photo_ids.extend(new_grants)
    batch_st.dup_promoted_folder_ids.extend(promoted)
    return True


def _twin_rows_and_source_hash(state, db, checker, token, *, rel,
                               source_file):
    """Cataloged twin rows for ``token`` and the source hash to verify
    them against, or ``None`` when hashing the source failed (the file is
    booked failed).
    """
    if token[0] == "hash":
        return _hash_twin_rows(db, token[1]), token[1]
    twin_rows = _key_twin_rows(db, token[1])
    # Hash the current source so a key match can be
    # confirmed against a cataloged (or intra-run)
    # twin's actual bytes. Reading a removable-media
    # source can fail (card yanked mid-check, I/O
    # error) — same as checker.match() and the copy
    # path, that must fail JUST this source rather
    # than escape and kill the whole background job.
    try:
        src_hash = checker.content_hash(source_file)
    except OSError as e:
        _fail(
            state, rel, source_file,
            f"duplicate check failed: {e}",
        )
        return None
    return twin_rows, src_hash


def _intra_run_token_proven(state, token, src_hash):
    """True when this run's own landing already proves ``token``'s bytes."""
    # An intra-run token is byte-proven by this session's
    # own copy_and_hash_verify — safe to skip without
    # hitting the archive, but ONLY when the token itself
    # carries bytes (``('hash', …)`` — the hash IS the
    # proof) or the current source's bytes match the run
    # twin's verified hash (``('key', …)`` — the metadata
    # key proves nothing about bytes; two different files
    # with the same filename+size+capture-second across
    # cards would otherwise be counted as skipped without
    # ever being byte-compared). Any other match
    # (catalog-side hash OR metadata-only key) is
    # stale-suspect: the photos.file_hash row could
    # describe an archive file that was deleted or
    # modified since the last scan, so a duplicate skip
    # must be backed by a cataloged twin that STILL holds
    # those bytes on disk. Without this, a stale hash row
    # would let the card be counted as skipped_duplicate
    # and safe_to_format go green while the card is the
    # only remaining copy of the bytes.
    if token not in state.run_dest_folders:
        return False
    if token[0] == "hash":
        return True
    run_hash = state.run_verified_hashes.get(token)
    return (
        src_hash is not None
        and run_hash is not None
        and src_hash == run_hash
    )


def _byte_verified_twins(state, batch_st, ctx, twin_rows, src_hash,
                         stop_requested):
    """Re-hash each off-card cataloged twin against ``src_hash``.

    Returns ``(accept, verified_twin_rows)``. A Stop that interrupts a
    twin read sets ``state.cancelled`` and ``dest_read_cancelled`` and
    ends the walk; the gate then answers ``_GATE_CANCELLED``.
    """
    accept = False
    # verified_twin_rows records only the twin(s) whose
    # bytes we actually hashed on disk this run and
    # matched against the source. Both 'hash' and 'key'
    # tokens can carry stale rows: 'key' is a filename+
    # size+capture-second bucket where individual rows
    # may hold unrelated bytes, and 'hash' shares the
    # token's stored file_hash by construction but that
    # column reflects the LAST scan — an archive file
    # deleted or overwritten between scans leaves a stale
    # hash row. Linking any twin folder we did not
    # re-hash would pull unrelated/missing archive folders
    # into the active workspace on a duplicate-only
    # import. See PR #1107 review.
    verified_twin_rows = []
    for twin in twin_rows:
        twin_path = os.path.join(
            twin["folder_path"], twin["filename"],
        )
        # A cataloged twin under any import source
        # root is (or may be) the card file being
        # imported this run — a stale scan of the
        # mounted card left a photos row whose path
        # IS the card. Hashing it just re-reads the
        # source, which proves nothing about an
        # archive copy; accepting it as duplicate
        # proof would flip safe_to_format green
        # while the card holds the only bytes. Only
        # an off-card twin can back a duplicate
        # skip. See PR #1107 review.
        if ctx.path_under_any_source(twin_path):
            continue
        try:
            twin_hash = _hash_dest_file(
                twin_path, stop_requested)
        except DestReadCancelled:
            state.cancelled = True
            batch_st.dest_read_cancelled = True
            break
        except OSError:
            continue
        if twin_hash is not None and twin_hash == src_hash:
            accept = True
            # Keep scanning to collect every
            # byte-verified twin — for both 'hash'
            # and 'key' tokens. Breaking at the
            # first match (or falling back to the
            # full twin_rows for 'hash') risks
            # linking a stale/off-destination twin:
            # _linkable_twin_dirs then either drops
            # a legitimate destination twin (leaving
            # the imported photo invisible in the
            # active workspace) or pulls an
            # unrelated folder in (if the catalog's
            # stored hash row no longer describes
            # the on-disk bytes). See PR #1107
            # review.
            verified_twin_rows.append(twin)
    return accept, verified_twin_rows
