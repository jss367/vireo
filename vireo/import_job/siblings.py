"""Companion (RAW+JPEG sibling) keys and destination slot logic."""

import os
import stat as stat_mod

from import_dedup import compute_file_hash
from import_job.copy_verify import DestReadCancelled, _hash_dest_file
from import_job.destination import _is_source_backed_dest


def _companion_key(source_file):
    """Group key for card files that must share a destination stem."""
    return (
        os.path.dirname(str(source_file)),
        os.path.splitext(os.path.basename(str(source_file)))[0].casefold(),
    )


def _companion_siblings(batch):
    """Map companion key -> source files, for keys with 2+ files in ``batch``."""
    groups = {}
    for source_file in batch:
        groups.setdefault(_companion_key(source_file), []).append(source_file)
    return {key: files for key, files in groups.items() if len(files) > 1}


def _slot_for(counter, anchor):
    """The numeric suffix the collision walk's ``counter``-th probe tries.

    With no anchor (or anchor 0) this is the identity. With an anchor, the
    first probe tries the anchor's suffix, then the walk continues through
    0, 1, 2, ... skipping it.
    """
    if not anchor:
        return counter
    if counter == 0:
        return anchor
    n = counter - 1
    return n if n < anchor else n + 1


def _sibling_blocks_slot(batch_st, source_file, stem, slot, *,
                          checker=None, stop_requested=None,
                          claims=None, ctx=None):
    """True if a same-stem card sibling could not share this ``slot``.

    Blocks on anything that would push the sibling's own walk to a
    numeric suffix — a non-regular entry, a size mismatch, or a
    same-size regular file whose bytes differ from the sibling's. Only
    an exact byte match (which the sibling walk adopts as a crash
    retry's earlier landing) passes. A sibling whose source cannot be
    stat'd is ignored; its own walk fails it. Any hash read failure
    (candidate or sibling) falls back to blocking, because keeping the
    pair together beats a split when the sibling's fate is unknown.

    On the remote (rsync) transport, earlier files in this batch are
    recorded in ``claims`` (``batch_st.claimed_basenames``) but not yet
    on disk until ``flush_batch``, so a purely filesystem-visible probe
    treats those destination names as free. Pass ``claims`` and ``ctx``
    to consult that reservation map first: a queued claim whose hash
    matches the sibling's own hash is the sibling's own future landing
    (the sibling walk will adopt it as an intra-batch duplicate), any
    other claim blocks. ``claims`` is ``None`` on the local path — the
    filesystem itself is the reservation there because bytes land inside
    ``enqueue``.
    """
    siblings = batch_st.companion_siblings.get(_companion_key(source_file))
    if not siblings:
        return False
    for sibling in siblings:
        if sibling == source_file:
            continue
        sib_suffix = os.path.splitext(sibling.name)[1]
        name = sibling.name if slot == 0 else f"{stem}_{slot}{sib_suffix}"
        if claims is not None and ctx is not None:
            claim_key = ctx.fold_basename(name)
            if claim_key in claims:
                if _claim_blocks_sibling(claims[claim_key], sibling, checker):
                    return True
                continue
        path = os.path.join(batch_st.dest_folder, name)
        try:
            dest_stat = os.stat(path)
        except FileNotFoundError:
            if os.path.lexists(path):
                return True  # dangling symlink: the sibling must advance
            continue
        except OSError:
            return True
        if not stat_mod.S_ISREG(dest_stat.st_mode):
            return True
        # A candidate that IS the sibling on the card, or a symlink into
        # any source root, would look byte-identical here (both stat and
        # any subsequent hash follow the link back to the card). The
        # sibling's own ``_resolve_dest_collision`` correctly rejects
        # such a slot via ``_is_source_backed_dest`` and advances, which
        # would split the pair if we accepted the slot here. Mirror that
        # guard so the walk anchors somewhere safe for both members. See
        # ``_is_source_backed_dest`` for the geometry and PR 7b.
        if ctx is not None and _is_source_backed_dest(ctx, sibling, path):
            return True
        try:
            sib_size = sibling.stat().st_size
        except OSError:
            continue
        if dest_stat.st_size != sib_size:
            return True
        if dest_stat.st_size == 0:
            # Zero-byte↔zero-byte: the sibling walk's zero-byte adopt
            # takes the slot, not a rename. See _resolve_dest_collision.
            continue
        if _sibling_bytes_differ(path, sibling, checker, stop_requested):
            return True
    return False


def _claim_blocks_sibling(claimed_hash, sibling, checker):
    """True if a batch claim on the sibling's slot name would split the pair.

    The rsync half of ``_sibling_blocks_slot``: the claimed name is not
    on disk yet, so the claim's recorded hash stands in for its bytes.
    """
    if checker is None:
        # Mirror the ``checker is not None`` gate in
        # ``_resolve_dest_collision``: with
        # ``skip_duplicates=False`` the sibling's own walk
        # refuses to adopt a same-hash claim and advances to
        # the next suffix, so this slot would split the pair
        # even for byte-identical bytes. Block it.
        return True
    try:
        sib_hash = checker.content_hash(sibling)
    except OSError:
        return True
    # A same-bytes claim under a checker does not block: the sibling's
    # own collision walk will see the intra-batch duplicate and skip
    # this slot as one, so the pair still settles here.
    return sib_hash is None or claimed_hash != sib_hash


def _sibling_bytes_differ(path, sibling, checker, stop_requested):
    """True unless the same-sized regular file at ``path`` holds exactly
    the sibling's bytes; any read failure counts as differing.
    """
    # Same-sized regular file: settle by content. Only identical
    # bytes let the sibling's collision walk adopt the slot;
    # anything else advances it to a numeric suffix and splits the
    # pair. Use _hash_dest_file for the candidate so a Stop and a
    # stalled network mount are observed the same way the sibling's
    # own walk would observe them.
    try:
        if stop_requested is not None:
            cand_hash = _hash_dest_file(path, stop_requested)
        else:
            cand_hash = compute_file_hash(path)
    except DestReadCancelled:
        raise
    except OSError:
        return True
    try:
        sib_hash = (
            checker.content_hash(sibling)
            if checker is not None
            else compute_file_hash(str(sibling))
        )
    except OSError:
        return True
    return sib_hash is None or cand_hash is None or sib_hash != cand_hash
