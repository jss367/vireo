"""Checking a move's copy against its source.

Before a merge copies anything: ``_find_content_conflict`` and its remote
counterpart refuse a same-name file whose bytes differ. After the copy,
before the originals are deleted: ``_first_missing_source_file``,
``_fresh_copy_mismatch`` and ``_remote_verify_complete``.
``remote_verify_files`` checks the import job's per-batch remote copies, and
``preview_merge`` classifies a merge for the move dialog the way the copy
and verifier will treat it.
"""

import contextlib
import filecmp
import os
import subprocess

from job_runner.subprocess_control import run_controlled_process

from .paths import _unsafe_destination_directory
from .remote import _ssh_rsh_string
from .transfer import FINDER_METADATA_FILES, _rsync_finder_metadata_exclude_file

try:
    from ..proc import no_window_kwargs
except ImportError:
    from proc import no_window_kwargs

# How long the remote verification rsync (a --checksum dry-run over SSH) may
# run before we give up and treat the move as unverified. This only gates the
# *verify* step, never the transfer: a timeout here is conservative-safe — it
# returns a verification failure, so the originals are preserved rather than
# deleted against an unconfirmed copy.
REMOTE_VERIFY_TIMEOUT = 7200  # 2 hours


def _remote_verify_complete(rsync_bin, src_path, rsync_target, remote,
                            *, is_merge=False, control=None):
    """Independent verification that the remote copy is complete and correct.

    The local move's safety check walks the destination filesystem before
    deleting originals; a remote destination isn't locally walkable, so this
    runs ``rsync -an --checksum`` (a dry run) and inspects what it WOULD
    transfer. rsync prints (via ``--out-format=%n``) only items it would
    change, comparing by *checksum* — so any regular-file line means that
    file is missing at the destination OR present with different content.
    Returns:
      * ``None`` — every source file is present at the destination with
        matching content; safe to delete originals.
      * ``(name, None)`` — first source file still absent/different; the
        move must preserve originals.
      * ``("__ERROR__", detail)`` — the verification rsync itself failed or
        timed out; treated as a verification failure (originals preserved).
    Bandwidth-cheap: the NAS checksums its own local disk and only hashes
    cross the wire, so no bulk data is re-transferred.

    ``is_merge`` excludes Finder metadata (``.DS_Store``) on both sides so a
    ``.DS_Store`` that the merge deliberately did not copy is never reported
    as a verification failure. A fresh move carries these along and asks
    for a full compare.
    """
    exclude_ctx = (_rsync_finder_metadata_exclude_file(src_path)
                   if is_merge else contextlib.nullcontext([]))
    with exclude_ctx as metadata_excludes:
        cmd = [rsync_bin, "-an", "--checksum", "--out-format=%n",
               "-e", _ssh_rsh_string(remote),
               *metadata_excludes,
               src_path + "/", rsync_target + "/"]
        try:
            if control:
                proc = run_controlled_process(cmd, control, timeout=REMOTE_VERIFY_TIMEOUT)
            else:
                proc = subprocess.run(cmd, capture_output=True, text=True,
                                      timeout=REMOTE_VERIFY_TIMEOUT,
                                      **no_window_kwargs())
        except subprocess.TimeoutExpired:
            return ("__ERROR__", f"verification timed out after "
                    f"{REMOTE_VERIFY_TIMEOUT // 60} minutes")
        except OSError as exc:
            return ("__ERROR__", str(exc))
        if proc.returncode != 0:
            return ("__ERROR__",
                    proc.stderr.strip() or f"rsync exit {proc.returncode}")
        for line in proc.stdout.splitlines():
            name = line.rstrip("\n")
            if not name or name.endswith("/"):
                continue  # directory entry, not a file needing transfer
            return (name, None)
        return None


def remote_verify_files(rsync_bin, src_specs, rsync_target, remote,
                        dest_is_dir=True):
    """Verify an explicit list of source FILES against a remote path.

    The file-list counterpart to ``_remote_verify_complete`` (which compares
    whole directories). The import job rsyncs a batch's card files flat by
    basename into ``rsync_target/``; this runs ``rsync -an --checksum
    <card files...> rsync_target/`` — a dry run that reports (via
    ``--out-format=%n``) every listed file whose counterpart at the remote is
    missing or differs by checksum. Because the sources are the actual CARD
    files, this genuinely confirms the card's bytes landed intact on the NAS
    (comparing the local SMB mount view against the NAS would be
    near-tautological — same physical storage — and would never catch a
    corrupt transfer). Basename-flat comparison lines up with how the
    transfer landed the files.

    ``dest_is_dir`` (default True) treats ``rsync_target`` as a directory
    (``rsync_target/``), so each source lands under its own basename. Set it
    False to verify a single source file against an explicit remote FILE path
    (no trailing ``/``): the import job uses this to verify a collision-
    renamed file (card ``DSC_0001.jpg`` landed at NAS ``DSC_0001_1.jpg``)
    against its actual NAS name.

    Returns:
      * ``None`` — every listed source file is present at the remote with
        matching content.
      * ``(name, None)`` — first source file (rsync's ``%n`` relative name)
        still absent/different at the remote.
      * ``("__ERROR__", detail)`` — the verification rsync itself failed or
        timed out; treated as a verification failure.

    Bandwidth-cheap: the NAS checksums its own local disk and only hashes
    cross the wire.
    """
    if not src_specs:
        return None
    # ``--copy-links`` matches the import-transfer rsync (``_run_remote_import
    # _job`` passes it): the transfer sends the REFERENCED file bytes for a
    # symlinked source, so the source-side hash here must be computed on the
    # same referenced file (not the symlink itself). Without it, ``rsync
    # -an`` (which includes ``-l``) sees a symlink at the source and a real
    # file on the NAS, treats them as mismatched, and reports the just-
    # transferred file as verification-failed. See PR #1113 review.
    cmd = [rsync_bin, "-an", "--checksum", "--copy-links",
           "--out-format=%n", "-e", _ssh_rsh_string(remote)]
    cmd += list(src_specs)
    cmd += [rsync_target + "/" if dest_is_dir else rsync_target]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True,
                              timeout=REMOTE_VERIFY_TIMEOUT,
                              **no_window_kwargs())
    except subprocess.TimeoutExpired:
        return ("__ERROR__", f"verification timed out after "
                f"{REMOTE_VERIFY_TIMEOUT // 60} minutes")
    except OSError as exc:
        return ("__ERROR__", str(exc))
    if proc.returncode != 0:
        return ("__ERROR__",
                proc.stderr.strip() or f"rsync exit {proc.returncode}")
    for line in proc.stdout.splitlines():
        name = line.rstrip("\n")
        if not name or name.endswith("/"):
            continue  # directory entry, not a file needing transfer
        return (name, None)
    return None


def _find_content_conflict(src_path, dest_path, *, checkpoint=None):
    """Return the relative path of the first source file that ALSO exists at
    dest_path but with different content, or None. Run before a merge copies
    anything: a same-name destination file is only safe to treat as "already
    there" if its bytes match the source. A size match alone is not enough —
    filecmp with shallow=False compares contents — so we never overwrite or
    later delete the source over a genuinely different destination file.

    Existing file and directory symlinks must be independent links with the
    same literal targets. Missing entries are left for the copy to create.

    Finder metadata (``.DS_Store``) is ignored only for REGULAR non-symlink
    source files: those are the ones ``_rsync_finder_metadata_excludes``
    drops from the transfer, so a same-name Finder-managed difference at
    the destination is not a photo collision and the source's own copy is
    preserved until the whole tree is removed after a successful merge. A
    ``.DS_Store`` symlink is NOT excluded by the rsync helper (which keeps
    such symlinks) and ``--ignore-existing`` then leaves any pre-existing
    destination entry in place; a basename-only skip here would then let
    a same-sized destination file (different bytes, so a real collision)
    pass the pre-merge check, and the verifier's size-only compare would
    also accept it (``os.path.getsize`` follows the source link), silently
    losing the source symlink when the tree is removed. So the exemption
    applies only to regular non-symlink Finder metadata files; every other
    entry type (symlink, missing source) falls through to the normal
    content check.
    """
    for root, directories, files in os.walk(src_path):
        mismatch = _directory_mismatch(
            root, directories, src_path, dest_path, allow_missing=True)
        if mismatch is not None:
            return mismatch
        rel = os.path.relpath(root, src_path)
        for fn in files:
            if checkpoint:
                checkpoint()
            src_file = os.path.join(root, fn)
            if fn in FINDER_METADATA_FILES and \
                    not os.path.islink(src_file) and \
                    os.path.isfile(src_file):
                continue
            rel_name = fn if rel == "." else os.path.join(rel, fn)
            dst_file = os.path.join(dest_path, rel_name)
            if os.path.islink(src_file):
                if os.path.lexists(dst_file) and not _matching_independent_symlink(src_file, dst_file):
                    return rel_name
                continue
            if os.path.isfile(dst_file) and \
                    not filecmp.cmp(src_file, dst_file, shallow=False):
                return rel_name
    return None


def _find_remote_content_conflict(rsync_bin, src_path, rsync_target, remote, *, control=None):
    """Remote counterpart to ``_find_content_conflict`` — find the first
    source file whose same-name twin at the remote destination differs in
    content. The destination is on the NAS so we can't filecmp it directly;
    instead run ``rsync -an --existing --checksum`` over SSH, which only
    inspects items that ALREADY exist at the receiver and reports any whose
    bytes don't match. Missing destination files are skipped — those are
    legitimate transfers the merge will perform, not conflicts.
    Returns:
      * ``None`` — no same-name file differs; safe to start the merge.
      * ``(name, None)`` — first colliding file (rsync's relative path).
      * ``("__ERROR__", detail)`` — the conflict probe itself failed; the
        caller treats this as a refusal so a half-transferred state never
        lands on the NAS.
    Without this, ``--ignore-existing`` would still copy every MISSING
    source file before the post-transfer ``--checksum`` verify could spot
    the conflict — leaving the newly-copied files orphaned on the NAS,
    breaking the local-merge contract that a content conflict cancels with
    nothing changed.

    Finder metadata (``.DS_Store``) is excluded on both sides so a Finder
    difference between the local staging tree and the NAS never refuses the
    merge; the local counterpart applies the same skip.
    """
    with _rsync_finder_metadata_exclude_file(src_path) as metadata_excludes:
        cmd = [rsync_bin, "-an", "--existing", "--checksum",
               "--out-format=%n",
               "-e", _ssh_rsh_string(remote),
               *metadata_excludes,
               src_path + "/", rsync_target + "/"]
        try:
            if control:
                proc = run_controlled_process(cmd, control, timeout=REMOTE_VERIFY_TIMEOUT)
            else:
                proc = subprocess.run(cmd, capture_output=True, text=True,
                                      timeout=REMOTE_VERIFY_TIMEOUT,
                                      **no_window_kwargs())
        except subprocess.TimeoutExpired:
            return ("__ERROR__", f"conflict check timed out after "
                    f"{REMOTE_VERIFY_TIMEOUT // 60} minutes")
        except OSError as exc:
            return ("__ERROR__", str(exc))
        if proc.returncode != 0:
            return ("__ERROR__",
                    proc.stderr.strip() or f"rsync exit {proc.returncode}")
        for line in proc.stdout.splitlines():
            name = line.rstrip("\n")
            if not name or name.endswith("/"):
                continue  # directory entry, not a file rsync would transfer
            return (name, None)
        return None


def _matching_independent_symlink(source, destination):
    """Preserve a link's target while rejecting aliases back to its own entry."""
    try:
        if not os.path.islink(destination):
            return False
        source_stat, destination_stat = os.lstat(source), os.lstat(destination)
        return (
            (source_stat.st_dev, source_stat.st_ino) != (destination_stat.st_dev, destination_stat.st_ino)
            and os.readlink(source) == os.readlink(destination)
        )
    except OSError:
        return False


def _directory_mismatch(root, directories, src_path, dest_path, *, allow_missing=False):
    """Verify physical destination parents and opaque directory links.

    ``os.walk(..., followlinks=False)`` puts directory symlinks in ``dirs``
    but never visits their contents. A file-only verifier would silently
    accept a missing link, a different link, or an ordinary directory.
    lstat also rejects a destination parent alias back into the source.
    Preflight allows missing destinations; final verification does not.
    """
    relative = os.path.relpath(root, src_path)
    destination_dir = dest_path if relative == "." else os.path.join(dest_path, relative)
    unsafe = _unsafe_destination_directory(dest_path, destination_dir, allow_missing=allow_missing)
    if unsafe is not None:
        return os.path.relpath(unsafe, dest_path)
    for name in directories:
        source = os.path.join(root, name)
        if not os.path.islink(source):
            continue
        relative = os.path.relpath(source, src_path)
        destination = os.path.join(dest_path, relative)
        if allow_missing and not os.path.lexists(destination):
            continue
        if not _matching_independent_symlink(source, destination):
            return relative
    return None


def _first_missing_source_file(src_path, dest_path, *, verify_contents=False,
                               is_merge=False, progress=None):
    """Return the relative path of the first source file absent (or
    size-mismatched, or a symlink) at dest_path, or None if every source
    file is present and matches. Used to verify a merge before deleting
    originals.

    With ``verify_contents``, compare fresh bytes as well before allowing
    managed temporary originals to be removed.

    Expected source symlinks must remain independent links with identical
    target strings. For regular source files, a symlinked destination is
    treated as missing: `os.path.isfile` /
    `os.path.getsize` follow the link, so a symlink pointing back into the
    source tree (directly, or via a symlinked parent directory) would pass
    a size compare even though no independent copy exists at the
    destination — and the post-copy `shutil.rmtree(src_path)` would then
    destroy the only copy. `lexists` lets a broken symlink count as
    missing instead of crashing later checks; `islink` catches the direct
    case; `samefile` catches the symlinked-parent case where `src_file`
    and `dst_file` resolve to the same inode.

    Finder metadata (``.DS_Store``) is skipped for REGULAR non-symlink
    source files, and only when ``is_merge`` says the caller is verifying
    a merge into an existing destination: those are the ones
    ``_rsync_finder_metadata_excludes`` drops from the transfer, so a
    missing or differently-sized destination counterpart is expected and
    must not refuse the delete. A fresh (non-merge) move with
    ``verify_contents`` also reaches this verifier, but there ``.DS_Store``
    IS transferred by the primary rsync path and the shutil fallback, so
    the destination must hold a matching copy before ``rmtree`` removes
    the source — a destination ``.DS_Store`` that disappears between copy
    and verify would otherwise be silently forgiven and take the source's
    copy with it. A ``.DS_Store`` symlink is NOT excluded by the rsync
    helper (which preserves symlinks with that name), but
    ``--ignore-existing`` skips it whenever the destination already holds
    an entry at that path; without a real check here, the source tree
    would then be deleted with the symlink never landing at the
    destination. So the exemption applies only to regular non-symlink
    files under a merge; every other entry type (symlink, directory,
    missing source) and every fresh-move call fall through to the normal
    verification.

    ``progress``, when given, is called as ``progress(checked, rel_name)``
    before each file is examined and ``progress(checked, "")`` once every
    file has passed, where ``checked`` counts the source files done so far.
    """
    checked = 0
    for root, directories, files in os.walk(src_path):
        mismatch = _directory_mismatch(root, directories, src_path, dest_path)
        if mismatch is not None:
            return mismatch
        rel = os.path.relpath(root, src_path)
        for fn in files:
            src_file = os.path.join(root, fn)
            rel_name = fn if rel == "." else os.path.join(rel, fn)
            if progress:
                progress(checked, rel_name)
            checked += 1
            if is_merge and fn in FINDER_METADATA_FILES and \
                    not os.path.islink(src_file) and \
                    os.path.isfile(src_file):
                continue
            dst_file = os.path.join(dest_path, rel_name)
            if os.path.islink(src_file):
                if not _matching_independent_symlink(src_file, dst_file):
                    return rel_name
                continue
            if not os.path.lexists(dst_file) or os.path.islink(dst_file):
                return rel_name
            try:
                if os.path.samefile(src_file, dst_file):
                    return rel_name
            except OSError:
                return rel_name
            if not os.path.isfile(dst_file) or \
                    os.path.getsize(src_file) != os.path.getsize(dst_file):
                return rel_name
            if verify_contents:
                # Read fresh bytes: filecmp caches by stat signatures and a
                # pre-copy conflict check may already have populated it.
                with open(src_file, "rb") as source, open(dst_file, "rb") as dest:
                    while chunk := source.read(1024 * 1024):
                        if chunk != dest.read(len(chunk)):
                            return rel_name
                    if dest.read(1):
                        return rel_name
    if progress:
        progress(checked, "")
    return None


def _verifier_would_accept_skip(src_file, dst_file):
    """True iff a same-name entry at ``dst_file`` would be accepted as
    already-present by ``_first_missing_source_file`` (the post-copy
    verifier the merge gates the source delete on). Mirrors that
    verifier's structural predicates — destination must exist, must NOT be
    a symlink for regular source files, must NOT resolve to the same inode
    as the source, and must be a regular file. Source symlinks require an
    independent destination link with the same literal target. The size
    check the verifier also performs is
    deliberately omitted: this backs ``preview_merge``, which is name-only
    by design and must not stat the destination tree's bytes. Same-size
    content collisions are caught separately by ``_find_content_conflict``
    before the merge runs; this only filters entries the verifier would
    reject structurally (symlink, directory, broken samefile probe), where
    rsync ``--ignore-existing`` skips the copy but the verifier then
    refuses to delete the originals with ``Verification failed``.
    """
    if os.path.islink(src_file):
        return _matching_independent_symlink(src_file, dst_file)
    if not os.path.lexists(dst_file) or os.path.islink(dst_file):
        return False
    try:
        if os.path.samefile(src_file, dst_file):
            return False
    except OSError:
        return False
    return os.path.isfile(dst_file)


def preview_merge(src_path, dest_path):
    """Classify how a merge of ``src_path`` into an existing ``dest_path``
    would play out, the same way the ``rsync --ignore-existing`` copy does:
    every source file absent at the destination is copied; every source file
    already present (by name) is left untouched.

    Returns a dict with ``will_copy``, ``will_skip``, ``will_block``, and
    ``source_total`` (their sum). The total counts *every* file under
    ``src_path`` — XMP sidecars and other companions rsync carries along,
    not just tracked photos — so it reflects what actually transfers,
    unlike a tracked-photo count.

    This is a name-only classification, matching what rsync actually copies:
    a same-name destination *file* whose bytes differ still counts as a
    skip here, because rsync would skip it. That genuine collision is
    caught separately by ``_find_content_conflict``, which refuses the
    whole merge before anything is copied — so this preview never reads
    file contents and stays fast on large trees.

    ``will_block`` covers source files whose destination entry is something
    the post-copy verifier (``_first_missing_source_file``) refuses to
    accept as already-present — a symlink, a directory, or a path that
    resolves to the same inode as the source. ``rsync --ignore-existing``
    silently skips those entries by name, but the verifier then rejects
    them and the merge aborts with "Verification failed", so they must not
    be presented to the user as "already present and will be left
    untouched". Surfaced separately so the confirm dialog can warn that
    the merge would not complete instead of implying a no-op resume.

    Directory symlinks under ``src_path`` count as one transfer item each:
    the rsync ``-a`` path and the shutil fallback (see
    ``_copy_tree_with_progress``) both recreate them as symlinks at the
    destination without descending, so omitting them would let the confirm
    dialog say "All 0 files are already present" for a source that is
    actually just a directory symlink, undercounting what the move
    transfers. File and directory symlinks are copied when absent, skipped
    only for independent links with identical target strings, and blocked
    for mismatched destination entries, just like the post-copy verifier.
    """
    will_copy = 0
    will_skip = 0
    will_block = 0
    # os.walk defaults to followlinks=False, so a symlinked subdirectory
    # appears in `dirs` but is not descended into — which is exactly the
    # transfer semantics the merge applies, so each such entry is one item.
    for root, dirs, files in os.walk(src_path):
        rel = os.path.relpath(root, src_path)
        target_dir = dest_path if rel == "." else os.path.join(dest_path, rel)
        unsafe_parent = _unsafe_destination_directory(dest_path, target_dir, allow_missing=True) is not None
        for d in dirs:
            src_sub = os.path.join(root, d)
            if not os.path.islink(src_sub):
                continue
            rel_name = d if rel == "." else os.path.join(rel, d)
            dst_sub = os.path.join(dest_path, rel_name)
            if unsafe_parent:
                will_block += 1
            elif not os.path.lexists(dst_sub):
                will_copy += 1
            elif _matching_independent_symlink(src_sub, dst_sub):
                will_skip += 1
            else:
                will_block += 1
        for fn in files:
            src_file = os.path.join(root, fn)
            # Merges discard Finder-managed regular ``.DS_Store`` files, so
            # they don't count as a transfer here. Every other entry type
            # with that name (symlink to a file, FIFO/socket, ...) is NOT
            # excluded by ``_rsync_finder_metadata_excludes`` — rsync would
            # copy it (or fail verification if the destination twin blocks
            # it). Apply the same regular-non-symlink predicate the exclude
            # helper uses so a source ``.DS_Store`` symlink still surfaces
            # as a copy/skip/block instead of a silent no-op that the merge
            # dialog reports as "0 files".
            if fn in FINDER_METADATA_FILES and not os.path.islink(src_file) \
                    and os.path.isfile(src_file):
                continue
            rel_name = fn if rel == "." else os.path.join(rel, fn)
            dst_file = os.path.join(dest_path, rel_name)
            if unsafe_parent:
                will_block += 1
            elif not os.path.lexists(dst_file):
                will_copy += 1
            elif _verifier_would_accept_skip(src_file, dst_file):
                will_skip += 1
            else:
                will_block += 1
    return {
        "will_copy": will_copy,
        "will_skip": will_skip,
        "will_block": will_block,
        "source_total": will_copy + will_skip + will_block,
    }


def _fresh_copy_mismatch(src_path, transfer_dest, progress=None):
    """Describe how a fresh move's destination differs from its source.

    Fresh move into a destination we created: walk the source and check
    each file has a same-sized counterpart at the destination. Recount
    the source here rather than reusing the pre-copy `total_files` — if
    a file appeared in the source after that upfront count (and rsync
    didn't pick it up), a stale count could spuriously match a naive
    dst_count and the rmtree below would delete the never-copied file.

    A whole-tree file count alone is not sufficient: ``_plan_moved_file_mtimes``
    above stats every catalog photo sequentially and can run for minutes
    on a network mount, so a destination photo stat'd early in that pass
    could be truncated or replaced afterwards while the pass keeps working
    through the rest of the tree. Neither the pass's remaining iterations
    nor a count-only check would notice, and rmtree(src) would then delete
    the intact original. Per-file size verification is the last thing
    that touches the destination before the catalog update, closing the
    window opened by the planning pass.

    Symlinks are matched structurally: rsync -a (and the shutil fallback)
    preserves source symlinks as destination symlinks with the same
    target string; os.path.getsize would follow the link, so lstat sizes
    and readlink targets are compared instead.

    Returns the mismatch description, or None when the copy is complete.
    ``progress`` is called as in ``_first_missing_source_file``.
    """
    src_count = 0
    for root, directories, files in os.walk(src_path):
        mismatch = _directory_mismatch(root, directories, src_path, transfer_dest)
        if mismatch is not None:
            return f"'{mismatch}' directory missing, linked, or different at destination"
        rel = os.path.relpath(root, src_path)
        for fn in files:
            src_file = os.path.join(root, fn)
            rel_name = fn if rel == "." else os.path.join(rel, fn)
            if progress:
                progress(src_count, rel_name)
            src_count += 1
            dst_file = os.path.join(transfer_dest, rel_name)
            if not os.path.lexists(dst_file):
                return f"'{rel_name}' missing at destination"
            src_is_link = os.path.islink(src_file)
            dst_is_link = os.path.islink(dst_file)
            if src_is_link != dst_is_link:
                return (
                    f"'{rel_name}' type mismatch (symlink vs regular file) "
                    f"at destination")
            if src_is_link:
                if not _matching_independent_symlink(src_file, dst_file):
                    return (
                        f"'{rel_name}' symlink target mismatch or source alias at destination")
                continue
            try:
                src_size = os.stat(src_file, follow_symlinks=False).st_size
                dst_size = os.stat(dst_file, follow_symlinks=False).st_size
            except OSError:
                return f"'{rel_name}' unreadable at destination"
            if src_size != dst_size:
                return (
                    f"'{rel_name}' size mismatch at destination "
                    f"(source={src_size}, dest={dst_size})")
    # Extras at destination — a leftover rsync temp file, or anything
    # else the source-driven walk above never looked for — would leave
    # the fresh destination in a state we do not fully understand.
    # Count both sides after the size check so a mismatch that a
    # per-source-file walk catches is not attributed to a stray extra.
    dst_count = sum(1 for _, _, f in os.walk(transfer_dest) for _ in f)
    if src_count != dst_count:
        return f"file count mismatch: source={src_count}, dest={dst_count}"
    if progress:
        progress(src_count, "")
    return None
