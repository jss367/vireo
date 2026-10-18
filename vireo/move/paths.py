"""Destination naming and path aliasing for moves.

``normalize_destination_name`` and ``resolve_folder_dest`` turn a move's
destination parent and optional new name into the landing path. The rest
answers "is this the same directory, or inside it?" across every alias
surface a move has to see through (symlinks, Windows case folding,
case-insensitive POSIX volumes), and finds tracked folders at, below or
above a destination.
"""

import contextlib
import os
import tempfile


def normalize_destination_name(destination_name):
    """Return a safe, single-component folder name for a move destination.

    Folder moves accept a destination *parent* separately from the name of the
    folder that lands inside it.  Keeping the leaf name separate makes rename-
    while-moving explicit and prevents an entered name from escaping the
    selected parent.  Both slash styles are rejected because moves can target
    a POSIX NAS from a Windows client (and vice versa).  Colons are rejected
    too: on Windows ``os.path.join(r"D:\\archive", "C:shoot")`` returns the
    drive-relative ``"C:shoot"``, so accepting a drive-qualified leaf would
    let the entered name escape the selected parent and land the copy — and
    the repointed ``catalog_path`` — outside the chosen destination.

    An empty value means "keep the source folder name" and is returned as an
    empty string for backwards-compatible callers.
    """
    if destination_name is None:
        return ""
    if not isinstance(destination_name, str):
        raise ValueError("Folder name must be a string")
    name = destination_name.strip()
    if not name:
        return ""
    if (
        name in (".", "..")
        or "/" in name
        or "\\" in name
        or ":" in name
        or "\0" in name
    ):
        raise ValueError(
            "Folder name must be a single name without slashes or colons"
        )
    return name


def resolve_folder_dest(folder_path, folder_name, destination,
                        destination_name=""):
    """Compute the final landing path for a folder move.

    The source folder is placed *inside* destination. By default it keeps its
    name (moving /local/birds to /nas/photos yields /nas/photos/birds), while
    ``destination_name`` allows an explicit rename during the move.
    Shared by move_folder() and the preflight route so the resolved path
    is computed in exactly one place.
    """
    name = normalize_destination_name(destination_name) or folder_name \
        or os.path.basename(folder_path.rstrip("/\\"))
    return os.path.join(destination, name)


def _samefile_tristate(a, b):
    """Whether two paths resolve to the same inode, or None if samefile
    raised (broken symlink, permission error, transient race — the probe
    is INCONCLUSIVE, not negative). Callers that need a plain boolean wrap
    with `_samefile_or_false`; callers that have to differentiate
    "definitely different" from "couldn't check" use this directly."""
    try:
        return os.path.samefile(a, b)
    except OSError:
        return None


def _samefile_or_false(a, b):
    result = _samefile_tristate(a, b)
    return False if result is None else result


def _walk_up_paths(p):
    prev = None
    while p and p != prev:
        yield p
        prev = p
        p = os.path.dirname(p)


def _is_case_insensitive_path(path):
    """Whether the filesystem holding `path` treats two case variants as the
    same name (default macOS APFS, Windows).

    Walks up to the deepest existing ancestor of `path`, then creates a
    short-lived probe dir *inside* it with a known case-flippable suffix
    and asks samefile whether the swapped spelling resolves to the same
    inode. The probe name didn't exist a moment ago, so the result reflects
    FS behavior rather than a pre-existing user alias (hard link / symlink
    on a case-sensitive FS), and is conclusive in both directions — True on
    a case-folding FS, raises (and `_samefile_or_false` returns False) on a
    case-sensitive one. Probing inside the ancestor rather than via its own
    basename matters when the deepest existing ancestor is a mount point —
    a case-sensitive APFS volume mounted at `/Volumes/Photos` under default
    case-insensitive macOS HFS+ would otherwise be misread as
    case-insensitive (the basename probe tests `/Volumes`'s handling of
    `Photos`, not what the mounted volume does below it).

    Falls back to scanning existing children for NEGATIVE evidence only
    when the temp probe can't write (read-only ancestor): if any
    letter-bearing child's case-flipped spelling also exists as a DISTINCT
    file (`samefile` == False), the FS is case-sensitive — that can't
    happen on a case-folding FS. `samefile` == True via a pre-existing
    child name is never trusted, since it could be a user-created hard link
    or symlink alias on a case-sensitive FS. A previous version of this
    function scanned children first as an optimization to short-circuit on
    that definitive False, but `move_folder()` reaches this on every move,
    and on the typical case-sensitive destination with no case-twin
    children every per-entry samefile probe is inconclusive (the flipped
    name doesn't exist; samefile raises) — turning a single move into
    O(entries) wasted stats before the temp probe ran anyway.

    Returns False on case-sensitive POSIX (Linux ext4/btrfs, opt-in APFS)
    and when no probe is possible at all (ancestor not a directory, or
    read-only AND no child evidence) — the safe default, since spuriously
    folding case could merge two genuinely distinct paths.
    """
    if os.name == "nt":
        return True
    cur = path
    while cur and not os.path.exists(cur):
        parent = os.path.dirname(cur)
        if parent == cur:
            return False
        cur = parent
    if not cur or not os.path.isdir(cur):
        return False
    try:
        probe = tempfile.mkdtemp(prefix=".vireo_case_probe_", suffix="A",
                                 dir=cur)
    except OSError:
        probe = None
    if probe is not None:
        try:
            flipped = probe[:-1] + probe[-1].swapcase()
            return _samefile_or_false(probe, flipped)
        finally:
            with contextlib.suppress(OSError):
                os.rmdir(probe)
    # Temp probe denied (read-only ancestor). Scan existing children for a
    # definitive False (two distinct case-twin entries — impossible on a
    # case-folding FS). With no temp probe available we have no way to
    # confirm a positive answer, so True via a child name is not trusted
    # and the function returns False if no definitive False is found.
    try:
        entries = os.listdir(cur)
    except OSError:
        return False
    for entry in entries:
        flipped = entry.swapcase()
        if flipped == entry:
            continue
        if _samefile_tristate(
            os.path.join(cur, entry),
            os.path.join(cur, flipped),
        ) is False:
            return False
    return False


def _case_insensitive_root(path):
    """Realpath of the deepest existing ancestor of `path` whose filesystem
    folds case, or None when no such ancestor exists. Used to scope the
    case-folded fallback in `_path_equal_or_descends` to the subtree that
    actually folds — see that function's docstring for why a full-path
    `.lower()` would otherwise collapse genuinely distinct paths on the
    parent (case-sensitive) filesystem.

    Walks the same deepest-existing-ancestor path as the probe inside
    `_is_case_insensitive_path` and delegates to it for the fold check, so
    test monkeypatches of `_is_case_insensitive_path` still take effect.
    On Windows, the boundary is the deepest existing ancestor's drive root.
    """
    if os.name == "nt":
        if not _is_case_insensitive_path(path):
            return None
        cur = path
        while cur and not os.path.exists(cur):
            parent = os.path.dirname(cur)
            if parent == cur:
                return None
            cur = parent
        if not cur:
            return None
        drive, _ = os.path.splitdrive(os.path.realpath(cur))
        return (drive + os.sep) if drive else os.path.realpath(cur)
    if not _is_case_insensitive_path(path):
        return None
    cur = path
    while cur and not os.path.exists(cur):
        parent = os.path.dirname(cur)
        if parent == cur:
            return None
        cur = parent
    if not cur or not os.path.isdir(cur):
        return None
    return os.path.realpath(cur)


# Sentinel for "case-insensitive root not yet probed" — distinct from the
# probed-and-None result (the FS is case-sensitive). Callers inside a loop
# pass the probed value, including a literal None, so lazy re-probing per
# row never happens — the regression `_tracked_destination_overlap` is
# guarding against would otherwise re-run the probe per scanned row on any
# case-sensitive POSIX host (Linux ext4, opt-in APFS).
_UNPROBED_CI_ROOT = object()


def _path_equal_or_descends(candidate, ancestor,
                            case_insensitive_root=_UNPROBED_CI_ROOT):
    """True if `candidate` resolves to the same directory as `ancestor`, or is
    a descendant of it.

    Folds together every directory-alias surface the move guards need:
      - Symlinks: os.path.realpath.
      - Windows case folding: os.path.normcase.
      - Case-insensitive POSIX (default macOS APFS), where the above two are
        not enough — os.path.realpath does not fold case and os.path.normcase
        is a no-op on POSIX, so paths differing only by case string-compare
        unequal even though they resolve to the same inode: os.path.samefile
        (device + inode) is FS-truth on every platform and is used as a
        fallback, including a walk-up ancestor check for the descendant case
        where `candidate` itself doesn't exist yet.
      - Two missing leaves on case-insensitive POSIX, where neither path
        exists yet so samefile has nothing to compare: probe the FS for
        case-insensitivity via the deepest existing ancestor and, if so,
        redo the string compare case-folded — but only over the portion of
        the path actually on the case-folding filesystem.

    `case_insensitive_root`: realpath of the deepest existing case-insensitive
    ancestor of `ancestor`, or None for case-sensitive (no case-folded fallback
    runs). Pass the result of `_case_insensitive_root(ancestor)` if you've
    already computed it (e.g., inside a loop with a fixed ancestor) so the
    probe doesn't re-run per call. Omit (sentinel default) to probe lazily;
    an explicit None means "already probed, no case-insensitive root" and
    skips the lazy probe.

    Scoping the case fold to inside this root matters when only part of the
    path tree folds — a case-insensitive APFS/CIFS volume mounted at
    `/mnt/photos` on a case-sensitive Linux root FS, for example. A stale row
    `/MNT/photos/dst/src` and a move into `/mnt/photos/dst/src` are distinct
    paths because `/MNT` does not resolve to `/mnt` on the parent FS; folding
    the full path with `.lower()` would wrongly refuse the valid move.
    """
    real_c = os.path.normcase(os.path.realpath(candidate))
    real_a = os.path.normcase(os.path.realpath(ancestor))
    if real_c == real_a or real_c.startswith(real_a + os.sep):
        return True

    if os.path.exists(candidate) and os.path.exists(ancestor) \
            and _samefile_or_false(candidate, ancestor):
        return True

    # Containment via case-only alias: walk up candidate's existing ancestors
    # looking for one whose inode matches `ancestor`. Handles the case where
    # the candidate leaf doesn't exist yet but its parent (or an ancestor) is
    # a case-only alias of `ancestor` on a case-insensitive POSIX filesystem.
    if os.path.exists(ancestor):
        for anc in _walk_up_paths(os.path.dirname(candidate)):
            if os.path.exists(anc) and _samefile_or_false(anc, ancestor):
                return True

    # Both leaves missing on a case-insensitive POSIX volume: e.g., a stale
    # folders.path row that differs from the resolved destination only by
    # case before either path has been created on disk. samefile can't fold
    # case for paths that don't exist; probe the FS for case-insensitivity
    # and redo the string compare case-folded so the stale row is still
    # caught before any copy. Restrict the case-folded compare to the subtree
    # below the probed case-insensitive ancestor — anything above it is on
    # the parent (case-sensitive) FS and must match exactly, character-for-
    # character, or we'd collapse distinct paths.
    if case_insensitive_root is _UNPROBED_CI_ROOT:
        case_insensitive_root = _case_insensitive_root(ancestor)
    if case_insensitive_root:
        return _case_folded_descends(real_c, real_a, case_insensitive_root)
    return False


def _case_folded_descends(real_c, real_a, root):
    """``_path_equal_or_descends``'s case-folded compare, scoped to the
    case-insensitive subtree at ``root`` (a realpath)."""
    # `root` may already end with `os.sep` — the filesystem root "/"
    # (deepest existing ancestor is `/` when everything below the
    # destination is missing on a case-insensitive POSIX volume mounted
    # at /), or a Windows drive root like "C:\\". `root + os.sep` would
    # double the boundary to "//" / "C:\\\\" and never match any real
    # path, silently skipping the case-folded compare and letting a
    # stale row like `/photos/src` slip past a move into `/Photos/src`.
    root_with_sep = root if root.endswith(os.sep) else root + os.sep
    if not (real_a == root or real_a.startswith(root_with_sep)):
        return False  # ancestor isn't actually inside the probed root.
    # The case-fold root can appear under a case-only alias in the
    # candidate (stale row `/Photos/DST/src` against probed root
    # `/Photos/dst`). Match the root prefix case-insensitively, then
    # confirm via samefile that the candidate's variant is the same
    # on-disk directory — that distinguishes a real case-fold alias
    # from a distinct path on a case-sensitive parent FS (e.g. `/mnt`
    # vs `/MNT` mount-point pair on Linux), where the case-only twin
    # of the root doesn't exist and samefile raises.
    real_c_low = real_c.lower()
    root_low = root.lower()
    root_low_with_sep = root_with_sep.lower()
    if real_c_low != root_low \
            and not real_c_low.startswith(root_low_with_sep):
        return False  # candidate is above or beside the case-fold subtree.
    candidate_root = real_c[:len(root)]
    if candidate_root != root \
            and not _samefile_or_false(candidate_root, root):
        return False  # case-variant of the root is distinct on the parent FS.
    suffix_c = real_c[len(root):].lower()
    suffix_a = real_a[len(root):].lower()
    # suffix_a == "" means real_a IS the root itself; we've already
    # established real_c is at or under root, so real_c descends from
    # real_a unconditionally. Without this, a root like `/` strips
    # to "" for the ancestor while leaving "photos/src" for the
    # candidate — and `suffix_c.startswith("" + os.sep)` is False
    # because the leading separator was already consumed by the root.
    return (suffix_a == "" or suffix_c == suffix_a
            or suffix_c.startswith(suffix_a + os.sep))


def _destination_overlaps_source(src_path, dest_path):
    """True if dest_path equals src_path or one is a descendant of the other.

    The post-copy rmtree(src_path) would delete the only copy of the files if
    dest and src refer to the same on-disk directory, so this is checked
    before any copy. See `_path_equal_or_descends` for the alias surface.
    """
    return (_path_equal_or_descends(dest_path, src_path)
            or _path_equal_or_descends(src_path, dest_path))


def _tracked_destination_overlap(db, folder_id, dest_path):
    """Return another tracked folder at or below dest_path, if one exists.

    When both an exact-match row (a tracked folder alias-equal to
    ``dest_path``) AND a strict-descendant row exist, the exact-match row
    is returned. Otherwise the caller's exact-vs-descendant branch would
    fire non-deterministically based on the arbitrary order SQLite happened
    to return the rows in (e.g. ``/Photos/USA`` inserted before its later-
    scanned parent ``/Photos``): selecting the exact tracked parent would
    get rejected as the unsupported "wrap around a tracked subfolder" case
    just because the child row was seen first.

    The case-insensitive root of `dest_path` is probed once and reused for
    every row — otherwise the probe (an os.listdir of the deepest existing
    ancestor, plus samefile of two child paths) re-runs per non-matching row,
    making this O(tracked_folders × destination_entries) before any copy on
    large catalogs or network-backed destinations.
    """
    dest_ci_root = _case_insensitive_root(dest_path)
    descendant = None
    for row in db.moves_merge.folders_except(folder_id):
        if _path_equal_or_descends(
            row["path"], dest_path,
            case_insensitive_root=dest_ci_root,
        ):
            # Prefer an exact (alias-folded) match: ``row["path"]`` is
            # at-or-below ``dest_path``, so if ``dest_path`` is ALSO
            # at-or-below ``row["path"]`` the two paths alias-fold to the
            # same directory and this row IS the destination the caller
            # can accept as a merge. Otherwise the row is a strict
            # descendant (the "wrap around" case); remember it as a
            # fallback in case no exact-match row turns up later.
            if _path_equal_or_descends(dest_path, row["path"]):
                return row
            if descendant is None:
                descendant = row
    return descendant


def _rebase_under_stored_ancestor(catalog_path, ancestor_path):
    """Return ``catalog_path`` with any alias-prefix folded to STORED
    ``ancestor_path``.

    ``catalog_path`` is at or below ``ancestor_path`` per
    ``_tracked_destination_ancestor``, but the match may have been via
    ``_path_equal_or_descends``' alias surface (symlink resolution, Windows
    case-fold via ``normcase``, POSIX case-insensitive ``samefile``). In those
    cases ``catalog_path``'s leading components differ character-for-character
    from ``ancestor_path`` even though they resolve to the same on-disk
    directory. ``merge_staged_tree_into_archive`` reads parent rows by exact
    ``WHERE path = ?`` — an alias-prefixed base would miss the tracked rows and
    create a parallel alias-path row set outside the managed archive tree
    (with ``parent_id=NULL``), so re-root the base on the stored form here.

    The exact-overlap branch of ``move_folder`` passes ``tracked["path"]`` as
    the reconcile base directly (there IS no suffix — the destination IS the
    tracked folder); this helper is for the ancestor case where a real
    relative suffix has to be reattached below the stored ancestor.
    """
    if catalog_path == ancestor_path:
        return ancestor_path
    prefix = ancestor_path + os.sep
    if catalog_path.startswith(prefix):
        return catalog_path
    # Symlink alias: realpath collapses the link. On POSIX ``normcase`` is a
    # no-op so the raw realpath is enough; on Windows ``normcase`` also
    # folds case so the ``normcase(realpath(...))`` compare handles both.
    real_ancestor = os.path.realpath(ancestor_path)
    real_catalog = os.path.realpath(catalog_path)
    if real_catalog == real_ancestor:
        return ancestor_path
    real_prefix = real_ancestor + os.sep
    if real_catalog.startswith(real_prefix):
        return ancestor_path + real_catalog[len(real_ancestor):]
    norm_ancestor = os.path.normcase(real_ancestor)
    norm_catalog = os.path.normcase(real_catalog)
    if norm_catalog == norm_ancestor:
        return ancestor_path
    if norm_catalog.startswith(os.path.normcase(real_prefix)):
        return ancestor_path + real_catalog[len(real_ancestor):]
    # Case-only POSIX alias: neither ``realpath`` nor ``normcase`` folds it,
    # so walk up ``catalog_path`` looking for an existing ancestor whose inode
    # matches ``ancestor_path`` — the same samefile fallback
    # ``_path_equal_or_descends`` uses to accept the destination in the first
    # place. Then join the remaining components onto ``ancestor_path``.
    parts = []
    cur = catalog_path
    while cur:
        parent = os.path.dirname(cur)
        if parent == cur:
            break
        if os.path.exists(cur) and _samefile_or_false(cur, ancestor_path):
            if not parts:
                return ancestor_path
            return os.path.join(ancestor_path, *parts)
        parts.insert(0, os.path.basename(cur))
        cur = parent
    # No alias match after all. ``_tracked_destination_ancestor`` already
    # matched via one of the surfaces above, so this shouldn't happen — if
    # something in the alias surface changes and the walk falls through,
    # keep today's behaviour (catalog_path unchanged) rather than silently
    # corrupting the reconcile base.
    return catalog_path


def _tracked_destination_ancestor(db, folder_id, dest_path):
    """Return a tracked folder that is an ancestor of dest_path, if any.

    The complement of ``_tracked_destination_overlap``: that one catches
    tracked folders AT or BELOW ``dest_path``; this one catches tracked
    folders ABOVE it on the path tree.

    The pipeline's local-processing archive step uses ``db.move_folder_path``
    to repoint the catalog after rsync, but ``move_folder_path`` only rewrites
    the moved folder's own path (and cascades to its tracked children). It
    does NOT reparent the moved row under a tracked ancestor of the new path.
    If the user picks an archive destination inside an already-tracked root
    (catalog manages ``/Photos`` and they pick ``/Photos/NewShoot``), the
    archive move succeeds on disk but leaves the catalog with two unrelated
    workspace roots whose path strings overlap — a permanently confusing
    folder tree that breaks future scans of the ancestor root. Reject
    upfront so the user picks a different archive folder before the pipeline
    spends time staging and processing.

    Passes ``case_insensitive_root=None`` to skip the per-row case-fold
    probe; the realpath/normcase comparison at the top of
    ``_path_equal_or_descends`` already catches every same-case ancestor,
    which is the only practical case for archive destinations (the user is
    typing a fresh subfolder name into a UI, not chasing a stale catalog row
    that differs only by case).
    """
    for row in db.moves_merge.folders_except(folder_id):
        if _path_equal_or_descends(
            dest_path, row["path"],
            case_insensitive_root=None,
        ):
            return row
    return None
