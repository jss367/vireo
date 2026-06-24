"""Tests for photo move operations."""

import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pytest
from db import Database


def _tmp_folds_case():
    """True if the filesystem holding pytest's temp dir folds case (macOS
    APFS default, Windows NTFS). Some tests below build scenarios where two
    case variants of an ancestor path must resolve to DISTINCT on-disk
    directories — only possible on a case-sensitive parent FS. On a folding
    host the two variants are the same directory and the assertion under
    test can't hold; skip rather than miscompare.
    """
    with tempfile.TemporaryDirectory(suffix="A") as d:
        flipped = d[:-1] + "a"
        if flipped == d:
            return False
        try:
            return os.path.samefile(d, flipped)
        except OSError:
            return False


_TMP_FOLDS_CASE = _tmp_folds_case()


@pytest.fixture
def move_env(tmp_path):
    """Set up a DB with two folders and photos on disk."""
    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)

    src = tmp_path / "src"
    src.mkdir()
    dst = tmp_path / "dst"
    dst.mkdir()

    fid_src = db.add_folder(str(src), name="src")
    fid_dst = db.add_folder(str(dst), name="dst")

    # Create real files
    (src / "bird1.jpg").write_bytes(b"\xff\xd8" + b"\x00" * 100)
    (src / "bird1.xmp").write_text("<xmp/>")
    (src / "bird2.jpg").write_bytes(b"\xff\xd8" + b"\x00" * 200)

    p1 = db.add_photo(folder_id=fid_src, filename="bird1.jpg", extension=".jpg",
                       file_size=102, file_mtime=1.0)
    p2 = db.add_photo(folder_id=fid_src, filename="bird2.jpg", extension=".jpg",
                       file_size=202, file_mtime=2.0)

    return {
        "db": db, "tmp_path": tmp_path,
        "src": src, "dst": dst,
        "fid_src": fid_src, "fid_dst": fid_dst,
        "p1": p1, "p2": p2,
    }


def test_move_photos_copies_and_deletes(move_env):
    """move_photos copies files to destination and removes originals."""
    from move import move_photos

    env = move_env
    result = move_photos(
        db=env["db"],
        photo_ids=[env["p1"], env["p2"]],
        destination=str(env["dst"]),
    )
    assert result["moved"] == 2
    assert result["errors"] == []
    # Files exist at destination
    assert (env["dst"] / "bird1.jpg").exists()
    assert (env["dst"] / "bird2.jpg").exists()
    # XMP sidecar also moved
    assert (env["dst"] / "bird1.xmp").exists()
    # Originals removed
    assert not (env["src"] / "bird1.jpg").exists()
    assert not (env["src"] / "bird2.jpg").exists()
    assert not (env["src"] / "bird1.xmp").exists()


def test_move_photos_updates_db(move_env):
    """move_photos updates folder_id in the database."""
    from move import move_photos

    env = move_env
    move_photos(db=env["db"], photo_ids=[env["p1"]], destination=str(env["dst"]))
    photo = env["db"].get_photo(env["p1"])
    assert photo["folder_id"] == env["fid_dst"]


def test_move_photos_collision_skips(move_env):
    """move_photos reports collision and skips conflicting files."""
    from move import move_photos

    env = move_env
    # Pre-place a file at destination
    (env["dst"] / "bird1.jpg").write_bytes(b"existing")

    result = move_photos(db=env["db"], photo_ids=[env["p1"]], destination=str(env["dst"]))
    assert result["moved"] == 0
    assert len(result["errors"]) == 1
    assert "bird1.jpg" in result["errors"][0]
    # Original still exists
    assert (env["src"] / "bird1.jpg").exists()


def test_move_photos_creates_dest_folder_record(move_env):
    """move_photos creates a new folder DB record if destination folder is new."""
    from move import move_photos

    env = move_env
    new_dst = env["tmp_path"] / "new_dest"
    new_dst.mkdir()

    move_photos(db=env["db"], photo_ids=[env["p1"]], destination=str(new_dst))
    row = env["db"].conn.execute("SELECT id FROM folders WHERE path = ?", (str(new_dst),)).fetchone()
    assert row is not None


def test_move_photos_companion_files(move_env):
    """move_photos moves companion (RAW) files alongside the photo."""
    from move import move_photos

    env = move_env
    # Set companion_path on p1
    env["db"].conn.execute("UPDATE photos SET companion_path = 'bird1.nef' WHERE id = ?", (env["p1"],))
    env["db"].conn.commit()
    (env["src"] / "bird1.nef").write_bytes(b"\x00" * 50)

    move_photos(db=env["db"], photo_ids=[env["p1"]], destination=str(env["dst"]))
    assert (env["dst"] / "bird1.nef").exists()
    assert not (env["src"] / "bird1.nef").exists()


def test_move_folder_copies_tree(move_env):
    """move_folder moves entire folder tree preserving structure."""
    from move import move_folder

    env = move_env
    # Create subfolder with file
    sub = env["src"] / "sub"
    sub.mkdir()
    (sub / "nest.jpg").write_bytes(b"\xff\xd8" + b"\x00" * 50)
    fid_sub = env["db"].add_folder(str(sub), name="sub", parent_id=env["fid_src"])
    env["db"].add_photo(folder_id=fid_sub, filename="nest.jpg", extension=".jpg",
                        file_size=52, file_mtime=3.0)

    result = move_folder(db=env["db"], folder_id=env["fid_src"], destination=str(env["dst"]))
    assert result["moved"] >= 2  # at least bird1 + bird2 from src
    # Subfolder structure preserved
    assert (env["dst"] / "src" / "sub" / "nest.jpg").exists()
    # DB paths updated
    folder = env["db"].conn.execute("SELECT path FROM folders WHERE id = ?", (env["fid_src"],)).fetchone()
    assert folder["path"] == str(env["dst"] / "src")


def test_move_folder_refuses_existing_dest_without_merge(move_env):
    """move_folder refuses an existing destination and preserves originals."""
    from move import move_folder

    env = move_env
    # Pre-create the resolved landing path (dst/src)
    landing = env["dst"] / "src"
    landing.mkdir()

    result = move_folder(db=env["db"], folder_id=env["fid_src"], destination=str(env["dst"]))
    assert result["moved"] == 0
    assert result.get("needs_merge") is True
    assert any("already exists" in e for e in result["errors"])
    # Originals untouched
    assert (env["src"] / "bird1.jpg").exists()
    assert (env["src"] / "bird2.jpg").exists()


def test_move_folder_merge_resumes(move_env):
    """merge=True copies only missing files into an existing destination,
    verifies, then removes originals and updates the DB path."""
    from move import move_folder

    env = move_env
    landing = env["dst"] / "src"
    landing.mkdir()
    # Simulate a partially-completed prior move: bird1 already copied.
    (landing / "bird1.jpg").write_bytes((env["src"] / "bird1.jpg").read_bytes())
    # And a leftover rsync temp file from the interrupted run.
    (landing / ".bird2.jpg.AbCdEf").write_bytes(b"partial")

    result = move_folder(
        db=env["db"], folder_id=env["fid_src"], destination=str(env["dst"]), merge=True
    )
    assert result["errors"] == []
    # Missing file got copied; both now present at destination
    assert (landing / "bird1.jpg").exists()
    assert (landing / "bird2.jpg").exists()
    # Originals removed
    assert not env["src"].exists()
    # DB path updated to the merged location
    folder = env["db"].conn.execute(
        "SELECT path FROM folders WHERE id = ?", (env["fid_src"],)
    ).fetchone()
    assert folder["path"] == str(landing)


def test_move_folder_merge_never_overwrites_differing_dest_file(move_env):
    """A same-name destination file with DIFFERENT content (different size)
    is a hard conflict: the merge aborts before copying, leaving both the
    destination file and the originals untouched."""
    from move import move_folder

    env = move_env
    landing = env["dst"] / "src"
    landing.mkdir()
    # User's own file sharing bird1's name but with different content/size.
    (landing / "bird1.jpg").write_bytes(b"USER DATA - do not clobber")

    result = move_folder(
        db=env["db"], folder_id=env["fid_src"], destination=str(env["dst"]), merge=True
    )
    assert result["moved"] == 0
    assert any("Conflict" in e for e in result["errors"])
    # Pre-existing destination file untouched, originals preserved.
    assert (landing / "bird1.jpg").read_bytes() == b"USER DATA - do not clobber"
    assert (env["src"] / "bird1.jpg").exists()


def test_move_folder_merge_detects_same_size_different_content(move_env):
    """The dangerous case: a destination file with the SAME size but different
    bytes must still be detected as a conflict (size alone is insufficient),
    so the source is never deleted in favor of the wrong destination bytes."""
    from move import move_folder

    env = move_env
    src_bytes = (env["src"] / "bird1.jpg").read_bytes()
    landing = env["dst"] / "src"
    landing.mkdir()
    # Same length as the source, different content.
    decoy = bytes((b + 1) % 256 for b in src_bytes)
    assert len(decoy) == len(src_bytes) and decoy != src_bytes
    (landing / "bird1.jpg").write_bytes(decoy)

    result = move_folder(
        db=env["db"], folder_id=env["fid_src"], destination=str(env["dst"]), merge=True
    )
    assert result["moved"] == 0
    assert any("Conflict" in e for e in result["errors"])
    # Destination decoy untouched, source preserved (no silent data loss).
    assert (landing / "bird1.jpg").read_bytes() == decoy
    assert (env["src"] / "bird1.jpg").exists()


def test_move_folder_merge_refuses_tracked_destination(move_env):
    """Merging into a destination Vireo already tracks as a folder is refused
    (a correct tracked-tree merge is out of scope and would dangle descendant
    paths). Originals are preserved."""
    from move import move_folder

    env = move_env
    landing = env["dst"] / "src"
    landing.mkdir()
    for fn in ("bird1.jpg", "bird1.xmp", "bird2.jpg"):
        (landing / fn).write_bytes((env["src"] / fn).read_bytes())
    # Destination already exists as its own folder row in the DB.
    env["db"].add_folder(str(landing), name="src")

    result = move_folder(
        db=env["db"], folder_id=env["fid_src"], destination=str(env["dst"]), merge=True
    )
    assert result["moved"] == 0
    assert any("already manage" in e for e in result["errors"])
    # Source intact.
    assert (env["src"] / "bird1.jpg").exists()


def test_move_folder_merge_refuses_tracked_descendant(move_env):
    """A tracked folder *below* the (untracked) destination must also block the
    merge — its path would collide when the source's children cascade onto it."""
    from move import move_folder

    env = move_env
    landing = env["dst"] / "src"
    (landing / "sub").mkdir(parents=True)
    # A tracked folder below the destination root.
    env["db"].add_folder(str(landing / "sub"), name="sub")

    result = move_folder(
        db=env["db"], folder_id=env["fid_src"], destination=str(env["dst"]), merge=True
    )
    assert result["moved"] == 0
    assert any("already manage" in e for e in result["errors"])
    assert (env["src"] / "bird1.jpg").exists()


def test_move_folder_refuses_missing_tracked_destination_before_copy(move_env):
    """A stale tracked destination row must block the move even when the
    resolved destination does not currently exist on disk."""
    from move import move_folder

    env = move_env
    landing = env["dst"] / "src"
    assert not landing.exists()
    env["db"].add_folder(str(landing), name="src")

    result = move_folder(
        db=env["db"], folder_id=env["fid_src"], destination=str(env["dst"])
    )

    assert result["moved"] == 0
    assert any("already manage" in e for e in result["errors"])
    assert (env["src"] / "bird1.jpg").exists()
    assert not landing.exists()


def test_move_folder_refuses_missing_tracked_descendant_before_copy(move_env):
    """A stale tracked descendant row would collide when source children
    cascade to the destination path, so it must block before any copy."""
    from move import move_folder

    env = move_env
    source_child = env["src"] / "sub"
    source_child.mkdir()
    env["db"].add_folder(str(source_child), name="sub")

    landing = env["dst"] / "src"
    tracked_child = landing / "sub"
    assert not landing.exists()
    env["db"].add_folder(str(tracked_child), name="sub")

    result = move_folder(
        db=env["db"], folder_id=env["fid_src"], destination=str(env["dst"])
    )

    assert result["moved"] == 0
    assert any("already manage" in e for e in result["errors"])
    assert (env["src"] / "bird1.jpg").exists()
    assert source_child.exists()
    assert not landing.exists()


def test_move_folder_merge_refuses_symlinked_tracked_destination(move_env):
    """A tracked folder reached via a symlink alias must be detected by the
    tracked-destination check (canonical realpath compare), not slip past a
    raw-string match."""
    from move import move_folder

    env = move_env
    real_dst = env["tmp_path"] / "realdst"
    landing = real_dst / "src"
    landing.mkdir(parents=True)
    env["db"].add_folder(str(landing), name="src")  # tracked at its real path

    link = env["tmp_path"] / "dstlink"
    try:
        os.symlink(str(real_dst), str(link))
    except (OSError, NotImplementedError):
        pytest.skip("symlinks not supported on this platform")

    # Destination via the alias resolves (realpath) to the tracked landing.
    result = move_folder(
        db=env["db"], folder_id=env["fid_src"], destination=str(link), merge=True
    )
    assert result["moved"] == 0
    assert any("already manage" in e for e in result["errors"])
    assert (env["src"] / "bird1.jpg").exists()


def test_move_folder_merge_refuses_symlinked_self_destination(move_env):
    """A destination that is a symlink alias of the source's parent resolves
    (via realpath) to the source itself and must be refused, not no-op-copied
    then deleted."""
    from move import move_folder

    env = move_env
    link = env["tmp_path"] / "alias"
    try:
        os.symlink(str(env["src"].parent), str(link))
    except (OSError, NotImplementedError):
        pytest.skip("symlinks not supported on this platform")

    # destination = alias → resolved dest == alias/src, realpath == src path
    result = move_folder(
        db=env["db"], folder_id=env["fid_src"], destination=str(link), merge=True
    )
    assert result["moved"] == 0
    assert any("overlaps the source" in e for e in result["errors"])
    assert (env["src"] / "bird1.jpg").exists()


def test_move_folder_merge_refuses_case_alias_self_destination(move_env, monkeypatch):
    """On a case-insensitive POSIX filesystem (default macOS APFS),
    os.path.normcase is a no-op and os.path.realpath does not fold case, so
    a destination that differs from the source only by case still resolves
    to the same inode but string-compares unequal. The overlap guard must
    fall back to os.path.samefile (device + inode) so the merge is refused
    before shutil.rmtree deletes the only copy of the source files.

    Simulated on Linux by:
      1. Using a symlink so two distinct path strings share an inode.
      2. Patching realpath/normcase to no-ops so the existing string-based
         check doesn't pre-empt the samefile fallback we want to exercise.
    """
    from move import move_folder

    env = move_env
    alias_parent = env["tmp_path"] / "alias_parent"
    try:
        os.symlink(str(env["src"].parent), str(alias_parent))
    except (OSError, NotImplementedError):
        pytest.skip("symlinks not supported on this platform")

    monkeypatch.setattr(os.path, "realpath", lambda p: p)
    monkeypatch.setattr(os.path, "normcase", lambda p: p)

    # destination=alias_parent → resolved dest = alias_parent/src; samefile
    # against env["src"] returns True because the symlink shares the inode.
    result = move_folder(
        db=env["db"], folder_id=env["fid_src"], destination=str(alias_parent), merge=True
    )
    assert result["moved"] == 0
    assert any("overlaps the source" in e for e in result["errors"])
    assert (env["src"] / "bird1.jpg").exists()
    assert (env["src"] / "bird2.jpg").exists()


def test_move_folder_merge_refuses_case_alias_tracked_destination(move_env, monkeypatch):
    """On a case-insensitive POSIX filesystem (default macOS APFS), a tracked
    folder reached via a case-only alias must also be refused. realpath +
    normcase don't fold case there, so the tracked-destination string compare
    misses; the samefile fallback in `_path_equal_or_descends` catches it and
    keeps two folder rows from managing the same on-disk tree.

    Simulated on Linux the same way as the self-destination case-alias test:
      1. Symlink so two distinct path strings share an inode.
      2. realpath/normcase patched to no-ops so the string-based check doesn't
         pre-empt the samefile fallback we want to exercise.
    """
    from move import move_folder

    env = move_env
    real_dst = env["tmp_path"] / "realdst"
    landing = real_dst / "src"
    landing.mkdir(parents=True)
    # Pre-populate landing so dest_exists is True and the tracked check runs.
    (landing / "bird1.jpg").write_bytes((env["src"] / "bird1.jpg").read_bytes())
    env["db"].add_folder(str(landing), name="src")  # tracked at the real path

    alias_dst = env["tmp_path"] / "alias_dst"
    try:
        os.symlink(str(real_dst), str(alias_dst))
    except (OSError, NotImplementedError):
        pytest.skip("symlinks not supported on this platform")

    monkeypatch.setattr(os.path, "realpath", lambda p: p)
    monkeypatch.setattr(os.path, "normcase", lambda p: p)

    # destination=alias_dst → resolved dest = alias_dst/src; string-compares
    # unequal to the tracked landing path, but samefile makes them collapse.
    result = move_folder(
        db=env["db"], folder_id=env["fid_src"], destination=str(alias_dst), merge=True
    )
    assert result["moved"] == 0
    assert any("already manage" in e for e in result["errors"])
    # Source intact.
    assert (env["src"] / "bird1.jpg").exists()
    assert (env["src"] / "bird2.jpg").exists()


def test_move_folder_refuses_case_alias_missing_tracked_destination(move_env, monkeypatch):
    """On a case-insensitive POSIX filesystem (default macOS APFS), a stale
    tracked folder row whose path differs from the resolved destination only
    by case must still be refused even when both leaves are missing on disk.
    realpath/normcase don't fold case there and samefile has nothing to
    compare across two non-existing paths, so without the FS case-insensitivity
    probe, _path_equal_or_descends would let the move copy first and leave two
    folder rows managing the same on-disk tree.

    Simulated on Linux by patching the case-insensitivity probe to True so the
    case-folded fallback runs without requiring an actual case-insensitive FS.
    """
    import move as move_mod

    env = move_env
    # User asks to move src into a missing destination whose path differs only
    # by case from a stale tracked DB row. Both parent and leaf are absent so
    # samefile has nothing to compare, even on a case-insensitive filesystem.
    dest_input = env["tmp_path"] / "missing_dest"
    assert not dest_input.exists()
    stale_parent = env["tmp_path"] / "MISSING_DEST"
    assert not stale_parent.exists()
    # On a case-insensitive FS this is the same directory as the resolved
    # landing /tmp_path/missing_dest/src, but neither path exists on disk.
    stale_landing = stale_parent / "src"
    assert not stale_landing.exists()
    env["db"].add_folder(str(stale_landing), name="src")

    monkeypatch.setattr(move_mod, "_is_case_insensitive_path", lambda p: True)

    result = move_mod.move_folder(
        db=env["db"], folder_id=env["fid_src"], destination=str(dest_input)
    )
    assert result["moved"] == 0
    assert any("already manage" in e for e in result["errors"])
    # Source untouched, no copy started.
    assert (env["src"] / "bird1.jpg").exists()
    assert not stale_landing.exists()
    assert not stale_parent.exists()
    assert not dest_input.exists()


@pytest.mark.skipif(
    _TMP_FOLDS_CASE,
    reason="Scenario requires a case-sensitive parent FS so the "
           "case-flipped ancestor names resolve to distinct directories.",
)
def test_path_equal_or_descends_case_fold_scoped_to_ci_root(tmp_path):
    """The case-folded fallback in `_path_equal_or_descends` must only fold
    the suffix below the probed case-insensitive root — not the whole path.
    Otherwise a tracked row whose path differs from the resolved destination
    in a component ABOVE the case-folding subtree (a case-insensitive APFS
    or CIFS volume mounted at /mnt/photos under a case-sensitive Linux root
    FS: stale row /MNT/photos/dst/src vs move into /mnt/photos/dst/src) would
    falsely collapse as overlap and refuse a valid move, even though `/MNT`
    is a distinct directory from `/mnt` on the parent (case-sensitive) FS.
    Case-only differences BELOW the root must still collapse."""
    import move as move_mod

    ci_root = tmp_path / "ci_root"
    ci_root.mkdir()
    ci_root_real = os.path.realpath(str(ci_root))
    dest = str(ci_root / "dst" / "src")

    # Above the root — a case-flipped component on the case-sensitive parent
    # FS is a genuinely distinct path. Must NOT register as overlap.
    above_root = str(tmp_path / "CI_ROOT" / "dst" / "src")
    assert move_mod._path_equal_or_descends(
        above_root, dest, case_insensitive_root=ci_root_real
    ) is False

    # Below the root — case-only difference on the case-folding FS aliases
    # to the same on-disk directory. Must register as overlap.
    below_root = str(ci_root / "DST" / "src")
    assert move_mod._path_equal_or_descends(
        below_root, dest, case_insensitive_root=ci_root_real
    ) is True

    # Sibling of the root on the parent FS — distinct path, must NOT
    # register even when both paths happen to case-fold to the same string
    # when lowercased in full.
    sibling = str(tmp_path / "ci_root2" / "dst" / "src")
    assert move_mod._path_equal_or_descends(
        sibling, dest, case_insensitive_root=ci_root_real
    ) is False


@pytest.mark.skipif(
    _TMP_FOLDS_CASE,
    reason="Sibling-root assertion requires a case-sensitive parent FS so "
           "the case-flipped root spelling is a distinct on-disk directory.",
)
def test_path_equal_or_descends_folds_root_case_alias_in_candidate(
    tmp_path, monkeypatch,
):
    """The probed case-insensitive root itself can be spelled differently
    by case in a stale candidate row (e.g. row `/Photos/DST/src` against
    move target `/Photos/dst/src`, where `/Photos/dst` is the deepest
    existing case-folding ancestor). The literal `startswith(root)` check
    rejected such a row before the suffix was folded, so
    `_tracked_destination_overlap` missed the stale row and a move could
    leave two folder rows managing the same on-disk tree. The fix matches
    the root prefix case-insensitively and confirms via samefile that the
    candidate's variant is the same on-disk directory."""
    import move as move_mod

    base = tmp_path / "Photos"
    base.mkdir()
    root_dir = base / "dst"
    root_dir.mkdir()
    root_real = os.path.realpath(str(root_dir))

    # Move target's missing leaf under the existing case-folding root.
    dest = str(root_dir / "src")
    # Stale tracked row whose root-level segment is a case-only alias of
    # `root_dir`. Missing on disk on the case-sensitive Linux CI host.
    stale = str(base / "DST" / "src")

    real_samefile = move_mod._samefile_or_false

    def fake_samefile(a, b):
        # Mimic case-folding FS at `base`: two children of `base` whose
        # basenames differ only by case resolve to the same inode, even
        # when one of them doesn't actually exist on the underlying
        # case-sensitive host FS. Fall through to the real probe for
        # everything else so unrelated paths still behave normally.
        if (os.path.dirname(a) == str(base) and os.path.dirname(b) == str(base)
                and os.path.basename(a).lower() == os.path.basename(b).lower()):
            return True
        return real_samefile(a, b)

    monkeypatch.setattr(move_mod, "_samefile_or_false", fake_samefile)

    assert move_mod._path_equal_or_descends(
        stale, dest, case_insensitive_root=root_real
    ) is True

    # Sanity: an ABOVE-root case variant (parent FS case-sensitive) must
    # still be rejected — the samefile check on the candidate's root-level
    # prefix is what distinguishes the two scenarios.
    sibling_root = str(tmp_path / "PHOTOS" / "dst" / "src")
    assert move_mod._path_equal_or_descends(
        sibling_root, dest, case_insensitive_root=root_real
    ) is False


@pytest.mark.skipif(
    os.name == "nt",
    reason="POSIX filesystem-root scenario; Windows uses drive-rooted paths",
)
def test_path_equal_or_descends_handles_case_fold_root_at_filesystem_root():
    """When the probed case-insensitive root is the filesystem root itself
    (a writable case-insensitive POSIX volume mounted at `/`, or any
    destination whose first missing component sits directly under `/`),
    `root + os.sep` doubles to "//" and no real path starts with it.
    Without the trailing-separator handling, the case-folded compare is
    silently skipped and a stale tracked row like `/photos/src` slips past
    `_tracked_destination_overlap` against a move into `/Photos/src` —
    leaving two folder rows managing the same on-disk tree once the copy
    starts.
    """
    import move as move_mod

    # Both paths missing on disk (typical stale-row + fresh-move scenario).
    # The existence-based samefile checks early-return None for missing
    # paths, so the case-folded suffix compare is the only path that
    # catches this — with case_insensitive_root="/", the root prefix is the
    # bare separator and the suffix strip leaves the full path minus "/".
    assert move_mod._path_equal_or_descends(
        "/__vireo_missing_for_test/PHOTOS/src",
        "/__vireo_missing_for_test/photos/src",
        case_insensitive_root="/",
    ) is True

    # Equal paths under the root (canonical match through the case-folded
    # fallback when both leaves are missing).
    assert move_mod._path_equal_or_descends(
        "/__vireo_missing_for_test/photos/src",
        "/__vireo_missing_for_test/photos/src",
        case_insensitive_root="/",
    ) is True

    # Descendant of a case-aliased ancestor — the candidate's longer suffix
    # must still match the ancestor's via the prefix compare.
    assert move_mod._path_equal_or_descends(
        "/__vireo_missing_for_test/Photos/dst/inner",
        "/__vireo_missing_for_test/photos/dst",
        case_insensitive_root="/",
    ) is True

    # Genuinely distinct paths (not just case-different) still return False
    # — the case-fold-at-root edge must not cause over-folding.
    assert move_mod._path_equal_or_descends(
        "/__vireo_missing_for_test/other/src",
        "/__vireo_missing_for_test/photos/src",
        case_insensitive_root="/",
    ) is False


@pytest.mark.skipif(
    _TMP_FOLDS_CASE,
    reason="Scenario requires a case-sensitive parent FS so the "
           "above-root case-flipped row is a distinct on-disk path.",
)
def test_tracked_destination_overlap_skips_rows_outside_ci_root(move_env, monkeypatch):
    """End-to-end: `_tracked_destination_overlap` must NOT return a stale
    row whose path differs from the destination above the case-insensitive
    boundary. Before the fix, the full-path `.lower()` fallback collapsed
    /MNT/photos/dst/src with /mnt/photos/dst/src on a Linux box with a
    case-insensitive subvolume mounted at /mnt/photos and refused valid
    moves into that subvolume as "already managed"."""
    import move as move_mod

    env = move_env
    ci_root = env["tmp_path"] / "ci_root"
    ci_root.mkdir()
    dest = str(ci_root / "dst" / "src")
    assert not os.path.exists(dest)

    above_root = str(env["tmp_path"] / "CI_ROOT" / "dst" / "src")
    env["db"].add_folder(above_root, name="src")

    # Force the probe True so the case-fold branch is exercised regardless
    # of the host FS (CI tends to be case-sensitive ext4).
    monkeypatch.setattr(move_mod, "_is_case_insensitive_path", lambda p: True)

    assert move_mod._tracked_destination_overlap(
        env["db"], env["fid_src"], dest
    ) is None


def test_tracked_destination_overlap_caches_case_insensitivity_probe(move_env, monkeypatch):
    """The FS case-insensitivity probe is os.listdir-backed and re-running it
    per tracked-folder row turns the preflight guard into
    O(tracked_folders × destination_entries) before any copy starts. The
    overlap check must compute it once for the resolved destination and
    reuse that result for every row it scans.
    """
    import move as move_mod

    env = move_env
    # Many unrelated tracked rows. Each forces the missing-leaves fallback
    # in _path_equal_or_descends (realpath compare unequal, both leaves
    # missing or ancestor missing), which is the branch that calls the
    # probe — so without caching, the probe fires once per row.
    dest_input = env["tmp_path"] / "missing_dest_cache_probe"
    assert not dest_input.exists()
    for i in range(8):
        env["db"].add_folder(
            str(env["tmp_path"] / f"unrelated_{i}" / "leaf"), name="leaf"
        )

    calls = []
    real_probe = move_mod._is_case_insensitive_path

    def counting_probe(path):
        calls.append(path)
        return real_probe(path)

    monkeypatch.setattr(move_mod, "_is_case_insensitive_path", counting_probe)

    move_mod.move_folder(
        db=env["db"], folder_id=env["fid_src"], destination=str(dest_input)
    )

    # Count probes whose argument is the resolved destination. move_folder
    # resolves `destination` to `destination/<source_folder_name>` before
    # the overlap check, so the probed path is the landing dir, not the raw
    # input. The expected pattern with caching is 2 probes: one from
    # _destination_overlaps_source(src, dest)'s symmetric check against
    # dest, and one cached probe from _tracked_destination_overlap. Without
    # caching, the row loop would add 8 more (one per added row) — the
    # regression we're guarding against. The exact bound matters: bumping
    # the row count must not bump the probe count.
    resolved_dest = os.path.realpath(str(dest_input / "src"))
    dest_probes = [p for p in calls if os.path.realpath(p) == resolved_dest]
    # Lower bound on POSIX: the destination probe MUST be exercised at
    # least once. Without this, the upper-bound assertion passes vacuously
    # if a future refactor skips the resolved-destination probe entirely.
    # Windows short-circuits to True without listing, so no probe fires.
    if os.name != "nt":
        assert len(dest_probes) >= 1, (
            f"expected at least one resolved-destination probe on POSIX, "
            f"got 0 (all calls: {calls})"
        )
    assert len(dest_probes) <= 2, (
        f"expected dest probe to be cached (≤2 calls), "
        f"got {len(dest_probes)} (all calls: {calls})"
    )


@pytest.mark.skipif(
    os.name == "nt",
    reason="Windows paths are treated case-insensitive without POSIX probing",
)
def test_is_case_insensitive_path_probes_inside_target_fs(tmp_path, monkeypatch):
    """The case-insensitivity probe must test the FS at the deepest existing
    ancestor by case-flipping a CHILD entry, not by case-flipping the
    ancestor's own basename. Otherwise a case-sensitive APFS volume mounted
    at /Volumes/Photos under default case-insensitive macOS HFS+ is wrongly
    classified case-insensitive (the basename probe asks /Volumes how it
    resolves "Photos", which is case-insensitive, instead of asking the
    mounted volume how it resolves its own children), and valid moves into
    the volume get refused as overlapping a stale case-only alias.
    """
    import move as move_mod

    mount = tmp_path / "Mount"
    mount.mkdir()
    # Letter-named child so a child probe has something to flip.
    (mount / "child").mkdir()
    mount_str = str(mount)

    def fake_samefile(a, b):
        # Mimic the misleading scenario: case-flipping the mount point's
        # own basename in its parent directory collapses (the parent FS
        # folds case), while case-flipping a child name under the mount
        # does not (the mount's FS is case-sensitive).
        names = {os.path.basename(a), os.path.basename(b)}
        return names == {"Mount", "mOUNT"}

    monkeypatch.setattr(move_mod, "_samefile_tristate", fake_samefile)

    # Deepest existing ancestor of the missing leaf is `mount`. The fixed
    # probe must look inside `mount` and report case-sensitive (False).
    # The pre-fix code probed `os.path.basename(mount)` in its parent and
    # returned True — the regression we're guarding against.
    assert move_mod._is_case_insensitive_path(
        os.path.join(mount_str, "missing", "sub")
    ) is False


def test_is_case_insensitive_path_detects_case_insensitive_fs(tmp_path, monkeypatch):
    """When the deepest existing ancestor's FS folds case, the probe must
    return True — confirmed via the temp-dir probe, since `samefile` True
    on a pre-existing child name could be a user-created hard link / symlink
    alias rather than real case folding."""
    import move as move_mod

    target = tmp_path / "dir"
    target.mkdir()
    (target / "child").touch()
    target_str = str(target)

    def fake_samefile(a, b):
        # Mimic case-insensitive FS at `target`: any two child names
        # (including the temp probe dir we create) that differ only by
        # case resolve to the same inode.
        return (
            os.path.dirname(a) == target_str
            and os.path.dirname(b) == target_str
            and os.path.basename(a).lower() == os.path.basename(b).lower()
        )

    # Child probe alone is no longer authoritative for True — patch both
    # entry points so the temp-dir confirmation also sees the case fold.
    monkeypatch.setattr(move_mod, "_samefile_tristate", fake_samefile)
    monkeypatch.setattr(move_mod, "_samefile_or_false", fake_samefile)

    assert move_mod._is_case_insensitive_path(
        os.path.join(target_str, "missing_leaf")
    ) is True


@pytest.mark.skipif(
    os.name == "nt",
    reason="Windows paths are treated case-insensitive without POSIX probing",
)
def test_is_case_insensitive_path_inconclusive_child_probe_keeps_scanning(
    tmp_path, monkeypatch,
):
    """A letter-bearing child whose case-flipped probe raises (broken
    symlink, permission/race) must not abort the scan as case-sensitive —
    only a definitive `samefile` False (two distinct entries) does that.
    Inconclusive Nones are skipped; True is non-authoritative and confirmed
    via the temp-dir probe below.
    """
    import move as move_mod

    target = tmp_path / "dir"
    target.mkdir()
    (target / "broken").touch()
    (target / "child").touch()
    target_str = str(target)

    def fake_tristate(a, b):
        names = {os.path.basename(a).lower(), os.path.basename(b).lower()}
        if "broken" in names:
            # Mimic stat raising on the flipped spelling of a broken entry.
            return None
        # `child`/`CHILD` looks like an alias on a case-insensitive FS, but
        # the new logic doesn't trust child True — the result comes from the
        # temp-probe path below.
        return os.path.basename(a).lower() == os.path.basename(b).lower()

    def fake_samefile_or_false(a, b):
        # Temp-probe confirmation: mimic case-insensitive FS at `target`.
        return (
            os.path.dirname(a) == target_str
            and os.path.dirname(b) == target_str
            and os.path.basename(a).lower() == os.path.basename(b).lower()
        )

    monkeypatch.setattr(move_mod, "_samefile_tristate", fake_tristate)
    monkeypatch.setattr(move_mod, "_samefile_or_false", fake_samefile_or_false)

    assert move_mod._is_case_insensitive_path(
        os.path.join(target_str, "missing_leaf")
    ) is True


@pytest.mark.skipif(
    os.name == "nt" or _TMP_FOLDS_CASE,
    reason="Setup creates `foo` and `FOO` as distinct sibling files; only "
           "possible on a case-sensitive parent FS (not Windows / macOS APFS).",
)
def test_is_case_insensitive_path_child_alias_confirmed_by_temp_probe(
    tmp_path, monkeypatch,
):
    """On a case-SENSITIVE POSIX FS, `samefile` can be True for two distinct
    child entries when they are hard links or symlink aliases (e.g. `foo`
    and `FOO` both pointing to the same inode by user choice). Trusting that
    child True would misclassify Linux ext4 as case-insensitive and let
    `_tracked_destination_overlap` refuse a valid move into `dst/src` just
    because a stale row `Dst/src` exists. The temp-dir probe is the
    confirmation: it creates a fresh entry whose flipped spelling can't be
    a pre-existing alias.
    """
    import move as move_mod

    target = tmp_path / "dir"
    target.mkdir()
    (target / "foo").touch()
    (target / "FOO").touch()  # pretend these are aliases for samefile
    target_str = str(target)

    def fake_tristate(a, b):
        # Pre-existing children look aliased (matches a user hard-link /
        # symlink setup on a case-sensitive FS).
        names = {os.path.basename(a), os.path.basename(b)}
        if names == {"foo", "FOO"}:
            return True
        return None

    # Temp probe uses the real `_samefile_or_false`; on real case-sensitive
    # POSIX tmp_path the probe and its flipped spelling do NOT resolve to
    # the same inode, so the final answer is False.
    monkeypatch.setattr(move_mod, "_samefile_tristate", fake_tristate)

    assert move_mod._is_case_insensitive_path(
        os.path.join(target_str, "missing_leaf")
    ) is False

    # Probe dir cleaned up — only the originals remain.
    assert sorted(os.listdir(target)) == ["FOO", "foo"]


@pytest.mark.skipif(
    os.name == "nt",
    reason="Windows paths are treated case-insensitive without POSIX probing",
)
def test_is_case_insensitive_path_child_scan_short_circuits_on_definitive_false(
    tmp_path, monkeypatch,
):
    """When the temp probe can't write (read-only ancestor), the child-scan
    fallback returns False as soon as it sees a case-twin pair with
    distinct inodes (`samefile` == False) — impossible on a case-folding
    FS — without statting every other entry under the ancestor."""
    import move as move_mod

    target = tmp_path / "dir"
    target.mkdir()
    (target / "alpha").touch()
    (target / "ALPHA").touch()
    target_str = str(target)

    def fake_mkdtemp(*_a, **_kw):
        # Simulate a read-only ancestor so the child-scan fallback runs.
        raise OSError("read-only — exercise child-scan fallback")

    def fake_tristate(a, b):
        # Mimic real case-sensitive FS: alpha and ALPHA are distinct files
        # and samefile returns False on the pair.
        names = {os.path.basename(a), os.path.basename(b)}
        if names == {"alpha", "ALPHA"}:
            return False
        return None

    monkeypatch.setattr(move_mod.tempfile, "mkdtemp", fake_mkdtemp)
    monkeypatch.setattr(move_mod, "_samefile_tristate", fake_tristate)

    assert move_mod._is_case_insensitive_path(
        os.path.join(target_str, "missing_leaf")
    ) is False


@pytest.mark.skipif(
    os.name == "nt",
    reason="Windows paths are treated case-insensitive without POSIX probing",
)
def test_is_case_insensitive_path_temp_probe_runs_before_child_scan(
    tmp_path, monkeypatch,
):
    """The temp probe is conclusive in both directions with O(1) syscalls,
    so it must run first when the ancestor is writable — the child-scan
    fallback is read-only and only worthwhile when mkdtemp can't write.
    Before this ordering, `move_folder()` re-stat'd every entry under the
    destination's deepest existing ancestor on every move (the per-entry
    samefile probe is inconclusive on a typical case-sensitive directory
    with no case-twin children), turning the preflight guard into
    O(entries) wasted syscalls per move on big photo trees.
    """
    import move as move_mod

    target = tmp_path / "dir"
    target.mkdir()
    # Many letter-bearing entries — the OLD scan-first order would call
    # _samefile_tristate once per entry before reaching the temp probe.
    for i in range(50):
        (target / f"entry_{i}").touch()

    call_order = []
    real_mkdtemp = move_mod.tempfile.mkdtemp
    real_tristate = move_mod._samefile_tristate

    def tracked_mkdtemp(*a, **kw):
        call_order.append("mkdtemp")
        return real_mkdtemp(*a, **kw)

    def tracked_tristate(a, b):
        # Only count per-entry child-scan probes against pre-existing
        # entries; ignore the temp probe's own samefile call (which goes
        # through _samefile_or_false → _samefile_tristate).
        if not (os.path.basename(a).startswith(".vireo_case_probe_")
                or os.path.basename(b).startswith(".vireo_case_probe_")):
            call_order.append("tristate")
        return real_tristate(a, b)

    monkeypatch.setattr(move_mod.tempfile, "mkdtemp", tracked_mkdtemp)
    monkeypatch.setattr(move_mod, "_samefile_tristate", tracked_tristate)

    # Real tmp_path is case-sensitive on Linux CI; the temp probe creates
    # a fresh dir whose flipped name doesn't exist, so samefile raises and
    # `_samefile_or_false` returns False. That answer is final, with zero
    # child-scan calls — the regression we're guarding against.
    move_mod._is_case_insensitive_path(
        os.path.join(str(target), "missing_leaf")
    )

    assert "mkdtemp" in call_order, "temp probe should have run"
    assert "tristate" not in call_order, (
        f"child-scan probes should not run when temp probe succeeds; "
        f"call order was {call_order}"
    )


@pytest.mark.skipif(
    os.name == "nt",
    reason="Windows paths are treated case-insensitive without POSIX probing",
)
def test_is_case_insensitive_path_all_inconclusive_falls_back_to_temp_probe(
    tmp_path, monkeypatch,
):
    """When every letter-bearing child's probe is inconclusive (every
    flipped spelling raises), the scan must fall through to the temp-dir
    probe rather than declaring the FS case-sensitive off the inconclusive
    children. Mirrors the no-letter-children fallback for the case where
    letter children exist but can't be resolved.
    """
    import move as move_mod

    target = tmp_path / "dir"
    target.mkdir()
    (target / "broken1").touch()
    (target / "broken2").touch()
    target_str = str(target)

    def fake_tristate(a, b):
        # Every child probe inconclusive — but the temp-probe fallback
        # uses `_samefile_or_false`, which we leave alone so the real
        # probe-dir path runs. With the case-insensitive simulation
        # below, the temp probe should return True.
        return None

    def fake_samefile_or_false(a, b):
        # Mimic case-insensitive FS at `target`: anything whose dirname
        # is `target` and whose basenames match case-folded is "same".
        return (
            os.path.dirname(a) == target_str
            and os.path.dirname(b) == target_str
            and os.path.basename(a).lower() == os.path.basename(b).lower()
        )

    monkeypatch.setattr(move_mod, "_samefile_tristate", fake_tristate)
    monkeypatch.setattr(move_mod, "_samefile_or_false", fake_samefile_or_false)

    assert move_mod._is_case_insensitive_path(
        os.path.join(target_str, "missing_leaf")
    ) is True

    # Probe dir cleaned up — only the originals remain.
    assert sorted(os.listdir(target)) == ["broken1", "broken2"]


@pytest.mark.skipif(
    os.name == "nt",
    reason="Windows paths are treated case-insensitive without POSIX probing",
)
def test_is_case_insensitive_path_no_letter_children_falls_back_to_temp_probe(
    tmp_path, monkeypatch,
):
    """When no existing child has a letter to flip, the probe creates its
    own temp dir inside the ancestor and asks samefile whether the
    case-flipped spelling resolves to the same inode. The probe result is
    returned, and the probe dir must be cleaned up so the destination is left
    as the function found it."""
    import move as move_mod

    target = tmp_path / "digits"
    target.mkdir()
    (target / "123").touch()
    (target / "456").mkdir()
    monkeypatch.setattr(move_mod, "_samefile_or_false", lambda _a, _b: False)

    assert move_mod._is_case_insensitive_path(
        os.path.join(str(target), "missing")
    ) is False

    # No leftover probe dir — listdir still sees only the originals.
    assert sorted(os.listdir(target)) == ["123", "456"]


def test_is_case_insensitive_path_empty_ancestor_detects_case_insensitive_fs(
    tmp_path, monkeypatch,
):
    """A fresh case-insensitive POSIX destination tree (default macOS APFS at
    /Volumes/Photos with nothing in it yet) must still be classified as
    case-insensitive, so a stale tracked row like /Photos/Dst/src vs a
    move into /Photos/dst/src is still caught by the case-folded overlap
    check. Before the temp-probe fallback, an empty ancestor returned
    False unconditionally and let the stale alias slip through, leaving
    two folder rows managing the same on-disk tree.
    """
    import move as move_mod

    target = tmp_path / "empty_dest_root"
    target.mkdir()
    target_str = str(target)

    def fake_samefile(a, b):
        # Mimic case-insensitive FS at `target`: any two sibling names
        # under it that differ only by case resolve to the same inode.
        return (
            os.path.dirname(a) == target_str
            and os.path.dirname(b) == target_str
            and os.path.basename(a).lower() == os.path.basename(b).lower()
        )

    monkeypatch.setattr(move_mod, "_samefile_or_false", fake_samefile)

    assert move_mod._is_case_insensitive_path(
        os.path.join(target_str, "missing", "sub")
    ) is True

    # Probe dir cleaned up — only originals (none, here) remain.
    assert os.listdir(target) == []


@pytest.mark.skipif(
    os.name == "nt",
    reason="Windows paths are treated case-insensitive without POSIX probing",
)
def test_is_case_insensitive_path_empty_ancestor_unwritable_returns_false(tmp_path):
    """If the deepest existing ancestor is empty AND read-only, the
    temp-probe fallback can't write a probe dir. Return False rather than
    guessing — spuriously folding case could collapse two genuinely
    distinct paths."""
    if hasattr(os, "geteuid") and os.geteuid() == 0:
        pytest.skip("root bypasses dir-write permissions")
    import move as move_mod

    target = tmp_path / "ro_empty"
    target.mkdir()
    original_mode = target.stat().st_mode
    os.chmod(target, 0o500)  # readable+executable, not writable
    try:
        assert move_mod._is_case_insensitive_path(
            os.path.join(str(target), "missing")
        ) is False
    finally:
        os.chmod(target, original_mode)


@pytest.mark.skipif(
    os.name == "nt",
    reason="Windows paths are treated case-insensitive without POSIX probing",
)
def test_is_case_insensitive_path_unreadable_ancestor_falls_back_to_temp_probe(
    tmp_path, monkeypatch,
):
    """A +wx (drop-box) ancestor without read permission can still receive
    a fresh temp dir, so an os.listdir denial must be inconclusive — fall
    through to the temp probe instead of declaring the FS case-sensitive.
    Otherwise a stale tracked row like /Photos/Dst/src can slip past the
    case-folded overlap check on a case-insensitive POSIX destination."""
    if hasattr(os, "geteuid") and os.geteuid() == 0:
        pytest.skip("root bypasses dir-read permissions")
    import move as move_mod

    target = tmp_path / "dropbox"
    target.mkdir()
    target_str = str(target)

    def fake_samefile(a, b):
        return (
            os.path.dirname(a) == os.path.dirname(b)
            and os.path.basename(a).lower() == os.path.basename(b).lower()
        )

    monkeypatch.setattr(move_mod, "_samefile_or_false", fake_samefile)

    original_mode = target.stat().st_mode
    os.chmod(target, 0o300)  # write+execute, not readable
    try:
        with pytest.raises(OSError):
            os.listdir(target_str)
        assert move_mod._is_case_insensitive_path(
            os.path.join(target_str, "missing", "sub")
        ) is True
    finally:
        os.chmod(target, original_mode)

    # Probe dir cleaned up.
    assert os.listdir(target) == []


def test_move_folder_refuses_self_overlapping_destination(move_env):
    """A move whose resolved destination overlaps the source (here, the
    source's own parent → resolved dest == source) must be refused rather
    than no-op-copy then delete the source."""
    from move import move_folder

    env = move_env
    parent = str(env["src"].parent)  # resolve_folder_dest(...) == src path
    result = move_folder(
        db=env["db"], folder_id=env["fid_src"], destination=parent, merge=True
    )
    assert result["moved"] == 0
    assert any("overlaps" in e for e in result["errors"])
    # Source folder fully intact.
    assert (env["src"] / "bird1.jpg").exists()
    assert (env["src"] / "bird2.jpg").exists()


def test_move_folder_merge_verify_fail_preserves_originals_and_dest(move_env, monkeypatch):
    """If a source file is missing at the destination after copy, the merge
    aborts without deleting originals or the pre-existing destination."""
    import move as move_mod

    env = move_env
    landing = env["dst"] / "src"
    landing.mkdir()
    sentinel = landing / "user_file.txt"
    sentinel.write_text("pre-existing")

    # Force the copy step to be a no-op so a source file is "missing" at dest.
    monkeypatch.setattr(move_mod, "_run_rsync_streamed",
                        lambda *a, **k: (0, "", False))

    result = move_mod.move_folder(
        db=env["db"], folder_id=env["fid_src"], destination=str(env["dst"]), merge=True
    )
    assert result["moved"] == 0
    assert any("Verification failed" in e for e in result["errors"])
    # Originals preserved, pre-existing destination file untouched
    assert (env["src"] / "bird1.jpg").exists()
    assert sentinel.exists()


def test_move_folder_merge_rejects_symlinked_dest_file(move_env, monkeypatch):
    """A destination entry that is a symlink to the source file must fail
    verification, not be accepted as an independent copy. Otherwise rsync
    --ignore-existing leaves the symlink alone and the post-copy
    rmtree(src_path) would destroy the symlink's target — the only copy."""
    import move as move_mod

    env = move_env
    landing = env["dst"] / "src"
    landing.mkdir()
    # Place real copies of the files we DON'T want the test to trip over,
    # so verification reaches the symlinked entry rather than failing on
    # the first plain-missing file.
    (landing / "bird1.xmp").write_bytes((env["src"] / "bird1.xmp").read_bytes())
    (landing / "bird2.jpg").write_bytes((env["src"] / "bird2.jpg").read_bytes())
    # bird1.jpg at dest is a SYMLINK back to the source file.
    try:
        os.symlink(str(env["src"] / "bird1.jpg"), str(landing / "bird1.jpg"))
    except (OSError, NotImplementedError):
        pytest.skip("symlinks not supported on this platform")

    # Force rsync to no-op (it would normally honor --ignore-existing and
    # leave the symlink anyway; this just removes the dependency on rsync).
    monkeypatch.setattr(move_mod, "_run_rsync_streamed",
                        lambda *a, **k: (0, "", False))

    result = move_mod.move_folder(
        db=env["db"], folder_id=env["fid_src"], destination=str(env["dst"]), merge=True
    )
    assert result["moved"] == 0
    assert any("Verification failed" in e and "bird1.jpg" in e
               for e in result["errors"])
    # The source file MUST still exist — that's the safety property.
    assert (env["src"] / "bird1.jpg").exists()
    assert (env["src"] / "bird1.jpg").read_bytes() != b""


def test_move_folder_merge_rejects_symlinked_dest_subdir(move_env, monkeypatch):
    """A destination subdirectory that is a symlink back into the source
    tree must also fail verification. os.path.isfile/getsize on the joined
    path would silently follow the link to the source's own bytes and
    pass — and then rmtree(src_path) would destroy the only copy."""
    import move as move_mod

    env = move_env
    # Reshape source: put bird1 inside a real subdirectory.
    sub_src = env["src"] / "sub"
    sub_src.mkdir()
    (env["src"] / "bird1.jpg").rename(sub_src / "bird1.jpg")
    (env["src"] / "bird1.xmp").rename(sub_src / "bird1.xmp")

    landing = env["dst"] / "src"
    landing.mkdir()
    # Real copy of the file in src root (bird2) so verification reaches
    # the symlinked subdirectory entries.
    (landing / "bird2.jpg").write_bytes((env["src"] / "bird2.jpg").read_bytes())
    # The "sub" dir at the destination is a SYMLINK back into the source.
    try:
        os.symlink(str(sub_src), str(landing / "sub"))
    except (OSError, NotImplementedError):
        pytest.skip("symlinks not supported on this platform")

    monkeypatch.setattr(move_mod, "_run_rsync_streamed",
                        lambda *a, **k: (0, "", False))

    result = move_mod.move_folder(
        db=env["db"], folder_id=env["fid_src"], destination=str(env["dst"]), merge=True
    )
    assert result["moved"] == 0
    assert any("Verification failed" in e for e in result["errors"])
    # Source files still present at the real path — not destroyed via the
    # symlinked-parent shortcut.
    assert (sub_src / "bird1.jpg").exists()


def test_move_folder_merge_rejects_broken_symlink(move_env, monkeypatch):
    """A broken symlink at the destination (lexists True, isfile False) must
    also fail verification, not be silently accepted as missing-then-fine."""
    import move as move_mod

    env = move_env
    landing = env["dst"] / "src"
    landing.mkdir()
    (landing / "bird1.xmp").write_bytes((env["src"] / "bird1.xmp").read_bytes())
    (landing / "bird2.jpg").write_bytes((env["src"] / "bird2.jpg").read_bytes())
    # Broken symlink — target doesn't exist.
    try:
        os.symlink(str(env["tmp_path"] / "nope.jpg"), str(landing / "bird1.jpg"))
    except (OSError, NotImplementedError):
        pytest.skip("symlinks not supported on this platform")

    monkeypatch.setattr(move_mod, "_run_rsync_streamed",
                        lambda *a, **k: (0, "", False))

    result = move_mod.move_folder(
        db=env["db"], folder_id=env["fid_src"], destination=str(env["dst"]), merge=True
    )
    assert result["moved"] == 0
    assert any("Verification failed" in e for e in result["errors"])
    assert (env["src"] / "bird1.jpg").exists()


def test_resolve_folder_dest():
    """resolve_folder_dest places the folder inside the destination."""
    from move import resolve_folder_dest

    # Compare against os.path.join so the expectation is platform-correct
    # (Windows joins with a backslash).
    assert resolve_folder_dest("/a/birds", "birds", "/nas/photos") == \
        os.path.join("/nas/photos", "birds")
    # Falls back to basename when name is empty
    assert resolve_folder_dest("/a/birds/", "", "/nas/photos") == \
        os.path.join("/nas/photos", "birds")


def test_move_folder_updates_counts(move_env):
    """move_folder updates photo_count on folders."""
    from move import move_folder

    env = move_env
    move_folder(db=env["db"], folder_id=env["fid_src"], destination=str(env["dst"]))
    env["db"].update_folder_counts()
    row = env["db"].conn.execute(
        "SELECT photo_count FROM folders WHERE path = ?",
        (str(env["dst"] / "src"),),
    ).fetchone()
    assert row["photo_count"] == 2


def test_move_photos_progress_callback(move_env):
    """move_photos calls progress callback with current/total."""
    from move import move_photos

    env = move_env
    calls = []
    move_photos(
        db=env["db"],
        photo_ids=[env["p1"], env["p2"]],
        destination=str(env["dst"]),
        progress_cb=lambda cur, tot, fn: calls.append((cur, tot, fn)),
    )
    assert len(calls) == 2
    assert calls[0][0] == 1  # current=1
    assert calls[1][0] == 2  # current=2
    assert calls[0][1] == 2  # total=2


def test_move_folder_reports_phases_and_per_file_progress(move_env):
    """move_folder streams the move through named phases and reports each
    file as it is copied, instead of one progress call at the end."""
    from move import move_folder

    env = move_env
    calls = []
    result = move_folder(
        db=env["db"],
        folder_id=env["fid_src"],
        destination=str(env["dst"]),
        progress_cb=lambda cur, tot, fn, phase: calls.append(
            (cur, tot, fn, phase)
        ),
    )
    assert result["errors"] == []
    phases = [c[3] for c in calls]
    # The move walks through these phases in order.
    for expected in ("Checking destination", "Copying files",
                     "Verifying copy", "Updating catalog",
                     "Removing originals", "Done"):
        assert expected in phases, f"missing phase {expected!r} in {phases}"

    # The copy phase reports a real denominator (3 files: 2 jpgs + 1 xmp)
    # and at least one per-file update naming the file being copied.
    copy_calls = [c for c in calls if c[3] == "Copying files"]
    assert copy_calls, "no copy-phase progress reported"
    total = copy_calls[-1][1]
    assert total == 3
    named = [c for c in copy_calls if c[0] > 0 and c[2]]
    assert named, "no per-file progress during copy"
    assert all(c[1] == total for c in copy_calls)


def test_move_folder_progress_shutil_fallback(move_env, monkeypatch):
    """When rsync is unavailable, the shutil fallback still reports per-file
    copy progress through the same phase contract."""
    import move as move_mod

    def _no_rsync(*a, **k):
        raise FileNotFoundError("rsync")

    monkeypatch.setattr(move_mod.subprocess, "Popen", _no_rsync)

    env = move_env
    calls = []
    result = move_mod.move_folder(
        db=env["db"],
        folder_id=env["fid_src"],
        destination=str(env["dst"]),
        progress_cb=lambda cur, tot, fn, phase: calls.append(
            (cur, tot, fn, phase)
        ),
    )
    assert result["errors"] == []
    assert (env["dst"] / "src" / "bird1.jpg").exists()
    copy_calls = [c for c in calls if c[3] == "Copying files" and c[0] > 0]
    assert copy_calls, "shutil fallback reported no per-file progress"
    assert copy_calls[-1][1] == 3  # same 3-file denominator


def test_move_folder_shutil_fallback_preserves_dir_symlink(move_env, monkeypatch):
    """The shutil fallback (rsync unavailable) must not silently drop a
    symlinked subdirectory. os.walk doesn't recurse into one, so without
    explicit handling its contents would never reach the destination yet the
    count verification would still pass and delete the originals. The symlink
    must be recreated at the destination, matching rsync -a."""
    import move as move_mod

    env = move_env
    # A symlinked subdirectory inside the source pointing at an external dir.
    external = env["tmp_path"] / "external"
    external.mkdir()
    (external / "target.txt").write_text("payload")
    try:
        os.symlink(str(external), str(env["src"] / "linkdir"))
    except (OSError, NotImplementedError):
        pytest.skip("symlinks not supported on this platform")

    monkeypatch.setattr(move_mod.subprocess, "Popen",
                        lambda *a, **k: (_ for _ in ()).throw(
                            FileNotFoundError("rsync")))

    result = move_mod.move_folder(
        db=env["db"], folder_id=env["fid_src"], destination=str(env["dst"])
    )
    assert result["errors"] == []
    landing = env["dst"] / "src"
    link = landing / "linkdir"
    # The symlink is preserved as a symlink, still resolving to the payload.
    assert link.is_symlink()
    assert (link / "target.txt").read_text() == "payload"
    # External target untouched; source removed after a verified copy.
    assert (external / "target.txt").exists()
    assert not env["src"].exists()


def test_move_folder_shutil_fallback_aborts_on_unreadable_subdir(move_env, monkeypatch):
    """If the shutil fallback can't read a source subdirectory, it must abort
    the move rather than silently skip the subtree. The count verification
    would otherwise also skip it (same default walk), match, and delete the
    originals leaving the destination incomplete."""
    import move as move_mod

    env = move_env
    sub = env["src"] / "sub"
    sub.mkdir()
    (sub / "nest.jpg").write_bytes(b"\xff\xd8" + b"\x00" * 30)

    monkeypatch.setattr(move_mod.subprocess, "Popen",
                        lambda *a, **k: (_ for _ in ()).throw(
                            FileNotFoundError("rsync")))

    # Force os.walk to surface a scandir error for the subdirectory, as it
    # would on a permission failure (default os.walk swallows this).
    real_walk = move_mod.os.walk

    def _walk_with_error(path, *a, **k):
        onerror = k.get("onerror")
        for root, dirs, files in real_walk(path, *a, **k):
            if onerror is not None and os.path.basename(root) == "sub":
                onerror(OSError(13, "Permission denied", str(sub)))
            yield root, dirs, files

    monkeypatch.setattr(move_mod.os, "walk", _walk_with_error)

    result = move_mod.move_folder(
        db=env["db"], folder_id=env["fid_src"], destination=str(env["dst"])
    )
    # Move aborted, originals preserved.
    assert result["moved"] == 0
    assert any("Copy failed" in e for e in result["errors"])
    assert env["src"].exists()
    assert (env["src"] / "bird1.jpg").exists()


def test_move_folder_shutil_fallback_preserves_dir_mode(move_env, monkeypatch):
    """The shutil fallback must preserve directory permissions on a fresh
    move (matching rsync -a / the old copytree). os.makedirs alone would
    drop a private 0700 folder down to the umask default before the source
    is deleted, permanently losing the metadata."""
    import move as move_mod

    if os.name == "nt":
        pytest.skip("POSIX directory modes not meaningful on Windows")

    env = move_env
    sub = env["src"] / "private"
    sub.mkdir()
    (sub / "secret.jpg").write_bytes(b"\xff\xd8" + b"\x00" * 20)
    os.chmod(str(sub), 0o700)

    monkeypatch.setattr(move_mod.subprocess, "Popen",
                        lambda *a, **k: (_ for _ in ()).throw(
                            FileNotFoundError("rsync")))

    result = move_mod.move_folder(
        db=env["db"], folder_id=env["fid_src"], destination=str(env["dst"])
    )
    assert result["errors"] == []
    dest_sub = env["dst"] / "src" / "private"
    assert dest_sub.is_dir()
    assert (os.stat(str(dest_sub)).st_mode & 0o777) == 0o700
    assert not env["src"].exists()


def test_move_folder_shutil_fallback_preserves_file_symlink(move_env, monkeypatch):
    """The shutil fallback must preserve a symlinked file as a symlink rather
    than dereferencing it through copy2 (matching rsync -a). Dereferencing
    would silently replace the link with its target's bytes and could write
    through a symlinked destination entry."""
    import move as move_mod

    env = move_env
    external = env["tmp_path"] / "ext_target.txt"
    external.write_text("external payload")
    try:
        os.symlink(str(external), str(env["src"] / "alias.txt"))
    except (OSError, NotImplementedError):
        pytest.skip("symlinks not supported on this platform")

    monkeypatch.setattr(move_mod.subprocess, "Popen",
                        lambda *a, **k: (_ for _ in ()).throw(
                            FileNotFoundError("rsync")))

    result = move_mod.move_folder(
        db=env["db"], folder_id=env["fid_src"], destination=str(env["dst"])
    )
    assert result["errors"] == []
    dest_link = env["dst"] / "src" / "alias.txt"
    assert dest_link.is_symlink()
    assert os.readlink(str(dest_link)) == str(external)
    # External target untouched; source removed after a verified copy.
    assert external.read_text() == "external payload"
    assert not env["src"].exists()


def test_move_folder_fresh_move_detects_late_source_file(move_env, monkeypatch):
    """A file added to the source after the upfront file count but before
    verification — a concurrent writer race rsync's scan missed — must be
    detected before `shutil.rmtree(src_path)` would silently delete it.

    Trusting the upfront `total_files` as the source count makes this
    silently incorrect: src now holds total_files+1 files, rsync copied
    only total_files of them, dst_count equals total_files, and a stale
    count compare passes — then rmtree wipes the new source file.
    Verification must re-examine the source against the destination
    instead of reusing the pre-copy count."""
    import shutil as _shutil

    import move as move_mod

    env = move_env
    landing = env["dst"] / "src"
    assert not landing.exists()  # fresh move (no merge path)

    def _copy_then_inject(src_path, dest_path, *args, **kwargs):
        # Stand in for rsync: copy the tree as it looked when scanning began,
        # then simulate a concurrent writer adding a new file to the source
        # before verification runs.
        _shutil.copytree(src_path, dest_path)
        (env["src"] / "late_arrival.jpg").write_bytes(b"new content")
        return 0, "", False

    monkeypatch.setattr(move_mod, "_run_rsync_streamed", _copy_then_inject)

    result = move_mod.move_folder(
        db=env["db"], folder_id=env["fid_src"], destination=str(env["dst"]),
    )
    # The move must abort — assert the safety property (originals survive,
    # late arrival is not silently deleted) without coupling to the exact
    # error wording. A count-mismatch fix surfaces this differently from a
    # per-source-file presence check; both are valid signals.
    assert result["moved"] == 0
    assert result["errors"], "expected an error explaining why verification failed"
    assert (env["src"] / "late_arrival.jpg").exists(), \
        "the late source file was silently deleted"
    assert (env["src"] / "late_arrival.jpg").read_bytes() == b"new content"
    # The pre-existing source files must also still be present — a fresh
    # move that aborts at verification preserves originals.
    assert (env["src"] / "bird1.jpg").exists()
    assert (env["src"] / "bird2.jpg").exists()
