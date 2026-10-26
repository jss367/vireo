"""Case semantics apply to names inside a sidecar's containing directory."""

import os
from types import SimpleNamespace

import pytest
from services import photo_ingest, workspace_folders
from sidecar_identity import directory_folds_case, missing_sidecar_identity, sidecar_paths_alias
from sync import _sidecar_target_identities


@pytest.fixture
def sidecars(tmp_path, db, monkeypatch):
    """Simulate Windows parent aliases with either per-directory case flag.

    Only directory entries/stat lookups are simulated. The alias probe, queue
    cancellation, and sync retry identity calculation run their real code.
    """
    folder = tmp_path / 'Photos'
    folder.mkdir()
    def setup(folds, different_parent, sidecar_inodes=None, filenames=('A.jpg', 'a.nef'), name_key=str.casefold):
        for name in filenames:
            (folder / name).write_text('photo')
        other = folder.with_name('photos') if different_parent else folder
        folders = [workspace_folders.add_folder(db, str(p), name=p.name) for p in (folder, other)]
        photos = [photo_ingest.add_photo(db, fid, name, os.path.splitext(name)[1], 1, 1.0)
                  for fid, name in zip(folders, filenames, strict=True)]
        paths = [str(p / (os.path.splitext(name)[0] + '.xmp'))
                 for p, name in zip((folder, other), filenames, strict=True)]
        stat, scandir, realpath = os.stat, os.scandir, os.path.realpath

        def mapped(path):
            path = os.fspath(path)
            if path.casefold() == str(folder).casefold():
                return str(folder)
            if os.path.dirname(path).casefold() == str(folder).casefold():
                name = os.path.basename(path)
                names = [p.name for p in folder.iterdir()]
                if folds:
                    match = next((entry for entry in names if name_key(entry) == name_key(name)), None)
                    if match is None:
                        raise FileNotFoundError(path)
                    name = match
                elif name not in names:
                    raise FileNotFoundError(path)
                return str(folder / name)
            return path

        def fake_stat(path, *args, **kwargs):
            if sidecar_inodes is not None and os.fspath(path) in paths:
                return SimpleNamespace(st_dev=1, st_ino=sidecar_inodes[paths.index(os.fspath(path))])
            return stat(mapped(path), *args, **kwargs)

        def fake_realpath(path, *args, **kwargs):
            if os.path.dirname(os.fspath(path)).casefold() == str(folder).casefold():
                return os.fspath(path)
            return realpath(path, *args, **kwargs)

        monkeypatch.setattr(os, 'stat', fake_stat)
        monkeypatch.setattr(os, 'scandir', lambda path: scandir(mapped(path)))
        monkeypatch.setattr(os.path, 'realpath', fake_realpath)
        # Exercise the Windows bug even on a case-sensitive POSIX host.
        monkeypatch.setattr(os.path, 'normcase', lambda path: os.fspath(path).lower())
        return photos, paths

    return setup


@pytest.mark.parametrize('folds', [False, True])
@pytest.mark.parametrize('different_parent', [False, True])
def test_missing_sidecars_use_containing_directory_case_semantics(db, sidecars, folds, different_parent):
    photos, paths = sidecars(folds, different_parent)
    db.queue_change(photos[0], 'keyword_add', 'Robin')
    db.queue_change(photos[1], 'keyword_add', 'Robin')
    assert db.pending_changes.keyword_sidecar_alias(photos[0], db._active_workspace_id, 'Robin') is folds
    identities = _sidecar_target_identities(dict(zip(paths, paths, strict=True)))
    assert (identities[paths[0]] == identities[paths[1]]) is folds
    db._flip_pending_keyword_change(photos[0], 'Robin', 'keyword_add', 'keyword_remove')
    own_rows = [row for row in db.pending_changes.list_all() if row['photo_id'] == photos[0]]
    assert [row['change_type'] for row in own_rows] == (['keyword_remove'] if folds else [])


@pytest.mark.parametrize('different_parent', [False, True])
def test_matching_leaf_aliases_even_inside_case_sensitive_directory(sidecars, different_parent):
    _, paths = sidecars(False, different_parent)
    other = os.path.join(os.path.dirname(paths[1]), 'A.xmp')
    assert sidecar_paths_alias(paths[0], other)


@pytest.mark.parametrize('entries', [[], ['123']])
def test_unprobeable_directory_does_not_prove_alias(tmp_path, entries):
    for name in entries:
        (tmp_path / name).touch()
    first, second = [str(tmp_path / name) for name in ('A.xmp', 'a.xmp')]
    assert directory_folds_case(str(tmp_path)) is None
    assert not sidecar_paths_alias(first, second)
    # Retry cleanup remains conservative when there is no evidence either way.
    identities = _sidecar_target_identities({first: first, second: second})
    assert identities[first] == identities[second]


def test_distinct_case_parent_directories_never_alias(tmp_path):
    upper, lower = tmp_path / 'Dir', tmp_path / 'dir'
    upper.mkdir()
    try:
        lower.mkdir()
    except FileExistsError:
        pytest.skip('requires distinct case-sensitive parent names')
    for folder in (upper, lower):
        (folder / 'photo.jpg').touch()
    first, second = str(upper / 'a.xmp'), str(lower / 'a.xmp')
    assert not sidecar_paths_alias(first, second)
    identities = _sidecar_target_identities({first: first, second: second})
    assert identities[first] != identities[second]


@pytest.mark.parametrize('error', [PermissionError, OSError])
def test_unreadable_directory_does_not_prove_alias(tmp_path, monkeypatch, error):
    def fail(_path):
        raise error('unreadable directory')

    monkeypatch.setattr(os, 'scandir', fail)
    first, second = [str(tmp_path / name) for name in ('A.xmp', 'a.xmp')]
    assert directory_folds_case(str(tmp_path)) is None
    assert not sidecar_paths_alias(first, second)


def test_zero_parent_inode_is_inconclusive(tmp_path, monkeypatch):
    monkeypatch.setattr(os, 'stat', lambda _path: SimpleNamespace(st_dev=1, st_ino=0))
    monkeypatch.setattr('sidecar_identity.directory_folds_case', lambda _path: True)
    assert missing_sidecar_identity(str(tmp_path / 'a.xmp')) is None


@pytest.mark.parametrize('different_parent', [False, True])
@pytest.mark.parametrize('shared', [False, True])
def test_existing_sidecar_inodes_override_windows_normcase(db, sidecars, different_parent, shared):
    photos, paths = sidecars(False, different_parent, sidecar_inodes=[1, 1 if shared else 2])
    db.queue_change(photos[1], 'keyword_add', 'Robin')
    assert db.pending_changes.keyword_sidecar_alias(photos[0], db._active_workspace_id, 'Robin') is shared
    identities = _sidecar_target_identities(dict(zip(paths, paths, strict=True)))
    assert (identities[paths[0]] == identities[paths[1]]) is shared


def test_unknown_missing_identity_keeps_entire_retry_group_coupled(tmp_path, monkeypatch):
    first, second = [str(tmp_path / name) for name in ('A.xmp', 'a.xmp')]
    monkeypatch.setattr('sync.missing_sidecar_identity', lambda path: (1, 2, 'a.xmp') if path == first else None)
    identities = _sidecar_target_identities({first: first, second: second})
    assert identities[first] == identities[second]


def test_dangling_directory_entry_is_not_case_sensitivity_evidence(tmp_path, monkeypatch):
    original = str(tmp_path / 'Dangling')
    swapped = str(tmp_path / 'dANGLING')
    stat = os.stat

    def missing(path, *args, **kwargs):
        if os.fspath(path) in (original, swapped):
            raise FileNotFoundError(path)
        return stat(path, *args, **kwargs)

    class Entries:
        def __enter__(self):
            return iter([SimpleNamespace(name='Dangling')])

        def __exit__(self, *args):
            return False

    monkeypatch.setattr(os, 'scandir', lambda _path: Entries())
    monkeypatch.setattr(os, 'stat', missing)
    assert directory_folds_case(str(tmp_path)) is None


@pytest.mark.parametrize('different_parent', [False, True])
@pytest.mark.parametrize('names, shared', [
    (('straße.jpg', 'STRASSE.nef'), False),
    (('café.jpg', 'CAFÉ.nef'), True),
])
def test_missing_unicode_names_use_actual_directory_lookups(db, sidecars, different_parent, names, shared):
    # Simulate case-insensitive NTFS: simple Unicode case pairs alias, but
    # the multi-character ß/SS expansion does not. Casefold alone merges both.
    photos, paths = sidecars(True, different_parent, filenames=names, name_key=str.lower)
    assert directory_folds_case(os.path.dirname(paths[0])) is True
    db.queue_change(photos[0], 'keyword_add', 'Robin')
    db.queue_change(photos[1], 'keyword_add', 'Robin')
    assert db.pending_changes.keyword_sidecar_alias(photos[0], db._active_workspace_id, 'Robin') is shared
    identities = _sidecar_target_identities(dict(zip(paths, paths, strict=True)))
    assert (identities[paths[0]] == identities[paths[1]]) is shared
    db._flip_pending_keyword_change(photos[0], 'Robin', 'keyword_add', 'keyword_remove')
    own = [row['change_type'] for row in db.pending_changes.list_all() if row['photo_id'] == photos[0]]
    assert own == (['keyword_remove'] if shared else [])


# -- discovery and deletion of a photo's existing sidecars ---------------------


def _write(path, text='<x/>'):
    path.write_text(text)
    return path


def test_photo_sidecars_lists_both_naming_styles_once(tmp_path):
    from sidecar_identity import photo_sidecars, stem_sidecar

    photo = tmp_path / 'IMG_1.CR3'
    stem_style = _write(tmp_path / 'IMG_1.XMP')
    full_style = _write(tmp_path / 'IMG_1.CR3.xmp')

    found = photo_sidecars(str(photo))

    # A case-folding directory answers the ``.xmp`` probe for ``IMG_1.XMP``;
    # each file is still listed exactly once.
    assert len(found) == 2
    assert os.path.samefile(found[0], stem_style)
    assert os.path.samefile(found[1], full_style)
    assert os.path.samefile(stem_sidecar(str(photo)), stem_style)
    assert stem_sidecar(str(tmp_path / 'other.jpg')) is None


def test_photo_sidecars_keep_case_variants_apart_in_case_sensitive_directory(tmp_path):
    from sidecar_identity import photo_sidecars, stem_sidecar

    _write(tmp_path / 'IMG_1.xmp', 'lower')
    if directory_folds_case(str(tmp_path)):
        pytest.skip('case-folding directory: IMG_1.XMP is IMG_1.xmp')
    _write(tmp_path / 'IMG_1.XMP', 'upper')

    found = photo_sidecars(str(tmp_path / 'IMG_1.jpg'))

    assert [os.path.basename(p) for p in found] == ['IMG_1.xmp', 'IMG_1.XMP']
    assert os.path.basename(stem_sidecar(str(tmp_path / 'IMG_1.jpg'))) == 'IMG_1.xmp'


def test_delete_orphaned_sidecars_keeps_sidecar_another_file_owns(tmp_path):
    from sidecar_identity import delete_orphaned_sidecars

    # The RAW is gone; its JPEG twin survives and shares IMG_1.xmp.
    (tmp_path / 'IMG_1.JPG').write_bytes(b'jpeg')
    shared = _write(tmp_path / 'IMG_1.xmp')
    own = _write(tmp_path / 'IMG_1.CR3.xmp')
    # A second missing photo whose sidecar nothing else owns.
    lone = _write(tmp_path / 'IMG_2.XMP')

    cleaned, skipped = delete_orphaned_sidecars([
        str(tmp_path / 'IMG_1.CR3'),
        str(tmp_path / 'IMG_2.CR3'),
        str(tmp_path / 'IMG_3.CR3'),  # no sidecar at all
    ])

    assert (cleaned, skipped) == (2, 1)
    assert shared.exists()
    assert not own.exists()
    assert not lone.exists()


def test_delete_orphaned_sidecars_treats_restored_original_as_owner(tmp_path):
    from sidecar_identity import delete_orphaned_sidecars

    (tmp_path / 'bird.jpg').write_bytes(b'jpeg')
    sidecar = _write(tmp_path / 'bird.xmp')

    assert delete_orphaned_sidecars([str(tmp_path / 'bird.jpg')]) == (0, 1)
    assert sidecar.exists()


def test_delete_orphaned_sidecars_deletes_nothing_in_unlistable_directory(tmp_path, monkeypatch):
    import sidecar_identity

    sidecar = _write(tmp_path / 'ghost.xmp')

    def fail(_path):
        raise PermissionError('denied')

    monkeypatch.setattr(sidecar_identity.os, 'scandir', fail)

    assert sidecar_identity.delete_orphaned_sidecars([str(tmp_path / 'ghost.NEF')]) == (0, 1)
    assert sidecar.exists()


def test_directory_named_like_the_owner_does_not_own_sidecar(tmp_path):
    from sidecar_identity import sidecar_has_owner

    (tmp_path / 'ghost').mkdir()
    sidecar = _write(tmp_path / 'ghost.xmp')

    assert not sidecar_has_owner(str(sidecar))
