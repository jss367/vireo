"""Import job: copy card -> archive with hash verification."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


def test_copy_and_hash_verify_roundtrip(tmp_path):
    from import_dedup import compute_file_hash
    from import_job import copy_and_hash_verify

    src = tmp_path / "card" / "DSC_0001.jpg"
    src.parent.mkdir()
    src.write_bytes(b"pixels" * 1000)
    dst = tmp_path / "archive" / "2026" / "DSC_0001.jpg"

    ok, file_hash = copy_and_hash_verify(str(src), str(dst))
    assert ok is True
    assert file_hash == compute_file_hash(str(src))
    assert dst.read_bytes() == src.read_bytes()


def test_copy_and_hash_verify_detects_corruption(tmp_path, monkeypatch):
    """A copy whose destination bytes differ must fail without deleting any
    previously verified archive file at the destination path."""
    import shutil

    from import_job import copy_and_hash_verify

    src = tmp_path / "card" / "DSC_0002.jpg"
    src.parent.mkdir()
    src.write_bytes(b"good bytes")
    dst = tmp_path / "archive" / "DSC_0002.jpg"
    dst.parent.mkdir()
    dst.write_bytes(b"existing verified archive bytes")

    real_copy2 = shutil.copy2

    def corrupting_copy2(s, d):
        real_copy2(s, d)
        with open(d, "r+b") as f:
            f.write(b"BAD")

    monkeypatch.setattr("import_job.shutil.copy2", corrupting_copy2)
    ok, file_hash = copy_and_hash_verify(str(src), str(dst))
    assert ok is False
    assert file_hash is None
    assert dst.read_bytes() == b"existing verified archive bytes"
    assert not list(dst.parent.glob(".DSC_0002.jpg.*.tmp"))
