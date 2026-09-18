"""Tests for the consolidated vireo.xmp module."""

import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest
from xmp import (
    read_hierarchical_keywords,
    read_keywords,
    read_sync_preview_metadata,
    remove_keywords,
    remove_vireo_gps_location,
    write_edit_recipe,
    write_gps_location,
    write_pick_flag,
    write_rating,
    write_sidecar,
)

# ── Fixtures ────────────────────────────────────────────────────────────

SAMPLE_XMP = """\
<?xml version='1.0' encoding='utf-8'?>
<x:xmpmeta xmlns:x="adobe:ns:meta/">
  <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">
    <rdf:Description crs:Version="15.1" xmlns:crs="http://ns.adobe.com/camera-raw-settings/1.0/">
      <dc:subject xmlns:dc="http://purl.org/dc/elements/1.1/">
        <rdf:Bag>
          <rdf:li>Bird</rdf:li>
          <rdf:li>Raptor</rdf:li>
        </rdf:Bag>
      </dc:subject>
      <lr:hierarchicalSubject xmlns:lr="http://ns.adobe.com/lightroom/1.0/">
        <rdf:Bag>
          <rdf:li>Animals|Birds|Raptor</rdf:li>
          <rdf:li>Location|Forest</rdf:li>
        </rdf:Bag>
      </lr:hierarchicalSubject>
    </rdf:Description>
  </rdf:RDF>
</x:xmpmeta>"""


@pytest.fixture
def sample_xmp(tmp_path):
    """Create a sample XMP file and return its path."""
    p = tmp_path / "photo.xmp"
    p.write_text(SAMPLE_XMP)
    return str(p)


@pytest.fixture
def missing_xmp(tmp_path):
    """Return a path to a non-existent XMP file."""
    return str(tmp_path / "does_not_exist.xmp")


@pytest.mark.parametrize("writer,args", [
    (write_sidecar, ({"Eagle"}, set())),
    (write_rating, (5,)),
    (write_pick_flag, ("flagged",)),
    (write_gps_location, (10, 20)),
    (remove_vireo_gps_location, ()),
    (write_edit_recipe, ('{"exposure":1}',)),
    (remove_keywords, ({"Bird"},)),
])
@pytest.mark.parametrize("failure", ["serialize", "replace"])
def test_failed_sidecar_update_preserves_original(sample_xmp, monkeypatch, writer, args, failure):
    import xmp

    write_gps_location(sample_xmp, 30, 40)
    path = Path(sample_xmp)
    original = path.read_bytes()

    def failed_serializer(write, *args, **kwargs):
        write("<partial")
        raise OSError(28, "disk full")

    def failed_replace(*args):
        raise OSError("replacement failed")

    if failure == "serialize":
        monkeypatch.setitem(xmp.ET._serialize, "xml", failed_serializer)
    else:
        monkeypatch.setattr(xmp.os, "replace", failed_replace)
    with pytest.raises(OSError):
        writer(sample_xmp, *args)

    assert path.read_bytes() == original
    assert set(path.parent.iterdir()) == {path}


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX permission bits and symlinks")
def test_atomic_sidecar_update_preserves_mode_and_symlink(sample_xmp, tmp_path):
    import stat

    path = Path(sample_xmp)
    path.chmod(0o640)
    link = tmp_path / "linked.xmp"
    link.symlink_to(path)
    write_rating(link, 5)
    assert link.is_symlink()
    assert stat.S_IMODE(path.stat().st_mode) == 0o640
    assert read_sync_preview_metadata(path)["rating"] == "5"


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX umask")
def test_new_sidecar_uses_normal_creation_permissions(tmp_path):
    import stat

    reference = tmp_path / "reference.txt"
    reference.write_text("normal file")
    sidecar = tmp_path / "photo.xmp"
    write_sidecar(sidecar, {"Bird"}, set())
    assert stat.S_IMODE(sidecar.stat().st_mode) == stat.S_IMODE(reference.stat().st_mode)


# ── read_keywords ───────────────────────────────────────────────────────

def test_read_keywords_normal(sample_xmp):
    result = read_keywords(sample_xmp)
    assert result == {"Bird", "Raptor"}


def test_read_keywords_missing_file(missing_xmp):
    result = read_keywords(missing_xmp)
    assert result == set()


def test_read_keywords_corrupt_file(tmp_path):
    p = tmp_path / "corrupt.xmp"
    p.write_text("<<<not valid xml>>>")
    result = read_keywords(str(p))
    assert result == set()


# ── read_hierarchical_keywords ──────────────────────────────────────────

def test_read_hierarchical_keywords_normal(sample_xmp):
    result = read_hierarchical_keywords(sample_xmp)
    assert set(result) == {"Animals|Birds|Raptor", "Location|Forest"}


def test_read_hierarchical_keywords_missing_file(missing_xmp):
    result = read_hierarchical_keywords(missing_xmp)
    assert result == []


# ── write_sidecar ───────────────────────────────────────────────────────

def test_write_sidecar_new_file(tmp_path):
    p = str(tmp_path / "new.xmp")
    write_sidecar(p, {"Eagle", "Hawk"}, {"Animals|Birds|Eagle"})

    # Verify written keywords are readable
    assert read_keywords(p) == {"Eagle", "Hawk"}
    assert "Animals|Birds|Eagle" in read_hierarchical_keywords(p)


def test_write_sidecar_merge_existing(sample_xmp):
    # Merge new keywords into existing file
    write_sidecar(sample_xmp, {"Eagle"}, {"Animals|Birds|Eagle"})

    # Original keywords should still be present
    kw = read_keywords(sample_xmp)
    assert "Bird" in kw
    assert "Raptor" in kw
    assert "Eagle" in kw

    hier = read_hierarchical_keywords(sample_xmp)
    assert "Animals|Birds|Raptor" in hier
    assert "Animals|Birds|Eagle" in hier

    # crs:Version attribute should be preserved
    with open(sample_xmp) as f:
        content = f.read()
    assert "crs:Version" in content


# ── write_rating ────────────────────────────────────────────────────────

def test_write_rating_normal(sample_xmp):
    write_rating(sample_xmp, 4)

    with open(sample_xmp) as f:
        content = f.read()
    assert 'xmp:Rating="4"' in content


def test_write_rating_no_file(missing_xmp):
    # Should be a no-op, not raise
    write_rating(missing_xmp, 3)
    assert not os.path.exists(missing_xmp)


# ── write_pick_flag ─────────────────────────────────────────────────────

def test_write_pick_flag_existing_sidecar(sample_xmp):
    write_pick_flag(sample_xmp, "flagged")

    with open(sample_xmp) as f:
        content = f.read()
    assert 'xmpDM:pick="1"' in content


def test_write_pick_flag_rejected_creates_sidecar(missing_xmp):
    write_pick_flag(missing_xmp, "rejected")

    with open(missing_xmp) as f:
        content = f.read()
    assert 'xmpDM:pick="-1"' in content


# ── write_gps_location / remove_vireo_gps_location ──────────────────────

def test_write_gps_location_writes_exif_gps(sample_xmp):
    write_gps_location(sample_xmp, 48.8566, 2.3522)

    with open(sample_xmp) as f:
        content = f.read()
    assert 'exif:GPSLatitude="48,51.396000N"' in content
    assert 'exif:GPSLongitude="2,21.132000E"' in content
    assert 'exif:GPSMapDatum="WGS-84"' in content
    assert 'vireo:gpsSource="assigned"' in content


def test_remove_vireo_gps_location_only_when_marked(sample_xmp):
    write_gps_location(sample_xmp, 48.8566, 2.3522)

    assert remove_vireo_gps_location(sample_xmp) is True

    with open(sample_xmp) as f:
        content = f.read()
    assert "GPSLatitude" not in content
    assert "GPSLongitude" not in content
    assert "vireo:gpsSource" not in content


def test_remove_vireo_gps_location_preserves_unmarked_gps(sample_xmp):
    from xml.etree import ElementTree as ET

    ns_rdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
    ns_exif = "http://ns.adobe.com/exif/1.0/"
    tree = ET.parse(sample_xmp)
    desc = tree.getroot().find(f".//{{{ns_rdf}}}Description")
    desc.set(f"{{{ns_exif}}}GPSLatitude", "48,51.396000N")
    desc.set(f"{{{ns_exif}}}GPSLongitude", "2,21.132000E")
    tree.write(sample_xmp, xml_declaration=True, encoding="unicode")

    assert remove_vireo_gps_location(sample_xmp) is False

    with open(sample_xmp) as f:
        content = f.read()
    assert 'exif:GPSLatitude="48,51.396000N"' in content
    assert 'exif:GPSLongitude="2,21.132000E"' in content


def test_remove_vireo_gps_location_restores_previous_gps(sample_xmp):
    from xml.etree import ElementTree as ET

    ns_rdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
    ns_exif = "http://ns.adobe.com/exif/1.0/"
    tree = ET.parse(sample_xmp)
    desc = tree.getroot().find(f".//{{{ns_rdf}}}Description")
    desc.set(f"{{{ns_exif}}}GPSLatitude", "40,46.974000N")
    desc.set(f"{{{ns_exif}}}GPSLongitude", "73,57.924000W")
    tree.write(sample_xmp, xml_declaration=True, encoding="unicode")

    write_gps_location(sample_xmp, 48.8566, 2.3522)
    assert remove_vireo_gps_location(sample_xmp) is True

    with open(sample_xmp) as f:
        content = f.read()
    assert 'exif:GPSLatitude="40,46.974000N"' in content
    assert 'exif:GPSLongitude="73,57.924000W"' in content
    assert "previousGPSLatitude" not in content
    assert "previousGPSLongitude" not in content
    assert "vireo:gpsSource" not in content


def test_read_sync_preview_metadata_reports_current_and_previous_gps(sample_xmp):
    """Sync review gets decimal before/after inputs from one sidecar parse."""
    from xml.etree import ElementTree as ET

    ns_rdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
    ns_exif = "http://ns.adobe.com/exif/1.0/"
    tree = ET.parse(sample_xmp)
    desc = tree.getroot().find(f".//{{{ns_rdf}}}Description")
    desc.set(f"{{{ns_exif}}}GPSLatitude", "40,46.974000N")
    desc.set(f"{{{ns_exif}}}GPSLongitude", "73,57.924000W")
    tree.write(sample_xmp, xml_declaration=True, encoding="unicode")
    write_gps_location(sample_xmp, 48.8566, 2.3522, source="keyword")
    write_rating(sample_xmp, 4)
    write_pick_flag(sample_xmp, "flagged")

    metadata = read_sync_preview_metadata(sample_xmp)

    assert metadata["status"] == "ok"
    assert metadata["keywords"] == {"Bird", "Raptor"}
    assert metadata["hierarchical_keywords"] == {
        "Animals|Birds|Raptor",
        "Location|Forest",
    }
    assert metadata["rating"] == "4"
    assert metadata["rating_writable"] is True
    assert metadata["flag"] == "flagged"
    assert metadata["location"]["latitude"] == pytest.approx(48.8566)
    assert metadata["location"]["longitude"] == pytest.approx(2.3522)
    assert metadata["previous_location"]["latitude"] == pytest.approx(40.7829)
    assert metadata["previous_location"]["longitude"] == pytest.approx(-73.9654)
    assert metadata["location_source"] == "keyword"


def test_read_sync_preview_metadata_distinguishes_missing_and_unreadable(tmp_path):
    missing = read_sync_preview_metadata(tmp_path / "missing.xmp")
    assert missing["status"] == "missing"
    assert missing["rating_writable"] is False

    corrupt = tmp_path / "corrupt.xmp"
    corrupt.write_text("not xml")
    assert read_sync_preview_metadata(corrupt)["status"] == "unreadable"


def test_write_gps_location_rejects_out_of_range_coords(sample_xmp):
    with pytest.raises(ValueError, match="latitude"):
        write_gps_location(sample_xmp, 91.0, 2.3522)
    with pytest.raises(ValueError, match="longitude"):
        write_gps_location(sample_xmp, 48.8566, 181.0)


# ── write_edit_recipe ───────────────────────────────────────────────────

def test_write_edit_recipe_creates_vireo_marker(missing_xmp):
    recipe_json = '{"crop":{"h":0.8,"w":0.7,"x":0.1,"y":0.1},"version":1}'

    assert write_edit_recipe(missing_xmp, recipe_json) is True

    with open(missing_xmp) as f:
        content = f.read()
    assert 'vireo:editRecipe="' in content
    assert "&quot;crop&quot;" in content
    assert 'vireo:editRecipeSchema="1"' in content


def test_write_edit_recipe_removes_vireo_marker(missing_xmp):
    write_edit_recipe(missing_xmp, '{"rotation":90,"version":1}')

    assert write_edit_recipe(missing_xmp, "") is True

    with open(missing_xmp) as f:
        content = f.read()
    assert "vireo:editRecipe" not in content
    assert "vireo:editRecipeSchema" not in content


# ── remove_keywords ─────────────────────────────────────────────────────

def test_remove_keywords_normal(sample_xmp):
    remove_keywords(sample_xmp, {"bird"})  # case-insensitive

    kw = read_keywords(sample_xmp)
    assert "Bird" not in kw
    assert "Raptor" in kw

    # Hierarchical entry containing "Birds" segment should NOT be removed
    # because we removed "bird", not "birds"
    hier = read_hierarchical_keywords(sample_xmp)
    assert "Animals|Birds|Raptor" in hier


def test_remove_keywords_removes_hierarchical(sample_xmp):
    # "Raptor" appears as a segment in "Animals|Birds|Raptor"
    remove_keywords(sample_xmp, {"Raptor"})

    kw = read_keywords(sample_xmp)
    assert "Raptor" not in kw

    hier = read_hierarchical_keywords(sample_xmp)
    assert "Animals|Birds|Raptor" not in hier
    # "Location|Forest" should remain
    assert "Location|Forest" in hier


def test_remove_keywords_no_file(missing_xmp):
    # Should be a no-op, not raise
    remove_keywords(missing_xmp, {"Bird"})
    assert not os.path.exists(missing_xmp)


def test_remove_keywords_matches_normalized_edge_quote_variant(tmp_path):
    """Removing 'apapane' must also clear a sidecar '‘apapane' variant.

    add_keyword() normalizes on insert, so a DB tag stored as 'apapane' may
    have originated from an XMP '‘apapane'. If remove_keywords compares raw
    lowercased text, the stray-quote <rdf:li> stays in the sidecar and gets
    re-added on the next XMP import.
    """
    p = tmp_path / "photo.xmp"
    p.write_text(
        "<?xml version='1.0' encoding='utf-8'?>\n"
        "<x:xmpmeta xmlns:x=\"adobe:ns:meta/\">\n"
        "  <rdf:RDF xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\">\n"
        "    <rdf:Description>\n"
        "      <dc:subject xmlns:dc=\"http://purl.org/dc/elements/1.1/\">\n"
        "        <rdf:Bag>\n"
        "          <rdf:li>‘apapane</rdf:li>\n"
        "          <rdf:li>Raptor</rdf:li>\n"
        "        </rdf:Bag>\n"
        "      </dc:subject>\n"
        "    </rdf:Description>\n"
        "  </rdf:RDF>\n"
        "</x:xmpmeta>\n",
        encoding="utf-8",
    )
    remove_keywords(str(p), {"apapane"})

    kw = read_keywords(str(p))
    assert "‘apapane" not in kw
    assert "Raptor" in kw


def test_remove_keywords_matches_hierarchical_edge_quote_segment(tmp_path):
    """Hierarchical segments must be matched using the normalized key too."""
    p = tmp_path / "photo.xmp"
    p.write_text(
        "<?xml version='1.0' encoding='utf-8'?>\n"
        "<x:xmpmeta xmlns:x=\"adobe:ns:meta/\">\n"
        "  <rdf:RDF xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\">\n"
        "    <rdf:Description>\n"
        "      <lr:hierarchicalSubject xmlns:lr=\"http://ns.adobe.com/lightroom/1.0/\">\n"
        "        <rdf:Bag>\n"
        "          <rdf:li>Birds|‘apapane</rdf:li>\n"
        "          <rdf:li>Location|Forest</rdf:li>\n"
        "        </rdf:Bag>\n"
        "      </lr:hierarchicalSubject>\n"
        "    </rdf:Description>\n"
        "  </rdf:RDF>\n"
        "</x:xmpmeta>\n",
        encoding="utf-8",
    )
    remove_keywords(str(p), {"apapane"})

    hier = read_hierarchical_keywords(str(p))
    assert "Birds|‘apapane" not in hier
    assert "Location|Forest" in hier


def test_remove_keywords_ignores_empty_normalized_input(sample_xmp):
    """A removal request whose only entry normalizes to empty must not
    accidentally match empty hierarchical segments (e.g. `"|Birds|"` splits
    into `["", "Birds", ""]`) and it must not blow away every keyword.
    """
    remove_keywords(sample_xmp, {"'"})

    assert read_keywords(sample_xmp) == {"Bird", "Raptor"}
    hier = read_hierarchical_keywords(sample_xmp)
    assert set(hier) == {"Animals|Birds|Raptor", "Location|Forest"}


def test_remove_keywords_flat_only_preserves_hierarchies(sample_xmp):
    """Flat-only mode strips dc:subject matches but leaves hierarchies alone.

    Regression test for the sync path: when the sync engine canonicalizes
    sidecar variants for a queued keyword_add (e.g. removing a legacy
    `‘apapane` before writing the clean `apapane`), it must not delete an
    unrelated hierarchy that happens to share the added keyword as one of
    its segments. Using the default full-semantics removal would drop
    `Animals|Birds|Raptor` when the caller "removes" flat `Raptor`.
    """
    remove_keywords(sample_xmp, {"Raptor"}, hierarchical=False)

    kw = read_keywords(sample_xmp)
    assert "Raptor" not in kw
    hier = read_hierarchical_keywords(sample_xmp)
    assert "Animals|Birds|Raptor" in hier
    assert "Location|Forest" in hier


@pytest.mark.parametrize("writer,args", [
    (write_sidecar, ({"Eagle"}, set())),
    (write_rating, (5,)),
    (write_pick_flag, ("flagged",)),
    (write_gps_location, (10, 20)),
    (remove_vireo_gps_location, ()),
    (write_edit_recipe, ('{"exposure":1}',)),
    (remove_keywords, ({"Bird"},)),
])
def test_sidecar_writers_respect_read_only_files(sample_xmp, writer, args):
    write_gps_location(sample_xmp, 30, 40)
    path = Path(sample_xmp)
    original = path.read_bytes()
    path.chmod(0o444)
    try:
        if os.access(path, os.W_OK):
            pytest.skip("current user can bypass read-only permissions")
        with pytest.raises(PermissionError):
            writer(sample_xmp, *args)
        assert path.read_bytes() == original
        assert set(path.parent.iterdir()) == {path}
    finally:
        path.chmod(0o644)


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX extended attributes")
def test_atomic_sidecar_preserves_extended_attributes(sample_xmp):
    name = "com.vireo.test" if sys.platform == "darwin" else "user.vireo-test"
    if sys.platform == "darwin":
        import subprocess

        subprocess.run(["xattr", "-w", name, "shared metadata", sample_xmp], check=True)
    else:
        os.setxattr(sample_xmp, name, b"shared metadata")
    before = os.stat(sample_xmp)
    write_rating(sample_xmp, 5)
    after = os.stat(sample_xmp)
    if sys.platform == "darwin":
        value = subprocess.check_output(["xattr", "-p", name, sample_xmp]).strip()
    else:
        value = os.getxattr(sample_xmp, name)
    assert value == b"shared metadata"
    assert (after.st_uid, after.st_gid) == (before.st_uid, before.st_gid)
    assert read_sync_preview_metadata(sample_xmp)["rating"] == "5"


@pytest.mark.skipif(os.name != "posix", reason="POSIX group ownership")
def test_atomic_sidecar_preserves_nondefault_group(sample_xmp):
    original_gid = os.stat(sample_xmp).st_gid
    group = next((gid for gid in os.getgroups() if gid != original_gid), None)
    if group is None:
        pytest.skip("current user has no supplementary group")
    os.chown(sample_xmp, -1, group)
    write_rating(sample_xmp, 5)
    assert os.stat(sample_xmp).st_gid == group


@pytest.mark.skipif(sys.platform != "darwin", reason="macOS ACL API")
def test_atomic_sidecar_preserves_macos_acl(sample_xmp):
    import subprocess

    subprocess.run(["chmod", "+a", "group:everyone allow read", sample_xmp], check=True)

    def acl():
        return subprocess.check_output(["ls", "-le", sample_xmp], text=True).splitlines()[1:]

    before = acl()
    assert before
    write_rating(sample_xmp, 5)
    assert acl() == before
    assert read_sync_preview_metadata(sample_xmp)["rating"] == "5"


@pytest.mark.skipif(sys.platform != "linux", reason="Linux POSIX ACL attributes")
def test_atomic_sidecar_preserves_linux_acl(sample_xmp):
    import struct

    # Linux POSIX ACL v2: owner, named user, owning group, mask, other.
    entries = [(1, 6, 0xFFFFFFFF), (2, 4, 65534), (4, 4, 0xFFFFFFFF),
               (16, 4, 0xFFFFFFFF), (32, 0, 0xFFFFFFFF)]
    acl = struct.pack("<I", 2) + b"".join(struct.pack("<HHI", *e) for e in entries)
    os.setxattr(sample_xmp, "system.posix_acl_access", acl)
    write_rating(sample_xmp, 5)
    assert os.getxattr(sample_xmp, "system.posix_acl_access") == acl
    assert read_sync_preview_metadata(sample_xmp)["rating"] == "5"


def test_failed_access_metadata_copy_preserves_sidecar(sample_xmp, monkeypatch):
    import xmp

    path = Path(sample_xmp)
    original = path.read_bytes()

    def fail_copy(*args):
        raise PermissionError("cannot preserve ownership or ACL")

    monkeypatch.setattr(xmp, "_preserve_sidecar_access", fail_copy)
    with pytest.raises(PermissionError):
        write_rating(sample_xmp, 5)
    assert path.read_bytes() == original
    assert set(path.parent.iterdir()) == {path}


@pytest.mark.skipif(sys.platform != "darwin", reason="macOS ACL API")
def test_atomic_sidecar_preserves_xattrs_under_writeextattr_deny(sample_xmp):
    """The ordering rule, exercised against a real deny-writeextattr ACL.

    The stub test below pins the call order everywhere; this one proves the
    OS behaviour the order exists for. With the ACL applied first, setxattr
    against the temp file returns EACCES, ``_copy_xattrs`` skips the
    attribute as non-critical, and the published sidecar silently loses the
    source's metadata.
    """
    import subprocess

    subprocess.run(["xattr", "-w", "com.vireo.test", "shared metadata",
                    sample_xmp], check=True)
    user = subprocess.check_output(["id", "-un"], text=True).strip()
    subprocess.run(["chmod", "+a", f"{user} deny writeextattr", sample_xmp],
                   check=True)
    try:
        subprocess.run(["xattr", "-w", "com.vireo.probe", "x", sample_xmp],
                       check=True, capture_output=True)
    except subprocess.CalledProcessError:
        pass
    else:
        # The deny entry did not bind. Skip rather than assert a guarantee
        # this machine is not actually providing.
        subprocess.run(["xattr", "-d", "com.vireo.probe", sample_xmp],
                       check=False)
        pytest.skip("deny writeextattr does not bind this user")

    write_rating(sample_xmp, 5)

    value = subprocess.check_output(
        ["xattr", "-p", "com.vireo.test", sample_xmp], text=True).strip()
    assert value == "shared metadata"
    acl = subprocess.check_output(["ls", "-le", sample_xmp], text=True)
    assert "deny writeextattr" in acl
    assert read_sync_preview_metadata(sample_xmp)["rating"] == "5"


def test_preserve_sidecar_access_copies_xattrs_before_acl_on_darwin(
    monkeypatch, tmp_path,
):
    """Non-ACL xattrs must be copied before the source ACL is applied.

    A macOS ACL can allow file-data writes but deny writeextattr. Applying
    the ACL first would then make ``setxattr`` on the destination return
    EACCES; ``_copy_xattrs`` would skip the (non-critical) source metadata,
    and ``os.replace`` would publish a sidecar without it. Portable because
    every darwin-specific dependency is patched out.
    """
    import xmp

    source = tmp_path / "source"
    source.write_bytes(b"src")
    destination = tmp_path / "destination"
    destination.write_bytes(b"dst")

    events = []

    def fake_copy_xattrs(src, dst, *rest):
        events.append("xattrs")

    class _FakeCopyfile:
        argtypes = None
        restype = None

        def __call__(self, *args, **kwargs):
            events.append("acl")
            return 0

    class _FakeCDLL:
        copyfile = _FakeCopyfile()

    class _FakeCtypes:
        c_char_p = object()
        c_void_p = object()
        c_uint32 = object()
        c_int = object()

        @staticmethod
        def CDLL(name, use_errno=False):
            return _FakeCDLL()

        @staticmethod
        def get_errno():
            return 0

    monkeypatch.setattr(xmp, "_copy_xattrs", fake_copy_xattrs)
    monkeypatch.setattr(xmp, "_darwin_xattr", lambda: (_FakeCtypes, None))
    # Force the darwin branch even off macOS so this is testable everywhere.
    monkeypatch.setattr(xmp.sys, "platform", "darwin")

    xmp._preserve_sidecar_access(source, destination, source.stat())
    assert events == ["xattrs", "acl"], events


# ── Extended-attribute preservation ─────────────────────────────────────

def _fake_xattr_store(source_attrs, refuse=None, refuse_errno=None):
    """Build list/get/set/remove callables over in-memory xattr dicts."""
    import errno as errno_module

    store = {"source": dict(source_attrs), "dest": {}}

    def which(path):
        return store[path]

    def list_xattrs(path):
        return list(which(path))

    def get_xattr(path, name):
        return which(path)[name]

    def set_xattr(path, name, value):
        if name == refuse:
            code = refuse_errno or errno_module.EACCES
            raise OSError(code, os.strerror(code), str(path))
        which(path)[name] = value

    def remove_xattr(path, name):
        which(path).pop(name, None)

    return store, list_xattrs, get_xattr, set_xattr, remove_xattr


def test_copy_xattrs_skips_attributes_no_process_may_set():
    """A kernel-owned xattr must not sink the whole sidecar write.

    macOS stamps ``com.apple.provenance`` on written files and refuses every
    attempt to set it. Copying attributes as a block made each already-written
    sidecar on an SMB share permanently unwritable.
    """
    import xmp

    store, *api = _fake_xattr_store(
        {"com.apple.provenance": b"", "com.vireo.keep": b"value"},
        refuse="com.apple.provenance",
    )
    xmp._copy_xattrs("source", "dest", *api)
    assert store["dest"] == {"com.vireo.keep": b"value"}


def test_copy_xattrs_raises_on_errors_that_are_not_structural():
    """A full disk is a real failure — do not publish a half-copied sidecar."""
    import errno

    import xmp

    _, *api = _fake_xattr_store(
        {"com.vireo.keep": b"value"}, refuse="com.vireo.keep",
        refuse_errno=errno.ENOSPC,
    )
    with pytest.raises(OSError):
        xmp._copy_xattrs("source", "dest", *api)


def test_copy_xattrs_never_skips_access_control_attributes():
    """Linux stores ACLs as xattrs; dropping one would widen access."""
    import xmp

    _, *api = _fake_xattr_store(
        {"system.posix_acl_access": b"acl"}, refuse="system.posix_acl_access",
    )
    with pytest.raises(PermissionError):
        xmp._copy_xattrs("source", "dest", *api)


@pytest.mark.parametrize(
    "attribute",
    ["system.nfs4_acl", "system.richacl", "security.selinux"],
)
def test_copy_xattrs_never_skips_non_posix_acl_namespaces(attribute):
    """NFSv4/RichACL/security xattrs are access control too.

    The Linux ``system.`` namespace is reserved for kernel-managed access
    control -- ``system.nfs4_acl`` on NFSv4 mounts, ``system.richacl`` on
    RichACL mounts. If ``setxattr`` returns EACCES/EPERM/ENOTSUP, skipping
    the attribute would publish a sidecar with weaker access than the
    original; the previous all-or-nothing copy aborted in that case.
    """
    import xmp

    _, *api = _fake_xattr_store({attribute: b"acl"}, refuse=attribute)
    with pytest.raises(PermissionError):
        xmp._copy_xattrs("source", "dest", *api)


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX extended attributes")
def test_sidecar_update_survives_unsettable_source_xattr(sample_xmp, monkeypatch):
    """End to end: an uncopyable source attribute still rewrites the sidecar."""
    import errno

    import xmp

    name = "com.apple.provenance"
    if sys.platform == "darwin":
        target, list_fn, get_fn, set_fn = (
            xmp, "_darwin_list_xattrs", "_darwin_get_xattr", "_darwin_set_xattr",
        )
    else:
        target, list_fn, get_fn, set_fn = (
            os, "listxattr", "getxattr", "setxattr",
        )
    real_list = getattr(target, list_fn)
    real_get = getattr(target, get_fn)
    real_set = getattr(target, set_fn)

    def listing(path, *args, **kwargs):
        names = list(real_list(path, *args, **kwargs))
        return names + [name] if str(path) == str(sample_xmp) else names

    def reading(path, attr, *args, **kwargs):
        if attr == name:
            return b""
        return real_get(path, attr, *args, **kwargs)

    def writing(path, attr, value, *args, **kwargs):
        if attr == name:
            raise OSError(errno.EACCES, "Permission denied", str(path))
        return real_set(path, attr, value, *args, **kwargs)

    monkeypatch.setattr(target, list_fn, listing)
    monkeypatch.setattr(target, get_fn, reading)
    monkeypatch.setattr(target, set_fn, writing)

    write_rating(sample_xmp, 5)
    assert read_sync_preview_metadata(sample_xmp)["rating"] == "5"


# ── SidecarEditor: one publish per sidecar ──────────────────────────────

def _count_publishes(monkeypatch):
    """Record every sidecar publish, delegating to the real writer."""
    import xmp

    published = []
    original = xmp._write_tree_atomic

    def counting(tree, xmp_path):
        published.append(str(xmp_path))
        return original(tree, xmp_path)

    monkeypatch.setattr(xmp, "_write_tree_atomic", counting)
    return published


def test_editor_publishes_every_mutation_in_one_write(sample_xmp, monkeypatch):
    """Keywords, flag, GPS, recipe and rating cost one publish, not five."""
    from xmp import SidecarEditor

    published = _count_publishes(monkeypatch)
    editor = SidecarEditor(sample_xmp)
    editor.remove_keywords({"Bird"})
    editor.add_keywords({"Eagle"}, {"Animals|Birds|Eagle"})
    editor.set_pick_flag("flagged")
    editor.set_gps_location(48.8566, 2.3522)
    editor.set_edit_recipe('{"exposure":1}')
    editor.set_rating(4)
    assert editor.commit() is True

    assert len(published) == 1
    metadata = read_sync_preview_metadata(sample_xmp)
    assert metadata["keywords"] == {"Raptor", "Eagle"}
    assert "Animals|Birds|Eagle" in metadata["hierarchical_keywords"]
    assert metadata["flag"] == "flagged"
    assert metadata["rating"] == "4"
    assert metadata["location_source"] == "assigned"
    assert metadata["edit_recipe"] == '{"exposure":1}'


def test_editor_skips_the_write_when_nothing_changed(sample_xmp, monkeypatch):
    """Re-applying metadata the sidecar already carries publishes nothing."""
    from xmp import SidecarEditor

    write_rating(sample_xmp, 4)
    write_pick_flag(sample_xmp, "flagged")
    original = Path(sample_xmp).read_bytes()

    published = _count_publishes(monkeypatch)
    editor = SidecarEditor(sample_xmp)
    editor.add_keywords({"Bird", "Raptor"}, {"Location|Forest"})
    editor.set_pick_flag("flagged")
    editor.set_rating(4)
    editor.remove_keywords({"Nothing here"})
    assert editor.commit() is False

    assert published == []
    assert Path(sample_xmp).read_bytes() == original


def test_editor_keeps_the_canonical_spelling_while_stripping_variants(tmp_path):
    """``keep_exact`` removes only the variants of a keyword, not the keyword."""
    from xmp import SidecarEditor

    path = tmp_path / "photo.xmp"
    path.write_text(SAMPLE_XMP.replace(
        "<rdf:li>Bird</rdf:li>",
        "<rdf:li>Bird</rdf:li>\n          <rdf:li>‘Bird</rdf:li>",
    ), encoding="utf-8")
    editor = SidecarEditor(str(path))
    assert editor.remove_keywords({"Bird"}, hierarchical=False, keep_exact=True)
    editor.commit()

    assert read_keywords(path) == {"Bird", "Raptor"}


def test_editor_rating_persists_into_a_sidecar_the_same_batch_creates(missing_xmp):
    """A rating alone creates nothing, but rides along when a flag does."""
    from xmp import SidecarEditor

    editor = SidecarEditor(missing_xmp)
    editor.set_rating(5)
    assert editor.commit() is False
    assert not os.path.exists(missing_xmp)

    editor = SidecarEditor(missing_xmp)
    editor.set_pick_flag("rejected")
    editor.set_rating(5)
    assert editor.commit() is True

    metadata = read_sync_preview_metadata(missing_xmp)
    assert metadata["rating"] == "5"
    assert metadata["flag"] == "rejected"


def test_editor_removals_leave_a_corrupt_sidecar_untouched(tmp_path):
    """Pruning operations never rebuild a sidecar they could not read."""
    from xmp import SidecarEditor

    path = tmp_path / "corrupt.xmp"
    path.write_text("<x:xmpmeta><unclosed>")
    editor = SidecarEditor(str(path))
    assert editor.remove_keywords({"Bird"}) is False
    assert editor.remove_vireo_gps_location() is False
    assert editor.set_edit_recipe("") is False
    assert editor.set_rating(3) is False
    assert editor.commit() is False

    assert path.read_text() == "<x:xmpmeta><unclosed>"


def test_repeated_writer_calls_do_not_republish_unchanged_metadata(
    sample_xmp, monkeypatch,
):
    """The module-level writers inherit the editor's no-op skip."""
    write_rating(sample_xmp, 5)
    write_gps_location(sample_xmp, 10, 20)

    published = _count_publishes(monkeypatch)
    write_rating(sample_xmp, 5)
    write_gps_location(sample_xmp, 10, 20)
    write_sidecar(sample_xmp, {"Bird"}, set())
    assert remove_keywords(sample_xmp, {"Absent"}) is None
    assert published == []

    # A real change still publishes.
    write_rating(sample_xmp, 3)
    assert len(published) == 1


# ── Location keywords ───────────────────────────────────────────────────

def test_set_location_keywords_writes_leaf_and_hierarchy(tmp_path):
    """The place goes into dc:subject, the whole chain into lr:hierarchicalSubject."""
    from xmp import SidecarEditor

    path = str(tmp_path / "photo.xmp")
    editor = SidecarEditor(path)
    editor.set_location_keywords(
        ["United States", "California", "San Carlos", "Kumeyaay Lake"]
    )
    assert editor.commit() is True

    assert read_keywords(path) == {"Kumeyaay Lake"}
    assert read_hierarchical_keywords(path) == [
        "United States|California|San Carlos|Kumeyaay Lake"
    ]
    metadata = read_sync_preview_metadata(path)
    assert metadata["location_keywords"] == (
        "United States|California|San Carlos|Kumeyaay Lake"
    )


def test_set_location_keywords_is_a_no_op_when_already_written(tmp_path):
    """Re-syncing an unchanged place costs a read, not a publish."""
    from xmp import SidecarEditor

    path = str(tmp_path / "photo.xmp")
    editor = SidecarEditor(path)
    editor.set_location_keywords(["France", "Camargue"])
    editor.commit()

    again = SidecarEditor(path)
    again.set_location_keywords(["France", "Camargue"])
    assert again.commit() is False


def test_set_location_keywords_replaces_the_previous_place(tmp_path):
    """A photo moved to another place keeps only its current location keywords."""
    from xmp import SidecarEditor

    path = str(tmp_path / "photo.xmp")
    editor = SidecarEditor(path)
    editor.add_keywords(flat_keywords={"House finch"})
    editor.set_location_keywords(["United States", "California", "Kumeyaay Lake"])
    editor.commit()

    moved = SidecarEditor(path)
    moved.set_location_keywords(["France", "Camargue", "Pont de Gau"])
    assert moved.commit() is True

    assert read_keywords(path) == {"House finch", "Pont de Gau"}
    assert read_hierarchical_keywords(path) == ["France|Camargue|Pont de Gau"]


def test_set_location_keywords_canonicalizes_a_case_variant(tmp_path):
    """A sidecar spelling of the place is rewritten, not duplicated."""
    from xmp import SidecarEditor

    path = str(tmp_path / "photo.xmp")
    write_sidecar(path, flat_keywords={"kumeyaay lake"}, hierarchical_keywords=set())

    editor = SidecarEditor(path)
    editor.set_location_keywords(["United States", "Kumeyaay Lake"])
    editor.commit()

    assert read_keywords(path) == {"Kumeyaay Lake"}


def test_remove_vireo_location_keywords_leaves_user_keywords_alone(tmp_path):
    """Only the entries recorded by Vireo's own write are removed."""
    from xmp import SidecarEditor

    path = str(tmp_path / "photo.xmp")
    editor = SidecarEditor(path)
    editor.add_keywords(
        flat_keywords={"House finch", "Backyard"},
        hierarchical_keywords={"Places|Backyard"},
    )
    editor.set_location_keywords(["United States", "Kumeyaay Lake"])
    editor.commit()

    cleanup = SidecarEditor(path)
    assert cleanup.remove_vireo_location_keywords() is True
    cleanup.commit()

    assert read_keywords(path) == {"House finch", "Backyard"}
    assert read_hierarchical_keywords(path) == ["Places|Backyard"]
    assert read_sync_preview_metadata(path)["location_keywords"] is None


def test_remove_vireo_location_keywords_without_a_marker_is_a_no_op(tmp_path):
    """A place the user typed in Lightroom is not Vireo's to remove."""
    from xmp import SidecarEditor

    path = str(tmp_path / "photo.xmp")
    write_sidecar(
        path,
        flat_keywords={"Kumeyaay Lake"},
        hierarchical_keywords={"United States|Kumeyaay Lake"},
    )

    editor = SidecarEditor(path)
    assert editor.remove_vireo_location_keywords() is False
    assert editor.commit() is False
    assert read_keywords(path) == {"Kumeyaay Lake"}


def test_set_location_keywords_with_an_empty_chain_removes_them(tmp_path):
    """An unset location takes the same removal path as the disabled setting."""
    from xmp import SidecarEditor

    path = str(tmp_path / "photo.xmp")
    editor = SidecarEditor(path)
    editor.set_location_keywords(["United States", "Kumeyaay Lake"])
    editor.commit()

    cleared = SidecarEditor(path)
    cleared.set_location_keywords([])
    cleared.commit()

    assert read_keywords(path) == set()
    assert read_hierarchical_keywords(path) == []


def test_read_vireo_location_keywords_on_a_missing_sidecar(tmp_path):
    from xmp import read_vireo_location_keywords

    assert read_vireo_location_keywords(str(tmp_path / "nope.xmp")) is None


def test_set_location_keywords_does_not_claim_a_users_flat_leaf(tmp_path):
    """A pre-existing flat leaf the user typed is not Vireo's to remove."""
    from xmp import SidecarEditor, read_vireo_location_keywords_owned

    path = str(tmp_path / "photo.xmp")
    # The user (or Lightroom, or another Vireo keyword) already has the leaf
    # in dc:subject before Vireo assigns the place.
    write_sidecar(path, flat_keywords={"Kumeyaay Lake"}, hierarchical_keywords=set())

    editor = SidecarEditor(path)
    editor.set_location_keywords(["United States", "California", "Kumeyaay Lake"])
    editor.commit()

    # Vireo owns only the hierarchy it added; the flat entry was already there.
    assert read_vireo_location_keywords_owned(path) == "hier"

    cleanup = SidecarEditor(path)
    assert cleanup.remove_vireo_location_keywords() is True
    cleanup.commit()

    # The user's flat leaf survives; only the hierarchy Vireo wrote is removed.
    assert read_keywords(path) == {"Kumeyaay Lake"}
    assert read_hierarchical_keywords(path) == []


def test_set_location_keywords_does_not_claim_a_users_hierarchy(tmp_path):
    """A pre-existing hierarchy the user typed is not Vireo's to remove."""
    from xmp import SidecarEditor, read_vireo_location_keywords_owned

    path = str(tmp_path / "photo.xmp")
    write_sidecar(
        path,
        flat_keywords=set(),
        hierarchical_keywords={"United States|California|Kumeyaay Lake"},
    )

    editor = SidecarEditor(path)
    editor.set_location_keywords(["United States", "California", "Kumeyaay Lake"])
    editor.commit()

    assert read_vireo_location_keywords_owned(path) == "flat"

    cleanup = SidecarEditor(path)
    assert cleanup.remove_vireo_location_keywords() is True
    cleanup.commit()

    assert read_keywords(path) == set()
    assert read_hierarchical_keywords(path) == [
        "United States|California|Kumeyaay Lake"
    ]


def test_set_location_keywords_claims_neither_when_both_pre_exist(tmp_path):
    """Neither entry becomes Vireo's when the user typed both first."""
    from xmp import SidecarEditor, read_vireo_location_keywords_owned

    path = str(tmp_path / "photo.xmp")
    write_sidecar(
        path,
        flat_keywords={"Kumeyaay Lake"},
        hierarchical_keywords={"United States|California|Kumeyaay Lake"},
    )

    editor = SidecarEditor(path)
    editor.set_location_keywords(["United States", "California", "Kumeyaay Lake"])
    editor.commit()

    # Vireo still stamps the marker (so a later re-run knows the state) but
    # claims neither entry, since add_keywords() inserted nothing.
    assert read_vireo_location_keywords_owned(path) == ""

    cleanup = SidecarEditor(path)
    assert cleanup.remove_vireo_location_keywords() is True
    cleanup.commit()

    # Both user entries survive.
    assert read_keywords(path) == {"Kumeyaay Lake"}
    assert read_hierarchical_keywords(path) == [
        "United States|California|Kumeyaay Lake"
    ]


def test_set_location_keywords_marks_both_when_it_inserts_both(tmp_path):
    """A fresh sidecar gets both entries and both are Vireo's to remove."""
    from xmp import SidecarEditor, read_vireo_location_keywords_owned

    path = str(tmp_path / "photo.xmp")
    editor = SidecarEditor(path)
    editor.set_location_keywords(["United States", "California", "Kumeyaay Lake"])
    editor.commit()

    assert read_vireo_location_keywords_owned(path) == "flat,hier"


def test_set_location_keywords_preserves_ownership_on_a_no_op_rewrite(tmp_path):
    """A repeat write of the same path keeps the earlier ownership claim."""
    from xmp import SidecarEditor, read_vireo_location_keywords_owned

    path = str(tmp_path / "photo.xmp")
    editor = SidecarEditor(path)
    editor.set_location_keywords(["United States", "California", "Kumeyaay Lake"])
    editor.commit()
    assert read_vireo_location_keywords_owned(path) == "flat,hier"

    # A second call finds both entries already present -- they are the ones
    # the first call inserted -- and must not downgrade the claim.
    again = SidecarEditor(path)
    again.set_location_keywords(["United States", "California", "Kumeyaay Lake"])
    again.commit()
    assert read_vireo_location_keywords_owned(path) == "flat,hier"


def test_set_location_keywords_replaces_previous_only_removes_what_it_owned(tmp_path):
    """Moving between places does not delete a keyword the user added between."""
    from xmp import SidecarEditor

    path = str(tmp_path / "photo.xmp")
    editor = SidecarEditor(path)
    editor.set_location_keywords(["United States", "California", "Kumeyaay Lake"])
    editor.commit()

    # Between the two writes the user adds their own flat "Pont de Gau" -- the
    # future new leaf -- in Lightroom.
    write_sidecar(path, flat_keywords={"Pont de Gau"}, hierarchical_keywords=set())

    moved = SidecarEditor(path)
    moved.set_location_keywords(["France", "Camargue", "Pont de Gau"])
    moved.commit()

    # The user's "Pont de Gau" survives even though the new place shares its
    # leaf: the flat is not claimed on the new write, and a later clear won't
    # touch it.
    assert read_keywords(path) == {"Pont de Gau"}
    assert read_hierarchical_keywords(path) == ["France|Camargue|Pont de Gau"]

    cleanup = SidecarEditor(path)
    cleanup.remove_vireo_location_keywords()
    cleanup.commit()
    assert read_keywords(path) == {"Pont de Gau"}
    assert read_hierarchical_keywords(path) == []


def test_set_location_keywords_does_not_claim_a_users_flat_case_variant(tmp_path):
    """A pre-existing normalized flat variant is the user's, not Vireo's.

    The canonicalization step in ``set_location_keywords`` strips a leaf
    spelling like ``kumeyaay lake`` before ``add_keywords`` writes the
    canonical ``Kumeyaay Lake``. A snapshot taken after that removal would
    misread the canonical entry as a fresh Vireo insert, so a later clear
    or setting-toggle would delete the user's keyword under a new spelling.
    """
    from xmp import SidecarEditor, read_vireo_location_keywords_owned

    path = str(tmp_path / "photo.xmp")
    write_sidecar(path, flat_keywords={"kumeyaay lake"}, hierarchical_keywords=set())

    editor = SidecarEditor(path)
    editor.set_location_keywords(["United States", "California", "Kumeyaay Lake"])
    editor.commit()

    # Vireo added the hierarchy but not the flat leaf (it was the user's).
    assert read_vireo_location_keywords_owned(path) == "hier"
    assert read_keywords(path) == {"Kumeyaay Lake"}

    cleanup = SidecarEditor(path)
    assert cleanup.remove_vireo_location_keywords() is True
    cleanup.commit()

    # The user's flat leaf (now canonicalized) survives the removal.
    assert read_keywords(path) == {"Kumeyaay Lake"}
    assert read_hierarchical_keywords(path) == []


def test_set_location_keywords_does_not_claim_a_users_hier_case_variant(tmp_path):
    """A pre-existing normalized hierarchy variant is the user's, not Vireo's.

    ``_remove_location_keyword_entries`` matches hierarchical entries on
    normalized keys, so if Vireo claimed the canonical hierarchy it added
    beside a user's ``united states|california|kumeyaay lake`` variant, a
    later removal would strip the user's entry too.
    """
    from xmp import SidecarEditor, read_vireo_location_keywords_owned

    path = str(tmp_path / "photo.xmp")
    write_sidecar(
        path,
        flat_keywords=set(),
        hierarchical_keywords={"united states|california|kumeyaay lake"},
    )

    editor = SidecarEditor(path)
    editor.set_location_keywords(["United States", "California", "Kumeyaay Lake"])
    editor.commit()

    # Vireo added the flat leaf but not the hierarchy (it was the user's).
    assert read_vireo_location_keywords_owned(path) == "flat"

    cleanup = SidecarEditor(path)
    assert cleanup.remove_vireo_location_keywords() is True
    cleanup.commit()

    # The user's hierarchy variant survives the removal.
    assert read_keywords(path) == set()
    assert read_hierarchical_keywords(path) == [
        "united states|california|kumeyaay lake"
    ]


def test_set_location_keywords_refuses_a_name_with_a_pipe(tmp_path, caplog):
    """A location whose name contains ``|`` cannot round-trip through
    Lightroom's hierarchy delimiter, so the write is skipped rather than
    corrupted.

    ``get_or_create_text_location`` accepts any free-text name, but the
    Lightroom hierarchical-subject format and Vireo's own marker parser
    both split on ``|``. Writing a single-part place named ``Home|Cabin``
    would leave a flat keyword the cleanup step could never find (its
    marker parses as a two-level path with leaf ``Cabin``) and an import
    would misread the entry as a two-node hierarchy. There is no
    reversible encoding: Lightroom renders escape sequences literally.
    """
    import logging

    from xmp import SidecarEditor, read_vireo_location_keywords

    path = str(tmp_path / "photo.xmp")
    editor = SidecarEditor(path)
    with caplog.at_level(logging.WARNING, logger="xmp"):
        assert editor.set_location_keywords(["Home|Cabin"]) is False
    editor.commit()

    # Nothing was written -- no keywords, no marker, no ownership record.
    assert read_keywords(path) == set()
    assert read_hierarchical_keywords(path) == []
    assert read_vireo_location_keywords(path) is None
    assert any(
        "Home|Cabin" in record.getMessage() for record in caplog.records
    )


def test_set_location_keywords_refuses_a_pipe_in_an_ancestor(tmp_path):
    """Any segment containing ``|`` disqualifies the whole chain."""
    from xmp import SidecarEditor, read_vireo_location_keywords

    path = str(tmp_path / "photo.xmp")
    editor = SidecarEditor(path)
    assert editor.set_location_keywords(
        ["United|States", "California", "Kumeyaay Lake"]
    ) is False
    editor.commit()

    assert read_keywords(path) == set()
    assert read_hierarchical_keywords(path) == []
    assert read_vireo_location_keywords(path) is None


def test_remove_vireo_location_keywords_defaults_to_both_on_legacy_marker(tmp_path):
    """A sidecar missing the ownership companion is treated as pre-fix.

    Vireo shipped a first version of this feature that wrote the marker but
    no ownership record. Removal on such a sidecar must still clear both
    entries -- the pre-fix behaviour -- or an upgraded Vireo would leak
    stale keywords into every catalog that ran the old release.
    """
    from xmp import LOCATION_KEYWORDS_MARKER, SidecarEditor

    path = str(tmp_path / "photo.xmp")
    write_sidecar(
        path,
        flat_keywords={"Kumeyaay Lake"},
        hierarchical_keywords={"United States|Kumeyaay Lake"},
    )
    # Simulate a legacy write: the marker is present, the companion is not.
    editor = SidecarEditor(path)
    desc = editor._description()
    desc.set(LOCATION_KEYWORDS_MARKER, "United States|Kumeyaay Lake")
    editor._dirty = True
    editor.commit()

    cleanup = SidecarEditor(path)
    assert cleanup.remove_vireo_location_keywords() is True
    cleanup.commit()

    assert read_keywords(path) == set()
    assert read_hierarchical_keywords(path) == []
