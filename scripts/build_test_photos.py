"""Sample a small test photo set from the user's real library.

Usage:
    python scripts/build_test_photos.py --source /path/to/real/photos \\
        --dest ~/vireo-test-photos

Writes ~100 photos into `dest` with a mix intended to exercise the 9 first-cut
user-first scenarios: GPS/no-GPS for map, burst for cull, duplicates for
resolver, mix of RAW/JPEG, variety for pagination.

Only ever *reads* the source and only ever *writes* under the destination.
Idempotent: re-running skips files already present.
"""
import argparse
import hashlib
import os
import shutil
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

RAW_EXTS = {".cr2", ".cr3", ".nef", ".arw", ".dng", ".raf", ".orf", ".rw2"}
JPEG_EXTS = {".jpg", ".jpeg"}
IMAGE_EXTS = RAW_EXTS | JPEG_EXTS | {".tif", ".tiff", ".png"}


def classify_ext(path):
    ext = path.suffix.lower()
    if ext in RAW_EXTS:
        return "raw"
    if ext in JPEG_EXTS:
        return "jpeg"
    return "other" if ext in IMAGE_EXTS else "skip"


def content_hash(path, chunk_size=65536):
    h = hashlib.md5()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def has_gps(path):
    """Best-effort GPS-EXIF check. Returns None when we can't tell."""
    try:
        from PIL import ExifTags, Image
    except ImportError:
        return None
    try:
        with Image.open(path) as im:
            exif = im.getexif()
            if not exif:
                return False
            # GPSInfo tag is 34853
            gps_tag = next(
                (k for k, v in ExifTags.TAGS.items() if v == "GPSInfo"), 34853
            )
            gps = exif.get_ifd(gps_tag) if hasattr(exif, "get_ifd") else exif.get(gps_tag)
            if not gps:
                return False
            return 2 in gps and 4 in gps
    except Exception:
        return None


def find_duplicates(files):
    """Return a list of (hash, [paths]) groups where len(paths) > 1."""
    groups = defaultdict(list)
    for f in files:
        groups[content_hash(f)].append(f)
    return [(h, paths) for h, paths in groups.items() if len(paths) > 1]


def find_burst(files, window_seconds=2):
    """Return the longest run of files whose mtimes are within `window_seconds`."""
    sorted_files = sorted(files, key=lambda p: p.stat().st_mtime)
    best_run = []
    current = []
    last_mtime = None
    for f in sorted_files:
        m = f.stat().st_mtime
        if last_mtime is not None and m - last_mtime <= window_seconds:
            current.append(f)
        else:
            current = [f]
        if len(current) > len(best_run):
            best_run = current[:]
        last_mtime = m
    return best_run


def _safe_dest(dest):
    """Validate destination is not inside ~/.vireo/ or $HOME."""
    dest = Path(dest).expanduser().resolve()
    home = Path.home().resolve()
    if dest == home:
        sys.exit(f"refusing to write to $HOME: {dest}")
    real_vireo = (home / ".vireo").resolve()
    try:
        dest.relative_to(real_vireo)
        sys.exit(f"refusing to write under ~/.vireo/: {dest}")
    except ValueError:
        pass
    return dest


def sample(source, dest, counts=None, dry_run=False):
    """Walk `source`, pick a representative subset, copy to `dest`.

    Returns a dict summarizing what was copied.
    """
    source = Path(source).expanduser().resolve()
    dest = _safe_dest(dest)

    counts = counts or {
        "gps_yes": 10, "gps_no": 10, "raws": 10, "jpegs": 10, "random": 50,
    }

    all_files = []
    for root, _, filenames in os.walk(source):
        for name in filenames:
            p = Path(root) / name
            if classify_ext(p) != "skip":
                all_files.append(p)

    gps_yes, gps_no, raws, jpegs = [], [], [], []
    for f in all_files:
        cls = classify_ext(f)
        if cls == "raw":
            raws.append(f)
        elif cls == "jpeg":
            jpegs.append(f)
        if cls == "jpeg":
            gps = has_gps(f)
            if gps is True:
                gps_yes.append(f)
            elif gps is False:
                gps_no.append(f)

    burst = find_burst(all_files[:500])[:5]
    dups = find_duplicates(all_files[:500])
    dup_pair = dups[0][1][:2] if dups else []

    picks = {
        "gps_yes": gps_yes[: counts["gps_yes"]],
        "gps_no": gps_no[: counts["gps_no"]],
        "raws": raws[: counts["raws"]],
        "jpegs": jpegs[: counts["jpegs"]],
        "burst": burst,
        "duplicates": dup_pair,
        "random": all_files[: counts["random"]],
    }

    copied = {}
    if not dry_run:
        dest.mkdir(parents=True, exist_ok=True)
        for category, files in picks.items():
            cat_dir = dest / category
            cat_dir.mkdir(exist_ok=True)
            copied[category] = []
            for src in files:
                tgt = cat_dir / src.name
                if tgt.exists():
                    copied[category].append(tgt)
                    continue
                shutil.copy2(src, tgt)
                copied[category].append(tgt)

        manifest = dest / "MANIFEST.md"
        lines = [
            f"# Vireo test photo set — built {datetime.now().isoformat()}",
            f"Source: `{source}`",
            "",
        ]
        for category, files in copied.items():
            lines.append(f"## {category} ({len(files)} files)")
            for f in files:
                lines.append(f"- {f.relative_to(dest)}")
            lines.append("")
        manifest.write_text("\n".join(lines))

    return {k: len(v) for k, v in picks.items()}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--source", required=True, help="Real photo library root")
    p.add_argument("--dest", default="~/vireo-test-photos", help="Destination directory")
    p.add_argument("--dry-run", action="store_true", help="Report picks without copying")
    args = p.parse_args()
    result = sample(args.source, args.dest, dry_run=args.dry_run)
    print("Picks:")
    for k, v in result.items():
        print(f"  {k}: {v}")


if __name__ == "__main__":
    main()
