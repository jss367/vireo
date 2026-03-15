"""Export Lightroom keywords to XMP sidecar files.

Usage:
    python lr-migration/export_keywords.py --catalogs *.lrcat [--write]
"""

import argparse
import logging
import sys
from pathlib import Path

from catalog_reader import read_catalog
from xmp_writer import write_xmp_sidecar

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s: %(message)s",
)
log = logging.getLogger(__name__)


def run(catalogs, photos_root, write=False):
    """Process catalogs and write/report XMP sidecars.

    Args:
        catalogs: list of paths to .lrcat files
        photos_root: root directory of photo files (unused for path resolution
                     but used for logging context)
        write: if True, write XMP sidecars; if False, dry-run only

    Returns:
        dict with stats: catalogs_processed, files_with_keywords, files_not_found,
                         multi_catalog_files, sidecars_written
    """
    # Merge keyword data from all catalogs
    merged = {}  # file_path -> {"flat_keywords": set, "hierarchical_keywords": set, "catalogs": list}

    for cat_path in catalogs:
        log.info("Reading catalog: %s", cat_path)
        try:
            data = read_catalog(cat_path)
        except Exception:
            log.exception("Failed to read catalog: %s", cat_path)
            continue

        cat_name = Path(cat_path).stem
        for file_path, kw_data in data.items():
            if file_path not in merged:
                merged[file_path] = {
                    "flat_keywords": set(),
                    "hierarchical_keywords": set(),
                    "catalogs": [],
                }
            entry = merged[file_path]
            entry["flat_keywords"].update(kw_data["flat_keywords"])
            entry["hierarchical_keywords"].update(kw_data["hierarchical_keywords"])
            entry["catalogs"].append(cat_name)

    # Detect multi-catalog overlaps
    multi_catalog_files = 0
    for file_path, entry in merged.items():
        if len(entry["catalogs"]) > 1:
            multi_catalog_files += 1
            log.warning(
                "File in multiple catalogs: %s (catalogs: %s)",
                file_path,
                ", ".join(entry["catalogs"]),
            )

    # Write or report sidecars
    files_not_found = 0
    sidecars_written = 0

    for file_path, entry in sorted(merged.items()):
        path = Path(file_path)
        if not path.exists():
            files_not_found += 1
            log.debug("File not found: %s", file_path)
            continue

        xmp_path = path.with_suffix(".xmp")

        if write:
            write_xmp_sidecar(
                str(xmp_path),
                entry["flat_keywords"],
                entry["hierarchical_keywords"],
            )
            sidecars_written += 1
            if sidecars_written % 1000 == 0:
                log.info("Progress: %d sidecars written", sidecars_written)
        else:
            log.info(
                "[DRY RUN] Would write %s (%d keywords)",
                xmp_path,
                len(entry["flat_keywords"]),
            )

    stats = {
        "catalogs_processed": len(catalogs),
        "files_with_keywords": len(merged),
        "files_not_found": files_not_found,
        "multi_catalog_files": multi_catalog_files,
        "sidecars_written": sidecars_written,
    }

    log.info("--- Summary ---")
    log.info("Catalogs processed:    %d", stats["catalogs_processed"])
    log.info("Files with keywords:   %d", stats["files_with_keywords"])
    log.info("Files not found:       %d", stats["files_not_found"])
    log.info("Multi-catalog files:   %d", stats["multi_catalog_files"])
    log.info("Sidecars written:      %d", stats["sidecars_written"])

    return stats


def main():
    parser = argparse.ArgumentParser(
        description="Export Lightroom keywords to XMP sidecar files."
    )
    parser.add_argument(
        "--catalogs",
        nargs="+",
        required=True,
        help="Paths to .lrcat catalog files",
    )
    parser.add_argument(
        "--photos-root",
        default="/Volumes/Photography/Raw Files",
        help="Root directory of photo files (default: /Volumes/Photography/Raw Files)",
    )
    parser.add_argument(
        "--write",
        action="store_true",
        help="Actually write XMP sidecars (default is dry-run)",
    )
    args = parser.parse_args()

    run(
        catalogs=args.catalogs,
        photos_root=args.photos_root,
        write=args.write,
    )


if __name__ == "__main__":
    main()
