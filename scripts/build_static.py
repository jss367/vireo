#!/usr/bin/env python3
"""Resolve {% include %} directives to produce static HTML files.

Usage:
    python scripts/build_static.py [--outdir BUILD_DIR]

Reads templates from vireo/templates/, resolves includes, writes to outdir.
"""
import argparse
import os
import re
import sys

TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), "..", "vireo", "templates")
INCLUDE_RE = re.compile(r"\{%\s*include\s+['\"]([^'\"]+)['\"]\s*%\}")


def resolve_includes(content, template_dir):
    """Replace {% include 'file' %} with the file's content."""
    def replacer(match):
        include_name = match.group(1)
        include_path = os.path.join(template_dir, include_name)
        with open(include_path) as f:
            return f.read()
    return INCLUDE_RE.sub(replacer, content)


def build(outdir):
    template_dir = os.path.abspath(TEMPLATE_DIR)
    os.makedirs(outdir, exist_ok=True)

    count = 0
    for fname in sorted(os.listdir(template_dir)):
        if fname.startswith("_") or not fname.endswith(".html"):
            continue
        src = os.path.join(template_dir, fname)
        with open(src) as f:
            content = f.read()

        content = resolve_includes(content, template_dir)
        dest = os.path.join(outdir, fname)
        with open(dest, "w") as f:
            f.write(content)
        count += 1
        print(f"  {fname}")

    print(f"\nBuilt {count} files to {outdir}")


def main():
    parser = argparse.ArgumentParser(description="Build static HTML from templates")
    parser.add_argument(
        "--outdir",
        default=os.path.join(os.path.dirname(__file__), "..", "build"),
        help="Output directory (default: ./build)",
    )
    args = parser.parse_args()
    build(args.outdir)


if __name__ == "__main__":
    main()
