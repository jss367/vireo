#!/usr/bin/env python3
"""Preview, apply, or recover an offline Vireo storage migration."""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "vireo"))

from services.storage_migration import run_cli  # noqa: E402

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    actions = parser.add_mutually_exclusive_group()
    actions.add_argument("--apply", action="store_const", const="apply", dest="action")
    actions.add_argument("--rollback", action="store_const", const="rollback", dest="action")
    raise SystemExit(run_cli(parser.parse_args().action or "preview"))
