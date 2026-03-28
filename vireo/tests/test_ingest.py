import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from datetime import datetime

from ingest import build_destination_path


def test_build_destination_path_default_template():
    dt = datetime(2026, 3, 28, 14, 30, 0)
    assert build_destination_path(dt) == "2026/03/28"


def test_build_destination_path_custom_template():
    dt = datetime(2026, 3, 28, 14, 30, 0)
    assert build_destination_path(dt, "%Y/%m") == "2026/03"


def test_build_destination_path_none_returns_unsorted():
    assert build_destination_path(None) == "unsorted"
