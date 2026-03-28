import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "vireo"))

from db import Database


@pytest.fixture
def db(tmp_path):
    """Return a Database backed by a temp file."""
    return Database(str(tmp_path / "test.db"))
