"""Tests for photo export operations."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from export import resolve_template, sanitize_filename


def test_resolve_template_original():
    photo = {"filename": "DSC_4521.jpg", "timestamp": "2024-06-15T14:30:22",
             "rating": 3, "folder_name": "June_Trip"}
    result = resolve_template("{original}", photo, species="Red-tailed Hawk", seq=1)
    assert result == "DSC_4521"


def test_resolve_template_species_date_seq():
    photo = {"filename": "DSC_4521.jpg", "timestamp": "2024-06-15T14:30:22",
             "rating": 3, "folder_name": "June_Trip"}
    result = resolve_template("{species}/{date}_{seq}", photo,
                              species="Red-tailed Hawk", seq=5)
    assert result == "Red-tailed Hawk/2024-06-15_005"


def test_resolve_template_datetime():
    photo = {"filename": "DSC_4521.jpg", "timestamp": "2024-06-15T14:30:22",
             "rating": 3, "folder_name": "June_Trip"}
    result = resolve_template("{datetime}_{original}", photo,
                              species="unknown", seq=1)
    assert result == "2024-06-15_143022_DSC_4521"


def test_resolve_template_all_variables():
    photo = {"filename": "bird.jpg", "timestamp": "2024-01-10T08:00:00",
             "rating": 5, "folder_name": "Safari"}
    result = resolve_template("{folder}/{species}/{rating}_{seq}", photo,
                              species="Elephant", seq=12)
    assert result == "Safari/Elephant/5_012"


def test_resolve_template_missing_timestamp():
    photo = {"filename": "bird.jpg", "timestamp": None,
             "rating": 0, "folder_name": "Photos"}
    result = resolve_template("{date}_{original}", photo, species="unknown", seq=1)
    assert result == "unknown-date_bird"


def test_resolve_template_no_species():
    photo = {"filename": "bird.jpg", "timestamp": "2024-01-10T08:00:00",
             "rating": 0, "folder_name": "Photos"}
    result = resolve_template("{species}_{seq}", photo, species=None, seq=1)
    assert result == "unknown_001"


def test_sanitize_filename_slashes():
    assert sanitize_filename("Black/White Warbler") == "Black_White Warbler"


def test_sanitize_filename_special_chars():
    assert sanitize_filename('bird: "best"') == "bird_ _best_"
