"""Type-checker declarations that must track runtime wiring."""

import importlib
from collections.abc import Callable
from typing import Any

import pytest


@pytest.mark.parametrize(
    "module_name, class_name",
    [
        ("repositories.keywords", "KeywordRepository"),
        ("repositories.keyword_taxa", "KeywordTaxonRepository"),
        ("repositories.keyword_tree", "KeywordTreeRepository"),
        ("repositories.keyword_provenance", "KeywordProvenanceRepository"),
        ("repositories.photos", "PhotoRepository"),
        ("repositories.predictions", "PredictionRepository"),
    ],
)
def test_facade_method_declarations_match_facade_methods(module_name, class_name):
    """Each repository declares exactly the façade methods ``__init__`` installs.

    ``__init__`` binds every name in ``FACADE_METHODS`` with ``setattr``,
    which basedpyright cannot see, so the class body declares them. A name
    added to ``FACADE_METHODS`` without a declaration is invisible to the
    type checker; a declaration left behind after a name is removed claims
    an attribute that no longer exists.
    """
    module = importlib.import_module(module_name)
    cls = getattr(module, class_name)
    declared = {
        name for name, annotation in cls.__annotations__.items()
        if annotation == Callable[..., Any]
    }
    assert declared == set(module.FACADE_METHODS)
