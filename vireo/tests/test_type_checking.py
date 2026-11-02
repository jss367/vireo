"""Type-checker declarations that must track runtime wiring."""

import importlib
import inspect
from collections.abc import Callable
from pathlib import Path
from typing import Any

REPOSITORIES = Path(__file__).resolve().parents[1] / "repositories"


def test_no_repository_declares_methods_bound_from_the_facade():
    """No repository binds ``Database`` methods onto itself, so none declares one.

    A repository that copied ``Database`` methods onto itself with
    ``setattr`` had to declare each as a ``Callable[..., Any]`` class
    annotation for basedpyright to see it. Repositories depend only
    downward now (``test_db_facade_structure.FACADE_BINDING_MODULES`` is
    empty), so such a declaration claims an attribute nothing installs.
    """
    offenders = []
    for path in sorted(REPOSITORIES.glob("*.py")):
        module = importlib.import_module(f"repositories.{path.stem}")
        assert not hasattr(module, "FACADE_METHODS"), path.stem
        for name, cls in inspect.getmembers(module, inspect.isclass):
            if cls.__module__ != module.__name__:
                continue
            # ``inspect.get_annotations(cls, eval_str=True)`` resolves string
            # annotations against the class's own module globals, so a
            # declaration in a ``from __future__ import annotations`` module
            # stored as ``"Callable[..., Any]"`` is compared as the type it
            # names, not as a string that will never equal the type.
            annotations = inspect.get_annotations(cls, eval_str=True)
            offenders += [
                f"{path.stem}.{name}.{attr}"
                for attr, annotation in annotations.items()
                if annotation == Callable[..., Any]
            ]
    assert not offenders, offenders
