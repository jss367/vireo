"""Type-checker declarations that must track runtime wiring."""

import ast
import importlib
import textwrap
from pathlib import Path

REPOSITORIES = Path(__file__).resolve().parents[1] / "repositories"

# ``Callable[..., Any]`` has one meaning but several ways to write it in
# source: bare, dotted through ``typing`` or ``collections.abc``, each
# of which may also appear as an explicitly quoted forward reference
# (``callback: "Callable[..., Any]"``). We compare the AST text of each
# annotation against this set after unwrapping quoted strings, so the
# ratchet sees the declaration whether or not the repository uses
# ``from __future__ import annotations`` and whether or not the author
# quoted the annotation for the type checker.
_FORBIDDEN_CALLABLE_FACADE_FORMS = frozenset(
    {
        "Callable[..., Any]",
        "typing.Callable[..., Any]",
        "collections.abc.Callable[..., Any]",
    }
)


def _annotation_text(node: ast.expr) -> str:
    """Return the source text of ``node``, unwrapping a quoted forward reference.

    ``callback: "Callable[..., Any]"`` reaches this function as an
    ``ast.Constant`` holding the string ``"Callable[..., Any]"``. We
    treat the inner string as the annotation's intent so the matcher
    sees the same form a plain ``callback: Callable[..., Any]`` writes.
    This matters most under ``from __future__ import annotations``, where
    ``inspect.get_annotations(eval_str=True)`` evaluates the stored
    string once and returns the inner string, not the type it names —
    the exact gap this ratchet must not have.
    """
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value.strip()
    return ast.unparse(node)


def _callable_facade_offenders(source: str, label: str) -> list[str]:
    """Return ``"{label}.{Class}.{attr}"`` for every class attribute whose
    annotation names ``Callable[..., Any]`` in any supported form."""
    tree = ast.parse(source)
    offenders: list[str] = []
    for node in tree.body:
        if not isinstance(node, ast.ClassDef):
            continue
        for stmt in node.body:
            if not isinstance(stmt, ast.AnnAssign):
                continue
            if not isinstance(stmt.target, ast.Name):
                continue
            if _annotation_text(stmt.annotation) in _FORBIDDEN_CALLABLE_FACADE_FORMS:
                offenders.append(f"{label}.{node.name}.{stmt.target.id}")
    return offenders


def test_no_repository_declares_methods_bound_from_the_facade():
    """No repository binds ``Database`` methods onto itself, so none declares one.

    A repository that copied ``Database`` methods onto itself with
    ``setattr`` had to declare each as a ``Callable[..., Any]`` class
    annotation for basedpyright to see it. Repositories depend only
    downward now (``test_db_facade_structure.FACADE_BINDING_MODULES`` is
    empty), so such a declaration claims an attribute nothing installs.
    """
    offenders: list[str] = []
    for path in sorted(REPOSITORIES.glob("*.py")):
        module = importlib.import_module(f"repositories.{path.stem}")
        assert not hasattr(module, "FACADE_METHODS"), path.stem
        offenders += _callable_facade_offenders(path.read_text(), path.stem)
    assert not offenders, offenders


def test_callable_facade_matcher_catches_every_quoting_form():
    """Inert fixture: the matcher must catch each way to write the forbidden form.

    The four shapes map to the two source-level choices the type system
    exposes: whether the module uses ``from __future__ import
    annotations``, and whether the author quoted the annotation. AST
    inspection is uniform across both axes because the parser sees the
    same tree either way; the ``ast.Constant`` branch of
    ``_annotation_text`` is what handles the quoted shape, which
    ``inspect.get_annotations(eval_str=True)`` evaluates to a bare
    string under future annotations and never compares equal to the
    runtime type. ``collections.abc.Callable[..., Any]`` and
    ``typing.Callable[..., Any]`` cover the two documented spellings a
    repository might import under; a plain ``Callable[[], int]``
    (``effective_config.resolve_workspace_id``) is the live negative
    control from the tree, pinned so a future tightening does not sweep
    it in.
    """
    source = textwrap.dedent(
        """
        from __future__ import annotations

        from collections.abc import Callable
        from typing import Any
        import typing
        import collections.abc


        class Bare:
            callback: Callable[..., Any]


        class Quoted:
            callback: "Callable[..., Any]"


        class DottedTyping:
            callback: typing.Callable[..., Any]


        class DottedAbc:
            callback: collections.abc.Callable[..., Any]


        class Legitimate:
            resolve_workspace_id: Callable[[], int]
        """
    )
    offenders = _callable_facade_offenders(source, "fixture")
    assert sorted(offenders) == [
        "fixture.Bare.callback",
        "fixture.DottedAbc.callback",
        "fixture.DottedTyping.callback",
        "fixture.Quoted.callback",
    ]
