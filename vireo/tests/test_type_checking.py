"""Type-checker declarations that must track runtime wiring."""

import ast
import importlib
import textwrap
from pathlib import Path

import pytest

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


def _annotation_text(node: ast.expr, aliases: dict[str, str] | None = None) -> str:
    """Normalize annotation syntax, recursively unwrapping forward-reference literals.

    ``callback: "Callable[..., Any]"`` reaches this function as an
    ``ast.Constant`` holding the string ``"Callable[..., Any]"``. We
    treat the inner string as the annotation's intent so the matcher
    sees the same form a plain ``callback: Callable[..., Any]`` writes.
    This matters most under ``from __future__ import annotations``, where
    ``inspect.get_annotations(eval_str=True)`` evaluates the stored
    string once and returns the inner string, not the type it names —
    the exact gap this ratchet must not have.
    """
    while isinstance(node, ast.Constant) and isinstance(node.value, str):
        node = ast.parse(node.value, mode="eval").body
    for name in ast.walk(node):
        if isinstance(name, ast.Name) and aliases:
            name.id = aliases.get(name.id, name.id)
    return ast.unparse(node)


def _callable_facade_offenders(source: str, label: str) -> list[str]:
    """Return ``"{label}.{Class}.{attr}"`` for every class attribute whose
    annotation names ``Callable[..., Any]`` in any supported form."""
    tree = ast.parse(source)
    # Resolve only direct imports of the two names this guard recognizes.
    # Annotation text and arbitrary imports are never executed.
    aliases = {
        item.asname or item.name: item.name
        for stmt in tree.body if isinstance(stmt, ast.ImportFrom)
        for item in stmt.names
        if (stmt.module in {"typing", "collections.abc"} and item.name == "Callable")
        or (stmt.module == "typing" and item.name == "Any")
    }
    offenders: list[str] = []
    for node in tree.body:
        if not isinstance(node, ast.ClassDef):
            continue
        for stmt in node.body:
            if not isinstance(stmt, ast.AnnAssign):
                continue
            if not isinstance(stmt.target, ast.Name):
                continue
            if _annotation_text(stmt.annotation, aliases) in _FORBIDDEN_CALLABLE_FACADE_FORMS:
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


def test_callable_facade_matcher_normalizes_quoted_annotation_syntax():
    for annotation in [
        '"Callable[...,Any]"', "'typing.Callable [ ..., Any ]'",
        repr(repr("Callable[..., Any]")),
    ]:
        source = (
            "from __future__ import annotations\n"
            f"class Repository:\n    callback: {annotation}\n"
        )
        assert _callable_facade_offenders(source, "fixture") == ["fixture.Repository.callback"]


def test_annotation_normalization_never_executes_expressions():
    source = (
        "raise AssertionError('source must not execute')\n"
        "class Repository:\n"
        "    callback: \"__import__('untrusted').callback()\"\n"
    )
    assert _callable_facade_offenders(source, "fixture") == []


@pytest.mark.parametrize("future", ["", "from __future__ import annotations\n"])
@pytest.mark.parametrize("quoted", [False, True])
@pytest.mark.parametrize("imports, annotation", [
    ("from collections.abc import Callable as C\nfrom typing import Any\n", "C[..., Any]"),
    ("from typing import Callable as C, Any as A\n", "C[..., A]"),
])
def test_callable_facade_matcher_preserves_import_alias_coverage(future, quoted, imports, annotation):
    if quoted:
        annotation = repr(annotation)
    source = future + imports + f"class Repository:\n    callback: {annotation}\n"
    assert _callable_facade_offenders(source, "fixture") == ["fixture.Repository.callback"]


def test_unrelated_imports_and_typed_callables_are_not_forbidden():
    assert _callable_facade_offenders(
        "from unrelated import Callable as C\nclass Repository:\n    callback: C[..., Any]\n", "fixture",
    ) == []
    assert _callable_facade_offenders(
        "from typing import Callable as C, Any as A\nclass Repository:\n    callback: C[[], int]\n", "fixture",
    ) == []
