"""Type-checker declarations that must track runtime wiring."""

import ast
import importlib
import textwrap
from pathlib import Path

REPOSITORIES = Path(__file__).resolve().parents[1] / "repositories"

# ``Callable[..., Any]`` has one meaning but several ways to write it in
# source: bare, dotted through ``typing`` or ``collections.abc``, each
# of which may also appear as an explicitly quoted forward reference
# (``callback: "Callable[..., Any]"``) and each component may be imported
# under a local alias (``from collections.abc import Callable as C``;
# ``import typing as t``). We normalize the annotation's AST text by
# resolving import aliases to their canonical dotted names, then compare
# against this set. The ratchet therefore sees the declaration whether
# or not the repository uses ``from __future__ import annotations``,
# whether or not the author quoted the annotation for the type checker,
# and whether or not either component or the module it came from was
# renamed at import.
_FORBIDDEN_CALLABLE_FACADE_FORMS = frozenset(
    {
        "Callable[..., Any]",
        "typing.Callable[..., Any]",
        "collections.abc.Callable[..., Any]",
    }
)


def _collect_import_aliases(tree: ast.Module) -> dict[str, str]:
    """Return a map ``{local_name: canonical_dotted_name}`` for imports.

    Only names relevant to ``Callable[..., Any]`` detection are tracked:
    aliases of ``Callable`` and ``Any``, and aliases of the ``typing``
    and ``collections.abc`` modules through which the two names may be
    accessed. A local name that no tracked import binds is omitted and
    the canonicalizer treats it as its own canonical form.

    We walk the entire tree (including ``if TYPE_CHECKING:`` bodies and
    function-local imports), accepting the slight over-approximation of
    treating a function-local alias as module-scoped: this is a ratchet,
    so recognizing more alias forms makes it harder to bypass, not
    easier.
    """
    aliases: dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            module = node.module or ""
            for alias in node.names:
                local = alias.asname or alias.name
                if module in {"typing", "collections.abc"} and alias.name == "Callable":
                    aliases[local] = "Callable"
                elif module == "typing" and alias.name == "Any":
                    aliases[local] = "Any"
                elif module == "collections" and alias.name == "abc":
                    aliases[local] = "collections.abc"
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == "typing":
                    aliases[alias.asname or "typing"] = "typing"
                elif alias.name == "collections.abc" and alias.asname is not None:
                    # ``import collections.abc`` without ``as`` binds the
                    # top-level ``collections`` name; the dotted access
                    # ``collections.abc.Callable`` canonicalizes to its own
                    # text without needing an alias entry.
                    aliases[alias.asname] = "collections.abc"
    return aliases


def _canonical_annotation_text(node: ast.expr, aliases: dict[str, str]) -> str:
    """Return the dotted/subscript source form of ``node`` with aliases resolved.

    ``callback: "Callable[..., Any]"`` reaches this function as an
    ``ast.Constant`` holding the string, which we parse and recurse on
    so the matcher sees the same canonical form as the bare declaration.
    A ``Name`` whose id is in ``aliases`` is substituted for its canonical
    name, so ``C[..., A]`` with ``C`` aliasing ``Callable`` and ``A``
    aliasing ``Any`` reads as ``Callable[..., Any]``.
    """
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        try:
            inner = ast.parse(node.value.strip(), mode="eval").body
        except SyntaxError:
            return node.value.strip()
        return _canonical_annotation_text(inner, aliases)
    if isinstance(node, ast.Constant) and node.value is Ellipsis:
        return "..."
    if isinstance(node, ast.Name):
        return aliases.get(node.id, node.id)
    if isinstance(node, ast.Attribute):
        prefix = _canonical_annotation_text(node.value, aliases)
        if prefix == "typing" and node.attr == "Any":
            return "Any"
        return f"{prefix}.{node.attr}"
    if isinstance(node, ast.Subscript):
        value_text = _canonical_annotation_text(node.value, aliases)
        slice_text = _canonical_annotation_text(node.slice, aliases)
        return f"{value_text}[{slice_text}]"
    if isinstance(node, ast.Tuple):
        return ", ".join(_canonical_annotation_text(elt, aliases) for elt in node.elts)
    return ast.unparse(node)


def _callable_facade_offenders(source: str, label: str) -> list[str]:
    """Return ``"{label}.{Class}.{attr}"`` for every class attribute whose
    annotation names ``Callable[..., Any]`` in any supported form."""
    tree = ast.parse(source)
    aliases = _collect_import_aliases(tree)
    offenders: list[str] = []
    for node in tree.body:
        if not isinstance(node, ast.ClassDef):
            continue
        for stmt in node.body:
            if not isinstance(stmt, ast.AnnAssign):
                continue
            if not isinstance(stmt.target, ast.Name):
                continue
            text = _canonical_annotation_text(stmt.annotation, aliases)
            if text in _FORBIDDEN_CALLABLE_FACADE_FORMS:
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


def test_callable_facade_matcher_resolves_import_aliases():
    """Inert fixture: the matcher must catch the forbidden form when either
    ``Callable`` or ``Any``, or the module they come through, was imported
    under a local alias.

    The eight shapes cover both components and both access paths:
    ``from collections.abc import Callable as C`` and
    ``from typing import Callable as C`` for the aliased name import,
    ``from typing import Any as A`` for the aliased slice name,
    ``import typing as t`` / ``import collections.abc as cabc`` /
    ``from collections import abc`` / ``from collections import abc as X``
    for the aliased module access, each paired with a class attribute that
    uses the alias. ``NotCallable.callback`` (``Callable[[], int]``) is a
    negative control: alias resolution must not sweep in signatures that
    are not the forbidden form.
    """
    source = textwrap.dedent(
        """
        from __future__ import annotations

        from collections.abc import Callable as C
        from typing import Callable as TC, Any as A, Callable, Any
        import typing as t
        import collections.abc as cabc
        from collections import abc
        from collections import abc as X


        class AliasCallableFromAbc:
            callback: C[..., Any]


        class AliasCallableFromTyping:
            callback: TC[..., Any]


        class AliasAny:
            callback: Callable[..., A]


        class AliasBoth:
            callback: C[..., A]


        class AliasTypingModule:
            callback: t.Callable[..., Any]


        class AliasAbcModule:
            callback: cabc.Callable[..., Any]


        class AbcSubmoduleAttribute:
            callback: abc.Callable[..., Any]


        class AbcSubmoduleAliased:
            callback: X.Callable[..., Any]


        class QuotedWithAlias:
            callback: "C[..., A]"


        class NotCallable:
            callback: Callable[[], int]
        """
    )
    offenders = _callable_facade_offenders(source, "fixture")
    assert sorted(offenders) == [
        "fixture.AbcSubmoduleAliased.callback",
        "fixture.AbcSubmoduleAttribute.callback",
        "fixture.AliasAbcModule.callback",
        "fixture.AliasAny.callback",
        "fixture.AliasBoth.callback",
        "fixture.AliasCallableFromAbc.callback",
        "fixture.AliasCallableFromTyping.callback",
        "fixture.AliasTypingModule.callback",
        "fixture.QuotedWithAlias.callback",
    ]


def test_callable_facade_matcher_normalizes_module_qualified_any():
    source = (
        "from __future__ import annotations\nimport typing as t\nimport collections.abc as cabc\n"
        "class Typing:\n    callback: t.Callable[..., t.Any]\n"
        "class Abc:\n    callback: cabc.Callable[..., t.Any]\n"
        "class Quoted:\n    callback: \"t.Callable[..., t.Any]\"\n"
        "class Legitimate:\n    callback: t.Callable[[], t.Any]\n"
    )
    assert sorted(_callable_facade_offenders(source, "fixture")) == [
        "fixture.Abc.callback", "fixture.Quoted.callback", "fixture.Typing.callback",
    ]
