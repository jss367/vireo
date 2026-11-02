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


def _collect_module_aliases(tree: ast.Module) -> dict[str, ast.expr]:
    """Return a map ``{local_name: value_ast_expr}`` for module-level aliases.

    A repository could sidestep the import-alias ratchet by defining
    ``FacadeMethod = Callable[..., Any]`` once and declaring
    ``callback: FacadeMethod`` on every class, which the old runtime
    equality check caught (``FacadeMethod`` evaluates to the same object
    as ``Callable[..., Any]``) but a plain AST text comparison does not.
    The canonicalizer resolves these names by substituting the alias's
    right-hand side AST and recursing, so an aliased forbidden form
    reads as the forbidden form.

    Collected forms, all at module top level:
      * ``FacadeMethod = Callable[..., Any]`` — ``ast.Assign``, single Name target.
      * ``FacadeMethod: TypeAlias = Callable[..., Any]`` — ``ast.AnnAssign`` with a value.
      * ``type FacadeMethod = Callable[..., Any]`` — ``ast.TypeAlias`` (PEP 695).

    Class-body and function-body assignments are deliberately skipped:
    those are scoped bindings, not aliases a class annotation at module
    scope would reach.
    """
    module_aliases: dict[str, ast.expr] = {}
    for stmt in tree.body:
        if isinstance(stmt, ast.Assign):
            for target in stmt.targets:
                if isinstance(target, ast.Name):
                    module_aliases[target.id] = stmt.value
        elif isinstance(stmt, ast.AnnAssign):
            if isinstance(stmt.target, ast.Name) and stmt.value is not None:
                module_aliases[stmt.target.id] = stmt.value
        elif isinstance(stmt, ast.TypeAlias) and isinstance(stmt.name, ast.Name):
            module_aliases[stmt.name.id] = stmt.value
    return module_aliases


def _canonical_annotation_text(
    node: ast.expr,
    aliases: dict[str, str],
    module_aliases: dict[str, ast.expr] | None = None,
    _visited: frozenset[str] = frozenset(),
) -> str:
    """Return the dotted/subscript source form of ``node`` with aliases resolved.

    ``callback: "Callable[..., Any]"`` reaches this function as an
    ``ast.Constant`` holding the string, which we parse and recurse on
    so the matcher sees the same canonical form as the bare declaration.
    A ``Name`` whose id is in ``aliases`` is substituted for its canonical
    name, so ``C[..., A]`` with ``C`` aliasing ``Callable`` and ``A``
    aliasing ``Any`` reads as ``Callable[..., Any]``. A ``Name`` whose id
    is in ``module_aliases`` is resolved to the aliased expression and
    canonicalized recursively; ``_visited`` guards against a cycle such
    as ``A = B; B = A`` so a self-referential alias canonicalizes to the
    name rather than looping.
    """
    if module_aliases is None:
        module_aliases = {}
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        try:
            inner = ast.parse(node.value.strip(), mode="eval").body
        except SyntaxError:
            return node.value.strip()
        return _canonical_annotation_text(inner, aliases, module_aliases, _visited)
    if isinstance(node, ast.Constant) and node.value is Ellipsis:
        return "..."
    if isinstance(node, ast.Name):
        if node.id in module_aliases and node.id not in _visited:
            return _canonical_annotation_text(
                module_aliases[node.id],
                aliases,
                module_aliases,
                _visited | {node.id},
            )
        return aliases.get(node.id, node.id)
    if isinstance(node, ast.Attribute):
        prefix = _canonical_annotation_text(node.value, aliases, module_aliases, _visited)
        # ``typing.Any`` (bare or via a module alias such as ``import typing
        # as t`` -> ``t.Any``) is semantically the same as a bare ``Any``;
        # the forbidden set only carries the bare form, so normalize the
        # qualified access here. ``Any`` lives only in ``typing``, so no
        # analogous rule is needed for ``collections.abc``.
        if prefix == "typing" and node.attr == "Any":
            return "Any"
        return f"{prefix}.{node.attr}"
    if isinstance(node, ast.Subscript):
        value_text = _canonical_annotation_text(node.value, aliases, module_aliases, _visited)
        slice_text = _canonical_annotation_text(node.slice, aliases, module_aliases, _visited)
        return f"{value_text}[{slice_text}]"
    if isinstance(node, ast.Tuple):
        return ", ".join(
            _canonical_annotation_text(elt, aliases, module_aliases, _visited)
            for elt in node.elts
        )
    return ast.unparse(node)


def _callable_facade_offenders(source: str, label: str) -> list[str]:
    """Return ``"{label}.{Class}.{attr}"`` for every class attribute whose
    annotation names ``Callable[..., Any]`` in any supported form."""
    tree = ast.parse(source)
    aliases = _collect_import_aliases(tree)
    module_aliases = _collect_module_aliases(tree)
    offenders: list[str] = []
    for node in tree.body:
        if not isinstance(node, ast.ClassDef):
            continue
        for stmt in node.body:
            if not isinstance(stmt, ast.AnnAssign):
                continue
            if not isinstance(stmt.target, ast.Name):
                continue
            text = _canonical_annotation_text(stmt.annotation, aliases, module_aliases)
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


        class DottedTypingBothQualified:
            callback: typing.Callable[..., typing.Any]


        class DottedAbcQualifiedAny:
            callback: collections.abc.Callable[..., typing.Any]


        class Legitimate:
            resolve_workspace_id: Callable[[], int]
        """
    )
    offenders = _callable_facade_offenders(source, "fixture")
    assert sorted(offenders) == [
        "fixture.Bare.callback",
        "fixture.DottedAbc.callback",
        "fixture.DottedAbcQualifiedAny.callback",
        "fixture.DottedTyping.callback",
        "fixture.DottedTypingBothQualified.callback",
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


        class AliasTypingModuleQualifiedAny:
            callback: t.Callable[..., t.Any]


        class AliasAbcModule:
            callback: cabc.Callable[..., Any]


        class AliasAbcModuleQualifiedAny:
            callback: cabc.Callable[..., t.Any]


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
        "fixture.AliasAbcModuleQualifiedAny.callback",
        "fixture.AliasAny.callback",
        "fixture.AliasBoth.callback",
        "fixture.AliasCallableFromAbc.callback",
        "fixture.AliasCallableFromTyping.callback",
        "fixture.AliasTypingModule.callback",
        "fixture.AliasTypingModuleQualifiedAny.callback",
        "fixture.QuotedWithAlias.callback",
    ]


def test_callable_facade_matcher_resolves_module_level_aliases():
    """Inert fixture: the matcher must catch the forbidden form when the
    repository bound ``Callable[..., Any]`` to a module-level name and
    declared the class attribute as that name.

    The three shapes cover the ways to spell a module-level alias:
    plain assignment (``FacadeMethod = Callable[..., Any]``), annotated
    assignment with ``TypeAlias`` (``FacadeMethod: TypeAlias = Callable
    [..., Any]``), and PEP 695 ``type`` statement (``type AliasPep695
    = Callable[..., Any]``). ``AliasedThroughAlias`` chains an alias
    through another alias so the resolver recurses, ``AliasQuoted``
    puts the aliased name inside a quoted annotation to prove the
    quote-handling branch recurses too, and ``AliasImportAlias``
    composes an import alias with a module-level alias.

    Negative controls: ``PlainTuple`` is a module-level alias that
    resolves to an unrelated type (``tuple[int, str]``) so the matcher
    must not sweep it in on name alone; ``Cycle`` and ``CycleIndirect``
    exercise self-referential aliases (``A = A`` / ``A = B; B = A``)
    so the recursion guard terminates and the result is the alias name,
    which is not in the forbidden set.
    """
    source = textwrap.dedent(
        """
        from __future__ import annotations

        from collections.abc import Callable
        from typing import Any, TypeAlias
        from typing import Callable as TC


        FacadeMethod = Callable[..., Any]
        AnnotatedAlias: TypeAlias = Callable[..., Any]
        type AliasPep695 = Callable[..., Any]
        ChainedAlias = FacadeMethod
        ImportAliased = TC[..., Any]

        Unrelated = tuple[int, str]
        A = A
        B = C
        C = B


        class AliasedPlain:
            callback: FacadeMethod


        class AliasedAnnotated:
            callback: AnnotatedAlias


        class AliasedPep695:
            callback: AliasPep695


        class AliasedThroughAlias:
            callback: ChainedAlias


        class AliasImportAlias:
            callback: ImportAliased


        class AliasQuoted:
            callback: "FacadeMethod"


        class UnrelatedAlias:
            callback: Unrelated


        class Cycle:
            callback: A


        class CycleIndirect:
            callback: B
        """
    )
    offenders = _callable_facade_offenders(source, "fixture")
    assert sorted(offenders) == [
        "fixture.AliasImportAlias.callback",
        "fixture.AliasQuoted.callback",
        "fixture.AliasedAnnotated.callback",
        "fixture.AliasedPep695.callback",
        "fixture.AliasedPlain.callback",
        "fixture.AliasedThroughAlias.callback",
    ]
