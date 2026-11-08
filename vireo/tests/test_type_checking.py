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


def _collect_import_aliases(tree: ast.Module) -> dict[str, frozenset[str]]:
    """Return a map ``{local_name: frozenset(canonical_dotted_names)}`` for imports.

    Only names relevant to ``Callable[..., Any]`` detection are tracked:
    aliases of ``Callable`` and ``Any``, and aliases of the ``typing``
    and ``collections`` / ``collections.abc`` modules through which they may be
    accessed. A local name that no tracked import binds is omitted and
    the canonicalizer treats it as its own canonical form.

    The value is set-valued because competing ``TYPE_CHECKING`` branches
    may bind one local name to different canonical names — e.g. the
    ``if TYPE_CHECKING:`` branch doing ``from collections.abc import
    Callable as Alias`` while the ``else:`` branch does ``from typing
    import Any as Alias``. ``ast.walk`` visits both branches, so a
    single-value map would let the second import overwrite the first
    and the matcher would miss that ``Alias[..., Any]`` is the
    forbidden form under the type checker's selected branch. By
    recording every binding, the canonicalizer probes each alternative
    the same way it does for module-level aliases, matching the
    over-approximating treatment ``_collect_module_aliases`` already
    gives ``if TYPE_CHECKING: ... else: ...`` assignments.

    We walk the entire tree (including ``if TYPE_CHECKING:`` bodies and
    function-local imports), accepting the slight over-approximation of
    treating a function-local alias as module-scoped: this is a ratchet,
    so recognizing more alias forms makes it harder to bypass, not
    easier.
    """
    raw: dict[str, set[str]] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            module = node.module or ""
            for alias in node.names:
                local = alias.asname or alias.name
                if module in {"typing", "collections.abc"} and alias.name == "Callable":
                    raw.setdefault(local, set()).add("Callable")
                elif module == "typing" and alias.name == "Any":
                    raw.setdefault(local, set()).add("Any")
                elif module == "collections" and alias.name == "abc":
                    raw.setdefault(local, set()).add("collections.abc")
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == "typing":
                    raw.setdefault(alias.asname or "typing", set()).add("typing")
                elif alias.name == "collections":
                    raw.setdefault(alias.asname or "collections", set()).add("collections")
                elif alias.name == "collections.abc" and alias.asname is not None:
                    # ``import collections.abc`` without ``as`` binds the
                    # top-level ``collections`` name; the dotted access
                    # ``collections.abc.Callable`` canonicalizes to its own
                    # text without needing an alias entry.
                    raw.setdefault(alias.asname, set()).add("collections.abc")
    return {name: frozenset(canonicals) for name, canonicals in raw.items()}


_TRY_NODE_TYPES: tuple[type, ...] = (
    (ast.Try, ast.TryStar) if hasattr(ast, "TryStar") else (ast.Try,)
)


def _iter_module_scope_statements(tree: ast.Module) -> list[ast.stmt]:
    """Return every statement that binds at module scope.

    Descends through module-scope control-flow (``if TYPE_CHECKING:``,
    ``try``/``except``/``except*``/``finally``, ``with``, ``match``)
    because an assignment nested inside one still binds at module scope
    — the type checker sees ``if TYPE_CHECKING: FacadeMethod = Callable
    [..., Any]`` exactly like the direct-body form, so the ratchet must
    too. ``ast.TryStar`` (PEP 654 ``try``/``except*``, Python 3.11+)
    shares the same body/handlers/orelse/finalbody shape as ``ast.Try``,
    so handling it alongside ``ast.Try`` reaches aliases inside an
    ``except*`` branch. Class and function bodies are deliberately
    skipped: those are scoped bindings, not aliases a module-scope class
    annotation would reach.
    """
    out: list[ast.stmt] = []
    stack: list[ast.stmt] = list(reversed(tree.body))
    while stack:
        stmt = stack.pop()
        out.append(stmt)
        if isinstance(stmt, ast.If):
            stack.extend(reversed(stmt.body))
            stack.extend(reversed(stmt.orelse))
        elif isinstance(stmt, _TRY_NODE_TYPES):
            stack.extend(reversed(stmt.body))
            for handler in stmt.handlers:
                stack.extend(reversed(handler.body))
            stack.extend(reversed(stmt.orelse))
            stack.extend(reversed(stmt.finalbody))
        elif isinstance(stmt, (ast.With, ast.AsyncWith)):
            stack.extend(reversed(stmt.body))
        elif isinstance(stmt, ast.Match):
            for case in stmt.cases:
                stack.extend(reversed(case.body))
    return out


def _collect_module_aliases(tree: ast.Module) -> dict[str, list[ast.expr]]:
    """Return ``{local_name: [value_ast_expr, ...]}`` for module-level aliases.

    A repository could sidestep the import-alias ratchet by defining
    ``FacadeMethod = Callable[..., Any]`` once and declaring
    ``callback: FacadeMethod`` on every class, which the old runtime
    equality check caught (``FacadeMethod`` evaluates to the same object
    as ``Callable[..., Any]``) but a plain AST text comparison does not.
    The canonicalizer resolves these names by substituting the alias's
    right-hand side AST and recursing, so an aliased forbidden form
    reads as the forbidden form.

    When control-flow produces alternatives for one name — e.g.
    ``if not TYPE_CHECKING: Alias = tuple[int, str]`` paired with
    ``else: Alias = Callable[..., Any]`` — both bindings are recorded
    rather than one overwriting the other. The type checker binds the
    branch its gating selects (basedpyright treats ``TYPE_CHECKING`` as
    True), so the matcher is an over-approximation that probes every
    alternative: an alias with any branch resolving to the forbidden
    form is reported.

    Collected forms, at module scope (direct top-level or nested under
    module-scope control-flow such as ``if TYPE_CHECKING:``):
      * ``FacadeMethod = Callable[..., Any]`` — ``ast.Assign``, single Name target.
      * ``FacadeMethod: TypeAlias = Callable[..., Any]`` — ``ast.AnnAssign`` with a value.
      * ``type FacadeMethod = Callable[..., Any]`` — ``ast.TypeAlias`` (PEP 695).

    Class-body and function-body assignments are deliberately skipped:
    those are scoped bindings, not aliases a class annotation at module
    scope would reach.
    """
    module_aliases: dict[str, list[ast.expr]] = {}
    for stmt in _iter_module_scope_statements(tree):
        if isinstance(stmt, ast.Assign):
            for target in stmt.targets:
                if isinstance(target, ast.Name):
                    module_aliases.setdefault(target.id, []).append(stmt.value)
        elif isinstance(stmt, ast.AnnAssign):
            if isinstance(stmt.target, ast.Name) and stmt.value is not None:
                module_aliases.setdefault(stmt.target.id, []).append(stmt.value)
        elif isinstance(stmt, ast.TypeAlias) and isinstance(stmt.name, ast.Name):
            module_aliases.setdefault(stmt.name.id, []).append(stmt.value)
    return module_aliases


def _canonical_annotation_texts(
    node: ast.expr,
    aliases: dict[str, frozenset[str]],
    module_aliases: dict[str, list[ast.expr]] | None = None,
    _visited: frozenset[str] = frozenset(),
) -> frozenset[str]:
    """Return conservative syntactic canonical candidates without evaluating code.

    Alternative bindings are combined independently across names and
    subexpressions, without preserving control-flow correlation. Cartesian
    products can therefore combine mutually exclusive bindings: importing
    both ``Outer`` and ``Inner`` as ``Callable`` in one branch and both as
    ``Any`` in another synthesizes ``Callable[..., Any]`` for
    ``Outer[..., Inner]``, although neither branch has that annotation.
    The architectural ratchet deliberately rejects such a candidate and
    may report harmless declarations. A match is not proof that a type
    checker or runtime can reach the synthesized form.

    ``callback: "Callable[..., Any]"`` reaches this function as an
    ``ast.Constant`` holding the string, which we parse and recurse on
    so the matcher sees the same canonical form as the bare declaration.
    A ``Name`` whose id is in ``aliases`` is substituted for its canonical
    name, so ``C[..., A]`` with ``C`` aliasing ``Callable`` and ``A``
    aliasing ``Any`` reads as ``Callable[..., Any]``. ``aliases`` is now
    set-valued: a ``Name`` whose id maps to several canonical names (as
    competing ``TYPE_CHECKING`` branches produce) contributes every one
    to the result set, so an alias with any branch resolving to the
    forbidden component is reached. A ``Name`` whose id is in
    ``module_aliases`` is resolved to each aliased expression and
    canonicalized recursively; ``_visited`` guards against a cycle such
    as ``A = B; B = A`` so a self-referential alias canonicalizes to the
    name rather than looping. A name present in both maps unions the
    results from each: the matcher never discards an alternative merely
    because one branch happens to be an import and another a module
    assignment, so an import binding and a module-level assignment
    binding one local name contribute syntactic candidates without
    asserting that their combinations occur on a feasible branch.
    """
    if module_aliases is None:
        module_aliases = {}
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        try:
            inner = ast.parse(node.value.strip(), mode="eval").body
        except SyntaxError:
            return frozenset({node.value.strip()})
        return _canonical_annotation_texts(inner, aliases, module_aliases, _visited)
    if isinstance(node, ast.Constant) and node.value is Ellipsis:
        return frozenset({"..."})
    if isinstance(node, ast.Name):
        results: set[str] = set()
        if node.id in module_aliases and node.id not in _visited:
            for alt in module_aliases[node.id]:
                results |= _canonical_annotation_texts(
                    alt, aliases, module_aliases, _visited | {node.id}
                )
        if node.id in aliases:
            results |= aliases[node.id]
        if not results:
            return frozenset({node.id})
        return frozenset(results)
    if isinstance(node, ast.Attribute):
        prefixes = _canonical_annotation_texts(
            node.value, aliases, module_aliases, _visited
        )
        # ``typing.Any`` (bare or via a module alias such as ``import typing
        # as t`` -> ``t.Any``) is semantically the same as a bare ``Any``;
        # the forbidden set only carries the bare form, so normalize the
        # qualified access here. ``Any`` lives only in ``typing``, so no
        # analogous rule is needed for ``collections.abc``.
        attr_results: set[str] = set()
        for prefix in prefixes:
            if prefix == "typing" and node.attr == "Any":
                attr_results.add("Any")
            else:
                attr_results.add(f"{prefix}.{node.attr}")
        return frozenset(attr_results)
    if isinstance(node, ast.Subscript):
        value_texts = _canonical_annotation_texts(
            node.value, aliases, module_aliases, _visited
        )
        slice_texts = _canonical_annotation_texts(
            node.slice, aliases, module_aliases, _visited
        )
        return frozenset(f"{v}[{s}]" for v in value_texts for s in slice_texts)
    if isinstance(node, ast.Tuple):
        combos: list[str] = [""]
        for i, elt in enumerate(node.elts):
            sep = ", " if i > 0 else ""
            elt_texts = _canonical_annotation_texts(
                elt, aliases, module_aliases, _visited
            )
            combos = [combo + sep + text for combo in combos for text in elt_texts]
        return frozenset(combos)
    return frozenset({ast.unparse(node)})


def _callable_facade_offenders(source: str, label: str) -> list[str]:
    """Return ``"{label}.{Class}.{attr}"`` for every class attribute whose
    conservative annotation candidates include ``Callable[..., Any]``.

    Independent branch alternatives may produce false positives; this is
    a fail-closed architectural rule, not runtime type evaluation.
    """
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
            texts = _canonical_annotation_texts(
                stmt.annotation, aliases, module_aliases
            )
            if _FORBIDDEN_CALLABLE_FACADE_FORMS & texts:
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
        offenders += _callable_facade_offenders(path.read_text(encoding="utf-8"), path.stem)
    assert not offenders, offenders



def test_repository_facade_scan_uses_utf8_with_windows_default(monkeypatch):
    """Source inspection must not depend on the Windows locale encoding."""
    original_read_text = Path.read_text

    def read_text(path, encoding=None, errors=None):
        if path.parent == REPOSITORIES and encoding is None:
            encoding = "cp1252"
        return original_read_text(path, encoding=encoding, errors=errors)

    monkeypatch.setattr(Path, "read_text", read_text)
    test_no_repository_declares_methods_bound_from_the_facade()


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
    quote-handling branch recurses too, ``AliasImportAlias`` composes
    an import alias with a module-level alias, and ``AliasedTypeChecking``
    / ``AliasedTryExcept`` / ``AliasedWith`` hide the alias under
    module-scope control-flow (``if TYPE_CHECKING:``, ``try``/``except``,
    ``with``) so the matcher must descend into those bodies rather than
    stopping at the ``ast.If`` / ``ast.Try`` / ``ast.With`` wrapper.

    ``AliasedBranch`` pins the branch-alternative case: ``if not
    TYPE_CHECKING: X = tuple[int, str]`` paired with ``else: X =
    Callable[..., Any]`` would, in a single-alias map, let the body's
    unrelated assignment overwrite the ``else`` branch's forbidden one
    and silently pass; the matcher now records both alternatives so the
    forbidden form is still reported. ``AliasedTryStarBody`` /
    ``AliasedTryStarExcept`` pin ``try`` / ``except*`` (PEP 654), whose
    wrapper is ``ast.TryStar`` rather than ``ast.Try`` — the module-scope
    walker handles both so an alias nested inside an ``except*`` branch
    is reached.

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

        import contextlib
        from collections.abc import Callable
        from typing import TYPE_CHECKING, Any, TypeAlias
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

        if TYPE_CHECKING:
            TypeCheckingAlias = Callable[..., Any]

        try:
            TryBodyAlias = Callable[..., Any]
        except ImportError:
            TryExceptAlias = Callable[..., Any]

        with contextlib.ExitStack():
            WithBodyAlias = Callable[..., Any]

        if not TYPE_CHECKING:
            AlternativeAlias = tuple[int, str]
        else:
            AlternativeAlias = Callable[..., Any]

        try:
            TryStarBodyAlias = Callable[..., Any]
        except* ImportError:
            TryStarExceptAlias = Callable[..., Any]


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


        class AliasedTypeChecking:
            callback: TypeCheckingAlias


        class AliasedTryBody:
            callback: TryBodyAlias


        class AliasedTryExcept:
            callback: TryExceptAlias


        class AliasedWith:
            callback: WithBodyAlias


        class AliasedBranch:
            callback: AlternativeAlias


        class AliasedTryStarBody:
            callback: TryStarBodyAlias


        class AliasedTryStarExcept:
            callback: TryStarExceptAlias


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
        "fixture.AliasedBranch.callback",
        "fixture.AliasedPep695.callback",
        "fixture.AliasedPlain.callback",
        "fixture.AliasedThroughAlias.callback",
        "fixture.AliasedTryBody.callback",
        "fixture.AliasedTryExcept.callback",
        "fixture.AliasedTryStarBody.callback",
        "fixture.AliasedTryStarExcept.callback",
        "fixture.AliasedTypeChecking.callback",
        "fixture.AliasedWith.callback",
    ]


def test_callable_facade_matcher_tracks_import_branch_alternatives():
    """Inert fixture: the matcher must catch the forbidden form when
    competing ``TYPE_CHECKING`` branches bind the same imported local
    name to different canonical names.

    The type checker (basedpyright treats ``TYPE_CHECKING`` as True)
    selects the branch that binds ``Alias`` to ``Callable``, so a class
    annotation ``callback: Alias[..., Any]`` is the forbidden form —
    but a single-value import map would let the branch ``ast.walk``
    visits last overwrite the first, dropping ``Callable`` from the
    mapping and silently passing the ratchet. Set-valued import aliases
    record every binding, matching the branch-alternative treatment
    ``_collect_module_aliases`` already gives competing module-level
    assignments; the matcher reports an alias whose any branch resolves
    to the forbidden component.

    The fixture pins three bounded shapes the comment thread called
    out:
      * ``AliasedImportBranchBodyFirst`` / ``AliasedImportBranchElseFirst``
        — both ``TYPE_CHECKING`` branch orders, since BFS traversal
        order would otherwise matter.
      * ``AliasedImportBranchQuoted`` — the explicitly quoted
        forward-reference form over the competing imports, so the
        ``ast.Constant`` recursion still reaches the set-valued
        canonical lookup.
      * ``AliasedImportBranchAssignComposed`` — composes a competing
        import alias with a module-level ``FacadeMethodComposed =
        Imported[..., Any]`` assignment, so the module-alias branch's
        recursion into the import map sees every alternative.

    Negative controls: ``AliasedBothAny`` makes both branches bind
    ``Alias`` to ``Any`` (so no reachable branch is the forbidden
    form, and the matcher must not sweep it in on BFS visit order
    alone), and ``LegitImportBranch`` keeps the live
    ``Callable[[], int]`` signature pattern.
    """
    source = textwrap.dedent(
        """
        from __future__ import annotations

        from typing import TYPE_CHECKING, Any

        if TYPE_CHECKING:
            from collections.abc import Callable as Alias
        else:
            from typing import Any as Alias

        if not TYPE_CHECKING:
            from typing import Any as AliasReverse
        else:
            from collections.abc import Callable as AliasReverse

        if TYPE_CHECKING:
            from collections.abc import Callable as Imported
        else:
            from typing import Any as Imported

        FacadeMethodComposed = Imported[..., Any]

        if TYPE_CHECKING:
            from typing import Any as AliasBothAny
        else:
            from typing import Any as AliasBothAny


        class AliasedImportBranchBodyFirst:
            callback: Alias[..., Any]


        class AliasedImportBranchElseFirst:
            callback: AliasReverse[..., Any]


        class AliasedImportBranchQuoted:
            callback: "Alias[..., Any]"


        class AliasedImportBranchAssignComposed:
            callback: FacadeMethodComposed


        class AliasedBothAny:
            callback: AliasBothAny[..., Any]


        class LegitImportBranch:
            resolve_workspace_id: Alias[[], int]
        """
    )
    offenders = _callable_facade_offenders(source, "fixture")
    assert sorted(offenders) == [
        "fixture.AliasedImportBranchAssignComposed.callback",
        "fixture.AliasedImportBranchBodyFirst.callback",
        "fixture.AliasedImportBranchElseFirst.callback",
        "fixture.AliasedImportBranchQuoted.callback",
    ]


def test_callable_facade_matcher_deliberately_rejects_uncorrelated_candidates():
    """Mutually exclusive bindings may synthesize an intentionally rejected form."""
    both_callable = "from collections.abc import Callable as Outer, Callable as Inner"
    both_any = "from typing import Any as Outer, Any as Inner"
    forbidden = (
        "from collections.abc import Callable as Outer\n"
        "from typing import Any as Inner"
    )

    def fixture(imports: str, annotation: str = "Outer[..., Inner]") -> str:
        return imports + f"\nclass Example:\n    callback: {annotation}\n"

    # Each feasible branch alone is harmless; a real Callable/Any pairing
    # must still fail, while the application's explicit signature stays legal.
    assert _callable_facade_offenders(fixture(both_callable), "fixture") == []
    assert _callable_facade_offenders(fixture(both_any), "fixture") == []
    assert _callable_facade_offenders(fixture(forbidden), "fixture") == ["fixture.Example.callback"]
    assert _callable_facade_offenders(fixture(forbidden, "Outer[[], int]"), "fixture") == []

    correlated = textwrap.dedent(
        """
        from typing import TYPE_CHECKING
        if TYPE_CHECKING:
            from collections.abc import Callable as Outer, Callable as Inner
        else:
            from typing import Any as Outer, Any as Inner

        class Example:
            callback: Outer[..., Inner]
        """
    )
    tree = ast.parse(correlated)
    annotation = tree.body[-1].body[0].annotation
    candidates = _canonical_annotation_texts(annotation, _collect_import_aliases(tree))
    assert candidates == frozenset({
        "Callable[..., Callable]", "Any[..., Any]",
        "Callable[..., Any]", "Any[..., Callable]",
    })
    assert _callable_facade_offenders(correlated, "fixture") == ["fixture.Example.callback"]


def test_callable_facade_matcher_resolves_top_level_collections_alias():
    """Parse aliases without executing imports or quoted annotations."""
    source = textwrap.dedent('''
        from typing import Any
        import collections as c
        class Forbidden:
            direct: c.abc.Callable[..., Any]
            quoted: "c.abc.Callable[..., Any]"
        class Allowed:
            specific: c.abc.Callable[[], int]
            values: c.abc.Iterable[Any]
    ''')
    assert _callable_facade_offenders(source, "fixture") == [
        "fixture.Forbidden.direct", "fixture.Forbidden.quoted",
    ]
