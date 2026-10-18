"""SQL stays behind the ``Database`` façade and ``vireo/repositories/``.

``test_db_facade_structure.py`` keeps SQL out of ``Database`` itself; these
tests hold the other side of the boundary. Code outside the data layer reaches
the catalog through ``Database`` methods, never through its connection or its
active-workspace state:

- No module outside ``db.py`` and ``vireo/repositories/`` reads or writes
  ``Database._active_workspace_id`` or calls ``Database._ws_id()``. Read the
  active workspace with ``db.active_workspace_id`` (``None`` when unset) or
  ``db.require_workspace_id()`` (raises when unset), and change it with
  ``db.set_active_workspace``.
- ``<expr>.conn`` outside the data layer (``db.conn.execute(...)``,
  ``self.db.conn``, ``thread_db.conn.commit()``, a stored ``self.conn``) may only shrink. Each file is
  capped at its current count; move the SQL into a repository method through a
  domain accessor and lower the cap. Aliasing the connection to a local
  (``conn = db.conn``) to get under the cap defeats the point: the SQL still
  runs outside the data layer.

- Nothing in the repository closes a catalog handle through the raw
  connection: not ``<expr>.conn.close()``, and not ``conn.close()`` on a local
  name assigned (plain or annotated, on any earlier path of the function)
  from ``<expr>.conn``. ``Database.close()`` is the one method
  that does (its body is the only exemption): it also drops ``conn`` and
  makes ``db.closed`` true, so a raw close leaves a ``Database`` that claims
  to be open while every call raises ``sqlite3.ProgrammingError``. This rule
  scans every first-party Python file, including tests, the harness, ``db.py``
  itself, ``tools/`` and root-level modules.

``canonical_schema.py`` is part of the data layer, and ``vireo/testing/`` is
the test harness, so neither is scanned for the first two rules.
"""
import ast
from pathlib import Path

import pytest

VIREO = Path(__file__).resolve().parents[1]

DATA_LAYER = {"db.py", "canonical_schema.py"}
SKIPPED_DIRS = {"repositories", "tests", "testing"}

PRIVATE_WORKSPACE_ATTRS = {"_active_workspace_id", "_ws_id"}

# ``<expr>.conn`` uses per module (path relative to vireo/). A module not
# listed here may have none. Lower an entry (or delete it at zero) when you
# move a module's SQL into a repository; never raise one.
CONN_USE_LIMITS = {
    "app.py": 8,
    "audit.py": 10,
    "capture_time.py": 7,
    "card_cleanup.py": 4,
    "computation_cache.py": 37,
    "culling.py": 6,
    "duplicate_scan.py": 1,
    "export.py": 2,
    "file_identity.py": 2,
    "import_dedup.py": 7,
    "importer.py": 1,
    "ingest.py": 2,
    "jobs.py": 6,
    "keyword_identity.py": 48,
    "label_source_identities.py": 8,
    "local_masks.py": 1,
    "location_review.py": 1,
    "misses.py": 4,
    "move.py": 31,
    "move_cleanup.py": 11,
    "new_images.py": 5,
    "pending_archives.py": 3,
    "pipeline.py": 18,
    "pipeline_job.py": 9,
    "pipeline_plan.py": 2,
    "pipeline_stages/classification.py": 2,
    "pipeline_stages/detection.py": 4,
    "pipeline_stages/features.py": 4,
    "pipeline_stages/media.py": 2,
    "pipeline_stages/scanning.py": 1,
    "preview_cache.py": 4,
    "render_source.py": 2,
    "schema.py": 1,
    "site_export.py": 3,
    "species_identity.py": 5,
    "species_identity_repair.py": 3,
    "staging_recovery.py": 1,
    "subjects.py": 20,
    "sync.py": 5,
    "thumbnails.py": 11,
    "volume_reachability.py": 8,
    "working_copy_cache.py": 9,
}


REPO = VIREO.parent
# Directory names that never hold first-party code.
RAW_CLOSE_SKIPPED_DIRS = {"node_modules", "build", "dist", "__pycache__"}


def _every_module():
    """Every first-party Python file in the repository, ``db.py`` included."""
    for path in sorted(REPO.rglob("*.py")):
        rel = path.relative_to(REPO)
        if any(part.startswith(".") or part in RAW_CLOSE_SKIPPED_DIRS for part in rel.parts):
            continue
        yield rel.as_posix(), ast.parse(path.read_text(encoding="utf-8"))


def _is_conn_read(node):
    """``<expr>.conn``, ``getattr(<expr>, "conn", ...)`` or a walrus of either."""
    if isinstance(node, ast.Attribute) and node.attr == "conn":
        return True
    if isinstance(node, ast.NamedExpr):
        return _is_conn_read(node.value)
    return (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "getattr"
        and len(node.args) >= 2
        and isinstance(node.args[1], ast.Constant)
        and node.args[1].value == "conn"
    )


def _scope_bindings(scope):
    """Names that shadow an enclosing binding in a function or lambda.

    Parameters, local assignments, imports, nested definitions, except
    targets and ``global`` declarations shadow; ``nonlocal`` names the
    enclosing binding itself, so it never does.
    """
    args = scope.args
    parameters = [*args.posonlyargs, *args.args, *args.kwonlyargs]
    parameters += [arg for arg in (args.vararg, args.kwarg) if arg is not None]
    names = {arg.arg for arg in parameters}
    nonlocals = set()

    def collect(node):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            names.add(node.name)
            return
        if isinstance(node, (ast.Lambda, ast.ListComp, ast.SetComp, ast.DictComp, ast.GeneratorExp)):
            return
        if isinstance(node, ast.Name) and isinstance(node.ctx, (ast.Store, ast.Del)):
            names.add(node.id)
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            names.update(alias.asname or alias.name.split(".")[0] for alias in node.names)
        if isinstance(node, ast.ExceptHandler) and node.name:
            names.add(node.name)
        if isinstance(node, ast.Global):
            # Refers to the module binding, not the enclosing function's:
            # ``_raw_connection_closes`` re-adds it if the module aliased it.
            names.update(node.names)
        if isinstance(node, ast.Nonlocal):
            nonlocals.update(node.names)
        for child in ast.iter_child_nodes(node):
            collect(child)

    for node in [scope.body] if isinstance(scope, ast.Lambda) else scope.body:
        collect(node)
    return names - nonlocals


def _declared_globals(scope):
    """Names a function or lambda body declares ``global``."""
    found = set()

    def collect(node):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Lambda)):
            return
        if isinstance(node, ast.Global):
            found.update(node.names)
        for child in ast.iter_child_nodes(node):
            collect(child)

    for node in [scope.body] if isinstance(scope, ast.Lambda) else scope.body:
        collect(node)
    return found


def _module_aliases(tree):
    """Module-level names bound from a connection read, wherever in the module."""
    found = set()

    def collect(node):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Lambda)):
            return
        if isinstance(node, ast.Assign) and _is_conn_read(node.value):
            found.update(t.id for t in node.targets if isinstance(t, ast.Name))
        if isinstance(node, (ast.AnnAssign, ast.NamedExpr)) and isinstance(node.target, ast.Name) and _is_conn_read(node.value):
            found.add(node.target.id)
        for child in ast.iter_child_nodes(node):
            collect(child)

    collect(tree)
    return found


COMPREHENSIONS = (ast.ListComp, ast.SetComp, ast.DictComp, ast.GeneratorExp)


def _raw_connection_closes(tree, *, exempt=()):
    """Line numbers of raw connection closes.

    Flags ``<expr>.conn.close()`` and, within one function, ``name.close()``
    where ``name`` was assigned (plain or annotated) from ``<expr>.conn`` or
    ``getattr(<expr>, "conn", ...)`` anywhere earlier in that function. The
    Captured aliases carry into nested functions unless parameters or local
    bindings shadow them. Class attributes are not method closure bindings.
    Tracking is syntactic and fails closed: a later rebinding does not clear
    the alias, because a rebinding inside one branch would otherwise hide a
    close on the other path. Use a different name for a different
    connection. ``exempt`` names ``Class.method`` qualnames whose bodies are
    skipped.
    """
    offenders = []
    module_aliases = _module_aliases(tree)

    def visit(node, aliases, qualname, class_enclosing=None, shadowed=frozenset()):
        # ``shadowed``: names bound by an enclosing function scope, which hide
        # a module alias of the same name from everything nested inside.
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda, ast.ClassDef)):
            name = getattr(node, "name", "<lambda>")
            inner = f"{qualname}.{name}" if qualname else name
            enclosing = aliases if class_enclosing is None else class_enclosing
            # Decorators, bases, type parameters, defaults and annotations run
            # where the definition is evaluated: before the parameters shadow
            # anything, and outside any body exemption.
            outer_parts = list(getattr(node, "decorator_list", []))
            outer_parts += list(getattr(node, "type_params", []))
            if isinstance(node, ast.ClassDef):
                outer_parts += node.bases + [k.value for k in node.keywords]
            else:
                outer_parts.append(node.args)
                if getattr(node, "returns", None) is not None:
                    outer_parts.append(node.returns)
            for part in outer_parts:
                visit(part, aliases, qualname, class_enclosing, shadowed)
            if inner in exempt:
                return
            if isinstance(node, ast.ClassDef):
                # A class namespace is not a closure scope: a nested class
                # body starts from the enclosing function/module aliases, not
                # from the containing class's.
                class_aliases = enclosing.copy()
                for child in node.body:
                    visit(child, class_aliases, inner, enclosing, shadowed)
                return
            bindings = _scope_bindings(node)
            inner_shadowed = shadowed | bindings
            # Module aliases are visible from any function body unless this
            # or an enclosing function binds the name; they resolve at call
            # time (hence the module pre-pass). ``global`` bypasses shadows.
            aliases = (enclosing - bindings) | (module_aliases - inner_shadowed)
            aliases |= _declared_globals(node) & module_aliases
            body = node.body if isinstance(node.body, list) else [node.body]
            for child in body:
                visit(child, aliases, inner, None, inner_shadowed)
            return
        if isinstance(node, COMPREHENSIONS):
            # The first iterable is evaluated in the enclosing scope; the
            # comprehension's own targets shadow aliases everywhere else.
            first, *rest = node.generators
            visit(first.iter, aliases, qualname, class_enclosing, shadowed)
            bound = {
                n.id for g in node.generators for n in ast.walk(g.target)
                if isinstance(n, ast.Name)
            }
            comp_aliases = aliases - bound
            for part in first.ifs + [x for g in rest for x in [g.iter, *g.ifs]]:
                visit(part, comp_aliases, qualname, class_enclosing, shadowed)
            for part in [node.key, node.value] if isinstance(node, ast.DictComp) else [node.elt]:
                visit(part, comp_aliases, qualname, class_enclosing, shadowed)
            return
        if isinstance(node, ast.Assign) and _is_conn_read(node.value):
            aliases.update(t.id for t in node.targets if isinstance(t, ast.Name))
        if isinstance(node, (ast.AnnAssign, ast.NamedExpr)) and isinstance(node.target, ast.Name) and _is_conn_read(node.value):
            aliases.add(node.target.id)
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "close"
            and (
                _is_conn_read(node.func.value)
                or (isinstance(node.func.value, ast.Name) and node.func.value.id in aliases)
            )
        ):
            offenders.append(node.lineno)
        for child in ast.iter_child_nodes(node):
            visit(child, aliases, qualname, class_enclosing, shadowed)

    visit(tree, set(), "")
    return offenders


def _modules():
    for path in sorted(VIREO.rglob("*.py")):
        rel = path.relative_to(VIREO)
        if rel.parts[0] in SKIPPED_DIRS or str(rel) in DATA_LAYER:
            continue
        yield rel.as_posix(), ast.parse(path.read_text(encoding="utf-8"))


def _uses(tree, attrs):
    """Line numbers of ``<expr>.<attr>`` for ``attr`` in ``attrs``."""
    return [
        node.lineno
        for node in ast.walk(tree)
        if isinstance(node, ast.Attribute) and node.attr in attrs
    ]


def test_no_private_workspace_state_outside_data_layer():
    offenders = {
        name: lines
        for name, tree in _modules()
        if (lines := _uses(tree, PRIVATE_WORKSPACE_ATTRS))
    }
    assert not offenders, (
        "Private Database workspace state used outside the data layer "
        f"(module: lines): {offenders}. Use db.active_workspace_id, "
        "db.require_workspace_id() or db.set_active_workspace() instead."
    )


def test_connection_use_outside_data_layer_only_shrinks():
    actual = {
        name: count
        for name, tree in _modules()
        if (count := len(_uses(tree, {"conn"})))
    }
    grown = {
        name: (count, CONN_USE_LIMITS.get(name, 0))
        for name, count in actual.items()
        if count > CONN_USE_LIMITS.get(name, 0)
    }
    assert not grown, (
        "Catalog connection use outside the data layer grew past its limit "
        f"(uses, limit): {grown}. Put the SQL in a vireo/repositories/ module "
        "through a Database domain accessor and call that instead."
    )
    shrunk = {
        name: (actual.get(name, 0), limit)
        for name, limit in CONN_USE_LIMITS.items()
        if actual.get(name, 0) < limit
    }
    assert not shrunk, (
        "Catalog connection use outside the data layer shrank below its limit "
        f"(uses, limit): {shrunk}. Lower CONN_USE_LIMITS in "
        "vireo/tests/test_sql_boundary.py to match (delete entries at zero) so "
        "the move sticks."
    )


def test_connections_close_through_database_close():
    offenders = {
        name: lines
        for name, tree in _every_module()
        if (lines := _raw_connection_closes(
            tree, exempt=("Database.close",) if name == "vireo/db.py" else (),
        ))
    }
    assert not offenders, (
        "A catalog connection was closed through its raw handle "
        f"(module: lines): {offenders}. Call db.close() instead: it closes "
        "the connection, drops db.conn and makes db.closed true, so the "
        "Database never claims to be open over a dead handle."
    )


def test_raw_close_scan_includes_docs_prototypes(tmp_path, monkeypatch):
    monkeypatch.setitem(globals(), "REPO", tmp_path)
    name = "docs/plans/example-prototype/check.py"
    path = tmp_path / name
    path.parent.mkdir(parents=True)
    path.write_text("db.conn.close()\n", encoding="utf-8")

    modules = dict(_every_module())
    assert name in modules
    assert _raw_connection_closes(modules[name]) == [1]


@pytest.mark.parametrize(("source", "lines"), [
    ("db.conn.close()", [1]),
    ("conn = db.conn\nconn.close()", [2]),
    ("conn = getattr(db, 'conn', None)\nconn.close()", [2]),
    ("conn: Connection = db.conn\nconn.close()", [2]),
    ("conn: Connection = getattr(db, 'conn', None)\nconn.close()", [2]),
    ("conn = db.conn\nconn: Connection\nconn.close()", [3]),
    ("conn = db.conn\nconn: Connection = other\nconn.close()", [3]),
    ("conn = db.conn\nconn = other\nconn.close()", [3]),
    ("conn = db.conn\nconn = consume(conn.close())", [2]),
    ("conn = db.conn\nconn: Connection = consume(conn.close())", [2]),
    ("conn = db.conn\nconn = consume(conn.close())\nconn.close()", [2, 3]),
    ("conn = db.conn\nif replace:\n conn = other\nconn.close()", [4]),
    ("conn = db.conn\nif replace:\n pass\nelse:\n conn = other\nconn.close()", [6]),
    ("if choose:\n conn = db.conn\nelse:\n conn = other\nconn.close()", [5]),
    ("conn = db.conn\nif choose:\n conn = other\nelse:\n conn = another\nconn.close()", [6]),
    ("conn = db.conn\nif choose:\n conn = other\n conn.close()\nconn.close()", [4, 5]),
    ("conn = db.conn\nif choose:\n if replace:\n  conn = other\nconn.close()", [5]),
    ("conn = db.conn\ndef unrelated(conn):\n conn.close()\nconn.close()", [4]),
    ("def first(db):\n conn = db.conn\ndef second(conn):\n conn.close()", []),
    ("def outer(db):\n conn = db.conn\n def inner(conn):\n  conn.close()\n conn.close()", [5]),
    ("def outer(db):\n conn = db.conn\n def cleanup():\n  conn.close()", [4]),
    ("def outer(db):\n conn = db.conn\n return lambda: conn.close()", [3]),
    ("def outer(db):\n def early():\n  conn.close()\n conn = db.conn", []),
    ("conn = db.conn\nclass Unrelated:\n conn = other\nconn.close()", [4]),
    ("def typed(db):\n conn: Connection = db.conn\n conn.close()", [3]),
    ("def conditional(db, replace):\n conn = db.conn\n if replace:\n  conn = other\n conn.close()", [5]),
    ("def rebind(db):\n conn = db.conn\n conn = consume(conn.close())", [3]),
    ("def outer(db):\n conn = db.conn\n def cleanup(conn=conn.close()):\n  pass", [3]),
    ("def outer(db):\n conn = db.conn\n def cleanup(*, conn=consume(conn.close())):\n  pass", [3]),
    ("def outer(db):\n conn = db.conn\n def cleanup(conn: conn.close()):\n  pass", [3]),
    ("def outer(db):\n conn = db.conn\n def cleanup(conn) -> conn.close():\n  pass", [3]),
    ("conn = db.conn\ndef cleanup():\n global conn\n conn.close()", [4]),
    ("def outer(db):\n conn = db.conn\n def cleanup():\n  nonlocal conn\n  conn = other\n  conn.close()", [6]),
    ("conn = db.conn\ndef cleanup[T: conn.close()]():\n pass", [2]),
    ("conn = db.conn\nclass K[T: conn.close()]:\n pass", [2]),
    ("conn = db.conn\n@consume(conn.close())\nclass K:\n pass", [2]),
    ("def cleanup():\n global conn\n conn.close()\nconn = db.conn", [3]),
    ("if (conn := db.conn):\n conn.close()", [2]),
    ("conn = db.conn\n[conn.close() for conn in others]", []),
    ("conn = db.conn\n{k: conn.close() for k, conn in pairs}", []),
    ("conn = db.conn\n[conn.close() for x in others]", [2]),
    ("conn = db.conn\n[x for x in conn.close()]", [2]),
    ("class Outer:\n conn = db.conn\n class Inner:\n  conn.close()", []),
    ("class Outer:\n conn = db.conn\n class Inner(consume(conn.close())):\n  pass", [3]),
    ("def cleanup():\n conn.close()\nconn = db.conn", [2]),
    ("def cleanup():\n conn = other\n conn.close()\nconn = db.conn", []),
    ("(conn := db.conn).close()", [1]),
    ("conn = db.conn\ndef outer(conn):\n def cleanup():\n  conn.close()", []),
    ("conn = db.conn\ndef outer():\n conn = other\n def cleanup():\n  conn.close()", []),
    ("conn = db.conn\ndef outer():\n import conn\n def cleanup():\n  conn.close()", []),
    ("conn = db.conn\ndef outer(conn):\n def cleanup():\n  global conn\n  conn.close()", [5]),
    ("conn = db.conn\ndef outer(x):\n def cleanup():\n  conn.close()", [4]),
    ("conn = sqlite3.connect(path)\nconn.close()", []),
    ("db.close()\nother.close()", []),
])
def test_raw_close_fail_closed_alias_contract(source, lines):
    assert _raw_connection_closes(ast.parse(source)) == lines


def test_raw_close_exempts_only_database_close():
    tree = ast.parse(
        "class Database:\n"
        " def close(self):\n  conn = getattr(self, 'conn', None)\n  conn.close()\n"
        " def unsafe(self):\n  self.conn.close()\n"
        "class Other:\n def close(self):\n  self.conn.close()\n"
    )
    assert _raw_connection_closes(tree) == [4, 6, 9]
    assert _raw_connection_closes(tree, exempt=("Database.close",)) == [6, 9]


def test_raw_close_preserves_actual_database_close_owner():
    tree = ast.parse((VIREO / "db.py").read_text(encoding="utf-8"))
    assert _raw_connection_closes(tree), "The real owned handle close must be recognized"
    assert _raw_connection_closes(tree, exempt=("Database.close",)) == []


@pytest.mark.parametrize(("source", "lines"), [
    ("def outer(db):\n conn = db.conn\n def cleanup():\n  conn.close()", [4]),
    ("def outer(db):\n conn: Connection = getattr(db, 'conn')\n async def cleanup():\n  conn.close()", [4]),
    ("def outer(db):\n conn = db.conn\n def cleanup(conn):\n  conn.close()", []),
    ("def outer(db):\n conn = db.conn\n def cleanup(*, conn):\n  conn.close()", []),
    ("def outer(db):\n conn = db.conn\n def cleanup():\n  conn = other\n  conn.close()", []),
    ("def outer(db):\n conn = db.conn\n def cleanup():\n  conn.close()\n  conn = other", []),
    ("def outer(db):\n conn = db.conn\n def cleanup():\n  conn: Connection\n  conn.close()", []),
    ("def outer(db):\n conn = db.conn\n def cleanup():\n  conn = db.conn\n  conn.close()", [5]),
    ("def outer(db):\n conn = db.conn\n def cleanup():\n  nonlocal conn\n  conn.close()", [5]),
    ("def outer(db):\n conn = db.conn\n def middle():\n  def cleanup():\n   conn.close()", [5]),
    ("def outer(db):\n conn = db.conn\n cleanup = lambda: conn.close()", [3]),
    ("def outer(db):\n conn = db.conn\n cleanup = lambda conn: conn.close()", []),
    ("def outer(db):\n conn = db.conn\n def cleanup():\n  [conn for conn in others]\n  conn.close()", [5]),
    ("class Container:\n conn = db.conn\n def cleanup(self):\n  conn.close()", []),
    ("def outer(db):\n conn = db.conn\n class Container:\n  conn = other\n  def cleanup(self):\n   conn.close()", [6]),
    ("def outer(db):\n conn = db.conn\n def cleanup():\n  global conn\n  conn.close()", []),
])
def test_raw_close_captured_aliases_respect_shadowing(source, lines):
    assert _raw_connection_closes(ast.parse(source)) == lines


def test_raw_close_exemption_covers_only_the_body():
    tree = ast.parse(
        "class Database:\n"
        " @consume(db.conn.close())\n"
        " def close(self, conn=other.conn.close()):\n"
        "  conn = getattr(self, 'conn', None)\n  conn.close()\n"
    )
    assert _raw_connection_closes(tree, exempt=("Database.close",)) == [2, 3]
