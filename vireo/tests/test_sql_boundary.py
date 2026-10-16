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
  name assigned from ``<expr>.conn``. ``Database.close()`` is the one method
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
    "best_batch.py": 2,
    "capture_time.py": 7,
    "card_cleanup.py": 4,
    "classify_job.py": 19,
    "computation_cache.py": 37,
    "culling.py": 6,
    "duplicate_scan.py": 1,
    "export.py": 2,
    "file_identity.py": 2,
    "import_dedup.py": 7,
    "import_job.py": 20,
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
    "scanner.py": 96,
    "schema.py": 1,
    "site_export.py": 3,
    "species_identity.py": 5,
    "species_identity_repair.py": 3,
    "staging_recovery.py": 1,
    "subjects.py": 20,
    "sync.py": 5,
    "taxonomy.py": 31,
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
    """``<expr>.conn`` or ``getattr(<expr>, "conn", ...)``."""
    if isinstance(node, ast.Attribute) and node.attr == "conn":
        return True
    return (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "getattr"
        and len(node.args) >= 2
        and isinstance(node.args[1], ast.Constant)
        and node.args[1].value == "conn"
    )


def _raw_connection_closes(tree, *, exempt=()):
    """Line numbers of raw connection closes.

    Flags ``<expr>.conn.close()`` and, within one function, ``name.close()``
    where ``name`` was last assigned from ``<expr>.conn`` or
    ``getattr(<expr>, "conn", ...)``. The alias tracking is syntactic and
    per function: a name rebound to anything else stops being an alias.
    ``exempt`` names ``Class.method`` qualnames whose bodies are skipped.
    """
    offenders = []

    def visit(node, aliases, qualname):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            inner = f"{qualname}.{node.name}" if qualname else node.name
            if inner in exempt:
                return
            aliases = set() if not isinstance(node, ast.ClassDef) else aliases
            for child in ast.iter_child_nodes(node):
                visit(child, aliases, inner)
            return
        if isinstance(node, ast.Assign):
            names = {t.id for t in node.targets if isinstance(t, ast.Name)}
            (aliases.update if _is_conn_read(node.value) else aliases.difference_update)(names)
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
            visit(child, aliases, qualname)

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
