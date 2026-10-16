"""Schema DDL lives in the schema modules, not in the code that writes rows.

``canonical_schema.py`` builds every table, index, trigger and view;
``schema.py`` holds the numbered migrations that bring an older catalog to
that shape; ``photo_visibility_schema.py`` and ``metadata_search.py`` hold
the DDL those two import. Everything else used to create what it needed on
first write (``CREATE TABLE IF NOT EXISTS job_history`` in the job runner,
one ``ALTER`` per new column), which is why ``PRAGMA user_version`` said
nothing about whether a catalog had those objects. Migration 18 moved the
last of them; this test keeps new ones from appearing.

Scratch tables (``CREATE TEMP TABLE``, and the index the scanner puts on its
temp scope table) are not schema and are not counted.

Deadline: ``DDL_LIMITS`` is empty by 2026-12-31
(``docs/plans/2026-10-16-frontend-schema-consolidation-plan.md``).
"""
import ast
import re
from pathlib import Path

VIREO = Path(__file__).resolve().parents[1]
SCHEMA_MODULES = {
    "canonical_schema.py", "schema.py", "photo_visibility_schema.py", "metadata_search.py",
}
SKIPPED_DIRS = {"tests", "testing"}

# DDL statements per module, outside the schema modules. Lower an entry when
# you move one into canonical_schema.py (delete it at zero); never raise one:
# new objects go in canonical_schema.py with a migration in schema.py.
DDL_LIMITS = {
    # An index on the scanner's CREATE TEMP TABLE working_copy_scope: scratch,
    # but spelled without TEMP so the scan cannot tell.
    "scanner.py": 1,
}

_DDL_RE = re.compile(
    r"\b(?:CREATE\s+(?:UNIQUE\s+)?(?:TABLE|INDEX|TRIGGER|VIEW|VIRTUAL\s+TABLE)|ALTER\s+TABLE)\b"
)


def _docstrings(tree):
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            body = node.body
            if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant):
                yield body[0].value


def _ddl_statements():
    """DDL keywords in each module's string literals (docstrings and comments
    describe the schema; only code that could execute it counts)."""
    counts = {}
    for path in sorted(VIREO.rglob("*.py")):
        rel = path.relative_to(VIREO)
        if rel.parts[0] in SKIPPED_DIRS or rel.as_posix() in SCHEMA_MODULES:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"))
        docstrings = set(map(id, _docstrings(tree)))
        count = sum(
            len(_DDL_RE.findall(node.value))
            for node in ast.walk(tree)
            if isinstance(node, ast.Constant) and isinstance(node.value, str)
            and id(node) not in docstrings
        )
        if count:
            counts[rel.as_posix()] = count
    return counts


def test_ddl_outside_schema_modules_only_shrinks():
    actual = _ddl_statements()
    grown = {
        name: (count, DDL_LIMITS.get(name, 0))
        for name, count in actual.items()
        if count > DDL_LIMITS.get(name, 0)
    }
    assert not grown, (
        f"DDL outside the schema modules grew (statements, limit): {grown}. "
        "Create the object in canonical_schema.py and add a schema.py "
        "migration for existing catalogs instead of creating it on first write."
    )
    shrunk = {
        name: (actual.get(name, 0), limit)
        for name, limit in DDL_LIMITS.items()
        if actual.get(name, 0) < limit
    }
    assert not shrunk, (
        f"DDL outside the schema modules shrank (statements, limit): {shrunk}. "
        "Lower DDL_LIMITS in vireo/tests/test_schema_ddl_boundary.py (delete "
        "the entry at zero) so the move sticks."
    )
