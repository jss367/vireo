"""Transaction ownership only moves toward the unit of work.

The target (``docs/plans/2026-10-25-unit-of-work-design.md``): repository and
``Database`` data methods never begin, commit or roll back a transaction, and
each route, job or service owns its transaction with ``with
db.transaction():``. Two counts measure the distance from that target, and
each is capped per module at its current value, both ways: a count may not
grow, and when it shrinks the cap must be lowered in the same change so the
progress sticks (delete an entry at zero).

``COMMIT_FLAG_LIMITS`` counts commit flags in every first-party module under
``vireo/`` (outside ``tests/`` and ``testing/``):

- a parameter named ``_commit`` or ``commit`` (``def tag_photo(...,
  _commit=True)``), the switch a callee uses to decide whether it ends its
  caller's transaction;
- a call argument passed by one of those names (``_commit=False``,
  ``commit=False``), each a caller asking a callee not to commit;
- a function named ``*_no_commit``, the same switch spelled as a twin method.

``DATA_LAYER_TRANSACTION_CONTROL_LIMITS`` counts transaction control inside
the data layer itself, ``vireo/db.py`` and ``vireo/repositories/*.py``:

- a call to ``commit``, ``rollback``, ``commit_with_retry``, ``begin``,
  ``begin_immediate``, ``transaction`` or ``_commits_held`` on any receiver
  (``self.conn.commit()``, ``self.commit_with_retry(self.conn)``,
  ``history.commit()``, ``with repo.transaction():``);
- ``execute``/``executescript``/``execute_with_retry`` of a constant
  ``BEGIN``, ``COMMIT``, ``END``, ``ROLLBACK``, ``SAVEPOINT`` or ``RELEASE``
  statement. Releasing an outermost savepoint commits its writes, so an
  ad-hoc ``SAVEPOINT``/``RELEASE`` block is a callee-owned transaction
  boundary the migration intends to remove and is counted. A ``ROLLBACK TO``
  a savepoint undoes work without ending it and is not counted;
- ``with <expr>.conn:`` or ``with conn:``, sqlite3's committing context.

The bodies of ``Database``'s transaction-control methods are the one place
transaction control belongs, so they are not counted: ``commit``,
``rollback``, ``commit_with_retry``, ``begin``, ``begin_immediate``,
``transaction``, ``close``, the commit hold ``_commits_held`` and the
``_Connection`` subclass behind it, and the module-level
``commit_with_retry`` helper.

Run ``python vireo/tests/test_transaction_ownership.py`` to print the current
counts in the form the limits below use.
"""
from __future__ import annotations

import ast
import re
import sys
from pathlib import Path

VIREO = Path(__file__).resolve().parents[1]
SKIPPED_DIRS = {"tests", "testing"}

COMMIT_FLAG_NAMES = {"_commit", "commit"}
CONTROL_CALLS = {
    "commit", "rollback", "commit_with_retry", "_commit_with_retry",
    "begin", "begin_immediate", "transaction", "_commits_held",
}
EXECUTE_CALLS = {"execute", "executescript", "execute_with_retry"}
ENDING_SQL = re.compile(
    r"\s*(BEGIN|COMMIT|END|ROLLBACK|SAVEPOINT|RELEASE)\b(?!\s+TO\b)",
    re.IGNORECASE,
)
EXEMPT_DATA_LAYER_QUALNAMES = {
    "vireo/db.py": {
        "_Connection",
        "commit_with_retry",
        "Database.commit",
        "Database.rollback",
        "Database.commit_with_retry",
        "Database.begin",
        "Database.begin_immediate",
        "Database.transaction",
        "Database.close",
        "Database._commits_held",
    },
}

# Commit flags per module (path relative to the repository root). A module not
# listed may have none. Lower an entry (delete it at zero) when a domain's
# data methods stop committing and lose their flag; never raise one.
COMMIT_FLAG_LIMITS: dict[str, int] = {
    "vireo/app.py": 1,
    "vireo/audit.py": 5,
    "vireo/db.py": 50,
    "vireo/import_job/cataloging.py": 2,
    "vireo/keyword_identity.py": 19,
    "vireo/label_source_identities.py": 2,
    "vireo/offline_cache.py": 1,
    "vireo/pipeline_stages/features.py": 5,
    "vireo/preview_cache.py": 1,
    "vireo/repositories/audit.py": 1,
    "vireo/repositories/caches.py": 1,
    "vireo/repositories/collections.py": 1,
    "vireo/repositories/edit_history.py": 1,
    "vireo/repositories/edits.py": 1,
    "vireo/repositories/folders.py": 4,
    "vireo/repositories/identity_alternatives.py": 1,
    "vireo/repositories/keyword_provenance.py": 12,
    "vireo/repositories/keyword_tree.py": 2,
    "vireo/repositories/keywords.py": 7,
    "vireo/repositories/locations.py": 1,
    "vireo/repositories/masks_features.py": 5,
    "vireo/repositories/meta.py": 1,
    "vireo/repositories/photo_keywords.py": 2,
    "vireo/repositories/photo_review.py": 10,
    "vireo/repositories/photos.py": 1,
    "vireo/repositories/predictions.py": 5,
    "vireo/repositories/species_curation.py": 11,
    "vireo/repositories/sync.py": 2,
    "vireo/repositories/workspace_folders.py": 3,
    "vireo/services/analysis_retry.py": 4,
    "vireo/services/bulk_species_assignments.py": 3,
    "vireo/services/catalog_deletion.py": 2,
    "vireo/services/edit_history.py": 1,
    "vireo/services/grouping_history.py": 13,
    "vireo/services/identity_alternatives.py": 8,
    "vireo/services/import_locations.py": 1,
    "vireo/services/imports.py": 9,
    "vireo/services/local_folder.py": 2,
    "vireo/services/local_workspace.py": 2,
    "vireo/services/maintenance_history.py": 1,
    "vireo/services/mask_extraction.py": 4,
    "vireo/services/pending_changes.py": 13,
    "vireo/services/photo_deletion.py": 1,
    "vireo/services/photo_review.py": 9,
    "vireo/services/render_cache.py": 10,
    "vireo/services/review_group_changes.py": 2,
    "vireo/services/subject_masks.py": 1,
    "vireo/services/workspace_folders.py": 1,
    "vireo/species_identity_repair.py": 1,
    "vireo/sync.py": 3,
    "vireo/taxonomy.py": 5,
    "vireo/volume_reachability.py": 1,
    "vireo/web/batch.py": 10,
    "vireo/web/encounters.py": 7,
    "vireo/web/highlights.py": 14,
    "vireo/web/job_launchers.py": 1,
    "vireo/web/keywords.py": 2,
    "vireo/web/locations.py": 3,
    "vireo/web/photo_edit_recipes.py": 3,
    "vireo/web/pipeline.py": 4,
    "vireo/web/predictions.py": 30,
    "vireo/web/species.py": 1,
    "vireo/web/sync.py": 4,
    "vireo/web/web_types.py": 1,
}

# Transaction control inside the data layer, per module. Same rules.
DATA_LAYER_TRANSACTION_CONTROL_LIMITS: dict[str, int] = {
    "vireo/db.py": 19,
    "vireo/repositories/audit.py": 2,
    "vireo/repositories/caches.py": 9,
    "vireo/repositories/collections.py": 7,
    "vireo/repositories/detections.py": 6,
    "vireo/repositories/duplicates.py": 2,
    "vireo/repositories/edit_history.py": 15,
    "vireo/repositories/edits.py": 5,
    "vireo/repositories/exif_search.py": 1,
    "vireo/repositories/folders.py": 13,
    "vireo/repositories/identity_alternatives.py": 5,
    "vireo/repositories/imports.py": 2,
    "vireo/repositories/inat.py": 1,
    "vireo/repositories/job_history.py": 1,
    "vireo/repositories/keyword_provenance.py": 4,
    "vireo/repositories/keyword_taxa.py": 1,
    "vireo/repositories/keyword_tree.py": 5,
    "vireo/repositories/keywords.py": 10,
    "vireo/repositories/locations.py": 10,
    "vireo/repositories/masks_features.py": 9,
    "vireo/repositories/meta.py": 1,
    "vireo/repositories/model_runs.py": 4,
    "vireo/repositories/moves_merge.py": 12,
    "vireo/repositories/pending_archives.py": 3,
    "vireo/repositories/photo_keywords.py": 2,
    "vireo/repositories/photo_labels.py": 4,
    "vireo/repositories/photo_review.py": 13,
    "vireo/repositories/photos.py": 12,
    "vireo/repositories/pipeline_features.py": 2,
    "vireo/repositories/predictions.py": 13,
    "vireo/repositories/processes.py": 3,
    "vireo/repositories/species_curation.py": 13,
    "vireo/repositories/stats.py": 2,
    "vireo/repositories/sync.py": 5,
    "vireo/repositories/working_copies.py": 2,
    "vireo/repositories/workspace_folders.py": 9,
    "vireo/repositories/workspaces.py": 7,
}


def _first_party_modules():
    for path in sorted(VIREO.rglob("*.py")):
        rel = path.relative_to(VIREO)
        if rel.parts[0] in SKIPPED_DIRS:
            continue
        yield (
            path.relative_to(VIREO.parent).as_posix(),
            ast.parse(path.read_text(encoding="utf-8")),
        )


def _is_data_layer(name):
    return name == "vireo/db.py" or name.startswith("vireo/repositories/")


def commit_flags(tree):
    """Line numbers of commit-flag parameters, arguments and ``*_no_commit`` defs."""
    lines = []
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name.endswith("_no_commit"):
            lines.append(node.lineno)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)):
            args = node.args
            for arg in [*args.posonlyargs, *args.args, *args.kwonlyargs]:
                if arg.arg in COMMIT_FLAG_NAMES:
                    lines.append(arg.lineno)
        if isinstance(node, ast.Call):
            lines.extend(
                keyword.value.lineno
                for keyword in node.keywords
                if keyword.arg in COMMIT_FLAG_NAMES
            )
    return sorted(lines)


def _callee_name(call):
    func = call.func
    if isinstance(func, ast.Attribute):
        return func.attr
    if isinstance(func, ast.Name):
        return func.id
    return None


def _is_ending_sql(call):
    if _callee_name(call) not in EXECUTE_CALLS:
        return False
    # ``conn.execute(sql)`` passes the statement first; the module helper
    # ``execute_with_retry(conn, sql)`` passes it second.
    for arg in call.args[:2]:
        if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
            return bool(ENDING_SQL.match(arg.value))
    return False


def _is_connection_context(expr):
    return (
        (isinstance(expr, ast.Attribute) and expr.attr == "conn")
        or (isinstance(expr, ast.Name) and expr.id == "conn")
    )


def transaction_control(tree, *, exempt=frozenset()):
    """Line numbers of transaction control outside the ``exempt`` qualnames."""
    lines = []

    def visit(node, qualname):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            inner = f"{qualname}.{node.name}" if qualname else node.name
            if inner in exempt:
                return
            for child in ast.iter_child_nodes(node):
                visit(child, inner)
            return
        if isinstance(node, ast.Call) and (
            _callee_name(node) in CONTROL_CALLS or _is_ending_sql(node)
        ):
            lines.append(node.lineno)
        if isinstance(node, (ast.With, ast.AsyncWith)):
            lines.extend(
                item.context_expr.lineno
                for item in node.items
                if _is_connection_context(item.context_expr)
            )
        for child in ast.iter_child_nodes(node):
            visit(child, qualname)

    visit(tree, "")
    return sorted(lines)


def current_counts():
    """``(commit flags, data-layer transaction control)`` per module."""
    flags, control = {}, {}
    for name, tree in _first_party_modules():
        if count := len(commit_flags(tree)):
            flags[name] = count
        if _is_data_layer(name):
            exempt = EXEMPT_DATA_LAYER_QUALNAMES.get(name, frozenset())
            if count := len(transaction_control(tree, exempt=exempt)):
                control[name] = count
    return flags, control


def _assert_matches(actual, limits, what, fix):
    grown = {
        name: (count, limits.get(name, 0))
        for name, count in actual.items()
        if count > limits.get(name, 0)
    }
    assert not grown, (
        f"{what} grew past its limit (count, limit): {grown}. {fix}"
    )
    shrunk = {
        name: (actual.get(name, 0), limit)
        for name, limit in limits.items()
        if actual.get(name, 0) < limit
    }
    assert not shrunk, (
        f"{what} shrank below its limit (count, limit): {shrunk}. Lower the "
        "entry in vireo/tests/test_transaction_ownership.py to match (delete "
        "it at zero) so the move sticks."
    )


def test_commit_flags_only_shrink():
    flags, _ = current_counts()
    _assert_matches(
        flags, COMMIT_FLAG_LIMITS, "Commit flags (_commit=/commit= parameters and arguments)",
        "Do not add a commit switch: make the callee never commit and wrap "
        "the caller's work in one `with db.transaction():`.",
    )


def test_data_layer_transaction_control_only_shrinks():
    _, control = current_counts()
    _assert_matches(
        control, DATA_LAYER_TRANSACTION_CONTROL_LIMITS,
        "Transaction control inside repository/Database data methods",
        "Data methods must not begin, commit or roll back: the route, job or "
        "service that calls them owns the transaction.",
    )


def test_limits_name_existing_modules():
    root = VIREO.parent
    missing = sorted(
        name
        for name in {*COMMIT_FLAG_LIMITS, *DATA_LAYER_TRANSACTION_CONTROL_LIMITS}
        if not (root / name).is_file()
    )
    assert not missing, f"Limits name modules that no longer exist: {missing}"


def test_commit_flag_scan_recognizes_each_shape():
    tree = ast.parse(
        "def a(x, _commit=True): pass\n"
        "def b(*, commit=False): pass\n"
        "f(_commit=False)\n"
        "g(x, commit=commit)\n"
        "def add_no_commit(x): pass\n"
        "h(committed=True, _commits=1)\n"
        "lambda _commit=True: None\n"
    )
    assert commit_flags(tree) == [1, 2, 3, 4, 5, 7]


def test_transaction_control_scan_recognizes_each_shape():
    tree = ast.parse(
        "class R:\n"
        "    def a(self):\n"
        "        self.conn.commit()\n"
        "        self.commit_with_retry(self.conn)\n"
        "        self.conn.rollback()\n"
        "        self.conn.execute('BEGIN IMMEDIATE')\n"
        "        self.conn.execute('ROLLBACK TO sp')\n"
        "        self.conn.execute('SAVEPOINT sp')\n"
        "        self.conn.execute('RELEASE sp')\n"
        "        with self.conn:\n"
        "            pass\n"
        "        with repo.transaction():\n"
        "            pass\n"
        "        execute_with_retry(self.conn, 'COMMIT')\n"
        "        self.conn.execute('SELECT 1')\n"
    )
    assert transaction_control(tree) == [3, 4, 5, 6, 8, 9, 10, 12, 14]


def test_transaction_control_scan_skips_exempt_bodies_only():
    tree = ast.parse(
        "class Database:\n"
        "    def commit(self):\n"
        "        self.conn.commit()\n"
        "    def tag(self):\n"
        "        self.conn.commit()\n"
    )
    assert transaction_control(tree, exempt={"Database.commit"}) == [5]


def _print_limits():
    flags, control = current_counts()
    for title, counts in (
        ("COMMIT_FLAG_LIMITS", flags),
        ("DATA_LAYER_TRANSACTION_CONTROL_LIMITS", control),
    ):
        print(f"{title}: dict[str, int] = {{")
        for name in sorted(counts):
            print(f'    "{name}": {counts[name]},')
        print("}")
        print(f"# total: {sum(counts.values())}")


if __name__ == "__main__":
    sys.exit(_print_limits())
