"""Patch a name in every ``import_job`` module that looks it up.

``import_job`` is a package whose modules import shared helpers by name
(``from import_job.copy_verify import _hash_dest_file``), so each module that
calls a helper holds its own binding. Patching the helper in one module
leaves the others calling the original, silently. When ``import_job`` was a
single module one ``monkeypatch.setattr(import_job, name, ...)`` replaced the
only binding; ``patch_import_job`` replaces every binding, which is the same
effect.

Use it for a name that more than one module looks up (``_hash_dest_file``,
``compute_file_hash``, ``_key_twin_rows``, ``_hash_twin_rows``,
``_publish_resume_scope``). A name with one lookup site is patched on that
module directly (``import_job.planning.IMPORT_BATCH_SIZE``).
"""

import importlib
import pkgutil

import import_job


def import_job_modules():
    """Every module of the ``import_job`` package, the package included."""
    modules = [import_job]
    for info in pkgutil.iter_modules(import_job.__path__):
        modules.append(importlib.import_module(f"import_job.{info.name}"))
    return modules


def patch_import_job(monkeypatch, name, value):
    """Set ``name`` to ``value`` in every ``import_job`` module that binds it.

    Returns the patched module names; fails if no module binds ``name``, so
    a renamed or moved helper cannot turn the patch into a silent no-op.
    """
    patched = []
    for module in import_job_modules():
        if name in vars(module):
            monkeypatch.setattr(module, name, value)
            patched.append(module.__name__)
    assert patched, f"no import_job module binds {name!r}"
    return patched
