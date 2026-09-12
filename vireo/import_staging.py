"""Managed local originals for imports that process before archiving."""

import os
import posixpath
import uuid

from local_processing import staging_root


def plan_staged_import(vireo_dir, destination, remote_archive=None, parent=None, known_mounted_roots=None):
    """Freeze the final destination and reuse only server-recorded retry storage.

    The staging leaf has the final folder's name so the normal verified
    folder move preserves the user's layout, including an empty template.
    No directories are created until the accepted job starts.
    """
    destination = (os.path.normpath(destination) if remote_archive
                   else os.path.realpath(destination))
    target = dict(remote_archive["target"]) if remote_archive else {
        "id": "", "name": "NAS", "transport": "mounted",
    }
    target["mount_path"] = os.path.dirname(destination)
    if remote_archive:
        target["remote_path"] = posixpath.dirname(remote_archive["ssh_final"])
    # Compare destination and transport before trusting the parent's storage.
    identity = {"destination": destination, "target": dict(target)}
    if parent and parent["identity"] != identity:
        raise ValueError("The original import's final destination has changed. Start a new import instead of retrying.")
    local_destination = (
        parent["destination"] if parent else staging_root(
            vireo_dir, "import-" + uuid.uuid4().hex, destination,
        )
    )
    root = os.path.dirname(local_destination)
    from path_guard import contains_resolved

    if (contains_resolved(destination, root)
            or contains_resolved(root, destination)
            or not contains_resolved(os.path.join(vireo_dir, "staging"), root)):
        raise ValueError("Temporary processing storage must be separate from the final destination.")
    target["local_archive_root"] = root
    target["managed_staging_root"] = root
    plan = {"destination": local_destination, "identity": identity}
    if not remote_archive:
        from pipeline_job import _archive_mount_baseline, _mount_identity_baseline
        baseline = _archive_mount_baseline(destination, known_mounted_roots)
        for mount, was_mounted in (parent or {}).get("mount_baseline", {}).items():
            baseline[mount] = baseline.get(mount, False) or was_mounted
        target["mount_baseline"] = baseline
        plan["mount_baseline"] = baseline
        identities = _mount_identity_baseline(baseline)
        identities.update((parent or {}).get("mount_identities", {}))
        target["mount_identities"] = identities
        plan["mount_identities"] = identities
    return plan, target


def check_staged_mount(destination, baseline, identities):
    """Refuse an unavailable or replaced destination before deleting originals."""
    from pipeline_job import (
        _changed_mount_since_baseline,
        _missing_archive_mount_root,
        _unmounted_since_baseline,
    )

    # JSON persistence turns the helper's tuple identities into lists.
    identities = {root: tuple(value) if isinstance(value, list) else value
                  for root, value in (identities or {}).items()}
    changed = _changed_mount_since_baseline(identities)
    if changed:
        raise ValueError(f"NAS volume changed: {changed}. Local originals are preserved; restore the original volume before retrying.")
    offline = (_unmounted_since_baseline(baseline or {})
               or _missing_archive_mount_root(destination))
    if offline:
        raise ValueError(f"NAS volume unavailable: {offline}. Local originals are preserved; reconnect it and retry.")
