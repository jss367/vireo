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

    root_real = os.path.realpath(root)
    destination_real = os.path.realpath(destination)
    if (contains_resolved(destination_real, root_real)
            or contains_resolved(root_real, destination_real)
            or not contains_resolved(os.path.realpath(os.path.join(vireo_dir, "staging")), root_real)):
        raise ValueError("Temporary processing storage must be separate from the final destination.")
    target["local_archive_root"] = root
    target["managed_staging_root"] = root
    plan = {"destination": local_destination, "identity": identity}
    if not remote_archive:
        from pipeline_job import _archive_mount_baseline
        baseline = _archive_mount_baseline(destination, known_mounted_roots)
        for mount, was_mounted in (parent or {}).get("mount_baseline", {}).items():
            baseline[mount] = baseline.get(mount, False) or was_mounted
        target["mount_baseline"] = baseline
        plan["mount_baseline"] = baseline
        identities = _archive_mount_identities(baseline)
        identities.update((parent or {}).get("mount_identities", {}))
        target["mount_identities"] = identities
        plan["mount_identities"] = identities
    return plan, target


def _archive_mount_identities(baseline):
    """Identify network shares across reconnects, retaining strict disk checks."""
    from pipeline_job import _mount_identity_baseline
    from source_scan_policy import classify_sources

    identities = _mount_identity_baseline(baseline)
    for policy in classify_sources(list(identities)):
        key = policy["volume_key"]
        if policy["storage"] != "network" or key == "unknown-volume" or key.startswith("windows:drive:"):
            continue
        root = policy["path"]
        instance = identities[root]
        # Linux bind mounts can expose different subdirectories of one share.
        # Keep that root, but discard the transient mount ID and device number.
        subtree = instance[3] if instance and instance[0] == "mountinfo" else "/"
        identities[root] = ("network-share", key, subtree)
    return identities


def check_staged_mount(destination, baseline, identities):
    """Refuse an unavailable or replaced destination before deleting originals."""
    from pipeline_job import (
        _missing_archive_mount_root,
        _unmounted_since_baseline,
    )

    # JSON persistence turns the helper's tuple identities into lists.
    identities = {root: tuple(value) if isinstance(value, list) else value
                  for root, value in (identities or {}).items()}
    current = _archive_mount_identities({root: True for root in identities})
    changed = next((root for root, prior in identities.items()
                    if prior is None or current.get(root) != prior), None)
    if changed:
        raise ValueError(f"NAS volume changed: {changed}. Local originals are preserved; restore the original volume before retrying.")
    offline = (_unmounted_since_baseline(baseline or {})
               or _missing_archive_mount_root(destination))
    if offline:
        raise ValueError(f"NAS volume unavailable: {offline}. Local originals are preserved; reconnect it and retry.")
