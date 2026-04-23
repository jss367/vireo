"""Content-addressable fingerprint for a classifier's label set.

The classifier's output is a pure function of (model, labels, input). We key
cached predictions by (classifier_model, labels_fingerprint) so two workspaces
running the same model with different regional lists stay disjoint rather than
conflicting or silently clobbering each other.
"""

import hashlib

TOL_SENTINEL = "tol"
LEGACY_SENTINEL = "legacy"


def compute_fingerprint(labels):
    """sha256 hex prefix of sorted, deduped labels. TOL_SENTINEL when empty."""
    if not labels:
        return TOL_SENTINEL
    canonical = "\n".join(sorted(set(labels))).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()[:12]
