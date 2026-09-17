"""Can this install classify right now? Shared by onboarding and /api/models/status."""


def classification_readiness(db):
    """Whether the active model can actually classify right now.

    A downloaded model is only usable if it can run label-free — a
    Tree-of-Life BioCLIP model, or a timm model with its intrinsic fixed
    class head — or an active species list exists. Mirrors the label
    gate in classify_job._load_labels and pipeline_plan
    (model_type == "timm" is never blocked) so this readiness signal
    answers the question callers actually ask — "can this install
    classify?" — not the cheaper "is a model on disk?" (CORE_PHILOSOPHY:
    no black boxes). Returns a dict with the active model and the flags
    the status endpoint and the onboarding redirects both consume.
    """
    from models import get_active_model, tree_of_life_ready

    active = get_active_model()
    model_downloaded = bool(active and active.get("downloaded"))
    # tree_of_life_ready (not just supports_tree_of_life) so an install
    # whose optional ToL artifacts weren't downloaded (bioclip-2.5
    # before its HF upload landed, or after a skipped optional
    # download) is not falsely reported as "classification ready" —
    # otherwise the pipeline would crash in Classifier's constructor.
    label_free = bool(active and (
        tree_of_life_ready(
            active.get("model_str"), active.get("weights_path"),
        )
        or active.get("model_type") == "timm"
    ))
    # A fixed class head never reads a species list: classify_job returns
    # before touching labels for timm, so a broken selection cannot block
    # it. Every other model type consumes the selection when one exists,
    # Tree-of-Life-capable ones included.
    reads_labels = bool(active and active.get("model_type") != "timm")
    labels_ready = False
    labels_blocked = False
    if model_downloaded and reads_labels:
        try:
            import os

            from labels import (
                get_active_labels,
                get_saved_labels,
                load_merged_labels,
            )

            ws_labels = db.get_workspace_active_labels()
            if ws_labels is not None:
                saved_by_file = {
                    s["labels_file"]: s for s in get_saved_labels()
                }
                active_sets = [
                    saved_by_file.get(p, {"labels_file": p}) for p in ws_labels
                ]
            else:
                active_sets = get_active_labels()
            # Require a non-empty MERGED label list, not just an existing
            # path. classify_job._load_labels and the planner load the
            # files and treat an empty merged list as "no labels", so an
            # active-but-blank file must not report ready — otherwise we'd
            # redirect to /browse and then block/fail at classify.
            merged = load_merged_labels(active_sets) if active_sets else []
            labels_ready = len(merged) > 0
            # Asked even of a Tree-of-Life-ready model: a selection that
            # exists and classifies nothing makes _load_labels raise, so
            # this install cannot classify as configured however capable
            # the model is at running without labels.
            # Same rule as classify_job._any_present — a selection naming
            # only deleted files is a fallback, not a block.
            labels_blocked = not labels_ready and any(
                os.path.exists(ls.get("labels_file", "")) for ls in active_sets
            )
        except Exception:
            labels_ready = False
            labels_blocked = False

    usable = (label_free or labels_ready) and not labels_blocked
    return {
        "active": active,
        "model_downloaded": model_downloaded,
        "labels_ready": usable,
        "ready": model_downloaded and usable,
    }
