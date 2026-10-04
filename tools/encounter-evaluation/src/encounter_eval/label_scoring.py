"""Label-based species and short-sequence evaluation; labels never enter inference."""

from collections import Counter

from .library import timestamp
from .scoring import score


def measure(photos, answers, groups):
    c = Counter(score(photos, answers, groups))
    membership = {pid: i for i, g in enumerate(groups) for pid in g.photo_ids}
    for a, b in zip(photos, photos[1:], strict=False):
        aa, bb = answers.get(str(a["id"])), answers.get(str(b["id"]))
        if not aa or not bb:
            continue
        ta, tb = timestamp(a.get("timestamp")), timestamp(b.get("timestamp"))
        if ta is None or tb is None or a.get("folder_id") is None or a.get("folder_id") != b.get("folder_id"):
            continue
        if not 0 <= (tb - ta).total_seconds() <= 3:
            continue
        if len(aa["taxa"]) != 1 or len(bb["taxa"]) != 1:
            c["multiple_label_adjacent_pairs"] += 1
            continue
        same = aa["taxa"] == bb["taxa"]
        cut = membership[a["id"]] != membership[b["id"]]
        c["same_label_short_pairs" if same else "different_label_short_pairs"] += 1
        c["same_label_short_splits" if same else "different_label_short_joins"] += cut if same else not cut
        if aa["sources"] == bb["sources"] == ["manual"]:
            c["manual_same_label_pairs" if same else "manual_different_label_pairs"] += 1
            c["manual_same_label_splits" if same else "manual_different_label_joins"] += cut if same else not cut
    # Keep all reference labels in scoring, including unknown-provenance imports.
    for p in photos:
        a = answers.get(str(p["id"]))
        if a and a["sources"] == ["manual"]:
            c["manual_positive_labels"] += len(a["taxa"])
            g = groups[membership[p["id"]]]
            c["manual_recovered_positive_labels"] += len(set(a["taxa"]) & set(g.roster or ()))
    return c


def metrics(counts):
    c = Counter(counts)

    def ratio(a, b):
        return c[a] / c[b] if c[b] else None

    recall = ratio("recovered_positive_labels", "positive_labels")
    fragmentation = ratio("same_label_short_splits", "same_label_short_pairs")
    conflicts = ratio("different_label_short_joins", "different_label_short_pairs")
    return {
        "counts": dict(c),
        "positive_recall": recall,
        "same_label_fragmentation_rate": fragmentation,
        "different_label_join_rate": conflicts,
        "manual_positive_recall": ratio("manual_recovered_positive_labels", "manual_positive_labels"),
        "objective": None if recall is None else 1 - recall + 0.25 * (fragmentation or 0) + (conflicts or 0),
        "objective_scope": "Positive label recovery + 0.25 × short same-label split rate + short different-label join rate. Grouping references inferred from labels/time; different partial labels are a conservative proxy, not proof of a species switch.",
    }


def eligible(result, baseline):
    c, b = Counter(result["counts"]), Counter(baseline["counts"])
    recovered_gain = c["recovered_positive_labels"] - b["recovered_positive_labels"]
    return (
        result["objective"] is not None
        and recovered_gain >= 0
        and c["different_label_short_joins"] <= b["different_label_short_joins"]
        # Unknown additions are not false positives. Permit a bounded increase
        # alongside measured recovery, but never reward adding arbitrary species.
        and c["unverified_additions"] - b["unverified_additions"] <= recovered_gain
    )
