"""Whether a classified region matched background better than any listed species.

A CLIP classifier's softmax ranks only the labels that were loaded, so a
detector box on a tree trunk, or a whole frame of bare twigs, still comes
back with a confident species winner. The classifier therefore also scores
every region against a fixed set of background prompts (bark, twigs, water,
sky, ...; ``classifier.BACKGROUND_PROMPTS``), encoded through the same
templates as the species labels so the two cosines share one scale. This
module holds the one rule that turns those two numbers into a verdict.

Calibrated on the 2026-11-07 import (BioCLIP-2.5, 1,255-label California
list): crops of real animals score their best background prompt 0.25 to
0.45 below their best species label, while trunk strips, bare branches and
empty frames come within 0.00 to 0.18 of it. The margin sits in the gap.
It is a pure rule over stored scores, so a different margin can be tried
without re-running the model.
"""

COSINE = "cosine"

# A region is background-like when its best background prompt comes within
# this much cosine similarity of its best species label.
BACKGROUND_MARGIN = 0.15


def background_gap(max_match_score, background_score):
    """``background_score - max_match_score``, or None when either is unrecorded.

    Positive means a background prompt beat every listed species outright.
    """
    if max_match_score is None or background_score is None:
        return None
    return float(background_score) - float(max_match_score)


def is_background_like(max_match_score, background_score, score_kind=COSINE):
    """True when the region's background match is within ``BACKGROUND_MARGIN``
    of its best species match.

    Only cosine scores are comparable to the background prompts (a supervised
    classifier's logits have no background prompt to compare with), so any
    other ``score_kind`` and any missing score answer False: an unmeasured
    region is never called background.
    """
    if score_kind != COSINE:
        return False
    gap = background_gap(max_match_score, background_score)
    return gap is not None and gap >= -BACKGROUND_MARGIN
