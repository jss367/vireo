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

The gap cannot tell "a small bird in tamarisk" from "tamarisk": a bird
that fills a fraction of its box embeds as the foliage around it. Checked
on the same import after the first reclassify, every one of the strongest
boxes the gap flagged (detector 0.76 to 0.87) held a real bird, while the
trunk strips and empty frames the gate exists for were all weak boxes
(0.21 to 0.43) or whole frames. So the verdict defers to the detector:
a box at or above ``CONFIDENT_DETECTION`` is never called background.
"""

COSINE = "cosine"

# A region is background-like when its best background prompt comes within
# this much cosine similarity of its best species label.
BACKGROUND_MARGIN = 0.15

# A detector box at or above this confidence is trusted to hold an animal
# and is never called background, whatever its prompt gap. Below it, and for
# the whole-frame row (confidence 0), the gap decides.
CONFIDENT_DETECTION = 0.5


def background_gap(max_match_score, background_score):
    """``background_score - max_match_score``, or None when either is unrecorded.

    Positive means a background prompt beat every listed species outright.
    """
    if max_match_score is None or background_score is None:
        return None
    return float(background_score) - float(max_match_score)


def is_confident_detection(detector_confidence):
    """True when the detector's confidence is at or above ``CONFIDENT_DETECTION``.

    None (confidence unknown) is not confident: the gap alone decides.
    """
    return detector_confidence is not None and float(detector_confidence) >= CONFIDENT_DETECTION


def is_background_like(max_match_score, background_score, score_kind=COSINE,
                       detector_confidence=None):
    """True when the region's background match is within ``BACKGROUND_MARGIN``
    of its best species match and the detector was not confident about it.

    Only cosine scores are comparable to the background prompts (a supervised
    classifier's logits have no background prompt to compare with), so any
    other ``score_kind`` and any missing score answer False: an unmeasured
    region is never called background. A box at or above
    ``CONFIDENT_DETECTION`` answers False too (:func:`is_confident_detection`);
    ``detector_confidence`` None means unknown and leaves the gap to decide.
    """
    if score_kind != COSINE or is_confident_detection(detector_confidence):
        return False
    gap = background_gap(max_match_score, background_score)
    return gap is not None and gap >= -BACKGROUND_MARGIN
