"""Verify subject protection, mask polarity, and bounded edge blending."""

import numpy as np
import pytest
from masked_blend import composite, protected_weight


def test_subject_holes_are_protected_and_transition_is_outside():
    mask = np.zeros((100, 100), np.uint8)
    mask[40:60, 40:60] = 255
    mask[48:52, 48:52] = 0
    w, core = protected_weight(mask, guard=4, transition=8)
    assert core[50, 50] and np.all(w[core] == 1)
    assert w[50, 63] == 1 and w[50, 80] == 0
    assert np.all(np.diff(w[50, 60:80]) <= 0)
    subject = np.full((100, 100, 3), [230, 10, 80], np.uint8)
    background = np.full((100, 100, 3), [20, 240, 5], np.uint8)
    out = composite(subject, background, w)
    assert np.array_equal(out[core], subject[core])
    assert np.array_equal(out[w == 0], background[w == 0])
    assert np.all(out >= np.minimum(subject, background))
    assert np.all(out <= np.maximum(subject, background))


@pytest.mark.parametrize("value", [0, 255])
def test_degenerate_mask_fails_closed(value):
    with pytest.raises(ValueError, match="both subject and background"):
        protected_weight(np.full((20, 20), value, np.uint8))


def test_bad_weight_or_alignment_is_rejected():
    im = np.zeros((20, 20, 3), np.uint8)
    with pytest.raises(ValueError, match="geometry"):
        composite(im, im, np.zeros((10, 10)))
    with pytest.raises(ValueError, match="Invalid mask"):
        composite(im, im, np.full((20, 20), np.nan))
