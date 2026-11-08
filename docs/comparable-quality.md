# Quality that compares across encounters

`quality_composite`, the number Process Review and Cull rank frames by, is a
rank inside one encounter: its focus, noise and background terms are
percentiles among that encounter's frames. Two values from different
encounters say nothing about which photo is better, which is why Cull records
no measurement when a reject loses to a photo from another outing. Culling
one species across outings needs a value that means the same thing for every
photo in the catalog. This document describes that value, what it measures,
and what it does not.

## Subject detail

`photos.subject_detail` (and `photo_masks.subject_detail` per SAM variant) is
`quality.compute_subject_detail`: the same multi-scale Tenengrad operator as
`subject_tenengrad`, applied after the subject is resampled so its mask's
bounding box spans `DETAIL_SCALE_PX` (512) pixels on its longer side. The
crop keeps 8% of context around the box so the gradient window at the mask
edge reads real pixels, the image is resampled with LANCZOS and the mask with
NEAREST, and the mean squared gradient is taken over the resampled mask.

The frame is decoded at whatever size puts the subject at that scale
(`quality.detail_decode_edge`, capped at the native size). The 1536 px proxy
the mask stage already holds serves when the subject spans 512 px in it; in
this catalog the median subject covers 1.25% of the frame, about 170 px in
the proxy, so most photos are read again from the working copy or original.
A subject smaller than 512 px even at native resolution is upsampled and
reads low, which is the honest answer: that frame holds little detail.

`photos.subject_detail_px` is the subject box's longer side in native pixels,
kept so a reader can see what the value came from ("measured at 512 px from
an 840 px subject").

The proxy-scale `subject_tenengrad` is not comparable this way. At a fixed
frame scale a small subject's edges fall on fewer pixels and read as sharper:
on a 180-photo sample of this catalog, log `subject_tenengrad` correlated
−0.56 with the subject's size in pixels, log `subject_detail` +0.20.

## The composite

`scoring.comparable_quality(photo)` combines absolute terms only:

| term | source | weight |
| --- | --- | --- |
| detail | `subject_detail` through a fixed log-sigmoid (`detail_score`) | 0.54 |
| exposure | `scoring.exposure_score` of the subject's clip fractions and median luminance | 0.24 |
| crop | `crop_complete`, the fraction of the subject outline clear of the frame edge | 0.10 |
| area | `scoring.area_score` of the subject's share of the frame | 0.12 |

The weights are the encounter scorer's for the same terms, renormalized
without its relative terms (focus percentile, background separation, noise
rank), which are left out rather than faked. The result carries every term,
the detail measurement, its scale and `measurement_name`, so a card or an
API client can show what the score is made of.

The detail mapping is a log-sigmoid with its midpoint at 8,000 and a width of
0.35 decades, so 2,000 maps near 0.15 and 40,000 near 0.9. Those constants
were chosen from the sample below so the catalog spreads across the range;
they order photos by detail and set how much a detail difference is worth
against clipping, a clipped wing or a small subject. They are not a
calibrated sharpness scale.

## What it is not

* Not texture- or light-invariant. Tenengrad sums squared gradients, so a
  streaky plumage in sun measures higher than a plain one in shade at the
  same focus. Within one species the texture is similar, which is where
  the number is most trustworthy; a contrast-normalized variant (detail
  divided by the subject's luminance variance plus a floor) was tried and
  separated picks from rejects less well (see below), so the plain
  measurement ships.
* Not a defocus classifier. `docs/relative-focus-evidence.md` still holds:
  no validated absolute focus threshold exists, and no automatic rejection
  reads this value.
* Not a replacement for the encounter rank. Inside one encounter the
  scorer's own evidence remains the comparison Cull records; since the
  detail became the scorer's preferred basis (below), that evidence is
  usually the detail ranked among the encounter's frames, and the fixed
  scale is what lets the same number serve across encounters too.

## Sample used to choose the constants

180 photos of this catalog with masks and working copies, 60 each flagged,
rejected and unflagged, measured from the 4096 px working copies in
November 2026. The area under the curve (AUC) for ranking a flagged photo
above a rejected one:

| value | AUC flagged > rejected |
| --- | ---: |
| `subject_detail` (this measurement) | 0.76 |
| `subject_detail` / (subject variance + 16²) | 0.70 |
| `subject_detail` / (subject variance + 32²) | 0.72 |
| stored `subject_tenengrad` (proxy scale) | 0.83 |
| subject size in pixels | 0.39 |

The stored value's higher AUC is partly circular: many of the flags were set
from Vireo's own burst ranking, which reads that value, and the rejected
sample skews toward large subjects (90th percentile 4,087 px against 1,398
for flagged), which the proxy-scale value reads as soft. Medians: flagged
detail 15,870, rejected 7,030; the midpoint of their spread is the mapping's
anchor. Measuring took 0.04 s per photo from a working copy.

## As the encounter's focus basis

The scorer (`scoring.score_encounter`) ranks each photo's focus on the
detail when it has been measured, in preference to the proxy-scale
`subject_tenengrad` and after the eye window when the user has turned eye
detection on (`docs/relative-focus-evidence.md`). That order follows the
photographer's own decisions on this catalog: in the 43 bursts (192 frame
pairs, November 2026) where one frame was flagged and others rejected, the
chance that a measure ranks the flagged frame above a rejected one, and how
often its top frame is the flagged one:

| measure, within the burst | AUC flagged > rejected | top frame flagged |
| --- | ---: | ---: |
| `subject_detail` at 512 px | 0.927 | 40 of 43 |
| stored `subject_tenengrad` (proxy scale) | 0.875 | 36 of 43 |
| `eye_tenengrad` (110 pairs with an eye on both frames) | 0.855 | |

Flagged against unflagged frames in the same burst is near chance for every
measure (0.58 to 0.65): an unflagged frame is usually as sharp as the chosen
one and lost on pose or framing, so that label says nothing about sharpness.
A head-region Tenengrad (SuperAnimal keypoints boxing bill, crown, eyes and
nape) was measured on the same bursts and ranked the flagged frame above a
rejected one 0.925 of the time with its top frame flagged in 28 of 33, no
better than the whole subject on the same frames, so no head measurement is
stored.

## Where it flows

* The mask stage (`pipeline_stages/features.py`) and the standalone Extract
  Masks job (`services/mask_extraction.py`) measure it for every new mask,
  and for a cached mask whose row has no measurement yet: that photo is a
  cache hit (no SAM), read from the source for the measurement alone. The
  Process plan counts those as work ("Will measure subject detail for N
  cached masks"); a source that cannot be read leaves the measurement
  pending and fails the stage with a message saying so.
* Scoring ranks encounter focus on it (above) and attaches
  `comparable_quality` next to `quality_composite`. Cached
  results are re-scored from the live feature columns whenever they are
  loaded (`services.saved_review.refresh_saved_suggestions`), so a
  measurement made after the last regroup reaches the pages without one.
* Cull shows "Detail N · quality P% across encounters" on cards, orders a
  day's suggested favorites by it when measured, and records it as the
  `comparison` (`basis: "detail"`) when a reject loses to a photo from
  another encounter.
* Best Batch and Compare cards carry it as `comparable_quality` and show
  "Subject detail N at 512 px, quality P/100 across encounters".
