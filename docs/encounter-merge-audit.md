# Auditing joins between differing species tags

The continuity experiments in October 2026 measured missed joins carefully, but
never measured wrong merges. Their only merge-side check was that a candidate
could not join a differing-label control pair the previous algorithm kept apart.
The joins that already existed were held fixed and never inspected. This audit
asks the photographer about each of them.

## What the installed default joins

Across the 54,739 training and development photos (137 sessions), 564 adjacent
same-folder pairs within sixty seconds carry different singleton species tags.
The installed default (PR #1954) puts **175** of them in one encounter. Former
test sessions are not opened.

| Why the cut did not fire | Pairs |
| --- | ---: |
| Neither photo has a confident species prediction | 135 |
| Only one photo has a confident species prediction | 36 |
| Both photos are confidently the same species | 1 |
| The merge pass rejoined two first-pass segments | 2 |
| Same camera burst | 1 |

| Time between the photos | Pairs |
| --- | ---: |
| At most 3 seconds | 16 |
| 3–10 seconds | 59 |
| 10–60 seconds | 100 |

A species cut needs a confident prediction on both sides: the models must agree
at 0.80 or above, 0.60 ahead of any other species in the frame. In 171 of the 175
joins, at least one photo fails that test, so time and image similarity alone
decided the boundary. Some photos that fail it are confident about their main
subject, but a second species elsewhere in the frame narrows the margin. In the
first case reviewed, a Cinnamon Teal frame reached 0.92, but a hybrid-teal
runner-up left a margin of 0.59.

The 175 pairs span 124 species combinations. Many come from mixed wetland
scenes (flamingos with herons, coots with grebes, godwits with willets), and a
few are predator–prey pairs such as Belted Kingfisher and Pink Salmon. Some of
these will be one encounter that holds two species, and some will be a change
of subject. Existing tags cannot tell which, so this needs a human answer.

## Review

The private audit is `merge-audit-20261004` in the evaluation runs directory.
When it was built, 61 of the 175 cases lacked a cached image for at least one
photo of the pair, because the photo library volume was not mounted. Previews
generated later appear when the page is reloaded.

The results will set the next step:

- If wrong merges are rare, the differing-label controls can stay a guard rather
  than an objective, and work returns to the remaining splits.
- If wrong merges are common, their dominant join reason identifies the
  candidate rule. For example, if they cluster where neither photo is confident,
  that points to a cut on species disagreement below the confident threshold. If
  they cluster in the 10–60 second gaps, that points to the time weight. Each
  reviewed pair becomes an explicit boundary constraint that a candidate must
  respect.
- Pairs marked as a wrong tag are tagging errors to fix in Vireo, not grouping
  errors.
