# Remaining encounter splits: follow-up experiment

The October 4, 2026 follow-up found a promising extension to weak-detection
continuity. It remains an offline experiment; production behavior is unchanged.
The candidate needs two ambiguous sequence reviews and an independent final
test on newly captured days before a production recommendation.

## What still splits

The baseline is the exact merged implementation from PR #1950, including the
independent-model disagreement checks added during review. It runs on the
retained model evidence from the previous comparison, with labels supplied
only to scoring. This keeps the comparison paired and reproducible.

On 41,788 training photos, the baseline left 947 adjacent same-label splits
within three seconds and another 70 between three and ten seconds. Of these
1,017 boundaries, 981 involved a photo marked subject absent, while only 19
were confident species-change cuts. These are inferred continuity references,
not verified individual-bird identities.

The absent sides comprised 836 distinct photos: 763 still had a positive
animal detection, 328 had crop predictions, and 457 had full-image predictions.
Only four had image embeddings. This makes weak-detection recovery a more
useful immediate target than relaxing the isolated-species rule.

## Comparison and selection

Ten challengers tested greater box movement, longer weak runs, weaker matching
classifications, anchor-only context, and wider isolated-classification windows.
The code and recipes were fixed before the comparison. The best eligible
challenger in each family advanced from training to development; four
challengers advanced. No former final-test bundles were loaded.

The selected rule allows up to **eight weak frames within ten seconds** between
strong matching anchors, instead of three frames within three seconds. Every
weak frame still needs a real animal box, overlap with both anchors, and
matching crop or full-image classifier evidence at confidence 0.80 and margin
0.60. Independent conflicting classifiers, competing subjects, and contradictory
available image embeddings veto additional recovery. Existing production repairs
run first; the experiment does not use repaired isolated predictions as anchors.

| Partition | Photos | Recovered reference labels | Same-label splits, at most 3 seconds | Same-label splits, 3–10 seconds |
| --- | ---: | ---: | ---: | ---: |
| Training | 41,788 | 21,936 → 21,983 | 947 → 928 | 70 → 69 |
| Development | 12,951 | 6,822 → 6,838 | 460 → 453 | 29 → 26 |
| Combined | 54,739 | 28,758 → 28,821 | 1,407 → 1,381 | 99 → 95 |

The selected candidate recovers 63 additional existing species labels and
removes 30 short same-label splits. It loses no previously recovered reference
species, adds no unverified species suggestions, and passes all 43 saved
reviewed constraints. Across 564 differing-label control pairs within sixty
seconds, joins remain 175 before and after; no previously separate control
pair is newly joined. Existing joins are not established errors because tags
may omit species.

The development objective combines missing-label rate, 0.25 times the split
rate within three seconds, the differing-label join rate within three seconds,
and 0.10 times the split rate at three to ten seconds. Eligibility additionally
requires preserving every previously recovered label and every previously
separate differing-label control through sixty seconds. Aggregate gains cannot
hide a lost reference label elsewhere.

An anchor-only variant reduced more splits but recovered fewer labels; the
predeclared objective favored the longer run with direct classifier evidence.
The three wider isolated-classification variants made no additional training
changes and did not advance. No combinations were tuned after seeing the
development results.

## Review and retained dataset

The selected candidate changes 15 sequences: 13 have label-supported changed
boundaries, and two spotted towhee sequences from April 25 contain unlabeled
boundaries. Only those two are presented for human review, with all 80 photo
previews available. The existing decision export/import format preserves their
exact before/after features for later regression replay.

The private run `encounter-continuity-followup-20261004` retains diagnostic
results, the search design and original experiment source, all training
scores, development scores, the frozen selection, changed cases, and review
snapshots. An append-only copy of its inferred checks lives in the private
`review/inferred-continuity-checks` directory. The 45,260 inferred references
contain 44,696 same-label join proxies and 564 differing-label controls, with
source-session digests and explicit non-human provenance. They do not add
approvals to the cumulative human review database.

The library inventory had no capture dates later than October 3. Consequently
there is **no fresh final-test result** for this candidate. Newly captured days
must remain outside further tuning, and should include actual species changes
as well as continuous single-species runs. Prior test days remain excluded.
Unknown near-duplicates and imported-label provenance retain the limitations
of the [earlier evaluation](encounter-continuity-evaluation.md).
