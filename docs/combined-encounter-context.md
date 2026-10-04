# Combining classifier evidence with neighboring-frame context

The next experiment, completed October 4, 2026, found that longer runs with
matching classifier predictions and neighboring-frame context recover different
misses. Their combination improves on the previous experimental winner. These
were training/development results. The selected rule is now the default in
Vireo; the historical production validation is recorded below.

## Question and fixed comparison

The reference algorithm is the previous experiment's eight-frame, ten-second
weak-detection rule, including direct classifier support on every weak frame.
Five challengers combined that rule with single-frame, short-run, or longer-run
neighbor context, or with lower-confidence matching classifier predictions.
The recipes were committed before the comparison. The best eligible recipe in
each of three families advanced from training to development; the selection
objective and eligibility checks were unchanged from the previous experiment.

Every stage reads the same original production features. Repairs from one stage
cannot become another stage's anchors. When two rules qualify, the earlier rule
with direct classifier support takes precedence. Labels remain scoring answers
and are not supplied to the candidate algorithm.

## Selected behavior

The winning combination first applies the prior classifier-supported rule. It
then allows up to eight weak frames within ten seconds to borrow context from
matching strong neighbors. For this additional rule:

- Every middle frame must have a real animal box with detector confidence at
  least 0.03 and box intersection-over-union at least 0.20 with both anchors.
- Each anchor must have exactly one normal-confidence animal detection and
  agree on the species at classifier confidence at least 0.80 and margin 0.60.
- Qualifying contradictory evidence from any classifier or secondary detection,
  and contradictory available image embeddings, veto the recovery.
- When a middle frame lacks a qualifying prediction, it abstains from the
  species vote; the surrounding predictions supply the encounter suggestion.
  No classifier probability or photo tag is invented or overwritten.

The stronger overlap and box-confidence requirements distinguish this context
rule from the longer rule that requires direct classifier support throughout.

## Incremental results over the previous experiment

| Partition | Photos | Recovered reference labels | Same-label splits, at most 3 seconds | Same-label splits, 3–10 seconds |
| --- | ---: | ---: | ---: | ---: |
| Training | 41,788 | 21,983 → 22,019 | 928 → 888 | 69 → 67 |
| Development | 12,951 | 6,838 → 6,858 | 453 → 434 | 26 → 25 |
| Combined | 54,739 | 28,821 → 28,877 | 1,381 → 1,322 | 95 → 92 |

This recovers **56 additional reference labels** and removes **62 additional
same-label splits** within ten seconds. No previously recovered labels are
lost, no unverified species suggestions are added, and none of the 564
differing-label control pairs within sixty seconds are newly joined. All 43
saved human-reviewed constraints pass. Their earlier decisions remain the
only human approvals used by selection.

The smaller context variants improved training results, but the longer context
variant won that family's training comparison. Lowering classifier confidence
also helped, but its development improvements were smaller than the selected
context rule. No thresholds or combinations were changed after these results.

The training and development days have now supported multiple rounds of
experimentation. Their improvements are useful for choosing a candidate but
are not independent evidence of generalization. The library still contains no
capture dates after October 3; the consumed former test days remain excluded.

## Review and preservation

There are 33 additional changed sequences relative to the previous winner.
Existing labels support 31; two have uncertain boundaries: an 82-photo
red-crowned parrot sequence and a 12-photo song sparrow sequence. All 94 previews
are embedded in the new review page. The two spotted towhee cases from the
previous round still lack imported decisions, so the private review index
links both rounds without merging their export identities.

The private run `combined-encounter-context-20261004` retains the fixed search
design, source copies and commit identity, all training trials, development
comparisons, frozen selection, changed cases, and review snapshots. The same
45,260 inferred references are reused without duplicating them in the cumulative
reference store; the 33 new comparison outcomes are saved separately with their
source identity. Inferred checks are not entered as human approvals.

The command is `encounter_eval.continuity_followup --experiment combined`;
see the [evaluation README](../tools/encounter-evaluation/README.md) for its
scope, constraints, report, and decision-import arguments. The report preserves
the previous experimental baseline in its frozen before/after snapshots.

## Default production behavior and historical validation

The selected combination now runs by default in `load_photo_features`, with
no new setting or opt-in. The existing ability to disable weak-detection rescue
still applies. It reuses cached model evidence; no new photos or model runs are
required. Grouping cache version 3 makes the next grouping run recompute results
under these rules. Normal burst segmentation remains unchanged.

The implementation preserves the earlier narrow repairs, then evaluates the
longer classifier-supported rule and the neighboring-context rule against the
same baseline. Both are capped at eight middle frames and ten seconds from
anchor to anchor. The classifier-supported rule requires a matching prediction
on every middle frame, overlap of at least 0.02, and a positive animal box; the
context rule uses the stronger 0.03 detector and 0.20 overlap thresholds above.
Contrary classifiers, multiple strong anchor animals, folder boundaries, and
available contradictory embeddings retain their vetoes. The review trace
explains when neighbors supply the species suggestion without a middle-frame
species vote. Stored predictions and photo tags are unchanged.

The database evidence preselection includes nearby photos even when a strong
person or vehicle detection marks a weak animal frame as present. The animal
evidence, rather than that whole-photo state, determines the actual anchors.
A historical replay exposed this integration case; a regression test now
covers it.

Both the retained-evidence adapter and the real database loader were compared
with the frozen experimental winner from source commit `d666f7219` across
**71,153 photos in 177 sessions**. Every encounter membership and species roster
matched, and all **43 saved human-reviewed constraints** passed on the database
loader's output. This replay includes the previously used test partition; it
checks implementation equivalence and historical outcomes, not a new independent
estimate of generalization. No parameters were changed after scoring it.

Compared with the prior production algorithm:

| Historical scope | Photos | Additional recovered reference labels | Fewer same-label splits within ten seconds |
| --- | ---: | ---: | ---: |
| Training and development | 54,739 | 119 | 92 |
| Previously used test partition | 16,414 | 23 | 18 |
| All retained sessions | 71,153 | 142 | 110 |

Across all sessions, no previously recovered reference labels were lost, no
unverified additions increased, and none of the 740 differing-label control
pairs within sixty seconds were newly joined. Existing tags remain partial
positive reference answers; these counts do not prove that every species is
present in the tags or that every merge is correct.

The four pending review cases remain unreviewed; their status is preserved in
the dataset. Shipping this default relies on the historical results and saved
regressions, without requiring fresh captures or additional opt-in review.
Private replay inputs, scripts, results, and partition counts are preserved in
`default-encounter-continuity-20261004` under the local evaluation runs directory.

The offline challenger runner explicitly retains its version 2 baseline through
`apply_previous_continuity`, so shipping the winner cannot silently move the
baseline of the two recorded searches. The default retained-feature adapter
and app both call the new production implementation.
