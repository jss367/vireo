# Encounter grouping evaluation

Compare and tune Vireo's encounter grouping using cached classifier evidence and
current human species labels. Each new run reads the latest library data. The
comparison and tuning commands never write to the library, run image models, or change app settings. The review server can also apply human corrections to Vireo tags when explicitly enabled.

This developer package lives outside the application package and has its own
dependencies. Full-library experiments are not part of application builds.

## Install from a Vireo checkout

Use a separate environment; the application itself does not need to be installed:

```sh
python3 -m venv .context/encounter-evaluation-venv
.context/encounter-evaluation-venv/bin/python -m pip install -e 'tools/encounter-evaluation[test]'
```

The examples below assume that environment is activated:

```sh
source .context/encounter-evaluation-venv/bin/activate
vireo-evaluate-encounters inventory
vireo-evaluate-encounters compare --workspace 22 --max-sessions 40
vireo-evaluate-encounters tune --workspace 22 --max-sessions 80 --trials 12 --seconds 300
```

Choose your workspace ID from `inventory`; 22 is only an example. Database access
defaults to `~/.vireo/vireo.db`; override it with `--db`. The tool detects the
checkout from an editable installation, or accepts `--repo` explicitly.

Runs are written to `~/.vireo/encounter-evaluation/runs/<time-id>/`. Open the
printed `report.html` locally. It contains comparative scores, the largest
session improvements and regressions, photo context around changed boundaries,
human labels, and existing thumbnails where their absolute paths are available.
Images are not uploaded or copied into the repository.

Use `--capture-date YYYY-MM-DD` to limit a new comparison to complete sessions
on a particular capture day. This uses the evaluator's timestamp normalization
(UTC for timezone-aware timestamps; stored calendar date for camera timestamps
without an offset). Date selection preserves day split membership and duplicate
checks across the workspace. It never moves an existing test day into review.

## Review disagreements and correct reference labels

Build a browser review queue from retained comparison inputs, then open it:

```sh
vireo-review-encounters build --run /path/to/run \
  --output ~/.vireo/encounter-evaluation/review/species-review.sqlite
vireo-review-encounters serve \
  --queue ~/.vireo/encounter-evaluation/review/species-review.sqlite --open
```

The queue recomputes the three default algorithms on retained training and
development evidence and records the current source signature separately from
the original run. Test sessions are never loaded. All disagreements are retained;
representatives of repeated sequences come first, with 200 seeded random photos
whose species suggestions agree interleaved for checking shared mistakes. Use
`--agreement-sample` to change that sample size. Identity-only differences have
their own view and are never silently merged by matching display names.

Inspect the large cached preview and neighboring frames, choose species from
the catalog, and explicitly confirm when every target species has been labeled.
An explicitly confirmed empty list records no target species; a missing model
prediction never does. Partial labels remain positive-only. Each save applies
to one photo, persists in the review database with revision history, and does
not edit Vireo tags or sidecars. Model suggestions are hidden until revealed.
Missing previews are shown explicitly; the tool does not decode originals.
Close and reopen the server with the same queue to resume reviewing.

To also update Vireo's photo tags on each save, start the server with:

```sh
vireo-review-encounters serve \
  --queue ~/.vireo/encounter-evaluation/review/species-review.sqlite \
  --update-vireo-tags --open
```

With this option, complete reviews replace species tags (including removing all
species tags for a confirmed empty photo); partial reviews only add the selected
species. Other keywords and higher-rank taxonomy tags are preserved. The writer
uses Vireo's identity-aware keyword methods, manual provenance, per-keyword edit
history, and normal pending-XMP queue. Sync from Vireo to write the sidecars.
Existing evaluation-only reviews are not retroactively applied; open and save
one to apply it. Vireo's undo changes its tags, not the separate reference answer.

Tag updates verify the photo still matches the captured identity and workspace.
If an update fails, the reference answer remains saved and the photo stays in
“Not yet reviewed” with an explicit pending-tag warning and retry button.
Library-side receipts make retries safe after a crash between the two database
commits. Restart with the same option to retry pending updates. Unresolved
name-only identities must be replaced with a resolved catalog selection.

Load those corrections into a **new** comparison or tuning run:

```sh
vireo-evaluate-encounters compare --workspace 22 \
  --review-labels ~/.vireo/encounter-evaluation/review/species-review.sqlite
```

Reviews replace reference answers only, never algorithm features. Import checks
the library, workspace, photo identity, and stable partition; a changed identity
or partition requires reconciliation. Retained runs stay immutable, so resume
does not pick up new reviews. The queue is deliberately enriched for difficult
cases; its reviewed subset is not a representative library-accuracy estimate.
Keep the final test partition untouched until candidate selection is complete.
The server binds only to loopback and prints an access-token URL; keep that link
and the private review database local. `--media-root` overrides `~/.vireo` when
cached previews and thumbnails are stored elsewhere.

## Compare the encounter continuity repair at larger scale

The paired comparison captures the previous feature loader from a specified
Git revision and runs it alongside the proposed loader on the same read-only
SQLite snapshot for each scope. Both versions use the same grouping and burst
rules and the same existing reference labels. It materializes training and
development sessions only; held-out feature bundles are never loaded.

```sh
python -m encounter_eval.continuity_compare --baseline-revision REVISION_BEFORE_REPAIR \
  --scope 22 --scope 5:2026-10-03 \
  --split-registry 22=/path/to/established-workspace-22-splits.json \
  --split-registry 5=/path/to/established-workspace-5-splits.json \
  --output ~/.vireo/encounter-evaluation/runs/encounter-continuity-comparison
```

Replace the example workspace IDs and optional capture date with your scopes.
Supply each workspace’s established registry explicitly; missing files are
rejected, and each registry’s original seed and held-out memberships are reused.
Use the `split_registry_path` recorded by the earlier evaluation manifest.
The baseline revision must precede the full-image continuity repair. Retained
baseline source, paired input bundles, hashes, split membership, and separate
training/development metrics make the comparison inspectable. Fully overlapping
sessions are counted once; partial overlaps are rejected.

Open `Review encounter grouping changes.html` in the output directory. It shows
whole before/after encounters, burst counts, frames supported by sequence
context, and existing species tags. Changed groups that lose known species or
combine different reference-tag sets are prioritized for inspection. A separate
view lists short unresolved interruptions between matching species suggestions;
these are candidates for review, not proven missed merges. Browser-local grouping
judgments can be exported as JSON. This report does not edit photo tags.

## Algorithms and tuning

### Compare using existing labels as automatic sequence references

The label benchmark scores short sequences directly against existing species
tags, alongside recovered species labels. Matching singleton tags on adjacent
same-folder frames within three seconds supply join references; different
partial tags are a conservative boundary proxy, not proof of absent species.
Missing labels and multi-species tags do not manufacture boundary answers.
Manual-only associations are reported separately from imported labels.

```sh
python -m encounter_eval.label_benchmark \
  --scope /path/to/retained-run/scope-0-workspace-22 \
  --scope /path/to/retained-run/scope-1-workspace-5 \
  --output ~/.vireo/encounter-evaluation/runs/new-label-comparison
```

The bounded search compares 34 configurations on training data and the best
of each algorithm family on development data. It saves the recipes, source and
manifest hashes, individual results, selection policy, and frozen selection.
It requires a new output directory. Optional `--constraints /path/to/cases.json`
accepts explicit reviewed boundary expectations: a JSON list with `id`,
`workspace`, `session`, ordered `ids`, and `expected_groups` per case. Optional
`photos` identity records verify filenames. Only training/development reviews
may constrain selection; imprecise review decisions are not converted into
invented expected boundaries.

By default, test feature bundles and answers are never loaded. Add
`--evaluate-test` only for a final evaluation milestone: the runner freezes
one selected candidate before scoring it on the test partition. Do not tune
again on that test set after seeing the result. Exact duplicate file hashes
and photo IDs crossing partitions cause an error; near-duplicate auditing is
still required for claims of independent generalization.

The baseline groups the retained photo features as captured; the continuity
candidate applies the shared production continuity rules to those features.
To replay the original before/after experiment, use a snapshot captured before
the new rules were installed. A new snapshot from the updated loader already
contains the repairs, so applying them again is normally a no-op.

See [the completed continuity evaluation](../../docs/encounter-continuity-evaluation.md)
for the selected parameters, results, limitations, and private artifact layout.

### Investigate remaining splits without reusing the final test

The follow-up compares ten bounded continuity challengers against the merged
rules on retained training/development sessions only. It adds differing-label
controls through sixty seconds and rejects any loss of an already recovered
reference label. The winning candidate remains provisional until a fresh final
test; this command has no option to consume the former test partition.

```sh
python -m encounter_eval.continuity_followup \
  --scope /path/to/retained-scope \
  --constraints /path/to/reviewed-boundaries.json \
  --output /path/to/new-comparison
python -m encounter_eval.continuity_followup_report \
  --comparison /path/to/new-comparison \
  --output /path/to/new-review
```

Constraints use the same explicit `expected_groups` format as the label
benchmark. Start from the pre-continuity retained snapshot to replay the October
experiment exactly. The baseline reapplies the current production repairs;
challengers adjust inference features only, without receiving reference labels.

The comparison saves `inferred-regression-checks.json` separately from human
judgments, plus frozen recipes, scores, and changed cases. Preserve an append-only
copy of those inferred checks in the private evaluation dataset. The report
includes only ambiguous cases, embeds available previews, and writes paired
feature snapshots compatible with `encounter_eval.grouping_dataset import`.
Use **Export decisions**, then import that export with the new review directory
as `--run` and the existing cumulative review database as `--dataset`.

See [the follow-up findings](../../docs/encounter-continuity-followup.md) for
results, candidate selection, and the still-pending fresh final test.

Add `--experiment combined` to run the next fixed comparison: five challengers
combine longer classifier-supported runs with conservative neighbor context or
lower-confidence matching predictions. Its baseline is the previous experimental
winner, not the installed algorithm. Every stage reads the same original
production features, with direct classifier evidence taking precedence; stages
cannot build new anchors from another stage's repairs. The saved baseline recipe
also drives the review report's **Previous experiment** column.

See [the combined-rule experiment](../../docs/combined-encounter-context.md) for
the incremental results and remaining validation requirements.

### Audit joins between differing species tags

Every continuity experiment so far has required "no newly joined differing-label
controls", which holds the existing joins fixed without checking them. The merge
audit lists every adjacent pair, within sixty seconds, that the installed default
keeps in one encounter even though the two photos carry different singleton tags.
For each pair it shows the photos, the surrounding frames in the encounter, each
model's leading predictions, and why the cut did not fire.

```sh
python -m encounter_eval.merge_audit build \
  --scope /path/to/retained-scope --scope /path/to/another-scope \
  --output ~/.vireo/encounter-evaluation/runs/merge-audit-YYYYMMDD
```

Only training and development sessions are opened. Open `Review merged
encounters.html` and decide each pair: keep together, keep together because a
species tag is wrong, split, or unsure. Number keys choose and arrow keys move.
Cases are in a seeded random order, so a review stopped partway is still a random
sample as long as no case is skipped. Pages use cached previews or working copies
in place. `missing-previews.json` lists photos with neither; generate their
previews in Vireo and reload the page. Rebuilding would start a new audit and
lose the browser's saved decisions.

Export the decisions, then score them:

```sh
python -m encounter_eval.merge_audit results \
  --audit ~/.vireo/encounter-evaluation/runs/merge-audit-YYYYMMDD \
  --decisions ~/Downloads/'Merged encounter review decisions.json' \
  --existing /path/to/reviewed-constraints.json \
  --output ~/.vireo/encounter-evaluation/runs/merge-audit-YYYYMMDD-results
```

The results report the wrong-merge rate with a 95% Wilson interval, broken down
by join reason, time gap, and partition. They also estimate the count across all
joined pairs. Each decided pair becomes an explicit two-photo constraint in the
`expected_groups` format: `[[a, b]]` to keep together and `[[a], [b]]` to split.
`reviewed-constraints.json` appends these to `--existing` for the next
`continuity_followup` or `label_benchmark` run. Unsure decisions add nothing.
`tag-corrections.json` lists pairs marked as wrong tags, to fix in Vireo; the
audit never edits tags.

`compare` evaluates the real production encounter implementation, a conservative
per-photo species-set candidate, and a sequence candidate by default. All use
the same materialized evidence. Production uses its existing flattened top-five
representation, while experimental candidates retain detection/source identity.
This first comparison therefore measures both representation and grouping
changes; independent versus sequence isolates the effect of sequence reasoning.

The sequence candidate infers continuity through short detector misses and
splits on inferred species-set changes. A credible second species in a single
frame is retained. An unclassified second subject or qualified model
disagreement remains unresolved. It is an experimental heuristic, not a
calibrated probability model or a production change. Empty detector output never
becomes a verified empty-photo label.

`tune` searches sequence parameters by default. It scores trials on training
sessions, then evaluates at most three eligible finalists on development
sessions. It never scores test sessions. Selection requires the configured
review coverage and encounter-count limit, then minimizes the documented error
cost. A selected candidate is the best eligible experiment; the report separately
states whether it improves the baseline. Selection is not permission to deploy.

```sh
vireo-evaluate-encounters tune --workspace 22 --candidate production --trials 20
vireo-evaluate-encounters tune --workspace 22 --space parameter-space.json --method grid --trials 16
```

Example `parameter-space.json` for the sequence candidate:

```json
{
  "confidence": [0.4, 0.55, 0.7],
  "margin": [0.1, 0.2],
  "context_frames": [2, 4],
  "transition_penalty": [0.2, 0.5, 0.9]
}
```

`--seconds` is a cooperative search deadline checked between trials, including
baseline/finalist evaluation time. Preparation and report generation are outside
that budget; a running trial finishes before the deadline is checked again.
`--trials` bounds attempted search combinations. If no candidate passes the
constraints, or time expires before development evaluation, no candidate file is
written. Increase the budget rather than interpreting that outcome as failure
of the algorithm.

Use `--resume /path/to/run` to reuse retained inputs and completed trial results,
possibly with a larger trial/time budget. Resume requires matching source and
Python/NumPy versions. It intentionally does not pick up new labels; start a new
run for that. Trial results are keyed by data, code, algorithm, parameters, and
partition. The command is single-process; do not write concurrently to the same
run or split registry.

Evaluate a selected candidate on held-out sessions explicitly:

```sh
vireo-evaluate-encounters compare --resume /path/to/run --partition test \
  --candidate-file /path/to/run/selected-candidate.json
```

Treat this as a selection milestone. Repeatedly tuning after viewing test scores
would turn those sessions into development data. `--partition all` is useful for
descriptive diagnostics, but its scores must not be reported as held-out results.

## Labels, coverage, and evolving data

Labels are hidden before feature preparation, including the production loader's
weak-detection rescue. Algorithms receive no keyword assignments, review status,
ratings, or expected rosters. Taxonomy normalization remains available because
taxon identity is not a per-photo answer.

By default all species keywords are positive-only reference labels. A missing
tag does not assert species absence. Imported tags with unknown provenance are
usable and reported separately from manual-only associations. `--label-source
manual` restricts reference labels to associations recorded as manual; this can
exclude photographer-authored labels imported from sidecars.

For folders you know have complete species tagging, repeat `--complete-folder`:

```sh
vireo-evaluate-encounters compare --workspace 22 --complete-folder 17 --complete-folder 29
```

This declares completeness only for tagged photos in those folders. Untagged
photos remain unlabeled, never verified empty. The current database does not
provide a trusted explicit empty-photo review signal to this tool.

The report measures incorrect additions only on complete rosters. For partial
labels it reports recovered/missing positives and unverified additional species.
The search objective is:

```text
(2 × incorrect additions + missing species on resolved photos
 + unresolved labeled photos + 0.02 × encounter count) / labeled photos
```

Lower is better. Coverage and fragmentation are reported alongside error. A
uniform group with the wrong species fails, abstaining on every photo has a cost,
and unnecessary fragmentation has a small cost. With positive-only labels,
false additions are not measurable: treat optimization as exploratory until
enough complete rosters are available. Scores are not estimates of human time
saved. Same-species subdivisions can be legitimate photographic events.

Every selected session retains all neighboring photos, including untagged and
rejected images and frames without predictions. Sessions use folder/day ordering
and a 30-minute hard gap; candidate grouping may subdivide them further.
`--max-sessions` samples whole eligible sessions using a seeded hash. Missing
prediction coverage is visible rather than silently excluded.

Entire capture days across folders receive stable 60% training, 20% development,
and 20% test assignments. Exact file hashes link duplicate capture days. Split
membership is persisted beside the run directories in `split-membership-<namespace>.json`.
The namespace uses the resolved database path and workspace ID, so unrelated
libraries and workspaces do not share assignments or quarantined dates. The
manifest records the exact registry path. Use the same registry and seed across
experiments; when moving a library or retaining an older `split-membership.json`,
pass its registry explicitly with `--split-registry`. Newly discovered duplicate
links that cross existing partitions quarantine those days rather than leaking
them across splits. Label changes do not reshuffle membership. Different-date
near-duplicates without matching hashes still need an audit. Missing timestamps
fall back to folder/filename order and cannot establish temporal continuity.

New comparisons read a consistent database view, save the necessary session
records, then close the database before optimization. Current and candidate
algorithms run against those same records. Retained gzip JSON inputs, manifests,
and trial records allow replay. The manifest includes source-content signatures
(including uncommitted code), data signatures, configuration, source fingerprints,
coverage, and partition membership. The records may contain private filenames
and labels; keep run directories outside Git. Rerun the baseline whenever labels
change; scores on different data revisions are not a controlled comparison.

The baseline uses recorded application defaults unless `--config` supplies a
JSON object with `detector_confidence`, `classification_threshold`, and/or
`pipeline` settings. It does not read user credentials or silently depend on live
workspace overrides. Cached embedding variants are accepted by default and the
production grouping code handles dimension differences; set
`pipeline.dinov2_variant` explicitly to filter them. This may differ from the
active app's feature selection and is stated in the report.

Source taxon IDs take precedence over names, including when two species share
the same display name. Older libraries without source-ID columns are read through
connection-local compatibility views; the tool never migrates the live database.

Stored sources use the same most-recent fingerprint selection as production.
Existing classifiers are treated as exclusive. Custom multi-label sources,
full label-list coverage validation, and reliable automatic absence inference
need dedicated adapters. Stored inference may already reflect label lists
chosen using human knowledge; this is cached-evidence evaluation, not a claim
about an untouched historical new import.

## Development and packaging checks

```sh
python -m pytest -c tools/encounter-evaluation/pyproject.toml tools/encounter-evaluation/tests -q
ruff check tools/encounter-evaluation
```

The dedicated workflow runs on tool/shared-code changes with synthetic fixtures.
It does not access the private library. Tests verify that the app wheel excludes
the tool, application imports do not depend on it, and the executable archive
guard detects accidental inclusion. The app build explicitly excludes
`encounter_eval` and inspects archive indexes before copying/signing the binary.
No optimizer dependencies were added to Vireo's runtime or `dev` dependencies.

Add new candidates to `algorithms.py` behind `run_algorithm(name, photos, params,
grouping_config)`. They return ordered contiguous `Group` objects and cannot
drop, duplicate, or reorder photos. Keep scoring in `scoring.py` and search in
`runner.py`. Once a candidate is ready to ship, move the inference code into
`vireo/` and have the tool call that shared implementation; retain search and
reporting here.

## Preserve grouping reviews and check future changes

Export decisions from the grouping comparison page, then import them into a
private, cumulative dataset outside Git:

```sh
python -m encounter_eval.grouping_dataset import \
  --dataset ~/.vireo/encounter-evaluation/review/grouping-reviews.sqlite \
  --run /path/to/paired-comparison \
  --decisions ~/Downloads/'Encounter grouping review decisions.json'
python -m encounter_eval.grouping_dataset check \
  --dataset ~/.vireo/encounter-evaluation/review/grouping-reviews.sqlite \
  --features after --output /path/to/grouping-regression-results.json
```

The SQLite dataset retains the exact browser export, review timestamps and
history, photo identities, full session evidence before and after feature
loading, reference labels, partition assignments, and algorithm/configuration
provenance. It does not depend on the source run remaining on disk. Reimporting
an identical export is a no-op; importing an older review preserves history
without replacing the newer judgment. Only training/development cases qualify.

A “Grouping looks right” review specifies joins and splits **inside** the
reviewed sequence. Neither outside boundary is inferred. “Needs a split”,
“Should join more”, and “Unsure” are retained but require exact boundary review
before becoming scored reference answers. Grouping approval does not confirm
species labels or establish individual bird identity. Overlapping review cases
are reported as cases, not as independent accuracy samples.

Use `--features before` to replay grouping on the original feature snapshot;
`after` replays the proposed snapshot. Both modes run the current grouping code
with captured settings. Use `--params overrides.json` for explicit production
grouping parameter experiments. These frozen checks isolate grouping changes;
they do not rerun detector/classifier models or feature preparation.

To also exercise the current native feature loader on the same photo identities:

```sh
python -m encounter_eval.grouping_dataset check \
  --dataset ~/.vireo/encounter-evaluation/review/grouping-reviews.sqlite \
  --features live --db ~/.vireo/vireo.db \
  --output /path/to/live-grouping-regression-results.json
```

Live mode is read-only, excludes photo tags from model features, verifies photo
identity/workspace membership, and uses the captured settings. Cached model
predictions may have changed since capture; this is explicitly distinguished
from frozen replay. Checks exit unsuccessfully on any violated reviewed
boundary, or when no cases can be scored. Outputs record the current source
signature. Keep this curated regression dataset separate from estimates of
library-wide accuracy and from a final untouched test set.

## Next continuity experiment

The first reviewed comparison covered 54,739 photos: all 21 changed sequences
were accepted by the photographer, while 348 short unresolved interruptions
remained candidates for inspection. This supports the narrow repair, not a
claim that the current rules recover all valid encounters.

1. Diagnose the remaining candidates using their retained evidence: absent
   whole-image predictions, classifier disagreement or weak confidence, absent
   animal boxes, motion that fails box overlap, and ambiguous multiple subjects.
   Record every failed condition; a case can fail more than one.
2. Sample across species, capture days, and failure conditions. Include sequences
   that must remain separate and a seeded sample of unchanged groups. Obtain
   explicit internal boundaries; do not convert an imprecise “join more” or
   “needs a split” judgment into invented reference boundaries.
3. Test one relaxation at a time on training/development data. Start with motion
   tolerance when independent species evidence is strong, then confidence/margin
   sensitivity; longer time spans and longer dropout runs are separate trials.
   Preserve confident conflicting-species and multiple-subject protections.
4. Require the accepted-grouping regression checks to pass, and report both
   recovered joins and incorrect merges on the newly reviewed cases. Review
   newly changed sequences before accepting a broader variant. Do not optimize
   for fewer groups alone.
5. Freeze the selected algorithm and settings before final held-out evaluation.
   Record any final-test use and reserve fresh untouched data for later releases.

The reviewed sequence dataset measures grouping behavior. Keep species-label
corrections in the species review queue, with Vireo tag updates enabled when
requested; neither kind of review should silently substitute for the other.

Review queues validate the source-library path recorded in the run manifest.
Use that library with `review build --db`; older runs without this identity must
be rebuilt before creating a queue. A captured file hash must still be present
and match in the live library before tag updates or live replay can proceed.
