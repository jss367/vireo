# Species review evidence for agents

Agents can request identification evidence for an exact photo selection without
constructing a separate contact-sheet and metadata workflow. The result keeps
each detected region, model prediction, species-list source and saved assignment
distinct. It never establishes same-animal continuity from capture times or model
agreement, and reading it never runs models or writes labels.

## Read a bounded selection

Use the MCP tool `get_species_evidence(photo_ids=[101, 102])`, or:

```http
POST /api/v1/photos/species-evidence
X-Vireo-Token: <runtime token>
Content-Type: application/json

{"photo_ids": [101, 102]}
```

The unversioned `/api/photos/species-evidence` route is also available. Requests
accept 1–100 unique positive integer IDs, preserve their order, and fail the whole
selection if any photo is missing or hidden from the active workspace. There is
no implicit encounter expansion or truncation. Page a larger selection explicitly.

The response contains:

- `photos`: each photo's full-frame preview URL, saved species assignments and
  provenance, every stored detection and its predictions, and review reasons.
- Per-subject `crop_url`: bound to the exact retained animal detection. Full-image
  fallback, non-animal, invalid-box and nonretained regions receive no crop URL.
  MCP clients can display the same region with
  `view_photo(photo_id=101, view="subject", detection_id=501)`. An unavailable or
  mismatched detection fails instead of falling back to another animal.
- Per-prediction model, label fingerprint, review status and historical state.
  Current Review rows reuse Review's score explanations, raw match assessment,
  subject assessment and disagreement between models on the **same detection**.
  Separate animals are not evidence of disagreement between classifiers.
- `current_label_selection`: the workspace or global default lists, source,
  availability, usable count and content fingerprint. These describe current
  defaults, not an individual job's possibly different request settings.
- `stored_label_sets` and `stored_classifier_models`: the sources recorded for
  stored fingerprints, and prediction-row counts per model. Unknown historical
  source metadata is not invented.
- `matches_current_label_selection`: false when a comparable stored custom-list
  fingerprint differs from the currently selected merged lists. A latest stored
  prediction is not automatically from the currently selected lists. Intrinsic
  class heads, Tree of Life, legacy fingerprints and unavailable selection
  fingerprints are not asserted to match a custom list.
- `candidate_selections`: an index of top current, unrejected model candidates by
  taxon identity, with exact photo/detection/prediction IDs. Historical,
  differently configured, nonlocalized and deferred-subject evidence is not
  promoted into these sets. Larger localized boxes are offered as up to three
  `inspection_candidates`; they are not certified diagnostic views. Scores are
  not compared across models to select a biological winner.

Every result declares `automatic_assignment_eligible: false` and
`continuity.verified: false`. Every candidate selection also declares
`propagation_verified: false`. Matching predictions, high relative scores and
membership in a capture-time group are not proof of identification or continuity.
A photo with no retained region needs visual inspection; the response does not
claim the photo contains no animal.

Changing a workspace from California + Washington to California alone does not
rewrite stored predictions. The packet exposes the stored combined-list source
and its mismatch with the new current selection. Requesting fresh classification
is a separate action, and reclassification does not repair confirmed keywords.

## Reject a region that holds no animal

The detector proposes boxes and the classifier ranks species inside every one
of them, so a stump, a pine branch or a rock can carry a strong "Mule Deer"
with no animal in it. A high score cannot tell that case from a real
identification; only looking can. Once you have viewed the region
(`view_photo(view="subject", detection_id=...)`) and the frame and found no
animal, record a subject verdict:

- MCP: `reject_region(detection_id, reason, source?)`; `restore_region` reverses it.
- HTTP: `POST /api/detections/<detection_id>/subject-verdict` with
  `{"verdict": "not_animal", "reason": "...", "source": "..."}`, or
  `{"verdict": null}` to retain the region again. The response carries the
  photo's subject panel (`subjects`, `rejected`, `primary_detection_id`).
- Vireo: the Review card's "Not an animal: reject region" button, or the
  lightbox Subjects panel's "Not an animal" next to each region, where rejected
  regions stay listed with a Restore button.

The verdict is a fact about the pixels, so it is catalog-wide rather than
per workspace, and it is one Undo step with your `source` and `reason` in the
Undo panel. From then on the region is not retained anywhere: its predictions
leave Review, `get_species_evidence` (the subject reports `retained: false`
with the reason "The reviewer rejected this region as not an animal"), the
pipeline's encounter species label and burst candidates, and primary-subject
selection, so the photo's next box becomes its primary and burst ranking
stops scoring the stump. Nothing is deleted: the predictions, masks and
review rows wait, so a restore brings them back without reclassifying.
Process Review's saved results are refreshed for that photo only; the
encounter label is recomputed from the remaining regions and no species is
applied to any other frame. A detector rerun that finds the same box keeps the
verdict (detection ids are content-addressed); a rerun that no longer proposes
the box retires it together with the verdict.

This does not draw a missing box. A frame whose real subject the detector
never localized (a bird behind foliage with the box on the branches below)
still needs a region before it can be identified; rejecting the wrong box
only stops it from standing in as evidence.

## Confirm only reviewed photo IDs

1. Read the evidence and inspect the full frame and specific subject. Review
   mixed subjects, score ambiguity, list mismatches and subject deferrals before
   making an identification. These fields are triage evidence, not an acceptance
   rule or a calibrated probability of correctness.
2. Call `preview_species_assignment` with exact `photo_ids`, `species` and
   `provenance`. The connector requires the actual `reviewer`, a visual method
   (`direct_visual` or `adjacent_frames`), and a `reason` or `evidence_source`.
   Record supporting photo IDs for sequence inference. Unreviewed model proposals
   remain predictions; they are not accepted by this connector confirmation path.
3. Inspect the returned per-photo additions, existing assignments and provenance.
   The existing species confirmation API requires the selection to fit within one
   cached encounter. Partition an intended selection explicitly when necessary;
   never add unreviewed sibling photos to satisfy that restriction.
4. Call `apply_species_assignment` with the same IDs, species and provenance plus
   the returned `preview_token`. The token binds requested provenance and live
   saved assignment provenance as well as the existing photo/species evidence.
   Changes require a fresh preview. Application is additive, preserves unrelated
   species and flags, and returns a compact response with an Undo reference.
   The connector sets `preserve_existing_provenance: true`: a photo already
   carrying the species keeps its saved evidence and reviewer attribution.
   It also sets `preserve_keyword_metadata: true`, reusing existing species rows
   without promoting or rebinding shared keywords. An incompatible general or
   higher-rank keyword returns 409 before writes and needs explicit review in
   keyword management. A compatible unlinked species row retains its null taxon
   link; adding a reviewed label does not silently repair shared catalog metadata.
5. Read `get_photo(...).species_assignments` to verify the saved identification,
   reviewer, method and supporting frames. Preserve user-reviewed assignments.

This first integration provides evidence and guarded edits through the connector.
It does not add a new classifier, validate species ranges from GPS, prove animal
tracking, require agreement between all models, or repair earlier labels. Those
claims require separate evidence and evaluation.
