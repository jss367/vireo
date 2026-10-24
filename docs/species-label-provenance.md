# Species label provenance

Browse’s photo detail panel shows the evidence for each assigned species. Expand a species evidence row to see its reviewer, reason, evidence source, supporting frames, subject detection, and confidence meaning. The editor updates that assignment without changing any photographer flags.

The identification methods are:

- **Manual / unspecified:** someone added the label without recording how they identified it. Historical manual confirmations retain this state.
- **Direct visual confirmation:** the reviewer identified the species from the assigned photo.
- **Inferred from adjacent frames:** the reviewer used context from other frames. This requires supporting frame IDs or an explanation.
- **Model proposed:** the identification is a machine proposal, distinct from a human confirmation.
- **Imported / unknown:** the original identification method is unknown.

An omitted numerical confidence stays unknown. Supplying a number requires an explicit explanation of what it measures; a model similarity score need not be a calibrated probability.

API clients can include a `provenance` object in `POST /api/encounters/species` when adding or confirming a species:

```json
{
  "species": "Mallard",
  "photo_ids": [123],
  "add": true,
  "provenance": {
    "method": "adjacent_frames",
    "reviewer": "Wildlife reviewer",
    "reason": "The same subject is clearly visible in the following frame",
    "supporting_photo_ids": [124],
    "evidence_source": "Burst review",
    "confidence": null
  }
}
```

Update evidence for an existing species assignment with `POST /api/photos/{photo_id}/species/{keyword_id}/provenance`, using the provenance object itself as the request body. `GET /api/photos/{photo_id}` includes `species_assignments`, keyed by the actual attached keyword IDs. Multiple species on one photo retain independent evidence. An optional `detection_id` must belong to that photo and can only be used with a single-photo assignment.

Attribution is stored on the catalog’s photo-keyword association, independently of the existing keyword authorship stamp. Per-item history preserves the before and after attribution for undo and redo, including encounter edits that also change cached grouping. Supporting frames must be visible in the active workspace when evidence is recorded. The standard keyword synchronization queue continues to handle species labels; these richer evidence fields are stored in the catalog and edit history.
