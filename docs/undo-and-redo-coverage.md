# Undo and redo coverage

Audited on October 12, 2026. Undo and Redo work for many photo edits, but **not every reversible action is covered**. The audit follows saved writes from the browser through their routes, history records, replay handlers, sidecar queues, and page refreshes. An audit-log entry alone does not make an action undoable.

The route inventory in [undo_coverage.json](../vireo/tests/contracts/undo_coverage.json) classifies all 257 registered POST, PUT, PATCH, and DELETE method/route pairs. Some are queries or previews rather than changes. A regression test compares the inventory with the live application so new write routes require an explicit undo decision. The inventory describes coverage; it does not claim that every listed route has a browser test.

**Covered photo actions**

| Action | Coverage |
| --- | --- |
| Pick, reject, and clear photo flags | Single photo and batch; Best Batch and culling apply record their mixed decisions. |
| Star ratings and color labels | Single photo and batch. |
| Add and remove photo keywords | Single photo and batch, with corrective sidecar work. |
| Accept predictions and additional subjects | Species tag and prediction status replay; grouped acceptance is recorded. |
| Reject predictions and mark reviewed | Newly covered. Exact statuses and alternative siblings are captured; a burst rejection is one action. |
| Exclude photos from wildlife classification | Single photo and batch. |
| Assign, replace, and clear photo locations | Newly covered. Saved locations, Google places, free text, batch assignment, EXIF GPS assignment, and GPS tagging during import use complete photo-assignment snapshots. All former location tags and their provenance are restored. |
| Saved photo-development recipes | Save, reset, and bulk application. Replay invalidates render caches and queues metadata sync. |
| Unsaved development edits | Editor gestures have local undo/redo before Save, including crop and painted mask corrections. |
| Change encounter species, detach photos/bursts, regroup, and use an encounter label for a burst | Cache-linked history; actions superseded by new analysis are retired with an explanation rather than silently undoing a different action. |
| Highlights species confirmation and relabel | Keywords and the curated state moved by relabel have existing history. |
| Reject and restore Misses photos | Photo-flag history. |

New location edits are recorded as `photo_location`, and new reject/review decisions as `prediction_status`. These separate action types leave old `location_set`, `location_clear`, `prediction_reject`, and `prediction_reviewed` audit records excluded: those rows do not contain enough before-state to reverse safely. Keyword rows and map coordinates created or filled while choosing a location remain in the saved vocabulary when an assignment is undone; reverting vocabulary metadata requires separate history.

New location writes, pending sidecar work, and history recording now share a transaction. A failure while writing history rolls back the whole location batch. Reassigning an identical location or clearing an already empty assignment adds no history and preserves Redo, while still queueing a sidecar refresh so newly supplied saved-place coordinates can sync. A history action without a replay handler now fails without advancing the cursor.

Life List and Location Review now refresh after global Undo/Redo. Location Review rebuilds its queue from the current source; partly completed assignments block history changes until retried or abandoned. Browse, Review, Highlights, the editor, culling, Misses, and pipeline review already provide history refresh hooks.

**Reversible gaps that should be tracked**

| Area | Missing actions and required before-state |
| --- | --- |
| Replace species from a prediction | `/api/predictions/<id>/replace-keywords` still logs an excluded `prediction_replace_species` entry. Needs removed keyword IDs/provenance, all changed prediction statuses, and the highlight/representative rows moved by replacement. |
| Prediction group dialog | Picks record flag and keyword/accept edits separately. Rejected prediction statuses and removal from the group are not fully recorded. Needs one composite action for the entire Apply gesture. |
| Species representatives and ordered highlights | Set/clear a representative; add/remove/reorder highlights; legacy photo-preference writes. Needs prior selection order/ranks and promotion effects. |
| Collections | Create, rename, change rules/visual expression, add photos, duplicate, delete, and Save Highlights as Collection. Needs stable collection IDs and complete prior definitions. |
| Keyword vocabulary | Rename, retype, reparent, merge/reconcile, delete, clean unused keywords, link a location keyword to a Google place, and fill saved map coordinates. Needs association provenance, dependent vocabulary, sidecar work, retargeted history, and curated state. Undoing a photo assignment does not reverse a separate vocabulary edit. |
| Primary subject and active mask choice | Select/reset a primary subject and switch active mask variant. Needs previous selection and invalidation of dependent preview state. |
| GPS discrepancy review | Keep original GPS and queue assigned-place GPS corrections remain audit-only. Needs the previous review fingerprint and exact pending queue; a correction already written to disk requires a compensating metadata write. |
| Pending metadata queue | Discard individual/all pending changes and queue location writes. `discard` is intentionally excluded today, but a queue snapshot could make it reversible. |
| Workspace organization | Create/rename/delete workspaces, pin/reorder tabs, attach/detach/move folder memberships, subject-type selections, and new-image snapshots. Needs visibility and workspace-owned state; deleting a workspace also removes its history. |
| Catalog maintenance | Remove missing originals from the catalog, remove a folder from the catalog, relocate a folder path, accept a new hash, and mark audit findings resolved. These need catalog snapshots even when originals remain on disk. |
| Duplicate resolution before trash | Resolving duplicates changes loser flags and merges metadata onto the winner without complete undo. Requires the winner's original metadata and every affected association/visibility row. |
| Settings and saved presets | Global/workspace settings, processing recipes, move rules, development/export presets, crop defaults, label descriptions, active models/labels, saved Misses thresholds, and recent destinations. No shared history today; global state also requires protection from edits in another workspace. Authentication credentials need their own explicit policy. |

Some currently supported setters still record an unchanged rating, flag, or label as an edit. Undo is safe but can appear to do nothing and those records can invalidate Redo. Also, generic keyword removal restores a tag as manually authored rather than preserving its previous provenance. These are consistency gaps; the new location snapshots preserve provenance explicitly.

**Actions outside general history**

| Action | Current boundary |
| --- | --- |
| Delete/trash originals, sidecars, models, caches, or staging files | No guarantee of restoration from the shared buttons. OS Trash or a separate recovery workflow may help. Catalog-only deletion is listed above as a reversible gap. |
| Import, move, stage, archive, sync, export, publish, or submit to iNaturalist | Original disk/remote effects have no general replay. User-chosen import keywords and newly recorded import GPS assignments can be undone independently of the imported files. Moving files may be reversible, but needs destination-conflict and availability checks rather than a simple database replay. |
| Scan/classify/develop/analyze/download/recompute jobs | Job controls can pause/cancel applicable work; completed generated results have no general before/after snapshot. Capture-time repair and metadata repair deserve separate reversible snapshots if their results are expected to support Undo. |
| Query, preview, validation, reveal/open, workspace navigation, job pause/resume/cancel | These are queries or have direct controls. Native text-field undo and unsaved editor history take precedence over catalog keyboard shortcuts. |

Undo history is workspace-scoped and bounded by `max_edit_history`. Catalog metadata and representatives can be shared between workspaces, so an older edit can encounter changes made elsewhere. Deleted vocabulary is deliberately not recreated by keyword/location replay, and prediction rows replaced by classification are skipped by checking a non-reusable instance token and current photo scope rather than trusting a reusable row ID or second-level timestamp. In-place enrichment of the same prediction preserves replay, including confidence, taxonomy, and full-fingerprint backfills. Schema version 16 adds lazily created prediction-history instance tokens that are removed automatically when their prediction is deleted. Earlier prediction-status snapshots without an instance token cannot prove identity and do not replay. These limitations need conflict-aware replay if stronger restoration guarantees are desired.

**Verification**

The focused regression tests cover location replacement with multiple old tags and mixed batch originals, preserved provenance and GPS review state, repeated undo/redo, pending sidecar corrections, no-op behavior, legacy-entry exclusion, and rollback after an injected history failure. Prediction tests cover single/batch/burst rejection, reviewed decisions, alternative siblings, and isolation between label sets. The browser tests exercise the actual navbar buttons for flags, ratings, labels, keywords, locations, batch locations, prediction rejection/review, and saved development recipes, plus history transport guards, unsaved editor history, and page refreshes.

Validation completed: more than 1,500 relevant unit tests and 38 browser tests passed. Python lint, the frontend lint/type/test checks, and the backend type checker passed. The full unit suite was not run.

Source references: [history composition](../vireo/db.py), [history persistence](../vireo/repositories/edit_history.py), [new edit capture](../vireo/services/edit_history.py), [route inventory and replay regressions](../vireo/tests/test_undo_coverage.py), and [navbar browser tests](../tests/e2e/test_history_coverage.py).
