# Pipeline Review page JavaScript

`vireo/templates/pipeline_review.html` loads these classic scripts after the
shared navbar, lightbox, search, and photo helpers. Functions remain global for
the existing inline event handlers. Load `boot.js` last: the other files only
declare functions and initialize state; boot starts requests, installs shared
hooks, and binds events after every definition is available.

| File | Responsibility |
| --- | --- |
| `state.js` | Shared results, scope, filters, and persisted view preferences |
| `view-controls.js` | Sidebar, sorting, search, filters, and display controls |
| `summary.js` | Review totals and the focused encounter algorithm trace |
| `coverage.js` | Stable import provenance, current membership and defined review coverage |
| `species-conflicts.js` | Species identity comparison, evidence, and warnings |
| `results.js` | Encounter grid reconciliation and photo cards |
| `tuning.js` | Scoring/grouping controls, defaults, reflow, and regrouping |
| `inspection.js` | Photo lookup and the inspection overlay |
| `flags.js` | Photo/group flags and coordination of in-flight writes |
| `scope.js` | Saved review, folder/collection filters, regrouped views, and read-only guards |
| `loading.js` | Initialization, empty states, readiness, and Browse handoff |
| `species-editor.js` | Species menus, search, confirmation, and history hooks |
| `species-preview.js` | Live photo/species matrix, subject outlines, frame selection, explicit add/replace and transition split |
| `group-session.js` | Group Review state, opening, seeding, closing, and detaching |
| `group-render.js` | Group Review cards, subject selection, and loupe selection |
| `group-changes.js` | Staged decisions, zone moves, keyboard actions, and apply |
| `group-resolution.js` | Image resolution and loupe zoom levels |
| `group-viewport.js` | Thumbnail sizes, transforms, pan, and loupe interaction |
| `metadata.js` | Species predictions, consensus, and photo metadata |
| `photo-actions.js` | Selection, ratings, reveal/copy, wildlife exclusion, and editing |
| `organization.js` | Collection/keyword dialogs and their ownership state |
| `context-menu.js` | Photo context menu construction and event routing |
| `lightbox-integration.js` | Page-specific options and guards for the shared lightbox |
| `events.js` | Photo deletion and life-list change reconciliation |
| `boot.js` | Ordered setup and page startup |

Cross-file function calls run after boot. Page-wide results and preferences live
in `state.js`; domain state stays beside its functions. Group Review shares
`grmState` from `group-session.js` across its rendering, staging, and viewport
files. The template lists each script explicitly; adding a file requires adding
its script tag before boot. Keep requests, DOM listeners, and shared hook
registration in setup functions called from boot, preserving their order there.

Folder and Collection scopes filter the saved review in place:
`pipelineResults` stays the whole saved review and `renderResults` hides
photos outside `reviewScopePhotoIds`, so edits keep their encounter indices.
Because that view stays editable, a hidden photo must never become a write
target. `isPhotoActionableInReviewScope()` / `photoIdsInReviewScope()` in
`scope.js` are the one rule for that, applied at the shared choke points:
`openInspect`, `openPipelineLightbox`, `getActiveSelection`, the context-menu
builder, and the writers (`setPipelineReviewFlag`, `setPipelineReviewRating`,
`setPipelineReviewGroupFlag`, `setWildlifeExcludedFor`, the collection and
keyword dialogs, `openPipelinePhotoEditor`, `detachPhoto`). Route any new
entry point that acts on a photo ID through them rather than re-checking
scope in the caller; `reconcileReviewScopeTargets()` re-applies the rule to
remembered targets after a scope load or membership refresh. Members of the
open Group Review session are exempt, because Group Review deliberately edits
the whole burst. `vireo/tests/pipeline_review_scope_guard.cjs` drives every
entry point with a hidden photo's ID.

Whole workspace scope and the Regroup button replace `pipelineResults` with a
regroup-live result; `isScopedReviewView()` makes those views read-only.
Scope changes in `scope.js` protect writes across the page; Group Review's
session checks protect asynchronous seeding and apply. Flag writes in `flags.js`
coordinate direct and group edits. Keep these guards intact when changing
callers. The shared lightbox implementation remains in `vireo/static/lightbox/`;
this directory only supplies Pipeline Review's integration hooks.

## Strict page checks

`tsconfig.frontend-pages/pipeline-review.json` checks every implementation in this
directory together as classic scripts. Endpoint and shared-helper contracts live
in `types/pages/pipeline-review.d.ts`; the page continues to load the same files
in the template's explicit order. Required snapshot and indexed-member reads use
`currentReviewResults()` / `requireReviewValue()` from `state.js`, including reads
inside deferred callbacks. Tests that extract a single page function must load
these two small invariant helpers as well.

Species confirmations from the grid and Group Review first open the same
native dialog. Add is the default, preserving confirmed species on other
subjects. Replace names only the species the reviewer intends to remove.
The per-photo matrix uses current catalog keywords and preserves subject IDs;
latest stored label sets are visible and superseded sets stay in a separate
collapsed history disclosure. Evidence below the workspace detector threshold
is marked explicitly. A frame range or individual checkboxes narrows the write;
only the explicit transition checkbox separates frames into another encounter.
The server previews additions/removals without writes, then revalidates its
returned token under the grouping and decision locks on Apply. A changed
membership, keyword identity/provenance, subject or prediction decision blocks
the apply. Scoped confirmations rebuild labels from every actual group member,
so selected frames cannot accidentally confirm the rest. Species and optional
membership changes share one durable undo entry.

When both Group Review checkboxes are selected, the preview carries the complete
staged pick/reject/removal plan. Apply sends one species request that validates
the plan and preview together, writes both halves under the same workspace and
database locks, saves the cache once, and records one Undo entry. Cancellation
saves nothing; failure at any write, history, cache save or commit stage rolls
back both species and cull decisions. The flags-only endpoint remains available
for culling without a species edit.
