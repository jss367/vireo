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
| `species-conflicts.js` | Species identity comparison, evidence, and warnings |
| `results.js` | Encounter grid reconciliation and photo cards |
| `tuning.js` | Scoring/grouping controls, defaults, reflow, and regrouping |
| `inspection.js` | Photo lookup and the inspection overlay |
| `flags.js` | Photo/group flags and coordination of in-flight writes |
| `scope.js` | Cached/scoped results, collection scope, and read-only guards |
| `loading.js` | Initialization, empty states, readiness, and Browse handoff |
| `species-editor.js` | Species menus, search, confirmation, and history hooks |
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

Scope changes in `scope.js` protect writes across the page; Group Review's
session checks protect asynchronous seeding and apply. Flag writes in `flags.js`
coordinate direct and group edits. Keep these guards intact when changing
callers. The shared lightbox implementation remains in `vireo/static/lightbox/`;
this directory only supplies Pipeline Review's integration hooks.
