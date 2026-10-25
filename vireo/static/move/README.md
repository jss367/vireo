# Move page JavaScript

`vireo/templates/move.html` (Move Photos, at `/move`) is a **module page**
(`frontend-module-pages.json`; see `vireo/static/misses/README.md` for the
pattern). After the shared `vireo-rsync-help.js`, the template loads one entry
module:

```html
<script type="module" src="/static/move/boot.js"></script>
```

and the browser loads every other file through `boot.js`'s imports.

| File | Responsibility |
| --- | --- |
| `state.js` | Page state: the `state` object (`allFolders`, the photo grid list and selection, the folder browser's target input and path, recent destinations, `confirmCallback`, `editingRuleId`, `quickMoveBusy`, the remote targets, the preflight sequence and timer, the keep-visible request and readiness, and `rulesList`) |
| `shared.js` | The boundary with shared code: `safeFetch` (typed per endpoint in `types/pages/move.d.ts`), `showToast`, `escapeHtml` / `escapeAttr`, `rsyncInstallHint` (`vireo-rsync-help.js`), and the thumbnail URL |
| `remote-targets.js` | Remote (SSH) move targets: `loadRemoteTargets()`, `quickRemoteTargetId()`, and the destination-mode select |
| `folders.js` | `switchTab()`, `loadFolders()` (`/api/folders`), and the Quick Move / Photo Move folder selects (with the `?folder_id=` preselect) |
| `quick-move-form.js` | Quick Move inputs: the source folder, the editable folder name, the capture-date folder mode and format presets, and their validation results |
| `quick-move-preview.js` | The Quick Move summary panel, `updateQuickMoveBtn()`, `updateQuickMovePreview()`, `resolvedMoveDest()`, and the path helpers that mirror `move/remote.py` (`rsyncDestSpec`, `normalizeRemoteSubpath`, `_isAbsolutePath`) |
| `quick-move-preflight.js` | The debounced `/api/move-folder/preflight` status line: local, remote and capture-date preflights, the exists warning, and the exact-count follow-up |
| `quick-move-start.js` | `startQuickMove()` (the preview preflight and the confirm or merge dialog) and `executeQuickMove()` (`/api/jobs/move-folder`) |
| `photo-move.js` | Photo Move: paged loading of a folder's photos, the grid, click/shift/ctrl selection, the selection bar, `startPhotoMove()` and `executePhotoMove()` (`/api/jobs/move-photos`) |
| `move-visibility.js` | The remembered keep-visible choice (`/api/config`) and the affected-workspaces check (`/api/move-photos/visibility`), shared by Quick Move, Photo Move and rules |
| `rules.js` | The move rule list (`/api/move-rules`), criteria summaries, and running, editing and deleting a rule |
| `rule-form.js` | The rule editor: `showRuleForm()`, `gatherCriteria()`, preview, save, and `hideRuleForm()` |
| `folder-browser.js` | The destination browser modal: `openBrowser()`, `browseTo()` (Pictures and Volumes shortcuts), the folder list, selection, and New Folder |
| `recent-destinations.js` | The browser's recent-destination shortcuts and `/api/recent-destinations` |
| `confirm-modal.js` | `showConfirm()`, the folder-move confirm (`showMoveConfirm()`), the merge review (`showMergeConfirm()` and its route, stat and note builders), `closeConfirm()` and `confirmOk()` |
| `events.js` | The template's `data-move-on` bindings and the delegated listeners for the photo grid, the rule list and the rule form |
| `boot.js` | The entry module: binds the markup (`events.js`), then the old inline script's two `DOMContentLoaded` handlers: the initial fetches, then the destination-input and folder-browser listeners |

Page state is the one `state` object in `state.js`; files write its fields,
since a module cannot reassign another module's binding. The page registers
no page hooks and writes no `window` globals.

## Markup handlers

The template carries no inline `on*=` attributes. A control names its handler
in `data-move-on="event:name"`, with `data-move-arg` for the one argument some
take (`switchTab`'s tab, `openBrowser`'s target input, `browseTo`'s shortcut;
`browseTo` with no argument opens the default start). `events.js` binds each
from its `MOVE_HANDLERS` table; an unknown name throws. Rendered markup
carries data attributes read by delegated listeners on its container: the
photo grid's cards (`data-id` / `data-idx`), the rule list's buttons
(`data-rule-action="run|edit|delete"` in a `.rule-item[data-id]`), and the
rule form's buttons (`data-rule-form="browse|save|preview|cancel"`). Boot
keeps the old order: the initial fetches (`loadFolders()`,
`loadRemoteTargets()`, `loadRules()`, `loadRecentDestinations()`,
`loadMoveVisibilityPreference()`), then the `input` listeners on the
destination fields and the delegated `dblclick` handler on the browser list.

Browser tests reach the modules through `tests/e2e/module_pages.py`
(`install_page_module_exposure(page, "move", "moveState")`), e.g.
`moveState.selectedPhotos` and `pageModules.photoMove.updatePhotoSelectionUI()`.

This page starts jobs that move originals. Keep these guards intact when
changing this code:

- `state.quickPreflightSeq` is bumped on every `scheduleQuickPreflight()` /
  `scheduleQuickDatePreflight()` call, not only when a request is sent, so a
  response for a previous destination can never write its exists/new-folder
  text over the current one. `requestExactDestCount()` reuses the same seq.
- The preview path, the preflight body and the move request share
  `normalizeRemoteSubpath()`, `rsyncDestSpec()` and `resolvedMoveDest()`,
  so the path the user confirms is the path the backend writes.
- `quickFolderNameResult()` returns `rename: false` with an empty value when
  the name input is unchanged from the source folder's name, so a no-op
  submit sends no `destination_name` and the backend keeps `folder.name`
  verbatim.
- `state.quickMoveBusy` disables Review move while the preview preflight runs, and
  `startQuickMove()` returns early if it is set.
- `state.moveVisibilityReady` gates Photo Move, rule runs and capture-date Quick
  Moves until the remembered keep-visible choice has loaded; a settings write
  clears it until the write settles. `refreshMoveVisibility()` drops stale
  responses through `state.moveVisibilityRequest`.
- `state.photoLoadSeq` drops a superseded folder's photo pages; `state.browseSeq` drops a
  superseded browser listing (so a slow initial fetch can't clobber a
  Pictures/Volumes shortcut click); `state.recentDestinationsSeq` drops a stale
  recents payload.
