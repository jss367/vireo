# Move page JavaScript

`vireo/templates/move.html` (Move Photos, at `/move`) loads these classic
scripts after the shared navbar scripts and `vireo-rsync-help.js`, where its
inline page script used to be. Functions remain global for the page markup's
`onclick=` / `onchange=` attributes (`switchTab`, `onQuickFolderChange`,
`startQuickMove`, `loadPhotosForFolder`, `showRuleForm`, `openBrowser`,
`confirmOk`, ...) and the rule list's and rule form's inline handlers. The
page relies on the navbar's `safeFetch` and `showToast`, the shared
`escapeHtml` / `escapeAttr` from `vireo-utils.js`, and `rsyncInstallHint`
from `vireo-rsync-help.js`.

| File | Responsibility |
| --- | --- |
| `state.js` | Page state: `allFolders`, the photo grid list and selection, the folder browser's target input and path, recent destinations, `confirmCallback`, `editingRuleId`, `quickMoveBusy` |
| `remote-targets.js` | Remote (SSH) move targets: `loadRemoteTargets()`, `quickRemoteTargetId()`, and the destination-mode select |
| `folders.js` | `switchTab()`, `loadFolders()` (`/api/folders`), and the Quick Move / Photo Move folder selects (with the `?folder_id=` preselect) |
| `quick-move-form.js` | Quick Move inputs: the source folder, the editable folder name, the capture-date folder mode and format presets, and their validation results |
| `quick-move-preview.js` | The Quick Move summary panel, `updateQuickMoveBtn()`, `updateQuickMovePreview()`, `resolvedMoveDest()`, and the path helpers that mirror `move.py` (`rsyncDestSpec`, `normalizeRemoteSubpath`, `_isAbsolutePath`) |
| `quick-move-preflight.js` | The debounced `/api/move-folder/preflight` status line: local, remote and capture-date preflights, the exists warning, and the exact-count follow-up |
| `quick-move-start.js` | `startQuickMove()` (the preview preflight and the confirm or merge dialog) and `executeQuickMove()` (`/api/jobs/move-folder`) |
| `photo-move.js` | Photo Move: paged loading of a folder's photos, the grid, click/shift/ctrl selection, the selection bar, `startPhotoMove()` and `executePhotoMove()` (`/api/jobs/move-photos`) |
| `move-visibility.js` | The remembered keep-visible choice (`/api/config`) and the affected-workspaces check (`/api/move-photos/visibility`), shared by Quick Move, Photo Move and rules |
| `rules.js` | The move rule list (`/api/move-rules`), criteria summaries, and running, editing and deleting a rule |
| `rule-form.js` | The rule editor: `showRuleForm()`, `gatherCriteria()`, preview, save, and `hideRuleForm()` |
| `folder-browser.js` | The destination browser modal: `openBrowser()`, `browseTo()` (Pictures and Volumes shortcuts), the folder list, selection, and New Folder |
| `recent-destinations.js` | The browser's recent-destination shortcuts and `/api/recent-destinations` |
| `confirm-modal.js` | `showConfirm()`, the merge review (`showMergeConfirm()` and its route, stat and note builders), `closeConfirm()` and `confirmOk()` |
| `boot.js` | The two `DOMContentLoaded` handlers: the initial fetches, then the destination-input and folder-browser listeners |

Load `boot.js` last. The other files only declare functions and initialize
passive state (`var` declarations and one `new Set()`), so their relative
order does not matter at load time. Boot keeps the inline script's two
`DOMContentLoaded` handlers in their original order: the initial fetches
(`loadFolders()`, `loadRemoteTargets()`, `loadRules()`,
`loadRecentDestinations()`, `loadMoveVisibilityPreference()`), then the
`input` listeners on the destination fields and the delegated `dblclick`
handler on the browser list. The scripts sit after the folder browser and
confirm modal markup, so every element the page looks up is parsed before
boot runs. Adding a script requires an explicit template tag before boot.
These are not ES modules and need no bundler.

Page-wide state lives in `state.js`. Domain state stays beside its functions:
`quickRemoteTargets` / `quickRsyncAvailable` in `remote-targets.js`,
`quickPreflightSeq` / `quickPreflightTimer` in `quick-move-preflight.js`,
`moveVisibilityRequest` / `moveVisibilityReady` in `move-visibility.js`, and
`rulesList` in `rules.js`.

This page starts jobs that move originals. Keep these guards intact when
changing this code:

- `quickPreflightSeq` is bumped on every `scheduleQuickPreflight()` /
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
- `quickMoveBusy` disables Review move while the preview preflight runs, and
  `startQuickMove()` returns early if it is set.
- `moveVisibilityReady` gates Photo Move, rule runs and capture-date Quick
  Moves until the remembered keep-visible choice has loaded; a settings write
  clears it until the write settles. `refreshMoveVisibility()` drops stale
  responses through `moveVisibilityRequest`.
- `photoLoadSeq` drops a superseded folder's photo pages; `browseSeq` drops a
  superseded browser listing (so a slow initial fetch can't clobber a
  Pictures/Volumes shortcut click); `recentDestinationsSeq` drops a stale
  recents payload.
