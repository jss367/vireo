# Workspace page JavaScript

`vireo/templates/workspace.html` (Workspace, at `/workspace`) is a **module
page** (`frontend-module-pages.json`; see `vireo/static/misses/README.md` for
the conventions). The template loads the shared Work Locally script, then one
entry module:

```html
<script defer src="/static/vireo-work-locally.js"></script>
<script type="module" src="/static/workspace/boot.js"></script>
```

| File | Responsibility |
| --- | --- |
| `state.js` | The `state` object (the folder rows by id), `byId`, and the active-workspace and workspace-list requests |
| `shared.js` | The boundary with shared code: `safeFetch`, `showToast`, `formatDuration`, the escape helpers, `window.vireoLocalFolders` (from `vireo-work-locally.js`), the navbar's workspace switcher and edit-navigation cursors, and `registerPageHooks` |
| `folders.js` | The Folders section: each folder's row with its Work Locally state and actions, removing a folder, the delegated row listeners, and in-place job progress |
| `move-folders.js` | The Move Selected bar and the Move Folders modal |
| `subject-types.js` | "What counts as identified?" |
| `history.js` | Recent Jobs |
| `workspaces.js` | All Workspaces: rename in place, Switch, Delete, and their delegated listeners |
| `deletion.js` | The in-page Delete Workspace dialog and request (was the classic `workspace-deletion.js`) |
| `boot.js` | The entry module: registers the page hook, binds every listener, and loads each section |

## Page hook

`vireo-work-locally.js` re-renders the folder rows after each local-folder
status load and job start. It used to call the page's `loadWsFolders` global
by name; the page now registers it as the `loadWsFolders` page hook
(`vireo-page-hooks.js`), and the shared script asks for it with
`Vireo.pageHook('loadWsFolders')`. The work-locally script runs first
(deferred, before the module), but its first status load is still in flight
when `boot.js` registers the hook, so the first reload is not missed.

## Rendered markup

Rows are re-rendered on every load, so they carry data attributes and the
containers have delegated listeners: Work Locally buttons
`data-local-action="stage|sync|discard"` with `data-folder-id`, the remove
button `data-remove-folder` / `data-workspace-id`, the workspace rows'
`data-rename-workspace`, `data-activate-workspace` and `data-delete-workspace`
(with `data-workspace-name`, so a name with quotes reaches the dialog
unchanged).
