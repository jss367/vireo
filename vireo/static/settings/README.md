# Settings page JavaScript

`vireo/templates/settings.html` loads these classic scripts after the shared
navbar, search, folder-browser, transfer-help, and filter helpers. Functions
remain global for the existing inline event handlers. Each file keeps the
state for its responsibility alongside the functions that use it.

| File | Responsibility |
| --- | --- |
| `page.js` | Page search, collapsible sections, and theme selection |
| `autosave.js` | Initial-load readiness, save generations, serialized writes, and save status |
| `workspace.js` | Workspace overrides and their debounced saves |
| `external-editors.js` | External-editor list and form serialization |
| `quick-filters.js` | Filter-shortcut editing, validation, and serialization |
| `remote-targets.js` | Remote destination editor, folder browser, and connection checks |
| `nas-setup.js` | Network-attached storage setup wizard |
| `config.js` | Curated form loading/saving, secret-field tracking, and pipeline defaults |
| `system-tools.js` | System information, platform support, metadata tools, and raw rendering tools |
| `models.js` | Model downloads, verification, selection, and removal |
| `labels.js` | Species lists, taxonomy, place search, and embedding precomputation |
| `maintenance.js` | Cache management, scan roots, and keyword cleanup |
| `desktop.js` | Version, native file pickers, and updater controls |
| `all-settings.js` | Schema-driven settings, scope selection, resets, and settings import |
| `boot.js` | Initial requests, event setup, and the storage-wizard deep link |

Load `boot.js` last. Earlier files only declare functions and initialize state;
page requests and event binding start after every definition is available.
`remote-targets.js` must precede `nas-setup.js`, whose style constants use the
remote editor's input style. Other cross-file function calls happen after boot.

Autosave is shared across both settings forms and workspace overrides. Keep its
generation checks, per-path write queues, and import suspension intact when
changing those flows. Settings import in `all-settings.js` deliberately waits
for pending writes and refreshes the curated form through `loadConfig()` before
re-enabling autosave.
