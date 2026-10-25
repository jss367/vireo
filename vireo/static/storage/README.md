# Storage page JavaScript

`vireo/templates/storage.html` (Storage, at `/storage`) is a **module page**
(`frontend-module-pages.json`; see `vireo/static/misses/README.md` for the
pattern). After the shared `vireo-release-originals.js`, the template loads
one entry module:

```html
<script type="module" src="/static/storage/boot.js"></script>
```

and the browser loads every other file through `boot.js`'s imports.

The shared pending-transfer panel above the storage totals shows local
originals awaiting NAS transfer in the active workspace. Its catalog size
estimates are separate from measured storage totals and safe-cache cleanup.

| File | Responsibility |
| --- | --- |
| `state.js` | Page state: the `state` object (the autosave timer, the committed and pending working-copy quota and measured usage, and the last masks, files, overview, retained-storage and Hugging Face cache data) |
| `shared.js` | The boundary with shared code: `safeFetch`, `safeEventSource`, `showToast`, `VireoReleaseOriginals.storageButton`, the `VireoTime` formatters, and `registerPageHooks()` |
| `format.js` | `storageEscapeHtml`, `storageEscapeAttr`, `storageFormatBytes` |
| `config.js` | Loading the cache settings and their debounced autosave (everything except the working-copy quota) |
| `working-copy-limit.js` | The quota's explicit Apply flow: measuring usage, the reduction confirmation dialog, and committing (a server-side 409 reroutes into the dialog) |
| `panel.js` | `loadStoragePanel`: totals, free space, locations, the working-copy status, the category cards and the auxiliary breakdown; Refresh, Clear safe caches, Open folder |
| `masks.js` | The Masks card and its Manage dialog |
| `files-modal.js` | The Manage dialog for thumbnails, previews and label embeddings |
| `caches.js` | Preview, detection and embedding cache status and clears, and the embedding matrix |
| `overview.js` | The category cards and their sort |
| `retained.js` | Retained generated payloads and grouping undo snapshots, and their confirmation dialog |
| `hf-cache.js` | The Hugging Face cache card and dialog (unit-tested by `tests/frontend/hf-cache-outcomes.test.mjs`) |
| `boot.js` | The entry module: registers the page hook, binds the template's `data-storage-on` controls and the delegated listeners on rendered markup, and loads the page on `DOMContentLoaded` |

Page state is the one `state` object in `state.js`; files write its fields,
since a module cannot reassign another module's binding. The page writes no
`window` globals (`sharedWrites` is empty).

## Markup handlers

The template carries no inline `on*=` attributes. A control names its
handlers in `data-storage-on="event:name ..."`; `boot.js` binds each from its
`STORAGE_HANDLERS` table, passing the element (so `if (event.target === el)`
backdrop closes and the retention form's button work as before). An unknown
name throws. Rendered markup carries data attributes read by delegated
listeners: `data-storage-location` (Open folder), `data-storage-manage`
(the category cards' Manage buttons), `data-masks-action`, and the file
list's checkboxes. The retained-storage rows and their confirmation dialog
bind with `addEventListener`.

## Page hook

`vireo-release-originals.js` (shared with Browse) reloads the overview after a
release job through the `loadStoragePanel` page hook, which `boot.js`
registers.

## Tests

Browser tests reach the modules through `tests/e2e/module_pages.py`:
`test_storage.py` overrides the `page` fixture with
`install_page_module_exposure(page, "storage", "storageState")` and calls
`pageModules.panel.loadStoragePanel()` and the like.

## Guards

- The working-copy quota never rides the autosave payload: a debounced
  autosave firing during Apply would POST the old quota. Apply flushes a
  pending autosave first and stops if that fails.
- A background refresh syncs the committed quota from the server but only
  overwrites the input when the user has not edited it and no confirmation
  dialog is open.
- A failed `/api/storage` scan leaves usage unknown (`null`), so Apply cannot
  skip the eviction confirmation on a stale zero.
