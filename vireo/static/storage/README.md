# Storage page JavaScript

`vireo/templates/storage.html` (Storage, at `/storage`) loads classic scripts
from `vireo/static/storage/` by responsibility, deferred behind the navbar's
module entry and after the shared `vireo-release-originals.js`, with
`boot.js` last. They share page globals, as the inline script and the older
`storage-*.js` files did.

| File | Responsibility |
| --- | --- |
| `format.js` | `storageEscapeHtml`, `storageEscapeAttr`, `storageFormatBytes` |
| `config.js` | Loading the cache settings, their debounced autosave (everything except the working-copy quota), and the quota state the other files share (`_committedWorkingCopyQuotaMb`, `_workingCopySizeBytes`, ...) |
| `working-copy-limit.js` | The quota's explicit Apply flow: measuring usage, the reduction confirmation dialog, and committing (a server-side 409 reroutes into the dialog) |
| `panel.js` | `loadStoragePanel`: totals, free space, locations, the working-copy status, the category cards and the auxiliary breakdown; Refresh, Clear safe caches, Open folder |
| `masks.js` | The Masks card and its Manage dialog |
| `files-modal.js` | The Manage dialog for thumbnails, previews and label embeddings |
| `caches.js` | Preview, detection and embedding cache status and clears, and the embedding matrix |
| `overview.js` | The category cards and their sort (was `storage-overview.js`) |
| `retained.js` | Retained generated payloads and grouping undo snapshots (was `storage-retained.js`) |
| `hf-cache.js` | The Hugging Face cache card and dialog (was `storage-hf-cache.js`) |
| `boot.js` | Registers the page hook, binds the template's `data-storage-on` controls and the delegated listeners on rendered markup, and loads the page on `DOMContentLoaded` |

## Markup handlers

The template carries no inline `on*=` attributes. A control names its
handlers in `data-storage-on="event:name ..."`; `boot.js` binds each from its
`STORAGE_HANDLERS` table, passing the element (so `if (event.target === el)`
backdrop closes and the retention form's button work as before). An unknown
name throws. Rendered markup carries data attributes read by delegated
listeners: `data-storage-location` (Open folder), `data-storage-manage`
(the category cards' Manage buttons), `data-masks-action`, and the file
list's checkboxes.

## Page hook

`vireo-release-originals.js` (shared with Browse) reloads the overview after a
release job. It used to call `loadStoragePanel` by global name; it now asks for
the `loadStoragePanel` page hook, which `boot.js` registers.

## Guards

- The working-copy quota never rides the autosave payload: a debounced
  autosave firing during Apply would POST the old quota. Apply flushes a
  pending autosave first and stops if that fails.
- A background refresh syncs the committed quota from the server but only
  overwrites the input when the user has not edited it and no confirmation
  dialog is open.
- A failed `/api/storage` scan leaves usage unknown (`null`), so Apply cannot
  skip the eviction confirmation on a stale zero.
