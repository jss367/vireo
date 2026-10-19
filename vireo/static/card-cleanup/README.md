# Card cleanup page JavaScript

`vireo/templates/card_cleanup.html` (Free up card space, at `/card-cleanup`)
is a **module page** (`frontend-module-pages.json`; see
`vireo/static/misses/README.md` for the conventions). The template loads the
shared folder browser (deferred) and one entry module:

```html
<script defer src="/static/vireo-folder-browser.js"></script>
<script type="module" src="/static/card-cleanup/boot.js"></script>
```

The page deletes card files only where the archive copy is verified; every
count it shows comes straight from the scan manifest.

| File | Responsibility |
| --- | --- |
| `state.js` | The `state` object (the scan and the one job this page watches, its stream, the manifest, rendered lists, the confirm dialog's Esc handler, busy state), the manifest reasons it answers (`CARD_CLEANUP_VERIFY_REASON`, `CARD_CLEANUP_HASH_FAILED_REASON`), busy state, file counts and the error line |
| `shared.js` | The boundary with shared code: `formatBytes`, `VireoFolderBrowser`, and the desktop app's `pickDirectory` / `isTauri` |
| `folder-browser.js` | Choosing the card folder: native picker in the desktop app, the shared in-page browser otherwise |
| `jobs.js` | Scanning, following a job's stream (scan, verify, delete), and finishing it, falling back to polling when the stream drops |
| `manifest.js` | Loading the manifest (stale responses are dropped) and rendering the buckets |
| `verification.js` | The card-scoped archive verification and the hash-failed callout |
| `delete.js` | Bucket lists (built when first opened), the delete confirmation, deleting, the result, and Cancel |
| `source.js` | Volume suggestions and the import page's `?source=` / `?others=` links |
| `boot.js` | The entry module: binds the controls that used to carry inline handlers and the confirmation's backdrop, then applies the query parameters and loads suggestions |

The page registers no page hooks and writes no `window` globals.

## Guards

- One job at a time: the start buttons are disabled while any job runs
  (`cardCleanupSetBusy`), so a second start cannot orphan the first job's
  completion.
- `cardCleanupFinishJob` runs once per job even when the stream fires both
  `complete` and `error`.
- `cardCleanupLoadManifest` captures `state.scanJobId` before the fetch and
  drops the response (success or error) if a newer scan started meanwhile.
- Each bucket `<details>` gets one `toggle` listener, which reads the manifest
  current when it is opened.
