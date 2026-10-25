# Dashboard page JavaScript

`vireo/templates/stats.html` (Dashboard, at `/dashboard`) is a **module
page** (`frontend-module-pages.json`; see `vireo/static/misses/README.md` for
the conventions). The template loads one entry module after the shared
navbar:

```html
<script type="module" src="/static/dashboard/boot.js"></script>
```

The shared pending-transfer panel also appears above the scope controls. It
lists the active workspace's local originals across all Dashboard scopes,
with catalog size estimates, retention age, destination and transfer status.

| File | Responsibility |
| --- | --- |
| `state.js` | The `state` object: the scope and its options, the last stats, the per-folder coverage cache, and which detail panels are open |
| `shared.js` | The boundary with shared code: `safeFetch`, `showToast`, the escape helpers |
| `scope.js` | The scope's query parameters, the scope controls and summary, and opening Browse or Process with the same scope (Process is refused with a reason when it cannot keep the scope) |
| `drill.js` | Bar rows that open the matching photos in Browse (`data-dashboard-drill`) |
| `loading.js` | Loading the scope options (from the URL, skipping disabled collections) and reloading every section |
| `coverage.js` | Pipeline and attribute coverage, per scope and per folder |
| `overview.js` | `/api/stats`: the headline counts and the attention cards |
| `charts.js` | Species, months and trend, ratings, flags, classification, detection, quality and hours |
| `inventory.js` | The classification inventory |
| `details.js` | Starting a metadata sync, and the folder and pending-change detail panels |
| `boot.js` | The entry module: binds the controls that used to carry inline handlers and the delegated listeners on rendered markup, and loads the page on `DOMContentLoaded` |

## Rendered markup

Rendered markup carries data attributes, read by delegated listeners in
`boot.js`: `data-dashboard-drill` (bar rows, click or Enter/Space),
`data-inv-model` (inventory model rows), `data-folder-cov` and
`data-browse-folder` (folder coverage rows), and `data-attention-action`
(the attention cards' buttons: `process-classify`, `process-previews`,
`browse-unlocated`, `sync`, `duplicates`). The page registers no page hooks
and writes no `window` globals.
