# Lightroom Import page JavaScript

`vireo/templates/lightroom.html` (Lightroom Import, at `/lightroom`) is a
**module page** (`frontend-module-pages.json`; see
`vireo/static/misses/README.md` for the conventions). The template loads one
entry module after the shared navbar:

```html
<script type="module" src="/static/lightroom/boot.js"></script>
```

The page is a three-phase form: pick catalogs, preview, then import.

| File | Responsibility |
| --- | --- |
| `state.js` | The `state` object (the catalog paths added) and `byId` |
| `shared.js` | The boundary with shared code: `safeFetch`, `safeEventSource`, `escapeHtml`, and `isTauri` / `pickFile` from `tauri-bridge.js` |
| `catalogs.js` | Phase 1: adding and removing catalogs, the delegated remove buttons, the native file picker, and `lrShowPhase` |
| `preview.js` | Phase 2: `/api/import/preview` and the conflict list |
| `import-job.js` | Phase 3: starting the `import` job, following its stream, the result summary, and Done |
| `boot.js` | The entry module: binds the buttons and the Enter key that used to carry inline handlers, and shows the desktop-only Browse button |

The catalog list renders each remove button with `data-catalog-index`; one
delegated listener on `#catalogList` removes the clicked one. The page
registers no page hooks and writes no `window` globals.
