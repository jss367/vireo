# Logs page JavaScript

`vireo/templates/logs.html` (Logs, at `/logs`) is a **module page**
(`frontend-module-pages.json`; see `vireo/static/misses/README.md` for the
conventions). The template loads one entry module after the shared navbar:

```html
<script type="module" src="/static/logs/boot.js"></script>
```

| File | Responsibility |
| --- | --- |
| `state.js` | `LEVEL_ORDER` and the `state` object (the line count) |
| `shared.js` | The boundary with shared code: `safeFetch`, `showToast`, `escapeHtml`, `VireoTextSearch` |
| `lines.js` | Appending a log record, the level and text filter, and Clear |
| `boot.js` | The entry module: binds the toolbar (level, filter, match-case and whole-word toggles, Clear), loads the last 500 lines, and opens the `/api/logs/stream` event source, closed on unload or on a stream error |

The page registers no page hooks and writes no `window` globals.

## Load order

Module scripts run in document order after the navbar's entry module, so
`safeFetch` and `showToast` exist when `boot.js` runs; it runs after the
document is parsed, so every toolbar element exists too. The inline script
this replaced waited for `DOMContentLoaded` before its first fetch for the
same reason.
