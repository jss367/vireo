# Audit page JavaScript

`vireo/templates/audit.html` (Audit, at `/audit`) is a **module page**
(`frontend-module-pages.json`; see `vireo/static/misses/README.md` for the
conventions). The template loads one entry module after the shared navbar:

```html
<script type="module" src="/static/audit/boot.js"></script>
```

| File | Responsibility |
| --- | --- |
| `state.js` | The `state` object (each check's last results) and the check and hash-status labels |
| `shared.js` | The boundary with shared code: `safeFetch`, `showToast`, the escape helpers |
| `helpers.js` | `byId`, the status line's found/clean/error states, the paint-before-scan pause, and posting an audit action |
| `summary.js` | The tabs, the summary banner and its chips, and Run All Checks |
| `drift.js` | Drift between the catalog and XMP sidecars, resolved per row or in bulk |
| `orphans.js` | Catalog entries whose file is gone |
| `untracked.js` | Files the catalog does not know: import, or send their folders to Process |
| `sidecars.js` | Stray `.xmp` files, moved to the Trash |
| `gps.js` | GPS excursions in shot order and reversible catalog coordinate corrections |
| `clocks.js` | Camera Clocks: folders whose camera clock or time zone was wrong, with the evidence and per-group fixes run through the capture-time job (preview first, then an in-page confirmation) |
| `integrity.js` | The verify-hashes job, the flagged files, and Accept Current File |
| `boot.js` | The entry module: binds the tabs and buttons that used to carry inline handlers and the delegated row buttons, then loads the summary and the Integrity tab |

Every check refreshes the summary banner when it finishes. Rendered rows carry
`data-photo-id` (Drift also `data-direction`) or a row index `data-idx`
(Untracked, Sidecars: paths are looked up from state by index, never written
into markup); one delegated listener per table reads them. Camera Clocks fix
rows carry `data-finding` and `data-fix` indexes into `state.clockData`, so the
photo ids a fix rewrites never pass through markup. The summary chips
carry `data-tab`. The page registers no page hooks and writes no `window`
globals.
