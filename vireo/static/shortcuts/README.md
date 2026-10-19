# Keyboard Shortcuts page JavaScript

`vireo/templates/shortcuts.html` (Keyboard Shortcuts, at `/shortcuts`) is a
**module page** (`frontend-module-pages.json`; see
`vireo/static/misses/README.md` for the conventions). The template loads one
entry module after the shared navbar:

```html
<script type="module" src="/static/shortcuts/boot.js"></script>
```

| File | Responsibility |
| --- | --- |
| `labels.js` | Each context's title and action labels (`SHORTCUT_LABELS`) and default bindings (`SHORTCUT_DEFAULTS`) |
| `state.js` | The `state` object: the bindings being edited and the button capturing a key |
| `shared.js` | The boundary with shared code: `safeFetch`, `parseShortcut` / `formatShortcut`, and pausing and resuming the `Keymap` dispatcher |
| `editor.js` | Rendering the editor, conflict and bare-key checks, resets, saving to `/api/config`, and the first load |
| `capture.js` | `startCapture`: waiting for the next key press on one binding |
| `boot.js` | The entry module: one delegated click listener on `#shortcutsEditor` (key buttons, per-action reset, Reset All) and the first load |

The editor re-renders after every change, so its buttons carry
`data-ctx` / `data-action` (or `data-shortcut-reset-context`) instead of
inline handlers, and one delegated listener reads them.

## Guards

- While a key is being captured, the global `Keymap` dispatcher is paused
  (`pauseKeymapDispatch(onKey)`), so a navigation binding cannot navigate
  away, and the native Find command can complete the capture
  (`vireo/tests/page_find_dom.cjs` runs `capture.js` against a mock DOM).
- A bare letter for a navigation binding that a page uses is cleared
  (`isBarePageShortcut`), both when captured and when loaded.
- Presence, not truthiness, decides whether a binding is set: an explicit
  `''` stays unassigned instead of falling back to the default.

The page registers no page hooks and writes no `window` globals.
