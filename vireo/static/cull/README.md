# Cull page JavaScript

`vireo/templates/cull.html` (Cull, at `/cull`) loads classic scripts from
`vireo/static/cull/` by responsibility, deferred behind the navbar's module
entry and the shared `vireo-browse-compare.js`, with `boot.js` last. They share
page globals, as the inline script and the older `cull-favorites.js` did.

| File | Responsibility |
| --- | --- |
| `state.js` | Shared state: the analysis on screen, the busy and pending flags, pinned and applied decisions, the collection scope, expanded species, the focused group, the slider timers and defaults |
| `collections.js` | The collection picker and the slider defaults from the pipeline config |
| `settings.js` | The scoring and grouping sliders: request configs, defaults, resets, Save grouping defaults, and the debounced recompute |
| `analysis.js` | Storing results, the request body for the current scope, the pending state behind Apply, reflow and regroup, and loading the cached results |
| `history.js` | The undo/redo page hooks (`beforeHistoryChange`, `afterHistoryChange`) |
| `decisions.js` | Species names, flag and label to action, pin counts, `resolveCullDecision`, encounter labels |
| `data.js` | `pipelineToCullData`: species groups, scene groups, each photo's decision, and the summary totals |
| `render.js` | The summary's decision stats and actions, and `renderCulling` |
| `focus.js` | The focused scene group and arrow-key movement |
| `actions.js` | Expanding a species, changing, releasing and clearing decisions, and Apply |
| `favorites.js` | Scope filters, favorites, comparison and the rejected-photos dialog (was `cull-favorites.js`) |
| `boot.js` | Registers the page hooks, binds the template's `data-cull-on` controls and the arrow keys, and loads the page on `DOMContentLoaded` |

## Markup handlers

The template carries no inline `on*=` attributes. A control names its handler
in `data-cull-on="event:name"`; `boot.js` binds it from `CULL_HANDLERS`,
passing the element (the sliders hand themselves to `onScoringChange` /
`onGroupingChange`, as `onchange(this)` did). An unknown name throws. Cards
and summary links rendered by `render.js` and `favorites.js` still carry
inline handlers that call these globals; moving them to delegated listeners
comes with converting the page to modules.

## Guards

- Your pins (`cullManualDecisions`) outrank saved flags and fresh suggestions,
  so a slider tweak never silently undoes a decision. Review pins stay pinned
  after Apply (`cullAppliedDecisions`) but stop counting as unsaved work.
- `cullOperationPending()` disables Apply and refuses undo/redo while an
  analysis, a debounced recompute, a delete or a history change is in flight.
- `cullAnalysisSeq` / `cullAnalysisPending` drop superseded recompute
  responses (see `recomputeCulling` in `favorites.js`).
