# Welcome page JavaScript

`vireo/templates/welcome.html` (first-run setup, at `/welcome`) is a **module
page** (`frontend-module-pages.json`; see `vireo/static/misses/README.md` for
the conventions). It has no navbar: it loads `vireo-api.js`,
`vireo-utils.js` and the page-find panel, then one entry module:

```html
<script type="module" src="/static/welcome/boot.js"></script>
```

| File | Responsibility |
| --- | --- |
| `state.js` | `MODEL_ID`, the `state` object (model and classification readiness, the chosen region, the search debounce) and `byId` |
| `shared.js` | The page's own `safeFetch` and `safeEventSource`: without a navbar those globals never exist, so these standalone versions (the old inline fallbacks) are all it uses |
| `system-check.js` | The advisory ExifTool check |
| `model-step.js` | Step 1: Get Started downloads the model, then finishes setup or moves to step 2; `finishSetup` |
| `labels-step.js` | Step 2: region search, wildlife groups, the species-list job, and embedding warm-up |
| `boot.js` | The entry module: binds the buttons and skip links that used to carry inline handlers, runs the ExifTool check, and enables Get Started (or resumes at step 2) once the model status is known |

Place names from the iNaturalist search and the ExifTool install hint are
rendered with `textContent`, never `innerHTML`. The page registers no page
hooks and writes no `window` globals.
