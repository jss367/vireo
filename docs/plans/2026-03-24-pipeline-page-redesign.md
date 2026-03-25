# Pipeline Page Redesign

Split the current Pipeline page into two pages: **Pipeline** (configure & run) and **Pipeline Review** (inspect & tune results). Merge the Classify page into Pipeline as Stage 1. Scope the entire pipeline to a selected collection.

## Pipeline Page (`/pipeline`)

Single-column layout with a collection picker at the top and 3 stage cards stacked vertically.

### Top Bar

- Collection dropdown (same data as current classify page)
- Photo count for selected collection
- Optional collapsible thumbnail preview strip

### Stage Cards

Each card shows:
- Stage number + name
- Status indicator: not started / ready / running / complete
- Summary stats when complete
- Expandable settings section (model pickers, thresholds)
- Run button (disabled until prerequisites met)
- Inline progress bar + SSE status when running

Cards enforce top-to-bottom flow: card 2 requires card 1 complete, card 3 requires card 2 complete. Disabled cards explain why.

### Card 1: Classify

- **Settings:** Model picker (downloaded models), Labels picker (checkboxes), Threshold slider
- **Readiness checklist:** model downloaded? embeddings cached?
- **Actions:** Run button + Re-classify toggle
- **Complete state:** X detections, Y predictions stored, model name used

### Card 2: Extract Features

- **Settings:** SAM2 variant dropdown, DINOv2 variant dropdown, Proxy resolution slider
- **Includes:** Sharpness scoring (no separate button, runs automatically)
- **Actions:** Run button
- **Complete state:** X masks, X embeddings, X sharpness scores

### Card 3: Group & Score

- **Settings:** None (tuning sliders live on Review page)
- **Actions:** Run button
- **Complete state:** X encounters, X bursts, XK / XR / XX triage summary
- **Navigation:** "View Results" button links to Pipeline Review

### Shared Card Behavior

- Running a card invalidates downstream cards (marks them stale with warning indicator)
- Progress streamed via SSE, shown inline on the card
- Each card remembers settings used for its last run (visible diff if current selection differs)

## Pipeline Review Page (`/pipeline/review`)

Lift-and-shift of the current pipeline results view into its own page.

### Sidebar

- Summary stats (photos, encounters, bursts, keep/review/reject)
- Filter buttons (All / Keep / Review / Reject)
- Scoring threshold sliders (hard reject floors, MMR selection)
- Grouping threshold sliders (encounter cut/merge, burst time/phash/embedding)
- Reset to defaults button
- Live reflow on slider change

### Main Area

- Encounter cards with photo grids, species labels, triage badges
- Photo inspection panel (click for quality features, scores, reject reasons)

### Empty State

If no results exist: "Run the pipeline first" with link to `/pipeline`.

## Navbar Changes

- Remove "Classify" nav item
- Keep "Pipeline" nav item (points to `/pipeline`)
- Add "Pipeline Review" nav item (points to `/pipeline/review`)

## API Changes Required

- Extract Features API: accept `collection_id` parameter (currently operates on full workspace)
- Group & Score API: accept `collection_id` parameter
- New route: `/pipeline/review`
- Remove route: `/classify`

## Pages Removed

- `classify.html` — all functionality absorbed into Pipeline page Card 1
