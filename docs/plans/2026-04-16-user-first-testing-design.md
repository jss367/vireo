# User-First Testing — Design

**Status:** approved, ready for implementation
**Date:** 2026-04-16

## Motivation

Code-first review misses bugs that only show up when the app is actually used. A concrete example: `/static/help.js` and `/static/vendor/fuse.min.js` were missing from `vireo/static/` for weeks (commit `180750d`). The `<script>` tags in `_navbar.html` looked fine on review; the files 404'd on every page at runtime; the help modal silently did nothing. Found in minutes by running the app in a headless browser and collecting network errors.

Claude's typical workflow is code-first: read templates, check endpoints, assert "tests pass." That catches logic bugs but not missing-asset, missing-wire, missing-button bugs. A user-first test run actually drives the UI and reports what it observes.

## Goals

1. **Find bugs code review misses** — missing assets, broken JS wiring, missing DOM elements, network failures.
2. **Force verification before "done"** — any UI change requires a browser-driven run; no more "tests pass" without the feature having been exercised.
3. **Reduce real-data-loss risk** — the test harness must never touch `~/.vireo/` or the user's real photo library.
4. **Regression-proof by accretion** — every bug found becomes a scenario that runs forever.

## Non-goals

- Full end-to-end acceptance testing (out of scope; would need deterministic ML model).
- Cross-browser testing (Chromium only, sufficient for finding missing-asset / wiring bugs).
- Running in CI on every PR (deferred; goal is local-only first, promote later).

## Decisions

| # | Question | Choice | Reasoning |
|---|----------|--------|-----------|
| Q1 | Where does this live? | In-repo, local-only | Compounding value in-repo; CI deferred to avoid "flaky scenarios block merges" pain until scenarios are stable |
| Q2 | What DB does it run against? | Fixture DB per-scenario on a test profile | Originally proposed real-DB copy for sweep; revised to single test profile after Q3 raised real-data-loss risk |
| Q3 | Where do test photos live? | Local at `~/vireo-test-photos/` | One-developer, one-machine; can promote to NAS later |
| Q4 | Scenario scope for first cut? | Tier 1 + Tier 2 (9 scenarios) | Covers daily workflow + frequent flows; Tier 3 deferred |
| Q5 | Who builds the test dataset? | Script samples from real library + user adds photos later | Fast start; user supplements with known edge cases as needed |
| Q6 | When is this skill triggered? | Any UI change + before claiming UI work complete | Closes the "claimed done without using it" loop that motivated this work |

## Architecture

### Layout

```
vireo/testing/userfirst/
├── __init__.py
├── harness.py            # vireo_session() context manager
├── profile.py            # test profile path resolution + safety guard
├── sweep.py              # generic route-walker
├── scenarios/
│   ├── __init__.py
│   ├── browse.py         # Tier 1
│   ├── cull.py
│   ├── scan.py
│   ├── rate_flag.py
│   ├── pipeline_review.py
│   ├── keywords.py       # Tier 2
│   ├── workspaces.py
│   ├── duplicates.py
│   └── map_geo.py
├── seeds/
│   ├── realistic.sql     # for sweep mode
│   ├── keywords_basic.sql
│   └── (one per scenario as needed)
└── report.py             # finding formatter (markdown + screenshot paths)

scripts/
└── build_test_photos.py

tests/
└── test_userfirst_meta.py  # harness starts, sweep runs, safety guard rejects real paths
```

### Test profile

One env var drives everything: `VIREO_PROFILE=~/vireo-test-profile/`.

```
~/vireo-test-profile/
├── vireo.db
├── thumbnails/
├── labels/
├── config.json
└── runs/<run-id>/        # artifacts per run; auto-pruned to last 20
    ├── screens/
    ├── dumps/            # full-page HTML only on pages with findings
    ├── findings.json
    └── report.md
```

Photos live separately at `~/vireo-test-photos/` so the profile can be rebuilt without re-sampling photos.

### Safety invariant

Enforced in `harness.py` at startup, before Flask boots. **Any violation is a hard exit, not a warning.**

1. `VIREO_PROFILE` env var must be set; resolved path must not be under `~/.vireo/`.
2. DB path, thumb dir, labels dir, config path — every one must live under the resolved profile dir.
3. Every `workspace_folders` entry in the test DB must live under `~/vireo-test-photos/`.
4. Startup banner: `USER-FIRST TEST MODE — profile=X, photos=Y` so it's obvious in logs.

`build_test_photos.py` has a complementary guard: it only ever *reads* from the real library and only ever *writes* under `~/vireo-test-photos/` or `~/vireo-test-profile/`. Copies, never moves.

## Harness API

```python
from vireo.testing.userfirst import vireo_session

with vireo_session(seed="keywords_basic") as session:
    page = session.page            # Playwright page, listeners pre-wired
    session.goto("/keywords")      # wrapped; records timing + errors
    session.click("text=Location")
    session.screenshot("after-filter")
    rows = session.eval("document.querySelectorAll('.keyword-row').length")
    session.assert_that(rows > 0, "expected Location keywords visible")

# On exit: stops app, closes browser, writes findings + returns report object.
```

`vireo_session` responsibilities:

1. Validate safety invariant.
2. Reset test DB from `seeds/<seed>.sql`.
3. Start Flask on a free port, wait for `/api/health` = 200.
4. Launch Playwright Chromium; wire `console`, `requestfailed`, `response` listeners.
5. On exit: stop server, close browser, write `findings.json` + `report.md`, auto-prune old runs.

`session.assert_that(cond, msg)` is a **soft assert** — records a `[BUG]` finding but does not abort. One run can surface multiple problems.

## Scenario contract

One file per scenario, exports `run(session)`:

```python
# vireo/testing/userfirst/scenarios/keywords.py
def run(session):
    session.goto("/keywords")
    session.screenshot("initial")
    session.click('text="Add keyword"')
    session.fill("#keyword-name-input", "TestBird")
    session.click('text="Save"')
    session.wait_for_response("/api/keywords", method="POST")
    session.assert_that(
        session.page.locator("text=TestBird").is_visible(),
        "keyword should appear in list after save",
    )
```

Seeds live in `seeds/*.sql`: hand-written minimal SQL inserting only the state that scenario needs. Kept tiny and readable.

## Findings and report

### Finding types

- `[BUG]` — HTTP 4xx/5xx on same-origin requests, JS exceptions, failed assertions, missing DOM elements expected by scenario
- `[SUSPECT]` — observations worth raising but possibly real data (e.g., "0/707 photos have GPS"). Explicit: `session.flag("suspect", ...)`
- `[PERF]` — any request > 2s or page load > 5s
- `[WARN]` — console warnings, deprecated APIs

### Report format

Markdown summary pasted into chat; artifacts saved under `<profile>/runs/<run-id>/`:

```
## User-first run — scenario: keywords_add_and_filter
**Result:** 1 BUG, 0 SUSPECT, 0 PERF, 2 WARN
**Duration:** 4.2s
**Artifacts:** ~/vireo-test-profile/runs/20260416-143022/

### Findings
- [BUG] after clicking "Save", expected "TestBird" visible — not found
  (screenshot: 03-after-save.png, console: no errors, network: POST /api/keywords 200)
- [WARN] console: "Deprecated: event.keyCode" at /static/vireo-utils.js:42

### Steps
1. goto /keywords (200, 180ms)
2. click text="Add keyword"
3. fill #keyword-name-input "TestBird"
4. click text="Save"
5. wait POST /api/keywords → 200 (340ms)
6. assertion failed
```

## Activation workflow

Encoded in the skill doc at `.claude/skills/user-first-testing/SKILL.md`:

1. **Any time Claude edits** `vireo/templates/*.html`, `vireo/static/*`, or a route handler → after the edit, before claiming done, run **sweep** + the scenario(s) relevant to the changed page. Report attached to PR body or chat response.
2. **When the user says** "test X", "check Y", "find problems in Z" → run the relevant scenario or sweep.
3. **When a BUG finding appears in Claude's own run** → stop, report, fix in the same branch (don't silently paper over), re-run until green.
4. **Suspect findings must be independently reproduced** (curl / API call) before being reported as bugs. Hardened from today's session: the user's skepticism about the 404 forced a re-verification with curl; that verification should be mandatory, not optional.

## `build_test_photos.py`

Walks the real photo library, samples ~100 photos:

- 10 with GPS EXIF, 10 without (map/geo)
- 1 burst of 5 photos within 2-second window (cull)
- 2 exact duplicates, different filenames (duplicates resolver)
- 10 RAWs (CR2/NEF/ARW/DNG), 10 JPEGs (working-copy)
- 50 random across folders (pagination)

Writes `~/vireo-test-photos/MANIFEST.md` naming each photo's purpose. Idempotent — re-running skips files already present. User supplements with known edge cases as desired.

## Rollout plan

Each PR independently useful:

1. **PR #1 — Harness + safety + sweep.** `harness.py`, `profile.py`, `sweep.py`, `report.py`, `scripts/build_test_photos.py`, `test_userfirst_meta.py`. Once merged, Claude can use sweep mode to find bugs like today's 404.
2. **PR #2 — First 3 Tier-1 scenarios** (browse, cull, rate_flag). Forces the scenario contract + seed pattern to be real.
3. **PR #3 — Remaining 6 scenarios** (scan, pipeline_review, keywords, workspaces, duplicates, map_geo).
4. **PR #4 — Skill doc** at `.claude/skills/user-first-testing/SKILL.md`, finalized after 2-3 real UI changes have exercised the harness.

## Open questions (deferred)

- **Promotion to CI** — when and how. Requires Chromium in CI + scenarios stable enough that flakes don't block merges. Revisit after PR #3 merges and scenarios have run for a few weeks.
- **Test dataset on NAS** — promote from local when/if multi-machine becomes relevant.
- **Visual-diff testing** (screenshot comparison against a baseline) — considered overkill for now; would need a baseline-management workflow. Defer until sweep + scenarios stop finding the obvious bugs.
