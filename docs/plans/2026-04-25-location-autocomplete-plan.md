# Location Autocomplete Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Wire iNaturalist-style Google Places autocomplete into Vireo's location keyword flow, populate `keywords.latitude/longitude`, build a parent chain via `parent_id`, and surface coords on the existing Leaflet map.

**Architecture:** Hybrid client/server. Per-keystroke autocomplete uses the Google Maps JS Places library client-side with the API key embedded (referrer-restricted by the user in Google Cloud console). Place Details on user pick + reverse-geocode for EXIF go through Flask proxy endpoints (key in `~/.vireo/config.json`, never sent to browser). Reverse-geocode results are cached in SQLite by ~110m grid.

**Tech Stack:** Flask, Jinja2, vanilla JS, SQLite (no ORM, no migration system), Leaflet (existing), Google Maps Places (new), pytest + Playwright e2e.

**Design doc:** `docs/plans/2026-04-25-location-autocomplete-design.md`

---

## Conventions (read first)

- DB access in routes: `db = _get_db()` (`vireo/app.py:484`).
- JSON request body: `request.get_json(silent=True) or {}`.
- JSON errors: `return json_error("msg", status=400)` (`vireo/app.py:480`).
- JSON success: `return jsonify({...})`.
- Schema changes: no migrations system. New tables use `CREATE TABLE IF NOT EXISTS` in `Database._create_tables()`. New columns on existing tables use a guarded `ALTER TABLE ... ADD COLUMN` pattern (check `PRAGMA table_info(<table>)` first).
- Test harness: `tests/conftest.py` provides a `db` fixture (Database with tmp_path). API tests use the Flask test client. E2E uses `tests/e2e/` with Playwright + `live_server`.
- Pre-existing test failures on main as of 2026-04-22 should not block — see `~/.claude/projects/-Users-julius-git-vireo/memory/project_preexisting_test_failures.md`.

Each task ends with a commit. Frequent commits.

---

## Task 1: Schema — `place_id` column + unique index

**Files:**
- Modify: `vireo/db.py` (the `Database._create_tables()` method)
- Test: `vireo/tests/test_db.py`

**Step 1: Write the failing test**

Add to `vireo/tests/test_db.py`:
```python
def test_keywords_has_place_id_column_and_unique_index(db):
    cols = {row[1] for row in db._conn.execute("PRAGMA table_info(keywords)").fetchall()}
    assert "place_id" in cols

    db._conn.execute(
        "INSERT INTO keywords (name, type, place_id) VALUES (?, ?, ?)",
        ("Central Park", "location", "ChIJ_test_1"),
    )
    with pytest.raises(sqlite3.IntegrityError):
        db._conn.execute(
            "INSERT INTO keywords (name, type, place_id) VALUES (?, ?, ?)",
            ("Different Name", "location", "ChIJ_test_1"),
        )

    db._conn.execute(
        "INSERT INTO keywords (name, type, place_id) VALUES (?, ?, NULL)",
        ("free text 1", "location"),
    )
    db._conn.execute(
        "INSERT INTO keywords (name, type, place_id) VALUES (?, ?, NULL)",
        ("free text 2", "location"),
    )
```

**Step 2: Run test to verify it fails**

`python -m pytest vireo/tests/test_db.py::test_keywords_has_place_id_column_and_unique_index -v`
Expected: FAIL (column missing).

**Step 3: Implement**

In `Database._create_tables()` (after the `keywords` table is created), add a guarded column add and the partial unique index:
```python
cur.execute("PRAGMA table_info(keywords)")
kw_cols = {row[1] for row in cur.fetchall()}
if "place_id" not in kw_cols:
    cur.execute("ALTER TABLE keywords ADD COLUMN place_id TEXT")
cur.execute(
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_keywords_place_id "
    "ON keywords(place_id) WHERE place_id IS NOT NULL"
)
```

**Step 4: Re-run test → PASS**

**Step 5: Commit**

```bash
git add vireo/db.py vireo/tests/test_db.py
git commit -m "feat(db): add place_id column + unique index to keywords"
```

---

## Task 2: Schema — `place_reverse_geocode_cache` table

**Files:**
- Modify: `vireo/db.py`
- Test: `vireo/tests/test_db.py`

**Step 1: Failing test**

```python
def test_place_reverse_geocode_cache_table_exists(db):
    cols = {row[1] for row in db._conn.execute(
        "PRAGMA table_info(place_reverse_geocode_cache)"
    ).fetchall()}
    assert cols >= {"lat_grid", "lng_grid", "place_id", "response", "fetched_at"}
```

**Step 2: Run → FAIL**

**Step 3: Implement**

Add to `_create_tables()`:
```sql
CREATE TABLE IF NOT EXISTS place_reverse_geocode_cache (
    lat_grid    INTEGER NOT NULL,
    lng_grid    INTEGER NOT NULL,
    place_id    TEXT,
    response    TEXT NOT NULL,
    fetched_at  INTEGER NOT NULL,
    PRIMARY KEY (lat_grid, lng_grid)
)
```

**Step 4: Run → PASS**

**Step 5: Commit**
```bash
git commit -am "feat(db): add place_reverse_geocode_cache table"
```

---

## Task 3: Config — `google_maps_api_key`

**Files:**
- Modify: `vireo/config.py`
- Test: `vireo/tests/test_config.py`

**Step 1: Failing test**
```python
def test_google_maps_api_key_default_is_empty(tmp_path, monkeypatch):
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    assert cfg.load().get("google_maps_api_key") == ""
```

**Step 2: Run → FAIL**

**Step 3: Implement** — add `"google_maps_api_key": ""` to `DEFAULTS` in `vireo/config.py`.

**Step 4: Run → PASS**

**Step 5: Commit**
```bash
git commit -am "feat(config): add google_maps_api_key field"
```

---

## Task 4: Google Places API wrapper module

**Files:**
- Create: `vireo/places.py`
- Test: `vireo/tests/test_places.py`

**Goal:** Thin wrapper around Google's HTTP APIs. Two functions:
- `place_details(place_id, api_key) -> dict | None` — calls Place Details API; returns dict with `name`, `lat`, `lng`, `place_id`, `address_components` (list of `{place_id?, name, types[], short_name}`).
- `reverse_geocode(lat, lng, api_key) -> dict | None` — calls Geocoding API; returns same shape (or None for no match).

Use `urllib.request` (no new dependency). Endpoints:
- Place Details: `https://maps.googleapis.com/maps/api/place/details/json?place_id=...&key=...&fields=place_id,name,geometry/location,address_components`
- Reverse geocode: `https://maps.googleapis.com/maps/api/geocode/json?latlng=lat,lng&key=...`

**Step 1: Failing tests**

Mock `urllib.request.urlopen` with fixed JSON. Three tests:
1. `test_place_details_parses_response` — feed canned JSON, assert returned dict has expected keys.
2. `test_place_details_returns_none_on_zero_results` — Google returns `{"status": "ZERO_RESULTS"}` → `None`.
3. `test_reverse_geocode_parses_response` — same shape.

**Step 2: Run → FAIL** (`ModuleNotFoundError`).

**Step 3: Implement** `vireo/places.py` with the two functions + a small `_get_json(url)` helper. No retry, no caching at this layer.

**Step 4: Run → PASS**

**Step 5: Commit**
```bash
git add vireo/places.py vireo/tests/test_places.py
git commit -m "feat(places): add Google Places HTTP wrapper"
```

---

## Task 5: DB — place keyword upsert (parent chain)

**Files:**
- Modify: `vireo/db.py` (add `Database.upsert_place_chain`)
- Test: `vireo/tests/test_db.py`

**Goal:** Given a Place Details dict (from `places.place_details`), upsert the leaf keyword + a parent-chain of keywords from `address_components`. Returns the leaf keyword `id`.

**Behavior:**
- Walk `address_components` from broadest (`country`) → narrowest, building parents one at a time.
- For each level, dedupe by `place_id` if Google supplied one for the component, else by `(name, parent_id)`.
- Set `type='location'`, `latitude`/`longitude` (only on leaf for v1), `place_id` where available.
- Final leaf = the place itself (using top-level `name` + `place_id` + `geometry.location`).

**Step 1: Failing test**

Feed a canned Place Details dict (Central Park-style with country/state/city components). Assert:
- Leaf keyword exists with correct name, place_id, lat, lng, type='location'.
- Parent chain walks 4 levels (country → state → city → leaf).
- Calling twice returns the same leaf id (idempotent).

**Step 2: Run → FAIL**

**Step 3: Implement** the method. Helper `_upsert_one_keyword(name, parent_id, place_id) -> int` that does the conflict-tolerant insert.

**Step 4: Run → PASS**

**Step 5: Commit**
```bash
git commit -am "feat(db): add upsert_place_chain for Google Places"
```

---

## Task 6: DB — set/clear photo location, free-text location, link existing keyword

**Files:**
- Modify: `vireo/db.py`
- Test: `vireo/tests/test_db.py`

Three new methods:
- `set_photo_location(photo_id, leaf_keyword_id)` — removes any existing `type='location'` keyword links for the photo, inserts the new link.
- `clear_photo_location(photo_id)` — removes `type='location'` keyword links.
- `get_or_create_text_location(name) -> int` — finds or creates a `type='location'` keyword with no `place_id`, returns its id.
- `link_keyword_to_place(keyword_id, details)` — attaches `place_id`, coords, and parent chain to an existing keyword. Detects unique conflict on `place_id` and merges (re-points `photo_keywords` rows from old → canonical, deletes old).

**Step 1: Failing tests** — one per method. Cover the merge case for `link_keyword_to_place`.

**Step 2: Run → FAIL**

**Step 3: Implement**

**Step 4: Run → PASS**

**Step 5: Commit**
```bash
git commit -am "feat(db): photo location set/clear + keyword link-place"
```

---

## Task 7: DB — reverse-geocode cache get/put

**Files:**
- Modify: `vireo/db.py`
- Test: `vireo/tests/test_db.py`

Methods:
- `reverse_geocode_cache_get(lat, lng) -> dict | None` — looks up by rounded grid; returns `{place_id, response}` or `None`.
- `reverse_geocode_cache_put(lat, lng, place_id, response_json)` — upsert at grid cell.

Grid: `int(round(lat * 1000))`, `int(round(lng * 1000))`.

**Step 1-5:** Standard TDD. Test that put-then-get round-trips, that two coords in the same ~110m grid hit the same cell, that negative results (`place_id=None`) are also cached.

```bash
git commit -am "feat(db): reverse-geocode cache get/put"
```

---

## Task 8: API — `POST/DELETE /api/photos/<id>/location`

**Files:**
- Modify: `vireo/app.py`
- Test: `vireo/tests/test_photos_api.py` (or new `test_location_api.py`)

**Endpoints:**

`POST /api/photos/<int:photo_id>/location`:
1. Parse body `{place_id}`.
2. Read `google_maps_api_key` from config; if empty, return `json_error("no_api_key", 400)`.
3. `details = places.place_details(place_id, key)`; if `None`, return `json_error("place_not_found", 404)`.
4. `leaf_id = db.upsert_place_chain(details)`.
5. `db.set_photo_location(photo_id, leaf_id)`.
6. Return JSON: `{"location": {...rendered fields for the section UI...}}`.

`DELETE /api/photos/<int:photo_id>/location`:
1. `db.clear_photo_location(photo_id)`.
2. Return `{"ok": true}`.

**Step 1: Failing tests**

Use Flask test client. Mock `places.place_details` (monkeypatch). Two tests:
- POST with valid place_id → 200, photo has `type='location'` keyword link with correct lat/lng.
- DELETE → 200, no `type='location'` keyword links remain.
- POST with empty config key → 400 with `error: "no_api_key"`.

**Step 2-4: Standard TDD.**

**Step 5: Commit**
```bash
git commit -am "feat(api): POST/DELETE photo location"
```

---

## Task 9: API — `POST /api/photos/<id>/location/text`

**Files:**
- Modify: `vireo/app.py`
- Test: same test file as Task 8

Body: `{name}`. Path: free-text fallback. No Google call.

1. `leaf_id = db.get_or_create_text_location(name)`.
2. `db.set_photo_location(photo_id, leaf_id)`.
3. Return `{"location": {...}}`.

**TDD as above.** Commit:
```bash
git commit -am "feat(api): POST photo location text fallback"
```

---

## Task 10: API — `GET /api/places/reverse-geocode`

**Files:**
- Modify: `vireo/app.py`
- Test: same file

Query params: `lat`, `lng`. Behavior:
1. Validate floats.
2. `cached = db.reverse_geocode_cache_get(lat, lng)`. If hit, return `{place_id, summary}` (summary derived from cached `response`).
3. Miss: read API key from config; if empty, return cached miss as `{place_id: null}` (don't 400 — degrade gracefully so the UI can just hide the suggestion).
4. `details = places.reverse_geocode(lat, lng, key)`.
5. `db.reverse_geocode_cache_put(lat, lng, details and details["place_id"], json.dumps(details or {}))`.
6. Return `{place_id, summary}` (or `{place_id: null}` if Google had no match).

**Summary string** — derived from `details`: leaf name + first 2 parent names. Helper `_summarize_details(details) -> str`.

**TDD:** monkeypatch `places.reverse_geocode`. Cache hit, cache miss, no-API-key paths.

```bash
git commit -am "feat(api): reverse-geocode proxy with SQLite cache"
```

---

## Task 11: API — `POST /api/keywords/<id>/link-place`

**Files:**
- Modify: `vireo/app.py`
- Test: same file

Body: `{place_id}`. Steps:
1. Read API key; empty → 400.
2. `details = places.place_details(place_id, key)`; None → 404.
3. `result = db.link_keyword_to_place(keyword_id, details)`. Returns `{keyword_id, merged: bool}` (merged=True means an existing place_id-bearing keyword absorbed the link target).
4. Return JSON with the resulting keyword fields.

**TDD:** test the link, test the merge case. Commit:
```bash
git commit -am "feat(api): link existing keyword to Google place"
```

---

## Task 12: UI — Settings page Google Maps key input

**Files:**
- Modify: `vireo/templates/settings.html`
- Test: `tests/e2e/test_settings.py` (extend if exists, else create)

**Steps:**
- Add a section "Google Maps" with a password-style input bound to `cfg.google_maps_api_key`. Same wiring pattern as existing `inat_token` (read on render, POST on save to `/api/config`).
- One-line help: "Used for location autocomplete. Add an HTTP referrer restriction in Google Cloud console (e.g. `localhost:8080/*`)."

**Manual verification (no e2e required for v1):**
1. `python vireo/app.py --db ~/.vireo/vireo.db --port 8080`
2. Open http://localhost:8080/settings.
3. Enter a key, save, reload. Confirm it persists in `~/.vireo/config.json`.

```bash
git commit -am "feat(settings): add Google Maps API key input"
```

---

## Task 13: UI — Location section in photo detail panel

**Files:**
- Modify: `vireo/templates/browse.html`

This is the biggest UI task. Break it into substeps with a single commit at the end.

**Substep 13.1: HTML scaffold**

In the photo detail panel (above or just below the Keywords pill area), add:
```html
<div class="location-section">
  <div class="location-label">Location</div>
  <div id="locationFilled" hidden></div>
  <div id="locationEmpty">
    <input id="locationInput" class="add-kw-input" placeholder="📍 Add location..." />
    <div id="locationExifSuggestion" hidden></div>
  </div>
</div>
```

**Substep 13.2: Lazy-load Google Maps JS**

Helper `loadGoogleMapsJs()` that, on first call, injects `<script src="https://maps.googleapis.com/maps/api/js?key={{ google_maps_api_key }}&libraries=places&loading=async">` and resolves a Promise when ready. Memoize. Pass the API key from the route into the template context (extend the existing browse route in `app.py` to include `google_maps_api_key=cfg.load().get('google_maps_api_key', '')`).

If the key is empty, skip loading and leave the input as plain text (Enter creates a free-text location).

**Substep 13.3: Bind autocomplete on focus**

When `locationInput` first gains focus, call `loadGoogleMapsJs()`, then `new google.maps.places.Autocomplete(input, {types: ['geocode', 'establishment']})`. On `place_changed` event, grab `place.place_id`, POST to `/api/photos/<id>/location`, render filled state from response.

**Substep 13.4: Free-text Enter fallback**

If user hits Enter without picking from the dropdown (`autocomplete.getPlace().place_id` is undefined or input value doesn't match a picked place), POST to `/api/photos/<id>/location/text` with `{name: input.value}`.

**Substep 13.5: Filled state**

Renders leaf name (bold) + parent breadcrumbs (muted, `·` separated) + clear button (×). Clear → DELETE `/api/photos/<id>/location` → swap back to empty state.

**Substep 13.6: Render server-side on initial load**

When the photo detail panel renders for a photo that already has a `type='location'` keyword link, server-side resolve the leaf + parent chain and pre-render the filled state (no client roundtrip needed).

**Manual test:**
1. Run app, set API key in Settings.
2. Open a photo, focus Location input, type "Central". Confirm Google dropdown appears.
3. Pick "Central Park, NY". Confirm filled state renders with breadcrumbs.
4. Reload the page; confirm location is still filled.
5. Click ×; confirm cleared.
6. Type "the dog park behind my house" + Enter; confirm a free-text location is created (no breadcrumbs).
7. Clear the API key; confirm autocomplete is gone but free-text + Enter still works.

```bash
git commit -am "feat(ui): add Location section with Places autocomplete"
```

---

## Task 14: UI — EXIF GPS suggest-on-open

**Files:**
- Modify: `vireo/templates/browse.html`

When opening a photo:
- If photo has `latitude/longitude` (EXIF) AND no `type='location'` keyword link AND `google_maps_api_key` is set:
  - Fire `GET /api/places/reverse-geocode?lat=...&lng=...`.
  - On `{place_id, summary}`, render `<div id="locationExifSuggestion">💡 EXIF says: {summary} <button>Accept</button></div>` below the input.
  - On Accept click, POST `/api/photos/<id>/location` with that `place_id` (same path as autocomplete pick).
  - On `{place_id: null}`, hide the suggestion silently.

**Manual test:**
- Open a GPS-tagged photo with no location keyword. Confirm suggestion appears within ~1s.
- Click Accept. Confirm location is filled, suggestion disappears.
- Re-open the same photo. Confirm no suggestion (already has location).

```bash
git commit -am "feat(ui): EXIF reverse-geocode suggest-on-open"
```

---

## Task 15: UI — `keywords.html` Link-to-Google button

**Files:**
- Modify: `vireo/templates/keywords.html`

For each row where `type='location' AND place_id IS NULL`, render a `📍 Link…` button. Click opens a small modal:
- Single autocomplete input (same `places.Autocomplete` widget; reuse `loadGoogleMapsJs()` from browse.html or extract to a small shared `static/js/places.js`).
- Pre-fill input value with the keyword's current name.
- On pick → `POST /api/keywords/<id>/link-place` with `place_id` → close modal, re-render row with breadcrumb + linked badge.
- If response says `merged: true`, show a small toast: "Merged into existing 'Central Park'".

Add a filter chip "Locations · N unlinked" near the existing keyword filters.

**Manual test:**
- Create a free-text location keyword via the existing keyword flow.
- Visit /keywords, find it, click Link, pick a Google match.
- Confirm coords + breadcrumb appear, button disappears.
- Confirm photos previously tagged with that keyword now show on /map.

```bash
git commit -am "feat(ui): link existing location keywords to Google places"
```

---

## Task 16: Map — fall back to keyword coords + provenance

**Files:**
- Modify: `vireo/db.py` (`get_geolocated_photos`)
- Modify: `vireo/templates/map.html`
- Test: `vireo/tests/test_db.py`

**DB change:** Modify `get_geolocated_photos` to also return photos that lack EXIF coords but have a `type='location'` keyword with non-null lat/lng. Use a `COALESCE` pattern with a subquery:

```sql
SELECT
  p.id,
  COALESCE(p.latitude, kl.latitude)  AS latitude,
  COALESCE(p.longitude, kl.longitude) AS longitude,
  CASE WHEN p.latitude IS NOT NULL THEN 'exif' ELSE 'keyword' END AS coord_source,
  kl.name AS keyword_location_name,
  ...
FROM photos p
LEFT JOIN (
  SELECT pk.photo_id, k.latitude, k.longitude, k.name, k.id,
         ROW_NUMBER() OVER (PARTITION BY pk.photo_id ORDER BY k.parent_id IS NULL, k.id) AS rn
  FROM photo_keywords pk
  JOIN keywords k ON k.id = pk.keyword_id
  WHERE k.type='location' AND k.latitude IS NOT NULL
) kl ON kl.photo_id = p.id AND kl.rn = 1
WHERE COALESCE(p.latitude, kl.latitude) IS NOT NULL
  AND COALESCE(p.longitude, kl.longitude) IS NOT NULL
  AND ...workspace/folder filters...
```

(Choosing leaf via `parent_id IS NULL` proxy isn't perfect; for v1, picking the lowest-id row with non-null coords is acceptable. A cleaner "deepest in chain" picker can come in v1.5.)

**Step 1: Failing test** — insert a photo with no EXIF GPS but with a `type='location'` keyword that has coords; assert `get_geolocated_photos` returns it with `coord_source='keyword'`.

**Step 2-4: Standard TDD.**

**map.html change:** Read `coord_source` and `keyword_location_name` from the marker payload. Append a one-line provenance footer to the popup: `📍 from EXIF GPS` or `📍 from location: {name}`.

**Manual test:**
- Tag a non-GPS photo with a location keyword that has coords.
- Open /map; confirm the photo appears at the keyword's coords with the right provenance footer.

**Step 5: Commit**
```bash
git commit -am "feat(map): fall back to keyword coords + provenance footer"
```

---

## Task 17: Final integration test pass

Run the full test suite per `CLAUDE.md`:

```bash
python -m pytest tests/test_workspaces.py vireo/tests/test_db.py vireo/tests/test_app.py vireo/tests/test_photos_api.py vireo/tests/test_edits_api.py vireo/tests/test_jobs_api.py vireo/tests/test_darktable_api.py vireo/tests/test_config.py -v
```

Manual end-to-end smoke test:
1. Start app, set API key in Settings.
2. Open a photo with EXIF GPS, accept the suggestion. Confirm location fills.
3. Open a photo without EXIF GPS, type a location, pick from dropdown. Confirm location fills + breadcrumbs render.
4. Open a photo without EXIF GPS, type a free-text location, hit Enter. Confirm it's created without coords.
5. Visit /keywords, link a free-text location to Google. Confirm coords + breadcrumb appear.
6. Visit /map, confirm both EXIF-source and keyword-source markers appear with correct provenance.
7. Clear the key in Settings, reload a photo, confirm autocomplete gracefully degrades (free-text + Enter still works).
8. Open /map, /keywords, /settings — no console errors.

If everything passes:
```bash
gh pr create --base main --title "feat: location autocomplete with Google Places" --body "$(cat <<'EOF'
## Summary
- Adds iNaturalist-style Google Places autocomplete on the location keyword.
- Single-leaf-with-parent-chain data model; `keywords.place_id` column dedupes Google places.
- Hybrid client/server: JS Places library client-side; Place Details + reverse-geocode (with SQLite cache) server-side.
- EXIF GPS reverse-geocode "suggest on open" with Accept button.
- Existing free-text location keywords get a "📍 Link..." button on `/keywords` to attach Google data.
- `/map` falls back to keyword coords for photos without EXIF GPS.
- Graceful degrade when no API key configured.

Design: `docs/plans/2026-04-25-location-autocomplete-design.md`
Plan: `docs/plans/2026-04-25-location-autocomplete-plan.md`

## Test plan
- [ ] Unit: db schema, places wrapper, photo location set/clear, keyword link-place, reverse-geocode cache
- [ ] API: POST/DELETE photo location, free-text fallback, reverse-geocode proxy, link-keyword
- [ ] Manual: 8-step smoke test in plan Task 17
EOF
)"
```

---

## Task ordering / dependencies

- Tasks 1-3 are independent and can be done in any order (but commit each separately).
- Task 4 depends on nothing (pure module).
- Tasks 5-7 depend on Task 1 (`place_id` column).
- Tasks 8-11 depend on 5-7 + 4.
- Task 12 is independent (just a settings input).
- Tasks 13-15 depend on 8-11 (UI talks to API).
- Task 16 depends on 5-7 (data has to exist before map can render it).
- Task 17 is final.

Recommended order: 1 → 2 → 3 → 4 → 5 → 6 → 7 → 8 → 9 → 10 → 11 → 12 → 13 → 14 → 15 → 16 → 17.
