# Mask-based local adjustments (subject / background)

## Motivation

Vireo already knows where the bird is — MegaDetector boxes prompt SAM masks
that exist for most keeper photos. No generic editor has that. Local
adjustments turn it into the editor's differentiating feature: brighten and
sharpen the subject, quiet down the background, one slider each, no manual
brushing.

## What exists (facts the design builds on)

- `photo_masks`: one row per (photo, variant); PNG single-channel masks
  (0/255) on disk at `~/.vireo/masks/{photo_id}.{variant}.png`, generated at
  working resolution in the **orientation-corrected working image space**
  — the pixel space of `image_loader.load_image` / `render_proxy` output,
  which applies `ImageOps.exif_transpose` (both standard images and RAW
  postprocess), before any recipe geometry (rotation/flip/straighten/crop)
  is applied. The prompt detection box is stored per mask;
  `photos.active_mask_variant` picks the live one.
- RAW files have **two** decode modes with potentially different pixel
  dimensions and aspect ratios: `RAW_DECODE_JPEG_FIRST` (default —
  embedded camera JPEG when it satisfies the requested size) and
  `RAW_DECODE_PRESERVE_HIGHLIGHTS` (libraw demosaic with auto-bright off,
  used by every edit-quality path — thumbnails, previews, exports — see
  `image_loader.py:19` and `export.py:303`). Detection and mask
  extraction (`detector.py:330`, `masking.py:498`) go through the default
  `load_image`, so today's `photo_masks` for RAWs are typically in
  JPEG-first space; edit-render sources for the same photos are in
  preserve-highlights space. On RAWs where the embedded JPEG differs from
  the demosaiced output (different long-edge size, cropped-to-16:9
  companion, etc.) the two spaces do not agree pixel-for-pixel.
- Masks regenerate when the detection prompt changes and can be deleted by
  storage cleanup (`delete_stale_masks`, variant deletion). Detection IDs are
  not stable across detector re-runs.
- The lightbox mask overlay currently hides itself when a recipe has
  geometry because the raw mask no longer lines up — a display-layer
  workaround this feature replaces with a real transform.
- The tone pipeline (`tone.py`) is strictly per-pixel; the detail pass
  (`detail.py`) is neighborhood-based, runs last at output resolution with
  scale-adjusted kernels.

## Design

### Recipe schema

New optional `local` section — a single object carrying the shared mask
reference plus an array of region entries (v1 supports at most one `subject`
and one `background` entry):

```json
"local": {
  "mask": {
    "ref": "a1b2c3d4",
    "source_digest": "e7b8f2c1",
    "decodes": [
      {"mode": "preserve_highlights", "long_edge": 3600},
      {"mode": "standard", "long_edge": 3200}
    ],
    "feather": 12.0
  },
  "regions": [
    {
      "region": "subject",
      "adjustments": {"exposure": 0.6, "shadows": 25, "sharpen": 30}
    },
    {
      "region": "background",
      "adjustments": {"exposure": -0.4, "saturation": -15, "noise_reduction": 40}
    }
  ]
}
```

- The mask reference lives at `local.mask`, not per region — subject and
  background always share one snapshot (background is its inverse), and
  hoisting the ref means a **background-only** recipe still records the
  snapshot needed for deterministic renders, stale-mask comparison, and
  snapshot GC (no fall-through to the live active mask). Feather is a
  Gaussian softening radius in **native pixels**, scaled at render time like
  detail kernels.
- `mask.source_digest` is a hash over the source `photo_masks` row's bytes
  plus its stored detection prompt and detector version — the **inputs**
  the snapshot was derived from, not the transformed snapshot pixels. This
  is what staleness compares against (see below), so a RAW snapshot in
  preserve-highlights space is not flagged stale just because it does not
  byte-match its JPEG-first source mask.
- `mask.decodes` is a list of the pixel-space bases the snapshot was built
  in — one entry per on-disk variant, each with `mode` (`standard` /
  `preserve_highlights`) and `long_edge` in native pixels. Non-RAWs load in
  standard space only, so a single `standard` entry. RAWs whose edit path
  is preserve-highlights get a `preserve_highlights` entry; RAWs that also
  have a companion JPEG (and can therefore fall back to standard decode at
  render time — see §Snapshot decode basis) get both entries. The
  weight-map builder picks the entry whose `mode` matches the current
  render source's decode; it is not a requirement that the entry's
  `long_edge` equal the render source's pixel dimensions.
- `local` is dropped from normalization entirely when `regions` is empty or
  every region normalizes away — the shared `mask` never persists without at
  least one active region referencing it.
- Allowed per-region adjustments (v1): `exposure`, `highlights`, `shadows`,
  `contrast`, `saturation`, plus detail's `sharpen`/`sharpen_radius`/
  `noise_reduction`. Same ranges and normalization rules as global
  adjustments; zero values normalize away, empty entries are dropped.

### Mask snapshots, not live references

The recipe's `local.mask.ref` points to a **content-addressed snapshot**
materialized under `<db_dir>/edit-masks/{photo_id}.{sha1[:12]}.{decode}.png`
at the moment the first local adjustment is added — where `<db_dir>` is
`dirname(--db)`, the same root as the existing `<db_dir>/masks/` store, and
`{decode}` selects among the on-disk variants enumerated in
`mask.decodes` (see below). Vireo supports arbitrary `--db` paths, so a
global `~/.vireo/edit-masks/` keyed only on `photo_id` would collide
across separate databases (tests, alternate libraries) and let one
database's snapshot GC delete another's referenced snapshots.

**Snapshot decode basis matches the edit-render source.** The snapshot has
to line up with whichever pixel grid the render source is actually
delivering, and for RAW photos that isn't a single grid.
`recipe_render_source` (`vireo/render_source.py:312-335`) and
`export.py:327-343` both fall back to the companion JPEG when the RAW is
missing or its decode fails; and `recipe_render_source` has a further
last-resort fallback (`vireo/render_source.py:331-334`) that returns the
photo's `working_copy_path` — a pre-extracted JPEG under `<vireo_dir>` —
when the RAW original is offline *and* no companion is usable. A single
RAW photo can therefore render as preserve-highlights on one recipe
render, as standard-decode companion JPEG on another, and as a
standard-decode working-copy JPEG on a third. Silently disabling local
adjustments on those fallback renders would drop user edits on
first-class supported paths.

The working-copy JPEG collapses back to the preserve-highlights basis on
its own: `extract_working_copy` (`vireo/image_loader.py:511-537`) decodes
the source with `RAW_DECODE_PRESERVE_HIGHLIGHTS` before saving the JPEG,
so its pixel grid (aspect, orientation, cropping) is the same basis as
the preserve-highlights variant — only its `long_edge` may be smaller,
which the builder resamples through anyway. That means the working-copy
fallback does not need its own on-disk variant; it reuses
`preserve_highlights`.

Snapshot creation therefore materializes one on-disk variant per
**distinct decode basis** the render source might use, keyed by decode
mode, all sharing one `local.mask.ref`:

- **Non-RAWs:** one variant, `standard`, copied as-is from the existing
  `photo_masks` file (which is already in edit-render space).
- **RAWs whose folder has no companion JPEG:** one variant,
  `preserve_highlights`, generated fresh — it serves both the primary
  preserve-highlights RAW render and the working-copy fallback, whose
  JPEG was itself extracted in preserve-highlights space.
- **RAWs with a companion JPEG:** two variants —
  `preserve_highlights` (for the primary RAW path *and* the working-copy
  fallback) *and* `standard` (for the companion-fallback path).

For each variant, two things have to be right for alignment with the
render load:

- **Space of the mask itself.** The variant is rendered from a `load_image`
  in that decode mode (`raw_decode=RAW_DECODE_PRESERVE_HIGHLIGHTS` for the
  preserve-highlights variant; the default standard decode for the
  standard variant, loading the companion JPEG for RAW+companion pairs),
  so its pixel grid (dimensions, aspect, orientation) is the same basis
  as any edit render that comes back on the same decode.
- **Space of the prompt that produced it.** The `photo_masks` row stores a
  **normalized** detection bbox that was generated against the JPEG-first
  proxy (that's the space MegaDetector ran in for RAWs today, since
  `detector.py` goes through the default `load_image`). Reusing that
  normalized box directly in a different decode is unsafe when the two
  decodes have different aspects or embedded-JPEG crops — the same
  `{x, y, w, h}` in [0,1] points at a physically different region of the
  scene. Snapshot creation re-runs detection (MegaDetector → SAM) on each
  variant's own load and uses the resulting native box as SAM's prompt; the
  stored `photo_masks` prompt is only carried forward when the variant's
  decode dimensions and aspect agree with the JPEG-first proxy's.

On disk the variants live alongside each other under the shared ref, e.g.
`<db_dir>/edit-masks/{photo_id}.{sha1[:12]}.preserve_highlights.png` and
`<db_dir>/edit-masks/{photo_id}.{sha1[:12]}.standard.png`; `local.mask.decodes`
enumerates which variants exist for that ref.

Cost is a few seconds of one-time work per (photo, mask content, decode),
amortized across every subsequent edit render of that recipe — doubled for
RAW+companion, still one-shot. In exchange the snapshot is by construction
the same decode basis as edit renders (including fallback renders) — no
per-recipe alignment metadata to keep in sync with future decode changes,
no runtime aspect fix-ups. `mask.decodes[i].long_edge` is basis metadata,
**not** a size gate. Renders happen at many sizes (working-resolution
previews, thumbnails, full-res exports), and the weight-map builder
resamples the picked variant to whatever the current render source is (see
§Rendering); a variant whose `long_edge` differs from the current render
source is normal and not an error. The pass is only disabled when no
`mask.decodes` entry matches the current render source's decode mode
(which would indicate an actual basis change — e.g., a future decode mode
added after the snapshot was taken), with the same "warn + disable both
regions" failure mode as a missing snapshot.

Why not reference the live active mask: renders must stay a deterministic
function of (source pixels, recipe). Preview/thumbnail caches invalidate on
recipe change and `EDIT_MATH_VERSION` bumps only — a mask silently
regenerating under a live reference would change render output with no cache
invalidation and no user-visible cause, violating the no-black-boxes rule.

Staleness is surfaced, not automated, and it's compared on **source-side
inputs**, not on the snapshot pixels. When a snapshot is created, the
recipe records `mask.source_digest` — a hash over the current
`photos.active_mask_variant` row's source mask file bytes, its stored
detection prompt, and the detector version that produced it. Staleness
means the current active mask's re-computed digest differs from
`mask.source_digest`; that's the signal shown as "Newer subject mask
available — Update", and Update rewrites `local.mask.ref` and
`mask.source_digest` together (a normal recipe edit: undoable,
cache-invalidating). This is deliberately not a byte comparison against
the snapshot: for RAWs the snapshot pixels come from a re-detection in
preserve-highlights space and will never byte-match the JPEG-first
`photo_masks.path` file even when nothing about the underlying detection
has changed, so a snapshot-byte check would report every RAW snapshot as
stale on first render and after every Update. If the snapshot file is
missing entirely, the renderer **disables the whole local pass** for
that photo — both subject *and* background weights held at zero, so no
region's adjustments apply — and the editor shows a warning. (A no-op /
all-zero subject mask would leave background weight = `1 − 0` = 1
everywhere, silently applying background edits to the entire frame;
disabling both regions keeps missing snapshots from turning into a
whole-photo edit.)

Snapshot lifecycle: created on first use, garbage-collected when no recipe
(current or in edit history) references them — same sweep style as other
storage cleanups, surfaced in `/api/storage`.

### Rendering

Order stays geometry → tone → resize → detail, with local weights woven in.
Tone runs at pre-resize working-image dimensions inside `apply_recipe`, then
`apply_recipe_to_loaded_image` calls `thumbnail(max_size)`, then detail runs
at the post-resize render size — so a single weight map cannot serve both
passes. The weight map is built once and materialized at both scales:

1. **Weight map.** Pick the `mask.decodes` entry whose `mode` matches the
   current render source's **effective decode basis** — `preserve_highlights`
   when the RAW decoded successfully *or* when the render fell back to the
   working-copy JPEG (`extract_working_copy` decodes with
   `RAW_DECODE_PRESERVE_HIGHLIGHTS`, so its pixel grid shares the
   preserve-highlights basis); `standard` when the render fell back to the
   companion JPEG or the photo is non-RAW. `recipe_render_source` already
   distinguishes these paths internally, so it returns the effective basis
   alongside the source path for the weight-map builder to consume. Load
   the picked variant's on-disk file and bilinearly resample it to the
   current render source's pre-geometry pixel dimensions (the entry's
   stored `long_edge` is a basis marker, not a size gate — a full-res
   export and a working-resolution preview both resample the same variant
   to their own source size). Then apply the recipe's geometry
   (rotation/flip/straighten/crop — same transforms as the image, bilinear).
   From that geometry-transformed mask, materialize two aligned copies with
   feather applied at each target scale (Gaussian radius scaled the same way
   as detail kernels), normalize to [0,1]:
   - **Tone weight** — sized to the pre-resize working image, consumed by
     the tone pass inside `apply_recipe`.
   - **Detail weight** — sized to the post-`thumbnail` render size, consumed
     by the detail pass.

   Background weight = 1 − subject weight, computed independently at each
   scale so tone and detail each see aligned subject/background pairs.
2. **Local tone** runs inside the existing tone pass with per-pixel strength:
   e.g. exposure becomes `lin * 2^(ev_global + ev_subject·w + ev_bg·(1−w))`;
   range/saturation controls interpolate their amounts by `w` the same way.
   This keeps every op per-pixel — it adds a weight *input*, not a
   neighborhood — so the WebGL live preview can adopt it later by sampling
   the mask as a second texture (out of scope for v1; the lightbox preview
   approximates and snaps to the server render, as it already does for
   detail and re-edits).
3. **Local detail** runs the existing detail pass **twice** on the
   pre-detail image — once with the **subject branch** parameters, once
   with the **background branch** — and blends the two outputs by the
   detail weight map: `out = subject_out · w + background_out · (1 − w)`.
   Each branch's parameters are `global + region_delta` per field:
   `sharpen = global.sharpen + subject.sharpen`, `noise_reduction =
   global.noise_reduction + subject.noise_reduction`, and the same for
   background — mirroring the tone contract (`ev_global + ev_subject·w
   + ev_bg·(1−w)`) so local sliders read as deltas on top of the
   whole-photo baseline. `sharpen_radius` prefers the region's value when
   set, else falls back to the global (radius is a kernel size, not a
   strength, so add-on semantics don't apply). All resulting scalars pass
   through the same clamp/range as normal global detail. When the recipe
   has **no** `local` block or every region normalizes away, the
   whole-photo detail pass runs exactly as today (byte-identical — that's
   what keeps `EDIT_MATH_VERSION` unbumped). A single pass with combined
   scalars cannot represent both regions when `local` is present:
   `_run_detail` in `detail.py` applies NR to `out` before sharpen reads
   it, so subject-sharpen + background-NR would noise-reduce the subject
   before its sharpen delta (or drop one region's setting). Optimization:
   if both branches resolve to identical scalars (e.g., both regions have
   zero deltas, or one region's zeros produce the same `global +
   delta` as the other's), one pass runs and its output is used for both
   sides of the blend — the blend then simplifies to that single output,
   matching the global-only path.

No `EDIT_MATH_VERSION` bump: recipes without `local` render byte-identically.

### Editor UI

- Two new bands, **Subject** and **Background**, shown only when the photo
  has a usable mask; otherwise one honest line ("No subject mask — run the
  pipeline's mask stage") instead of dead sliders.
- Each band: its allowed sliders + a shared **Feather** slider on the
  Subject band. A small overlay toggle on the preview shows exactly which
  pixels count as subject — the transparency rule applied to masks — but
  served from a **new** `/api/local-mask/<pid>/preview.png` endpoint that
  returns the recipe's weight map (snapshot at `local.mask.ref` → recipe
  geometry → feather → preview size), the same uncropped weight map used
  by the editor preview (see §Editor preview). Reusing the existing
  `/api/masks/<pid>/<variant>.png` would serve the live
  `photo_masks.path` unchanged, so the overlay would disagree with the
  saved edit exactly when the recipe has rotation, crop, feather, or a
  stale active mask — the cases this feature exists to fix.
- Stale-snapshot banner with an explicit Update action as above.
- Copy Settings / presets: local adjustments are **not** included in presets
  (they reference a photo-specific mask); Copy Settings to a group copies
  the local slider values and re-snapshots each target photo's own active
  mask, skipping (and reporting) photos without one.

### Editor preview

The editor's server renders pick up local adjustments automatically (they
render the full recipe). The uncropped editor preview uses the
geometry-transformed-but-uncropped weight map, consistent with how crop is
previewed today.

## Phasing

1. **PR 1 — renderer + schema.** `local` normalization, mask snapshot
   plumbing + GC, weight-map builder, weighted tone + detail, API round-trip.
   Fully testable without UI (synthetic masks in tests).
2. **PR 2 — editor UI.** Subject/Background bands, feather, overlay toggle,
   stale-mask update flow, copy-to-group behavior.
3. **PR 3 (optional) — live preview.** Mask texture in the lightbox WebGL
   shader for instant local-tone feedback.

## Open questions (decide during PR 1)

- Feather default: start at ~2% of the long edge, clamp to [0, 60px] native?
- Should `background` NR default on softly when the subject is sharp and the
  background is high-ISO mush? (Tempting, but silent defaults fight the
  transparency rule — v1 says no.)
- Mask resolution: snapshots are working-resolution; is bilinear upscale to
  full-res export acceptable at feather radii we allow? (Expected yes — the
  feather blur dominates interpolation error.)
