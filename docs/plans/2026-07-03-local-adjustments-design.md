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
  `photos.active_mask_variant` picks the live one. **This describes only
  the mask's source-side space**, not the space the renderer aligns
  against — for RAWs, the working image space is the *default*
  (JPEG-first) `load_image` output, which does not agree pixel-for-pixel
  with the preserve-highlights demosaic that edited previews and exports
  actually load. Aspect and long-edge can differ, and on cameras whose
  embedded JPEG is cropped relative to the sensor they differ every
  time. Orientation-corrected space alone is therefore *not* enough of a
  contract to align a mask against an edit render, because for RAWs the
  specific decode mode that produced this pixel grid matters — see next
  bullet — and the two RAW decodes can differ in aspect and long-edge.
  The design therefore does not store a single space-agnostic snapshot:
  instead it materializes one snapshot variant per decode mode the
  render source can actually deliver, each recorded with its own `mode`
  and `long_edge` in `local.mask.decodes` and **generated from a
  `load_image` in that same decode mode as the edit render that will
  consume it** — the preserve-highlights variant is produced by a
  `RAW_DECODE_PRESERVE_HIGHLIGHTS` load of the RAW, not by transforming
  the JPEG-first `photo_masks` file (§Snapshot decode basis). The
  weight-map builder picks the entry whose `mode` matches the render
  source's effective decode, so a snapshot is by construction the same
  pixel-space basis as the edit render that consumes it — no separate
  alignment metadata or transform is needed on top.
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
    "decodes": [
      {"mode": "preserve_highlights", "long_edge": 3600, "source_digest": "e7b8f2c1"},
      {"mode": "standard",            "long_edge": 3200, "source_digest": "9d4c60ab"}
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
- `mask.decodes` is a list of the pixel-space bases the snapshot was built
  in — one entry per on-disk variant, each with `mode` (`standard` /
  `preserve_highlights`), `long_edge` in native pixels, and a
  **per-variant** `source_digest`. Non-RAWs load in standard space only, so
  a single `standard` entry. RAWs whose edit path is preserve-highlights
  get a `preserve_highlights` entry; RAWs that also have a companion JPEG
  (and can therefore fall back to standard decode at render time — see
  §Snapshot decode basis) get both entries. The weight-map builder picks
  the entry whose `mode` matches the current render source's decode; it is
  not a requirement that the entry's `long_edge` equal the render source's
  pixel dimensions.
- Each entry's `source_digest` is a hash over the **inputs** that
  variant was derived from — not the transformed snapshot pixels — so a
  RAW snapshot in preserve-highlights space is not flagged stale just
  because it does not byte-match its JPEG-first source mask. Digests are
  per-variant because a RAW+JPEG pair's two variants have different input
  sources: the `preserve_highlights` variant re-runs detection on the RAW's
  demosaic and hashes the `photos.active_mask_variant` row's file bytes +
  stored prompt + detector version + **the active SAM mask variant/model
  name** (`photos.active_mask_variant` — the name of the SAM model that
  produced the segmentation, e.g. `sam2-small` vs `sam2-large`) + **the
  RAW source file's identity** (`photos.file_hash`, which the scanner
  already maintains as mtime + size + sha1 over the primary file) + **the
  preserve-space detection prompt that actually produced this snapshot**
  (the re-detection MegaDetector ran on the preserve-highlights load, not
  the JPEG-first source mask's stored prompt); the `standard` variant
  re-runs detection on the companion JPEG and hashes the **companion JPEG
  bytes** (via mtime + size + sha1 over the companion file) + the
  re-detection's own prompt bytes + detector version + **the same active
  SAM variant/model name** used to segment its own re-run.
  Both digests include the SAM variant because switching
  `photos.active_mask_variant` (e.g., `sam2-small` → `sam2-large`) changes
  the segmentation shape even when the source image bytes and the
  MegaDetector prompt are unchanged; omitting it from the standard digest
  would let companion/working-copy fallback renders keep using the old SAM
  segmentation with no stale banner. A single top-level digest could not
  detect the case where the companion JPEG (or its detection) changes
  while the RAW-side active mask stays the same: fallback/offline renders
  on the standard basis would keep using an out-of-date snapshot with no
  stale banner. Staleness is per-variant, and Update rewrites only the
  variants whose digests changed.
- `local` is dropped from normalization entirely when `regions` is empty or
  every region normalizes away — the shared `mask` never persists without at
  least one active region referencing it.
- Allowed per-region adjustments (v1): `exposure`, `highlights`, `shadows`,
  `contrast`, `saturation`, plus detail's `sharpen`/`sharpen_radius`/
  `noise_reduction`. Bidirectional tone fields (`exposure`, `highlights`,
  `shadows`, `contrast`, `saturation`) use the same signed ranges as their
  global counterparts. Detail's `sharpen` and `noise_reduction` are stored
  globally in `0..100` but per-region they are **signed deltas** in
  `-100..100` — the rendering contract treats them as
  `global + region_delta`, so a positive delta strengthens the baseline
  for that region while a negative delta subtracts from it (e.g., global
  NR=40 with subject NR=−40 renders NR=0 on the subject and NR=40 on the
  background, letting a photographer keep whole-photo noise reduction
  while leaving the bird unsmoothed). Restricting local detail to `0..100`
  the way global does would only let a region **increase** the baseline,
  which contradicts §Rendering's `global + region_delta` contract and
  strands the "keep the subject unsmoothed while NR-ing the background"
  case this feature exists for. The clamp to detail's actual `0..100`
  input range happens **after** the branch resolves — validation accepts
  the signed delta, and `_run_detail` sees the clamped resolved scalar.
  `sharpen_radius` is not a delta (it overrides the global — see
  §Rendering) so it stays in its normal positive range. Zero values
  normalize away and empty entries are dropped, with one exception:
  `sharpen_radius` is kept whenever the *resolved branch* sharpen is
  non-zero (`clamp(global.sharpen + region.sharpen, 0, 100) != 0`), not
  only when the region's own `sharpen` delta is non-zero. Applying
  global's rule literally — drop `sharpen_radius` unless the same
  object's `sharpen` is non-zero — would silently discard a subject-only
  radius change with zero strength delta, so the branch would fall back
  to the global radius and contradict §Rendering's local-detail rule
  where a region radius overrides the global one.

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

Working copies come in three flavors and their basis follows the decode
that actually produced the on-disk pixels. On the happy path
`extract_working_copy` (`vireo/image_loader.py:511-537`) decodes the RAW
source with `RAW_DECODE_PRESERVE_HIGHLIGHTS`; when libraw can demosaic
the sensor data, the resulting JPEG's pixel grid (aspect, orientation,
cropping) matches the preserve-highlights variant — only its
`long_edge` may be smaller, which the builder resamples through
anyway. When libraw *cannot* demosaic (Nikon HE\*/TicoRAW today, other
unsupported RAWs in the future), `_load_raw`
(`vireo/image_loader.py:577-600`) silently falls back to the embedded
JPEG **inside the same preserve-highlights call**, so the working copy
still writes but its pixel grid is the embedded JPEG's — potentially
cropped or a different aspect than the sensor's, and specifically not
the preserve-highlights basis. The scanner cannot detect this after the
fact from `working_copy_path` alone, so provenance must be recorded at
extraction time. When RAW extraction *fails* on a RAW+JPEG pair, Vireo
intentionally re-extracts the working copy from the companion JPEG
(`vireo/scanner.py:973-987` on scan; `vireo/app.py:19112-19122` and
`19141-19148` on the on-demand original route); that JPEG is decoded
without a RAW step, so its pixel grid is the companion's
standard-decode basis, not preserve-highlights.

The existing `working_copy_failed_source='source'` field cannot serve as
this provenance: it is a RAW-failure request-routing marker consumed by
`_has_current_working_copy_failure`, and its writers do not line up
with actual working-copy origin. It is *set without regenerating the
on-disk working copy* by `record_working_copy_failure`
(`vireo/render_source.py:407-421`) on a request-time RAW retry failure
and by `vireo/thumbnails.py:99-107` on a thumbnail RAW decode
fallback — so a RAW-derived working copy from the scanner's happy path
can end up flagged `='source'` after a later stale-marker refresh,
even though its pixel grid is still preserve-highlights. And it is
*not set* by the on-demand `/original` companion re-extract branches
(`vireo/app.py:19123-19133` and `19151-19162`), which write
`working_copy_path` and dimensions only — so a genuinely
companion-derived working copy from that path carries no marker at
all. Inferring basis from this field would therefore misalign local
weights in both directions.

PR 1 records working-copy provenance in a new dedicated
`photos.working_copy_source` column (`'raw'` | `'companion'` |
`'embedded_jpeg'` | `'standard'`), written by **every** path that
materializes `working_copy_path`. Scanner's happy RAW extraction
writes `'raw'` when `_load_raw` returned a demosaiced result, or
`'embedded_jpeg'` when it returned via the libraw-failure
embedded-JPEG fallback. This
must not be inferred from returned image dimensions: `extract_working_copy`
calls `load_image(..., max_size=working_copy_max_size)`, so `load_image`
thumbnails the RAW after decode — an ordinary 6000×4000 demosaiced RAW
saved at 4096×2731 would compare unequal to `raw.sizes.width`/`.height`
and be misclassified as `'embedded_jpeg'`, disabling local adjustments
on every offline render of a valid RAW-derived working copy. PR 1
plumbs an explicit `used_embedded_fallback` flag out of `_load_raw`
(set in the same libraw-failure branch that today falls through to the
embedded JPEG) and threads it through `extract_working_copy` to the
scanner, so provenance follows the actual decode path rather than a
downstream size heuristic. Scanner's
RAW-then-companion fallback writes `'companion'` (alongside the
existing `working_copy_failed_source='source'` routing marker it also
writes), as do both on-demand `/original` companion re-extract
branches. **Non-RAW primaries** (ordinary JPEG/PNG source photos)
also produce working copies — `_extract_working_copies` extracts from
the paired companion JPEG when one exists and otherwise from the
non-RAW primary itself (`vireo/scanner.py:908-913`), and
`recipe_render_source` may return that working copy at render time
for non-RAW recipes (`vireo/render_source.py:295-301`). Both branches
decode in the same standard-decode space as the primary (no libraw
step is involved), so the scanner writes `'standard'` for every
non-RAW working-copy row regardless of which side extraction ran
against; the RAW-only `'companion'` value stays reserved for the
RAW-then-companion fallback semantics that
`working_copy_failed_source='source'` is written alongside. Without
this fourth value, non-RAW working-copy rows would have no
representable provenance — they would stay NULL (and disable the
local pass on every non-RAW working-copy render) or get mislabeled
as `'companion'`, which would collide with the RAW-failure semantics
above. Weight-map basis selection reads this column directly:
`'raw'` shares the `preserve_highlights` basis, `'companion'` and
`'standard'` both share `standard` (they differ only in the reason
the working copy exists — RAW-failure vs. ordinary non-RAW — not in
the pixel-grid basis, so both align against a `standard`
`mask.decodes` entry), and `'embedded_jpeg'` has no snapshot basis
at all because the embedded preview's crop/aspect against the sensor
is per-camera and not recorded anywhere Vireo can map to. Rows migrated
from before the column existed start NULL, but the schema migration
itself fills the value in immediately for every row whose provenance
is unambiguous from cheap DB-only signals — no source re-extraction
needed. Concretely, the same DDL migration that adds the column runs
a follow-up `UPDATE` that sets `working_copy_source='standard'` for
every row whose primary file is a **non-RAW extension** (JPEG/PNG/
HEIC/etc. — the same set `_is_raw_file` in the scanner excludes) and
whose `working_copy_path IS NOT NULL`. This is safe to do without
touching the filesystem because non-RAW working copies are always
decoded in the same standard-decode space as their source (either
the paired companion JPEG or the primary itself — no libraw step is
involved, so there is no second decode basis a non-RAW working copy
could have come from), and their pixel-grid basis is therefore known
from the primary's extension alone. Without this DB-only step, non-RAW
legacy rows would stay NULL until the backfill/lazy path re-extracted
them — but re-extraction needs the source file to be reachable, so an
oversized non-RAW library whose originals live on an offline drive
would keep rendering local adjustments as missing-basis indefinitely,
even though the design already commits to `'standard'` for every non-
RAW working copy the scanner ever writes. Any other row — a RAW
primary whose `working_copy_path` was written before the column
existed — stays NULL after the DDL: for RAWs the working-copy basis
genuinely depends on which decode branch produced the on-disk pixels
(demosaic vs. embedded fallback vs. companion re-extraction), and the
migration cannot know that from DB state alone. Those still-NULL RAW
rows are treated as unknown provenance — the local pass is disabled
with the same warn-and-hold-zero fallback as a missing snapshot —
until the recovery paths below run. For RAW NULL rows the
scanner's existing `_working_copy_candidate_predicate` skips them
(its `working_copy_path IS NULL` clause excludes any row that already
has a working copy on disk) and the on-demand `/original` route
trusts an existing full-res working copy without regenerating
(`vireo/app.py:18789-18793`), so their column would stay NULL
indefinitely. PR 1 therefore adds an explicit backfill for those RAW
rows, but keeps it
**off the blocking migration path**: the schema migration itself only
adds the (initially NULL) column and the cheap non-RAW UPDATE, both
DDL/DB-only, so first launch after upgrade completes in DB-init time
regardless of library size.
The re-extraction work is deferred to a **resumable background job**
scheduled after `Database` init finishes (queued onto the existing
`JobRunner` alongside scan/classify/thumbnail jobs), which walks every
remaining row where `working_copy_path IS NOT NULL AND
working_copy_source IS NULL` (RAW primaries after the DDL's non-RAW
UPDATE) in chunks — checkpointing progress per row so it survives
restarts and can be paused/resumed like other long-running jobs — and
re-runs the extraction logic against the current source/companion
state to write both the working copy (replacing the existing file
only if the fresh extraction changed the pixel grid) and the new
column. The job reuses `_extract_working_copies`' machinery via a
temporary predicate that ignores the `working_copy_path IS NULL`
clause. To avoid a "wait for the sweep to reach my photo" cliff
between upgrade and full backfill, a still-NULL RAW row also **classifies
lazily on first use**: when a recipe render is about to consume a
working copy whose `working_copy_source` is still NULL, the render
path re-runs the same classification for that single row inline,
updates the column, and proceeds — so the local pass on any given
photo recovers as soon as it is next rendered, not only after the
background sweep gets to it. Until either path has run for a row,
that render treats the working copy as unknown provenance and
disables the local pass with warn-and-hold-zero, matching the
missing-snapshot fallback.

**Both paths invalidate the photo's tracked render caches whenever
they touch a working copy.** Backfill and lazy classification only
mutate `photos.working_copy_source` or `working_copy_path`, not
`recipe_json` or `EDIT_MATH_VERSION`, so preview caches keyed off
those signals will happily keep serving pre-backfill bytes with the
local pass disabled (or, worse, applied against a
misidentified basis). `_serve_preview` returns tracked preview
caches before rendering (`vireo/app.py:18314-18323`), and
`generate_thumbnail`'s cached-output check has the same shape, so a
render taken during the NULL-provenance window survives across the
backfill fix. The contract is: **whenever the backfill or lazy path
either (a) transitions `working_copy_source` from NULL to a
definite value, or (b) rewrites `working_copy_path` on disk, it
invalidates every one of the photo's tracked render caches** —
preview *and* thumbnail *and* external-edit handoff caches, using
the same full render-cache invalidator `record_working_copy_failure`
/ scanner file-changed paths already run for stale-marker
transitions — before returning from the row's update. The external
handoff cache matters here because `_external_edit_handoff_path`
returns a cached `external-edits/<id>.jpg` when the recipe,
source path, source mtime, and edit-math metadata still match
(`vireo/app.py:9173-9184`), and a RAW-offline handoff rendered
during the NULL-provenance window is keyed only by those signals —
none of which change when the backfill/lazy path later fixes the
basis, so a stale local-disabled handoff would keep going out to
the external editor on every subsequent open until the cache was
cleared by hand. The render
whose call triggered lazy classification then re-renders once
inline (equivalent to a cache miss on the invalidated key) and the
background sweep does the same for every row it flips. Skipping
this step would leave RAW+JPEG libraries with a permanent split
between the DB (correct basis) and the cache (rendered with local
disabled) until the user forced a preview/thumbnail/handoff
regeneration by
hand, which is exactly the "silent, deferred correctness bug"
failure mode this feature is meant to avoid. `'embedded_jpeg'` rows stay
disabled after backfill by design — that's the honest signal for RAWs
whose only decode is a camera preview of unknown crop, and the
alternative (materializing an `embedded_jpeg` snapshot variant whose
transform to the sensor is unknown) would silently misalign local
weights on exactly the cases that already have decoding trouble.
Guessing from `working_copy_failed_source` or from disk inspection
was considered and rejected: both false-positive and false-negative
paths above make the guess unsound, and silently picking the wrong
basis is exactly the failure mode local adjustments must avoid. The
`raw` and `companion` flavors don't need their own on-disk snapshot
variant — each reuses an existing one — but the
`working_copy_source` column has to be checked at render time;
treating every working copy as preserve-highlights would misalign
local weights on the offline-RAW companion-derived-working-copy path
and on the libraw-failure embedded-JPEG path.

Snapshot creation therefore materializes one on-disk variant per
**distinct decode basis** the render source might use, keyed by decode
mode, all sharing one `local.mask.ref`. Export's `darktable`-developed
output path is deliberately **not** given a variant — recipes with
`local` bypass developed outputs and render through the RAW / working
copy / companion / original chain instead, so one of the bases below
always applies (see the "Darktable-developed exports bypass" paragraph
after the enumeration for why):

- **Non-RAWs:** one variant, `standard`, copied as-is from the existing
  `photo_masks` file (which is already in edit-render space).
- **RAWs whose folder has no companion JPEG:** one variant,
  `preserve_highlights`, generated fresh — it serves both the primary
  preserve-highlights RAW render and the working-copy fallback *when
  the working copy is truly RAW-derived* (`working_copy_source='raw'`,
  meaning `_load_raw` returned demosaiced pixels). If the working copy
  is `'embedded_jpeg'` — libraw couldn't demosaic and
  `extract_working_copy` silently returned the embedded preview — the
  local pass is disabled on that render (warn-and-hold-zero) rather
  than reusing the preserve-highlights variant, because the embedded
  preview's crop and aspect against the sensor are per-camera unknowns
  and no snapshot basis can be trusted to line up. The same
  fallthrough can happen on the **primary direct-RAW render** too:
  `load_image(..., RAW_DECODE_PRESERVE_HIGHLIGHTS)` calls `_load_raw`,
  and `_load_raw` returns the embedded JPEG in the same libraw-failure
  branch (`vireo/image_loader.py:577-600`) — so a "successful" direct
  RAW load can still be embedded-preview pixels on unsupported RAWs
  with no usable companion. The `used_embedded_fallback` flag PR 1
  plumbs out of `_load_raw` for `extract_working_copy` is threaded
  through the same call at **every** direct-RAW render site
  (previews, thumbnails, exports, handoffs, warmups — the same list
  §Rendering enumerates for the basis contract) and returned alongside
  the loaded image. Render code treats a `used_embedded_fallback=True`
  direct-RAW load as `embedded_jpeg` basis (no `mask.decodes` entry
  matches) and disables the local pass with warn-and-hold-zero, the
  same as an `embedded_jpeg` working copy. Without this the direct-RAW
  render would silently pick the `preserve_highlights` variant and
  blend it into embedded-preview pixels of unknown crop/aspect —
  exactly the failure mode the working-copy case was written to
  prevent, just on a different call path. **Snapshot creation uses
  the same signal.** The load that materializes the
  `preserve_highlights` variant when the user adds the first local
  adjustment goes through `_load_raw` too, and if that call returns
  `used_embedded_fallback=True` the on-disk pixels would be the
  embedded JPEG rather than a demosaic — labeling them
  `mode: preserve_highlights` in `mask.decodes` would be a lie, and
  every later render on the same photo would call `_load_raw` again,
  see the same fallback, treat the load as `embedded_jpeg` basis,
  and disable the local pass because nothing in `mask.decodes`
  matches. Materializing the variant under an `embedded_jpeg` label
  instead is also rejected: that basis has no matching `mask.decodes`
  entry at render time by design, so the snapshot would sit on disk
  but be dead on arrival for every render. So no `preserve_highlights`
  entry is written when snapshot creation hits the fallback. For a
  RAW with no companion that means the whole snapshot family fails
  to materialize and the local add is **refused at creation time**
  with the same "No subject mask — run the pipeline's mask stage"
  surface the UI shows for a missing mask (a distinct log line keeps
  the two failures separable in diagnostics), rather than leaving
  the user with a saved recipe whose local pass every subsequent
  render silently disables. The RAW+JPEG case below inherits the
  same rule for its `preserve_highlights` variant — see there for
  how the `standard` variant on the companion keeps the local add
  viable when the RAW-side snapshot creation hits the fallback.
- **RAWs with a companion JPEG:** two variants —
  `preserve_highlights` (for the primary RAW path *and* working copies
  with `photos.working_copy_source='raw'`) *and* `standard` (for the
  companion-fallback path *and* working copies with
  `photos.working_copy_source='companion'`, where the working copy was
  re-extracted from the companion JPEG after RAW extraction failed).
  `working_copy_source='embedded_jpeg'` rows disable the local pass
  the same way as above; a direct-RAW render whose `_load_raw`
  returned `used_embedded_fallback=True` disables it the same way, on
  the same `embedded_jpeg` basis rationale (this can happen on
  RAW+JPEG pairs when the companion route was not taken for reasons
  unrelated to RAW decode — e.g., the caller resolved to the RAW
  original first and hit the silent embedded fallback inside libraw).
  Snapshot creation applies the same signal per-variant: if the
  RAW-side `_load_raw` returns `used_embedded_fallback=True`, the
  `preserve_highlights` entry is **omitted from `mask.decodes`**
  while the `standard` variant on the companion still materializes.
  At render time, a direct-RAW load whose `_load_raw` returns
  `used_embedded_fallback=True` is treated as a companion-fallback
  trigger *when the photo's `mask.decodes` carries a `standard`
  entry* — the same swap the pipeline already applies for
  undersized embedded previews (`vireo/app.py:18395-18453`,
  `vireo/export.py:307-365`): the render source is switched to the
  companion JPEG, the basis becomes `standard`, and the local pass
  serves off the `standard` snapshot. Without this swap, the direct-
  RAW render would hit the `embedded_jpeg` basis and disable the
  local pass even though a viable companion snapshot exists on
  disk — the exact case the `standard` variant was materialized to
  cover. Companion and companion-derived working-copy renders
  already serve the local edit off `standard` under the standing
  basis contract, so the swap only closes the direct-RAW hole; the
  local add is not refused as long as at least one variant is
  viable. If the swap target itself becomes unavailable later
  (companion deleted or unreadable at render time), that direct-RAW
  render falls back to the standing `embedded_jpeg`-disables-local
  rule for that render only (warn-and-hold-zero) and swap-and-serve
  resumes the next time the companion is reachable. If both sides
  fail at snapshot creation time (RAW hits the fallback *and* the
  companion is missing or itself fails to load), the local add is
  refused with the same "No subject mask" surface as the no-
  companion case above.

**Darktable-developed exports bypass, they don't get their own variant.**
Export prefers a darktable-developed output ahead of RAW / working copy /
original when one exists (`vireo/export.py:188-214`), and the developed
file can carry a style crop, a lens correction, or a different aspect
ratio applied by darktable's own pipeline — none of which are recorded
anywhere Vireo can read. A separate `developed` snapshot basis would
therefore have no reliable transform to line the mask up with the
developed pixels. Recipes with a `local` block are photo-specific
subject/background edits keyed to the SAM mask over Vireo's own
pipeline, so mixing them with a darktable-developed output is
ambiguous by design. Export handles this by **skipping the developed
preference entirely when the recipe has a `local` block** and rendering
through the RAW / working copy / companion / original chain instead —
one of the three decode bases above then applies, exactly as with any
other edited render. The developed-output export path is only for
recipes with no local adjustments.

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
inputs**, not on the snapshot pixels. Each entry in `mask.decodes` carries
its own `source_digest` (see schema), so staleness is evaluated per
variant against the inputs *that variant was actually built from*:
- `preserve_highlights` — a hash over the current
  `photos.active_mask_variant` row's source mask file bytes, its stored
  detection prompt, the detector version that produced it, the active
  SAM variant/model name (`photos.active_mask_variant`), the **RAW
  source file's identity** (`photos.file_hash` — mtime + size + sha1
  the scanner already maintains on the primary file), and the
  **preserve-space detection prompt/detector version that actually
  produced this snapshot** (the re-detection MegaDetector/SAM ran on
  the preserve-highlights load). Including the RAW file identity
  catches the case where the RAW is replaced or repaired in place: the
  scanner does invalidate thumbnails, working copies, and preview
  caches on `file_hash` change (`vireo/scanner.py:1840-1853`) but does
  *not* rewrite the existing `photo_masks` row, so a pure source-mask
  digest would stay clean while edit renders decoded different RAW
  pixels and the preserve snapshot silently misaligned. Including the
  preserve-space prompt catches re-detections that only shift in the
  preserve basis (e.g., a highlight-recovered demosaic that changes
  the bird's box slightly) even when the JPEG-first source mask is
  unchanged.
- `standard` — the digest inputs depend on which non-preserve source
  produced the on-disk pixels for this variant:
  - **RAW+JPEG companion** — a hash over the companion JPEG's
    file identity (mtime + size + sha1), the prompt/detector version used
    when detection re-ran on the companion, and the active SAM
    variant/model name used to segment that re-run.
  - **Non-RAW (ordinary JPEG/PNG source)** — a hash over the primary
    photo's `photos.file_hash` (mtime + size + sha1 the scanner already
    maintains, and which changes when the source file is edited or
    replaced), the current `photos.active_mask_variant` row's source
    mask file bytes, its stored detection prompt, the detector version
    that produced it, and the active SAM variant/model name. This is
    the same shape as the preserve digest with the RAW-specific
    preserve-space prompt/detector fields dropped, because for a
    non-RAW source there is no second decode basis to re-run detection
    against — the standard variant is byte-for-byte the active mask
    resampled/transformed to snapshot storage.
  Switching SAM variants (`sam2-small` → `sam2-large`) with unchanged
  source bytes must invalidate this entry on either path, so the SAM
  variant is a first-class digest input on every basis. Without a
  non-RAW definition, PR 1 would have no specified digest to compare
  when an ordinary JPEG's source mask is regenerated or its
  `photos.active_mask_variant` changes, and the editor could never
  surface Update on non-RAW photos.
Staleness for a variant means the current inputs' re-computed digest
differs from that entry's `source_digest`. A **decode basis the current
source state now expects but which has no entry in `mask.decodes`** is
also treated as stale (a "materializable" variant), so Update can add
the missing snapshot family instead of leaving fallback renders with
warn-and-hold-zero forever. The expected set is the same list §Snapshot
creation enumerates against the current source state — one `standard`
for non-RAW primaries; one `preserve_highlights` for RAWs whose folder
has no companion; both for RAWs with a companion — with the same
`used_embedded_fallback` / `working_copy_source='embedded_jpeg'` gates
that drop `preserve_highlights` from the expected set on unsupported
RAWs. Concretely this covers: a RAW-only recipe whose folder later
gains a companion JPEG (a `standard` variant is now expected because
the companion-fallback render path exists — Update materializes it
against the companion), and a RAW+JPEG recipe whose original snapshot
skipped `preserve_highlights` because `_load_raw` hit the embedded
fallback and whose RAW is later replaced/repaired so `_load_raw` now
demosaics (a `preserve_highlights` variant is now expected — Update
materializes it against the demosaiced load). It does not cover
transitioning **out** of the embedded-fallback state solely because
libraw was upgraded server-side without a source-file change: the
`preserve_highlights` variant's expected inputs (RAW `file_hash` and
the preserve-space re-detection prompt) haven't changed, so nothing on
the source-digest side flags it either; the user has to trigger a
re-scan or an explicit "regenerate local snapshots" action for that
state transition, and PR 1's Update rule catches it the first time
the source-side inputs (or `working_copy_source`) do change. It also
does not cover companion-JPEG removal: when the source state now
expects **fewer** variants than `mask.decodes` currently holds, the
extra entry is left in place rather than pruned, so any render still
resolving to that decode keeps rendering off the existing snapshot;
GC eventually reclaims it once no recipe (current or history)
references its ref. Update's job is to add missing coverage, not to
delete coverage that some historical recipe or edge-case render path
may still legitimately use. The editor surfaces "Newer subject mask
available — Update" whenever **any** variant is stale (digest-drift
or materializable), and Update **always rotates `local.mask.ref` to a
fresh content-addressed value** and writes a complete new family
under it — regenerating the variants whose digests changed,
**materializing** the ones expected-but-absent (fresh
detection/segmentation against that decode's own load, same as first
snapshot creation), and **copying the untouched variant files
byte-for-byte** to the new ref's filenames. The ref rotation is
**atomic against readers**: the publish order is (a) write every
regenerated variant to
`<db_dir>/edit-masks/<photo_id>.<new_ref>.<decode>.png.tmp`, (b) copy
every untouched variant to its `.<new_ref>.<decode>.png.tmp` sibling,
(c) fsync each `.tmp` file, (d) atomically rename each `.tmp` to its
final `.png` name, (e) only then commit the recipe's new
`local.mask.ref` / `mask.decodes` via the existing DB write. A crash
between (a) and (d) leaves an incomplete family of `.tmp` files under
the new ref, but no recipe row references that ref yet, so no render
consults them — GC treats orphaned `.tmp` and unreferenced `.<ref>.png`
files the same way. A crash between (d) and (e) leaves a complete
new-ref family on disk that no recipe references — again harmless. A
render that starts before the DB commit sees the old ref and its
still-intact files; a render that starts after the DB commit sees the
new ref and its complete new family. There is no window where
`local.mask.ref` points at an incomplete family.

**Publish is atomic against a concurrent GC sweep, not only against
crashes.** Between steps (d) and (e) the new-ref family exists on disk
but no `local.mask.ref` yet points at it, so a naive "unreferenced =>
deletable" sweep running in the same window would delete a complete
new family before the DB commit exposes it and the very next render
against the new ref would see missing files and fall back to
warn-and-hold-zero. Snapshot GC therefore treats a file as reachable
whenever **either** a recipe references its ref **or** the ref is held
by an in-flight publish. Concretely, publish takes a per-`photo_id`
publish lock at step (a) and releases it after step (e); the sweep
acquires the same lock (non-blocking `try_acquire`) before evaluating
any of that photo's refs and skips the photo entirely if the lock is
held, deferring it to the next sweep. A backstop grace period —
files (including both `.tmp` and finalized `.<ref>.<decode>.png`) whose
mtime is within the sweep's configured GC grace window are not eligible
for deletion — covers the case where a publish crashes between (d) and
(e) with the lock held: the lock is per-process, so it releases on
process exit, but the grace period keeps the orphaned new-ref files
alive long enough for the next process to either reissue the publish
(if the caller retries) or let the normal orphan-file GC reclaim them
once the grace window elapses. The lock is scoped to `photo_id`, not
to a global mask-family lock, so unrelated photos' GC keeps running in
parallel with a slow publish. The old ref's files
stay intact on disk (they are not deleted or overwritten by Update);
they are still referenced by any edit-history recipe whose snapshot
they were made for, so re-rendering those historical recipes remains
deterministic and byte-identical. The old ref eventually GCs away on
its own once no current or history recipe references it. Rotating in place — mutating
`{photo_id}.{ref}.{decode}.png` under an unchanged ref — was
considered and rejected: snapshot GC explicitly keeps files while any
current or edit-history recipe still references them, so an undo/redo
back to an earlier local-adjustments recipe would find the file at the
same on-disk path but with **new** pixels, silently changing what that
historical recipe renders. Rotating the ref instead is what preserves
the (source pixels, recipe) → output determinism the design commits to.
Copying untouched-variant files (rather than leaving them under the old
ref) keeps the on-disk family under a single ref always complete, so the
weight-map builder never has to walk multiple refs to find the variant
that matches the current decode. The rotation covers both partial
refreshes (only one variant's digest changed) and full re-snapshots
(mask deletion, manual reset — every variant regenerates); the on-disk
behavior differs only in how many files are regenerated versus copied.
This matters for RAW+JPEG offline renders in two ways: if the companion
JPEG (or its detection) changes while the RAW active mask stays the
same, the standard variant's digest catches it and only that variant
regenerates under the new ref (the `preserve_highlights` file is copied
across); and any recipe still pointing at the old ref keeps rendering
against its own on-disk files, unaffected by the Update. Recipe edits
are undoable and cache-invalidating.
Comparing on inputs is deliberately not a byte comparison against the
snapshot: for RAWs the snapshot pixels come from a re-detection in
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
   when the RAW decoded successfully via libraw's demosaic (`_load_raw`
   returned `used_embedded_fallback=False`) *or* when the render fell
   back to a RAW-derived working-copy JPEG
   (`photos.working_copy_source='raw'`: `extract_working_copy` decoded
   the RAW with `RAW_DECODE_PRESERVE_HIGHLIGHTS` and libraw actually
   demosaiced, so its pixel grid shares the preserve-highlights basis);
   `standard` when the render fell back to the companion JPEG, when the
   working copy is companion-derived
   (`photos.working_copy_source='companion'` — re-extracted from the
   companion JPEG after RAW extraction failed by scanner or by the
   on-demand `/original` route, so its pixel grid is the companion's
   standard-decode basis), when the working copy is a non-RAW
   primary's own (`photos.working_copy_source='standard'` —
   extracted from the non-RAW primary itself or its companion in
   standard-decode space), or when the photo is a non-RAW rendered
   directly from its source file. A working copy
   with `working_copy_source='embedded_jpeg'` (RAW decode fell through
   to the embedded preview inside `_load_raw`) or a NULL value (a
   legacy row from before the column existed and before the migration
   backfill has run) is treated as unknown basis and the local pass is
   disabled with warn-and-hold-zero, exactly as with a missing
   snapshot. A **direct-RAW load whose `_load_raw` returned
   `used_embedded_fallback=True`** (the same libraw-failure
   fallthrough on the primary render path,
   `vireo/image_loader.py:577-600`) is routed through the same
   swap-and-serve rule §Snapshot decode basis defines for that
   signal, not classified as embedded_jpeg-and-disabled here:
   **when the photo's `mask.decodes` carries a `standard` entry**
   (RAW+JPEG pair whose companion side materialized), the render
   source is switched to the companion JPEG, the effective basis
   becomes `standard`, and the weight-map builder picks the
   `standard` entry — the same swap the pipeline already applies for
   undersized embedded previews (`vireo/app.py:18395-18453`,
   `vireo/export.py:307-365`). Only when no `standard` entry exists
   (RAW-only recipe, or the companion side of the snapshot family
   failed to materialize, or the companion has since become
   unreadable at render time) does the direct-RAW
   `used_embedded_fallback=True` path fall back to the
   `embedded_jpeg`-disables-local rule above. Without this the two
   sections would contradict each other on the same signal — the
   snapshot section would materialize a `standard` variant to cover
   the direct-RAW fallback while the rendering pass would still
   disable local on that variant's whole reason to exist. The
   `used_embedded_fallback` signal is returned by `load_image` on
   every direct-RAW render call site — not just extraction — so the
   basis check (and the swap decision) catches embedded-preview loads
   at render time even when no working-copy row exists to consult.
   NULL rows recover automatically once the migration backfill
   described in §Snapshot decode basis populates the column;
   `'embedded_jpeg'` working-copy rows stay disabled by design. `recipe_render_source` already
   distinguishes these paths internally, so it returns the effective basis
   alongside the source path for the weight-map builder to consume.
   The Open External and iNaturalist handoff paths do **not** go through
   `_recipe_render_source` for the initial source pick unless the recipe
   has a crop: `_external_edit_recipe_source`
   (`vireo/app.py:9086-9139`) and `_inat_edit_recipe_source`
   (`vireo/app.py:11428-11472`) each re-implement the RAW-offline
   fallback (swap in the working copy or companion JPEG when the RAW
   original is missing) in their own no-crop branches, so the initial
   basis for those handoffs is set inside those resolvers — not by
   `_recipe_render_source`. Both resolvers therefore return an
   effective basis alongside the source path, mirroring
   `_recipe_render_source`'s contract and applying the same
   `working_copy_source` / companion-vs-RAW rules from the enumeration
   above (companion → `standard`; working copy → whichever basis the
   row's `working_copy_source` column names; RAW original →
   `preserve_highlights` for a RAW primary, `standard` otherwise).
   Without that, an offline-RAW handoff or upload would render a
   preserve-highlights basis against companion-standard pixels for
   exactly the RAW+JPEG cases the fallback exists to serve — a
   misalignment that would happen at *initial* resolution, before any
   late swap runs, so the late-swap contract below cannot rescue it.
   Whichever resolver produced it, the initial basis is only the
   *starting* value, and **every** recipe render call site that can
   late-swap the loaded image after initial resolution has to update
   the basis in lock-step with the swap. Today those sites
   are: `serve_preview` and the edit-preview route, which drop an
   undersized embedded-JPEG or failed RAW decode in favor of the
   companion JPEG (`vireo/app.py:18395-18453`, `18630-18691`); the
   **edited `/photos/<id>/original` route**, where `serve_original_photo`
   applies the same RAW-decode-failure or undersized-embedded-JPEG →
   companion swap before calling `apply_recipe_to_loaded_image`
   (`vireo/app.py:18921-18969`, call at `18978-18982`) — this is the
   full-size / 1:1 edited render served to the lightbox, so omitting it
   would misalign local weights on exactly the "view at 100%" path users
   trust to show the finished edit; the export pipeline, which does the
   same after `load_image` (`vireo/export.py:319-363`); the **Open
   External handoff**, which applies the recipe to the loaded image and
   has the same RAW-decode-failure or undersized-embedded-JPEG →
   companion JPEG switch before `apply_recipe_to_loaded_image`
   (`vireo/app.py:9195-9266`); the **iNaturalist upload** path, which
   mirrors the handoff and does the same swap before rendering
   (`vireo/app.py:11540-11594`); the **preview warmup job**
   (`vireo/app.py:13199-13334`), which materializes the tracked warmed
   preview cache — it repeats the same undersized-embedded-JPEG and
   failed-RAW-decode → companion swap before `apply_recipe_to_loaded_image`,
   and its output is served on subsequent cache-hit reads without any
   further basis check; the **pipeline preview warmup**
   (`vireo/pipeline_job.py:2220-2344`), which does the same swap-then-render
   for the pipeline's preview cache write; the **thumbnail
   generation paths** (both the background thumbnail job's
   `_retry_thumbnail_with_companion` inside `generate_all`, at
   `vireo/thumbnails.py:80` called from `295-319`, and the on-request
   `serve_thumbnail` self-heal route at `vireo/app.py:15387-15433`),
   which each retry the same thumbnail render against the companion
   JPEG when the RAW decode fails — `generate_thumbnail` calls
   `apply_recipe_to_loaded_image` on the post-swap loaded image
   (`vireo/thumbnails.py:177-181`) and writes the result to the on-disk
   grid-thumbnail cache, so a mismatched basis here bakes shifted local
   weights into the browser's grid thumbnails until the cache is
   deleted; and the **pipeline job's own thumbnail stage**, which
   inlines the same companion fallback via a duplicated
   `_retry_thumbnail_with_companion` at `vireo/pipeline_job.py:175`
   called from both the scan-driven thumbnail worker
   (`vireo/pipeline_job.py:1884-1928`) and the collection-driven
   thumbnail worker (`vireo/pipeline_job.py:1991-2035`), each of which
   resolves a RAW recipe source, renders via `generate_thumbnail` with
   preserve-highlights decoding, and on failure retries against the
   companion JPEG — omitting these workers would let a pipeline run
   write the same misaligned grid thumbnails the background-job path
   was fixed to avoid, on exactly the RAW+JPEG photos whose RAW decode
   is unsupported or undersized. All these switches change the effective basis from
   `preserve_highlights` to `standard`, so the weight-map builder
   consumes the basis that reflects the *actual* loaded image at each
   site, not the one `recipe_render_source` chose up front — every one
   of these call sites updates the basis alongside the source swap
   before invoking the local pass. Restricting the fix to preview/export
   would silently misalign local weights on the edited `/original`, Open
   External, and iNat upload renders for exactly the RAW+JPEG cases the
   fallback exists to serve; leaving the warmup and thumbnail paths out
   is worse still because a mismatched basis there bakes the
   misalignment into a persistent cache — the UI keeps showing shifted
   local edits (in the grid as well as the lightbox) on every
   subsequent read until the cache is invalidated, even after the code
   is fixed. If the final basis has no
   matching `mask.decodes` entry the pass is disabled, warn-and-hold-zero,
   as above. Load
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
   Each field's resolved per-pixel amount is **clamped to that field's
   validated global range** (the same ranges `vireo/image_edits.py`
   enforces on the global recipe) *before* the per-pixel op consumes it —
   the local pass explicitly matches detail's clamp-after-resolution
   contract. Without this clamp a same-signed global + region_delta pair
   pushes the resolved amount outside the range the tone pipeline was
   written for: e.g. global `contrast=-100` plus background
   `contrast=-100` resolves to `-200`, and `vireo/tone.py` computes
   `1 + contrast/100`, producing a **negative** contrast factor
   (image inversion) instead of the intended minimum-contrast render.
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
  mask, skipping (and reporting) photos without one. This contract lives
  in the **backend**, not the UI: the existing bulk-apply endpoint
  `/api/photos/edit-recipe/apply` (`vireo/app.py:4057-4084`) validates one
  recipe and writes it to every target verbatim, and once PR 1 makes
  `local` a valid recipe field the same endpoint could otherwise copy a
  source photo's `local.mask.ref` to every target — leaving each target
  pointing at a snapshot filename (`<target_id>.<ref>.{decode}.png`)
  that does not exist, so every copied render disables the local pass.
  PR 1 therefore updates the bulk endpoint to intercept `local`
  server-side: for each target, strip the incoming `local.mask.ref` /
  `mask.decodes`, look up the target's own `photos.active_mask_variant`,
  materialize a fresh snapshot family in the target's decode bases (as if
  local adjustments had been added to the target photo directly), and
  substitute the new ref into the per-target recipe before writing. The
  region entries and shared feather carry over unchanged (they are
  slider values, not mask-identity). Targets with no usable mask are
  skipped and reported in the response, matching the UI behavior.

  **The resnapshot itself does not run inside the request.** A single
  snapshot family costs a few seconds per `(photo, decode)` — image
  load + detection + SAM — and RAW+companion targets pay it twice; the
  bulk paste flow can hit dozens or hundreds of targets in one POST,
  so materializing them inline would tie up the Flask worker (already
  a scarce resource with the SSE panel) for minutes and time the UI
  out before the response landed. PR 1 instead queues the per-target
  resnapshot as a **`JobRunner` background job** (same runner as scan
  / classify / thumbnails, so it inherits SSE progress, pause/resume,
  and per-workspace scoping): the endpoint validates the incoming
  recipe once, immediately writes each target's `local`-stripped
  recipe with a placeholder ref that renders as missing-snapshot
  (warn-and-hold-zero — no partial local edits leak out), durably
  persists a `local_resnapshot_jobs` row carrying the payload
  `(source_recipe, target_ids, active_workspace, placeholder_tokens)`
  inside the same transaction as the placeholder writes and only then
  enqueues the in-memory JobRunner closure that consumes that row,
  and returns synchronously with the job id plus a `pending` list of
  target ids whose resnapshot is queued. The job runs the same
  strip-and-resnapshot helper per target, rewrites each target's
  `local.mask.ref` / `mask.decodes` in-place under the same
  publish-lock protocol as single-photo Update (so an in-flight
  snapshot GC never races the family the job just wrote), and emits
  an SSE event per target as its snapshot finalizes. The response
  shape changes accordingly: instead of the current single `recipe`
  field, `/api/photos/edit-recipe/apply` returns a map keyed by
  target `photo_id` to that target's `local`-stripped placeholder
  recipe (plus the same top-level `applied` / `skipped` id lists it
  returns today, and a new `pending` list of ids the resnapshot job
  will finish asynchronously, plus the `job_id` the UI can subscribe
  to for completion). Recipes with **no** `local` block keep the
  fully synchronous path — the job is only enqueued when the incoming
  recipe carries `local`, so the common "paste global adjustments to
  10 photos" case still responds inline. Callers that need the
  resnapshotted recipes in the response (rare — the UI always
  refreshes per-target state on the completion SSE) can poll the same
  per-target recipe fetch the editor already uses.

  **The `local_resnapshot_jobs` row is what makes the job crash-safe.**
  `JobRunner` today keeps non-pipeline work closures only in memory,
  and startup marks every persisted queued/running row failed rather
  than rebuilding its closure (`vireo/jobs.py:51-57`,
  `vireo/jobs.py:106-119`); a naive enqueue-then-write order would
  strand a whole selection in the missing-snapshot state after any
  crash between the placeholder writes and the job finishing. PR 1
  therefore inserts the `local_resnapshot_jobs` row inside the same
  DB transaction that stamps the per-target placeholder recipes, and
  only after commit does the in-memory JobRunner closure start
  consuming it. Startup reconciles by **re-enqueueing** (not failing)
  any `local_resnapshot_jobs` row in `queued` or `running` state — the
  row is the recovery record, the generic mark-failed sweep in
  `JobRunner` skips this job type, and each row's payload
  (`source_recipe`, `target_ids`, `active_workspace`,
  `placeholder_tokens`) is enough to rebuild the work closure
  end-to-end. Any restart therefore resumes pending resnapshots
  against the same placeholder-marked targets rather than leaving
  them stranded.

  **Each placeholder ref is a compare-and-swap token.** The
  placeholder's `local.mask.ref` is a `pending:<placeholder_token>`
  sentinel — unique per `(job_id, target_id)` and recorded in the
  durable row's `placeholder_tokens` field. Finalization CAS's on it:
  under the same per-`photo_id` publish-lock as single-photo Update,
  the job reads the target's current `local.mask.ref` and substitutes
  the finalized ref only if the read equals the expected
  `pending:<placeholder_token>`. If the user (or any other write path)
  has cleared, re-pasted, or otherwise overwritten the target's
  recipe in the meantime, the read differs and the target is dropped
  from the per-target work — SSE emits `finalize_skipped` and the id
  moves to the response's `skipped` list on completion — so an older
  bulk job can never silently reintroduce local adjustments into a
  newer recipe. A `pending:*` ref also fails `mask.decodes` lookup by
  construction (no on-disk snapshot filename matches), so a target
  still holding a placeholder renders as missing-snapshot
  (warn-and-hold-zero) exactly as intended during the pending window,
  and cache-invalidation on the finalization transaction flips it
  atomically to the completed local render.

  **Finalization patches the edit-history and XMP sync records the
  endpoint wrote from the placeholder.** The bulk endpoint's own
  edit-history insert (`vireo/app.py:4084-4097`) and XMP-sync queue
  entry both record the recipe value written *before* the response
  returned — i.e. the placeholder — and redo replays
  `edit_history_items.new_value` verbatim (`vireo/db.py:11303-11308`)
  while XMP sync exports the queued recipe verbatim. Without a patch
  step, undo→redo on a bulk-pasted target would reinstate the
  placeholder recipe (local pass disabled) even after the background
  job succeeded, and the same photo's XMP sidecar would export a
  disabled `local` block. The finalization step therefore updates
  each target's edit-history `new_value` and its pending XMP sync
  entry in the same transaction that swaps `local.mask.ref` to its
  finalized value; a target dropped by the CAS above has its
  edit-history / sync entries rewritten to the **local-stripped
  bulk-paste recipe** — the placeholder recipe with its `local` block
  removed — never the target's *current* recipe and never the
  placeholder itself. This is the only rewrite target that keeps
  history semantics honest: mirroring the current recipe would make
  redo of the bulk paste (which replays `edit_history_items.new_value`
  verbatim, `vireo/db.py:11303-11308`) reapply whatever the user
  happens to have kept in the interim rather than the paste's
  original intent, so after undoing the later edit and redoing the
  bulk entry the target would jump to the later edit's recipe instead
  of the local-stripped bulk state — a landmine dressed as a redo.
  Rewriting to the local-stripped recipe instead makes redo replay
  the paste's non-local adjustments (a no-op if the paste had no
  non-local content), so history replay and sync export both stay
  consistent with what is actually persisted on the photo *and* with
  the bulk paste's original intent on the skipped target.

  **The XMP sync job skips `pending:*` refs so the queue row stays
  in place for finalization to overwrite.** `sync_to_xmp` snapshots
  `pending_changes` at job start, writes each queued
  `edit_recipe_json` to the sidecar, and then clears the synced ids
  in one pass. Left as-is, a `/api/jobs/sync` run that fires while
  the bulk resnapshot is still pending would copy the
  `pending:<placeholder_token>` recipe to the sidecar and clear the
  queue row before finalization got to patch it — leaving the
  sidecar carrying a disabled local ref and no queue row left to
  rewrite. PR 1 therefore has `sync_to_xmp` skip any queue row whose
  recipe carries a `local.mask.ref` matching a placeholder token
  recorded on a still-`queued`/`running` `local_resnapshot_jobs`
  row (matched against that row's `placeholder_tokens`, not the
  `pending:` prefix in isolation, so a hypothetical unrelated user
  string is not caught). Skipped rows stay in `pending_changes`;
  finalization then rewrites the row's `edit_recipe_json` to the
  finalized recipe (or, on CAS-skip / cancel / terminal failure,
  the local-stripped variant per the same rules below) in the same
  transaction that swaps `local.mask.ref`, and the next sync run
  exports the correct value. The user-visible effect during the
  pending window is a delayed XMP export for the affected targets —
  the same wait-until-ready contract the render pipeline already
  applies to their previews.

  **Rendered-output paths refuse to bake `pending:*` refs into
  permanent artifacts.** Previews and grid thumbnails only cache
  regenerable bytes — a pending target reads as warn-and-hold-zero
  during the pending window and flips to the finalized render as
  soon as the resnapshot job invalidates its cache — so treating a
  missing snapshot as "render without local" is safe there. But
  three rendered-output paths write bytes that survive the pending
  window and cannot be repaired by finalization after the fact:
  `export_photos` (`vireo/export.py:166-380`) reads the current
  recipe and calls `apply_recipe_to_loaded_image` before writing
  the file to disk under a user-chosen location; the Open External
  handoff (`_external_edit_recipe_source` → `apply_recipe_to_loaded_image`,
  `vireo/app.py:9195-9257`) renders once into `external-edits/<id>.jpg`
  and hands the path to another application which then owns the file;
  and the iNat upload path (`vireo/app.py:11540-11585`) renders
  and POSTs the bytes to a remote server. If any of these ran
  against a target still holding `pending:<placeholder_token>`,
  the resulting file / upload would permanently omit the local
  edits the user pasted, and no later finalization event could
  reach the on-disk export, the editor the user already opened, or
  the iNat observation the server already accepted. PR 1 therefore
  has each of these paths gate on the placeholder-token check
  before rendering: any recipe whose `local.mask.ref` matches a
  placeholder recorded on a still-`queued`/`running`
  `local_resnapshot_jobs` row is treated as **not-ready** —
  `export_photos` skips the photo with a per-photo reason
  (`local adjustments pending`), records it in the job's skipped
  list so the user learns which photos to re-export after the
  resnapshot finishes, and continues with the rest of the
  selection; Open External and iNat return a 409 with the same
  reason and the UI surfaces "wait for the local resnapshot to
  finish" rather than opening or uploading. Same detection surface
  as XMP sync (matched against `local_resnapshot_jobs.placeholder_tokens`,
  not the `pending:` prefix alone), same wait-until-ready contract,
  and the render invalidation on finalization already re-arms the
  next attempt. This deliberately errs on the side of skipping a
  handful of targets over silently exporting local-disabled bytes:
  the "warn-and-hold-zero" fallback exists for regenerable caches,
  not for artifacts that leave Vireo's control.

  **And it scrubs the placeholder token from every later history and
  sync record that captured it, not only the endpoint's own row.** A
  target the user edits *while the bulk resnapshot is still
  pending* — clearing the local block, pasting a different recipe,
  opening the editor and saving — records the placeholder recipe as
  the new edit-history row's `old_value` (that later edit's
  "state before this change"), and any pending XMP sync entry it
  queues captures the same token; `_apply_undo` replays `old_value`
  verbatim (`vireo/db.py`'s undo path). If the finalization / CAS-skip
  step only patched the bulk endpoint's own row, undoing that later
  edit would resurrect the `pending:<placeholder_token>` ref after
  the job had already CAS-skipped, and the photo would go back into
  the missing-snapshot state permanently — the failure mode the CAS
  was written to prevent, moved one edit downstream. So the same
  transaction that either finalizes or drops the endpoint's own row
  also scans `edit_history_items.old_value` / `new_value` **and** the
  pending XMP sync queue for that `photo_id`, and rewrites every
  remaining occurrence of the token:
  - **On CAS match (job finalized normally):** every occurrence of
    `pending:<placeholder_token>` in later history rows or queued
    sync payloads for that photo is rewritten to the finalized
    `local.mask.ref` / `mask.decodes`, so undo/redo of any later
    edit restores the local block against the real on-disk snapshot
    family.
  - **On CAS skip (target overwritten before finalize):** every
    remaining occurrence has its `local` block stripped (the
    placeholder is replaced with the same recipe minus `local`),
    and any queued XMP sync rows still carrying the token are
    **rewritten in place to that same local-stripped recipe**, not
    dropped from the queue. `_queue_edit_recipe_sync` deletes any
    prior `edit_recipe` row for the photo before inserting the
    placeholder value (one queued `edit_recipe` per photo by
    construction), so the placeholder row is the *only* pending
    export of any non-local adjustments the bulk paste also carried;
    dropping it would leave the sidecar out of date on exactly those
    non-local edits after the photo's on-disk recipe has already lost
    only the `local` block. Rewriting keeps the queue row aligned
    with the persisted recipe on skip. The bulk paste becomes a
    retroactive no-op on that target's local edits — matching the
    response's `skipped` list — while any non-local adjustments the
    same paste carried still export on the next sync run, instead of
    leaving a landmine in history that only detonates on undo.
  The scan is bounded and deterministic: `placeholder_tokens` is
  unique per `(job_id, target_id)`, the search is scoped to the
  single target's `photo_id`, and the durable `local_resnapshot_jobs`
  row is the authoritative index of which tokens ever existed — so
  recovery after a restart runs the same scan against the recovered
  token list rather than trusting whatever happens to still be in
  memory. The scrub is idempotent: a token that no longer appears
  anywhere is a valid terminal state.

  **Cancel and terminal-failure paths run the same cleanup.**
  `JobRunner.cancel_job` exposes cancellation for queued/running rows,
  and per-target work can fail terminally after retries (image load
  failure, mask store corruption, disk full). The crash-recovery and
  CAS-skip paths above cover restart and user-overwrite; a cancelled
  or terminally failed job would otherwise leave every unreached
  target rendering `pending:<placeholder_token>` — missing-snapshot
  local edits indefinitely, the same permanent landmine those paths
  were written to prevent, arriving via a third terminal state. PR 1
  therefore treats cancel and terminal failure as **the CAS-skip
  cleanup applied to every remaining pending target for that job**:
  the durable `local_resnapshot_jobs` row's terminal handler walks
  its `placeholder_tokens` list, and for each target whose current
  `local.mask.ref` still equals its recorded
  `pending:<placeholder_token>` (read under the same per-`photo_id`
  publish-lock as single-photo Update) strips the `local` block from
  the placeholder recipe — leaving all non-local adjustments
  intact — and then runs the same later-history / XMP-sync scrub the
  CAS-skip branch defines above, replacing every remaining occurrence
  of the token with the local-stripped recipe and **rewriting any
  queued XMP sync rows that still carry it to that same local-stripped
  recipe**, never dropping them. The rationale is the same as the
  CAS-skip path: the placeholder queue row is the only pending
  export of any non-local adjustments the bulk paste also carried
  (because `_queue_edit_recipe_sync` had already deleted any prior
  `edit_recipe` row for that photo), so dropping it on
  cancel/failure would strand those non-local edits unsynced even
  though they remain on the photo. The `local_resnapshot_jobs` row
  transitions to `cancelled` / `failed` in the same DB transaction as
  those writes so startup reconciliation stops re-enqueueing it;
  targets that already finalized under their own per-photo
  publish-lock before the terminal event stay finalized (their refs
  are no longer `pending:*` and the cleanup no-ops them). SSE emits
  `finalize_skipped` per cleaned-up target with the terminal reason
  (`cancelled` / `failed:<error>`), and the completion event surfaces
  "resnapshot cancelled — local adjustments dropped from N photos"
  so the user learns which targets lost their local block instead of
  discovering missing-snapshot state photo-by-photo. This is
  deliberately not a retry path: a cancelled job stays cancelled and
  terminally-failed target work is not silently re-attempted, because
  both cases require user attention (fixing a mask, freeing disk,
  deciding whether to re-paste) and reintroducing local edits under
  the same placeholder token would race any intervening edits the
  user made against the cleaned-up recipe.

  This matters because the paste UI in `vireo/templates/browse.html`
  currently caches `data.recipe` for every applied id
  (`vireo/templates/browse.html:4282-4285`) — with a singular response,
  every target's client-side cache would end up holding the source
  photo's `local.mask.ref`, and the next UI edit or copy from any of
  those targets would persist that wrong ref back to the server (its
  snapshot family only exists under the source's photo id, so the
  render would then disable the local pass). Per-target recipes in the
  response let the UI install each target's own placeholder recipe
  into its cache immediately — matching what the server just wrote —
  and the resnapshot job's SSE completion events update the same
  cache entries with the finalized `local.mask.ref` / `mask.decodes`
  as each target's snapshot family materializes. Skipped targets omit
  a per-target recipe (they appear only in the `skipped` list); pending
  targets appear in both the per-target map (with their placeholder
  recipe) and the top-level `pending` list, and the UI treats a pending
  target's local pass as disabled-until-ready.

  **Single-photo recipe writes verify the ref belongs to that photo.**
  The normal editor save path (`POST /api/photos/<int:photo_id>/edit-recipe`,
  `vireo/app.py:3968-3986`) accepts an arbitrary recipe payload and
  stores it via `db.set_photo_edit_recipe`; nothing today ties the
  incoming `local.mask.ref` to `photo_id`. If a client — a scripted
  caller, a browser extension, or a hand-crafted request — POSTs a
  recipe copied from another photo (or lifted from the paste UI's
  cache before finalization), the endpoint would happily persist a
  foreign `<other_id>.<ref>...` reference on this photo's row,
  and every subsequent render would look for
  `<photo_id>.<ref>.{decode}.png` on disk, find nothing, and disable
  the local pass — the same silent-failure the bulk and pairing
  paths were rewritten to prevent, arriving via the single-photo
  door. PR 1 therefore has the single-photo write path validate
  ownership before storing the recipe: if the payload carries a
  `local` block, the endpoint loads the target's current recipe
  and accepts the incoming `local.mask.ref` only if it either
  equals the target's current ref (a no-op re-save from the editor)
  or is already recorded in the target's snapshot family under
  `<photo_id>.<ref>.{decode}.png` (an ownership check against the
  target's own on-disk snapshot files, not just the DB). Any other
  supplied ref — including a `pending:*` placeholder token, which
  is never a valid single-photo save target — is rejected with a
  409 (`local ref not owned by this photo`) and the recipe is not
  persisted; the client is expected to route foreign refs through
  the bulk-apply endpoint (which will resnapshot them) instead of
  bypassing the strip-and-resnapshot helper. This closes the last
  same-shape hole: recipe writes to a single photo now either
  keep the ref they were given (because it was already this
  photo's ref) or refuse it, and never persist a ref whose
  snapshot family lives under someone else's photo id.

  The resnapshot is one instance of a more general
  contract: **any code path that transfers a `recipe_json` from one
  `photo_id` to another must go through the same strip-and-resnapshot
  helper.** The bulk-apply endpoint is the obvious case, but not the
  only one — RAW/JPEG pairing in `vireo/scanner.py:481-488` copies the
  companion JPEG's `recipe_json` verbatim into the RAW primary (via
  `INSERT OR IGNORE INTO photo_edit_recipes ... SELECT ... FROM ... WHERE
  photo_id = companion.id`) and then reassigns the companion's
  edit-history items to the RAW; if that companion's recipe already
  carries `local.mask.ref = "abcd"`, the on-disk snapshot family lives
  at `<companion_id>.abcd.{decode}.png`, and after pairing the RAW
  primary's renderer will look for `<raw_id>.abcd.{decode}.png` — which
  does not exist — and disable the local pass on every subsequent
  render of that recipe (and on every history entry that was reassigned).
  PR 1 therefore routes the pairing transfer through the same helper as
  bulk apply: the SELECT/INSERT is replaced with a Python-side load of
  the companion's `recipe_json`, strip-and-resnapshot against the RAW
  primary's own active mask (in the primary's decode bases), and write
  the resnapshotted recipe under the RAW primary's id. Edit-history
  items reassigned in the same block get the same resnapshot treatment
  (their stored `recipe_json`, if any, is re-snapshotted the same way).
  **Queued XMP sync payloads reassigned by pairing get the same
  treatment.** Pairing already transfers `pending_changes` rows from the
  companion to the RAW primary verbatim
  (`vireo/scanner.py:458-465` — a blind `UPDATE pending_changes SET
  photo_id`), so a companion `edit_recipe` row queued for XMP sync
  arrives at the RAW primary still carrying the companion's
  `<companion_id>.<ref>.{decode}.png` snapshot family in its
  `local.mask.ref`. The next `sync_to_xmp` run would then write a
  sidecar for the RAW primary that references a snapshot family living
  under the companion id — files that do not exist under the RAW
  primary — so external consumers and any later import that reads the
  sidecar would see the local block disabled on a photo whose in-Vireo
  render is happily using the freshly resnapshotted RAW-primary
  family. The pairing helper therefore includes `pending_changes.value`
  entries whose `change_type='edit_recipe'` in the same
  strip-and-resnapshot pass: each such row is loaded, its recipe is run
  through the same helper against the RAW primary's active mask (or
  the consumed-companion-mask path below when the primary has none),
  and the row is rewritten with the resnapshotted recipe before the
  `photo_id` reassignment commits. On the same "no viable basis"
  terminal case that drops `local` from the persisted recipe, the
  queued row's `local` block is stripped the same way so the sidecar
  export matches the on-disk render.
  **When the RAW primary has no usable mask of its own, pairing
  consumes the companion's mask before deleting the companion row**
  rather than dropping `local` outright. Concretely, the pairing
  helper reads the companion's `photo_masks` rows before the delete,
  and if the primary has none, it runs the same strip-and-resnapshot
  against the *companion's* active mask — materializing only the
  bases the primary's render chain can actually consume against a
  RAW-primary source (a `preserve_highlights` variant is skipped if
  the primary's `_load_raw` hits `used_embedded_fallback=True` at
  materialization time, matching §Snapshot decode basis; a `standard`
  variant is generated against the companion pixels *before* the
  companion file itself is removed, and remains valid afterwards
  because it lives under the RAW primary's photo id and its own
  content-addressed ref). Only when both sides fail — the RAW
  primary has no usable mask *and* the companion has none either, or
  the companion's mask exists but no viable decode basis remains for
  the RAW primary (e.g. RAW-only recipe with `used_embedded_fallback`
  and the companion is being removed as part of pairing so no
  `standard` basis will survive) — is `local` dropped from the
  transferred recipe (the same "skip and report" behavior the bulk
  endpoint uses, reported per-photo in the pairing job's SSE stream
  so the user learns which pairings lost their local block). This
  keeps the "add a RAW to a JPEG with local edits" flow from silently
  discarding the local adjustments in the common case where the
  companion is the only side that ever had a mask, while still
  guaranteeing pairing never leaves an orphaned ref behind. The
  contract is: no cross-photo transfer of a recipe carrying `local` may
  bypass the resnapshot helper, so a future new transfer site
  (auto-stack promotion, split-photo tooling, workspace copies) inherits
  the same guarantee by construction. This is a bulk-only concern only
  in the sense that a normal single-photo editor save writes the recipe
  under the same photo id that made the snapshot, so the ref already
  points at a valid on-disk family.

### Editor preview

The editor's server renders pick up local adjustments automatically (they
render the full recipe). The uncropped editor preview uses the
geometry-transformed-but-uncropped weight map, consistent with how crop is
previewed today.

## Phasing

1. **PR 1 — renderer + schema.** `local` normalization, mask snapshot
   plumbing + GC, weight-map builder, weighted tone + detail, API round-trip
   (including the bulk `/api/photos/edit-recipe/apply` per-target
   resnapshot described under Copy Settings, so the endpoint stays
   consistent from day one even before the PR 2 UI). Fully testable
   without UI (synthetic masks in tests).
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
