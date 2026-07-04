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
`'embedded_jpeg'`), written by **every** path that materializes
`working_copy_path`. Scanner's happy RAW extraction writes `'raw'`
when `_load_raw` returned a demosaiced result, or `'embedded_jpeg'`
when it returned via the libraw-failure embedded-JPEG fallback. This
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
branches. Weight-map basis selection reads this column directly:
`'raw'` shares the `preserve_highlights` basis, `'companion'` shares
`standard`, and `'embedded_jpeg'` has no snapshot basis at all
because the embedded preview's crop/aspect against the sensor is
per-camera and not recorded anywhere Vireo can map to. Rows migrated
from before the column existed carry a NULL value and are treated as
unknown provenance — the local pass is disabled with the same
warn-and-hold-zero fallback as a missing snapshot. For NULL rows the
scanner's existing `_working_copy_candidate_predicate` skips them
(its `working_copy_path IS NULL` clause excludes any row that already
has a working copy on disk) and the on-demand `/original` route
trusts an existing full-res working copy without regenerating
(`vireo/app.py:18789-18793`), so the column would stay NULL
indefinitely. PR 1 therefore adds an explicit backfill: on the same
schema migration that adds the column, a one-time job walks every row
where `working_copy_path IS NOT NULL AND working_copy_source IS NULL`
and re-runs the extraction logic against the current
source/companion state to write both the working copy (replacing the
existing file only if the fresh extraction changed the pixel grid) and
the new column. The job reuses `_extract_working_copies`' machinery
via a temporary predicate that ignores the `working_copy_path IS
NULL` clause, so all three flavors are labelled correctly on the
next startup after the migration, and the local pass recovers
without waiting for user-driven scans. `'embedded_jpeg'` rows stay
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
  and no snapshot basis can be trusted to line up.
- **RAWs with a companion JPEG:** two variants —
  `preserve_highlights` (for the primary RAW path *and* working copies
  with `photos.working_copy_source='raw'`) *and* `standard` (for the
  companion-fallback path *and* working copies with
  `photos.working_copy_source='companion'`, where the working copy was
  re-extracted from the companion JPEG after RAW extraction failed).
  `working_copy_source='embedded_jpeg'` rows disable the local pass
  the same way as above.

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
- `standard` (RAW+JPEG only) — a hash over the companion JPEG's
  file identity (mtime + size + sha1), the prompt/detector version used
  when detection re-ran on the companion, and the active SAM
  variant/model name used to segment that re-run. Switching SAM variants
  (`sam2-small` → `sam2-large`) with unchanged companion bytes must
  invalidate this entry, so the SAM variant is a first-class digest input
  on both bases.
Staleness for a variant means the current inputs' re-computed digest
differs from that entry's `source_digest`; the editor surfaces "Newer
subject mask available — Update" whenever **any** variant is stale, and
Update **always rotates `local.mask.ref` to a fresh content-addressed
value** and writes a complete new family under it — regenerating the
variants whose digests changed and **copying the untouched variant files
byte-for-byte** to the new ref's filenames. The old ref's files stay
intact on disk (they are not deleted or overwritten by Update); they are
still referenced by any edit-history recipe whose snapshot they were
made for, so re-rendering those historical recipes remains deterministic
and byte-identical. The old ref eventually GCs away on its own once no
current or history recipe references it. Rotating in place — mutating
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
   when the RAW decoded successfully *or* when the render fell back to a
   RAW-derived working-copy JPEG (`photos.working_copy_source='raw'`:
   `extract_working_copy` decoded the RAW with
   `RAW_DECODE_PRESERVE_HIGHLIGHTS`, so its pixel grid shares the
   preserve-highlights basis); `standard` when the render fell back to
   the companion JPEG, when the working copy is companion-derived
   (`photos.working_copy_source='companion'` — re-extracted from the
   companion JPEG after RAW extraction failed by scanner or by the
   on-demand `/original` route, so its pixel grid is the companion's
   standard-decode basis), or when the photo is non-RAW. A working copy
   with `working_copy_source='embedded_jpeg'` (RAW decode fell through
   to the embedded preview inside `_load_raw`) or a NULL value (a
   legacy row from before the column existed and before the migration
   backfill has run) is treated as unknown basis and the local pass is
   disabled with warn-and-hold-zero, exactly as with a missing
   snapshot. NULL rows recover automatically once the migration
   backfill described in §Snapshot decode basis populates the column;
   `'embedded_jpeg'` rows stay disabled by design. `recipe_render_source` already
   distinguishes these paths internally, so it returns the effective basis
   alongside the source path for the weight-map builder to consume.
   `recipe_render_source`'s return value is only the *initial* basis, and
   **every** recipe render call site that can late-swap the loaded image
   has to update the basis in lock-step with the swap. Today those sites
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
