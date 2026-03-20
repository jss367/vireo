# Photo Quality Scoring Pipeline Design

**Goal:** Automatically rank photos within burst groups to answer "here's your best picture of a nuthatch" — combining species classification, subject detection, and sharpness scoring in one pass.

**Key decisions:**
- MegaDetector for subject detection (bounding boxes)
- Subject sharpness is the dominant ranking signal (unfixable in post)
- Subject size is secondary (cropping loses resolution)
- Position and exposure computed for display but don't affect ranking
- Everything runs in the existing classification job — one button, one pass
- Quality scores live on the photo record, not tied to a classification run

---

## Pipeline Architecture

The classification job becomes a three-stage pipeline processing each photo once:

**Stage 1: Classify** — Load image, run BioCLIP, get species prediction. Group bursts by timestamp, compute consensus. (Existing.)

**Stage 2: Detect** — Run MegaDetector on the same loaded image. Get bounding box(es) for animals. Store box coordinates and detection confidence. If no animal detected, fall back to whole-image scoring.

**Stage 3: Score** — Using the bounding box:
- Subject sharpness: Laplacian variance within the bounding box
- Subject size: box area as percentage of frame
- Overall sharpness: whole-image Laplacian (tiebreaker)
- Position & exposure: computed and stored but weight = 0

Combined score: `0.7 * subject_sharpness_normalized + 0.3 * subject_size_normalized`

Weights configurable in settings. All individual scores stored so the UI can show breakdowns.

---

## MegaDetector Integration

MegaDetector v5 (YOLOv5-based, ~200MB) added to the model registry alongside BioCLIP. Downloaded via Settings > Models.

Output per photo: one or more bounding boxes with `{x, y, w, h}` (normalized 0-1), confidence, and category (animal/person/vehicle — we only use animal).

Multiple detections: use highest-confidence animal box for scoring. All detections stored.

No detection (confidence < 0.2): fall back to whole-image sharpness. Photo still gets classified.

Loaded once per job, reused across all photos.

---

## Database Changes

New columns on `photos` table:

```sql
detection_box      TEXT,   -- JSON: {"x": 0.1, "y": 0.2, "w": 0.5, "h": 0.6}
detection_conf     REAL,   -- MegaDetector confidence
subject_sharpness  REAL,   -- sharpness within bounding box
subject_size       REAL,   -- box area as fraction of frame
quality_score      REAL,   -- combined ranking score
```

Existing `sharpness` column stays as overall image sharpness. No new tables. Running quality scoring again overwrites scores.

---

## UI Changes

**Browse:** "Quality (best)" sort option. Quality score badge on cards. Detail panel shows score breakdown with bounding box visualization.

**Classify:** "Run Classification" becomes "Classify & Score." Progress shows per-photo. Summary: "548 classified, 412 detected, 38 groups, 12 best shots flagged."

**Review:** Quality score on prediction cards. Burst groups ordered by quality. Best-in-group indicator.

**Settings:** MegaDetector in Models section. Quality weight sliders. Detection threshold.

---

## Implementation Order

1. Add MegaDetector to model registry (download handler, settings entry)
2. Add database columns with migrations
3. Build `vireo/detector.py` wrapping MegaDetector
4. Integrate into classify job (detect + score in same pass)
5. Update UI (quality sort, score display, burst ordering)
6. Absorb standalone sharpness scoring into combined pipeline

---

## Future: Eye Detection

This pipeline provides the bounding box infrastructure needed for eye detection later. The path:
1. Within the animal bounding box, run an eye/keypoint detector
2. Measure sharpness specifically on the eye region
3. Eye sharpness becomes the dominant quality signal, replacing subject sharpness

The scoring infrastructure, database schema, and UI all carry over unchanged.
