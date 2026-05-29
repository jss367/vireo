# Content-addressed detection IDs

**Status:** design
**Date:** 2026-05-28
**Motivation:** Fix the detect-write race Codex flagged on PR #907 (SLOT_CAP=2).

## Problem

`detections.id` is `INTEGER PRIMARY KEY` (auto-rowid). `db.write_detection_batch(photo_id, detector_model, detections)` does:

```sql
DELETE FROM detections WHERE photo_id = ? AND detector_model = ?
INSERT INTO detections (...) VALUES (...)  -- returns new lastrowid each time
```

With SLOT_CAP=2, two pipelines can run the same photo concurrently. Pipeline A INSERTs, gets IDs `[101, 102]`, caches them in `detect_state`, and starts classify. Pipeline B then DELETEs (CASCADE removes A's predictions via the FK) and INSERTs `[103, 104]`. A's classify now writes `predictions(detection_id=101)` — but row 101 no longer exists. Either FK insert fails or, worse, B's later DELETE silently CASCADEs away predictions A wrote against rows B never knew about.

We already mitigate one slice of this with `acquire_photo_detect(photo_id)` in `pipeline_locks.py`, but a serialising lock is a workaround, not a fix. The underlying issue: detection rows have no stable identity. Two pipelines producing the same detections produce different IDs.

## Fix: content-addressed IDs

Compute each detection's ID as a hash of its content. Two pipelines that produce the same detection produce the same row. DELETE+INSERT becomes effectively an UPSERT — concurrent writers converge to the same state.

### Why content addressing (vs surrogate ID + UNIQUE constraint)

A detection isn't an entity with independent identity. Photos have identity — a user can rename, tag, edit one, and it's still that photo. A detection is a tuple: "the detector said there's an animal here, at these coordinates, in this photo, when run with this model." Two boxes at the same position, in the same photo, from the same detector, **are the same detection** — not two different detections that happen to share content. The natural key isn't just unique; it's constitutive. That's the signature of something that should be content-addressed.

A surrogate-ID + `UNIQUE (photo_id, detector_model, box_x, box_y, box_w, box_h, category)` constraint would also fix the race, but every code path that touches detection rows would still need to think about the surrogate vs. natural distinction. Content addressing collapses the two — there is only one identity.

## ID formula

```python
def positive_int_hash(*parts: str) -> int:
    """52-bit SHA-256 digest, JS-safe-int by construction."""
    digest = hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()
    return int(digest[:13], 16)  # 13 hex chars = 52 bits

def detection_id(photo_id: int, detector_model: str, box, category: str) -> int:
    qx, qy, qw, qh = [f"{round(v, 4):.4f}" for v in (box.x, box.y, box.w, box.h)]
    return positive_int_hash(str(photo_id), detector_model, qx, qy, qw, qh, category)
```

### Natural key composition

| Field             | In hash? | Reason |
|-------------------|----------|--------|
| `photo_id`        | yes      | Different photos = different detections. |
| `detector_model`  | yes      | Future `megadetector-v7` should coexist with v6 on identical boxes. |
| `box_x/y/w/h`     | yes      | Quantized to 4 decimals (see below). |
| `category`        | yes      | Part of model output. If the model flips animal↔person on the same box across runs, those are different rows so DELETE+INSERT retires the stale one. |
| `detector_confidence` | no   | Float; drifts between runs; not identity. |
| `created_at`      | no       | Would defeat the point. |

### Bbox quantization: 4 decimal places

Bbox coords are normalized to `[0, 1]`. We quantize before hashing to absorb ONNX float drift between providers (CUDA / CoreML / CPU produce mantissa-bit-level differences on identical inputs).

| Precision | Pixel slop on 4K image | Verdict |
|-----------|------------------------|---------|
| 3 decimals | ~4 px | Too coarse — risks collision between adjacent small-bird boxes. |
| **4 decimals** | **~0.4 px** | **Chosen.** Comfortably above ONNX drift (~1e-6); comfortably below NMS-enforced separation. |
| 5 decimals | ~0.04 px | Vulnerable to inter-provider float drift. |
| 6 decimals | sub-pixel | Definitely vulnerable. |

### Bit width: 52 bits

JavaScript `Number.MAX_SAFE_INTEGER` is `2^53 - 1`. A 63-bit integer (SQLite's positive INTEGER range) loses precision the moment it round-trips through `JSON.parse`. Truncating SHA-256 to 52 bits keeps every value exactly representable in JS — no string-serialisation gymnastics, no future foot-gun if some new endpoint sends detection IDs to the frontend.

Collision math: birthday-paradox 50% collision in a 2^52 space requires ~95M items. Vireo's lifetime detection count is in the millions, not billions — collision probability ~10⁻⁹. Effectively zero.

## Schema change

```sql
-- Before:
CREATE TABLE detections (
    id INTEGER PRIMARY KEY,
    ...
)

-- After: same DDL. SQLite's INTEGER PRIMARY KEY accepts explicit values;
-- the AUTOINCREMENT-like auto-rowid behavior only kicks in when the INSERT
-- omits `id`. We just stop omitting it.
```

No migration. Old auto-rowid rows (small integers) coexist with new hash rows (large integers) in the same column. The first time `write_detection_batch` runs for a (photo, model) pair after the change, its DELETE wipes the old rows and the INSERT writes new content-addressed ones. Existing CASCADE FKs handle the predictions cleanup — that's identical to today's re-detect semantics.

## write_detection_batch change

```python
def write_detection_batch(self, photo_id, detector_model, detections):
    # No more DELETE+INSERT — UPSERT by computed ID.
    ids = []
    for det in detections:
        det_id = detection_id(photo_id, detector_model, det.box, det.category)
        self.conn.execute("""
            INSERT INTO detections
              (id, photo_id, detector_model, box_x, box_y, box_w, box_h,
               detector_confidence, category)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
              detector_confidence = excluded.detector_confidence
        """, (det_id, photo_id, detector_model, det.box.x, det.box.y,
              det.box.w, det.box.h, det.confidence, det.category))
        ids.append(det_id)
    # detector_runs ON CONFLICT update stays as-is.
    ...
    return ids
```

Two pipelines concurrently writing the same (photo, model) now produce identical IDs and the same row content. `INSERT ... ON CONFLICT(id) DO UPDATE` is idempotent for matching rows and crucially does NOT fire `ON DELETE CASCADE` on `predictions.detection_id` the way `INSERT OR REPLACE` would (replace deletes the conflicting row, then inserts). Stale detections (boxes the model no longer produces) need explicit cleanup — handled by deleting any existing row whose ID is *not* in the new ID set:

```python
new_ids = set(ids)
existing = self.conn.execute(
    "SELECT id FROM detections WHERE photo_id = ? AND detector_model = ?",
    (photo_id, detector_model),
).fetchall()
stale = [row["id"] for row in existing if row["id"] not in new_ids]
if stale:
    placeholders = ",".join("?" for _ in stale)
    self.conn.execute(
        f"DELETE FROM detections WHERE id IN ({placeholders})", stale,
    )
```

This DELETE is narrow (only stale-by-content rows), runs *after* the INSERTs, and is safe under concurrent writers: if pipeline A and B compute the same `new_ids`, neither deletes anything the other inserted.

## Lock cleanup

`acquire_photo_detect(photo_id)` in `pipeline_locks.py` and its call site in `pipeline_job.py` become unnecessary — the race is fixed at the data layer, not by serialisation. Remove the lock and its tests (or leave the lock as belt-and-suspenders; lean toward remove to avoid dead infrastructure).

## Test surface

- **New:** unit test for `detection_id()` — same inputs ⇒ same ID; quantization absorbs sub-quarter-pixel drift; different category / different model / different photo ⇒ different ID.
- **New:** regression test in `vireo/tests/test_db.py` — two threads calling `write_detection_batch(same_photo, same_model, same_detections)` concurrently; assert row count, ID set, and predictions FK validity.
- **Update:** `vireo/tests/test_classify_job.py` mocks. Lines 194, 511, 214, 254, 530-532, 944, 1837-1839 use hardcoded `[101]`, `[101, 102]`, `999` for detection IDs. Replace with `detection_id(...)` computed from the same inputs the mock receives.
- **Remove:** `test_pipeline_locks.py::test_photo_detect_lock_*` (3 tests) if we delete the lock.

## Audit summary (from explore agent, 2026-05-28)

| Surface | Status | Notes |
|---------|--------|-------|
| `db.write_detection_batch` | rewrite | Only producer. |
| `classify_job.detect_state` consumer | safe | Stores IDs in dict; no arithmetic. |
| `predictions.detection_id` FK | safe | Works with any INTEGER value. |
| `app.py` SQL queries on detections.id | safe | Parameterised; no `MAX(id)`, no `BETWEEN`. |
| `app.py` JSON returning detection IDs | safe at 52 bits | At 63 bits this would be a JS precision bug. |
| `vireo/templates/*.html` JS | safe | No arithmetic on detection IDs. |
| `test_classify_job.py` mock values | update | Hardcoded small IDs need to use `detection_id()`. |

## Out of scope

- Other tables with the same DELETE+INSERT pattern (predictions, keypoints, etc.). Address those if/when concurrency exposes them; don't rewrite the data layer speculatively.
- Migration of existing auto-rowid rows. Lazy replacement on next re-detect, per the [[project_solo_user_app]] philosophy.

## Sequencing

This PR lands **before** the SLOT_CAP=2 lift (PR #907) so the cap lift becomes safe. After merge, rebase #907 onto main; the `acquire_photo_detect` removal and content-addressed IDs together replace the lock-based mitigation #907 currently relies on.
