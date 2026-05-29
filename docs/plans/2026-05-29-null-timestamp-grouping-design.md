# Null-timestamp grouping + warning

## Problem

Photos with `timestamp IS NULL` (scan I/O errors, unreadable files) each became
their own singleton encounter, and all sorted to the very top of the pipeline
review page. Cause:

- `cut_microsegments` / `detect_bursts` sort key falls back to `datetime.min`
  for null timestamps, piling them at the start of the timeline.
- `_time_delta_seconds(None, x)` returns `inf`, so the hard-time-cut at
  `dt > hard_cut_time` fires on every adjacent pair touching a null timestamp →
  every null-timestamp photo becomes its own encounter.

Result reads as "the whole pipeline failed to group anything" because the
singleton wall is the first thing the user sees.

## Design

### 1. Sort null timestamps to the end, ordered by file

In `cut_microsegments` (encounters.py) and `detect_bursts` (bursts.py), change
the sort key so timestamped photos sort first by real time, and null-timestamp
photos sort last, ordered by `(folder_id, filename)`. Consecutive NEFs from one
shoot stay adjacent even without EXIF.

### 2. Don't cut between two null-timestamp photos

In `cut_microsegments`, add a `both_null` branch *before* the time/score cuts:
when both adjacent photos have null timestamps, keep them in the same segment.
Rationale: unreadable files have no timestamp AND no embeddings/species/meta
(the detector never ran on them), so every similarity signal is ~0 and the
score cut would otherwise split them into singletons. With no reliable basis to
separate them, group contiguous nulls by file-order adjacency.

Asymmetric pairs (one null, one real) are unaffected: `dt = inf > hard_cut_time`
still fires a clean cut, so the null cluster never contaminates a real encounter.

`_time_delta_seconds` is **not** changed — it's shared with the merge stage
(`compute_s_seg`, `_merge_microsegments_with_map`) where a `None` return would
crash. `compute_s_enc` already drops the time signal for nulls
(`used["time"] = dt != inf`), so no change needed there either.

### 3. Surface a per-encounter warning

`serialize_results` (pipeline.py) adds `missing_timestamp_count` to each
serialized encounter (count of photos with falsy timestamp). The review page
renders a ⚠ badge in the encounter header when the count is non-zero, with a
tooltip explaining the photos lack EXIF timestamps and are grouped by file
order.

## Effect on real data (ws16)

The 16 hardware-unreadable NEFs in `2026-05-25/` (`DSC_8039`–`8056`) collapse
from 16 singletons into one "missing metadata" encounter at the bottom of the
page, flagged with the warning.

## Tests

- `test_encounters.py`: both-null no cut; asymmetric still cuts; sort order
  puts nulls last; contiguous nulls form one segment.
- `test_bursts.py`: sort order parity.
- `test_pipeline.py`: `missing_timestamp_count` present and correct; zero when
  all timestamped.
