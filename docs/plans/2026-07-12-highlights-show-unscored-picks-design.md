# Highlights: show unscored photos (and picks) — design

**Date:** 2026-07-12
**Branch:** highlights-picks-not-showing

## Problem

On the Highlights page, photos the user explicitly flagged as **picks** can
silently disappear. Diagnosed on the `'anianiau` species bucket: the user had
3 picks, but only 1 appeared.

Root cause: `get_highlights_candidates` (`db.py`) hard-excludes any photo whose
`quality_score IS NULL`. Two of the three picks had never been quality-scored
(the folder is only partially analyzed — 18 of 90 `'anianiau` photos have
scores), so they were dropped from Highlights entirely — not mis-ranked,
invisible. The one scored pick *did* surface first, because the bucket scorer
already floats picks to the front (`_highlight_score_bucket(..., picked_first=True)`).

This violates `CORE_PHILOSOPHY.md` ("no black boxes"): a user action (pick)
produced a silently-vanishing result.

## Goal

Show all of a species' photos in Highlights — including unscored ones — while
keeping the analyzed, quality-ranked photos as the curated default. Picks are
always visible regardless of analysis state. Unscored photos are honestly
labeled as not-yet-analyzed rather than masquerading as quality-ranked
highlights.

## Decisions (validated with user)

1. **Picks always visible.** A flagged photo appears even with no quality
   score. Within the picks group, scored picks rank above unscored picks.
2. **Labeled boundary.** A divider — *"Not yet analyzed — N photos"* — marks
   where the quality-ranked photos end and the chronological unscored tail
   begins.
3. **Unscored tail in capture order**, earliest-first.
4. **Backfill the collapsed view to the limit** with unscored photos when there
   aren't enough picks/scored to fill it (avoids near-empty cards for
   mostly-unanalyzed species). The divider may therefore appear inside the
   collapsed card, which is fine — it's labeled.

## Design

### 1. Data source — let unscored photos in

`get_highlights_candidates` (`db.py`) currently gates:

```sql
AND p.quality_score IS NOT NULL
AND p.quality_score >= :min_quality
```

Change to:

```sql
AND (p.quality_score >= :min_quality
     OR (:min_quality <= 0 AND p.quality_score IS NULL))
```

- At the default `min_quality = 0`, unscored photos flow through.
- If the user raises the quality floor, unscored photos (no measured quality)
  drop out — a quality filter means "analyzed photos only."
- The `flag != 'rejected'` guard stays; rejects remain excluded.

This is the single source query feeding every Highlights path
(`_build_highlights_payload`, `api_highlights_bucket`, per-photo eligibility),
so they stay consistent. Life-list / best-photo use different candidate
queries and are untouched.

Each photo dict in `_collect_highlight_buckets` gains
`is_analyzed = quality_score is not None`, driving the ordering tier and the
frontend divider.

Consequence: a species whose photos are *all* unscored now forms a bucket (it
didn't before). It ranks at the bottom of the species list (best score ~0),
which is correct.

### 2. Ordering within a bucket

Extend the sort in `_highlight_score_bucket` (`picked_first=True` path) so the
ordered list is three contiguous regions:

1. **Picks** (flagged): scored first (by score desc), then unscored (capture
   time asc).
2. **Scored non-picks**: by `highlight_score` desc.
3. **Unscored non-picks**: capture time asc (earliest first).

New key:

```python
key = (
    0 if flagged else 1,                        # picks first
    0 if quality_score is not None else 1,      # scored before unscored
    -(highlight_score or 0),                    # ranks scored rows; inert for unscored (~0)
    (timestamp is None, timestamp or 0, id),    # ranks unscored chronologically; stable tiebreak
)
```

Because the scored-before-unscored tier separates the groups, the score term
only moves scored rows and the timestamp term only moves unscored rows — they
don't fight. `timestamp is None` pushes capture-time-less photos to the end of
their group.

Regression guards:
- The new tiers only bite when a list contains unscored photos. The
  `picked_first=False` caller draws from all-scored candidate sets, so its
  output is unchanged in practice — pinned by a regression test.
- The unidentified section uses the same scorer, so unscored unidentified
  photos get the same picks-first / chronological treatment. Intended.

### 3. Collapsed view and divider

**Collapsed view:** no slicing change. `limited_bucket` already takes
`photos[:limit_per_bucket]`; since picks → scored → unscored share one ordered
list, the slice auto-backfills with unscored photos. `has_more` stays
`len(photos) > limit`; "more" pages through the rest via existing offset
pagination in `api_highlights_bucket`.

**Divider:** two new bucket fields —
- `unanalyzed_count` — number of unscored non-pick photos in the bucket
- (per photo) `is_analyzed`

Frontend (`highlights.html`) renders a thin divider —
*"Not yet analyzed — {N} photos"* — immediately before the first rendered
photo where `!is_analyzed && flag !== 'flagged'`. The ordering guarantees the
unscored non-picks are one contiguous tail, so the condition fires exactly once.
Unscored picks (flagged, at the front) are skipped by the `!== 'flagged'`
clause. Works across pagination: wherever the boundary lands, the client sees
`is_analyzed` flip and draws the divider. No divider if `unanalyzed_count == 0`.
Reuse existing divider/subheading styling in `highlights.html`.

## Testing

`vireo/tests/test_app.py` (alongside existing highlights tests):

- Unscored photos appear in their species bucket; fully-unscored species ranks
  last.
- Ordering: bucket with scored picks, unscored picks, scored non-picks,
  unscored non-picks + shuffled timestamps → exact three-region order, unscored
  tails chronological.
- The `'anianiau` case: one scored pick + two unscored picks → all three
  render, scored pick first, both unscored picks ahead of every non-pick.
- `is_analyzed` and `unanalyzed_count` correct; divider-boundary photo is the
  first unscored non-pick.
- `min_quality > 0` excludes unscored photos.
- Regression pin: `_highlight_score_bucket(picked_first=False)` unchanged for an
  all-scored list.

## Edge cases

- Missing timestamp → end of its group.
- Unscored unidentified photos handled by the shared scorer.
- Bucket ranking driven by best analyzed score, so unanalyzed species sink.

## Risks

1. **Payload size / performance.** Relaxing the gate makes the candidate query
   return all non-rejected photos, not just scored ones. Workspace-scope views
   on large libraries build bigger in-memory buckets (`limit_per_bucket` caps
   what's rendered, not what's processed). Spot-check timing; don't optimize
   preemptively (YAGNI).
2. **Curation eligibility.** An unscored photo can now be chosen as a
   highlight/representative. Intended — you should be able to curate a pick
   before analysis — but noted so it's a conscious choice.
