"""SQL fragments the photo queries share across repositories.

The column lists a photo row is read with (``PHOTO_COLS``, and
``PHOTO_DETAIL_COLS`` for the detail view), the date orderings, the
Life List ancestor suppression clause, the displayed-prediction confidence
expression and the inclusive date bound. ``CollectionRepository``,
``PhotoRepository``, ``LocationRepository``, ``PredictionRepository`` and
``SpeciesCurationRepository`` import them from here.
"""

from repositories.prediction_review import prediction_review_join, prediction_review_status

# Columns to return in photo list queries (excludes large fields)
PHOTO_COLS = """id, folder_id, filename, extension, file_size, file_mtime, xmp_mtime,
                timestamp, width, height, rating, flag, thumb_path, sharpness,
                subject_sharpness, subject_size, quality_score,
                latitude, longitude, companion_path, working_copy_path,
                wildlife_excluded, miss_no_subject, miss_clipped, miss_oof,
                camera_make, camera_model, iso, lens, focal_length, aperture"""


# Columns for single-photo detail queries (includes exif_data JSON +
# eye-focus fields consumed by the review lightbox's crosshair overlay)
PHOTO_DETAIL_COLS = (
    PHOTO_COLS
    + ", exif_data, eye_x, eye_y, eye_conf, eye_tenengrad,"
    + " working_copy_failed_at, working_copy_failed_mtime,"
    + " working_copy_failed_source"
)


# Life-list ancestor suppression. Broadening the identification-keyword
# rank guard admits linked genus/family/class rows, but Lightroom catalog
# imports copy every ``includeParents`` ancestor onto the photo as a flat
# keyword (see ``vireo/catalog.py``) and the classifier already treats
# ancestor tags as "broader labels for the same taxon" rather than
# separate identifications (see ``_can_auto_accept_detection_prediction``
# in ``vireo/classify_job/store_predictions.py``). Without a hierarchy filter, a normal
# species-tagged robin would also inflate ``Turdus`` / ``Turdidae`` /
# ``Aves`` Life List buckets on those catalogs.
#
# Suppress a linked higher-rank taxonomy keyword on a photo whenever the
# same photo carries another linked taxonomy keyword whose taxon is a
# strict descendant of it. Species-rank keywords are never suppressed;
# unlinked keywords (``taxon_id IS NULL``) can't be checked for ancestry
# and pass through unchanged. The clause references outer aliases
# ``pk`` (photo_keywords) and ``k`` (keywords); callers don't need to
# join ``taxa`` themselves because the rank/ancestry checks resolve
# ``k.taxon_id`` via inner subqueries.
LIFE_LIST_ANCESTOR_SUPPRESSION_CLAUSE = """
    AND NOT (
        k.taxon_id IS NOT NULL
        AND EXISTS (
            SELECT 1 FROM taxa t_sup_rank
            WHERE t_sup_rank.id = k.taxon_id
              AND t_sup_rank.rank IS NOT NULL
              AND t_sup_rank.rank != 'species'
        )
        AND EXISTS (
            SELECT 1 FROM photo_keywords pk_sup
            JOIN keywords k_sup ON k_sup.id = pk_sup.keyword_id
             AND (k_sup.is_species = 1 OR k_sup.type = 'taxonomy')
             AND k_sup.taxon_id IS NOT NULL
            WHERE pk_sup.photo_id = pk.photo_id
              AND k_sup.id != k.id
              AND k.taxon_id IN (
                  WITH RECURSIVE anc(id) AS (
                      SELECT parent_id FROM taxa
                       WHERE id = k_sup.taxon_id
                         AND parent_id IS NOT NULL
                      UNION ALL
                      SELECT t_anc.parent_id
                      FROM taxa t_anc JOIN anc a ON t_anc.id = a.id
                       WHERE t_anc.parent_id IS NOT NULL
                  )
                  SELECT id FROM anc
              )
        )
    )
"""


def inclusive_date_to(date_to):
    """Pad a date_to bound so it includes sub-second timestamps.

    The frontend sends either 'YYYY-MM-DD' (date picker) or
    'YYYY-MM-DDTHH:MM:SS' (timeline click).  With sub-second precision
    timestamps like '23:59:59.500000', a naive '<= 23:59:59' comparison
    excludes them.  Append '.999999' so the bound covers the full second.
    Fractional seconds shorter than 6 digits are padded with '9's so that
    lexical comparison remains inclusive (e.g. '.5' → '.599999').
    """
    if date_to is None:
        return None
    if not isinstance(date_to, str):
        return None
    if len(date_to) == 10:  # bare date
        return date_to + "T23:59:59.999999"
    if len(date_to) == 19:  # date + time, no fractional seconds
        return date_to + ".999999"
    # Has fractional seconds — pad to 6 digits with '9' for inclusive upper bound
    dot_idx = date_to.rfind(".")
    if dot_idx >= 0:
        frac = date_to[dot_idx + 1:]
        if len(frac) < 6:
            return date_to + "9" * (6 - len(frac))
    return date_to


PHOTO_DATE_ASC_ORDER = "p.timestamp IS NULL, p.timestamp ASC, p.filename ASC, p.id ASC"


PHOTO_DATE_DESC_ORDER = "p.timestamp IS NULL, p.timestamp DESC, p.filename ASC, p.id ASC"


# A photo's "prediction confidence" is the score of the one prediction the UI
# presents as its strongest guess — not "some prediction row exists with this
# score". The eligibility gates are exactly the ones every other surface that
# speaks for the *displayed* prediction applies, so the sort key and the badge
# cannot rank a photo on a guess Browse hides:
#
# * ``species IS NOT NULL`` — a detection-only row ("animal", no species) is
#   not a species guess and has no confidence to rank by.
# * ``detector_confidence >= <workspace floor>`` — detections under the
#   floor are hidden everywhere (``detection_store.get_detections_for_photos``, the
#   dashboard counters, ``_prediction_exists``), so their predictions must
#   not position a card either.
# * status not ``rejected`` — a guess the user threw away must not keep the
#   photo at the top of a confidence sort.
# * status not ``alternative`` — runner-up rows are dropped from top-level
#   results by ``/api/predictions``, matching ``_prediction_exists``.
# * newest ``labels_fingerprint`` per (detection, classifier) — the same pin
#   ``get_top_prediction_for_photo`` and ``_prediction_exists`` use, so a
#   reclassified photo ranks on its current label set rather than on a
#   stale row left behind by the previous one.
#
# Correlates on the outer ``p.id``, so every caller must expose ``photos``
# (or a projection carrying ``id``) as ``p``. Takes the two parameters
# ``Database.top_prediction_confidence_params()`` returns, in that order.
# It is deliberately a correlated subquery rather than a grouped CTE: at 88k
# photos / 192k predictions the CTE form materializes every photo in the
# catalog (~0.35s even for a 60-photo folder), while the correlated form is
# index-driven per scoped row — 0.03s on a small workspace and no slower
# than the CTE on the largest one.
TOP_PREDICTION_CONFIDENCE_EXPR = f"""(
            SELECT MAX(conf_pr.confidence)
            FROM detections conf_d
            JOIN predictions conf_pr ON conf_pr.detection_id = conf_d.id
            {prediction_review_join('conf_pr', review_alias='conf_prv')}
            WHERE conf_d.photo_id = p.id
              AND conf_d.detector_confidence >= ? AND conf_d.subject_verdict IS NULL
              AND conf_pr.species IS NOT NULL
              AND {prediction_review_status('conf_prv')}
                  NOT IN ('rejected', 'alternative')
              AND conf_pr.labels_fingerprint = (
                  SELECT conf_pr2.labels_fingerprint FROM predictions conf_pr2
                  WHERE conf_pr2.detection_id = conf_pr.detection_id
                    AND conf_pr2.classifier_model = conf_pr.classifier_model
                  ORDER BY conf_pr2.created_at DESC, conf_pr2.id DESC
                  LIMIT 1
              )
        )"""
