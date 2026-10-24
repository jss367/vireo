"""Review evidence: relative scores never become a correctness probability."""

from __future__ import annotations

import math
from typing import TYPE_CHECKING
from urllib.parse import urlencode

import match_confidence
from services.service_types import JsonDict
from species_identity import SpeciesResolver

if TYPE_CHECKING:
    from db import Database

DECISIONS = frozenset({'unreviewed', 'not_enough_evidence', 'invalid_subject', 'subject_valid'})


def geographic_context(row: JsonDict, prediction: JsonDict) -> JsonDict:
    """GPS supplies context, never an inferred species range or exclusion."""
    lat, lon = row.get('latitude'), row.get('longitude')
    valid = (
        isinstance(lat, (float, int)) and isinstance(lon, (float, int))
        and math.isfinite(lat) and math.isfinite(lon)
        and -90 <= lat <= 90 and -180 <= lon <= 180
    )
    if not valid:
        return {'state': 'unknown', 'explanation': 'Geographic plausibility unknown: no usable catalog GPS.'}
    params = {'lat': lat, 'lng': lon, 'radius': 50}
    name = prediction.get('scientific_name') or prediction.get('species')
    if name:
        params['taxon_name'] = name
    return {
        'state': 'unknown', 'latitude': lat, 'longitude': lon,
        'source': 'Catalog GPS (camera metadata or manual assignment)',
        'observations_url': 'https://www.inaturalist.org/observations?' + urlencode(params),
        'explanation': (
            'Geographic plausibility unknown: no validated species-range assessment. '
            'Nearby iNaturalist observations are context, not a complete range; '
            'missing sightings do not exclude a species. Check location accuracy, season and vagrants.'
        ),
    }


def _subject_context(prediction: JsonDict, context: JsonDict) -> JsonDict:
    full_image = prediction.get('detector_model') == 'full-image'
    return {
        'decision': context.get('decision', 'unreviewed'),
        'revision': context.get('revision', 0),
        'detector_model': prediction.get('detector_model'),
        'detector_score': prediction.get('detector_confidence'),
        'category': context.get('subject_category'),
        'explanation': (
            'Full-image classification: no localized animal evidence.' if full_image else
            'Detector score measures proposed region strength, not species correctness or proof of an animal.'
        ),
    }


def _review_reasons(prediction: JsonDict, evidence: JsonDict) -> list[str]:
    reasons = []
    decision = evidence['subject']['decision']
    if decision == 'not_enough_evidence':
        reasons.append('Not enough evidence — revisit identification')
    if decision == 'invalid_subject':
        reasons.append('Reviewer marked region as not an identifiable animal')
    if evidence['disagreement']:
        reasons.append('Models propose different species for this region')
    if prediction.get('effective_category') == 'disagreement':
        reasons.append('Prediction conflicts with current species keywords')
    if decision != 'subject_valid':
        score = prediction.get('detector_confidence')
        category = evidence['subject']['category']
        if prediction.get('detector_model') == 'full-image' or score is None or score < 0.5:
            reasons.append('Subject region needs inspection')
        elif category and str(category).lower() not in ('animal', 'bird', '1'):
            reasons.append('Detector did not label this region as an animal')
    if evidence['match']['state'] == match_confidence.UNLISTED:
        reasons.append('Best raw match is below this model’s configured match threshold')
    if prediction.get('status') == 'pending' and (prediction.get('confidence') or 0) >= 0.95:
        reasons.append('High relative score — audit for a possible high-score error')
    return reasons


def attach_prediction_evidence(db: Database, predictions: list[JsonDict], config: JsonDict) -> None:
    """Batch subject evidence and cross-model names, independent of row filters."""
    if not predictions:
        return
    siblings = [dict(row) for row in db.predictions.get_rows(
        photo_ids=list({p['photo_id'] for p in predictions}),
    )]
    contexts = db.predictions.evidence_context([p['id'] for p in predictions])
    resolver = SpeciesResolver(db=db)
    # Compare each model's top raw candidate on the SAME detection. Two
    # animals in one photo do not establish cross-model disagreement.
    tops: dict[int, dict[str, JsonDict]] = {}
    for sibling in siblings:
        model = sibling['model']
        models = tops.setdefault(sibling['detection_id'], {})
        if model not in models or (sibling.get('confidence') or 0) > (models[model].get('confidence') or 0):
            models[model] = sibling
    for prediction in predictions:
        context = contexts.get(prediction['id'], {})
        candidates = list(tops.get(prediction['detection_id'], {}).values())
        identities = {resolver.prediction(p).key for p in candidates}
        evidence = {
            'score': {
                'model': prediction['model'], 'kind': 'candidate_relative_softmax',
                'value': prediction.get('confidence'), 'label_count': context.get('label_count'),
                'raw_value': prediction.get('match_score'), 'raw_kind': context.get('score_kind'),
                'calibrated_correctness': False,
                'explanation': 'Softmax share within this model’s candidate list; not a probability of correct identification. Scores from different models or label lists are not directly comparable.',
            },
            'subject': _subject_context(prediction, context),
            'disagreement': len(identities) > 1,
            'model_candidates': [{
                'model': p['model'], 'species': p['species'],
                'relative_score': p['confidence'],
            } for p in candidates],
            'match': match_confidence.assess(
                prediction['model'], context.get('score_kind'),
                context.get('max_match_score'), context.get('match_margin'), config,
            ),
            'geography': geographic_context(context, prediction),
        }
        evidence['reasons'] = _review_reasons(prediction, evidence)
        prediction['evidence'] = evidence
