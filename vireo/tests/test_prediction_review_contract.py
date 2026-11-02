"""Review-aware prediction reads must use the shared workspace SQL fragments.

Raw classifier output is legitimate for inference caches, coverage, identity
repair and writers. Keep that exception inventory explicit: a new reader must
choose the shared review join or document why its result ignores decisions.
Counts stop an existing exception from silently admitting an extra raw query.
"""

import ast
import re
from collections import Counter
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
HELPER = "repositories/prediction_review.py"

# These protect global immutable output from deletion/replacement whenever ANY
# workspace has a manual decision, or carry ALL workspaces' reviews on pairing.
# Scoping them to the active workspace would lose another workspace's decisions.
CROSS_WORKSPACE_REVIEWS = {
    "repositories/companion_pairing.py:CompanionPairingRepository.carry_prediction_reviews_to_survivors",
    "repositories/computation_cache.py:ComputationCacheRepository.manual_review_exists",
    "repositories/model_runs.py:ModelRunsRepository.detector_run_is_pinned",
    "repositories/model_runs.py:ModelRunsRepository.get_detector_run_photo_ids",
    "repositories/model_runs.py:ModelRunsRepository.get_classifier_run_keys",
    "repositories/model_runs.py:ModelRunsRepository.get_classifier_run_key_gate",
    "repositories/model_runs.py:ModelRunsRepository.classifier_cache_run_filter",
    "repositories/model_runs.py:_runtime_predicate",
}

# Existing classifier-output, coverage, identity, id/scope lookup and writer
# reads. These do not present workspace review state. Keep their query counts
# fixed; a new reader must explicitly opt out of workspace review semantics.
RAW_READS = {
    # Scope/id and latest-label lookups embedded in otherwise reviewed reads.
    'repositories/collections.py:_RuleQueryBuilder._prediction_exists': 1,
    'repositories/keyword_provenance.py:_reject_detection_alternatives': 1,
    'repositories/keyword_provenance.py:_live_neighbour_species': 1,
    'repositories/predictions.py:PredictionRepository.get_rows': 1,
    'repositories/predictions.py:PredictionRepository.subject_accept_target': 1,
    # Upgrade discovery reads the immutable migration sentinel, not species
    # decisions: accepting/rejecting a prediction does not regenerate raw boxes.
    'repositories/predictions.py:PredictionRepository.legacy_detection_folders': 1,
    # Classified-photo and unscored-output counts are independent of decisions.
    'repositories/stats.py:StatsRepository._dashboard_classification_counts': 2,
    'repositories/collections.py:CollectionRepository._suggest_field_sql': 1,
    'repositories/companion_pairing.py:CompanionPairingRepository.carry_prediction_reviews_to_survivors': 1,
    'repositories/computation_cache.py:ComputationCacheRepository.delete_run_predictions': 1,
    'repositories/computation_cache.py:ComputationCacheRepository.manual_review_exists': 1,
    'repositories/computation_cache.py:ComputationCacheRepository.run_predictions': 1,
    'repositories/culling.py:CullingRepository.top_prediction': 1,
    'repositories/detections.py:DetectionsRepository.get_predictions': 1,
    'repositories/detections.py:DetectionsRepository.workspace_miss_rows': 1,
    'repositories/edit_history.py:EditHistoryRepository.ensure_prediction_instances': 1,
    'repositories/edit_history.py:EditHistoryRepository.prediction_scope': 1,
    'repositories/edit_history.py:EditHistoryRepository.undo_prediction_accept_statuses': 2,
    'repositories/masks_features.py:MasksFeaturesRepository.list_photos_for_eye_keypoint_stage': 1,
    'repositories/model_runs.py:CACHE_PREDICTIONS_EXIST_SQL': 1,
    'repositories/model_runs.py:ModelRunsRepository.classifier_cache_run_filter': 1,
    'repositories/model_runs.py:ModelRunsRepository.current_prediction_detector_confidences': 1,
    'repositories/model_runs.py:ModelRunsRepository.detector_run_is_pinned': 1,
    'repositories/model_runs.py:ModelRunsRepository.get_classifier_run_key_gate': 1,
    'repositories/model_runs.py:ModelRunsRepository.get_classifier_run_keys': 1,
    'repositories/model_runs.py:ModelRunsRepository.get_detector_run_photo_ids': 1,
    'repositories/model_runs.py:ModelRunsRepository.get_match_scores_for_photo': 1,
    'repositories/model_runs.py:ModelRunsRepository.get_unscored_current_prediction_runs': 1,
    'repositories/model_runs.py:ModelRunsRepository.release_incomplete_classifier_run': 1,
    'repositories/model_runs.py:_CACHE_HIT_OUTPUT_SQL': 1,
    'repositories/model_runs.py:_runtime_predicate': 1,
    'repositories/moves_merge.py:MovesMergeRepository.query_rule_matches': 2,
    'repositories/photos.py:PhotoRepository._browse_summary_aggregates': 1,
    'repositories/pipeline_features.py:PipelineFeaturesRepository.count_eye_keypoint_attempts': 1,
    'repositories/pipeline_features.py:PipelineFeaturesRepository.prediction_rows': 2,
    'repositories/predictions.py:PredictionRepository._clear_prediction_chunk': 1,
    'repositories/predictions.py:PredictionRepository.count_in_workspace': 1,
    'repositories/predictions.py:PredictionRepository.evidence_context': 1,
    'repositories/predictions.py:PredictionRepository.get_existing_photo_ids': 2,
    'repositories/predictions.py:PredictionRepository.get_for_photo': 2,
    'repositories/predictions.py:PredictionRepository.get_scope': 1,
    'repositories/predictions.py:PredictionRepository.get_top_for_photo': 2,
    'repositories/predictions.py:PredictionRepository.get_top_per_detection_for_photo': 1,
    'repositories/predictions.py:PredictionRepository.id_for_key': 1,
    'repositories/predictions.py:PredictionRepository.out_of_workspace_ids': 1,
    'repositories/predictions.py:PredictionRepository.photo_ids_by_prediction': 1,
    'repositories/predictions.py:PredictionRepository.reconcile_match_review_state': 1,
    'repositories/predictions.py:PredictionRepository.require_identifiable_subjects': 1,
    'repositories/predictions.py:PredictionRepository.retain_candidates': 2,
    'repositories/predictions.py:PredictionRepository.superseded_ids': 1,
    'repositories/predictions.py:PredictionRepository.update_status_by_photo': 1,
    'repositories/species_identity_repairs.py:SpeciesIdentityRepairRepository._unstamped': 1,
    'repositories/species_identity_repairs.py:SpeciesIdentityRepairRepository.bioclip_correction_candidates': 1,
    'repositories/species_identity_repairs.py:SpeciesIdentityRepairRepository.has_unstamped_legacy_prediction': 1,
    'repositories/species_identity_repairs.py:SpeciesIdentityRepairRepository.has_unstamped_prediction_for_fingerprint': 1,
    'repositories/species_identity_repairs.py:SpeciesIdentityRepairRepository.unstamped_legacy_species': 1,
    'repositories/stats.py:StatsRepository.classification_inventory_counts': 1,
    'repositories/stats.py:StatsRepository.count_eye_keypoint_attemptable': 1,
    'repositories/stats.py:StatsRepository.count_eye_keypoint_eligible': 1,
    'repositories/stats.py:StatsRepository.count_eye_keypoint_stale': 1,
    'repositories/stats.py:StatsRepository.get_coverage': 1,
    'repositories/stats.py:StatsRepository.get_folder_coverage': 1,
    'repositories/stats.py:StatsRepository.sampled_top1_medians': 1,
    'repositories/workspace_folders.py:WorkspaceFolderRepository._move_prediction_reviews': 2,
}


def _scopes(tree, prefix="", *, assignments=True):
    """Functions and module SQL assignments, excluding nested functions' bodies."""
    for node in tree.body:
        if isinstance(node, ast.ClassDef):
            yield from _scopes(node, prefix + node.name + ".")
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            yield prefix + node.name, node
            yield from _scopes(node, prefix + node.name + ".", assignments=False)
        elif assignments and isinstance(node, (ast.Assign, ast.AnnAssign)):
            target = node.targets[0] if isinstance(node, ast.Assign) else node.target
            if isinstance(target, ast.Name):
                yield prefix + target.id, node


def _nodes(node):
    yield node
    for child in ast.iter_child_nodes(node):
        if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            continue
        if isinstance(child, ast.Expr) and isinstance(child.value, ast.Constant):
            continue  # docstrings
        yield from _nodes(child)


def _sql_strings(node):
    """Outermost SQL expressions, joining adjacent literals and f-string parts."""
    if isinstance(node, (ast.Constant, ast.JoinedStr, ast.BinOp)):
        text = " ".join(n.value for n in _nodes(node)
                        if isinstance(n, ast.Constant) and isinstance(n.value, str))
        if re.search(r"\b(?:FROM|JOIN)\s+predictions\b|\bJOIN\s+prediction_review\b", text, re.I):
            yield node, text
            return
    for child in ast.iter_child_nodes(node):
        if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            continue
        if isinstance(child, ast.Expr) and isinstance(child.value, ast.Constant):
            continue
        yield from _sql_strings(child)


def _has_review_join(node, bindings):
    return any(
        (isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
         and n.func.id == "prediction_review_join")
        or (isinstance(n, ast.Name) and n.id in bindings)
        for n in _nodes(node)
    )


def _review_bindings(scope):
    bindings = set()
    assignments = [n for n in _nodes(scope) if isinstance(n, (ast.Assign, ast.AnnAssign))]
    while True:
        previous = bindings.copy()
        for node in assignments:
            if node.value is not None and _has_review_join(node.value, bindings):
                targets = node.targets if isinstance(node, ast.Assign) else [node.target]
                bindings.update(t.id for t in targets if isinstance(t, ast.Name))
        if previous == bindings:
            return bindings


def _audit(source, path):
    raw_reads = Counter()
    violations = []
    for name, scope in _scopes(ast.parse(source)):
        key = f"{path}:{name}"
        bindings = _review_bindings(scope)
        for expression, sql in _sql_strings(scope):
            if re.search(r"\bJOIN\s+prediction_review\b", sql, re.I) and key not in CROSS_WORKSPACE_REVIEWS:
                violations.append(f"{key}: handwritten review join")
            if re.search(r"COALESCE\(\w+\.status,\s*'pending'\)", sql, re.I):
                violations.append(f"{key}: handwritten pending default")
            if not _has_review_join(expression, bindings):
                raw_reads[key] += 1
    return raw_reads, violations


def test_prediction_reads_use_shared_review_sql():
    raw_reads = Counter()
    violations = []
    paths = [ROOT / "db.py", *sorted((ROOT / "repositories").glob("*.py"))]
    for path in paths:
        rel = path.relative_to(ROOT).as_posix()
        if rel == HELPER:
            continue
        found, errors = _audit(path.read_text(encoding="utf-8"), rel)
        raw_reads.update(found)
        violations.extend(errors)
    _assert_contract(raw_reads, violations)


def _assert_contract(raw_reads, violations):
    assert not violations, "\n".join(violations)
    assert raw_reads == Counter(RAW_READS), (
        "Prediction reads without the workspace review helper changed. "
        "Use prediction_review_join, or document a review-independent read.\n"
        f"New/expanded: {dict(raw_reads - Counter(RAW_READS))}\n"
        f"Removed/shrunk (remove exemptions): {dict(Counter(RAW_READS) - raw_reads)}"
    )


def test_intentionally_bad_query_missing_join_is_not_exempt():
    source = '''
class Reader:
    def species(self):
        return self.conn.execute("SELECT pr.species FROM predictions pr")
'''
    raw, violations = _audit(source, "repositories/new_reader.py")
    assert raw == {"repositories/new_reader.py:Reader.species": 1}
    with pytest.raises(AssertionError, match="New/expanded"):
        _assert_contract(raw, violations)


def test_handwritten_join_and_default_cannot_return():
    source = '''
SQL = """SELECT pr.* FROM predictions pr
LEFT JOIN prediction_review rv ON rv.prediction_id = pr.id AND rv.workspace_id = ?
WHERE COALESCE(rv.status, 'pending') != 'rejected'"""
'''
    _, violations = _audit(source, "repositories/new_reader.py")
    assert len(violations) == 2



def test_raw_query_beside_a_reviewed_query_is_still_rejected():
    source = '''
class Reader:
    def species(self):
        self.conn.execute(f"SELECT pr.* FROM predictions pr {prediction_review_join('pr')}")
        return self.conn.execute("SELECT pr.species FROM predictions pr")
'''
    raw, violations = _audit(source, "repositories/new_reader.py")
    assert raw == {"repositories/new_reader.py:Reader.species": 1}
    with pytest.raises(AssertionError, match="New/expanded"):
        _assert_contract(raw, violations)


def test_nested_raw_reader_is_not_hidden_by_outer_review_join():
    source = '''
class Reader:
    def species(self):
        review = prediction_review_join('pr')
        def unreviewed():
            return self.conn.execute("SELECT pr.species FROM predictions pr")
        return unreviewed()
'''
    raw, violations = _audit(source, "repositories/new_reader.py")
    assert raw == {"repositories/new_reader.py:Reader.species.unreviewed": 1}
    with pytest.raises(AssertionError, match="New/expanded"):
        _assert_contract(raw, violations)
