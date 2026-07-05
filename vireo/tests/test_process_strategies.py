"""Strategy presets are pure data: name -> PipelineParams overrides."""
import pytest
from process_strategies import STRATEGIES, resolve_strategy


def test_known_strategies():
    # The whitelist is deliberately narrow: only *processing* presets.
    # The design doc's "None" / import-only choice is NOT a fourth
    # entry here — it lives at the import→process boundary (workspace
    # default in Task 1.5, chaining hook in PR 3). Adding an entry like
    # "none" here would make the process-job API accept it and enqueue
    # a no-op run instead of letting the chaining hook short-circuit.
    assert set(STRATEGIES) == {"full", "cull_ready", "quick_look"}


def test_full_skips_nothing():
    flags = resolve_strategy("full")
    assert not any(v for k, v in flags.items() if k.startswith("skip_"))
    assert flags["miss_enabled"] is True


def test_cull_ready_skips_expensive_extras():
    flags = resolve_strategy("cull_ready")
    assert flags["skip_extract_masks"] is True
    assert flags["skip_eye_keypoints"] is True
    assert flags["miss_enabled"] is False
    # classify and regroup stay on: review pages need predictions + encounters
    assert flags["skip_classify"] is False
    assert flags["skip_regroup"] is False


def test_quick_look_is_thumbs_and_previews_only():
    flags = resolve_strategy("quick_look")
    assert flags["skip_classify"] is True
    assert flags["skip_extract_masks"] is True
    assert flags["skip_eye_keypoints"] is True
    assert flags["skip_regroup"] is True
    assert flags["miss_enabled"] is False


def test_unknown_strategy_raises():
    with pytest.raises(ValueError, match="unknown strategy"):
        resolve_strategy("yolo")


@pytest.mark.parametrize("bad", [None, 5, True, ["cull_ready"], {"name": "full"}])
def test_non_string_strategy_raises_value_error(bad):
    """Both callers surface ``ValueError`` as 400 (``/api/jobs/pipeline`` and
    ``api_update_workspace`` for ``pipeline.default_strategy``). A dict/list
    reaching ``name not in STRATEGIES`` would otherwise raise ``TypeError``
    (unhashable type) and escape as a 500 from the workspace endpoint, which
    lacks the string type-guard the pipeline route has."""
    with pytest.raises(ValueError, match="strategy must be a string"):
        resolve_strategy(bad)
