# vireo/tests/test_detector.py
"""Tests for MegaDetector loading and detection.

The key test here verifies that MegaDetector loads successfully even after
BioCLIP/ultralytics have been imported (which captures internal references
to torch.load). This catches the PyTorch 2.6+ weights_only=True
compatibility issue.
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


def test_megadetector_loads_after_bioclip():
    """MegaDetector must load even when BioCLIP was imported first.

    This is the exact order that happens in the classify job:
    BioCLIP classifier is initialized (phase 3), then MegaDetector
    loads (phase 4). The BioCLIP import triggers ultralytics/open_clip
    imports that cache references to torch.load internally.

    PyTorch 2.6+ changed torch.load to default weights_only=True,
    which rejects the pickled classes in MegaDetector weights.
    Our detector.py patches around this, but the patch must work
    even after all the imports have happened.
    """
    try:
        import torch
    except ImportError:
        pytest.skip("torch not installed")

    # Phase 1: Import BioCLIP (same as classify job phase 3)
    try:
        import classifier  # noqa: F401
    except Exception:
        pytest.skip("BioCLIP/classifier not available")

    # Phase 2: Import PytorchWildlife (happens inside _get_detector)
    try:
        from PytorchWildlife.models import detection as pw_detection
    except ImportError:
        pytest.skip("PytorchWildlife not installed")

    # Phase 3: Load MegaDetector using our detector module
    # Reset singleton so we actually test loading
    import detector
    from detector import _get_detector
    detector._detector = None

    try:
        det = _get_detector()
        assert det is not None
    except RuntimeError as e:
        if "PytorchWildlife" in str(e):
            pytest.skip("PytorchWildlife not installed")
        raise


def test_megadetector_loads_with_force_weights_only_env():
    """MegaDetector must load even when TORCH_FORCE_WEIGHTS_ONLY_LOAD is set.

    Some environments (e.g. conda) set this env var globally. Our detector
    must clear it during loading to avoid the conflict with our
    TORCH_FORCE_NO_WEIGHTS_ONLY_LOAD workaround.
    """
    try:
        import torch
    except ImportError:
        pytest.skip("torch not installed")

    try:
        import classifier  # noqa: F401
    except Exception:
        pytest.skip("BioCLIP/classifier not available")

    try:
        from PytorchWildlife.models import detection as pw_detection
    except ImportError:
        pytest.skip("PytorchWildlife not installed")

    import detector
    detector._detector = None

    # Simulate the hostile environment
    os.environ["TORCH_FORCE_WEIGHTS_ONLY_LOAD"] = "1"
    try:
        det = detector._get_detector()
        assert det is not None
    except RuntimeError as e:
        if "PytorchWildlife" in str(e):
            pytest.skip("PytorchWildlife not installed")
        raise
    finally:
        os.environ.pop("TORCH_FORCE_WEIGHTS_ONLY_LOAD", None)
        detector._detector = None


def test_megadetector_weights_load_directly():
    """Verify MegaDetector weights can be loaded with weights_only=False.

    This is a simpler test that just checks the weights file itself
    is loadable, without going through the full MegaDetector init.
    """
    import glob

    try:
        import torch
    except ImportError:
        pytest.skip("torch not installed")

    paths = glob.glob(
        os.path.expanduser("~/.cache/torch/hub/checkpoints/MDV6*.pt")
    )
    if not paths:
        pytest.skip("MegaDetector weights not downloaded")

    ckpt = torch.load(paths[0], map_location="cpu", weights_only=False)
    assert isinstance(ckpt, dict)
    assert "model" in ckpt or "ema" in ckpt
