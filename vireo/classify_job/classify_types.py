"""Type names shared by the classify job's annotations.

Nothing here runs: the aliases are plain ``dict``/``Mapping`` types, named
so the job's phases (and the pipeline's classify stage, which calls into
them) say what they pass each other.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from classifier import Classifier
    from timm_classifier import TimmClassifier

    # The loaded classifier a pass runs.
    AnyClassifier = Classifier | TimmClassifier

# A photo the job reads by column name: a catalog ``sqlite3.Row``, or the
# ``photo`` a raw result carries.
PhotoRecord = sqlite3.Row | Mapping[str, Any]

# A detection as the job passes it around: ``id``, ``box_x``/``box_y``/
# ``box_w``/``box_h``, ``confidence``, ``category``, ``detector_model``. Fresh
# detector output and stored rows differ in which optional keys they carry,
# so the values are ``Any``.
DetectionDict = dict[str, Any]

# photo id -> the photo's classifier-candidate detections.
DetectionMap = dict[int, list[DetectionDict]]

# One classified (or cache-hit) detection on its way to grouping and
# storage: ``photo``, ``detection_id``, ``prediction``, ``confidence``,
# ``taxonomy``, ``embedding``, ``alternatives``, ... A cache hit adds
# ``_existing``; storage adds ``_replace_prediction_outputs``.
RawResult = dict[str, Any]

# One prepared crop in an inference batch: ``photo``, ``detection_id``,
# ``folder_path``, ``image_path``, ``img`` (and ``input_recipe`` when the
# pipeline's classify stage builds it). Both the classify job and that stage
# build these, so they are read as mappings.
BatchEntry = Mapping[str, Any]

# One ranked prediction from a classifier: ``species``, ``score``,
# ``raw_score``, ``taxonomy``, ...
Prediction = dict[str, Any]

# A model record from ``models.get_models()`` (``id``, ``name``,
# ``model_str``, ``weights_path``, ``model_type``, ``files``, ...); the keys
# present depend on the model's source.
ModelSpec = dict[str, Any]

# A label set's saved metadata (``labels.get_saved_labels()`` entries:
# ``labels_file``, ``name``, ...).
LabelSetMeta = dict[str, Any]
