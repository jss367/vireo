# timm iNat21 Classifier Integration

## Problem

Vireo uses BioCLIP for species classification — a zero-shot CLIP model that matches image embeddings against text embeddings of species names. While flexible (works with any label set), it can be confidently wrong: e.g. predicting "Lewis's Woodpecker" at 95% for a Common Starling. Zero-shot text matching doesn't learn visual field marks the way supervised training does.

## Solution

Add the timm iNat21 model (EVA-02 Large fine-tuned on iNaturalist 2021) as an alternative classifier. This is a supervised model trained on millions of real species photos, covering 10,000 species with 92% top-1 accuracy. Users can run it alongside or instead of BioCLIP — the existing multi-model vote summary surfaces agreements and disagreements.

## Design

### Model Registry

New entry in `KNOWN_MODELS` in `models.py`:

```python
{
    "id": "timm-inat21-eva02-l",
    "name": "iNat21 (EVA-02 Large)",
    "model_type": "timm",
    "model_str": "eva02_large_patch14_clip_336.merged2b_ft_inat21",
    "source": "timm",
    "description": "EVA-02 Large fine-tuned on iNaturalist 2021. 10K species, 92% top-1. No label files needed.",
    "size_mb": 1200,
    "architecture": "EVA-02 Large",
    "parameters": "304M",
}
```

Existing BioCLIP entries get `"model_type": "bioclip"` (defaulting to `"bioclip"` when absent for backwards compatibility).

### TimmClassifier

New file `timm_classifier.py` with a `TimmClassifier` class that has the same interface as the existing `Classifier`:

```python
class TimmClassifier:
    def __init__(self, model_name):
        self.model = timm.create_model(model_name, pretrained=True)
        self.transform = create_transform(...)
        self.class_names = [...]  # 10K iNat21 scientific names

    def classify(self, image_path, threshold=0.1):
        # Load image, transform, forward pass, softmax
        # Return top predictions above threshold as:
        #   [{"species": "European Starling", "score": 0.87, ...}]
```

Key differences from BioCLIP `Classifier`:
- No label files needed — the 10K class set is fixed in the model
- Lower default threshold (0.1 vs 0.4) since probability is spread across 10K classes
- No image embedding output (no CLIP-style embedding)
- Does not use `classify_with_embedding()` — just `classify()`

### Class Name Mapping

The iNat21 model outputs scientific names (e.g. "Sturnus vulgaris"). Vireo displays common names.

Mapping strategy:
1. Load Vireo's existing `taxonomy.json` which has scientific → common name mappings
2. Build a lookup dict at classifier init time
3. Map each prediction's scientific name to common name
4. Fall back to scientific name if no common name found

### Download Flow

timm models auto-download from HuggingFace on first `create_model()` call. For the UI's explicit download step:

1. `download_model("timm-inat21-eva02-l")` calls `timm.create_model(model_name, pretrained=True)` to trigger the HuggingFace download
2. Registers the model with `weights_path` pointing to the timm/HF cache location
3. Subsequent loads use the cached weights

### Classify Job Changes

The classify job in `app.py` checks `model_type` from the model registry:

- **`"bioclip"` (default):** Current flow unchanged — load labels → init Classifier → MegaDetector detect → crop → classify → group → store
- **`"timm"`:** Init TimmClassifier → MegaDetector detect → crop → classify → group → store. Skips label loading entirely.

Both paths:
- Use MegaDetector for subject detection/cropping (timm benefits from cropped input)
- Store predictions in the same `predictions` table with model name in the `model` column
- Use timestamp-based grouping for consensus predictions

The timm path skips embedding-based similarity refinement in the grouping step since it doesn't produce CLIP embeddings. Timestamp grouping still works.

### What Stays the Same

- `predictions` table schema — already supports multiple models per photo
- Species vote summary on pipeline page — already aggregates across models
- Species correction UI — model-agnostic, works on encounter/burst level
- Pending changes / XMP sync — unchanged
- Model management UI on classify page — timm model appears alongside BioCLIP models

## Scope

- New file: `vireo/timm_classifier.py`
- Modified: `vireo/models.py` (new KNOWN_MODELS entry, model_type field, download handler)
- Modified: `vireo/app.py` (classify job branches on model_type)
- Tests for TimmClassifier

## Not In Scope

- SpeciesNet integration (skipped — camera-trap focused, weak on birds)
- Ensemble/voting logic — the existing multi-model vote summary handles this naturally
- Embedding-based grouping refinement for timm (timestamp grouping is sufficient)
- TaxaBind integration (interesting but too new, revisit later)
