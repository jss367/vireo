# Tree of Life embeddings (enabling label-free classification for a model)

Vireo's **Tree of Life** (ToL) mode classifies against the full TreeOfLife-10M
taxonomy (~867k taxa) with **no label list** — open-vocabulary zero-shot. It is
a precomputed per-model artifact, not computed at runtime.

## How it works

Each ToL-capable BioCLIP model needs two files in its `~/.vireo/models/<id>/`
dir, served from the `jss367/vireo-onnx-models` HF repo:

| File | What | Per-model? |
|------|------|------------|
| `tol_classes.json` | ~867k taxonomy dicts (kingdom…species, sometimes common_name) | **No** — identical for every BioCLIP variant |
| `tol_embeddings.npy` | `(embedding_dim, 867455)` float32, L2-normalized columns | **Yes** — each model has its own embedding space |

At inference (`vireo/classifier.py`): `logits = 100.0 * (image_features @ tol_embeddings)`, softmax. Column *i* is the text embedding of taxon *i* in `tol_classes.json` order.

The **caption** for each taxon is its taxonomic name — ranks present joined as
`Kingdom Phylum Class Order Family Genus species` — embedded through the OpenAI
ImageNet templates, normalized per-template, averaged, re-normalized. This
matches both imageomics/bioclip and Vireo's own custom-label path
(`classifier._compute_embeddings_with_progress`).

**Template count.** The generator averages over the first `--n-templates`
templates (default 80). Empirically, top-1 retrieval among the hardest-negative
(taxonomically-adjacent) taxa is already perfect at **N=1**, so a small N (we
used 8 for bioclip-2.5) gives a large speedup with no measured classification
loss.

**Validation is retrieval-based, not cosine.** Regenerating with the
ONNX-exported text encoder lands only ~0.97 cosine to a shipped file that was
made with the original PyTorch encoder — an unavoidable export-precision gap.
It does not matter: using each shipped embedding as a query against the
regenerated ones, top-1 self-retrieval among adjacent taxa is ~1.0, so the
embeddings are classification-equivalent. Generating with the ONNX text encoder
is in fact *more* consistent with Vireo's ONNX image encoder at inference than a
PyTorch-made file would be. `--validate` therefore gates on retrieval ≥ 0.98,
not raw cosine.

## Enabling ToL for a new BioCLIP model

Four steps. The single source of truth for "which models support ToL" is
`TOL_SUPPORTED_MODEL_STRS` / `supports_tree_of_life()` in `vireo/models.py`;
the classifier, pipeline planner, and UI readiness flags all consult it.

1. **Generate the embeddings** with the model's text encoder (GPU strongly
   recommended). On an 11 GB GPU the ViT-H/14 text encoder OOMs in fp32 beyond
   batch-8; convert it to fp16 first (numerically identical here — cosine
   1.00000, 100% retrieval vs fp32) for memory headroom + ~3× throughput:

   ```bash
   # fp16 build of the text encoder (keeps int64 in / fp32 out)
   python - <<'PY'
   import onnx, os; from onnxconverter_common import float16
   d=os.path.expanduser("~/.vireo/models/bioclip-2.5-vith14")
   m=float16.convert_float_to_float16(onnx.load(f"{d}/text_encoder.onnx"),
                                      keep_io_types=True, disable_shape_infer=True)
   onnx.save(m, f"{d}/text_encoder_fp16.onnx", save_as_external_data=True,
             all_tensors_to_one_file=True, location="text_encoder_fp16.onnx.data")
   PY

   # Confirm the recipe reproduces the shipped bioclip-2 artifact (retrieval
   # gate). Requires bioclip-2's text encoder + tol_embeddings.npy locally.
   python scripts/generate_tol_embeddings.py \
       --model-dir ~/.vireo/models/bioclip-2 --validate --sample 512 \
       --ref-npy ~/.vireo/models/bioclip-2/tol_embeddings.npy
   # Expect: retrieval top1 ~1.0 → PASS  (cosine ~0.97 is fine — see above)

   # Generate, sharded across two GPUs (~1.5 h total for 867k taxa here):
   M=~/.vireo/models/bioclip-2.5-vith14
   OPTS="--model-dir $M --text-encoder text_encoder_fp16.onnx --n-templates 8 --batch-taxa 48"
   CUDA_VISIBLE_DEVICES=0 python scripts/generate_tol_embeddings.py $OPTS \
       --start 0 --end 433728 --output shard0.npy &
   CUDA_VISIBLE_DEVICES=1 python scripts/generate_tol_embeddings.py $OPTS \
       --start 433728 --end 867455 --output shard1.npy &
   wait
   python -c "import numpy as np; np.save('$M/tol_embeddings.npy', \
       np.concatenate([np.load('shard0.npy'), np.load('shard1.npy')], axis=1))"
   ```

   `tol_classes.json` is shared — the script auto-downloads bioclip-2's copy,
   or pass `--tol-classes <path>`.

2. **Upload** `tol_embeddings.npy` and `tol_classes.json` to the model's
   subdir in `jss367/vireo-onnx-models`:

   ```bash
   huggingface-cli upload jss367/vireo-onnx-models \
       ~/.vireo/models/bioclip-2.5-vith14/tol_embeddings.npy \
       bioclip-2.5-vith14/tol_embeddings.npy --repo-type model
   huggingface-cli upload jss367/vireo-onnx-models \
       ~/.vireo/models/bioclip-2.5-vith14/tol_classes.json \
       bioclip-2.5-vith14/tol_classes.json --repo-type model
   ```

3. **List the files** in the model's `files` manifest in `vireo/models.py`
   (add `tol_embeddings.npy` and `tol_classes.json`) so the downloader fetches
   them.

4. **Register support** by adding the model's `model_str` to
   `TOL_SUPPORTED_MODEL_STRS` in `vireo/models.py`.

> Order matters: do **not** ship steps 3–4 before step 2 completes. A model
> listed in `TOL_SUPPORTED_MODEL_STRS` whose `tol_embeddings.npy` isn't yet on
> HF will advertise ToL in the UI and then fail at classify time with a missing
> file. bioclip-2.5 already has steps 3–4 applied — its artifact must be
> uploaded before this is released.
