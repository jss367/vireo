#!/usr/bin/env python3
"""Generate Tree of Life text embeddings for a BioCLIP model.

Vireo's "Tree of Life" classify mode is open-vocabulary zero-shot over the
full TreeOfLife-10M taxonomy (~867k taxa). It needs two files per model,
served from the ``jss367/vireo-onnx-models`` HF repo:

  - ``tol_classes.json``    list of taxonomy dicts (MODEL-INDEPENDENT — shared
                            by every BioCLIP variant; reused as-is here)
  - ``tol_embeddings.npy``  (embedding_dim, num_classes) float32, L2-normalized
                            columns (MODEL-SPECIFIC — this is what we generate)

At inference (vireo/classifier.py) the prediction is
``logits = 100.0 * (image_features @ tol_embeddings)`` followed by softmax, so
each column must be the L2-normalized text embedding of one taxon in the SAME
order as ``tol_classes.json``.

Caption convention (matches imageomics/bioclip + pybioclip, and Vireo's own
``classifier._compute_embeddings_with_progress`` used for custom labels):
for each taxon we build the taxonomic name from whatever ranks are present
(``Kingdom Phylum Class Order Family Genus species``), render it through all
80 OpenAI ImageNet templates, encode each, L2-normalize, average, and
re-normalize. This is identical to how the custom-label embeddings are built,
so a model's ToL and custom-label spaces stay consistent.

Templates: the mean is over the first ``--n-templates`` OpenAI templates
(default: all 80). Empirically, retrieval among the hardest-negative
(taxonomically-adjacent) taxa is already perfect at N=1, so a small N trades
ensemble smoothing for a large speedup with no measured classification loss.

Usage
-----
    # Verify the recipe against the SHIPPED bioclip-2 artifact BEFORE trusting a
    # new model's output. Pass criterion is retrieval, not raw cosine (the ONNX
    # text encoder lands ~0.97 cosine to the PyTorch-made shipped file, but
    # top-1 self-retrieval among adjacent taxa is ~1.0 — classification is
    # unaffected). Use a local --ref-npy to avoid HTTP range reads.
    python scripts/generate_tol_embeddings.py \
        --model-dir ~/.vireo/models/bioclip-2 --validate --sample 512 \
        --ref-npy ~/.vireo/models/bioclip-2/tol_embeddings.npy

    # Generate for a new model. On an 11 GB GPU the ViT-H/14 encoder needs an
    # fp16 build + modest batch; shard across GPUs with --start/--end.
    CUDA_VISIBLE_DEVICES=0 python scripts/generate_tol_embeddings.py \
        --model-dir ~/.vireo/models/bioclip-2.5-vith14 \
        --text-encoder text_encoder_fp16.onnx --n-templates 8 --batch-taxa 48 \
        --start 0 --end 433728 --output shard0.npy
    # (second shard on CUDA_VISIBLE_DEVICES=1 with --start 433728 --end 867455,
    #  then np.concatenate([shard0, shard1], axis=1) -> tol_embeddings.npy)

This is a dev-only script. It depends only on onnxruntime + tokenizers +
numpy (already Vireo runtime deps); no torch / open_clip needed. See
docs/tol-embeddings.md for the full runbook.
"""

import argparse
import ast
import json
import logging
import os
import sys
import time
import urllib.request

import numpy as np

# Import Vireo's own helpers so tokenization + templates are bit-for-bit the
# same as runtime classification (the package dir is vireo/).
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "vireo"))
import onnx_runtime  # noqa: E402
from classifier import (  # noqa: E402
    OPENAI_IMAGENET_TEMPLATE,
    _load_tokenizer,
    _normalize,
    _tokenize,
)

log = logging.getLogger("generate_tol_embeddings")

ONNX_REPO = "jss367/vireo-onnx-models"
# tol_classes.json is model-independent; bioclip-2 is the canonical source.
TOL_CLASSES_URL = (
    f"https://huggingface.co/{ONNX_REPO}/resolve/main/bioclip-2/tol_classes.json"
)
TOL_EMB_URL = (
    f"https://huggingface.co/{ONNX_REPO}/resolve/main/bioclip-2/tol_embeddings.npy"
)

# Taxonomy ranks in the order BioCLIP's `naming.taxonomic` joins them.
_RANK_ORDER = ("kingdom", "phylum", "class", "order", "family", "genus")


def build_caption(entry):
    """Build the taxonomic name string for a tol_classes entry.

    Mirrors imageomics/bioclip ``Taxon.taxonomic``: ranks present joined by
    spaces, higher ranks capitalized and the species epithet lowercased, with
    duplicate whitespace collapsed. Entries are sparse — only the ranks that
    exist are included.
    """
    parts = []
    for rank in _RANK_ORDER:
        val = entry.get(rank)
        if val:
            parts.append(val.capitalize())
    species = entry.get("species")
    if species:
        parts.append(species.lower())
    name = " ".join(parts)
    return " ".join(name.split())


def load_tol_classes(path=None):
    """Load tol_classes.json from disk, or download the shared copy from HF."""
    if path and os.path.isfile(path):
        log.info("Loading tol_classes from %s", path)
        with open(path) as f:
            return json.load(f)
    log.info("Downloading shared tol_classes.json from %s", TOL_CLASSES_URL)
    with urllib.request.urlopen(TOL_CLASSES_URL) as resp:
        return json.load(resp)


def _resolve_text_session(model_dir, text_encoder="text_encoder.onnx"):
    """Open the text-encoder ONNX session and return (session, input_name)."""
    text_path = os.path.join(model_dir, text_encoder)
    if not os.path.isfile(text_path):
        raise FileNotFoundError(f"{text_encoder} not found in {model_dir}")
    session = onnx_runtime.create_session(text_path)
    input_name = session.get_inputs()[0].name
    return session, input_name


def compute_embeddings(model_dir, classes, indices=None, batch_taxa=32,
                       text_encoder="text_encoder.onnx", templates=None,
                       log_every=2000):
    """Compute (embedding_dim, N) L2-normalized text embeddings.

    Each taxon's embedding is the mean over the 80 OpenAI templates of its
    flat taxonomic caption (per-template L2-normalized, averaged, re-normalized
    — identical to classifier._compute_embeddings_with_progress). Taxa are
    processed ``batch_taxa`` at a time so the text encoder sees a large
    (batch_taxa * 80, 77) batch per forward pass — the difference between ~8
    taxa/s (one taxon per call) and GPU-saturating throughput.

    Args:
        model_dir: directory holding text_encoder.onnx + tokenizer.json
        classes: full tol_classes list
        indices: optional iterable of indices to compute (for sampling /
            validation / sharding). Defaults to every class, in order.
        batch_taxa: number of taxa per forward pass.

    Returns a float32 array of shape (embedding_dim, len(indices)).
    """
    tokenizer = _load_tokenizer(os.path.join(model_dir, "tokenizer.json"))
    session, input_name = _resolve_text_session(model_dir, text_encoder)

    if indices is None:
        indices = range(len(classes))
    indices = list(indices)
    total = len(indices)

    if templates is None:
        templates = OPENAI_IMAGENET_TEMPLATE
    n_templates = len(templates)
    columns = []
    start = time.time()
    for base in range(0, total, batch_taxa):
        chunk = indices[base:base + batch_taxa]
        # (len(chunk) * n_templates) captions, grouped taxon-major.
        txts = [
            tmpl(build_caption(classes[idx]))
            for idx in chunk
            for tmpl in templates
        ]
        tokens = _tokenize(tokenizer, txts)
        feats = session.run(None, {input_name: tokens})[0].astype(np.float32)
        feats = _normalize(feats)                      # per-template L2
        feats = feats.reshape(len(chunk), n_templates, -1)
        means = _normalize(feats.mean(axis=1))         # average + re-normalize
        columns.append(means)                          # (len(chunk), D)
        done = min(base + batch_taxa, total)
        if done % log_every < batch_taxa or done == total:
            rate = done / max(time.time() - start, 1e-6)
            eta = (total - done) / max(rate, 1e-6)
            log.info(
                "%d/%d taxa — %.1f taxa/s, ETA %.0f min",
                done, total, rate, eta / 60.0,
            )
    return np.concatenate(columns, axis=0).T  # (embedding_dim, N)


def _read_npy_header(url):
    """Return (embedding_dim, num_classes, data_offset) for the shipped npy."""
    with urllib.request.urlopen(
        urllib.request.Request(url, headers={"Range": "bytes=0-255"})
    ) as r:
        hdr = r.read()
    hlen = int.from_bytes(hdr[8:10], "little")
    # The header is a Python-literal dict — parse it without executing code.
    meta = ast.literal_eval(hdr[10:10 + hlen].decode("latin1"))
    embedding_dim, num_classes = meta["shape"]
    return embedding_dim, num_classes, 10 + hlen


def _download_reference_columns(indices, embedding_dim, num_classes, data_offset):
    """Fetch specific columns of the shipped bioclip-2 npy via HTTP range.

    Stored C-order as (embedding_dim, num_classes), so column j is the strided
    set {row*num_classes + j}. Reading scattered single floats would be tens of
    thousands of requests; instead we read each ROW once over the minimal
    contiguous span covering the requested columns and slice them out.
    """
    itemsize = 4
    lo, hi = min(indices), max(indices)
    span = hi - lo + 1
    out = np.empty((embedding_dim, len(indices)), dtype=np.float32)
    rel = [i - lo for i in indices]
    for row in range(embedding_dim):
        row_start = data_offset + (row * num_classes + lo) * itemsize
        row_end = row_start + span * itemsize - 1
        req = urllib.request.Request(
            TOL_EMB_URL, headers={"Range": f"bytes={row_start}-{row_end}"}
        )
        with urllib.request.urlopen(req) as r:
            buf = np.frombuffer(r.read(), dtype="<f4")
        out[row, :] = buf[rel]
    return out


def validate(model_dir, classes, sample, text_encoder="text_encoder.onnx",
             templates=None, ref_npy=None):
    """Regenerate a block of bioclip-2 embeddings and check them against the
    shipped npy — the correctness gate before generating a new model's artifact.

    The pass criterion is RETRIEVAL, not raw cosine. Regenerating with the
    ONNX-exported text encoder lands ~0.97 cosine to the shipped file (which
    was made with the original PyTorch encoder — an unavoidable export gap),
    yet classification is unaffected: using each shipped embedding as a query
    against the regenerated ones, top-1 self-retrieval among a block of
    taxonomically-adjacent (hardest-negative) taxa is ~1.0. That is what
    determines whether the embeddings classify correctly, so it is the gate.
    Cosine is reported for information only.

    Args:
        ref_npy: optional path to a local shipped tol_embeddings.npy (avoids
            HTTP range reads, which some hosts block/throttle). Falls back to
            range-reading the HF copy.
    """
    # Contiguous window of taxa (row-major npy → per-row range reads cover a
    # tight [lo, hi]; a scattered sample would span the whole 867k-col file).
    if ref_npy and os.path.isfile(ref_npy):
        ref_all = np.load(ref_npy, mmap_mode="r")
        embedding_dim, num_classes = ref_all.shape
    else:
        ref_all = None
        embedding_dim, num_classes, data_offset = _read_npy_header(TOL_EMB_URL)
    log.info("Shipped bioclip-2 npy: dim=%d, classes=%d", embedding_dim, num_classes)

    upper = min(num_classes, len(classes))
    rng = np.random.default_rng(0)
    start = int(rng.integers(0, max(1, upper - sample)))
    idxs = list(range(start, min(start + sample, upper)))
    log.info("Validating on taxa [%d, %d) ...", idxs[0], idxs[-1] + 1)

    ours = compute_embeddings(model_dir, classes, indices=idxs,
                              text_encoder=text_encoder, templates=templates,
                              log_every=128)
    if ref_all is not None:
        ref = np.array(ref_all[:, idxs[0]:idxs[-1] + 1], dtype=np.float32)
    else:
        ref = _download_reference_columns(idxs, embedding_dim, num_classes,
                                          data_offset)

    ours_n = ours / np.maximum(np.linalg.norm(ours, axis=0, keepdims=True), 1e-8)
    ref_n = ref / np.maximum(np.linalg.norm(ref, axis=0, keepdims=True), 1e-8)
    cos = (ours_n * ref_n).sum(axis=0)
    # Retrieval: each shipped column as query vs all regenerated columns.
    sims = ref_n.T @ ours_n                    # (n_query, n_cand)
    n = len(idxs)
    top1 = float((sims.argmax(axis=1) == np.arange(n)).mean())
    log.info(
        "vs shipped — cosine mean=%.4f min=%.4f | retrieval top1=%.4f "
        "(among %d adjacent taxa)",
        float(cos.mean()), float(cos.min()), top1, n,
    )
    if top1 >= 0.98:
        log.info("PASS: regenerated embeddings are classification-equivalent "
                 "to the shipped artifact.")
        return True
    log.error(
        "FAIL: retrieval top1 %.3f below 0.98 — caption/template logic likely "
        "differs. Inspect build_caption() before generating a new artifact.",
        top1,
    )
    return False


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--model-dir", required=True,
                   help="Model dir with text_encoder.onnx + tokenizer.json")
    p.add_argument("--tol-classes", default=None,
                   help="Path to tol_classes.json (default: download shared copy)")
    p.add_argument("--output", default=None,
                   help="Output .npy path (default: <model-dir>/tol_embeddings.npy)")
    p.add_argument("--validate", action="store_true",
                   help="Check regenerated sample against shipped bioclip-2 npy "
                        "(retrieval-based; run against --model-dir bioclip-2)")
    p.add_argument("--sample", type=int, default=512,
                   help="Sample size for --validate (default 512)")
    p.add_argument("--ref-npy", default=None,
                   help="Local shipped tol_embeddings.npy for --validate "
                        "(default: HTTP range-read the HF copy)")
    p.add_argument("--limit", type=int, default=None,
                   help="Only generate the first N taxa (smoke test)")
    p.add_argument("--start", type=int, default=None,
                   help="First taxon index (inclusive) — for GPU sharding")
    p.add_argument("--end", type=int, default=None,
                   help="Last taxon index (exclusive) — for GPU sharding")
    p.add_argument("--batch-taxa", type=int, default=32,
                   help="Taxa per forward pass (default 32)")
    p.add_argument("--text-encoder", default="text_encoder.onnx",
                   help="Text encoder filename in model-dir (e.g. an fp16 build)")
    p.add_argument("--n-templates", type=int, default=None,
                   help="Use only the first N of the 80 OpenAI templates "
                        "(default: all 80). Retrieval is unchanged down to N=1, "
                        "so a small N trades ensemble smoothing for speed.")
    p.add_argument("-v", "--verbose", action="store_true")
    args = p.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s", datefmt="%H:%M:%S",
    )

    model_dir = os.path.expanduser(args.model_dir)
    classes = load_tol_classes(
        os.path.expanduser(args.tol_classes) if args.tol_classes else None
    )
    log.info("Loaded %d tol_classes; sample caption: %r",
             len(classes), build_caption(classes[0]))

    templates = (OPENAI_IMAGENET_TEMPLATE[:args.n_templates]
                 if args.n_templates else None)

    if args.validate:
        ok = validate(model_dir, classes, args.sample,
                      text_encoder=args.text_encoder, templates=templates,
                      ref_npy=os.path.expanduser(args.ref_npy) if args.ref_npy else None)
        sys.exit(0 if ok else 1)

    if args.start is not None or args.end is not None:
        indices = range(args.start or 0, args.end or len(classes))
    elif args.limit:
        indices = range(args.limit)
    else:
        indices = None
    emb = compute_embeddings(model_dir, classes, indices=indices,
                             batch_taxa=args.batch_taxa,
                             text_encoder=args.text_encoder,
                             templates=templates)
    out = os.path.expanduser(args.output) if args.output else os.path.join(
        model_dir, "tol_embeddings.npy")
    np.save(out, emb)
    log.info("Wrote %s  shape=%s  (%.1f MB)",
             out, emb.shape, os.path.getsize(out) / 1e6)


if __name__ == "__main__":
    main()
