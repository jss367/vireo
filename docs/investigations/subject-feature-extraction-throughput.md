# Subject feature extraction throughput on a large RAW import

Investigation of the Extract features stage (SAM2 masks, quality analysis and DINOv2 embeddings) after a Full process run spent most of its time there. Measured November 1, 2026 on an Apple M3 Max (12 performance and 4 efficiency cores, 64 GB) with onnxruntime 1.24.4. The catalog and original photos were read only; the end-to-end check ran on an isolated copy.

## The run

Job `pipeline-1793524441090-4`, saved process 2 (Full) on collection 253 ("Import 2026-11-01 01:14", workspace USA2026): 2,549 Nikon NEF files from one card, 3 h 51 m 37 s in total. The workspace's settings were `sam2-large`, `dinov2 vit-b14`, a 2048 px proxy, `detector_confidence` 0.2 and no RAW subject analysis.

| Step | Duration | Items |
| --- | ---: | ---: |
| Thumbnails | 2 m 16 s | 2,549 |
| Previews | 7 m 39 s | 2,549 |
| Detect subjects | 16 m 53 s | 2,549 |
| Classify (BioCLIP-2.5) | 27 m 41 s | 2,549 |
| **Extract features** | **2 h 52 m 53 s** | **2,288** |
| Eye keypoints | 4 m 06 s | 2,258 |
| Group encounters, flag misses | 9 s | |

Extract features ran with no failures (2,275 masked, 13 with no retained animal, 255 photos below the detector floor). That is 4.53 s per photo, or 13.2 photos a minute, with all its work on one thread.

## Where the time goes

`scripts/profile_subject_features.py` repeats the stage's per-photo work on sampled photos from the batch, reusing each photo's stored prompt box. Its masks matched the stored production masks exactly (IoU 1.000 on every sampled photo), so it measures the same work. The machine was shared with other jobs during these measurements, so absolute times run about 25% above the production run's 4.53 s, but the shares are stable across repeats.

| Phase | Mean | Share |
| --- | ---: | ---: |
| SAM2-large image encoder (1024 px, 8 CPU threads) | 4.00 s | 70% |
| DINOv2 vit-b14, subject crop and full frame (one batch of two) | 1.16 s | 20% |
| Quality analysis (Tenengrad, exposure, background ring, pHash, noise) | 0.33 s | 6% |
| Decode (NEF embedded JPEG, resized to 2048 px) | 0.20 s | 3.5% |
| SAM2 decoder, mask resize, PNG write | 0.03 s | 0.5% |

Decoding is not the bottleneck: the embedded JPEG path costs 0.2 s. The stage is bound by model inference on the CPU, and the encoder of the chosen SAM2 variant dominates it.

## What was tried

**CoreML.** The code already drops CoreML for SAM2 (`masking._get_sam2_sessions`) and for external-data models such as DINOv2 (`onnx_runtime.create_session`), from measurements made with the default NeuralNetwork model format. Retried here with `ModelFormat=MLProgram`: CoreML claims 2,547 of the encoder's 4,264 nodes in 338 partitions, and a forward pass takes 14.7 s (`CPUAndGPU`) or 18.0 s (`ALL`), against 7.2 s on the CPU under the same load. Still slower; the exclusion stands.

**More CPU threads.** Inference sessions use a fixed 8 threads (`resource_ledger.DEFAULT_CPU_INFERENCE_THREADS`). For the SAM2-large encoder, 12 threads gained at most about 10% over 8, and 16 threads were slower than 8, because the efficiency cores hold back the static work split. Two concurrent streams looked faster only because a contended machine gives more threads a larger share of it. Not worth weakening the CPU budget for.

**int8 dynamic quantization of the encoder** (`quantize_dynamic` on MatMul weights). The encoder ran 1.85–1.97x faster. On a first sample of 12 photos the masks barely moved (mean IoU 0.988), but on 30 photos one (photo 228538, `DSC_2548.NEF`) got a materially different mask (IoU 0.62, subject Tenengrad 325 vs 510), per-tensor and per-channel alike. About one photo in 30 would get different subject measurements, so this does not meet the bar of equivalent results. It would also need a separately named variant, so mask provenance and caching stay correct, and a hosted quantized model, because `onnx` (which the quantizer needs) is only in the `export` extra.

**A smaller SAM2 variant.** `sam2-base-plus` runs the encoder 2.3x faster than `sam2-large`, but its masks differ more than int8's do: mean IoU 0.953, minimum 0.78, with subject Tenengrad up to 56% off on 12 photos. The variant is a setting (Settings → Pipeline → SAM2 variant, per workspace or global), and that trade-off is the user's to make.

**Overlapping CPU work with inference.** The quality analysis and the DINOv2 embedding read only the proxy and the mask, and ONNX releases the GIL. Running the analysis on a helper thread during the embedding saved 0.40 s per photo in an isolated benchmark (1.56 s to 1.15 s for the pair), with identical results, and 0.30 s per photo inside the real stage (below).

## Change

`pipeline_stages/features.py` now:

- runs each photo's quality analysis beside its DINOv2 embedding (one helper thread per stage run; it waits for the analysis before the photo's lock is released, and a failure there fails that photo, as before);
- reports `phase_seconds` (`decode`, `segment`, `analyze`, `embed`, `store`) on the Extract features stage result, so every run records where its time went. The analysis overlaps the embedding, so the phases can add up to more than the stage's wall time.

## Verification

60 photos sampled from the batch, on an isolated catalog copy (`VACUUM INTO`, scratch `HOME`, every mask row for those photos removed so both runs computed from scratch). The real `run_pipeline_job` ran Extract features only, once with `origin/main` and once with this change:

- every stored result was identical between the two: the `photo_masks` row, the mask PNG bytes and the photo row, including both DINOv2 embeddings (60 of 60);
- against the production run's rows, both agree to float noise (embedding cosine ≥ 0.9999998; one mask's `subject_size` differs by 7e-7). That noise is the same for both runs, so it is ONNX run-to-run variation, not this change;
- nothing in the live masks folder was written.

Timing, from the same copy, with each run on its own fresh clone and both wrapping the decoder, encoder, analysis and embedding calls in identical timers:

| | `origin/main` | This change |
| --- | ---: | ---: |
| Extract features step, 60 photos | 270.7 s | 249.9 s |
| SAM2 (`generate_mask`) | 187.3 s | 184.6 s |
| Step minus SAM2 and decode | 72.6 s | 54.6 s |

The analysis no longer adds to the step: 0.30 s per photo, or 7.7% of the stage. At the production rate that is about 11 minutes off this batch's 2 h 53 m. An earlier pair of runs at a higher machine load landed within 1% of each other (275.8 s and 279.0 s), because the encoder alone varied by more than the saving between runs. On that pair, the branch's critical-path phases added up to 275.5 of its 275.8 s, so the analysis was hidden there too.

## What would make a real dent

At 4.5 s per photo, about 3.4 s of it the SAM2-large encoder, no change that keeps results identical can make this stage much faster on this machine. The options that would change results:

1. Use `sam2-base-plus` for imports where speed matters more than mask detail (about 2.3x on the encoder; different masks on some photos).
2. Ship an int8 `sam2-large` as its own variant (about 1.9x on the encoder; about 1 photo in 30 gets a different mask), after checking its masks across more than one batch.
3. A GPU path that does not go through CoreML's partitioned graph (for example a PyTorch MPS export). Untested here: PyTorch is not a runtime dependency.

## Follow-up experiment on encoder precision

On November 17, 2026 we compared the installed SAM2-large FP32 encoder with a
dynamically quantized copy of its MatMul weights. We kept the FP32 decoder,
stored box prompts, 2048 px RAW proxies and DINOv2 vit-b14 fixed. Each arm
used the merged #2409 workload at revision `d16d8baa4`, including its
overlap of quality analysis with embedding. The timed interval runs from RAW
decode through the mask PNG write. It excludes session construction, database
writes, native-resolution subject detail and pipeline scheduling. This isolates
the part of Extract features affected by encoder precision.

The sample comprised ten seeded draws without replacement from each of three
other Nikon NEF imports. The October 25, November 7 and November 15 imports
had 5,103, 3,475 and 1,786 photos with stored SAM2-large prompts respectively.
Sampling from stored prompts makes the two arms comparable and limits the
conclusion to previously masked photos. Each arm ran in a separate process,
one photo at a time, on the shared 64 GiB M3 Max. Both used Vireo's CPU
inference sessions (eight intra-op threads, one inter-op thread) and a single
analysis helper. We reversed arm order on
the middle import to reduce sensitivity to cache warming and machine load.

### Results and decision

Keep the installed FP32 encoder for SAM2-large. The int8 encoder used 20.1%
fewer process CPU seconds across the sample and less peak memory in each
process. Its elapsed time improved on two imports and regressed on one.
The 30 photos took 429.9 s with FP32 and 452.2 s with int8. These single runs
under varying contention do not establish a reliable import speedup.

| Import (10 photos each) | Encoder | Wall (s) | Photos/min | Mean segment (s/photo) | CPU (s) | Peak RSS (GiB) |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| October 25 | FP32 | 222.9 | 2.69 | 15.02 | 527.5 | 5.52 |
| October 25 | int8 | 203.5 | 2.95 | 12.15 | 398.5 | 4.14 |
| November 7 | FP32 | 106.5 | 5.63 | 7.76 | 444.1 | 6.20 |
| November 7 | int8 | 159.5 | 3.76 | 9.28 | 374.4 | 3.79 |
| November 15 | FP32 | 100.5 | 5.97 | 7.43 | 417.5 | 6.11 |
| November 15 | int8 | 89.2 | 6.73 | 5.57 | 337.2 | 4.37 |

The timing variation extends beyond SAM2. On November 7 the same decode
averaged 2.51 s/photo in the first (int8) arm and 0.67 s/photo in the
second. The earlier encoder-only speedup cannot predict wall time here.

Mean mask intersection over union was 0.989 across the 30 pairs, with a
minimum of 0.937. The largest relative focus change came on a different
photo (230725), where mask overlap was 0.990 but subject Tenengrad fell from
11,365 to 10,222 (10.1%). Median subject luminance changed on eight photos
and the subject crop's perceptual hash changed on eleven. All 60 evaluations
produced masks. FP32 is a comparison reference, not a ground-truth annotation.

The focus difference here and the earlier 0.62-overlap outlier rule out
substituting int8 under the existing model identity. This experiment ends
without adding a production variant. Shipping int8 separately would require
its own model artifact, explicit selection and cache identity. Production
recovery, readiness and selected-subject ownership continue to use the
existing extraction path.

### Reproduction

Use an environment with Vireo's dependencies, ONNX 1.21.0 and ONNX Runtime
1.24.4. Install SAM2-large and DINOv2 vit-b14 first. From the checkout,
run the following in Bash with the originals mounted. For a different catalog,
substitute ID ranges covering distinct imports. The catalog is opened read-only.
Keep generated models and masks in the scratch directory.

```bash
set -eu
export OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 VECLIB_MAXIMUM_THREADS=1
scratch=$(mktemp -d)
model="$HOME/.vireo/models/sam2-large"
python - "$model/image_encoder.onnx" "$scratch/encoder-int8.onnx" <<'PY'
import sys
from onnxruntime.quantization import QuantType, quantize_dynamic

quantize_dynamic(sys.argv[1], sys.argv[2], op_types_to_quantize=["MatMul"],
                 weight_type=QuantType.QInt8)
PY

for batch in "252 221564 227371 fp int" "254 229921 233986 int fp" "257 236434 238450 fp int"; do
    read -r collection first last a b <<< "$batch"
    for arm in "$a" "$b"; do
        encoder="$model/image_encoder.onnx"
        if [[ "$arm" == int ]]; then encoder="$scratch/encoder-int8.onnx"; fi
        python scripts/benchmark_sam2_encoder.py run --db "$HOME/.vireo/vireo.db" \
            --photo-ids "$first" "$last" --encoder "$encoder" \
            --decoder "$model/mask_decoder.onnx" --output "$scratch/$collection-$arm"
    done
    python scripts/benchmark_sam2_encoder.py compare "$scratch/$collection-fp" "$scratch/$collection-int"
done
```

The reference encoder and decoder SHA-256 digests are
`840bfe5872ef0d55f9f44d183301ce0b309aa2f7802ae9ee4fe629443717f63b` and
`8dfcf127524aa1207346f86b265d9d223d1e9d15204b0b7c5582c5af8b0d2226`.
Each run writes the actual masks, per-photo phase timings, CPU seconds,
quality features and process peak resident memory to its output directory.
CPU seconds include all process threads. Peak memory includes model loading.
