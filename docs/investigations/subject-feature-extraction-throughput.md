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
