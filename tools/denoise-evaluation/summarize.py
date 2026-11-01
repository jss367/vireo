"""Summarize measured run artifacts without inventing quality judgments."""

import argparse
import html
import json
import statistics
from pathlib import Path


def summarize(run, output):
    results = [json.loads(p.read_text()) for p in run.glob("*/result.json")]
    if not results:
        raise ValueError("No completed photos")
    rows = []
    for result in sorted(results, key=lambda r: r["photo_id"]):
        photo = result["fingerprint"]["photo"]
        rows.append(
            {
                "photo_id": photo["id"],
                "species": ", ".join(photo["species"]),
                "iso": photo["iso"],
                "camera": photo["camera_model"],
                "neural_seconds": result["neural"]["inference_and_blending_seconds"],
                "decode_and_calibration_seconds": result["decode_and_calibration_seconds"],
                "total_seven_treatments_seconds": result["total_seconds"],
                "color_validation_error": result["calibration"]["max_validation_error_linear_rgb"],
                "untouched_scene_error": result["calibration"]["max_untouched_scene_error_linear_rgb"],
                "source_unchanged": result["source_unchanged"],
            }
        )
    times = [r["neural_seconds"] for r in rows]
    summary = {
        "completed_photos": len(rows),
        "treatments_per_photo": 7,
        "photographer_quality_judgments": "Not collected; no preference or fidelity conclusion",
        "neural_seconds": {"median": statistics.median(times), "min": min(times), "max": max(times)},
        "peak_process_rss_gib": max(r["process_peak_rss_bytes"] for r in results) / 1024**3,
        "all_sources_unchanged": all(r["source_unchanged"] for r in rows),
        "max_color_validation_error": max(r["color_validation_error"] for r in rows),
        "max_untouched_scene_error": max(r["untouched_scene_error"] for r in rows),
        "scope": "Nikon D850 development set only; selected Z8 High Efficiency* originals excluded",
        "timing_scope": "ONNX Runtime CPU, sequential tiles, 4 threads; shared-machine measurements. Neural time excludes source decode, color calibration, tone rendering, and PNG encoding.",
        "not_measured": [
            "held-out preference rate",
            "true marking fidelity against clean captures",
            "LightCraft Rust/GPU speed",
            "production neural-cache reopening latency",
        ],
        "photos": rows,
    }
    output.mkdir(parents=True, exist_ok=True)
    (output / "results.json").write_text(json.dumps(summary, indent=2))
    table = "".join(
        f"<tr><td>{html.escape(r['species'])}</td><td>{r['iso']}</td>"
        f"<td>{r['neural_seconds']:.1f} s</td><td>{r['decode_and_calibration_seconds']:.1f} s</td>"
        f"<td>{r['total_seven_treatments_seconds']:.1f} s</td><td>{r['color_validation_error']:.6f}</td></tr>"
        for r in rows
    )
    page = f"""<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Wildlife denoising experiment — measured results</title><style>body{{font:16px/1.6 system-ui;background:#101715;color:#edf3ee;max-width:1050px;padding:30px;margin:auto}}h1{{line-height:1.2}}a{{color:#b2ebc0}}table{{width:100%;border-collapse:collapse;font-size:14px}}td,th{{text-align:left;padding:12px;border-bottom:1px solid #405448}}.note{{background:#23362a;padding:18px;border-radius:8px}}code{{font-size:13px}}</style>
<h1>Wildlife RAW denoising: the first comparison</h1>
<p>{len(rows)} D850 photographs · 7 treatments each · exploratory development set</p>
<p class="note"><strong>The comparison images are ready. Quality judgments have not been collected.</strong>
The next step is the photographer's blinded review. A smooth image can still have lost or invented detail.</p>
<p>Median neural inference and blending: <strong>{statistics.median(times):.1f} seconds</strong> per 45 MP photograph
(range {min(times):.1f}–{max(times):.1f} seconds), on the CPU. Peak process memory: {summary["peak_process_rss_gib"]:.1f} GiB.
These timings exclude RAW decoding, calibration, rendering, and encoding. They are not LightCraft GPU measurements.</p>
<p>All source files passed before/after SHA-256 checks. Maximum color-calibration error:
{summary["max_color_validation_error"]:.6f} in linear RGB, below the predeclared 0.001 limit.
Maximum change in sampled untouched calibration pixels: {summary["max_untouched_scene_error"]:.6f}.</p>
<table><thead><tr><th>Subject</th><th>ISO</th><th>Neural</th><th>Decode + calibration</th><th>All 7 treatments</th><th>Calibration error</th></tr></thead><tbody>{table}</tbody></table>
<p><strong>Coverage limit:</strong> ten initially selected Z8 files failed sensor decoding. The checked example uses Nikon High Efficiency* compression.
No embedded JPEG was substituted. Results apply to this D850 set; they do not establish support or quality for those Z8 files.</p>
<p>Every treatment uses +1 EV, the same Vireo color/tone pipeline, and no sharpening, clarity, texture, or saved edits.
The model jointly denoises and demosaics; the comparison therefore measures both changes together.</p>
<p>There are no paired clean captures or held-out preferences in this first batch. The four calibration validation patches
test color conversion, not wildlife-detail reconstruction. Production cache latency and GPU performance remain unmeasured.</p>
<p><a href="gallery/">Open the blinded comparison gallery</a> · <a href="results.json">Download measured results</a></p></html>"""
    (output / "results.html").write_text(page)
    print(json.dumps({k: v for k, v in summary.items() if k != "photos"}, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    summarize(args.run, args.output)
