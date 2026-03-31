# Merge Import into Pipeline

## Summary

The Import page is absorbed into the Pipeline page, which becomes the single entry point for all photo processing. The Import Wizard and standalone task buttons (scan, thumbnails, previews) are removed. Lightroom catalog import gets its own separate page as a one-time setup tool.

## Pipeline stages

6 stages presented as expandable cards:

### Stage 1: Source (always enabled)

Two source modes:

- **Import photos** — add one or more paths via a unified input that supports typing a path, browsing, or picking from a dropdown of detected volumes (SD cards, external drives, etc.). Options: file type filter (JPEG, RAW, both), optional copy-to-destination with folder template and skip-duplicates toggle.
- **Use existing collection** — dropdown to select a collection already in the system, shows photo count.

Readiness indicator shows green when a valid source is configured.

### Stage 2: Scan & Import (always enabled)

No user configuration. Card shows scan progress: files found, EXIF read, current file path. Auto-completes and advances to next stage.

### Stage 3: Thumbnails & Previews (always enabled)

One control: preview quality dropdown (1280px / 1920px / 2560px / 3840px / full resolution). Shows progress for both thumbnail and preview generation.

### Stage 4: Classify (optional, checkbox)

Model picker, label picker, re-classify toggle. Same as current Pipeline.

### Stage 5: Extract Features (optional, checkbox, depends on Stage 4)

SAM2 variant selector, DINOv2 variant selector, proxy resolution slider. Same as current Pipeline.

### Stage 6: Group & Score (optional, checkbox, depends on Stage 5)

Encounter grouping, burst clustering, quality scoring, triage. Links to Pipeline Review page for tuning. Same as current Pipeline.

## Stage dependency chain

- Stages 1–3 are always enabled (foundational).
- Stages 4–6 each have a checkbox to include or skip.
- Unchecking a stage auto-unchecks and disables all downstream stages.
- Checking a stage auto-checks all upstream stages (4–6 only, since 1–3 are always on).

## Lightroom import

Moved to its own dedicated page. It's a one-time setup tool with its existing multi-phase workflow: select catalogs, preview conflicts, choose resolution strategy, execute.

## Audit panel integration

The audit panel's "Untracked" tab links to the Pipeline, pre-filling Stage 1 with the relevant folder paths. Audit detects, Pipeline processes.

## What's removed

- The Import page (entirely)
- The Import Wizard
- Standalone "Scan a folder" button
- Standalone "Generate Thumbnails" button
- Standalone "Generate Previews" button
- "Scan for changes" button (replaced by audit panel's untracked detection flowing into Pipeline)
