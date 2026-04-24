<p align="center">
  <img src="logo.png" width="200" alt="Vireo logo — a songbird integrated with a camera aperture">
</p>

<h1 align="center">Vireo</h1>

<p align="center">
  AI-powered wildlife photo organizer that respects your filesystem and never hides what it's doing.
</p>

---

Vireo helps wildlife photographers triage thousands of photos using machine learning. It detects animals, identifies species, scores image quality, groups photos into encounters, and recommends which to keep — all while storing metadata in standard XMP sidecars so you're never locked in.

## Features

- **Species classification** — Multiple models including BioCLIP, BioCLIP-2, BioCLIP-2.5, and an iNat21 fine-tuned classifier covering 10K+ species
- **Wildlife detection** — MegaDetector v6 for animal/person/vehicle localization
- **Automated triage pipeline** — Groups photos into encounters and bursts, scores quality (sharpness, exposure, composition, noise), and labels each photo KEEP/REVIEW/REJECT
- **Subject-aware quality scoring** — Uses SAM2 segmentation masks and DINOv2 embeddings to evaluate the actual subject, not just the frame
- **iNaturalist integration** — Taxonomy lookup and direct observation uploads
- **Browse, review, and cull** — Filter, search, rate, keyword, and flag photos in a responsive web UI
- **Map view** — Geographic visualization of geotagged photos
- **Workspaces** — Isolated projects with independent predictions, collections, and settings
- **Lightroom migration** — Import keyword hierarchies from `.lrcat` catalogs via XMP sidecars
- **Transparent by design** — Live log panel, job progress streaming, pipeline inspector, and full audit system

## Philosophy

- **XMP is truth, the database is a cache.** The SQLite database can be rebuilt from your filesystem at any time.
- **Show the user what's happening.** No black boxes — every scan, download, classification, and failure is visible.
- **Work with the ecosystem.** Import from Lightroom, sync to XMP, submit to iNaturalist. Vireo orchestrates; it doesn't try to own the pipeline.

See [CORE_PHILOSOPHY.md](CORE_PHILOSOPHY.md) for more.

## Getting started

### Requirements

- Python 3.11+
- A GPU is recommended for classification but not required

### Install

```bash
git clone https://github.com/jss367/vireo.git
cd vireo
pip install -e .
```

For development:

```bash
pip install -e ".[dev]"
```

### Run

```bash
python vireo/app.py --db ~/.vireo/vireo.db --port 8080
```

Then open [http://localhost:8080](http://localhost:8080).

ML models are downloaded automatically from HuggingFace on first use.

## Tests

```bash
python -m pytest tests/ vireo/tests/ -q
```

## Scripting & automation

Vireo exposes a small stable HTTP API under `/api/v1` for scripts and agents. A running instance advertises its port and auth token via `~/.vireo/runtime.json`. See [docs/headless-api.md](docs/headless-api.md) for discovery, spawning a headless instance, authentication, and a worked `curl` example.
