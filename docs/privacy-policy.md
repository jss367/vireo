# Privacy Policy

*Effective date: March 28, 2026*

Vireo is a wildlife photo organizer that runs entirely on your device. This policy explains what data Vireo accesses, what stays on your device, and what leaves it.

## Data that stays on your device

Vireo stores all data locally in `~/.vireo/` on your machine:

- Your photo library (Vireo never modifies your original photo files, but does create and update XMP sidecar files alongside them for keywords and ratings)
- A SQLite database containing photo metadata, keywords, predictions, and workspace settings
- Thumbnails, AI model weights, text embeddings, segmentation masks, and taxonomy data
- Configuration and preferences

Photo metadata — including EXIF data such as GPS coordinates, timestamps, and camera information — is read from your photos and stored in the local database. This data never leaves your device unless you explicitly send it to a third party (see below).

All AI classification and detection runs locally on your device. No photos are sent to any server for analysis.

## Data that leaves your device

Vireo contacts external services in the following ways:

- **iNaturalist** — Vireo contacts iNaturalist in two ways. First, on startup it may query the iNaturalist taxonomy API to resolve unknown keyword names — only the keyword text is sent, no photos or personal data. Second, when you submit an observation, your photo, GPS coordinates, species name, observation date, and geoprivacy setting are sent to iNaturalist. Observation submissions require an API token you provide. Submitted data is governed by [iNaturalist's privacy policy](https://www.inaturalist.org/pages/privacy).

- **HuggingFace Hub** — When you download AI models, Vireo fetches model weights from HuggingFace. No photos or personal data are sent. If you provide a HuggingFace token for private model access, it is sent as an authentication header.

- **iNaturalist Open Data / AWS** — Vireo downloads public taxonomy and species list data. No personal data is sent.

- **Zenodo** — Vireo downloads the MegaDetector model weights from Zenodo. No personal data is sent.

Vireo has no analytics, telemetry, or automated crash reporting.

## Credentials

API tokens you provide (for iNaturalist and HuggingFace) are stored in plaintext in `~/.vireo/config.json` on your device. Vireo does not transmit these tokens to anyone other than the respective service they belong to. Protect this file as you would any file containing credentials.

## Changes to this policy

If this policy changes, the updated version will be posted in the Vireo repository with a new effective date.

## Contact

For privacy questions, open an issue at [github.com/jss367/vireo](https://github.com/jss367/vireo/issues).
