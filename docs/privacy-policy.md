# Privacy Policy

*Effective date: June 25, 2026*

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

- **HuggingFace Hub** — When you download AI models, Vireo fetches model weights from HuggingFace. This includes the species classifiers and the MegaDetector subject detector. No photos or personal data are sent. If you provide a HuggingFace token for private model access, it is sent as an authentication header.

- **iNaturalist Open Data / AWS** — Vireo downloads public taxonomy and species list data. No personal data is sent.

- **Google Maps Platform** — Only if you configure a Google Maps API key, Vireo uses Google's Geocoding, Maps JavaScript, and Places APIs in two ways. First, it turns your photos' GPS coordinates into place names, sending the GPS coordinates of geotagged photos to Google. Second, the browse and keywords pages load Google's Maps JavaScript/Places library so the location search box and the link-place input can autocomplete; as you type, your queries and the details of the place you select are sent to Google. If you do not provide a key, no location data is sent and these features are inactive.

- **Map tile providers** — When you open the Map page, Vireo loads the Leaflet library from `unpkg.com` and downloads map tiles from third-party providers (OpenStreetMap, Esri/ArcGIS World Imagery, OpenTopoMap) regardless of whether you have a Google Maps API key configured. The map auto-fits to your photos' GPS markers, so the resulting tile requests can reveal the regions of the world where your geotagged photos are located. The marker positions themselves are computed and rendered locally; only the tile-coordinate requests reach those providers.

- **Bug reports** — Only when you choose to submit an in-app issue report, Vireo sends your description along with diagnostics — recent application logs, photo/folder/prediction counts, system information, recent job history, and your configuration — to a reporting endpoint. By default this is a Google Apps Script endpoint operated by the Vireo project, so the bundle is received and processed on Google's infrastructure. Credentials (tokens, keys, secrets, and passwords) are redacted, but the bundle can still include local file paths, such as the photo folders you have added and the source paths of recent scans or imports. Nothing is sent unless you submit a report.

Beyond the bug reports you choose to submit, Vireo has no analytics, telemetry, or automated crash reporting.

## Credentials

API tokens and keys you provide (for iNaturalist, HuggingFace, and Google Maps) are stored in plaintext in `~/.vireo/config.json` on your device. Vireo does not transmit these credentials to anyone other than the respective service they belong to. Protect this file as you would any file containing credentials.

## Changes to this policy

If this policy changes, the updated version will be posted in the Vireo repository with a new effective date.

## Contact

For privacy questions, open an issue at [github.com/jss367/vireo](https://github.com/jss367/vireo/issues).
