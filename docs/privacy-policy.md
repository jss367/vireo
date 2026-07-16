# Privacy Policy

*Effective date: July 14, 2026*

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

- **Google Maps Platform** — Only if you configure a Google Maps API key, Vireo uses Google's Geocoding, Maps JavaScript, and Places APIs. It can turn photo coordinates into place names, provide location autocomplete, and show nearby parks, landmarks, and administrative areas while you review photo locations. These features send the relevant photo coordinates, search text, and selected place details to Google. If you do not provide a key, these Google-powered features are inactive.

- **Map tile providers** — When you open the Map page, or review photo locations without a Google Maps key, Vireo loads the Leaflet library from `unpkg.com` and downloads map tiles from third-party providers (OpenStreetMap, Esri/ArcGIS World Imagery, OpenTopoMap). The map auto-fits to your photos' coordinate markers, so the resulting tile requests can reveal the regions of the world where your geotagged photos are located. The marker positions themselves are computed and rendered locally; only the tile-coordinate requests reach those providers.

- **Bug reports** — Only when you choose to submit an in-app issue report, Vireo sends your description along with diagnostics — recent application logs, photo/folder/prediction counts, system information, recent job history, and your configuration — to a reporting endpoint. By default this is a Google Apps Script endpoint operated by the Vireo project, so the bundle is received and processed on Google's infrastructure. Credentials (tokens, keys, secrets, and passwords) are redacted, but the bundle can still include local file paths, such as the photo folders you have added and the source paths of recent scans or imports. Nothing is sent unless you submit a report.

Beyond the bug reports you choose to submit, the Vireo application has no analytics, usage telemetry, advertising trackers, persistent installation identifiers, or automated crash reporting.

## Website measurement

When you visit `vireo.photo`, we use Cloudflare Web Analytics to understand aggregate website traffic and performance. The information available to us includes pages visited, referring sites, browser, operating system, country, and page-performance metrics such as page load time and Core Web Vitals. Cloudflare Web Analytics does not record URL query strings. We use this information only to understand how people discover Vireo and how the website performs.

Cloudflare Web Analytics does not use cookies, browser storage, persistent identifiers, or cross-site tracking. Cloudflare says Web Analytics does not collect or use visitors' personal data and does not track individual end users across customers' websites. Vireo does not receive or store your IP address. Learn more in [Cloudflare's Web Analytics documentation](https://developers.cloudflare.com/web-analytics/).

Installer files are hosted by GitHub. GitHub provides the Vireo project with an aggregate download count for each release file and handles download requests under [GitHub's privacy statement](https://docs.github.com/en/site-policy/privacy-policies/github-general-privacy-statement). These counts are not connected to identifiable website visitors by Vireo.

## Credentials

API tokens and keys you provide (for iNaturalist, HuggingFace, and Google Maps) are stored in plaintext in `~/.vireo/config.json` on your device. Vireo does not transmit these credentials to anyone other than the respective service they belong to. Protect this file as you would any file containing credentials.

## Changes to this policy

If this policy changes, the updated version will be posted in the Vireo repository with a new effective date.

## Contact

For privacy questions, open an issue at [github.com/jss367/vireo](https://github.com/jss367/vireo/issues).
