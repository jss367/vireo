# Download Measurement

Vireo measures interest in downloads without adding analytics or identifiers to the desktop application. Two independent sources answer different questions:

- Plausible Analytics reports aggregate visits to `vireo.photo`, referral sources, campaign parameters, approximate location and device information, and outbound-link clicks, including installer links.
- GitHub reports the cumulative download count for each release asset.

A download-button click is not proof that a download completed. A GitHub asset download is not a unique person or confirmed installation; retries, automated traffic, and maintainer testing may be included.

## Enable website measurement

1. Add `vireo.photo` as a site in Plausible Analytics.
2. In Plausible's site installation settings, enable **Outbound links** and leave other optional measurements disabled unless the privacy policy is updated first.
3. Copy the unique script URL from the Plausible installation snippet. It has the form `https://plausible.io/js/pa-….js`.
4. In the GitHub repository, open **Settings → Secrets and variables → Actions → Variables** and create `PLAUSIBLE_SCRIPT_URL` with that URL as its value.
5. Run the **Deploy Website** workflow or merge a website change to `main`.

The website includes the tracker only when `PUBLIC_PLAUSIBLE_SCRIPT_URL` is present at build time. Local builds do not send analytics by default.

To verify the deployed integration, view the page source and confirm that the Plausible script URL and `plausible.init()` are present. Then click an installer link from a separate browser session and confirm that an `Outbound Link: Click` event appears in Plausible.

## Measure campaigns

Point announcements at the Vireo download page with descriptive campaign parameters, for example:

```text
https://vireo.photo/download/?utm_source=reddit&utm_campaign=windows_public_beta
```

Use only broad source and campaign names. Do not place names, email addresses, usernames, or other person-specific information in campaign parameters.

## Report GitHub downloads

Run:

```bash
python scripts/report_release_downloads.py
```

Use `--json` for structured output or `--limit 0` to include every published release. The script uses `GITHUB_TOKEN` when available and otherwise calls GitHub's public API without authentication.

The report groups the public installer formats linked from the website:

- macOS: `.dmg`
- Windows: `-setup.exe`
- Linux: `.deb`

macOS automatic updates use a separate `.app.tar.gz` asset, and Linux automatic updates use a separate `.AppImage` asset. Windows automatic updates reuse the `-setup.exe` installer, so the Windows GitHub count includes both manual downloads and automatic updates. Use Plausible installer-link clicks to estimate manual Windows download intent.

GitHub counts are cumulative. Save periodic JSON output if historical snapshots are needed.
