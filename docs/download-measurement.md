# Download Measurement

Vireo measures interest in downloads without adding analytics or identifiers to the desktop application. Two independent sources answer different questions:

- Cloudflare Web Analytics reports aggregate visits to `vireo.photo`, referral sources, approximate location and device information, and page-performance metrics. Cloudflare Web Analytics does not record query strings, so campaign parameters like `utm_source` are not available.
- GitHub reports the cumulative download count for each release asset.

A website visit is not proof that someone downloaded Vireo. A GitHub asset download is not a unique person or confirmed installation; retries, automated traffic, and maintainer testing may be included.

## Enable website measurement

1. In the Cloudflare dashboard, open **Web Analytics**.
2. Select **Add a site** and add `vireo.photo`.
3. Open **Manage site** and copy the token from the JavaScript beacon snippet. The snippet loads `https://static.cloudflareinsights.com/beacon.min.js` as a module script and includes a `data-cf-beacon` value like `{"token":"..."}`.
4. In the GitHub repository, open **Settings → Secrets and variables → Actions → Variables** and create `CLOUDFLARE_WEB_ANALYTICS_TOKEN` with that token as its value.
5. Run the **Deploy Website** workflow or merge a website change to `main`.

The website includes the tracker only when `PUBLIC_CLOUDFLARE_WEB_ANALYTICS_TOKEN` is present at build time. Local builds do not send analytics by default.

To verify the deployed integration, view the page source and confirm that `https://static.cloudflareinsights.com/beacon.min.js` and `data-cf-beacon` are present. Then load the site from a separate browser session and confirm that traffic appears in Cloudflare Web Analytics. Cloudflare says data can take a few minutes to appear.

## Attribute announcements

Cloudflare Web Analytics reports the referring site for each visit but does not record URL query strings, so `utm_source` and similar campaign parameters are dropped. To distinguish traffic from a specific announcement, either rely on the referrer that Cloudflare records for the source (for example, `reddit.com`) or point the announcement at a distinct URL path or fragment that Cloudflare will log as its own page view.

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

macOS automatic updates use a separate `.app.tar.gz` asset, and Linux automatic updates use a separate `.AppImage` asset. Windows automatic updates reuse the `-setup.exe` installer, so the Windows GitHub count includes both manual downloads and automatic updates.

GitHub counts are cumulative. Save periodic JSON output if historical snapshots are needed.
