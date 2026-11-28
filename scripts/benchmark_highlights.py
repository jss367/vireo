#!/usr/bin/env python3
"""Time workspace Highlights navigation and pagination on an isolated synthetic catalog.

Run from a checkout with Playwright Chromium installed. To compare revisions,
run this same script from each checkout's root.
"""

import base64
import os
import sqlite3
import sys
import tempfile
import threading
import time
from pathlib import Path
from unittest.mock import patch

from playwright.sync_api import sync_playwright
from werkzeug.serving import make_server

# Use the checkout being measured, including when this script is outside it.
sys.path[:0] = [str(Path.cwd() / "scripts"), str(Path.cwd() / "vireo")]

THUMBNAIL = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+iKX8AAAAASUVORK5CYII="
)


def seed(path):
    from benchmark_library import _seed

    _seed(path, 83_955)
    with sqlite3.connect(path) as conn:
        conn.executemany(
            "INSERT INTO keywords(name, is_species, type) VALUES (?, 1, 'taxonomy')",
            [(f"Bird {i}",) for i in range(725)],
        )
        first = conn.execute("SELECT id FROM keywords WHERE name = 'Bird 0'").fetchone()[0]
        conn.execute("INSERT INTO photo_keywords(photo_id,keyword_id) SELECT id, ?+id%725 FROM photos", (first,))
        conn.execute("UPDATE photos SET quality_score=(id%101)/100., subject_tenengrad=id%701")
        conn.execute("""INSERT INTO detections(photo_id, detector_confidence, category, box_x, box_y, box_w, box_h)
                        SELECT id, 0.9, 'animal', 0.1, 0.1, 0.4, 0.5 FROM photos WHERE id%3=0""")
        conn.execute("""INSERT INTO predictions(detection_id, classifier_model, species, confidence)
                        SELECT id, 'sim', 'Bird '||(photo_id%725), 0.91 FROM detections""")
        conn.execute("""INSERT INTO photo_keywords(photo_id,keyword_id)
                        SELECT id, ?+(id+1)%725 FROM photos WHERE id%150=0""", (first,))


def measure(page, url):
    page.route("**/thumbnails/**", lambda route: route.fulfill(content_type="image/png", body=THUMBNAIL))
    for visit in range(1, 4):
        start = time.perf_counter()
        page.goto(url + "/highlights", wait_until="domcontentloaded")
        page.locator("section.bucket .highlights-card").first.wait_for(state="visible", timeout=60_000)
        page.evaluate("() => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve)))")
        elapsed = time.perf_counter() - start
        assert page.locator("section.bucket").count() == 725
        print(f"Visit {visit}, first ranked card: {elapsed:.3f}s", flush=True)
        page.wait_for_timeout(250)
    for request in (
        "/api/highlights?scope=workspace&view=cards&limit_per_bucket=20",
        "/api/highlights/bucket?scope=workspace&view=cards&species=Bird%201&offset=20&limit=500",
    ):
        start = time.perf_counter()
        response = page.request.get(url + request)
        assert response.ok
        size = len(response.body())
        print(f"{request}: {time.perf_counter() - start:.3f}s, {size} bytes", flush=True)


def main():
    with sync_playwright() as playwright, tempfile.TemporaryDirectory(prefix="vireo-highlights-") as tmp:
        browser = playwright.chromium.launch()
        with patch.dict(os.environ, HOME=tmp, VIREO_DISABLE_BROWSER_AUTH="1", VIREO_DISABLE_STARTUP_BACKFILL_TIMERS="1"):
            path = os.path.join(tmp, "library.db")
            seed(path)
            from app import create_app

            app = create_app(db_path=path, thumb_cache_dir=os.path.join(tmp, "thumbnails"))
            with make_server("127.0.0.1", 0, app, threaded=True) as server:
                threading.Thread(target=server.serve_forever, daemon=True).start()
                try:
                    measure(browser.new_page(), f"http://127.0.0.1:{server.server_port}")
                finally:
                    browser.close()
                    server.shutdown()
                    app._cleanup_app_resources()


if __name__ == "__main__":
    main()
