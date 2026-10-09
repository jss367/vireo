"""E2E: a RAW+JPEG pair's card names both formats.

The pair is one photo whose extension is the RAW's; the JPEG lives only in
``companion_path``. The extension filter matches the pair by its JPEG, so a
card that only said "NEF" would contradict the filter that found it.
"""

import config as cfg
from playwright.sync_api import expect


def test_browse_card_extension_badge_names_companion(live_server, page):
    db = live_server["db"]
    pair, plain = live_server["data"]["photos"][:2]
    db.conn.execute(
        "UPDATE photos SET filename = '_D854674.NEF', extension = '.nef',"
        " companion_path = '_D854674.jpg' WHERE id = ?",
        (pair,),
    )
    db.conn.commit()
    settings = cfg.load()
    settings["browse_card_fields"] = ["filename", "extension"]
    cfg.save(settings)

    page.goto(f"{live_server['url']}/browse")

    pair_badge = page.locator(f'[data-id="{pair}"] .grid-card-ext')
    expect(pair_badge).to_have_text("NEF + JPG", timeout=10000)
    expect(pair_badge).to_have_attribute("title", "_D854674.NEF + _D854674.jpg")
    expect(page.locator(f'[data-id="{plain}"] .grid-card-ext')).to_have_text("JPG")
