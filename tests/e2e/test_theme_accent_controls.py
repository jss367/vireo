"""Native controls take the theme's accent on every page.

`accent-color` is inherited from `:root` (vireo-theme.css), so a slider needs
no rule of its own. Highlights and the photo editor style their sliders without
one, and the browser drew them in its default blue.
"""

import pytest

SLIDER_ACCENTS = """() => {
    const probe = document.createElement('span');
    probe.style.color = 'var(--accent)';
    document.body.appendChild(probe);
    const accent = getComputedStyle(probe).color;
    probe.remove();
    const sliders = [...document.querySelectorAll('input[type="range"]')];
    return {
        accent,
        count: sliders.length,
        other: sliders.filter(s => getComputedStyle(s).accentColor !== accent).map(s => s.id),
    };
}"""


@pytest.mark.parametrize("theme", ["vireo-gold", "vireo-dark"])
@pytest.mark.parametrize("path", ["/highlights", "/edit/{photo_id}"])
def test_sliders_use_the_theme_accent(live_server, page, path, theme):
    photo_id = live_server["data"]["photos"][0]
    page.goto(live_server["url"] + path.format(photo_id=photo_id))
    page.evaluate("theme => document.documentElement.setAttribute('data-theme', theme)", theme)
    found = page.evaluate(SLIDER_ACCENTS)
    assert found["count"] > 0
    assert found["other"] == [], found
