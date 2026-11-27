"""No native JavaScript dialogs anywhere in the app's pages.

The desktop app's webview replaces ``window.confirm()`` with an async
function, so ``if (!confirm(...)) return;`` sees a promise (always truthy) and
the action runs unasked; ``window.alert()`` shows nothing and
``window.prompt()`` returns null. Pages ask through ``Vireo.confirm``,
``Vireo.prompt`` and ``Vireo.alert`` (vireo-utils.js) instead.

ESLint's ``no-alert`` holds the scripts under ``vireo/static/`` to that. It
does not read templates, so this test does: no inline ``<script>`` block and
no ``on*=`` handler may call a native dialog.
"""
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
TEMPLATES = ROOT / "vireo" / "templates"

# confirm(...), alert(...) or prompt(...) called bare or on the global object.
# A method of anything else (Vireo.confirm, browser.confirm) is not native.
NATIVE_DIALOG = re.compile(
    r"(?<![.\w$])(?:(?:window|globalThis|self)\s*\.\s*)?(confirm|alert|prompt)\s*\("
)
INLINE_SCRIPT = re.compile(r"<script(?![^>]*\bsrc=)[^>]*>(.*?)</script>", re.S | re.I)
INLINE_HANDLER = re.compile(r"""\son[a-z]+\s*=\s*(?:"([^"]*)"|'([^']*)')""", re.I)
BLOCK_COMMENT = re.compile(r"/\*.*?\*/", re.S)
# A // comment, but not the // of a URL inside a string.
LINE_COMMENT = re.compile(r"(?<![:\"'])//[^\n]*")


def native_dialog_calls(html):
    """The native dialog calls in a template's inline scripts and handlers."""
    code = [BLOCK_COMMENT.sub("", LINE_COMMENT.sub("", block)) for block in INLINE_SCRIPT.findall(html)]
    code += [double or single for double, single in INLINE_HANDLER.findall(html)]
    return [match.group(0) for block in code for match in NATIVE_DIALOG.finditer(block)]


@pytest.mark.parametrize("html, found", [
    ("<script>if (!confirm('Sure?')) return;</script>", ["confirm("]),
    ("<script>window.alert('Done');</script>", ["window.alert("]),
    ("<script>var name = globalThis . prompt ('Name');</script>", ["globalThis . prompt ("]),
    ('<button onclick="if(confirm(\'Delete?\')) remove()">Delete</button>', ["confirm("]),
    ("<script>await Vireo.confirm({title: 'Sure?'});</script>", []),
    ("<script>browser.confirm(); reconfirm(); // alert('x')</script>", []),
    ("<script>/* window.prompt() returns null */ load('http://host/alert');</script>", []),
    ('<script src="/static/page/boot.js"></script><p>alert(1)</p>', []),
])
def test_the_scan_finds_native_dialog_calls(html, found):
    assert native_dialog_calls(html) == found


def test_no_template_calls_a_native_dialog():
    offenders = {}
    for template in sorted(TEMPLATES.rglob("*.html")):
        calls = native_dialog_calls(template.read_text(encoding="utf-8"))
        if calls:
            offenders[str(template.relative_to(ROOT))] = calls
    assert not offenders, (
        "Native dialogs do nothing in the desktop app. Ask with Vireo.confirm, "
        f"Vireo.prompt or Vireo.alert (vireo-utils.js) instead: {offenders}"
    )


def test_eslint_forbids_native_dialogs_in_every_static_script():
    config = (ROOT / "eslint.config.mjs").read_text(encoding="utf-8")
    base = config[config.index("files: ['vireo/static/**/*.js']"):]
    base = base[:base.index("\n  },\n")]
    assert "'no-alert': 'error'" in base
    # No other block may set the rule, so none switches it back off for some files.
    assert re.findall(r"['\"]no-alert['\"]\s*:\s*([^,}\n]+)", config) == ["'error'"]
