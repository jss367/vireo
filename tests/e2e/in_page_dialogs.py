"""Answer the page's own questions and notices from a test.

``Vireo.confirm``, ``Vireo.prompt`` and ``Vireo.alert`` (vireo-utils.js) are
ordinary DOM, not native dialogs: Playwright raises no ``dialog`` event for
them, so ``page.on("dialog", ...)`` never runs. A test either drives the open
dialog itself (``accept_dialog`` / ``dismiss_dialog`` wait for it), or arms an
answer before the click that opens it (``answer_dialogs``). Either way the
real buttons are pressed, and ``dialogs(page)`` lists what each dialog that
``answer_dialogs`` answered said.
"""
import json

from playwright.sync_api import expect

DIALOG = "#vireoConfirmDialog"
INPUT = f"{DIALOG} [data-vireo-confirm=input]"
CANCEL = f"{DIALOG} [data-vireo-confirm=cancel]"
ACCEPT = f"{DIALOG} [data-vireo-confirm=accept]"

# What tauri-plugin-dialog's init script does to window.confirm in the desktop
# app, plus the two dialogs its webview never shows. A page that still gated an
# action on a native dialog runs it unasked under these.
DESKTOP_NATIVE_DIALOGS = """
window.confirm = async function() { return true; };
window.alert = function() {};
window.prompt = function() { return null; };
"""

# The dialog's text as a reader meets it: title, paragraphs, then list rows.
_TEXT = """dialog => [dialog.querySelector('h3')]
  .concat(Array.from(dialog.querySelectorAll('.vireo-confirm-text, .vireo-confirm-details li')))
  .map(node => node.textContent).join('\\n')"""

_ARM = """config => {
  if (window.__vireoDialogAnswer) window.__vireoDialogAnswer.disconnect();
  let remaining = config.times;
  const text = %s;
  const observer = new MutationObserver(records => {
    for (const record of records) {
      for (const node of record.addedNodes) {
        if (!(node instanceof HTMLDialogElement) || node.id !== 'vireoConfirmDialog') continue;
        window.__vireoDialogSaid(text(node));
        const input = node.querySelector('[data-vireo-confirm=input]');
        if (input && config.text !== null) input.value = config.text;
        const cancel = node.querySelector('[data-vireo-confirm=cancel]');
        const accept = node.querySelector('[data-vireo-confirm=accept]');
        (config.accept || !cancel ? accept : cancel).click();
        if (remaining !== null && --remaining <= 0) observer.disconnect();
      }
    }
  });
  const start = () => observer.observe(document.body, {childList: true});
  if (document.body) start();
  else document.addEventListener('DOMContentLoaded', start);
  window.__vireoDialogAnswer = observer;
}""" % _TEXT


def _log(page):
    """The page's record of answered dialogs. It lives in the test process,
    so it survives the reloads and navigations a dialog's answer can cause."""
    log = getattr(page, "_vireo_dialog_log", None)
    if log is None:
        log = []
        page._vireo_dialog_log = log
        page.expose_function("__vireoDialogSaid", lambda said: log.append(said))
    return log


def dialog_text(page):
    """The open dialog's title, paragraphs and list rows, one per line."""
    return page.locator(DIALOG).evaluate(_TEXT)


def accept_dialog(page, text=None):
    """Wait for a dialog, put ``text`` in its field if given, press its
    confirm button, and return what it said."""
    expect(page.locator(DIALOG)).to_be_visible()
    said = dialog_text(page)
    if text is not None:
        field = page.locator(INPUT)
        if field.evaluate("node => node.tagName") == "SELECT":
            field.select_option(text)
        else:
            field.fill(text)
    page.locator(ACCEPT).click()
    expect(page.locator(DIALOG)).to_have_count(0)
    return said


def dismiss_dialog(page):
    """Wait for a question, press Cancel, and return what it said."""
    expect(page.locator(DIALOG)).to_be_visible()
    said = dialog_text(page)
    page.locator(CANCEL).click()
    expect(page.locator(DIALOG)).to_have_count(0)
    return said


def answer_dialogs(page, accept=True, text=None, times=None):
    """Answer every dialog the page opens from now on: confirm (or Cancel
    with ``accept=False``), after putting ``text`` in a prompt's field.

    Stands in for ``page.on("dialog", ...)``. With ``times`` it answers only
    that many, in the current document. Without it the answer also survives
    navigations, as a native handler did. A later call replaces this one.
    """
    _log(page)
    config = {"accept": accept, "text": text, "times": times}
    if times is None:
        page.add_init_script(f"({_ARM})({json.dumps(config)})")
    page.evaluate(_ARM, config)


def dialogs(page):
    """What each dialog answered by ``answer_dialogs`` said, oldest first."""
    log = _log(page)
    # A round trip, so a dialog answered a moment ago has reported in.
    page.evaluate("1")
    return list(log)
