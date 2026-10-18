"""Observe and drive shared navbar behavior from the outside.

The navbar and lightbox are ES modules: their own calls to ``showToast``,
``safeFetch`` or ``safeEventSource`` go to the imported function, so replacing
``window.showToast`` in a test no longer intercepts them. Tests observe the
toast the user sees and answer the requests the page makes instead.
"""
import json

_RECORD_TOASTS = """() => {
  window.__toasts = [];
  const container = document.getElementById('toastContainer');
  new MutationObserver(records => {
    for (const record of records) {
      for (const node of record.addedNodes) {
        if (node.nodeType === Node.ELEMENT_NODE) {
          window.__toasts.push({message: node.textContent, type: node.dataset.type});
        }
      }
    }
  }).observe(container, {childList: true});
}"""


def record_toasts(page):
    """Record every toast the page shows from now on in ``window.__toasts``
    as ``{message, type}``."""
    page.evaluate(_RECORD_TOASTS)


def toasts(page, prefix=""):
    """The toasts recorded so far whose message starts with ``prefix``."""
    return [toast for toast in page.evaluate("window.__toasts") if toast["message"].startswith(prefix)]


def fulfill_sse(route, event, payload):
    """Answer an EventSource request with one ``event`` carrying ``payload``."""
    route.fulfill(
        status=200,
        headers={"Content-Type": "text/event-stream", "Cache-Control": "no-cache"},
        body=f"event: {event}\ndata: {json.dumps(payload)}\n\n",
    )
