// Standing notices: the rows under "Needs attention" in the notifications
// panel (missing folders and originals, new images, duplicate cleanup, the
// CPU-only warning, and a page's pending XMP changes). Each owner still shows
// and hides its own row by setting an inline display; this module moves page
// rows into the panel, counts the rows that are showing, and reports that
// count to the bell.
import { closeNotificationsForAction, setStandingNotices } from './navbar-notifications.js';

const container = document.getElementById('standingNotices');
const emptyNote = document.getElementById('standingNoticesEmpty');

/** A row shows only while its owner has set an inline display other than
 * none; the stylesheet alone never shows one.
 * @param {Element} element */
function isShowing(element) {
  if (!(element instanceof HTMLElement) || element.hidden) return false;
  return !!element.style.display && element.style.display !== 'none';
}

/** @param {Element} notice */
function noticeText(notice) {
  const text = notice.querySelector('[data-notice-text]');
  return ((text || notice).textContent || '').replace(/\s+/g, ' ').trim();
}

/** @param {Element[]} notices */
export function summarizeStandingNotices(notices) {
  const showing = notices.filter(isShowing);
  return {
    count: showing.length,
    urgent: showing.some(notice => notice.getAttribute('data-notice-level') === 'warning'),
    summary: showing.map(noticeText).filter(Boolean).join('\n'),
  };
}

function report() {
  if (!container) return;
  const state = summarizeStandingNotices(Array.from(container.children));
  if (emptyNote) emptyNote.hidden = state.count > 0;
  setStandingNotices(state);
}

/** @param {Element} action */
function focusNoticeDialog(action) {
  const dialogId = action.getAttribute('data-notice-dialog');
  const dialog = dialogId ? document.getElementById(dialogId) : null;
  if (!dialog || !dialog.getClientRects().length) return;
  const controls = dialog.querySelectorAll('button:not(:disabled), a[href], input:not(:disabled), select:not(:disabled), textarea:not(:disabled), [tabindex]:not([tabindex="-1"])');
  const first = Array.from(controls).find(control => control instanceof HTMLElement && control.getClientRects().length);
  if (first instanceof HTMLElement) first.focus();
  else {
    dialog.tabIndex = -1;
    dialog.focus();
  }
}

if (container) {
  // A page partial parsed after the navbar (the sync panel) declares its row
  // in place; it belongs with the others. Module scripts run after parsing,
  // so every such row exists by now.
  document.querySelectorAll('[data-standing-notice]').forEach(notice => {
    if (notice.parentElement !== container) container.appendChild(notice);
  });
  // An action that opens a dialog or leaves the page must not stay covered by
  // the panel. The row's own handler has already run when this one fires.
  container.addEventListener('click', event => {
    const target = event.target instanceof Element ? event.target : null;
    const action = target?.closest('[data-notice-closes-panel]');
    if (action) {
      focusNoticeDialog(action);
      closeNotificationsForAction();
    }
  });
  new MutationObserver(report).observe(container, {
    subtree: true, childList: true, characterData: true,
    attributes: true, attributeFilter: ['style', 'hidden'],
  });
  report();
}
