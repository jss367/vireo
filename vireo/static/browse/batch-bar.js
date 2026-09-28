/* Browse: the floating batch action bar.
   Classic script sharing page globals; browse.html loads the
   browse/*.js files in dependency order. */

// Wrapping and inspector/sidebar resizing change the space the bar overlays.
new ResizeObserver(function(entries) {
  var bar = entries[0].target;
  bar.parentElement.style.setProperty('--batch-bar-height', bar.offsetHeight + 'px');
}).observe(document.getElementById('batchBar'));

// The bar floats over the photo pane, and the click that raises it is also
// the first click of a double-click. If the bar accepted clicks the moment
// it appeared, a batch button — Delete among them — would sit under the
// cursor in time to take the second click, so the photo opens nothing and
// a destructive action is one pixel of bad luck away. Show the bar as
// inert (visible for feedback but pointer-events: none, so the second
// click passes through to the photo underneath) and only activate it once
// the click gesture is clearly done. A fixed hide-delay cannot be trusted
// alone because platform double-click thresholds vary widely (macOS around
// 500 ms, Windows around 500, and accessibility settings can push either
// past a second), so what ends the inert state is the pointer leaving the
// card it was pressed on: every batch button is somewhere it has to travel
// to, and a double-click does not travel. (1) That movement releases the
// bar at once, so buttons respond as soon as someone actually reaches for
// them; (2) a quiet timer releases it anyway for a pointer that never
// moves, so the bar is not stuck inert; (3) until the pointer does move, a
// click landing on the bar is redirected to the card underneath as a
// synthetic dblclick — which is what covers a double-click slower than
// that timer, without anyone having to guess the interval. Every fresh
// card mousedown re-inerts the bar and restarts the timer. Hiding stays
// immediate.
var BATCH_BAR_ACTIVATE_QUIET_MS = 1500;
// Distance the pointer has to travel before we treat the movement as a
// deliberate reach for a batch button. Well within the noise of a
// stationary hand, well below the distance from any card centre to the
// nearest batch button.
var BATCH_BAR_ACTIVATE_MOVE_PX = 8;
var batchBarActivateTimer = null;
var batchBarLastCardMouseDown = null;
// Whether the pointer has left that mousedown's neighbourhood. This, and not
// any elapsed time, is what says the click gesture is over: every batch button
// is somewhere the pointer has to travel to, and a double-click does not
// travel. While it stays false the gesture is still open no matter how long
// the platform's double-click interval is, so neither the release below nor
// the straggling-click redirect needs a window to guess at.
var batchBarPointerMovedSinceCardDown = true;

function scheduleBatchBarActivation() {
  if (batchBarActivateTimer) clearTimeout(batchBarActivateTimer);
  batchBarActivateTimer = setTimeout(activateBatchBar, BATCH_BAR_ACTIVATE_QUIET_MS);
}

function activateBatchBar() {
  var bar = document.getElementById('batchBar');
  if (bar) bar.classList.remove('batch-bar-inert');
  if (batchBarActivateTimer) {
    clearTimeout(batchBarActivateTimer);
    batchBarActivateTimer = null;
  }
}

// A mousedown on a photo card could be the first (or Nth) click of a
// double-click. Record where it landed and reset the moved-since flag the
// guards below read (recorded even before the bar is visible, so the very
// first click of a fresh double-click is covered), and re-inert the bar
// and extend the quiet window whenever the bar is already up — this
// covers both the initial gesture that raises the bar and gestures on
// cards while the bar is already visible from an earlier selection.
// Mousedowns targeting the bar itself (a batch button) are left to the
// click guard below. Capture phase so this runs even if a card's handler
// stops propagation.
document.addEventListener('mousedown', function(event) {
  if (event.button !== 0) return;
  var target = event.target;
  if (!target || !target.closest) return;
  if (target.closest('.grid-card, .browse-stack-member')) {
    batchBarLastCardMouseDown = {x: event.clientX, y: event.clientY};
    batchBarPointerMovedSinceCardDown = false;
  }
  var bar = document.getElementById('batchBar');
  if (!bar || bar.style.display !== 'flex') return;
  if (target.closest('#batchBar')) return;
  if (!target.closest('.grid-card, .browse-stack-member')) return;
  if (!bar.classList.contains('batch-bar-inert')) {
    bar.classList.add('batch-bar-inert');
  }
  scheduleBatchBarActivation();
}, true);

// Pointer movement past a small threshold from the last card mousedown
// means the user is reaching for a button rather than continuing to
// click at the same spot. Activate immediately so batch buttons respond
// as soon as the user actually wants them to.
document.addEventListener('mousemove', function(event) {
  var last = batchBarLastCardMouseDown;
  if (!last || batchBarPointerMovedSinceCardDown) return;
  var dx = event.clientX - last.x;
  var dy = event.clientY - last.y;
  if (dx * dx + dy * dy < BATCH_BAR_ACTIVATE_MOVE_PX * BATCH_BAR_ACTIVATE_MOVE_PX) return;
  // Recorded whatever the bar is doing, because the straggling-click redirect
  // below reads it after the quiet timer has already released the bar.
  batchBarPointerMovedSinceCardDown = true;
  var bar = document.getElementById('batchBar');
  if (bar && bar.style.display === 'flex'
      && bar.classList.contains('batch-bar-inert')) {
    activateBatchBar();
  }
}, true);

// A click that lands on the bar without the pointer having left the card
// it was last pressed on is the second half of a double-click that raised
// the bar over that card. If the bar activated between the two clicks —
// a double-click interval slower than the quiet timer, which any platform
// can be configured to have — this cancels the click on the bar and
// dispatches a dblclick on the card underneath, so the photo opens
// instead of the button firing.
document.addEventListener('click', function(event) {
  var target = event.target;
  if (!target || !target.closest) return;
  if (!target.closest('#batchBar')) return;
  var bar = document.getElementById('batchBar');
  if (!bar || bar.style.display !== 'flex') return;
  // A keyboard-activated click (Enter/Space on a focused batch button, or a
  // script-dispatched click on one) belongs to that button, not to any
  // mouse gesture. The pointer never moved for it — its coordinates are
  // meaningless for elementFromPoint — so the guards below would misread
  // it as the stationary tail of a card's double-click and swallow it. The
  // browser marks such clicks with detail === 0; a real mouse click is 1
  // or more.
  if (event.detail === 0) return;
  if (!batchBarLastCardMouseDown) return;
  // The pointer has not left the card it was pressed on, so this click cannot
  // be a reach for a button — every button is somewhere it would have had to
  // travel to. It is the second half of that card's double-click, arriving
  // after the quiet timer gave up waiting. No elapsed-time bound: the whole
  // point is that the platform's interval is not ours to guess. Once the
  // pointer does move, the release above fires and this stops applying.
  if (batchBarPointerMovedSinceCardDown) return;
  event.stopImmediatePropagation();
  event.preventDefault();
  bar.classList.add('batch-bar-inert');
  scheduleBatchBarActivation();
  var priorPointerEvents = bar.style.pointerEvents;
  bar.style.pointerEvents = 'none';
  var under = document.elementFromPoint(event.clientX, event.clientY);
  bar.style.pointerEvents = priorPointerEvents;
  var card = under && under.closest
    ? under.closest('.grid-card, .browse-stack-member')
    : null;
  if (!card) return;
  card.dispatchEvent(new MouseEvent('dblclick', {
    bubbles: true, cancelable: true, view: window,
    button: 0, clientX: event.clientX, clientY: event.clientY,
  }));
}, true);

function updateBatchBar() {
  var bar = document.getElementById('batchBar');
  if (!bar) return;
  var ids = getActiveSelection();
  if (ids.length >= 1) {
    if (bar.style.display !== 'flex') {
      // Inert-until-quiet only exists to protect an in-flight card
      // double-click from being intercepted by the bar raised over that
      // card. Non-card selection flows — Ctrl/Cmd+A, a stack tray's
      // Select all, script-driven selection — have no such gesture to
      // shield, and starting inert there would let a batch-button click
      // in the quiet window pass through to the grid beneath and
      // replace the selection we just made. Only inert when a card
      // mousedown is actually pending.
      var cardGesturePending = batchBarLastCardMouseDown
        && !batchBarPointerMovedSinceCardDown;
      if (cardGesturePending) {
        bar.classList.add('batch-bar-inert');
        scheduleBatchBarActivation();
      } else {
        bar.classList.remove('batch-bar-inert');
      }
      bar.style.display = 'flex';
    }
    document.getElementById('batchCount').textContent =
      ids.length.toLocaleString() + ' selected' + browseSelectionStackNote(ids);
  } else {
    if (batchBarActivateTimer) {
      clearTimeout(batchBarActivateTimer);
      batchBarActivateTimer = null;
    }
    bar.classList.remove('batch-bar-inert');
    bar.style.display = 'none';
  }
  updateCompareButton(ids);
  updateBestBatchButton(ids);
  updateBurstReviewButton(ids);
  updateSelectionPanel(ids);
}

function openSelectedInBurstReview() {
  var ids = getActiveSelection();
  if (ids.length < 2) {
    showToast('Select at least two photos to review as a burst.', 'error');
    return;
  }
  if (ids.length > 500) {
    showToast('Select 500 or fewer photos to review as a burst.', 'error');
    return;
  }
  try {
    window.sessionStorage.setItem('vireo.browseBurstReviewIds', JSON.stringify(ids));
  } catch (e) {
    showToast('Could not open burst review for this selection.', 'error');
    return;
  }
  window.location.href = '/pipeline/review?browse_burst=1';
}
