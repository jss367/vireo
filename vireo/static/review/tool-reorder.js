// Keep the Review tools' DOM order in step with their visual order.
// An opened tool is given full flex-basis so it sits on its own row below
// the compact controls. Without a matching DOM move, keyboard focus still
// leaves the expanded panel into the row above it, jumping backward past
// unrelated compact controls. Moving the opened node to the end of
// .review-tools keeps tab order aligned with what the user sees.

/** @param {Element} el */
function reviewToolIsOpen(el) {
  if (el.tagName === 'DETAILS') return /** @type {HTMLDetailsElement} */ (el).open;
  if (el.classList && el.classList.contains('review-queue')) {
    var cb = /** @type {HTMLInputElement|null} */ (el.querySelector('#rqEnabled'));
    return !!(cb && cb.checked);
  }
  return false;
}

function reorderReviewTools() {
  var container = /** @type {HTMLElement|null} */ (document.querySelector('.review-tools'));
  if (!container) return;
  var active = document.activeElement;
  var children = /** @type {Element[]} */ (Array.prototype.slice.call(container.children));
  children.forEach(function(/** @type {Element} */ el, /** @type {number} */ i) {
    if (!el.hasAttribute('data-review-tool-order')) {
      el.setAttribute('data-review-tool-order', String(i));
    }
  });
  /** @type {Element[]} */ var closed = [];
  /** @type {Element[]} */ var open = [];
  children.forEach(function(/** @type {Element} */ el) {
    if (reviewToolIsOpen(el)) open.push(el);
    else closed.push(el);
  });
  var byOriginal = function(/** @type {Element} */ a, /** @type {Element} */ b) {
    return parseInt(a.getAttribute('data-review-tool-order') || '0', 10)
         - parseInt(b.getAttribute('data-review-tool-order') || '0', 10);
  };
  closed.sort(byOriginal);
  open.sort(byOriginal);
  var host = container;
  closed.concat(open).forEach(function(/** @type {Element} */ el, /** @type {number} */ index) {
    if (host.children[index] !== el) host.insertBefore(el, host.children[index] || null);
  });
  // A necessary DOM move can blur a focused descendant. Preserve the user's
  // active summary, checkbox or editor without scrolling it to a new position.
  if (active instanceof HTMLElement && host.contains(active) && document.activeElement !== active) {
    active.focus({preventScroll: true});
  }
}

function bindReviewToolReorder() {
  var container = /** @type {HTMLElement|null} */ (document.querySelector('.review-tools'));
  if (!container) return;
  reorderReviewTools();
  // Details' native toggle event is queued later; reorder at the attribute
  // change so the next Tab step already follows the displayed controls.
  new MutationObserver(reorderReviewTools).observe(container, {
    subtree: true, attributes: true, attributeFilter: ['open'],
  });
  var rqEnabled = document.getElementById('rqEnabled');
  if (rqEnabled) rqEnabled.addEventListener('change', reorderReviewTools);
}
