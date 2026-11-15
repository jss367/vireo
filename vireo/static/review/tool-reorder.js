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
  closed.concat(open).forEach(function(/** @type {Element} */ el) { host.appendChild(el); });
}

function bindReviewToolReorder() {
  var container = /** @type {HTMLElement|null} */ (document.querySelector('.review-tools'));
  if (!container) return;
  reorderReviewTools();
  var children = /** @type {Element[]} */ (Array.prototype.slice.call(container.children));
  children.forEach(function(/** @type {Element} */ el) {
    if (el.tagName === 'DETAILS') el.addEventListener('toggle', reorderReviewTools);
  });
  var rqEnabled = document.getElementById('rqEnabled');
  if (rqEnabled) rqEnabled.addEventListener('change', reorderReviewTools);
}

document.addEventListener('DOMContentLoaded', bindReviewToolReorder);
