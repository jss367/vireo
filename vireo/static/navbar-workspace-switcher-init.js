import { vireoEditNav } from './navbar-edit-nav.js';
import { safeFetch } from './navbar-shared-helpers.js';

export var vireoWorkspaceSwitcher = VireoWorkspaceSwitcher.create({
  // safeFetch is declared in the later shared-helpers script.
  /** @template T @param {string} url @param {RequestInit} [init] @param {{toast?: boolean}} [options] @returns {Promise<T>} */
  fetch: function(url, init, options) { return safeFetch(url, init, options); },
  navigate: function(path) { window.location.href = path; },
  clearWorkspaceCursors: function() {
    if (vireoEditNav) vireoEditNav.clearWorkspaceScopedCursors();
  }
});
document.addEventListener('DOMContentLoaded', function() {
  vireoWorkspaceSwitcher.loadCurrentName();
});

/* ---------- XMP drift check on workspace switch ---------- */
if (sessionStorage.getItem('vireo_ws_switched')) {
  sessionStorage.removeItem('vireo_ws_switched');
  document.addEventListener('DOMContentLoaded', function() {
  /** @type {Promise<unknown[]>} */ (safeFetch('/api/audit/drift', {}, { toast: false })).then(function(/** @type {unknown[]} */ drifts) {
    if (drifts && drifts.length > 0) {
      var driftToast = document.createElement('div');
      driftToast.style.cssText = 'position:fixed;bottom:24px;right:24px;background:var(--bg-secondary);border:1px solid var(--warning);border-radius:8px;padding:12px 16px;z-index:2000;box-shadow:0 4px 16px rgba(0,0,0,0.4);max-width:360px;font-size:13px;color:var(--text-primary);';
      driftToast.innerHTML = '<div style="font-weight:600;margin-bottom:4px;color:var(--warning);">XMP Drift Detected</div>'
        + '<div style="color:var(--text-secondary);">' + drifts.length + ' photo' + (drifts.length > 1 ? 's have' : ' has')
        + ' keywords that differ between the database and XMP sidecars.</div>'
        + '<div style="margin-top:8px;display:flex;gap:8px;">'
        + '<a href="/audit" style="color:var(--accent);text-decoration:none;font-weight:500;">Review in Audit</a>'
        + '<span data-drift-dismiss style="color:var(--text-muted);cursor:pointer;margin-left:auto;">Dismiss</span>'
        + '</div>';
      /** @type {HTMLElement} */ (driftToast.querySelector('[data-drift-dismiss]')).addEventListener('click', function() {
        driftToast.remove();
      });
      document.body.appendChild(driftToast);
      setTimeout(function() { if (driftToast.parentElement) driftToast.remove(); }, 15000);
    }
  }).catch(function() {});
  });
}
