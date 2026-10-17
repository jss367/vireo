/* Workspace-specific descriptions for the five photo color labels. */
(function () {
  'use strict';

  const COLORS = ['red', 'yellow', 'green', 'blue', 'purple'];
  const MAX_LENGTH = 120;
  /** @type {Record<string, unknown>} */
  let descriptions = {};
  let loaded = false;
  /** @type {Promise<Record<string, unknown>> | null} */
  let loadPromise = null;
  /** @type {string | null} */
  let editingColor = null;
  /** @type {number | null} */
  let escToken = null;

  /** @param {string} color */
  function colorName(color) {
    return color.charAt(0).toUpperCase() + color.slice(1);
  }

  /** @param {string} color */
  function isValid(color) {
    return COLORS.includes(color);
  }

  /** @param {string} url @param {RequestInit} [options] @returns {Promise<Record<string, unknown>>} */
  function request(url, options) {
    if (window.Vireo && window.Vireo.api) {
      return window.Vireo.api.json(url, options, { toast: false });
    }
    return fetch(url, options).then(function (response) {
      if (response.ok) return response.json();
      return response.json().catch(function () { return {}; }).then(function (body) {
        throw new Error(body.error || body.message || ('HTTP ' + response.status));
      });
    });
  }

  function ensureLoaded() {
    if (loaded) return Promise.resolve(descriptions);
    if (loadPromise) return loadPromise;
    loadPromise = request('/api/color-label-descriptions')
      .then(function (data) {
        descriptions = data && typeof data === 'object' ? data : {};
        loaded = true;
        refreshControls();
        return descriptions;
      })
      .finally(function () { loadPromise = null; });
    return loadPromise;
  }

  /** @param {string} color @returns {string} */
  function description(color) {
    const value = descriptions[color];
    return typeof value === 'string' ? value : '';
  }

  /** @param {string} color @param {string} [baseTitle] */
  function title(color, baseTitle) {
    const base = baseTitle || (colorName(color) + ' label');
    const text = description(color);
    return text
      ? base + ' — ' + text + ' · Right-click to edit'
      : base + ' · Right-click to add a description';
  }

  /** @param {HTMLElement} element */
  function refreshControl(element) {
    const color = element.dataset.color;
    if (!color || !COLORS.includes(color)) return;
    if (!element.dataset.colorLabelBaseTitle) {
      element.dataset.colorLabelBaseTitle = element.title || (colorName(color) + ' label');
    }
    if (!element.dataset.colorLabelBaseAria) {
      element.dataset.colorLabelBaseAria =
        element.getAttribute('aria-label') || (colorName(color) + ' label');
    }
    const text = description(color);
    element.title = title(color, element.dataset.colorLabelBaseTitle);
    element.setAttribute(
      'aria-label',
      text ? element.dataset.colorLabelBaseAria + ': ' + text : element.dataset.colorLabelBaseAria
    );
  }

  /** @param {ParentNode} [root] */
  function refreshControls(root) {
    const scope = root || document;
    scope.querySelectorAll('[data-color-label-control][data-color]').forEach(function(element) {
      if (element instanceof HTMLElement) refreshControl(element);
    });
  }

  /** @param {string} color */
  async function openEditor(color) {
    if (!color || !COLORS.includes(color)) return;
    if (typeof window.closeContextMenu === 'function') window.closeContextMenu();
    try {
      await ensureLoaded();
    } catch (error) {
      if (typeof window.showToast === 'function') {
        window.showToast((error instanceof Error ? error.message : '') || 'Could not load color label descriptions', 'error');
      }
      return;
    }

    editingColor = color;
    const modal = /** @type {HTMLElement} */ (document.getElementById('colorLabelDescriptionModal'));
    const input = /** @type {HTMLInputElement} */ (document.getElementById('colorLabelDescriptionInput'));
    const error = /** @type {HTMLElement} */ (document.getElementById('colorLabelDescriptionError'));
    /** @type {HTMLElement} */ (document.getElementById('colorLabelDescriptionHeading')).textContent =
      colorName(color) + ' label description';
    /** @type {HTMLElement} */ (document.getElementById('colorLabelDescriptionSwatch')).dataset.color = color;
    input.value = description(color);
    error.style.display = 'none';
    error.textContent = '';
    updateCount();
    modal.classList.add('open');
    if (window.Keymap) {
      if (escToken !== null) window.Keymap.popEsc(escToken);
      escToken = window.Keymap.pushEsc(closeEditor);
    }
    setTimeout(function () { input.focus(); input.select(); }, 0);
  }

  function closeEditor() {
    const modal = /** @type {HTMLElement} */ (document.getElementById('colorLabelDescriptionModal'));
    if (modal) modal.classList.remove('open');
    if (escToken !== null && window.Keymap) window.Keymap.popEsc(escToken);
    escToken = null;
    editingColor = null;
  }

  function updateCount() {
    const input = /** @type {HTMLInputElement} */ (document.getElementById('colorLabelDescriptionInput'));
    const counter = /** @type {HTMLElement} */ (document.getElementById('colorLabelDescriptionCount'));
    if (input && counter) counter.textContent = input.value.length + ' / ' + MAX_LENGTH;
  }

  async function saveEditor() {
    if (!editingColor) return;
    const color = editingColor;
    const input = /** @type {HTMLInputElement} */ (document.getElementById('colorLabelDescriptionInput'));
    const button = /** @type {HTMLButtonElement} */ (document.getElementById('colorLabelDescriptionSave'));
    const error = /** @type {HTMLElement} */ (document.getElementById('colorLabelDescriptionError'));
    button.disabled = true;
    error.style.display = 'none';
    try {
      const data = await request('/api/color-label-descriptions/' + color, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ description: input.value }),
      });
      if (data.description) descriptions[color] = data.description;
      else delete descriptions[color];
      refreshControls();
      closeEditor();
      if (typeof window.showToast === 'function') {
        window.showToast(
          data.description
            ? colorName(color) + ' label description saved'
            : colorName(color) + ' label description removed',
          'success'
        );
      }
    } catch (requestError) {
      error.textContent = (requestError instanceof Error ? requestError.message : '') || 'Could not save the description';
      error.style.display = 'block';
    } finally {
      button.disabled = false;
    }
  }

  window.VireoColorLabels = {
    ensureLoaded: ensureLoaded,
    isValid: isValid,
    description: description,
    title: title,
    refreshControls: refreshControls,
    openEditor: openEditor,
    closeEditor: closeEditor,
    saveEditor: saveEditor,
  };

  document.addEventListener('contextmenu', function (event) {
    const control = event.target instanceof Element ? event.target.closest('[data-color-label-control][data-color]') : null;
    if (!(control instanceof HTMLElement) || !control.dataset.color || !COLORS.includes(control.dataset.color)) return;
    event.preventDefault();
    event.stopImmediatePropagation();
    openEditor(control.dataset.color);
  }, true);

  document.addEventListener('DOMContentLoaded', function () {
    const input = /** @type {HTMLInputElement} */ (document.getElementById('colorLabelDescriptionInput'));
    if (input) {
      input.addEventListener('input', updateCount);
      input.addEventListener('keydown', function (event) {
        if (event.key === 'Enter') {
          event.preventDefault();
          saveEditor();
        }
      });
    }
    refreshControls();
    ensureLoaded().catch(function () {});
  });
})();
