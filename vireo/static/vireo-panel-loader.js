/* Request ownership and recoverable failures for a read-only panel.
   The owner supplies its data and rendering contracts; row actions stay there. */
(function(/** @type {Window} */ global) {
  'use strict';

  /**
   * @template T, R
   * @param {BrowsePanelLoaderOptions<T, R>} options
   * @returns {BrowsePanelLoader<T>}
   */
  function create(options) {
    const panel = options.panel;
    /** @type {{input: T, isCurrent: () => boolean} | null} */
    let failed = null;

    /** @param {T} input */
    async function load(input) {
      if (!panel || !options.accepts(input) || !options.isActive(input)) return;
      const snapshot = options.snapshot(input);
      const request = options.lane.begin(options.key(snapshot));
      if (!request) return;
      failed = null;
      options.reset();
      // Messages are fixed, first-party configuration; insert them as text.
      panel.textContent = '';
      const loading = document.createElement('div');
      loading.className = 'selection-empty';
      loading.setAttribute('role', 'status');
      loading.textContent = options.loadingMessage;
      panel.appendChild(loading);
      try {
        const data = await options.request(snapshot);
        if (!request.isCurrent() || !options.isActive(snapshot)) return;
        options.render(data, snapshot);
      } catch {
        if (!request.fail() || !options.isActive(snapshot)) return;
        failed = {input: snapshot, isCurrent: options.lane.observe()};
        panel.textContent = '';
        const message = document.createElement('div');
        message.className = 'selection-empty';
        message.setAttribute('role', 'status');
        message.textContent = options.errorMessage + ' ';
        const button = document.createElement('button');
        button.type = 'button';
        button.className = 'panel-retry';
        button.setAttribute('data-panel-retry', '');
        button.setAttribute('aria-label', options.retryLabel);
        button.textContent = 'Retry';
        message.appendChild(button);
        panel.appendChild(message);
      }
    }

    async function retry() {
      const failure = failed;
      if (!failure || !failure.isCurrent() || !options.isActive(failure.input)) return;
      // load() synchronously retires the retry and paints loading before its
      // first await. A second click cannot launch another request.
      const hadFocus = !!panel && panel.contains(document.activeElement);
      const pending = load(failure.input);
      if (panel && hadFocus) {
        panel.tabIndex = -1;
        panel.focus({preventScroll: true});
      }
      await pending;
    }

    panel?.addEventListener('click', function(event) {
      if (!(event.target instanceof Element)) return;
      const button = event.target.closest('button[data-panel-retry]');
      if (!(button instanceof HTMLButtonElement) || !panel.contains(button) || button.disabled) return;
      void retry();
    });
    return {load: load, retry: retry};
  }

  global.VireoPanelLoader = {create: create};
})(window);
