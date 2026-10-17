(function (global) {
  'use strict';

  /** @template {string | boolean | string[]} T @param {T | (() => T) | undefined} option @param {T} fallback @returns {T} */
  function valueOf(option, fallback) {
    if (typeof option === 'function') return option();
    return option === undefined ? fallback : option;
  }

  /** @param {unknown} value */
  function formatPhotoCount(value) {
    var count = Number(value || 0);
    return count.toLocaleString() + ' photo' + (count === 1 ? '' : 's');
  }

  class FolderBrowser {
    /** @param {FolderBrowserOptions} options */
    constructor(options) {
      this.options = options;
      this.overlay = document.getElementById(this.options.overlayId);
      if (!this.overlay) {
        throw new Error('Folder browser overlay not found: ' + this.options.overlayId);
      }
      this.title = /** @type {HTMLElement} */ (this.overlay.querySelector('#folderBrowserTitle'));
      this.pathLabel = /** @type {HTMLElement} */ (this.overlay.querySelector('#folderBrowserPath'));
      this.list = /** @type {HTMLElement} */ (this.overlay.querySelector('#folderBrowserList'));
      this.selectionLabel = /** @type {HTMLElement | null} */ (this.overlay.querySelector('#folderBrowserSelection'));
      this.selectButton = /** @type {HTMLButtonElement} */ (this.overlay.querySelector('#folderBrowserSelectBtn'));
      this.modeName = this.options.defaultMode || Object.keys(this.options.modes)[0] || '';
      this.path = '';
      this.homePath = '';
      this.seq = 0;
      /** @type {AbortController | null} */
      this.countsAbort = null;
      /** @type {Record<string, FolderBrowserCount>} */
      this.countCache = {};
      /** @type {((event: KeyboardEvent) => void) | null} */
      this.escHandler = null;
      /** @type {string[]} */
      this.selectedPaths = [];
      this.selectAnchorPath = '';
      this._bindChrome();
    }

    get sequence() { return this.seq; }

    /** @returns {FolderBrowserMode} */
    _mode() {
      return this.options.modes[this.modeName] || {};
    }

    _bindChrome() {
      var self = this;
      this.overlay.querySelectorAll('[data-folder-browser-action="close"]').forEach(
        function (button) { button.addEventListener('click', function () { self.close(); }); }
      );
      this.overlay.querySelector('[data-folder-browser-path="home"]')?.addEventListener(
        'click', function () { self.browse(null); }
      );
      this.overlay.querySelector('[data-folder-browser-path="pictures"]')?.addEventListener(
        'click', function () { self.browse('__pictures__'); }
      );
      this.overlay.querySelector('[data-folder-browser-path="volumes"]')?.addEventListener(
        'click', function () { self.browse('__volumes__'); }
      );
      this.selectButton.addEventListener('click', function () { self.confirm(); });
    }

    /** @param {string} modeName @param {{skipInitialBrowse?: boolean}} [opts] */
    open(modeName, opts) {
      if (!this.options.modes || !this.options.modes[modeName]) {
        throw new Error('Unknown folder browser mode: ' + modeName);
      }
      this.modeName = modeName;
      var mode = this._mode();
      this.title.textContent = valueOf(mode.title, 'Select Folder');
      this.selectedPaths = [];
      this.selectAnchorPath = '';
      this._updateSelection();
      this.overlay.classList.add('open');
      var self = this;
      this.overlay.onclick = function (event) {
        if (event.target === self.overlay) self.close();
      };
      if (this.escHandler) document.removeEventListener('keydown', this.escHandler);
      this.escHandler = function (event) {
        if (event.key === 'Escape') self.close();
      };
      document.addEventListener('keydown', this.escHandler);
      document.body.style.overflow = 'hidden';
      var firstAction = this.overlay.querySelector('button');
      if (firstAction) firstAction.focus();
      var startPath = valueOf(mode.startPath, '');
      if (!(opts && opts.skipInitialBrowse)) this.browse(startPath || null);
    }

    close() {
      this.overlay.classList.remove('open');
      if (this.countsAbort) {
        this.countsAbort.abort();
        this.countsAbort = null;
      }
      if (this.escHandler) {
        document.removeEventListener('keydown', this.escHandler);
        this.escHandler = null;
      }
      document.body.style.overflow = '';
    }

    /** @param {string} message */
    _showBrowseError(message) {
      this.pathLabel.textContent = message || 'Could not open folder.';
      this.list.innerHTML = '<div class="folder-browser-empty">Could not open folder.</div>';
      if (typeof this.options.onError === 'function') this.options.onError(message);
    }

    /** @param {string | null} path */
    async browse(path) {
      var seq = ++this.seq;
      if (this.countsAbort) {
        this.countsAbort.abort();
        this.countsAbort = null;
      }
      this.path = '';
      this.selectedPaths = [];
      this.selectAnchorPath = '';
      this._updateSelection();
      this.selectButton.disabled = true;
      this.pathLabel.textContent = 'Loading…';
      this.list.innerHTML = '<div class="folder-browser-empty">Loading…</div>';

      var url = '/api/browse';
      if (path === '__pictures__') {
        if (!this.homePath) {
          try {
            var homeResp = await fetch('/api/browse');
            if (seq !== this.seq || !homeResp.ok) return;
            /** @type {FolderBrowserListing} */
            var home = await homeResp.json();
            this.homePath = home.path || '';
          } catch (error) {
            if (seq === this.seq) this._showBrowseError(String(error instanceof Error ? error.message : error));
            return;
          }
        }
        url = '/api/browse?path=' + encodeURIComponent(this.homePath + '/Pictures');
      } else if (path === '__volumes__') {
        if (navigator.userAgent.indexOf('Mac') < 0) {
          try {
            var volumesResp = await fetch('/api/volumes');
            if (seq !== this.seq || !volumesResp.ok) return;
            /** @type {FolderBrowserDirectory[]} */
            var volumes = await volumesResp.json();
            this.path = '';
            this.render({path: '', dirs: volumes || []}, {syntheticRoot: true});
          } catch (error) {
            if (seq === this.seq) this._showBrowseError(String(error instanceof Error ? error.message : error));
          }
          return;
        }
        url = '/api/browse?path=' + encodeURIComponent('/Volumes');
      } else if (path) {
        url = '/api/browse?path=' + encodeURIComponent(path);
      }

      try {
        var response = await fetch(url);
        if (seq !== this.seq) return;
        if (!response.ok) {
          var failure = await response.json().catch(function () { return {}; });
          this._showBrowseError(failure.error || 'Could not open folder.');
          return;
        }
        /** @type {FolderBrowserListing} */
        var data = await response.json();
        if (seq !== this.seq) return;
        this.path = data.path || '';
        this.selectedPaths = [];
        this.selectAnchorPath = '';
        if (!path) this.homePath = this.path;
        this.render(data);
      } catch (error) {
        if (seq === this.seq) this._showBrowseError(String(error instanceof Error ? error.message : error));
      }
    }

    /** @param {FolderBrowserListing} data @param {{syntheticRoot?: boolean}} [opts] */
    render(data, opts) {
      var mode = this._mode();
      var multiple = !!mode.multiple;
      var showCounts = !!mode.showCounts;
      this.pathLabel.textContent = data.path || 'Volumes';
      var dirs = data.dirs || [];
      var html = '';
      var syntheticRoot = opts && opts.syntheticRoot;
      var parent = '';
      if (data.path && data.path.indexOf('\\') !== -1) {
        parent = data.path.replace(/[\\/]?[^\\/]+[\\/]?$/, '') || data.path;
        if (/^[A-Za-z]:$/.test(parent)) parent += '\\';
      } else if (data.path) {
        parent = data.path.substring(0, data.path.lastIndexOf('/')) || '/';
      }
      if (data.path && data.path !== '/' && parent && parent !== data.path) {
        html += '<div class="folder-browser-item" data-browse-path="' + escapeHtml(parent) + '" data-parent-link="1">' +
          '<span class="folder-browser-item-name">&#128193; ..</span></div>';
      }
      dirs.forEach(function (dir) {
        html += '<div class="folder-browser-item" data-browse-path="' + escapeHtml(dir.path) + '"' +
          (multiple ? ' data-folder-path="' + escapeHtml(dir.path) + '"' : '') + '>' +
          '<span class="folder-browser-item-name">&#128193; ' + escapeHtml(dir.name || dir.path) + '</span>' +
          (showCounts ? '<span class="folder-browser-count" data-count-path="' + escapeHtml(dir.path) + '"></span>' : '') +
          '</div>';
      });
      if (!html) {
        html = '<div class="folder-browser-empty">' +
          (syntheticRoot ? 'No volumes found.' : 'No subfolders.') + '</div>';
      }
      this.list.innerHTML = html;

      var self = this;
      this.list.onclick = function (event) {
        var item = event.target instanceof Element ? event.target.closest('.folder-browser-item[data-browse-path]') : null;
        if (!item) return;
        if (item.getAttribute('data-parent-link') === '1' || !multiple) {
          self.browse(item.getAttribute('data-browse-path'));
          return;
        }
        self._selectItem(item, event);
      };
      this.list.ondblclick = multiple ? function (event) {
        var item = event.target instanceof Element ? event.target.closest('.folder-browser-item[data-browse-path]') : null;
        if (item && item.getAttribute('data-parent-link') !== '1') {
          self.browse(item.getAttribute('data-browse-path'));
        }
      } : null;
      this._updateSelection();
      if (showCounts) this._refreshCounts(dirs, this.seq);
    }

    /** @param {Element} item @param {MouseEvent} [event] */
    _selectItem(item, event) {
      var path = item.getAttribute('data-folder-path') || item.getAttribute('data-browse-path');
      if (!path) return;
      var items = Array.from(this.list.querySelectorAll('.folder-browser-item[data-folder-path]'));
      var shift = event && event.shiftKey;
      var toggle = event && (event.metaKey || event.ctrlKey);
      if (shift && this.selectAnchorPath) {
        var anchorIdx = items.findIndex(candidate => candidate.getAttribute('data-folder-path') === this.selectAnchorPath);
        var clickIdx = items.findIndex(function (candidate) {
          return candidate.getAttribute('data-folder-path') === path;
        });
        if (clickIdx === -1) return;
        if (anchorIdx === -1) anchorIdx = clickIdx;
        var lo = Math.min(anchorIdx, clickIdx);
        var hi = Math.max(anchorIdx, clickIdx);
        this.selectedPaths = items.slice(lo, hi + 1).map(function (candidate) {
          return candidate.getAttribute('data-folder-path') || '';
        });
      } else if (toggle) {
        var idx = this.selectedPaths.indexOf(path);
        if (idx === -1) this.selectedPaths.push(path);
        else this.selectedPaths.splice(idx, 1);
        this.selectAnchorPath = path;
      } else {
        this.selectedPaths = [path];
        this.selectAnchorPath = path;
      }
      this._updateSelection();
    }

    _updateSelection() {
      var mode = this._mode();
      /** @type {Record<string, boolean>} */
      var selected = {};
      this.selectedPaths.forEach(function (path) { selected[path] = true; });
      this.list.querySelectorAll('.folder-browser-item[data-folder-path]').forEach(function (item) {
        item.classList.toggle('selected', !!selected[item.getAttribute('data-folder-path') || '']);
      });
      if (this.selectionLabel) {
        if (mode.multiple && this.selectedPaths.length > 1) {
          this.selectionLabel.textContent = this.selectedPaths.length + ' folders selected';
        } else if (mode.multiple && this.selectedPaths.length === 1) {
          this.selectionLabel.textContent = this.selectedPaths[0] || '';
        } else {
          this.selectionLabel.textContent = '';
        }
      }
      if (mode.multiple) {
        this.selectButton.textContent = this.selectedPaths.length > 1
          ? 'Add ' + this.selectedPaths.length + ' Folders'
          : 'Add This Folder';
        this.selectButton.disabled = !this.path && this.selectedPaths.length === 0;
      } else {
        this.selectButton.textContent = 'Select This Folder';
        this.selectButton.disabled = !this.path;
      }
    }

    /** @param {string} path @param {string | string[]} fileTypes */
    _countKey(path, fileTypes) {
      var normalized = Array.isArray(fileTypes)
        ? fileTypes.slice().sort().join(',')
        : String(fileTypes || 'both');
      return path + '\n' + normalized;
    }

    // ``entry`` is ``{count, incomplete, skipped}``. ``incomplete`` means the
    // server started scanning this folder but ran out of its per-folder time
    // budget, so the number is a lower bound. ``skipped`` means the request
    // budget ran out before this folder got its turn, so the server never
    // looked at it — the folder may be small and merely ordered after a slow
    // sibling, so don't claim it holds too many files.
    /** @param {string} path @param {FolderBrowserCount} entry */
    _applyCount(path, entry) {
      if (!entry.count && !entry.incomplete && !entry.skipped) return;
      var text = '';
      if (entry.skipped) {
        text = 'Not counted';
      } else if (entry.incomplete) {
        text = entry.count ? Number(entry.count).toLocaleString() + '+ photos' : 'Not counted';
      } else {
        text = formatPhotoCount(entry.count);
      }
      this.list.querySelectorAll('.folder-browser-count').forEach(function (badge) {
        if (badge.getAttribute('data-count-path') === path) {
          badge.textContent = text;
          if (entry.skipped) {
            badge.setAttribute('title', 'Not reached before the counting deadline.');
          } else if (entry.incomplete) {
            badge.setAttribute('title', 'Stopped counting: this folder holds too many files to count quickly.');
          }
        }
      });
    }

    /** @param {FolderBrowserDirectory[]} dirs @param {number} seq */
    async _refreshCounts(dirs, seq) {
      if (!dirs.length || seq !== this.seq) return;
      var fileTypes = valueOf(this._mode().fileTypes, 'both');
      /** @type {string[]} */
      var uncached = [];
      dirs.forEach(dir => {
        var key = this._countKey(dir.path, fileTypes);
        if (Object.prototype.hasOwnProperty.call(this.countCache, key)) {
          var cached = this.countCache[key];
          if (cached) this._applyCount(dir.path, cached);
        } else {
          uncached.push(dir.path);
        }
      });
      if (!uncached.length) return;

      this.countsAbort = new AbortController();
      try {
        var response = await fetch('/api/browse/photo-counts', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({paths: uncached, file_types: fileTypes}),
          signal: this.countsAbort.signal,
        });
        if (!response.ok || seq !== this.seq) return;
        /** @type {{counts?: Record<string, number>, incomplete?: string[]}} */
        var data = await response.json();
        if (seq !== this.seq) return;
        var counts = data.counts || {};
        var incomplete = data.incomplete || [];
        uncached.forEach(path => {
          if (!Object.prototype.hasOwnProperty.call(counts, path)) {
            // The request deadline can skip this folder entirely. Keep it
            // uncached so reopening the picker retries it, and mark it as
            // ``skipped`` so the badge says it wasn't reached rather than
            // claiming it holds too many files.
            this._applyCount(path, {count: 0, skipped: true});
            return;
          }
          var entry = {count: counts[path] || 0, incomplete: incomplete.indexOf(path) !== -1};
          this.countCache[this._countKey(path, fileTypes)] = entry;
          this._applyCount(path, entry);
        });
      } catch (error) {
        if (!(error instanceof Error) || error.name !== 'AbortError') { /* Counts are helpful but non-blocking. */ }
      } finally {
        if (seq === this.seq) this.countsAbort = null;
      }
    }

    confirm() {
      var mode = this._mode();
      var selection;
      if (mode.multiple) {
        selection = this.selectedPaths.length
          ? this.selectedPaths.slice()
          : (this.path ? [this.path] : []);
        if (!selection.length) return;
      } else {
        if (!this.path) return;
        selection = this.path;
      }
      if (mode.multiple && Array.isArray(selection)) {
        if (mode.onSelect) mode.onSelect(selection);
      } else if (!mode.multiple && typeof selection === 'string') {
        if (mode.onSelect) mode.onSelect(selection);
      }
      this.close();
    }

  }

  global.VireoFolderBrowser = FolderBrowser;
})(window);
