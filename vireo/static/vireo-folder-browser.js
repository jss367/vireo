(function (global) {
  'use strict';

  function valueOf(option, fallback) {
    if (typeof option === 'function') return option();
    return option === undefined ? fallback : option;
  }

  function escapeHtml(value) {
    return String(value).replace(/[&<>"']/g, function (ch) {
      return {
        '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
      }[ch];
    });
  }

  function formatPhotoCount(value) {
    var count = Number(value || 0);
    return count.toLocaleString() + ' photo' + (count === 1 ? '' : 's');
  }

  function FolderBrowser(options) {
    this.options = options || {};
    this.overlay = document.getElementById(this.options.overlayId);
    if (!this.overlay) {
      throw new Error('Folder browser overlay not found: ' + this.options.overlayId);
    }
    this.title = this.overlay.querySelector('#folderBrowserTitle');
    this.pathLabel = this.overlay.querySelector('#folderBrowserPath');
    this.list = this.overlay.querySelector('#folderBrowserList');
    this.selectionLabel = this.overlay.querySelector('#folderBrowserSelection');
    this.selectButton = this.overlay.querySelector('#folderBrowserSelectBtn');
    this.modeName = this.options.defaultMode || Object.keys(this.options.modes || {})[0];
    this.path = '';
    this.homePath = '';
    this.seq = 0;
    this.countsAbort = null;
    this.countCache = {};
    this.escHandler = null;
    this.selectedPaths = [];
    this.selectAnchorPath = '';
    this._bindChrome();
  }

  Object.defineProperty(FolderBrowser.prototype, 'sequence', {
    get: function () { return this.seq; },
  });

  FolderBrowser.prototype._mode = function () {
    return (this.options.modes || {})[this.modeName] || {};
  };

  FolderBrowser.prototype._bindChrome = function () {
    var self = this;
    this.overlay.querySelectorAll('[data-folder-browser-action="close"]').forEach(
      function (button) { button.addEventListener('click', function () { self.close(); }); }
    );
    this.overlay.querySelector('[data-folder-browser-path="home"]').addEventListener(
      'click', function () { self.browse(null); }
    );
    this.overlay.querySelector('[data-folder-browser-path="pictures"]').addEventListener(
      'click', function () { self.browse('__pictures__'); }
    );
    this.overlay.querySelector('[data-folder-browser-path="volumes"]').addEventListener(
      'click', function () { self.browse('__volumes__'); }
    );
    this.selectButton.addEventListener('click', function () { self.confirm(); });
  };

  FolderBrowser.prototype.open = function (modeName, opts) {
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
  };

  FolderBrowser.prototype.close = function () {
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
  };

  FolderBrowser.prototype._showBrowseError = function (message) {
    this.pathLabel.textContent = message || 'Could not open folder.';
    this.list.innerHTML = '<div class="folder-browser-empty">Could not open folder.</div>';
    if (typeof this.options.onError === 'function') this.options.onError(message);
  };

  FolderBrowser.prototype.browse = async function (path) {
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
          var home = await homeResp.json();
          this.homePath = home.path || '';
        } catch (error) {
          if (seq === this.seq) this._showBrowseError(String(error.message || error));
          return;
        }
      }
      url = '/api/browse?path=' + encodeURIComponent(this.homePath + '/Pictures');
    } else if (path === '__volumes__') {
      if (navigator.userAgent.indexOf('Mac') < 0) {
        try {
          var volumesResp = await fetch('/api/volumes');
          if (seq !== this.seq || !volumesResp.ok) return;
          var volumes = await volumesResp.json();
          this.path = '';
          this.render({path: '', dirs: volumes || []}, {syntheticRoot: true});
        } catch (error) {
          if (seq === this.seq) this._showBrowseError(String(error.message || error));
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
      var data = await response.json();
      if (seq !== this.seq) return;
      this.path = data.path || '';
      this.selectedPaths = [];
      this.selectAnchorPath = '';
      if (!path) this.homePath = this.path;
      this.render(data);
    } catch (error) {
      if (seq === this.seq) this._showBrowseError(String(error.message || error));
    }
  };

  FolderBrowser.prototype.render = function (data, opts) {
    var mode = this._mode();
    var multiple = !!mode.multiple;
    var showCounts = !!mode.showCounts;
    this.pathLabel.textContent = data.path || 'Volumes';
    var dirs = data.dirs || [];
    var html = '';
    var syntheticRoot = opts && opts.syntheticRoot;
    var parent = '';
    if (data.path && data.path.indexOf('\\') !== -1) {
      parent = data.path.replace(/[\\\/]?[^\\\/]+[\\\/]?$/, '') || data.path;
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
      var item = event.target.closest('.folder-browser-item[data-browse-path]');
      if (!item) return;
      if (item.getAttribute('data-parent-link') === '1' || !multiple) {
        self.browse(item.getAttribute('data-browse-path'));
        return;
      }
      self._selectItem(item, event);
    };
    this.list.ondblclick = multiple ? function (event) {
      var item = event.target.closest('.folder-browser-item[data-browse-path]');
      if (item && item.getAttribute('data-parent-link') !== '1') {
        self.browse(item.getAttribute('data-browse-path'));
      }
    } : null;
    this._updateSelection();
    if (showCounts) this._refreshCounts(dirs, this.seq);
  };

  FolderBrowser.prototype._selectItem = function (item, event) {
    var path = item.getAttribute('data-folder-path') || item.getAttribute('data-browse-path');
    if (!path) return;
    var items = Array.from(this.list.querySelectorAll('.folder-browser-item[data-folder-path]'));
    var shift = event && event.shiftKey;
    var toggle = event && (event.metaKey || event.ctrlKey);
    if (shift && this.selectAnchorPath) {
      var anchorIdx = items.findIndex(function (candidate) {
        return candidate.getAttribute('data-folder-path') === this.selectAnchorPath;
      }, this);
      var clickIdx = items.findIndex(function (candidate) {
        return candidate.getAttribute('data-folder-path') === path;
      });
      if (clickIdx === -1) return;
      if (anchorIdx === -1) anchorIdx = clickIdx;
      var lo = Math.min(anchorIdx, clickIdx);
      var hi = Math.max(anchorIdx, clickIdx);
      this.selectedPaths = items.slice(lo, hi + 1).map(function (candidate) {
        return candidate.getAttribute('data-folder-path');
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
  };

  FolderBrowser.prototype._updateSelection = function () {
    var mode = this._mode();
    var selected = {};
    this.selectedPaths.forEach(function (path) { selected[path] = true; });
    this.list.querySelectorAll('.folder-browser-item[data-folder-path]').forEach(function (item) {
      item.classList.toggle('selected', !!selected[item.getAttribute('data-folder-path')]);
    });
    if (this.selectionLabel) {
      if (mode.multiple && this.selectedPaths.length > 1) {
        this.selectionLabel.textContent = this.selectedPaths.length + ' folders selected';
      } else if (mode.multiple && this.selectedPaths.length === 1) {
        this.selectionLabel.textContent = this.selectedPaths[0];
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
  };

  FolderBrowser.prototype._countKey = function (path, fileTypes) {
    var normalized = Array.isArray(fileTypes)
      ? fileTypes.slice().sort().join(',')
      : String(fileTypes || 'both');
    return path + '\n' + normalized;
  };

  // ``entry`` is ``{count, incomplete}``. An incomplete count ran out of the
  // server's time budget, so the number is only what was found before it
  // stopped: show it as a lower bound and say why.
  FolderBrowser.prototype._applyCount = function (path, entry) {
    if (!entry.count && !entry.incomplete) return;
    var text = entry.incomplete
      ? (entry.count ? Number(entry.count).toLocaleString() + '+ photos' : 'Not counted')
      : formatPhotoCount(entry.count);
    this.list.querySelectorAll('.folder-browser-count').forEach(function (badge) {
      if (badge.getAttribute('data-count-path') === path) {
        badge.textContent = text;
        if (entry.incomplete) {
          badge.title = 'Stopped counting: this folder holds too many files to count quickly.';
        }
      }
    });
  };

  FolderBrowser.prototype._refreshCounts = async function (dirs, seq) {
    if (!dirs.length || seq !== this.seq) return;
    var fileTypes = valueOf(this._mode().fileTypes, 'both');
    var uncached = [];
    dirs.forEach(function (dir) {
      var key = this._countKey(dir.path, fileTypes);
      if (Object.prototype.hasOwnProperty.call(this.countCache, key)) {
        this._applyCount(dir.path, this.countCache[key]);
      } else {
        uncached.push(dir.path);
      }
    }, this);
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
      var data = await response.json();
      if (seq !== this.seq) return;
      var counts = data.counts || {};
      var incomplete = data.incomplete || [];
      Object.keys(counts).forEach(function (path) {
        var entry = {count: counts[path], incomplete: incomplete.indexOf(path) !== -1};
        this.countCache[this._countKey(path, fileTypes)] = entry;
        this._applyCount(path, entry);
      }, this);
    } catch (error) {
      if (error.name !== 'AbortError') { /* Counts are helpful but non-blocking. */ }
    } finally {
      if (seq === this.seq) this.countsAbort = null;
    }
  };

  FolderBrowser.prototype.confirm = function () {
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
    if (typeof mode.onSelect === 'function') mode.onSelect(selection);
    this.close();
  };

  global.VireoFolderBrowser = FolderBrowser;
})(window);
