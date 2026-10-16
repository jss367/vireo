// Legacy ?collection_id= / ?folder_id= deep links: resolving them into filter
// rules, the fail-closed scope error, and the scope banner.
// Classic page script; load boot.js after all definitions.

function clearLegacyScopeError() {
  if (!legacyScopeError) return;
  legacyScopeError = null;
  // Keep the tuning controls (sliders, save-defaults, reset, Recompute)
  // disabled until the replacement expression has loaded. Re-enabling them
  // here would briefly let a slider nudge preview — or a fast Recompute
  // click — operate on the empty fail-closed snapshot, which the preview
  // endpoint would widen back to the whole workspace.
  legacyScopeRecoveryPending = true;
  renderMissScopeBanner();
}

function normalizedRuleRoot(raw) {
  if (Array.isArray(raw)) return { mode: 'all', rules: raw };
  if (raw && Array.isArray(raw.rules)) return raw;
  return { mode: 'all', rules: [] };
}

function appendRuleRoot(target, source) {
  var root = normalizedRuleRoot(source);
  if (!root.rules.length) return;
  if ((root.mode || 'all') === 'all') {
    root.rules.forEach(function(rule) { target.rules.push(rule); });
  } else {
    target.rules.push(root);
  }
}

function noteLegacyScopeError(message) {
  // Append when multiple deep-link params fail (e.g. a link with both a
  // missing collection and a missing folder) so the banner surfaces both.
  legacyScopeError = legacyScopeError
    ? legacyScopeError + ' ' + message
    : message;
  if (typeof showToast === 'function') showToast(message, 'error');
  renderMissScopeBanner();
  // Freeze the whole tuning group — not just Recompute. Preview and reset
  // still call /api/misses/preview with the current (empty) rules, which
  // widens results to every miss outside the unresolved scope; card and
  // selection handlers on that grid could then mutate photos the user
  // never scoped. Sliders re-enable once a replacement scope loads.
  setMissTuningDisabled(true);
}

function renderMissScopeBanner() {
  var b = document.getElementById('scopeBanner');
  if (!b) return;
  var since = getSinceParam();
  var message = legacyScopeError || (since
    ? 'Showing only the current pipeline run.'
    : '');
  b.textContent = message ? message + ' ' : '';
  if (message) {
    // Build the link as a node so collection names and API errors remain text.
    var reset = document.createElement('a');
    reset.href = '/misses';
    reset.textContent = 'Show all misses';
    b.appendChild(reset);
  }
  b.hidden = !message;
}

function parseLegacyScopeParam(params, key) {
  if (!params.has(key)) return { present: false, valid: true, value: null };
  var raw = params.get(key);
  var normalized = String(raw == null ? '' : raw).trim();
  if (!/^[+-]?\d+$/.test(normalized)) {
    return { present: true, valid: false, value: null, raw: raw };
  }
  var value = Number(normalized);
  if (!Number.isSafeInteger(value)) {
    return { present: true, valid: false, value: null, raw: raw };
  }
  return { present: true, valid: true, value: value, raw: raw };
}

async function applyLegacyMissScopes() {
  var current = new URL(window.location.href).searchParams;
  var collectionParam = parseLegacyScopeParam(current, 'collection_id');
  var folderParam = parseLegacyScopeParam(current, 'folder_id');
  if (!collectionParam.present && !folderParam.present) return;
  var collectionId = collectionParam.present && collectionParam.valid
    ? collectionParam.value : NaN;
  var folderId = folderParam.present && folderParam.valid
    ? folderParam.value : NaN;

  var legacyFilterKeys = [
    'rating_min', 'flag', 'color_label', 'date_from', 'date_to', 'keyword',
    'filters',
  ];
  var keepCompiledUrlRules = legacyFilterKeys.some(function(key) {
    return current.has(key);
  });
  var combined = { mode: 'all', rules: [] };
  if (keepCompiledUrlRules) appendRuleRoot(combined, VireoFilter.getUserRules());
  // A plain folder/collection deep link overrides restored page state. Keep a
  // visual clause only when it came from this URL's handoff payload; a visual
  // collection below supplies its own clause explicitly.
  var visual = keepCompiledUrlRules && VireoFilter.getVisual
    ? VireoFilter.getVisual()
    : null;

  if (!collectionParam.valid) {
    noteLegacyScopeError(
      'Invalid ?collection_id=' + collectionParam.raw + ' deep link. ' +
      'Showing no misses so bulk actions do not touch the wrong collection.'
    );
  }
  if (!folderParam.valid) {
    noteLegacyScopeError(
      'Invalid ?folder_id=' + folderParam.raw + ' deep link. ' +
      'Showing no misses so bulk actions do not touch the wrong folder.'
    );
  }

  // Track fetch failures separately from empty responses: a failed
  // /api/collections must fail closed instead of being treated as an
  // empty list (which would silently drop the collection rule and load
  // every miss in the workspace).
  var collectionsFetchFailed = false;
  var foldersFetchFailed = false;
  var requests = [];
  requests.push(
    isNaN(collectionId)
      ? Promise.resolve([])
      : fetch('/api/collections').then(function(r) {
          if (!r.ok) { collectionsFetchFailed = true; return []; }
          return r.json();
        }).catch(function() { collectionsFetchFailed = true; return []; })
  );
  requests.push(
    isNaN(folderId)
      ? Promise.resolve([])
      : fetch('/api/folders').then(function(r) {
          if (!r.ok) { foldersFetchFailed = true; return []; }
          return r.json();
        }).catch(function() { foldersFetchFailed = true; return []; })
  );
  var results = await Promise.all(requests);

  if (!isNaN(collectionId)) {
    if (collectionsFetchFailed) {
      noteLegacyScopeError(
        'Could not load collections to resolve the ?collection_id=' +
        collectionId + ' deep link. Showing no misses to avoid ' +
        'bulk-rejecting the whole workspace.'
      );
    } else {
      var collection = (results[0] || []).find(function(c) { return c.id === collectionId; });
      if (!collection) {
        noteLegacyScopeError(
          'Collection #' + collectionId + ' is missing or was deleted. ' +
          'Showing no misses so bulk actions do not touch the whole workspace.'
        );
      } else if (collection.count_error) {
        noteLegacyScopeError(
          'Collection "' + (collection.name || ('#' + collectionId)) +
          '" is degraded (' + collection.count_error + '). ' +
          'Showing no misses so bulk actions do not touch the whole workspace.'
        );
      } else {
        try { appendRuleRoot(combined, JSON.parse(collection.rules || '[]')); } catch (e) {}
        try {
          if (collection.visual_json) visual = JSON.parse(collection.visual_json);
        } catch (e) {}
      }
    }
  }
  if (!isNaN(folderId)) {
    if (foldersFetchFailed) {
      noteLegacyScopeError(
        'Could not load folders to resolve the ?folder_id=' + folderId +
        ' deep link. Showing no misses to avoid bulk-rejecting the whole workspace.'
      );
    } else {
      var folder = (results[1] || []).find(function(f) { return f.id === folderId; });
      if (folder) {
        combined.rules.push({
          field: 'folder', op: 'under', value: folder.path || folder.name || '',
        });
      } else {
        noteLegacyScopeError(
          'Folder #' + folderId + ' is missing or was removed. ' +
          'Showing no misses so bulk actions do not touch the whole workspace.'
        );
      }
    }
  }
  VireoFilter.loadExpression(combined, visual, { reason: 'legacyScopeLoaded' });
}
