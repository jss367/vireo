// Loading the comparison: collections, the models shown, building and
// fetching a page of rows (stale responses are dropped), and reloading.
// Classic page script.

/** @param {string} url @param {RequestInit=} options */
async function jsonFetch(url, options) {
  var resp = await fetch(url, options || {});
  var data = await resp.json().catch(function() { return {}; });
  if (!resp.ok || data.error) {
    var err = /** @type {CompareRequestError} */ (new Error(data.error || ('Request failed: ' + resp.status)));
    // Keep the outcome class on the error, not just its text. A 4xx (or any
    // response carrying an `error` body) is the server *answering* — it
    // refused, and wrote nothing. A 5xx or a thrown fetch is an unknown
    // outcome: the decision routes commit inside BEGIN IMMEDIATE, so a
    // failure after that commit is indistinguishable from one before it at
    // this end. Callers must not report the second as "not applied".
    err.status = resp.status;
    // An `error` body is itself the answer: the server got far enough to
    // explain the refusal in its own words, whatever status it chose to send
    // it under. Classifying on the status alone reported those as unconfirmed,
    // which understates what we know — so both shapes count as answered, and
    // only a status-only failure (5xx, or a fetch that threw) stays unknown.
    err.answered = Boolean(data.error) || (resp.status >= 400 && resp.status < 500);
    throw err;
  }
  return data;
}

async function loadCollections() {
  try {
    var data = await jsonFetch('/api/collections');
    var sel = /** @type {HTMLSelectElement} */ (document.getElementById('collectionPicker'));
    data.forEach(function(/** @type {{id: number, name: string, count_error?: unknown, has_visual?: boolean}} */ c) {
      var opt = document.createElement('option');
      opt.value = String(c.id);
      opt.textContent = c.name;
      // count_error means /api/collections couldn't resolve this collection's
      // rules — comparing against it would just 400 the downstream fetch.
      // Show it disabled so the state is visible instead of offering a
      // source that can't render.
      if (c.count_error) {
        opt.disabled = true;
        opt.textContent = c.name + ' (unavailable — edit rules to fix)';
        opt.title = "This collection's rules could not be resolved. Edit it in Browse to make it usable.";
      } else if (c.has_visual) {
        // /api/predictions/compare consumes the collection via
        // get_collection_photos, which is rules-only — a visual
        // collection would silently widen the comparison to every
        // metadata match. Match the server-side 400 in the picker.
        opt.disabled = true;
        opt.textContent = c.name + ' (visual — open in Browse)';
        opt.title = 'Visual collections can only be used from Browse, where the filter bar resolves the visual-search clause.';
      }
      sel.appendChild(opt);
    });
    var all = data.find(function(/** @type {{id: number, name: string, count_error?: unknown, has_visual?: boolean}} */ c) { return c.name === 'All Photos' && !c.count_error && !c.has_visual; });
    if (all) {
      sel.value = String(all.id);
      loadComparison();
    }
  } catch(e) {
    /** @type {HTMLElement} */ (document.getElementById('compareContent')).innerHTML =
      '<div class="compare-empty">Could not load collections.</div>';
  }
}

function visibleModels() {
  var a = /** @type {HTMLSelectElement} */ (document.getElementById('modelA')).value;
  var b = /** @type {HTMLSelectElement} */ (document.getElementById('modelB')).value;
  if (!a || a === 'all') return compareModels.slice();
  var models = [a];
  if (b && b !== a) models.push(b);
  return models;
}

// Sent to the server as the models to compare. "All models" sends nothing at
// all, so a model that appears while the page is open is included instead of
// being pinned out by a stale list.
function requestedModelParams() {
  var a = /** @type {HTMLSelectElement} */ (document.getElementById('modelA')).value;
  if (!a || a === 'all') return [];
  return visibleModels();
}

function setVisibleModels() {
  reloadRows();
}

/** @param {boolean} active */
function showLoading(active) {
  var note = /** @type {HTMLElement} */ (document.getElementById('loadingNote'));
  if (note) note.hidden = !active;
  var content = /** @type {HTMLElement} */ (document.getElementById('compareContent'));
  if (content) content.classList.toggle('loading', !!active);
}

/** @param {CompareRowsOptions} options */
function buildRowsUrl(options) {
  var collId = /** @type {HTMLSelectElement} */ (document.getElementById('collectionPicker')).value;
  var params = new URLSearchParams();
  params.set('collection_id', collId);
  if (compareToken) params.set('token', compareToken);
  requestedModelParams().forEach(function(model) { params.append('model', model); });
  params.set('min_confidence', String(conflictMinConfidence));
  params.set('filter', activeFilter);
  params.set('sort', activeSort);
  Object.keys(excludeState).forEach(function(id) {
    if (excludeState[id]) params.append('exclude', id);
  });
  var search = /** @type {HTMLInputElement} */ (document.getElementById('compareSearch'));
  var q = search ? search.value.trim() : '';
  if (q) params.set('q', q);
  VireoTextSearch.appendParams(params, VireoTextSearch.readOptions('compareSearch'));
  params.set('page', String(options.page || comparePage));
  params.set('per_page', String(comparePerPage));
  (options.refreshPhotoIds || []).forEach(function(id) {
    params.append('refresh_photo_id', String(id));
  });
  return '/api/predictions/compare?' + params.toString();
}

// One request answers everything on screen: the rows for this page, the
// counts on every chip and tile, and the total the pager reports.
/** @param {CompareRowsOptions=} options @returns {Promise<void>} */
async function fetchRows(options) {
  options = options || {};
  var collId = /** @type {HTMLSelectElement} */ (document.getElementById('collectionPicker')).value;
  if (!collId) return;
  var fullLoadSeq = loadingSeq;
  showLoading(true);
  // A control the user just touched — filter, sort, search, threshold, pager
  // — must not overtake a decision refresh that is still in flight. The
  // server rebuilds the changed photos inside the stored comparison while
  // that refresh runs, and a query that reads the comparison first would take
  // the newer sequence number and then discard the refresh's answer, leaving
  // the counts and the decided row exactly as they were before the decision.
  // Waiting for the queue lets the decision land first, so this query reads
  // the comparison the decision produced. Only user controls wait: the
  // refresh's own queries run inside the queue and would deadlock on it.
  if (options.control) {
    await decisionRefreshQueue;
  }
  // Claimed after the wait, so the query the user asked for most recently is
  // the one that wins rather than the one that stopped waiting last.
  var seq = ++requestSeq;
  var data;
  try {
    data = await jsonFetch(buildRowsUrl(options));
  } catch (e) {
    if (seq !== requestSeq) return;
    showLoading(false);
    /** @type {HTMLElement} */ (document.getElementById('compareContent')).innerHTML =
      '<div class="compare-empty">Error loading comparison: ' + escapeHtml(e instanceof Error ? e.message : e) + '</div>';
    throw e;
  }
  // A newer request — or a collection load — has taken over. Its response is
  // the current one; painting this stale body over it would show rows that
  // do not belong to the comparison on screen.
  if (seq !== requestSeq || fullLoadSeq !== loadingSeq) return;
  showLoading(false);
  var modelSelectionReset = applyRowsResponse(data, options);
  // The queue the page opens on is Needs review. When it is empty there is
  // nothing to review, so show the collection rather than an empty table —
  // but only when the page chose the filter, never when the user did. An
  // empty queue proves nothing when the model selection was just reset: this
  // response was assessed under a model the collection does not carry, and
  // the corrected re-query already carries the decision on the real counts.
  if (
    !modelSelectionReset
    && !options.filterResolved
    && (options.initial || options.afterDecision)
    && activeFilter === 'needs_review'
    && (data.summary || {}).needs_review === 0
  ) {
    activeFilter = 'all';
    comparePage = 1;
    await fetchRows({filterResolved: true, page: 1});
  }
}

// Returns true when the response was assessed under a model the collection
// does not carry, so a corrected re-query is already on its way and the caller
// must not read anything into these counts.
/** @param {CompareResponse} data @param {CompareRowsOptions} options */
function applyRowsResponse(data, options) {
  compareData = data;
  compareRows = data.photos || [];
  compareToken = data.token || compareToken;
  compareTotal = data.total || 0;
  comparePage = data.page || 1;
  comparePerPage = data.per_page || comparePerPage;
  compareModels = data.models || [];
  compareFilterDefs = data.filters || compareFilterDefs;
  compareSortDefs = data.sorts || compareSortDefs;
  compareExcludeDefs = data.excludes || compareExcludeDefs;
  compareExcludeDefs.forEach(function(item) {
    if (!(item.id in excludeState)) excludeState[item.id] = false;
  });
  var modelSelectionReset = populateModelSelects(compareModels, data.visible_models || []);
  populateSortSelect();
  // A row that left the page — decided, filtered out, or simply on another
  // page — must not stay in a selection whose batch buttons still claim it.
  var visible = new Set(compareRows.map(function(photo) { return photo.photo_id; }));
  Array.from(selectedRows).forEach(function(id) {
    if (!visible.has(id)) selectedRows.delete(id);
  });
  renderCompare();
  // The response was assessed with a specific model the new collection
  // does not carry; the model controls just reset to what is actually
  // available. Re-query so the counts, statuses and sort match the columns
  // the user now sees rather than the model that was silently dropped. The
  // re-query inherits this load's intent, so the default-filter decision the
  // caller would have made is made once, on the corrected counts.
  if (modelSelectionReset) {
    reloadRows({
      initial: options.initial,
      afterDecision: options.afterDecision,
      internal: true,
    });
  }
  return modelSelectionReset;
}

// A full load of the current collection: everything else on this page is a
// re-query of the same comparison.
async function loadComparison() {
  var collId = /** @type {HTMLSelectElement} */ (document.getElementById('collectionPicker')).value;
  var content = /** @type {HTMLElement} */ (document.getElementById('compareContent'));
  selectedRows.clear();
  compareToken = null;
  comparePage = 1;
  loadingSeq++;
  updateBatchControls();

  if (!collId) {
    compareData = null;
    compareRows = [];
    ['summaryGrid', 'sortRow', 'excludeRow', 'filterRow', 'pagerRow', 'batchRow']
      .forEach(function(id) { /** @type {HTMLElement} */ (document.getElementById(id)).style.display = 'none'; });
    content.innerHTML = '<div class="compare-empty">Select a collection to compare keywords and predictions.</div>';
    return;
  }

  content.innerHTML = '<div class="compare-empty">Loading keyword comparison...</div>';
  try {
    await fetchRows({initial: true, page: 1});
  } catch (_e) {
    // fetchRows already rendered the failure.
  }
}

// Re-query the comparison with the current controls, back at the first page.
// A caller that is re-querying on behalf of a load still owed a default-filter
// decision passes that intent through so the decision survives the detour.
// Only those three flags are carried: this also runs as a plain change
// handler, and handlers are called with arguments of their own.
/** @param {CompareRowsOptions=} carry */
function reloadRows(carry) {
  comparePage = 1;
  selectedRows.clear();
  /** @type {CompareRowsOptions} */
  var options = {page: 1};
  if (carry && carry.initial) options.initial = true;
  if (carry && carry.afterDecision) options.afterDecision = true;
  // A change handler fired by a control the user touched has to queue behind
  // any decision refresh still running. The one caller that re-queries from
  // inside the page's own machinery says so, because it can already be
  // running inside that queue.
  if (!(carry && carry.internal)) options.control = true;
  return fetchRows(options).catch(function() {});
}
