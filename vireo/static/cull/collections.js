// The collection picker that scopes the analysis, and the slider defaults
// from the pipeline config.
// Classic page script.

async function loadCollections() {
  try {
    var collections = /** @type {{id: number, name: string, count_error?: unknown, has_visual?: boolean}[]} */ (
      await safeFetch('/api/collections', {}, { toast: false }));
    var sel = /** @type {HTMLSelectElement} */ (document.getElementById('cullCollection'));
    collections.forEach(function(c) {
      var opt = document.createElement('option');
      opt.value = String(c.id);
      opt.textContent = c.name;
      // count_error means /api/collections couldn't resolve this collection's
      // rules — scoping the cull to it would just 400 the downstream /photos
      // fetch. Show it disabled so the state is visible instead of offering
      // a source that can't run.
      if (c.count_error) {
        opt.disabled = true;
        opt.textContent = c.name + ' (unavailable — edit rules to fix)';
        opt.title = "This collection's rules could not be resolved. Edit it in Browse to make it usable.";
      } else if (c.has_visual) {
        // Cull consumes the collection via get_collection_photos, which
        // is rules-only — picking a visual collection here would silently
        // scope the analysis to every metadata match. The regroup-live
        // endpoint 400s visual collections; disable the option so the
        // user sees why up front.
        opt.disabled = true;
        opt.textContent = c.name + ' (visual — open in Browse)';
        opt.title = 'Visual collections can only be used from Browse, where the filter bar resolves the visual-search clause.';
      }
      sel.appendChild(opt);
    });
    sel.addEventListener('change', onCullCollectionChange);
  } catch(e) {}
}

async function onCullCollectionChange() {
  var val = /** @type {HTMLSelectElement} */ (document.getElementById('cullCollection')).value;
  var parsed = val ? parseInt(val, 10) : NaN;
  selectedCollectionId = isFinite(parsed) ? parsed : null;
  // Re-run analysis whenever the scope changes so KEEP/REVIEW/REJECT
  // labels are computed against the selected collection's photos only.
  // Filtering the existing workspace-wide results client-side would
  // leave the labels influenced by photos outside the collection.
  if (pipelineResults || cullSourceResults) {
    cullScopeFailed = true;
    recomputeCulling('/api/pipeline/regroup-live', 'Analysis complete!', true);
  }
}

async function loadPipelineSliderDefaults() {
  try {
    var cfg = await safeFetch('/api/config', {}, { toast: false });
    var defaults = VireoPipelineConfig.buildSliderDefaults(cfg);
    SCORING_DEFAULTS = defaults.scoring;
    GROUPING_DEFAULTS = defaults.grouping;
  } catch(e) {
    console.warn('Could not load pipeline config:', e);
    var fallback = VireoPipelineConfig.buildSliderDefaults();
    SCORING_DEFAULTS = fallback.scoring;
    GROUPING_DEFAULTS = fallback.grouping;
  }
  applyScoringDefaults();
  applyGroupingDefaults();
}
