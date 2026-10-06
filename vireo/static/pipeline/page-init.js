// Page-init data, classify and dashboard-action deep links.
// Classic page script; load boot.js after all definitions.

// -- Page init: fetch pipeline data from API and populate the page --
// Signals that /api/pipeline/page-init's success/failure handler has run,
// so tests (and any other code that races the initial load) can wait
// before touching stage-enable checkboxes. The handler assigns
// enableEyeKeypoints.checked from cfg.eye_detect_enabled; interacting
// with the checkbox while this is still true risks having that
// assignment silently overwrite the interaction.
window._pageInitPending = true;
var _dashboardActionScopeError = '';
function initPipelinePage() {
  fetch('/api/pipeline/page-init')
    .then(function(r) { return r.json(); })
    .then(function(data) {
      var cfg = data.pipeline_config;

      // Hide taxonomy checkbox if already downloaded
      if (data.taxonomy_available) {
        var taxWrap = document.getElementById('chkDownloadTaxonomy').closest('div');
        if (taxWrap) taxWrap.style.display = 'none';
      }

      // Config selects and range
      document.getElementById('cfgSam2').value = cfg.sam2_variant;
      document.getElementById('cfgDinov2').value = cfg.dinov2_variant;
      document.getElementById('cfgProxy').value = cfg.proxy_longest_edge;
      document.getElementById('valProxy').textContent = cfg.proxy_longest_edge;
      var previewSummary = document.getElementById('cfgPreviewSizeSummary');
      if (previewSummary) {
        var previewMax = cfg.preview_max_size;
        previewSummary.textContent = previewMax === 0
          ? 'Original files'
          : ((previewMax || 1920) + 'px');
      }

      // Eye keypoints stage default tracks the global eye_detect_enabled
      // config. Missing config defaults off; the checkbox is the per-run
      // override.
      var ekCb = document.getElementById('enableEyeKeypoints');
      if (ekCb) ekCb.checked = cfg.eye_detect_enabled === true;

      // Update saved config defaults from server
      _savedModelConfig.sam2_variant = cfg.sam2_variant;
      _savedModelConfig.dinov2_variant = cfg.dinov2_variant;
      _savedModelConfig.proxy_longest_edge = cfg.proxy_longest_edge;
      _savedModelConfig.eye_detect_enabled = cfg.eye_detect_enabled === true;

      updateCardStates();
      if (typeof updateReadiness === 'function') updateReadiness();
      if (typeof updateExtractReadiness === 'function') updateExtractReadiness();
      if (typeof renderSam2Coverage === 'function') {
        renderSam2Coverage(data.mask_variant_coverage || []);
      }
      if (typeof updateSamVariantWarning === 'function') {
        updateSamVariantWarning(data.sam_variant_warning || null);
      }
    })
    .catch(function(e) {
      console.error('Failed to load pipeline page data:', e);
    })
    .finally(function() {
      window._pageInitPending = false;
    });

  // Load collections, models, and labels in parallel
  loadFolderScopeList();
  var collectionsReady = loadCollections();
  // Models and labels populate the pickers the plan endpoint reads from,
  // so chain the first plan refresh once both are in the DOM. Without
  // this, the initial plan would key off zero models / zero labels and
  // mislabel Classify as "No models selected" until the user clicks
  // something.
  Promise.all([loadModels(), loadLabels(), collectionsReady]).then(function() {
    applyClassifyQueryParams();
    applyDashboardActionQueryParams();
    refreshPipelinePlan();
  });

  // Pre-fill folders from query params (e.g., from audit panel)
  var urlParams = new URLSearchParams(window.location.search);
  var prefillFolders = urlParams.getAll('folder');
  if (prefillFolders.length > 0) {
    // Paths from the deep link; loadFolderScopeList checks the matching
    // scope boxes once the workspace folder list arrives.
    _prefillFolderPaths = prefillFolders;
    selectSourceMode('folders');
    updateStartButton();
  }
}

// Pre-select classify model and label set from query params (e.g. dashboard
// inventory's ▶ links). Runs after loadModels()/loadLabels() so the
// checkboxes exist. Transient: state is not persisted unless the user
// actually clicks Run.
function applyClassifyQueryParams() {
  var qs = new URLSearchParams(window.location.search);
  // Use getAll() so multiple selections come through as repeated params
  // (?models=a&models=b). Splitting a single decoded value on commas would
  // mishandle filenames or IDs that contain a literal comma — get() decodes
  // %2C back to ',' before any split would run.
  var qsModels = qs.getAll('models').filter(Boolean);
  var qsLabels = qs.getAll('labels').filter(Boolean);
  if (qsModels.length === 0 && qsLabels.length === 0) return;
  if (qsModels.length > 0) {
    var wantModels = new Set(qsModels);
    document.querySelectorAll('.model-checkbox').forEach(function(cb) {
      cb.checked = wantModels.has(cb.value);
    });
  }
  if (qsLabels.length > 0) {
    var wantLabels = new Set(qsLabels);
    document.querySelectorAll('.labels-run-cb').forEach(function(cb) {
      // Match by full path or by basename (the inventory passes basenames).
      // Split on both POSIX (/) and Windows (\) separators so deep links work
      // regardless of OS.
      var parts = cb.value.split(/[/\\]/);
      var base = parts[parts.length - 1];
      cb.checked = wantLabels.has(cb.value) || wantLabels.has(base);
    });
  }
  if (typeof updateLabelsPickerState === 'function') updateLabelsPickerState();
}

// Dashboard attention cards deep-link to a deliberately narrow Process
// setup.  The user still reviews the plan and clicks Run; this only selects
// the intended scope and optional stages.
function applyDashboardActionQueryParams() {
  var qs = new URLSearchParams(window.location.search);
  var stage = qs.get('dashboard_stage');
  if (stage !== 'classify' && stage !== 'previews') return;

  var classify = document.getElementById('enableClassify');
  var extract = document.getElementById('enableExtract');
  var eyes = document.getElementById('enableEyeKeypoints');
  var group = document.getElementById('enableGroup');
  var misses = document.getElementById('enableMisses');
  var speciesReview = document.getElementById('enableSpeciesReview');
  if (classify) classify.checked = stage === 'classify';
  if (extract) extract.checked = false;
  if (eyes) eyes.checked = false;
  if (group) group.checked = false;
  if (misses) misses.checked = false;
  if (speciesReview) speciesReview.checked = false;

  var collectionId = qs.get('collection_id');
  var picker = document.getElementById('collectionPicker');
  if (collectionId) {
    var option = picker && picker.querySelector(
      'option[value="' + CSS.escape(collectionId) + '"]');
    if (!option || option.disabled) {
      _dashboardActionScopeError =
        'The Dashboard collection is unavailable. Return to Dashboard and choose another scope.';
    } else {
      picker.value = collectionId;
      selectSourceMode('collection');
      onCollectionChange();
    }
  }

  var card = document.getElementById(stage === 'classify' ? 'card-classify' : 'card-previews');
  if (card) card.classList.add('expanded');
  updateCardStates();
  updateStartButton();
}
