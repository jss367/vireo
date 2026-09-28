function openPipeline(photoId) {
  var alreadyOpen = !!window._pipelineEscToken;
  if (alreadyOpen) Keymap.popEsc(window._pipelineEscToken);
  window._pipelineEscToken = Keymap.pushEsc(function() { closePipeline(); });

  var overlay = document.getElementById('pipelineOverlay');
  var content = document.getElementById('pipelineContent');
  overlay.classList.add('active');
  if (!alreadyOpen) Keymap.lockBodyScroll();
  content.innerHTML = '<p style="color:var(--text-dim);">Loading pipeline data...</p>';

  safeFetch('/api/photos/' + photoId + '/pipeline', {}, { toast: false })
    .then(function(data) { renderPipeline(data, photoId); })
    .catch(function(e) { content.innerHTML = '<p style="color:var(--danger);">Failed to load: ' + escapeHtml(e.message) + '</p>'; });
}

function closePipeline() {
  var wasOpen = !!window._pipelineEscToken;
  if (wasOpen) { Keymap.popEsc(window._pipelineEscToken); window._pipelineEscToken = null; }
  document.getElementById('pipelineOverlay').classList.remove('active');
  if (wasOpen) Keymap.unlockBodyScroll();
}

// Raw match scores come in two scales that must never be mixed: a bounded
// cosine similarity (BioCLIP) and an unbounded class logit (iNat21). Render
// each at the precision its scale deserves; the server already refuses to
// compare one against a threshold calibrated for the other.
function formatMatchScore(value, scoreKind) {
  if (value == null) return '—';
  return scoreKind === 'logit' ? Number(value).toFixed(1) : Number(value).toFixed(3);
}

// The one line that answers what a confidence pill gets read as: did anything
// in the list actually match? "uncalibrated" and "unavailable" deliberately
// render as neither pass nor fail — no threshold means no verdict, and
// dressing a missing verdict up as a passing one is the exact failure this
// whole surface exists to remove.
var MATCH_SUMMARY_STYLES = {
  unlisted:     { color: 'var(--warning)', label: 'No good match in this label list' },
  listed:       { color: 'var(--accent)',  label: 'Matched a label in this list' },
  uncalibrated: { color: 'var(--text-dim)', label: 'Match strength recorded, not judged' },
  unavailable:  { color: 'var(--text-dim)', label: 'Match strength not recorded' },
};

// The photo-level state alone is not safe to render. It resolves to `listed`
// as soon as ONE run clears its floor, and a photo can hold two species or be
// scored by two models — so "matched a label" would sit above a table in which
// a specific run explicitly matched nothing. Whenever a positively-failing run
// exists it takes the headline, and the failing runs are named individually.
function renderMatchSummary(summary) {
  if (!summary || !summary.state) return '';
  var failures = summary.unlisted_runs || [];
  var mixed = summary.state !== 'unlisted' && failures.length > 0;
  var style = mixed
    ? { color: 'var(--warning)',
        label: failures.length + (failures.length === 1 ? ' run' : ' runs') +
               ' matched nothing in the label list' }
    : MATCH_SUMMARY_STYLES[summary.state];
  if (!style) return '';
  var html = '<div class="pipeline-match-summary" style="border-left:3px solid ' +
    style.color + ';padding:8px 10px;margin:8px 0;background:var(--bg-secondary);border-radius:0 4px 4px 0;">';
  html += '<div style="color:' + style.color + ';font-weight:600;font-size:13px;">' +
    escapeHtml(style.label) + '</div>';
  if (mixed) {
    // Name the detection as well as the model: with two animals in the frame
    // "BioCLIP failed" is ambiguous about which subject it failed on, and the
    // per-run table below is keyed by detection.
    failures.forEach(function(run) {
      html += '<div style="color:var(--text-dim);font-size:12px;margin-top:4px;">' +
        escapeHtml(run.classifier_model || run.model || '') +
        ' on detection ' + escapeHtml(String(run.detection_id)) + ': ' +
        escapeHtml(run.explanation || '') + '</div>';
    });
    // Why the photo is not being called unidentifiable has three different
    // answers, and saying the wrong one would invent a passing grade nobody
    // gave. `listed` means another run genuinely cleared its floor. Otherwise
    // a run was never judged — but "no floor was calibrated for it" and "it
    // recorded no match strength at all" are different facts with different
    // fixes (calibrate vs. re-classify), so the blocking runs are read back
    // rather than assumed (CodeRabbit on ecb275c).
    var blockers = (summary.runs || []).filter(function(r) {
      return r && (r.state === 'uncalibrated' ||
                   (r.state === 'unavailable' && r.blocks_unlisted));
    });
    var blockedByUncalibrated = blockers.some(function(r) {
      return r.state === 'uncalibrated';
    });
    var blockedByUnrecorded = blockers.some(function(r) {
      return r.state === 'unavailable';
    });
    var mixedReason;
    if (summary.state === 'listed') {
      mixedReason = 'Another run on this photo did clear its floor, so the ' +
        'photo is not unidentifiable — but the predictions from the runs ' +
        'above are the closest available labels, not matches.';
    } else {
      var unjudgedBecause = blockedByUncalibrated && blockedByUnrecorded
        ? 'one with no calibrated floor for its score, another with no ' +
          'recorded match strength at all'
        : blockedByUnrecorded
          ? 'with no recorded match strength'
          : 'with no calibrated floor for its score';
      mixedReason = 'Another run on this photo went unjudged (' +
        unjudgedBecause + '), so the photo is not being called unmatched on ' +
        'the strength of the runs above — but nothing has vouched for it ' +
        'either.';
    }
    html += '<div style="color:var(--text-muted);font-size:11px;margin-top:6px;">' +
      escapeHtml(mixedReason) + '</div>';
  } else {
    (summary.assessments || []).forEach(function(a) {
      if (!a || !a.explanation) return;
      html += '<div style="color:var(--text-dim);font-size:12px;margin-top:4px;">' +
        escapeHtml(a.model) + ': ' + escapeHtml(a.explanation) + '</div>';
    });
  }
  // Only offer calibration when calibration is actually the missing piece. A
  // run that recorded no match strength has nothing for a floor to judge, so
  // pointing the user at the calibration script would send them to fix
  // something that is not broken.
  var suggestCalibration = summary.state === 'uncalibrated' && (
    !summary.runs ||
    summary.runs.some(function(r) { return r && r.state === 'uncalibrated'; })
  );
  if (suggestCalibration) {
    html += '<div style="color:var(--text-muted);font-size:11px;margin-top:6px;">' +
      'Run scripts/calibrate_match_threshold.py to derive a floor from your own confirmed IDs.</div>';
  }
  html += '</div>';
  return html;
}

// One run's own verdict, for the table that lists every run this photo has.
// A run against a label list the user has since replaced is labelled
// superseded rather than pass/fail: the number is real, but judging it beside
// the current list's rows would put a verdict from an abandoned list next to
// predictions it never produced. `is_current` is set by the same
// latest-fingerprint rule the predictions table is pinned to.
var MATCH_RUN_VERDICTS = {
  unlisted:     { color: 'var(--warning)',  label: 'no match',
                  title: 'Nothing in this label list matched, by the calibrated floor for this model.' },
  listed:       { color: 'var(--accent)',   label: 'match',
                  title: 'The best label cleared the calibrated floor for this model.' },
  uncalibrated: { color: 'var(--text-dim)', label: 'not judged',
                  title: 'Score recorded, but no threshold is calibrated for this model.' },
  unavailable:  { color: 'var(--text-dim)', label: 'not recorded',
                  title: 'No raw score was stored for this run.' },
};

function matchRunVerdict(row) {
  if (row.is_current === 0 || row.is_current === false) {
    return {
      color: 'var(--text-muted)', label: 'superseded',
      title: 'Run against a label list that has since been replaced for this ' +
             'detection. Kept for history; it does not affect the verdict above.',
    };
  }
  var state = (row.assessment && row.assessment.state) || 'unavailable';
  return MATCH_RUN_VERDICTS[state] || MATCH_RUN_VERDICTS.unavailable;
}

// Every recorded run on this photo, including the ones step 4's table cannot
// show: full-image classification and detections under the current detector
// threshold. Those are routinely where the species a user is asking about
// actually came from, so listing them here is the difference between the
// inspector explaining the photo and quietly omitting the relevant half.
function renderMatchScoreTable(rows) {
  if (!rows || rows.length === 0) return '';
  var html = '<div style="margin-top:10px;">';
  html += '<div class="pipeline-stat" style="color:var(--text-muted);">Match strength per run</div>';
  html += '<table class="pipeline-table"><tr><th>Model</th><th>Best label</th><th>Match</th>' +
    '<th>Margin</th><th>Labels</th><th>Detection</th><th>Verdict</th></tr>';
  rows.forEach(function(r) {
    var det = r.detector_model === 'full-image'
      ? 'full image'
      : escapeHtml(String(r.detector_model || '')) + ' ' +
        (r.detector_confidence == null ? '' : Math.round(r.detector_confidence * 100) + '%');
    var verdict = matchRunVerdict(r);
    html += '<tr>' +
      '<td style="color:var(--text-dim);">' + escapeHtml(r.classifier_model) + '</td>' +
      '<td>' + escapeHtml(r.top_species || '—') + '</td>' +
      '<td>' + escapeHtml(formatMatchScore(r.max_match_score, r.score_kind)) + '</td>' +
      '<td style="color:var(--text-dim);">' + escapeHtml(formatMatchScore(r.match_margin, r.score_kind)) + '</td>' +
      '<td style="color:var(--text-dim);">' + (r.label_count == null ? '—' : r.label_count) + '</td>' +
      '<td style="color:var(--text-dim);">' + det + '</td>' +
      '<td style="color:' + verdict.color + ';" title="' + escapeAttr(verdict.title) + '">' +
        escapeHtml(verdict.label) + '</td>' +
    '</tr>';
  });
  html += '</table></div>';
  return html;
}

function renderPipeline(data, photoId) {
  var c = document.getElementById('pipelineContent');
  var diag = data.classification_diagnostics || {};
  var detectorThreshold = diag.detector_confidence_threshold != null ? diag.detector_confidence_threshold : 0.2;
  var detectorThresholdPct = Math.round(detectorThreshold * 100);
  var maxDetectorPct = diag.max_detector_confidence != null ? Math.round(diag.max_detector_confidence * 100) : null;
  var html = '<div class="pipeline-steps">';

  // Step 1: Original image
  html += '<div class="pipeline-step">' +
    '<div class="pipeline-step-header"><span class="pipeline-step-num">1</span><span class="pipeline-step-title">Original Image</span></div>' +
    '<div class="pipeline-stat"><b>' + escapeHtml(data.filename) + '</b></div>' +
    (data.width ? '<div class="pipeline-stat">' + data.width + ' &times; ' + data.height + '</div>' : '') +
    (data.timestamp ? '<div class="pipeline-stat">' + data.timestamp + '</div>' : '') +
    '</div>';

  // Step 2: Detection
  var hasDetection = data.detection_box && data.detection_box.x != null;
  html += '<div class="pipeline-step">' +
    '<div class="pipeline-step-header"><span class="pipeline-step-num">2</span><span class="pipeline-step-title">Subject Detection (MegaDetector)</span></div>';

  if (hasDetection) {
    var db = data.detection_box;
    var cropBox = data.crop_box || {};
    html += '<div class="pipeline-images">' +
      '<div class="pipeline-img-wrap">' +
        '<img src="/photos/' + photoId + '/full" alt="Detection">' +
        '<div class="pipeline-det-box" style="left:' + (db.x*100) + '%;top:' + (db.y*100) + '%;width:' + (db.w*100) + '%;height:' + (db.h*100) + '%;"></div>' +
        (cropBox.x != null ? '<div class="pipeline-crop-box" style="left:' + (cropBox.x*100) + '%;top:' + (cropBox.y*100) + '%;width:' + (cropBox.w*100) + '%;height:' + (cropBox.h*100) + '%;"></div>' : '') +
        '<div class="pipeline-img-label">Green = detection box, Yellow dashed = crop sent to classifier</div>' +
      '</div>' +
    '</div>' +
    '<div style="margin-top:8px;">' +
      '<div class="pipeline-stat">Confidence: <b>' + Math.round(data.detection_conf * 100) + '%</b></div>' +
      '<div class="pipeline-stat">Subject size: <b>' + Math.round((db.w * db.h) * 10000) / 100 + '%</b> of frame</div>' +
    '</div>';
  } else if ((diag.hidden_detection_count || 0) > 0) {
    html += '<p style="color:var(--text-dim);font-size:13px;">No subject above the current detector threshold. ' +
      diag.hidden_detection_count + ' lower-confidence detection' + (diag.hidden_detection_count === 1 ? ' is' : 's are') +
      ' stored' + (maxDetectorPct != null ? ' (best ' + maxDetectorPct + '%, threshold ' + detectorThresholdPct + '%)' : '') + '.</p>';
  } else {
    html += '<p style="color:var(--text-dim);font-size:13px;">No subject detected</p>';
  }
  html += '</div>';

  // Step 3: Classification crop
  html += '<div class="pipeline-step">' +
    '<div class="pipeline-step-header"><span class="pipeline-step-num">3</span><span class="pipeline-step-title">Classification Input (BioCLIP)</span></div>' +
    '<div class="pipeline-images">' +
      '<div class="pipeline-img-wrap" style="max-width:300px;">' +
        '<img src="/photos/' + photoId + '/crop" alt="Crop">' +
        '<div class="pipeline-img-label">' + (hasDetection ? 'Cropped + padded region' : 'Full image (no detection)') + '</div>' +
      '</div>' +
    '</div>' +
  '</div>';

  // Step 4: Predictions
  html += '<div class="pipeline-step">' +
    '<div class="pipeline-step-header"><span class="pipeline-step-num">4</span><span class="pipeline-step-title">Classification Results</span></div>';

  // The confidence column below is a softmax over whichever label list ran,
  // so it always sums to 1 and always crowns a winner — it cannot say whether
  // anything in the list actually fit. Lead with the verdict that can, so a
  // 99% is never read as agreement when the list simply had nothing right in
  // it. Rendered above the table because it qualifies every row in it.
  html += renderMatchSummary(data.match_summary);

  if (data.predictions && data.predictions.length > 0) {
    html += '<table class="pipeline-table"><tr><th>Species</th><th>Confidence</th><th>Match</th><th>Model</th><th>Status</th><th>Category</th></tr>';
    data.predictions.forEach(function(p) {
      var pct = Math.round(p.confidence * 100);
      var statusColor = p.status === 'accepted' ? 'var(--accent)' : p.status === 'rejected' ? 'var(--danger)' : 'var(--text-dim)';
      // Species/model values can carry user-typed text (freeform species
      // confirms, custom label files) — escape before interpolating.
      html += '<tr>' +
        '<td>' + escapeHtml(p.species) + (p.scientific_name ? '<br><span style="font-size:11px;color:var(--text-dim);font-style:italic;">' + escapeHtml(p.scientific_name) + '</span>' : '') + '</td>' +
        '<td><span class="pipeline-conf-bar" style="width:' + pct + 'px;"></span> ' + pct + '%</td>' +
        '<td style="color:var(--text-dim);" title="Raw pre-softmax match score — absolute, and unchanged by which other labels are in the list">' +
          (p.match_score == null ? '&mdash;' : escapeHtml(formatMatchScore(p.match_score))) + '</td>' +
        '<td style="color:var(--text-dim);">' + escapeHtml(p.model) + '</td>' +
        '<td style="color:' + statusColor + ';">' + escapeHtml(p.status) + '</td>' +
        '<td style="color:var(--text-dim);">' + escapeHtml(p.category) + '</td>' +
      '</tr>';
    });
    html += '</table>';

    // Taxonomy hierarchy
    var withTax = data.predictions.find(function(p) { return p.taxonomy_order; });
    if (withTax) {
      var parts = [];
      if (withTax.taxonomy_kingdom) parts.push(withTax.taxonomy_kingdom);
      if (withTax.taxonomy_phylum) parts.push(withTax.taxonomy_phylum);
      if (withTax.taxonomy_class) parts.push(withTax.taxonomy_class);
      if (withTax.taxonomy_order) parts.push(withTax.taxonomy_order);
      if (withTax.taxonomy_family) parts.push(withTax.taxonomy_family);
      if (withTax.taxonomy_genus) parts.push(withTax.taxonomy_genus);
      html += '<div style="margin-top:8px;font-size:12px;color:var(--text-dim);">' +
        '<span style="color:var(--text-muted);">Taxonomy:</span> ' +
        parts.map(escapeHtml).join(' &rsaquo; ') +
      '</div>';
    }

    // Group info
    var grouped = data.predictions.find(function(p) { return p.group_id; });
    if (grouped) {
      html += '<div style="margin-top:8px;">' +
        '<div class="pipeline-stat">Burst group: <b>' + grouped.group_id + '</b></div>' +
        '<div class="pipeline-stat">Votes: <b>' + grouped.vote_count + '/' + grouped.total_votes + '</b></div>' +
      '</div>';
    }
  } else {
    var currentPreds = diag.current_prediction_count || 0;
    var hiddenPreds = diag.hidden_prediction_count || 0;
    var classifierRuns = diag.classifier_run_count || 0;
    var fullImagePreds = diag.full_image_prediction_count || 0;
    var fullImageRuns = diag.full_image_classifier_run_count || 0;
    if (hiddenPreds > 0) {
      html += '<p style="color:var(--text-dim);font-size:13px;">Classification ran, but ' +
        hiddenPreds + ' stored prediction' + (hiddenPreds === 1 ? ' is' : 's are') +
        ' attached to detection' + (hiddenPreds === 1 ? '' : 's') +
        ' below the current detector threshold' +
        (maxDetectorPct != null ? ' (best detection ' + maxDetectorPct + '%, threshold ' + detectorThresholdPct + '%)' : '') + '.</p>';
      // Both conditions routinely hold at once — a weak box plus a full-image
      // pass. Naming only the first left the species the user is looking at
      // unexplained, because it usually came from the full-image run this
      // table excludes. The per-run table below lists them.
      if (fullImagePreds > 0) {
        html += '<p style="color:var(--text-dim);font-size:13px;">' + fullImagePreds +
          ' further prediction' + (fullImagePreds === 1 ? '' : 's') +
          ' came from classifying the full image, which this table excludes.</p>';
      }
    } else if (fullImagePreds > 0) {
      html += '<p style="color:var(--text-dim);font-size:13px;">Classification ran using the full image because no detector box was available.</p>';
    } else if (fullImageRuns > 0) {
      html += '<p style="color:var(--text-dim);font-size:13px;">Classification ran using the full image, but no predictions were stored above the classification threshold.</p>';
    } else if (classifierRuns > 0 && currentPreds === 0) {
      html += '<p style="color:var(--text-dim);font-size:13px;">Classification ran, but no predictions were stored above the classification threshold.</p>';
    } else if ((diag.hidden_detection_count || 0) > 0) {
      html += '<p style="color:var(--text-dim);font-size:13px;">No predictions visible because all stored detections are below the current detector threshold.</p>';
    } else {
      html += '<p style="color:var(--text-dim);font-size:13px;">No predictions yet &mdash; run classification first</p>';
    }
  }
  html += renderMatchScoreTable(data.match_scores);
  html += '</div>';

  // Step 5: Quality scoring
  html += '<div class="pipeline-step">' +
    '<div class="pipeline-step-header"><span class="pipeline-step-num">5</span><span class="pipeline-step-title">Quality Scoring</span></div>' +
    '<div class="pipeline-stat">Overall sharpness: <b>' + (data.sharpness != null ? Math.round(data.sharpness) : 'N/A') + '</b></div>' +
    '<div class="pipeline-stat">Subject sharpness: <b>' + (data.subject_sharpness != null ? Math.round(data.subject_sharpness) : 'N/A') + '</b></div>' +
    '<div class="pipeline-stat">Quality score: <b>' + (data.quality_score != null ? data.quality_score : 'N/A') + '</b></div>' +
  '</div>';

  // Step 6: Keywords
  if (data.keywords && data.keywords.length > 0) {
    html += '<div class="pipeline-step">' +
      '<div class="pipeline-step-header"><span class="pipeline-step-num">6</span><span class="pipeline-step-title">Current Keywords</span></div>' +
      '<div style="display:flex;gap:6px;flex-wrap:wrap;">';
    data.keywords.forEach(function(k) {
      // Keyword names are user-controlled (typed confirms, XMP imports).
      html += '<span style="background:var(--bg-tertiary);padding:3px 8px;border-radius:4px;font-size:12px;">' + escapeHtml(k.name) + '</span>';
    });
    html += '</div></div>';
  }

  html += '</div>';
  c.innerHTML = html;
}
