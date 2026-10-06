// The move-folder route: From/To paths and the capture-date folder note.
// Classic page script; load boot.js after all definitions.

// How many date folders the route lists before collapsing the rest into
// "+ N more". Mirrors ``MOVE_DATE_DEST_PREVIEW_LIMIT`` in services/folder_moves.py, which
// caps the planned list server-side; a finished job's ``result`` carries
// every folder it used, so the same cap is applied here.
var DATE_FOLDER_ROWS = 8;

// Strip the selected root off a planned/actual destination so the folder
// list reads as the date folders themselves ("2026-09-12") rather than
// repeating the root on every row. Falls back to the full path when the
// path does not sit under the root we were given.
function pathUnderRoot(path, root) {
  if (!path) return '';
  if (root && path.indexOf(root) === 0) {
    var rest = path.slice(root.length).replace(/^[\/\\]+/, '');
    if (rest) return rest;
  }
  return path;
}

// Build the route's view of a date-organized move: which folders to name,
// how many photos each holds, and the single path (if any) the whole move
// lands in.
//
// The job config holds the ENQUEUE-TIME PLAN. The worker re-plans when it
// actually runs, so a capture-time edit or a scan landing between enqueue
// and start can send a photo to a different date folder than the plan
// predicted. While the job is live the plan is the best answer available
// and the note says "planned" rather than asserting where photos already
// are. Once the job finishes, ``result.destinations`` records where they
// really went (with per-folder ``planned``/``moved`` counts), so the
// finished route is drawn from the result and the config snapshot supplies
// only the template and the selected root. That way history can never
// describe a folder the move didn't use.
function moveDateView(cfg, job) {
  var root = cfg.destination || '';
  var plannedFolders = Array.isArray(cfg.date_destinations)
    ? cfg.date_destinations : [];
  var plannedFolderCount = cfg.date_destination_count || 0;
  var plannedPhotos = cfg.date_photo_count || 0;
  var planView = {
    actual: false,
    destination: cfg.resolved_destination,
    folderCount: plannedFolderCount,
    listTotal: plannedFolderCount,
    photoCount: plannedPhotos,
    plannedFolderCount: plannedFolderCount,
    plannedPhotoCount: plannedPhotos,
    folders: plannedFolders.map(function(item) {
      var count = item.photo_count || 0;
      return {
        path: item.path || '',
        label: item.relative_path || pathUnderRoot(item.path, root),
        text: count.toLocaleString() + ' ' + plural(count, 'photo'),
      };
    }),
  };

  var result = job && job.result;
  var dests = result && Array.isArray(result.destinations)
    ? result.destinations : null;
  if (isLiveStatus(job && job.status) || !dests ||
      typeof result.moved !== 'number') {
    return planView;
  }

  var shown = dests.slice(0, DATE_FOLDER_ROWS);
  // Count only the folders that actually received photos. A group whose
  // every photo was skipped (missing source, or a same-name file already
  // at the destination) still appears in ``destinations`` with
  // ``moved: 0``; counting it would claim more landing folders than the
  // move produced. It still gets a row in the list, reporting 0 of N.
  var landed = dests.filter(function(item) {
    return (item.moved || 0) + (item.already_in_place || 0) > 0;
  });
  // The denominator has to come from the same plan that produced
  // ``moved``. A photo scanned into the source subtree between enqueue and
  // the worker's re-plan is in the worker's plan but not in the config
  // snapshot, so pairing an actual ``moved`` with the enqueue-time total
  // can render "500 of 499 photos landed". Fall back to the snapshot only
  // when a result predates the per-folder ``planned`` counts.
  var plannedInResult = dests.reduce(function(sum, item) {
    return sum + (item.planned || 0);
  }, 0);
  return {
    actual: true,
    // One folder in the result means every moved photo landed there, so
    // name it. Several means there is no single landing path and the root
    // is the honest answer, with the folders listed underneath.
    destination: dests.length === 1
      ? dests[0].path : (root || cfg.resolved_destination),
    folderCount: landed.length,
    listTotal: dests.length,
    photoCount: result.moved + (result.already_in_place || 0),
    alreadyInPlace: result.already_in_place || 0,
    plannedFolderCount: plannedFolderCount,
    plannedPhotoCount: plannedInResult > 0 ? plannedInResult : plannedPhotos,
    folders: shown.map(function(item) {
      var moved = (item.moved || 0) + (item.already_in_place || 0);
      var planned = item.planned || 0;
      var inPlaceNote = item.already_in_place
        ? ' (' + item.already_in_place.toLocaleString() + ' already in place)' : '';
      return {
        path: item.path || '',
        label: pathUnderRoot(item.path, root),
        text: moved === planned
          ? moved.toLocaleString() + ' ' + plural(moved, 'photo') + inPlaceNote
          : moved.toLocaleString() + ' of ' + planned.toLocaleString() +
            ' ' + plural(planned, 'photo') +
            // "moved" stays exact when nothing was already there; with
            // in-place photos the count includes them, so it says where
            // they are rather than claiming they all moved.
            (item.already_in_place ? ' at destination' : ' moved') + inPlaceNote,
      };
    }),
  };
}

// The note under a date-organized move's To row. The path above it is the
// exact landing folder when the move resolves to a single output path (all
// photos map to one rendered folder — not necessarily one capture date;
// ``%Y`` or ``%Y/%m`` collapse many dates), and the selected root when it
// fans out, so this has to say which of the two it is and name the folders
// involved. Jobs enqueued before the plan snapshot existed carry no counts;
// they fall back to the template-only sentence.
function moveDateNote(cfg, job) {
  var template = '<code>' + esc(cfg.folder_template) + '</code>';
  if (!(cfg.date_destination_count || 0)) {
    return '<span class="job-move-route-note">Organizing photos into ' +
      'capture-date folders using ' + template + '</span>';
  }
  var view = moveDateView(cfg, job);
  var count = view.photoCount;
  var planned = view.plannedPhotoCount;
  // "All N" only when nothing was left behind; otherwise name both numbers.
  var photos = (!view.actual || count === planned)
    ? 'All ' + count.toLocaleString() + ' ' + plural(count, 'photo')
    : count.toLocaleString() + ' of ' + planned.toLocaleString() + ' ' +
      plural(planned, 'photo');
  var verb = view.actual
    ? (view.alreadyInPlace ? ' are in ' : ' landed in ') : ' planned to land in ';

  // Take the single-folder wording only when the move has exactly one
  // folder to talk about. One folder that received photos alongside others
  // that received none still needs the list, or the empty ones vanish.
  if (view.folderCount === 1 && view.listTotal === 1) {
    // A single folder only proves the move resolves to one rendered path;
    // a coarser template like ``%Y`` or ``%Y/%m`` can collapse many
    // distinct capture dates into it. Say nothing about capture dates.
    //
    // And only claim the template resolved to one path when one folder is
    // also what the plan expected: a cancelled fan-out that finished its
    // first group lands in one folder without the template being the
    // reason, and saying otherwise would explain the result wrongly.
    var tail = (!view.actual || view.plannedFolderCount === 1)
      ? ' — ' + template + ' resolves to one path for this set'
      : ' — the plan at start had ' +
        view.plannedFolderCount.toLocaleString() + ' folders';
    return '<span class="job-move-route-note">' + photos + verb +
      'this single folder' + tail + '</span>';
  }

  // "All N planned to land in 5 folders" reads as a claim about the split
  // that hasn't happened yet; a fan-out still being planned just states the
  // plan.
  var header = view.actual
    ? photos + (view.alreadyInPlace ? ' are in ' : ' landed in ') + view.folderCount.toLocaleString()
    : count.toLocaleString() + ' ' + plural(count, 'photo') +
      ' planned across ' + view.folderCount.toLocaleString();
  var html = '<span class="job-move-route-note">' + header +
    ' capture-date ' + plural(view.folderCount, 'folder') +
    ' under this path, named by ' + template + ':</span>';
  html += '<ul class="job-move-route-dates">';
  view.folders.forEach(function(item) {
    html += '<li title="' + escapeAttr(item.path) + '">' +
      esc(item.label) + ' · ' + item.text + '</li>';
  });
  if (view.listTotal > view.folders.length) {
    var rest = view.listTotal - view.folders.length;
    html += '<li class="job-move-route-dates-more">+ ' + rest.toLocaleString() +
      ' more ' + plural(rest, 'folder') + '</li>';
  }
  // The worker re-plans, so a finished move can use a different number of
  // folders than the enqueue-time plan predicted (a re-plan, or a cancel
  // that never reached the later groups). State the plan as a plain fact
  // instead of guessing which specific folders went unused.
  if (view.actual && view.plannedFolderCount !== view.listTotal) {
    html += '<li class="job-move-route-dates-more">Planned at start: ' +
      view.plannedFolderCount.toLocaleString() + ' ' +
      plural(view.plannedFolderCount, 'folder') + '</li>';
  }
  html += '</ul>';
  return html;
}

function renderMoveRoute(job) {
  if (!job || job.type !== 'move-folder') return '';
  var cfg = jobConfig(job);
  var source = cfg.source_path;
  var destination = cfg.resolved_destination;
  // Older history entries predate the route snapshot. Avoid presenting
  // their destination parent as though it were the exact landing folder.
  if (!source || !destination) return '';
  // A finished date-organized move knows where its photos actually went;
  // prefer that over the enqueue-time plan the config snapshot holds.
  if (cfg.folder_template) {
    destination = moveDateView(cfg, job).destination || destination;
  }

  var html = '<div class="job-move-route" aria-label="Move locations">';
  html += '<span class="job-move-route-label">From</span>';
  html += '<span class="job-move-route-path" title="' +
    escapeAttr(source) + '">' + esc(source) + '</span>';
  html += '<span class="job-move-route-label">To</span>';
  html += '<span class="job-move-route-path" title="' +
    escapeAttr(destination) + '">' + esc(destination) + '</span>';
  if (cfg.folder_template) {
    html += moveDateNote(cfg, job);
  } else if (cfg.merge) {
    html += '<span class="job-move-route-note">Merging with the existing destination folder</span>';
  }
  html += '</div>';
  return html;
}
