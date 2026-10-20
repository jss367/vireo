// Import Retry and Resume: in-flight retries, takeovers, and the retry body.
// Classic page script; load boot.js after all definitions.

/** @param {JobsRow} job */ function canRestartInPlace(job) {
  if (!job || job.type !== 'import-in-place' ||
      ['failed', 'cancelled'].indexOf(job.status) < 0) return false;
  var cfg = jobConfig(job);
  return cfg.source_snapshot_id != null ||
    (Array.isArray(cfg.sources) && cfg.sources.length > 0);
}

/** @param {JobsRow} job */ function inPlaceRestartJobId(job) {
  var local = inPlaceRestarts[job.id];
  if (local && local.jobId) return local.jobId;
  var child = activeJobs.concat(historyJobs).find(function(candidate) {
    return candidate.type === 'import-in-place' &&
      jobConfig(candidate).restarted_from === job.id;
  });
  return child ? child.id : null;
}

// A retry job carries its parent's id in ``config.parent_import_job_id``
// and the original failed import's id in ``config.root_import_job_id``
// (see api_job_import_photos). Suppresses the Retry button on the
// original failed import while any active-or-recently-launched retry
// in that chain is still in flight — a second click would otherwise
// enqueue a parallel import of the same source and destination,
// letting the after-import chains and any ``after_process_move`` race
// with the first retry's move (which may already have relocated the
// folders). Matches either field so a retry-of-retry — whose direct
// ``parent_import_job_id`` points at the first retry, not the
// original failed import — still blocks the original's Retry button.
/** @param {string} parentId */ function hasActiveRetryFor(parentId) {
  if (!parentId) return false;
  if (pendingImportRetries[parentId]) return true;
  return activeJobs.some(function(candidate) {
    if (!candidate || candidate.type !== 'import') return false;
    var status = candidate.status;
    if (status === 'completed' || status === 'failed' ||
        status === 'cancelled') return false;
    var cfg = jobConfig(candidate);
    if (!cfg) return false;
    return cfg.parent_import_job_id === parentId ||
           cfg.root_import_job_id === parentId;
  });
}

// An import a Vireo restart cut short. Resumable only when its
// checkpoint recorded which photos it had already landed
// (``photo_ids``, even if empty): a resume must carry those into the
// after-import processing, and an older interrupted row without the
// list would silently leave them unprocessed.
/** @param {JobsRow} job @param {JobsRow[]} rows */ function isResumableImport(job, rows) {
  var result = job && job.result;
  if (!(job && job.type === 'import' && job.status === 'failed' &&
      result && result.interrupted && Array.isArray(result.photo_ids))) {
    return false;
  }
  if (job.import_resume_pending) return false;
  if (typeof job.import_resume_allowed === 'boolean') return job.import_resume_allowed;
  // An import whose chain ran AND whose tag pass was marked applied
  // finished everything but the final write; resuming would collect
  // and process its photos twice. A ``chained`` mark on a row whose
  // import failed (``ok === false``) came from ``_chain_after_import``
  // returning early via its "import failed" branch — the chain never
  // actually ran, so the row stays resumable.
  var neverStarted = new Set((rows || []).filter(function(row) {
    return pipelineNeverStarted(row);
  }).map(function(row) { return row.id; }));
  var chainInvalidated = importChainInvalidated(jobConfig(job), result, rows, job.id, neverStarted);
  if (!chainInvalidated && (jobConfig(job).chain_already_ran || (result.chained && result.ok !== false)) && result.tags_applied) return false;
  // Chain ran but tag/GPS work is still owed (a Stop cut the pass
  // short, or an error left tags unmarked). The resume replays only
  // the tag pass and skips re-chaining server-side.
  return true;
}

/** @param {JobsResult | null | undefined} result */ function hasFailedImportFiles(result) {
  return !!result && typeof result.failed === 'number' && result.failed > 0;
}

/** @param {JobsConfig} config @param {JobsResult} result @param {JobsRow[]} rows @param {string} jobId @param {boolean=} inferLegacy */ function importChainProcessJobIds(config, result, rows, jobId, inferLegacy) {
  var byId = new Map((rows || []).filter(function(row) {
    return row.type === 'import';
  }).map(function(row) { return [row.id, row]; }));
  if (inferLegacy !== false && (config.chain_already_ran || (!('chain_already_ran' in config) && importFailedBeforeStart(result)) ||
      result.after_import_skipped === 'chain already ran on the interrupted parent') && !(config.chain_process_job_ids || []).length &&
      !result.process_job_id && config.parent_import_job_id) {
    var scopeRoot = config.parent_import_job_id;
    var cutoff = (byId.get(jobId) || {}).started_at || '';
    var found = new Set([scopeRoot]), changed = true;
    while (changed) {
      changed = false;
      byId.forEach(function(row, id) {
        if (id === jobId || found.has(id) || (cutoff && (row.started_at || '') > cutoff)) return;
        var cfg = jobConfig(row);
        if (found.has(cfg.parent_import_job_id || '') || cfg.root_import_job_id === scopeRoot) {
          found.add(id);
          changed = true;
        }
      });
    }
    var inherited = new Set();
    found.forEach(function(id) {
      var r = (byId.get(id) || {}).result || {};
      if (r.process_job_id) inherited.add(r.process_job_id);
    });
    if (inherited.size) return Array.from(inherited).sort();
  }
  var seen = new Set();
  while (true) {
    if (result.process_job_id) return [result.process_job_id];
    if (result.after_import_skipped === 'import-only' || result.after_import_skipped === 'no new photos') return [];
    if (config.chain_process_job_ids && config.chain_process_job_ids.length) return config.chain_process_job_ids.slice();
    var parentId = config.parent_import_job_id;
    if (!parentId || seen.has(parentId) || !byId.has(parentId)) return [];
    seen.add(parentId);
    var row = byId.get(parentId); if (!row) return [];
    config = jobConfig(row);
    result = row.result || {};
  }
}

/** @param {JobsConfig} config @param {JobsResult} result @param {JobsRow[]} rows @param {string} jobId */ function importChainProcessJobGroups(config, result, rows, jobId) {
  var recorded = importChainProcessJobIds(config, result, rows, jobId, false);
  if (!recorded.length) {
    var candidates = importChainProcessJobIds(config, result, rows, jobId);
    return candidates.length ? [candidates] : [];
  }
  var byId = new Map((rows || []).filter(function(row) {
    return row.type === 'import';
  }).map(function(row) { return [row.id, row]; }));
  var seen = new Set();
  while (true) {
    if (result.process_job_id) return [[result.process_job_id]];
    if ((config.chain_process_job_ids || []).length) {
      var groups = config.chain_process_job_groups;
      if (Array.isArray(groups) && groups.length && groups.every(function(group) {
        return Array.isArray(group) && group.length && group.every(function(id) { return typeof id === 'string'; });
      })) {
        var covered = new Set(groups.flat());
        if (covered.size === new Set(recorded).size && recorded.every(function(id) { return covered.has(id); })) return groups;
      }
      return recorded.map(function(id) { return [id]; });
    }
    var parentId = config.parent_import_job_id;
    if (!parentId || seen.has(parentId) || !byId.has(parentId)) return recorded.map(function(id) { return [id]; });
    seen.add(parentId);
    var row = byId.get(parentId); if (!row) return [];
    config = jobConfig(row);
    result = row.result || {};
  }
}

/** @param {JobsConfig} config @param {JobsResult} result @param {JobsRow[]} rows @param {string} jobId @param {Set<string>} neverStarted */ function importChainInvalidated(config, result, rows, jobId, neverStarted) {
  return importChainProcessJobGroups(config, result, rows, jobId).some(function(group) {
    return group.every(function(id) { return neverStarted.has(id); });
  });
}

/** @param {JobsRow} row */ function pipelineNeverStarted(row) {
  return row.type === 'pipeline' && ((row.result && row.result.never_started) ||
    (row.status === 'cancelled' && row.result == null && row.duration === null));
}

// Whether a later run took over an import's Resume (interrupted by a
// restart) or Retry (failed files). The run that continues it marks
// ``tags_applied`` / ``chained`` on its own row, never on the parent's,
// so the parent's row alone still offers its button after that run
// finished; a second click would process the photos again (and a
// Resume would re-tag them). ``rows`` are finished history rows (the
// page's ``historyJobs``: a descendant always starts after its parent,
// so it is in the same newest-first window). Mirrors
// ``import_resume_takeover`` in services/imports.py, which the server
// enforces with a 409; keep the two equivalent. ``by`` is the
// descendant to continue from, null while the parent itself is still
// the place to continue; ``kind`` is "done" (nothing left), "resume"
// (it was interrupted) or "retry" (it failed files after its tag pass).
/** @param {JobsRow | undefined} parent @param {JobsRow[]} rows */ function importResumeTakeover(parent, rows) {
  var parentResult = parent && parent.result;
  var parentInterrupted = !!(parentResult && parentResult.interrupted);
  if (!(parent && parent.type === 'import' && parentResult &&
      (parentInterrupted || hasFailedImportFiles(parentResult) || importFailedBeforeStart(parentResult)))) {
    return {tagsApplied: !!(parentResult && parentResult.tags_applied),
            chained: !!(parentResult && parentResult.chained),
            by: null, byStartedAt: null, kind: null, parentInterrupted: false};
  }
  var neverStartedProcesses = new Set((rows || []).filter(function(row) {
    return pipelineNeverStarted(row);
  }).map(function(row) { return row.id; }));
  /** @type {Record<string,JobsImportEntry[]>} */ var children = {};
  /** @type {JobsImportEntry[]} */ var candidates = [];
  (rows || []).forEach(function(row) {
    if (!row || row.type !== 'import' || row.id === parent.id) return;
    if (['completed', 'failed', 'cancelled'].indexOf(row.status) < 0) return;
    var result = (row.result && typeof row.result === 'object') ? row.result : {};
    var cfg = jobConfig(row) || {};
    var entry = {id: row.id, status: row.status,
                 startedAt: row.started_at || '', result: result,
                 root: cfg.root_import_job_id, config: cfg,
                 parent: cfg.parent_import_job_id};
    candidates.push(entry);
    var parentKey = String(cfg.parent_import_job_id);
    (children[parentKey] = children[parentKey] || []).push(entry);
  });
  /** @type {Record<string,JobsImportEntry>} */ var found = {};
  var frontier = [parent.id];
  candidates.forEach(function(e) {
    if (e.root === parent.id && !found[e.id]) {
      found[e.id] = e;
      frontier.push(e.id);
    }
  });
  while (frontier.length) {
    (children[String(frontier.pop())] || []).forEach(function(child) {
      if (!found[child.id]) {
        found[child.id] = child;
        frontier.push(child.id);
      }
    });
  }
  var descendants = Object.values(found)
    .filter(function(e) { return e !== undefined && !e.result.never_started; });
  descendants.forEach(function(e) {
    var r = e.result;
    var tags, chainStep;
    if ('tags_applied' in r || 'chained' in r || r.interrupted) {
      tags = !!r.tags_applied;
      chainStep = !!r.chained;
    } else {
      // Finished before the marks were kept on the final row; only a
      // run that reached the chain after a clean import has a collection.
      var tagOnlyCompleted = e.status === 'completed' &&
        r.after_import_skipped === 'chain already ran on the interrupted parent' &&
        !((r.tagging || {}).errors || []).length;
      tags = tagOnlyCompleted || (r.collection_id != null &&
        !((r.tagging || {}).errors || []).length);
      chainStep = tagOnlyCompleted || (!r.cancelled && (
        r.process_job_id != null || r.after_import_skipped === 'import-only' ||
        r.after_import_skipped === 'no new photos'));

    }
    e.tagsApplied = tags;
    // After a failed import the chain step marks but skips processing.
    e.chainProcessJobIds = importChainProcessJobIds(e.config, r, rows, e.id);
    e.chained = (!!e.config.chain_already_ran ||
      (chainStep && r.ok !== false && !neverStartedProcesses.has(r.process_job_id || ''))) &&
      !importChainInvalidated(e.config, r, rows, e.id, neverStartedProcesses);
    e.resumable = e.status === 'failed' && !!r.interrupted &&
      Array.isArray(r.photo_ids) && !(tags && e.chained);
    e.hasFailedFiles = hasFailedImportFiles(r);
  });
  // What a Resume replays: the parent's marks plus what descendants paid.
  /** @param {JobsConfig} cfg @param {JobsResult} result */ function tagScope(cfg, result) {
    var scope = new Set(), identities = new Map();
    var fingerprints = Object.assign({}, cfg.carry_photo_fingerprints || {},
      result.photo_fingerprints || {}, result.carried_photo_fingerprints || {});
    [result.photo_ids, result.carried_photo_ids, result.recovered_photo_ids,
     cfg.carry_photo_ids, cfg.untagged_photo_ids].forEach(function(values) {
      (values || []).forEach(function(pid) {
        if (!Number.isInteger(pid) || pid <= 0) return;
        var fp = fingerprints[pid];
        var identity = typeof fp === 'string' ? fp.substring(fp.lastIndexOf('|s=') + 3) : '';
        if (typeof fp === 'string' && fp.indexOf('|s=') < 0) identity = fp;
        var token = JSON.stringify(['photo', pid, identity]);
        scope.add(token);
        identities.set(pid, token);
      });
    });
    [cfg.recover_landed_files, result.landed_files].forEach(function(files) {
      if (!files || typeof files !== 'object') return;
      Object.keys(files).forEach(function(path) {
        var identity = files[path];
        if (Array.isArray(identity) && identity.length >= 3)
          scope.add(JSON.stringify(['file', identity[2] || path]));
      });
    });
    return {scope: scope, identities: identities};
  }
  /** @param {JobsConfig} cfg @param {Map<number,string>} identities */ function paidCarriedScope(cfg, identities) {
    var paid = new Set(), carried = new Set(cfg.carry_photo_ids || []);
    (cfg.untagged_photo_ids || []).forEach(function(pid) { carried.delete(pid); });
    (cfg.paid_tag_photo_ids || []).forEach(function(pid) { carried.add(pid); });
    carried.forEach(function(pid) {
      if (!identities.has(pid)) return;
      var token = identities.get(pid); if (token === undefined) return;
      paid.add(token);
      var identity = JSON.parse(token)[2], index = identity.lastIndexOf('|h=');
      if (index >= 0 && identity.substring(index + 3))
        paid.add(JSON.stringify(['file', identity.substring(index + 3)]));
    });
    return paid;
  }
  var parentCfg = jobConfig(parent) || {};
  var parentScope = tagScope(parentCfg, parentResult);
  var tagIdentities = new Map(parentScope.identities);
  var paidScope = new Set(parentResult.tags_applied ? parentScope.scope : []);
  paidCarriedScope(parentCfg, tagIdentities).forEach(function(token) { paidScope.add(token); });
  var allScope = new Set(parentScope.scope);
  var inheritedScopes = new Map([[parent.id, parentScope.scope]]);
  descendants.slice().sort(function(a, b) { return a.startedAt.localeCompare(b.startedAt); }).forEach(function(e) {
    var own = tagScope(e.config, e.result);
    (inheritedScopes.get(e.parent || '') || inheritedScopes.get(e.root || '') || new Set()).forEach(function(token) { own.scope.add(token); });
    inheritedScopes.set(e.id, own.scope);
    own.identities.forEach(function(token, pid) { tagIdentities.set(pid, token); });
    own.scope.forEach(function(token) {
      allScope.add(token);
      if (e.tagsApplied) paidScope.add(token);
    });
    paidCarriedScope(e.config, own.identities).forEach(function(token) { paidScope.add(token); });
  });
  var unpaidScope = Array.from(allScope).some(function(token) { return !paidScope.has(token); });
  if (allScope.size) {
    descendants.forEach(function(e) {
      if (e.result.after_import_skipped === 'no new photos') e.chained = false;
    });
  }
  var tagsApplied = (!!parentResult.tags_applied ||
    descendants.some(function(e) { return e.tagsApplied; })) && !unpaidScope;
  // The parent's chain step marks after a failed import too, but then
  // skips the collection and processing; apply the same ``ok`` filter
  // to the parent's own mark as to a descendant's (above). That is also
  // why a failed-files parent still owes processing to its Retry.
  var parentChainIds = importChainProcessJobIds(parentCfg, parentResult, rows, parent.id);
  var chained = (!!parentCfg.chain_already_ran ||
    (!!parentResult.chained && parentResult.ok !== false) ||
    (!('chain_already_ran' in parentCfg) && importFailedBeforeStart(parentResult) && parentChainIds.length > 0)) &&
    !importChainInvalidated(parentCfg, parentResult, rows, parent.id, neverStartedProcesses) ||
    descendants.some(function(e) { return e.chained; });
  if (allScope.size && parentResult.after_import_skipped === 'no new photos') {
    chained = descendants.some(function(e) { return e.chained; });
  }
  // A Retry of a parent that was not interrupted replays no tags.
  var tagsPaid = (tagsApplied || !parentInterrupted) && !unpaidScope;
  var marked = descendants.filter(function(e) { return e.tagsApplied || e.chained; });
  // Failing files after its tag pass makes a descendant's own Retry the
  // next step; one cancelled or crashed before that did none of the work.
  var nextSteps = descendants.filter(function(e) {
    return e.resumable || (e.hasFailedFiles && e.tagsApplied);
  });
  /** @param {JobsImportEntry[]} entries */ function newest(entries) {
    return entries.reduce(function(best, e) {
      return e.startedAt > best.startedAt ? e : best;
    });
  }
  var by = null;
  var kind = null;
  if (marked.length && tagsPaid && chained) {
    by = newest(marked);
    kind = 'done';
  } else if (nextSteps.length) {
    by = newest(nextSteps);
    kind = by.resumable ? 'resume' : 'retry';
  }
  return {tagsApplied: tagsApplied, chained: chained, by: by ? by.id : null,
          byStartedAt: by ? by.startedAt : null, kind: kind,
          parentInterrupted: parentInterrupted};
}

/** @param {JobsTakeover} takeover */ function importTakeoverNote(takeover) {
  var when = takeover.byStartedAt ? new Date(takeover.byStartedAt) : null;
  var which = when && !isNaN(when.getTime())
    ? 'the import started ' + window.VireoTime.dateTime(when)
    : 'a later import';
  var resuming = takeover.parentInterrupted;
  var lead = (resuming ? 'Already resumed by ' : 'Already retried by ') + which;
  if (takeover.kind === 'done') {
    return lead + ', which finished the work this import owed. Nothing is left to ' +
      (resuming ? 'resume.' : 'retry.');
  }
  if (takeover.kind === 'resume') {
    return lead + ', which was interrupted' + (resuming ? ' too' : '') +
      '. Resume that import instead; it carries these photos.';
  }
  return lead + ', which had files fail. Retry them from that import instead; it carries these photos.';
}

/** @param {JobsRow} job */ function importResumeHint(job) {
  var landed = job.result?.photo_ids?.length || 0;
  // Files are recorded as landed before they're cataloged, so the
  // checkpointed photo ids can lag them. Count photos only when the ids
  // cover every landed file (a RAW+JPEG pair is two files, one photo).
  var landedFiles = Object.keys(job.result?.landed_files || {}).length;
  if (!landed && !landedFiles) {
    return 'Same source and destination. Nothing was imported before it stopped.';
  }
  var hint = landedFiles > landed
    ? 'Same source and destination. Files it had already imported won\'t be copied again'
    : 'Same source and destination. The ' + landed.toLocaleString() +
      ' photo' + (landed === 1 ? '' : 's') + ' already imported won\'t be copied again';
  if (jobConfig(job).after_import != null) {
    hint += '; they\'re processed with the rest';
  }
  return hint + '.';
}

/** @param {JobsRow | undefined} job */ function importRetryBody(job) {
  var cfg = jobConfig(job);
  if (!job || job.type !== 'import' || cfg.mode === 'in_place' ||
      !Array.isArray(cfg.sources) || !cfg.sources.length) return null;
  // parent_import_job_id lets the server bind carry_photo_ids to this
  // parent's imported scope and, when the parent used a remote
  // target, verify the target still resolves to the enqueue-time
  // host/root/mount. Without a parent id the server rejects the
  // carry list, so the retry couldn't chain properly.
  if (!job.id) return null;
  // See import.html retryBodyFromFinishedJob for the full rationale:
  // an empty custom folder_template means "copy into the archive
  // root" and must not be overwritten by the default; the parent's
  // skip_duplicates setting must not be forced on, or a failed file
  // that matches some other catalog entry gets skipped again instead
  // of copied.
  var folderTemplate = cfg.folder_template != null
    ? cfg.folder_template : '%Y/%Y-%m-%d';
  var parentSkipDuplicates = cfg.skip_duplicates != null
    ? !!cfg.skip_duplicates : true;
  /** @type {JobsConfig} */ var body = {
    sources: cfg.sources.slice(),
    destination: cfg.destination,
    recursive: cfg.recursive !== false,
    folder_template: folderTemplate,
    file_types: cfg.file_types || 'both',
    skip_duplicates: parentSkipDuplicates,
    verify_by_hash: !!cfg.verify_by_hash,
    trust_likely_duplicates: !!cfg.trust_likely_duplicates,
    after_import: cfg.after_import == null ? null : cfg.after_import,
    tags: Array.isArray(cfg.tags) ? cfg.tags.slice() : [], location_from_gps: !!cfg.location_from_gps,
    location_choices: Array.isArray(cfg.location_choices) ? cfg.location_choices.slice() : [],
    allow_missing_exiftool: !!cfg.allow_missing_exiftool,
    parent_import_job_id: job.id,
  };
  // Recover the complete original scope. See import.html for the full
  // rationale; the summary: the parent's failed run skipped chaining
  // entirely, so previously-landed files still need processing, and
  // on repeated retries the accumulated carry list has to keep
  // growing rather than reset to only the last retry's landed files.
  var parentCarry = Array.isArray(cfg.carry_photo_ids)
    ? cfg.carry_photo_ids : [];
  // Everything the parent carried (its own carry list plus photos a
  // resume recovered from an interrupted run) stays in scope too.
  /** @type {number[]} */ var parentImported = [];
  [/** @type {const} */ ('photo_ids'), /** @type {const} */ ('carried_photo_ids'), /** @type {const} */ ('recovered_photo_ids')].forEach(function(key) {
    var ids = job.result && job.result[key];
    if (Array.isArray(ids)) parentImported = parentImported.concat(ids);
  });
  /** @type {number[]} */ var carry = [];
  /** @type {Record<number,boolean>} */ var carrySeen = {};
  parentCarry.concat(parentImported).forEach(function(pid) {
    if (typeof pid !== 'number' || !Number.isInteger(pid) || pid <= 0) return;
    if (carrySeen[pid]) return;
    carrySeen[pid] = true;
    carry.push(pid);
  });
  if (carry.length) body.carry_photo_ids = carry;
  // Recover the parent's per-file selection so the retry stays scoped
  // to the same files. See import.html retryBodyFromFinishedJob for the
  // full rationale: without this the retry either fails the drift check
  // or, once past it, silently re-imports the files the user
  // deselected on the parent run. The three selection fields must
  // travel together — the server 400s on a partial set.
  if (Array.isArray(cfg.include_paths) && cfg.include_paths.length
      && typeof cfg.previewed_count === 'number'
      && typeof cfg.checked_count === 'number') {
    body.include_paths = cfg.include_paths.slice();
    body.previewed_count = cfg.previewed_count;
    body.checked_count = cfg.checked_count;
  }
  if (cfg.remote_target_id) {
    delete body.destination;
    body.remote_target_id = cfg.remote_target_id;
    body.remote_subpath = cfg.remote_subpath;
  }
  if (cfg.after_process_move && cfg.after_process_move.remote_target_id) {
    body.after_process_move = {
      remote_target_id: cfg.after_process_move.remote_target_id,
    };
  }
  body.local_processing = !!cfg.local_processing;
  body.defer_nas_transfer = !!cfg.defer_nas_transfer;
  return body;
}
