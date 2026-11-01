// Who started a job and what it follows: the origin chip, the follow-up
// line, the repeat note, and grouping a list into operations (a root job
// followed by the jobs it started). See job_runner/lineage.py.

import { formatDateTime, formatJobType } from './shared.js';

/** @param {JobsRow} job */ export function isSystemJob(job) {
  return job.origin === 'system';
}

/** @param {JobsRow} job @param {string} filter */ export function matchesOriginFilter(job, filter) {
  if (!filter) return true;
  return filter === 'system' ? isSystemJob(job) : !isSystemJob(job);
}

/**
 * Find a job by id among the rows the page has (active first, then history).
 * @param {string | null | undefined} id @param {JobsRow[][]} lists
 */
export function findJob(id, lists) {
  if (!id) return null;
  for (var i = 0; i < lists.length; i++) {
    var hit = (lists[i] || []).find(function(j) { return j.id === id; });
    if (hit) return hit;
  }
  return null;
}

/**
 * The sentence naming what a follow-up job follows, or '' for a root job.
 * @param {JobsRow} job @param {JobsRow | null} parent
 */
export function followUpText(job, parent) {
  if (!job.parent_job_id) return '';
  if (!parent) return 'Follow-up of an earlier job';
  var owner = isSystemJob(parent) ? "Vireo's " : 'your ';
  return 'Following ' + owner + formatJobType(parent.type);
}

/**
 * How often a system task failed with this error in a row, or '' for one run.
 * Only failures with the same error collapse into one row (history.py).
 * @param {JobsRow} job
 */
export function repeatText(job) {
  var count = Number(job.repeat_count || 1);
  if (!(count > 1)) return '';
  var since = job.repeat_since ? ' since ' + formatDateTime(new Date(job.repeat_since)) : '';
  return 'Failed ' + count + ' times in a row' + since;
}

/**
 * Group rows into operations, keeping the rows' order for the roots and
 * putting each root's follow-ups (at any depth) after it, oldest first. A
 * row whose parent is not in ``rows`` is a root of its own.
 * @param {JobsRow[]} rows
 * @returns {{root: JobsRow, followUps: JobsRow[]}[]}
 */
export function groupOperations(rows) {
  /** @type {Record<string, JobsRow>} */ var byId = {};
  rows.forEach(function(j) { byId[j.id] = j; });
  /** @type {Record<string, JobsRow[]>} */ var children = {};
  /** @type {JobsRow[]} */ var roots = [];
  rows.forEach(function(j) {
    var parentId = j.parent_job_id;
    if (parentId && byId[parentId] && parentId !== j.id) {
      (children[parentId] = children[parentId] || []).push(j);
    } else {
      roots.push(j);
    }
  });
  /** @type {Record<string, boolean>} */ var seen = {};
  /** @param {JobsRow} job @param {JobsRow[]} out */
  function collect(job, out) {
    (children[job.id] || []).slice().sort(function(a, b) {
      return String(a.started_at).localeCompare(String(b.started_at));
    }).forEach(function(child) {
      if (seen[child.id]) return;
      seen[child.id] = true;
      out.push(child);
      collect(child, out);
    });
  }
  var groups = roots.map(function(root) {
    seen[root.id] = true;
    /** @type {JobsRow[]} */ var followUps = [];
    collect(root, followUps);
    return { root: root, followUps: followUps };
  });
  // A parent cycle (never written by the runner) leaves rows no root
  // reaches; show them rather than drop them.
  rows.forEach(function(j) {
    if (!seen[j.id]) groups.push({ root: j, followUps: [] });
  });
  return groups;
}
